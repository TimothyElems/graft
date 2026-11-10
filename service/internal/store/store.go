// Package store reads and writes branches and runs in Postgres. Every function
// takes a db.DBTX, so it can run alone or inside a caller's transaction (the
// Idempotency-Key handling needs the create and the stored response in one).
package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"graft/service/internal/db"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrNameTaken   = errors.New("branch name taken")
	ErrBranchLimit = errors.New("branch limit reached")
	ErrBadCursor   = errors.New("bad cursor")
)

// MaxLiveBranches per project. The contract's 409 example uses this number.
const MaxLiveBranches = 50

// StepSpec is one step as accepted by the API and stored in runs.spec (jsonb).
type StepSpec struct {
	ID          string   `json:"id"`
	Needs       []string `json:"needs,omitempty"`
	Image       string   `json:"image,omitempty"`
	Command     []string `json:"command,omitempty"`
	MaxAttempts int      `json:"max_attempts"`
	TimeoutMs   int64    `json:"timeout_ms"`
	BackoffMs   int64    `json:"backoff_ms"`
}

// RunSpec is the whole stored pipeline: runs.spec.
type RunSpec struct {
	Steps       []StepSpec `json:"steps"`
	Concurrency int        `json:"concurrency"`
}

type Branch struct {
	ID        string
	Project   string
	Name      string
	Status    string
	GitRef    *string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type StepResult struct {
	ID         string
	Status     string
	Attempts   int
	StartedMs  *int64
	FinishedMs *int64
	Reason     *string
}

type Run struct {
	ID         string
	BranchID   string
	Status     string
	Spec       RunSpec
	CreatedAt  time.Time
	FinishedAt *time.Time
	Steps      []StepResult // in spec order; "pending" until the run finishes
}

// Micro truncates to what Postgres stores (microseconds), so a value read back,
// or put in a cursor, compares equal to the stored one.
func Micro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// ------------------------------------------------------------------ branches

// CreateBranch inserts b, creating its project on first use. It locks the project
// row first, so concurrent creates cannot both pass the branch-limit check.
func CreateBranch(ctx context.Context, q db.DBTX, b Branch) (Branch, error) {
	b.CreatedAt, b.ExpiresAt = Micro(b.CreatedAt), Micro(b.ExpiresAt)
	if _, err := q.Exec(ctx, "INSERT INTO projects (slug) VALUES ($1) ON CONFLICT DO NOTHING", b.Project); err != nil {
		return Branch{}, err
	}
	var live int
	if err := q.QueryRow(ctx, `
		SELECT count(*) FROM branches
		 WHERE project = (SELECT slug FROM projects WHERE slug = $1 FOR UPDATE)
		   AND status NOT IN ('deleting','deleted')`, b.Project).Scan(&live); err != nil {
		return Branch{}, err
	}
	if live >= MaxLiveBranches {
		return Branch{}, ErrBranchLimit
	}
	_, err := q.Exec(ctx, `INSERT INTO branches (id, project, name, status, git_ref, created_at, expires_at)
	                       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		b.ID, b.Project, b.Name, b.Status, b.GitRef, b.CreatedAt, b.ExpiresAt)
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" && pe.ConstraintName == "branches_live_name" {
		return Branch{}, ErrNameTaken
	}
	return b, err
}

const branchCols = "id, project, name, status, git_ref, created_at, expires_at"

func scanBranch(r pgx.Row) (Branch, error) {
	var b Branch
	err := r.Scan(&b.ID, &b.Project, &b.Name, &b.Status, &b.GitRef, &b.CreatedAt, &b.ExpiresAt)
	b.CreatedAt, b.ExpiresAt = b.CreatedAt.UTC(), b.ExpiresAt.UTC()
	return b, err
}

func GetBranch(ctx context.Context, q db.DBTX, id string) (Branch, error) {
	b, err := scanBranch(q.QueryRow(ctx, "SELECT "+branchCols+" FROM branches WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Branch{}, ErrNotFound
	}
	return b, err
}

// Cursor is the position after the last row of a page: (created_at, id).
// Both are needed: created_at alone repeats or skips rows that share a timestamp.
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

func (c Cursor) Encode() string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "|" + c.ID))
}

func DecodeCursor(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, ErrBadCursor
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	us, err := strconv.ParseInt(ts, 10, 64)
	if !ok || err != nil || id == "" {
		return Cursor{}, ErrBadCursor
	}
	return Cursor{CreatedAt: time.UnixMicro(us).UTC(), ID: id}, nil
}

// ListBranches returns one page, newest first, and the cursor for the next page
// (nil at the end). It fetches limit+1 rows to know whether more exist.
func ListBranches(ctx context.Context, q db.DBTX, project string, after *Cursor, limit int) ([]Branch, *Cursor, error) {
	var at *time.Time
	var id *string
	if after != nil {
		at, id = &after.CreatedAt, &after.ID
	}
	rows, err := q.Query(ctx, "SELECT "+branchCols+` FROM branches
		WHERE project = $1 AND status <> 'deleted'
		  AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`, project, at, id, limit+1)
	if err != nil {
		return nil, nil, err
	}
	page, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Branch, error) { return scanBranch(r) })
	if err != nil {
		return nil, nil, err
	}
	if len(page) <= limit {
		return page, nil, nil
	}
	page = page[:limit]
	last := page[limit-1]
	return page, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// ------------------------------------------------------------------ runs

func CreateRun(ctx context.Context, q db.DBTX, id, branchID string, spec RunSpec, createdAt time.Time) (Run, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return Run{}, err
	}
	createdAt = Micro(createdAt)
	if _, err := q.Exec(ctx, `INSERT INTO runs (id, branch_id, status, spec, created_at) VALUES ($1, $2, 'queued', $3, $4)`,
		id, branchID, b, createdAt); err != nil {
		return Run{}, err
	}
	return Run{ID: id, BranchID: branchID, Status: "queued", Spec: spec, CreatedAt: createdAt, Steps: pending(spec)}, nil
}

func pending(spec RunSpec) []StepResult {
	out := make([]StepResult, len(spec.Steps))
	for i, s := range spec.Steps {
		out[i] = StepResult{ID: s.ID, Status: "pending"}
	}
	return out
}

func GetRun(ctx context.Context, q db.DBTX, id string) (Run, error) {
	var r Run
	var spec []byte
	err := q.QueryRow(ctx, `SELECT id, branch_id, status, spec, created_at, finished_at FROM runs WHERE id = $1`, id).
		Scan(&r.ID, &r.BranchID, &r.Status, &spec, &r.CreatedAt, &r.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, err
	}
	if err := json.Unmarshal(spec, &r.Spec); err != nil {
		return Run{}, fmt.Errorf("run %s: stored spec: %w", id, err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	if r.FinishedAt != nil {
		f := r.FinishedAt.UTC()
		r.FinishedAt = &f
	}
	rows, err := q.Query(ctx, `SELECT step_id, status, attempts, started_ms, finished_ms, reason
		FROM step_runs WHERE run_id = $1 ORDER BY position`, id)
	if err != nil {
		return Run{}, err
	}
	r.Steps, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (StepResult, error) {
		var s StepResult
		err := row.Scan(&s.ID, &s.Status, &s.Attempts, &s.StartedMs, &s.FinishedMs, &s.Reason)
		return s, err
	})
	if err != nil {
		return Run{}, err
	}
	if len(r.Steps) == 0 {
		r.Steps = pending(r.Spec)
	}
	return r, nil
}

func QueueDepth(ctx context.Context, q db.DBTX) (int, error) {
	var n int
	err := q.QueryRow(ctx, "SELECT count(*) FROM runs WHERE status = 'queued'").Scan(&n)
	return n, err
}
