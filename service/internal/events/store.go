// Package events stores each run's events with a per-run seq that starts at 1 and
// has no gaps, so a client can resume with ?after=<seq> and miss nothing.
//
// Why not a Postgres SEQUENCE: nextval() is never rolled back, so any failed
// transaction leaves a hole. Instead each run row carries event_seq, bumped by
// UPDATE in the same transaction that inserts the events. The UPDATE takes the run's
// row lock, so concurrent writers to one run take turns, and a rollback undoes the
// bump together with the inserts.
package events

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"graft/kernel/pipeline"
	"graft/service/internal/db"
)

// Channel is the Postgres NOTIFY channel Append signals on, with the run id as payload.
const Channel = "graft_events"

// Event is one row of the events table, in the contract's shape.
type Event struct {
	ID       string    `json:"id"`
	Seq      int       `json:"seq"`
	RunID    string    `json:"run_id"`
	BranchID string    `json:"branch_id"`
	At       time.Time `json:"at"`
	Type     string    `json:"type"`
	AtMs     int64     `json:"at_ms"`
	Step     *string   `json:"step"`
	Attempt  int       `json:"attempt"`
	Reason   *string   `json:"reason"`
}

// New is an event to append: what the kernel (or the service) decided.
type New struct {
	Type    string
	AtMs    int64
	Step    string // "" for run-level events
	Attempt int
	Reason  string // "" for none
}

func newID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return "evt_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

// Append adds events to a run inside tx and returns the last seq written.
// It takes a pgx.Tx, not a pool, on purpose: outside a transaction the counter bump
// and the inserts could commit separately, which is exactly how gaps happen.
func Append(ctx context.Context, tx pgx.Tx, runID, branchID string, evs []New) (int, error) {
	if len(evs) == 0 {
		return 0, nil
	}
	var last int
	if err := tx.QueryRow(ctx, `UPDATE runs SET event_seq = event_seq + $2 WHERE id = $1 RETURNING event_seq`,
		runID, len(evs)).Scan(&last); err != nil {
		return 0, fmt.Errorf("bump event_seq for %s: %w", runID, err)
	}
	return last, insert(ctx, tx, runID, branchID, last, evs)
}

// insert writes evs as seq last-len+1 .. last, and wakes stream subscribers.
func insert(ctx context.Context, tx pgx.Tx, runID, branchID string, last int, evs []New) error {
	first := last - len(evs) + 1
	batch := &pgx.Batch{}
	for i, e := range evs {
		batch.Queue(`INSERT INTO events (id, run_id, branch_id, seq, type, at_ms, step, attempt, reason)
		             VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, NULLIF($9, ''))`,
			newID(), runID, branchID, first+i, e.Type, e.AtMs, e.Step, e.Attempt, e.Reason)
	}
	// Wake stream subscribers. NOTIFY is transactional: Postgres delivers it only if
	// this transaction commits, and after the rows are visible. The payload is a hint
	// (which run); subscribers always read the events themselves, by seq.
	batch.Queue(`SELECT pg_notify($1, $2)`, Channel, runID)
	return tx.SendBatch(ctx, batch).Close()
}

// FromKernel converts kernel events to events to append.
func FromKernel(kevs []pipeline.Event) []New {
	evs := make([]New, 0, len(kevs))
	for _, k := range kevs {
		evs = append(evs, New{Type: k.Type, AtMs: k.AtMs, Step: k.Step, Attempt: k.Attempt, Reason: k.Reason})
	}
	return evs
}

// ErrNotOwner: the run is not running under this owner's lease any more.
var ErrNotOwner = errors.New("run is not leased to this owner")

// AppendFenced is Append for a worker: it only writes if `owner` still holds the
// run's lease, so a worker that lost its lease cannot add events to a run another
// worker now owns. The fence and the counter bump are the same UPDATE.
func AppendFenced(ctx context.Context, tx pgx.Tx, runID, branchID, owner string, evs []New) (int, error) {
	if len(evs) == 0 {
		return 0, nil
	}
	var last int
	err := tx.QueryRow(ctx, `UPDATE runs SET event_seq = event_seq + $3
		WHERE id = $1 AND lease_owner = $2 AND status = 'running' RETURNING event_seq`, runID, owner, len(evs)).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotOwner
	}
	if err != nil {
		return 0, err
	}
	return last, insert(ctx, tx, runID, branchID, last, evs)
}

// List returns a run's events with seq > after, oldest first, at most limit of them.
func List(ctx context.Context, q db.DBTX, runID string, after, limit int) ([]Event, error) {
	rows, err := q.Query(ctx, `
		SELECT id, seq, run_id, branch_id, at, type, at_ms, step, attempt, reason
		  FROM events WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, runID, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.Seq, &e.RunID, &e.BranchID, &e.At, &e.Type, &e.AtMs, &e.Step, &e.Attempt, &e.Reason)
		return e, err
	})
}
