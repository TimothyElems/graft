package queue

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/runner"
)

// SaveAttempt stores what one attempt printed (the runner's last 8 KB) and how it
// ended, for the support bundle. Fenced like events: only the worker holding the
// run's lease can write, so a worker that lost its lease adds nothing (ErrLeaseLost).
//
// Step output is arbitrary bytes; Postgres text refuses NUL and invalid UTF-8, so
// both are replaced rather than losing the whole log to one bad byte.
func SaveAttempt(ctx context.Context, pool *pgxpool.Pool, runID string, claim int, owner string, a runner.Attempt, r runner.Result) error {
	out := strings.ToValidUTF8(strings.ReplaceAll(r.Output, "\x00", "�"), "�")
	tag, err := pool.Exec(ctx, `
		INSERT INTO attempt_logs (run_id, claim, step_id, attempt, outcome, exit_code, took_ms, output)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8
		 WHERE EXISTS (SELECT 1 FROM runs WHERE id = $1 AND lease_owner = $9 AND status = 'running')`,
		runID, claim, a.StepID, a.N, string(r.Outcome), r.ExitCode, r.Took.Milliseconds(), out, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
