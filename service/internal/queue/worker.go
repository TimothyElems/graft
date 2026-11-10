package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/kernel/pipeline"
	"graft/service/internal/events"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

// Worker claims runs, executes them through the shell, and records the kernel's result.
type Worker struct {
	Pool      *pgxpool.Pool
	Shell     sched.Shell
	Owner     string        // unique per process, e.g. hostname-pid
	Lease     time.Duration // default DefaultLease
	Heartbeat time.Duration // default DefaultHeartbeat
	Idle      time.Duration // sleep when the queue is empty; default 200ms
	Log       *slog.Logger
}

func (w *Worker) defaults() {
	if w.Lease == 0 {
		w.Lease = DefaultLease
	}
	if w.Heartbeat == 0 {
		w.Heartbeat = DefaultHeartbeat
	}
	if w.Idle == 0 {
		w.Idle = 200 * time.Millisecond
	}
	if w.Log == nil {
		w.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
}

// Run processes runs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.defaults()
	for ctx.Err() == nil {
		did, err := w.Step(ctx)
		if err != nil && ctx.Err() == nil {
			w.Log.Error("worker step", "owner", w.Owner, "err", err)
		}
		if !did {
			select {
			case <-ctx.Done():
			case <-time.After(w.Idle):
			}
		}
	}
}

// Step claims and processes at most one run. It reports whether it claimed one.
func (w *Worker) Step(ctx context.Context) (bool, error) {
	w.defaults()
	c, err := Claim(ctx, w.Pool, w.Owner, w.Lease)
	if err != nil || c == nil {
		return false, err
	}
	var spec store.RunSpec
	if err := json.Unmarshal(c.Spec, &spec); err != nil {
		return true, w.finish(ctx, c, spec, nil, fmt.Errorf("bad stored spec: %w", err))
	}

	// Heartbeat while working. If the lease is lost, cancel the work: someone else owns it.
	work, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go Heartbeat(work, w.Pool, c.ID, w.Owner, w.Lease, w.Heartbeat, func(err error) { cancel(err) })

	res, runErr := w.Shell.Execute(spec)
	if cause := context.Cause(work); cause != nil && errors.Is(cause, ErrLeaseLost) {
		w.Log.Warn("lease lost mid-run; dropping result", "run_id", c.ID, "owner", w.Owner)
		return true, ErrLeaseLost
	}
	if runErr != nil {
		return true, w.finish(ctx, c, spec, nil, runErr)
	}
	return true, w.finish(ctx, c, spec, &res, nil)
}

func (w *Worker) finish(ctx context.Context, c *Claimed, spec store.RunSpec, res *pipeline.Result, runErr error) error {
	err := Finish(ctx, w.Pool, c.ID, c.BranchID, w.Owner, spec, res, runErr)
	if errors.Is(err, ErrLeaseLost) {
		w.Log.Warn("lease lost before commit; result dropped", "run_id", c.ID, "owner", w.Owner)
	}
	return err
}

// Finish records a run's outcome, its step results and its events in ONE transaction,
// and only if owner still holds the lease (a fence). A worker that lost its lease
// gets ErrLeaseLost and changes nothing, so a run is finished exactly once.
func Finish(ctx context.Context, pool *pgxpool.Pool, runID, branchID, owner string, spec store.RunSpec,
	res *pipeline.Result, runErr error) error {
	status, errText := "failed", ""
	if runErr != nil {
		errText = runErr.Error()
	} else {
		status = res.RunStatus
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE runs SET status = $3, finished_at = now(), lease_owner = NULL, lease_until = NULL,
			                error = NULLIF($4, '')
			 WHERE id = $1 AND lease_owner = $2 AND status = 'running'`, runID, owner, status, errText)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		if res == nil {
			return nil
		}
		for i, st := range spec.Steps {
			r := res.Steps[st.ID]
			if _, err := tx.Exec(ctx, `
				INSERT INTO step_runs (run_id, step_id, position, status, attempts, started_ms, finished_ms, reason)
				VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''))`,
				runID, st.ID, i, r.Status, r.Attempts, r.StartedMs, r.FinishedMs, r.Reason); err != nil {
				return err
			}
		}
		_, err = events.AppendKernel(ctx, tx, runID, branchID, res.Events)
		return err
	})
}
