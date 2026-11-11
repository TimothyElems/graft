package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"graft/kernel/pipeline"
	"graft/service/internal/events"
	"graft/service/internal/runner"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

// Worker claims runs and executes them in real time through the shell.
//
// Execution is at-least-once, completion is exactly once: if a worker dies mid-run,
// another reclaims the run after its lease lapses, removes the dead worker's leftover
// containers, and runs the pipeline again from the start (run_started says so).
// Steps should therefore be idempotent. Only one worker can ever record the result.
type Worker struct {
	Pool       *pgxpool.Pool
	Clock      sched.Clock
	Owner      string        // unique per process, e.g. hostname-pid-w0
	Lease      time.Duration // default DefaultLease
	Heartbeat  time.Duration // default DefaultHeartbeat
	CancelPoll time.Duration // how often a running run checks for a cancel request; default 1s
	Idle       time.Duration // sleep when the queue is empty; default 200ms
	// WriteTimeout bounds each database write the run makes (events, the finish).
	// Default 30s.
	WriteTimeout time.Duration
	Log          *slog.Logger
	Hooks        Hooks        // optional, for metrics
	Tracer       trace.Tracer // optional: a span per run
}

// Hooks lets observability watch the worker without the worker knowing about it.
type Hooks struct {
	Claimed  func(claims int, was string)         // a run was claimed; claims > 1 is a restart (was: queued=released, running=lease expired)
	Events   func([]pipeline.Event)               // events were recorded for a run
	Finished func(status string, d time.Duration) // a run's result was recorded
}

func (w *Worker) defaults() {
	if w.Lease == 0 {
		w.Lease = DefaultLease
	}
	if w.Heartbeat == 0 {
		w.Heartbeat = DefaultHeartbeat
	}
	if w.CancelPoll == 0 {
		w.CancelPoll = time.Second
	}
	if w.Idle == 0 {
		w.Idle = 200 * time.Millisecond
	}
	if w.WriteTimeout == 0 {
		w.WriteTimeout = 30 * time.Second
	}
	if w.Log == nil {
		w.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
}

// Run processes runs until ctx ends; see RunUntil.
func (w *Worker) Run(ctx context.Context) { w.RunUntil(ctx, ctx) }

// RunUntil claims new runs until `claim` ends, and lets a run in progress continue
// until `work` ends. Graceful shutdown cancels claim first (stop taking work), then
// work after a grace period: a run still going then is stopped (no attempt left
// running) and released back to the queue for another worker.
func (w *Worker) RunUntil(claim, work context.Context) {
	w.defaults()
	for claim.Err() == nil {
		did, err := w.StepWith(claim, work)
		if err != nil && claim.Err() == nil {
			w.Log.Error("worker step", "owner", w.Owner, "err", err)
		}
		if !did {
			select {
			case <-claim.Done():
			case <-time.After(w.Idle):
			}
		}
	}
}

// Step claims and processes at most one run. It reports whether it claimed one.
func (w *Worker) Step(ctx context.Context) (bool, error) { return w.StepWith(ctx, ctx) }

// StepWith claims under `claim` and runs under `work` (see RunUntil).
func (w *Worker) StepWith(claimCtx, ctx context.Context) (bool, error) {
	w.defaults()
	if claimCtx.Err() != nil {
		return false, nil
	}
	c, err := Claim(claimCtx, w.Pool, w.Owner, w.Lease)
	if err != nil || c == nil {
		return false, err
	}
	if w.Hooks.Claimed != nil {
		w.Hooks.Claimed(c.Claims, c.Was)
	}
	if w.Tracer != nil {
		var span trace.Span
		ctx, span = w.Tracer.Start(ctx, "run", trace.WithAttributes(attribute.String("run_id", c.ID),
			attribute.String("branch_id", c.BranchID), attribute.Int("claims", c.Claims)))
		defer span.End()
		w.Log.InfoContext(ctx, "run claimed", "run_id", c.ID, "branch_id", c.BranchID, "claims", c.Claims)
	}
	began := time.Now()
	// Writes that must still happen while shutting down: not cancelled with ctx, but
	// keeping its values (the run's trace span). Each write gets its own deadline
	// (w.bounded): a deadline for the whole run would fail every run that outlasts it.
	bg := context.WithoutCancel(ctx)

	var spec store.RunSpec
	if err := json.Unmarshal(c.Spec, &spec); err != nil {
		return true, w.finish(bg, c, spec, nil, nil, fmt.Errorf("bad stored spec: %w", err), began)
	}

	started := ""
	if c.Claims > 1 {
		// The previous owner died holding this run. Its containers may still be running.
		cctx, done := w.bounded(bg)
		err := w.Clock.Runner.Cleanup(cctx, c.ID)
		done()
		if err != nil {
			w.Log.Warn("cleanup after lost lease", "run_id", c.ID, "err", err)
		}
		why := "the previous worker lost its lease"
		if c.Was == "queued" {
			why = "the previous worker shut down and released it"
		}
		started = fmt.Sprintf("restarted (claim %d): %s", c.Claims, why)
	}
	if err := w.append(bg, c, []pipeline.Event{{Type: "run_started", Reason: started}}); err != nil {
		return true, err
	}

	work, stopWork := context.WithCancelCause(ctx)
	defer stopWork(nil)
	cancel := make(chan struct{})
	var once sync.Once
	requestCancel := func() { once.Do(func() { close(cancel) }) }
	qctx, qdone := w.bounded(bg)
	asked, err := CancelRequested(qctx, w.Pool, c.ID)
	qdone()
	if err == nil && asked {
		requestCancel() // cancelled while still queued: rule 5 at time 0
	}
	go Heartbeat(work, w.Pool, c.ID, w.Owner, w.Lease, w.Heartbeat, w.CancelPoll,
		func(err error) { stopWork(err) }, requestCancel)

	clock := w.Clock // a copy per run: the hook knows this run's claim
	clock.Attempted = func(a runner.Attempt, r runner.Result) {
		actx, done := w.bounded(bg)
		defer done()
		// Losing one log is not worth failing the run over; a lost lease shows up on the next event write.
		if err := SaveAttempt(actx, w.Pool, c.ID, c.Claims, w.Owner, a, r); err != nil {
			w.Log.Warn("attempt log not saved", "run_id", c.ID, "step", a.StepID, "attempt", a.N, "err", err)
		}
	}
	res, emitted, runErr := clock.Run(work, c.ID, spec, cancel, func(evs []pipeline.Event) error {
		return w.append(bg, c, evs)
	})
	switch {
	case errors.Is(context.Cause(work), ErrLeaseLost), errors.Is(runErr, ErrLeaseLost):
		w.Log.Warn("lease lost mid-run; result dropped", "run_id", c.ID, "owner", w.Owner)
		return true, ErrLeaseLost
	case errors.Is(runErr, sched.ErrInterrupted):
		// Shutting down: every attempt is already stopped. Hand the run back at once.
		rctx, rdone := w.bounded(bg)
		defer rdone()
		if err := Release(rctx, w.Pool, c.ID, w.Owner); err != nil {
			return true, err
		}
		w.Log.Info("run released on shutdown", "run_id", c.ID, "owner", w.Owner)
		return true, nil
	case runErr != nil:
		return true, w.finish(bg, c, spec, nil, nil, runErr, began)
	}
	return true, w.finish(bg, c, spec, &res, res.Events[emitted:], nil, began)
}

// bounded is the deadline for one database write (or one cleanup) made for a run.
func (w *Worker) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, w.WriteTimeout)
}

// append writes events as they happen, fenced: only while this worker holds the lease.
func (w *Worker) append(ctx context.Context, c *Claimed, kevs []pipeline.Event) error {
	ctx, done := w.bounded(ctx)
	defer done()
	err := pgx.BeginFunc(ctx, w.Pool, func(tx pgx.Tx) error {
		_, err := events.AppendFenced(ctx, tx, c.ID, c.BranchID, w.Owner, events.FromKernel(kevs))
		if errors.Is(err, events.ErrNotOwner) {
			return ErrLeaseLost
		}
		return err
	})
	if err == nil && w.Hooks.Events != nil {
		w.Hooks.Events(kevs)
	}
	return err
}

func (w *Worker) finish(ctx context.Context, c *Claimed, spec store.RunSpec, res *pipeline.Result, rest []pipeline.Event,
	runErr error, began time.Time) error {
	ctx, done := w.bounded(ctx)
	defer done()
	err := Finish(ctx, w.Pool, c.ID, c.BranchID, w.Owner, spec, res, rest, runErr)
	if err == nil && w.Hooks.Events != nil {
		w.Hooks.Events(rest)
	}
	if errors.Is(err, ErrLeaseLost) {
		w.Log.Warn("lease lost before commit; result dropped", "run_id", c.ID, "owner", w.Owner)
	}
	if err == nil && w.Hooks.Finished != nil {
		status := "failed"
		if res != nil {
			status = res.RunStatus
		}
		w.Hooks.Finished(status, time.Since(began))
	}
	return err
}

// Finish records a run's outcome, its step results and its remaining events in ONE
// transaction, and only if owner still holds the lease (a fence). A worker that lost
// its lease gets ErrLeaseLost and changes nothing, so a run is finished exactly once.
func Finish(ctx context.Context, pool *pgxpool.Pool, runID, branchID, owner string, spec store.RunSpec,
	res *pipeline.Result, rest []pipeline.Event, runErr error) error {
	status, errText := "failed", ""
	if runErr != nil {
		errText = runErr.Error()
	} else {
		status = res.RunStatus
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if res == nil { // the run could not execute at all: say so in the log too
			rest = []pipeline.Event{{Type: "run_finished", Reason: "failed"}}
		}
		if _, err := events.AppendFenced(ctx, tx, runID, branchID, owner, events.FromKernel(rest)); err != nil {
			if errors.Is(err, events.ErrNotOwner) {
				return ErrLeaseLost
			}
			return err
		}
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
		return nil
	})
}
