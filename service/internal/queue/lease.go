package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/db"
)

// Production lease timing. The heartbeat is a third of the lease, so one missed
// beat (a GC pause, a slow query) does not lose the run.
const (
	DefaultLease     = 30 * time.Second
	DefaultHeartbeat = 10 * time.Second
)

// ErrLeaseLost means another worker now owns the run. Stop working on it and
// do not write its result: the new owner will.
var ErrLeaseLost = errors.New("lease lost: another worker owns this run")

// Extend renews owner's lease and reports whether a cancel was requested. It only
// touches a run this owner still holds, so a worker that lost its lease cannot take
// it back by heartbeating.
func Extend(ctx context.Context, q db.DBTX, runID, owner string, lease time.Duration) (cancelRequested bool, err error) {
	err = q.QueryRow(ctx, `
		UPDATE runs SET lease_until = now() + make_interval(secs => $3)
		 WHERE id = $1 AND lease_owner = $2 AND status = 'running'
		RETURNING cancel_requested`, runID, owner, lease.Seconds()).Scan(&cancelRequested)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrLeaseLost
	}
	return cancelRequested, err
}

// CancelRequested is the cheap check the worker makes between heartbeats.
func CancelRequested(ctx context.Context, q db.DBTX, runID string) (bool, error) {
	var c bool
	err := q.QueryRow(ctx, `SELECT cancel_requested FROM runs WHERE id = $1`, runID).Scan(&c)
	return c, err
}

// Release hands a run back to the queue (graceful shutdown): status queued, no lease,
// so another worker claims it at once instead of waiting for the lease to lapse.
func Release(ctx context.Context, q db.DBTX, runID, owner string) error {
	_, err := q.Exec(ctx, `UPDATE runs SET status = 'queued', lease_owner = NULL, lease_until = NULL
		WHERE id = $1 AND lease_owner = $2 AND status = 'running'`, runID, owner)
	return err
}

// Heartbeat extends the lease every interval and checks for a cancel every cancelPoll,
// until ctx ends. On a lost lease it calls lost() once and returns; on a cancel request
// it calls cancel() once and keeps heartbeating (the run is still being wound down).
func Heartbeat(ctx context.Context, pool *pgxpool.Pool, runID, owner string, lease, interval, cancelPoll time.Duration,
	lost func(error), cancel func()) {
	beat := time.NewTicker(interval)
	defer beat.Stop()
	poll := time.NewTicker(cancelPoll)
	defer poll.Stop()
	asked := false
	ask := func(c bool) {
		if c && !asked {
			asked = true
			cancel()
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-beat.C:
			c, err := Extend(ctx, pool, runID, owner, lease)
			if err != nil {
				if ctx.Err() == nil {
					lost(err)
				}
				return
			}
			ask(c)
		case <-poll.C:
			if c, err := CancelRequested(ctx, pool, runID); err == nil {
				ask(c)
			}
		}
	}
}
