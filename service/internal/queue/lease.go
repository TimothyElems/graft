package queue

import (
	"context"
	"errors"
	"time"

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

// Extend renews owner's lease. It only touches a run this owner still holds,
// so a worker that lost its lease cannot take it back by heartbeating.
func Extend(ctx context.Context, q db.DBTX, runID, owner string, lease time.Duration) error {
	tag, err := q.Exec(ctx, `
		UPDATE runs SET lease_until = now() + make_interval(secs => $3)
		 WHERE id = $1 AND lease_owner = $2 AND status = 'running'`, runID, owner, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Heartbeat extends the lease every interval until ctx ends. If the lease is lost it
// calls lost() once and returns, so the caller can cancel the work it was doing.
func Heartbeat(ctx context.Context, pool *pgxpool.Pool, runID, owner string, lease, interval time.Duration, lost func(error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := Extend(ctx, pool, runID, owner, lease); err != nil {
				if ctx.Err() != nil {
					return
				}
				lost(err)
				return
			}
		}
	}
}
