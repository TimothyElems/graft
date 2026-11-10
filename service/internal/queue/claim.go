// Package queue is graftd's job queue: a Postgres table, not a broker.
//
// A worker claims one run at a time with FOR UPDATE SKIP LOCKED and holds a lease
// on it. The lease is extended by a heartbeat; if the worker dies, the lease lapses
// and another worker's claim picks the run up again.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"graft/service/internal/db"
)

// Claimed is a run a worker now owns until LeaseUntil.
type Claimed struct {
	ID         string
	BranchID   string
	Spec       []byte
	Claims     int // 1 on the first claim; more means a worker died holding it
	LeaseUntil time.Time
	Was        string // status before this claim: queued (new or released) or running (lease lapsed)
}

// claimSQL takes the oldest claimable run: queued, or running with a lease that has
// run out. SKIP LOCKED makes concurrent workers take different rows instead of
// waiting for (or both taking) the same one. The lease time comes from the
// database's clock, so every worker judges expiry by the same clock. It also returns
// the status the run had, so a restart can say why: released (queued) or lost (running).
const claimSQL = `
WITH pick AS (
  SELECT id, status AS was FROM runs
   WHERE status = 'queued'
      OR (status = 'running' AND lease_until < now())
   ORDER BY created_at, id
   FOR UPDATE SKIP LOCKED
   LIMIT 1)
UPDATE runs
   SET status = 'running',
       lease_owner = $1,
       lease_until = now() + make_interval(secs => $2),
       claims = claims + 1
  FROM pick
 WHERE runs.id = pick.id
RETURNING runs.id, runs.branch_id, runs.spec, runs.claims, runs.lease_until, pick.was`

// Claim returns the next run for owner, or nil if there is nothing to do.
func Claim(ctx context.Context, q db.DBTX, owner string, lease time.Duration) (*Claimed, error) {
	var c Claimed
	err := q.QueryRow(ctx, claimSQL, owner, lease.Seconds()).Scan(&c.ID, &c.BranchID, &c.Spec, &c.Claims, &c.LeaseUntil, &c.Was)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}
