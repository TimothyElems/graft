package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/pgtest"
	"graft/service/internal/queue"
)

// seed inserts one project, one branch and n queued runs (run_0000 ...).
func seed(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO projects (slug) VALUES ('demo');
		INSERT INTO branches (id, project, name, status, expires_at) VALUES ('br_q', 'demo', 'q', 'ready', now() + interval '1 day');`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO runs (id, branch_id, status, spec, created_at)
		SELECT 'run_' || lpad(i::text, 4, '0'), 'br_q', 'queued', '{"steps":[{"id":"a"}]}', now() + i * interval '1 microsecond'
		FROM generate_series(0, $1 - 1) AS i`, n)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEightWorkersClaimAThousandJobsEachExactlyOnce(t *testing.T) {
	const workers, jobs = 8, 1000
	pool := pgtest.New(t)
	seed(t, pool, jobs)
	ctx := context.Background()

	var mu sync.Mutex
	claimedBy := map[string][]string{} // run id -> workers that claimed it
	perWorker := make([]int, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", w)
			conn, err := pool.Acquire(ctx) // each worker on its own connection, like separate processes
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Release()
			<-start
			for {
				c, err := queue.Claim(ctx, conn, owner, 30*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				if c == nil {
					return
				}
				mu.Lock()
				claimedBy[c.ID] = append(claimedBy[c.ID], owner)
				perWorker[w]++
				mu.Unlock()
			}
		}(w)
	}
	close(start)
	wg.Wait()

	dupes := 0
	for id, by := range claimedBy {
		if len(by) != 1 {
			dupes++
			if dupes <= 3 {
				t.Errorf("%s claimed %d times, by %v", id, len(by), by)
			}
		}
	}
	if dupes > 0 || len(claimedBy) != jobs {
		t.Fatalf("%d distinct runs claimed (want %d), %d claimed more than once", len(claimedBy), jobs, dupes)
	}
	var stillQueued, badClaims int
	_ = pool.QueryRow(ctx, "SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE claims <> 1) FROM runs").
		Scan(&stillQueued, &badClaims)
	if stillQueued != 0 || badClaims != 0 {
		t.Fatalf("after draining: %d still queued, %d runs with claims <> 1", stillQueued, badClaims)
	}
	t.Logf("claims per worker: %v (all %d runs claimed exactly once)", perWorker, jobs)
}

func TestClaimTakesOldestFirstAndReturnsNilWhenEmpty(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool, 3)
	ctx := context.Background()
	for _, want := range []string{"run_0000", "run_0001", "run_0002"} {
		c, err := queue.Claim(ctx, pool, "w", time.Minute)
		if err != nil || c == nil || c.ID != want || c.Claims != 1 {
			t.Fatalf("got %+v, %v; want %s", c, err, want)
		}
	}
	if c, err := queue.Claim(ctx, pool, "w", time.Minute); c != nil || err != nil {
		t.Fatalf("empty queue: got %+v, %v", c, err)
	}
}
