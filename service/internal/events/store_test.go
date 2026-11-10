package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/events"
	"graft/service/internal/pgtest"
	"graft/service/internal/queue"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

func seed(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO projects (slug) VALUES ('demo');
		INSERT INTO branches (id, project, name, status, expires_at) VALUES ('br_e', 'demo', 'e', 'ready', now() + interval '1 day');
		INSERT INTO runs (id, branch_id, status, spec) VALUES ('run_e', 'br_e', 'running', '{}');`)
	if err != nil {
		t.Fatal(err)
	}
}

func seqs(t *testing.T, pool *pgxpool.Pool) []int {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT seq FROM events WHERE run_id = 'run_e' ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func appendN(ctx context.Context, pool *pgxpool.Pool, n int, label string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		evs := make([]events.New, n)
		for i := range evs {
			evs[i] = events.New{Type: "step_started", Step: fmt.Sprintf("%s_%d", label, i), Attempt: 1}
		}
		_, err := events.Append(ctx, tx, "run_e", "br_e", evs)
		return err
	})
}

func TestConcurrentWritersGetOneToNWithNoGapsOrRepeats(t *testing.T) {
	const writers, each = 20, 5
	ctx := context.Background()
	pool := pgtest.New(t)
	seed(t, pool)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			if err := appendN(ctx, pool, each, fmt.Sprintf("w%d", w)); err != nil {
				t.Error(err)
			}
		}(w)
	}
	wg.Wait()
	got := seqs(t, pool)
	if len(got) != writers*each {
		t.Fatalf("%d events, want %d", len(got), writers*each)
	}
	for i, s := range got {
		if s != i+1 {
			t.Fatalf("seq[%d] = %d: gap or repeat (want 1..%d)", i, s, writers*each)
		}
	}
	// Each writer's batch is contiguous: its events are not interleaved with another's.
	rows, _ := pool.Query(ctx, "SELECT step FROM events WHERE run_id = 'run_e' ORDER BY seq")
	steps, _ := pgx.CollectRows(rows, pgx.RowTo[string])
	for i := 0; i < len(steps); i += each {
		for j := 1; j < each; j++ {
			if steps[i+j] != fmt.Sprintf("%s_%d", steps[i][:len(steps[i])-2], j) {
				t.Fatalf("batch starting at seq %d is interleaved: %v", i+1, steps[i:i+each])
			}
		}
	}
}

func TestRolledBackAppendLeavesNoGap(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	seed(t, pool)
	boom := errors.New("boom")
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := events.Append(ctx, tx, "run_e", "br_e", []events.New{{Type: "step_started"}, {Type: "step_started"}}); err != nil {
			return err
		}
		return boom // something later in the same transaction fails
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if err := appendN(ctx, pool, 2, "ok"); err != nil {
		t.Fatal(err)
	}
	if got := seqs(t, pool); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("after a rollback, next events got seq %v; want [1 2]", got)
	}
}

// The state change and its events commit together or not at all.
func TestFinishIsAtomicWithItsEvents(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	seed(t, pool)
	_, _ = pool.Exec(ctx, "UPDATE runs SET lease_owner = 'w', lease_until = now() + interval '1 minute' WHERE id = 'run_e'")
	// Plant a row at seq 1 WITHOUT bumping the counter, so Finish's event insert will collide.
	_, _ = pool.Exec(ctx, "INSERT INTO events (id, run_id, branch_id, seq, type, at_ms) VALUES ('evt_x', 'run_e', 'br_e', 1, 'run_started', 0)")

	spec := store.RunSpec{Concurrency: 1, Steps: []store.StepSpec{{ID: "a", MaxAttempts: 1}}}
	res, _ := sched.Shell{Runner: sched.FakeRunner{}}.Execute(spec)
	err := queue.Finish(ctx, pool, "run_e", "br_e", "w", spec, &res, nil)
	if err == nil {
		t.Fatal("Finish should fail on the planted duplicate seq")
	}
	var status string
	var owner *string
	var stepRows, counter int
	_ = pool.QueryRow(ctx, `SELECT status, lease_owner, event_seq, (SELECT count(*) FROM step_runs WHERE run_id = 'run_e')
		FROM runs WHERE id = 'run_e'`).Scan(&status, &owner, &counter, &stepRows)
	if status != "running" || owner == nil || stepRows != 0 || counter != 0 {
		t.Fatalf("a failed Finish left partial state: status=%s owner=%v step_runs=%d event_seq=%d", status, owner, stepRows, counter)
	}
}

func TestListResumesAfterSeq(t *testing.T) {
	ctx := context.Background()
	pool := pgtest.New(t)
	seed(t, pool)
	if err := appendN(ctx, pool, 7, "s"); err != nil {
		t.Fatal(err)
	}
	got, err := events.List(ctx, pool, "run_e", 4, 100)
	if err != nil || len(got) != 3 || got[0].Seq != 5 || got[2].Seq != 7 {
		t.Fatalf("after=4: %v %v", got, err)
	}
	b, _ := json.Marshal(got[0])
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["reason"] != nil || m["step"] != "s_4" {
		t.Fatalf("contract shape: %s", b)
	}
	if page, _ := events.List(ctx, pool, "run_e", 0, 2); len(page) != 2 || page[1].Seq != 2 {
		t.Fatalf("limit: %v", page)
	}
}
