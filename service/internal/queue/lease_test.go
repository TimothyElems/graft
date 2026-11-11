package queue_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/db"
	"graft/service/internal/pgtest"
	"graft/service/internal/queue"
	"graft/service/internal/runner"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

// Short timings so the test takes seconds. Same ratio as production (heartbeat = lease / 5..3).
const (
	testLease     = 1500 * time.Millisecond
	testHeartbeat = 300 * time.Millisecond
)

// TestMain lets this test binary also act as a separate worker PROCESS, so the
// test can kill it with a real SIGKILL: no deferred cleanup, no graceful exit.
func TestMain(m *testing.M) {
	if os.Getenv("GRAFT_LEASE_CHILD") == "1" {
		leaseChild()
		return
	}
	os.Exit(m.Run())
}

// blockRunner simulates a step that runs for a very long time (until cancelled).
type blockRunner struct{}

func (blockRunner) Run(ctx context.Context, _ runner.Attempt) (runner.Result, error) {
	<-ctx.Done()
	return runner.Result{Outcome: runner.Cancelled}, nil
}

func (blockRunner) Cleanup(context.Context, string) error { return nil }

func leaseChild() {
	ctx := context.Background()
	pool, err := db.Open(ctx, os.Getenv("GRAFT_LEASE_DB"))
	if err != nil {
		os.Stderr.WriteString(err.Error())
		os.Exit(3)
	}
	w := &queue.Worker{Pool: pool, Clock: sched.Clock{Runner: blockRunner{}}, Owner: "child",
		Lease: testLease, Heartbeat: testHeartbeat}
	_, _ = w.Step(ctx) // claims the run, heartbeats, and blocks in the "long step" until killed
}

func freshDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	url := pgtest.NewEmpty(t)
	pool, err := db.Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool, url
}

func seedRun(t *testing.T, pool *pgxpool.Pool, id string) store.RunSpec {
	t.Helper()
	spec := store.RunSpec{Concurrency: 1, Steps: []store.StepSpec{
		{ID: "migrate", MaxAttempts: 2, Command: []string{"graft-fake", "fail:10", "ok:10"}},
		{ID: "seed", Needs: []string{"migrate"}, MaxAttempts: 1},
	}}
	b, _ := json.Marshal(spec)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO projects (slug) VALUES ('demo') ON CONFLICT DO NOTHING;
		INSERT INTO branches (id, project, name, status, expires_at)
		  VALUES ('br_l', 'demo', 'l', 'ready', now() + interval '1 day') ON CONFLICT DO NOTHING;`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO runs (id, branch_id, status, spec) VALUES ($1, 'br_l', 'queued', $2)`, id, b); err != nil {
		t.Fatal(err)
	}
	return spec
}

type runRow struct {
	Status     string
	Owner      *string
	LeaseUntil *time.Time
	Claims     int
}

func row(t *testing.T, pool *pgxpool.Pool, id string) runRow {
	t.Helper()
	var r runRow
	if err := pool.QueryRow(context.Background(),
		"SELECT status, lease_owner, lease_until, claims FROM runs WHERE id = $1", id).
		Scan(&r.Status, &r.Owner, &r.LeaseUntil, &r.Claims); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestKillNineWorkerRunIsReclaimedAndFinishesExactlyOnce(t *testing.T) {
	ctx := context.Background()
	pool, url := freshDB(t)
	spec := seedRun(t, pool, "run_kill")

	// 1. A separate worker process claims the run and starts its "long step".
	var out bytes.Buffer
	child := exec.Command(os.Args[0], "-test.run=^$")
	child.Env = append(os.Environ(), "GRAFT_LEASE_CHILD=1", "GRAFT_LEASE_DB="+url)
	child.Stdout, child.Stderr = &out, &out
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill() })
	deadline := time.Now().Add(10 * time.Second)
	for row(t, pool, "run_kill").Owner == nil {
		if time.Now().After(deadline) {
			t.Fatalf("child never claimed the run; output: %s", out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	first := row(t, pool, "run_kill")
	t.Logf("child pid %d claimed run_kill; lease until %s", child.Process.Pid, first.LeaseUntil.Format("15:04:05.000"))

	// 2. While it lives, its heartbeat keeps the lease: wait longer than one lease and check.
	time.Sleep(testLease + 500*time.Millisecond)
	alive := row(t, pool, "run_kill")
	if alive.Owner == nil || *alive.Owner != "child" || !alive.LeaseUntil.After(*first.LeaseUntil) {
		t.Fatalf("heartbeat did not extend the lease: before %v, after %+v", first.LeaseUntil, alive)
	}
	if c, err := queue.Claim(ctx, pool, "parent", testLease); err != nil || c != nil {
		t.Fatalf("a live worker's run must not be claimable; got %+v %v", c, err)
	}

	// 3. kill -9.
	if err := child.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	killedAt := time.Now()
	err := child.Wait()
	t.Logf("child exited: %v", err)
	if ws, ok := child.ProcessState.Sys().(syscall.WaitStatus); !ok || ws.Signal() != syscall.SIGKILL {
		t.Fatalf("child was not killed by SIGKILL: %v", err)
	}

	// 4. Right after the kill, the lease is still valid: nobody may take the run yet.
	if c, err := queue.Claim(ctx, pool, "parent", testLease); err != nil || c != nil {
		t.Fatalf("run reclaimed before its lease ran out: %+v %v", c, err)
	}

	// 5. Once the lease lapses, another worker claims it.
	var got *queue.Claimed
	for got == nil {
		if time.Since(killedAt) > testLease+3*time.Second {
			t.Fatal("run was never reclaimed after the lease ran out")
		}
		time.Sleep(50 * time.Millisecond)
		if got, err = queue.Claim(ctx, pool, "parent", testLease); err != nil {
			t.Fatal(err)
		}
	}
	waited := time.Since(killedAt)
	t.Logf("reclaimed by parent %s after the kill (lease %s), claims=%d", waited.Round(10*time.Millisecond), testLease, got.Claims)
	if got.Claims != 2 || waited > testLease+time.Second {
		t.Fatalf("claims=%d waited=%s; want 2 and about one lease", got.Claims, waited)
	}

	// 6. The dead worker's result can never land: its owner no longer holds the lease.
	res, _ := sched.Simulate(spec)
	if err := queue.Finish(ctx, pool, "run_kill", "br_l", "child", spec, &res, res.Events, nil); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale owner's Finish: got %v, want ErrLeaseLost", err)
	}

	// 7. The new owner finishes it, exactly once. The dead worker had already written
	// some events live (run_started, step_started); they stay, and seq continues after them.
	var before int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM events WHERE run_id = 'run_kill'").Scan(&before)
	if before < 1 {
		t.Fatalf("the child wrote no events live (%d)", before)
	}
	if err := queue.Finish(ctx, pool, "run_kill", "br_l", "parent", spec, &res, res.Events, nil); err != nil {
		t.Fatal(err)
	}
	if err := queue.Finish(ctx, pool, "run_kill", "br_l", "parent", spec, &res, res.Events, nil); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("second Finish: got %v, want ErrLeaseLost (already finished)", err)
	}
	var status string
	var steps, evs, maxSeq, runFinished int
	_ = pool.QueryRow(ctx, `SELECT status,
		(SELECT count(*) FROM step_runs WHERE run_id = r.id),
		(SELECT count(*) FROM events WHERE run_id = r.id),
		(SELECT coalesce(max(seq), 0) FROM events WHERE run_id = r.id),
		(SELECT count(*) FROM events WHERE run_id = r.id AND type = 'run_finished')
		FROM runs r WHERE id = 'run_kill'`).Scan(&status, &steps, &evs, &maxSeq, &runFinished)
	t.Logf("events: %d written live by the killed worker, %d by the new owner; seq 1..%d", before, len(res.Events), maxSeq)
	if status != "succeeded" || steps != 2 || evs != before+len(res.Events) || maxSeq != evs || runFinished != 1 {
		t.Fatalf("status=%s step_runs=%d events=%d max_seq=%d run_finished=%d", status, steps, evs, maxSeq, runFinished)
	}
}

func TestHeartbeatReportsALostLease(t *testing.T) {
	ctx := context.Background()
	pool, _ := freshDB(t)
	seedRun(t, pool, "run_steal")
	if c, _ := queue.Claim(ctx, pool, "a", 200*time.Millisecond); c == nil {
		t.Fatal("a could not claim")
	}
	time.Sleep(400 * time.Millisecond) // a's lease lapses (a "paused" worker)
	if c, _ := queue.Claim(ctx, pool, "b", time.Minute); c == nil || c.Claims != 2 {
		t.Fatalf("b should reclaim the lapsed run, got %+v", c)
	}
	if _, err := queue.Extend(ctx, pool, "run_steal", "a", time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("a's heartbeat after losing the lease: got %v, want ErrLeaseLost", err)
	}
	if r := row(t, pool, "run_steal"); *r.Owner != "b" {
		t.Fatalf("a's heartbeat must not take the run back; owner is %s", *r.Owner)
	}
	lost := make(chan error, 1)
	hb, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go queue.Heartbeat(hb, pool, "run_steal", "a", time.Minute, 50*time.Millisecond, time.Hour, func(err error) { lost <- err }, func() {})
	select {
	case err := <-lost:
		if !errors.Is(err, queue.ErrLeaseLost) {
			t.Fatal(err)
		}
	case <-hb.Done():
		t.Fatal("Heartbeat never reported the lost lease")
	}
}

func TestWorkerStepRunsTheKernelAndRecordsIt(t *testing.T) {
	ctx := context.Background()
	pool, _ := freshDB(t)
	seedRun(t, pool, "run_ok")
	w := &queue.Worker{Pool: pool, Clock: sched.Clock{Runner: runner.Script{}}, Owner: "w1"}
	did, err := w.Step(ctx)
	if !did || err != nil {
		t.Fatalf("did=%v err=%v", did, err)
	}
	r := row(t, pool, "run_ok")
	if r.Status != "succeeded" || r.Owner != nil || r.LeaseUntil != nil || r.Claims != 1 {
		t.Fatalf("%+v", r)
	}
	var attempts, finished int64
	_ = pool.QueryRow(ctx, "SELECT attempts, finished_ms FROM step_runs WHERE run_id='run_ok' AND step_id='migrate'").
		Scan(&attempts, &finished)
	// Real time now: fail after ~10 ms, retry ok after another ~10 ms.
	if attempts != 2 || finished < 20 || finished > 200 {
		t.Fatalf("migrate attempts=%d finished_ms=%d; want 2 and about 20 (fail at ~10, retry ok at ~20)", attempts, finished)
	}
	if did, _ := w.Step(ctx); did {
		t.Fatal("queue should be empty")
	}
}

// A run may last longer than one database write is allowed to take. The write
// timeout must bound each write, not the whole run: with a per-run deadline, every
// run longer than it failed to record its events, kept its lease until it lapsed,
// and was re-run from scratch by the next worker, forever.
func TestARunLongerThanTheWriteTimeoutStillFinishes(t *testing.T) {
	ctx := context.Background()
	pool, _ := freshDB(t)
	seedRun(t, pool, "run_seed") // creates the project and branch
	spec, _ := json.Marshal(store.RunSpec{Concurrency: 1, Steps: []store.StepSpec{
		{ID: "a", Command: []string{"graft-fake", "ok:400"}},
		{ID: "b", Needs: []string{"a"}, Command: []string{"graft-fake", "ok:400"}},
	}})
	if _, err := pool.Exec(ctx, `UPDATE runs SET status = 'succeeded' WHERE id = 'run_seed';
		INSERT INTO runs (id, branch_id, status, spec) VALUES ('run_long', 'br_l', 'queued', '`+string(spec)+`')`); err != nil {
		t.Fatal(err)
	}
	w := &queue.Worker{Pool: pool, Clock: sched.Clock{Runner: runner.Script{}}, Owner: "w1",
		WriteTimeout: 300 * time.Millisecond} // each write takes milliseconds; the run takes ~800 ms
	did, err := w.Step(ctx)
	if !did || err != nil {
		t.Fatalf("did=%v err=%v", did, err)
	}
	if r := row(t, pool, "run_long"); r.Status != "succeeded" || r.Owner != nil {
		t.Fatalf("a run longer than the write timeout did not finish: %+v", r)
	}
}

// Every attempt's output is kept, per claim, for the support bundle.
func TestWorkerKeepsEachAttemptsOutput(t *testing.T) {
	ctx := context.Background()
	pool, _ := freshDB(t)
	seedRun(t, pool, "run_logs") // migrate: fail then ok; seed: ok
	w := &queue.Worker{Pool: pool, Clock: sched.Clock{Runner: runner.Script{}}, Owner: "w1"}
	if did, err := w.Step(ctx); !did || err != nil {
		t.Fatalf("did=%v err=%v", did, err)
	}
	rows, err := pool.Query(ctx, `SELECT claim, step_id, attempt, outcome, output FROM attempt_logs
		WHERE run_id = 'run_logs' ORDER BY step_id, attempt`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var claim, attempt int
		var step, outcome, output string
		if err := rows.Scan(&claim, &step, &attempt, &outcome, &output); err != nil {
			t.Fatal(err)
		}
		if output == "" {
			t.Errorf("%s attempt %d: empty output", step, attempt)
		}
		got = append(got, fmt.Sprintf("%d/%s/%d/%s", claim, step, attempt, outcome))
	}
	want := []string{"1/migrate/1/fail", "1/migrate/2/ok", "1/seed/1/ok"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt logs %v, want %v", got, want)
	}
}

// Fenced like events: a worker that no longer holds the lease writes nothing.
func TestAttemptLogFromAWorkerWithoutTheLeaseIsDropped(t *testing.T) {
	ctx := context.Background()
	pool, _ := freshDB(t)
	seedRun(t, pool, "run_fence")
	c, err := queue.Claim(ctx, pool, "owner-a", time.Minute)
	if err != nil || c == nil {
		t.Fatalf("claim: %v %v", c, err)
	}
	res := runner.Result{Outcome: runner.OK, Output: "hello"}
	err = queue.SaveAttempt(ctx, pool, c.ID, c.Claims, "owner-b", runner.Attempt{StepID: "migrate", N: 1}, res)
	if !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stranger's write: err=%v, want ErrLeaseLost", err)
	}
	if err := queue.SaveAttempt(ctx, pool, c.ID, c.Claims, "owner-a", runner.Attempt{StepID: "migrate", N: 1}, res); err != nil {
		t.Fatalf("owner's write: %v", err)
	}
	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM attempt_logs WHERE run_id = 'run_fence'").Scan(&n)
	if n != 1 {
		t.Fatalf("%d rows, want only the owner's", n)
	}
}
