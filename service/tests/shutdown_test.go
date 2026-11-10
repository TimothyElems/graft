package tests

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"graft/service/internal/pgtest"
)

func waitStep(t *testing.T, p *proc, runID, step string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		for _, e := range eventsOf(t, p, runID) {
			if e["type"] == "step_started" && e["step"] == step {
				return
			}
		}
	}
	t.Fatalf("step %s never started", step)
}

func sigterm(t *testing.T, p *proc) (time.Duration, int) {
	t.Helper()
	start := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = p.cmd.Wait()
	return time.Since(start), p.cmd.ProcessState.ExitCode()
}

func runRow(t *testing.T, dbURL, id string) (status string, owner *string, claims int) {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	_ = c.QueryRow(context.Background(), "SELECT status, lease_owner, claims FROM runs WHERE id = $1", id).Scan(&status, &owner, &claims)
	return
}

// A run longer than the grace period is stopped and RELEASED: with a 30 s lease, the
// next graftd can only pick it up quickly if the first one handed it back.
func TestSigtermReleasesARunThatOutlastsTheGracePeriod(t *testing.T) {
	dbURL := pgtest.NewEmpty(t)
	knobs := []string{"GRAFT_LEASE=30s", "GRAFT_HEARTBEAT=10s", "GRAFT_SHUTDOWN_GRACE=500ms", "GRAFT_SHUTDOWN_DEADLINE=5s"}
	a := start(t, dbURL, knobs...)
	br := api(t, a, "POST", "/v1/projects/demo/branches", `{"name":"term"}`)
	id := api(t, a, "POST", "/v1/branches/"+br["id"].(string)+"/runs", `{"steps":[{"id":"long","command":["graft-fake","ok:6000"]}]}`)["id"].(string)
	waitStep(t, a, id, "long")

	took, code := sigterm(t, a)
	status, owner, _ := runRow(t, dbURL, id)
	t.Logf("graftd A exited %d after %s; run is %s, lease owner %v", code, took.Round(time.Millisecond), status, owner)
	if code != 0 || took > 5*time.Second {
		t.Fatalf("exit %d after %s; want a clean exit within the 5s deadline\n%s", code, took, a.log)
	}
	if status != "queued" || owner != nil {
		t.Fatalf("run not released: status %s, owner %v", status, owner)
	}

	b := start(t, dbURL, knobs...)
	began := time.Now()
	var final map[string]any
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if final = api(t, b, "GET", "/v1/runs/"+id, ""); final["status"] == "succeeded" {
			break
		}
	}
	if final["status"] != "succeeded" || time.Since(began) > 15*time.Second {
		t.Fatalf("B: %v after %s (a lapsed 30 s lease would take longer)", final["status"], time.Since(began))
	}
	var reason string
	for _, e := range eventsOf(t, b, id) {
		if r, _ := e["reason"].(string); e["type"] == "run_started" && r != "" {
			reason = r
		}
	}
	t.Logf("graftd B finished it %s after starting; restart reason: %q", time.Since(began).Round(time.Millisecond), reason)
	if reason != "restarted (claim 2): the previous worker shut down and released it" {
		t.Fatalf("restart reason %q", reason)
	}
}

func TestSigtermLetsAShortRunFinish(t *testing.T) {
	dbURL := pgtest.NewEmpty(t)
	a := start(t, dbURL, "GRAFT_SHUTDOWN_GRACE=5s", "GRAFT_SHUTDOWN_DEADLINE=8s")
	br := api(t, a, "POST", "/v1/projects/demo/branches", `{"name":"term"}`)
	id := api(t, a, "POST", "/v1/branches/"+br["id"].(string)+"/runs", `{"steps":[{"id":"short","command":["graft-fake","ok:800"]}]}`)["id"].(string)
	waitStep(t, a, id, "short")
	took, code := sigterm(t, a)
	status, _, claims := runRow(t, dbURL, id)
	t.Logf("graftd exited %d after %s; run %s, claims %d", code, took.Round(time.Millisecond), status, claims)
	if code != 0 || status != "succeeded" || claims != 1 {
		t.Fatalf("want the run to finish within the grace period, exactly once\n%s", a.log)
	}
}
