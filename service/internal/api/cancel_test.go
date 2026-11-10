package api

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestCancelARunningRun(t *testing.T) {
	ts := newTest(t)
	id := createRun(t, ts, `{"concurrency":2,"steps":[{"id":"a","command":["graft-fake","ok:30"]},
		{"id":"slow","command":["graft-fake","ok:60000"]},{"id":"after","needs":["slow"]}]}`)
	waitStatus(t, ts, id, "running")
	time.Sleep(100 * time.Millisecond) // a (30 ms) has finished; slow is mid-attempt
	code, run, _ := call(t, ts, "POST", "/v1/runs/"+id+"/cancel", "", true)
	if code != 202 {
		t.Fatalf("cancel: %d %v", code, run)
	}
	start := time.Now()
	done := waitRun(t, ts, id)
	steps := map[string]string{}
	for _, s := range done["steps"].([]any) {
		m := s.(map[string]any)
		steps[m["id"].(string)] = m["status"].(string)
	}
	t.Logf("cancelled in %s: %v", time.Since(start).Round(time.Millisecond), steps)
	if done["status"] != "cancelled" || steps["a"] != "succeeded" || steps["slow"] != "cancelled" || steps["after"] != "cancelled" {
		t.Fatalf("rule 5: what finished counts, the rest is cancelled; got %v %v", done["status"], steps)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("cancel took %s", time.Since(start))
	}
	if code, _, _ := call(t, ts, "POST", "/v1/runs/"+id+"/cancel", "", true); code != 202 {
		t.Fatalf("cancel again (safe to repeat): %d", code)
	}
}

func TestCancelFinishedRunIsAcceptedAndChangesNothing(t *testing.T) {
	ts := newTest(t)
	id := createRun(t, ts, `{"steps":[{"id":"a"}]}`)
	waitRun(t, ts, id)
	code, run, _ := call(t, ts, "POST", "/v1/runs/"+id+"/cancel", "", true)
	if code != 202 || run["status"] != "succeeded" {
		t.Fatalf("%d %v", code, run)
	}
}

func TestCancelUnknownRunIs404(t *testing.T) {
	ts := newTest(t)
	code, p, hdr := call(t, ts, "POST", "/v1/runs/run_nope/cancel", "", true)
	wantProblem(t, code, p, hdr, 404, "not-found")
}

func waitStatus(t *testing.T, ts *httptest.Server, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, r, _ := call(t, ts, "GET", "/v1/runs/"+id, "", true); r["status"] == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s never reached %s", id, want)
}
