package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/pgtest"
	"graft/service/internal/queue"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

const token = "test-token-0123456789"

// newTestWith starts graftd's handler on a fresh migrated database, with one real
// queue.Worker draining runs, and lets the caller adjust the server (e.g. its clock).
func newTestWith(t *testing.T, adjust func(*Server)) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	pool := pgtest.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	shell := sched.Shell{Runner: sched.FakeRunner{}}
	w := &queue.Worker{Pool: pool, Shell: shell, Owner: "test-worker", Idle: 5 * time.Millisecond}
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done }) // stop the worker before the database is dropped
	s := New(Server{Pool: pool, Shell: shell, Token: token,
		Log: slog.New(slog.NewTextHandler(testLog{t}, &slog.HandlerOptions{Level: slog.LevelError}))})
	if adjust != nil {
		adjust(s)
	}
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, pool
}

// testLog sends the server's error logs to the test output, so a 500 explains itself.
type testLog struct{ t *testing.T }

func (l testLog) Write(p []byte) (int, error) { l.t.Helper(); l.t.Log(string(p)); return len(p), nil }

func newTest(t *testing.T) *httptest.Server {
	ts, _ := newTestWith(t, nil)
	return ts
}

func call(t *testing.T, ts *httptest.Server, method, path, body string, auth bool) (int, map[string]any, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

func branch(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	code, b, _ := call(t, ts, "POST", "/v1/projects/demo/branches", `{"name":"`+name+`"}`, true)
	if code != 201 {
		t.Fatalf("create branch: %d %v", code, b)
	}
	return b["id"].(string)
}

func waitRun(t *testing.T, ts *httptest.Server, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, r, _ := call(t, ts, "GET", "/v1/runs/"+id, "", true)
		if s := r["status"]; s != "queued" && s != "running" {
			return r
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s never finished", id)
	return nil
}

func wantProblem(t *testing.T, code int, body map[string]any, hdr http.Header, wantCode int, kind string) {
	t.Helper()
	if code != wantCode {
		t.Fatalf("status %d, want %d: %v", code, wantCode, body)
	}
	if body["type"] != "https://graft.dev/problems/"+kind {
		t.Fatalf("type %v, want %s", body["type"], kind)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/problem+json" {
		t.Fatalf("content type %q", ct)
	}
	rid, _ := body["request_id"].(string)
	if rid == "" || rid != hdr.Get("X-Request-Id") {
		t.Fatalf("request_id %q must equal X-Request-Id %q", rid, hdr.Get("X-Request-Id"))
	}
}

func TestHealthzNeedsNoAuth(t *testing.T) {
	ts := newTest(t)
	code, body, _ := call(t, ts, "GET", "/healthz", "", false)
	if code != 200 || body["status"] != "ok" || body["queue_depth"] != float64(0) {
		t.Fatalf("%d %v", code, body)
	}
}

func TestEveryV1RouteNeedsAuth(t *testing.T) {
	ts := newTest(t)
	for _, c := range [][2]string{
		{"POST", "/v1/projects/demo/branches"}, {"GET", "/v1/branches/br_x"},
		{"POST", "/v1/branches/br_x/runs"}, {"GET", "/v1/runs/run_x"},
	} {
		code, body, hdr := call(t, ts, c[0], c[1], `{}`, false)
		wantProblem(t, code, body, hdr, 401, "unauthorized")
	}
}

func TestWrongTokenIsRejected(t *testing.T) {
	ts := newTest(t)
	req, _ := http.NewRequest("GET", ts.URL+"/v1/runs/run_x", nil)
	req.Header.Set("Authorization", "Bearer "+token+"x")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
}

func TestCreateBranchShapeAndDefaults(t *testing.T) {
	ts := newTest(t)
	code, b, _ := call(t, ts, "POST", "/v1/projects/demo/branches", `{"name":"feat-x"}`, true)
	if code != 201 {
		t.Fatalf("%d %v", code, b)
	}
	if !regexp.MustCompile(`^br_[A-Za-z0-9_]+$`).MatchString(b["id"].(string)) || b["status"] != "ready" || b["git_ref"] != nil {
		t.Fatalf("bad branch %v", b)
	}
	c, _ := time.Parse(time.RFC3339, b["created_at"].(string))
	e, _ := time.Parse(time.RFC3339, b["expires_at"].(string))
	if e.Sub(c) != 24*time.Hour {
		t.Fatalf("default ttl should be 86400s, got %v", e.Sub(c))
	}
	code, got, _ := call(t, ts, "GET", "/v1/branches/"+b["id"].(string), "", true)
	if code != 200 || got["id"] != b["id"] {
		t.Fatalf("get: %d %v", code, got)
	}
}

func TestCreateBranchValidation(t *testing.T) {
	ts := newTest(t)
	for _, body := range []string{`{}`, `{"name":"Feat"}`, `{"name":"-x"}`, `{"name":"ok","ttl_seconds":59}`,
		`{"name":"ok","ttl_seconds":604801}`, `{"name":"ok","extra":1}`, `not json`} {
		code, p, hdr := call(t, ts, "POST", "/v1/projects/demo/branches", body, true)
		wantProblem(t, code, p, hdr, 422, "validation")
	}
	code, p, hdr := call(t, ts, "POST", "/v1/projects/Demo/branches", `{"name":"ok"}`, true)
	wantProblem(t, code, p, hdr, 422, "validation")
}

func TestBranchNameTakenIsPerProject(t *testing.T) {
	ts := newTest(t)
	branch(t, ts, "feat-x")
	code, p, hdr := call(t, ts, "POST", "/v1/projects/demo/branches", `{"name":"feat-x"}`, true)
	wantProblem(t, code, p, hdr, 409, "branch-name-taken")
	if code, _, _ := call(t, ts, "POST", "/v1/projects/other/branches", `{"name":"feat-x"}`, true); code != 201 {
		t.Fatalf("same name in another project should be allowed, got %d", code)
	}
}

func TestBranchLimit(t *testing.T) {
	ts := newTest(t)
	for i := 0; i < store.MaxLiveBranches; i++ {
		branch(t, ts, "b"+strings.Repeat("x", i%10)+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	code, p, hdr := call(t, ts, "POST", "/v1/projects/demo/branches", `{"name":"one-too-many"}`, true)
	wantProblem(t, code, p, hdr, 409, "branch-limit")
}

func TestNotFound(t *testing.T) {
	ts := newTest(t)
	for _, path := range []string{"/v1/branches/br_nope", "/v1/runs/run_nope", "/v1/runs/not-an-id"} {
		code, p, hdr := call(t, ts, "GET", path, "", true)
		wantProblem(t, code, p, hdr, 404, "not-found")
	}
	code, p, hdr := call(t, ts, "POST", "/v1/branches/br_nope/runs", `{"steps":[{"id":"a"}]}`, true)
	wantProblem(t, code, p, hdr, 404, "not-found")
}

func TestCreateRunIsQueuedThenFinishedByTheKernel(t *testing.T) {
	ts := newTest(t)
	bid := branch(t, ts, "feat-x")
	code, run, _ := call(t, ts, "POST", "/v1/branches/"+bid+"/runs", `{"steps":[
		{"id":"migrate","max_attempts":3,"backoff_ms":5,"command":["graft-fake","fail:10","fail:10","ok:10"]},
		{"id":"seed","needs":["migrate"]}]}`, true)
	if code != 202 || run["status"] != "queued" || run["finished_at"] != nil {
		t.Fatalf("create: %d %v", code, run)
	}
	steps := run["steps"].([]any)
	if steps[0].(map[string]any)["status"] != "pending" || steps[0].(map[string]any)["reason"] != nil {
		t.Fatalf("queued steps should be pending with null reason: %v", steps)
	}
	done := waitRun(t, ts, run["id"].(string))
	if done["status"] != "succeeded" || done["finished_at"] == nil {
		t.Fatalf("%v", done)
	}
	m := done["steps"].([]any)[0].(map[string]any)
	s := done["steps"].([]any)[1].(map[string]any)
	// fixture 03's timeline: start 0, fail 10, start 15, fail 25, start 30, ok 40
	if m["attempts"] != float64(3) || m["finished_ms"] != float64(40) || s["started_ms"] != float64(40) {
		t.Fatalf("migrate %v seed %v", m, s)
	}
	// The kernel says reason "" for a success; the contract says null.
	if r, present := m["reason"]; !present || r != nil {
		t.Fatalf("succeeded step reason must be JSON null, got %#v (present=%v)", r, present)
	}
}

func TestFailedRunSkipsDependentsWithReason(t *testing.T) {
	ts := newTest(t)
	bid := branch(t, ts, "feat-x")
	_, run, _ := call(t, ts, "POST", "/v1/branches/"+bid+"/runs",
		`{"steps":[{"id":"migrate","command":["graft-fake","fail:10"]},{"id":"seed","needs":["migrate"]}]}`, true)
	done := waitRun(t, ts, run["id"].(string))
	seed := done["steps"].([]any)[1].(map[string]any)
	if done["status"] != "failed" || seed["status"] != "skipped" || seed["reason"] != "dependency:migrate" {
		t.Fatalf("%v", done)
	}
}

func TestCreateRunValidation(t *testing.T) {
	ts := newTest(t)
	bid := branch(t, ts, "feat-x")
	for _, body := range []string{
		`{"steps":[]}`, `{"steps":[{"id":"Bad"}]}`, `{"steps":[{"id":"a","max_attempts":0}]}`,
		`{"steps":[{"id":"a","max_attempts":11}]}`, `{"steps":[{"id":"a","timeout_ms":-1}]}`,
		`{"steps":[{"id":"a"}],"concurrency":17}`, `{"steps":[{"id":"a"}],"script":{}}`,
		`{"steps":[{"id":"a","outcome":"ok"}]}`,
	} {
		code, p, hdr := call(t, ts, "POST", "/v1/branches/"+bid+"/runs", body, true)
		wantProblem(t, code, p, hdr, 422, "validation")
	}
}

func TestBadPipelineIsRefusedWithTheKernelsError(t *testing.T) {
	ts := newTest(t)
	bid := branch(t, ts, "feat-x")
	for body, detail := range map[string]string{
		`{"steps":[{"id":"a","needs":["b"]},{"id":"b","needs":["a"]}]}`: "cycle",
		`{"steps":[{"id":"a"},{"id":"a"}]}`:                             "duplicate_step:a",
		`{"steps":[{"id":"a","needs":["ghost"]}]}`:                      "unknown_dependency:ghost",
		`{"steps":[{"id":"a","command":["graft-fake","maybe:1"]}]}`:     `step a: bad fake outcome "maybe:1", want ok:<ms> or fail:<ms>`,
	} {
		code, p, hdr := call(t, ts, "POST", "/v1/branches/"+bid+"/runs", body, true)
		wantProblem(t, code, p, hdr, 422, "invalid-pipeline")
		if p["detail"] != detail {
			t.Fatalf("detail %q, want %q", p["detail"], detail)
		}
	}
}
