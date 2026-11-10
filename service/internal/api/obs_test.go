package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"graft/service/internal/obs"
)

// The request_id in an error body is the trace id of the request's span, and the
// response's traceparent header carries that same trace id.
func TestRequestIDIsTheTraceID(t *testing.T) {
	ts := newTest(t)
	req, _ := http.NewRequest("GET", ts.URL+"/v1/runs/run_nope", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	tp := strings.Split(resp.Header.Get("Traceparent"), "-") // 00-<trace id>-<span id>-01
	if len(tp) != 4 || len(tp[1]) != 32 {
		t.Fatalf("traceparent %q", resp.Header.Get("Traceparent"))
	}
	if body["request_id"] != "req_"+tp[1] || resp.Header.Get("X-Request-Id") != "req_"+tp[1] {
		t.Fatalf("request_id %v, X-Request-Id %s, trace id %s", body["request_id"], resp.Header.Get("X-Request-Id"), tp[1])
	}
}

// A caller's traceparent is continued: its trace id becomes the request id, so the
// SDK user's trace and graftd's spans are one trace.
func TestIncomingTraceparentIsContinued(t *testing.T) {
	ts := newTest(t)
	const trace = "4bf92f3577b34da6a3ce929d0e0e4736"
	req, _ := http.NewRequest("GET", ts.URL+"/v1/runs/run_nope", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Traceparent", "00-"+trace+"-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Request-Id"); got != "req_"+trace {
		t.Fatalf("X-Request-Id %s, want req_%s", got, trace)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func TestLogLinesCarryTheTraceID(t *testing.T) {
	logs := &syncBuf{}
	ts, _ := newTestWith(t, func(s *Server) {
		s.Log = slog.New(obs.LogHandler(slog.NewJSONHandler(logs, nil)))
	})
	code, body, _ := call(t, ts, "GET", "/v1/runs/run_nope", "", true)
	if code != 404 {
		t.Fatal(code)
	}
	id := strings.TrimPrefix(body["request_id"].(string), "req_")
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		_ = json.Unmarshal([]byte(line), &m)
		if m["msg"] == "request" && m["route"] == "GET /v1/runs/{id}" {
			if m["trace_id"] != id {
				t.Fatalf("log line trace_id %v, request id %s:\n%s", m["trace_id"], id, line)
			}
			return
		}
	}
	t.Fatalf("no request log line:\n%s", logs.String())
}

func TestMetricsCountRunsRetriesQueueAndRequests(t *testing.T) {
	var srv *Server
	ts, _ := newTestWith(t, func(s *Server) { srv = s })
	id := createRun(t, ts, `{"steps":[{"id":"migrate","max_attempts":3,"command":["graft-fake","fail:5","fail:5","ok:5"]}]}`)
	waitRun(t, ts, id)
	rec := httptest.NewRecorder()
	srv.Obs.MetricsHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	text, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`graft_run_duration_seconds_count{status="succeeded"} 1`,
		`graft_step_retries_total 2`,
		`graft_queue_depth 0`,
		`graft_http_requests_total{code="Accepted",route="POST /v1/branches/{id}/runs"} 1`,
	} {
		if !strings.Contains(string(text), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if t.Failed() {
		for _, l := range strings.Split(string(text), "\n") {
			if strings.HasPrefix(l, "graft_") {
				t.Log(l)
			}
		}
	}
}
