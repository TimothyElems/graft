package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type resp struct {
	code   int
	body   map[string]any
	header http.Header
}

func post(t *testing.T, ts *httptest.Server, path, key, body string) resp {
	t.Helper()
	req, _ := http.NewRequest("POST", ts.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return resp{r.StatusCode, m, r.Header}
}

func TestSameKeySameBodyReplaysTheFirstResponse(t *testing.T) {
	ts, pool := newTestWith(t, nil)
	a := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-x"}`)
	b := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-x"}`)
	if a.code != 201 || b.code != 201 || a.body["id"] != b.body["id"] {
		t.Fatalf("first %d %v / retry %d %v", a.code, a.body["id"], b.code, b.body["id"])
	}
	if a.header.Get("Idempotent-Replayed") != "" || b.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay header: first %q retry %q", a.header.Get("Idempotent-Replayed"), b.header.Get("Idempotent-Replayed"))
	}
	var n int
	_ = pool.QueryRow(t.Context(), "SELECT count(*) FROM branches").Scan(&n)
	if n != 1 {
		t.Fatalf("%d branches created, want 1", n)
	}
}

func TestSameKeyDifferentBodyIs422(t *testing.T) {
	ts := newTest(t)
	post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-x"}`)
	r := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-y"}`)
	wantProblem(t, r.code, r.body, r.header, 422, "idempotency-key-reused")
}

func TestWhitespaceAndKeyOrderAreTheSameRequest(t *testing.T) {
	ts := newTest(t)
	a := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-x","ttl_seconds":3600}`)
	b := post(t, ts, "/v1/projects/demo/branches", "k-1", "{ \"ttl_seconds\": 3600,\n  \"name\": \"feat-x\" }")
	if b.code != 201 || a.body["id"] != b.body["id"] {
		t.Fatalf("reformatted retry: %d %v (first %v)", b.code, b.body, a.body["id"])
	}
}

func TestSameKeyOnAnotherEndpointIsIndependent(t *testing.T) {
	ts := newTest(t)
	a := post(t, ts, "/v1/projects/demo/branches", "shared", `{"name":"feat-x"}`)
	b := post(t, ts, "/v1/projects/other/branches", "shared", `{"name":"feat-x"}`)
	if a.code != 201 || b.code != 201 || a.body["id"] == b.body["id"] {
		t.Fatalf("different paths must not share a key: %v %v", a.body, b.body)
	}
}

func TestFailedRequestIsNotStoredSoTheKeyCanBeRetried(t *testing.T) {
	ts := newTest(t)
	bad := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"Bad Name"}`)
	good := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"good"}`)
	if bad.code != 422 || good.code != 201 {
		t.Fatalf("bad %d, corrected retry %d (want 422 then 201)", bad.code, good.code)
	}
}

func TestKeyLongerThan64IsRejected(t *testing.T) {
	ts := newTest(t)
	r := post(t, ts, "/v1/projects/demo/branches", strings.Repeat("k", 65), `{"name":"feat-x"}`)
	wantProblem(t, r.code, r.body, r.header, 422, "validation")
}

func TestConcurrentRetriesCreateOneRun(t *testing.T) {
	// Hold each keyed request open for 50 ms after its handler, so concurrent
	// retries really overlap. Without the per-key lock they would all create runs.
	ts, pool := newTestWith(t, func(s *Server) { s.idemBeforeStore = func() { time.Sleep(50 * time.Millisecond) } })
	bid := branch(t, ts, "feat-x")
	const n = 12
	var wg sync.WaitGroup
	results := make([]resp, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = post(t, ts, "/v1/branches/"+bid+"/runs", "retry-storm", `{"steps":[{"id":"a"}]}`)
		}(i)
	}
	close(start)
	wg.Wait()
	ids := map[any]int{}
	for _, r := range results {
		if r.code != 202 {
			t.Fatalf("a concurrent retry got %d %v", r.code, r.body)
		}
		ids[r.body["id"]]++
	}
	var runs int
	_ = pool.QueryRow(t.Context(), "SELECT count(*) FROM runs").Scan(&runs)
	if len(ids) != 1 || runs != 1 {
		t.Fatalf("%d concurrent retries produced run ids %v and %d runs; want one", n, ids, runs)
	}
}

func TestExpiredKeyCanBeReused(t *testing.T) {
	ts, pool := newTestWith(t, nil)
	post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-x"}`)
	if _, err := pool.Exec(t.Context(), "UPDATE idempotency_keys SET created_at = now() - interval '25 hours'"); err != nil {
		t.Fatal(err)
	}
	r := post(t, ts, "/v1/projects/demo/branches", "k-1", `{"name":"feat-y"}`)
	if r.code != 201 || r.body["name"] != "feat-y" {
		t.Fatalf("after 24h the key is free again; got %d %v", r.code, r.body)
	}
}

func TestNoKeyMeansNoIdempotency(t *testing.T) {
	ts, pool := newTestWith(t, nil)
	bid := branch(t, ts, "feat-x")
	post(t, ts, "/v1/branches/"+bid+"/runs", "", `{"steps":[{"id":"a"}]}`)
	post(t, ts, "/v1/branches/"+bid+"/runs", "", `{"steps":[{"id":"a"}]}`)
	var runs, keys int
	_ = pool.QueryRow(t.Context(), "SELECT (SELECT count(*) FROM runs), (SELECT count(*) FROM idempotency_keys)").Scan(&runs, &keys)
	if runs != 2 || keys != 0 {
		t.Fatalf("runs=%d keys=%d; without a key each POST creates", runs, keys)
	}
}
