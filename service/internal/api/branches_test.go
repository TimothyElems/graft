package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type page struct {
	Items      []branchJSON `json:"items"`
	NextCursor *string      `json:"next_cursor"`
}

func getPage(t *testing.T, base, project, cursor string, limit int) page {
	t.Helper()
	q := url.Values{"limit": {fmt.Sprint(limit)}}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	req, _ := http.NewRequest("GET", base+"/v1/projects/"+project+"/branches?"+q.Encode(), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list: %d", resp.StatusCode)
	}
	var p page
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	return p
}

// Notebook case 07: 120 branches with the SAME created_at, paged 50 at a time,
// must come back exactly once each: no repeats, no misses.
func TestCursorPagesAcross120BranchesCreatedInTheSameMillisecond(t *testing.T) {
	ts, pool := newTestWith(t, nil)
	// 120 exceeds the 50-live-branch limit (tested separately), so insert directly:
	// this test is about listing. Ids are random, like real ones, so id order != insert order.
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO projects (slug) VALUES ('demo');
		INSERT INTO branches (id, project, name, status, created_at, expires_at)
		SELECT 'br_' || md5(i::text), 'demo', 'b-' || i, 'ready',
		       '2026-11-10 09:00:00.123+00', '2026-11-11 09:00:00+00'
		FROM generate_series(1, 120) AS i`); err != nil {
		t.Fatal(err)
	}
	var distinct int
	_ = pool.QueryRow(t.Context(), "SELECT count(DISTINCT created_at) FROM branches").Scan(&distinct)
	if distinct != 1 {
		t.Fatalf("setup: want every branch at the same created_at, got %d distinct values", distinct)
	}

	seen := map[string]int{}
	var sizes []int
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("too many pages: the cursor is not advancing")
		}
		p := getPage(t, ts.URL, "demo", cursor, 50)
		sizes = append(sizes, len(p.Items))
		for _, b := range p.Items {
			seen[b.ID]++
		}
		if p.NextCursor == nil {
			break
		}
		cursor = *p.NextCursor
	}
	t.Logf("page sizes %v, %d distinct branches", sizes, len(seen))
	if fmt.Sprint(sizes) != "[50 50 20]" {
		t.Errorf("page sizes %v, want [50 50 20]", sizes)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s returned %d times", id, n)
		}
	}
	if len(seen) != 120 {
		t.Fatalf("saw %d distinct branches, want 120", len(seen))
	}
}

func TestListOrderAndEmptyAndBadCursor(t *testing.T) {
	clock := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	ts, _ := newTestWith(t, func(s *Server) {
		s.Now = func() time.Time { clock = clock.Add(time.Second); return clock }
	})
	for _, n := range []string{"old", "mid", "new"} {
		branch(t, ts, n)
	}
	p := getPage(t, ts.URL, "demo", "", 50)
	if len(p.Items) != 3 || p.Items[0].Name != "new" || p.Items[2].Name != "old" || p.NextCursor != nil {
		t.Fatalf("want newest first, no next cursor: %+v", p)
	}
	if e := getPage(t, ts.URL, "nobody", "", 50); len(e.Items) != 0 || e.NextCursor != nil {
		t.Fatalf("unknown project should be an empty page: %+v", e)
	}
	for _, bad := range []string{"not-base64!", "Zm9v", "MTIz"} { // garbage, "foo", "123"
		code, p, hdr := call(t, ts, "GET", "/v1/projects/demo/branches?cursor="+bad, "", true)
		wantProblem(t, code, p, hdr, 422, "bad-cursor")
	}
	code, p2, hdr := call(t, ts, "GET", "/v1/projects/demo/branches?limit=201", "", true)
	wantProblem(t, code, p2, hdr, 422, "validation")
}

func TestDeletedNameCanBeReused(t *testing.T) {
	ts, pool := newTestWith(t, nil)
	id := branch(t, ts, "feat-x")
	// No delete endpoint yet (planned); mark it deleted the way it will.
	if _, err := pool.Exec(t.Context(), "UPDATE branches SET status = 'deleted' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if code, b, _ := call(t, ts, "POST", "/v1/projects/demo/branches", `{"name":"feat-x"}`, true); code != 201 {
		t.Fatalf("notebook case 06: re-creating a deleted branch's name got %d %v", code, b)
	}
}

func TestRunEventsArePagedBySeq(t *testing.T) {
	ts := newTest(t)
	bid := branch(t, ts, "feat-x")
	_, run, _ := call(t, ts, "POST", "/v1/branches/"+bid+"/runs",
		`{"steps":[{"id":"migrate","max_attempts":2,"command":["graft-fake","fail:10","ok:10"]},{"id":"seed","needs":["migrate"]}]}`, true)
	waitRun(t, ts, run["id"].(string))
	code, all, _ := call(t, ts, "GET", "/v1/runs/"+run["id"].(string)+"/events", "", true)
	items := all["items"].([]any)
	// run_started + start/retry/start/succeed (migrate) + start/succeed (seed) + run_finished
	if code != 200 || len(items) != 8 || all["last_seq"] != float64(8) {
		t.Fatalf("%d %v", code, all)
	}
	types := ""
	for i, it := range items {
		e := it.(map[string]any)
		if e["seq"] != float64(i+1) {
			t.Fatalf("event %d has seq %v", i, e["seq"])
		}
		types += e["type"].(string)[:5] + " "
	}
	t.Logf("events: %s", types)
	_, rest, _ := call(t, ts, "GET", "/v1/runs/"+run["id"].(string)+"/events?after=6", "", true)
	if r := rest["items"].([]any); len(r) != 2 || r[0].(map[string]any)["seq"] != float64(7) {
		t.Fatalf("after=6: %v", rest)
	}
}
