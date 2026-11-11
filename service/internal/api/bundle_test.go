package api

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"graft/kernel/pipeline"
	"graft/service/internal/bundle"
	"graft/service/internal/events"
)

func getBundle(t *testing.T, url string, auth bool) (*http.Response, map[string][]byte) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if auth {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return resp, nil
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	tr := tar.NewReader(gz)
	files := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("bad tar: %v", err)
		}
		files[h.Name], _ = io.ReadAll(tr)
	}
	return resp, files
}

func TestBundleHasEverythingAndNoSecrets(t *testing.T) {
	const dbPassword = "hunter2hunter2"
	ts, pool := newTestWith(t, func(s *Server) {
		s.Secrets = []string{dbPassword}
		s.Config = map[string]any{"runner": map[string]any{"kind": "script"}}
		s.Version = "v-test"
	})
	id := createRun(t, ts, `{"concurrency":2,"steps":[{"id":"migrate","max_attempts":2,"backoff_ms":5,"command":["graft-fake","fail:10","ok:10"]},
		{"id":"seed","needs":["migrate"],"command":["graft-fake","ok:10"]}]}`)
	waitRun(t, ts, id)
	// A step that prints secrets: the service's own token and its database password.
	if _, err := pool.Exec(context.Background(), `UPDATE attempt_logs SET output = output || $2
		WHERE run_id = $1 AND step_id = 'seed'`, id,
		"connecting to postgres://graft:"+dbPassword+"@db:5432/graft with Authorization: Bearer "+token+"\n"+
			"retrying with "+token+" in plain sight\n"); err != nil {
		t.Fatal(err)
	}

	resp, files := getBundle(t, ts.URL+"/v1/runs/"+id+"/bundle", true)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("content type %q", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, `filename="graft-bundle-`+id+`.tar.gz"`) {
		t.Fatalf("content disposition %q", cd)
	}
	for _, name := range []string{"manifest.json", "run.json", "events.json", "case.json", "config.json",
		"attempts/claim-1/migrate-attempt-1.log", "attempts/claim-1/migrate-attempt-2.log", "attempts/claim-1/seed-attempt-1.log"} {
		if _, ok := files[name]; !ok {
			t.Errorf("bundle has no %s; has %v", name, keys(files))
		}
	}
	for name, body := range files {
		for _, secret := range []string{token, dbPassword} {
			if strings.Contains(string(body), secret) {
				t.Errorf("%s contains a secret %q", name, secret[:4]+"...")
			}
		}
	}
	var m bundle.Manifest
	if err := json.Unmarshal(files["manifest.json"], &m); err != nil {
		t.Fatal(err)
	}
	if m.Format != bundle.Format || m.RunID != id || m.ServiceVersion != "v-test" || m.Redactions < 2 || len(m.Files) != len(files)-1 {
		t.Fatalf("manifest: %+v", m)
	}
	if !strings.Contains(string(files["attempts/claim-1/seed-attempt-1.log"]), "postgres://graft:[REDACTED]@db") {
		t.Fatalf("seed log:\n%s", files["attempts/claim-1/seed-attempt-1.log"])
	}

	// case.json replays in the kernel to the same events the run recorded.
	var c pipeline.Case
	if err := json.Unmarshal(files["case.json"], &c); err != nil {
		t.Fatal(err)
	}
	res, err := pipeline.Simulate(c)
	if err != nil {
		t.Fatal(err)
	}
	var evs []events.Event
	if err := json.Unmarshal(files["events.json"], &evs); err != nil {
		t.Fatal(err)
	}
	if got, want := types(res.Events), types(bundle.KernelEvents(evs)); !reflect.DeepEqual(got, want) {
		t.Fatalf("replay differs\n got:  %v\n want: %v", got, want)
	}
}

func TestBundleOfARunningRunExplainsWhyThereIsNoCase(t *testing.T) {
	ts := newTest(t)
	id := createRun(t, ts, `{"steps":[{"id":"slow","command":["graft-fake","ok:60000"]}]}`)
	waitStatus(t, ts, id, "running")
	resp, files := getBundle(t, ts.URL+"/v1/runs/"+id+"/bundle", true)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if _, ok := files["case.json"]; ok {
		t.Fatal("case.json for a run that has not finished")
	}
	if !strings.Contains(string(files["manifest.json"]), "case.json is not included: the run has not finished") {
		t.Fatalf("manifest does not say why:\n%s", files["manifest.json"])
	}
	call(t, ts, "POST", "/v1/runs/"+id+"/cancel", "", true)
	waitRun(t, ts, id)
}

func TestBundleNeedsAuthAndARealRun(t *testing.T) {
	ts := newTest(t)
	if resp, _ := getBundle(t, ts.URL+"/v1/runs/run_nope/bundle", false); resp.StatusCode != 401 {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	code, p, hdr := call(t, ts, "GET", "/v1/runs/run_nope/bundle", "", true)
	wantProblem(t, code, p, hdr, 404, "not-found")
}

func types(evs []pipeline.Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Type+" "+e.Step)
	}
	return out
}

func keys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
