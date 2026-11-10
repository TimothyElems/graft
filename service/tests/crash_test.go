// Package tests holds whole-binary tests: they build graftd and run it as a process.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"graft/service/internal/pgtest"
)

const token = "crash-test-token-0123456789"

var graftd string // built once in TestMain

func TestMain(m *testing.M) {
	dir, _ := os.MkdirTemp("", "graftd-bin")
	graftd = filepath.Join(dir, "graftd")
	if out, err := exec.Command("go", "build", "-o", graftd, "../cmd/graftd").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build graftd: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	cmd  *exec.Cmd
	addr string
	log  *bytes.Buffer
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs graftd against dbURL and waits until /healthz answers.
func start(t *testing.T, dbURL string, extra ...string) *proc {
	t.Helper()
	p := &proc{addr: freeAddr(t), log: &bytes.Buffer{}}
	p.cmd = exec.Command(graftd)
	p.cmd.Env = append(os.Environ(), "GRAFT_TOKEN="+token, "GRAFT_DATABASE_URL="+dbURL, "GRAFT_ADDR="+p.addr,
		"GRAFT_WORKERS=1", "GRAFT_LEASE=1500ms", "GRAFT_HEARTBEAT=300ms")
	p.cmd.Env = append(p.cmd.Env, extra...)
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.cmd.Process.Kill(); _, _ = p.cmd.Process.Wait() })
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if r, err := http.Get("http://" + p.addr + "/healthz"); err == nil && r.StatusCode == 200 {
			r.Body.Close()
			return p
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("graftd never became healthy:\n%s", p.log)
	return nil
}

func api(t *testing.T, p *proc, method, path, body string) map[string]any {
	t.Helper()
	req, _ := http.NewRequest(method, "http://"+p.addr+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func eventsOf(t *testing.T, p *proc, runID string) []map[string]any {
	t.Helper()
	page := api(t, p, "GET", "/v1/runs/"+runID+"/events?limit=200", "")
	var out []map[string]any
	for _, it := range page["items"].([]any) {
		out = append(out, it.(map[string]any))
	}
	return out
}

func TestKillNineGraftdMidRunThenRestartFinishesExactlyOnce(t *testing.T) {
	dbURL := pgtest.NewEmpty(t)
	a := start(t, dbURL)
	br := api(t, a, "POST", "/v1/projects/demo/branches", `{"name":"crash"}`)
	run := api(t, a, "POST", "/v1/branches/"+br["id"].(string)+"/runs", `{"steps":[
		{"id":"clone","command":["graft-fake","ok:300"]},
		{"id":"migrate","needs":["clone"],"command":["graft-fake","ok:3000"]},
		{"id":"seed","needs":["migrate"],"command":["graft-fake","ok:100"]}]}`)
	id := run["id"].(string)

	// Wait until graftd A is in the middle of the long step.
	deadline := time.Now().Add(10 * time.Second)
	for {
		evs := eventsOf(t, a, id)
		if len(evs) > 0 && evs[len(evs)-1]["type"] == "step_started" && evs[len(evs)-1]["step"] == "migrate" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never reached migrate: %v", evs)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// kill -9: no shutdown hook runs, no lease is released, nothing is flushed.
	if err := a.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = a.cmd.Wait()
	if ws := a.cmd.ProcessState.Sys().(syscall.WaitStatus); ws.Signal() != syscall.SIGKILL {
		t.Fatalf("graftd A was not killed by SIGKILL: %v", a.cmd.ProcessState)
	}
	killed := time.Now()

	// A brand-new graftd process. It must reclaim the run after the lease lapses and finish it.
	b := start(t, dbURL)
	var final map[string]any
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		final = api(t, b, "GET", "/v1/runs/"+id, "")
		if final["status"] == "succeeded" || final["status"] == "failed" {
			break
		}
	}
	if final["status"] != "succeeded" {
		t.Fatalf("run %v after restart; graftd B log:\n%s", final["status"], b.log)
	}

	evs := eventsOf(t, b, id)
	var finished, started int
	var restarted string
	for i, e := range evs {
		if int(e["seq"].(float64)) != i+1 {
			t.Fatalf("seq gap or repeat at %d: %v", i, e["seq"])
		}
		switch e["type"] {
		case "run_finished":
			finished++
		case "run_started":
			started++
			if r, _ := e["reason"].(string); r != "" {
				restarted = r
			}
		}
	}
	t.Logf("killed A mid-migrate; B finished the run %s later; %d events, seq 1..%d; second run_started: %q",
		time.Since(killed).Round(10*time.Millisecond), len(evs), len(evs), restarted)
	if finished != 1 || started != 2 || !strings.Contains(restarted, "claim 2") {
		t.Fatalf("run_finished=%d (want 1), run_started=%d (want 2: A's and B's), restart reason %q", finished, started, restarted)
	}
	_ = context.Background
}
