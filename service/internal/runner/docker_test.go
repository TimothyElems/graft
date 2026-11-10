package runner

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// Docker tests run only with GRAFT_DOCKER=1 (CI sets it). With it set and no Docker
// reachable, they FAIL: a sandbox test that silently skips proves nothing.
func dockerRunner(t *testing.T) Docker {
	t.Helper()
	if os.Getenv("GRAFT_DOCKER") != "1" {
		t.Skip("set GRAFT_DOCKER=1 to run the Docker sandbox tests")
	}
	d := Docker{}.withDefaults()
	if ctx := os.Getenv("DOCKER_CONTEXT"); ctx != "" {
		d.Env = []string{"DOCKER_CONTEXT=" + ctx}
	}
	pullOnce.Do(func() {
		c := exec.Command(d.Bin, "pull", "-q", d.DefaultImage)
		c.Env = append(c.Environ(), d.Env...)
		pullOut, pullErr = c.CombinedOutput()
	})
	if pullErr != nil {
		t.Fatalf("GRAFT_DOCKER=1 but docker is not usable: %v: %s", pullErr, pullOut)
	}
	return d
}

var (
	pullOnce sync.Once
	pullOut  []byte
	pullErr  error
)

func sh(script string) []string { return []string{"sh", "-c", script} }

func run(t *testing.T, d Docker, runID, script string, timeout time.Duration) Result {
	t.Helper()
	res, err := d.Run(context.Background(), Attempt{RunID: runID, StepID: "s", N: 1, Command: sh(script), Timeout: timeout})
	if err != nil {
		t.Fatalf("infrastructure error: %v", err)
	}
	return res
}

func noneLeft(t *testing.T, d Docker, runID string) {
	t.Helper()
	ids, err := d.Containers(context.Background(), runID)
	if err != nil || len(ids) != 0 {
		t.Fatalf("containers left for %s: %v %v", runID, ids, err)
	}
}

func TestDockerExitCodesAndOutput(t *testing.T) {
	d := dockerRunner(t)
	if res := run(t, d, "run_ok", "echo hello; exit 0", 0); res.Outcome != OK || !strings.Contains(res.Output, "hello") {
		t.Fatalf("%+v", res)
	}
	if res := run(t, d, "run_fail", "exit 3", 0); res.Outcome != Fail || res.ExitCode != 3 {
		t.Fatalf("%+v", res)
	}
}

// Read the limits back from INSIDE the container: a flag that was passed but not
// enforced (e.g. no cgroup v2) would show up here, not in the argument list.
func TestDockerSandboxSettingsAreInEffect(t *testing.T) {
	d := dockerRunner(t)
	res := run(t, d, "run_cfg", `id -u; cat /sys/fs/cgroup/pids.max; cat /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/cpu.max; ls /sys/class/net | tr '\n' ' '`, 0)
	got := strings.Split(strings.TrimSpace(res.Output), "\n")
	want := []string{"65534", "64", "268435456", "100000 100000", "lo"}
	if res.Outcome != OK || len(got) != len(want) {
		t.Fatalf("%+v", res)
	}
	for i := range want {
		if strings.TrimSpace(got[i]) != want[i] {
			t.Errorf("setting %d: got %q, want %q (user, pids.max, memory.max, cpu.max, network interfaces)", i, got[i], want[i])
		}
	}
}

func TestDockerTimeoutKillsTheContainer(t *testing.T) {
	d := dockerRunner(t)
	start := time.Now()
	res := run(t, d, "run_timeout", "sleep 60", time.Second)
	if res.Outcome != Timeout || time.Since(start) > 15*time.Second {
		t.Fatalf("%s after %s", res.Outcome, time.Since(start))
	}
	noneLeft(t, d, "run_timeout")
}

func TestDockerCancelKillsTheContainer(t *testing.T) {
	d := dockerRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(time.Second); cancel() }()
	res, err := d.Run(ctx, Attempt{RunID: "run_cancel", StepID: "s", N: 1, Command: sh("sleep 60")})
	if err != nil || res.Outcome != Cancelled {
		t.Fatalf("%+v %v", res, err)
	}
	noneLeft(t, d, "run_cancel")
}

// A crashed worker leaves its containers running: the Docker daemon outlives it.
func TestDockerCleanupRemovesOrphans(t *testing.T) {
	d := dockerRunner(t)
	orphan := d.cli(context.Background(), "run", "-d", "--label", "graft.run=run_orphan", d.DefaultImage, "sleep", "60")
	if out, err := orphan.CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if ids, _ := d.Containers(context.Background(), "run_orphan"); len(ids) != 1 {
		t.Fatalf("setup: %v", ids)
	}
	if err := d.Cleanup(context.Background(), "run_orphan"); err != nil {
		t.Fatal(err)
	}
	noneLeft(t, d, "run_orphan")
}

func TestDockerMissingImageIsAnInfrastructureError(t *testing.T) {
	d := dockerRunner(t)
	_, err := d.Run(context.Background(), Attempt{RunID: "run_img", StepID: "s", N: 1, Image: "graft-test/no-such-image:0"})
	if err == nil {
		t.Fatal("a missing image is not the step's fault: want an error, not Outcome Fail")
	}
}
