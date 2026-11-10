package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Docker runs each attempt in a fresh, locked-down container via the docker CLI.
//
// Every flag closes one escape route (docs/threat-model.md says what is NOT covered):
//
//	--network none                      no network at all
//	--read-only + noexec tmpfs on /tmp  cannot change the image or run downloaded binaries
//	--pids-limit                        fork bombs stop at the limit
//	--memory (= --memory-swap), --cpus  cannot starve the host
//	--user nobody, --cap-drop ALL,      no root, no capabilities, and no way to regain
//	  no-new-privileges                   them through setuid binaries
//	no -v / --mount, ever               no host paths, no Docker socket
type Docker struct {
	Bin          string   // default "docker"
	Env          []string // extra environment for the CLI, e.g. DOCKER_CONTEXT=colima
	DefaultImage string   // for steps without an image; default busybox:1.37
	Memory       string   // default 256m
	CPUs         string   // default 1
	Pids         int      // default 64
	User         string   // default 65534:65534 (nobody)
	TmpSize      string   // default 16m
}

func (d Docker) withDefaults() Docker {
	if d.Bin == "" {
		d.Bin = "docker"
	}
	if d.DefaultImage == "" {
		d.DefaultImage = "busybox:1.37"
	}
	if d.Memory == "" {
		d.Memory = "256m"
	}
	if d.CPUs == "" {
		d.CPUs = "1"
	}
	if d.Pids == 0 {
		d.Pids = 64
	}
	if d.User == "" {
		d.User = "65534:65534"
	}
	if d.TmpSize == "" {
		d.TmpSize = "16m"
	}
	return d
}

var unsafeName = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

// ContainerName is unique per attempt, so a kill or cleanup can name it exactly.
func ContainerName(a Attempt) string {
	return unsafeName.ReplaceAllString(fmt.Sprintf("graft-%s-%s-%d", a.RunID, a.StepID, a.N), "_")
}

// Args is the full `docker run` argument list for an attempt (exported for tests and docs).
func (d Docker) Args(a Attempt) []string {
	d = d.withDefaults()
	image := a.Image
	if image == "" {
		image = d.DefaultImage
	}
	args := []string{"run", "--rm",
		"--name", ContainerName(a),
		"--label", "graft.run=" + a.RunID, "--label", "graft.step=" + a.StepID,
		"--network", "none",
		"--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=" + d.TmpSize,
		"--pids-limit", strconv.Itoa(d.Pids),
		"--memory", d.Memory, "--memory-swap", d.Memory,
		"--cpus", d.CPUs,
		"--user", d.User,
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		image}
	return append(args, a.Command...)
}

// tail keeps only the last OutputTail bytes written to it.
type tail struct {
	mu  sync.Mutex
	buf []byte
}

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > OutputTail {
		t.buf = t.buf[len(t.buf)-OutputTail:]
	}
	return len(p), nil
}

func (t *tail) String() string { t.mu.Lock(); defer t.mu.Unlock(); return string(t.buf) }

func (d Docker) cli(ctx context.Context, args ...string) *exec.Cmd {
	c := exec.CommandContext(ctx, d.Bin, args...)
	c.Env = append(c.Environ(), d.Env...)
	return c
}

func (d Docker) Run(ctx context.Context, a Attempt) (Result, error) {
	d = d.withDefaults()
	start := time.Now()
	out := &tail{}
	// Not CommandContext: killing the docker CLI would NOT stop the container. We stop
	// the container itself, by name, on timeout or cancel.
	cmd := exec.Command(d.Bin, d.Args(a)...)
	cmd.Env = append(cmd.Environ(), d.Env...)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("start docker: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	var deadline <-chan time.Time
	if a.Timeout > 0 {
		t := time.NewTimer(a.Timeout)
		defer t.Stop()
		deadline = t.C
	}
	stop := func(why Outcome) (Result, error) {
		d.kill(ContainerName(a))
		<-done // the CLI exits once its container is gone
		return Result{Outcome: why, ExitCode: -1, Output: out.String(), Took: time.Since(start)}, nil
	}
	select {
	case err := <-done:
		res := Result{Outcome: OK, Output: out.String(), Took: time.Since(start)}
		var ee *exec.ExitError
		switch {
		case err == nil:
		case errors.As(err, &ee):
			res.ExitCode = ee.ExitCode()
			res.Outcome = Fail
			// 125: docker itself failed (daemon down, bad image): infrastructure, not the step.
			if res.ExitCode == 125 {
				return res, fmt.Errorf("docker run failed (exit 125): %s", strings.TrimSpace(res.Output))
			}
		default:
			return res, err
		}
		return res, nil
	case <-deadline:
		return stop(Timeout)
	case <-ctx.Done():
		return stop(Cancelled)
	}
}

// kill removes the container (SIGKILL to its whole process tree), with its own short
// deadline: it must work even when the caller's context is already cancelled.
func (d Docker) kill(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = d.cli(ctx, "rm", "-f", name).Run()
}

// Cleanup removes every container of a run: after a crashed worker, its containers
// are still running under the Docker daemon, which outlives the worker.
func (d Docker) Cleanup(ctx context.Context, runID string) error {
	d = d.withDefaults()
	ids, err := d.cli(ctx, "ps", "-aq", "--filter", "label=graft.run="+runID).Output()
	if err != nil {
		return fmt.Errorf("list containers of %s: %w", runID, err)
	}
	fields := strings.Fields(string(bytes.TrimSpace(ids)))
	if len(fields) == 0 {
		return nil
	}
	if out, err := d.cli(ctx, append([]string{"rm", "-f"}, fields...)...).CombinedOutput(); err != nil {
		return fmt.Errorf("remove containers of %s: %w: %s", runID, err, out)
	}
	return nil
}

// Containers lists the ids of a run's containers (tests use it to prove none are left).
func (d Docker) Containers(ctx context.Context, runID string) ([]string, error) {
	d = d.withDefaults()
	ids, err := d.cli(ctx, "ps", "-aq", "--filter", "label=graft.run="+runID).Output()
	return strings.Fields(string(ids)), err
}
