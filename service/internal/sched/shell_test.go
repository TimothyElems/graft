package sched

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"graft/kernel/pipeline"
	"graft/service/internal/runner"
	"graft/service/internal/store"
)

// realTolerance is how far a real-clock event may be from the kernel's virtual time.
const realTolerance = 30 // ms

type key struct {
	Type, Step, Reason string
	Attempt            int
}

func keys(evs []pipeline.Event) []key {
	out := make([]key, len(evs))
	for i, e := range evs {
		out[i] = key{e.Type, e.Step, e.Reason, e.Attempt}
	}
	return out
}

// runBoth runs spec through the real-clock shell (Script runner, real sleeps) and the
// kernel's Simulate, and requires the same events in the same order, at about the
// same times, and the same step results.
func runBoth(t *testing.T, spec store.RunSpec, cancelAt time.Duration) {
	t.Helper()
	var want pipeline.Result
	var err error
	if cancelAt > 0 {
		c := pipeline.Case{Steps: steps(spec), Concurrency: spec.Concurrency, Script: map[string][]pipeline.Attempt{}}
		for _, s := range spec.Steps {
			outs, _, _ := runner.ParseFake(s.Command)
			for _, o := range outs {
				k := map[bool]string{true: "ok", false: "fail"}[o.OK]
				c.Script[s.ID] = append(c.Script[s.ID], pipeline.Attempt{Outcome: k, DurMs: o.Dur.Milliseconds()})
			}
		}
		at := cancelAt.Milliseconds()
		c.CancelAtMs = &at
		want, err = pipeline.Simulate(c)
	} else {
		want, err = Simulate(spec)
	}
	if err != nil {
		t.Fatal(err)
	}

	var live []pipeline.Event
	cancel := make(chan struct{})
	if cancelAt > 0 {
		go func() { time.Sleep(cancelAt); close(cancel) }()
	}
	got, emitted, err := Clock{Runner: runner.Script{}}.Run(context.Background(), "run_eq", spec, cancel,
		func(evs []pipeline.Event) error { live = append(live, evs...); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if emitted != len(live) || len(got.Events) != len(live)+1 {
		t.Fatalf("emitted %d events live, result has %d (want live + run_finished)", len(live), len(got.Events))
	}

	if fmt.Sprint(keys(got.Events)) != fmt.Sprint(keys(want.Events)) {
		t.Fatalf("event sequence differs from the kernel\n real:   %v\n kernel: %v", keys(got.Events), keys(want.Events))
	}
	var worst int64
	for i := range want.Events {
		d := got.Events[i].AtMs - want.Events[i].AtMs
		if d < 0 {
			d = -d
		}
		worst = max(worst, d)
		if d > realTolerance {
			t.Fatalf("event %d (%v) at %d ms, kernel says %d ms", i, keys(want.Events)[i], got.Events[i].AtMs, want.Events[i].AtMs)
		}
	}
	for id, w := range want.Steps {
		g := got.Steps[id]
		if g.Status != w.Status || g.Attempts != w.Attempts || g.Reason != w.Reason {
			t.Fatalf("step %s: real %+v, kernel %+v", id, g, w)
		}
	}
	if got.RunStatus != want.RunStatus {
		t.Fatalf("run status %s, kernel %s", got.RunStatus, want.RunStatus)
	}
	t.Logf("%d events identical in order; real times within %d ms of the kernel's", len(want.Events), worst)
}

// No two things happen at the same virtual moment in these cases: in real time there
// are no exact ties, so tied events could come out in either order.
func TestRealClockMatchesKernel(t *testing.T) {
	spec := store.RunSpec{Concurrency: 2, Steps: []store.StepSpec{
		{ID: "clone", MaxAttempts: 1, Command: []string{"graft-fake", "ok:40"}},
		{ID: "migrate", Needs: []string{"clone"}, MaxAttempts: 3, TimeoutMs: 60, BackoffMs: 30,
			Command: []string{"graft-fake", "ok:100", "fail:20", "ok:35"}}, // timeout, fail, ok
		{ID: "deploy", Needs: []string{"clone"}, MaxAttempts: 1, Command: []string{"graft-fake", "ok:75"}},
		{ID: "seed", Needs: []string{"migrate"}, MaxAttempts: 1, Command: []string{"graft-fake", "ok:25"}},
	}}
	runBoth(t, spec, 0)
}

func TestRealClockMatchesKernelOnFailureAndSkip(t *testing.T) {
	spec := store.RunSpec{Concurrency: 1, Steps: []store.StepSpec{
		{ID: "a", MaxAttempts: 2, BackoffMs: 25, Command: []string{"graft-fake", "fail:20", "fail:30"}},
		{ID: "b", Needs: []string{"a"}, MaxAttempts: 1},
		{ID: "c", Needs: []string{"b"}, MaxAttempts: 1},
		{ID: "d", MaxAttempts: 1, Command: []string{"graft-fake", "ok:15"}},
	}}
	runBoth(t, spec, 0)
}

func TestRealClockMatchesKernelOnCancel(t *testing.T) {
	spec := store.RunSpec{Concurrency: 2, Steps: []store.StepSpec{
		{ID: "a", MaxAttempts: 1, Command: []string{"graft-fake", "ok:40"}},
		{ID: "b", MaxAttempts: 1, Command: []string{"graft-fake", "ok:500"}},
		{ID: "c", Needs: []string{"a"}, MaxAttempts: 1, Command: []string{"graft-fake", "ok:500"}},
	}}
	runBoth(t, spec, 120*time.Millisecond) // a done at 40; b and c running at 120 are cancelled
}

func TestInterruptStopsEveryAttemptBeforeReturning(t *testing.T) {
	spec := store.RunSpec{Concurrency: 2, Steps: []store.StepSpec{
		{ID: "a", MaxAttempts: 1, Command: []string{"graft-fake", "ok:5000"}},
		{ID: "b", MaxAttempts: 1, Command: []string{"graft-fake", "ok:5000"}},
	}}
	tr := &tracking{inner: runner.Script{}}
	ctx, stop := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); stop() }()
	start := time.Now()
	_, _, err := Clock{Runner: tr}.Run(ctx, "run_int", spec, nil, nil)
	if err != ErrInterrupted || time.Since(start) > time.Second {
		t.Fatalf("got %v after %s", err, time.Since(start))
	}
	if tr.running() != 0 {
		t.Fatalf("%d attempts still running after Run returned", tr.running())
	}
}

func TestValidateRefusesBadPipelinesAndScripts(t *testing.T) {
	for _, s := range []store.RunSpec{
		{Steps: []store.StepSpec{{ID: "a", Needs: []string{"b"}}, {ID: "b", Needs: []string{"a"}}}},
		{Steps: []store.StepSpec{{ID: "a", Command: []string{"graft-fake", "maybe:1"}}}},
	} {
		if err := Validate(s); err == nil {
			t.Errorf("%v: want an error", s)
		}
	}
	if err := Validate(store.RunSpec{Steps: []store.StepSpec{{ID: "a", Command: []string{"psql"}}}}); err != nil {
		t.Fatal(err)
	}
}

// tracking counts attempts currently inside the runner.
type tracking struct {
	inner runner.Runner
	mu    sync.Mutex
	n     int
}

func (r *tracking) running() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

func (r *tracking) Run(ctx context.Context, a runner.Attempt) (runner.Result, error) {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.n--; r.mu.Unlock() }()
	return r.inner.Run(ctx, a)
}

func (r *tracking) Cleanup(context.Context, string) error { return nil }
