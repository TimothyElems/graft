package sched

import (
	"reflect"
	"testing"

	"graft/kernel/pipeline"
	"graft/service/internal/store"
)

func TestFakeRunnerParsesScript(t *testing.T) {
	got, err := FakeRunner{}.Script(store.StepSpec{ID: "m", Command: []string{"graft-fake", "fail:10", "ok:5"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []pipeline.Attempt{{Outcome: "fail", DurMs: 10}, {Outcome: "ok", DurMs: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFakeRunnerTreatsOtherCommandsAsDefault(t *testing.T) {
	for _, cmd := range [][]string{nil, {"psql", "-f", "migrate.sql"}} {
		got, err := FakeRunner{}.Script(store.StepSpec{ID: "m", Command: cmd})
		if err != nil || got != nil {
			t.Fatalf("%v: got %v, %v; want nil script (kernel default)", cmd, got, err)
		}
	}
}

func TestFakeRunnerRejectsBadOutcomes(t *testing.T) {
	for _, w := range []string{"maybe:10", "ok", "ok:-1", "ok:ten", "fail:"} {
		if _, err := (FakeRunner{}).Script(store.StepSpec{ID: "m", Command: []string{"graft-fake", w}}); err == nil {
			t.Errorf("%q: want error", w)
		}
	}
	if _, err := (FakeRunner{}).Script(store.StepSpec{ID: "m", Command: []string{"graft-fake"}}); err == nil {
		t.Error("graft-fake with no outcomes: want error")
	}
}

// The shell must pass every field through to the kernel. The expected value is
// built by hand from the kernel's own types, NOT through Shell.Case, so a
// mapping bug in the shell cannot hide by appearing on both sides.
func TestExecuteEqualsKernelOnHandBuiltCase(t *testing.T) {
	run := store.Run{
		Concurrency: 2,
		Steps: []store.StepSpec{
			{ID: "clone", MaxAttempts: 1, Command: []string{"graft-fake", "ok:800"}},
			{ID: "migrate", Needs: []string{"clone"}, MaxAttempts: 3, TimeoutMs: 30, BackoffMs: 7,
				Command: []string{"graft-fake", "ok:50", "fail:10", "ok:20"}},
			{ID: "deploy", Needs: []string{"clone"}, MaxAttempts: 1, Command: []string{"graft-fake", "ok:100"}},
			{ID: "seed", Needs: []string{"migrate"}, MaxAttempts: 1},
		},
	}
	want, err := pipeline.Simulate(pipeline.Case{
		Concurrency: 2,
		Steps: []pipeline.Step{
			{ID: "clone", MaxAttempts: 1},
			{ID: "migrate", Needs: []string{"clone"}, MaxAttempts: 3, TimeoutMs: 30, BackoffMs: 7},
			{ID: "deploy", Needs: []string{"clone"}, MaxAttempts: 1},
			{ID: "seed", Needs: []string{"migrate"}, MaxAttempts: 1},
		},
		Script: map[string][]pipeline.Attempt{
			"clone":   {{Outcome: "ok", DurMs: 800}},
			"migrate": {{Outcome: "ok", DurMs: 50}, {Outcome: "fail", DurMs: 10}, {Outcome: "ok", DurMs: 20}},
			"deploy":  {{Outcome: "ok", DurMs: 100}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Shell{Runner: FakeRunner{}}.Execute(run)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shell result differs from kernel\n got: %+v\nwant: %+v", got, want)
	}
	// Sanity: this case really exercises timeout, retry and backoff, or the test proves little.
	m := got.Steps["migrate"]
	if m.Attempts != 3 || *m.FinishedMs != 800+30+7+10+7+20 {
		t.Fatalf("migrate did not take the timeout+retry path: %+v (finished %d)", m, *m.FinishedMs)
	}
}

func TestCaseRefusesBadPipelineWithKernelError(t *testing.T) {
	_, err := Shell{Runner: FakeRunner{}}.Case([]store.StepSpec{
		{ID: "a", Needs: []string{"b"}, MaxAttempts: 1}, {ID: "b", Needs: []string{"a"}, MaxAttempts: 1},
	}, 1)
	if err == nil || err.Error() != "cycle" {
		t.Fatalf("got %v, want the kernel's 'cycle'", err)
	}
}
