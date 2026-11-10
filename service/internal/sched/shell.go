// Package sched turns a stored run into kernel input, runs the kernel, and
// stores what the kernel decided. The shell never makes a scheduling decision
// itself: statuses, attempts, times and reasons all come from pipeline.Simulate.
//
// v0.2 uses a FakeRunner that executes nothing. v0.6 replaces it with a Docker
// runner behind the same Runner interface, and a real clock.
package sched

import (
	"fmt"
	"strconv"
	"strings"

	"graft/kernel/pipeline"
	"graft/service/internal/store"
)

// FakeCommand is the first word of a step command that the fake runner scripts.
//
//	command: ["graft-fake", "fail:10", "ok:10"]  -> attempt 1 fails after 10 ms, attempt 2 succeeds after 10 ms
//
// Any other command (or none) is treated as "ok in 10 ms": the fake runner runs nothing.
const FakeCommand = "graft-fake"

// Runner decides what each attempt of a step does.
type Runner interface {
	Script(step store.StepSpec) ([]pipeline.Attempt, error)
}

type FakeRunner struct{}

func (FakeRunner) Script(step store.StepSpec) ([]pipeline.Attempt, error) {
	if len(step.Command) == 0 || step.Command[0] != FakeCommand {
		return nil, nil // the kernel's default: ok in 10 ms
	}
	if len(step.Command) == 1 {
		return nil, fmt.Errorf("step %s: %s needs at least one outcome like ok:10", step.ID, FakeCommand)
	}
	out := make([]pipeline.Attempt, 0, len(step.Command)-1)
	for _, w := range step.Command[1:] {
		outcome, dur, ok := strings.Cut(w, ":")
		ms, err := strconv.ParseInt(dur, 10, 64)
		if !ok || (outcome != "ok" && outcome != "fail") || err != nil || ms < 0 {
			return nil, fmt.Errorf("step %s: bad fake outcome %q, want ok:<ms> or fail:<ms>", step.ID, w)
		}
		out = append(out, pipeline.Attempt{Outcome: outcome, DurMs: ms})
	}
	return out, nil
}

type Shell struct {
	Runner Runner
}

// Case builds the kernel input for a run. Exported so the API can validate a
// pipeline (and the fake script) before accepting it.
func (s Shell) Case(steps []store.StepSpec, concurrency int) (pipeline.Case, error) {
	c := pipeline.Case{Concurrency: concurrency, Script: map[string][]pipeline.Attempt{}}
	for _, st := range steps {
		c.Steps = append(c.Steps, pipeline.Step{
			ID: st.ID, Needs: st.Needs, MaxAttempts: st.MaxAttempts,
			TimeoutMs: st.TimeoutMs, BackoffMs: st.BackoffMs,
		})
		script, err := s.Runner.Script(st)
		if err != nil {
			return pipeline.Case{}, err
		}
		if script != nil {
			c.Script[st.ID] = script
		}
	}
	if err := pipeline.Validate(c.Steps); err != nil {
		return pipeline.Case{}, err
	}
	return c, nil
}

// Execute runs the kernel for one run's spec. The result is the kernel's, unchanged.
func (s Shell) Execute(spec store.RunSpec) (pipeline.Result, error) {
	c, err := s.Case(spec.Steps, spec.Concurrency)
	if err != nil {
		return pipeline.Result{}, err
	}
	return pipeline.Simulate(c)
}
