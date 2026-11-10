// Package sched runs a pipeline for real: the kernel's Machine decides what runs
// when; a runner.Runner runs each attempt; this shell supplies real time and connects
// the two. It makes no scheduling decision of its own.
package sched

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"graft/kernel/pipeline"
	"graft/service/internal/runner"
	"graft/service/internal/store"
)

// ErrInterrupted means ctx ended (shutdown, lease lost) before the run finished.
// Every attempt was stopped; nothing is left running.
var ErrInterrupted = errors.New("run interrupted before it finished")

// Emit receives new events as they happen. An error (for example: this worker lost
// its lease) stops the run.
type Emit func([]pipeline.Event) error

// Clock is the real-time shell.
type Clock struct {
	Runner runner.Runner
	Now    func() time.Time // default time.Now
}

func steps(spec store.RunSpec) []pipeline.Step {
	out := make([]pipeline.Step, len(spec.Steps))
	for i, s := range spec.Steps {
		out[i] = pipeline.Step{ID: s.ID, Needs: s.Needs, MaxAttempts: s.MaxAttempts, TimeoutMs: s.TimeoutMs, BackoffMs: s.BackoffMs}
	}
	return out
}

// Validate refuses a pipeline the kernel would refuse, or a malformed graft-fake script,
// before it is accepted.
func Validate(spec store.RunSpec) error {
	for _, s := range spec.Steps {
		if _, _, err := runner.ParseFake(s.Command); err != nil {
			return fmt.Errorf("step %s: %w", s.ID, err)
		}
	}
	return pipeline.Validate(steps(spec))
}

// Simulate is what the kernel decides for a spec whose steps are graft-fake scripts,
// in virtual time. Tests compare the real-clock shell against it.
func Simulate(spec store.RunSpec) (pipeline.Result, error) {
	c := pipeline.Case{Steps: steps(spec), Concurrency: spec.Concurrency, Script: map[string][]pipeline.Attempt{}}
	for _, s := range spec.Steps {
		outs, _, err := runner.ParseFake(s.Command)
		if err != nil {
			return pipeline.Result{}, err
		}
		for _, o := range outs {
			kind := "fail"
			if o.OK {
				kind = "ok"
			}
			c.Script[s.ID] = append(c.Script[s.ID], pipeline.Attempt{Outcome: kind, DurMs: o.Dur.Milliseconds()})
		}
	}
	return pipeline.Simulate(c)
}

type finished struct {
	step    int
	outcome string
	at      int64
	err     error
}

func kernelOutcome(o runner.Outcome) string {
	switch o {
	case runner.OK:
		return "ok"
	case runner.Timeout:
		return "timeout"
	default:
		return "fail" // includes a Cancelled we did not ask for
	}
}

// Run executes spec in real time. It returns the kernel's result and how many of its
// events were already passed to emit (the caller stores the rest, with the final
// state, in one transaction). On ctx end it returns ErrInterrupted; on a cancel
// signal it applies rule 5 and returns a cancelled result. In every case, no attempt
// is still running when it returns.
func (c Clock) Run(ctx context.Context, runID string, spec store.RunSpec, cancel <-chan struct{}, emit Emit) (pipeline.Result, int, error) {
	now := c.Now
	if now == nil {
		now = time.Now
	}
	m, err := pipeline.NewMachine(steps(spec), spec.Concurrency)
	if err != nil {
		return pipeline.Result{}, 0, err
	}
	start := now()
	ms := func() int64 { return now().Sub(start).Milliseconds() }

	// Attempts get their own context, not ctx: on shutdown we stop them ourselves and
	// WAIT for the runner to confirm (for Docker: the container is gone) before returning.
	attempts, stopAttempts := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { stopAttempts(); wg.Wait() }()
	done := make(chan finished, len(spec.Steps))

	emitted := 0
	flush := func() error {
		evs := m.Events()[emitted:]
		emitted = len(m.Events())
		if len(evs) == 0 || emit == nil {
			return nil
		}
		return emit(evs)
	}
	record := func(f finished) error {
		if f.err != nil {
			return fmt.Errorf("step %s attempt: %w", spec.Steps[f.step].ID, f.err)
		}
		m.Finish(f.at, f.step, f.outcome)
		return nil
	}
	drain := func() error { // results that already arrived are recorded first (rule 5: a finish at T counts)
		for {
			select {
			case f := <-done:
				if err := record(f); err != nil {
					return err
				}
			default:
				return nil
			}
		}
	}

	cancelled := false
	for {
		t := ms()
		for _, s := range m.Settle(t, cancelled) {
			st := spec.Steps[s.Step]
			a := runner.Attempt{RunID: runID, StepID: st.ID, N: s.Attempt, Image: st.Image, Command: st.Command,
				Timeout: time.Duration(s.TimeoutMs) * time.Millisecond}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				res, err := c.Runner.Run(attempts, a)
				done <- finished{step: i, outcome: kernelOutcome(res.Outcome), at: ms(), err: err}
			}(s.Step)
		}
		if err := flush(); err != nil {
			return pipeline.Result{}, emitted, err
		}
		if cancelled || !m.Active() {
			break
		}
		var wake <-chan time.Time
		var timer *time.Timer
		if r, ok := m.NextReady(t); ok {
			timer = time.NewTimer(time.Duration(r-ms()) * time.Millisecond)
			wake = timer.C
		}
		stopTimer := func() {
			if timer != nil {
				timer.Stop()
			}
		}
		select {
		case f := <-done:
			if err := record(f); err != nil {
				return pipeline.Result{}, emitted, err
			}
			if err := drain(); err != nil {
				return pipeline.Result{}, emitted, err
			}
		case <-wake:
		case <-cancel:
			if err := drain(); err != nil {
				return pipeline.Result{}, emitted, err
			}
			cancelled = true
			stopAttempts() // running attempts are cancelled by the Settle above on the next loop
		case <-ctx.Done():
			stopTimer()
			return pipeline.Result{}, emitted, ErrInterrupted
		}
		stopTimer()
	}
	stopAttempts()
	wg.Wait() // nothing still running when we report the result
	return m.Result(ms()), emitted, nil
}
