// Package runner executes one attempt of one step, for real.
//
// The scheduler decides WHAT runs WHEN (kernel/pipeline); a Runner only runs it and
// reports how it ended. Three implementations:
//
//	Script  "graft-fake" commands: scripted outcomes, in real time (tests, demos)
//	Docker  image + command in a locked-down container
//	Router  graft-fake to Script, everything else to Docker
package runner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Outcome string

const (
	OK        Outcome = "ok"
	Fail      Outcome = "fail"
	Timeout   Outcome = "timeout"
	Cancelled Outcome = "cancelled"
)

// Attempt is one try of one step.
type Attempt struct {
	RunID   string
	StepID  string
	N       int // 1-based attempt number
	Image   string
	Command []string
	Timeout time.Duration // 0: no timeout
}

type Result struct {
	Outcome  Outcome
	ExitCode int
	Output   string // the last OutputTail bytes of stdout+stderr
	Took     time.Duration
}

// OutputTail is how much output is kept per attempt.
const OutputTail = 8 << 10

// Runner runs attempts. The error is for INFRASTRUCTURE failures (Docker unreachable,
// image missing): a step that runs and fails is Result{Outcome: Fail}, not an error.
// Run must return promptly when ctx ends, with Outcome Cancelled and nothing left running.
type Runner interface {
	Run(ctx context.Context, a Attempt) (Result, error)
	// Cleanup removes anything left over from a run, e.g. after a crashed worker.
	Cleanup(ctx context.Context, runID string) error
}

// FakeCommand marks a scripted step: ["graft-fake", "fail:10", "ok:10"] means attempt 1
// fails after 10 ms, attempt 2 succeeds after 10 ms; later attempts repeat the last.
const FakeCommand = "graft-fake"

// FakeOutcome is one scripted attempt.
type FakeOutcome struct {
	OK  bool
	Dur time.Duration
}

// ParseFake parses a graft-fake command. ok=false means it is not a fake command.
func ParseFake(cmd []string) (outs []FakeOutcome, ok bool, err error) {
	if len(cmd) == 0 || cmd[0] != FakeCommand {
		return nil, false, nil
	}
	if len(cmd) == 1 {
		return nil, true, fmt.Errorf("%s needs at least one outcome like ok:10", FakeCommand)
	}
	for _, w := range cmd[1:] {
		kind, dur, found := strings.Cut(w, ":")
		ms, perr := strconv.ParseInt(dur, 10, 64)
		if !found || (kind != "ok" && kind != "fail") || perr != nil || ms < 0 {
			return nil, true, fmt.Errorf("bad fake outcome %q, want ok:<ms> or fail:<ms>", w)
		}
		outs = append(outs, FakeOutcome{OK: kind == "ok", Dur: time.Duration(ms) * time.Millisecond})
	}
	return outs, true, nil
}

// Script runs graft-fake steps in real time. Any other command succeeds after 10 ms
// (the kernel's default), so pipelines without a real runner still flow.
type Script struct{}

func (Script) Run(ctx context.Context, a Attempt) (Result, error) {
	outs, _, err := ParseFake(a.Command)
	if err != nil {
		return Result{}, err
	}
	o := FakeOutcome{OK: true, Dur: 10 * time.Millisecond}
	if len(outs) > 0 {
		o = outs[min(a.N, len(outs))-1]
	}
	start := time.Now()
	wait, outcome := o.Dur, Fail
	if o.OK {
		outcome = OK
	}
	if a.Timeout > 0 && o.Dur > a.Timeout { // the kernel's rule: longer than the timeout ends AT the timeout
		wait, outcome = a.Timeout, Timeout
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return Result{Outcome: outcome, Took: time.Since(start)}, nil
	case <-ctx.Done():
		return Result{Outcome: Cancelled, Took: time.Since(start)}, nil
	}
}

func (Script) Cleanup(context.Context, string) error { return nil }

// Router sends graft-fake steps to Fake and everything else to Real.
type Router struct {
	Fake Runner
	Real Runner
}

func (r Router) Run(ctx context.Context, a Attempt) (Result, error) {
	if _, fake, _ := ParseFake(a.Command); fake || r.Real == nil {
		return r.Fake.Run(ctx, a)
	}
	return r.Real.Run(ctx, a)
}

func (r Router) Cleanup(ctx context.Context, runID string) error {
	if r.Real == nil {
		return nil
	}
	return r.Real.Cleanup(ctx, runID)
}
