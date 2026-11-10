// Package pipeline is the deterministic core of Graft's scheduler.
//
// It decides what runs, when, and what is skipped or cancelled. It never reads a
// clock and never starts a process. A shell around it (the real worker pool) supplies
// time and executes steps. Because the core is pure, the same input gives the same
// output on every run, and a Python reference can be checked against it event by event.
//
// Rules, all covered by fixtures in fixtures/pipeline:
//   - A step starts when all its needs have succeeded and a worker slot is free.
//   - Free slots go to ready steps in list order.
//   - An attempt that runs longer than timeout_ms ends at the timeout with reason "timeout".
//   - A failed attempt retries after backoff_ms until max_attempts is used up.
//   - When a step fails for good, every step that needs it, directly or not, is skipped.
//   - Cancel at time T: attempts that finish at T count. Everything else not finished is cancelled.
package pipeline

import (
	"errors"
	"fmt"
	"sort"
)

type Step struct {
	ID          string   `json:"id"`
	Needs       []string `json:"needs"`
	MaxAttempts int      `json:"max_attempts"`
	TimeoutMs   int64    `json:"timeout_ms"`
	BackoffMs   int64    `json:"backoff_ms"`
}

// Attempt is one scripted outcome. Outcome is "ok" or "fail". DurMs is how long it takes.
type Attempt struct {
	Outcome string `json:"outcome"`
	DurMs   int64  `json:"dur_ms"`
}

type Case struct {
	Name        string               `json:"name,omitempty"`
	Steps       []Step               `json:"steps"`
	Script      map[string][]Attempt `json:"script"`
	Concurrency int                  `json:"concurrency"`
	CancelAtMs  *int64               `json:"cancel_at_ms"`
}

type StepResult struct {
	Status     string `json:"status"`
	Attempts   int    `json:"attempts"`
	StartedMs  *int64 `json:"started_ms"`
	FinishedMs *int64 `json:"finished_ms"`
	Reason     string `json:"reason"`
}

type Event struct {
	AtMs    int64  `json:"at_ms"`
	Type    string `json:"type"`
	Step    string `json:"step"`
	Attempt int    `json:"attempt"`
	Reason  string `json:"reason"`
}

type Result struct {
	RunStatus  string                `json:"run_status"`
	FinishedMs int64                 `json:"finished_ms"`
	Steps      map[string]StepResult `json:"steps"`
	Events     []Event               `json:"events"`
}

const (
	Pending   = "pending"
	Succeeded = "succeeded"
	Failed    = "failed"
	Skipped   = "skipped"
	Cancelled = "cancelled"
)

var defaultAttempt = Attempt{Outcome: "ok", DurMs: 10}

// Validate rejects duplicate ids, unknown needs and cycles, in that order.
func Validate(steps []Step) error {
	seen := map[string]bool{}
	for _, s := range steps {
		if seen[s.ID] {
			return fmt.Errorf("duplicate_step:%s", s.ID)
		}
		seen[s.ID] = true
	}
	for _, s := range steps {
		for _, n := range s.Needs {
			if !seen[n] {
				return fmt.Errorf("unknown_dependency:%s", n)
			}
		}
	}
	indeg := map[string]int{}
	out := map[string][]string{}
	for _, s := range steps {
		indeg[s.ID] += 0
		for _, n := range s.Needs {
			indeg[s.ID]++
			out[n] = append(out[n], s.ID)
		}
	}
	var queue []string
	for _, s := range steps {
		if indeg[s.ID] == 0 {
			queue = append(queue, s.ID)
		}
	}
	done := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		done++
		for _, m := range out[id] {
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if done != len(steps) {
		return errors.New("cycle")
	}
	return nil
}

type state struct {
	status     string
	attempts   int
	started    *int64
	finished   *int64
	reason     string
	readyAt    int64
	runningNow bool
}

type running struct {
	i       int
	end     int64
	outcome string // "ok", "fail", "timeout"
	seq     int
}

func attemptFor(c Case, id string, n int) Attempt {
	sc := c.Script[id]
	if len(sc) == 0 {
		return defaultAttempt
	}
	if n-1 < len(sc) {
		return sc[n-1]
	}
	return sc[len(sc)-1]
}

func ptr(v int64) *int64 { return &v }

// ApplyTimeout is rule 2: an attempt longer than timeout_ms ends AT the timeout, with
// outcome "timeout". Real runners enforce the same rule with a deadline.
func ApplyTimeout(s Step, dur int64, outcome string) (int64, string) {
	if s.TimeoutMs > 0 && dur > s.TimeoutMs {
		return s.TimeoutMs, "timeout"
	}
	return dur, outcome
}

// Simulate runs the case in virtual time: it is the Machine driven by a virtual clock
// and scripted outcomes. The real-clock shell (service/internal/sched) drives the SAME
// Machine with real time and real attempts, so the rules exist once.
func Simulate(c Case) (Result, error) {
	m, err := NewMachine(c.Steps, c.Concurrency)
	if err != nil {
		return Result{}, err
	}
	var run []running
	seq := 0
	t := int64(0)
	for {
		// 1. Finish attempts that end now, oldest start first.
		sort.SliceStable(run, func(a, b int) bool {
			if run[a].end != run[b].end {
				return run[a].end < run[b].end
			}
			return run[a].seq < run[b].seq
		})
		var still []running
		for _, r := range run {
			if r.end > t {
				still = append(still, r)
				continue
			}
			m.Finish(t, r.i, r.outcome)
		}
		run = still

		// 2-4. Skip, cancel, start.
		cancel := c.CancelAtMs != nil && *c.CancelAtMs <= t
		for _, s := range m.Settle(t, cancel) {
			a := attemptFor(c, s.ID, s.Attempt)
			dur, outcome := ApplyTimeout(c.Steps[s.Step], a.DurMs, a.Outcome)
			seq++
			run = append(run, running{i: s.Step, end: t + dur, outcome: outcome, seq: seq})
		}
		if cancel {
			break
		}

		// 5. Move time forward.
		if !m.Active() {
			break
		}
		next := int64(-1)
		consider := func(v int64) {
			if next < 0 || v < next {
				next = v
			}
		}
		if r, ok := m.NextReady(t); ok {
			consider(r)
		}
		for _, r := range run {
			consider(r.end)
		}
		if c.CancelAtMs != nil && *c.CancelAtMs > t {
			consider(*c.CancelAtMs)
		}
		if next < 0 {
			break // nothing can move: cannot happen after Validate
		}
		t = next
	}
	return m.Result(t), nil
}
