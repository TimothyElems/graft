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

// Simulate runs the case in virtual time.
func Simulate(c Case) (Result, error) {
	if err := Validate(c.Steps); err != nil {
		return Result{}, err
	}
	steps := c.Steps
	n := len(steps)
	conc := c.Concurrency
	if conc < 1 {
		conc = 1
	}
	idx := map[string]int{}
	for i, s := range steps {
		idx[s.ID] = i
	}
	st := make([]state, n)
	for i := range st {
		st[i].status = Pending
	}
	var run []running
	var events []Event
	seq := 0
	t := int64(0)
	cancelled := false

	emit := func(typ string, i int, attempt int, reason string) {
		id := ""
		if i >= 0 {
			id = steps[i].ID
		}
		events = append(events, Event{AtMs: t, Type: typ, Step: id, Attempt: attempt, Reason: reason})
	}
	maxAttempts := func(i int) int {
		if steps[i].MaxAttempts < 1 {
			return 1
		}
		return steps[i].MaxAttempts
	}
	bad := func(s string) bool { return s == Failed || s == Skipped || s == Cancelled }

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
			s := &st[r.i]
			s.runningNow = false
			if r.outcome == "ok" {
				s.status = Succeeded
				s.finished = ptr(t)
				emit("step_succeeded", r.i, s.attempts, "")
				continue
			}
			reason := "failed"
			if r.outcome == "timeout" {
				reason = "timeout"
			}
			if s.attempts >= maxAttempts(r.i) {
				s.status = Failed
				s.finished = ptr(t)
				s.reason = reason
				emit("step_failed", r.i, s.attempts, reason)
			} else {
				s.readyAt = t + steps[r.i].BackoffMs
				emit("step_retry", r.i, s.attempts, reason)
			}
		}
		run = still

		// 2. Skip steps behind a failure, until nothing changes.
		for changed := true; changed; {
			changed = false
			for i := range steps {
				if st[i].status != Pending || st[i].attempts > 0 {
					continue
				}
				for _, need := range steps[i].Needs {
					if bad(st[idx[need]].status) {
						st[i].status = Skipped
						st[i].finished = ptr(t)
						st[i].reason = "dependency:" + need
						emit("step_skipped", i, 0, st[i].reason)
						changed = true
						break
					}
				}
			}
		}

		// 3. Cancel.
		if c.CancelAtMs != nil && *c.CancelAtMs <= t {
			for i := range steps {
				s := &st[i]
				if s.status == Pending {
					s.status = Cancelled
					s.finished = ptr(t)
					s.reason = "cancelled"
					s.runningNow = false
					emit("step_cancelled", i, s.attempts, "cancelled")
					cancelled = true
				}
			}
			run = nil
			break
		}

		// 4. Start ready steps.
		for i := range steps {
			if len(run) >= conc {
				break
			}
			s := &st[i]
			if s.status != Pending || s.runningNow || s.readyAt > t {
				continue
			}
			ok := true
			for _, need := range steps[i].Needs {
				if st[idx[need]].status != Succeeded {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			s.attempts++
			s.runningNow = true
			if s.started == nil {
				s.started = ptr(t)
			}
			a := attemptFor(c, steps[i].ID, s.attempts)
			dur, outcome := a.DurMs, a.Outcome
			if steps[i].TimeoutMs > 0 && dur > steps[i].TimeoutMs {
				dur, outcome = steps[i].TimeoutMs, "timeout"
			}
			seq++
			run = append(run, running{i: i, end: t + dur, outcome: outcome, seq: seq})
			emit("step_started", i, s.attempts, "")
		}

		// 5. Move time forward.
		next := int64(-1)
		consider := func(v int64) {
			if next < 0 || v < next {
				next = v
			}
		}
		active := len(run) > 0
		for i := range steps {
			if st[i].status == Pending {
				active = true
				if !st[i].runningNow && st[i].attempts > 0 && st[i].readyAt > t {
					consider(st[i].readyAt)
				}
			}
		}
		if !active {
			break
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

	res := Result{Steps: map[string]StepResult{}, FinishedMs: t}
	allOK := true
	for i, s := range steps {
		sr := st[i]
		if sr.status != Succeeded {
			allOK = false
		}
		res.Steps[s.ID] = StepResult{Status: sr.status, Attempts: sr.attempts, StartedMs: sr.started, FinishedMs: sr.finished, Reason: sr.reason}
	}
	switch {
	case cancelled:
		res.RunStatus = "cancelled"
	case allOK:
		res.RunStatus = "succeeded"
	default:
		res.RunStatus = "failed"
	}
	emit("run_finished", -1, 0, res.RunStatus)
	res.Events = events
	return res, nil
}
