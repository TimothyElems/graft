package pipeline

// Machine holds a run's scheduling state and applies the rules. It has no clock:
// callers tell it what time it is. Simulate drives it with a virtual clock and
// scripted outcomes; the real-clock shell drives it with time.Now and real attempts.
//
// One step of time, in this order (the order is part of the rules):
//
//	Finish(t, ...)   for each attempt that ended at t, in start order
//	Settle(t, ...)   skip behind failures, cancel if asked, start ready steps
//	NextReady(t)     when a waiting retry becomes ready, if any
type Machine struct {
	steps  []Step
	conc   int
	idx    map[string]int
	st     []state
	events []Event

	running   int
	cancelled bool
}

// Start is an attempt the caller must now run.
type Start struct {
	Step      int    // index into the steps
	ID        string // step id
	Attempt   int    // 1-based
	TimeoutMs int64  // 0: none
}

func NewMachine(steps []Step, concurrency int) (*Machine, error) {
	if err := Validate(steps); err != nil {
		return nil, err
	}
	if concurrency < 1 {
		concurrency = 1
	}
	m := &Machine{steps: steps, conc: concurrency, idx: map[string]int{}, st: make([]state, len(steps))}
	for i, s := range steps {
		m.idx[s.ID] = i
		m.st[i].status = Pending
	}
	return m, nil
}

func (m *Machine) emit(t int64, typ string, i int, attempt int, reason string) {
	id := ""
	if i >= 0 {
		id = m.steps[i].ID
	}
	m.events = append(m.events, Event{AtMs: t, Type: typ, Step: id, Attempt: attempt, Reason: reason})
}

func (m *Machine) maxAttempts(i int) int {
	if m.steps[i].MaxAttempts < 1 {
		return 1
	}
	return m.steps[i].MaxAttempts
}

// Finish records that step i's running attempt ended at t with outcome ok, fail or
// timeout (rule 3: a failure retries after backoff until max_attempts is used).
func (m *Machine) Finish(t int64, i int, outcome string) {
	s := &m.st[i]
	if !s.runningNow || s.status != Pending {
		return // not running (e.g. already cancelled): a late report changes nothing
	}
	s.runningNow = false
	m.running--
	if outcome == "ok" {
		s.status = Succeeded
		s.finished = ptr(t)
		m.emit(t, "step_succeeded", i, s.attempts, "")
		return
	}
	reason := "failed"
	if outcome == "timeout" {
		reason = "timeout"
	}
	if s.attempts >= m.maxAttempts(i) {
		s.status = Failed
		s.finished = ptr(t)
		s.reason = reason
		m.emit(t, "step_failed", i, s.attempts, reason)
		return
	}
	s.readyAt = t + m.steps[i].BackoffMs
	m.emit(t, "step_retry", i, s.attempts, reason)
}

func bad(s string) bool { return s == Failed || s == Skipped || s == Cancelled }

// Settle applies, at time t: rule 4 (skip everything behind a failed, skipped or
// cancelled need, to a fixed point), rule 5 (if cancel: everything not finished is
// cancelled), then rule 1 (start ready steps in list order while slots are free).
// It returns the attempts to start. After a cancel it returns none.
func (m *Machine) Settle(t int64, cancel bool) []Start {
	for changed := true; changed; {
		changed = false
		for i := range m.steps {
			if m.st[i].status != Pending || m.st[i].attempts > 0 {
				continue
			}
			for _, need := range m.steps[i].Needs {
				if bad(m.st[m.idx[need]].status) {
					m.st[i].status = Skipped
					m.st[i].finished = ptr(t)
					m.st[i].reason = "dependency:" + need
					m.emit(t, "step_skipped", i, 0, m.st[i].reason)
					changed = true
					break
				}
			}
		}
	}

	if cancel {
		for i := range m.steps {
			s := &m.st[i]
			if s.status == Pending {
				s.status = Cancelled
				s.finished = ptr(t)
				s.reason = "cancelled"
				if s.runningNow {
					s.runningNow = false
					m.running--
				}
				m.emit(t, "step_cancelled", i, s.attempts, "cancelled")
				m.cancelled = true
			}
		}
		return nil
	}

	var out []Start
	for i := range m.steps {
		if m.running >= m.conc {
			break
		}
		s := &m.st[i]
		if s.status != Pending || s.runningNow || s.readyAt > t {
			continue
		}
		ready := true
		for _, need := range m.steps[i].Needs {
			if m.st[m.idx[need]].status != Succeeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		s.attempts++
		s.runningNow = true
		m.running++
		if s.started == nil {
			s.started = ptr(t)
		}
		m.emit(t, "step_started", i, s.attempts, "")
		out = append(out, Start{Step: i, ID: m.steps[i].ID, Attempt: s.attempts, TimeoutMs: m.steps[i].TimeoutMs})
	}
	return out
}

// NextReady is the earliest time after t at which a step waiting out its backoff
// becomes ready to retry.
func (m *Machine) NextReady(t int64) (int64, bool) {
	next, ok := int64(0), false
	for i := range m.steps {
		s := m.st[i]
		if s.status == Pending && !s.runningNow && s.attempts > 0 && s.readyAt > t && (!ok || s.readyAt < next) {
			next, ok = s.readyAt, true
		}
	}
	return next, ok
}

// Active reports whether any step is still pending (waiting, ready or running).
func (m *Machine) Active() bool {
	for _, s := range m.st {
		if s.status == Pending {
			return true
		}
	}
	return false
}

// Events returns every event so far. Callers that stream events keep their own offset.
func (m *Machine) Events() []Event { return m.events }

// Result ends the run at time t: step results, the run status, and run_finished.
func (m *Machine) Result(t int64) Result {
	res := Result{Steps: map[string]StepResult{}, FinishedMs: t}
	allOK := true
	for i, s := range m.steps {
		sr := m.st[i]
		if sr.status != Succeeded {
			allOK = false
		}
		res.Steps[s.ID] = StepResult{Status: sr.status, Attempts: sr.attempts, StartedMs: sr.started, FinishedMs: sr.finished, Reason: sr.reason}
	}
	switch {
	case m.cancelled:
		res.RunStatus = "cancelled"
	case allOK:
		res.RunStatus = "succeeded"
	default:
		res.RunStatus = "failed"
	}
	m.emit(t, "run_finished", -1, 0, res.RunStatus)
	res.Events = m.events
	return res
}
