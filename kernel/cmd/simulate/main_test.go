package main

import (
	"strings"
	"testing"

	"graft/kernel/pipeline"
)

func ev(at int64, typ, step string, n int, reason string) pipeline.Event {
	return pipeline.Event{AtMs: at, Type: typ, Step: step, Attempt: n, Reason: reason}
}

func stored(at int64, typ, step string, n int, reason string) storedEvent {
	s := storedEvent{AtMs: at, Type: typ, Attempt: n}
	if step != "" {
		s.Step = &step
	}
	if reason != "" {
		s.Reason = &reason
	}
	return s
}

var sim = []pipeline.Event{
	ev(0, "step_started", "a", 1, ""),
	ev(10, "step_retry", "a", 1, "failed"),
	ev(20, "step_started", "a", 2, ""),
	ev(30, "step_succeeded", "a", 2, ""),
	ev(30, "run_finished", "", 0, "succeeded"),
}

func real(shift int64) []storedEvent {
	var out []storedEvent
	for _, e := range sim {
		out = append(out, stored(e.AtMs+shift, e.Type, e.Step, e.Attempt, e.Reason))
	}
	return out
}

func TestSameEventsWithinTheToleranceMatch(t *testing.T) {
	if d := compare(sim, real(3), 5); len(d) != 0 {
		t.Fatalf("differences: %v", d)
	}
}

func TestTimeBeyondTheToleranceIsReported(t *testing.T) {
	d := compare(sim, real(40), 5)
	if len(d) == 0 || !strings.Contains(d[0], "40 ms") {
		t.Fatalf("differences: %v", d)
	}
}

// Order is never jitter: the same events in another order is a different run.
func TestADifferentOrderIsReportedAtTheFirstDifference(t *testing.T) {
	r := real(0)
	r[1], r[2] = r[2], r[1]
	d := compare(sim, r, 1000)
	if len(d) == 0 || !strings.HasPrefix(d[0], "event 2:") {
		t.Fatalf("differences: %v", d)
	}
}

func TestAMissingEventIsReported(t *testing.T) {
	if d := compare(sim, real(0)[:4], 5); len(d) == 0 {
		t.Fatal("a missing run_finished was not reported")
	}
}

// Only the last claim is compared: an earlier claim was cut short by a lost lease and
// its events do not come from the run that finished.
func TestOnlyTheLastClaimIsCompared(t *testing.T) {
	first := []storedEvent{stored(0, "run_started", "", 0, ""), stored(0, "step_started", "a", 1, "")}
	second := append([]storedEvent{stored(0, "run_started", "", 0, "restarted (claim 2): the previous worker lost its lease")}, real(0)...)
	if d := compare(sim, lastClaim(append(first, second...)), 5); len(d) != 0 {
		t.Fatalf("differences: %v", d)
	}
}
