package pipeline

import "fmt"

// CaseFromEvents rebuilds the Case whose simulation produces evs: steps and
// concurrency come from the pipeline as submitted; the script and the cancel time
// come from the events. This is what makes a support bundle replayable, because a
// real run stores events, not the script that caused them.
//
// Each attempt's duration is its end minus its start, and the outcome follows the
// end event:
//
//	step_succeeded                     ok,   end - start
//	step_retry / step_failed "timeout" fail, timeout_ms + 1 (ran past the deadline)
//	step_retry / step_failed (other)   fail, end - start
//	step_cancelled (while running)     ok,   end - start + 1 (would have finished after the cancel)
//
// The cancel time is when run_finished says "cancelled". Events from outside the
// kernel (run_started) are ignored; pass only one run segment (the last claim).
//
// A run that has not finished is refused: an attempt still running has no duration.
func CaseFromEvents(steps []Step, concurrency int, evs []Event) (Case, error) {
	byID := map[string]Step{}
	for _, s := range steps {
		byID[s.ID] = s
	}
	c := Case{Steps: steps, Concurrency: concurrency, Script: map[string][]Attempt{}}
	type key struct {
		step string
		n    int
	}
	open := map[key]int64{} // attempts started and not yet ended -> start time
	finished := false
	for i, e := range evs {
		k := key{e.Step, e.Attempt}
		switch e.Type {
		case "step_started":
			if _, ok := byID[e.Step]; !ok {
				return Case{}, fmt.Errorf("event %d: step %q is not in the pipeline", i, e.Step)
			}
			if e.Attempt != len(c.Script[e.Step])+1 {
				return Case{}, fmt.Errorf("event %d: %s attempt %d started after %d attempts", i, e.Step, e.Attempt, len(c.Script[e.Step]))
			}
			open[k] = e.AtMs
		case "step_succeeded", "step_retry", "step_failed", "step_cancelled":
			start, running := open[k]
			if !running {
				if e.Type == "step_cancelled" {
					continue // cancelled before it started (waiting, or in backoff): no attempt to script
				}
				return Case{}, fmt.Errorf("event %d: %s attempt %d ended but never started", i, e.Step, e.Attempt)
			}
			delete(open, k)
			a := Attempt{Outcome: "fail", DurMs: e.AtMs - start}
			switch {
			case e.Type == "step_succeeded":
				a.Outcome = "ok"
			case e.Type == "step_cancelled":
				a.Outcome, a.DurMs = "ok", e.AtMs-start+1
			case e.Reason == "timeout":
				if byID[e.Step].TimeoutMs <= 0 {
					return Case{}, fmt.Errorf("event %d: %s timed out but has no timeout_ms", i, e.Step)
				}
				a.DurMs = byID[e.Step].TimeoutMs + 1
			}
			if a.DurMs < 0 {
				return Case{}, fmt.Errorf("event %d: %s attempt %d ends before it starts", i, e.Step, e.Attempt)
			}
			c.Script[e.Step] = append(c.Script[e.Step], a)
		case "run_finished":
			finished = true
			if e.Reason == "cancelled" {
				at := e.AtMs
				c.CancelAtMs = &at
			}
		}
	}
	if !finished || len(open) > 0 {
		return Case{}, fmt.Errorf("the run has not finished (%d attempt(s) still running): nothing to replay yet", len(open))
	}
	return c, nil
}
