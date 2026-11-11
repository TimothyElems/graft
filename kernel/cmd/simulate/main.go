// simulate reads one Case as JSON on stdin and writes the Result as JSON on stdout.
// On a bad case it writes {"error":"..."} and exits 0, so a parity run can compare errors too.
//
// Replay (docs/replay.md):
//
//	simulate -compare events.json [-tolerance-ms 50] < case.json
//
// runs the case and compares the kernel's events with a real run's (a support
// bundle's events.json, last claim only). Order, types, steps, attempts and reasons
// must be identical; times may differ by the tolerance (real timers fire late, and
// the lateness adds up along a chain of waits). Exit 0: the run followed the rules.
// Exit 1: it did not, and the first difference is printed. Exit 2: bad input.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"graft/kernel/pipeline"
)

func main() {
	against := flag.String("compare", "", "a bundle's events.json: compare the kernel's events with it")
	tol := flag.Int64("tolerance-ms", 50, "with -compare: how far apart in time the same event may be")
	flag.Parse()

	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var c pipeline.Case
	if err := json.Unmarshal(raw, &c); err != nil {
		fmt.Fprintln(os.Stderr, "bad json:", err)
		os.Exit(2)
	}
	res, err := pipeline.Simulate(c)
	if *against == "" {
		enc := json.NewEncoder(os.Stdout)
		if err != nil {
			enc.Encode(map[string]string{"error": err.Error()})
			return
		}
		enc.Encode(res)
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "the case does not run in the kernel:", err)
		os.Exit(2)
	}
	b, err := os.ReadFile(*against)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var evs []storedEvent
	if err := json.Unmarshal(b, &evs); err != nil {
		fmt.Fprintln(os.Stderr, "events.json:", err)
		os.Exit(2)
	}
	real := lastClaim(evs)
	diffs := compare(res.Events, real, *tol)
	if len(diffs) == 0 {
		fmt.Printf("MATCH: %d events, same order and content, times within %d ms. Run status %s.\n",
			len(res.Events), *tol, res.RunStatus)
		fmt.Println("The service followed the scheduling rules for these attempt durations.")
		return
	}
	fmt.Printf("DIFFERENT: the real run did not do what the kernel does with the same durations.\n")
	for _, d := range diffs {
		fmt.Println("  " + d)
	}
	os.Exit(1)
}

// storedEvent is one event as graftd stores it (a bundle's events.json).
type storedEvent struct {
	Seq     int     `json:"seq"`
	AtMs    int64   `json:"at_ms"`
	Type    string  `json:"type"`
	Step    *string `json:"step"`
	Attempt int     `json:"attempt"`
	Reason  *string `json:"reason"`
}

func (e storedEvent) kernel() pipeline.Event {
	k := pipeline.Event{AtMs: e.AtMs, Type: e.Type, Attempt: e.Attempt}
	if e.Step != nil {
		k.Step = *e.Step
	}
	if e.Reason != nil {
		k.Reason = *e.Reason
	}
	return k
}

// lastClaim keeps the events after the last run_started: the claim that finished.
// (bundle.CaseFor builds case.json from the same segment.)
func lastClaim(evs []storedEvent) []storedEvent {
	start := 0
	for i, e := range evs {
		if e.Type == "run_started" {
			start = i + 1
		}
	}
	return evs[start:]
}

func describe(e pipeline.Event) string {
	s := e.Type
	if e.Step != "" {
		s += fmt.Sprintf(" %s#%d", e.Step, e.Attempt)
	}
	if e.Reason != "" {
		s += " (" + e.Reason + ")"
	}
	return fmt.Sprintf("%s at %d ms", s, e.AtMs)
}

// compare lists differences, the first one first. Events are compared position by
// position: after a difference in content the rest is not meaningful, so it stops there.
func compare(sim []pipeline.Event, real []storedEvent, tolMs int64) []string {
	var out []string
	for i := 0; i < len(sim) || i < len(real); i++ {
		n := i + 1
		switch {
		case i >= len(real):
			return append(out, fmt.Sprintf("event %d: kernel has %s; the real run has nothing more", n, describe(sim[i])))
		case i >= len(sim):
			return append(out, fmt.Sprintf("event %d: real run has %s; the kernel has nothing more", n, describe(real[i].kernel())))
		}
		k, r := sim[i], real[i].kernel()
		if k.Type != r.Type || k.Step != r.Step || k.Attempt != r.Attempt || k.Reason != r.Reason {
			return append(out, fmt.Sprintf("event %d: kernel has %s; the real run has %s", n, describe(k), describe(r)))
		}
		if d := r.AtMs - k.AtMs; d > tolMs || d < -tolMs {
			out = append(out, fmt.Sprintf("event %d: %s is %d ms from the kernel's time (tolerance %d ms)", n, describe(r), abs(d), tolMs))
		}
	}
	return out
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
