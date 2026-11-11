package pipeline

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// roundTrip: the events a case produces must be enough to rebuild a case that
// produces the SAME events and the same result. This is what makes a support
// bundle replayable: graftd stores events, not the script that caused them.
func roundTrip(t *testing.T, name string, c Case) {
	t.Helper()
	want, err := Simulate(c)
	if err != nil {
		return // invalid pipelines produce no events; nothing to replay
	}
	got, err := CaseFromEvents(c.Steps, c.Concurrency, want.Events)
	if err != nil {
		t.Fatalf("%s: CaseFromEvents: %v", name, err)
	}
	again, err := Simulate(got)
	if err != nil {
		t.Fatalf("%s: rebuilt case does not run: %v", name, err)
	}
	if !reflect.DeepEqual(again, want) {
		g, _ := json.Marshal(again.Events)
		w, _ := json.Marshal(want.Events)
		c1, _ := json.Marshal(c)
		c2, _ := json.Marshal(got)
		t.Fatalf("%s: replay differs\n case: %s\n rebuilt: %s\n got:  %s\n want: %s", name, c1, c2, g, w)
	}
}

func TestReplayRoundTripsEveryFixture(t *testing.T) {
	files, _ := filepath.Glob("../../fixtures/pipeline/*.json")
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var fx fixture
		if err := json.Unmarshal(raw, &fx); err != nil {
			t.Fatal(err)
		}
		roundTrip(t, filepath.Base(f), fx.Case)
	}
}

// Random pipelines: retries, timeouts, backoff, a DAG, concurrency limits, and
// sometimes a cancel, including cancels that land exactly on a finish (rule 5's tie).
func TestReplayRoundTripsRandomCases(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 2000; i++ {
		n := 1 + r.Intn(6)
		c := Case{Concurrency: 1 + r.Intn(3), Script: map[string][]Attempt{}}
		for j := 0; j < n; j++ {
			s := Step{ID: fmt.Sprintf("s%d", j), MaxAttempts: 1 + r.Intn(3), BackoffMs: int64(r.Intn(3) * 10)}
			if r.Intn(3) == 0 {
				s.TimeoutMs = int64(10 + r.Intn(30))
			}
			for k := 0; k < j; k++ {
				if r.Intn(3) == 0 {
					s.Needs = append(s.Needs, fmt.Sprintf("s%d", k))
				}
			}
			c.Steps = append(c.Steps, s)
			for a := 0; a < s.MaxAttempts; a++ {
				out := "ok"
				if r.Intn(2) == 0 {
					out = "fail"
				}
				c.Script[s.ID] = append(c.Script[s.ID], Attempt{Outcome: out, DurMs: int64(r.Intn(5) * 10)})
			}
		}
		if r.Intn(3) == 0 {
			at := int64(r.Intn(12) * 10) // multiples of 10 so cancels often tie with finishes
			c.CancelAtMs = &at
		}
		roundTrip(t, fmt.Sprintf("random #%d", i), c)
	}
}

// A run that has not finished cannot be replayed: an attempt still running has no
// known duration. Say so instead of guessing.
func TestReplayRefusesAnUnfinishedRun(t *testing.T) {
	evs := []Event{{AtMs: 0, Type: "step_started", Step: "a", Attempt: 1}}
	if _, err := CaseFromEvents([]Step{{ID: "a"}}, 1, evs); err == nil {
		t.Fatal("rebuilt a case from a run that never finished")
	}
}
