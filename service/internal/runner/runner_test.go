package runner

import (
	"context"
	"testing"
	"time"
)

func TestScriptFollowsTheScriptInRealTime(t *testing.T) {
	cmd := []string{"graft-fake", "fail:30", "ok:20"}
	for n, want := range map[int]Outcome{1: Fail, 2: OK, 3: OK} { // attempt 3 repeats the last entry
		start := time.Now()
		res, err := Script{}.Run(context.Background(), Attempt{N: n, Command: cmd})
		took := time.Since(start)
		if err != nil || res.Outcome != want {
			t.Fatalf("attempt %d: %+v %v, want %s", n, res, err, want)
		}
		if exp := map[int]time.Duration{1: 30, 2: 20, 3: 20}[n] * time.Millisecond; took < exp {
			t.Fatalf("attempt %d took %s, less than its scripted %s: not real time", n, took, exp)
		}
	}
}

func TestScriptTimeoutEndsAtTheTimeout(t *testing.T) {
	start := time.Now()
	res, _ := Script{}.Run(context.Background(), Attempt{N: 1, Command: []string{"graft-fake", "ok:2000"}, Timeout: 50 * time.Millisecond})
	if res.Outcome != Timeout || time.Since(start) > time.Second {
		t.Fatalf("got %s after %s; want timeout at ~50ms", res.Outcome, time.Since(start))
	}
}

func TestScriptCancelReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	res, _ := Script{}.Run(ctx, Attempt{N: 1, Command: []string{"graft-fake", "ok:5000"}})
	if res.Outcome != Cancelled || time.Since(start) > time.Second {
		t.Fatalf("got %s after %s", res.Outcome, time.Since(start))
	}
}

func TestParseFake(t *testing.T) {
	if _, fake, err := ParseFake([]string{"psql"}); fake || err != nil {
		t.Fatal("psql is not a fake command")
	}
	for _, bad := range [][]string{{"graft-fake"}, {"graft-fake", "maybe:1"}, {"graft-fake", "ok:-1"}, {"graft-fake", "ok"}} {
		if _, _, err := ParseFake(bad); err == nil {
			t.Errorf("%v: want error", bad)
		}
	}
}

func TestRouterSendsFakeToScriptAndTheRestToReal(t *testing.T) {
	real := &recorder{}
	r := Router{Fake: Script{}, Real: real}
	_, _ = r.Run(context.Background(), Attempt{N: 1, Command: []string{"graft-fake", "ok:1"}})
	_, _ = r.Run(context.Background(), Attempt{N: 1, Command: []string{"psql", "-f", "m.sql"}})
	if real.calls != 1 {
		t.Fatalf("real runner called %d times, want 1 (only the non-fake step)", real.calls)
	}
}

type recorder struct{ calls int }

func (r *recorder) Run(context.Context, Attempt) (Result, error) {
	r.calls++
	return Result{Outcome: OK}, nil
}
func (r *recorder) Cleanup(context.Context, string) error { return nil }
