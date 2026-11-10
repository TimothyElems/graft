package lifecycle

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestWorkersThatFinishInTheGracePeriodAreNotStopped(t *testing.T) {
	done := make(chan struct{})
	go func() { time.Sleep(50 * time.Millisecond); close(done) }()
	var stopped, exited atomic.Bool
	took := Shutdown(Plan{Grace: time.Second, Deadline: 2 * time.Second, Log: quiet,
		StopHTTP: func(context.Context) error { return nil }, StopWork: func() { stopped.Store(true) },
		WorkersDone: done, Exit: func(int) { exited.Store(true) }})
	if stopped.Load() || exited.Load() || took > 500*time.Millisecond {
		t.Fatalf("stopped=%v exited=%v took=%s", stopped.Load(), exited.Load(), took)
	}
}

func TestAfterTheGracePeriodRunsAreStopped(t *testing.T) {
	done := make(chan struct{})
	var stopped atomic.Bool
	took := Shutdown(Plan{Grace: 100 * time.Millisecond, Deadline: 2 * time.Second, Log: quiet,
		StopHTTP:    func(context.Context) error { return nil },
		StopWork:    func() { stopped.Store(true); close(done) }, // stopping lets the workers return
		WorkersDone: done, Exit: func(int) { t.Error("exit called") }})
	if !stopped.Load() || took < 100*time.Millisecond || took > time.Second {
		t.Fatalf("stopped=%v took=%s", stopped.Load(), took)
	}
}

func TestWorkersThatNeverReturnHitTheDeadline(t *testing.T) {
	never := make(chan struct{})
	exited := make(chan int, 1)
	go Shutdown(Plan{Grace: 50 * time.Millisecond, Deadline: 200 * time.Millisecond, Log: quiet,
		StopHTTP: func(context.Context) error { return nil }, StopWork: func() {},
		WorkersDone: never, Exit: func(code int) { exited <- code }})
	select {
	case code := <-exited:
		if code != 1 {
			t.Fatalf("exit code %d, want 1", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a stuck shutdown never hit the deadline")
	}
}
