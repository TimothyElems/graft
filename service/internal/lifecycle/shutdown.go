// Package lifecycle is graftd's graceful shutdown, kept out of main so it can be tested.
package lifecycle

import (
	"context"
	"log/slog"
	"time"
)

// Plan is what Shutdown does, in order.
type Plan struct {
	Grace    time.Duration // runs in progress may finish within this
	Deadline time.Duration // the process exits by this, whatever happens

	StopHTTP    func(ctx context.Context) error // stop accepting requests; ctx bounds the drain
	StopWork    func()                          // stop runs still in progress (they are released)
	WorkersDone <-chan struct{}                 // closed when every worker has returned
	Exit        func(code int)                  // os.Exit in production
	Log         *slog.Logger
}

// Shutdown runs after claiming has already stopped. It drains HTTP, waits up to Grace
// for runs in progress, then stops and releases them, and calls Exit(1) if the whole
// thing takes longer than Deadline. It returns how long it took when it finishes in time.
func Shutdown(p Plan) time.Duration {
	began := time.Now()
	hard := time.AfterFunc(p.Deadline, func() {
		p.Log.Error("shutdown deadline exceeded; exiting anyway", "after", time.Since(began).Round(time.Millisecond))
		p.Exit(1)
	})
	defer hard.Stop()

	httpCtx, cancel := context.WithTimeout(context.Background(), p.Grace)
	defer cancel()
	if err := p.StopHTTP(httpCtx); err != nil {
		p.Log.Warn("http shutdown", "err", err)
	}
	select {
	case <-p.WorkersDone:
		p.Log.Info("runs in progress finished within the grace period")
	case <-time.After(p.Grace - time.Since(began)):
		p.Log.Info("grace period over: stopping and releasing runs in progress")
		p.StopWork()
		<-p.WorkersDone
	}
	took := time.Since(began)
	p.Log.Info("shutdown complete", "took", took.Round(time.Millisecond))
	return took
}
