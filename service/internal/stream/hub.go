// Package stream serves /v1/stream: one run's events over a websocket, replayed
// from a seq, then live.
//
// The events table is the source of truth. Postgres NOTIFY (sent by events.Append,
// delivered only at commit) is just a wake-up: each connection then reads
// `seq > last` itself. So a missed notification delays an event by at most Poll,
// and never loses it.
//
// Nothing here can slow the scheduler. The worker's only cost is one pg_notify per
// commit. The hub's fan-out is a non-blocking send on a 1-slot channel per
// subscriber, and each connection writes at its own pace, under a deadline.
package stream

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/events"
)

type Hub struct {
	Pool         *pgxpool.Pool
	Log          *slog.Logger
	WriteTimeout time.Duration // a reader slower than this per frame is disconnected (default 10s)
	Batch        int           // events read from the database at a time (default 100)
	Poll         time.Duration // safety re-read when no notification arrives (default 2s)

	mu   sync.Mutex
	subs map[string]map[*subscriber]struct{}
	done <-chan struct{} // closed when Run's context ends: connections close with 1001

	// Counters, for tests and metrics.
	SlowClosed atomic.Int64
	Served     atomic.Int64
}

type subscriber struct {
	wake chan struct{} // capacity 1: a pending wake-up is enough; extra ones coalesce
}

func (h *Hub) defaults() {
	if h.WriteTimeout == 0 {
		h.WriteTimeout = 10 * time.Second
	}
	if h.Batch == 0 {
		h.Batch = 100
	}
	if h.Poll == 0 {
		h.Poll = 2 * time.Second
	}
	if h.Log == nil {
		h.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
}

func (h *Hub) subscribe(runID string) *subscriber {
	s := &subscriber{wake: make(chan struct{}, 1)}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs == nil {
		h.subs = map[string]map[*subscriber]struct{}{}
	}
	if h.subs[runID] == nil {
		h.subs[runID] = map[*subscriber]struct{}{}
	}
	h.subs[runID][s] = struct{}{}
	return s
}

func (h *Hub) unsubscribe(runID string, s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs[runID], s)
	if len(h.subs[runID]) == 0 {
		delete(h.subs, runID)
	}
}

// wake never blocks: if a subscriber already has a wake-up pending, this one is dropped.
func (h *Hub) wake(runID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs[runID] {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (h *Hub) wakeAll() {
	h.mu.Lock()
	ids := make([]string, 0, len(h.subs))
	for id := range h.subs {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	for _, id := range ids {
		h.wake(id)
	}
}

// Run listens for notifications until ctx ends. It uses its own connection, not one
// from the pool: a LISTENing connection must not be handed to someone else.
func (h *Hub) Run(ctx context.Context) {
	h.defaults()
	h.mu.Lock()
	h.done = ctx.Done()
	h.mu.Unlock()
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := h.listen(ctx)
		if ctx.Err() != nil {
			return
		}
		h.Log.Warn("stream listener lost; reconnecting", "err", err, "backoff", backoff)
		h.wakeAll() // we may have missed notifications: everyone re-reads
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (h *Hub) listen(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, h.Pool.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN "+events.Channel); err != nil {
		return err
	}
	h.wakeAll() // anything committed before LISTEN started is picked up by a re-read
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		h.wake(n.Payload)
	}
}

func (h *Hub) stopping() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.done
}
