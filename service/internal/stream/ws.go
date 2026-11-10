package stream

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"graft/service/internal/events"
)

// Close codes. 1000/1001/1011 are standard; 4000-4999 are ours (RFC 6455 7.4.2).
const (
	CloseSlowConsumer websocket.StatusCode = 4008
)

// Serve upgrades the request and streams runID's events with seq > after.
// The caller has already checked auth, parameters and that the run exists, so
// those errors are ordinary HTTP problems, not close codes.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, runID string, after int) {
	h.defaults()
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept already wrote an HTTP error
	}
	defer c.CloseNow()
	h.Served.Add(1)

	// We never expect messages from the client, but someone must read to process
	// pings and the close handshake. CloseRead does, and cancels ctx when the peer goes.
	ctx := c.CloseRead(r.Context())

	sub := h.subscribe(runID)
	defer h.unsubscribe(runID, sub)
	poll := time.NewTicker(h.Poll)
	defer poll.Stop()
	stopping := h.stopping()

	last := after
	reason := "client went away"
	defer func() {
		h.Log.Info("stream ended", "run_id", runID, "after", after, "last_seq", last, "reason", reason)
	}()
	for {
		evs, err := events.List(ctx, h.Pool, runID, last, h.Batch)
		if err != nil {
			if ctx.Err() == nil {
				h.Log.Error("stream read", "run_id", runID, "err", err)
				_ = c.Close(websocket.StatusInternalError, "server error: reconnect with after="+itoa(last))
			}
			return
		}
		for _, e := range evs {
			b, _ := json.Marshal(e)
			wctx, cancel := context.WithTimeout(ctx, h.WriteTimeout)
			err := c.Write(wctx, websocket.MessageText, b)
			slow := errors.Is(wctx.Err(), context.DeadlineExceeded)
			cancel()
			if err != nil {
				reason = "write failed: " + err.Error()
				if slow {
					reason = "slow consumer"
					// The reader is not keeping up. Its socket buffer is full, so a close
					// frame usually cannot get through either: best effort, then hang up.
					// Either way the client reconnects with after=<last seq it saw>.
					h.SlowClosed.Add(1)
					h.Log.Warn("slow consumer disconnected", "run_id", runID, "last_seq", last)
					_ = c.Close(CloseSlowConsumer, "slow consumer: reconnect with after="+itoa(last))
				}
				return
			}
			last = e.Seq
			if e.Type == "run_finished" {
				reason = "run finished"
				_ = c.Close(websocket.StatusNormalClosure, "run finished")
				return
			}
		}
		if len(evs) == h.Batch {
			continue // more already waiting: keep reading before sleeping
		}
		select {
		case <-ctx.Done():
			return
		case <-stopping:
			reason = "server shutting down"
			_ = c.Close(websocket.StatusGoingAway, "server shutting down: reconnect with after="+itoa(last))
			return
		case <-sub.wake:
		case <-poll.C:
		}
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
