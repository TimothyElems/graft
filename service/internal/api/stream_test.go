package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"graft/service/internal/events"
	"graft/service/internal/pgtest"
	"graft/service/internal/queue"
	"graft/service/internal/sched"
	"graft/service/internal/stream"
)

// streamEnv is a full graftd (API, worker, hub) on a fresh database, with knobs.
type streamEnv struct {
	ts      *httptest.Server
	pool    *pgxpool.Pool
	hub     *stream.Hub
	stopHub context.CancelFunc
	stall   *stallListener
}

type envOpts struct {
	hubPoll      time.Duration
	writeTimeout time.Duration
	stall        bool // wrap the listener so a test can stall one connection
}

func newStreamEnv(t *testing.T, o envOpts) *streamEnv {
	t.Helper()
	pool := pgtest.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	hubCtx, stopHub := context.WithCancel(ctx)
	shell := sched.Shell{Runner: sched.FakeRunner{}}
	w := &queue.Worker{Pool: pool, Shell: shell, Owner: "test-worker", Idle: 5 * time.Millisecond}
	hub := &stream.Hub{Pool: pool, Poll: o.hubPoll, WriteTimeout: o.writeTimeout,
		Log: slog.New(slog.NewTextHandler(testLog{t}, nil))}
	wDone, hDone := make(chan struct{}), make(chan struct{})
	go func() { w.Run(ctx); close(wDone) }()
	go func() { hub.Run(hubCtx); close(hDone) }()
	ts := httptest.NewUnstartedServer(New(Server{Pool: pool, Shell: shell, Token: token, Hub: hub}).Handler())
	var sl *stallListener
	if o.stall {
		sl = &stallListener{Listener: ts.Listener}
		ts.Listener = sl
	}
	ts.Start()
	t.Cleanup(func() { ts.CloseClientConnections(); ts.Close(); cancel(); <-wDone; <-hDone })
	return &streamEnv{ts: ts, pool: pool, hub: hub, stopHub: stopHub, stall: sl}
}

// stallListener can make the NEXT accepted connection behave like a full socket: it
// takes 8 KB (enough for the upgrade response), then every write blocks until the
// connection is closed. Buffer sizes are the OS's choice (measured: macOS grows a
// stalled reader's receive window to ~320 KB, Linux honours SO_RCVBUF), so the test
// simulates the full socket here instead of hoping the kernel's buffers fill.
type stallListener struct {
	net.Listener
	stallNext atomic.Bool
}

func (l *stallListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil && l.stallNext.CompareAndSwap(true, false) {
		return &stalledConn{Conn: c, budget: 8 << 10, closed: make(chan struct{})}, nil
	}
	return c, err
}

type stalledConn struct {
	net.Conn
	mu      sync.Mutex
	written int
	budget  int
	closed  chan struct{}
	once    sync.Once
}

func (c *stalledConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	ok := c.written+len(p) <= c.budget
	if ok {
		c.written += len(p)
	}
	c.mu.Unlock()
	if ok {
		return c.Conn.Write(p)
	}
	<-c.closed // a full socket: nothing moves until someone closes the connection
	return 0, net.ErrClosed
}

func (c *stalledConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func dial(t *testing.T, base, query string) (*websocket.Conn, error) {
	t.Helper()
	opts := &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}}
	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(base, "http")+"/v1/stream?"+query, opts)
	if c != nil {
		c.SetReadLimit(1 << 20)
	}
	return c, err
}

// readAll reads frames until the server closes; returns raw frames and the close status.
func readAll(t *testing.T, c *websocket.Conn, within time.Duration) ([][]byte, websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var frames [][]byte
	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			return frames, websocket.CloseStatus(err)
		}
		frames = append(frames, b)
	}
}

func createRun(t *testing.T, ts *httptest.Server, body string) string {
	t.Helper()
	bid := branch(t, ts, fmt.Sprintf("s-%d", time.Now().UnixNano()))
	code, run, _ := call(t, ts, "POST", "/v1/branches/"+bid+"/runs", body, true)
	if code != 202 {
		t.Fatalf("create run: %d %v", code, run)
	}
	return run["id"].(string)
}

// bigRun: 50 steps, each failing 4 times then succeeding = 500 kernel events + run_started.
func bigRun() string {
	var steps []string
	for i := 0; i < 50; i++ {
		steps = append(steps, fmt.Sprintf(`{"id":"s%02d","max_attempts":5,"command":["graft-fake","fail:1","fail:1","fail:1","fail:1","ok:1"]}`, i))
	}
	return `{"concurrency":16,"steps":[` + strings.Join(steps, ",") + `]}`
}

var eventSchema = func() *jsonschema.Schema {
	c := jsonschema.NewCompiler()
	f, err := os.Open("../../../contract/events.schema.json")
	if err != nil {
		panic(err)
	}
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		panic(err)
	}
	if err := c.AddResource("events.schema.json", doc); err != nil {
		panic(err)
	}
	return c.MustCompile("events.schema.json")
}()

func TestStreamReplaysEveryEventValidAgainstTheSchemaThenCloses1000(t *testing.T) {
	env := newStreamEnv(t, envOpts{})
	id := createRun(t, env.ts, `{"steps":[{"id":"migrate","max_attempts":3,"command":["graft-fake","fail:10","fail:10","ok:10"]},{"id":"seed","needs":["migrate"]}]}`)
	waitRun(t, env.ts, id)
	c, err := dial(t, env.ts.URL, "run="+id)
	if err != nil {
		t.Fatal(err)
	}
	frames, status := readAll(t, c, 5*time.Second)
	if status != websocket.StatusNormalClosure {
		t.Fatalf("close status %d, want 1000", status)
	}
	_, rest, _ := call(t, env.ts, "GET", "/v1/runs/"+id+"/events", "", true)
	items := rest["items"].([]any)
	if len(frames) != len(items) || len(frames) != 10 {
		t.Fatalf("%d frames, %d events over REST; want 10 each", len(frames), len(items))
	}
	for i, f := range frames {
		var got, want any
		_ = json.Unmarshal(f, &got)
		b, _ := json.Marshal(items[i])
		_ = json.Unmarshal(b, &want)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("frame %d differs from REST:\n ws:   %s\n rest: %s", i, f, b)
		}
		inst, _ := jsonschema.UnmarshalJSON(strings.NewReader(string(f)))
		if err := eventSchema.Validate(inst); err != nil {
			t.Fatalf("frame %d breaks events.schema.json: %v\n%s", i, err, f)
		}
	}
}

func TestResumeSendsStrictlyAfterTheGivenSeq(t *testing.T) {
	env := newStreamEnv(t, envOpts{})
	id := createRun(t, env.ts, `{"steps":[{"id":"a","max_attempts":3,"command":["graft-fake","fail:1","fail:1","ok:1"]}]}`)
	waitRun(t, env.ts, id)
	c, err := dial(t, env.ts.URL, "run="+id+"&after=5")
	if err != nil {
		t.Fatal(err)
	}
	frames, _ := readAll(t, c, 5*time.Second)
	var first struct{ Seq int }
	_ = json.Unmarshal(frames[0], &first)
	if first.Seq != 6 {
		t.Fatalf("after=5 must start at seq 6 (notebook case 02: >= sends 5 twice); got %d", first.Seq)
	}
}

// Live delivery must come from NOTIFY: the safety poll is set to an hour, so if the
// events arrive at all within seconds, the notification woke the connection.
func TestLiveEventsArriveThroughNotifyNotPolling(t *testing.T) {
	env := newStreamEnv(t, envOpts{hubPoll: time.Hour})
	ctx := context.Background()
	bid := branch(t, env.ts, "live")
	// A run the worker will not take: running, leased to "elsewhere" for an hour.
	if _, err := env.pool.Exec(ctx, `INSERT INTO runs (id, branch_id, status, spec, lease_owner, lease_until)
		VALUES ('run_live', $1, 'running', '{"steps":[{"id":"a"}],"concurrency":1}', 'elsewhere', now() + interval '1 hour')`, bid); err != nil {
		t.Fatal(err)
	}
	c, err := dial(t, env.ts.URL, "run=run_live")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan [][]byte, 1)
	go func() { f, _ := readAll(t, c, 10*time.Second); got <- f }()
	time.Sleep(300 * time.Millisecond) // connected, read nothing, now waiting on its wake channel
	for _, batch := range [][]events.New{{{Type: "run_started"}, {Type: "step_started", Step: "a", Attempt: 1}},
		{{Type: "step_succeeded", Step: "a", Attempt: 1, AtMs: 10}, {Type: "run_finished", AtMs: 10, Reason: "succeeded"}}} {
		if err := pgx.BeginFunc(ctx, env.pool, func(tx pgx.Tx) error {
			_, err := events.Append(ctx, tx, "run_live", bid, batch)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case frames := <-got:
		if len(frames) != 4 {
			t.Fatalf("got %d live frames, want 4", len(frames))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live events never arrived: NOTIFY did not wake the stream")
	}
}

func TestSlowConsumerIsDisconnectedAndBlocksNobody(t *testing.T) {
	env := newStreamEnv(t, envOpts{writeTimeout: 300 * time.Millisecond, stall: true})
	big := createRun(t, env.ts, bigRun())
	waitRun(t, env.ts, big)

	// A reader whose socket is full: the server's writes to it block.
	env.stall.stallNext.Store(true)
	stalled, err := dial(t, env.ts.URL, "run="+big)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.CloseNow()

	// Meanwhile: a normal reader on the same run, and the worker finishing a NEW run.
	start := time.Now()
	fast, err := dial(t, env.ts.URL, "run="+big)
	if err != nil {
		t.Fatal(err)
	}
	frames, status := readAll(t, fast, 5*time.Second)
	fastTook := time.Since(start)
	other := createRun(t, env.ts, `{"steps":[{"id":"a"}]}`)
	otherRun := waitRun(t, env.ts, other)

	deadline := time.Now().Add(3 * time.Second)
	for env.hub.SlowClosed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("fast reader: %d frames, close %d, %s; slow consumers disconnected: %d",
		len(frames), status, fastTook.Round(time.Millisecond), env.hub.SlowClosed.Load())
	if len(frames) != 502 || status != websocket.StatusNormalClosure {
		t.Fatalf("the fast reader got %d frames (close %d), want all 502 and 1000", len(frames), status)
	}
	if fastTook > 2*time.Second {
		t.Fatalf("the fast reader took %s: the stalled one slowed it down", fastTook)
	}
	if otherRun["status"] != "succeeded" {
		t.Fatalf("the worker was blocked: another run is %v", otherRun["status"])
	}
	if env.hub.SlowClosed.Load() != 1 {
		t.Fatalf("the stalled reader was not disconnected (SlowClosed=%d)", env.hub.SlowClosed.Load())
	}
	// From the stalled client's side: it can read what reached it before the stall,
	// then the connection is gone (not just quiet: the read fails well before the timeout).
	began := time.Now()
	got, _ := readAll(t, stalled, 3*time.Second)
	if len(got) >= 502 || time.Since(began) > 2*time.Second {
		t.Fatalf("stalled client: %d frames, read ended after %s; want a partial stream and a closed connection",
			len(got), time.Since(began).Round(time.Millisecond))
	}
	t.Logf("stalled client received %d frames, then the server hung up; it resumes with after=%d", len(got), len(got))
}

func TestShutdownClosesWith1001(t *testing.T) {
	env := newStreamEnv(t, envOpts{})
	bid := branch(t, env.ts, "shutdown")
	if _, err := env.pool.Exec(context.Background(), `INSERT INTO runs (id, branch_id, status, spec, lease_owner, lease_until)
		VALUES ('run_wait', $1, 'running', '{"steps":[{"id":"a"}],"concurrency":1}', 'elsewhere', now() + interval '1 hour')`, bid); err != nil {
		t.Fatal(err)
	}
	c, err := dial(t, env.ts.URL, "run=run_wait")
	if err != nil {
		t.Fatal(err)
	}
	var status atomic.Int64
	done := make(chan struct{})
	go func() { _, s := readAll(t, c, 5*time.Second); status.Store(int64(s)); close(done) }()
	time.Sleep(200 * time.Millisecond)
	env.stopHub()
	<-done
	if websocket.StatusCode(status.Load()) != websocket.StatusGoingAway {
		t.Fatalf("close status %d, want 1001", status.Load())
	}
}

func TestStreamErrorsAreHTTPProblemsBeforeTheUpgrade(t *testing.T) {
	env := newStreamEnv(t, envOpts{})
	for q, want := range map[string]int{"": 422, "run=bad": 422, "run=run_nope": 404, "run=run_nope&after=-1": 422} {
		_, err := dial(t, env.ts.URL, q)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprint(want)) {
			t.Errorf("query %q: got %v, want HTTP %d", q, err, want)
		}
	}
	_, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(env.ts.URL, "http")+"/v1/stream?run=run_x", nil)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("no token: got %v, want HTTP 401", err)
	}
}
