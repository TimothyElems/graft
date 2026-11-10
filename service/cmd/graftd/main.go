// graftd serves the Graft API.
//
//	GRAFT_TOKEN        required: the bearer token clients must send
//	GRAFT_DATABASE_URL required: postgres://... (migrations run at start)
//	GRAFT_ADDR         listen address, default :8080
//	GRAFT_WORKERS      number of run workers, default 2
//
// `graftd -healthcheck` GETs /healthz and exits 0 or 1, for container health
// checks in an image that has no curl.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"graft/service/internal/api"
	"graft/service/internal/db"
	"graft/service/internal/lifecycle"
	"graft/service/internal/queue"
	"graft/service/internal/runner"
	"graft/service/internal/sched"
	"graft/service/internal/stream"
)

var version = "v0.6.0-dev"

func main() {
	health := flag.Bool("healthcheck", false, "GET /healthz on this machine and exit")
	flag.Parse()
	addr := env("GRAFT_ADDR", ":8080")
	if *health {
		os.Exit(healthcheck(addr))
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	token := os.Getenv("GRAFT_TOKEN")
	if len(token) < 16 {
		log.Error("GRAFT_TOKEN must be set (16+ characters); refusing to start without auth")
		os.Exit(2)
	}
	workers, err := strconv.Atoi(env("GRAFT_WORKERS", "2"))
	if err != nil || workers < 1 {
		log.Error("GRAFT_WORKERS must be a positive integer", "value", os.Getenv("GRAFT_WORKERS"))
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("GRAFT_DATABASE_URL")
	if dbURL == "" {
		log.Error("GRAFT_DATABASE_URL must be set")
		os.Exit(2)
	}
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	applied, err := db.Migrate(ctx, pool)
	if err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	log.Info("migrations", "applied", applied)

	var run runner.Runner = runner.Script{}
	if os.Getenv("GRAFT_RUNNER") == "docker" {
		run = runner.Router{Fake: runner.Script{}, Real: runner.Docker{}}
	}
	clock := sched.Clock{Runner: run}
	lease, err1 := durationEnv("GRAFT_LEASE", queue.DefaultLease)
	beat, err2 := durationEnv("GRAFT_HEARTBEAT", queue.DefaultHeartbeat)
	if err := errors.Join(err1, err2); err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	grace, err3 := durationEnv("GRAFT_SHUTDOWN_GRACE", 10*time.Second)
	deadline, err4 := durationEnv("GRAFT_SHUTDOWN_DEADLINE", 25*time.Second)
	if err := errors.Join(err3, err4); err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	// Two contexts: `ctx` (cancelled by SIGTERM) stops claiming; `work` (cancelled after
	// the grace period) stops runs still in progress, which are then released.
	work, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	host, _ := os.Hostname()
	var workersDone sync.WaitGroup
	for i := 0; i < workers; i++ {
		w := &queue.Worker{Pool: pool, Clock: clock, Owner: fmt.Sprintf("%s-%d-w%d", host, os.Getpid(), i),
			Lease: lease, Heartbeat: beat, Log: log}
		workersDone.Add(1)
		go func() { defer workersDone.Done(); w.RunUntil(ctx, work) }()
	}
	hub := &stream.Hub{Pool: pool, Log: log}
	go hub.Run(ctx)
	srv := api.New(api.Server{Pool: pool, Token: token, Version: version, Log: log, Hub: hub})

	hs := &http.Server{Addr: addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		log.Info("listening", "addr", addr, "version", version, "workers", workers)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down: no new runs", "grace", grace, "deadline", deadline)
	finished := make(chan struct{})
	go func() { workersDone.Wait(); close(finished) }()
	lifecycle.Shutdown(lifecycle.Plan{Grace: grace, Deadline: deadline, Log: log,
		StopHTTP: hs.Shutdown, StopWork: stopWork, WorkersDone: finished, Exit: os.Exit})
}

func durationEnv(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s=%q: want a positive duration like 30s", k, v)
	}
	return d, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func healthcheck(addr string) int {
	port := addr
	if len(port) > 0 && port[0] == ':' {
		port = "127.0.0.1" + port
	}
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(fmt.Sprintf("http://%s/healthz", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
