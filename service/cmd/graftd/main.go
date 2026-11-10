// graftd serves the Graft API and runs pipelines.
//
// Settings come from graft.toml (see the file in the repo for every key), overridden
// by GRAFT_* environment variables, and are validated at start: a bad value stops
// graftd with a message naming the key.
//
//	-config / GRAFT_CONFIG  path to graft.toml (default ./graft.toml if it exists)
//	GRAFT_TOKEN             required secret: the bearer token clients must send
//	GRAFT_DATABASE_URL      required secret: postgres://... (migrations run at start)
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
	"strings"
	"sync"
	"syscall"
	"time"

	"graft/service/internal/api"
	"graft/service/internal/config"
	"graft/service/internal/db"
	"graft/service/internal/lifecycle"
	"graft/service/internal/obs"
	"graft/service/internal/queue"
	"graft/service/internal/runner"
	"graft/service/internal/sched"
	"graft/service/internal/store"
	"graft/service/internal/stream"
)

var version = "v0.6.0-dev"

func main() {
	health := flag.Bool("healthcheck", false, "GET /healthz on this machine and exit")
	path := flag.String("config", envOr("GRAFT_CONFIG", "graft.toml"), "path to graft.toml")
	flag.Parse()

	cfg, err := config.Load(*path, os.Getenv)
	if err != nil {
		// Plain text, one problem per line: this is read by a person, at startup.
		fmt.Fprintf(os.Stderr, "graftd: invalid configuration, not starting:\n  %s\n",
			strings.ReplaceAll(err.Error(), "\n", "\n  "))
		os.Exit(2)
	}
	if *health {
		os.Exit(healthcheck(cfg.Server.Addr))
	}

	log := slog.New(obs.LogHandler(slog.NewJSONHandler(os.Stdout, nil)))
	token := os.Getenv("GRAFT_TOKEN")
	if len(token) < 16 {
		log.Error("GRAFT_TOKEN must be set (16+ characters); refusing to start without auth")
		os.Exit(2)
	}
	dbURL := os.Getenv("GRAFT_DATABASE_URL")
	if dbURL == "" {
		log.Error("GRAFT_DATABASE_URL must be set")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	if cfg.Runner.Kind == "docker" {
		run = runner.Router{Fake: runner.Script{}, Real: runner.Docker{
			DefaultImage: cfg.Runner.DefaultImage, Memory: cfg.Runner.Memory, CPUs: cfg.Runner.CPUs,
			Pids: cfg.Runner.Pids, User: cfg.Runner.User, TmpSize: cfg.Runner.TmpSize}}
	}

	o, err := obs.New(ctx, func(c context.Context) (int, error) { return store.QueueDepth(c, pool) })
	if err != nil {
		log.Error("observability", "err", err)
		os.Exit(1)
	}
	defer func() { _ = o.Shutdown(context.Background()) }()
	// Metrics on their own, internal address: not on the public API port.
	ms := &http.Server{Addr: cfg.Server.MetricsAddr, Handler: o.MetricsHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ms.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	defer ms.Close()

	// Two contexts: `ctx` (cancelled by SIGTERM) stops claiming; `work` (cancelled after
	// the grace period) stops runs still in progress, which are then released.
	work, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	clock := sched.Clock{Runner: run}
	host, _ := os.Hostname()
	var workersDone sync.WaitGroup
	for i := 0; i < cfg.Workers.Count; i++ {
		w := &queue.Worker{Pool: pool, Clock: clock, Owner: fmt.Sprintf("%s-%d-w%d", host, os.Getpid(), i),
			Lease: cfg.Workers.Lease.Duration, Heartbeat: cfg.Workers.Heartbeat.Duration,
			CancelPoll: cfg.Workers.CancelPoll.Duration, Log: log, Tracer: o.Tracer,
			Hooks: queue.Hooks{Claimed: o.Claimed, Events: o.Events, Finished: o.Finished}}
		workersDone.Add(1)
		go func() { defer workersDone.Done(); w.RunUntil(ctx, work) }()
	}
	hub := &stream.Hub{Pool: pool, Log: log, WriteTimeout: cfg.Stream.WriteTimeout.Duration, Poll: cfg.Stream.Poll.Duration}
	go hub.Run(ctx)
	srv := api.New(api.Server{Pool: pool, Token: token, Version: version, Log: log, Hub: hub, Obs: o})

	hs := &http.Server{Addr: cfg.Server.Addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		log.Info("listening", "addr", cfg.Server.Addr, "metrics_addr", cfg.Server.MetricsAddr, "version", version,
			"workers", cfg.Workers.Count, "runner", cfg.Runner.Kind)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down: no new runs", "grace", cfg.Shutdown.Grace, "deadline", cfg.Shutdown.Deadline)
	finished := make(chan struct{})
	go func() { workersDone.Wait(); close(finished) }()
	lifecycle.Shutdown(lifecycle.Plan{Grace: cfg.Shutdown.Grace.Duration, Deadline: cfg.Shutdown.Deadline.Duration, Log: log,
		StopHTTP: hs.Shutdown, StopWork: stopWork, WorkersDone: finished, Exit: os.Exit})
}

func envOr(k, def string) string {
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
