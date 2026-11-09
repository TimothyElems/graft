// Package api is graftd's HTTP surface, written to contract/openapi.yaml.
// v0.2 implements healthz, createBranch, getBranch, createRun and getRun.
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"graft/kernel/pipeline"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

const maxBody = 1 << 20

var (
	projectRe = regexp.MustCompile(`^[a-z][a-z0-9-]{1,40}$`)
	branchRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	stepRe    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	branchID  = regexp.MustCompile(`^br_[A-Za-z0-9_]+$`)
	runID     = regexp.MustCompile(`^run_[A-Za-z0-9_]+$`)
)

type Server struct {
	Store   *store.Memory
	Shell   sched.Shell
	Token   string
	Version string
	Now     func() time.Time
	Log     *slog.Logger
	queue   chan string
}

// New wires a server. Workers run queued runs until ctx is cancelled.
func New(ctx context.Context, s Server, workers int) *Server {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Log == nil {
		s.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	s.queue = make(chan string, 1024)
	srv := &s
	for i := 0; i < workers; i++ {
		go srv.worker(ctx)
	}
	return srv
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.Handle("POST /v1/projects/{project}/branches", s.auth(s.createBranch))
	mux.Handle("GET /v1/branches/{id}", s.auth(s.getBranch))
	mux.Handle("POST /v1/branches/{id}/runs", s.auth(s.createRun))
	mux.Handle("GET /v1/runs/{id}", s.auth(s.getRun))
	return s.requestID(mux)
}

// ---------------------------------------------------------------- plumbing

type ctxKey struct{}

func newID(prefix string) string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return prefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newID("req_")
		w.Header().Set("X-Request-Id", id)
		start := s.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
		s.Log.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "ms", s.Now().Sub(start).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

func reqID(r *http.Request) string { id, _ := r.Context().Value(ctxKey{}).(string); return id }

func (s *Server) auth(h http.HandlerFunc) http.Handler {
	want := []byte("Bearer " + s.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="graft"`)
			problem(w, r, http.StatusUnauthorized, "unauthorized", "Missing or invalid token",
				"Send Authorization: Bearer <token>.")
			return
		}
		h(w, r)
	})
}

// problem writes an RFC 9457 problem. Each kind has its own type URI so a client
// can branch on it, not on the status code (422 alone means two different things).
func problem(w http.ResponseWriter, r *http.Request, status int, kind, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "https://graft.dev/problems/" + kind, "title": title, "status": status,
		"detail": detail, "request_id": reqID(r),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields() // contract: additionalProperties: false
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			problem(w, r, http.StatusRequestEntityTooLarge, "body-too-large", "Request body too large",
				fmt.Sprintf("The limit is %d bytes.", maxBody))
			return false
		}
		problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request body", err.Error())
		return false
	}
	if dec.More() {
		problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request body", "Trailing data after the JSON object.")
		return false
	}
	return true
}

func invalid(w http.ResponseWriter, r *http.Request, detail string) {
	problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request", detail)
}

// ---------------------------------------------------------------- wire shapes

type branchJSON struct {
	ID        string  `json:"id"`
	Project   string  `json:"project"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	GitRef    *string `json:"git_ref"`
	CreatedAt string  `json:"created_at"`
	ExpiresAt string  `json:"expires_at"`
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func toBranch(b store.Branch) branchJSON {
	return branchJSON{b.ID, b.Project, b.Name, b.Status, b.GitRef, ts(b.CreatedAt), ts(b.ExpiresAt)}
}

type stepRunJSON struct {
	ID         string  `json:"id"`
	Status     string  `json:"status"`
	Attempts   int     `json:"attempts"`
	StartedMs  *int64  `json:"started_ms"`
	FinishedMs *int64  `json:"finished_ms"`
	Reason     *string `json:"reason"`
}

type runJSON struct {
	ID         string        `json:"id"`
	BranchID   string        `json:"branch_id"`
	Status     string        `json:"status"`
	CreatedAt  string        `json:"created_at"`
	FinishedAt *string       `json:"finished_at"`
	Steps      []stepRunJSON `json:"steps"`
}

// toRun maps the kernel's result onto the contract's Run, in the order the steps were submitted.
func toRun(r store.Run) runJSON {
	out := runJSON{ID: r.ID, BranchID: r.BranchID, Status: r.Status, CreatedAt: ts(r.CreatedAt), Steps: []stepRunJSON{}}
	if r.FinishedAt != nil {
		f := ts(*r.FinishedAt)
		out.FinishedAt = &f
	}
	for _, st := range r.Steps {
		sr := stepRunJSON{ID: st.ID, Status: "pending"}
		if r.Result != nil {
			k := r.Result.Steps[st.ID]
			sr.Status, sr.Attempts, sr.StartedMs, sr.FinishedMs = k.Status, k.Attempts, k.StartedMs, k.FinishedMs
			if k.Reason != "" { // the kernel uses "", the contract uses null
				reason := k.Reason
				sr.Reason = &reason
			}
		}
		out.Steps = append(out.Steps, sr)
	}
	return out
}

// ---------------------------------------------------------------- handlers

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "graftd %s\n\nRuns migration pipelines on short-lived database branches.\n"+
		"An independent learning project, early (v0.2): in-memory store, fake step runner.\n\n"+
		"GET /healthz\nAPI: /v1/... (bearer token required)\nContract and source: https://github.com/TimothyElems/graft\n", s.Version)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "queue_depth": s.Store.QueueDepth()})
}

type createBranchReq struct {
	Name       *string `json:"name"`
	GitRef     *string `json:"git_ref"`
	TTLSeconds *int64  `json:"ttl_seconds"`
}

func (s *Server) createBranch(w http.ResponseWriter, r *http.Request) {
	project := r.PathValue("project")
	if !projectRe.MatchString(project) {
		invalid(w, r, "project must match ^[a-z][a-z0-9-]{1,40}$")
		return
	}
	var req createBranchReq
	if !decode(w, r, &req) {
		return
	}
	if req.Name == nil || !branchRe.MatchString(*req.Name) {
		invalid(w, r, "name is required and must match ^[a-z0-9][a-z0-9-]{0,62}$")
		return
	}
	ttl := int64(86400)
	if req.TTLSeconds != nil {
		ttl = *req.TTLSeconds
	}
	if ttl < 60 || ttl > 604800 {
		invalid(w, r, "ttl_seconds must be between 60 and 604800")
		return
	}
	now := s.Now()
	b, err := s.Store.CreateBranch(store.Branch{
		ID: newID("br_"), Project: project, Name: *req.Name, Status: "ready", GitRef: req.GitRef,
		CreatedAt: now, ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
	})
	switch {
	case errors.Is(err, store.ErrNameTaken):
		problem(w, r, http.StatusConflict, "branch-name-taken", "Branch name taken",
			fmt.Sprintf("Project %s already has a live branch named %s.", project, *req.Name))
		return
	case errors.Is(err, store.ErrBranchLimit):
		problem(w, r, http.StatusConflict, "branch-limit", "Branch limit reached",
			fmt.Sprintf("Project %s already has %d live branches.", project, store.MaxLiveBranches))
		return
	case err != nil:
		problem(w, r, http.StatusInternalServerError, "internal", "Internal error", "")
		return
	}
	writeJSON(w, http.StatusCreated, toBranch(b))
}

func (s *Server) getBranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	b, err := s.Store.GetBranch(id)
	if !branchID.MatchString(id) || err != nil {
		problem(w, r, http.StatusNotFound, "not-found", "Branch not found", fmt.Sprintf("No branch %s.", id))
		return
	}
	writeJSON(w, http.StatusOK, toBranch(b))
}

type stepSpecReq struct {
	ID          *string  `json:"id"`
	Needs       []string `json:"needs"`
	Image       string   `json:"image"`
	Command     []string `json:"command"`
	MaxAttempts *int     `json:"max_attempts"`
	TimeoutMs   *int64   `json:"timeout_ms"`
	BackoffMs   *int64   `json:"backoff_ms"`
}

type createRunReq struct {
	Steps       []stepSpecReq `json:"steps"`
	Concurrency *int          `json:"concurrency"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	bid := r.PathValue("id")
	b, err := s.Store.GetBranch(bid)
	if !branchID.MatchString(bid) || err != nil {
		problem(w, r, http.StatusNotFound, "not-found", "Branch not found", fmt.Sprintf("No branch %s.", bid))
		return
	}
	var req createRunReq
	if !decode(w, r, &req) {
		return
	}
	if len(req.Steps) < 1 || len(req.Steps) > 64 {
		invalid(w, r, "steps must have between 1 and 64 items")
		return
	}
	conc := 1
	if req.Concurrency != nil {
		conc = *req.Concurrency
	}
	if conc < 1 || conc > 16 {
		invalid(w, r, "concurrency must be between 1 and 16")
		return
	}
	steps := make([]store.StepSpec, 0, len(req.Steps))
	for i, st := range req.Steps {
		if st.ID == nil || !stepRe.MatchString(*st.ID) {
			invalid(w, r, fmt.Sprintf("steps[%d].id is required and must match ^[a-z][a-z0-9_]{0,31}$", i))
			return
		}
		spec := store.StepSpec{ID: *st.ID, Needs: st.Needs, Image: st.Image, Command: st.Command, MaxAttempts: 1}
		if st.MaxAttempts != nil {
			spec.MaxAttempts = *st.MaxAttempts
		}
		if spec.MaxAttempts < 1 || spec.MaxAttempts > 10 {
			invalid(w, r, fmt.Sprintf("steps[%d].max_attempts must be between 1 and 10", i))
			return
		}
		if st.TimeoutMs != nil {
			spec.TimeoutMs = *st.TimeoutMs
		}
		if st.BackoffMs != nil {
			spec.BackoffMs = *st.BackoffMs
		}
		if spec.TimeoutMs < 0 || spec.BackoffMs < 0 {
			invalid(w, r, fmt.Sprintf("steps[%d]: timeout_ms and backoff_ms must be 0 or more", i))
			return
		}
		steps = append(steps, spec)
	}
	// Refuse a bad pipeline now, with the kernel's own error, rather than accept it and fail later.
	if _, err := s.Shell.Case(steps, conc); err != nil {
		problem(w, r, http.StatusUnprocessableEntity, "invalid-pipeline", "Invalid pipeline", err.Error())
		return
	}
	run := s.Store.CreateRun(store.Run{
		ID: newID("run_"), BranchID: b.ID, Status: "queued", Steps: steps, Concurrency: conc, CreatedAt: s.Now(),
	})
	select {
	case s.queue <- run.ID:
	default:
		s.finish(run.ID, nil, errors.New("queue full"))
	}
	writeJSON(w, http.StatusAccepted, toRun(run))
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := s.Store.GetRun(id)
	if !runID.MatchString(id) || err != nil {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	writeJSON(w, http.StatusOK, toRun(run))
}

// ---------------------------------------------------------------- worker

func (s *Server) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-s.queue:
			run, err := s.Store.UpdateRun(id, func(r *store.Run) { r.Status = "running" })
			if err != nil {
				continue
			}
			res, err := s.Shell.Execute(run)
			s.finish(id, &res, err)
		}
	}
}

func (s *Server) finish(id string, res *pipeline.Result, err error) {
	now := s.Now()
	_, _ = s.Store.UpdateRun(id, func(r *store.Run) {
		r.FinishedAt = &now
		if err != nil {
			r.Status, r.Err = "failed", err.Error()
			s.Log.Error("run failed in shell", "run_id", id, "err", err)
			return
		}
		r.Result = res
		r.Status = res.RunStatus // the kernel decides; the shell only records it
	})
}
