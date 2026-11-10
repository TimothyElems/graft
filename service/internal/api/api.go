// Package api is graftd's HTTP surface, written to contract/openapi.yaml.
// v0.3: healthz, branches (create, get, list with cursor pages), runs (create,
// get, events). State lives in Postgres; queue.Worker executes runs.
package api

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/sched"
	"graft/service/internal/store"
	"graft/service/internal/stream"
)

const maxBody = 1 << 20

type Server struct {
	Pool    *pgxpool.Pool
	Shell   sched.Shell
	Token   string
	Version string
	Now     func() time.Time
	Log     *slog.Logger
	Hub     *stream.Hub

	idemBeforeStore func() // tests only: runs after a keyed handler, before its key is stored
}

func New(s Server) *Server {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Log == nil {
		s.Log = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.Handle("POST /v1/projects/{project}/branches", s.auth(s.idempotent(s.createBranch)))
	mux.Handle("GET /v1/projects/{project}/branches", s.auth(s.listBranches))
	mux.Handle("GET /v1/branches/{id}", s.auth(s.getBranch))
	mux.Handle("POST /v1/branches/{id}/runs", s.auth(s.idempotent(s.createRun)))
	mux.Handle("GET /v1/runs/{id}", s.auth(s.getRun))
	mux.Handle("GET /v1/runs/{id}/events", s.auth(s.listRunEvents))
	mux.Handle("GET /v1/stream", s.auth(s.stream))
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
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
		s.Log.Info("request", "request_id", id, "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "ms", time.Since(start).Milliseconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }

// Hijack and Unwrap let a websocket upgrade (and http.ResponseController) reach the
// real connection through this wrapper.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer cannot hijack")
	}
	r.status = http.StatusSwitchingProtocols
	return h.Hijack()
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func reqID(r *http.Request) string { id, _ := r.Context().Value(ctxKey{}).(string); return id }

func (s *Server) auth(h http.HandlerFunc) http.Handler {
	want := []byte("Bearer " + s.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="graft"`)
			problem(w, r, http.StatusUnauthorized, "unauthorized", "Missing or invalid token",
				"Send Authorization: Bearer <token>.")
			return
		}
		h(w, r)
	})
}

// problem writes an RFC 9457 problem. Each kind has its own type URI so a client
// can branch on it, not on the status code (422 alone means several things).
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

func (s *Server) internal(w http.ResponseWriter, r *http.Request, err error) {
	s.Log.Error("internal error", "request_id", reqID(r), "err", err)
	problem(w, r, http.StatusInternalServerError, "internal", "Internal error",
		"Something went wrong on our side. Quote the request_id if you report it.")
}

func decodeBody(body []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields() // contract: additionalProperties: false
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after the JSON object")
	}
	return nil
}

// readBody reads at most maxBody bytes. Handlers get the bytes, not the stream,
// because idempotency needs to hash exactly what was sent.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			problem(w, r, http.StatusRequestEntityTooLarge, "body-too-large", "Request body too large",
				fmt.Sprintf("The limit is %d bytes.", maxBody))
			return nil, false
		}
		problem(w, r, http.StatusBadRequest, "bad-request", "Could not read the request body", err.Error())
		return nil, false
	}
	return b, true
}

func invalid(w http.ResponseWriter, r *http.Request, detail string) {
	problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request", detail)
}

func limitParam(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 200 {
		invalid(w, r, "limit must be an integer between 1 and 200")
		return 0, false
	}
	return n, true
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// ---------------------------------------------------------------- simple routes

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "graftd %s\n\nRuns migration pipelines on short-lived database branches.\n"+
		"An independent learning project, early: Postgres-backed queue, fake step runner.\n\n"+
		"GET /healthz\nAPI: /v1/... (bearer token required)\nContract and source: https://github.com/TimothyElems/graft\n", s.Version)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	n, err := store.QueueDepth(r.Context(), s.Pool)
	if err != nil {
		problem(w, r, http.StatusServiceUnavailable, "unavailable", "Database unavailable", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "queue_depth": n})
}
