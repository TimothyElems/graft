package api

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"graft/service/internal/events"
	"graft/service/internal/sched"
	"graft/service/internal/store"
)

var (
	stepRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	runID  = regexp.MustCompile(`^run_[A-Za-z0-9_]+$`)
)

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

func toRun(r store.Run) runJSON {
	out := runJSON{ID: r.ID, BranchID: r.BranchID, Status: r.Status, CreatedAt: ts(r.CreatedAt), Steps: []stepRunJSON{}}
	if r.FinishedAt != nil {
		f := ts(*r.FinishedAt)
		out.FinishedAt = &f
	}
	for _, s := range r.Steps {
		out.Steps = append(out.Steps, stepRunJSON{s.ID, s.Status, s.Attempts, s.StartedMs, s.FinishedMs, s.Reason})
	}
	return out
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
	notFound := func() {
		problem(w, r, http.StatusNotFound, "not-found", "Branch not found", fmt.Sprintf("No branch %s.", bid))
	}
	if !branchID.MatchString(bid) {
		notFound()
		return
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var req createRunReq
	if err := decodeBody(body, &req); err != nil {
		problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request body", err.Error())
		return
	}
	spec, msg := validateRun(req)
	if msg != "" {
		invalid(w, r, msg)
		return
	}
	// Refuse a bad pipeline now, with the kernel's own error, rather than accept it and fail later.
	if err := sched.Validate(spec); err != nil {
		problem(w, r, http.StatusUnprocessableEntity, "invalid-pipeline", "Invalid pipeline", err.Error())
		return
	}
	q := dbtx(r, s.Pool)
	if _, err := store.GetBranch(r.Context(), q, bid); errors.Is(err, store.ErrNotFound) {
		notFound()
		return
	} else if err != nil {
		s.internal(w, r, err)
		return
	}
	run, err := store.CreateRun(r.Context(), q, newID("run_"), bid, spec, s.Now())
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toRun(run))
}

func validateRun(req createRunReq) (store.RunSpec, string) {
	if len(req.Steps) < 1 || len(req.Steps) > 64 {
		return store.RunSpec{}, "steps must have between 1 and 64 items"
	}
	spec := store.RunSpec{Concurrency: 1}
	if req.Concurrency != nil {
		spec.Concurrency = *req.Concurrency
	}
	if spec.Concurrency < 1 || spec.Concurrency > 16 {
		return store.RunSpec{}, "concurrency must be between 1 and 16"
	}
	for i, st := range req.Steps {
		if st.ID == nil || !stepRe.MatchString(*st.ID) {
			return store.RunSpec{}, fmt.Sprintf("steps[%d].id is required and must match ^[a-z][a-z0-9_]{0,31}$", i)
		}
		step := store.StepSpec{ID: *st.ID, Needs: st.Needs, Image: st.Image, Command: st.Command, MaxAttempts: 1}
		if st.MaxAttempts != nil {
			step.MaxAttempts = *st.MaxAttempts
		}
		if step.MaxAttempts < 1 || step.MaxAttempts > 10 {
			return store.RunSpec{}, fmt.Sprintf("steps[%d].max_attempts must be between 1 and 10", i)
		}
		if st.TimeoutMs != nil {
			step.TimeoutMs = *st.TimeoutMs
		}
		if st.BackoffMs != nil {
			step.BackoffMs = *st.BackoffMs
		}
		if step.TimeoutMs < 0 || step.BackoffMs < 0 {
			return store.RunSpec{}, fmt.Sprintf("steps[%d]: timeout_ms and backoff_ms must be 0 or more", i)
		}
		spec.Steps = append(spec.Steps, step)
	}
	return spec, ""
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := store.GetRun(r.Context(), s.Pool, id)
	if !runID.MatchString(id) || errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toRun(run))
}

type eventPage struct {
	Items   []events.Event `json:"items"`
	LastSeq int            `json:"last_seq"`
}

func (s *Server) listRunEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runID.MatchString(id) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	if _, err := store.GetRun(r.Context(), s.Pool, id); errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	} else if err != nil {
		s.internal(w, r, err)
		return
	}
	after := 0
	if v := r.URL.Query().Get("after"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			invalid(w, r, "after must be an integer >= 0")
			return
		}
		after = n
	}
	limit, ok := limitParam(w, r)
	if !ok {
		return
	}
	evs, err := events.List(r.Context(), s.Pool, id, after, limit)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	out := eventPage{Items: evs, LastSeq: after}
	if out.Items == nil {
		out.Items = []events.Event{}
	}
	if len(evs) > 0 {
		out.LastSeq = evs[len(evs)-1].Seq
	}
	writeJSON(w, http.StatusOK, out)
}

// stream checks everything that can fail as a plain HTTP problem BEFORE the upgrade,
// so a client gets 401/404/422 it can act on, not a mysterious close code.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("run")
	if !runID.MatchString(id) {
		invalid(w, r, "run is required and must look like run_...")
		return
	}
	after := 0
	if v := r.URL.Query().Get("after"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			invalid(w, r, "after must be an integer >= 0")
			return
		}
		after = n
	}
	if _, err := store.GetRun(r.Context(), s.Pool, id); errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	} else if err != nil {
		s.internal(w, r, err)
		return
	}
	// The server's read/write timeouts are deadlines on the connection, and they survive
	// a hijack: left alone, every stream would be cut after WriteTimeout. Clear them;
	// the hub enforces its own per-frame write deadline instead.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})
	_ = rc.SetWriteDeadline(time.Time{})
	s.Hub.Serve(w, r, id, after)
}

// cancelRun asks the worker running this run to stop (rule 5: what finishes before the
// cancel takes effect counts; everything else is cancelled). Safe to repeat. A queued
// run is cancelled at time 0 when a worker picks it up; a finished run is returned
// unchanged with 202 (contract gap 6: decided as "accepted, nothing to do").
func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runID.MatchString(id) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	tag, err := s.Pool.Exec(r.Context(), `UPDATE runs SET cancel_requested = true
		WHERE id = $1 AND status IN ('queued', 'running')`, id)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	_ = tag
	run, err := store.GetRun(r.Context(), s.Pool, id)
	if errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toRun(run))
}
