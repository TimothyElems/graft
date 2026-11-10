package api

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"graft/service/internal/store"
)

var (
	projectRe = regexp.MustCompile(`^[a-z][a-z0-9-]{1,40}$`)
	branchRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	branchID  = regexp.MustCompile(`^br_[A-Za-z0-9_]+$`)
)

type branchJSON struct {
	ID        string  `json:"id"`
	Project   string  `json:"project"`
	Name      string  `json:"name"`
	Status    string  `json:"status"`
	GitRef    *string `json:"git_ref"`
	CreatedAt string  `json:"created_at"`
	ExpiresAt string  `json:"expires_at"`
}

func toBranch(b store.Branch) branchJSON {
	return branchJSON{b.ID, b.Project, b.Name, b.Status, b.GitRef, ts(b.CreatedAt), ts(b.ExpiresAt)}
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
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	var req createBranchReq
	if err := decodeBody(body, &req); err != nil {
		problem(w, r, http.StatusUnprocessableEntity, "validation", "Invalid request body", err.Error())
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
	b, err := store.CreateBranch(r.Context(), dbtx(r, s.Pool), store.Branch{
		ID: newID("br_"), Project: project, Name: *req.Name, Status: "ready", GitRef: req.GitRef,
		CreatedAt: now, ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
	})
	switch {
	case errors.Is(err, store.ErrNameTaken):
		problem(w, r, http.StatusConflict, "branch-name-taken", "Branch name taken",
			fmt.Sprintf("Project %s already has a live branch named %s.", project, *req.Name))
	case errors.Is(err, store.ErrBranchLimit):
		problem(w, r, http.StatusConflict, "branch-limit", "Branch limit reached",
			fmt.Sprintf("Project %s already has %d live branches.", project, store.MaxLiveBranches))
	case err != nil:
		s.internal(w, r, err)
	default:
		writeJSON(w, http.StatusCreated, toBranch(b))
	}
}

func (s *Server) getBranch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !branchID.MatchString(id) {
		problem(w, r, http.StatusNotFound, "not-found", "Branch not found", fmt.Sprintf("No branch %s.", id))
		return
	}
	b, err := store.GetBranch(r.Context(), s.Pool, id)
	if errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Branch not found", fmt.Sprintf("No branch %s.", id))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBranch(b))
}

type branchPage struct {
	Items      []branchJSON `json:"items"`
	NextCursor *string      `json:"next_cursor"`
}

// listBranches pages newest first. The cursor is opaque to clients: base64 of
// (created_at, id). Both parts are needed; see store.Cursor.
func (s *Server) listBranches(w http.ResponseWriter, r *http.Request) {
	project := r.PathValue("project")
	if !projectRe.MatchString(project) {
		invalid(w, r, "project must match ^[a-z][a-z0-9-]{1,40}$")
		return
	}
	limit, ok := limitParam(w, r)
	if !ok {
		return
	}
	var after *store.Cursor
	if c := r.URL.Query().Get("cursor"); c != "" {
		cur, err := store.DecodeCursor(c)
		if err != nil {
			problem(w, r, http.StatusUnprocessableEntity, "bad-cursor", "Invalid cursor",
				"Pass next_cursor exactly as the previous page returned it.")
			return
		}
		after = &cur
	}
	page, next, err := store.ListBranches(r.Context(), s.Pool, project, after, limit)
	if err != nil {
		s.internal(w, r, err)
		return
	}
	out := branchPage{Items: make([]branchJSON, 0, len(page))}
	for _, b := range page {
		out.Items = append(out.Items, toBranch(b))
	}
	if next != nil {
		c := next.Encode()
		out.NextCursor = &c
	}
	writeJSON(w, http.StatusOK, out)
}
