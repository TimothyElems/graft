package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"graft/service/internal/bundle"
	"graft/service/internal/store"
)

// runBundle serves GET /v1/runs/{id}/bundle: the run's support bundle as .tar.gz
// (see package bundle for what is inside). It is built in memory first, so a failure
// part way is a problem+json error, not a truncated archive with a 200.
//
// The server removes the secrets only it knows (its token, its database password) and
// anything shaped like a credential; `graft doctor` scrubs again on the client.
func (s *Server) runBundle(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !runID.MatchString(id) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	body, m, err := bundle.Build(r.Context(), s.Pool, id, bundle.Options{
		Version: s.Version, Config: s.Config, Secrets: append([]string{s.Token}, s.Secrets...), Now: s.Now})
	if errors.Is(err, store.ErrNotFound) {
		problem(w, r, http.StatusNotFound, "not-found", "Run not found", fmt.Sprintf("No run %s.", id))
		return
	}
	if err != nil {
		s.internal(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="graft-bundle-%s.tar.gz"`, id))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("X-Graft-Redactions", strconv.Itoa(m.Redactions))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
