package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/db"
)

// IdempotencyTTL is how long a key's first response is kept for replay.
const IdempotencyTTL = "24 hours"

type txKey struct{}

// dbtx returns the request's transaction if idempotency opened one, else the pool.
func dbtx(r *http.Request, pool *pgxpool.Pool) db.DBTX {
	if tx, ok := r.Context().Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return pool
}

// canonical hashes the request's meaning, not its bytes: whitespace and key order in
// the JSON body do not change it. A body that is not JSON is hashed as raw bytes.
func canonical(method, path string, body []byte) []byte {
	var v any
	norm := body
	if json.Unmarshal(body, &v) == nil {
		if b, err := json.Marshal(v); err == nil { // Go marshals map keys sorted
			norm = b
		}
	}
	h := sha256.New()
	h.Write([]byte(method + " " + path + "\n"))
	h.Write(norm)
	return h.Sum(nil)
}

// captured is a response held back until the transaction commits.
type captured struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (c *captured) Header() http.Header         { return c.header }
func (c *captured) WriteHeader(code int)        { c.status = code }
func (c *captured) Write(b []byte) (int, error) { return c.body.Write(b) }

// idempotent wraps a create handler. With an Idempotency-Key header:
//   - same key, same request: the first successful response is replayed; nothing new is created
//   - same key, different request: 422 idempotency-key-reused
//   - concurrent requests with one key: serialised by an advisory lock; later ones replay
//
// The handler runs inside a transaction together with storing its response, so a
// create and its key commit together or not at all. Only 2xx responses are stored:
// a request that failed (e.g. validation) can be retried with the same key.
func (s *Server) idempotent(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			h(w, r)
			return
		}
		if len(key) > 64 {
			invalid(w, r, "Idempotency-Key must be at most 64 characters")
			return
		}
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		scope := r.Method + " " + r.URL.Path
		hash := canonical(r.Method, r.URL.Path, body)
		ctx := r.Context()

		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			s.internal(w, r, err)
			return
		}
		defer tx.Rollback(context.Background()) //nolint:errcheck // no-op after Commit

		// One request per (scope, key) at a time. Released at commit or rollback.
		// (Newline separator: Postgres text cannot hold a NUL byte, and a path cannot hold "\n".)
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", scope+"\n"+key); err != nil {
			s.internal(w, r, err)
			return
		}
		var storedHash, storedBody []byte
		var storedStatus int
		err = tx.QueryRow(ctx, `
			SELECT request_hash, status_code, response FROM idempotency_keys
			 WHERE scope = $1 AND key = $2 AND created_at > now() - $3::interval`, scope, key, IdempotencyTTL).
			Scan(&storedHash, &storedStatus, &storedBody)
		switch {
		case err == nil && !bytes.Equal(storedHash, hash):
			problem(w, r, http.StatusUnprocessableEntity, "idempotency-key-reused", "Idempotency-Key reused",
				"This key was already used with a different request. Use a new key for a new request.")
			return
		case err == nil:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(storedStatus)
			_, _ = w.Write(storedBody)
			return
		case !errors.Is(err, pgx.ErrNoRows):
			s.internal(w, r, err)
			return
		}

		// First time (or expired): run the handler inside this transaction.
		rec := &captured{header: http.Header{}, status: http.StatusOK}
		r2 := r.WithContext(context.WithValue(ctx, txKey{}, tx))
		r2.Body = io.NopCloser(bytes.NewReader(body))
		h(rec, r2)

		if rec.status >= 200 && rec.status < 300 {
			if s.idemBeforeStore != nil {
				s.idemBeforeStore() // test seam: widens the race window
			}
			// Replace a row only if it has expired. If a LIVE row for this key appears
			// here, the lock above did not hold (it always should): refuse rather than
			// overwrite another request's stored response.
			tag, err := tx.Exec(ctx, `
				INSERT INTO idempotency_keys (scope, key, request_hash, status_code, response)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (scope, key) DO UPDATE
				  SET request_hash = $3, status_code = $4, response = $5, created_at = now()
				  WHERE idempotency_keys.created_at <= now() - $6::interval`,
				scope, key, hash, rec.status, rec.body.Bytes(), IdempotencyTTL)
			if err != nil {
				s.internal(w, r, err)
				return
			}
			if tag.RowsAffected() == 0 {
				problem(w, r, http.StatusConflict, "idempotency-key-in-use", "Idempotency-Key in use",
					"Another request with this key is being processed. Retry shortly.")
				return
			}
			if err := tx.Commit(ctx); err != nil {
				s.internal(w, r, err)
				return
			}
		}
		for k, v := range rec.header {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body.Bytes())
	}
}
