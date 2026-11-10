// Package db applies graftd's migrations and opens its connection pool.
package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/migrations"
)

// migrateLock is an arbitrary constant: every graftd uses the same advisory lock id,
// so only one process migrates at a time and the rest wait, then find nothing to do.
const migrateLock = 7_210_331

var fileRe = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string
}

// Load reads the embedded migrations in version order and rejects gaps or bad names.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		if e.IsDir() || e.Name() == "embed.go" {
			continue
		}
		m := fileRe.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q: name must look like 0001_what_it_does.sql", e.Name())
		}
		b, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		v, _ := strconv.Atoi(m[1])
		sum := sha256.Sum256(b)
		out = append(out, Migration{Version: v, Name: e.Name(), SQL: string(b), Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration %s: expected version %04d (no gaps, no duplicates)", m.Name, i+1)
		}
	}
	return out, nil
}

// Migrate applies every migration not yet recorded, each in its own transaction,
// holding a session-level advisory lock for the whole run. It returns the names applied.
// An already-applied migration whose file changed is an error: never edit history, add a file.
func Migrate(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	return MigrateFS(ctx, pool, migrations.FS)
}

func MigrateFS(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) ([]string, error) {
	ms, err := Load(fsys)
	if err != nil {
		return nil, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrateLock); err != nil {
		return nil, err
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrateLock) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version integer PRIMARY KEY, name text NOT NULL, checksum text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	applied := map[int]string{}
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			return nil, err
		}
		applied[v] = sum
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var done []string
	for _, m := range ms {
		if sum, ok := applied[m.Version]; ok {
			if sum != m.Checksum {
				return done, fmt.Errorf("migration %s was edited after it was applied (checksum differs); add a new migration instead", m.Name)
			}
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("migration %s: %w", m.Name, err)
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
				m.Version, m.Name, m.Checksum)
			return err
		})
		if err != nil {
			return done, err
		}
		done = append(done, m.Name)
	}
	return done, nil
}

// Open connects and pings, so a bad DATABASE_URL fails at start, not on the first request.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
