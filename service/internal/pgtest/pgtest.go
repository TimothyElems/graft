// Package pgtest gives each test its own empty, migrated Postgres database.
//
// GRAFT_TEST_DATABASE_URL must point at a server where the user may CREATE DATABASE
// (any database on it, usually "postgres"). Tests FAIL, never skip, when it is unset:
// a database test that silently skips in CI is a test that never runs.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/db"
)

const EnvURL = "GRAFT_TEST_DATABASE_URL"

// URL returns the admin URL or fails the test.
func URL(t testing.TB) string {
	t.Helper()
	u := os.Getenv(EnvURL)
	if u == "" {
		t.Fatalf("%s is not set. Start Postgres and set it, e.g. postgres://graft@127.0.0.1:56543/postgres (see scripts/pg-dev.sh)", EnvURL)
	}
	return u
}

// NewEmpty creates a fresh database and returns its URL. It is dropped when the test ends.
func NewEmpty(t testing.TB) string {
	t.Helper()
	ctx := context.Background()
	admin := URL(t)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "graft_test_" + hex.EncodeToString(b)

	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("connect %s: %v", EnvURL, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	// Build the URL for the new database. (pgx.Config.ConnString() returns the ORIGINAL
	// string, so editing cfg.Database and calling it would silently reuse the admin database.)
	u, err := url.Parse(admin)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatalf("%s must be a postgres:// URL: %v", EnvURL, err)
	}
	u.Path = "/" + name
	dbURL := u.String()
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), admin)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	// Guard: prove the URL really reaches the new database, not the admin one.
	check, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect new database: %v", err)
	}
	defer check.Close(ctx)
	var got string
	if err := check.QueryRow(ctx, "SELECT current_database()").Scan(&got); err != nil || got != name {
		t.Fatalf("test database URL reached %q, want %q (%v)", got, name, err)
	}
	return dbURL
}

// New returns a pool on a fresh, fully migrated database.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := db.Open(ctx, NewEmpty(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}
