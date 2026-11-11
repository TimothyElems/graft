package db_test

import (
	"context"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"

	"graft/service/internal/db"
	"graft/service/internal/pgtest"
	"graft/service/migrations"
)

func open(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := db.Open(context.Background(), pgtest.NewEmpty(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigrateAppliesOnceThenNothing(t *testing.T) {
	ctx := context.Background()
	pool := open(t)
	first, err := db.Migrate(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := db.Load(migrations.FS)
	if len(first) != len(want) || first[0] != "0001_init.sql" {
		t.Fatalf("first run applied %v", first)
	}
	again, err := db.Migrate(ctx, pool)
	if err != nil || len(again) != 0 {
		t.Fatalf("second run applied %v, err %v; want nothing", again, err)
	}
}

func TestConcurrentStartsApplyEachMigrationOnce(t *testing.T) {
	ctx := context.Background()
	pool := open(t)
	var mu sync.Mutex
	var all []string
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done, err := db.Migrate(ctx, pool)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			all = append(all, done...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	want, _ := db.Load(migrations.FS)
	seen := map[string]int{}
	for _, name := range all {
		seen[name]++
	}
	for _, m := range want {
		if seen[m.Name] != 1 {
			t.Errorf("%s applied %d times by six concurrent migrators; want exactly once", m.Name, seen[m.Name])
		}
	}
	if len(all) != len(want) {
		t.Fatalf("six concurrent migrators applied %v; want each of %d migrations once", all, len(want))
	}
}

func TestEditedMigrationIsRefused(t *testing.T) {
	ctx := context.Background()
	pool := open(t)
	v1 := fstest.MapFS{"0001_a.sql": {Data: []byte("CREATE TABLE a (x int);")}}
	if _, err := db.MigrateFS(ctx, pool, v1); err != nil {
		t.Fatal(err)
	}
	edited := fstest.MapFS{"0001_a.sql": {Data: []byte("CREATE TABLE a (x int, y int);")}}
	_, err := db.MigrateFS(ctx, pool, edited)
	if err == nil || !strings.Contains(err.Error(), "edited after it was applied") {
		t.Fatalf("got %v, want an 'edited' error", err)
	}
}

func TestFailedMigrationLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	pool := open(t)
	fsys := fstest.MapFS{
		"0001_ok.sql":  {Data: []byte("CREATE TABLE ok (x int);")},
		"0002_bad.sql": {Data: []byte("CREATE TABLE half (x int); SELECT * FROM no_such_table;")},
	}
	done, err := db.MigrateFS(ctx, pool, fsys)
	if err == nil || len(done) != 1 {
		t.Fatalf("done %v err %v; want 0001 applied and 0002 failing", done, err)
	}
	var half, recorded int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE tablename = 'half'").Scan(&half)
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&recorded)
	if half != 0 || recorded != 1 {
		t.Fatalf("table 'half' exists=%d, migrations recorded=%d; the failed migration must roll back whole", half, recorded)
	}
}

func TestVersionGapsAreRejected(t *testing.T) {
	_, err := db.Load(fstest.MapFS{"0001_a.sql": {Data: []byte("")}, "0003_c.sql": {Data: []byte("")}})
	if err == nil {
		t.Fatal("0001 then 0003 must be rejected")
	}
}

// catalog describes a database's schema well enough to compare two of them.
func catalog(t *testing.T, pool *pgxpool.Pool) map[string][]string {
	t.Helper()
	q := map[string]string{
		"columns": `SELECT table_name || '.' || column_name || ' ' || data_type || ' null=' || is_nullable || ' default=' || coalesce(column_default, '')
		            FROM information_schema.columns WHERE table_schema = 'public' AND table_name <> 'schema_migrations' ORDER BY 1`,
		"constraints": `SELECT conrelid::regclass::text || ' ' || pg_get_constraintdef(oid) FROM pg_constraint
		                WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text <> 'schema_migrations' ORDER BY 1`,
		"indexes": `SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename <> 'schema_migrations' ORDER BY 1`,
	}
	out := map[string][]string{}
	for k, sql := range q {
		rows, err := pool.Query(context.Background(), sql)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out[k] = append(out[k], s)
		}
	}
	return out
}

// db/schema.sql is the readable schema; the migrations are what actually runs.
// This fails the moment they disagree.
func TestSchemaFileMatchesMigrations(t *testing.T) {
	ctx := context.Background()
	migrated := open(t)
	if _, err := db.Migrate(ctx, migrated); err != nil {
		t.Fatal(err)
	}
	sql, err := os.ReadFile("../../../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	fromFile := open(t)
	if _, err := fromFile.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("db/schema.sql does not apply: %v", err)
	}
	a, b := catalog(t, migrated), catalog(t, fromFile)
	for _, k := range []string{"columns", "constraints", "indexes"} {
		if !reflect.DeepEqual(a[k], b[k]) {
			t.Errorf("%s differ between migrations and db/schema.sql\n migrations: %v\n schema.sql: %v", k, a[k], b[k])
		}
	}
	if len(a["columns"]) < 40 {
		t.Fatalf("suspiciously few columns (%d); is the catalog query right?", len(a["columns"]))
	}
}
