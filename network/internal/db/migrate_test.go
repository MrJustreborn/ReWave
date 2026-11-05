package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"
)

func timeMustParse(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	d, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func fsWithMigrations(files map[string]string) fstest.MapFS {
	mapFS := make(fstest.MapFS, len(files))
	for name, content := range files {
		mapFS["migrations/"+name] = &fstest.MapFile{Data: []byte(content)}
	}
	return mapFS
}

const createFoo = "CREATE TABLE foo (id INTEGER PRIMARY KEY);"
const createBar = "CREATE TABLE bar (id INTEGER PRIMARY KEY);"

func TestApplyFreshDatabase(t *testing.T) {
	d := testDB(t)
	fsys := fsWithMigrations(map[string]string{
		"0001_init.sql": createFoo,
		"0002_next.sql": createBar,
	})
	applied, err := applyMigrations(context.Background(), d, fsys)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(applied) != 2 || applied[0] != "0001_init.sql" || applied[1] != "0002_next.sql" {
		t.Fatalf("applied = %v", applied)
	}
	// Re-applying is a no-op.
	applied, err = applyMigrations(context.Background(), d, fsys)
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if len(applied) != 0 {
		t.Fatalf("re-apply applied %v, want none", applied)
	}
}

func TestApplyRejectsEditedMigration(t *testing.T) {
	d := testDB(t)
	original := fsWithMigrations(map[string]string{"0001_init.sql": createFoo})
	if _, err := applyMigrations(context.Background(), d, original); err != nil {
		t.Fatalf("apply: %v", err)
	}
	edited := fsWithMigrations(map[string]string{"0001_init.sql": createFoo + "CREATE TABLE oops (id INTEGER);"})
	if _, err := applyMigrations(context.Background(), d, edited); err == nil {
		t.Fatal("expected checksum-mismatch error")
	}
}

func TestApplyRejectsDeletedMigration(t *testing.T) {
	d := testDB(t)
	fsys := fsWithMigrations(map[string]string{
		"0001_init.sql": createFoo,
		"0002_next.sql": createBar,
	})
	if _, err := applyMigrations(context.Background(), d, fsys); err != nil {
		t.Fatalf("apply: %v", err)
	}
	removed := fsWithMigrations(map[string]string{"0001_init.sql": createFoo})
	if _, err := applyMigrations(context.Background(), d, removed); err == nil {
		t.Fatal("expected deleted-migration error")
	}
}

func TestApplyRejectsOutOfOrderMigration(t *testing.T) {
	d := testDB(t)
	fsys := fsWithMigrations(map[string]string{"0001_init.sql": createFoo})
	if _, err := applyMigrations(context.Background(), d, fsys); err != nil {
		t.Fatalf("apply: %v", err)
	}
	outOfOrder := fsWithMigrations(map[string]string{
		"0001_init.sql":  createFoo,
		"0000_early.sql": "CREATE TABLE early (id INTEGER PRIMARY KEY);",
	})
	if _, err := applyMigrations(context.Background(), d, outOfOrder); err == nil {
		t.Fatal("expected out-of-order error")
	}
}

func TestApplyRejectsBadFileName(t *testing.T) {
	d := testDB(t)
	fsys := fsWithMigrations(map[string]string{"init.sql": createFoo})
	if _, err := applyMigrations(context.Background(), d, fsys); err == nil {
		t.Fatal("expected naming-convention error")
	}
}

func TestStatus(t *testing.T) {
	d := testDB(t)
	onlyFirst := fsWithMigrations(map[string]string{"0001_init.sql": createFoo})
	if _, err := applyMigrations(context.Background(), d, onlyFirst); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// 0002 exists on disk but has not been applied yet → pending.
	entries, err := migrationStatus(context.Background(), d, fsWithMigrations(map[string]string{
		"0001_init.sql": createFoo,
		"0002_next.sql": createBar,
	}))
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %v", entries)
	}
	if entries[0].State != MigrationApplied || entries[1].State != MigrationPending {
		t.Fatalf("states = %s, %s", entries[0].State, entries[1].State)
	}

	// Apply 0002 as well, then drop 0001's file: 0001 → missing.
	both := fsWithMigrations(map[string]string{
		"0001_init.sql": createFoo,
		"0002_next.sql": createBar,
	})
	if _, err := applyMigrations(context.Background(), d, both); err != nil {
		t.Fatalf("apply both: %v", err)
	}
	missingFS := fsWithMigrations(map[string]string{"0002_next.sql": createBar})
	entries, err = migrationStatus(context.Background(), d, missingFS)
	if err != nil {
		t.Fatalf("status with missing: %v", err)
	}
	states := map[string]string{}
	for _, entry := range entries {
		states[entry.Name] = entry.State
	}
	if states["0001_init.sql"] != MigrationMissing {
		t.Fatalf("0001 should be missing, got %v", states)
	}
	if states["0002_next.sql"] != MigrationApplied {
		t.Fatalf("0002 should be applied, got %v", states)
	}
}

func TestEmbeddedMigrationsApply(t *testing.T) {
	d := testDB(t)
	applied, err := ApplyMigrations(context.Background(), d)
	if err != nil {
		t.Fatalf("apply embedded: %v", err)
	}
	if len(applied) != 1 || applied[0] != "0001_init.sql" {
		t.Fatalf("applied = %v", applied)
	}
	// The real schema must create the expected tables.
	for _, table := range []string{"channels", "videos", "ad_breaks"} {
		var count int
		err := d.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count)
		if err != nil || count != 1 {
			t.Fatalf("table %s missing (count=%d err=%v)", table, count, err)
		}
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	utc := timeMustParse(t, "2026-11-05T18:45:03Z")
	formatted := FormatTimestamp(utc)
	if formatted != "2026-11-05 18:45:03" {
		t.Fatalf("formatted = %q", formatted)
	}
	parsed, err := ParseTimestamp(formatted)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !parsed.Equal(utc) {
		t.Fatalf("round trip: %v != %v", parsed, utc)
	}
	if _, err := ParseTimestamp("2026-11-05T18:45:03Z"); err == nil {
		t.Fatal("expected RFC3339 input to be rejected")
	}
}
