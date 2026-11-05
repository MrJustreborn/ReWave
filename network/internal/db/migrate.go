package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationsDir = "migrations"

var migrationNamePattern = regexp.MustCompile(`^\d{4}_[a-z0-9_]+\.sql$`)

// Migration state values reported by MigrationStatus.
const (
	MigrationApplied = "applied"
	MigrationPending = "pending"
	MigrationMissing = "missing"
)

// MigrationStatusEntry describes one migration file (or, for "missing",
// one applied history row whose file no longer exists).
type MigrationStatusEntry struct {
	Name      string
	State     string
	AppliedAt string // SQLite timestamp, empty when pending
}

// ApplyMigrations validates the migration history and applies pending
// migrations in order, returning the names of the newly applied files.
//
// Migrations are forward-only: deleting, renaming or editing an already
// applied file is rejected, and so is adding a file that sorts before an
// already applied one. Never edit an applied migration — add a new
// numbered NNNN_description.sql instead.
func ApplyMigrations(ctx context.Context, d *sql.DB) ([]string, error) {
	return applyMigrations(ctx, d, migrationsFS)
}

// MigrationStatus lists all migration files with their state, plus
// "missing" entries for applied migrations whose files no longer exist.
func MigrationStatus(ctx context.Context, d *sql.DB) ([]MigrationStatusEntry, error) {
	return migrationStatus(ctx, d, migrationsFS)
}

func applyMigrations(ctx context.Context, d *sql.DB, fsys fs.FS) ([]string, error) {
	files, err := listMigrationFiles(fsys)
	if err != nil {
		return nil, err
	}
	history, err := loadHistory(ctx, d, fsys, files)
	if err != nil {
		return nil, err
	}

	applied := make([]string, 0, len(files))
	for _, file := range files {
		if _, ok := history[file]; ok {
			continue
		}
		content, err := fs.ReadFile(fsys, migrationsDir+"/"+file)
		if err != nil {
			return nil, err
		}
		err = withTransaction(ctx, d, func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, string(content)); err != nil {
				return fmt.Errorf("migration %s failed: %w", file, err)
			}
			_, err := tx.ExecContext(ctx,
				"INSERT INTO schema_migrations (name, checksum) VALUES (?, ?)",
				file, checksumOf(content))
			return err
		})
		if err != nil {
			return nil, err
		}
		applied = append(applied, file)
	}
	return applied, nil
}

func migrationStatus(ctx context.Context, d *sql.DB, fsys fs.FS) ([]MigrationStatusEntry, error) {
	files, err := listMigrationFiles(fsys)
	if err != nil {
		return nil, err
	}
	if err := ensureHistoryTable(ctx, d); err != nil {
		return nil, err
	}
	rows, err := d.QueryContext(ctx, "SELECT name, applied_at FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	appliedAtByName := make(map[string]string)
	for rows.Next() {
		var name, appliedAt string
		if err := rows.Scan(&name, &appliedAt); err != nil {
			return nil, err
		}
		appliedAtByName[name] = appliedAt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	fileSet := make(map[string]bool, len(files))
	for _, file := range files {
		fileSet[file] = true
	}
	entries := make([]MigrationStatusEntry, 0, len(files))
	for _, file := range files {
		appliedAt, ok := appliedAtByName[file]
		if !ok {
			entries = append(entries, MigrationStatusEntry{Name: file, State: MigrationPending})
			continue
		}
		entries = append(entries, MigrationStatusEntry{Name: file, State: MigrationApplied, AppliedAt: appliedAt})
	}
	for name, appliedAt := range appliedAtByName {
		if !fileSet[name] {
			entries = append(entries, MigrationStatusEntry{Name: name, State: MigrationMissing, AppliedAt: appliedAt})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

// loadHistory validates the applied history against the migration files
// and returns the set of applied file names.
func loadHistory(ctx context.Context, d *sql.DB, fsys fs.FS, files []string) (map[string]string, error) {
	if err := ensureHistoryTable(ctx, d); err != nil {
		return nil, err
	}
	rows, err := d.QueryContext(ctx, "SELECT name, checksum FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type historyRow struct {
		name     string
		checksum string
	}
	var historyRows []historyRow
	for rows.Next() {
		var row historyRow
		if err := rows.Scan(&row.name, &row.checksum); err != nil {
			return nil, err
		}
		historyRows = append(historyRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	fileSet := make(map[string]bool, len(files))
	for _, file := range files {
		fileSet[file] = true
	}
	history := make(map[string]string, len(historyRows))
	lastApplied := ""
	for _, row := range historyRows {
		if !fileSet[row.name] {
			return nil, fmt.Errorf(
				"applied migration %q no longer exists — restore it instead of deleting or renaming", row.name)
		}
		history[row.name] = row.checksum
		if row.name > lastApplied {
			lastApplied = row.name
		}
	}

	for _, file := range files {
		checksum, ok := history[file]
		if !ok {
			if file < lastApplied {
				return nil, fmt.Errorf(
					"out-of-order migration %q — it sorts before already-applied %q; apply migrations in order",
					file, lastApplied)
			}
			continue
		}
		content, err := fs.ReadFile(fsys, migrationsDir+"/"+file)
		if err != nil {
			return nil, err
		}
		if checksum != "" && checksum != checksumOf(content) {
			return nil, fmt.Errorf(
				"migration %q changed since it was applied (checksum mismatch) — restore the original or add a new migration",
				file)
		}
	}
	return history, nil
}

func ensureHistoryTable(ctx context.Context, d *sql.DB) error {
	_, err := d.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		checksum TEXT NOT NULL,
		applied_at TEXT NOT NULL DEFAULT (datetime('now'))
	)`)
	return err
}

func listMigrationFiles(fsys fs.FS) ([]string, error) {
	entries, err := fs.ReadDir(fsys, migrationsDir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, entry.Name())
	}
	sort.Strings(files)
	for _, file := range files {
		if !migrationNamePattern.MatchString(file) {
			return nil, fmt.Errorf("migration file %q violates the naming convention NNNN_description.sql", file)
		}
	}
	return files, nil
}

func checksumOf(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func withTransaction(ctx context.Context, d *sql.DB, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}
