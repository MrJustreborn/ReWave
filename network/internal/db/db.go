// Package db owns the SQLite connection, the timestamp convention and the
// forward-only migration runner. The media library is single-writer and
// owned exclusively by the network.
package db

import (
	"database/sql"
	"fmt"

	// Registers the "sqlite" driver (pure Go, no cgo).
	_ "modernc.org/sqlite"
)

// Open opens (creating if necessary) the SQLite database at path with the
// pragmas the network relies on: WAL journaling and enforced foreign keys.
// SQLite is single-writer, so the connection pool is capped at one.
func Open(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	d.SetMaxOpenConns(1)
	if err := d.Ping(); err != nil {
		d.Close()
		return nil, fmt.Errorf("open sqlite database %s: %w", path, err)
	}
	return d, nil
}
