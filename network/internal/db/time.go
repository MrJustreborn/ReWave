package db

import (
	"fmt"
	"time"
)

// Timestamps are stored as SQLite UTC strings in the layout
// "YYYY-MM-DD HH:MM:SS" (what SQLite's datetime('now') produces).

const timestampLayout = "2006-01-02 15:04:05"

// FormatTimestamp renders t as a SQLite UTC timestamp.
func FormatTimestamp(t time.Time) string {
	return t.UTC().Format(timestampLayout)
}

// ParseTimestamp parses a SQLite UTC timestamp. The result is UTC.
func ParseTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(timestampLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("not a SQLite UTC timestamp: %q", value)
	}
	return parsed.UTC(), nil
}
