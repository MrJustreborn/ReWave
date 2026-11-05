package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"rewave/network/internal/apperr"
	"rewave/network/internal/db"
)

// Store is the SQLite repository for channels. JSON columns
// (content_pool, schedule) are encoded here and nowhere else.
type Store struct {
	db *sql.DB
}

// NewStore creates the channel repository.
func NewStore(d *sql.DB) *Store {
	return &Store{db: d}
}

const channelColumns = "id, name, type, content_pool, schedule, status, created_at"

// FindAll returns all channels ordered by name.
func (s *Store) FindAll(ctx context.Context) ([]Channel, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+channelColumns+" FROM channels ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var channels []Channel
	for rows.Next() {
		ch, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		channels = append(channels, ch)
	}
	return channels, rows.Err()
}

// FindByID returns the channel with the given ID, or nil when absent.
func (s *Store) FindByID(ctx context.Context, id string) (*Channel, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+channelColumns+" FROM channels WHERE id = ?", id)
	return scanChannelRow(row)
}

// FindByName returns the channel with the given name, or nil when
// absent.
func (s *Store) FindByName(ctx context.Context, name string) (*Channel, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+channelColumns+" FROM channels WHERE name = ?", name)
	return scanChannelRow(row)
}

// Add inserts a new channel. A duplicate name maps to a conflict error.
func (s *Store) Add(ctx context.Context, ch Channel) error {
	poolJSON, err := json.Marshal(ch.Definition.ContentPool)
	if err != nil {
		return err
	}
	scheduleJSON, err := json.Marshal(ch.Definition.Schedule)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO channels (id, name, type, content_pool, schedule, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		ch.ID, ch.Definition.Name, ch.Definition.Type, string(poolJSON), string(scheduleJSON),
		string(ch.Status), db.FormatTimestamp(ch.CreatedAt))
	if isUniqueViolation(err) {
		return apperr.Conflict(fmt.Sprintf("channel name %q already exists", ch.Definition.Name))
	}
	return err
}

// Save updates a channel's definition and status.
func (s *Store) Save(ctx context.Context, ch Channel) error {
	poolJSON, err := json.Marshal(ch.Definition.ContentPool)
	if err != nil {
		return err
	}
	scheduleJSON, err := json.Marshal(ch.Definition.Schedule)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		"UPDATE channels SET name = ?, type = ?, content_pool = ?, schedule = ?, status = ? WHERE id = ?",
		ch.Definition.Name, ch.Definition.Type, string(poolJSON), string(scheduleJSON),
		string(ch.Status), ch.ID)
	if err != nil {
		if isUniqueViolation(err) {
			return apperr.Conflict(fmt.Sprintf("channel name %q already exists", ch.Definition.Name))
		}
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return apperr.NotFound(fmt.Sprintf("Channel %s not found", ch.ID))
	}
	return nil
}

// Remove deletes the channel; it reports whether a row was deleted.
func (s *Store) Remove(ctx context.Context, id string) (bool, error) {
	result, err := s.db.ExecContext(ctx, "DELETE FROM channels WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanChannelRow(row rowScanner) (*Channel, error) {
	var (
		id, name, typ, status  string
		poolJSON, scheduleJSON string
		createdAt              string
	)
	if err := row.Scan(&id, &name, &typ, &poolJSON, &scheduleJSON, &status, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	pool, err := parsePoolJSON(poolJSON)
	if err != nil {
		return nil, err
	}
	schedule, err := parseScheduleJSON(scheduleJSON)
	if err != nil {
		return nil, err
	}
	created, err := db.ParseTimestamp(createdAt)
	if err != nil {
		return nil, err
	}
	ch, err := Reconstitute(id, Definition{Name: name, Type: typ, ContentPool: pool, Schedule: schedule}, Status(status), created)
	if err != nil {
		return nil, err
	}
	return &ch, nil
}

func scanChannel(rows *sql.Rows) (Channel, error) {
	ch, err := scanChannelRow(rows)
	if err != nil {
		return Channel{}, err
	}
	return *ch, nil
}

func parsePoolJSON(raw string) (ContentPool, error) {
	var pool ContentPool
	if err := json.Unmarshal([]byte(raw), &pool); err != nil {
		return ContentPool{}, fmt.Errorf("channels.content_pool does not contain valid JSON: %s", raw)
	}
	return pool, nil
}

func parseScheduleJSON(raw string) (Schedule, error) {
	var schedule Schedule
	if err := json.Unmarshal([]byte(raw), &schedule); err != nil {
		return Schedule{}, fmt.Errorf("channels.schedule does not contain valid JSON: %s", raw)
	}
	return schedule, nil
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
