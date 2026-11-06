package media

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

// Store is the SQLite repository for the media library (videos and
// ad_breaks).
type Store struct {
	db *sql.DB
}

// NewStore creates the media repository.
func NewStore(d *sql.DB) *Store {
	return &Store{db: d}
}

// Filter narrows List results; nil means no filter.
type Filter struct {
	Processed bool
}

// FindAll returns videos ordered by creation time, then path. A nil
// filter lists everything.
//
// The single-connection pool forbids queries while a result set is
// open, so the videos are loaded first and the ad breaks in a second
// pass.
func (s *Store) FindAll(ctx context.Context, filter *Filter) ([]Video, error) {
	query := "SELECT id, path, title, duration, tags, processed, created_at FROM videos"
	var args []any
	if filter != nil {
		query += " WHERE processed = ?"
		processed := 0
		if filter.Processed {
			processed = 1
		}
		args = append(args, processed)
	}
	query += " ORDER BY created_at, path"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		id, path, title, tagsJSON, createdAt string
		duration                             float64
		processed                            int
	}
	for rows.Next() {
		var r struct {
			id, path, title, tagsJSON, createdAt string
			duration                             float64
			processed                            int
		}
		if err := rows.Scan(&r.id, &r.path, &r.title, &r.duration, &r.tagsJSON, &r.processed, &r.createdAt); err != nil {
			rows.Close()
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	videos := make([]Video, 0, len(raw))
	for _, r := range raw {
		v, err := s.videoFromRow(ctx, r.id, r.path, r.title, r.duration, r.tagsJSON, r.processed, r.createdAt)
		if err != nil {
			return nil, err
		}
		videos = append(videos, v)
	}
	return videos, nil
}

// FindByID returns the video with the given ID, or nil when absent.
func (s *Store) FindByID(ctx context.Context, id string) (*Video, error) {
	return s.findBy(ctx, "id", id)
}

// FindByPath returns the video with the given path, or nil when absent.
func (s *Store) FindByPath(ctx context.Context, path string) (*Video, error) {
	return s.findBy(ctx, "path", path)
}

func (s *Store) findBy(ctx context.Context, column, value string) (*Video, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT id, path, title, duration, tags, processed, created_at FROM videos WHERE "+column+" = ?", value)
	var (
		id, path, title, tagsJSON, createdAt string
		duration                             float64
		processed                            int
	)
	if err := row.Scan(&id, &path, &title, &duration, &tagsJSON, &processed, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	v, err := s.videoFromRow(ctx, id, path, title, duration, tagsJSON, processed, createdAt)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// videoFromRow reconstitutes a video from scanned column values,
// fetching its ad breaks (only valid once the scanning result set is
// closed — the pool holds a single connection).
func (s *Store) videoFromRow(ctx context.Context, id, path, title string, duration float64, tagsJSON string, processed int, createdAt string) (Video, error) {
	var tags []string
	if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
		return Video{}, fmt.Errorf("videos.tags does not contain valid JSON: %s", tagsJSON)
	}
	breaks, err := s.breaksFor(ctx, id)
	if err != nil {
		return Video{}, err
	}
	created, err := db.ParseTimestamp(createdAt)
	if err != nil {
		return Video{}, err
	}
	return Reconstitute(id, path, title, duration, tags, processed == 1, breaks, created)
}

// Add inserts a new video. A duplicate path maps to a conflict error.
func (s *Store) Add(ctx context.Context, v Video) error {
	tagsJSON, err := json.Marshal(nonNil(v.Tags))
	if err != nil {
		return err
	}
	processed := 0
	if v.Processed {
		processed = 1
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO videos (id, path, title, duration, tags, processed, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
		v.ID, v.Path, v.Title, v.Duration, string(tagsJSON), processed, db.FormatTimestamp(v.CreatedAt))
	if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return apperr.Conflict(fmt.Sprintf("video path %q already exists", v.Path))
	}
	return err
}

// Save updates a video and replaces its ad-break positions atomically,
// so concurrent readers never see processed videos with half-written
// breaks.
func (s *Store) Save(ctx context.Context, v Video) error {
	tagsJSON, err := json.Marshal(nonNil(v.Tags))
	if err != nil {
		return err
	}
	processed := 0
	if v.Processed {
		processed = 1
	}
	return db.WithTransaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx,
			"UPDATE videos SET path = ?, title = ?, duration = ?, tags = ?, processed = ?, created_at = ? WHERE id = ?",
			v.Path, v.Title, v.Duration, string(tagsJSON), processed, db.FormatTimestamp(v.CreatedAt), v.ID)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return apperr.NotFound(fmt.Sprintf("Video %s not found", v.ID))
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM ad_breaks WHERE video_id = ?", v.ID); err != nil {
			return err
		}
		for _, br := range v.AdBreaks {
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO ad_breaks (video_id, position) VALUES (?, ?)", v.ID, br.Position); err != nil {
				return err
			}
		}
		return nil
	})
}

// Remove deletes a video (ad breaks cascade); it reports whether a row
// was deleted.
func (s *Store) Remove(ctx context.Context, id string) (bool, error) {
	result, err := s.db.ExecContext(ctx, "DELETE FROM videos WHERE id = ?", id)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *Store) breaksFor(ctx context.Context, id string) ([]AdBreak, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT position FROM ad_breaks WHERE video_id = ? ORDER BY position", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var breaks []AdBreak
	for rows.Next() {
		var position float64
		if err := rows.Scan(&position); err != nil {
			return nil, err
		}
		breaks = append(breaks, AdBreak{Position: position})
	}
	return breaks, rows.Err()
}

func nonNil(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}
