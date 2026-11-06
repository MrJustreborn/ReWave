package media

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/uuid"
)

// AddVideo is the command for adding a video to the pool. Title
// defaults to the file name when empty.
type AddVideo struct {
	Path  string
	Title string
	Tags  []string
}

// minDetectDuration is the default minimal video length for ad-break
// detection (shorter videos get no breaks).
const minDetectDuration = 3600.0

// Service implements the media library use cases: listing, adding (with
// ingestion), fetching and removing videos.
type Service struct {
	store    *Store
	detector Detector
	now      func() time.Time
}

// NewService creates the media service. The clock defaults to time.Now;
// tests inject a fixed one.
func NewService(store *Store, detector Detector, now ...func() time.Time) *Service {
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	return &Service{store: store, detector: detector, now: clock}
}

// List returns the media library, optionally filtered by ingestion
// state.
func (s *Service) List(ctx context.Context, filter *Filter) ([]Video, error) {
	return s.store.FindAll(ctx, filter)
}

// Get returns one video.
func (s *Service) Get(ctx context.Context, id string) (Video, error) {
	v, err := s.store.FindByID(ctx, id)
	if err != nil {
		return Video{}, err
	}
	if v == nil {
		return Video{}, apperr.NotFound(fmt.Sprintf("Video %s not found", id))
	}
	return *v, nil
}

// Add registers a video, probes its duration synchronously and triggers
// ad-break detection asynchronously: the returned video has processed
// set to false; poll the video detail to watch the ad-break points fill
// in.
func (s *Service) Add(ctx context.Context, cmd AddVideo) (Video, error) {
	if cmd.Path == "" {
		return Video{}, apperr.Validation("path must be a non-empty string")
	}
	if cmd.Title == "" {
		cmd.Title = defaultTitle(cmd.Path)
	}
	existing, err := s.store.FindByPath(ctx, cmd.Path)
	if err != nil {
		return Video{}, err
	}
	if existing != nil {
		return Video{}, apperr.Conflict(fmt.Sprintf("video path %q already exists", cmd.Path))
	}

	duration, err := s.detector.ProbeDuration(ctx, cmd.Path)
	if err != nil {
		return Video{}, apperr.Validation(fmt.Sprintf("cannot probe %s: %v", cmd.Path, err))
	}

	v, err := NewVideo(uuid.New(), cmd.Path, cmd.Title, duration, cmd.Tags, s.now().UTC())
	if err != nil {
		return Video{}, err
	}
	if err := s.store.Add(ctx, v); err != nil {
		return Video{}, err
	}

	go s.ingest(v)
	return v, nil
}

// ingest runs ad-break detection in the background and records the
// result. Failures are logged and leave the video unprocessed.
func (s *Service) ingest(v Video) {
	ctx := context.Background()
	breaks, err := s.detector.DetectAdBreaks(ctx, v.Path, minDetectDuration)
	if err != nil {
		slog.Error("ad-break detection failed", "path", v.Path, "error", err.Error())
		return
	}
	processed, err := v.WithProcessedAdBreaks(breaks)
	if err != nil {
		slog.Error("recording ad breaks failed", "path", v.Path, "error", err.Error())
		return
	}
	// The video may have been deleted meanwhile — that is fine, the
	// update then affects zero rows and is dropped.
	if err := s.store.Save(ctx, processed); err != nil {
		if apperr.HTTPStatus(err) != 404 {
			slog.Error("storing ad breaks failed", "path", v.Path, "error", err.Error())
		}
		return
	}
	slog.Info("video ingested", "path", v.Path, "ad_breaks", len(breaks))
}

// Delete removes a video and its ad-break points. Playlists already
// resolved are not rewritten.
func (s *Service) Delete(ctx context.Context, id string) error {
	removed, err := s.store.Remove(ctx, id)
	if err != nil {
		return err
	}
	if !removed {
		return apperr.NotFound(fmt.Sprintf("Video %s not found", id))
	}
	return nil
}

// defaultTitle derives a display title from the file name.
func defaultTitle(path string) string {
	base := filepath.Base(path)
	if ext := filepath.Ext(base); ext != "" {
		base = base[:len(base)-len(ext)]
	}
	if base == "" {
		return path
	}
	return base
}
