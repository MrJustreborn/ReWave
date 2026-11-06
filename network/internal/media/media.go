// Package media is the media library context: the video pool (SQLite,
// owned and written only by the network), ad-break ingestion and the
// ffmpeg-based ad-break detection ported from the former TypeScript
// prototype.
package media

import (
	"fmt"
	"time"

	"rewave/network/internal/apperr"
)

// AdBreak is a detected ad-break position within a video.
type AdBreak struct {
	Position float64 `json:"position"`
}

// Video is a video in the media library.
type Video struct {
	ID        string
	CreatedAt time.Time
	Path      string
	Title     string
	Duration  float64
	Tags      []string
	Processed bool
	AdBreaks  []AdBreak
}

// NewVideo validates creation input (duration 0, unprocessed, no
// breaks).
func NewVideo(id, path, title string, duration float64, tags []string, createdAt time.Time) (Video, error) {
	v := Video{ID: id, Path: path, Title: title, Duration: duration, Tags: tags, CreatedAt: createdAt}
	if err := v.validate(); err != nil {
		return Video{}, err
	}
	return v, nil
}

// WithProcessedAdBreaks marks the video processed with the detected
// break positions. Already-processed videos cannot be re-processed.
func (v Video) WithProcessedAdBreaks(positions []float64) (Video, error) {
	if v.Processed {
		return Video{}, apperr.Validation(fmt.Sprintf("video %q is already processed", v.Path))
	}
	breaks := make([]AdBreak, len(positions))
	for i, position := range positions {
		if position < 0 {
			return Video{}, apperr.Validation("adBreak.position must be a finite number >= 0")
		}
		breaks[i] = AdBreak{Position: position}
	}
	v.AdBreaks = breaks
	v.Processed = true
	return v, nil
}

// Reconstitute rebuilds a video from persisted state.
func Reconstitute(id, path, title string, duration float64, tags []string, processed bool, breaks []AdBreak, createdAt time.Time) (Video, error) {
	v := Video{
		ID: id, Path: path, Title: title, Duration: duration, Tags: tags,
		Processed: processed, AdBreaks: breaks, CreatedAt: createdAt,
	}
	if err := v.validate(); err != nil {
		return Video{}, err
	}
	for _, br := range breaks {
		if br.Position < 0 {
			return Video{}, apperr.Validation("adBreak.position must be a finite number >= 0")
		}
	}
	return v, nil
}

func (v Video) validate() error {
	if v.Path == "" {
		return apperr.Validation("video.path must be a non-empty string")
	}
	if v.Title == "" {
		return apperr.Validation("video.title must be a non-empty string")
	}
	if v.Duration < 0 {
		return apperr.Validation("video.duration must be a finite number >= 0")
	}
	for _, tag := range v.Tags {
		if tag == "" {
			return apperr.Validation("video.tags entries must be non-empty strings")
		}
	}
	return nil
}
