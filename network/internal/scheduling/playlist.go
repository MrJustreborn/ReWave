// Package scheduling resolves channel schedules into concrete playlists:
// per-day templates plus a content pool become dated slots filled with
// stream entries. The resolved playlist is exactly what the network
// pushes to a channel.
package scheduling

import (
	"fmt"
	"regexp"
	"time"

	"rewave/network/internal/apperr"
)

// EntryType is the role of a stream entry in the slot.
type EntryType string

const (
	EntryPreroll EntryType = "preroll"
	EntryContent EntryType = "content"
	EntryAd      EntryType = "ad"
)

// Entry is one playback item within a resolved slot. StartOffset and
// EndOffset are seek/stop offsets (HH:MM:SS) into the source file, used
// when a video is trimmed to fit the slot.
type Entry struct {
	File        string
	Type        EntryType
	Duration    float64
	StartOffset string
	EndOffset   string
}

// Slot is a scheduled slot resolved to concrete times and content.
// tags carries the source slot's content-matching tags through
// resolution; it is not part of the resolved result.
type Slot struct {
	Start  time.Time
	End    time.Time
	Stream []Entry

	tags []string
}

// Playlist is a resolved playlist.
type Playlist struct {
	Slots []Slot
}

var timecodePattern = regexp.MustCompile(`^[0-9]{2}:[0-5][0-9]:[0-5][0-9]$`)

// Validate checks the playlist invariants: non-empty streams, ordered
// times, well-formed entries and offsets.
func (p Playlist) Validate() error {
	for _, slot := range p.Slots {
		if len(slot.Stream) == 0 {
			return apperr.Validation("resolved slot must contain at least one stream entry")
		}
		if !slot.End.After(slot.Start) {
			return apperr.Validation("resolved slot end must be after its start")
		}
		for _, entry := range slot.Stream {
			if entry.File == "" {
				return apperr.Validation("stream entry file must be a non-empty string")
			}
			switch entry.Type {
			case EntryPreroll, EntryContent, EntryAd:
			default:
				return apperr.Validation("stream entry type must be one of preroll, content, ad")
			}
			if entry.Duration < 0 {
				return apperr.Validation("stream entry duration must be a finite number >= 0")
			}
			if entry.StartOffset != "" && !timecodePattern.MatchString(entry.StartOffset) {
				return apperr.Validation("stream entry start must be an HH:MM:SS timecode")
			}
			if entry.EndOffset != "" && !timecodePattern.MatchString(entry.EndOffset) {
				return apperr.Validation("stream entry end must be an HH:MM:SS timecode")
			}
		}
	}
	return nil
}

// Timecode formats seconds as an HH:MM:SS offset.
func Timecode(seconds float64) string {
	total := int(seconds)
	return fmt.Sprintf("%02d:%02d:%02d", total/3600, (total/60)%60, total%60)
}
