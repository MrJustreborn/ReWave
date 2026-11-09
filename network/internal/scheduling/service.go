package scheduling

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
	"rewave/network/internal/media"
)

// ChannelSource supplies channels to the scheduling context.
type ChannelSource interface {
	FindByID(ctx context.Context, id string) (*channel.Channel, error)
}

// VideoSource supplies the media pool to the scheduling context.
type VideoSource interface {
	FindAll(ctx context.Context, filter *media.Filter) ([]media.Video, error)
}

// Service resolves channel schedules into playlists and keeps the
// latest resolved playlist of each channel (in memory — a playlist is
// the exact payload pushed to a channel and becomes stale as soon as
// the channel is restarted with a fresh resolution).
type Service struct {
	channels ChannelSource
	videos   VideoSource
	store    *playlistStore
	now      func() time.Time
}

// NewService creates the scheduling service. The clock defaults to
// time.Now; tests inject a fixed one.
func NewService(channels ChannelSource, videos VideoSource, now ...func() time.Time) *Service {
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	return &Service{channels: channels, videos: videos, store: newPlaylistStore(), now: clock}
}

// Resolve resolves the channel's schedule into a playlist, stores it as
// the channel's current playlist and returns it.
func (s *Service) Resolve(ctx context.Context, channelID string) (Playlist, error) {
	ch, err := s.lookup(ctx, channelID)
	if err != nil {
		return Playlist{}, err
	}
	videos, err := s.videos.FindAll(ctx, nil)
	if err != nil {
		return Playlist{}, err
	}
	playlist := Resolve(Inputs{Now: s.now(), Channel: *ch, Videos: videos})
	if err := playlist.Validate(); err != nil {
		return Playlist{}, err
	}
	s.store.set(channelID, playlist)
	return playlist, nil
}

// Stored returns the channel's currently resolved playlist (empty
// slots when nothing has been resolved yet).
func (s *Service) Stored(ctx context.Context, channelID string) (Playlist, error) {
	if _, err := s.lookup(ctx, channelID); err != nil {
		return Playlist{}, err
	}
	return s.store.get(channelID), nil
}

// PlaylistFor resolves a channel's playlist and returns it in the flat
// JSON format the current channel prototype consumes via its PLAYLIST
// environment variable.
//
// TEMPORARY SHIM: the playlist schema (slots with timestamp/file/type)
// matches what today's channel Python prototype expects, not the
// network's own resolved-playlist contract — the channel HTTP API
// (roadmap) will replace this.
func (s *Service) PlaylistFor(ctx context.Context, ch channel.Channel) (string, error) {
	playlist, err := s.Resolve(ctx, ch.ID)
	if err != nil {
		return "", err
	}
	return channelPlaylistJSON(playlist)
}

func (s *Service) lookup(ctx context.Context, channelID string) (*channel.Channel, error) {
	ch, err := s.channels.FindByID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, apperr.NotFound(fmt.Sprintf("Channel %s not found", channelID))
	}
	return ch, nil
}

// channelPlaylistJSON serializes a resolved playlist into the flat
// per-entry slots the channel prototype reads (sorted by timestamp).
func channelPlaylistJSON(playlist Playlist) (string, error) {
	type shimSlot struct {
		Timestamp string `json:"timestamp"`
		File      string `json:"file"`
		Type      string `json:"type"`
		Start     string `json:"start"`
		End       string `json:"end"`
	}
	slots := make([]shimSlot, 0, 16)
	for _, slot := range playlist.Slots {
		cursor := slot.Start
		for _, entry := range slot.Stream {
			end := cursor.Add(time.Duration(entry.Duration * float64(time.Second)))
			slots = append(slots, shimSlot{
				Timestamp: formatRFC3339(cursor),
				File:      entry.File,
				Type:      string(entry.Type),
				Start:     formatRFC3339(slot.Start),
				End:       formatRFC3339(end),
			})
			cursor = end
		}
	}
	raw, err := json.Marshal(map[string]any{"slots": slots})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func formatRFC3339(t time.Time) string {
	return t.Format(time.RFC3339)
}

// playlistStore keeps the latest resolved playlist per channel.
type playlistStore struct {
	mu        sync.RWMutex
	byChannel map[string]Playlist
}

func newPlaylistStore() *playlistStore {
	return &playlistStore{byChannel: make(map[string]Playlist)}
}

func (s *playlistStore) set(channelID string, playlist Playlist) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byChannel[channelID] = playlist
}

func (s *playlistStore) get(channelID string) Playlist {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byChannel[channelID]
}
