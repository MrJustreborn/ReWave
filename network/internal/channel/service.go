package channel

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/uuid"
)

// PlaylistProvider resolves a channel's playlist into the payload pushed
// to the channel (implemented by the scheduling context; wired in the
// composition root).
type PlaylistProvider interface {
	PlaylistFor(ctx context.Context, ch Channel) (string, error)
}

// Service implements the channel use cases: registry CRUD and
// lifecycle control (start/stop through the container runtime).
type Service struct {
	store     *Store
	runtime   Runtime
	playlists PlaylistProvider
	now       func() time.Time
}

// NewService creates the channel service. The clock defaults to
// time.Now; tests inject a fixed one.
func NewService(store *Store, runtime Runtime, playlists PlaylistProvider, now ...func() time.Time) *Service {
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	return &Service{store: store, runtime: runtime, playlists: playlists, now: clock}
}

// List returns all channels ordered by name.
func (s *Service) List(ctx context.Context) ([]Channel, error) {
	return s.store.FindAll(ctx)
}

// Create registers a new channel (status "created", not running). A
// duplicate name is a conflict.
func (s *Service) Create(ctx context.Context, def Definition) (Channel, error) {
	if err := def.Validate(); err != nil {
		return Channel{}, err
	}
	existing, err := s.store.FindByName(ctx, def.Name)
	if err != nil {
		return Channel{}, err
	}
	if existing != nil {
		return Channel{}, apperr.Conflict(fmt.Sprintf("channel name %q already exists", def.Name))
	}
	ch := Channel{
		ID:         uuid.New(),
		CreatedAt:  s.now().UTC(),
		Status:     StatusCreated,
		Definition: def,
	}
	if err := s.store.Add(ctx, ch); err != nil {
		return Channel{}, err
	}
	return ch, nil
}

// Get returns one channel.
func (s *Service) Get(ctx context.Context, id string) (Channel, error) {
	ch, err := s.store.FindByID(ctx, id)
	if err != nil {
		return Channel{}, err
	}
	if ch == nil {
		return Channel{}, apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	return *ch, nil
}

// Replace swaps a channel's definition. Status changes take effect the
// next time the playlist is resolved; a running channel is not
// restarted.
func (s *Service) Replace(ctx context.Context, id string, def Definition) (Channel, error) {
	if err := def.Validate(); err != nil {
		return Channel{}, err
	}
	existing, err := s.store.FindByID(ctx, id)
	if err != nil {
		return Channel{}, err
	}
	if existing == nil {
		return Channel{}, apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	if namesake, err := s.store.FindByName(ctx, def.Name); err != nil {
		return Channel{}, err
	} else if namesake != nil && namesake.ID != id {
		return Channel{}, apperr.Conflict(fmt.Sprintf("channel name %q already exists", def.Name))
	}
	updated := existing.WithDefinition(def)
	if err := s.store.Save(ctx, updated); err != nil {
		return Channel{}, err
	}
	return updated, nil
}

// Delete removes a channel: its container is stopped first (if any),
// then the definition is deleted.
func (s *Service) Delete(ctx context.Context, id string) error {
	ch, err := s.store.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if ch == nil {
		return apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	if err := s.runtime.Stop(ctx, *ch); err != nil {
		return err
	}
	removed, err := s.store.Remove(ctx, id)
	if err != nil {
		return err
	}
	if !removed {
		return apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	return nil
}

// Start launches the channel's container with a freshly resolved
// playlist. Idempotent — starting a running channel is a no-op.
func (s *Service) Start(ctx context.Context, id string) (Channel, error) {
	ch, err := s.store.FindByID(ctx, id)
	if err != nil {
		return Channel{}, err
	}
	if ch == nil {
		return Channel{}, apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	if ch.Status == StatusRunning {
		return *ch, nil
	}
	playlist, err := s.playlists.PlaylistFor(ctx, *ch)
	if err != nil {
		return Channel{}, err
	}
	if err := s.runtime.EnsureRunning(ctx, *ch, playlist); err != nil {
		s.recordError(ctx, *ch)
		return Channel{}, err
	}
	running := ch.Started()
	if err := s.store.Save(ctx, running); err != nil {
		return Channel{}, err
	}
	return running, nil
}

// Stop removes the channel's container. Idempotent — the container is
// always stopped/removed (it may exist even when the recorded status
// says otherwise, e.g. after a failed start).
func (s *Service) Stop(ctx context.Context, id string) (Channel, error) {
	ch, err := s.store.FindByID(ctx, id)
	if err != nil {
		return Channel{}, err
	}
	if ch == nil {
		return Channel{}, apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	if err := s.runtime.Stop(ctx, *ch); err != nil {
		s.recordError(ctx, *ch)
		return Channel{}, err
	}
	stopped := ch.Stopped()
	if stopped.Status != ch.Status {
		if err := s.store.Save(ctx, stopped); err != nil {
			return Channel{}, err
		}
	}
	return stopped, nil
}

// recordError persists the error state when a lifecycle operation
// fails. Storage failures are logged, not returned — the caller already
// has the real error.
func (s *Service) recordError(ctx context.Context, ch Channel) {
	if ch.Status == StatusError {
		return
	}
	ch.Status = StatusError
	if err := s.store.Save(ctx, ch); err != nil {
		slog.Error("recording channel error state failed", "channel", ch.ID, "error", err.Error())
	}
}
