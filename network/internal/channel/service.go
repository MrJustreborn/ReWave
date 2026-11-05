package channel

import (
	"context"
	"fmt"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/uuid"
)

// Service implements the channel use cases: registry CRUD. Lifecycle
// control (start/stop via the Docker runtime) is added on top of this.
type Service struct {
	store *Store
	now   func() time.Time
}

// NewService creates the channel service. The clock defaults to
// time.Now; tests inject a fixed one.
func NewService(store *Store, now ...func() time.Time) *Service {
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	return &Service{store: store, now: clock}
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

// Delete removes a channel definition (a running channel is stopped
// first once the runtime is wired in).
func (s *Service) Delete(ctx context.Context, id string) error {
	removed, err := s.store.Remove(ctx, id)
	if err != nil {
		return err
	}
	if !removed {
		return apperr.NotFound(fmt.Sprintf("Channel %s not found", id))
	}
	return nil
}
