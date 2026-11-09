package watching

import (
	"context"
	"log/slog"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
)

// Service implements the on-demand watch use case: heartbeats keep a
// channel running; when the last viewer's keep-alive window lapses, the
// channel is stopped.
type Service struct {
	channels  *channel.Service
	registry  *Registry
	keepAlive time.Duration
	now       func() time.Time
}

// NewService creates the watching service.
func NewService(channels *channel.Service, keepAlive time.Duration, now ...func() time.Time) *Service {
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	return &Service{
		channels:  channels,
		registry:  NewRegistry(keepAlive),
		keepAlive: keepAlive,
		now:       clock,
	}
}

// Result is the state of an on-demand channel after a heartbeat.
type Result struct {
	Viewers        int
	Status         channel.Status
	KeepAliveUntil *time.Time
}

// Watch records a viewer heartbeat. An on-demand channel starts on its
// first heartbeat; the response reports the current viewer count and
// channel state.
func (s *Service) Watch(ctx context.Context, channelID, viewerID string) (Result, error) {
	ch, err := s.channels.Get(ctx, channelID)
	if err != nil {
		return Result{}, err // 404 for unknown channels, storage errors otherwise
	}
	state := s.registry.Heartbeat(channelID, viewerID, s.now())
	if ch.Status != channel.StatusRunning {
		started, err := s.channels.Start(ctx, channelID)
		if err != nil {
			return Result{}, err
		}
		ch = started
	}
	return Result{
		Viewers:        state.Viewers,
		Status:         ch.Status,
		KeepAliveUntil: state.KeepAliveUntil,
	}, nil
}

// Run drives the keep-alive expiry: it periodically prunes stale
// viewers and stops channels whose last viewer left. Run blocks until
// ctx is cancelled.
func (s *Service) Run(ctx context.Context) {
	interval := s.keepAlive / 4
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.ReapOnce(ctx, now)
		}
	}
}

// ReapOnce prunes stale viewers once and stops emptied, still-running
// channels. Also exposed for tests.
func (s *Service) ReapOnce(ctx context.Context, now time.Time) {
	for _, channelID := range s.registry.Prune(now) {
		ch, err := s.channels.Get(ctx, channelID)
		if err != nil {
			if apperrIsNotFound(err) {
				continue // channel deleted while being watched
			}
			slog.Error("keep-alive expiry: fetching channel failed", "channel", channelID, "error", err.Error())
			continue
		}
		if ch.Status != channel.StatusRunning {
			continue
		}
		if _, err := s.channels.Stop(ctx, channelID); err != nil {
			slog.Error("keep-alive expiry: stopping channel failed", "channel", channelID, "error", err.Error())
			continue
		}
		slog.Info("keep-alive expired: channel stopped", "channel", channelID)
	}
}

func apperrIsNotFound(err error) bool {
	return apperr.HTTPStatus(err) == 404
}
