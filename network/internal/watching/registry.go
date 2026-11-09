// Package watching implements smart on-demand channels for the tv-ui:
// a channel starts when someone is watching and keeps running for a
// keep-alive window after the last viewer leaves (per-viewer
// heartbeats).
package watching

import (
	"sync"
	"time"
)

// State is the watch state of one channel after a heartbeat (or
// pruning).
type State struct {
	Viewers        int
	KeepAliveUntil *time.Time
}

// Registry tracks the last heartbeat time of every viewer per channel.
type Registry struct {
	mu         sync.Mutex
	keepAlive  time.Duration
	perChannel map[string]map[string]time.Time
}

// NewRegistry creates a registry with the given keep-alive window.
func NewRegistry(keepAlive time.Duration) *Registry {
	return &Registry{keepAlive: keepAlive, perChannel: make(map[string]map[string]time.Time)}
}

// Heartbeat records a viewer's heartbeat and returns the channel's
// current watch state: viewers whose last heartbeat is older than the
// keep-alive window are pruned first; the keep-alive horizon is the
// newest heartbeat plus the window.
func (r *Registry) Heartbeat(channelID, viewerID string, at time.Time) State {
	r.mu.Lock()
	defer r.mu.Unlock()
	viewers := r.perChannel[channelID]
	if viewers == nil {
		viewers = make(map[string]time.Time)
		r.perChannel[channelID] = viewers
	}
	viewers[viewerID] = at
	return r.stateLocked(viewers, at)
}

// Prune drops stale viewers from all channels and returns the IDs of
// channels whose last viewer just left (stop candidates), together
// with the newest remaining heartbeat.
func (r *Registry) Prune(now time.Time) (emptied []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for channelID, viewers := range r.perChannel {
		before := len(viewers)
		r.stateLocked(viewers, now)
		if len(viewers) == 0 && before > 0 {
			delete(r.perChannel, channelID)
			emptied = append(emptied, channelID)
		}
	}
	return emptied
}

// Active returns the IDs of channels that currently have viewers.
func (r *Registry) Active() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	active := make([]string, 0, len(r.perChannel))
	for channelID, viewers := range r.perChannel {
		if len(viewers) > 0 {
			active = append(active, channelID)
		}
	}
	return active
}

func (r *Registry) stateLocked(viewers map[string]time.Time, at time.Time) State {
	var newest *time.Time
	for viewerID, lastSeen := range viewers {
		if at.Sub(lastSeen) > r.keepAlive {
			delete(viewers, viewerID)
			continue
		}
		if newest == nil || lastSeen.After(*newest) {
			t := lastSeen
			newest = &t
		}
	}
	if newest == nil {
		return State{Viewers: 0}
	}
	until := newest.Add(r.keepAlive)
	return State{Viewers: len(viewers), KeepAliveUntil: &until}
}
