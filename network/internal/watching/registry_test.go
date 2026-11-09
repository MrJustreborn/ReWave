package watching

import (
	"testing"
	"time"
)

func TestRegistryHeartbeatAndPruning(t *testing.T) {
	registry := NewRegistry(5 * time.Minute)
	base := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

	// First viewer.
	state := registerHeartbeat(t, registry, "ch", "alice", base)
	if state.Viewers != 1 {
		t.Fatalf("viewers: %d", state.Viewers)
	}
	if state.KeepAliveUntil == nil || !state.KeepAliveUntil.Equal(base.Add(5*time.Minute)) {
		t.Fatalf("keep-alive: %v", state.KeepAliveUntil)
	}

	// Second viewer 2 minutes later.
	sameChannel, differentViewer := "ch", "bob"
	state = registry.Heartbeat(sameChannel, differentViewer, base.Add(2*time.Minute))
	if state.Viewers != 2 {
		t.Fatalf("viewers: %d", state.Viewers)
	}
	if !state.KeepAliveUntil.Equal(base.Add(2*time.Minute + 5*time.Minute)) {
		t.Fatalf("keep-alive follows the newest heartbeat: %v", state.KeepAliveUntil)
	}

	// A third channel's viewers are independent.
	state = registerHeartbeat(t, registry, "other", "carol", base)
	if state.Viewers != 1 {
		t.Fatalf("channels are independent: %d", state.Viewers)
	}

	// Pruning at base+6m: alice (last seen base, window base+5m) is stale,
	// but bob (window base+7m) still watches "ch" — only "other"
	// (carol, last seen base) is emptied.
	emptied := registry.Prune(base.Add(6 * time.Minute))
	if len(emptied) != 1 || emptied[0] != "other" {
		t.Fatalf("emptied at +6m: %v", emptied)
	}
	state = registerHeartbeat(t, registry, "ch", "dave", base.Add(6*time.Minute)) // dave revives
	_ = state
	// At +9m bob's window (base+7m) has also lapsed; dave's (base+11m)
	// has not — "ch" survives.
	emptied = registry.Prune(base.Add(9 * time.Minute))
	if len(emptied) != 0 {
		t.Fatalf("dave is still watching: %v", emptied)
	}
	// Past dave's window, the channel is emptied.
	emptied = registry.Prune(base.Add(6*time.Minute + 5*time.Minute + time.Second))
	if len(emptied) != 1 || emptied[0] != "ch" {
		t.Fatalf("emptied: %v", emptied)
	}
	if active := registry.Active(); len(active) != 0 {
		t.Fatalf("active after emptying: %v", active)
	}
}

func registerHeartbeat(t *testing.T, registry *Registry, channelID, viewerID string, at time.Time) State {
	t.Helper()
	return registry.Heartbeat(channelID, viewerID, at)
}
