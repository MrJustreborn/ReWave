package watching

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"rewave/network/internal/channel"
	"rewave/network/internal/db"
)

// Channel-side fakes for the watching tests (the channel package's own
// fakes live in its test files and are not importable).
type recordingRuntime struct {
	starts   int
	stops    int
	startErr error
}

func (r *recordingRuntime) EnsureRunning(context.Context, channel.Channel, string) error {
	if r.startErr != nil {
		return r.startErr
	}
	r.starts++
	return nil
}

func (r *recordingRuntime) Stop(context.Context, channel.Channel) error {
	r.stops++
	return nil
}

type constantPlaylist struct{}

func (constantPlaylist) PlaylistFor(context.Context, channel.Channel) (string, error) {
	return `{"slots":[]}`, nil
}

func watchingFixture(t *testing.T, keepAlive time.Duration, now func() time.Time) (*Service, *channel.Service, *recordingRuntime) {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := db.ApplyMigrations(context.Background(), d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	runtime := &recordingRuntime{}
	channelService := channel.NewService(channel.NewStore(d), runtime, constantPlaylist{}, now)
	watchingService := NewService(channelService, keepAlive, now)
	return watchingService, channelService, runtime
}

func recorder() *recordingRuntime { return &recordingRuntime{} }

func createChannel(t *testing.T, svc *channel.Service) channel.Channel {
	t.Helper()
	def, err := channel.NewDefinition("movies-1", "movies", channel.ContentPool{}, channel.Schedule{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := svc.Create(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func TestWatchStartsChannelOnFirstHeartbeat(t *testing.T) {
	now := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	watchingService, channelService, runtime := watchingFixture(t, 5*time.Minute, clock)
	ctx := context.Background()
	ch := createChannel(t, channelService)

	result, err := watchingService.Watch(ctx, ch.ID, "alice")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if result.Viewers != 1 || result.Status != channel.StatusRunning || result.KeepAliveUntil == nil {
		t.Fatalf("result: %+v", result)
	}
	if runtime.starts != 1 {
		t.Fatalf("runtime starts: %d", runtime.starts)
	}

	// Second viewer — the channel is running, no new start.
	result, err = watchingService.Watch(ctx, ch.ID, "bob")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if result.Viewers != 2 || runtime.starts != 1 {
		t.Fatalf("result: %+v, starts: %d", result, runtime.starts)
	}

	// Heartbeats from the same viewer don't stack up.
	result, err = watchingService.Watch(ctx, ch.ID, "alice")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if result.Viewers != 2 {
		t.Fatalf("duplicate viewer counted: %+v", result)
	}

	// Unknown channel → 404.
	if _, err := watchingService.Watch(ctx, "00000000-0000-4000-8000-000000000000", "alice"); err == nil {
		t.Fatal("expected 404")
	}
}

func TestKeepAliveExpiryStopsChannel(t *testing.T) {
	current := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return current }
	watchingService, channelService, runtime := watchingFixture(t, 5*time.Minute, clock)
	ctx := context.Background()
	ch := createChannel(t, channelService)

	if _, err := watchingService.Watch(ctx, ch.ID, "alice"); err != nil {
		t.Fatal(err)
	}
	if runtime.starts != 1 {
		t.Fatalf("starts: %d", runtime.starts)
	}

	// Too early — the keep-alive window is still open.
	current = current.Add(4 * time.Minute)
	watchingService.ReapOnce(ctx, current)
	if runtime.stops != 0 {
		t.Fatal("channel stopped too early")
	}

	// After the window lapses, the reaper stops the channel.
	current = current.Add(2 * time.Minute)
	watchingService.ReapOnce(ctx, current)
	if runtime.stops != 1 {
		t.Fatalf("stops: %d", runtime.stops)
	}
	stopped, err := channelService.Get(ctx, ch.ID)
	if err != nil || stopped.Status != channel.StatusStopped {
		t.Fatalf("status after expiry: %s (%v)", stopped.Status, err)
	}
}

func TestWatchStartFailurePropagates(t *testing.T) {
	now := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	watchingService, channelService, runtime := watchingFixture(t, 5*time.Minute, clock)
	ch := createChannel(t, channelService)

	runtime.startErr = errBoom
	if _, err := watchingService.Watch(context.Background(), ch.ID, "alice"); err == nil {
		t.Fatal("watch must fail when the channel cannot start")
	}
}

type constErr string

func (e constErr) Error() string { return string(e) }

var errBoom = constErr("docker daemon unreachable")
