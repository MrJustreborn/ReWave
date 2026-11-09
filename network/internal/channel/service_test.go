package channel

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/db"
	"rewave/network/internal/uuid"
)

// fakeRuntime records lifecycle calls.
type fakeRuntime struct {
	started     []string
	stopped     []string
	playlistFor string
	startErr    error
	stopErr     error
}

func (f *fakeRuntime) EnsureRunning(_ context.Context, _ Channel, playlistEnv string) error {
	if f.startErr != nil {
		return f.startErr
	}
	f.started = append(f.started, playlistEnv)
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, _ Channel) error {
	if f.stopErr != nil {
		return f.stopErr
	}
	f.stopped = append(f.stopped, "stop")
	return nil
}

type fakePlaylistProvider struct {
	playlist string
}

func (f fakePlaylistProvider) PlaylistFor(_ context.Context, _ Channel) (string, error) {
	return f.playlist, nil
}

func lifecycleFixture(t *testing.T) (*Service, *fakeRuntime) {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := db.ApplyMigrations(context.Background(), d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	runtime := &fakeRuntime{}
	service := NewService(NewStore(d), runtime, fakePlaylistProvider{playlist: `{"slots":[]}`},
		func() time.Time { return time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC) })
	return service, runtime
}

func createTestChannel(t *testing.T, svc *Service, name string) Channel {
	t.Helper()
	def, err := NewDefinition(name, "movies", ContentPool{}, Schedule{})
	if err != nil {
		t.Fatalf("definition: %v", err)
	}
	ch, err := svc.Create(context.Background(), def)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return ch
}

func TestStartStopLifecycle(t *testing.T) {
	svc, runtime := lifecycleFixture(t)
	ctx := context.Background()
	ch := createTestChannel(t, svc, "movies-1")

	// Start resolves the playlist, starts the container, flips the status.
	started, err := svc.Start(ctx, ch.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if started.Status != StatusRunning {
		t.Fatalf("started status: %s", started.Status)
	}
	if len(runtime.started) != 1 || runtime.started[0] != `{"slots":[]}` {
		t.Fatalf("runtime calls: %v", runtime.started)
	}

	// Idempotent start — no second runtime call.
	startedAgain, err := svc.Start(ctx, ch.ID)
	if err != nil || startedAgain.Status != StatusRunning {
		t.Fatalf("idempotent start: %v %s", err, startedAgain.Status)
	}
	if len(runtime.started) != 1 {
		t.Fatalf("idempotent start called runtime again: %v", runtime.started)
	}

	// Stop removes the container and records the status.
	stopped, err := svc.Stop(ctx, ch.ID)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if stopped.Status != StatusStopped {
		t.Fatalf("stopped status: %s", stopped.Status)
	}
	if len(runtime.stopped) != 1 {
		t.Fatalf("runtime stops: %v", runtime.stopped)
	}

	// Starting again recreates the container (fresh playlist).
	if _, err := svc.Start(ctx, ch.ID); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if len(runtime.started) != 2 {
		t.Fatalf("restart runtime calls: %v", runtime.started)
	}

	// Unknown channel → 404 on both operations.
	if _, err := svc.Start(ctx, uuid.New()); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("start unknown: %v", err)
	}
	if _, err := svc.Stop(ctx, uuid.New()); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("stop unknown: %v", err)
	}
}

func TestLifecycleFailureMarksErrorState(t *testing.T) {
	svc, runtime := lifecycleFixture(t)
	ctx := context.Background()
	ch := createTestChannel(t, svc, "movies-1")

	runtime.startErr = fmt.Errorf("docker daemon unreachable")
	if _, err := svc.Start(ctx, ch.ID); err == nil {
		t.Fatal("start must fail")
	}
	failed, err := svc.Get(ctx, ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != StatusError {
		t.Fatalf("status after failed start: %s", failed.Status)
	}

	// Stop on a channel in error state still removes the container.
	runtime.startErr = nil
	runtime.stopErr = nil
	stopped, err := svc.Stop(ctx, ch.ID)
	if err != nil {
		t.Fatalf("stop after error: %v", err)
	}
	if stopped.Status != StatusStopped {
		t.Fatalf("stop after error status: %s", stopped.Status)
	}
}

func TestDeleteStopsContainerFirst(t *testing.T) {
	svc, runtime := lifecycleFixture(t)
	ctx := context.Background()
	ch := createTestChannel(t, svc, "movies-1")

	if _, err := svc.Start(ctx, ch.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := svc.Delete(ctx, ch.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(runtime.stopped) != 1 {
		t.Fatalf("delete must stop the container first: %v", runtime.stopped)
	}
	if _, err := svc.Get(ctx, ch.ID); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("get after delete: %v", err)
	}
}

func TestContainerName(t *testing.T) {
	ch := Channel{ID: "3f6bd1c2-9c5e-4d20-8a3d-6f2a5c9e7b11", Definition: Definition{Name: "Movie Channel! 1"}}
	got := ContainerName(ch)
	want := "rewave-channel-movie-channel-1-3f6bd1c2"
	if got != want {
		t.Fatalf("container name = %q, want %q", got, want)
	}
	if !strings.HasPrefix(got, "rewave-channel-") {
		t.Fatal("container name must carry the rewave prefix")
	}
	// Nameless channels still get a valid container name.
	anon := Channel{ID: "3f6bd1c2-9c5e-4d20-8a3d-6f2a5c9e7b11", Definition: Definition{Name: "///"}}
	if got := ContainerName(anon); got != "rewave-channel-channel-3f6bd1c2" {
		t.Fatalf("anonymous name = %q", got)
	}
}
