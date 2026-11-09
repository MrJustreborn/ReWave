package channel

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/db"
	"rewave/network/internal/uuid"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := db.ApplyMigrations(context.Background(), d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewStore(d)
}

func TestStoreCRUD(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	fixedNow := time.Date(2026, 11, 5, 18, 45, 0, 0, time.UTC)

	def, err := NewDefinition("channel-1", "movies",
		ContentPool{VideoIDs: []string{uuid.New()}, Tags: []string{"movie"}},
		validSchedule())
	if err != nil {
		t.Fatalf("definition: %v", err)
	}
	svc := NewService(store, noopRuntime{}, fakePlaylistProvider{}, func() time.Time { return fixedNow })

	created, err := svc.Create(ctx, def)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID == "" || created.Status != StatusCreated || !created.CreatedAt.Equal(fixedNow) {
		t.Fatalf("created channel: %+v", created)
	}

	// Duplicate name → conflict.
	if _, err := svc.Create(ctx, def); apperr.HTTPStatus(err) != 409 {
		t.Fatalf("duplicate create: %v", err)
	}

	// Get round-trips the definition (JSON columns included).
	fetched, err := svc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if fetched.Definition.Name != "channel-1" || fetched.Definition.Type != "movies" {
		t.Fatalf("fetched: %+v", fetched)
	}
	if len(fetched.Definition.ContentPool.VideoIDs) != 1 || len(fetched.Definition.ContentPool.Tags) != 1 {
		t.Fatalf("pool round trip: %+v", fetched.Definition.ContentPool)
	}
	if len(fetched.Definition.Schedule.Templates) != 1 || fetched.Definition.Schedule.Days["monday"] != "default" {
		t.Fatalf("schedule round trip: %+v", fetched.Definition.Schedule)
	}
	if fetched.Definition.Schedule.Templates["default"][0].Start != "20:15" {
		t.Fatalf("slot round trip: %+v", fetched.Definition.Schedule.Templates)
	}

	// Unknown id → 404.
	if _, err := svc.Get(ctx, uuid.New()); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("get unknown: %v", err)
	}

	// Replace keeps status and creation time, swaps the definition.
	running := fetched.Started()
	if err := store.Save(ctx, running); err != nil {
		t.Fatalf("save running: %v", err)
	}
	nextDef, err := NewDefinition("channel-1", "series", ContentPool{}, Schedule{})
	if err != nil {
		t.Fatalf("next def: %v", err)
	}
	replaced, err := svc.Replace(ctx, created.ID, nextDef)
	if err != nil {
		t.Fatalf("replace: %v", err)
	}
	if replaced.Status != StatusRunning || !replaced.CreatedAt.Equal(fixedNow) || replaced.Definition.Type != "series" {
		t.Fatalf("replaced: %+v", replaced)
	}

	// Renaming to an existing other channel's name → conflict.
	second, err := svc.Create(ctx, mustDefinition(t, "channel-2", "cartoons"))
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	if _, err := svc.Replace(ctx, second.ID, mustDefinition(t, "channel-1", "cartoons")); apperr.HTTPStatus(err) != 409 {
		t.Fatalf("rename conflict: %v", err)
	}

	// List is ordered by name.
	list, err := svc.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 || list[0].Definition.Name != "channel-1" || list[1].Definition.Name != "channel-2" {
		t.Fatalf("list: %+v", list)
	}

	// Delete, then 404.
	if err := svc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := svc.Delete(ctx, created.ID); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("delete unknown: %v", err)
	}
}

func mustDefinition(t *testing.T, name, typ string) Definition {
	t.Helper()
	def, err := NewDefinition(name, typ, ContentPool{}, Schedule{})
	if err != nil {
		t.Fatalf("definition %s: %v", name, err)
	}
	return def
}

// noopRuntime satisfies Runtime without side effects (CRUD tests don't
// exercise the lifecycle).
type noopRuntime struct{}

func (noopRuntime) EnsureRunning(context.Context, Channel, string) error { return nil }
func (noopRuntime) Stop(context.Context, Channel) error                  { return nil }
