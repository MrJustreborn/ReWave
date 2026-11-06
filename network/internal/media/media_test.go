package media

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/db"
)

func testService(t *testing.T) (*Service, *Store) {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := db.ApplyMigrations(context.Background(), d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := NewStore(d)
	return NewService(store, fakeDetector{duration: 8100, breaks: []float64{2430, 4050}}), store
}

type fakeDetector struct {
	duration float64
	breaks   []float64
}

func (f fakeDetector) ProbeDuration(_ context.Context, _ string) (float64, error) {
	return f.duration, nil
}

func (f fakeDetector) DetectAdBreaks(_ context.Context, _ string, _ float64) ([]float64, error) {
	return f.breaks, nil
}

func TestParseSceneCuts(t *testing.T) {
	output := `[Parsed_showinfo @ 0x55f] n:   0 pts:      0 pts_time:0        pos:      0
[Parsed_showinfo @ 0x55f] n:   1 pts:   3600 pts_time:40       pos:  12345
[Parsed_showinfo @ 0x55f] n:   2 pts:  11700 pts_time:130      pos:  99999
[av_log] unrelated line without showinfo
[Parsed_showinfo @ 0x55f] n:   3 pts:  12600 pts_time:140.25    pos: 123456
`
	cuts := parseSceneCuts(output, 500)
	want := []float64{500, 540, 630, 640.25}
	if len(cuts) != len(want) {
		t.Fatalf("cuts = %v, want %v", cuts, want)
	}
	for i := range want {
		if cuts[i] != want[i] {
			t.Fatalf("cuts = %v, want %v", cuts, want)
		}
	}
	if parseSceneCuts("no pts_time here", 0) != nil {
		t.Fatal("no cuts expected")
	}
}

func TestNearestTo(t *testing.T) {
	if got := nearestTo([]float64{100, 200, 205, 300}, 203); got != 205 {
		t.Fatalf("nearest = %v", got)
	}
	if got := nearestTo([]float64{100, 200, 205, 300}, 102); got != 100 {
		t.Fatalf("nearest = %v", got)
	}
}

func TestVideoValidationAndTransitions(t *testing.T) {
	if _, err := NewVideo("id", "", "title", 0, nil, time.Now()); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("empty path: %v", err)
	}
	if _, err := NewVideo("id", "p", "", 0, nil, time.Now()); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("empty title: %v", err)
	}
	if _, err := NewVideo("id", "p", "t", -1, nil, time.Now()); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("negative duration: %v", err)
	}
	if _, err := NewVideo("id", "p", "t", 0, []string{""}, time.Now()); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("empty tag: %v", err)
	}

	v, err := NewVideo("id", "p", "t", 10, []string{"a"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	processed, err := v.WithProcessedAdBreaks([]float64{3, 9.5})
	if err != nil {
		t.Fatal(err)
	}
	if !processed.Processed || len(processed.AdBreaks) != 2 || processed.AdBreaks[1].Position != 9.5 {
		t.Fatalf("processed: %+v", processed)
	}
	if _, err := processed.WithProcessedAdBreaks([]float64{1}); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("re-processing: %v", err)
	}
	if _, err := v.WithProcessedAdBreaks([]float64{-1}); apperr.HTTPStatus(err) != 400 {
		t.Fatalf("negative break: %v", err)
	}
}

func TestServiceAddAndIngest(t *testing.T) {
	svc, store := testService(t)
	ctx := context.Background()
	var mu sync.Mutex // serializes assertions against the ingest goroutine

	added, err := svc.Add(ctx, AddVideo{Path: "movies/movie.mp4", Tags: []string{"movie"}})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	mu.Lock()
	if added.Title != "movie" || added.Duration != 8100 || added.Processed {
		t.Fatalf("added: %+v", added)
	}
	mu.Unlock()

	// Duplicate path conflicts.
	if _, err := svc.Add(ctx, AddVideo{Path: "movies/movie.mp4"}); apperr.HTTPStatus(err) != 409 {
		t.Fatalf("duplicate: %v", err)
	}

	// Ingestion completes in the background.
	deadline := time.Now().Add(2 * time.Second)
	for {
		v, err := store.FindByID(ctx, added.ID)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil && v.Processed {
			if len(v.AdBreaks) != 2 || v.AdBreaks[0].Position != 2430 {
				t.Fatalf("breaks: %+v", v.AdBreaks)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ingestion did not complete")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Filtered listing.
	unprocessed, err := svc.List(ctx, &Filter{Processed: false})
	if err != nil || len(unprocessed) != 0 {
		t.Fatalf("unprocessed list: %v %+v", err, unprocessed)
	}

	// Delete then 404.
	if err := svc.Delete(ctx, added.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := svc.Delete(ctx, added.ID); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("delete unknown: %v", err)
	}
}

func TestDefaultTitle(t *testing.T) {
	tests := []struct{ path, want string }{
		{"/media-pool/movies/new-movie.mp4", "new-movie"},
		{"plain.avi", "plain"},
		{"/media-pool/no-ext", "no-ext"},
	}
	for _, tt := range tests {
		if got := defaultTitle(tt.path); got != tt.want {
			t.Errorf("defaultTitle(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}
