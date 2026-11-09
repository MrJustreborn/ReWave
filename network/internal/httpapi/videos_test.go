package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"rewave/network/internal/httpapi/gen"
)

// waitForProcessed polls the video detail until ingestion has marked it
// processed (the fake detector resolves instantly, but the ingestion
// runs on a background goroutine).
func waitForProcessed(t *testing.T, env *testEnv, videoID string) gen.Video {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		rec := do(t, env.handler, http.MethodGet, "/v1/videos/"+videoID, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("get video: %d %s", rec.Code, rec.Body.String())
		}
		var v gen.Video
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Processed {
			return v
		}
		if time.Now().After(deadline) {
			t.Fatalf("video %s was not processed in time", videoID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestVideosFlow(t *testing.T) {
	env := newTestServer(t)

	// Add → 201, processed=false, title defaulted from the file name.
	rec := do(t, env.handler, http.MethodPost, "/v1/videos",
		`{"path": "/media-pool/movies/new-movie.mp4", "tags": ["movie"]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("add: %d %s", rec.Code, rec.Body.String())
	}
	var added gen.Video
	if err := json.Unmarshal(rec.Body.Bytes(), &added); err != nil {
		t.Fatal(err)
	}
	if added.Title != "new-movie" {
		t.Fatalf("title default: %q", added.Title)
	}
	if added.Processed || len(added.AdBreaks) != 0 {
		t.Fatalf("added must be pending: %+v", added)
	}
	if added.Duration != 1800 {
		t.Fatalf("duration from probe: %v", added.Duration)
	}
	if rec.Header().Get("Location") != "/v1/videos/"+added.Id.String() {
		t.Fatalf("location: %q", rec.Header().Get("Location"))
	}

	// Duplicate path → 409.
	rec = do(t, env.handler, http.MethodPost, "/v1/videos", `{"path": "/media-pool/movies/new-movie.mp4"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d", rec.Code)
	}

	// Async ingestion fills in the ad breaks.
	processed := waitForProcessed(t, env, added.Id.String())
	if len(processed.AdBreaks) != 1 || processed.AdBreaks[0].Position != 900 {
		t.Fatalf("processed breaks: %+v", processed.AdBreaks)
	}

	// List with filter.
	rec = do(t, env.handler, http.MethodGet, "/v1/videos?processed=false", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list unprocessed: %d %s", rec.Code, rec.Body.String())
	}
	var list gen.VideoList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Videos) != 0 {
		t.Fatalf("unprocessed list should be empty: %+v", list)
	}

	rec = do(t, env.handler, http.MethodGet, "/v1/videos", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Videos) != 1 || list.Videos[0].Processed != true {
		t.Fatalf("full list: %+v", list)
	}

	// Unknown video → 404; delete → 204 → 404.
	rec = do(t, env.handler, http.MethodGet, "/v1/videos/00000000-0000-4000-8000-000000000000", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown: %d", rec.Code)
	}
	rec = do(t, env.handler, http.MethodDelete, "/v1/videos/"+added.Id.String(), "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec = do(t, env.handler, http.MethodGet, "/v1/videos/"+added.Id.String(), "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("after delete: %d", rec.Code)
	}
}
