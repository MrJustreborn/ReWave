package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"rewave/network/internal/httpapi/gen"
)

// createChannelWithContent creates a channel with a daily 10:00-11:00
// slot plus one 1800 s video (via the fake detector) and returns the
// channel id.
func createChannelWithContent(t *testing.T, env *testEnv) string {
	t.Helper()
	rec := do(t, env.handler, http.MethodPost, "/v1/videos", `{"path": "/media-pool/movie.mp4"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("add video: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, env.handler, http.MethodPost, "/v1/channels", `{
		"name": "movies-1",
		"type": "movies",
		"schedule": {
			"templates": {"default": [{"start": "10:00", "end": "11:00", "tags": []}]},
			"days": {"monday": "default", "tuesday": "default", "wednesday": "default",
			         "thursday": "default", "friday": "default", "saturday": "default",
			         "sunday": "default"}
		}
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create channel: %d %s", rec.Code, rec.Body.String())
	}
	var created gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	return created.Id.String()
}

func TestChannelLifecycleAndPlaylist(t *testing.T) {
	env := newTestServer(t)
	channelID := createChannelWithContent(t, env)

	// Before start: the resolved playlist is empty.
	rec := do(t, env.handler, http.MethodGet, "/v1/channels/"+channelID+"/playlist", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("playlist before start: %d %s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"slots":[]}` {
		t.Fatalf("playlist before start: %s", rec.Body.String())
	}

	// Start → running; the container receives the resolved playlist.
	rec = do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/start", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("start: %d %s", rec.Code, rec.Body.String())
	}
	var started gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &started); err != nil {
		t.Fatal(err)
	}
	if started.Status != gen.ChannelStatusRunning {
		t.Fatalf("started status: %s", started.Status)
	}
	env.runtime.mu.Lock()
	startedWith := len(env.runtime.started)
	playlistEnv := ""
	if startedWith == 1 {
		playlistEnv = env.runtime.started[0]
	}
	env.runtime.mu.Unlock()
	if startedWith != 1 {
		t.Fatalf("runtime calls: %d", startedWith)
	}
	if !strings.Contains(playlistEnv, `"file":"/media-pool/movie.mp4"`) ||
		!strings.Contains(playlistEnv, `"timestamp":"2026-11-09T10:00:00Z"`) ||
		!strings.Contains(playlistEnv, `"type":"content"`) {
		t.Fatalf("playlist env: %s", playlistEnv)
	}

	// Idempotent start.
	rec = do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/start", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("idempotent start: %d", rec.Code)
	}
	env.runtime.mu.Lock()
	startedWith = len(env.runtime.started)
	env.runtime.mu.Unlock()
	if startedWith != 1 {
		t.Fatalf("idempotent start hit the runtime: %d", startedWith)
	}

	// The resolved playlist is exposed via the API.
	rec = do(t, env.handler, http.MethodGet, "/v1/channels/"+channelID+"/playlist", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("playlist: %d", rec.Code)
	}
	var playlist gen.Playlist
	if err := json.Unmarshal(rec.Body.Bytes(), &playlist); err != nil {
		t.Fatal(err)
	}
	if len(playlist.Slots) == 0 {
		t.Fatal("playlist must have slots after start")
	}
	if got := playlist.Slots[0].Stream[0].File; got != "/media-pool/movie.mp4" {
		t.Fatalf("first stream entry: %q", got)
	}
	if playlist.Slots[0].Start.Year() != 2026 {
		t.Fatalf("slot start: %v", playlist.Slots[0].Start)
	}

	// Stop → stopped.
	rec = do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/stop", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("stop: %d %s", rec.Code, rec.Body.String())
	}
	var stopped gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &stopped); err != nil {
		t.Fatal(err)
	}
	if stopped.Status != gen.ChannelStatusStopped {
		t.Fatalf("stopped status: %s", stopped.Status)
	}

	// Delete removes the channel (stop included).
	rec = do(t, env.handler, http.MethodDelete, "/v1/channels/"+channelID, "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec = do(t, env.handler, http.MethodGet, "/v1/channels/"+channelID+"/playlist", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("playlist after delete: %d", rec.Code)
	}
}
