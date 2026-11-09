package httpapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"rewave/network/internal/httpapi/gen"
)

func TestWatchEndpoint(t *testing.T) {
	env := newTestServer(t)
	channelID := createChannelWithContent(t, env)

	// First heartbeat starts the channel.
	rec := do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/watch",
		`{"viewerId":"tvui-8f3c1a2b"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("watch: %d %s", rec.Code, rec.Body.String())
	}
	var response gen.WatchResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Viewers != 1 || response.Status != gen.ChannelStatusRunning {
		t.Fatalf("response: %+v", response)
	}
	if response.KeepAliveUntil == nil {
		t.Fatal("keepAliveUntil must be set while a viewer is watching")
	}
	if !response.KeepAliveUntil.Equal(env.fixedNow().Add(5 * time.Minute)) {
		t.Fatalf("keep-alive horizon: %v", response.KeepAliveUntil)
	}

	// A second viewer joins.
	rec = do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/watch",
		`{"viewerId":"tvui-99aa22bb"}`)
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Viewers != 2 {
		t.Fatalf("second viewer: %+v", response)
	}

	// The channel is running now.
	rec = do(t, env.handler, http.MethodGet, "/v1/channels/"+channelID, "")
	var ch gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &ch); err != nil {
		t.Fatal(err)
	}
	if ch.Status != gen.ChannelStatusRunning {
		t.Fatalf("channel after watch: %s", ch.Status)
	}

	// Invalid bodies are rejected.
	for _, body := range []string{
		`{"viewerId":""}`,
		`{"other":"x"}`, // missing viewerId
		`not json`,
	} {
		rec = do(t, env.handler, http.MethodPost, "/v1/channels/"+channelID+"/watch", body)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("bad body %q: %d %s", body, rec.Code, rec.Body.String())
		}
	}
}
