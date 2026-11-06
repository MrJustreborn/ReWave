package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"rewave/network/internal/channel"
	"rewave/network/internal/db"
	"rewave/network/internal/httpapi/gen"
	"rewave/network/internal/media"
)

// newTestServer builds the full API handler stack (no auth) over a
// throwaway SQLite database.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	d, err := db.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	if _, err := db.ApplyMigrations(context.Background(), d); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	channelService := channel.NewService(channel.NewStore(d))
	mediaService := media.NewService(media.NewStore(d), fakeDetector{})
	return CORS(gen.Handler(New(channelService, mediaService)))
}

// fakeDetector is a deterministic media.Detector for tests.
type fakeDetector struct{}

func (fakeDetector) ProbeDuration(_ context.Context, _ string) (float64, error) {
	return 8100, nil
}

func (fakeDetector) DetectAdBreaks(_ context.Context, _ string, _ float64) ([]float64, error) {
	return []float64{3900}, nil
}

func do(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

const channelDefinitionJSON = `{
	"name": "channel-1",
	"type": "movies",
	"contentPool": {
		"videoIds": ["5d1f9e3a-1b64-4c0a-9d2f-7a6e4b8c1d20"],
		"tags": ["movie"]
	},
	"schedule": {
		"templates": {
			"default": [
				{"start": "00:00", "end": "06:00", "tags": []},
				{"start": "20:15", "end": "22:30", "tags": ["movie", "primetime"]}
			]
		},
		"days": {"monday": "default", "tuesday": "default"}
	}
}`

func TestChannelsCRUDFlow(t *testing.T) {
	handler := newTestServer(t)

	// Empty list.
	rec := do(t, handler, http.MethodGet, "/v1/channels", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body.String())
	}
	if strings.TrimSpace(rec.Body.String()) != `{"channels":[]}` {
		t.Fatalf("empty list body: %s", rec.Body.String())
	}

	// Create → 201 + Location + body.
	rec = do(t, handler, http.MethodPost, "/v1/channels", channelDefinitionJSON)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("create body: %v", err)
	}
	if created.Name != "channel-1" || created.Status != gen.ChannelStatusCreated {
		t.Fatalf("created: %+v", created)
	}
	if created.Schedule == nil || created.Schedule.Templates["default"] == nil || len(created.Schedule.Templates["default"]) != 2 {
		t.Fatalf("created schedule: %+v", created.Schedule)
	}
	if location := rec.Header().Get("Location"); location != "/v1/channels/"+created.Id.String() {
		t.Fatalf("location: %q", location)
	}
	channelID := created.Id.String()

	// Duplicate name → 409.
	rec = do(t, handler, http.MethodPost, "/v1/channels", channelDefinitionJSON)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d %s", rec.Code, rec.Body.String())
	}

	// Get by id → 200 with full definition.
	rec = do(t, handler, http.MethodGet, "/v1/channels/"+channelID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
	var fetched gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.Id != created.Id || fetched.Type != "movies" {
		t.Fatalf("fetched: %+v", fetched)
	}
	if fetched.ContentPool == nil || fetched.ContentPool.Tags == nil || len(*fetched.ContentPool.Tags) != 1 {
		t.Fatalf("fetched pool: %+v", fetched.ContentPool)
	}

	// Replace → 200, updated definition, same id.
	rec = do(t, handler, http.MethodPut, "/v1/channels/"+channelID,
		`{"name": "channel-1", "type": "series"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("replace: %d %s", rec.Code, rec.Body.String())
	}
	var replaced gen.Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &replaced); err != nil {
		t.Fatal(err)
	}
	if replaced.Id != created.Id || replaced.Type != "series" || replaced.Schedule == nil {
		t.Fatalf("replaced: %+v", replaced)
	}

	// Invalid definitions → 400 with openapi Error body.
	for _, bad := range []string{
		`{"name": "", "type": "movies"}`, // empty name
		`{"name": "x", "type": "movies", "schedule": {"templates": {"d": []}, "days": {}}}`, // empty template
		`not json`, // garbage body
	} {
		rec = do(t, handler, http.MethodPost, "/v1/channels", bad)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("bad body %q: %d %s", bad, rec.Code, rec.Body.String())
		}
		var errBody gen.Error
		if json.Unmarshal(rec.Body.Bytes(), &errBody) != nil || errBody.Code != 400 || errBody.Message == "" {
			t.Fatalf("bad body error shape: %s", rec.Body.String())
		}
	}

	// Unknown channel → 404.
	unknown := uuid.New().String()
	rec = do(t, handler, http.MethodGet, "/v1/channels/"+unknown, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown get: %d %s", rec.Code, rec.Body.String())
	}

	// Delete → 204, then 404.
	rec = do(t, handler, http.MethodDelete, "/v1/channels/"+channelID, "")
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("delete: %d %q", rec.Code, rec.Body.String())
	}
	rec = do(t, handler, http.MethodGet, "/v1/channels/"+channelID, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d", rec.Code)
	}

	// Unimplemented operations still answer 501.
	rec = do(t, handler, http.MethodPost, fmt.Sprintf("/v1/channels/%s/start", unknown), "")
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("start: %d", rec.Code)
	}
	rec = do(t, handler, http.MethodGet, fmt.Sprintf("/v1/channels/%s/playlist", unknown), "")
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("playlist: %d", rec.Code)
	}
}
