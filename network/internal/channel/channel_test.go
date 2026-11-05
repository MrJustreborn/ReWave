package channel

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validSchedule() Schedule {
	return Schedule{
		Templates: map[string][]Slot{
			"default": {{Start: "20:15", End: "22:30", Tags: []string{"movie"}}},
		},
		Days: map[string]string{"monday": "default"},
	}
}

func TestDefinitionValidation(t *testing.T) {
	tests := []struct {
		name      string
		def       Definition
		wantError string
	}{
		{
			name: "valid full definition",
			def:  Definition{Name: "channel-1", Type: "movies", ContentPool: ContentPool{VideoIDs: []string{"5d1f9e3a-1b64-4c0a-9d2f-7a6e4b8c1d20"}, Tags: []string{"movie"}}, Schedule: validSchedule()},
		},
		{
			name:      "empty name",
			def:       Definition{Name: "", Type: "movies"},
			wantError: "channel name",
		},
		{
			name:      "empty type",
			def:       Definition{Name: "c1", Type: ""},
			wantError: "channel type",
		},
		{
			name:      "non-uuid video id",
			def:       Definition{Name: "c1", Type: "movies", ContentPool: ContentPool{VideoIDs: []string{"not-a-uuid"}}},
			wantError: "contentPool.videoIds",
		},
		{
			name:      "empty pool tag",
			def:       Definition{Name: "c1", Type: "movies", ContentPool: ContentPool{Tags: []string{""}}},
			wantError: "contentPool.tags",
		},
		{
			name:      "bad slot start",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {{Start: "25:00", End: "22:30", Tags: []string{}}}}, Days: map[string]string{}}},
			wantError: "start",
		},
		{
			name:      "bad slot end",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {{Start: "20:15", End: "2:30", Tags: []string{}}}}, Days: map[string]string{}}},
			wantError: "end",
		},
		{
			name:      "nil slot tags",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {{Start: "20:15", End: "22:30"}}}, Days: map[string]string{}}},
			wantError: "tags",
		},
		{
			name:      "empty template",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {}}, Days: map[string]string{}}},
			wantError: "at least one slot",
		},
		{
			name:      "bad template name",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"bad name!": {{Start: "20:15", End: "22:30", Tags: []string{}}}}, Days: map[string]string{}}},
			wantError: "template name",
		},
		{
			name:      "bad weekday",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {{Start: "20:15", End: "22:30", Tags: []string{}}}}, Days: map[string]string{"someday": "d"}}},
			wantError: "not a weekday",
		},
		{
			name:      "unknown template reference",
			def:       Definition{Name: "c1", Type: "movies", Schedule: Schedule{Templates: map[string][]Slot{"d": {{Start: "20:15", End: "22:30", Tags: []string{}}}}, Days: map[string]string{"monday": "other"}}},
			wantError: "unknown template",
		},
		{
			name: "zero schedule is a valid empty schedule",
			def:  Definition{Name: "c1", Type: "movies"},
		},
		{
			name: "midnight-crossing slot is valid",
			def: Definition{Name: "c1", Type: "movies", Schedule: Schedule{
				Templates: map[string][]Slot{"d": {{Start: "23:00", End: "01:00", Tags: []string{}}}},
				Days:      map[string]string{"friday": "d"},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.def.Validate()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error %q does not mention %q", err.Error(), tt.wantError)
			}
		})
	}
}

func TestScheduleJSONShape(t *testing.T) {
	// Zero schedule encodes with non-nil maps.
	raw, err := json.Marshal(Schedule{})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"templates":{},"days":{}}` {
		t.Fatalf("zero schedule encoded as %s", raw)
	}
	// Round trip preserves content.
	raw, err = json.Marshal(validSchedule())
	if err != nil {
		t.Fatal(err)
	}
	var decoded Schedule
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Templates) != 1 || len(decoded.Templates["default"]) != 1 || decoded.Days["monday"] != "default" {
		t.Fatalf("round trip mismatch: %+v", decoded)
	}

	// Empty pool encodes with non-nil arrays.
	raw, err = json.Marshal(ContentPool{})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"videoIds":[],"tags":[]}` {
		t.Fatalf("empty pool encoded as %s", raw)
	}
}

func TestStatusTransitions(t *testing.T) {
	created := Channel{ID: "id", Status: StatusCreated, Definition: Definition{Name: "c", Type: "movies"}, CreatedAt: time.Now()}

	running := created.Started()
	if running.Status != StatusRunning {
		t.Fatalf("start: %s", running.Status)
	}
	again := running.Started()
	if again.Status != StatusRunning {
		t.Fatalf("idempotent start: %s", again.Status)
	}

	stopped := running.Stopped()
	if stopped.Status != StatusStopped {
		t.Fatalf("stop: %s", stopped.Status)
	}
	idempotent := stopped.Stopped()
	if idempotent.Status != StatusStopped {
		t.Fatalf("idempotent stop: %s", idempotent.Status)
	}
	// Stopping a never-started channel is a no-op.
	if never := created.Stopped(); never.Status != StatusCreated {
		t.Fatalf("stop from created: %s", never.Status)
	}

	// WithDefinition keeps lifecycle state.
	redefined := running.WithDefinition(Definition{Name: "renamed", Type: "movies"})
	if redefined.Status != StatusRunning || redefined.CreatedAt != created.CreatedAt || redefined.Definition.Name != "renamed" {
		t.Fatalf("WithDefinition lost state: %+v", redefined)
	}

	// Reconstitute rejects invalid persisted status.
	if _, err := Reconstitute("id", Definition{Name: "c", Type: "movies"}, "exploded", time.Now()); err == nil {
		t.Fatal("expected invalid-status error")
	}
}
