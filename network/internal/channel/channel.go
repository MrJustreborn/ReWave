// Package channel is the channel bounded context: the registry of TV
// channels (definition, schedule, content pool) and their lifecycle
// state. The network is the only component that knows channels.
package channel

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/uuid"
)

// Status is the lifecycle state of a channel. Transitions are
// asynchronous (container start/stop); clients poll the channel detail.
type Status string

const (
	StatusCreated Status = "created"
	StatusRunning Status = "running"
	StatusStopped Status = "stopped"
	StatusError   Status = "error"
)

func (s Status) valid() bool {
	switch s {
	case StatusCreated, StatusRunning, StatusStopped, StatusError:
		return true
	}
	return false
}

// Slot is a TV slot within a day template (wall-clock times, e.g.
// primetime 20:15-22:30). An End earlier than Start means the slot
// crosses midnight.
type Slot struct {
	Start string   `json:"start"`
	End   string   `json:"end"`
	Tags  []string `json:"tags"`
}

// Schedule is per-day scheduling: named day templates plus a
// weekday-to-template mapping. Weekdays missing from Days fall back to
// the template named "default" when present (resolved by the scheduling
// context); otherwise the channel runs nothing that day.
type Schedule struct {
	Templates map[string][]Slot `json:"templates"`
	Days      map[string]string `json:"days"`
}

// ContentPool is the content a channel may draw from: the union of the
// listed video IDs and the videos carrying any of the listed tags. An
// empty pool means the entire media library (still filtered by slot
// tags).
type ContentPool struct {
	VideoIDs []string `json:"videoIds"`
	Tags     []string `json:"tags"`
}

// Definition is the operator-defined part of a channel.
type Definition struct {
	Name        string
	Type        string
	ContentPool ContentPool
	Schedule    Schedule
}

// Channel is a TV channel: definition plus current runtime status.
type Channel struct {
	ID         string
	CreatedAt  time.Time
	Status     Status
	Definition Definition
}

// NewDefinition validates and returns a channel definition.
func NewDefinition(name, typ string, pool ContentPool, schedule Schedule) (Definition, error) {
	def := Definition{Name: name, Type: typ, ContentPool: pool, Schedule: schedule}
	return def, def.Validate()
}

// Validate checks the definition: non-empty name and type, a well-formed
// content pool and schedule.
func (d Definition) Validate() error {
	if d.Name == "" {
		return apperr.Validation("channel name must be a non-empty string")
	}
	if d.Type == "" {
		return apperr.Validation("channel type must be a non-empty string")
	}
	if err := d.ContentPool.Validate(); err != nil {
		return err
	}
	return d.Schedule.Validate()
}

// Validate checks the content pool: UUID-shaped video IDs and non-empty
// tags.
func (p ContentPool) Validate() error {
	for _, id := range p.VideoIDs {
		if !uuid.IsValid(id) {
			return apperr.Validation(fmt.Sprintf("contentPool.videoIds entries must be UUIDs, got %q", id))
		}
	}
	for _, tag := range p.Tags {
		if tag == "" {
			return apperr.Validation("contentPool.tags entries must be non-empty strings")
		}
	}
	return nil
}

// Validate checks the schedule. The zero value (no templates, no days)
// is a valid empty schedule; a partially present schedule is rejected,
// mirroring the required templates/days fields of the API contract.
func (s Schedule) Validate() error {
	if s.isZero() {
		return nil
	}
	if s.Templates == nil {
		return apperr.Validation("schedule.templates must be an object")
	}
	if s.Days == nil {
		return apperr.Validation("schedule.days must be an object")
	}
	for name, slots := range s.Templates {
		if !templateNamePattern.MatchString(name) {
			return apperr.Validation(fmt.Sprintf("schedule template name %q must match %s", name, templateNamePatternSource))
		}
		if len(slots) == 0 {
			return apperr.Validation(fmt.Sprintf("schedule template %q must contain at least one slot", name))
		}
		for i, slot := range slots {
			if err := slot.validate(fmt.Sprintf("schedule.templates.%s[%d]", name, i)); err != nil {
				return err
			}
		}
	}
	for day, template := range s.Days {
		if !isWeekday(day) {
			return apperr.Validation(fmt.Sprintf("schedule day %q is not a weekday", day))
		}
		if template == "" {
			return apperr.Validation(fmt.Sprintf("schedule.days.%s must be a non-empty template name", day))
		}
		if _, ok := s.Templates[template]; !ok {
			return apperr.Validation(fmt.Sprintf("schedule.days.%s references unknown template %q", day, template))
		}
	}
	return nil
}

func (s Schedule) isZero() bool {
	return len(s.Templates) == 0 && len(s.Days) == 0
}

func (slot Slot) validate(field string) error {
	if !wallClockPattern.MatchString(slot.Start) {
		return apperr.Validation(fmt.Sprintf("%s.start must be an HH:MM wall-clock time, got %q", field, slot.Start))
	}
	if !wallClockPattern.MatchString(slot.End) {
		return apperr.Validation(fmt.Sprintf("%s.end must be an HH:MM wall-clock time, got %q", field, slot.End))
	}
	if slot.Tags == nil {
		return apperr.Validation(field + ".tags must be an array of strings")
	}
	for _, tag := range slot.Tags {
		if tag == "" {
			return apperr.Validation(field + ".tags entries must be non-empty strings")
		}
	}
	return nil
}

// MarshalJSON guarantees the storage/JSON shape: non-nil maps and arrays
// (an empty schedule encodes as {"templates":{},"days":{}}, an empty
// pool as {"videoIds":[],"tags":[]}).
func (s Schedule) MarshalJSON() ([]byte, error) {
	templates := s.Templates
	if templates == nil {
		templates = map[string][]Slot{}
	}
	days := s.Days
	if days == nil {
		days = map[string]string{}
	}
	for name, slots := range templates {
		if slots == nil {
			templates[name] = []Slot{}
		}
	}
	return json.Marshal(struct {
		Templates map[string][]Slot `json:"templates"`
		Days      map[string]string `json:"days"`
	}{templates, days})
}

// MarshalJSON guarantees the storage/JSON shape (non-nil arrays).
func (p ContentPool) MarshalJSON() ([]byte, error) {
	videoIDs := p.VideoIDs
	if videoIDs == nil {
		videoIDs = []string{}
	}
	tags := p.Tags
	if tags == nil {
		tags = []string{}
	}
	return json.Marshal(struct {
		VideoIDs []string `json:"videoIds"`
		Tags     []string `json:"tags"`
	}{videoIDs, tags})
}

// WithDefinition returns the channel with its definition replaced
// (status and creation time are kept).
func (c Channel) WithDefinition(def Definition) Channel {
	c.Definition = def
	return c
}

// Started returns the channel in the running state (idempotent).
func (c Channel) Started() Channel {
	if c.Status == StatusRunning {
		return c
	}
	c.Status = StatusRunning
	return c
}

// Stopped returns the channel in the stopped state. Running and error
// channels transition (an error channel that stops cleanly is back to
// stopped — the container was removed); stopping is idempotent
// otherwise.
func (c Channel) Stopped() Channel {
	if c.Status != StatusRunning && c.Status != StatusError {
		return c
	}
	c.Status = StatusStopped
	return c
}

// Reconstitute rebuilds a channel from persisted state, validating the
// status value.
func Reconstitute(id string, def Definition, status Status, createdAt time.Time) (Channel, error) {
	if !status.valid() {
		return Channel{}, fmt.Errorf("invalid channel status %q in persisted data", status)
	}
	return Channel{ID: id, Definition: def, Status: status, CreatedAt: createdAt}, nil
}

var (
	wallClockPattern          = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)
	templateNamePattern       = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	templateNamePatternSource = `^[A-Za-z0-9_-]+$`
)

func isWeekday(day string) bool {
	switch day {
	case "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday":
		return true
	}
	return false
}

// Weekdays lists the schedule day keys in calendar order.
func Weekdays() []string {
	return []string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
}
