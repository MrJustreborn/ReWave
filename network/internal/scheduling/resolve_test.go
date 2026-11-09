package scheduling

import (
	"context"
	"strings"
	"testing"
	"time"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
	"rewave/network/internal/media"
)

var testLocation = time.FixedZone("CET", 3600)

func at(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04", value, testLocation)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

// todaySlots returns the resolved slots that start on the same day as
// the given time (the horizon covers two days).
func todaySlots(playlist Playlist, day time.Time) []Slot {
	var result []Slot
	for _, slot := range playlist.Slots {
		startYear, startMonth, startDay := slot.Start.Date()
		dayYear, dayMonth, dayDay := day.Date()
		if startYear == dayYear && startMonth == dayMonth && startDay == dayDay {
			result = append(result, slot)
		}
	}
	return result
}

func video(id, path string, duration float64, tags ...string) media.Video {
	return media.Video{ID: id, Path: path, Title: path, Duration: duration, Tags: tags}
}

func scheduleWith(t *testing.T, slots ...channel.Slot) channel.Schedule {
	t.Helper()
	return channel.Schedule{
		Templates: map[string][]channel.Slot{"default": slots},
		Days:      map[string]string{"monday": "default", "tuesday": "default", "wednesday": "default", "thursday": "default", "friday": "default", "saturday": "default", "sunday": "default"},
	}
}

// resolve is the test entry: a channel with the given schedule and
// pool, videos, resolved at the given time.
func resolve(t *testing.T, now time.Time, schedule channel.Schedule, pool channel.ContentPool, videos []media.Video) Playlist {
	t.Helper()
	def, err := channel.NewDefinition("c", "movies", pool, schedule)
	if err != nil {
		t.Fatalf("definition: %v", err)
	}
	in := Inputs{Now: now, Channel: channel.Channel{ID: "id", Definition: def}, Videos: videos}
	playlist := Resolve(in)
	if err := playlist.Validate(); err != nil {
		t.Fatalf("resolved playlist invalid: %v", err)
	}
	return playlist
}

func TestResolveFillsAndTrims(t *testing.T) {
	now := at(t, "2026-11-09 10:15") // a Monday
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "11:00", Tags: []string{}}),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1200), video("v2", "b.mp4", 3600)})

	today := todaySlots(playlist, now)
	if len(today) != 1 {
		t.Fatalf("slots: %d", len(playlist.Slots))
	}
	slot := today[0]
	if !slot.Start.Equal(at(t, "2026-11-09 10:00")) || !slot.End.Equal(at(t, "2026-11-09 11:00")) {
		t.Fatalf("slot times: %v – %v", slot.Start, slot.End)
	}
	if len(slot.Stream) != 2 {
		t.Fatalf("stream: %+v", slot.Stream)
	}
	first, second := slot.Stream[0], slot.Stream[1]
	if first.File != "a.mp4" || first.Duration != 1200 || first.EndOffset != "" {
		t.Fatalf("first entry: %+v", first)
	}
	// b.mp4 (3600s) is trimmed to the remaining 2400s.
	if second.File != "b.mp4" || second.Duration != 2400 || second.EndOffset != "00:40:00" {
		t.Fatalf("second entry: %+v", second)
	}
}

func TestResolveCyclesVideos(t *testing.T) {
	now := at(t, "2026-11-09 10:00")
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "12:00", Tags: []string{}}),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 3600)})

	today := todaySlots(playlist, now)
	if len(today) != 1 || len(today[0].Stream) != 2 {
		t.Fatalf("playlist: %+v", playlist)
	}
	if today[0].Stream[0].File != "a.mp4" || today[0].Stream[1].File != "a.mp4" {
		t.Fatalf("cycling: %+v", today[0].Stream)
	}
}

func TestResolveSkipsPastSlotsAndDays(t *testing.T) {
	now := at(t, "2026-11-09 10:00") // Monday 10:00
	playlist := resolve(t, now,
		scheduleWith(t,
			channel.Slot{Start: "08:00", End: "09:00", Tags: []string{}}, // past
			channel.Slot{Start: "11:00", End: "12:00", Tags: []string{}}, // today
		),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1800)})

	today := todaySlots(playlist, now)
	if len(today) != 1 {
		t.Fatalf("today's slots: %d (past slot must be skipped)", len(today))
	}
	if !today[0].Start.Equal(at(t, "2026-11-09 11:00")) {
		t.Fatalf("slot start: %v", today[0].Start)
	}
	// Tomorrow both slots are planned (the 08:00 slot is not past there).
	if len(playlist.Slots) != 3 {
		t.Fatalf("total slots: %d", len(playlist.Slots))
	}
}

func TestResolveIncludesRunningSlotFromItsStart(t *testing.T) {
	// A slot that already began is included with its full start — the
	// channel handles late starts by seeking.
	now := at(t, "2026-11-09 10:30")
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "11:00", Tags: []string{}}),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1800)})

	today := todaySlots(playlist, now)
	if len(today) != 1 {
		t.Fatalf("today's slots: %d (running slot must be included)", len(today))
	}
	if !today[0].Start.Equal(at(t, "2026-11-09 10:00")) {
		t.Fatalf("slot start: %v", today[0].Start)
	}
}

func TestResolveMidnightCrossing(t *testing.T) {
	now := at(t, "2026-11-09 22:00")
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "23:00", End: "01:00", Tags: []string{}}),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1800)})

	today := todaySlots(playlist, now)
	if len(today) != 1 {
		t.Fatalf("today's slots: %d", len(today))
	}
	slot := playlist.Slots[0]
	if !slot.Start.Equal(at(t, "2026-11-09 23:00")) || !slot.End.Equal(at(t, "2026-11-10 01:00")) {
		t.Fatalf("crossing slot: %v – %v", slot.Start, slot.End)
	}
}

func TestResolveDaySelectionAndFallback(t *testing.T) {
	// Monday uses the "weekend" template; a weekday without a mapping
	// falls back to "default".
	schedule := channel.Schedule{
		Templates: map[string][]channel.Slot{
			"default": {{Start: "20:00", End: "21:00", Tags: []string{}}},
			"late":    {{Start: "23:00", End: "23:30", Tags: []string{}}},
		},
		Days: map[string]string{"monday": "late"},
	}
	now := at(t, "2026-11-09 10:00") // Monday
	def, err := channel.NewDefinition("c", "movies", channel.ContentPool{}, schedule)
	if err != nil {
		t.Fatal(err)
	}
	playlist := Resolve(Inputs{Now: now, Channel: channel.Channel{ID: "id", Definition: def},
		Videos: []media.Video{video("v1", "a.mp4", 1800)}})
	if len(playlist.Slots) != 2 || !playlist.Slots[0].Start.Equal(at(t, "2026-11-09 23:00")) {
		t.Fatalf("monday must use the late template, tuesday the default: %+v", playlist.Slots)
	}
	if !playlist.Slots[1].Start.Equal(at(t, "2026-11-10 20:00")) {
		t.Fatalf("tuesday fallback slot: %v", playlist.Slots[1].Start)
	}
	// Tuesday has no mapping → default template (20:00) on both days of
	// the horizon.
	playlist = Resolve(Inputs{Now: now.AddDate(0, 0, 1), Channel: channel.Channel{ID: "id", Definition: def},
		Videos: []media.Video{video("v1", "a.mp4", 1800)}})
	if len(playlist.Slots) != 2 {
		t.Fatalf("tuesday/wednesday default fallback: %d slots", len(playlist.Slots))
	}
}

func TestResolveContentMatching(t *testing.T) {
	now := at(t, "2026-11-09 10:00")
	videos := []media.Video{
		video("v1", "cartoon.mp4", 1800, "cartoon"),
		video("7c3b8d5e-2a14-4b6e-9c1f-0d8e7f6a5b4c", "movie.mp4", 1800, "movie"),
		video("v3", "tagged-cartoon.mp4", 1800, "cartoon", "primetime"),
		video("v4", "unplayable.mp4", 0), // zero duration excluded
	}

	// Pool tags filter the library; slot tags filter within the pool.
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "11:00", Tags: []string{"primetime"}}),
		channel.ContentPool{Tags: []string{"cartoon"}},
		videos)
	today := todaySlots(playlist, now)
	if len(today) != 1 || len(today[0].Stream) != 2 {
		t.Fatalf("primetime cartoons: %+v", playlist.Slots)
	}
	for _, entry := range today[0].Stream {
		if entry.File != "tagged-cartoon.mp4" {
			t.Fatalf("only the primetime cartoon matches: %+v", today[0].Stream)
		}
	}

	// Pool by explicit video IDs.
	playlist = resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "10:30", Tags: []string{}}),
		channel.ContentPool{VideoIDs: []string{"7c3b8d5e-2a14-4b6e-9c1f-0d8e7f6a5b4c"}},
		videos)
	today = todaySlots(playlist, now)
	if len(today) != 1 || today[0].Stream[0].File != "movie.mp4" {
		t.Fatalf("video id pool: %+v", playlist.Slots)
	}

	// No matching content → the slot is skipped entirely.
	playlist = resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "10:00", End: "11:00", Tags: []string{"news"}}),
		channel.ContentPool{},
		videos)
	if len(playlist.Slots) != 0 {
		t.Fatalf("no matching content: %+v", playlist.Slots)
	}
}

func TestResolveEmptyScheduleRunsNothing(t *testing.T) {
	now := at(t, "2026-11-09 10:00")
	playlist := resolve(t, now, channel.Schedule{}, channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1800)})
	if len(playlist.Slots) != 0 {
		t.Fatalf("empty schedule: %+v", playlist.Slots)
	}
}

func TestHorizonCoversTomorrow(t *testing.T) {
	now := at(t, "2026-11-09 23:30")
	playlist := resolve(t, now,
		scheduleWith(t, channel.Slot{Start: "23:00", End: "23:59", Tags: []string{}}),
		channel.ContentPool{},
		[]media.Video{video("v1", "a.mp4", 1800)})
	// Both today's remaining slot and tomorrow's are included.
	if len(playlist.Slots) != 2 {
		t.Fatalf("horizon slots: %d", len(playlist.Slots))
	}
	if !playlist.Slots[1].Start.Equal(at(t, "2026-11-10 23:00")) {
		t.Fatalf("tomorrow slot: %v", playlist.Slots[1].Start)
	}
}

func TestPlaylistValidation(t *testing.T) {
	base := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	valid := Playlist{Slots: []Slot{{
		Start:  base,
		End:    base.Add(time.Hour),
		Stream: []Entry{{File: "a.mp4", Type: EntryContent, Duration: 3600}},
	}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid: %v", err)
	}

	invalid := []Playlist{
		{Slots: []Slot{{Start: base, End: base.Add(time.Hour)}}},       // empty stream
		{Slots: []Slot{{Start: base, End: base, Stream: []Entry{{}}}}}, // end before start
		{Slots: []Slot{{Start: base, End: base.Add(time.Hour), Stream: []Entry{{File: "a.mp4", Type: "weird", Duration: 1}}}}},
		{Slots: []Slot{{Start: base, End: base.Add(time.Hour), Stream: []Entry{{File: "a.mp4", Type: EntryContent, Duration: 1, StartOffset: "1:02:03"}}}}},
		{Slots: []Slot{{Start: base, End: base.Add(time.Hour), Stream: []Entry{{File: "a.mp4", Type: EntryContent, Duration: 1, EndOffset: "00:99:00"}}}}},
	}
	for i, p := range invalid {
		if err := p.Validate(); apperr.HTTPStatus(err) != 400 {
			t.Fatalf("invalid[%d] accepted: %v", i, err)
		}
	}
}

func TestTimecode(t *testing.T) {
	if got := Timecode(3661); got != "01:01:01" {
		t.Fatalf("timecode: %q", got)
	}
	if got := Timecode(59); got != "00:00:59" {
		t.Fatalf("timecode: %q", got)
	}
	if got := Timecode(3600); got != "01:00:00" {
		t.Fatalf("timecode: %q", got)
	}
}

func TestServiceResolveStoreAndShim(t *testing.T) {
	fixedNow := func() time.Time { return at(t, "2026-11-09 10:00") }
	fakeChannels := &fakeChannelSource{channels: map[string]*channel.Channel{}}
	def, err := channel.NewDefinition("c1", "movies", channel.ContentPool{}, scheduleWith(t,
		channel.Slot{Start: "10:00", End: "11:00", Tags: []string{}}))
	if err != nil {
		t.Fatal(err)
	}
	ch := &channel.Channel{ID: "ch-1", Definition: def, Status: channel.StatusCreated, CreatedAt: fixedNow()}
	fakeChannels.channels["ch-1"] = ch
	fakeVideos := &fakeVideoSource{videos: []media.Video{video("v1", "a.mp4", 1800)}}

	svc := NewService(fakeChannels, fakeVideos, fixedNow)
	ctx := context.Background()

	// Unknown channel → 404.
	if _, err := svc.Resolve(ctx, "missing"); apperr.HTTPStatus(err) != 404 {
		t.Fatalf("resolve unknown: %v", err)
	}

	// Stored is empty before resolving.
	stored, err := svc.Stored(ctx, "ch-1")
	if err != nil || len(stored.Slots) != 0 {
		t.Fatalf("stored before resolve: %v %+v", err, stored)
	}

	// Resolve fills the store.
	playlist, err := svc.Resolve(ctx, "ch-1")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(playlist.Slots) < 1 || playlist.Slots[0].Stream[0].File != "a.mp4" {
		t.Fatalf("playlist: %+v", playlist)
	}
	stored, err = svc.Stored(ctx, "ch-1")
	if err != nil || len(stored.Slots) != len(playlist.Slots) {
		t.Fatalf("stored after resolve: %v %+v", err, stored)
	}

	// The channel shim JSON: flat slots with timestamp/file/type.
	raw, err := svc.PlaylistFor(ctx, *ch)
	if err != nil {
		t.Fatalf("playlist-for: %v", err)
	}
	if !strings.Contains(raw, `"timestamp":"2026-11-09T10:00:00+01:00"`) ||
		!strings.Contains(raw, `"file":"a.mp4"`) ||
		!strings.Contains(raw, `"type":"content"`) {
		t.Fatalf("shim json: %s", raw)
	}
}

type fakeChannelSource struct{ channels map[string]*channel.Channel }

func (f *fakeChannelSource) FindByID(_ context.Context, id string) (*channel.Channel, error) {
	return f.channels[id], nil // nil = not found
}

type fakeVideoSource struct{ videos []media.Video }

func (f *fakeVideoSource) FindAll(_ context.Context, _ *media.Filter) ([]media.Video, error) {
	return f.videos, nil
}
