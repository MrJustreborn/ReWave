package scheduling

import (
	"sort"
	"time"

	"rewave/network/internal/channel"
	"rewave/network/internal/media"
)

// horizonDays is how far ahead the resolver plans: the current day plus
// the next one (a channel started today has slots through tomorrow).
const horizonDays = 2

// minTailSeconds is the smallest remaining slot time that still gets
// filled with a trimmed video — tinier fragments are skipped.
const minTailSeconds = 30.0

// Inputs bundles everything the resolver needs. It is a pure function
// of these inputs (deterministic — same inputs, same playlist).
type Inputs struct {
	Now     time.Time
	Channel channel.Channel
	Videos  []media.Video
}

// Resolve fills the channel's schedule with content from its pool:
// every upcoming slot (within the horizon) becomes a resolved slot
// whose stream cycles through the matching videos, trimming the last
// entry to the slot's end. Slots in the past and slots without matching
// content are skipped.
func Resolve(in Inputs) Playlist {
	var slots []Slot
	for dayOffset := 0; dayOffset < horizonDays; dayOffset++ {
		day := atStartOfDay(in.Now).AddDate(0, 0, dayOffset)
		for _, slot := range slotsForDay(in.Channel.Definition.Schedule, day) {
			if !slot.End.After(in.Now) {
				continue // fully in the past
			}
			filled := fillSlot(slot, in.Channel.Definition.ContentPool, in.Videos)
			if filled != nil {
				slots = append(slots, *filled)
			}
		}
	}
	return Playlist{Slots: slots}
}

// slotsForDay expands a day into concrete slots using the weekday's
// template (falling back to the "default" template).
func slotsForDay(schedule channel.Schedule, day time.Time) []Slot {
	weekday := weekdayName(day)
	templateName, ok := schedule.Days[weekday]
	if !ok {
		templateName = "default"
	}
	template, ok := schedule.Templates[templateName]
	if !ok {
		return nil // channel runs nothing that day
	}

	sorted := append([]channel.Slot(nil), template...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })

	var slots []Slot
	for _, s := range sorted {
		start := atWallClock(day, s.Start)
		end := atWallClock(day, s.End)
		if !end.After(start) {
			// An end earlier than (or equal to) the start means the slot
			// crosses midnight.
			end = end.AddDate(0, 0, 1)
		}
		slots = append(slots, Slot{Start: start, End: end, tags: s.Tags})
	}
	return slots
}

// fillSlot fills one slot with content: the candidate videos are the
// channel pool intersected with the slot tags, cycled in path order;
// the last entry is trimmed to the slot end.
func fillSlot(slot Slot, pool channel.ContentPool, videos []media.Video) *Slot {
	candidates := make([]media.Video, 0, len(videos))
	for _, v := range videos {
		if v.Duration <= 0 {
			continue // unplayable and would loop forever
		}
		if !matchesPool(v, pool) || !matchesSlot(v, slot.tags) {
			continue
		}
		candidates = append(candidates, v)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Path < candidates[j].Path })

	var stream []Entry
	cursor := slot.Start
	for i := 0; ; i++ {
		remaining := slot.End.Sub(cursor).Seconds()
		if remaining < minTailSeconds {
			break
		}
		v := candidates[i%len(candidates)]
		entry := Entry{File: v.Path, Type: EntryContent, Duration: v.Duration}
		if entry.Duration > remaining {
			entry.Duration = remaining
			entry.EndOffset = Timecode(remaining) // play the first part
		}
		stream = append(stream, entry)
		cursor = cursor.Add(time.Duration(entry.Duration * float64(time.Second)))
	}
	if len(stream) == 0 {
		return nil
	}
	slot.Stream = stream
	slot.tags = nil
	return &slot
}

func matchesPool(v media.Video, pool channel.ContentPool) bool {
	if len(pool.VideoIDs) == 0 && len(pool.Tags) == 0 {
		return true // empty pool = entire library
	}
	for _, id := range pool.VideoIDs {
		if v.ID == id {
			return true
		}
	}
	return overlaps(v.Tags, pool.Tags)
}

func matchesSlot(v media.Video, slotTags []string) bool {
	if len(slotTags) == 0 {
		return true // empty slot tags = any video in the pool
	}
	return overlaps(v.Tags, slotTags)
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

func atStartOfDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}

func atWallClock(day time.Time, hhmm string) time.Time {
	hour := int(hhmm[0]-'0')*10 + int(hhmm[1]-'0')
	minute := int(hhmm[3]-'0')*10 + int(hhmm[4]-'0')
	year, month, date := day.Date()
	return time.Date(year, month, date, hour, minute, 0, 0, day.Location())
}

func weekdayName(t time.Time) string {
	names := []string{"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
	return names[int(t.Weekday())]
}
