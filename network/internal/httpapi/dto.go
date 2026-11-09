package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"

	"rewave/network/internal/apperr"
	"rewave/network/internal/channel"
	"rewave/network/internal/httpapi/gen"
	"rewave/network/internal/media"
	"rewave/network/internal/scheduling"
)

// definitionFromAPI converts and validates a ChannelDefinition DTO into
// the domain definition.
func definitionFromAPI(dto gen.ChannelDefinition) (channel.Definition, error) {
	pool := channel.ContentPool{}
	if dto.ContentPool != nil {
		if dto.ContentPool.VideoIds != nil {
			pool.VideoIDs = make([]string, len(*dto.ContentPool.VideoIds))
			for i, id := range *dto.ContentPool.VideoIds {
				pool.VideoIDs[i] = id.String()
			}
		}
		if dto.ContentPool.Tags != nil {
			pool.Tags = *dto.ContentPool.Tags
		}
	}
	schedule := channel.Schedule{}
	if dto.Schedule != nil {
		schedule.Templates = make(map[string][]channel.Slot, len(dto.Schedule.Templates))
		for name, slots := range dto.Schedule.Templates {
			converted := make([]channel.Slot, len(slots))
			for i, slot := range slots {
				converted[i] = channel.Slot{Start: slot.Start, End: slot.End, Tags: slot.Tags}
			}
			schedule.Templates[name] = converted
		}
		schedule.Days = dto.Schedule.Days
	}
	return channel.NewDefinition(dto.Name, dto.Type, pool, schedule)
}

// channelToAPI converts a domain channel into the Channel DTO.
func channelToAPI(ch channel.Channel) (gen.Channel, error) {
	id, err := uuid.Parse(ch.ID)
	if err != nil {
		return gen.Channel{}, fmt.Errorf("channel id %q is not a UUID", ch.ID)
	}

	pool := gen.ContentPool{}
	if len(ch.Definition.ContentPool.VideoIDs) > 0 {
		videoIDs := make([]uuid.UUID, len(ch.Definition.ContentPool.VideoIDs))
		for i, raw := range ch.Definition.ContentPool.VideoIDs {
			parsed, err := uuid.Parse(raw)
			if err != nil {
				return gen.Channel{}, fmt.Errorf("channel video id %q is not a UUID", raw)
			}
			videoIDs[i] = parsed
		}
		pool.VideoIds = &videoIDs
	}
	if len(ch.Definition.ContentPool.Tags) > 0 {
		tags := append([]string(nil), ch.Definition.ContentPool.Tags...)
		pool.Tags = &tags
	}

	templates := make(map[string][]gen.Slot, len(ch.Definition.Schedule.Templates))
	for name, slots := range ch.Definition.Schedule.Templates {
		converted := make([]gen.Slot, len(slots))
		for i, slot := range slots {
			converted[i] = gen.Slot{Start: slot.Start, End: slot.End, Tags: slot.Tags}
		}
		templates[name] = converted
	}

	return gen.Channel{
		Name:        ch.Definition.Name,
		Type:        ch.Definition.Type,
		ContentPool: &pool,
		Schedule:    &gen.Schedule{Templates: templates, Days: ch.Definition.Schedule.Days},
		Id:          id,
		Status:      gen.ChannelStatus(ch.Status),
		CreatedAt:   ch.CreatedAt,
	}, nil
}

// decodeJSON reads a JSON request body (capped at 1 MiB) into v.
func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := decoder.Decode(v); err != nil {
		return apperr.Validation("invalid JSON request body: " + err.Error())
	}
	return nil
}

// addVideoCommandFromAPI converts a NewVideo DTO into the add command.
func addVideoCommandFromAPI(dto gen.NewVideo) media.AddVideo {
	cmd := media.AddVideo{Path: dto.Path}
	if dto.Title != nil {
		cmd.Title = *dto.Title
	}
	if dto.Tags != nil {
		cmd.Tags = *dto.Tags
	}
	return cmd
}

// videoToAPI converts a domain video into the Video DTO.
func videoToAPI(v media.Video) (gen.Video, error) {
	id, err := uuid.Parse(v.ID)
	if err != nil {
		return gen.Video{}, fmt.Errorf("video id %q is not a UUID", v.ID)
	}
	breaks := make([]gen.AdBreak, len(v.AdBreaks))
	for i, br := range v.AdBreaks {
		breaks[i] = gen.AdBreak{Position: float32(br.Position)}
	}
	return gen.Video{
		Id:        id,
		Path:      v.Path,
		Title:     v.Title,
		Duration:  float32(v.Duration),
		Tags:      v.Tags,
		Processed: v.Processed,
		AdBreaks:  breaks,
	}, nil
}

// playlistToAPI converts a resolved playlist into the Playlist DTO.
func playlistToAPI(p scheduling.Playlist) gen.Playlist {
	slots := make([]gen.ResolvedSlot, len(p.Slots))
	for i, slot := range p.Slots {
		entries := make([]gen.StreamEntry, len(slot.Stream))
		for j, entry := range slot.Stream {
			entries[j] = gen.StreamEntry{
				File:     entry.File,
				Type:     gen.StreamEntryType(entry.Type),
				Duration: float32(entry.Duration),
			}
			if entry.StartOffset != "" {
				offset := entry.StartOffset
				entries[j].Start = &offset
			}
			if entry.EndOffset != "" {
				offset := entry.EndOffset
				entries[j].End = &offset
			}
		}
		slots[i] = gen.ResolvedSlot{Start: slot.Start, End: slot.End, Stream: entries}
	}
	return gen.Playlist{Slots: slots}
}
