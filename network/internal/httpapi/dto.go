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
