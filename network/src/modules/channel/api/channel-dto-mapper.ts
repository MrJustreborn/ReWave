import type { components } from "../../../generated/schema.js";
import { nonEmptyString } from "../../../shared/index.js";
import { createChannelDefinition, type Channel, type ChannelDefinition } from "../domain/channel.js";
import { ContentPool } from "../domain/content-pool.js";
import { Schedule } from "../domain/schedule.js";

type ChannelDto = components["schemas"]["Channel"];
type ChannelDefinitionDto = components["schemas"]["ChannelDefinition"];
type ChannelListDto = components["schemas"]["ChannelList"];
type ContentPoolDto = components["schemas"]["ContentPool"];

export function channelDefinitionDtoToDefinition(dto: ChannelDefinitionDto): ChannelDefinition {
  return createChannelDefinition(
    nonEmptyString(dto.name, "name"),
    nonEmptyString(dto.type, "type"),
    dto.contentPool === undefined ? ContentPool.empty() : ContentPool.fromJson(dto.contentPool),
    dto.schedule === undefined ? Schedule.empty() : Schedule.fromJson(dto.schedule),
  );
}

export function channelToDto(channel: Channel): ChannelDto {
  return {
    name: channel.name,
    type: channel.type,
    contentPool: contentPoolToDto(channel.contentPool),
    schedule: channel.schedule.toJson(),
    id: channel.id,
    status: channel.status,
    createdAt: channel.createdAt.toISOString(),
  };
}

export function channelsToDto(channels: readonly Channel[]): ChannelListDto {
  return { channels: channels.map(channelToDto) };
}

function contentPoolToDto(pool: ContentPool): ContentPoolDto {
  return {
    ...(pool.videoIds.length > 0 && { videoIds: [...pool.videoIds] }),
    ...(pool.tags.length > 0 && { tags: [...pool.tags] }),
  };
}
