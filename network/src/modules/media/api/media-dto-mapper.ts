import type { components } from "../../../generated/schema.js";
import { nonEmptyString, nonEmptyStringArray, ValidationError } from "../../../shared/index.js";
import type { AddVideoCommand } from "../application/media-service.js";
import type { Video } from "../domain/video.js";
import type { VideoFilter } from "../domain/video-repository.js";

type VideoDto = components["schemas"]["Video"];
type VideoListDto = components["schemas"]["VideoList"];
type NewVideoDto = components["schemas"]["NewVideo"];

export function videoToDto(video: Video): VideoDto {
  return {
    id: video.id,
    path: video.path,
    title: video.title,
    duration: video.duration,
    tags: [...video.tags],
    processed: video.processed,
    adBreaks: video.adBreaks.map((adBreak) => ({ position: adBreak.position })),
  };
}

export function videosToDto(videos: readonly Video[]): VideoListDto {
  return { videos: videos.map(videoToDto) };
}

export function newVideoDtoToCommand(dto: NewVideoDto): AddVideoCommand {
  return {
    path: nonEmptyString(dto.path, "path"),
    ...(dto.title !== undefined && { title: nonEmptyString(dto.title, "title") }),
    ...(dto.tags !== undefined && { tags: nonEmptyStringArray(dto.tags, "tags") }),
  };
}

export function processedQueryToFilter(value: string | undefined): VideoFilter | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === "true") {
    return { processed: true };
  }
  if (value === "false") {
    return { processed: false };
  }
  throw new ValidationError(`processed must be "true" or "false", got "${value}"`);
}
