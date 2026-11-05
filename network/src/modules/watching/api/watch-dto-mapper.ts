import type { components } from "../../../generated/schema.js";
import { nonEmptyString } from "../../../shared/index.js";
import type { WatchResult } from "../application/watching-service.js";

type WatchRequestDto = components["schemas"]["WatchRequest"];
type WatchResponseDto = components["schemas"]["WatchResponse"];

export function watchRequestToViewerId(dto: WatchRequestDto): string {
  return nonEmptyString(dto.viewerId, "viewerId");
}

export function watchResultToDto(result: WatchResult): WatchResponseDto {
  return {
    viewers: result.viewers,
    status: result.status,
    ...(result.keepAliveUntil !== undefined && { keepAliveUntil: result.keepAliveUntil.toISOString() }),
  };
}
