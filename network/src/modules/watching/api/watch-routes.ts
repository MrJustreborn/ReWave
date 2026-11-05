import type { FastifyInstance } from "fastify";
import type { components } from "../../../generated/schema.js";
import { channelId, type RouteConfig } from "../../../shared/index.js";
import type { WatchingService } from "../application/watching-service.js";
import { watchRequestToViewerId, watchResultToDto } from "./watch-dto-mapper.js";

interface ChannelIdParams {
  channelId: string;
}

type WatchRequestDto = components["schemas"]["WatchRequest"];

export function registerWatchingRoutes(app: FastifyInstance, watchingService: WatchingService): void {
  app.post<{ Params: ChannelIdParams; Body: WatchRequestDto }>(
    "/v1/channels/:channelId/watch",
    { config: { operationId: "watchChannel" } satisfies RouteConfig },
    async (request) =>
      watchResultToDto(
        await watchingService.recordHeartbeat(channelId(request.params.channelId), watchRequestToViewerId(request.body)),
      ),
  );
}
