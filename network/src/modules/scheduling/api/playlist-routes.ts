import type { FastifyInstance } from "fastify";
import { channelId, type RouteConfig } from "../../../shared/index.js";
import type { SchedulingService } from "../application/scheduling-service.js";
import { playlistToDto } from "./playlist-dto-mapper.js";

interface ChannelIdParams {
  channelId: string;
}

export function registerSchedulingRoutes(app: FastifyInstance, schedulingService: SchedulingService): void {
  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/playlist",
    { config: { operationId: "getChannelPlaylist" } satisfies RouteConfig },
    async (request) => playlistToDto(await schedulingService.getChannelPlaylist(channelId(request.params.channelId))),
  );
}
