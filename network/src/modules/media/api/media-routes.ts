import type { FastifyInstance } from "fastify";
import type { components } from "../../../generated/schema.js";
import { videoId, type RouteConfig } from "../../../shared/index.js";
import type { MediaService } from "../application/media-service.js";
import { newVideoDtoToCommand, processedQueryToFilter, videoToDto, videosToDto } from "./media-dto-mapper.js";

interface VideoIdParams {
  videoId: string;
}

type NewVideoDto = components["schemas"]["NewVideo"];

export function registerMediaRoutes(app: FastifyInstance, mediaService: MediaService): void {
  app.get<{ Querystring: { processed?: string } }>(
    "/v1/videos",
    { config: { operationId: "listVideos" } satisfies RouteConfig },
    async (request) => videosToDto(await mediaService.listVideos(processedQueryToFilter(request.query.processed))),
  );

  app.post<{ Body: NewVideoDto }>(
    "/v1/videos",
    { config: { operationId: "addVideo" } satisfies RouteConfig },
    async (request, reply) => {
      const video = await mediaService.addVideo(newVideoDtoToCommand(request.body));
      reply.code(201).header("Location", `/v1/videos/${video.id}`);
      return videoToDto(video);
    },
  );

  app.get<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "getVideo" } satisfies RouteConfig },
    async (request) => videoToDto(await mediaService.getVideo(videoId(request.params.videoId))),
  );

  app.delete<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "deleteVideo" } satisfies RouteConfig },
    async (request, reply) => {
      await mediaService.deleteVideo(videoId(request.params.videoId));
      reply.code(204);
    },
  );
}
