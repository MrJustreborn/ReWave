import type { FastifyInstance } from "fastify";
import type { components } from "../generated/schema.js";
import { notImplemented } from "./stubs.js";

interface VideoIdParams {
  videoId: string;
}

interface ListVideosQuerystring {
  processed?: boolean;
}

export function registerVideoRoutes(app: FastifyInstance): void {
  app.get<{ Querystring: ListVideosQuerystring }>(
    "/v1/videos",
    { config: { operationId: "listVideos" } },
    async (_request, reply) => notImplemented(reply, "listVideos"),
  );

  app.post<{ Body: components["schemas"]["NewVideo"] }>(
    "/v1/videos",
    { config: { operationId: "addVideo" } },
    async (_request, reply) => notImplemented(reply, "addVideo"),
  );

  app.get<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "getVideo" } },
    async (_request, reply) => notImplemented(reply, "getVideo"),
  );

  app.delete<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "deleteVideo" } },
    async (_request, reply) => notImplemented(reply, "deleteVideo"),
  );
}
