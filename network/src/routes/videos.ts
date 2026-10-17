import type { FastifyInstance } from "fastify";
import type { components } from "../generated/schema.js";
import type { RouteConfig } from "./route-config.js";
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
    { config: { operationId: "listVideos" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "listVideos"),
  );

  app.post<{ Body: components["schemas"]["NewVideo"] }>(
    "/v1/videos",
    { config: { operationId: "addVideo" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "addVideo"),
  );

  app.get<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "getVideo" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "getVideo"),
  );

  app.delete<{ Params: VideoIdParams }>(
    "/v1/videos/:videoId",
    { config: { operationId: "deleteVideo" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "deleteVideo"),
  );
}
