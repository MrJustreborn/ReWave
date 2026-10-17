import type { FastifyInstance } from "fastify";
import type { components } from "../generated/schema.js";
import type { RouteConfig } from "./route-config.js";
import { notImplemented } from "./stubs.js";

interface ChannelIdParams {
  channelId: string;
}

export function registerChannelRoutes(app: FastifyInstance): void {
  app.get("/v1/channels", { config: { operationId: "listChannels" } satisfies RouteConfig }, async (_request, reply) =>
    notImplemented(reply, "listChannels"),
  );

  app.post<{ Body: components["schemas"]["ChannelDefinition"] }>(
    "/v1/channels",
    { config: { operationId: "createChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "createChannel"),
  );

  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "getChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "getChannel"),
  );

  app.put<{ Params: ChannelIdParams; Body: components["schemas"]["ChannelDefinition"] }>(
    "/v1/channels/:channelId",
    { config: { operationId: "replaceChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "replaceChannel"),
  );

  app.delete<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "deleteChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "deleteChannel"),
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/start",
    { config: { operationId: "startChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "startChannel"),
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/stop",
    { config: { operationId: "stopChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "stopChannel"),
  );

  app.post<{ Params: ChannelIdParams; Body: components["schemas"]["WatchRequest"] }>(
    "/v1/channels/:channelId/watch",
    { config: { operationId: "watchChannel" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "watchChannel"),
  );

  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/playlist",
    { config: { operationId: "getChannelPlaylist" } satisfies RouteConfig },
    async (_request, reply) => notImplemented(reply, "getChannelPlaylist"),
  );
}
