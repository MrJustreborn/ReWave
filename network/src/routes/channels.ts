import type { FastifyInstance } from "fastify";
import type { components } from "../generated/schema.js";
import { notImplemented } from "./stubs.js";

interface ChannelIdParams {
  channelId: string;
}

export function registerChannelRoutes(app: FastifyInstance): void {
  app.get("/v1/channels", { config: { operationId: "listChannels" } }, async (_request, reply) =>
    notImplemented(reply, "listChannels"),
  );

  app.post<{ Body: components["schemas"]["ChannelDefinition"] }>(
    "/v1/channels",
    { config: { operationId: "createChannel" } },
    async (_request, reply) => notImplemented(reply, "createChannel"),
  );

  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "getChannel" } },
    async (_request, reply) => notImplemented(reply, "getChannel"),
  );

  app.put<{ Params: ChannelIdParams; Body: components["schemas"]["ChannelDefinition"] }>(
    "/v1/channels/:channelId",
    { config: { operationId: "replaceChannel" } },
    async (_request, reply) => notImplemented(reply, "replaceChannel"),
  );

  app.delete<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "deleteChannel" } },
    async (_request, reply) => notImplemented(reply, "deleteChannel"),
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/start",
    { config: { operationId: "startChannel" } },
    async (_request, reply) => notImplemented(reply, "startChannel"),
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/stop",
    { config: { operationId: "stopChannel" } },
    async (_request, reply) => notImplemented(reply, "stopChannel"),
  );

  app.post<{ Params: ChannelIdParams; Body: components["schemas"]["WatchRequest"] }>(
    "/v1/channels/:channelId/watch",
    { config: { operationId: "watchChannel" } },
    async (_request, reply) => notImplemented(reply, "watchChannel"),
  );

  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/playlist",
    { config: { operationId: "getChannelPlaylist" } },
    async (_request, reply) => notImplemented(reply, "getChannelPlaylist"),
  );
}
