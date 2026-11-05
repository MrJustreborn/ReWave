import type { FastifyInstance } from "fastify";
import type { components } from "../../../generated/schema.js";
import { channelId, type RouteConfig } from "../../../shared/index.js";
import type { ChannelService } from "../application/channel-service.js";
import { channelDefinitionDtoToDefinition, channelToDto, channelsToDto } from "./channel-dto-mapper.js";

interface ChannelIdParams {
  channelId: string;
}

type ChannelDefinitionDto = components["schemas"]["ChannelDefinition"];

export function registerChannelRoutes(app: FastifyInstance, channelService: ChannelService): void {
  app.get(
    "/v1/channels",
    { config: { operationId: "listChannels" } satisfies RouteConfig },
    async () => channelsToDto(await channelService.listChannels()),
  );

  app.post<{ Body: ChannelDefinitionDto }>(
    "/v1/channels",
    { config: { operationId: "createChannel" } satisfies RouteConfig },
    async (request, reply) => {
      const channel = await channelService.createChannel(channelDefinitionDtoToDefinition(request.body));
      reply.code(201).header("Location", `/v1/channels/${channel.id}`);
      return channelToDto(channel);
    },
  );

  app.get<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "getChannel" } satisfies RouteConfig },
    async (request) => channelToDto(await channelService.getChannel(channelId(request.params.channelId))),
  );

  app.put<{ Params: ChannelIdParams; Body: ChannelDefinitionDto }>(
    "/v1/channels/:channelId",
    { config: { operationId: "replaceChannel" } satisfies RouteConfig },
    async (request) =>
      channelToDto(
        await channelService.replaceChannel(
          channelId(request.params.channelId),
          channelDefinitionDtoToDefinition(request.body),
        ),
      ),
  );

  app.delete<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId",
    { config: { operationId: "deleteChannel" } satisfies RouteConfig },
    async (request, reply) => {
      await channelService.deleteChannel(channelId(request.params.channelId));
      reply.code(204);
    },
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/start",
    { config: { operationId: "startChannel" } satisfies RouteConfig },
    async (request) => channelToDto(await channelService.startChannel(channelId(request.params.channelId))),
  );

  app.post<{ Params: ChannelIdParams }>(
    "/v1/channels/:channelId/stop",
    { config: { operationId: "stopChannel" } satisfies RouteConfig },
    async (request) => channelToDto(await channelService.stopChannel(channelId(request.params.channelId))),
  );
}
