import type { FastifyInstance } from "fastify";
import type { SqliteDatabase } from "../../db/connection.js";
import type { Clock } from "../../shared/index.js";
import { ChannelService } from "./application/channel-service.js";
import { registerChannelRoutes } from "./api/channel-routes.js";
import { SqliteChannelRepository } from "./infrastructure/sqlite/sqlite-channel-repository.js";
import { DockerChannelRuntime } from "./infrastructure/docker/docker-channel-runtime.js";

export { Channel, createChannelDefinition, isChannelStatus } from "./domain/channel.js";
export type { ChannelDefinition, ChannelStatus } from "./domain/channel.js";
export { ContentPool } from "./domain/content-pool.js";
export { Schedule, Slot } from "./domain/schedule.js";
export type { ScheduleJson, SlotJson, Weekday, WallClockTime } from "./domain/schedule.js";
export { ChannelService } from "./application/channel-service.js";

export interface ChannelModuleDependencies {
  db: SqliteDatabase;
  dockerSocket: string;
  clock: Clock;
}

export interface ChannelModule {
  readonly service: ChannelService;
  registerRoutes(app: FastifyInstance): void;
}

export function createChannelModule(deps: ChannelModuleDependencies): ChannelModule {
  const service = new ChannelService(
    new SqliteChannelRepository(deps.db),
    new DockerChannelRuntime(deps.dockerSocket),
    deps.clock,
  );
  return {
    service,
    registerRoutes: (app) => registerChannelRoutes(app, service),
  };
}
