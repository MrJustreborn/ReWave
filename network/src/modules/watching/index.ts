import type { FastifyInstance } from "fastify";
import type { Clock } from "../../shared/index.js";
import { WatchingService } from "./application/watching-service.js";
import { registerWatchingRoutes } from "./api/watch-routes.js";
import { InMemoryViewerRegistry } from "./infrastructure/in-memory-viewer-registry.js";

export type { WatchState } from "./domain/watch-state.js";
export type { WatchResult } from "./application/watching-service.js";
export { WatchingService } from "./application/watching-service.js";

export interface WatchingModuleDependencies {
  clock: Clock;
  keepAliveSeconds: number;
}

export interface WatchingModule {
  readonly service: WatchingService;
  registerRoutes(app: FastifyInstance): void;
}

export function createWatchingModule(deps: WatchingModuleDependencies): WatchingModule {
  const service = new WatchingService(new InMemoryViewerRegistry(deps.keepAliveSeconds), deps.clock);
  return {
    service,
    registerRoutes: (app) => registerWatchingRoutes(app, service),
  };
}
