import type { FastifyInstance } from "fastify";
import { SchedulingService } from "./application/scheduling-service.js";
import { registerSchedulingRoutes } from "./api/playlist-routes.js";
import { InMemoryPlaylistStore } from "./infrastructure/in-memory-playlist-store.js";

export { ResolvedPlaylist, ResolvedSlot, StreamEntry } from "./domain/resolved-playlist.js";
export type { StreamEntryType, TimecodeOffset } from "./domain/resolved-playlist.js";
export { SchedulingService } from "./application/scheduling-service.js";

export interface SchedulingModule {
  readonly service: SchedulingService;
  registerRoutes(app: FastifyInstance): void;
}

export function createSchedulingModule(): SchedulingModule {
  const service = new SchedulingService(new InMemoryPlaylistStore());
  return {
    service,
    registerRoutes: (app) => registerSchedulingRoutes(app, service),
  };
}
