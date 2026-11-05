import type { FastifyInstance } from "fastify";
import type { SqliteDatabase } from "../../db/connection.js";
import { MediaService } from "./application/media-service.js";
import { registerMediaRoutes } from "./api/media-routes.js";
import { FfmpegAdBreakDetector } from "./infrastructure/ffmpeg/ffmpeg-ad-break-detector.js";
import { SqliteVideoRepository } from "./infrastructure/sqlite/sqlite-video-repository.js";

export { Video, AdBreak } from "./domain/video.js";
export type { VideoId } from "../../shared/index.js";
export { MediaService } from "./application/media-service.js";
export type { AddVideoCommand } from "./application/media-service.js";
export type { VideoFilter } from "./domain/video-repository.js";

export interface MediaModuleDependencies {
  db: SqliteDatabase;
}

export interface MediaModule {
  readonly service: MediaService;
  registerRoutes(app: FastifyInstance): void;
}

export function createMediaModule(deps: MediaModuleDependencies): MediaModule {
  const service = new MediaService(new SqliteVideoRepository(deps.db), new FfmpegAdBreakDetector());
  return {
    service,
    registerRoutes: (app) => registerMediaRoutes(app, service),
  };
}
