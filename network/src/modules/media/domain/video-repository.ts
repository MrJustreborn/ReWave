import type { VideoId } from "../../../shared/index.js";
import type { Video } from "./video.js";

export interface VideoFilter {
  readonly processed?: boolean;
}

export interface VideoRepository {
  findAll(filter?: VideoFilter): Video[];
  findById(id: VideoId): Video | undefined;
  findByPath(path: string): Video | undefined;
  add(video: Video): void;
  save(video: Video): void;
  remove(id: VideoId): boolean;
}
