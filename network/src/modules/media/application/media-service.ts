import { NotImplementedError } from "../../../shared/index.js";
import type { VideoId } from "../../../shared/index.js";
import type { Video } from "../domain/video.js";
import type { VideoFilter, VideoRepository } from "../domain/video-repository.js";
import type { AdBreakDetector } from "../domain/ad-break-detector.js";

export interface AddVideoCommand {
  readonly path: string;
  readonly title?: string;
  readonly tags?: string[];
}

export class MediaService {
  constructor(
    private readonly videos: VideoRepository,
    private readonly adBreakDetector: AdBreakDetector,
  ) {}

  async listVideos(filter?: VideoFilter): Promise<Video[]> {
    throw new NotImplementedError("listVideos is not implemented yet");
  }

  async addVideo(command: AddVideoCommand): Promise<Video> {
    throw new NotImplementedError("addVideo is not implemented yet");
  }

  async getVideo(id: VideoId): Promise<Video> {
    throw new NotImplementedError("getVideo is not implemented yet");
  }

  async deleteVideo(id: VideoId): Promise<void> {
    throw new NotImplementedError("deleteVideo is not implemented yet");
  }
}
