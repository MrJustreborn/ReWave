import { ValidationError, nonEmptyString, nonEmptyStringArray, videoId } from "../../../shared/index.js";
import type { VideoId } from "../../../shared/index.js";

export interface ContentPoolJson {
  videoIds?: string[];
  tags?: string[];
}

export class ContentPool {
  private constructor(
    readonly videoIds: readonly VideoId[],
    readonly tags: readonly string[],
  ) {}

  static empty(): ContentPool {
    return new ContentPool([], []);
  }

  static fromJson(raw: unknown): ContentPool {
    if (raw === undefined || raw === null) {
      return ContentPool.empty();
    }
    if (typeof raw !== "object" || Array.isArray(raw)) {
      throw new ValidationError("contentPool must be an object");
    }
    const record = raw as Record<string, unknown>;
    const videoIds =
      record["videoIds"] === undefined
        ? []
        : (record["videoIds"] as unknown[]).map((entry) =>
            videoId(nonEmptyString(entry, "contentPool.videoIds entries")),
          );
    const tags = record["tags"] === undefined ? [] : nonEmptyStringArray(record["tags"], "contentPool.tags");
    return new ContentPool(Object.freeze(videoIds), Object.freeze(tags));
  }

  toJson(): ContentPoolJson {
    return { videoIds: [...this.videoIds], tags: [...this.tags] };
  }
}
