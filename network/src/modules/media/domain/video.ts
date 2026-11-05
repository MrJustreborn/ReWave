import { ValidationError, nonEmptyString, nonEmptyStringArray, nonNegativeNumber } from "../../../shared/index.js";
import type { VideoId } from "../../../shared/index.js";

export class AdBreak {
  private constructor(readonly position: number) {}

  static create(position: number): AdBreak {
    return new AdBreak(nonNegativeNumber(position, "adBreak.position"));
  }
}

export interface VideoCreation {
  readonly id: VideoId;
  readonly path: string;
  readonly title: string;
  readonly duration: number;
  readonly tags: readonly string[];
  readonly createdAt: Date;
}

export interface VideoSnapshot {
  readonly id: VideoId;
  readonly path: string;
  readonly title: string;
  readonly duration: number;
  readonly tags: readonly string[];
  readonly processed: boolean;
  readonly adBreaks: readonly number[];
  readonly createdAt: Date;
}

export class Video {
  private constructor(
    readonly id: VideoId,
    readonly path: string,
    readonly title: string,
    readonly duration: number,
    readonly tags: readonly string[],
    readonly processed: boolean,
    readonly adBreaks: readonly AdBreak[],
    readonly createdAt: Date,
  ) {}

  static create(creation: VideoCreation): Video {
    const path = nonEmptyString(creation.path, "video.path");
    const title = nonEmptyString(creation.title, "video.title");
    const duration = nonNegativeNumber(creation.duration, "video.duration");
    const tags = Object.freeze(nonEmptyStringArray(creation.tags, "video.tags"));
    return new Video(creation.id, path, title, duration, tags, false, [], creation.createdAt);
  }

  static reconstitute(snapshot: VideoSnapshot): Video {
    const path = nonEmptyString(snapshot.path, "video.path");
    const title = nonEmptyString(snapshot.title, "video.title");
    const duration = nonNegativeNumber(snapshot.duration, "video.duration");
    const tags = Object.freeze(nonEmptyStringArray(snapshot.tags, "video.tags"));
    const adBreaks = Object.freeze(snapshot.adBreaks.map((position) => AdBreak.create(position)));
    return new Video(snapshot.id, path, title, duration, tags, snapshot.processed, adBreaks, snapshot.createdAt);
  }

  withProcessedAdBreaks(positions: readonly number[]): Video {
    if (this.processed) {
      throw new ValidationError(`video "${this.path}" is already processed`);
    }
    const adBreaks = Object.freeze(positions.map((position) => AdBreak.create(position)));
    return new Video(this.id, this.path, this.title, this.duration, this.tags, true, adBreaks, this.createdAt);
  }
}
