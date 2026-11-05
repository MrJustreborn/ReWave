import { findAds } from "./adseeker.js";
import type { AdBreakDetector } from "../../domain/ad-break-detector.js";

export class FfmpegAdBreakDetector implements AdBreakDetector {
  constructor(private readonly minDurationSeconds?: number) {}

  detect(videoPath: string): number[] {
    return findAds(
      videoPath,
      this.minDurationSeconds === undefined ? {} : { minDuration: this.minDurationSeconds },
    );
  }
}
