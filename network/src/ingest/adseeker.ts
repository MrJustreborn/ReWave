import { spawnSync } from "node:child_process";
import { probeDuration } from "./ffprobe.js";

export interface AdSeekerOptions {
  threshold?: number;
  window?: number;
  minDuration?: number;
  targetPositions?: number[];
}

const DEFAULT_THRESHOLD = 0.4;
const DEFAULT_WINDOW = 60;
const DEFAULT_MIN_DURATION = 3600;
const DEFAULT_TARGET_POSITIONS = [0.3, 0.5, 0.7];

function findSceneChanges(videoPath: string, start: number, length: number, threshold: number): number[] {
  const result = spawnSync(
    "ffmpeg",
    [
      "-hide_banner",
      "-loglevel",
      "info",
      "-ss",
      String(start),
      "-t",
      String(length),
      "-i",
      videoPath,
      "-vf",
      `select='gt(scene,${threshold})',showinfo`,
      "-f",
      "null",
      "-",
    ],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.error !== undefined) {
    throw new Error(`ffmpeg could not be started: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`ffmpeg exited with status ${result.status}: ${result.stderr}`);
  }
  const times: number[] = [];
  for (const line of result.stderr.split("\n")) {
    const match = /pts_time:([0-9.]+)/.exec(line);
    if (match !== null) {
      times.push(start + Number.parseFloat(match[1] ?? "0"));
    }
  }
  return times;
}

export function findAds(videoPath: string, options: AdSeekerOptions = {}): number[] {
  const threshold = options.threshold ?? DEFAULT_THRESHOLD;
  const window = options.window ?? DEFAULT_WINDOW;
  const minDuration = options.minDuration ?? DEFAULT_MIN_DURATION;
  const targetPositions = options.targetPositions ?? DEFAULT_TARGET_POSITIONS;

  const duration = probeDuration(videoPath);
  console.log(`Duration: ${(duration / 60).toFixed(1)} min`);

  if (duration < minDuration) {
    console.log(`Shorter than ${Math.round(minDuration / 60)} min -> no ads.`);
    return [];
  }

  const candidates: number[] = [];
  for (const position of targetPositions) {
    const center = duration * position;
    const start = Math.max(center - window, 0);
    const cuts = findSceneChanges(videoPath, start, window * 2, threshold);
    if (cuts.length > 0) {
      const best = cuts.reduce((a, b) => (Math.abs(b - center) < Math.abs(a - center) ? b : a));
      candidates.push(Math.round(best * 100) / 100);
      console.log(`Range ${(position * 100).toFixed(0)}% -> ${cuts.length} cuts, best spot: ${best.toFixed(2)}s`);
    } else {
      console.log(`Range ${(position * 100).toFixed(0)}%: no cuts found.`);
    }
  }
  return candidates;
}
