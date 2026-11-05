import { spawnSync } from "node:child_process";

export function probeDuration(videoPath: string): number {
  const result = spawnSync(
    "ffprobe",
    [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      videoPath,
    ],
    { encoding: "utf8" },
  );
  if (result.error !== undefined) {
    throw new Error(`ffprobe could not be started: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`ffprobe exited with status ${result.status}: ${result.stderr}`);
  }
  const duration = Number.parseFloat(result.stdout.trim());
  if (Number.isNaN(duration)) {
    throw new Error(`Could not parse duration from ffprobe output: "${result.stdout.trim()}"`);
  }
  return duration;
}
