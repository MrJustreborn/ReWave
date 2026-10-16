import { findAds } from "./adseeker.js";

interface CliOptions {
  video?: string;
  minDuration?: number;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--min-duration") {
      const value = argv[i + 1];
      if (value === undefined) {
        throw new Error("--min-duration requires a value");
      }
      const parsed = Number.parseFloat(value);
      if (Number.isNaN(parsed) || parsed < 0) {
        throw new Error(`Invalid --min-duration value: "${value}"`);
      }
      options.minDuration = parsed;
      i++;
    } else if (options.video === undefined) {
      options.video = arg;
    } else {
      throw new Error(`Unexpected argument: ${arg}`);
    }
  }
  return options;
}

const options = parseArgs(process.argv.slice(2));
if (options.video === undefined) {
  console.error("Usage: npm run adseeker -- <video> [--min-duration <seconds>]");
  process.exit(1);
}

const positions = findAds(
  options.video,
  options.minDuration === undefined ? {} : { minDuration: options.minDuration },
);
if (positions.length > 0) {
  console.log("\nRecommended ad breaks:");
  for (const position of positions) {
    console.log(` -> ${position.toFixed(2)}s`);
  }
}
