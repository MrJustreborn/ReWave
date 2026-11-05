import { formatSqliteTimestamp, parseSqliteTimestamp, videoId } from "../../../../shared/index.js";
import { Video } from "../../domain/video.js";

export interface VideoRow {
  id: string;
  path: string;
  title: string;
  duration: number;
  tags: string;
  processed: number;
  created_at: string;
}

export interface AdBreakRow {
  video_id: string;
  position: number;
}

function parseJsonColumn(raw: string, column: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error(`column "${column}" does not contain valid JSON: "${raw}"`);
  }
}

export function videoRowsToDomain(video: VideoRow, breaks: readonly AdBreakRow[]): Video {
  return Video.reconstitute({
    id: videoId(video.id),
    path: video.path,
    title: video.title,
    duration: video.duration,
    tags: parseJsonColumn(video.tags, "videos.tags") as string[],
    processed: video.processed === 1,
    adBreaks: breaks.map((entry) => entry.position),
    createdAt: parseSqliteTimestamp(video.created_at, "videos.created_at"),
  });
}

export function videoToRows(video: Video): { video: VideoRow; breaks: AdBreakRow[] } {
  return {
    video: {
      id: video.id,
      path: video.path,
      title: video.title,
      duration: video.duration,
      tags: JSON.stringify([...video.tags]),
      processed: video.processed ? 1 : 0,
      created_at: formatSqliteTimestamp(video.createdAt),
    },
    breaks: video.adBreaks.map((adBreak) => ({ video_id: video.id, position: adBreak.position })),
  };
}
