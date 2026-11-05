import type { SqliteDatabase } from "../../../../db/connection.js";
import type { VideoId } from "../../../../shared/index.js";
import type { Video } from "../../domain/video.js";
import type { VideoFilter, VideoRepository } from "../../domain/video-repository.js";
import { videoRowsToDomain, videoToRows, type AdBreakRow, type VideoRow } from "./video-row.js";

export class SqliteVideoRepository implements VideoRepository {
  constructor(private readonly db: SqliteDatabase) {}

  findAll(filter?: VideoFilter): Video[] {
    const rows: VideoRow[] =
      filter?.processed === undefined
        ? (this.db.prepare("SELECT * FROM videos ORDER BY created_at, path").all() as unknown as VideoRow[])
        : (this.db
            .prepare("SELECT * FROM videos WHERE processed = ? ORDER BY created_at, path")
            .all(filter.processed ? 1 : 0) as unknown as VideoRow[]);
    return rows.map((row) => videoRowsToDomain(row, this.breaksFor(row.id)));
  }

  findById(id: VideoId): Video | undefined {
    const row = this.db.prepare("SELECT * FROM videos WHERE id = ?").get(id) as VideoRow | undefined;
    return row === undefined ? undefined : videoRowsToDomain(row, this.breaksFor(row.id));
  }

  findByPath(path: string): Video | undefined {
    const row = this.db.prepare("SELECT * FROM videos WHERE path = ?").get(path) as VideoRow | undefined;
    return row === undefined ? undefined : videoRowsToDomain(row, this.breaksFor(row.id));
  }

  add(video: Video): void {
    const { video: row, breaks } = videoToRows(video);
    this.db
      .prepare(
        "INSERT INTO videos (id, path, title, duration, tags, processed, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(row.id, row.path, row.title, row.duration, row.tags, row.processed, row.created_at);
    this.insertBreaks(breaks);
  }

  save(video: Video): void {
    const { video: row, breaks } = videoToRows(video);
    this.db
      .prepare(
        "UPDATE videos SET path = ?, title = ?, duration = ?, tags = ?, processed = ?, created_at = ? WHERE id = ?",
      )
      .run(row.path, row.title, row.duration, row.tags, row.processed, row.created_at, row.id);
    this.db.prepare("DELETE FROM ad_breaks WHERE video_id = ?").run(row.id);
    this.insertBreaks(breaks);
  }

  remove(id: VideoId): boolean {
    return this.db.prepare("DELETE FROM videos WHERE id = ?").run(id).changes > 0;
  }

  private breaksFor(id: string): AdBreakRow[] {
    return this.db
      .prepare("SELECT video_id, position FROM ad_breaks WHERE video_id = ? ORDER BY position")
      .all(id) as unknown as AdBreakRow[];
  }

  private insertBreaks(breaks: readonly AdBreakRow[]): void {
    const insert = this.db.prepare("INSERT INTO ad_breaks (video_id, position) VALUES (?, ?)");
    for (const entry of breaks) {
      insert.run(entry.video_id, entry.position);
    }
  }
}
