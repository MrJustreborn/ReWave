import type { SqliteDatabase } from "../../../../db/connection.js";
import type { ChannelId } from "../../../../shared/index.js";
import type { Channel } from "../../domain/channel.js";
import type { ChannelRepository } from "../../domain/channel-repository.js";
import { channelRowToDomain, channelToRow, type ChannelRow } from "./channel-row.js";

export class SqliteChannelRepository implements ChannelRepository {
  constructor(private readonly db: SqliteDatabase) {}

  findAll(): Channel[] {
    const rows = this.db.prepare("SELECT * FROM channels ORDER BY name").all() as unknown as ChannelRow[];
    return rows.map(channelRowToDomain);
  }

  findById(id: ChannelId): Channel | undefined {
    const row = this.db.prepare("SELECT * FROM channels WHERE id = ?").get(id) as ChannelRow | undefined;
    return row === undefined ? undefined : channelRowToDomain(row);
  }

  findByName(name: string): Channel | undefined {
    const row = this.db.prepare("SELECT * FROM channels WHERE name = ?").get(name) as ChannelRow | undefined;
    return row === undefined ? undefined : channelRowToDomain(row);
  }

  add(channel: Channel): void {
    const row = channelToRow(channel);
    this.db
      .prepare(
        "INSERT INTO channels (id, name, type, content_pool, schedule, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(row.id, row.name, row.type, row.content_pool, row.schedule, row.status, row.created_at);
  }

  save(channel: Channel): void {
    const row = channelToRow(channel);
    this.db
      .prepare("UPDATE channels SET name = ?, type = ?, content_pool = ?, schedule = ?, status = ? WHERE id = ?")
      .run(row.name, row.type, row.content_pool, row.schedule, row.status, row.id);
  }

  remove(id: ChannelId): boolean {
    return this.db.prepare("DELETE FROM channels WHERE id = ?").run(id).changes > 0;
  }
}
