import {
  channelId,
  formatSqliteTimestamp,
  parseSqliteTimestamp,
  ValidationError,
} from "../../../../shared/index.js";
import { Channel, createChannelDefinition, isChannelStatus } from "../../domain/channel.js";
import { ContentPool } from "../../domain/content-pool.js";
import { Schedule } from "../../domain/schedule.js";

export interface ChannelRow {
  id: string;
  name: string;
  type: string;
  content_pool: string;
  schedule: string;
  status: string;
  created_at: string;
}

function parseJsonColumn(raw: string, column: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    throw new ValidationError(`column "${column}" does not contain valid JSON: "${raw}"`);
  }
}

export function channelRowToDomain(row: ChannelRow): Channel {
  if (!isChannelStatus(row.status)) {
    throw new ValidationError(`channels.status contains an invalid channel status "${row.status}"`);
  }
  return Channel.reconstitute({
    id: channelId(row.id),
    definition: createChannelDefinition(
      row.name,
      row.type,
      ContentPool.fromJson(parseJsonColumn(row.content_pool, "channels.content_pool")),
      Schedule.fromJson(parseJsonColumn(row.schedule, "channels.schedule")),
    ),
    status: row.status,
    createdAt: parseSqliteTimestamp(row.created_at, "channels.created_at"),
  });
}

export function channelToRow(channel: Channel): ChannelRow {
  return {
    id: channel.id,
    name: channel.name,
    type: channel.type,
    content_pool: JSON.stringify(channel.contentPool.toJson()),
    schedule: JSON.stringify(channel.schedule.toJson()),
    status: channel.status,
    created_at: formatSqliteTimestamp(channel.createdAt),
  };
}
