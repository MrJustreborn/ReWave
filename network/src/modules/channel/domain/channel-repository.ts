import type { ChannelId } from "../../../shared/index.js";
import type { Channel } from "./channel.js";

export interface ChannelRepository {
  findAll(): Channel[];
  findById(id: ChannelId): Channel | undefined;
  findByName(name: string): Channel | undefined;
  add(channel: Channel): void;
  save(channel: Channel): void;
  remove(id: ChannelId): boolean;
}
