import type { Channel } from "./channel.js";

export interface ChannelRuntime {
  start(channel: Channel): Promise<void>;
  stop(channel: Channel): Promise<void>;
}
