import type { ChannelId } from "../../../shared/index.js";

export interface WatchState {
  readonly viewers: number;
  readonly keepAliveUntil: Date | undefined;
}

export interface ViewerRegistry {
  recordHeartbeat(channelId: ChannelId, viewerId: string, at: Date): WatchState;
}
