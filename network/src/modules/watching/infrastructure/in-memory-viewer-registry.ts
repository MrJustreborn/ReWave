import type { ChannelId } from "../../../shared/index.js";
import type { ViewerRegistry, WatchState } from "../domain/watch-state.js";

export class InMemoryViewerRegistry implements ViewerRegistry {
  private readonly lastSeenByChannel = new Map<ChannelId, Map<string, Date>>();

  constructor(private readonly keepAliveSeconds: number) {}

  recordHeartbeat(channelId: ChannelId, viewerId: string, at: Date): WatchState {
    let viewers = this.lastSeenByChannel.get(channelId);
    if (viewers === undefined) {
      viewers = new Map();
      this.lastSeenByChannel.set(channelId, viewers);
    }
    viewers.set(viewerId, at);
    return this.watchState(viewers, at);
  }

  private watchState(viewers: Map<string, Date>, at: Date): WatchState {
    let newest: Date | undefined;
    for (const [viewerId, lastSeen] of viewers) {
      if (at.getTime() - lastSeen.getTime() > this.keepAliveSeconds * 1000) {
        viewers.delete(viewerId);
        continue;
      }
      newest = newest === undefined || lastSeen > newest ? lastSeen : newest;
    }
    return newest === undefined
      ? { viewers: 0, keepAliveUntil: undefined }
      : { viewers: viewers.size, keepAliveUntil: new Date(newest.getTime() + this.keepAliveSeconds * 1000) };
  }
}
