import { NotImplementedError } from "../../../shared/index.js";
import type { ChannelId, Clock } from "../../../shared/index.js";
import type { ChannelStatus } from "../../channel/index.js";
import type { ViewerRegistry, WatchState } from "../domain/watch-state.js";

export interface WatchResult extends WatchState {
  readonly status: ChannelStatus;
}

export class WatchingService {
  constructor(
    private readonly registry: ViewerRegistry,
    private readonly clock: Clock,
  ) {}

  async recordHeartbeat(channelId: ChannelId, viewerId: string): Promise<WatchResult> {
    throw new NotImplementedError("recordHeartbeat is not implemented yet");
  }
}
