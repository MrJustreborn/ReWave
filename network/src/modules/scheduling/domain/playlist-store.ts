import type { ChannelId } from "../../../shared/index.js";
import type { ResolvedPlaylist } from "./resolved-playlist.js";

export interface PlaylistStore {
  get(channelId: ChannelId): ResolvedPlaylist | undefined;
  put(channelId: ChannelId, playlist: ResolvedPlaylist): void;
}
