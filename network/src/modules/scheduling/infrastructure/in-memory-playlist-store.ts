import type { ChannelId } from "../../../shared/index.js";
import type { PlaylistStore } from "../domain/playlist-store.js";
import type { ResolvedPlaylist } from "../domain/resolved-playlist.js";

export class InMemoryPlaylistStore implements PlaylistStore {
  private readonly playlists = new Map<ChannelId, ResolvedPlaylist>();

  get(channelId: ChannelId): ResolvedPlaylist | undefined {
    return this.playlists.get(channelId);
  }

  put(channelId: ChannelId, playlist: ResolvedPlaylist): void {
    this.playlists.set(channelId, playlist);
  }
}
