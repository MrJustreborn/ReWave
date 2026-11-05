import { NotImplementedError } from "../../../shared/index.js";
import type { ChannelId } from "../../../shared/index.js";
import type { PlaylistStore } from "../domain/playlist-store.js";
import type { ResolvedPlaylist } from "../domain/resolved-playlist.js";

export class SchedulingService {
  constructor(private readonly playlists: PlaylistStore) {}

  async resolveChannelPlaylist(id: ChannelId): Promise<ResolvedPlaylist> {
    throw new NotImplementedError("resolveChannelPlaylist is not implemented yet");
  }

  async getChannelPlaylist(id: ChannelId): Promise<ResolvedPlaylist> {
    throw new NotImplementedError("getChannelPlaylist is not implemented yet");
  }
}
