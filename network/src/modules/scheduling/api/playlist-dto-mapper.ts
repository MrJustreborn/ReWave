import type { components } from "../../../generated/schema.js";
import type { ResolvedPlaylist } from "../domain/resolved-playlist.js";

type PlaylistDto = components["schemas"]["Playlist"];

export function playlistToDto(playlist: ResolvedPlaylist): PlaylistDto {
  return {
    slots: playlist.slots.map((slot) => ({
      start: slot.start.toISOString(),
      end: slot.end.toISOString(),
      stream: slot.stream.map((entry) => ({
        file: entry.file,
        type: entry.type,
        duration: entry.duration,
        ...(entry.startOffset !== undefined && { start: entry.startOffset }),
        ...(entry.endOffset !== undefined && { end: entry.endOffset }),
      })),
    })),
  };
}
