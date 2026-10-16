# Channel

A ReWave channel is one TV channel of the network — with a type such as
*movies* or *cartoons*. It is a stateless, replaceable player: it receives a
resolved playlist, reads media from the shared media-pool volume, and streams
it with FFmpeg.

A channel never talks to the network — it does not even know its address. All
lifecycle control comes in through the channel's own (planned) HTTP API, and
the media library database is off-limits here: the network pushes resolved
playlists (video paths plus ad-break points) to the channel.

## How it works today

`src/main.py` is a working prototype:

1. Reads a playlist (path from the `PLAYLIST` env var; default → repo-root
   `network/examples/playlist.json`).
2. For each entry: starts FFmpeg with `-re` (real-time), scales and pads to
   1920×1080, encodes H.264 + AC-3 into MPEG-TS on a named pipe
   (`/tmp/pipe_a` / `/tmp/pipe_b`, alternating between entries).
3. A relay thread reads 1316-byte chunks from the active pipe and sends them
   via UDP to the multicast target.

Late starts are handled by seeking into the file; entries that already
finished are skipped.

Ad-break detection is not channel work — it is ingestion work (ad-break points
are per-video library data, not per-channel). The prototype lives in the
network project: `../network/src/ads/adseeker.py` (standalone script,
CWD-relative paths) uses FFmpeg scene detection to find good ad-break points
in long videos.

## Configuration

Environment variables:

| Variable           | Default                                 | Purpose                                  |
| ------------------ | --------------------------------------- | ---------------------------------------- |
| `TARGET_MULTICAST` | `239.100.0.1`                           | UDP multicast target address            |
| `TARGET_PORT`      | `1234`                                  | UDP multicast target port               |
| `PLAYLIST`         | `../../network/examples/playlist.json` | Playlist path, relative to `src/main.py` |

The `PLAYLIST` default points into `../network/examples/` — dev sample data
from the network project, not an architectural dependency.

## Planned

- **HTTP API for the network** — start/stop, push a resolved playlist, status
  (now playing, health).
- **RTSP server output** — so the tv-ui and later plugins can watch the
  channel.
- **Playlist schema reconciliation** — `main()` expects slot-level
  `timestamp`/`file`/`type`, but `network/examples/playlist.json` uses
  `start`/`end` with
  a nested `stream` array.

## Running

Requires `ffmpeg` and `ffprobe` on PATH. They are **not installed in the
devcontainer** — they only exist inside the Docker image (`Dockerfile`).

```bash
python3 src/main.py   # run directly (needs local ffmpeg)
docker compose up    # from the repo root (Linux, host networking)
```

## Gotchas

- In the container the default playlist path resolves to
  `/network/examples/playlist.json`, which is neither copied nor mounted —
  compose currently cannot start the channel successfully. Mounting a
  playlist and setting `PLAYLIST` in compose would fix it.
- Code is stdlib-only; there are no pip dependencies.
