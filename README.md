# ReWave

ReWave is a self-hosted, Docker-based virtual TV network. It recreates a typical
TV station: a **network** manages any number of **channels** (movies, cartoons,
series, …), every channel runs FFmpeg and streams a scheduled playlist, and web
UIs are used to manage the network and to watch it like a real TV.

## Architecture

Control flow is strictly one-directional — UIs and services talk only to the
network, the network talks to the channels, and a channel never talks back:

```
management-ui ─┐
               │ REST
tv-ui ─────────┤ REST (watch / heartbeat)
               ▼
        network (master container)
        · channel lifecycle (Docker)
        · media library (SQLite, single writer)
        · video ingestion + ad-break detection
               │ create / configure / poll
               ▼
        channel containers — one per channel
        (FFmpeg playlist streaming)
               │ RTSP (today: UDP multicast MPEG-TS)
               ▼
   viewers: tv-ui · Kodi/Jellyfin plugins (planned) · DVB muxer (planned)
```

**Control plane:** the network is the only active manager. It creates, starts,
stops and polls channel containers through their HTTP APIs. A channel knows
nothing about the network — not even its address. Channels are stateless,
replaceable players, which is what makes on-demand start/stop (tv-ui) and later
24/7 operation (DVB) easy.

**Data plane:** sharing data is not coupling. The media pool is a shared volume
mounted read-only into channels. The media library itself is an SQLite database
owned by the network (single writer — never mounted into other containers). The
network resolves playlists (video paths plus ad-break points) and pushes them to
the channels; channels never read the database.

## Projects

This is a mono-repo. Each subproject has its own `README.md` (the spec) and its
own `AGENTS.md` with project-specific facts.

| Path             | Purpose                                                     | Status             |
| ---------------- | ----------------------------------------------------------- | ------------------ |
| `network/`       | Master & manager: channel lifecycle, media library, REST API | concept + first code |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming         | working prototype |
| `management-ui/` | Web UI to manage the network (channels, media, schedules)  | concept            |
| `tv-ui/`         | Virtual TV web UI with smart on-demand channel start/stop   | concept            |
| `sample-media/`  | Sample media (gitignored via `*.mov`; fresh clones start empty) | data            |

The network's "first code" is the ad-break detection prototype
(`network/src/ads/adseeker.py`) plus example data (`network/examples/`) — the
manager itself is not implemented yet.

## Current status

Code that exists today: the channel prototype (`channel/src/main.py` runs a
playlist through FFmpeg and relays MPEG-TS over UDP multicast) and the
network's ad-break detection prototype (`network/src/ads/adseeker.py`, a
standalone helper).

```bash
python3 channel/src/main.py   # requires ffmpeg + ffprobe on PATH
docker compose up             # Linux only — host networking for multicast
```

Caveats: ffmpeg is not installed in the devcontainer (it only exists inside the
Docker image), and `docker compose up` cannot start the channel yet because the
image does not contain the playlist. Details and known issues live in the root
`AGENTS.md`.

## Roadmap

1. **Channel API** — HTTP API so the network can start/stop/configure a
   channel; reconcile the playlist schema (`network/examples/playlist.json` vs. code).
2. **Network** — manager container: channel lifecycle (Docker), media library
   (SQLite: videos, metadata, ad-break points), ingestion pipeline with
   ad-break detection (the `adseeker` prototype in `network/src/ads/`), REST
   API for the UIs.
3. **Management UI** — channel management, media pool, playlist/schedule
   editing.
4. **TV UI + RTSP** — on-demand channels: watching a channel starts it, and a
   keep-alive window keeps it running after the last viewer leaves.
5. **Plugins** — Kodi and Jellyfin plugins exposing ReWave as a TV source with
   channels.
6. **DVB muxer** — separate container with hardware access (DVB-S2/DVB-C) that
   takes channel streams and feeds them to real TVs; channels shown there run
   24/7.

## Development

- Everything in English — READMEs and code comments alike.
- Before working in a subproject, read its `AGENTS.md` and `README.md`.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
