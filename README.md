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
| `network/`       | Master & manager: channel lifecycle, media library, REST API | working first version (Go) |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming         | working prototype (Python, provisional) |
| `management-ui/` | Web UI to manage the network (channels, media, schedules)  | concept            |
| `tv-ui/`         | Virtual TV web UI with smart on-demand channel start/stop   | concept            |
| `sample-media/`  | Sample media (gitignored via `*.mov`; fresh clones start empty) | data            |

The network is implemented in Go and functional end-to-end: channel registry
CRUD, Docker container lifecycle, media library with ad-break ingestion
(`network/internal/media/`, CLI: `go run ./cmd/adseeker <video>` from
`network/`), schedule-to-playlist resolution, on-demand watching with
keep-alive, and the full REST API of `network/openapi.yaml`. See
`network/README.md` for details and current limitations.

## Current status

Code that exists today: the channel prototype (`channel/src/main.py` runs a
playlist through FFmpeg and relays MPEG-TS over UDP multicast) and the
network manager (Go: channel registry and Docker lifecycle, SQLite media
library with ad-break ingestion, playlist resolution, viewer heartbeats —
all 13 REST operations of `network/openapi.yaml`).

```bash
python3 channel/src/main.py   # requires ffmpeg + ffprobe on PATH
cd network && make run       # network REST API on :8080
docker compose up            # Linux only — host networking for multicast
```

Caveats: the channel prototype's playlist defaults are broken in its own
container (details in the root `AGENTS.md`); the channel language is
provisional and will be reworked once the network settles.

## Roadmap

1. **Channel API** — HTTP API so the network can start/stop/configure a
   channel; reconcile the playlist schema (`network/examples/playlist.json`
   vs. code). Rework the channel (its Python is provisional).
2. **Network** — done to first complete version (lifecycle, media library,
   ingestion, scheduling, watching, REST API); next: channel API integration,
   ad-content model so playlists can use the detected ad breaks.
3. **Management UI** — channel management, media pool, playlist/schedule
   editing.
4. **TV UI + RTSP** — on-demand channels: watching a channel starts it, and a
   keep-alive window keeps it running after the last viewer leaves
   (network-side support is already implemented).
5. **Plugins** — Kodi and Jellyfin plugins exposing ReWave as a TV source with
   channels.
6. **DVB muxer** — separate container with hardware access (DVB-S2/DVB-C) that
   takes channel streams and feeds them to real TVs; channels shown there run
   24/7.

## Development

- Everything in English — READMEs and code comments alike.
- Before working in a subproject, read its `AGENTS.md` and `README.md`.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
