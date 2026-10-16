# Network

The network is ReWave's master and manager — the only component that knows the
whole picture. UIs and services talk only to the network, the network talks to
the channels, and channels never talk back.

## Responsibilities

- **Channel lifecycle** — add/remove channels (one Docker container per
  channel), start/stop them, poll their status through their HTTP APIs.
- **Channel registry** — channel definitions with a type/category (movies,
  cartoons, series, …) and their schedule/playlist templates.
- **Media library** — SQLite database owned and written only by the network:
  videos, metadata, ad-break points. Never mounted into other containers.
- **Video ingestion** — when a new video is added to the pool: check whether it
  is already processed, run ad-break detection (scene detection; the
  prototype lives at `src/ads/adseeker.py`), store the result.
- **Watcher tracking** — smart on-demand channels for the tv-ui: a channel
  starts when someone wants to watch it and keeps running for a keep-alive
  window after the last viewer leaves (per-viewer heartbeats).
- **REST API** — the only public surface, for management-ui and tv-ui.

## REST API

Specified API-first: the authoritative contract is
[`openapi.yaml`](openapi.yaml) (OpenAPI 3.1). Paths are versioned under
`/v1/` — breaking changes only in a future `/v2/`, additive changes stay
in `/v1/`. All endpoints require OAuth2 (the network is a pure resource
server; login/IdP stays out of scope and is configured at deploy time)
with the scopes `channels:read`, `channels:write`, `videos:read`,
`videos:write` and `watch`.

## Media library (SQLite)

Single-writer database owned by the network. Rough sketch:

- `videos` — path, duration, title, metadata, `processed` flag
- `ad_breaks` — per video: detected positions (scene-change based)

Channels never read this database. The network resolves a channel's playlist
(video paths plus ad-break points) and pushes it to the channel via its API.

If ingestion turns out to be heavy (scene detection is CPU-hungry FFmpeg work),
it can be split into its own library/ingest container later — the same
treatment as the DVB muxer.

## Examples

`examples/` sketches what the network will produce — not consumed by any code
yet:

- `playlist.json` — a resolved playlist as pushed to a channel (slots with
  `start`/`end` and a nested `stream` array of preroll/content/ad entries)
- `schedule.json` — per-day schedule templates per channel

## Roadmap

- **DVB muxing** — coordinate a separate muxer container with hardware access
  (DVB-S2/DVB-C) that takes channel streams and feeds them to real TVs;
  channels shown there run 24/7.
- **Kodi / Jellyfin plugins** — expose ReWave as a TV source with channels.

## Status

Early stage: the REST API contract ([`openapi.yaml`](openapi.yaml)) is
implemented as a TypeScript skeleton — API types are generated from the spec
at build time (gitignored output), a Fastify server with OAuth2
resource-server auth (`AUTH_MODE=off` is the dev bypass; `jwks` mode requires
`AUTH_JWKS_URL`, `AUTH_ISSUER` and `AUTH_AUDIENCE`), SQLite migrations, a
Docker client module, and the ad-break detection ported from the former
Python prototype to `src/ingest/` (`npm run adseeker -- <video>`). All REST
handlers are `501` stubs — the manager logic is still to be implemented; this
README remains the spec.

Run it: `npm install && npm run dev` (listens on `:8080`), or
`docker compose up network`.
