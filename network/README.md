# Network

The network is ReWave's master and manager — the only component that knows the
whole picture. UIs and services talk only to the network, the network talks to
the channels, and channels never talk back.

## Responsibilities

- **Channel lifecycle** — add/remove channels (one Docker container per
  channel), start/stop them through their runtime. A channel starts by
  resolving its schedule and pushing the playlist to the container.
- **Channel registry** — channel definitions with a type/category (movies,
  cartoons, series, …) and their schedule/content-pool templates.
- **Media library** — SQLite database owned and written only by the network:
  videos, metadata, ad-break points. Never mounted into other containers.
- **Video ingestion** — when a video is added: ffprobe its duration, then
  (asynchronously) run ad-break detection (FFmpeg scene-change based) and
  store the result.
- **Scheduling** — resolve a channel's day templates × content pool into a
  concrete, dated playlist; expose it via the API and push it to the channel.
- **Watcher tracking** — smart on-demand channels for the tv-ui: a channel
  starts when someone is watching and keeps running for a keep-alive window
  after the last viewer leaves (per-viewer heartbeats; a background reaper
  stops channels whose window lapses).
- **REST API** — the only public surface, for management-ui and tv-ui.

## Status

**Working manager, first complete version.** All 13 REST operations of the
API contract are implemented end-to-end (registry CRUD, lifecycle with real
Docker containers, media ingestion with ad-break detection, playlist
resolution, viewer heartbeats with keep-alive). Known v1 limits are listed
under [Limitations](#limitations) below.

## Stack

Go (1.27), deliberately few dependencies:

| Concern      | Choice                                                              |
| ------------ | ------------------------------------------------------------------- |
| HTTP         | Go 1.22+ `net/http.ServeMux` (no router framework)                  |
| SQLite       | `modernc.org/sqlite` (pure Go, no cgo → `CGO_ENABLED=0` binaries)   |
| Auth         | `lestrrat-go/jwx/v3` (OAuth2 resource server, JWKS caching)         |
| Docker       | official `github.com/docker/docker/client`                          |
| Codegen      | `oapi-codegen` v2.8+ (pinned as a `go tool`), OpenAPI 3.1           |
| Spec parsing | `github.com/getkin/kin-openapi` (test-only: scope-parity check)       |
| Logging      | stdlib `log/slog` (JSON to stdout)                                   |

## Layout

```
cmd/
├── network/        entrypoint: serve (default), migrate, migrate-status
└── adseeker/       ad-break detection CLI (same code path as ingestion)
internal/
├── config/         env + .env → typed configuration
├── db/             SQLite open + timestamp convention + migration runner
│   └── migrations/ embedded, forward-only, checksum-validated
├── apperr/         application error kinds → HTTP status mapping
├── uuid/           tiny UUID v4 helpers (no external dep)
├── auth/           OAuth2 resource server middleware + scope map
├── httpapi/        driving adapter: generated routes, DTO mappers, CORS
│   └── gen/        GENERATED from openapi.yaml (gitignored, always rebuilt)
├── channel/        registry domain, SQLite store, service, Docker runtime
├── media/          media library domain, store, service, ffmpeg detector
├── scheduling/    schedule × pool → resolved playlists (in-memory current)
└── watching/       viewer registry, heartbeat use case, keep-alive reaper
```

Pragmatic module boundaries (no DDD onion): each `internal/` package owns its
store and service; cross-package needs flow along `scheduling → channel,
media` and `watching → channel` (both through the `channel.Service` type or
small interfaces defined at the consumer). Go's `internal/` visibility and
package granularity enforce what the former TS architecture needed a
469-line custom checker for.

## REST API

Specified API-first: the authoritative contract is
[`openapi.yaml`](openapi.yaml) (OpenAPI 3.1). Paths are versioned under
`/v1/` — breaking changes only in a future `/v2/`, additive changes stay
in `/v1/`. All endpoints require OAuth2 (the network is a pure resource
server; login/IdP stays out of scope and is configured at deploy time) with
the scopes `channels:read`, `channels:write`, `videos:read`, `videos:write`
and `watch`.

- API types and the server interface are generated at build time
  (`go generate ./...` → `internal/httpapi/gen/`, gitignored).
- The per-operation scope requirements live in
  `internal/auth/scopes.go`; a test (`scopes_test.go`) loads `openapi.yaml`
  and fails the build if the map and the spec drift apart.

Replayable request files for manual testing live in
[`requests/`](requests/) (VS Code REST Client and JetBrains HTTP client;
see `requests/README.md`).

## Media library (SQLite)

Single-writer database owned by the network. Rough schema:

- `channels` — id, name (unique), type, content pool (JSON), schedule
  (JSON), status, created_at
- `videos` — id, path (unique), title, duration, tags (JSON), processed
  flag, created_at
- `ad_breaks` — per video: detected positions (scene-change based)

Channels never read this database. The network resolves a channel's
playlist (video paths plus ad-break points) and pushes it to the channel.

Migrations are **forward-only** with SHA-256 checksum validation: never
edit an already-applied `internal/db/migrations/*.sql` — add a new
`NNNN_description.sql` instead. They are embedded in the binary and applied
automatically at server startup.

## Local development

```bash
cd network/
cp .env.example .env    # optional — defaults work for local dev
make run                # generate + build + run, listens on :8080
```

- Environment: `.env` (gitignored) is loaded automatically — see
  [`.env.example`](.env.example) for all variables (`PORT`, `DB_PATH`,
  `AUTH_MODE`, `WATCH_KEEPALIVE_SECONDS`, `CHANNEL_IMAGE`, …). Real
  environment variables take precedence over `.env`.
- Database: `DB_PATH` (default `data/network.db`, gitignored) is created
  and migrated automatically; `./bin/network migrate-status` lists
  applied/pending migrations.
- Day-to-day commands:
  - `make build` / `go build ./...` — generate + compile
  - `go vet ./...`, `go test ./...` — static checks and the test suite
    (`go test ./...` is the repo's only "typecheck" for Go)
  - `go run ./cmd/adseeker <video> [--min-duration <seconds>]` —
    ad-break detection from the command line
- `ffmpeg`/`ffprobe` must be on `PATH` for ingestion and the adseeker CLI
  (in the devcontainer via the `ffmpeg-apt-get` feature; in the network
  container the image installs them).
- API without a frontend: open the files in `requests/` in your IDE.

## Limitations (v1)

- **Ad breaks don't shape playlists yet** — positions are detected and
  stored, but the resolver schedules whole videos (trimmed to slot ends);
  preroll/ad stream entries need an ad-content concept first.
- **Channel push is a shim** — the resolved playlist is serialized into the
  flat `timestamp`/`file`/`type` slot format today's channel prototype
  reads from its `PLAYLIST` env var. The planned channel HTTP API (see the
  root roadmap) replaces this.
- **No periodic re-resolution** — playlists are resolved when a channel
  starts; a running channel keeps its playlist until restarted.
- **Single binary network** — if ingestion gets heavy, split it into its
  own container (the same treatment as the planned DVB muxer).

## Roadmap

- **Channel API** — HTTP API on the channel side: start/stop, push a
  resolved playlist, status (replaces the PLAYLIST env shim).
- **DVB muxing** — coordinate a separate muxer container with hardware
  access (DVB-S2/DVB-C) that takes channel streams and feeds them to real
  TVs; channels shown there run 24/7.
- **Kodi / Jellyfin plugins** — expose ReWave as a TV source with channels.

The former TypeScript implementation is preserved on the git branch
`archive/ts-onion-network`.
