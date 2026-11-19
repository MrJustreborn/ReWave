# AGENTS.md

## What this is

ReWave is a mono-repo for a self-hosted, Docker-based virtual TV network,
consisting of multiple subprojects. Each subproject has its own `README.md`
(the spec) and its own `AGENTS.md` with project-specific facts — read both
before working there.

| Path             | Purpose                                                              |
| ---------------- | -------------------------------------------------------------------- |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming (working Python prototype; the language is provisional — see its README) |
| `network/`       | Master & manager: channel lifecycle, media library, scheduling, REST API (Go, first complete version) |
| `management-ui/` | Web UI to manage the network (concept, no code yet)                   |
| `tv-ui/`         | Virtual TV web UI with on-demand channel watching (concept, no code yet) |
| `sample-media/`  | Sample media, gitignored via `*.mov` (fresh clones start empty)       |

## Architecture rules (repo-wide)

- Control flow is strictly one-directional: UIs and services → network →
  channels. A channel never talks to the network, and channels never talk to
  each other.
- The media library (SQLite) is owned and written only by the network.
  Channels receive resolved playlists; they never read the database.
- Keep a change inside its subproject where possible; cross-cutting changes
  should say why.

## Repo-wide facts

- The network has a real test suite: `go test ./...` from `network/` (plus
  `go vet ./...` and compiling — that is the whole quality gate). Elsewhere
  there are still no tests, linters, or CI.
- Python code in `channel/` is stdlib-only: no `requirements.txt`, no pip
  dependencies (the venv under `channel/venv` is untracked local clutter; the
  Docker image installs nothing). Python is a prototype choice, not a
  decision — the channel will be reworked once the network settles.
- The `network/` subproject is Go (see its AGENTS.md). Its API types and
  server interface are generated from `network/openapi.yaml` into
  `network/internal/httpapi/gen/` at build time — gitignored and always
  regenerated; never edit or commit that directory.
- `ffmpeg` and `ffprobe` are hard requirements. They are installed into the
  devcontainer via the `ffmpeg-apt-get` feature in
  `.devcontainer/devcontainer.json` and exist inside both Docker images
  (channel and network — ingestion runs ffmpeg in-process). Verify with
  `command -v ffmpeg` before blaming the code. Note: the devcontainer uses
  Ubuntu 24.04's ffmpeg (6.1.x) while the images use Debian's — a minor
  version skew that is acceptable for dev work.
- Docker: `docker compose up` builds `channel/` (tagged `rewave-channel`) and
  `network/`; channels use `network_mode: host` (host networking is required
  for multicast; Linux only). **Never** run docker build/container smoke
  tests from inside the devcontainer — docker-in-docker is unreliable there
  (broken credential helper); use the host instead.

## Channel gotchas

- `main.py` reads the playlist from the `PLAYLIST` env var; its default is
  relative to the file (`../../network/examples/playlist.json` → repo-root
  `network/examples/playlist.json`). That default points into the network
  project — dev sample data only, not an architectural dependency; the
  channel API will replace it. The Dockerfile copies only `src/`, so in the
  container the default resolves to `/network/examples/playlist.json`, which
  is neither copied nor mounted — compose's `channel-1` service cannot start
  as-is (the network-created channel containers set `PLAYLIST` explicitly,
  which works).
- Schema mismatch: `main()` sorts slots by `entry["timestamp"]` and reads
  slot-level `file`/`type`, but `network/examples/playlist.json` slots have
  `start`/`end` plus a nested `stream` array — running main.py against the
  example playlist raises `KeyError`. The network's scheduling shim
  (`scheduling.PlaylistFor`) serializes playlists into the flat
  `timestamp`/`file`/`type` shape main.py expects; the channel HTTP API
  (roadmap) will replace the shim.

## Network gotchas

- The network creates channel containers from the image named by
  `CHANNEL_IMAGE` (default `rewave-channel` — compose tags the channel
  service accordingly; build it first: `docker compose build channel-1`).
- The channel push is a shim: the resolved playlist goes into the
  container's `PLAYLIST` env var in the Python prototype's flat format.
- The media pool bind for channel containers comes from `MEDIA_HOST_PATH`
  (host-side path — inside the network's own Docker it is NOT
  `MEDIA_ROOT`); `MEDIA_ROOT` is only the network's in-container view.
- SQLite is pure Go (`modernc.org/sqlite`), pool capped at one connection:
  never query while a result set is open (see `media.Store.FindAll` for the
  two-phase pattern); wrap multi-statement writes in `db.WithTransaction`.
- Migrations are forward-only, embedded and checksum-validated: never edit
  an already-applied `network/internal/db/migrations/*.sql` — add a new
  numbered one.
- Schedule slot times (HH:MM) resolve in the server's local timezone; set
  `TZ` in Docker if that matters.
- `network/examples/` is not consumed by any code (it sketches a resolved
  playlist and schedule templates); the live formats are `openapi.yaml` and
  the channel shim in `internal/scheduling/service.go`.
- The former TypeScript implementation (skeleton, all handlers stubbed) is
  preserved on the branch `archive/ts-onion-network`.

## Conventions

- Everything in English — READMEs, AGENTS files, code comments, and log
  messages. Historical German comments in `channel/src/` have been
  translated; do not reintroduce them.
- Identifiers are English. Match the surrounding style of the file you edit.
- Commits may be made for logical steps, but the message must always be
  prefixed with `[AI]`, e.g. `[AI] Cleanup` or `[AI] Implement <specify
  here>`.
