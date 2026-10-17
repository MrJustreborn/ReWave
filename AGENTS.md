# AGENTS.md

## What this is

ReWave is a mono-repo for a self-hosted, Docker-based virtual TV network,
consisting of multiple subprojects. Each subproject has its own `README.md`
(the spec) and its own `AGENTS.md` with project-specific facts — read both
before working there.

| Path             | Purpose                                                              |
| ---------------- | -------------------------------------------------------------------- |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming (prototype)        |
| `network/`       | Master & manager: channel lifecycle, media library, REST API (TypeScript skeleton; handlers stubbed) |
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

- There is no test suite, linter, or CI anywhere in this repo; don't go
  looking for test commands. The only build/typecheck entry points are the
  npm scripts in `network/` (`npm run typecheck`, `npm run build`).
- Python code in `channel/` is stdlib-only: no `requirements.txt`, no pip
  dependencies (the venv under `channel/venv` is untracked local clutter; the
  Docker image installs nothing).
- The `network/` subproject is TypeScript on Node.js (npm; see its AGENTS.md).
  Its API types are generated from `network/openapi.yaml` into
  `network/src/generated/` at install/build time — gitignored and always
  regenerated; never edit or commit that directory.
- `ffmpeg` and `ffprobe` are hard requirements. They exist inside the Docker
  image (`channel/Dockerfile`) and are installed into the devcontainer via the
  `ffmpeg-apt-get` feature in `.devcontainer/devcontainer.json` (takes effect
  after a devcontainer rebuild; for an already-running container,
  `sudo apt-get update && sudo apt-get install -y ffmpeg` works). Verify with
  `command -v ffmpeg` before blaming the code. Note: the devcontainer uses
  Ubuntu 24.04's ffmpeg (6.1.x) while the channel image uses Debian's — a minor
  version skew that is acceptable for dev work.
- Docker: `docker compose up` builds `channel/` with `network_mode: host`
  (host networking is required for multicast; Linux only).

## Channel gotchas

- `main.py` reads the playlist from the `PLAYLIST` env var; its default is
  relative to the file (`../../network/examples/playlist.json` → repo-root
  `network/examples/playlist.json`). That default points into the network
  project — dev sample data only, not an architectural dependency; the
  channel API will replace it. The Dockerfile copies only `src/`, so in the
  container the default resolves to `/network/examples/playlist.json`, which
  is neither copied nor mounted — compose currently cannot start the channel
  successfully (mounting a playlist and setting `PLAYLIST` in compose would
  fix it).
- Schema mismatch: `main()` sorts slots by `entry["timestamp"]` and reads
  slot-level `file`/`type`, but `network/examples/playlist.json` slots have
  `start`/`end` plus a nested `stream` array — running main.py against the
  example playlist raises `KeyError`. Expect schema-reconciliation work
  rather than "fixing" one side blindly.

## Network gotchas

- `network/` is a TypeScript skeleton: all REST handlers are `501` stubs.
  API types are generated from `openapi.yaml` into `src/generated/` during
  install/build (gitignored, always regenerated); the `predev`/`prebuild`/
  `typecheck` scripts run the generation themselves, so nothing depends on
  `postinstall` running (npm's install-scripts policy may skip it).
- Ad-break detection is `network/src/ingest/adseeker.ts` (ported from the
  former Python prototype; output parity was verified before deleting it).
  Run it from `network/` via
  `npm run adseeker -- <video> [--min-duration <seconds>]`.
- SQLite is Node's built-in `node:sqlite` (not better-sqlite3 — avoids
  native install scripts; hence the `>=24` engine pin).
- Migrations are forward-only with checksum validation: never edit an
  already-applied `network/src/db/migrations/` file — add a new numbered one.
- `network/examples/` is not consumed by any code yet (it sketches a resolved
  playlist and per-day schedule templates for the future manager).

## Conventions

- Everything in English — READMEs, AGENTS files, code comments, and log
  messages. Historical German comments in `channel/src/` have been
  translated; do not reintroduce them.
- Identifiers are English. Match the surrounding style of the file you edit.
- Commits may be made for logical steps, but the message must always be
  prefixed with `[AI]`, e.g. `[AI] Cleanup` or `[AI] Implement <specify
  here>`.
