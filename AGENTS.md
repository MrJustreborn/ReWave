# AGENTS.md

## What this is

ReWave is a mono-repo for a self-hosted, Docker-based virtual TV network,
consisting of multiple subprojects. Each subproject has its own `README.md`
(the spec) and its own `AGENTS.md` with project-specific facts — read both
before working there.

| Path             | Purpose                                                              |
| ---------------- | -------------------------------------------------------------------- |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming (prototype)        |
| `network/`       | Master & manager: channel lifecycle, media library, REST API (manager not implemented; contains the ad-break detection prototype and example data) |
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

- There is no test suite, linter, typecheck, or CI anywhere in this repo;
  don't go looking for test commands.
- Python code is stdlib-only: no `requirements.txt`, no pip dependencies
  (the venv under `channel/venv` is untracked local clutter; the Docker image
  installs nothing).
- `ffmpeg` and `ffprobe` are hard requirements. They are **not installed in
  the devcontainer** — they only exist inside the Docker image
  (`channel/Dockerfile`). Verify with `command -v ffmpeg` before blaming the
  code.
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

- `network/src/ads/adseeker.py` must be run from its own directory
  (`network/src/ads/`) because its `VIDEO` path is CWD-relative
  (`../../../sample-media/...`).
- `network/examples/` is not consumed by any code yet (it sketches a resolved
  playlist and per-day schedule templates for the future manager).

## Conventions

- Everything in English — READMEs, AGENTS files, code comments, and log
  messages. Historical German comments in `channel/src/` have been
  translated; do not reintroduce them.
- Identifiers are English. Match the surrounding style of the file you edit.
