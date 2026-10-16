# AGENTS.md

## What this is

ReWave is a mono-repo for a self-hosted, Docker-based virtual TV network,
consisting of multiple subprojects. Each subproject has its own `README.md`
(the spec) and its own `AGENTS.md` with project-specific facts — read both
before working there.

| Path             | Purpose                                                              |
| ---------------- | -------------------------------------------------------------------- |
| `channel/`       | One TV channel: scheduled FFmpeg playlist streaming (prototype)        |
| `network/`       | Master & manager: channel lifecycle, media library, REST API (concept, no code yet) |
| `management-ui/` | Web UI to manage the network (concept, no code yet)                   |
| `tv-ui/`         | Virtual TV web UI with on-demand channel watching (concept, no code yet) |
| `tests/`         | Playlist/schedule JSON **data**, not automated tests                   |
| `test-movie/`    | Sample media, gitignored via `*.mov` (fresh clones start empty)       |

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
- Channel code is stdlib-only: no `requirements.txt`, no pip dependencies
  (the venv under `channel/venv` is untracked local clutter; the Docker image
  installs nothing).
- `ffmpeg` and `ffprobe` are hard requirements. They are **not installed in
  the devcontainer** — they only exist inside the Docker image
  (`channel/Dockerfile`). Verify with `command -v ffmpeg` before blaming the
  code.
- Docker: `docker compose up` builds `channel/` with `network_mode: host`
  (host networking is required for multicast; Linux only).

## Channel gotchas

- `main.py` hardcodes the playlist path relative to itself:
  `../../tests/playlist.json` → repo-root `tests/playlist.json`. The Dockerfile
  copies only `src/`, so in the container that resolves to
  `/tests/playlist.json`, which is neither copied nor mounted — compose
  currently cannot start the channel successfully.
- Schema mismatch: `main()` sorts slots by `entry["timestamp"]` and reads
  slot-level `file`/`type`, but `tests/playlist.json` slots have `start`/`end`
  plus a nested `stream` array — running main.py against the example playlist
  raises `KeyError`. Expect schema-reconciliation work rather than "fixing"
  one side blindly.
- `tests/schedule.json` is not consumed by any code yet (it sketches per-day
  schedule templates for a future `network/`).
- `channel/src/ads/adseeker.py` must be run from `channel/src/ads/` because
  its `VIDEO` path is CWD-relative (`../../../test-movie/...`). Long-term it
  belongs to the network's ingestion pipeline (ad-break points are per-video
  library data, not per-channel).

## Conventions

- Everything in English — READMEs, AGENTS files, code comments, and log
  messages. Historical German comments in `channel/src/` have been
  translated; do not reintroduce them.
- Identifiers are English. Match the surrounding style of the file you edit.
