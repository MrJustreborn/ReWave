# AGENTS.md

## What this is

Python "TV station" that streams scheduled playlists as MPEG-TS over UDP multicast.

- `channel/` — the only real code. `channel/src/main.py` runs playlist slots through ffmpeg → named pipes (`/tmp/pipe_a`, `/tmp/pipe_b`) → a relay thread that UDP-sends 1316-byte chunks to a multicast target.
- `network/` — placeholder for a multi-channel combiner; no code yet.
- `tests/` — playlist/schedule JSON **data**, not automated tests. There is no test suite, linter, typecheck, or CI anywhere in this repo; don't go looking for test commands.

## Running

- Hard requirement: `ffmpeg` and `ffprobe` on PATH. They are **not installed in the devcontainer** — they only exist inside the Docker image (`channel/Dockerfile`). Verify with `command -v ffmpeg` before blaming the code.
- Code is stdlib-only: no `requirements.txt`, no pip dependencies (the venv under `channel/venv` is untracked local clutter; the Docker image installs nothing).
- Channel entrypoint: `python3 channel/src/main.py`. Config via env vars `TARGET_MULTICAST` (default `239.100.0.1`) and `TARGET_PORT` (default `1234`).
- Ad-break finder: `python3 adseeker.py` — must be run from `channel/src/ads/` because its `VIDEO` path is CWD-relative (`../../../test-movie/...`).
- Docker: `docker compose up` builds `channel/` with `network_mode: host` (host networking is required for multicast; Linux only).

## Gotchas

- `main.py` hardcodes the playlist path relative to itself: `../../tests/playlist.json` → repo-root `tests/playlist.json`. The Dockerfile copies only `src/`, so in the container that resolves to `/tests/playlist.json`, which is neither copied nor mounted — compose currently cannot start the channel successfully.
- Schema mismatch: `main()` sorts slots by `entry["timestamp"]` and reads slot-level `file`/`type`, but `tests/playlist.json` slots have `start`/`end` plus a nested `stream` array — running main.py against the example playlist raises `KeyError`. Expect schema-reconciliation work rather than "fixing" one side blindly.
- `tests/schedule.json` is not consumed by any code yet (it sketches per-day schedule templates for a future `network/`).
- `test-movie/big_buck_bunny_1080p_h264.mov` (~725 MB) is gitignored via `*.mov`; fresh clones won't have it. Only `ads/adseeker.py` references it.

## Conventions

- Comments, docstrings, and some log messages are German; identifiers are English. Match the surrounding style of the file you edit.
