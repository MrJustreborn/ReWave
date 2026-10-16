# AGENTS.md

This is the **channel** subproject of the ReWave mono-repo: one TV channel — a
stateless player that receives a resolved playlist and streams it with FFmpeg.

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project (how it works, configuration, planned channel API and RTSP
output).

Facts an agent needs here:

- Python, stdlib-only. Entry point: `src/main.py`; config via the
  `TARGET_MULTICAST` / `TARGET_PORT` environment variables.
- `ffmpeg` / `ffprobe` are required and only exist inside the Docker image —
  not in the devcontainer. Verify with `command -v ffmpeg` first.
- A channel never talks to the network. The media library database is
  off-limits here; playlists arrive already resolved.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
