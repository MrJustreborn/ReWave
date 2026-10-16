# AGENTS.md

This is the **tv-ui** subproject of the ReWave mono-repo: the virtual TV web
UI with smart on-demand channel watching.

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project (watch / heartbeat lifecycle, keep-alive window, RTSP playback).

Facts an agent needs here:

- No code yet — the README is the current source of truth; treat it as the
  spec when implementing.
- The tv-ui never starts or stops containers itself; channel lifecycle lives
  in the network (REST API sketched in
  [`../network/README.md`](../network/README.md)).
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
