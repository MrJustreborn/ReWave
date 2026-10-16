# AGENTS.md

This is the **network** subproject of the ReWave mono-repo: the master and
manager (channel lifecycle, media library, REST API for the UIs).

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project (responsibilities, planned REST API sketch, SQLite media library
design).

Facts an agent needs here:

- No code yet — the README is the current source of truth; treat it as the
  spec when implementing.
- The network is the only component allowed to know channels: control flow is
  strictly network → channel, never the reverse.
- The SQLite media library is single-writer and owned by the network; it is
  never mounted into other containers.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
