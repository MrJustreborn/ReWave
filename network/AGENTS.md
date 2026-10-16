# AGENTS.md

This is the **network** subproject of the ReWave mono-repo: the master and
manager (channel lifecycle, media library, REST API for the UIs).

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project (responsibilities, planned REST API sketch, SQLite media library
design).

Facts an agent needs here:

- Early stage: `src/ads/adseeker.py` (ad-break detection prototype, runs
  standalone from its own directory — its `VIDEO` path is CWD-relative) and
  `examples/` (example playlist/schedule data, not consumed by code) exist;
  the manager itself is not implemented. The README remains the spec.
- `openapi.yaml` is the authoritative REST API contract (OpenAPI 3.1,
  API-first): `/v1/` paths, OAuth2 resource-server auth with the scopes
  `channels:read`, `channels:write`, `videos:read`, `videos:write`,
  `watch`. No server code exists yet — keep spec and code in sync.
- The network is the only component allowed to know channels: control flow is
  strictly network → channel, never the reverse.
- The SQLite media library is single-writer and owned by the network; it is
  never mounted into other containers.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
