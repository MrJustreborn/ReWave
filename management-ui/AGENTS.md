# AGENTS.md

This is the **management-ui** subproject of the ReWave mono-repo: the
web-based admin UI for the network.

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project (planned features, its dependency on the network API).

Facts an agent needs here:

- No code yet — the README is the current source of truth; treat it as the
  spec when implementing.
- The management UI talks only to the network's REST API (sketched in
  [`../network/README.md`](../network/README.md)); never to channels directly.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
