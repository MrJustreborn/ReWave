# AGENTS.md

This is the **network** subproject of the ReWave mono-repo: the master and
manager (channel lifecycle, media library, REST API for the UIs).

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project.

Facts an agent needs here:

- TypeScript on Node.js (>= 24, strict, ESM), managed with npm. All commands
  run from this directory: `npm run dev` (tsx watch), `npm run build`,
  `npm run typecheck`, `npm run start` (dist/), `npm run migrate`,
  `npm run adseeker -- <video> [--min-duration <seconds>]`.
- `openapi.yaml` is the authoritative REST API contract (OpenAPI 3.1,
  API-first; `/v1/` paths, OAuth2 resource-server auth). API types are
  generated from it by `openapi-typescript` into `src/generated/` at
  install/build time: gitignored, always regenerated — never edit or commit
  that directory.
- Skeleton status: Fastify server, OAuth2 resource-server auth
  (`AUTH_MODE=off` default for dev; `jwks` requires `AUTH_JWKS_URL` plus
  optional `AUTH_ISSUER`/`AUTH_AUDIENCE`), SQLite migrations
  (`db/migrations/`), a dockerode client module and the ad-break detection
  port (`src/ingest/`) exist — but all REST handlers are `501` stubs. Keep
  spec and code in sync.
- Migrations are forward-only and run automatically at server startup
  (`npm run migrate` applies manually, `npm run migrate:status` lists
  applied/pending). Applied files are checksum-validated; renamed, deleted,
  edited-after-apply and out-of-order files are rejected — never edit an
  applied migration, add a new numbered `NNNN_description.sql` instead.
- SQLite is Node's built-in `node:sqlite` (`DatabaseSync`) — no native npm
  dependency; that is also why the engine is pinned to `>=24`. The database
  path defaults to `data/network.db` (`DB_PATH`).
- The network is the only component allowed to know channels: control flow is
  strictly network → channel, never the reverse.
- The SQLite media library is single-writer and owned by the network; it is
  never mounted into other containers.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
