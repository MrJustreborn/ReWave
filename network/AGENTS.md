# AGENTS.md

This is the **network** subproject of the ReWave mono-repo: the master and
manager (channel lifecycle, media library, REST API for the UIs).

**Read [`README.md`](README.md) in this directory first** — it is the spec for
this project, including its architecture section.

Facts an agent needs here:

- TypeScript on Node.js (>= 24, strict, ESM), managed with npm. All commands
  run from this directory: `npm run dev` (tsx watch), `npm run build`,
  `npm run typecheck`, `npm run start` (dist/), `npm run migrate`,
  `npm run migrate:status`, `npm run check:architecture`,
  `npm run adseeker -- <video> [--min-duration <seconds>]`.
- `openapi.yaml` is the authoritative REST API contract (OpenAPI 3.1,
  API-first; `/v1/` paths, OAuth2 resource-server auth). API types
  (`schema.ts` via `openapi-typescript`) and the auth-scope map
  (`security.ts` via `scripts/generate-security.ts`, typechecked through
  `tsconfig.scripts.json`) are generated from it into `src/generated/` at
  install/build time: gitignored, always regenerated — never edit or commit
  that directory. Auth scopes are never hardcoded: every operation must
  declare `uiAuth` security in the spec, and generation fails otherwise.
- Architecture: onion/ports-and-adapters with DDD modules under `src/modules/`
  (`media`, `channel`, `scheduling`, `watching`), each with
  `domain/` (entities, VOs, ports — pure), `application/` (use-case services),
  `infrastructure/` (adapters: SQLite rows+mappers+repos, ffmpeg, dockerode,
  in-memory stores), `api/` (Fastify routes + DTO mappers) and `index.ts`
  (public API + module factory). `src/main.ts` is the composition root (pure
  DI — no DI framework), `src/shared/` the shared kernel (imported only via
  `shared/index.ts`), `src/server/` the HTTP assembly, `src/db/` the shared
  SQLite connection + migrations.
- Module boundaries are machine-enforced by `npm run check:architecture`
  (`scripts/check-architecture.ts`, stdlib-only): modules are only importable
  via their `index.ts`; cross-module imports only in the `application/` layer
  and only along `MODULE_DEPENDENCIES` (scheduling → channel, media;
  watching → channel); domain/application have no framework/node-I/O
  dependencies; generated types are only used in module `api/` layers and
  `server/` (+ the type-only `generated/schema.ts` in the kernel). It runs as
  part of `npm run typecheck` and `npm run build`. When adding a module or
  changing allowed dependencies, update `MODULE_DEPENDENCIES` there.
- Three model shapes per aggregate, mapped explicitly: SQLite rows
  (`modules/*/infrastructure/**`, snake_case, the only code touching column
  names/JSON encoding), domain entities (`modules/*/domain/**`, branded ids,
  `Date`s, invariants) and generated REST DTOs (`modules/*/api/**`). Never
  leak a row or a DTO into the domain or application layer.
- Implementation status: layers, mappers, ports, SQLite repositories and
  wiring are done; **every use-case service method throws
  `NotImplementedError`**, which the server error mapper turns into the `501`
  responses — implementing an operation means filling in the service method
  (and domain logic), not writing a new route. `src/server/error-mapper.ts`
  maps `ValidationError` → 400, `NotFoundError` → 404, `ConflictError` → 409,
  `NotImplementedError` → 501 (bodies follow the openapi `Error` schema).
- Auth (`src/server/auth.ts`): OAuth2 resource server. `AUTH_MODE=off` (dev
  bypass, default) or `jwks` (requires `AUTH_JWKS_URL`, optional
  `AUTH_ISSUER`/`AUTH_AUDIENCE`). Scope requirements come from the generated
  `src/generated/security.ts` — never hardcode scopes.
- Environment: `src/config.ts` loads `.env` from the project directory
  automatically (`process.loadEnvFile`, ENOENT-tolerant) — real env vars win.
  All variables (with defaults) are documented in `.env.example` (copy to
  `.env`, which is gitignored). Runtime knobs: `PORT`, `HOST`, `LOG_LEVEL`,
  `DB_PATH`, `MEDIA_ROOT`, `DOCKER_SOCKET`, `WATCH_KEEPALIVE_SECONDS`,
  `AUTH_MODE`, `AUTH_JWKS_URL`, `AUTH_ISSUER`, `AUTH_AUDIENCE`.
- Migrations are forward-only and run automatically at server startup
  (`npm run migrate` applies manually, `npm run migrate:status` lists
  applied/pending). Applied files are checksum-validated; renamed, deleted,
  edited-after-apply and out-of-order files are rejected — never edit an
  applied migration, add a new numbered `NNNN_description.sql` instead.
- SQLite is Node's built-in `node:sqlite` (`DatabaseSync`) — no native npm
  dependency; that is also why the engine is pinned to `>=24`. The database
  path defaults to `data/network.db` (`DB_PATH`). Rows ↔ domain mapping stays
  in `modules/*/infrastructure/**`; timestamps are SQLite UTC
  (`YYYY-MM-DD HH:MM:SS`, helpers in `src/shared/time.ts`).
- Manual API testing without a frontend: `requests/*.http` (VS Code REST
  Client + JetBrains HTTP client; variables in `.vscode/settings.json` and
  `requests/http-client.env.json`, real tokens only in the gitignored
  `requests/http-client.private.env.json`). Keep request bodies in sync with
  the example objects in `openapi.yaml`.
- The network is the only component allowed to know channels: control flow is
  strictly network → channel, never the reverse.
- The SQLite media library is single-writer and owned by the network; it is
  never mounted into other containers.
- Repo-wide facts and gotchas live in the root `AGENTS.md`.
