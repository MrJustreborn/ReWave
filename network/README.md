# Network

The network is ReWave's master and manager — the only component that knows the
whole picture. UIs and services talk only to the network, the network talks to
the channels, and channels never talk back.

## Responsibilities

- **Channel lifecycle** — add/remove channels (one Docker container per
  channel), start/stop them, poll their status through their HTTP APIs.
- **Channel registry** — channel definitions with a type/category (movies,
  cartoons, series, …) and their schedule/playlist templates.
- **Media library** — SQLite database owned and written only by the network:
  videos, metadata, ad-break points. Never mounted into other containers.
- **Video ingestion** — when a new video is added to the pool: check whether it
  is already processed, run ad-break detection (scene detection, ported to
  TypeScript in `src/modules/media/infrastructure/ffmpeg/`), store the result.
- **Watcher tracking** — smart on-demand channels for the tv-ui: a channel
  starts when someone wants to watch it and keeps running for a keep-alive
  window after the last viewer leaves (per-viewer heartbeats).
- **REST API** — the only public surface, for management-ui and tv-ui.

## Architecture

The implementation follows an onion (ports and adapters) architecture with
DDD-style modules. Dependencies point strictly inwards — the domain knows
nothing about the outside world:

```
src/
├── main.ts             composition root: loads config, wires modules (pure DI)
├── config.ts           environment → typed configuration (loads .env)
├── server/             driving adapter: Fastify assembly, auth, error mapping
├── shared/             shared kernel: ids, errors, clock, validation, API contracts
├── db/                 shared persistence infrastructure: SQLite connection, migrations
└── modules/            bounded contexts (one DDD module each)
    ├── media/          media library + video ingestion
    ├── channel/       channel registry + lifecycle (Docker)
    ├── scheduling/    schedule → resolved playlists
    └── watching/      on-demand viewer tracking (tv-ui)
```

Every module has the same onion-shaped anatomy:

| Directory         | Role                                                            |
| ----------------- | --------------------------------------------------------------- |
| `domain/`         | entities, value objects, ports (repository, detector, runtime) — no framework, no I/O |
| `application/`   | use-case services orchestrating domain + ports                    |
| `infrastructure/`| driven adapters: SQLite rows + mappers + repositories, ffmpeg, dockerode, in-memory stores |
| `api/`           | driving adapter: Fastify routes + REST DTO mappers (generated types) |
| `index.ts`       | the module's public API + factory (pure DI wiring inside the module) |

### Model mapping (three shapes per aggregate)

The persistence model, the domain model and the REST contract are separate
types, connected by explicit mappers:

```
SQLite row (snake_case, module infrastructure)          REST DTO (generated from openapi)
        │                                                        ▲
        │ row ↔ domain mapper (infrastructure)                    │ domain ↔ DTO mapper (api)
        ▼                                                        │
        domain entity (invariants, branded ids, Date) ───────────┘
```

- REST DTOs come from `openapi.yaml` via `src/generated/` (regenerated, never
  edited) — the generated types are only used by module `api/` layers and
  `server/`.
- Rows live in `modules/*/infrastructure/**` and are the only code that knows
  SQLite column names and JSON encoding.

### Module boundaries

The rules are enforced by `npm run check:architecture`
(`scripts/check-architecture.ts`), which runs as part of `npm run typecheck`
and `npm run build`:

- Module code may only be imported through its `index.ts` public API — never
  from another module's `domain/`, `application/`, `infrastructure/` or `api/`.
- Cross-module imports are only allowed in the `application/` layer, and only
  along the sanctioned direction: `scheduling → channel, media`;
  `watching → channel`. Everything else goes through the composition root.
- `domain/` and `application/` stay pure: no fastify/dockerode/node:sqlite/node:fs.
- `src/shared/` is the shared kernel — imported only through `shared/index.ts`.
- When adding a module or changing allowed dependencies, update
  `MODULE_DEPENDENCIES` in `scripts/check-architecture.ts`.

## REST API

Specified API-first: the authoritative contract is
[`openapi.yaml`](openapi.yaml) (OpenAPI 3.1). Paths are versioned under
`/v1/` — breaking changes only in a future `/v2/`, additive changes stay
in `/v1/`. All endpoints require OAuth2 (the network is a pure resource
server; login/IdP stays out of scope and is configured at deploy time)
with the scopes `channels:read`, `channels:write`, `videos:read`,
`videos:write` and `watch`.

Replayable request files for manual testing live in
[`requests/`](requests/) (VS Code REST Client and JetBrains HTTP client;
see `requests/README.md`).

## Media library (SQLite)

Single-writer database owned by the network. Rough sketch:

- `videos` — path, duration, title, metadata, `processed` flag
- `ad_breaks` — per video: detected positions (scene-change based)

Channels never read this database. The network resolves a channel's playlist
(video paths plus ad-break points) and pushes it to the channel via its API.

If ingestion turns out to be heavy (scene detection is CPU-hungry FFmpeg work),
it can be split into its own library/ingest container later — the same
treatment as the DVB muxer.

## Local development

```bash
cd network/
npm install             # also regenerates src/generated/ from openapi.yaml
cp .env.example .env    # optional — defaults work for local dev
npm run dev             # listens on :8080, AUTH_MODE=off, migrations applied automatically
```

- Environment: `.env` (gitignored) is loaded automatically at startup via
  `process.loadEnvFile` — see [`.env.example`](.env.example) for all variables
  (`PORT`, `DB_PATH`, `AUTH_MODE`, `WATCH_KEEPALIVE_SECONDS`, …). Real
  environment variables take precedence over `.env`.
- Database: `DB_PATH` (default `data/network.db`, gitignored) is created and
  migrated automatically; `npm run migrate:status` lists applied/pending
  migrations.
- API without a frontend: open the files in `requests/` in your IDE
  (VS Code REST Client extension or the built-in JetBrains HTTP client) —
  `requests/README.md` explains the variable setup.
- Useful scripts: `npm run typecheck` (generates, checks module boundaries,
  typechecks), `npm run build`, `npm start` (dist/),
  `npm run adseeker -- <video> [--min-duration <seconds>]` (ad-break detection
  from the command line).

## Examples

`examples/` sketches what the network will produce — not consumed by any code
yet:

- `playlist.json` — a resolved playlist as pushed to a channel (slots with
  `start`/`end` and a nested `stream` array of preroll/content/ad entries)
- `schedule.json` — per-day schedule templates per channel

## Roadmap

- **DVB muxing** — coordinate a separate muxer container with hardware access
  (DVB-S2/DVB-C) that takes channel streams and feeds them to real TVs;
  channels shown there run 24/7.
- **Kodi / Jellyfin plugins** — expose ReWave as a TV source with channels.

## Status

Early stage: the REST API contract ([`openapi.yaml`](openapi.yaml)) is
implemented as a TypeScript skeleton over the onion/module architecture
described above — API types and the OAuth2 scope map are generated from the
spec at build time (gitignored output; scopes are never hardcoded), the
layers, modules, mappers, ports and adapters are in place (SQLite row mappers
and repositories are functional), and the ad-break detection is ported to
`src/modules/media/infrastructure/ffmpeg/` (`npm run adseeker -- <video>`).
**All REST handlers answer `501`** — every use-case service method throws
`NotImplementedError` and the server error mapper turns it into the 501
response. The manager logic is still to be implemented; this README remains
the spec.

Run it: `npm install && npm run dev` (listens on `:8080`), or
`docker compose up network`.
