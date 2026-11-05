# Manual API requests

Replayable requests for the network's REST API ([`../openapi.yaml`](../openapi.yaml)) —
no frontend needed. The files work in both:

- **JetBrains IDEs** (IntelliJ, WebStorm, PyCharm, …) — built-in HTTP client.
  Variables come from [`http-client.env.json`](http-client.env.json).
  Never put real tokens in that file — use `http-client.private.env.json`
  (gitignored) instead.
- **VS Code** — the [REST Client](https://marketplace.visualstudio.com/items?itemName=humao.rest-client)
  extension. Variables come from [`.vscode/settings.json`](../.vscode/settings.json)
  (`rest-client.environmentVariables`).

Both variable sources define the same names: `{{baseUrl}}`, `{{token}}`,
`{{channelId}}`, `{{videoId}}`. Edit whichever one your IDE reads; the defaults
point at a local dev server and use the example identifiers from
`openapi.yaml`.

## Local workflow

```bash
cd network/
cp .env.example .env   # optional; defaults are fine for local dev
npm install
npm run dev            # listens on :8080, AUTH_MODE=off by default
```

Then run requests from these files. With the default `AUTH_MODE=off` no
`Authorization` header is needed; `system.http` shows the bearer-token
pattern for `AUTH_MODE=jwks`.

Note: while the manager logic is not implemented yet, every business
endpoint answers `501` — each request below notes the response to expect
once its operation is implemented. `/healthz` is live.

## Files

- [`system.http`](system.http) — liveness and auth examples
- [`channels.http`](channels.http) — channel registry, lifecycle, watching, resolved playlists
- [`videos.http`](videos.http) — media library and ingestion

Keep request bodies in sync with the examples in `openapi.yaml` — they are
copies of the spec's example objects.
