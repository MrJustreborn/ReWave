# Management UI

The management UI is the web-based control panel for the ReWave network. It
talks **only** to the network's REST API — never to channels directly.

## Features (planned)

- Dashboard: network status, running channels, now playing
- Channels: list / add / remove, assign a type (movies, cartoons, …),
  start / stop
- Media pool: add videos (triggers ingestion and ad-break detection on the
  network side), see processed / unprocessed state
- Scheduling: edit playlist / schedule templates per channel and per day
  (cf. `../network/examples/schedule.json`)

## Status

Concept — no code yet. This README is the spec. The network API it depends on
is sketched in [`../network/README.md`](../network/README.md).
