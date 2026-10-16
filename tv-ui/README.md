# TV UI

The tv-ui is ReWave's virtual TV: a web UI to zap through channels and watch
them. It talks to the network's REST API (channel list, watch heartbeats) and
plays the channel's stream.

## Smart on-demand channels

The tv-ui never starts or stops containers itself — channel lifecycle lives in
the network:

1. A viewer opens a channel → tv-ui sends a watch request / heartbeat to the
   network.
2. The network starts the channel if it is not running yet.
3. While someone is watching, heartbeats keep the channel alive.
4. After the last viewer leaves, the network keeps the channel running for a
   keep-alive window (e.g. a few minutes) in case someone returns, then stops
   it.

Channels with a DVB/24/7 assignment are exempt (once the muxer exists).

## Streaming

RTSP is the intended protocol between channel and viewer. Design note: browsers
cannot play RTSP natively, so a gateway is needed (e.g. MediaMTX offering
WebRTC/HLS from the channel's RTSP output). The exact choice is an open design
decision.

## Status

Concept — no code yet. This README is the spec. The network API it depends on
is sketched in [`../network/README.md`](../network/README.md).
