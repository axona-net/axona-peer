# axona-peer

Phase 1 browser client for the [Axona](https://github.com/axona) protocol. Connects to an [`axona-bridge`](https://github.com/axona-net/axona-bridge) over WebSocket, sends a ping every second, and renders the connection state as a colored indicator.

This is the visible substrate of the network. Phase 2 will add browser-to-browser connections (WebRTC) and a QR code so you can join the mesh from a phone; for now it's a single client talking to a single bridge.

## Quickstart

No build step. Open `index.html` in a browser. By default it tries `ws://localhost:8080` — start an `axona-bridge` there first:

```bash
# in another terminal
cd ../axona-bridge && npm start
```

Then open the peer. The simplest way:

```bash
# from this directory
npx http-server -p 5173 -c-1
# → open http://localhost:5173
```

Or just `open index.html` — modern browsers will run it from `file://` for local testing.

## Indicator states

| Color  | Animation | Meaning |
|---|---|---|
| Red    | solid    | Not connected (boot, after socket close, between reconnect attempts) |
| Orange | blinking | Connecting, awaiting the first pong, or stale (no pong in > 3 s) |
| Green  | blinking | Connected and receiving pongs on schedule (≤ 3 s old) |

Orange handles both "we're trying" and "the line went quiet" because they're both states where the connection is uncertain. (Orange not orange: orange is hard to tell from green for red-green colorblind viewers.) Green requires *confirmed bidirectional* traffic — opening the socket alone isn't enough; we wait for the first pong.

## Configuration

Override the bridge URL with a query string parameter:

```
http://localhost:5173/?bridge=wss://bridge.axona.net
http://localhost:5173/?bridge=ws://10.0.0.5:8080
```

The URL must use `ws://` for plain TCP or `wss://` for TLS. Browsers refuse `ws://` from any page served over HTTPS, so GitHub Pages deployment (Phase 2) will only work with `wss://`.

## Wire format

Mirrors [axona-bridge](https://github.com/axona-net/axona-bridge):

| Direction | Type | Payload |
|---|---|---|
| client → bridge | `ping` | `{ type, t: <Date.now()> }` |
| bridge → client | `welcome` | `{ type, connId, serverT }` |
| bridge → client | `pong` | `{ type, t: <echo>, serverT }` |

RTT is `Date.now() - msg.t` — no clock-skew assumptions because the bridge echoes `t` unchanged.

## Layout

```
axona-peer/
├── index.html         # UI scaffolding
├── src/
│   ├── client.js      # WebSocket client + state machine
│   └── styles.css     # Three indicator states + layout
├── LICENSE
└── README.md
```

## Behavior detail

- **1 Hz ping loop.** `setInterval(send, 1000)`. Each ping carries `t = Date.now()`.
- **Auto-reconnect.** Exponential backoff starting at 1 s, doubling up to 16 s. Reset to 1 s on any successful open.
- **Stale-pong detection.** A 500 ms checker watches `lastPongAt`. If no pong arrives within 3 s the indicator drops to orange without waiting for the socket to close — production wires can sit half-open for minutes before the OS notices.
- **First-pong gate.** The indicator stays orange after `open` until the first pong actually arrives. Just having the socket up isn't enough; we want a confirmed round-trip.

## License

MIT
