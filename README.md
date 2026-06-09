# axona-peer

The reference **browser peer** for the [Axona](https://github.com/axona-net)
peer-to-peer protocol. It runs a full [`@axona/protocol`](https://github.com/axona-net/axona-protocol)
kernel node in the page: it connects to an [`axona-bridge`](https://github.com/axona-net/axona-bridge)
over WebSocket for bootstrap/signaling, forms a **WebRTC mesh** with the other
peers, and exposes identity, routing, and pub/sub through a small UI.

**v3.28.0** — on kernel **v2.32.0** (`axona/5` wire epoch). Deployed at
[axona.net](https://axona.net) (production — on the `axona/5` line since the
2026-06-08 flag-day cutover) and at [testnet.axona.net](https://testnet.axona.net)
(the staging line this branch targets, ahead of `main`).

## What it does

- **Runs a kernel peer in the browser.** Orchestration rides directly on the
  kernel's `webTransport()` (bridge WebSocket, reconnect/backoff, ping/pong,
  stale detection, the version-gate + authenticated `hello` handshake) and
  `AxonaPeer` + `AxonaDomain` + `NeuronNode` (routing and pub/sub). No
  hand-rolled mesh/codec lives in the app anymore.
- **WebRTC mesh + bridgeless connection.** Peers connect browser-to-browser
  over WebRTC data channels. Once on the mesh they relay each other's signaling
  (`meshRelay`), so new links can form with no bridge in the signaling path.
- **Identity.** A 264-bit Ed25519 identity anchored to an S2 region cell,
  derived in-page and persisted; the nodeId's top byte is the region prefix.
- **Region picker.** Choose your region by name or code (see the
  [region model](https://github.com/axona-net/axona-protocol#regions) — 192
  cells, two names each, both resolving to one code).
- **Pub/sub harness.** Subscribe to and publish on **region-keyed topics**
  shaped as `${region}/${event}`; late subscribers receive the replayed
  backlog. Plus a direct `lookup()` harness for routing inspection.
- **Share QR + version row.** A QR encodes the current URL (incl. any
  `?bridge=` override) so a phone joins the same mesh; the "version" row shows
  the peer, bridge, and kernel versions side by side.

## Quickstart

No build step — it's static files plus the vendored kernel under
`vendor/axona-protocol/`. Serve the directory and open it:

```bash
npx http-server -p 5173 -c-1     # → http://localhost:5173
```

By default the peer picks its bridge from the host it's served from:

- served from `testnet.axona.net` → the same-origin testnet bridge
  (`wss://testnet.axona.net`)
- anywhere else → production `wss://bridge.axona.net`

For local development, run a bridge and point at it:

```bash
cd ../axona-bridge && npm start            # ws://localhost:8080
# then open:
http://localhost:5173/?bridge=ws://localhost:8080
```

`ws://` is plain TCP, `wss://` is TLS. Browsers refuse `ws://` from a page
served over HTTPS, so any hosted deployment must use `wss://`.

## Mesh-row indicator states

The UI shows one row per connection — the bridge row first, then a row per
WebRTC peer — each with its own indicator:

| Color  | Meaning |
|---|---|
| **Red**    | disconnected / failed |
| **Orange** | connecting, signaling, awaiting first pong, or stale (no traffic on schedule) |
| **Green**  | open and pongs flowing on schedule |

Green requires *confirmed bidirectional* traffic — opening the channel isn't
enough; the peer waits for the first pong.

## Version gate

Every connection runs the kernel's `axona/5` authenticated handshake. A peer
on an incompatible wire epoch is cleanly rejected — the bridge closes the
WebSocket with code **4426** (`upgrade required`) and the client logs a
developer-visible `UPGRADE REQUIRED` error telling it to update. Because the
epoch is folded into the signed transcript, an `axona/5` peer and a peer on the
retired `axona/4` epoch **cannot** interconnect by design.

## Layout

```
axona-peer/
├── index.html              # UI scaffolding (mesh table, region picker, pub/sub harness)
├── src/
│   ├── client.js           # the app — webTransport + AxonaPeer wiring (~1500 lines)
│   ├── identity.js         # region list + identity derive/persist (kernel deriveIdentity)
│   ├── qr.js               # share-QR rendering   (src/vendor/qrcodegen.js)
│   ├── styles.css          # layout + indicator states
│   ├── wire.js · mesh.js · transport.js · …   # thin re-exports of the kernel modules
│   ├── contracts/          # re-exported kernel contracts
│   └── smoke_*.js          # identity / transport / kernel-path / version-gate smokes
├── vendor/axona-protocol/  # vendored kernel (synced via scripts/sync-protocol.sh)
├── scripts/sync-protocol.sh
├── LICENSE
└── README.md
```

The kernel is vendored, not npm-installed, so the page loads it as plain ESM.
Re-vendor with `bash scripts/sync-protocol.sh` after changing the kernel.

## Configuration

| Query param | Effect |
|---|---|
| `?bridge=ws[s]://host[:port]` | override the bridge URL (otherwise host-detected) |

## License

MIT
