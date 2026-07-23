# axona-peer — the reference browser peer (deprecated)

> ## ⛔ FROZEN — no further updates
>
> This repository is **deprecated and frozen as of kernel v4.38.0**. It is kept
> only as a bare-metal source reference; it is **not hosted** (GitHub Pages is
> disabled, no domain) and receives **no further changes** — not doc-link
> refreshes, whitepaper/PDF syncs, review copy, kernel re-vendors, or version
> bumps. Do not update anything in this repo. The kernel it vendors under
> `vendor/axona-protocol/` will drift from the live protocol and that is
> expected.
>
> **Where the living projects moved:**
> - **axona.net** (the website) → **[axona-net/axona-web](https://github.com/axona-net/axona-web)**
> - **a real application** → **<https://axona.chat>** ([axona-net/axona-chat](https://github.com/axona-net/axona-chat))
> - **the minimal build-along demo** → **<https://demo.axona.net>** (`apps/axona-minimal` in [axona-net/axona-protocol](https://github.com/axona-net/axona-protocol))
> - **headless node** → **[axona-net/axona-relay](https://github.com/axona-net/axona-relay)**

The original **axona.net** application: a full `@axona/protocol` kernel node
that runs in a browser tab. It bootstraps to an
[`axona-bridge`](https://github.com/axona-net/axona-bridge) over WebSocket,
forms a WebRTC mesh (with bridgeless, peer-relayed signaling), derives a
region-anchored identity in-page, and exposes a raw pub/sub + `lookup()`
harness. It is kept as a bare-metal reference — a view of the protocol with no
application on top.

> **This repo no longer serves axona.net.** The website moved to its own
> repository, **[axona-net/axona-web](https://github.com/axona-net/axona-web)**,
> which now owns the `axona.net` domain. For a real application use
> **<https://axona.chat>**
> ([repo](https://github.com/axona-net/axona-chat)); for the minimal
> build-along demo, **<https://demo.axona.net>**. GitHub Pages is disabled on
> this repo — the peer app lives here as source, not as a hosted site.

`peer-app.html` (+ `src/`) is the peer application; `index.html` is the old
story-site copy, superseded by axona-web. Everything below documents the peer
app.

- **Runs a kernel peer in the browser** on the kernel's `webTransport()`
  (reconnect/backoff, ping/pong, version gate + authenticated `hello`) and
  `AxonaPeer` (routing and pub/sub).
- **WebRTC mesh + bridgeless connection** — once on the mesh, peers relay each
  other's signaling, so new links form with no bridge in the signaling path.
- **Identity** — a 264-bit Ed25519 identity anchored to an S2 region cell,
  derived in-page and persisted.
- **Pub/sub harness** — subscribe/publish on region-keyed topics with replay,
  plus a `lookup()` harness for routing inspection.

Serve the directory statically to run it locally:

```bash
npx http-server -p 5173 -c-1     # → http://localhost:5173/peer-app.html
```

The peer picks its bridge from the serving host (testnet host → testnet
bridge; anywhere else → production `wss://bridge.axona.net`); override with
`?bridge=ws[s]://host[:port]`. The kernel is **vendored** under
`vendor/axona-protocol/` (plain ESM, no bundler) — re-sync with
`bash scripts/sync-protocol.sh` after a kernel change.

## Ecosystem

| | |
|---|---|
| Protocol kernel | [axona-net/axona-protocol](https://github.com/axona-net/axona-protocol) |
| Group chat | [axona-net/axona-chat](https://github.com/axona-net/axona-chat) → <https://axona.chat> |
| Signaling bridge | [axona-net/axona-bridge](https://github.com/axona-net/axona-bridge) |
| Headless supernode | [axona-net/axona-relay](https://github.com/axona-net/axona-relay) |
| Documentation | [axona-net/axona-docs](https://github.com/axona-net/axona-docs) |

## License

MIT
