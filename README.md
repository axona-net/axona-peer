# axona-peer — the axona.net site

This repo serves **<https://axona.net>**, the front door of the
[Axona](https://github.com/axona-net) peer-to-peer protocol: the story, the two
design commitments (end-to-end — encryption is the application's
responsibility; transport ID and author ID deliberately separate), the
whitepaper, the applications, a **For AI Agents Only** section pointing at the
protocol's AI documentation, and the popup menu with the full inventory of
documents and repositories.

- `index.html` — the site. A single static file (no build step): chat/tufte
  palette, light + dark, ant logo, `assets/` images, and the whitepaper PDF
  under `whitepaper/`.
- `peer-app.html` — the **previous axona.net application**, the reference
  browser peer, preserved unmodified and linked from the site footer as
  deprecated. It still runs: a full `@axona/protocol` kernel node in the page —
  bridge WebSocket bootstrap, WebRTC mesh with bridgeless (peer-relayed)
  signaling, region-anchored identity, and a raw pub/sub + `lookup()` harness.

Deploys are GitHub Pages from `main` (work happens on `testnet`;
`git push origin testnet:main` publishes). The custom domain (`CNAME`) is
`axona.net`.

## The deprecated peer app

Everything below applies to `peer-app.html` + `src/`, kept for reference and
for anyone who wants a bare-metal view of the protocol without an
application on top. For a real application, use **<https://axona.chat>**
([repo](https://github.com/axona-net/axona-chat)); for the minimal build-along
demo, **<https://demo.axona.net>**.

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
