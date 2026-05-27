# @axona/protocol

Pure-JS protocol kernel for the [Axona](https://axona.net) peer-to-peer mesh.
One node, one package — give it a Transport and it speaks the Axona wire
protocol. Runs unchanged in browsers, Node servers, and the dht-sim
simulator.

**v1.1.3** — production kernel. Powers [axona.net](https://axona.net) (browser
peers), [bridge.axona.net](https://bridge.axona.net) (signaling broker), and
the [`dht-sim`](https://github.com/axona-net/dht-sim) reference simulator at
50,000 peers.

## Install

```bash
npm install @axona/protocol
```

Pure JS, no native dependencies, ESM only. Node ≥ 20.

## Quickstart

A pub/sub roundtrip in one process using the in-memory `SimNetwork`:

```js
import {
  AxonaPeer, AxonaDomain, NeuronNode,
  AxonaManager, simTransport, SimNetwork,
  deriveIdentity,
} from '@axona/protocol';

const network = new SimNetwork();
const domain  = new AxonaDomain({ k: 20 });

async function makePeer(region) {
  const identity = await deriveIdentity(region);     // 264-bit Ed25519
  const transport = simTransport({ network, identity });
  await transport.start(identity.id);

  const node  = new NeuronNode({
    id:  BigInt('0x' + identity.id),
    lat: region.lat, lng: region.lng,
  });
  node.transport = transport;

  const peer = new AxonaPeer({ domain, node, identity, transport });
  await peer.start();
  return peer;
}

const alice = await makePeer({ lat: 38, lng: -77 });   // US-east
const bob   = await makePeer({ lat: 38, lng: -77 });

await bob.sub('hello-world', env => console.log('got:', env.message));
await alice.pub('hello-world', 'hi from alice');
```

For a runnable version with synaptome admission and the full
SimNetwork wiring, see [`examples/minimal-pubsub/`](examples/minimal-pubsub/).
`npm install && node index.js` — full pub/sub roundtrip in ~150 lines.

## The AxonaPeer surface

The `AxonaPeer` class is the per-node application API, organised into five
clusters:

| Cluster | Methods |
|---|---|
| **Lifecycle** | `start()` · `stop()` · `join(opts?)` · `leave({drain, notify, timeoutMs}?)` · `getNodeId()` |
| **Pub/sub** | `pub(topic, message, opts?)` · `sub(topic, handler, opts?)` · `pull(msgId, opts?)` · `metrics(topic, opts?)` |
| **Direct messaging** | `send(targetId, message)` · `notify(targetId, message)` · `onMessage(handler)` |
| **Mesh introspection** | `peers()` · `onPeerJoin(handler)` · `onPeerLeave(handler)` · `lookup(targetKey)` |
| **Telemetry** | `health()` · `onLog(level, handler)` · `onError(handler)` |
| **Snapshot escape hatch** | `snapshot()` · `fromSnapshot(blob)` |

Full reference with parameter types, return shapes, and worked examples:
[axona-docs Programmer Guide §API Reference](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/API-Reference.md).

## Transports

Three concrete transports implement the same 12-method `Transport` contract.
The protocol layer can't tell which one it's running on.

| Factory | Where it runs | Use case |
|---|---|---|
| [`simTransport({ network, identity })`](src/transport/sim/) | In-process | Tests, dht-sim, multi-peer-in-one-process demos |
| [`Transport.web({ identity, bridgeUrl })`](src/transport/web/) | Browser | WebRTC data channels + WebSocket bridge fallback. Used by [`axona-peer`](https://github.com/axona-net/axona-peer). |
| [`Transport.node({ identity, listen | bridgeUrl })`](src/transport/node/) | Headless server | Raw WebSocket. Used by [`axona-bridge`](https://github.com/axona-net/axona-bridge). |

All three speak the same wire protocol (`v1.1.x`); a peer using one can
communicate with a peer using either of the others.

## Persistence

`PersistenceAdapter` is the abstract contract; three implementations ship:

- `InMemoryPersistence` (default; loses state on stop)
- `IndexedDBPersistence` — browsers
- `FilePersistence` — Node

The platform-specific implementations are sub-path imports so neither pulls
`node:fs` into browser bundles nor `globalThis.indexedDB` into Node:

```js
import { IndexedDBPersistence } from '@axona/protocol/persistence/indexeddb.js';
import { FilePersistence }      from '@axona/protocol/persistence/file.js';
```

Pass to `AxonaPeer`:

```js
const peer = new AxonaPeer({ domain, node, identity, transport, persistence });
```

Persistence captures the synaptome, axonal-tree memberships, and identity
across restarts.

## Identity

264-bit Ed25519 identities anchored to an S2 geographic cell:

```js
import { deriveIdentity, dumpIdentity, loadIdentity } from '@axona/protocol';

const identity = await deriveIdentity({ lat: 38, lng: -77 });
console.log(identity.id);                       // 66-char hex nodeId
console.log(identity.publicKeyHex);             // 64-char hex pubkey

const blob = await dumpIdentity(identity);      // round-trip via JSON
const same = await loadIdentity(blob);
```

`nodeId = [8-bit S2 prefix] || [256-bit SHA-256(publicKey)]`. Topic IDs share
the same 264-bit keyspace.

## Errors

Typed error classes with stable codes for runtime branching:

```js
import { PublishError, UpgradeRequiredError, ErrorCodes } from '@axona/protocol';

try {
  await peer.pub(topic, message);
} catch (e) {
  if (e instanceof UpgradeRequiredError) /* peer needs newer wire version */;
  else if (e.code === ErrorCodes.NO_K_CLOSEST) /* …special-case */;
  else throw e;
}
```

Full list: `AxonaError`, `IdentityError`, `TransportError`, `PublishError`,
`SubscribeError`, `PullError`, `MetricsError`, `UpgradeRequiredError`.

## What's inside

```
@axona/protocol/
├── contracts/          # the contract surfaces every implementation must conform to
│   ├── DHT.js          # application-facing per-node contract (AxonaPeer fulfils this)
│   ├── Transport.js    # network-facing contract (12 methods)
│   ├── BootstrapService.js   # cold-start sponsor flow
│   ├── types.js        # shared JSDoc typedefs
│   └── index.js        # barrel export
│
├── dht/
│   ├── AxonaPeer.js    # per-node DHT contract impl. NH-1 routing + axonal pub/sub.
│   ├── AxonaDomain.js  # shared mesh state — k, simEpoch, per-domain config.
│   ├── DHTNode.js      # base node state (id, lat/lng, lifecycle)
│   ├── NeuronNode.js   # AxonaPeer's per-node state (synaptome, temperature, incomingSynapses)
│   ├── Synapse.js      # routing-table entry (peerId, weight, latency, stratum)
│   └── Subscription.js # handle returned by peer.sub() — .unsubscribe(), .topicId
│
├── pubsub/
│   ├── AxonaManager.js  # axonal-tree pub/sub membership protocol
│   ├── AxonPubSub.js   # feed-style application API (pub/sub/pull/metrics)
│   ├── envelope.js     # buildEnvelope / verifyEnvelope / computeMsgId
│   ├── post.js         # makePost + topic-id derivation + verification
│   └── ed25519.js      # Web Crypto Ed25519 wrapper (sign / verify)
│
├── transport/
│   ├── sim/            # in-process router + simTransport — tests, dht-sim
│   ├── web/            # WebRTC + WebSocket-bridge composite — browsers (axona-peer)
│   ├── node/           # raw WebSocket — Node servers (axona-bridge)
│   ├── handshake.js    # version negotiation on every fresh channel
│   └── wire.js         # frame shape, codecs
│
├── persistence/
│   ├── interface.js    # PersistenceAdapter + InMemoryPersistence
│   ├── indexeddb.js    # browser-only (sub-path import)
│   └── file.js         # Node-only (sub-path import)
│
├── identity/
│   ├── index.js        # deriveIdentity, dumpIdentity, loadIdentity
│   └── nodeid.js       # computeNodeId, S2-prefix bundling
│
├── utils/
│   ├── hexid.js        # 264-bit identifier math (XOR distance, S2 prefix split, etc.)
│   ├── geo.js          # haversine, propagation delay, continent detection
│   └── s2.js           # geographic-cell encoding (S2 Hilbert prefix)
│
└── errors.js           # typed error classes + ErrorCodes
```

## What's NOT inside

- The simulator itself (`SimulatedTransport`, the benchmark harness, the 3D
  globe visualiser) — those live in [`axona-net/dht-sim`](https://github.com/axona-net/dht-sim).
- The reference browser peer with the production UI, region picker, identity
  persistence flow, and bridge fallback — that's [`axona-net/axona-peer`](https://github.com/axona-net/axona-peer).
- The signaling broker — [`axona-net/axona-bridge`](https://github.com/axona-net/axona-bridge).
- The whitepaper, paper, explainer, programmer guide, and API reference —
  those live in [`axona-net/axona-docs`](https://github.com/axona-net/axona-docs).

## Examples

- [`examples/minimal-pubsub/`](examples/minimal-pubsub/) — two peers in
  one Node process, pub/sub roundtrip in ~150 lines. Pinned to the
  local kernel source via `file:../..`, so `npm install && node index.js`
  picks up whatever's in `src/`. The right starting point for new
  developers.
- [`examples/minimal-pubsub-browser/`](examples/minimal-pubsub-browser/)
  — browser version of the same demo, served live at
  [`demo.axona.net`](https://demo.axona.net). Connects to the production
  bridge over WebSocket and runs a kernel peer in the page.
- [`examples/s2-region-visualizer/`](examples/s2-region-visualizer/)
  — interactive 3D globe showing the 256 S2 cells that anchor every
  Axona nodeId and pub/sub topic. Click a cell in the legend to see
  it lit up on the globe; hit "Detect my location" to find your own
  region. Useful when explaining why polar cells shrink, or for
  picking a region label for a topic.

For real-world wiring (WebRTC + bridge fallback, identity persistence,
region pickers), the canonical example is
[`axona-peer/src/client.js`](https://github.com/axona-net/axona-peer/blob/main/src/client.js)
— the reference browser peer at ~1500 lines.

## Programmer guide

The full programmer-facing documentation lives in
[`axona-net/axona-docs`](https://github.com/axona-net/axona-docs):

| Document | When to read |
|---|---|
| [Quick Start](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Quick-Start.md) | You want a working pub/sub roundtrip in 5 minutes |
| [API Reference](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/API-Reference.md) | You're building and need a specific call's signature |
| [Programmer Guide](https://github.com/axona-net/axona-docs/blob/main/programmer-guide/Axona-Programmer-Guide.md) | You're starting a new application against Axona |

## Wire protocol

The frame shapes, message-type vocabulary, version handshake, and bridge
wire format are in [`axona-docs/implementation/`](https://github.com/axona-net/axona-docs/tree/main/implementation).
The handshake module at [`src/transport/handshake.js`](src/transport/handshake.js)
is the canonical implementation reference.

## Tests

```bash
npm test
```

Runs 21 smoke suites covering addressing, errors, persistence (in-memory,
file, IndexedDB), identity, all three transports, version handshake, pub/sub
(unified API, envelope, pull, metrics), direct messaging, mesh introspection,
health, lifecycle (join/leave/snapshot), and Ed25519 post signing.

## License

MIT — see [LICENSE](LICENSE).
