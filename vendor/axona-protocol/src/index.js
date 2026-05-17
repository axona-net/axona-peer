// =====================================================================
// @axona/protocol (browser-vendored slim) — public barrel export.
//
// Subset of @axona/protocol that runs in browsers without a build
// step.  Omits the pub/sub layer (AxonManager, AxonPubSub, post.js)
// because post.js imports `node:crypto`.  Pub/sub support in the
// browser awaits either a Web Crypto port of post.js or an esbuild
// bundle step in axona-peer.
//
// This file mirrors the upstream package's index.js MINUS the
// `./pubsub/*` re-exports.  Resync with the canonical source at
// github.com/axona-net/axona-protocol/src/index.js when its
// non-pubsub exports change.
// =====================================================================

// ── Contracts ────────────────────────────────────────────────────────
export { Transport }        from './contracts/Transport.js';
export { DHT }              from './contracts/DHT.js';
export { BootstrapService } from './contracts/BootstrapService.js';

// ── Per-node DHT implementation (NH-1) ──────────────────────────────
export { AxonaPeer } from './dht/AxonaPeer.js';
export { DHTNode, GEO_CELL_BITS } from './dht/DHTNode.js';
export { NeuronNode } from './dht/NeuronNode.js';
export { Synapse }    from './dht/Synapse.js';

// ── Utilities ──────────────────────────────────────────────────────
export {
  clz64,
  randomU32,
  randomU64,
  roundTripLatency,
  haversine,
} from './utils/geo.js';

export { geoCellId } from './utils/s2.js';
