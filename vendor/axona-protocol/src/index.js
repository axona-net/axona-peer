// =====================================================================
// @axona/protocol — public barrel export.
//
// Pure-JS protocol kernel for the Axona peer-to-peer mesh.  Three
// contract surfaces, one per-node DHT implementation (AxonaPeer with
// NH-1 routing + axonal pub/sub), supporting state classes, and
// geographic / hashing helpers.
//
// Consumers (axona-peer, axona-bridge, dht-sim) import named symbols
// from here.  Sub-path imports (e.g. `@axona/protocol/contracts/DHT.js`)
// are also supported via the `exports` map in package.json.
// =====================================================================

// ── Contracts ────────────────────────────────────────────────────────
export { Transport }        from './contracts/Transport.js';
export { DHT }              from './contracts/DHT.js';
export { BootstrapService } from './contracts/BootstrapService.js';

// ── Errors ────────────────────────────────────────────────────────────
export {
  AxonaError,
  IdentityError,
  TransportError,
  PublishError,
  SubscribeError,
  KillError,
  UnpubError,
  TouchError,
  PullError,
  MetricsError,
  UpgradeRequiredError,
  ErrorCodes,
  isWireError,
  fromWire,
} from './errors.js';

// ── Version handshake ─────────────────────────────────────────────────
// Wire frames + compatibility checks used by the web and node
// transports on each fresh signaling channel.  Most apps don't touch
// these directly — they're plumbed into the transport factories.
export {
  WIRE_VERSION,
  KERNEL_VERSION,
  UPGRADE_CLOSE_CODE,
  buildClientHello,
  buildServerHello,
  parseHello,
  parseVersion,
  compareVersions,
  wireCompatible,
  performClientHandshake,
  performServerHandshake,
} from './transport/handshake.js';

// ── Sim transport (in-process; tests + dht-sim) ──────────────────────
// Always-environment-neutral; safe to ship in the main barrel.
export {
  SimNetwork,
  SimTransport,
  simTransport,
} from './transport/sim/index.js';

// ── Persistence ───────────────────────────────────────────────────────
// PersistenceAdapter is the abstract contract; InMemoryPersistence is
// the reference implementation used by `persist: false` and by tests.
// Both are environment-neutral (no node:fs, no browser globals) so
// they ship in the main barrel.
//
// Platform-specific impls are sub-path imports only — including them
// here would pull node:fs into browser bundles or globalThis.indexedDB
// into Node module load:
//
//   import { FilePersistence }      from '@axona/protocol/persistence/file.js';
//   import { IndexedDBPersistence } from '@axona/protocol/persistence/indexeddb.js';
export {
  PersistenceAdapter,
  InMemoryPersistence,
} from './persistence/interface.js';

// ── Identity ─────────────────────────────────────────────────────────
// Ed25519 keypair + 264-bit nodeId + S2-prefix region anchor.
// deriveIdentity({ lat, lng }) generates a fresh identity;
// dumpIdentity/loadIdentity persist and restore. computeNodeId is
// the public helper for verifying a claimed nodeId against the
// stored pubkey + region.
export {
  deriveIdentity,
  dumpIdentity,
  loadIdentity,
  computeNodeId,
  computeNodeIdBigInt,
} from './identity/index.js';

// ── Per-node DHT implementation (NH-1) ──────────────────────────────
export { AxonaPeer }    from './dht/AxonaPeer.js';
export { AxonaDomain }  from './dht/AxonaDomain.js';
export { DHTNode, GEO_CELL_BITS } from './dht/DHTNode.js';
export { NeuronNode }   from './dht/NeuronNode.js';
export { Synapse }      from './dht/Synapse.js';
export { Subscription } from './dht/Subscription.js';

// ── Pub/sub primitives ─────────────────────────────────────────────
export { AxonaManager } from './pubsub/AxonaManager.js';
export { AxonPubSub }  from './pubsub/AxonPubSub.js';
export {
  makePost,
  deriveTopicId,
  verifyPostHash,
  verifyTopicOwnership,
  verifySignature,
  canonical,
  sha256Hex,
} from './pubsub/post.js';
export {
  buildEnvelope,
  verifyEnvelope,
  computeMsgId,
  checkFreshness,
  ENVELOPE_DOMAIN,
  MAX_PUBLISH_SKEW_MS,
} from './pubsub/envelope.js';
export {
  buildKill,
  verifyKill,
  KILL_DOMAIN,
} from './pubsub/kill.js';
export {
  buildTouch,
  verifyTouch,
  TOUCH_DOMAIN,
} from './pubsub/touch.js';
export {
  buildUnpub,
  verifyUnpub,
  UNPUB_DOMAIN,
} from './pubsub/unpub.js';

// ── Ed25519 helpers (Web Crypto wrapper) ─────────────────────────
// Optional companion to post.js for runtimes that support Web Crypto
// Ed25519 (Chrome 110+, Safari 17+, Firefox 130+, Node 20+).
// Applications on older runtimes can substitute @noble/ed25519 with
// the same shape — post.js's signer/verifier contracts are
// implementation-agnostic.
export {
  generateKeyPair,
  exportPublicKey,
  importPublicKey,
  sign,
  verify,
  makeSigner,
  makeVerifier,
} from './pubsub/ed25519.js';

// ── Utilities ──────────────────────────────────────────────────────
// The big ones the protocol uses directly are re-exported; the
// remaining geo.js helpers (haversine, roundTripLatency, continent
// detection, XOR routing-table builders, etc.) are reachable via
// the `@axona/protocol/utils/geo.js` sub-path import for consumers
// that need them.

// 264-bit identifier math — node ID and topic ID share the same
// keyspace: [8-bit S2 prefix] || [256-bit hash].
export {
  ID_BITS,
  HASH_BITS,
  S2_BITS,
  HEX_CHARS,
  MAX_ID,
  MAX_HASH,
  MAX_S2,
  toHex,
  fromHex,
  isHexId,
  assembleId,
  extractS2Prefix,
  extractHash,
  s2PrefixOfHex,
  xorDistance,
  stratumOf,
  clz264,
  randomU256,
} from './utils/hexid.js';

export {
  randomU32,
  roundTripLatency,
  haversine,
} from './utils/geo.js';

// S2 geographic cell math — the foundation of every Axona address.
// `geoCellId(lat, lng)` returns the 8-bit cell prefix that occupies
// the top byte of every nodeId and publisher-keyed topic ID.  The
// inverse and bound helpers let applications display, label, or
// route by region without reimplementing the S2 cube math.  The
// 8-bit cellId matches the top 8 bits of Google S2's level-3 cell
// ID — full interop with any standard S2 library.
export {
  geoCellId,
  geoCellCenter,
  geoCellCorners,
  geoCellFace,
  geoCellSubCenters,
  geoCellHalf,
  isValidCellId,
  S2_FACES,
  S2_CELL_COUNT,
  S2_RESERVED_FROM,
} from './utils/s2.js';

// Canonical human-readable names for all 192 regions (the 8-bit S2 cell that
// occupies the top byte of every id).  regionName(code)/regionCode(name) are a
// bijection; resolveRegion() accepts a name OR a numeric code so a region name
// can be used interchangeably with its code as a prefix.
export {
  REGION_NAMES,
  regionNames,
  regionName,
  regionCode,
  resolveRegion,
  regionNameForLatLng,
} from './utils/region-names.js';

// axona/4 authenticated-identity handshake.  Re-exported so consumers
// that drive their own channel lifecycle (the bridge's embedded peer,
// the node transport, custom transports) can build/verify authenticated
// hellos with the same primitive the web transport uses.
export {
  buildAuthHello,
  verifyAuthHello,
  pubkeyMatchesNodeId,
  makeNonce,
  cbvFromNonces,
  cbvFromFingerprints,
  AUTH_PROTO,
} from './transport/handshake-auth.js';
