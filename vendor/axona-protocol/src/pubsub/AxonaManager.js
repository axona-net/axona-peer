/**
 * AxonaManager — distributed pub/sub membership protocol.
 *
 * Implements the PubSubAdapter transport contract on top of a DHT that
 * provides the routed-messaging primitives (routeMessage, onRoutedMessage,
 * sendDirect, onDirectMessage).
 *
 * Identifier conventions (post-v1.5 refactor):
 *
 *   In-memory state          — BigInt (264-bit DHT addresses).  All
 *                              Map keys and Set entries that hold node
 *                              or topic identifiers are BigInts.
 *
 *   Wire payloads            — hex (66-char lowercase strings).  Every
 *                              JSON-over-transport field that carries
 *                              a node or topic id flows as hex.
 *                              Conversion happens at handler boundaries
 *                              (fromHex on receive, toHex on send).
 *
 *   Content-addressed ids    — strings, unchanged.  publishId is
 *                              `${nodeId.toString(10)}:counter`,
 *                              postHash is the sha256-hex of the
 *                              canonical envelope; neither is a DHT
 *                              address and both stay string-valued.
 *
 * Message types (over the DHT):
 *
 *   ROUTED:
 *     pubsub:subscribe   — payload: { topicId(hex), subscriberId(hex), lastSeenTs }
 *     pubsub:unsubscribe — payload: { topicId(hex), subscriberId(hex) }
 *     pubsub:publish     — payload: { topicId(hex), json, publishId, publishTs,
 *                                     postHash, publisher(hex) }
 *
 *   DIRECT:
 *     pubsub:deliver     — payload: { topicId(hex), json, ... }
 *
 * See documents/Phase3-Membership-Protocol-Plan.md for the full design
 * rationale, state model, and parameter defaults.
 */

import { toHex, fromHex, isHexId } from '../utils/hexid.js';
import { verifyEnvelope, checkFreshness, computeMsgId, MAX_PUBLISH_SKEW_MS } from './envelope.js';
import { powVerify } from '../pow/pow.js';
import { verifyKill } from './kill.js';
import { verifyTouch } from './touch.js';
import { verifyUnpub } from './unpub.js';
import { resolveTopic, deriveTopicId, sha256Hex } from './post.js';

// ── Defaults (simulator-tuned; production would use much longer values) ────

const DEFAULT_MAX_DIRECT_SUBS        = 20;           // §5.8 hysteresis (unused in 3a)
const DEFAULT_MIN_DIRECT_SUBS        = 5;            // §5.8 hysteresis (unused in 3a)
const DEFAULT_REFRESH_INTERVAL_MS    = 10_000;       // §5.5
const DEFAULT_MAX_SUBSCRIPTION_AGE_MS = 30_000;      // §5.7 — 3× refresh
const DEFAULT_ROOT_GRACE_MS          = 60_000;       // §5.7 — 6× refresh
const DEFAULT_ROOT_SET_SIZE          = 5;            // R — root-set size (pub/sub replication factor)
const DEFAULT_REPLAY_CACHE_SIZE      = 1024;         // per-role bounded ring, COUNT cap (§7.8 replay)
// Raised from 100 so a chunked file survives replay: a 2 MB image at ~11.5 KB
// chunks ≈ 175 messages, far over the old 100. Paired with a BYTE cap so a count
// of 1024 × up-to-16 KB entries can't OOM a small relay — eviction honors both
// (count OR bytes, whichever binds). Small-message topics get deep history
// cheaply; large-message (file) topics are bounded by memory, not the count.
const DEFAULT_REPLAY_CACHE_BYTES     = 16 * 1024 * 1024;  // per-role byte cap (16 MB)

// ── Inbound caps (D-1: bound attacker-controlled payloads) ─────────────────
// A peer must not be able to make us allocate unbounded memory from a
// single inbound message.  These cap the attacker-controlled arrays /
// payload sizes on the network-facing handlers; legitimate traffic is
// comfortably under each bound.
export const MAX_PUBLISH_BYTES = 256 * 1024;         // absolute hard ceiling (chars; see note)
// RELIABLE-delivery ceiling (finding O-5). A publish must be RECEIVABLE by an
// arbitrary peer on an arbitrary browser, across arbitrary intermediate hops —
// and the WebRTC SCTP maxMessageSize is negotiated per-connection, floored by
// the weakest hop and the receiving stack. The only size guaranteed receivable
// by every conformant implementation is the WebRTC-interoperable 16 KiB. Above
// it, delivery is a gamble on the receiver's browser (measured: node-datachannel
// silently drops ≥64 KB; Chrome tolerates ~256 KB — so a sender-side or
// single-hop measurement is NOT a safe bound). peer.pub rejects above this by
// default so oversize fails LOUD at the publisher instead of vanishing en route;
// larger payloads must go through @axona/protocol/std/chunk. Overridable per
// AxonaPeer only for controlled, known-homogeneous deployments (e.g. node-only
// relay fleets) — never the browser default.
// Set just under the 16 KiB interop floor: the enveloped publish is re-wrapped
// once more in a deliver/replay frame ({topicId,json,publishId,postHash,...},
// ~0.5 KB) before it hits the wire, so the guard leaves ~1 KiB of headroom so
// that OUTER frame still fits 16 KiB. std/chunk targets the same number.
export const MAX_RELIABLE_PUBLISH_BYTES = 15 * 1024;
// NOTE: this is the small/medium-message lane, not a blob channel. Large
// binary content (images/documents) should ride a content-reference manifest
// + out-of-band transfer (Tier 2: a DHT content store keyed by content hash),
// NOT the broadcast path — every publish is replicated to R root axons,
// cached in their replay rings, and fanned to all subscribers, so big blobs
// here amplify badly. Measured against json.length (UTF-16 code units), so
// heavily multi-byte payloads can exceed 256 KiB on the wire; a single
// message must still fit the WebRTC data-channel max-message size downstream.
const MAX_SUBSCRIBER_BATCH     = 512;                // adopt-subscribers subscriberIds[] ceiling
const MAX_PEER_ROOTS           = 32;                 // peerRoots[] ceiling (R is ~5)
// Replay-batch framing: each replay frame must itself fit the 16 KiB wire limit
// (O-5), so we DECOUPLE it from the cache size — a 1024-entry cache replays as
// MANY frames, each byte-bounded below, never one giant undeliverable frame.
// MAX_REPLAY_BATCH is the per-FRAME message-count ceiling (inbound D-1 guard);
// REPLAY_FRAME_BYTES is the per-frame serialized-bytes budget the SENDER honors.
const MAX_REPLAY_BATCH         = 256;                // messages[] ceiling PER FRAME (was = cache size)
const REPLAY_FRAME_BYTES       = 14 * 1024;          // sender's per-frame byte budget (< 16 KiB incl. wrapper)
// Gap-safe replay: a subscriber reports the recent postHashes it HOLDS so a root
// replays only what's actually missing. A single lastSeenTs high-water can't
// represent a hole — once you receive anything newer than a gap, the gap is
// masked forever (the "occasional missing message that never recovers" bug).
// Sized a bit above the replay cache so steady-state subscribers report a
// superset of any root's cache ⇒ nothing is re-sent.
// Capped so the subscribe-k frame carrying the digest (have[] of 66-char
// postHashes) stays under the 16 KiB wire limit even at the larger cache size:
// 200 × 66 ≈ 13 KB. (A compact digest that scales to the full cache is a
// follow-up; until then a re-subscriber holding >MAX_HAVE just gets a few
// already-held entries re-sent, which dedup drops.)
const MAX_HAVE                 = 200;

// C-2: per-publisher seq reorder tolerance.  seq is wall-clock-seeded
// (~ms units; see AxonaPeer._nextPubSeq), so this is effectively "reject a
// signed envelope whose seq is more than this many ms behind the
// publisher's high-water mark" — a captured-and-replayed envelope.  Wide
// enough to absorb benign in-flight reordering (a live message that takes
// a slower path arrives with a slightly lower seq and is still accepted);
// far tighter than the freshness window so the two gates reinforce.
const SEQ_REORDER_TOLERANCE_MS = 60_000;

// Phase A #2 (kill): how long a root axon keeps a tombstone for a killed
// msgId, so a lagging/rejoining replica can't resurrect the message by
// re-gossiping it after the kill.  Sized to the default message hold (24 h);
// once per-message TTL lands (#5) this aligns to the message's own remaining
// hold.  Bounded by MAX_TOMBSTONES (LRU) so it can't grow without limit.
const TOMBSTONE_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_TOMBSTONES   = 4096;
// Phase A #2 (kill convergence): after a root applies a creator-authorized kill,
// it re-gossips that SIGNED kill to the current K-closest root set for this long,
// so a replica that missed the original kill (churn, a dropped delivery, a root
// that joined the set late) still removes + tombstones the message — closing the
// "kill, reload, it comes back" resurrection. Re-gossiping the signed kill (not a
// bare msgId) keeps it self-authorizing: a receiver re-runs the full verifier, so
// this can't be turned into a censorship primitive. Bounded so kill traffic is
// proportional to recent retraction activity, never forever.
const KILL_REGOSSIP_MS = 10 * 60 * 1000;
const MAX_KILL_SYNC    = 64;            // kills per kill-sync message (ceiling)
// Fix 2 — root-to-root message anti-entropy: roots exchange digests of held
// postHashes with their K-closest siblings and pull what they're missing, so a
// subscriber attached to ANY root gets every publisher's feed (closes the
// publisher-local-K-closest divergence — a publish lands on the publisher's
// K-closest, which need not be the subscriber's). Round-robin a few topics per
// refresh tick to bound traffic; pulled messages are RE-VERIFIED (a sibling root
// is not trusted). MAX_HAVE / MAX_REPLAY_BATCH bound the digest / response.
const MSGSYNC_TOPICS_PER_TICK = 8;
// Cold-start drain: a freshly (re)started or newly-recruited root holds an EMPTY
// replay cache for every topic it hosts.  Steady-state round-robin (8/tick) would
// take minutes to backfill a keyspace host with hundreds of topics — during which
// it advertises as a root but answers replays empty (the "reload → 0 / partial"
// window after a relay restart).  So roles not yet reconciled even once ("cold")
// are drained with a much larger per-tick budget, ahead of the steady round-robin,
// so a restarted host converges in seconds.  Costs nothing in steady state (no
// cold roles); a one-time burst of ≤ this many msgsync exchanges after (re)join.
const MSGSYNC_COLD_BUDGET     = 64;

// Phase A #5: message hold time.  A message expires (is swept from replay
// caches and no longer served/pulled) at its (signed, freshness-clamped)
// timestamp + the hold.  DEFAULT_HOLD_MS is the ownerless/default; MAX_HOLD_MS
// is the absolute ceiling no sliding-pull (#6) may extend past.  Owner-set
// per-topic holds (≤ MAX_HOLD_MS) arrive with the config object (Phase B);
// role.maxHoldMs is the hook.
const DEFAULT_HOLD_MS = 24 * 60 * 60 * 1000;   // 24h
const MAX_HOLD_MS      = 48 * 60 * 60 * 1000;   // 48h ceiling

// ── Helpers ────────────────────────────────────────────────────────────────

/**
 * Wire→kernel: convert a hex-string nodeId/topicId from a JSON payload
 * field into a BigInt.  Returns null/undefined unchanged so optional
 * fields stay falsy.
 */
function _wire(hex) {
  if (hex === null || hex === undefined) return hex;
  if (typeof hex === 'bigint') return hex;       // tolerant: already canonical
  return fromHex(hex);
}

/**
 * Like `_wire`, but tolerant of malformed inbound wire data: returns `null`
 * instead of throwing on a non-hex / wrong-length id. Untrusted ingress must
 * never crash a handler — e.g. a peer tearing down mid-shutdown can deliver a
 * truncated `fromId`/`topicId`, and in an *async* handler a synchronous throw
 * becomes a rejected promise that escalates to a process-killing
 * unhandledRejection on Node. Use this for any id parsed from a received frame.
 */
function _wireSafe(hex) {
  if (hex === null || hex === undefined) return null;
  try { return _wire(hex); } catch { return null; }
}

// ── AxonaManager ────────────────────────────────────────────────────────────

export class AxonaManager {
  /**
   * @param {Object} opts
   * @param {MockDHTNode} opts.dht     — the DHT primitive (routeMessage etc.)
   * @param {number} [opts.maxDirectSubs]
   * @param {number} [opts.minDirectSubs]
   * @param {number} [opts.refreshIntervalMs]
   * @param {number} [opts.maxSubscriptionAgeMs]
   * @param {number} [opts.rootGraceMs]
   * @param {Function} [opts.now]      — clock (for deterministic testing)
   */
  constructor({
    dht,
    maxDirectSubs        = DEFAULT_MAX_DIRECT_SUBS,
    minDirectSubs        = DEFAULT_MIN_DIRECT_SUBS,
    refreshIntervalMs    = DEFAULT_REFRESH_INTERVAL_MS,
    maxSubscriptionAgeMs = DEFAULT_MAX_SUBSCRIPTION_AGE_MS,
    rootGraceMs          = DEFAULT_ROOT_GRACE_MS,
    rootSetSize          = DEFAULT_ROOT_SET_SIZE,    // R — root-set size (pub/sub replication factor)
    crossFragmentRoots   = 4,                        // NX-17: number of alternate-root direct copies per publish/subscribe.
    replayCacheSize      = DEFAULT_REPLAY_CACHE_SIZE, // per-role bounded ring (count cap)
    replayCacheBytes     = DEFAULT_REPLAY_CACHE_BYTES, // per-role byte cap (memory bound)
    pickRecruitPeer      = null,   // protocol-specific override (§5.9)
    pickRelayPeer        = null,   // NX-17+ batch-adoption path override
    shouldRecruitSubAxon = null,   // protocol-specific override
    now                  = () => Date.now(),
    emitLog              = null,   // (level, msg, context) sink — forwards the
                                   // 24 security-drop logs to peer.onLog. When
                                   // null the `this._emitLog?.()` calls no-op.
  } = {}) {
    if (!dht) throw Error('AxonaManager: dht is required');

    this.dht                   = dht;
    this.nodeId                = dht.getSelfId();    // BigInt (kernel-canonical)
    this.maxDirectSubs         = maxDirectSubs;
    this.minDirectSubs         = minDirectSubs;
    this.refreshIntervalMs     = refreshIntervalMs;
    this.maxSubscriptionAgeMs  = maxSubscriptionAgeMs;
    this.rootGraceMs           = rootGraceMs;
    this.rootSetSize           = rootSetSize;
    this.crossFragmentRoots    = crossFragmentRoots;
    this.replayCacheSize       = replayCacheSize;
    this.replayCacheBytes      = replayCacheBytes;
    this._now                  = now;
    // Security/observability log sink. Until this is wired (see setLogSink),
    // the 24 `this._emitLog?.(...)` drop-path calls — bad-signature, stale,
    // oversize, posthash-mismatch, unauthorized kill/touch/unpub, etc. — are
    // silent no-ops. Production attaches the peer's onLog surface so dropped
    // (potentially hostile) messages are actually observable.
    /** @type {((level: string, msg: string, context?: object) => void) | null} */
    this._emitLog              = emitLog;

    // Publisher-side: highest publishTs we have observed for each topic.
    // BigInt topicId key.
    /** @type {Map<bigint, number>} */
    this._lastSeenTsByTopic = new Map();
    this._haveByTopic = new Map();        // topicId → Set<postHash> recently received (gap-safe replay digest)

    // Set of publishIds this node has ever received, keyed by BigInt topicId.
    // publishId is a string (`${nodeId(decimal)}:counter` — not a DHT address).
    /** @type {Map<bigint, Set<string>>} */
    this._receivedPublishIds = new Map();

    // Per-node outgoing publishId counter (monotonic, combined with
    // nodeId to be globally unique across the network).
    this._publishCounter = 0;

    // Per-node LRU of publishIds we have already processed (string keys).
    this._seenPublishes = new Map();     // publishId(string) -> insertedAt (ms)
    this._seenPublishCap = 4096;
    // NB: dedup is size-capped only, NOT TTL-expired — expiring a "seen"
    // entry on a timer would let a publish replayed after the window
    // re-deliver, breaking exactly-once. The C-2 freshness window bounds
    // replays instead.

    // C-2: per-publisher monotonic-seq high-water marks, keyed by the
    // signed `signerPubkey` (64-char hex).  Bounded LRU.  Used at live
    // ingress to reject a captured envelope replayed with a seq well
    // behind the publisher's current stream.  Soft state — fail-open on a
    // cold entry (a freshly-promoted root axon learns the high-water from
    // the first message it sees; the freshness window still bounds replays
    // in that gap).
    this._publisherSeq    = new Map();   // signerPubkey(hex) -> highestSeq (number)
    this._publisherSeqCap = 4096;

    // Phase A #2 (kill): tombstones for retracted messages + a dedup set for
    // kill objects we've already processed (keyed by the kill's signature).
    this._tombstones  = new Map();       // msgId(hex) -> expiresAt (ms)
    this._seenKills   = new Map();       // kill.signature -> insertedAt (ms) | {at,kill,topicId} once authorized
    this._seenKillCap = 4096;
    this._lastKillAt  = 0;               // newest authorized-kill ts → steady-state reconciliation skip
    this._msgSyncCursor = 0;             // round-robin index into hosted topics for anti-entropy
    // Phase A #7 (touch): dedup set for touch objects already processed
    // (keyed by the touch's signature), same bound as kills.
    this._seenTouches   = new Map();     // touch.signature -> insertedAt (ms)
    this._seenTouchCap  = 4096;

    // Exactly-once gate for delivery to the LOCAL application callback.
    // Deliberately SEPARATE from `_seenPublishes`: that set means "this
    // node has processed/relayed this publish in some role" and is marked
    // even when the local app has never subscribed (lazy-axon relay).
    // App delivery needs its own idempotency so self-replay on subscribe
    // delivers genuine backlog exactly once without re-delivering it on
    // every periodic resubscribe.  publishId(string) -> insertedAt (ms).
    this._appDelivered    = new Map();
    this._appDeliveredCap = 8192;

    // Per-AxonaManager findKClosest cache.  Keyed by a string computed
    // from the BigInt topicId — see _findKClosest for the format.
    this._kClosestCache = new Map();     // `${topicHex}_${K}` -> { epoch, value: bigint[] }
    this._kClosestEpoch = 0;

    // Policy overrides — if provided, replace the default methods.
    if (pickRecruitPeer)      this.pickRecruitPeer      = pickRecruitPeer;
    if (shouldRecruitSubAxon) this.shouldRecruitSubAxon = shouldRecruitSubAxon;
    if (pickRelayPeer)        this.pickRelayPeer        = pickRelayPeer;

    /** @type {Map<bigint, TopicRole>} topicId(BigInt) → role */
    this.axonRoles = new Map();

    /** @type {Map<bigint, TopicSub>} topicId(BigInt) → sub state for our own subs */
    this.mySubscriptions = new Map();

    /**
     * @type {Map<bigint, {hostedAt:number}>} topicId(BigInt) → host state.
     * Topics this node HOSTS (stores + serves) without consuming. A hosted
     * topic is announced into its K-closest set on the same subscribe-k
     * heartbeat a subscriber uses — so the node is surfaced in neighbors'
     * synaptomes and recruited as a root/replica — but it is deliberately
     * NOT in mySubscriptions (no app delivery, no "my own feed" semantics).
     * Decouples "I will host this for others" from "I want to receive this."
     */
    this._hostedTopics = new Map();

    /**
     * When true, this node volunteers as a host for its OWN keyspace
     * neighborhood: each refresh it announces toward the ids closest to its
     * nodeId, so publishers' findKClosest surface it and demand-driven
     * recruitment makes it a root for whatever topics land near it. This is
     * the "host whatever lands near me" relay primitive.
     */
    this._hostKeyspace = false;

    /** Delivery callback — registered by PubSubAdapter via onPubsubDelivery. */
    this._deliveryCallback = null;

    // ── Post-level metrics state.  `_counters` is a nested Map:
    //   outer key: BigInt topicId
    //   inner key: postHash string (content hash, not a DHT address)
    /** @type {Map<bigint, Map<string, RelayCounters>>} */
    this._counters = new Map();
    // Post-level metrics are best-effort observability, so bound both
    // dimensions: a long-lived root axon would otherwise accumulate one inner
    // entry per post EVER seen on every topic it hosts, without eviction. Cap
    // posts-per-topic and total topics; oldest entries age out (metrics for
    // very old posts simply stop being reported — they're past the replay
    // window anyway). See _counterFor.
    this._countersTopicCap = 1024;
    this._countersPostCap  = 4096;

    /** requestId(string) -> { accumulated: [{responderId(hex), entries[]}] } */
    this._pendingMetricsReqs = new Map();
    this._metricsCounter = 0;

    /** requestId(string) -> { resolve } */
    this._pendingPullReqs = new Map();
    this._pullCounter     = 0;

    /** Dedup set for incoming metricsBroadcast.  string requestIds. */
    this._seenMetricsReqs    = new Set();
    this._seenMetricsReqCap  = 1024;

    // Register handlers with the DHT. Malformed-frame robustness lives ONE layer
    // down, in the AxonaPeer dispatch boundary that wraps every handler: a
    // corrupt sender id (`fromId`) is dropped there for all subsystems, and any
    // handler that throws on a malformed payload id is contained + classified
    // (AxonaPeer._onHandlerError). So these registrations stay plain — there's no
    // per-site guard to forget on the next handler added.
    dht.onRoutedMessage('pubsub:subscribe',    (p, m) => this._onSubscribe(p, m));
    dht.onRoutedMessage('pubsub:unsubscribe',  (p, m) => this._onUnsubscribe(p, m));
    dht.onRoutedMessage('pubsub:publish',      (p, m) => this._onPublish(p, m));
    dht.onRoutedMessage('pubsub:kill',         (p, m) => this._onKill(p, m));
    dht.onDirectMessage('pubsub:kill-k',       (p, m) => this._onKillDirect(p, m));
    dht.onDirectMessage('pubsub:kill-sync',    (p, m) => this._onKillSync(p, m));
    dht.onDirectMessage('pubsub:msgsync',      (p, m) => this._onMsgSync(p, m));
    dht.onDirectMessage('pubsub:msgsync-resp', (p, m) => this._onMsgSyncResp(p, m));
    dht.onRoutedMessage('pubsub:touch',        (p, m) => this._onTouch(p, m));
    dht.onDirectMessage('pubsub:touch-k',      (p, m) => this._onTouchDirect(p, m));
    dht.onRoutedMessage('pubsub:unpub',        (p, m) => this._onUnpub(p, m));
    dht.onDirectMessage('pubsub:unpub-k',      (p, m) => this._onUnpubDirect(p, m));
    dht.onDirectMessage('pubsub:deliver',      (p, m) => this._onDeliver(p, m));
    dht.onDirectMessage('pubsub:promote-axon',     (p, m) => this._onPromoteAxon(p, m));
    dht.onDirectMessage('pubsub:adopt-subscribers',(p, m) => this._onAdoptSubscribers(p, m));
    dht.onDirectMessage('pubsub:dissolve-hint',    (p, m) => this._onDissolveHint(p, m));
    dht.onDirectMessage('pubsub:replay-batch',     (p, m) => this._onReplayBatch(p, m));
    // K-closest mode (available when dht.findKClosest exists).
    dht.onDirectMessage('pubsub:subscribe-k',  (p, m) => this._onSubscribeDirect(p, m));
    dht.onDirectMessage('pubsub:publish-k',    (p, m) => this._onPublishDirect(p, m));
    dht.onDirectMessage('pubsub:unsubscribe-k',(p, m) => this._onUnsubscribeDirect(p, m));
    // ── Post-level metrics.
    dht.onRoutedMessage('pubsub:metricsReq',       (p, m) => this._onMetricsReq(p, m));
    dht.onDirectMessage('pubsub:metricsReq-k',     (p, m) => this._onMetricsReqDirect(p, m));
    dht.onDirectMessage('pubsub:metricsBroadcast', (p, m) => this._onMetricsBroadcast(p, m));
    dht.onDirectMessage('pubsub:metricsResp',      (p, m) => this._onMetricsResp(p, m));
    // ── Pull (on-demand fetch by post_hash) and reshare notifications.
    dht.onRoutedMessage('pubsub:pullReq',       (p, m) => this._onPullReq(p, m));
    dht.onDirectMessage('pubsub:pullResp',      (p, m) => this._onPullResp(p, m));
    dht.onRoutedMessage('pubsub:reshareNotify', (p, m) => this._onReshareNotify(p, m));

    this._timer = null;
  }

  /**
   * Attach (or replace) the security/observability log sink after construction.
   * Lets a peer that resolves an externally- or engine-supplied AxonaManager
   * forward the drop-path logs to its own onLog surface without re-constructing.
   * Idempotent and cheap; pass null to detach.
   * @param {((level: string, msg: string, context?: object) => void) | null} fn
   */
  setLogSink(fn) {
    this._emitLog = (typeof fn === 'function') ? fn : null;
  }

  /**
   * Bound a dedup/replay store to `cap` entries: when it overflows, evict the
   * oldest-inserted half (Map and Set both iterate keys in insertion order, so
   * this is a cheap FIFO-LRU). The single home for the eviction idiom that was
   * copy-pasted across every bounded store (_seenPublishes, _appDelivered,
   * _seenKills, _seenTouches, _publisherSeq, _seenMetricsReqs).
   * @param {Map<any,any>|Set<any>} store
   * @param {number} cap
   */
  _capStore(store, cap) {
    if (store.size <= cap) return;
    const toDrop = cap / 2;
    let i = 0;
    for (const k of store.keys()) {     // Set.keys() aliases values()
      if (i++ >= toDrop) break;
      store.delete(k);
    }
  }

  /** Start the periodic refresh/sweep timer. Idempotent. */
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => this.refreshTick(), this.refreshIntervalMs);
  }

  /** Stop the periodic timer. */
  stop() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
  }

  /**
   * Per-node findKClosest with epoch-keyed local cache.  topicId is
   * BigInt (kernel-canonical); cache key is a derived string.
   */
  async _findKClosest(topicId, K) {
    const keyHex = typeof topicId === 'bigint'
      ? topicId.toString(16)
      : String(topicId);
    const key = `${keyHex}_${K}`;
    const entry = this._kClosestCache.get(key);
    if (entry && entry.epoch === this._kClosestEpoch) {
      return entry.value;
    }
    const value = await this.dht.findKClosest(topicId, K);
    this._kClosestCache.set(key, { epoch: this._kClosestEpoch, value });
    return value;
  }

  invalidateKClosestCache() {
    this._kClosestEpoch++;
    if (this._kClosestCache.size > 256) this._kClosestCache.clear();
  }

  /**
   * Clear all pub/sub runtime state.  See original docstring for the
   * preservation contract.
   */
  resetState() {
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
    this._hostKeyspace = false;
    this._lastSeenTsByTopic.clear();
    this._receivedPublishIds.clear();
    this._seenPublishes.clear();
    this._publisherSeq.clear();
    this._tombstones.clear();
    this._seenKills.clear();
    this._kClosestCache.clear();
    this._kClosestEpoch = 0;
    this._deliveryCallback = null;
    this._publishCounter = 0;
  }

  // ── PubSubAdapter transport contract ────────────────────────────────

  /**
   * Publish to a topic.
   *
   * @param {bigint} topicId   BigInt 264-bit topic id (kernel-canonical).
   * @param {string} json      Application payload (string — typically
   *                           JSON.stringify of a SignedPost).
   * @param {Object} [meta]    Optional metadata.  meta.publisher is
   *                           hex-string nodeId (wire/display form).
   * @returns {string} publishId
   */
  pubsubPublish(topicId, json, meta) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubPublish: topicId must be bigint, got ${typeof topicId}`);
    }
    // publishId is the routing/exactly-once dedup key — OPAQUE, decoupled from
    // the transport id (which is now always ephemeral). The app may supply its
    // own (e.g. a persisted std/publisher stream id, for continuity across a
    // rotated transport id or idempotent re-publish); otherwise we mint a
    // random, collision-safe token. NOT `nodeId:counter` — that embedded the
    // (ephemeral) transport id's S2 prefix and reused values when the counter
    // reset to 0 on restart, letting a peer drop a genuinely-new publish as a dup.
    const publishId = (typeof meta?.publishId === 'string' && meta.publishId)
      ? meta.publishId
      : this._mintPublishId();
    const publishTs = this._now();
    this._asyncPublish(topicId, json, publishId, publishTs, meta)
      .catch(err => console.error('AxonaManager: publish failed:', err));
    return publishId;
  }

  /** Mint a random, S2-free, collision-safe publishId (opaque dedup token). */
  _mintPublishId() {
    const c = globalThis.crypto;
    if (c?.randomUUID) return 'p_' + c.randomUUID().replace(/-/g, '');
    if (c?.getRandomValues) {
      const b = c.getRandomValues(new Uint8Array(16));
      return 'p_' + Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
    }
    return 'p_' + this._now().toString(36) + Math.random().toString(36).slice(2);
  }

  /**
   * Retract a message (Phase A #2).  Routes the signed `kill` object to the
   * topic's R root axons, which authorize it (signer must match the
   * killed message's signer), drop it from their replay caches, tombstone
   * the msgId, and purge subscribers.  Fire-and-forget, same as publish.
   *
   * @param {bigint} topicId   BigInt 264-bit topic id (kernel-canonical).
   * @param {object} kill      signed kill object (see pubsub/kill.js).
   */
  pubsubKill(topicId, kill) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubKill: topicId must be bigint, got ${typeof topicId}`);
    }
    this._asyncKill(topicId, kill)
      .catch(err => console.error('AxonaManager: kill failed:', err));
  }

  /** @private — route the kill to R root axons (mirrors _asyncPublish). */
  async _asyncKill(topicId, kill) {
    const topicIdHex = toHex(topicId);
    const payload = { topicId: topicIdHex, kill };
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const target of roots) this.dht.sendDirect(target, 'pubsub:kill-k', payload);
        // Also apply locally if we're one of the roots (sendDirect to self
        // may be a no-op on some transports).
        if (roots.includes(this.nodeId)) this._handleKill(topicId, kill);
        return;
      }
    }
    // Axonal/routed fallback.
    this.dht.routeMessage(topicId, 'pubsub:kill', payload);
  }

  /**
   * Touch a message (Phase A #7) — creator-only keep-alive.  Routes the signed
   * `touch` to the topic's R root axons (mirrors `_asyncKill`); each root
   * that holds the message resets its hold-time expiry (bounded by the 48h
   * ceiling), moves it to the head of the replay queue, and bumps its eviction
   * recency.  Fire-and-forget.
   *
   * @param {bigint} topicId
   * @param {object} touch   signed touch object (see pubsub/touch.js).
   */
  pubsubTouch(topicId, touch) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubTouch: topicId must be bigint, got ${typeof topicId}`);
    }
    this._asyncTouch(topicId, touch)
      .catch(err => console.error('AxonaManager: touch failed:', err));
  }

  /** @private — route the touch to R root axons (mirrors _asyncKill). */
  async _asyncTouch(topicId, touch) {
    const topicIdHex = toHex(topicId);
    const payload = { topicId: topicIdHex, touch };
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const target of roots) this.dht.sendDirect(target, 'pubsub:touch-k', payload);
        // Apply locally too if we're one of the roots (sendDirect to self may
        // be a no-op on some transports).
        if (roots.includes(this.nodeId)) this._handleTouch(topicId, touch);
        return;
      }
    }
    // Axonal/routed fallback.
    this.dht.routeMessage(topicId, 'pubsub:touch', payload);
  }

  /**
   * Remove a topic's message queue (Phase A #3) — owner-only.  Routes the
   * signed `unpub` to the topic's R root axons.  Fire-and-forget.
   *
   * @param {bigint} topicId
   * @param {object} unpub   signed unpub object (see pubsub/unpub.js).
   */
  pubsubUnpub(topicId, unpub) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubUnpub: topicId must be bigint, got ${typeof topicId}`);
    }
    this._asyncUnpub(topicId, unpub)
      .catch(err => console.error('AxonaManager: unpub failed:', err));
  }

  /** @private — route the unpub to R root axons (mirrors _asyncKill). */
  async _asyncUnpub(topicId, unpub) {
    const payload = { topicId: toHex(topicId), unpub };
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const target of roots) this.dht.sendDirect(target, 'pubsub:unpub-k', payload);
        if (roots.includes(this.nodeId)) await this._handleUnpub(topicId, unpub);
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:unpub', payload);
  }

  /**
   * @private — burst-send pattern.  topicId is BigInt throughout the
   * body; wire payloads carry hex.
   */
  async _asyncPublish(topicId, json, publishId, publishTs, meta) {
    const postHash   = meta?.postHash   || null;
    const publisher  = meta?.publisher  || null;     // hex
    const references = meta?.references || null;

    const topicIdHex = toHex(topicId);
    // For every PostRef in this publish, route a reshare notify.
    // ref.topic_id is hex on the wire.
    if (postHash && Array.isArray(references) && references.length > 0) {
      for (const ref of references) {
        if (!ref?.topic_id || !ref?.post_hash) continue;
        const refTopicBig = isHexId(ref.topic_id) ? fromHex(ref.topic_id) : ref.topic_id;
        this.dht.routeMessage(refTopicBig, 'pubsub:reshareNotify', {
          refTopicId:        ref.topic_id,     // hex on wire
          refPostHash:       ref.post_hash,
          resharerTopicId:   topicIdHex,        // hex on wire
          resharerPostHash:  postHash,
          resharerPublisher: publisher,         // hex on wire
        });
      }
    }

    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const target of roots) {
          // target is BigInt; sendDirect takes BigInt; wire payload is hex.
          this.dht.sendDirect(target, 'pubsub:publish-k', {
            topicId: topicIdHex, json, publishId, publishTs,
            postHash, publisher,
          });
        }
        return;
      }
    }

    // NX-17 axonal mode with cross-fragment redundancy.
    const K = (this.crossFragmentRoots ?? 0) + 1;
    if (K > 1 && typeof this.dht.findKClosest === 'function') {
      const targets = await this._findKClosest(topicId, K);
      if (targets.length > 0) {
        for (const target of targets) {
          if (target === this.nodeId) continue;
          this.dht.sendDirect(target, 'pubsub:publish-k', {
            topicId: topicIdHex, json, publishId, publishTs,
            postHash, publisher,
          });
        }
        return;
      }
    }
    // Fallback (K=1 or no findKClosest): single routed walk.
    this.dht.routeMessage(topicId, 'pubsub:publish', {
      topicId: topicIdHex, json, publishId, publishTs, postHash, publisher,
    });
  }

  /**
   * Subscribe to a topic.
   *
   * @param {bigint} topicId  BigInt 264-bit topic id.
   */
  pubsubSubscribe(topicId) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubSubscribe: topicId must be bigint, got ${typeof topicId}`);
    }
    this.mySubscriptions.set(topicId, { subscribedAt: this._now() });
    const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
    this._asyncSubscribe(topicId, lastSeenTs)
      .catch(err => console.error('AxonaManager: subscribe failed:', err));
  }

  /** @private — burst-send pattern. */
  async _asyncSubscribe(topicId, lastSeenTs) {
    const topicIdHex   = toHex(topicId);
    const selfHex      = toHex(this.nodeId);
    const have         = this._haveFor(topicId);     // gap-safe digest: what we already hold
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        // peerRoots on the wire is hex[].
        const rootsHex = roots.map(r => typeof r === 'bigint' ? toHex(r) : r);
        for (const peerId of roots) {
          this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
            topicId: topicIdHex, subscriberId: selfHex,
            peerRoots: rootsHex, lastSeenTs, have,
          });
        }
        return;
      }
    }
    const K = (this.crossFragmentRoots ?? 0) + 1;
    if (K > 1 && typeof this.dht.findKClosest === 'function') {
      const targets = await this._findKClosest(topicId, K);
      if (targets.length > 0) {
        const targetsHex = targets.map(t => typeof t === 'bigint' ? toHex(t) : t);
        for (const target of targets) {
          if (target === this.nodeId) continue;
          this.dht.sendDirect(target, 'pubsub:subscribe-k', {
            topicId: topicIdHex, subscriberId: selfHex,
            peerRoots: targetsHex, lastSeenTs, have,
          });
        }
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:subscribe', {
      topicId: topicIdHex, subscriberId: selfHex, lastSeenTs, have,
    });
  }

  pubsubUnsubscribe(topicId) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubUnsubscribe: topicId must be bigint, got ${typeof topicId}`);
    }
    this.mySubscriptions.delete(topicId);
    // Forget this node's consumption state so a later since:'all' resubscribe
    // truly replays + re-delivers (the "re-subscribed hashtag never reappears"
    // bug). Safe even if we still HOST the topic — only subscriber-side state.
    this.pubsubResetTopicConsumption(topicId);
    this._asyncUnsubscribe(topicId)
      .catch(err => console.error('AxonaManager: unsubscribe failed:', err));
  }

  /**
   * Forget this node's CONSUMPTION state for a topic so a subsequent
   * `since:'all'` subscribe truly replays from the roots AND re-delivers to the
   * app. Three per-topic structures otherwise survive an unsub and each one
   * silently suppresses redelivery:
   *
   *   - `_haveByTopic`        — the gap-safe digest we send on (re)subscribe. If
   *     retained, the roots compute `missed = []` (we claim to already hold
   *     everything) and replay NOTHING. This is the dominant masking layer:
   *     it overrides even a `lastSeenTs = 0` floor, because the root's
   *     `Array.isArray(have)` branch takes precedence over the ts branch.
   *   - `_lastSeenTsByTopic`  — the legacy replay-floor timestamp.
   *   - `_appDelivered` (this topic's entries) — the exactly-once app gate; a
   *     replayed message whose publishId was delivered before the unsub is
   *     dropped in `_deliverToApp` before it reaches the handler.
   *
   * Does NOT touch the node's ROOT/host role (`axonRoles`/`replayCache`) — only
   * its subscriber-side delivery state — so a node that also hosts the topic
   * keeps serving others. Wire-compatible: it changes only what THIS node asks
   * for on its next subscribe.
   *
   * @param {bigint} topicId
   */
  pubsubResetTopicConsumption(topicId) {
    if (typeof topicId !== 'bigint') return;
    this._haveByTopic.delete(topicId);
    this._lastSeenTsByTopic.delete(topicId);
    const prefix = `${topicId}:`;
    for (const k of this._appDelivered.keys()) {
      if (typeof k === 'string' && k.startsWith(prefix)) this._appDelivered.delete(k);
    }
  }

  /** @private — burst-send pattern. */
  async _asyncUnsubscribe(topicId) {
    const topicIdHex = toHex(topicId);
    const selfHex    = toHex(this.nodeId);
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const peerId of roots) {
          this.dht.sendDirect(peerId, 'pubsub:unsubscribe-k', {
            topicId: topicIdHex, subscriberId: selfHex,
          });
        }
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:unsubscribe', {
      topicId: topicIdHex, subscriberId: selfHex,
    });
  }

  /** True when the underlying transport supports K-closest lookup. */
  _useKClosestMode() {
    return this.rootSetSize > 0 && typeof this.dht.findKClosest === 'function';
  }

  /**
   * Host a topic (store + serve for others) WITHOUT subscribing as a
   * consumer. Announces this node into the topic's K-closest set using the
   * SAME `pubsub:subscribe-k` heartbeat a subscriber uses — so the node is
   * surfaced in neighbors' synaptomes and recruited as a root/replica, and
   * the roots backfill it (via the `have` digest in `_asyncSubscribe`) so it
   * can serve replays. The difference from `pubsubSubscribe` is purely local:
   * the topic is tracked in `_hostedTopics` (re-announced every refresh) and
   * NOT in `mySubscriptions`, and the AxonaPeer layer registers no
   * Subscription/handler — so nothing is delivered to a local application.
   *
   * Wire-compatible with every existing kernel: a root processing our
   * subscribe-k can't tell a host from a subscriber, so no flag day.
   *
   * @param {bigint} topicId
   */
  pubsubHost(topicId) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubHost: topicId must be bigint, got ${typeof topicId}`);
    }
    this._hostedTopics.set(topicId, { hostedAt: this._now() });
    const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
    this._asyncSubscribe(topicId, lastSeenTs)
      .catch(err => console.error('AxonaManager: host failed:', err));
  }

  /**
   * Stop hosting a topic. Best-effort tells the root set to drop us
   * (unsubscribe-k removes only our OWN id — the B-1 invariant), and the
   * recruited role lapses naturally once traffic stops reaching us.
   * @param {bigint} topicId
   */
  pubsubUnhost(topicId) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubUnhost: topicId must be bigint, got ${typeof topicId}`);
    }
    this._hostedTopics.delete(topicId);
    this._asyncUnsubscribe(topicId).catch(() => { /* best-effort */ });
  }

  /**
   * Toggle keyspace hosting — volunteer as a host for whatever topics land
   * near this node's id. When on, `_announceKeyspace` runs now and on every
   * refresh tick.
   * @param {boolean} [on=true]
   */
  pubsubHostKeyspace(on = true) {
    this._hostKeyspace = !!on;
    if (this._hostKeyspace) {
      this._announceKeyspace().catch(err => console.error('AxonaManager: host-keyspace failed:', err));
    }
  }

  /**
   * Announce this node toward its own keyspace neighborhood so neighbors keep
   * it in their synaptome and surface it in find_closest_set responses — the
   * prerequisite for demand-driven recruitment as a root for nearby topics.
   * Reuses subscribe-k under topicId = our own nodeId (a self-anchored,
   * never-published "topic"); the role neighbors create for it is harmless
   * keyspace bookkeeping that TTLs out, while the side effect — being known —
   * is exactly what gets us recruited for the REAL topics near us.
   * @private
   */
  async _announceKeyspace() {
    if (!this._useKClosestMode()) return;
    const selfHex   = toHex(this.nodeId);
    const neighbors = await this._findKClosest(this.nodeId, this.rootSetSize);
    if (!neighbors || neighbors.length === 0) return;
    const neighborsHex = neighbors.map(r => typeof r === 'bigint' ? toHex(r) : r);
    for (const peerId of neighbors) {
      if (peerId === this.nodeId) continue;
      const p = this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
        topicId: selfHex, subscriberId: selfHex, peerRoots: neighborsHex,
      });
      if (p?.catch) p.catch(() => {});
    }
  }

  /**
   * B-2 proximity gate — may this node legitimately HOST (become a
   * root/axon for) `topicId`?  A node should only allocate a role +
   * replay cache for a topic it is plausibly among the K-closest to.
   * Without this, an attacker floods publishes for random topicIds and
   * every node on the delivery path lazily promotes itself, allocating
   * unbounded roles + caches → heap exhaustion / GC thrash (browser peers
   * crash).
   *
   * Cheap + local: `dht.findKClosest` here is a synaptome distance scan
   * (no network probe), so this is just "is self among the K ids I know
   * closest to the topic."  Fails OPEN when there's no local distance
   * view to consult (sim transports without findKClosest, an empty
   * synaptome, or an error) — those cases can't host an attack and we
   * must never let the gate drop legitimate delivery.
   *
   * @param {bigint} topicId
   * @returns {Promise<boolean>}
   */
  async _mayHostTopic(topicId) {
    if (!this._useKClosestMode()) return true;       // no local distance view → legacy behavior
    try {
      const closest = await this.dht.findKClosest(topicId, this.rootSetSize);
      if (!Array.isArray(closest) || closest.length === 0) return true;
      return closest.some(p => p === this.nodeId);
    } catch {
      return true;                                   // never let the gate break delivery
    }
  }

  /**
   * B-4 ingress signature check — may this publish be cached + fanned out?
   * Run at a root axon (the topic's K-closest ingress) BEFORE caching or
   * fan-out, so a flood of junk publishes carrying SPOOFED signatures is
   * dropped at the edge instead of being amplified through the axonal tree
   * (and replay cache) before the leaf nodes reject it.
   *
   * Only signed envelopes are gated: a payload that claims an Ed25519
   * `signature` must verify (publisher pubkey ↔ signature over the signed
   * core).  Unsigned/anonymous publishes and non-envelope/legacy payloads
   * carry no signature to forge, so they pass here and are validated (if
   * applicable) at the application edge as before.
   *
   * @param {string} json  serialized envelope (JSON.stringify of envelope)
   * @returns {Promise<boolean>}
   */
  async _publishSignatureOk(json) {
    if (typeof json !== 'string') return true;
    let env;
    try { env = JSON.parse(json); } catch { return true; }   // not an envelope → nothing to forge
    if (!env || typeof env !== 'object' || typeof env.signature !== 'string') return true; // unsigned
    try {
      const res = await verifyEnvelope(env);
      if (res?.ok !== true) return false;
      // Stage 2: publish-PoW gate (the §7a anti-flood anchor). Self-binding to
      // signerPubkey, so it is checked independently of the signature. INERT at
      // difficulty 0 — any/absent signerPow passes, so envelopes that predate
      // the field are accepted. Raising publish difficulty (Stage 4b) makes a
      // fresh signing key cost a puzzle, so a flooder can't rotate keys to dodge
      // the per-publisher quota for free.
      return powVerify({
        pubkeyHex: env.signerPubkey,
        nonce:     typeof env.signerPow === 'string' ? env.signerPow : '',
        role:      'publish',
      });
    } catch {
      return false;                                  // claims a signature but verification threw → drop
    }
  }

  /**
   * v0.3 topic-policy gate (run at a root, AFTER `_publishSignatureOk`). Two checks
   * keyed off the SIGNED topic descriptor `env.topic = { region, owner, name, write }`:
   *   1. Binding — recompute the topic id from the descriptor and require it equals
   *      the topic this message was routed to. Stops a message signed for topic A
   *      from being injected onto topic B (the descriptor is under the signature).
   *   2. Write policy — for `write: 'owner'`, the `signerPubkey` MUST equal the
   *      descriptor's `owner` (only the owner key may publish to an owned topic).
   * Envelopes missing a v3 descriptor are dropped at a root (flag-day boundary).
   * @param {string} json     serialized envelope
   * @param {bigint} topicId  the routed topic id
   * @returns {Promise<boolean>}
   */
  async _topicPolicyOk(json, topicId) {
    if (typeof json !== 'string') return true;          // non-string payload → nothing to bind
    let env;
    try { env = JSON.parse(json); } catch { return true; }
    if (!env || typeof env !== 'object' || !('message' in env)) return true; // not an envelope
    const d = env.topic;
    if (!d || typeof d !== 'object' || typeof d.name !== 'string') return false; // pre-v3 / malformed → drop at root
    let resolved;
    try { resolved = await resolveTopic(d); } catch { return false; }
    if (BigInt('0x' + resolved.topicId) !== topicId) return false;           // descriptor ≠ routed topic
    if (resolved.write === 'owner') {
      const signer = typeof env.signerPubkey === 'string' ? env.signerPubkey.toLowerCase() : null;
      if (!signer || signer !== resolved.owner) return false;                // owner-only: signer must be owner
    }
    return true;
  }

  /**
   * Reconcile the UNSIGNED wire `postHash` against the content-derived msgId of
   * the (already signature-verified) envelope.  postHash is the key the replay
   * cache, pull(), and kill()/tombstones all index on — but it travels as a
   * sibling wire field, NOT inside the signed bytes, so without this check a
   * publisher or relay could cache content under a postHash that is NOT its
   * true content hash.  That would defeat the v2.18.0 content-address guarantee
   * (pull(realMsgId) would miss, kill(realMsgId) would never match → an
   * un-killable message), and let an attacker poison a different message's id.
   *
   * The honest publish path sets `postHash = envelope.msgId = computeMsgId(...)`
   * (AxonaPeer.pub), so this is a no-op for honest traffic; it only drops a
   * tampered postHash.  Non-envelope / message-less payloads carry no canonical
   * content hash to reconcile and pass through (matching `_publishSignatureOk`'s
   * "nothing to forge" stance) — the protection targets the envelope path,
   * which is the only one `pub()` produces.
   *
   * @param {string} json          serialized envelope
   * @param {string} wirePostHash  the unsigned wire postHash
   * @returns {Promise<boolean>}   true if consistent (or nothing to reconcile)
   */
  async _postHashConsistent(json, wirePostHash) {
    // Only a PRESENT postHash is reconciled. An absent one asserts no content
    // address to verify — the message is simply un-addressable (pull/kill by id
    // can't reach it), which is no worse than before and never a poisoning
    // lever; the attack the reconciliation closes requires a postHash set to a
    // SPECIFIC value, which is exactly the present case. Honest publishes always
    // carry postHash = envelope.msgId (AxonaPeer.pub), so this never drops them.
    if (typeof wirePostHash !== 'string' || wirePostHash.length === 0) return true;
    if (typeof json !== 'string') return true;
    let env;
    try { env = JSON.parse(json); } catch { return true; }
    if (!env || typeof env !== 'object' || !('message' in env)) return true;
    const publisher = (typeof env.signature === 'string' && typeof env.signerPubkey === 'string')
      ? env.signerPubkey : null;
    let expected;
    try { expected = await computeMsgId({ publisher, message: env.message }); }
    catch { return false; }                          // can't hash the content → suspicious, drop
    return wirePostHash === expected;
  }

  /**
   * C-2 ingress freshness + per-publisher monotonic-seq gate.  Run at a
   * root axon on the LIVE-publish path ONLY (never on the replay path,
   * which deliberately serves cached history older than the freshness
   * window to late subscribers).  Call AFTER `_publishSignatureOk` so the
   * signed `ts`/`seq` are known genuine before they are trusted.
   *
   * Two reinforcing checks, both keyed off SIGNED fields (the unsigned wire
   * `publishTs` is attacker-controlled on a replay and is never consulted):
   *   1. Freshness — reject if the signed `ts` is outside ±MAX_PUBLISH_SKEW_MS
   *      of local time.  Kills re-injection of a captured envelope once it
   *      ages past the window.
   *   2. Monotonic seq — reject a signed envelope whose `seq` is more than
   *      SEQ_REORDER_TOLERANCE_MS behind the publisher's high-water mark
   *      (a replay from earlier in the stream).  Advance the high-water on
   *      accept.  Fail-open on a cold entry.
   *
   * Only signed envelopes are gated.  Unsigned/anonymous and non-envelope
   * payloads carry no attacker-immutable ts/seq, so freshness/ordering are
   * not meaningful for them (they have no replay protection by nature);
   * they pass here exactly as before.
   *
   * @param {string} json  serialized envelope
   * @param {number} now   local time (ms)
   * @returns {{ok: boolean, reason?: string}}
   */
  _publishFreshAndOrdered(json, now) {
    if (typeof json !== 'string') return { ok: true };
    let env;
    try { env = JSON.parse(json); } catch { return { ok: true }; }
    if (!env || typeof env !== 'object' || typeof env.signature !== 'string') {
      return { ok: true };                           // unsigned / non-envelope → not gated
    }
    // 1. Freshness window on the signed ts.
    const fresh = checkFreshness(env, { now, maxSkewMs: MAX_PUBLISH_SKEW_MS });
    if (!fresh.ok) return { ok: false, reason: fresh.reason };
    // 2. Per-publisher monotonic seq.
    const key = env.signerPubkey;
    if (typeof key === 'string' && typeof env.seq === 'number') {
      const hw = this._publisherSeq.get(key);
      if (hw !== undefined && env.seq <= hw - SEQ_REORDER_TOLERANCE_MS) {
        return { ok: false, reason: 'replay_seq' };
      }
      if (hw === undefined || env.seq > hw) {
        // LRU-touch: delete + re-insert so this publisher moves to the MRU end.
        // Critical for replay protection — a plain Map.set on an existing key
        // keeps its ORIGINAL insertion position, so _capStore (drop oldest-
        // inserted) would evict the longest-ACTIVE publishers first, reopening
        // their replay window precisely for the publishers that matter most.
        // With the touch, the cap evicts genuinely-idle publishers instead.
        this._publisherSeq.delete(key);
        this._publisherSeq.set(key, env.seq);
        this._capStore(this._publisherSeq, this._publisherSeqCap);
      }
    }
    return { ok: true };
  }

  // ── Kill (creator-only retraction, Phase A #2) ─────────────────────────

  /** Routed kill ingress.  Returns a routing verdict ('consumed'|'forward'). */
  async _onKill(payload, meta) {
    return this._handleKill(_wire(payload.topicId), payload?.kill);
  }

  /** Direct (K-closest) kill ingress.  We're a targeted root; verdict unused. */
  async _onKillDirect(payload, meta) {
    await this._handleKill(_wire(payload.topicId), payload?.kill);
  }

  /**
   * Core kill handler (shared by routed + direct paths).  Verifies the kill
   * signature + freshness, dedups, then — if we host the topic and hold the
   * target message — checks the kill signer matches the MESSAGE signer
   * (creator-only), drops it from the replay cache, tombstones the msgId so
   * a lagging replica can't resurrect it, and delivers a delete marker to
   * subscribers.  Self-authenticating: authority to kill IS authorship.
   *
   * @returns {Promise<'consumed'|'forward'>}
   */
  async _handleKill(topicId, kill) {
    // 1. signature.
    let v;
    try { v = await verifyKill(kill); } catch { v = { ok: false }; }
    if (!v.ok) { this._emitLog?.('debug', 'kill-bad-signature-dropped', {}); return 'consumed'; }
    // 2. freshness on the kill's OWN ts (anti-replay of the kill).
    if (!checkFreshness(kill, { now: this._now() }).ok) {
      this._emitLog?.('debug', 'kill-stale-dropped', {}); return 'consumed';
    }
    // 3. dedup by kill signature.
    if (this._seenKills.has(kill.signature)) return 'consumed';
    this._seenKills.set(kill.signature, this._now());
    this._capStore(this._seenKills, this._seenKillCap);
    // 4. do we host the topic?
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';                       // not hosting → route onward to a root
    // 5. find the target message in our replay cache.
    const cache = role.replayCache || [];
    const idx   = cache.findIndex(e => e.postHash === kill.msgId);
    if (idx === -1) return 'forward';                  // not here (yet/anymore) → let a root with it act
    // 6. authorize: the kill signer MUST be the message signer (creator-only).
    let env = null;
    try { env = JSON.parse(cache[idx].json); } catch { /* unparseable */ }
    const msgSigner = (env && typeof env.signerPubkey === 'string') ? env.signerPubkey : null;
    if (!msgSigner || msgSigner !== kill.signerPubkey) {
      this._emitLog?.('debug', 'kill-unauthorized-dropped', { msgId: kill.msgId });
      return 'consumed';                               // wrong signer (or unsigned msg) → reject, handled
    }
    // 7. retract: drop EVERY cache copy of this content (SP-11 — identical-content
    //    publishes share a content msgId but cache as separate entries, so a
    //    single splice would leave duplicates alive; same msgId ⟹ same publisher
    //    ⟹ same signer, so the authorization above covers all of them) +
    //    tombstone so a lagging replica can't resurrect it.
    const topicName = (env && typeof env.topic === 'string') ? env.topic : null;
    role.replayCache = cache.filter(e => e.postHash !== kill.msgId);
    this._addTombstone(kill.msgId);
    // Retain the SIGNED kill (now proven creator-authorized against the message
    // we hold) so refreshTick can re-gossip it to the current root set, healing
    // any replica that missed it. Upgrades the dedup entry set at step 3.
    this._seenKills.set(kill.signature, { at: this._now(), kill, topicId });
    this._lastKillAt = this._now();
    // 8. purge subscribers — delete-marked delivery on their sub() handler.
    const deleteJson = JSON.stringify({ deleted: true, msgId: kill.msgId, topic: topicName });
    const deliveryId = `kill:${kill.msgId}`;
    const topicIdHex = toHex(topicId);
    const now = this._now();
    const dead = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) { this._deliverToApp(topicId, deleteJson, deliveryId, now); continue; }
      const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
        topicId: topicIdHex, json: deleteJson, publishId: deliveryId, publishTs: now, postHash: kill.msgId,
      });
      if (!ok) dead.push(childId);
    }
    for (const d of dead) role.children.delete(d);
    this._emitLog?.('debug', 'kill-applied', { topicId: topicIdHex, msgId: kill.msgId });
    return 'consumed';
  }

  /**
   * Sibling-root kill reconciliation ingress (Phase A #2). A peer root re-sends
   * us the signed kills it has applied; we re-run each through the full verifier
   * (signature + freshness + dedup + creator-authorization against the message
   * we hold), so a replica that missed the original kill removes + tombstones
   * the message. Self-authorizing — an unauthorized or bogus kill is rejected by
   * _handleKill exactly as on the primary path, so this is not a censorship lever.
   */
  async _onKillSync(payload, meta) {
    const topicId = _wireSafe(payload?.topicId);
    if (topicId == null) return;                                 // malformed frame → drop, never throw
    const kills = Array.isArray(payload?.kills) ? payload.kills.slice(0, MAX_KILL_SYNC) : [];
    for (const kill of kills) {
      try { await this._handleKill(topicId, kill); } catch { /* per-kill best effort */ }
    }
  }

  /**
   * Push the kills we've recently applied for `topicId` to the CURRENT K-closest
   * root set, healing any replica that missed the original kill (churn / dropped
   * delivery / late-joining root) — this is what closes "kill, reload, it comes
   * back". No-op when there are no in-window kills, so steady-state cost is zero.
   * Called from refreshTick (itself gated on _lastKillAt).
   */
  async _syncKillsForTopic(topicId, role) {
    if (!role || !(role.isRoot || role.isInRootSet)) return;
    if (!this._useKClosestMode()) return;
    const now = this._now();
    const kills = [];
    for (const v of this._seenKills.values()) {
      if (v && typeof v === 'object' && v.kill && v.topicId === topicId && (now - v.at) < KILL_REGOSSIP_MS) {
        kills.push(v.kill);
        if (kills.length >= MAX_KILL_SYNC) break;
      }
    }
    if (kills.length === 0) return;
    const roots = await this._findKClosest(topicId, this.rootSetSize);
    const payload = { topicId: toHex(topicId), kills };
    for (const target of roots) {
      if (target === this.nodeId) continue;
      this.dht.sendDirect(target, 'pubsub:kill-sync', payload);
    }
  }

  // ── Fix 2: root-to-root message anti-entropy ───────────────────────────

  /**
   * Ask the current K-closest sibling roots for whatever this topic's replay
   * cache is MISSING, by sending each our digest of held postHashes. A sibling
   * replies (pubsub:msgsync-resp) with the complement, which we re-verify and
   * ingest. Steady-state (converged) ⇒ siblings reply with nothing.
   */
  async _antiEntropyTopic(topicId, role) {
    if (!role || !(role.isRoot || role.isInRootSet)) return;
    if (!this._useKClosestMode()) return;
    const roots = await this._findKClosest(topicId, this.rootSetSize);
    const siblings = roots.filter((r) => r !== this.nodeId);
    if (siblings.length === 0) return;
    const have = (role.replayCache || []).map((e) => e.postHash).filter(Boolean).slice(0, MAX_HAVE);
    const payload = { topicId: toHex(topicId), have, requesterId: toHex(this.nodeId) };
    for (const sib of siblings) this.dht.sendDirect(sib, 'pubsub:msgsync', payload);
    // No longer cold: we've initiated reconciliation against the current sibling
    // set, so the cold-drain stops prioritizing this role and the steady
    // round-robin carries it from here. (Only set once we HAD siblings to ask —
    // the early-return above leaves a siblingless role cold so it retries.)
    role.synced = true;
  }

  /** Sibling asked what they're missing: reply with the cache entries whose
   *  postHash isn't in their `have` digest (bounded, tombstones excluded). */
  async _onMsgSync(payload, meta) {
    const topicId = _wireSafe(payload?.topicId);
    if (topicId == null) return;                                 // malformed frame → drop, never throw
    const role = this.axonRoles.get(topicId);
    if (!role || !(role.isRoot || role.isInRootSet)) return;     // only a hosting root answers
    const requester = _wireSafe(meta?.fromId) ?? _wireSafe(payload?.requesterId);
    if (requester == null || requester === this.nodeId) return;
    this._sweepRole(role, this._now());
    const have = new Set(Array.isArray(payload.have) ? payload.have.slice(0, MAX_HAVE) : []);
    const missing = (role.replayCache || [])
      .filter((e) => e.postHash && !have.has(e.postHash) && !this._isTombstoned(e.postHash))
      .slice(0, MAX_REPLAY_BATCH);
    if (missing.length === 0) return;
    // Same 16 KiB per-frame bound as replay (O-5) — large chunked content
    // backfills between roots as multiple frames, not one oversize one.
    await this._sendFramedMessages(requester, 'pubsub:msgsync-resp', toHex(topicId), missing,
      (topicHex, batch) => ({ topicId: topicHex, messages: batch }));
  }

  /** Sibling's reply: re-verify + ingest each message we were missing. */
  async _onMsgSyncResp(payload, meta) {
    const topicId = _wireSafe(payload?.topicId);
    if (topicId == null) return;                                 // malformed frame → drop, never throw
    const role = this.axonRoles.get(topicId);
    if (!role || !(role.isRoot || role.isInRootSet)) return;
    const batch = Array.isArray(payload.messages) ? payload.messages.slice(0, MAX_REPLAY_BATCH) : [];
    let added = 0;
    for (const msg of batch) { if (await this._ingestSyncedMessage(topicId, role, msg)) added++; }
    if (added) this._emitLog?.('debug', 'msgsync-ingested', { topicId: toHex(topicId), added });
  }

  /**
   * Ingest a message pulled from a sibling root. SECURITY: a sibling root is NOT
   * trusted — re-verify the publisher signature + postHash integrity (the same
   * B-4 checks as live publish ingress). We deliberately SKIP the C-2 freshness/
   * seq check: backfilling older messages is the whole point of anti-entropy,
   * and signature + postHash already prove authenticity + integrity.
   * @returns {Promise<boolean>} true if newly cached.
   */
  async _ingestSyncedMessage(topicId, role, msg) {
    const { json, publishId, publishTs, postHash } = msg || {};
    let { publisher } = msg || {};
    if (typeof json !== 'string' || json.length > MAX_PUBLISH_BYTES) return false;
    if (typeof postHash !== 'string' || postHash.length === 0) return false;
    if (this._isTombstoned(postHash)) return false;
    if ((role.replayCache || []).some((e) => e.postHash === postHash)) return false;   // already hold (by content)
    if (!(await this._publishSignatureOk(json))) return false;          // forged signed envelope → drop
    if (!(await this._topicPolicyOk(json, topicId))) return false;      // descriptor/write-policy violation → drop
    if (!(await this._postHashConsistent(json, postHash))) return false; // poisoned content address → drop
    if (typeof publisher === 'string') { try { publisher = _wire(publisher); } catch { publisher = null; } }
    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });
    this._recordReceived(topicId, publishId, publishTs);
    this._recordHave(topicId, postHash);
    // Fan out to any local subscriber children that missed this publisher's feed.
    const topicIdHex = toHex(topicId);
    const publisherHex = publisher == null ? publisher : toHex(publisher);
    for (const [childId] of role.children) {
      if (childId === this.nodeId) { this._deliverToApp(topicId, json, publishId, publishTs, postHash); continue; }
      this.dht.sendDirect(childId, 'pubsub:deliver', { topicId: topicIdHex, json, publishId, publishTs, postHash, publisher: publisherHex });
    }
    return true;
  }

  // ── Touch (creator-only keep-alive, Phase A #7) ────────────────────────

  /** Routed touch ingress.  Returns a routing verdict ('consumed'|'forward'). */
  async _onTouch(payload, meta) {
    return this._handleTouch(_wire(payload.topicId), payload?.touch);
  }

  /** Direct (K-closest) touch ingress.  We're a targeted root; verdict unused. */
  async _onTouchDirect(payload, meta) {
    await this._handleTouch(_wire(payload.topicId), payload?.touch);
  }

  /**
   * Core touch handler (shared by routed + direct paths).  Verifies the touch
   * signature + freshness, dedups, then — if we host the topic and hold the
   * target message — checks the touch signer matches the MESSAGE signer
   * (creator-only) and refreshes the entry: resets its hold-time expiry to
   * `now + hold` (bounded by the absolute 48h ceiling, so a touch can never
   * pin a message past the cap a pull also respects), bumps its eviction
   * recency, and moves it to the head of the replay queue.  Self-authenticating:
   * the right to keep a message alive IS authorship.
   *
   * @returns {Promise<'consumed'|'forward'>}
   */
  async _handleTouch(topicId, touch) {
    // 1. signature.
    let v;
    try { v = await verifyTouch(touch); } catch { v = { ok: false }; }
    if (!v.ok) { this._emitLog?.('debug', 'touch-bad-signature-dropped', {}); return 'consumed'; }
    // 2. freshness on the touch's OWN ts (anti-replay of the touch).
    if (!checkFreshness(touch, { now: this._now() }).ok) {
      this._emitLog?.('debug', 'touch-stale-dropped', {}); return 'consumed';
    }
    // 3. dedup by touch signature.
    if (this._seenTouches.has(touch.signature)) return 'consumed';
    this._seenTouches.set(touch.signature, this._now());
    this._capStore(this._seenTouches, this._seenTouchCap);
    // 4. do we host the topic?
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';                       // not hosting → route onward to a root
    // 5. find the target message in our replay cache.
    const cache = role.replayCache || [];
    const idx   = cache.findIndex(e => e.postHash === touch.msgId);
    if (idx === -1) return 'forward';                  // not here (yet/anymore) → let a root with it act
    // 6. authorize by TOPIC OWNERSHIP (not message authorship).  A topic is
    //    "owned" iff its anchor is a real identity — a publisher nodeId whose
    //    low 256 bits are sha256(pubkey).  Two anchor shapes are UNOWNED:
    //    public topics (null anchor) and synthetic regional anchors
    //    (`prefix‖0^256`).  The rule the app asked for:
    //      · unowned topic → ANYONE may touch (any valid, fresh, signed touch);
    //      · owned topic   → only the OWNER may — the touch signer's pubkey must
    //        hash to the anchor's 256-bit suffix (the same pubkey↔nodeId bind
    //        `unpub` uses; the 8-bit geo prefix is the owner's own choice, so
    //        only the suffix is checked).
    const entry  = cache[idx];
    const LOW256 = (1n << 256n) - 1n;
    const anchor = (typeof entry.publisher === 'bigint') ? entry.publisher : null;
    const owned  = anchor !== null && (anchor & LOW256) !== 0n;
    if (owned) {
      let pubSuffix = null;
      try {
        const pkBytes = new Uint8Array((touch.signerPubkey.match(/../g) || []).map(h => parseInt(h, 16)));
        pubSuffix = await sha256Hex(pkBytes);
      } catch { /* malformed pubkey */ }
      const ownerSuffix = (anchor & LOW256).toString(16).padStart(64, '0');
      if (!pubSuffix || pubSuffix !== ownerSuffix) {
        this._emitLog?.('debug', 'touch-not-owner-dropped', { msgId: touch.msgId });
        return 'consumed';                             // not the owner of an owned topic → reject
      }
    }
    // 7. keep-alive: reset hold (bounded by ceiling), bump recency, move to head.
    const now    = this._now();
    const holdMs = Math.min(role.maxHoldMs || DEFAULT_HOLD_MS, MAX_HOLD_MS);
    if (typeof entry.ceilingAt !== 'number') {
      entry.ceilingAt = (typeof entry.ts === 'number' ? entry.ts : now) + MAX_HOLD_MS;
    }
    entry.expiresAt = Math.min(now + holdMs, entry.ceilingAt);
    // `touchedTs` is the touch's SIGNED ts (== now at the publisher); it
    // dominates eviction ordering (see _orderLt) so a touched message is
    // evicted last, and is identical across all K roots that apply the same
    // touch — keeping replicas convergent.
    entry.touchedTs = touch.ts;
    cache.splice(idx, 1);
    cache.unshift(entry);
    this._emitLog?.('debug', 'touch-applied', { topicId: toHex(topicId), msgId: touch.msgId });
    return 'consumed';
  }

  /** Record a tombstone for a killed msgId (bounded LRU). */
  _addTombstone(msgId) {
    if (!msgId) return;
    this._tombstones.set(msgId, this._now() + TOMBSTONE_TTL_MS);
    if (this._tombstones.size > MAX_TOMBSTONES) {
      const toDrop = MAX_TOMBSTONES / 2; let i = 0;
      for (const k of this._tombstones.keys()) { if (i++ >= toDrop) break; this._tombstones.delete(k); }
    }
  }

  /** Is `msgId` tombstoned (killed, still within the tombstone window)? */
  _isTombstoned(msgId) {
    if (!msgId) return false;
    const exp = this._tombstones.get(msgId);
    if (exp === undefined) return false;
    if (this._now() > exp) { this._tombstones.delete(msgId); return false; }
    return true;
  }

  // ── Hold time / expiry (Phase A #5) ────────────────────────────────────

  /** Has this cache entry passed its hold-time expiry? */
  _isExpired(entry, now = this._now()) {
    return typeof entry?.expiresAt === 'number' && now > entry.expiresAt;
  }

  /** Drop expired entries from one role's replay cache (in place). */
  _sweepRole(role, now = this._now()) {
    const cache = role?.replayCache;
    if (!cache || cache.length === 0) return;
    for (let i = cache.length - 1; i >= 0; i--) {
      if (this._isExpired(cache[i], now)) cache.splice(i, 1);
    }
  }

  /** Sweep expired messages from every hosted topic (called on refreshTick). */
  _sweepExpired(now = this._now()) {
    for (const role of this.axonRoles.values()) this._sweepRole(role, now);
  }

  // ── Unpub (owner-only queue removal, Phase A #3) ───────────────────────

  /** Routed unpub ingress.  Returns a routing verdict. */
  async _onUnpub(payload, meta) {
    return this._handleUnpub(_wire(payload.topicId), payload?.unpub);
  }

  /** Direct (K-closest) unpub ingress.  Verdict unused. */
  async _onUnpubDirect(payload, meta) {
    await this._handleUnpub(_wire(payload.topicId), payload?.unpub);
  }

  /**
   * Core unpub handler.  Verifies the unpub signature + freshness, dedups,
   * then authorizes it as coming from the topic OWNER — self-authenticatingly,
   * with no registry:
   *   (1) sha256(signerPubkey) === ownerNodeId[8:]   (pubkey ↔ nodeId bind;
   *       the 8-bit geo prefix is the owner's own choice, so only the suffix
   *       is checked), and
   *   (2) deriveTopicId(ownerNodeId, topicName) === topicId.
   * Only the genuine owner satisfies both.  On success: drop the whole
   * replay cache (tombstoning each msgId so it can't be resurrected) and,
   * when `destroy`, delete the hosting role entirely.
   *
   * @returns {Promise<'consumed'|'forward'>}
   */
  async _handleUnpub(topicId, unpub) {
    // 1. signature.
    let v;
    try { v = await verifyUnpub(unpub); } catch { v = { ok: false }; }
    if (!v.ok) { this._emitLog?.('debug', 'unpub-bad-signature-dropped', {}); return 'consumed'; }
    // 2. freshness on the unpub's own ts.
    if (!checkFreshness(unpub, { now: this._now() }).ok) {
      this._emitLog?.('debug', 'unpub-stale-dropped', {}); return 'consumed';
    }
    // 3. dedup (shares the kill seen-set; signatures are unique).
    if (this._seenKills.has(unpub.signature)) return 'consumed';
    this._seenKills.set(unpub.signature, this._now());
    // 4. owner authorization (v0.3) — self-authenticating. ownerNodeId is the
    // owner's Author ID; the signer must BE that owner, and the descriptor
    // (region, owner, name, write:'owner') must derive THIS topic. The region
    // byte is recovered from the routed topic id, so it need not travel in the unpub.
    const ownerId = typeof unpub.ownerNodeId === 'string' ? unpub.ownerNodeId.toLowerCase() : '';
    const signer  = typeof unpub.signerPubkey === 'string' ? unpub.signerPubkey.toLowerCase() : '';
    if (!ownerId || signer !== ownerId) {
      this._emitLog?.('debug', 'unpub-pubkey-not-owner', {});
      return 'consumed';                               // signer is not the claimed owner
    }
    const region = parseInt(toHex(topicId).slice(0, 2), 16);   // region byte ← routed topic id
    let derivedTopicId = null;
    try { derivedTopicId = (await resolveTopic({ region, owner: ownerId, name: unpub.topicName, write: 'owner' })).topicId; }
    catch { /* bad inputs */ }
    if (!derivedTopicId || derivedTopicId.toLowerCase() !== toHex(topicId).toLowerCase()) {
      this._emitLog?.('debug', 'unpub-topic-not-owned', {});
      return 'consumed';                               // owner didn't create this topic
    }
    // 5. authorized — find the role.
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';                       // not hosting → route onward to a root
    // 6. drop the queue + tombstone every msgId so it can't be resurrected.
    const cache = role.replayCache || [];
    for (const e of cache) this._addTombstone(e.postHash);
    role.replayCache = [];
    // 7. destroy → remove the hosting role state entirely (Phase A: no
    //    separate config/ACL objects yet; this clears all topic state we hold).
    if (unpub.destroy === true) {
      this.axonRoles.delete(topicId);
      this._receivedPublishIds.delete(topicId);
      this._lastSeenTsByTopic.delete(topicId);
    }
    this._emitLog?.('debug', 'unpub-applied', { topicId: toHex(topicId), destroy: unpub.destroy === true });
    return 'consumed';
  }

  onPubsubDelivery(callback) {
    this._deliveryCallback = callback;
  }

  // ── Handlers ────────────────────────────────────────────────────────
  //
  // Each handler converts the hex wire fields to BigInt up-front, then
  // operates in BigInt-land for the rest of the body.

  async _onSubscribe(payload, meta) {
    const topicId      = _wire(payload.topicId);
    const subscriberId = _wire(payload.subscriberId);
    const { lastSeenTs } = payload;
    const have = Array.isArray(payload.have)
      ? payload.have.slice(0, MAX_HAVE).filter(h => typeof h === 'string')
      : null;
    // B-1 (routed reflection/amplification): enrolling `subscriberId` makes
    // this node relay the topic's full feed — plus a ≤100-message replay
    // blast — directly to that id by nodeId.  On the routed path `meta.fromId`
    // is the axona/4-proven *previous hop*, which on a multi-hop route (or
    // from an attacker) is NOT the named subscriber.  So enroll ONLY the
    // authenticated channel peer — the same proven-fromId invariant the
    // direct subscribe-k path enforces (C4).  An unvouched subscriberId is
    // never seated: the message keeps routing and the genuine subscriber
    // enrolls via its own origin-checked direct path (the primary path in
    // K-closest mode; this routed path is only a no-roots fallback).
    // fromId === null ⇒ locally originated ⇒ trusted, as on the direct path.
    const fromId     = meta?.fromId == null ? null : _wire(meta.fromId);
    const vouchedFor = fromId === null || subscriberId === fromId;
    const role = this.axonRoles.get(topicId);
    const now = this._now();

    if (role) {
      // Self-subscribe path: don't register self as own child; let walker continue.
      if (subscriberId === this.nodeId) return 'forward';
      if (!vouchedFor) return 'forward';   // can't vouch for this id → keep routing, don't enroll
      await this._addOrRecruitChild(topicId, role, subscriberId, fromId);
      await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs, have);
      return 'consumed';
    }

    if (meta.isTerminal && vouchedFor) {
      // First subscriber creates the topic root — but only when its origin
      // is vouched for, so a relay can't seed a root keyed to a victim.
      this.axonRoles.set(topicId, {
        parentId:       null,
        isRoot:         true,
        children:       new Map([[subscriberId, { createdAt: now, lastRenewed: now }]]),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,
      });
      return 'consumed';
    }

    return 'forward';
  }

  // ── Shared add-or-recruit primitive ─────────────────────────────────

  /**
   * Add `subscriberId` as a child of `role`, or — if `role` is at
   * capacity — promote an existing child to sub-axon and delegate.
   *
   * @param {bigint} topicId       BigInt
   * @param {Object} role
   * @param {bigint} subscriberId  BigInt
   * @param {bigint|null} forwarderId  BigInt or null
   */
  async _addOrRecruitChild(topicId, role, subscriberId, forwarderId = null) {
    const now = this._now();
    const existing = role.children.get(subscriberId);
    if (existing) { existing.lastRenewed = now; return; }

    const topicIdHex = toHex(topicId);
    const selfHex    = toHex(this.nodeId);
    const subHex     = toHex(subscriberId);

    if (this.shouldRecruitSubAxon(role)) {
      // ── PREFERRED: batch adoption via pickRelayPeer (NX-17+) ──
      if (typeof this.pickRelayPeer === 'function') {
        const relayId = this.pickRelayPeer(role, subscriberId, forwarderId);
        if (relayId && relayId !== this.nodeId && relayId !== forwarderId
            && !role.children.has(relayId)) {
          const K = Math.max(1, Math.floor(this.maxDirectSubs / 2));
          const batch = this._selectChildrenClosestToRelay(role, relayId, K);
          batch.push(subscriberId);
          for (const id of batch) role.children.delete(id);
          role.children.set(relayId, {
            createdAt: now, lastRenewed: now, isSubaxon: true,
          });
          // Wire: subscriberIds is hex[].
          const batchHex = batch.map(id => toHex(id));
          await this.dht.sendDirect(relayId, 'pubsub:adopt-subscribers', {
            topicId:       topicIdHex,
            subscriberIds: batchHex,
          });
          return;
        }
      }

      // ── FALLBACK: legacy single-recruit from existing children ──
      const reuseId = this._pickExistingSubAxon(role, subscriberId, forwarderId);
      if (reuseId) {
        role.children.get(reuseId).lastRenewed = now;
        await this.dht.sendDirect(reuseId, 'pubsub:promote-axon', {
          topicId:         topicIdHex,
          newSubscriberId: subHex,
          parentId:        selfHex,
        });
        return;
      }

      const metaArg = { fromId: forwarderId ?? subscriberId };
      let recruitId = this.pickRecruitPeer(role, metaArg, subscriberId);
      if (!recruitId || recruitId === this.nodeId
          || recruitId === forwarderId
          || !role.children.has(recruitId)) {
        recruitId = this._pickExistingChildForRecruit(role, subscriberId, forwarderId);
      }
      if (recruitId) {
        const child = role.children.get(recruitId);
        child.lastRenewed = now;
        child.isSubaxon = true;
        await this.dht.sendDirect(recruitId, 'pubsub:promote-axon', {
          topicId:         topicIdHex,
          newSubscriberId: subHex,
          parentId:        selfHex,
        });
        return;
      }
      // No non-self child to recruit — fall through and add directly.
    }
    role.children.set(subscriberId, { createdAt: now, lastRenewed: now, isSubaxon: false });
  }

  /**
   * Top-K existing children ranked XOR-closest to relayId.  All BigInt.
   */
  _selectChildrenClosestToRelay(role, relayId, k) {
    const ranked = [];
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;
      if (childId === relayId)     continue;
      ranked.push({ childId, dist: childId ^ relayId });
    }
    ranked.sort((a, b) => (a.dist < b.dist ? -1 : a.dist > b.dist ? 1 : 0));
    return ranked.slice(0, k).map(r => r.childId);
  }

  /** [Legacy helper, kept for reference.] */
  _partitionChildrenForRelay(role, relayId) {
    const batch = [];
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;
      if (childId === relayId)     continue;
      const dRelay = childId ^ relayId;
      const dSelf  = childId ^ this.nodeId;
      if (dRelay < dSelf) batch.push(childId);
    }
    return batch;
  }

  /**
   * Pick an existing sub-axon child to route further overflow through.
   * All ids BigInt.
   */
  _pickExistingSubAxon(role, subscriberId, forwarderId) {
    const candidates = [];
    for (const [id, child] of role.children) {
      if (!child.isSubaxon) continue;
      if (id === this.nodeId || id === forwarderId) continue;
      candidates.push(id);
    }
    if (candidates.length === 0) return null;
    if (candidates.length === 1) return candidates[0];
    let best = null, bestDist = null;
    for (const id of candidates) {
      const d = id ^ subscriberId;
      if (bestDist === null || d < bestDist) { bestDist = d; best = id; }
    }
    return best;
  }

  // ── Policy hooks (overridable) ─────────────────────────────────────

  shouldRecruitSubAxon(role) {
    return role.children.size >= this.maxDirectSubs;
  }

  pickRecruitPeer(role, meta, subscriberId) {
    return this._pickExistingChildForRecruit(role, subscriberId, meta.fromId);
  }

  /** XOR-closest existing child to subscriberId, excluding self + forwarder.
   *  All ids BigInt. */
  _pickExistingChildForRecruit(role, subscriberId, excludeId = null) {
    if (role.children.size === 0) return null;
    let best = null;
    let bestDist = null;
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;
      if (childId === excludeId)   continue;
      const d = childId ^ subscriberId;
      if (bestDist === null || d < bestDist) { bestDist = d; best = childId; }
    }
    return best;
  }

  async _onUnsubscribe(payload, meta) {
    const topicId      = _wire(payload.topicId);
    const subscriberId = _wire(payload.subscriberId);
    // B-1 companion: only the authenticated channel peer may remove its own
    // subscription, else a relayed unsubscribe naming a victim silences that
    // victim's delivery (griefing).  Same proven-fromId invariant as the
    // routed subscribe + direct unsubscribe-k paths.
    const fromId = meta?.fromId == null ? null : _wire(meta.fromId);
    if (fromId !== null && subscriberId !== fromId) return 'forward';
    const role = this.axonRoles.get(topicId);
    if (role && role.children.has(subscriberId)) {
      role.children.delete(subscriberId);
      if (role.children.size === 0) role.emptiedAt = this._now();
      return 'consumed';
    }
    return 'forward';
  }

  async _onPublish(payload, meta) {
    const topicId   = _wire(payload.topicId);
    const publisher = _wire(payload.publisher);
    const { json, publishId, publishTs, postHash } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';
    if (!role.isRoot) return 'forward';
    if (this._alreadySeenPublish(publishId)) return 'consumed';
    // D-1: bound the per-publish payload so an oversized `json` can't bloat
    // the replay cache / fan-out buffers.
    if (typeof json === 'string' && json.length > MAX_PUBLISH_BYTES) {
      this._emitLog?.('debug', 'publish-oversize-dropped', { topicId: toHex(topicId), bytes: json.length });
      return 'consumed';
    }
    // B-4: verify the publisher signature at root ingress, before caching
    // or fan-out, so spoofed-signature spam is dropped here rather than
    // amplified through the tree + replay cache.
    if (!(await this._publishSignatureOk(json))) {
      this._emitLog?.('debug', 'publish-bad-signature-dropped', { topicId: toHex(topicId) });
      return 'consumed';
    }
    // v0.3: topic-descriptor binding + write-policy (owner-only) enforcement.
    if (!(await this._topicPolicyOk(json, topicId))) {
      this._emitLog?.('debug', 'publish-policy-dropped', { topicId: toHex(topicId) });
      return 'consumed';
    }
    // C-2: freshness + per-publisher monotonic seq on the live path. A
    // replayed/stale signed envelope is dropped here before it is cached
    // or fanned out (publishId already marked seen above, so a resend of
    // the drop is a no-op).
    const fo = this._publishFreshAndOrdered(json, this._now());
    if (!fo.ok) {
      this._emitLog?.('debug', 'publish-stale-dropped', { topicId: toHex(topicId), reason: fo.reason });
      return 'consumed';
    }
    // Content-address integrity: the cache/pull/kill key (postHash) must equal
    // the verified content hash, or the message is dropped (see
    // _postHashConsistent).  Keep this AFTER the signature check so the
    // signerPubkey it folds in is genuine.
    if (!(await this._postHashConsistent(json, postHash))) {
      this._emitLog?.('debug', 'publish-posthash-mismatch-dropped', { topicId: toHex(topicId) });
      return 'consumed';
    }
    // Phase A #2: a killed message must not be resurrected by a lagging
    // replica re-gossiping it.
    if (this._isTombstoned(postHash)) {
      this._emitLog?.('debug', 'publish-tombstoned-dropped', { topicId: toHex(topicId), msgId: postHash });
      return 'consumed';
    }

    // Re-publish of identical content (same msgId) upserts in the cache (see
    // _addToReplayCache: one entry per msgId, fresh hold + 48h ceiling) and then
    // fans out normally so EVERY replica refreshes its own hold — the point of a
    // re-publish is a fleet-wide hold refresh, not a single-root one. Exactly-once
    // app delivery is enforced downstream in _deliverToApp, keyed on msgId, so
    // re-fanning identical content never double-delivers to a subscriber.
    const quotaPerPublisher = await this._openTopicQuota(role, json, topicId);
    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher }, { quotaPerPublisher });
    this._recordReceived(topicId, publishId, publishTs);

    const topicIdHex  = toHex(topicId);
    const publisherHex = publisher === null || publisher === undefined ? publisher : toHex(publisher);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        this._deliverToApp(topicId, json, publishId, publishTs, postHash);
        if (postHash) this._bumpDelivery(topicId, postHash);
        continue;
      }
      // childId is BigInt; sendDirect contract takes BigInt; wire payload hex.
      const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
        topicId: topicIdHex, json, publishId, publishTs,
        postHash, publisher: publisherHex,
      });
      if (!ok) {
        deadChildren.push(childId);
      } else if (postHash) {
        this._bumpDelivery(topicId, postHash);
      }
    }
    for (const dead of deadChildren) role.children.delete(dead);
    return 'consumed';
  }

  /**
   * Add a message to a role's replay cache with a BOUNDED size and
   * DETERMINISTIC eviction (Phase A #4).
   *
   * Eviction order is the signed (seq, ts, msgId) tuple — derived entirely
   * from fields under the publisher's signature — so every replica evicts
   * the SAME message and the caches converge.  At maxMessages = 1 this is a
   * retained / latest-value slot (a new publish, higher seq, replaces the
   * prior one).
   *
   * `opts.quotaPerPublisher` (set only for OPEN/Model-1 topics — see
   * _openTopicQuota) caps how many of the queue's slots one `signerPubkey`
   * may hold, so a single anonymous publisher can't flood the topic and
   * evict everyone else.  Owned topics pass no quota (the owner-signed
   * publish ACL governs them — and the owner shouldn't be limited in their
   * own topic).
   */
  _addToReplayCache(role, entry, opts = {}) {
    if (!role.replayCache) role.replayCache = [];
    // Upsert by content id (postHash = msgId): one entry per msgId, always. A
    // re-publish of identical content (same author + message) replaces the
    // prior copy here — so EVERY ingress path (routed publish, direct publish-k,
    // sub-axon deliver, replay re-cache) converges to a single entry with a
    // fresh hold + 48h ceiling, no matter how many roots/paths carry the copy.
    // (Anti-entropy already drops a known postHash before calling, so this is a
    // no-op there. Different authors of identical text differ in postHash.)
    if (entry.postHash) {
      const dup = role.replayCache.findIndex(e => e.postHash === entry.postHash);
      if (dup !== -1) role.replayCache.splice(dup, 1);
    }
    // Enrich with the ordering key (parse once from the signed envelope).
    if (entry.seq === undefined) {
      let env = null;
      try { env = JSON.parse(entry.json); } catch { /* non-envelope */ }
      entry.seq          = (env && typeof env.seq === 'number') ? env.seq : 0;
      entry.ts           = (env && typeof env.ts  === 'number') ? env.ts  : (entry.publishTs || 0);
      entry.signerPubkey = (env && typeof env.signerPubkey === 'string') ? env.signerPubkey : null;
    }
    // Hold time (Phase A #5): absolute expiry off the message ts, CLAMPED to
    // receive-time.  Real (signed) traffic is within the C-2 freshness window
    // so its ts ≈ now and is used directly; an out-of-window ts (unsigned/
    // legacy toy timestamps, or a spoofed far-future value) falls back to the
    // receiver's clock, so hold time can't be gamed and "held for N hours"
    // means N hours from when this replica received it.  ceilingAt is the
    // hard 48h cap a sliding pull (#6) can't extend past.
    if (entry.expiresAt === undefined) {
      const now    = this._now();
      const baseTs = (typeof entry.ts === 'number' && Math.abs(now - entry.ts) <= MAX_HOLD_MS) ? entry.ts : now;
      const holdMs = Math.min(role.maxHoldMs || DEFAULT_HOLD_MS, MAX_HOLD_MS);
      entry.ceilingAt = baseTs + MAX_HOLD_MS;
      entry.expiresAt = Math.min(baseTs + holdMs, entry.ceilingAt);
    }
    const cache = role.replayCache;
    cache.push(entry);
    const max = role.maxMessages || this.replayCacheSize;

    // Per-publisher quota (open topics): evict THIS publisher's own
    // lowest-ordered entries first, so flooding self-limits. SP-10: anonymous
    // (null-signer) publishes all share a single 'anon' bucket, so an unsigned
    // flood can't bypass the cap simply by not signing. (Once publish-key PoW is
    // enforced — Stage 4b — open topics require a PoW-stamped signer and this
    // 'anon' bucket goes away.)
    const quota = opts.quotaPerPublisher;
    if (quota) {
      const quotaKey = entry.signerPubkey ?? 'anon';
      let mine = cache.filter(e => (e.signerPubkey ?? 'anon') === quotaKey);
      while (mine.length > quota) {
        const victim = this._lowestOrdered(mine);
        cache.splice(cache.indexOf(victim), 1);
        mine = mine.filter(e => e !== victim);
      }
    }
    // Global bound: evict the lowest-ordered until within BOTH the count cap
    // and the byte cap (whichever binds first). The byte cap keeps a large
    // count (1024) from OOMing a relay when entries are big (16 KB chunks):
    // 1024 small messages cost little, but 1024 × 16 KB would be ~16 MB/topic,
    // so file topics are bounded by bytes, small-message topics by count.
    const maxBytes = role.maxBytes || this.replayCacheBytes || Infinity;
    const sizeOf = (e) => (typeof e.json === 'string' ? e.json.length : 0);
    let bytes = 0; for (const e of cache) bytes += sizeOf(e);
    while (cache.length > max || (bytes > maxBytes && cache.length > 1)) {
      const victim = this._lowestOrdered(cache);
      const idx = cache.indexOf(victim);
      if (idx < 0) break;
      bytes -= sizeOf(victim);
      cache.splice(idx, 1);
    }
  }

  /** The lowest-ordered (oldest) cache entry by (seq, ts, msgId). */
  _lowestOrdered(entries) {
    return entries.reduce((a, b) => (this._orderLt(a, b) ? a : b));
  }

  /** True iff entry `a` is strictly lower-ordered (older) than `b`. */
  _orderLt(a, b) {
    // Phase A #7: a touched (kept-alive) entry always outranks an untouched
    // one, and among touched entries the more-recently-touched outranks — so
    // `touch` moves a message to the head of the queue and makes it the LAST
    // to be evicted. `touchedTs` is the touch's signed ts (identical across
    // replicas); untouched entries are 0, preserving prior ordering.
    const ka = a.touchedTs ?? 0, kb = b.touchedTs ?? 0;
    if (ka !== kb) return ka < kb;
    const sa = a.seq ?? 0, sb = b.seq ?? 0;
    if (sa !== sb) return sa < sb;
    const ta = a.ts ?? 0, tb = b.ts ?? 0;
    if (ta !== tb) return ta < tb;
    return String(a.postHash ?? '') < String(b.postHash ?? '');
  }

  /**
   * Per-publisher quota for an OPEN (Model 1 / public) topic, else null.
   * A topic is open iff its id is the public-mode derivation of its name —
   * `deriveTopicId(null, env.topic) === topicId` — which is verifiable from
   * the SIGNED `topic` field and can't be spoofed by the unsigned wire
   * `publisher` field.  Owned (publisher-keyed) topics return null.
   */
  async _openTopicQuota(role, json, topicId) {
    try {
      const env = JSON.parse(json);
      const d = env && env.topic;
      // Open (v0.3): the signed descriptor's write policy is 'open' (anyone may
      // publish). _topicPolicyOk has already bound the descriptor to THIS topic id
      // at ingress, so the write field can be trusted here.
      if (d && typeof d === 'object' && (d.write === 'open' || !d.owner)) {
        const max = role.maxMessages || this.replayCacheSize;
        return Math.max(1, Math.ceil(max / 4));
      }
    } catch { /* non-envelope */ }
    return null;
  }

  /** Record reception of a publishId + timestamp for this topic.  Topic
   *  key is BigInt; publishId is a string. */
  _recordReceived(topicId, publishId, publishTs) {
    if (!publishId) return;
    let set = this._receivedPublishIds.get(topicId);
    if (!set) { set = new Set(); this._receivedPublishIds.set(topicId, set); }
    set.add(publishId);
    if (publishTs) {
      const prev = this._lastSeenTsByTopic.get(topicId) || 0;
      if (publishTs > prev) this._lastSeenTsByTopic.set(topicId, publishTs);
    }
  }

  // Note we HOLD this content (by postHash) so a future re-subscribe can tell a
  // root exactly what to skip — gap-safe, unlike the lastSeenTs high-water.
  _recordHave(topicId, postHash) {
    if (!postHash) return;
    let set = this._haveByTopic.get(topicId);
    if (!set) { set = new Set(); this._haveByTopic.set(topicId, set); }
    set.add(postHash);
    if (set.size > MAX_HAVE) {                 // drop oldest (insertion-ordered Set)
      const drop = set.size - MAX_HAVE;
      let i = 0; for (const k of set) { if (i++ >= drop) break; set.delete(k); }
    }
  }

  // The recent postHashes this node holds for `topicId` — sent on (re)subscribe
  // so a root replays only the complement (what we're actually missing).
  _haveFor(topicId) {
    const set = this._haveByTopic.get(topicId);
    return set ? [...set] : [];
  }

  _alreadySeenPublish(publishId) {
    if (!publishId) return false;
    const now = this._now();
    if (this._seenPublishes.has(publishId)) return true;
    this._seenPublishes.set(publishId, now);
    this._capStore(this._seenPublishes, this._seenPublishCap);
    return false;
  }

  /**
   * Deliver a publish to the local application callback EXACTLY ONCE.
   * The single funnel for every delivery path (live deliver, self-as-child
   * publish, routed publish, replay-batch, and self-replay-on-subscribe) so
   * a publishId reaches the app at most once regardless of how many roles /
   * resubscribes route it here.  Bounded LRU, same shape as _seenPublishes.
   */
  _deliverToApp(topicId, json, publishId, publishTs, postHash) {
    if (!this._deliveryCallback) return;
    // Exactly-once key.  Prefer the CONTENT id (postHash = msgId) so a message
    // reaches the app at most once regardless of HOW it arrived: a re-publish
    // of identical content (same msgId, new random publishId) and the same
    // content fanned out by several K-closest roots both collapse to a single
    // delivery.  Fall back to publishId only for content-less frames (e.g. the
    // kill/delete notification, which carries no postHash).  Both keep the
    // `${topicId}:` prefix so pubsubResetTopicConsumption() can forget exactly
    // one topic's deliveries on unsub / since:'all' resubscribe.
    const dedupId = postHash || publishId;
    if (dedupId) {
      const dkey = `${topicId}:${dedupId}`;
      if (this._appDelivered.has(dkey)) return;     // already delivered locally
      this._appDelivered.set(dkey, this._now());
      this._capStore(this._appDelivered, this._appDeliveredCap);
    }
    try {
      this._deliveryCallback(topicId, json, publishId, publishTs);
    } catch (err) {
      console.error('AxonaManager deliveryCallback threw:', err);
    }
  }

  async _onDeliver(payload, meta) {
    const topicId   = _wire(payload.topicId);
    const publisher = _wire(payload.publisher);
    const { json, publishId, publishTs, postHash } = payload;
    if (this._alreadySeenPublish(publishId)) return;

    this._recordReceived(topicId, publishId, publishTs);
    this._recordHave(topicId, postHash);

    const role = this.axonRoles.get(topicId);
    if (role) {
      this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });

      if (!role.isRoot) {
        const topicIdHex   = toHex(topicId);
        const publisherHex = publisher === null || publisher === undefined ? publisher : toHex(publisher);
        const fromId = _wire(meta.fromId);
        // Sub-axon: re-fan to our children.  Skip upstream peer and self.
        for (const [childId] of role.children) {
          if (childId === fromId) continue;
          if (childId === this.nodeId) continue;
          const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
            topicId: topicIdHex, json, publishId, publishTs,
            postHash, publisher: publisherHex,
          });
          if (ok && postHash) this._bumpDelivery(topicId, postHash);
        }
      }
    }

    this._deliverToApp(topicId, json, publishId, publishTs, postHash);
  }

  async _onPromoteAxon(payload, meta) {
    const topicId         = _wire(payload.topicId);
    const newSubscriberId = _wire(payload.newSubscriberId);
    const parentId        = _wire(payload.parentId);
    const now = this._now();

    let role = this.axonRoles.get(topicId);
    if (!role) {
      role = {
        parentId,
        isRoot:         false,
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,
      };
      this.axonRoles.set(topicId, role);
      // Immediately refresh upward.  Wire fields hex.
      await this.dht.routeMessage(topicId, 'pubsub:subscribe', {
        topicId:      toHex(topicId),
        subscriberId: toHex(this.nodeId),
      });
    }

    await this._addOrRecruitChild(topicId, role, newSubscriberId, _wire(meta.fromId));
  }

  async _onAdoptSubscribers(payload, meta) {
    const topicId       = _wire(payload.topicId);
    const subscriberIdsHex = payload.subscriberIds;
    if (!Array.isArray(subscriberIdsHex) || subscriberIdsHex.length === 0) return;
    // D-1: cap the inbound batch so one adopt message can't make us
    // allocate an unbounded child map.
    const subscriberIds = subscriberIdsHex.slice(0, MAX_SUBSCRIBER_BATCH).map(h => _wire(h));
    const now = this._now();

    let role = this.axonRoles.get(topicId);
    if (!role) {
      role = {
        parentId:       null,
        isRoot:         false,
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,
      };
      this.axonRoles.set(topicId, role);
    }

    for (const subId of subscriberIds) {
      if (subId === this.nodeId) continue;
      if (role.children.has(subId)) {
        role.children.get(subId).lastRenewed = now;
        continue;
      }
      // D-1: adoption must respect maxDirectSubs — it previously added
      // children unconditionally, bypassing the cap the subscribe paths
      // enforce, so a parent (or attacker posing as one) could overfill
      // this node's child map.  Renewals of existing children above are
      // still allowed; only NEW children are capped.
      if (role.children.size >= this.maxDirectSubs) break;
      role.children.set(subId, { createdAt: now, lastRenewed: now, isSubaxon: false });
    }

    await this.dht.routeMessage(topicId, 'pubsub:subscribe', {
      topicId:      toHex(topicId),
      subscriberId: toHex(this.nodeId),
    });
  }

  async _onDissolveHint(payload, meta) {
    const topicId = _wire(payload.topicId);

    if (this.mySubscriptions.has(topicId)) {
      await this.dht.routeMessage(topicId, 'pubsub:subscribe', {
        topicId:      toHex(topicId),
        subscriberId: toHex(this.nodeId),
      });
    }

    const role = this.axonRoles.get(topicId);
    if (role && !role.isRoot) {
      role.parentId = null;
      if (role.children.size > 0) {
        await this.dht.routeMessage(topicId, 'pubsub:subscribe', {
          topicId:      toHex(topicId),
          subscriberId: toHex(this.nodeId),
        });
      }
    }
  }

  // ── K-closest direct handlers ───────────────────────────────────────

  async _onSubscribeDirect(payload, meta) {
    const topicId      = _wire(payload.topicId);
    const subscriberId = _wire(payload.subscriberId);
    // C4 (reflection/amplification DRDoS): a direct subscribe registers
    // `subscriberId` as a child this node will relay the topic's full feed
    // to — sent directly, by nodeId.  If we trusted the payload we'd be an
    // amplifier: an attacker names a victim and we fire the feed at them.
    // axona/4 makes `meta.fromId` the *proven* channel peer, so require the
    // subscriber to be the authenticated sender.  (Sub-axon delegation uses
    // separate adopt/promote messages — never subscribe-k — so this is a
    // strict equality with no legitimate exception on this path.)
    const fromId = meta?.fromId == null ? null : _wire(meta.fromId);
    if (fromId !== null && subscriberId !== fromId) return;   // spoofed → drop
    const peerRootsHex = payload.peerRoots;
    const { lastSeenTs } = payload;
    // D-1: cap peerRoots[] (K is ~5; anything beyond MAX_PEER_ROOTS is abuse).
    const peerRoots = Array.isArray(peerRootsHex)
      ? peerRootsHex.slice(0, MAX_PEER_ROOTS).map(h => _wire(h)).filter(p => p !== this.nodeId)
      : [];
    // D-1: cap the gap-safe digest (a legit `have` is ≤ the subscriber's cache).
    const have = Array.isArray(payload.have)
      ? payload.have.slice(0, MAX_HAVE).filter(h => typeof h === 'string')
      : null;
    const now = this._now();

    let role = this.axonRoles.get(topicId);
    if (!role) {
      role = {
        parentId:       null,
        isRoot:         true,
        isInRootSet:    true,
        peerRoots:      new Set(peerRoots),
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,
      };
      this.axonRoles.set(topicId, role);
    } else {
      role.isInRootSet = true;
      if (peerRoots.length > 0) {
        for (const p of peerRoots) role.peerRoots?.add(p);
      }
    }

    await this._addOrRecruitChild(topicId, role, subscriberId, subscriberId);
    await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs, have);
  }

  /**
   * Replay cached publishes a subscriber is missing. `have` (array of postHashes
   * the subscriber holds) is gap-safe: we replay exactly the complement. Falls
   * back to the lastSeenTs high-water only for pre-v2.37 subscribers that don't
   * send `have` (back-compat). subscriberId/topicId are BigInt.
   */
  async _maybeSendReplay(topicId, role, subscriberId, lastSeenTs, have = null) {
    if (!subscriberId) return;
    // Phase A #5: never replay expired messages; sweep them first.
    this._sweepRole(role, this._now());
    const cache = role.replayCache;
    if (!cache || cache.length === 0) return;

    // Self-replay special case — a node that is both a root/axon AND a
    // subscriber to this topic.  Delivers cached publishes to the LOCAL
    // app callback (lazy-axon promotion + lastSeenTs masking rationale).
    //
    // This MUST dedup like every other delivery path: refreshTick
    // re-issues subscribe-k to the K roots — including self — every
    // refresh interval, so without the _alreadySeenPublish gate this
    // branch re-fired the entire replay cache to the app every ~10 s
    // (the "earlier messages keep reappearing" bug).  Gating here makes
    // it idempotent: genuine backlog is delivered exactly once, and a
    // periodic self-resubscribe replays nothing already seen.
    if (subscriberId === this.nodeId) {
      for (const m of cache) {
        if (m.postHash && this._isTombstoned(m.postHash)) continue;   // never replay a killed message
        this._deliverToApp(topicId, m.json, m.publishId, m.publishTs, m.postHash);
      }
      return;
    }

    // Send-side tombstone backstop: a root must never replay a message it has
    // tombstoned, even if a stale cache entry lingers (defense in depth — the
    // receiver's own tombstone set is empty right after a reload).
    const fresh = cache.filter(m => !(m.postHash && this._isTombstoned(m.postHash)));
    let missed;
    if (Array.isArray(have)) {
      // Gap-safe: replay exactly what the subscriber doesn't already hold. An
      // entry with no postHash can't be deduped by content, so always include it.
      const haveSet = new Set(have);
      missed = fresh.filter(m => !m.postHash || !haveSet.has(m.postHash));
    } else if (lastSeenTs != null && lastSeenTs > 0) {
      missed = fresh.filter(m => m.publishTs > lastSeenTs);   // legacy fallback (maskable; pre-v2.37 subscribers)
    } else {
      missed = fresh.slice();
    }
    if (missed.length === 0) return;
    // Byte-bound the replay into MANY frames each < 16 KiB (O-5): the old
    // single-frame send was why a ≥64 KB-per-message topic returned nothing on
    // reload. responderId: which root served this (replica-divergence hunt).
    const responderId = toHex(this.nodeId);
    await this._sendFramedMessages(subscriberId, 'pubsub:replay-batch', toHex(topicId), missed,
      (topicHex, batch) => ({ topicId: topicHex, responderId, messages: batch }));
  }

  /**
   * Send `messages` to a peer as multiple frames, each bounded by
   * REPLAY_FRAME_BYTES and MAX_REPLAY_BATCH so no single frame exceeds the
   * 16 KiB WebRTC wire limit (O-5). Shared by replay-batch and msgsync-resp.
   */
  async _sendFramedMessages(target, type, topicHex, messages, makePayload) {
    let frame = [], bytes = 0;
    const flush = async () => {
      if (frame.length === 0) return;
      const batch = frame; frame = []; bytes = 0;
      await this.dht.sendDirect(target, type, makePayload(topicHex, batch));
    };
    for (const m of messages) {
      const sz = (typeof m.json === 'string' ? m.json.length : 0) + 256;   // entry + per-entry wrapper estimate
      if (frame.length > 0 && (bytes + sz > REPLAY_FRAME_BYTES || frame.length >= MAX_REPLAY_BATCH)) await flush();
      frame.push(m); bytes += sz;
    }
    await flush();
  }

  _onReplayBatch(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { messages } = payload;
    if (!Array.isArray(messages) || messages.length === 0) return;
    // Diagnostic (replica-divergence hunt): which root served this batch?
    const from = payload.responderId || (meta?.fromId != null ? toHex(_wire(meta.fromId)) : '?');
    // D-1: cap the inbound replay batch (a legit batch is ≤ replay cache).
    const batch = messages.slice(0, MAX_REPLAY_BATCH);
    this._emitLog?.('debug', 'replay-batch-recv', { from, topicId: toHex(topicId), n: batch.length });
    for (const msg of batch) {
      const { json, publishId, publishTs, postHash, publisher } = msg;
      // Phase A #2: never resurrect a killed message. A lagging replica may
      // still carry it in the batch it replays; without this guard a kill
      // could be undone the next time some relay replays its stale cache.
      if (postHash && this._isTombstoned(postHash)) {
        this._emitLog?.('debug', 'replay-skip-tombstoned', { from, msgId: postHash });
        continue;
      }
      // The smoking gun: a root served us this message on (re)subscribe. If it
      // was previously killed, `from` names the replica that missed the kill.
      this._emitLog?.('debug', 'replay-serve', { from, msgId: postHash, publishId });
      this._recordHave(topicId, postHash);     // now we hold it → won't re-request next time
      // App delivery is gated by _appDelivered (inside _deliverToApp), NOT by
      // the network-level _seenPublishes set. A node that relayed this publish
      // as a K-closest ROOT marked it in _seenPublishes WITHOUT delivering to
      // its app (its app had not subscribed yet). Gating the replay path on
      // _seenPublishes therefore silently DROPPED legitimate backlog for
      // exactly the subscribers that happen to also be roots for the topic —
      // a non-deterministic "late subscriber sees nothing" bug. So: always
      // attempt app delivery (idempotent), and only re-cache / re-record the
      // first time the router sees the publishId.
      if (!this._alreadySeenPublish(publishId)) {
        this._recordReceived(topicId, publishId, publishTs);
        const role = this.axonRoles.get(topicId);
        // Preserve postHash (and publisher) so a copy acquired via replay stays
        // addressable: kill() and pull() match on postHash, and the metrics
        // ownership gate samples publisher. Dropping them here made replay-
        // acquired entries silently unkillable / unpullable — they would be
        // re-served to future subscribers even after a successful kill.
        if (role) this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });
      }
      this._deliverToApp(topicId, json, publishId, publishTs, postHash);
    }
  }

  _onUnsubscribeDirect(payload, meta) {
    const topicId      = _wire(payload.topicId);
    const subscriberId = _wire(payload.subscriberId);
    // C4 (companion): only the authenticated subscriber may remove its own
    // subscription — otherwise an attacker could unsubscribe a victim
    // (griefing).  Same proven-fromId equality as the subscribe path.
    const fromId = meta?.fromId == null ? null : _wire(meta.fromId);
    if (fromId !== null && subscriberId !== fromId) return;   // spoofed → drop
    const role = this.axonRoles.get(topicId);
    if (role && role.children.has(subscriberId)) {
      role.children.delete(subscriberId);
      if (role.children.size === 0) role.emptiedAt = this._now();
    }
  }

  async _onPublishDirect(payload, meta) {
    const topicId   = _wire(payload.topicId);
    const publisher = _wire(payload.publisher);
    const { json, publishId, publishTs, postHash } = payload;
    if (this._alreadySeenPublish(publishId)) return;
    // D-1: bound the per-publish payload (see _onPublish).
    if (typeof json === 'string' && json.length > MAX_PUBLISH_BYTES) {
      this._emitLog?.('debug', 'publish-oversize-dropped', { topicId: toHex(topicId), bytes: json.length });
      return;
    }
    // B-4: verify the publisher signature at root ingress (this is a
    // K-closest root for the topic), before promotion, caching, or
    // fan-out — so spoofed-signature spam is dropped at the edge.
    if (!(await this._publishSignatureOk(json))) {
      this._emitLog?.('debug', 'publish-bad-signature-dropped', { topicId: toHex(topicId) });
      return;
    }
    // v0.3: topic-descriptor binding + write-policy (owner-only) enforcement.
    if (!(await this._topicPolicyOk(json, topicId))) {
      this._emitLog?.('debug', 'publish-policy-dropped', { topicId: toHex(topicId) });
      return;
    }
    // C-2: freshness + per-publisher monotonic seq (live path; see
    // _onPublish).  Drop a stale/replayed signed envelope before it can
    // trigger lazy promotion, caching, or fan-out.
    const fo = this._publishFreshAndOrdered(json, this._now());
    if (!fo.ok) {
      this._emitLog?.('debug', 'publish-stale-dropped', { topicId: toHex(topicId), reason: fo.reason });
      return;
    }
    // Content-address integrity (see _postHashConsistent / _onPublish).
    if (!(await this._postHashConsistent(json, postHash))) {
      this._emitLog?.('debug', 'publish-posthash-mismatch-dropped', { topicId: toHex(topicId) });
      return;
    }
    if (this._isTombstoned(postHash)) {
      this._emitLog?.('debug', 'publish-tombstoned-dropped', { topicId: toHex(topicId), msgId: postHash });
      return;
    }
    let role = this.axonRoles.get(topicId);
    if (!role) {
      // B-2: lazy-axon promotion (allocate a role + replay cache for a
      // topic we don't yet host) is gated on proximity — only promote if
      // we're plausibly in this topic's K-closest set.  A flood of
      // publishes for random topicIds therefore can't make us allocate
      // unbounded roles/caches.  publishId is already marked seen above,
      // so dropping here won't be reprocessed on resend.
      if (!(await this._mayHostTopic(topicId))) {
        this._emitLog?.('debug', 'lazy-axon-promotion-rejected', { topicId: toHex(topicId) });
        return;
      }
      // Lazy-axon promotion — see original docstring.
      const now = this._now();
      role = {
        parentId:       null,
        isRoot:         true,
        isInRootSet:    true,
        peerRoots:      new Set(),
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      now,
        lowWaterSince:  0,
        replayCache:    [],
      };
      this.axonRoles.set(topicId, role);
    }

    // Re-publish of identical content upserts (one entry per msgId, fresh hold;
    // see _addToReplayCache) then fans out normally so every replica refreshes
    // its hold; exactly-once app delivery is enforced in _deliverToApp by msgId.
    const quotaPerPublisher = await this._openTopicQuota(role, json, topicId);
    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher }, { quotaPerPublisher });
    this._recordReceived(topicId, publishId, publishTs);

    const topicIdHex   = toHex(topicId);
    const publisherHex = publisher === null || publisher === undefined ? publisher : toHex(publisher);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        this._deliverToApp(topicId, json, publishId, publishTs, postHash);
        if (postHash) this._bumpDelivery(topicId, postHash);
        continue;
      }
      const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
        topicId: topicIdHex, json, publishId, publishTs,
        postHash, publisher: publisherHex,
      });
      if (!ok) {
        deadChildren.push(childId);
      } else if (postHash) {
        this._bumpDelivery(topicId, postHash);
      }
    }
    for (const dead of deadChildren) role.children.delete(dead);
  }

  // ── Refresh and TTL sweep ───────────────────────────────────────────

  async refreshTick() {
    const now = this._now();

    // Phase A #5: drop messages past their hold-time expiry from every
    // hosted topic before the rest of the refresh runs.
    this._sweepExpired(now);

    // 0. Drop the K-closest cache so every re-subscribe / re-publish
    //    below recomputes against the CURRENT synaptome.  The cache is
    //    populated lazily on the first _findKClosest(topic) call and
    //    keyed by an epoch that nothing else bumps — so without this,
    //    a peer that first subscribed while its synaptome held only
    //    the bridge stays pinned to that initial, narrow axon set
    //    forever.  As the WebRTC mesh fills in, other peers compute a
    //    wider (different) K-closest set and route publishes to axons
    //    the stale subscriber never registered at — the message
    //    reaches some peers but silently misses others.  Flushing here,
    //    once per refresh interval (default 10 s), lets the re-subscribe
    //    in step 5 re-anchor each subscription on the converged set.
    this.invalidateKClosestCache();

    // Phase A #2 (kill convergence): re-gossip recently-applied kills to the
    // current root set so a replica that missed the original kill removes +
    // tombstones the message. Gated on recent kill activity ⇒ zero steady-state
    // cost; the K-closest cache was just invalidated above, so this re-derives
    // the converged root set.
    if (now - this._lastKillAt < KILL_REGOSSIP_MS) {
      for (const [topicId, role] of this.axonRoles) {
        await this._syncKillsForTopic(topicId, role);
      }
    }

    // Fix 2: root-to-root message anti-entropy. Round-robin a bounded slice of
    // hosted topics each tick so every K-closest root converges on every
    // publisher's feed (a publish only reached the PUBLISHER's K-closest set).
    if (this._useKClosestMode()) {
      const hosted = [...this.axonRoles.entries()].filter(([, r]) => r.isRoot || r.isInRootSet);
      if (hosted.length > 0) {
        // Cold-start drain first: roles never reconciled (e.g. right after a
        // restart/rejoin — empty caches) get backfilled with a large budget so
        // the host converges in a tick or two instead of the slow round-robin.
        let coldBudget = MSGSYNC_COLD_BUDGET;
        for (const [topicId, role] of hosted) {
          if (coldBudget <= 0) break;
          if (role.synced) continue;
          coldBudget--;
          await this._antiEntropyTopic(topicId, role);   // sets role.synced = true
        }
        // Steady-state round-robin (warm convergence + churn healing).
        for (let i = 0; i < Math.min(MSGSYNC_TOPICS_PER_TICK, hosted.length); i++) {
          const [topicId, role] = hosted[(this._msgSyncCursor + i) % hosted.length];
          await this._antiEntropyTopic(topicId, role);
        }
        this._msgSyncCursor = (this._msgSyncCursor + MSGSYNC_TOPICS_PER_TICK) % hosted.length;
      }
    }

    // 1. TTL sweep — drop stale children.
    for (const role of this.axonRoles.values()) {
      for (const [childId, entry] of role.children) {
        if (now - entry.lastRenewed > this.maxSubscriptionAgeMs) {
          role.children.delete(childId);
        }
      }
      if (role.children.size === 0 && role.emptiedAt === 0) {
        role.emptiedAt = now;
      }
    }

    // 2. §5.8 hysteresis dissolve.
    const dissolveHints = [];
    for (const [topicId, role] of this.axonRoles) {
      if (role.isRoot) continue;
      if (role.children.size === 0) continue;
      if (role.children.size >= this.minDirectSubs) {
        role.lowWaterSince = 0;
        continue;
      }
      if (role.lowWaterSince === 0) {
        role.lowWaterSince = now;
        continue;
      }
      if (now - role.lowWaterSince > this.refreshIntervalMs) {
        for (const [childId] of role.children) {
          dissolveHints.push({
            childId, topicId, suggestedParent: role.parentId,
          });
        }
        this.axonRoles.delete(topicId);
      }
    }

    // 3. GC empty roles.
    for (const [topicId, role] of this.axonRoles) {
      if (role.children.size > 0) continue;
      if (!role.isRoot) {
        this.axonRoles.delete(topicId);
        continue;
      }
      if (role.emptiedAt > 0 && now - role.emptiedAt > this.rootGraceMs) {
        this.axonRoles.delete(topicId);
      }
    }

    // 4. Fire dissolve-hints from the queue.
    for (const hint of dissolveHints) {
      const p = this.dht.sendDirect(hint.childId, 'pubsub:dissolve-hint', {
        topicId:         toHex(hint.topicId),
        suggestedParent: hint.suggestedParent === null ? null : toHex(hint.suggestedParent),
      });
      if (p && typeof p.catch === 'function') p.catch(() => {});
    }

    // 5. Leaf + axon refresh — re-issue subscribes.
    const altK = (this.crossFragmentRoots ?? 0) + 1;
    const selfHex = toHex(this.nodeId);
    const issueSubscribe = async (topicId, lastSeenTs, role) => {
      try {
        const topicIdHex = toHex(topicId);
        if (this._useKClosestMode()) {
          const roots = await this._findKClosest(topicId, this.rootSetSize);
          const rootsHex = roots.map(r => typeof r === 'bigint' ? toHex(r) : r);
          for (const peerId of roots) {
            const p = this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
              topicId: topicIdHex, subscriberId: selfHex,
              peerRoots: rootsHex, lastSeenTs,
            });
            if (p?.catch) p.catch(() => {});
          }
        } else if (altK > 1 && typeof this.dht.findKClosest === 'function') {
          const targets = await this._findKClosest(topicId, altK);
          const targetsHex = targets.map(t => typeof t === 'bigint' ? toHex(t) : t);
          for (const target of targets) {
            if (target === this.nodeId) continue;
            const p = this.dht.sendDirect(target, 'pubsub:subscribe-k', {
              topicId: topicIdHex, subscriberId: selfHex,
              peerRoots: targetsHex, lastSeenTs,
            });
            if (p?.catch) p.catch(() => {});
          }
        } else {
          const p = this.dht.routeMessage(topicId, 'pubsub:subscribe', {
            topicId: topicIdHex, subscriberId: selfHex, lastSeenTs,
          });
          if (p?.catch) p.catch(() => {});
        }
        if (role) role.parentLastSent = now;
      } catch (err) {
        console.error('AxonaManager refresh: subscribe issue failed:', err);
      }
    };

    for (const topicId of this.mySubscriptions.keys()) {
      const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
      issueSubscribe(topicId, lastSeenTs, null);
    }
    // Host heartbeat: re-announce explicitly-hosted topics (same wire as a
    // subscriber, no local delivery) so the root set keeps us in its serving
    // tree, and announce our keyspace so we keep getting recruited for
    // whatever lands near us.
    for (const topicId of this._hostedTopics.keys()) {
      if (this.mySubscriptions.has(topicId)) continue;   // already announced above
      const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
      issueSubscribe(topicId, lastSeenTs, null);
    }
    if (this._hostKeyspace) {
      this._announceKeyspace().catch(() => { /* best-effort */ });
    }
    for (const [topicId, role] of this.axonRoles) {
      if (role.children.size === 0) continue;
      const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
      issueSubscribe(topicId, lastSeenTs, role);
    }
  }

  // ── Post-level metrics ──────────────────────────────────────────────
  //
  // Counters keyed by (BigInt topicId, postHash string).

  _counterFor(topicId, postHash) {
    let byTopic = this._counters.get(topicId);
    if (!byTopic) {
      byTopic = new Map();
      this._counters.set(topicId, byTopic);
      // Bound the number of topics tracked (evicts oldest; the just-added
      // topic is newest so it survives).
      this._capStore(this._counters, this._countersTopicCap);
    }
    let c = byTopic.get(postHash);
    if (!c) {
      c = {
        post_hash:        postHash,
        topic_id:         toHex(topicId),
        delivery_count:   0,
        pull_count:       0,
        reshare_count:    0,
        first_seen:       this._now(),
        last_updated:     this._now(),
      };
      byTopic.set(postHash, c);
      // Bound posts-per-topic (oldest posts' metrics age out).
      this._capStore(byTopic, this._countersPostCap);
    }
    return c;
  }

  _bumpDelivery(topicId, postHash) {
    const c = this._counterFor(topicId, postHash);
    c.delivery_count += 1;
    c.last_updated = this._now();
  }

  _bumpPull(topicId, postHash) {
    const c = this._counterFor(topicId, postHash);
    c.pull_count += 1;
    c.last_updated = this._now();
  }

  _bumpReshare(topicId, postHash) {
    const c = this._counterFor(topicId, postHash);
    c.reshare_count += 1;
    c.last_updated = this._now();
  }

  /**
   * Count the LIVE (non-expired) messages currently retained in a role's
   * replay cache — the number of published events presently held in this
   * relay's slice of the topic tree.  Killed messages are already removed
   * from the cache, and expired ones are excluded here, so this is the
   * "current count" surfaced by peer.metrics (current_count).
   */
  _liveCacheCount(role, now = this._now()) {
    const cache = role?.replayCache;
    if (!cache || cache.length === 0) return 0;
    let n = 0;
    for (const e of cache) if (!this._isExpired(e, now)) n++;
    return n;
  }

  _findInReplayCache(role, postHash) {
    const cache = role?.replayCache;
    if (!cache) return null;
    const now = this._now();
    for (let i = cache.length - 1; i >= 0; i--) {
      if (cache[i].postHash !== postHash) continue;
      // Phase A #5: an expired message is gone — drop it and report a miss.
      if (this._isExpired(cache[i], now)) { cache.splice(i, 1); return null; }
      return cache[i];
    }
    return null;
  }

  /**
   * The most-recent LIVE message in a role's replay cache — highest-ordered
   * by the signed (seq, ts, msgId) tuple (Phase A #6, `pull` with no msgId).
   * Returns null if the cache is empty or all entries are expired.
   */
  _latestInReplayCache(role) {
    const cache = role?.replayCache;
    if (!cache || cache.length === 0) return null;
    const now = this._now();
    let best = null;
    for (const e of cache) {
      if (this._isExpired(e, now)) continue;
      if (best === null || this._orderLt(best, e)) best = e;
    }
    return best;
  }

  /**
   * Common access-control + response shape for metricsReq/Broadcast.
   * Wire fields are hex; payload comes in raw and we don't pre-convert
   * here (we only need topicId BigInt for the counter lookup).
   */
  /**
   * Build this relay's metrics response for a request, or null if the
   * access gate denies it.  Pure (no I/O) so the requester can also call
   * it locally when it is itself one of the K roots, instead of routing a
   * response to itself.
   */
  _buildMetricsResp(payload, role, topicIdBig, fromId = null) {
    const { postHashes, requesterId } = payload;
    const requesterBig = requesterId != null ? _wire(requesterId) : null;
    // C-3 (reflection/amplification): only ever answer the PROVEN sender.
    // `fromId === null` ⇒ locally originated (self-query) ⇒ trusted; otherwise
    // the claimed `requesterId` MUST equal the authenticated channel peer — the
    // same proven-fromId invariant as the B-1 subscribe path. An attacker can
    // therefore only aim a response at itself, never at a named victim. This
    // also drops a relayed `metricsBroadcast` (it arrives from a root, so
    // fromId ≠ requesterId).
    if (fromId !== null && requesterBig !== fromId) return null;
    // Ownership gate. A topic is "owned" only when it is anchored at a real
    // identity — a publisher nodeId whose low 256 bits are the SHA-256 of a
    // pubkey. Two anchor shapes are therefore UNOWNED, and their metrics are
    // readable by anyone:
    //   · public topics      — null publisher (top byte 0x00, no anchor);
    //   · synthetic regional  — `prefix || 0^256` (e.g. region-keyed topics).
    //     No key can hash to all-zero, so no node can ever own or unpub them.
    // Owned topics stay owner-only (self-authenticating: only the node whose
    // id IS the publisher anchor may read their metrics).
    const samplePost = role.replayCache?.[0];
    const anchor = samplePost?.publisher ?? null;          // BigInt | null
    const owned  = anchor !== null && (anchor & ((1n << 256n) - 1n)) !== 0n;
    if (owned && anchor !== requesterBig) {
      return null;
    }
    // C-3 (fail-CLOSED): the subscriber count is the owner-sensitive field. With
    // an empty replay cache the owner is INDETERMINATE (no cached post to
    // establish the anchor), so the topic could be owned — withhold the count
    // (`null`) rather than leak it. Non-sensitive counts are still returned, and
    // sibling roots that hold the queue answer the real subscriber count via the
    // metricsReq-k aggregation.
    const ownershipKnown = (role.replayCache?.length ?? 0) > 0;
    const byTopic = this._counters.get(topicIdBig) || new Map();
    const wantedHashes = (postHashes && postHashes.length > 0)
      ? postHashes
      : [...byTopic.keys()];
    const entries = [];
    for (const h of wantedHashes) {
      const c = byTopic.get(h);
      if (c) entries.push({ ...c });
    }
    return {
      requestId:     payload.requestId,
      responderId:   toHex(this.nodeId),
      entries,
      current_count: this._liveCacheCount(role),
      subscribers:   ownershipKnown ? (role.children?.size ?? 0) : null,
      timestamp:     this._now(),
    };
  }

  _maybeRespondMetrics(payload, role, topicIdBig, meta) {
    const fromId = meta?.fromId == null ? null : _wire(meta.fromId);
    const resp = this._buildMetricsResp(payload, role, topicIdBig, fromId);
    if (!resp) return false;
    // Reply only to the proven sender (fromId); when vouched it equals
    // requesterId. Never route a response to an attacker-named address.
    this.dht.sendDirect(fromId ?? _wire(payload.requesterId), 'pubsub:metricsResp', resp);
    return true;
  }

  _markMetricsReqSeen(requestId) {
    if (this._seenMetricsReqs.has(requestId)) return true;
    this._seenMetricsReqs.add(requestId);
    this._capStore(this._seenMetricsReqs, this._seenMetricsReqCap);
    return false;
  }

  async _onMetricsReq(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';
    if (this._markMetricsReqSeen(requestId)) return 'consumed';

    // Reply only to the proven sender; no child fan-out (the requester queries
    // the full K-closest root set directly via metricsReq-k, so the broadcast
    // was pure amplification — and the C-3 vouch check now drops it anyway).
    this._maybeRespondMetrics(payload, role, topicId, meta);
    return 'consumed';
  }

  /**
   * Direct metricsReq from a requester that fanned out to the whole
   * K-closest root set (pubsub:metricsReq-k), mirroring how publishes
   * replicate.  Same response + child-broadcast as the routed handler,
   * deduped by requestId so a root that also receives the routed walk or
   * a sibling's broadcast answers exactly once.
   */
  _onMetricsReqDirect(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return;
    if (this._markMetricsReqSeen(requestId)) return;
    this._maybeRespondMetrics(payload, role, topicId, meta);
  }

  // Retained for backward-compat with peers that still emit metricsBroadcast.
  // The C-3 vouch check makes it inert: a relayed request arrives from a root
  // (fromId ≠ requesterId), so it never produces a response, and there is no
  // further fan-out.
  _onMetricsBroadcast(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return;
    if (this._markMetricsReqSeen(requestId)) return;
    this._maybeRespondMetrics(payload, role, topicId, meta);
  }

  _onMetricsResp(payload, meta) {
    const { requestId, responderId, entries, current_count, subscribers } = payload;
    const pending = this._pendingMetricsReqs.get(requestId);
    if (!pending) return;
    pending.accumulated.push({ responderId, entries, current_count, subscribers });
  }

  // ── Pull (on-demand fetch by post_hash) ─────────────────────────────

  async _onPullReq(payload, meta) {
    const topicId    = _wire(payload.topicId);
    const requesterId = _wire(payload.requesterId);
    const { postHash, requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';

    // Phase A #6: a null/absent postHash means "give me the latest" — the
    // highest-ordered (by signed seq, ts, msgId) live message in the topic.
    const cached = postHash
      ? this._findInReplayCache(role, postHash)
      : this._latestInReplayCache(role);
    if (cached) {
      // Sliding hold (Phase A #6): a pull extends the message's life to
      // now + hold, BOUNDED by its absolute ceiling (it can never live past
      // ceilingAt).  Local to this replica — a read is not fanned as a write.
      if (typeof cached.ceilingAt === 'number') {
        const holdMs = Math.min(role.maxHoldMs || DEFAULT_HOLD_MS, MAX_HOLD_MS);
        cached.expiresAt = Math.min(this._now() + holdMs, cached.ceilingAt);
      }
      this.dht.sendDirect(requesterId, 'pubsub:pullResp', {
        requestId,
        postHash:    cached.postHash,         // the actual msgId served (latest may differ from request)
        status:      'FOUND',
        post:        cached.json,
        responderId: toHex(this.nodeId),
      });
      this._bumpPull(topicId, cached.postHash);
      return 'consumed';
    }
    if (role.isRoot) {
      this.dht.sendDirect(requesterId, 'pubsub:pullResp', {
        requestId,
        postHash,
        status:      'NOT_FOUND',
        responderId: toHex(this.nodeId),
      });
      return 'consumed';
    }
    return 'forward';
  }

  _onPullResp(payload, meta) {
    const { requestId, status, post } = payload;
    const pending = this._pendingPullReqs.get(requestId);
    if (!pending) return;
    this._pendingPullReqs.delete(requestId);
    if (status === 'FOUND' && post) {
      try { pending.resolve(JSON.parse(post)); }
      catch { pending.resolve(null); }
    } else {
      pending.resolve(null);
    }
  }

  /**
   * Publisher-side: issue a Pull for a specific post.
   * @param {bigint} topicId
   * @param {string} postHash
   */
  requestPull(topicId, postHash, { timeoutMs = 1000 } = {}) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.requestPull: topicId must be bigint, got ${typeof topicId}`);
    }
    const requestId = `${this.nodeId}:pl${++this._pullCounter}`;
    return new Promise(resolve => {
      this._pendingPullReqs.set(requestId, { resolve });
      this.dht.routeMessage(topicId, 'pubsub:pullReq', {
        topicId:     toHex(topicId),
        postHash,
        requesterId: toHex(this.nodeId),
        requestId,
      });
      setTimeout(() => {
        const pending = this._pendingPullReqs.get(requestId);
        if (pending) {
          this._pendingPullReqs.delete(requestId);
          pending.resolve(null);
        }
      }, timeoutMs);
    });
  }

  // ── Reshare notifications ───────────────────────────────────────────

  _onReshareNotify(payload, meta) {
    const refTopicId = _wire(payload.refTopicId);
    const { refPostHash } = payload;
    const role = this.axonRoles.get(refTopicId);
    if (!role) return 'forward';
    this._bumpReshare(refTopicId, refPostHash);
    return 'consumed';
  }

  /**
   * Publisher-side: issue a MetricsRequest.
   * @param {bigint} topicId
   */
  async requestMetrics(topicId, postHashes, { timeoutMs = 500 } = {}) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.requestMetrics: topicId must be bigint, got ${typeof topicId}`);
    }
    const requestId  = `${this.nodeId}:m${++this._metricsCounter}`;
    const reqPayload = {
      topicId:     toHex(topicId),
      postHashes:  postHashes ?? null,
      requesterId: toHex(this.nodeId),
      requestId,
    };
    const pending = { accumulated: [] };
    this._pendingMetricsReqs.set(requestId, pending);

    // Query the FULL K-closest root set directly — the same set publishes
    // replicate to (pubsub:publish-k) — rather than a single routed walk
    // that reaches only ONE root plus its subscriber-children.  Each root
    // answers with its own live cache count + subscriber count; the caller
    // aggregates with max.  Without this, the request lands on whichever
    // node is globally closest to the topicId, whose replayCache may be
    // empty or diverged while sibling roots (and the replay-on-subscribe
    // path) hold the queue — the "current_count: 0 even though a new
    // subscriber gets a full replay" bug.
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      for (const target of roots) {
        if (target === this.nodeId) {
          // Self is a root: fold in our own cache locally rather than
          // round-tripping a response to ourselves.  Mark seen so the
          // routed walk below (if it lands here) doesn't double-count.
          const role = this.axonRoles.get(topicId);
          if (role && !this._markMetricsReqSeen(requestId)) {
            const resp = this._buildMetricsResp(reqPayload, role, topicId);
            if (resp) {
              pending.accumulated.push({
                responderId:   resp.responderId,
                entries:       resp.entries,
                current_count: resp.current_count,
                subscribers:   resp.subscribers,
              });
            }
          }
          continue;
        }
        this.dht.sendDirect(target, 'pubsub:metricsReq-k', reqPayload);
      }
    }

    // Routed walk too: backward-compat with roots that predate
    // metricsReq-k, and a safety net when our K-closest view is stale.
    // Dedup by requestId means a root that receives both answers once.
    this.dht.routeMessage(topicId, 'pubsub:metricsReq', reqPayload);

    return new Promise(resolve => {
      setTimeout(() => {
        this._pendingMetricsReqs.delete(requestId);
        resolve(pending.accumulated);
      }, timeoutMs);
    });
  }

  /**
   * Read this node's local counters for a topic without going over the
   * wire.  topicId is BigInt.
   */
  getLocalCounters(topicId, postHashes) {
    const byTopic = this._counters.get(topicId) || new Map();
    if (!postHashes || postHashes.length === 0) {
      return [...byTopic.values()].map(c => ({ ...c }));
    }
    return postHashes
      .map(h => byTopic.get(h))
      .filter(Boolean)
      .map(c => ({ ...c }));
  }

  // ── Diagnostics ─────────────────────────────────────────────────────

  /**
   * Snapshot for tests: list of (topicId hex, role).  topicId is
   * converted to hex for the public display surface; children are also
   * hex-keyed.
   */
  inspectRoles() {
    const out = [];
    for (const [topicId, role] of this.axonRoles) {
      out.push({
        topicId:        typeof topicId === 'bigint' ? toHex(topicId) : topicId,
        isRoot:         role.isRoot,
        parentId:       (role.parentId === null || role.parentId === undefined) ? role.parentId
                       : (typeof role.parentId === 'bigint' ? toHex(role.parentId) : role.parentId),
        roleCreatedAt:  role.roleCreatedAt,
        emptiedAt:      role.emptiedAt,
        children: [...role.children.entries()].map(([id, e]) => ({
          id: typeof id === 'bigint' ? toHex(id) : id,
          createdAt: e.createdAt, lastRenewed: e.lastRenewed,
        })),
      });
    }
    return out;
  }

  /**
   * Host-mode introspection for health()/the relay TUI.
   * @returns {{keyspace:boolean, topics:string[]}}
   */
  inspectHosting() {
    return {
      keyspace: this._hostKeyspace,
      topics:   [...this._hostedTopics.keys()].map(t => typeof t === 'bigint' ? toHex(t) : t),
    };
  }

  /** Clean shutdown — stop the timer. */
  destroy() {
    this.stop();
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
    this._hostKeyspace = false;
    this._deliveryCallback = null;
  }
}
