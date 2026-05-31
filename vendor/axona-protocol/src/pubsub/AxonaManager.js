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
import { verifyEnvelope }          from './envelope.js';

// ── Defaults (simulator-tuned; production would use much longer values) ────

const DEFAULT_MAX_DIRECT_SUBS        = 20;           // §5.8 hysteresis (unused in 3a)
const DEFAULT_MIN_DIRECT_SUBS        = 5;            // §5.8 hysteresis (unused in 3a)
const DEFAULT_REFRESH_INTERVAL_MS    = 10_000;       // §5.5
const DEFAULT_MAX_SUBSCRIPTION_AGE_MS = 30_000;      // §5.7 — 3× refresh
const DEFAULT_ROOT_GRACE_MS          = 60_000;       // §5.7 — 6× refresh
const DEFAULT_ROOT_SET_SIZE          = 5;            // K in K-closest replication
const DEFAULT_REPLAY_CACHE_SIZE      = 100;          // per-role bounded ring (§7.8 replay)

// ── Inbound caps (D-1: bound attacker-controlled payloads) ─────────────────
// A peer must not be able to make us allocate unbounded memory from a
// single inbound message.  These cap the attacker-controlled arrays /
// payload sizes on the network-facing handlers; legitimate traffic is
// comfortably under each bound.
const MAX_PUBLISH_BYTES        = 256 * 1024;         // per-publish `json` payload ceiling (chars; see note)
// NOTE: this is the small/medium-message lane, not a blob channel. Large
// binary content (images/documents) should ride a content-reference manifest
// + out-of-band transfer (Tier 2: a DHT content store keyed by content hash),
// NOT the broadcast path — every publish is replicated to K root axons,
// cached in their replay rings, and fanned to all subscribers, so big blobs
// here amplify badly. Measured against json.length (UTF-16 code units), so
// heavily multi-byte payloads can exceed 256 KiB on the wire; a single
// message must still fit the WebRTC data-channel max-message size downstream.
const MAX_SUBSCRIBER_BATCH     = 512;                // adopt-subscribers subscriberIds[] ceiling
const MAX_PEER_ROOTS           = 32;                 // peerRoots[] ceiling (K is ~5)
const MAX_REPLAY_BATCH         = DEFAULT_REPLAY_CACHE_SIZE; // replay-batch messages[] ceiling

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
    rootSetSize          = DEFAULT_ROOT_SET_SIZE,    // K in K-closest replication
    crossFragmentRoots   = 4,                        // NX-17: number of alternate-root direct copies per publish/subscribe.
    replayCacheSize      = DEFAULT_REPLAY_CACHE_SIZE, // per-role bounded ring
    pickRecruitPeer      = null,   // protocol-specific override (§5.9)
    pickRelayPeer        = null,   // NX-17+ batch-adoption path override
    shouldRecruitSubAxon = null,   // protocol-specific override
    now                  = () => Date.now(),
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
    this._now                  = now;

    // Publisher-side: highest publishTs we have observed for each topic.
    // BigInt topicId key.
    /** @type {Map<bigint, number>} */
    this._lastSeenTsByTopic = new Map();

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
    this._seenPublishTtlMs = 60_000;

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

    /** Delivery callback — registered by PubSubAdapter via onPubsubDelivery. */
    this._deliveryCallback = null;

    // ── Post-level metrics state.  `_counters` is a nested Map:
    //   outer key: BigInt topicId
    //   inner key: postHash string (content hash, not a DHT address)
    /** @type {Map<bigint, Map<string, RelayCounters>>} */
    this._counters = new Map();

    /** requestId(string) -> { accumulated: [{responderId(hex), entries[]}] } */
    this._pendingMetricsReqs = new Map();
    this._metricsCounter = 0;

    /** requestId(string) -> { resolve } */
    this._pendingPullReqs = new Map();
    this._pullCounter     = 0;

    /** Dedup set for incoming metricsBroadcast.  string requestIds. */
    this._seenMetricsReqs    = new Set();
    this._seenMetricsReqCap  = 1024;

    // Register handlers with the DHT.
    dht.onRoutedMessage('pubsub:subscribe',    (p, m) => this._onSubscribe(p, m));
    dht.onRoutedMessage('pubsub:unsubscribe',  (p, m) => this._onUnsubscribe(p, m));
    dht.onRoutedMessage('pubsub:publish',      (p, m) => this._onPublish(p, m));
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
    dht.onDirectMessage('pubsub:metricsBroadcast', (p, m) => this._onMetricsBroadcast(p, m));
    dht.onDirectMessage('pubsub:metricsResp',      (p, m) => this._onMetricsResp(p, m));
    // ── Pull (on-demand fetch by post_hash) and reshare notifications.
    dht.onRoutedMessage('pubsub:pullReq',       (p, m) => this._onPullReq(p, m));
    dht.onDirectMessage('pubsub:pullResp',      (p, m) => this._onPullResp(p, m));
    dht.onRoutedMessage('pubsub:reshareNotify', (p, m) => this._onReshareNotify(p, m));

    this._timer = null;
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
    this._lastSeenTsByTopic.clear();
    this._receivedPublishIds.clear();
    this._seenPublishes.clear();
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
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    const publishTs = this._now();
    this._asyncPublish(topicId, json, publishId, publishTs, meta)
      .catch(err => console.error('AxonaManager: publish failed:', err));
    return publishId;
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
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        // peerRoots on the wire is hex[].
        const rootsHex = roots.map(r => typeof r === 'bigint' ? toHex(r) : r);
        for (const peerId of roots) {
          this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
            topicId: topicIdHex, subscriberId: selfHex,
            peerRoots: rootsHex, lastSeenTs,
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
            peerRoots: targetsHex, lastSeenTs,
          });
        }
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:subscribe', {
      topicId: topicIdHex, subscriberId: selfHex, lastSeenTs,
    });
  }

  pubsubUnsubscribe(topicId) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.pubsubUnsubscribe: topicId must be bigint, got ${typeof topicId}`);
    }
    this.mySubscriptions.delete(topicId);
    this._asyncUnsubscribe(topicId)
      .catch(err => console.error('AxonaManager: unsubscribe failed:', err));
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
      return res?.ok === true;
    } catch {
      return false;                                  // claims a signature but verification threw → drop
    }
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
      await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs);
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

    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });
    this._recordReceived(topicId, publishId, publishTs);

    const topicIdHex  = toHex(topicId);
    const publisherHex = publisher === null || publisher === undefined ? publisher : toHex(publisher);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        this._deliverToApp(topicId, json, publishId, publishTs);
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

  _addToReplayCache(role, entry) {
    if (!role.replayCache) role.replayCache = [];
    role.replayCache.push(entry);
    while (role.replayCache.length > this.replayCacheSize) role.replayCache.shift();
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

  _alreadySeenPublish(publishId) {
    if (!publishId) return false;
    const now = this._now();
    if (this._seenPublishes.has(publishId)) {
      this._seenPublishes.get(publishId);
      return true;
    }
    this._seenPublishes.set(publishId, now);
    if (this._seenPublishes.size > this._seenPublishCap) {
      const toDrop = this._seenPublishCap / 2;
      let i = 0;
      for (const k of this._seenPublishes.keys()) {
        if (i++ >= toDrop) break;
        this._seenPublishes.delete(k);
      }
    }
    return false;
  }

  /**
   * Deliver a publish to the local application callback EXACTLY ONCE.
   * The single funnel for every delivery path (live deliver, self-as-child
   * publish, routed publish, replay-batch, and self-replay-on-subscribe) so
   * a publishId reaches the app at most once regardless of how many roles /
   * resubscribes route it here.  Bounded LRU, same shape as _seenPublishes.
   */
  _deliverToApp(topicId, json, publishId, publishTs) {
    if (!this._deliveryCallback) return;
    if (publishId) {
      if (this._appDelivered.has(publishId)) return;     // already delivered locally
      this._appDelivered.set(publishId, this._now());
      if (this._appDelivered.size > this._appDeliveredCap) {
        const toDrop = this._appDeliveredCap / 2;
        let i = 0;
        for (const k of this._appDelivered.keys()) {
          if (i++ >= toDrop) break;
          this._appDelivered.delete(k);
        }
      }
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

    this._deliverToApp(topicId, json, publishId, publishTs);
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
    await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs);
  }

  /**
   * Replay any cached publishes newer than lastSeenTs to a subscriber.
   * subscriberId is BigInt; topicId is BigInt.
   */
  async _maybeSendReplay(topicId, role, subscriberId, lastSeenTs) {
    if (!subscriberId) return;
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
        this._deliverToApp(topicId, m.json, m.publishId, m.publishTs);
      }
      return;
    }

    const missed = (lastSeenTs != null && lastSeenTs > 0)
      ? cache.filter(m => m.publishTs > lastSeenTs)
      : cache.slice();
    if (missed.length === 0) return;
    await this.dht.sendDirect(subscriberId, 'pubsub:replay-batch', {
      topicId: toHex(topicId),
      messages: missed,
    });
  }

  _onReplayBatch(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { messages } = payload;
    if (!Array.isArray(messages) || messages.length === 0) return;
    // D-1: cap the inbound replay batch (a legit batch is ≤ replay cache).
    const batch = messages.slice(0, MAX_REPLAY_BATCH);
    for (const msg of batch) {
      const { json, publishId, publishTs } = msg;
      if (this._alreadySeenPublish(publishId)) continue;
      this._recordReceived(topicId, publishId, publishTs);
      const role = this.axonRoles.get(topicId);
      if (role) this._addToReplayCache(role, { json, publishId, publishTs });
      this._deliverToApp(topicId, json, publishId, publishTs);
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

    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });
    this._recordReceived(topicId, publishId, publishTs);

    const topicIdHex   = toHex(topicId);
    const publisherHex = publisher === null || publisher === undefined ? publisher : toHex(publisher);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        this._deliverToApp(topicId, json, publishId, publishTs);
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
    if (!byTopic) { byTopic = new Map(); this._counters.set(topicId, byTopic); }
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

  _findInReplayCache(role, postHash) {
    const cache = role?.replayCache;
    if (!cache) return null;
    for (let i = cache.length - 1; i >= 0; i--) {
      if (cache[i].postHash === postHash) return cache[i];
    }
    return null;
  }

  /**
   * Common access-control + response shape for metricsReq/Broadcast.
   * Wire fields are hex; payload comes in raw and we don't pre-convert
   * here (we only need topicId BigInt for the counter lookup).
   */
  _maybeRespondMetrics(payload, role, topicIdBig) {
    const { postHashes, requesterId, requestId } = payload;
    const requesterBig = _wire(requesterId);
    // §4.7 self-authenticating ownership check.
    const samplePost = role.replayCache?.[0];
    if (samplePost && samplePost.publisher && samplePost.publisher !== requesterBig) {
      return false;
    }
    const byTopic = this._counters.get(topicIdBig) || new Map();
    const wantedHashes = (postHashes && postHashes.length > 0)
      ? postHashes
      : [...byTopic.keys()];
    const entries = [];
    for (const h of wantedHashes) {
      const c = byTopic.get(h);
      if (c) entries.push({ ...c });
    }
    this.dht.sendDirect(requesterBig, 'pubsub:metricsResp', {
      requestId,
      responderId: toHex(this.nodeId),
      entries,
      timestamp: this._now(),
    });
    return true;
  }

  _markMetricsReqSeen(requestId) {
    if (this._seenMetricsReqs.has(requestId)) return true;
    this._seenMetricsReqs.add(requestId);
    if (this._seenMetricsReqs.size > this._seenMetricsReqCap) {
      const drop = this._seenMetricsReqCap / 2;
      let i = 0;
      for (const k of this._seenMetricsReqs) {
        if (i++ >= drop) break;
        this._seenMetricsReqs.delete(k);
      }
    }
    return false;
  }

  async _onMetricsReq(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';
    if (this._markMetricsReqSeen(requestId)) return 'consumed';

    if (!this._maybeRespondMetrics(payload, role, topicId)) return 'consumed';

    for (const [childId] of role.children) {
      if (childId === this.nodeId) continue;
      this.dht.sendDirect(childId, 'pubsub:metricsBroadcast', payload);
    }
    return 'consumed';
  }

  _onMetricsBroadcast(payload, meta) {
    const topicId = _wire(payload.topicId);
    const { requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return;
    if (this._markMetricsReqSeen(requestId)) return;

    if (!this._maybeRespondMetrics(payload, role, topicId)) return;

    const fromId = _wire(meta.fromId);
    for (const [childId] of role.children) {
      if (childId === this.nodeId) continue;
      if (childId === fromId) continue;
      this.dht.sendDirect(childId, 'pubsub:metricsBroadcast', payload);
    }
  }

  _onMetricsResp(payload, meta) {
    const { requestId, responderId, entries } = payload;
    const pending = this._pendingMetricsReqs.get(requestId);
    if (!pending) return;
    pending.accumulated.push({ responderId, entries });
  }

  // ── Pull (on-demand fetch by post_hash) ─────────────────────────────

  async _onPullReq(payload, meta) {
    const topicId    = _wire(payload.topicId);
    const requesterId = _wire(payload.requesterId);
    const { postHash, requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';

    const cached = this._findInReplayCache(role, postHash);
    if (cached) {
      this.dht.sendDirect(requesterId, 'pubsub:pullResp', {
        requestId,
        postHash,
        status:      'FOUND',
        post:        cached.json,
        responderId: toHex(this.nodeId),
      });
      this._bumpPull(topicId, postHash);
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
  requestMetrics(topicId, postHashes, { timeoutMs = 500 } = {}) {
    if (typeof topicId !== 'bigint') {
      throw new TypeError(`AxonaManager.requestMetrics: topicId must be bigint, got ${typeof topicId}`);
    }
    const requestId = `${this.nodeId}:m${++this._metricsCounter}`;
    return new Promise(resolve => {
      const pending = { accumulated: [] };
      this._pendingMetricsReqs.set(requestId, pending);
      this.dht.routeMessage(topicId, 'pubsub:metricsReq', {
        topicId:     toHex(topicId),
        postHashes:  postHashes ?? null,
        requesterId: toHex(this.nodeId),
        requestId,
      });
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

  /** Clean shutdown — stop the timer. */
  destroy() {
    this.stop();
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._deliveryCallback = null;
  }
}
