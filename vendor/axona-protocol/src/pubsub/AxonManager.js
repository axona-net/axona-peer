/**
 * AxonManager — distributed pub/sub membership protocol.
 *
 * Implements the PubSubAdapter transport contract on top of a DHT that
 * provides the routed-messaging primitives (routeMessage, onRoutedMessage,
 * sendDirect, onDirectMessage).
 *
 * This is Phase 3a: core membership with timestamped children, periodic
 * refresh, and TTL-based expiry. No sub-axon recruitment — a single axon
 * per topic holds every subscriber. Phase 3b introduces recruitment and
 * orderly collapse; Phase 3c adds explicit churn recovery.
 *
 * Message types (over the DHT):
 *
 *   ROUTED:
 *     pubsub:subscribe   — payload: { topicId, subscriberId }
 *     pubsub:unsubscribe — payload: { topicId, subscriberId }
 *     pubsub:publish     — payload: { topicId, json }
 *
 *   DIRECT:
 *     pubsub:deliver     — payload: { topicId, json }
 *
 * See documents/Phase3-Membership-Protocol-Plan.md for the full design
 * rationale, state model, and parameter defaults.
 */

// ── Defaults (simulator-tuned; production would use much longer values) ────

const DEFAULT_MAX_DIRECT_SUBS        = 20;           // §5.8 hysteresis (unused in 3a)
const DEFAULT_MIN_DIRECT_SUBS        = 5;            // §5.8 hysteresis (unused in 3a)
const DEFAULT_REFRESH_INTERVAL_MS    = 10_000;       // §5.5
const DEFAULT_MAX_SUBSCRIPTION_AGE_MS = 30_000;      // §5.7 — 3× refresh
const DEFAULT_ROOT_GRACE_MS          = 60_000;       // §5.7 — 6× refresh
const DEFAULT_ROOT_SET_SIZE          = 5;            // K in K-closest replication
const DEFAULT_REPLAY_CACHE_SIZE      = 100;          // per-role bounded ring (§7.8 replay)

// ── AxonManager ────────────────────────────────────────────────────────────

export class AxonManager {
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
    crossFragmentRoots   = 4,                        // NX-17: number of alternate-root direct copies per publish/subscribe. Total K = crossFragmentRoots + 1 = 5 by default. K=5 matches the K-overlap measurement (100 % set agreement at K=5 even under cap pressure), so publisher and subscriber target identical sets and fragmentation cannot occur. K=2/3 dropped to ~80 % delivery in v0.64.03/04 because top-3 ordering drifts under cap even when top-5 is set-stable.
    replayCacheSize      = DEFAULT_REPLAY_CACHE_SIZE, // per-role bounded ring
    pickRecruitPeer      = null,   // protocol-specific override (§5.9)
    pickRelayPeer        = null,   // NX-17+ batch-adoption path override
    shouldRecruitSubAxon = null,   // protocol-specific override
    now                  = () => Date.now(),
  } = {}) {
    if (!dht) throw Error('AxonManager: dht is required');

    this.dht                   = dht;
    this.nodeId                = dht.getSelfId();
    this.maxDirectSubs         = maxDirectSubs;
    this.minDirectSubs         = minDirectSubs;
    this.refreshIntervalMs     = refreshIntervalMs;
    this.maxSubscriptionAgeMs  = maxSubscriptionAgeMs;
    this.rootGraceMs           = rootGraceMs;
    this.rootSetSize           = rootSetSize;
    this.crossFragmentRoots    = crossFragmentRoots;
    this.replayCacheSize       = replayCacheSize;
    this._now                  = now;

    // Publisher-side: highest publishTs we have observed for each topic
    // (populated from _onDeliver and _onReplayBatch). Included as
    // `lastSeenTs` in every outgoing subscribe so the receiving axon
    // can replay anything newer from its own cache. First-time
    // subscribers have no entry → lastSeenTs is omitted, which tells
    // the receiving axon "replay everything you have."
    this._lastSeenTsByTopic = new Map();    // topicId → number (ms)

    // Set of publishIds this node has ever received, keyed by topic.
    // Used to: (a) deduplicate messages on receive and (b) drive the
    // cumulative-delivery metric in the live membership simulation
    // (see Engine.runMembershipPubSubTick — count of received publishes
    // vs total publishes per (sub, topic) across the whole run).
    this._receivedPublishIds = new Map();   // topicId → Set<publishId>

    // Per-node outgoing publishId counter (monotonic, combined with
    // nodeId to be globally unique across the network).
    this._publishCounter = 0;

    // Per-node LRU of publishIds we have already processed, so a node
    // never fans out or delivers the same publish twice. Without this,
    // the K-closest replication + sub-axon recruitment produces
    // exponential duplication: a subscriber that appears in multiple
    // sub-axons' children maps receives many copies, and if they are
    // themselves a sub-axon, each copy triggers another fan-out round.
    this._seenPublishes = new Map();     // publishId -> insertedAt (ms)
    this._seenPublishCap = 4096;         // bounded size (LRU-ish)
    this._seenPublishTtlMs = 60_000;     // entries expire after 60 s

    // ── Per-AxonManager findKClosest cache ──────────────────────────────
    // Each call to dht.findKClosest from this AxonManager (publish,
    // subscribe, refreshTick) is expensive at scale (~5-15ms at 25K).
    // The result for a given (topicId, K) depends on this node's local
    // routing table and the network topology; both are stable between
    // churn events. Caching per-(topicId, K) per-node lets a real
    // protocol implementation amortise the cost across many calls
    // within a stable interval — exactly what a real deployment would
    // do, and only using information local to this node.
    //
    // Invalidation: invalidateKClosestCache() bumps the epoch; cache
    // entries from prior epochs are discarded on next access. The
    // Engine calls it after churn events.
    this._kClosestCache = new Map();     // `${topicHex}_${K}` -> { epoch, value: peerIds[] }
    this._kClosestEpoch = 0;

    // Policy overrides — if provided, replace the default methods. The DHT
    // (e.g., NX-15) is the usual source; it injects its protocol-specific
    // selection using its own routing-table state.
    if (pickRecruitPeer)      this.pickRecruitPeer      = pickRecruitPeer;
    if (shouldRecruitSubAxon) this.shouldRecruitSubAxon = shouldRecruitSubAxon;
    // pickRelayPeer: batch-adoption path (NX-17+). Returns an external
    // synaptome peer id (hex) to act as a new sub-axon. If the override
    // returns null or is not provided, the axon falls back to the
    // legacy single-recruit path (picking from existing children).
    if (pickRelayPeer)        this.pickRelayPeer        = pickRelayPeer;

    /** topicHash -> TopicRole */
    this.axonRoles = new Map();

    /** topicHash -> TopicSub (my own subscription state — only if I called subscribe) */
    this.mySubscriptions = new Map();

    /** Delivery callback — registered by PubSubAdapter via onPubsubDelivery. */
    this._deliveryCallback = null;

    // ── Post-level metrics state (additive; only populated when callers
    //    pass meta.postHash on publish). Old tests are unaffected.
    /** topicId -> postHash -> { delivery_count, pull_count, reshare_count,
     *                          first_seen, last_updated }.
     *  Held at every node that runs a fan-out (root + sub-axons).
     *  delivery_count is bumped per forward-to-direct-subscriber. */
    this._counters = new Map();

    /** requestId -> { accumulated: [{responderId, entries[]}] } —
     *  in-flight metrics requests this node has issued. */
    this._pendingMetricsReqs = new Map();
    this._metricsCounter = 0;

    /** requestId -> { resolve } — in-flight pull() requests; resolved
     *  on first FOUND response or NOT_FOUND-from-anyone-or-timeout. */
    this._pendingPullReqs = new Map();
    this._pullCounter     = 0;

    /** Dedup set for incoming metricsBroadcast (one request can reach a
     *  node twice through a sub-axon tree's branching). Bounded ring. */
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
    // ── Post-level metrics (additive; only active when posts carry a
    //    postHash so existing callers see no change in behavior).
    dht.onRoutedMessage('pubsub:metricsReq',       (p, m) => this._onMetricsReq(p, m));
    dht.onDirectMessage('pubsub:metricsBroadcast', (p, m) => this._onMetricsBroadcast(p, m));
    dht.onDirectMessage('pubsub:metricsResp',      (p, m) => this._onMetricsResp(p, m));
    // ── Pull (on-demand fetch by post_hash) and reshare notifications.
    dht.onRoutedMessage('pubsub:pullReq',       (p, m) => this._onPullReq(p, m));
    dht.onDirectMessage('pubsub:pullResp',      (p, m) => this._onPullResp(p, m));
    dht.onRoutedMessage('pubsub:reshareNotify', (p, m) => this._onReshareNotify(p, m));

    // Periodic refresh + TTL sweep. We use a single timer driving both
    // actions; refreshTick() is exported for tests that prefer to pump
    // the state machine manually rather than rely on wall-clock timers.
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
   * Per-node findKClosest with epoch-keyed local cache. All AxonManager
   * call sites that need K-closest peers should use this helper instead
   * of calling `this.dht.findKClosest` directly — the cache amortises
   * cost across the pub/sub primitives (publish, subscribe, refreshTick)
   * within a single network-stability interval.
   *
   * Locality preserved: the cache is per-AxonManager (per-node). Each
   * node still computes its own answer on first miss using only its
   * local routing table; no inter-node sharing of results.
   */
  async _findKClosest(topicId, K) {
    const key = `${typeof topicId === 'string' ? topicId : topicId.toString(16).padStart(16, '0')}_${K}`;
    const entry = this._kClosestCache.get(key);
    if (entry && entry.epoch === this._kClosestEpoch) {
      return entry.value;
    }
    const value = await this.dht.findKClosest(topicId, K);
    this._kClosestCache.set(key, { epoch: this._kClosestEpoch, value });
    return value;
  }

  /**
   * Invalidate this node's K-closest cache. Called by the engine (or in a
   * real deployment, by a churn-detection mechanism) when network
   * topology may have changed enough to invalidate cached K-sets. Bumps
   * the epoch so all entries are stale on next access; the Map self-prunes
   * lazily on miss.
   */
  invalidateKClosestCache() {
    this._kClosestEpoch++;
    // Cap memory: full clear if the map has grown unboundedly. Subscribers
    // typically hold 1-5 topics, axon hosts up to ~maxDirectSubs roles —
    // so the natural size is small, but defensive clear handles edge cases.
    if (this._kClosestCache.size > 256) this._kClosestCache.clear();
  }

  /**
   * Clear all pub/sub runtime state so this AxonManager appears fresh
   * to the next test or test phase, while preserving:
   *   - DHT handler registrations (onRoutedMessage/onDirectMessage)
   *   - Configuration (maxDirectSubs, refreshIntervalMs, …)
   *   - Policy overrides (pickRecruitPeer, pickRelayPeer, …)
   *
   * Clears per-topic trees, subscriptions, replay caches, dedup sets,
   * and publishId counters. Synaptic weights and routing-table state
   * live at the NeuronNode/DHT level and are NOT touched — so any
   * pub/sub-driven LTP training survives across a reset.
   *
   * Intended for test infrastructure (Engine.runBenchmark runs several
   * pub/sub tests back-to-back and needs each to start independent).
   * Production nodes should use graceful unsubscribe + TTL sweep
   * instead of calling this.
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
   * Publish over the network. In K-closest mode, pick the best of the K
   * current closest-to-hash roots and sendDirect — they have replicated
   * subscriber lists, so any one of K is sufficient. Falls back to a
   * routed publish when the transport doesn't expose findKClosest.
   *
   * Every publish carries a globally-unique publishId (nodeId + counter).
   * Receivers track publishIds they've already processed to prevent
   * exponential duplication when the fan-out tree has overlapping paths
   * (common under K-closest replication + sub-axon recruitment).
   */
  /**
   * v0.70.16 — pubsubPublish is sync from the caller's perspective: it
   * stamps publishId + timestamp synchronously and returns the
   * publishId immediately so `entry.adapter.publish(...)` keeps its
   * old contract.  The actual DHT sendDirect/routeMessage primitives
   * run in the background as a fire-and-forget Promise chain — this
   * matches v0.70.15 behavior (the underlying network transport
   * — SimulatedNetwork — was already async via setTimeout, but the
   * AxonManager wrapper presented a sync API).
   */
  /**
   * Publish to a topic.
   *
   * @param {string|number} topicId
   * @param {string} json   Application payload (string — typically
   *                        JSON.stringify of a SignedPost).
   * @param {Object} [meta] Optional post-level metadata that the relay
   *                        layer uses for metrics. When omitted, no
   *                        counters are touched — preserves legacy
   *                        single-arg behavior.
   * @param {string} [meta.postHash]   Content hash of the post.
   * @param {string} [meta.publisher]  Publisher identifier.
   * @param {Array}  [meta.references] PostRefs this post points back at
   *                                   (used for reshare_count tracking
   *                                   in a later PR).
   * @returns {string} publishId
   */
  pubsubPublish(topicId, json, meta) {
    const publishId = `${this.nodeId}:${++this._publishCounter}`;
    const publishTs = this._now();
    this._asyncPublish(topicId, json, publishId, publishTs, meta)
      .catch(err => console.error('AxonManager: publish failed:', err));
    return publishId;
  }

  /**
   * @private
   * Burst-send: once findKClosest resolves we issue all sendDirect calls
   * synchronously in a single microtask so concurrent publishes don't
   * interleave at the wire layer.  Critical for tests like
   * test_integration's gap-detection that drop the first publish's
   * full K-fan-out as a contiguous prefix of wire transactions.
   */
  async _asyncPublish(topicId, json, publishId, publishTs, meta) {
    // meta is forwarded as { postHash, publisher } so relays can
    // attribute counters without parsing the opaque `json` payload.
    const postHash   = meta?.postHash   || null;
    const publisher  = meta?.publisher  || null;
    const references = meta?.references || null;

    // For every PostRef in this publish, route a one-shot notification
    // toward the referenced topic so that topic's root learns about
    // the reshare and bumps reshare_count. Fire-and-forget; the count
    // is best-effort, matching §6.4 (metrics are advisory).
    if (postHash && Array.isArray(references) && references.length > 0) {
      for (const ref of references) {
        if (!ref?.topic_id || !ref?.post_hash) continue;
        this.dht.routeMessage(ref.topic_id, 'pubsub:reshareNotify', {
          refTopicId:        ref.topic_id,
          refPostHash:       ref.post_hash,
          resharerTopicId:   topicId,
          resharerPostHash:  postHash,
          resharerPublisher: publisher,
        });
      }
    }

    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const target of roots) {
          this.dht.sendDirect(target, 'pubsub:publish-k', {
            topicId, json, publishId, publishTs, postHash, publisher,
          });
        }
        return;
      }
    }

    // NX-17 axonal mode with cross-fragment redundancy.  See v0.66.x
    // commit notes for the K-overlap rationale.
    const K = (this.crossFragmentRoots ?? 0) + 1;
    if (K > 1 && typeof this.dht.findKClosest === 'function') {
      const targets = await this._findKClosest(topicId, K);
      if (targets.length > 0) {
        for (const target of targets) {
          if (target === this.nodeId) continue;
          this.dht.sendDirect(target, 'pubsub:publish-k', {
            topicId, json, publishId, publishTs, postHash, publisher,
          });
        }
        return;
      }
    }
    // Fallback (K=1 or no findKClosest): single routed walk.
    this.dht.routeMessage(topicId, 'pubsub:publish', {
      topicId, json, publishId, publishTs, postHash, publisher,
    });
  }

  /**
   * Subscribe to a topic. In K-closest mode, STOREs the subscription at
   * each of the K nodes closest to hash(topic) so the publisher can hit
   * any one of them and still find us. Falls back to a routed subscribe
   * when the transport doesn't expose findKClosest.
   */
  /**
   * v0.70.16 — sync wrapper around an async _asyncSubscribe chain.
   * Records mySubscriptions immediately so callers that read it on
   * the next line see the subscription before the network ops drain.
   */
  pubsubSubscribe(topicId) {
    this.mySubscriptions.set(topicId, { subscribedAt: this._now() });
    const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
    this._asyncSubscribe(topicId, lastSeenTs)
      .catch(err => console.error('AxonManager: subscribe failed:', err));
  }

  /** @private — burst-send pattern; see _asyncPublish doc. */
  async _asyncSubscribe(topicId, lastSeenTs) {
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const peerId of roots) {
          this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
            topicId, subscriberId: this.nodeId, peerRoots: roots, lastSeenTs,
          });
        }
        return;
      }
    }
    const K = (this.crossFragmentRoots ?? 0) + 1;
    if (K > 1 && typeof this.dht.findKClosest === 'function') {
      const targets = await this._findKClosest(topicId, K);
      if (targets.length > 0) {
        for (const target of targets) {
          if (target === this.nodeId) continue;
          this.dht.sendDirect(target, 'pubsub:subscribe-k', {
            topicId, subscriberId: this.nodeId, peerRoots: targets, lastSeenTs,
          });
        }
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:subscribe',
                          { topicId, subscriberId: this.nodeId, lastSeenTs });
  }

  pubsubUnsubscribe(topicId) {
    this.mySubscriptions.delete(topicId);
    this._asyncUnsubscribe(topicId)
      .catch(err => console.error('AxonManager: unsubscribe failed:', err));
  }

  /** @private — burst-send pattern. */
  async _asyncUnsubscribe(topicId) {
    if (this._useKClosestMode()) {
      const roots = await this._findKClosest(topicId, this.rootSetSize);
      if (roots.length > 0) {
        for (const peerId of roots) {
          this.dht.sendDirect(peerId, 'pubsub:unsubscribe-k', {
            topicId, subscriberId: this.nodeId,
          });
        }
        return;
      }
    }
    this.dht.routeMessage(topicId, 'pubsub:unsubscribe',
                          { topicId, subscriberId: this.nodeId });
  }

  /** True when the underlying transport supports K-closest lookup. */
  _useKClosestMode() {
    return this.rootSetSize > 0 && typeof this.dht.findKClosest === 'function';
  }

  onPubsubDelivery(callback) {
    this._deliveryCallback = callback;
  }

  // ── Handlers ────────────────────────────────────────────────────────

  /**
   * SUBSCRIBE routed message handler.
   *
   * - If we are already the axon for topicId:
   *     - Renewal: bump lastRenewed.
   *     - New subscriber under capacity: add to children.
   *     - New subscriber over capacity: recruit a sub-axon (§5.2, §5.8).
   * - If we are the terminal (closest-to-hash) and no axon exists: become root.
   * - Otherwise: forward along the route.
   */
  async _onSubscribe(payload, meta) {
    const { topicId, subscriberId, lastSeenTs } = payload;
    const role = this.axonRoles.get(topicId);
    const now = this._now();

    if (role) {
      // Self-subscribe: every axon role (root AND non-root) periodically
      // re-issues its own subscribe toward topicId as a self-heal. Never
      // register the self-subscriber as our own child; let the walker
      // continue. Outcomes:
      //   - Non-root: walker continues upstream, reaches the current
      //     parent (or a new live axon on the path), which refreshes
      //     our registration as one of its children.
      //   - Root that is still closest: walker has no closer peer,
      //     globality check confirms, the walk ends at root as a no-op.
      //   - Root that has been superseded by a newly-joined closer
      //     node: globality check forwards us to that node; it becomes
      //     a new root with us as its first child, and subscribers
      //     eventually re-route through the same mechanism.
      if (subscriberId === this.nodeId) return 'forward';
      await this._addOrRecruitChild(topicId, role, subscriberId, meta.fromId);
      // Reply with any cached publishes the subscriber missed since
      // lastSeenTs. If lastSeenTs is omitted (brand-new subscriber),
      // replay the whole cache.
      await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs);
      return 'consumed';
    }

    if (meta.isTerminal) {
      // Become the root for this topic.
      this.axonRoles.set(topicId, {
        parentId:       null,
        isRoot:         true,
        children:       new Map([[subscriberId, { createdAt: now, lastRenewed: now }]]),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,   // §5.8 — when children.size first dropped below minDirectSubs
      });
      return 'consumed';
    }

    return 'forward';
  }

  // ── Shared add-or-recruit primitive ─────────────────────────────────

  /**
   * Add `subscriberId` as a child of `role`, or — if `role` is at
   * capacity — promote an existing child to sub-axon and delegate. The
   * recruit pathway always preserves `role.children.size ≤ maxDirectSubs`.
   *
   * This is invoked from every code path that materialises a new child:
   *   - _onSubscribe         (routed subscribe landing at a terminal)
   *   - _onSubscribeDirect   (K-closest STORE)
   *   - _onPromoteAxon       (parent telling us to take a delegated sub)
   *
   * Sharing the implementation is essential: without it, sub-axons that
   * receive repeated promote-axon messages accumulate subscribers past
   * the cap because the recruit-check was only in the subscribe paths.
   */
  async _addOrRecruitChild(topicId, role, subscriberId, forwarderId = null) {
    const now = this._now();
    const existing = role.children.get(subscriberId);
    if (existing) { existing.lastRenewed = now; return; }

    if (this.shouldRecruitSubAxon(role)) {
      // ── PREFERRED PATH: batch adoption via an external relay peer ──────
      // (NX-17+). When the DHT provides pickRelayPeer, we pick a peer
      // from its routing tables (not from our existing children) to
      // become a new sub-axon, partition the subscribers currently
      // attached to us by XOR direction toward that peer, and hand off
      // the "in-direction" subset in a single batch. The receiver
      // creates a role, adds all of them as children, and issues its
      // own subscribe upward which attaches it into the live tree.
      //
      // Children in this model don't track a parentId — which peer is
      // currently delivering to them is whoever most-recently sendDirect'd
      // them a publish. When that peer dies, the next refresh re-subscribe
      // re-attaches them into the live tree at whichever axon their
      // routed subscribe lands on.
      //
      // TODO (future redundancy pass — not part of this change): when a
      // subscriber has missed messages because of interim churn, let the
      // subscribe payload include a "last received timestamp". Relays
      // and the root can keep a bounded ring of recent publishes and
      // forward any unseen ones to the re-subscriber. Gives a level of
      // replay redundancy without requiring multiple-root replication.
      if (typeof this.pickRelayPeer === 'function') {
        const relayId = this.pickRelayPeer(role, subscriberId, forwarderId);
        if (relayId && relayId !== this.nodeId && relayId !== forwarderId
            && !role.children.has(relayId)) {
          // Batch is the TOP-K existing children ranked XOR-closest to
          // the new relay, plus the new subscriber. Using top-K (K =
          // maxDirectSubs/2) instead of the strict "closer to relay
          // than to self" partition guarantees that each overflow moves
          // a meaningful number of children off us even when every
          // existing child happens to be geographically nearer to us
          // than to the relay (the common case when subscribers cluster
          // in the topic's own cell). Without this guarantee, an empty
          // partition would let role.children keep growing on every
          // overflow, triggering an uncontrolled cascade of new-relay
          // recruitments when the relay's self-subscribe loops back.
          const K = Math.max(1, Math.floor(this.maxDirectSubs / 2));
          const batch = this._selectChildrenClosestToRelay(role, relayId, K);
          batch.push(subscriberId);
          for (const id of batch) role.children.delete(id);
          // Pre-add the relay as our sub-axon edge BEFORE sending the
          // adopt message. Reason: the relay's _onAdoptSubscribers
          // handler issues its own routed subscribe upward; in a high
          // fraction of cases that subscribe lands right back at us
          // (we're usually the closest-to-topicId live axon on its
          // path). Having the relay already in our children makes that
          // arrival idempotent (just refresh lastRenewed) instead of
          // looking like a brand-new overflow and recursing into
          // another relay recruitment.
          role.children.set(relayId, {
            createdAt: now, lastRenewed: now, isSubaxon: true,
          });
          await this.dht.sendDirect(relayId, 'pubsub:adopt-subscribers', {
            topicId,
            subscriberIds: batch,
          });
          return;
        }
      }

      // ── FALLBACK PATH: legacy single-recruit from existing children ────
      // Used for K-closest mode (NX-15 with rootSetSize > 0) where
      // pickRelayPeer is not configured, and when pickRelayPeer returns
      // nothing actionable.
      const reuseId = this._pickExistingSubAxon(role, subscriberId, forwarderId);
      if (reuseId) {
        role.children.get(reuseId).lastRenewed = now;
        await this.dht.sendDirect(reuseId, 'pubsub:promote-axon', {
          topicId,
          newSubscriberId: subscriberId,
          parentId:        this.nodeId,
        });
        return;
      }

      const meta = { fromId: forwarderId || subscriberId };
      let recruitId = this.pickRecruitPeer(role, meta, subscriberId);
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
          topicId,
          newSubscriberId: subscriberId,
          parentId:        this.nodeId,
        });
        return;
      }
      // No non-self child to recruit. Only happens if role.children is
      // empty or contains only us. Just add the subscriber directly —
      // over-capacity is a lesser evil than infinite recursion.
    }
    role.children.set(subscriberId, { createdAt: now, lastRenewed: now, isSubaxon: false });
  }

  /**
   * Return the top-K existing children ranked by XOR-closeness to the
   * relay id, excluding self and the relay itself. Used by the batch-
   * adoption path to guarantee forward progress on every overflow even
   * when no strict-partition children exist (i.e., every child is
   * technically closer to us than to the new relay — which is typical
   * when subscribers cluster in the topic's own cell and the relay is
   * an external peer in a different cell).
   */
  _selectChildrenClosestToRelay(role, relayId, k) {
    const relayBig = BigInt('0x' + relayId);
    const ranked = [];
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;
      if (childId === relayId)     continue;
      const cBig = BigInt('0x' + childId);
      ranked.push({ childId, dist: cBig ^ relayBig });
    }
    ranked.sort((a, b) => (a.dist < b.dist ? -1 : a.dist > b.dist ? 1 : 0));
    return ranked.slice(0, k).map(r => r.childId);
  }

  /**
   * [Legacy helper, kept for reference.] Strict partition: return children
   * whose XOR distance to `relayId` is smaller than their distance to us.
   * No longer the default overflow path because an empty partition
   * (common in practice) causes unbounded growth; see the batch logic
   * in _addOrRecruitChild for what's used instead.
   */
  _partitionChildrenForRelay(role, relayId) {
    const relayBig = BigInt('0x' + relayId);
    const selfBig  = BigInt('0x' + this.nodeId);
    const batch    = [];
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;
      if (childId === relayId)     continue;  // don't ship the relay to itself
      const cBig = BigInt('0x' + childId);
      const dRelay = cBig ^ relayBig;
      const dSelf  = cBig ^ selfBig;
      if (dRelay < dSelf) batch.push(childId);
    }
    return batch;
  }

  /**
   * Pick an existing child we've already promoted to sub-axon status, if
   * any, to route further overflow subscribers through. Excludes self and
   * the forwarder (loop protection — same reasoning as
   * _pickExistingChildForRecruit). When multiple sub-axon children exist
   * (a rare cascade case), returns the one XOR-closest to the new
   * subscriber for locality.
   */
  _pickExistingSubAxon(role, subscriberId, forwarderId) {
    const selfHex = this.nodeId;
    const candidates = [];
    for (const [id, child] of role.children) {
      if (!child.isSubaxon) continue;
      if (id === selfHex || id === forwarderId) continue;
      candidates.push(id);
    }
    if (candidates.length === 0) return null;
    if (candidates.length === 1) return candidates[0];
    const subBig = BigInt('0x' + subscriberId);
    let best = null, bestDist = null;
    for (const id of candidates) {
      const d = BigInt('0x' + id) ^ subBig;
      if (bestDist === null || d < bestDist) { bestDist = d; best = id; }
    }
    return best;
  }

  // ── Policy hooks (overridable by subclasses) ───────────────────────

  /**
   * Should this axon recruit a sub-axon for the next new subscriber?
   * Default: when children.size reaches maxDirectSubs.
   * Override on a subclass (e.g., NX-15) to use protocol-specific
   * knowledge — for instance to refuse recruitment when no high-weight
   * synapse is available as a recruit candidate.
   */
  shouldRecruitSubAxon(role) {
    return role.children.size >= this.maxDirectSubs;
  }

  /**
   * Pick which EXISTING child should be promoted to sub-axon. The return
   * value MUST be an id already present in `role.children` — we never
   * grow an axon beyond its cap during recruitment.
   *
   * Default: the existing child with smallest XOR distance to the new
   * subscriber. That child sits on the natural routing path toward future
   * subscribers in the same ID-space region.
   *
   * Override: NX-15 prefers existing children that also appear in the
   * node's synaptome with high weight.
   */
  pickRecruitPeer(role, meta, subscriberId) {
    // Always pick the XOR-closest non-self, non-forwarder existing child.
    // Short-cutting to meta.fromId caused a ping-pong loop when two
    // cross-recruited sub-axons each had the other in children:
    //   A picks B → promote-axon to B → B.meta.fromId = A → B picks A →
    //   promote-axon to A → A.meta.fromId = B → A picks B → loop.
    return this._pickExistingChildForRecruit(role, subscriberId, meta.fromId);
  }

  /** XOR-closest existing child to `subscriberId`, excluding self AND the
   *  peer that forwarded us this message. Recruiting either creates
   *  infinite loops:
   *    - Self: promote-axon to self → loop back into _onPromoteAxon → loop.
   *    - Forwarder: if they're a cross-recruited sub-axon (i.e., they
   *      have US in THEIR children too), they'll pick us as recruit
   *      when processing the promote, and we ping-pong. Both nodes
   *      being each other's children happens when a common parent
   *      delegated both as sub-axons with the other as newSubscriberId.
   */
  _pickExistingChildForRecruit(role, subscriberId, excludeId = null) {
    if (role.children.size === 0) return null;
    const subBig = BigInt('0x' + subscriberId);
    let best = null;
    let bestDist = null;
    for (const childId of role.children.keys()) {
      if (childId === this.nodeId) continue;    // never recruit self
      if (childId === excludeId)   continue;    // never recruit the forwarder
      const d = BigInt('0x' + childId) ^ subBig;
      if (bestDist === null || d < bestDist) { bestDist = d; best = childId; }
    }
    return best;
  }

  /**
   * UNSUBSCRIBE routed message handler.
   *
   * If we are the axon holding this subscriber, drop them from children.
   * We do NOT eagerly dissolve an emptied non-root axon — that is handled
   * by §5.7 TTL sweep (or §5.8 hysteresis in Phase 3b). This keeps
   * unsubscribe semantics uniform with TTL expiry and avoids flapping on
   * rapid subscribe/unsubscribe churn.
   */
  async _onUnsubscribe(payload, meta) {
    const { topicId, subscriberId } = payload;
    const role = this.axonRoles.get(topicId);
    if (role && role.children.has(subscriberId)) {
      role.children.delete(subscriberId);
      if (role.children.size === 0) role.emptiedAt = this._now();
      return 'consumed';
    }
    return 'forward';
  }

  /**
   * PUBLISH routed message handler.
   *
   * If we are the axon for this topic, fan out to all children via
   * sendDirect. Otherwise, forward along the route.
   */
  async _onPublish(payload, meta) {
    const { topicId, json, publishId, publishTs, postHash, publisher } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';
    // Only the ROOT consumes a routed publish and initiates fan-out.
    // Sub-axons on the path forward without intercepting: otherwise the
    // publisher's routeMessage path — which often crosses a sub-axon
    // before reaching the globally-closest root, especially in the
    // clustered case where the tree concentrates near the publisher —
    // would fan out to ONLY that sub-axon's subtree. All other
    // subscribers (root's direct leaves + other sub-axons' subtrees)
    // would silently miss. Forwarding at sub-axons lets the walk reach
    // the root, whose fan-out covers the full tree via the normal
    // root→children→sub-axon cascade.
    if (!role.isRoot) return 'forward';
    if (this._alreadySeenPublish(publishId)) return 'consumed';

    // Add to the replay cache BEFORE fan-out, so even if fan-out partly
    // fails, the cache reflects everything the root accepted.
    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });

    // Also track locally-received for publishId dedup and metric
    // accounting (the root itself is "receiving" its own publish when
    // we include self in children).
    this._recordReceived(topicId, publishId, publishTs);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
        // Self is also a direct subscriber when it appears in children.
        if (postHash) this._bumpDelivery(topicId, postHash);
        continue;
      }
      const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
        topicId, json, publishId, publishTs, postHash, publisher,
      });
      if (!ok) {
        deadChildren.push(childId);
      } else if (postHash) {
        // Count one delivery for each child the root successfully
        // forwarded to. Sub-axon children will count their own
        // sub-tree deliveries when they re-fan in _onDeliver, so
        // root counts the direct hop and sub-axons count the rest.
        this._bumpDelivery(topicId, postHash);
      }
    }
    for (const dead of deadChildren) role.children.delete(dead);
    return 'consumed';
  }

  /** Append a published message to a role's bounded replay cache. */
  _addToReplayCache(role, entry) {
    if (!role.replayCache) role.replayCache = [];
    role.replayCache.push(entry);
    while (role.replayCache.length > this.replayCacheSize) role.replayCache.shift();
  }

  /** Record reception of a publishId + timestamp for this topic. Used
   *  both for dedup on any future delivery (_alreadySeenPublish is a
   *  separate mechanism at the per-hop level; this one is per-topic
   *  and survives beyond the LRU window) and for the cumulative-
   *  delivery metric read by the engine via _receivedPublishIds. */
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

  /**
   * Publish-id deduplication. Returns true if we've already processed
   * this publishId and should drop; returns false (first-time) and
   * records it otherwise. Bounded by _seenPublishCap and TTL'd by
   * _seenPublishTtlMs; both are enforced opportunistically on insert.
   */
  _alreadySeenPublish(publishId) {
    if (!publishId) return false;            // legacy / untagged publishes never dedup
    const now = this._now();
    if (this._seenPublishes.has(publishId)) {
      this._seenPublishes.get(publishId);    // insertion order unchanged
      return true;
    }
    this._seenPublishes.set(publishId, now);
    if (this._seenPublishes.size > this._seenPublishCap) {
      // Evict oldest half. Map iterates in insertion order.
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
   * DIRECT delivery handler — fires when an axon fan-out reaches us.
   *
   * Re-fan rule: only NON-ROOT axons (sub-axons recruited beneath a root)
   * re-broadcast to their children. Roots — the K-closest replicas for a
   * topic — never re-fan from a received pubsub:deliver. Rationale:
   *
   *   The publisher chose ONE root out of K and sent publish-k to it.
   *   That root's _onPublishDirect already initiated a complete fan-out
   *   through its own subtree. The other K-1 roots are not involved in
   *   this publish at all; their subscriber lists are full replicas but
   *   the protocol only uses one root per publish.
   *
   *   Without this rule, a subscriber that happens to also be a K-closest
   *   root creates a cycle: Root A fans to child S (= Root B). S's
   *   _onDeliver fires, S re-fans to its children (including A). A's
   *   _onDeliver fires, re-fans to S. Ping-pong forever.
   *
   * Leaves (no role) and sub-axons (role.isRoot === false) still deliver
   * locally + fan down appropriately.
   */
  async _onDeliver(payload, meta) {
    const { topicId, json, publishId, publishTs, postHash, publisher } = payload;
    if (this._alreadySeenPublish(publishId)) return;

    // Track per-topic receipt + lastSeenTs regardless of whether we
    // hold a role. Populates the outgoing `lastSeenTs` for our future
    // subscribe refreshes, and feeds the cumulative-delivery metric.
    this._recordReceived(topicId, publishId, publishTs);

    const role = this.axonRoles.get(topicId);
    if (role) {
      // If we hold a role for this topic, cache the message so we can
      // replay it to any subscriber that arrives (or re-subscribes with
      // a stale lastSeenTs). Both roots and sub-axons cache.
      this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });

      if (!role.isRoot) {
        // Sub-axon: re-fan to our children. Skip the upstream peer and self.
        for (const [childId] of role.children) {
          if (childId === meta.fromId) continue;
          if (childId === this.nodeId) continue;
          const ok = await this.dht.sendDirect(childId, 'pubsub:deliver', {
            topicId, json, publishId, publishTs, postHash, publisher,
          });
          // Sub-axon counts each successful forward to its sub-tree.
          if (ok && postHash) this._bumpDelivery(topicId, postHash);
        }
      }
    }

    // Local delivery to the adapter — always fires, whether or not we
    // have a role for this topic. Matches the app-level "I subscribed
    // to this topic" expectation.
    if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
  }

  /**
   * DIRECT handler — an upstream axon has promoted us to sub-axon status.
   * Payload: { topicId, newSubscriberId, parentId }.
   *
   * If we already have a role for this topic, just add newSubscriberId to
   * our children (the promoter is idempotent; they may send multiple
   * promote-axon messages as new subscribers arrive through us).
   *
   * If we do not have a role, create one with parentId set to the
   * promoter. Our own refreshTick will issue subscribes upward so the
   * promoter keeps us in its children.
   */
  async _onPromoteAxon(payload, meta) {
    const { topicId, newSubscriberId, parentId } = payload;
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
      // Immediately refresh upward so the promoter sees us as one of
      // their children right away (without waiting for the next tick).
      await this.dht.routeMessage(topicId, 'pubsub:subscribe',
                                  { topicId, subscriberId: this.nodeId });
    }

    // Use the shared add-or-recruit path so we cascade into our OWN
    // recruitment when at capacity, instead of hoarding subscribers past
    // maxDirectSubs. The parent in the payload tells us who promoted us,
    // but from the new subscriber's perspective WE are its parent.
    await this._addOrRecruitChild(topicId, role, newSubscriberId, meta.fromId);
  }

  /**
   * DIRECT handler — an upstream relay is handing off a BATCH of
   * subscribers to us as a new sub-axon (NX-17+ batch-adoption path).
   * Payload: { topicId, subscriberIds }.
   *
   * We create a sub-axon role for this topic (no parentId — see
   * _addOrRecruitChild for the no-parent-tracking rationale), add every
   * subscriber in the batch as a child, then issue a routed subscribe of
   * our own toward topicId. That subscribe lands at whichever live axon
   * is currently on our path to the topic — most often the peer that
   * sent us the batch, but sometimes an intermediate live relay between
   * us and the root. Either way, we attach into the live tree and start
   * receiving publishes through the normal root→…→self cascade.
   */
  async _onAdoptSubscribers(payload, meta) {
    const { topicId, subscriberIds } = payload;
    if (!Array.isArray(subscriberIds) || subscriberIds.length === 0) return;
    const now = this._now();

    let role = this.axonRoles.get(topicId);
    if (!role) {
      role = {
        parentId:       null,              // deliberately NOT tracked
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
      if (subId === this.nodeId) continue;  // never add self as own child
      // Skip children we already have (idempotent adoption in case a
      // duplicate batch was delivered).
      if (role.children.has(subId)) {
        role.children.get(subId).lastRenewed = now;
        continue;
      }
      role.children.set(subId, { createdAt: now, lastRenewed: now, isSubaxon: false });
    }

    // Attach ourselves into the live tree. The routed subscribe falls
    // through to whichever live axon is on our path to topicId — may or
    // may not be the peer that just sent us the batch, per design.
    await this.dht.routeMessage(topicId, 'pubsub:subscribe',
                                { topicId, subscriberId: this.nodeId });
  }

  /**
   * DIRECT handler — our parent axon is dissolving and suggests we
   * re-attach via its own parent (or toward the hash).
   * Payload: { topicId, suggestedParent }.
   *
   * We immediately re-issue our own subscribe. The routed subscribe will
   * be intercepted by the first live axon on the path — which may be the
   * suggestedParent (the grandparent in the dissolved tree) or any
   * intermediate axon that still has capacity.
   *
   * Leaf subscribers receive this too — they just re-issue via their
   * mySubscriptions entry.
   */
  async _onDissolveHint(payload, meta) {
    const { topicId } = payload;

    // Case 1: we are a direct subscriber (the hint lands here because the
    // dissolving axon had us as a leaf child). Re-route the subscribe via
    // our own pubsubSubscribe path if we still want this topic.
    if (this.mySubscriptions.has(topicId)) {
      await this.dht.routeMessage(topicId, 'pubsub:subscribe',
                                  { topicId, subscriberId: this.nodeId });
    }

    // Case 2: we are a sub-axon whose parent dissolved. Clear our parent
    // pointer and re-issue our upward subscribe so we attach to whoever
    // is now on the path to the hash.
    const role = this.axonRoles.get(topicId);
    if (role && !role.isRoot) {
      role.parentId = null;
      if (role.children.size > 0) {
        await this.dht.routeMessage(topicId, 'pubsub:subscribe',
                                    { topicId, subscriberId: this.nodeId });
      }
    }
  }

  // ── K-closest direct handlers ───────────────────────────────────────
  //
  // In K-closest mode subscribers STORE at every node in the K-closest
  // root set via sendDirect. Each such node treats itself as "in the
  // root set" for the topic and independently runs axonal recruitment
  // below itself. These handlers mirror the routed versions but skip the
  // terminal-election check (caller already decided we're a root).

  async _onSubscribeDirect(payload, meta) {
    const { topicId, subscriberId, peerRoots, lastSeenTs } = payload;
    const now = this._now();

    let role = this.axonRoles.get(topicId);
    if (!role) {
      role = {
        parentId:       null,
        isRoot:         true,
        isInRootSet:    true,
        peerRoots:      new Set((peerRoots || []).filter(id => id !== this.nodeId)),
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      0,
        lowWaterSince:  0,
      };
      this.axonRoles.set(topicId, role);
    } else {
      // Refresh our view of peer roots if the subscriber sent us a fresher
      // K-set (e.g., after churn drifted the K-closest).
      role.isInRootSet = true;
      if (peerRoots) {
        for (const p of peerRoots) if (p !== this.nodeId) role.peerRoots?.add(p);
      }
    }

    await this._addOrRecruitChild(topicId, role, subscriberId, subscriberId);
    await this._maybeSendReplay(topicId, role, subscriberId, lastSeenTs);
  }

  /**
   * Replay any cached publishes newer than `lastSeenTs` to a subscriber
   * that just subscribed (or re-subscribed after refresh). Missing
   * lastSeenTs means the subscriber is brand-new → replay entire cache.
   *
   * All missed messages are combined into a single `pubsub:replay-batch`
   * direct send rather than N individual delivers: at a fan-out like our
   * tests (1 publish/sec × 100-entry cache), that's the difference
   * between 1 and 100 sendDirect calls per re-subscribe.
   */
  async _maybeSendReplay(topicId, role, subscriberId, lastSeenTs) {
    if (!subscriberId || subscriberId === this.nodeId) return;
    const cache = role.replayCache;
    if (!cache || cache.length === 0) return;
    const missed = (lastSeenTs != null && lastSeenTs > 0)
      ? cache.filter(m => m.publishTs > lastSeenTs)
      : cache.slice();
    if (missed.length === 0) return;
    await this.dht.sendDirect(subscriberId, 'pubsub:replay-batch', {
      topicId,
      messages: missed,
    });
  }

  /**
   * DIRECT handler — an axon is replaying messages we missed since our
   * most recent `lastSeenTs` for this topic. Iterate, dedup by
   * publishId, and fire the local delivery callback for each.
   */
  _onReplayBatch(payload, meta) {
    const { topicId, messages } = payload;
    if (!Array.isArray(messages) || messages.length === 0) return;
    for (const msg of messages) {
      const { json, publishId, publishTs } = msg;
      if (this._alreadySeenPublish(publishId)) continue;
      this._recordReceived(topicId, publishId, publishTs);
      // If we hold a role for this topic, cache the replay entries too so
      // we can forward them onward the next time a child re-subscribes
      // with a stale lastSeenTs. Without this, a subscriber that just
      // became a sub-axon (adopted) would have an empty cache and
      // couldn't help its own children catch up.
      const role = this.axonRoles.get(topicId);
      if (role) this._addToReplayCache(role, { json, publishId, publishTs });
      if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
    }
  }

  _onUnsubscribeDirect(payload, meta) {
    const { topicId, subscriberId } = payload;
    const role = this.axonRoles.get(topicId);
    if (role && role.children.has(subscriberId)) {
      role.children.delete(subscriberId);
      if (role.children.size === 0) role.emptiedAt = this._now();
    }
  }

  async _onPublishDirect(payload, meta) {
    const { topicId, json, publishId, publishTs, postHash, publisher } = payload;
    if (this._alreadySeenPublish(publishId)) return;
    let role = this.axonRoles.get(topicId);
    if (!role) {
      // axona-net patch (peer 0.15.0):
      // Lazy-axon promotion.  We're one of the K-closest to this topic
      // (the publisher's findKClosest landed here) but no subscriber
      // has registered with us yet — under the upstream protocol we'd
      // drop the message via a routed-walk that would land on another
      // empty-role node and give up.  Instead, promote ourselves to a
      // (childless) root role and add the message to the replay cache.
      // When a subscriber arrives later, _maybeSendReplay serves the
      // cache.  Empty-role rootGraceMs sweep GCs us if no children
      // ever arrive (60s default) so this stays bounded.
      const now = this._now();
      role = {
        parentId:       null,
        isRoot:         true,
        isInRootSet:    true,
        peerRoots:      new Set(),
        children:       new Map(),
        parentLastSent: 0,
        roleCreatedAt:  now,
        emptiedAt:      now,    // immediately "empty" — rootGraceMs sweep applies
        lowWaterSince:  0,
        replayCache:    [],
      };
      this.axonRoles.set(topicId, role);
    }

    this._addToReplayCache(role, { json, publishId, publishTs, postHash, publisher });
    this._recordReceived(topicId, publishId, publishTs);

    const deadChildren = [];
    for (const [childId] of role.children) {
      if (childId === this.nodeId) {
        if (this._deliveryCallback) this._deliveryCallback(topicId, json, publishId, publishTs);
        if (postHash) this._bumpDelivery(topicId, postHash);
        continue;
      }
      const ok = await this.dht.sendDirect(childId, 'pubsub:deliver',
        { topicId, json, publishId, publishTs, postHash, publisher });
      if (!ok) {
        deadChildren.push(childId);
      } else if (postHash) {
        this._bumpDelivery(topicId, postHash);
      }
    }
    for (const dead of deadChildren) role.children.delete(dead);
  }

  // ── Refresh and TTL sweep ───────────────────────────────────────────

  /**
   * One tick of the maintenance loop:
   *   1. Renew each of our own subscriptions (leaf refresh).
   *   2. Renew our axon memberships upward to parents (axon refresh).
   *      Phase 3a: single-axon-per-topic, so isRoot is the only branch.
   *   3. Sweep expired children from each axonRole.
   *   4. GC empty non-root roles. GC empty roots past rootGraceMs.
   *
   * Exposed publicly so tests can drive the state machine deterministically.
   */
  async refreshTick() {
    const now = this._now();

    // ── v0.70.16 ordering ────────────────────────────────────────────
    // Sync bookkeeping (TTL sweep, hysteresis-dissolve marking, role
    // GC) runs FIRST so callers that don't await — including
    // test_axon.js's `root.refreshTick(); assert(...)` pattern — see
    // a fully swept axonRoles map immediately on return-from-microtask.
    // After bookkeeping, leaf + axon refresh subscribes are issued via
    // fire-and-forget Promise chains so the public refreshTick can
    // still be awaited but doesn't block the test's tight timing
    // assumption.

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

    // 2. §5.8 hysteresis dissolve — a non-root axon whose child count has
    //    been below minDirectSubs for one full refresh interval sends a
    //    dissolve-hint to its children and removes its own role. The
    //    parent's next TTL sweep will drop this (now-silent) axon.
    //    Root never dissolves via hysteresis — it's bound to the hash.
    const dissolveHints = [];
    for (const [topicId, role] of this.axonRoles) {
      if (role.isRoot) continue;
      if (role.children.size === 0) continue;  // handled in step 3
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
        // Non-root empty: dissolve immediately (TTL-driven collapse — no
        // outbound message needed; parent's next sweep will drop us).
        this.axonRoles.delete(topicId);
        continue;
      }
      // Root empty: respect rootGrace.
      if (role.emptiedAt > 0 && now - role.emptiedAt > this.rootGraceMs) {
        this.axonRoles.delete(topicId);
      }
    }

    // 4. Fire dissolve-hints from the queue collected in step 2.
    //    Fire-and-forget; the dissolved role has already been removed
    //    from axonRoles, so timing of the actual hint-delivery doesn't
    //    affect future bookkeeping.
    for (const hint of dissolveHints) {
      const p = this.dht.sendDirect(hint.childId, 'pubsub:dissolve-hint', {
        topicId: hint.topicId, suggestedParent: hint.suggestedParent,
      });
      if (p && typeof p.catch === 'function') p.catch(() => {});
    }

    // 5. Leaf refresh — re-issue each of our own subscribes via the same
    //    transport mode used at original subscribe time (K-closest, NX-17
    //    multi-root, or routed). Refresh must mirror the subscribe path
    //    or we'd drift between root sets across rounds.  Fired async so
    //    the bookkeeping above is visible immediately.
    const altK = (this.crossFragmentRoots ?? 0) + 1;
    const issueSubscribe = async (topicId, lastSeenTs, role) => {
      try {
        if (this._useKClosestMode()) {
          const roots = await this._findKClosest(topicId, this.rootSetSize);
          for (const peerId of roots) {
            const p = this.dht.sendDirect(peerId, 'pubsub:subscribe-k', {
              topicId, subscriberId: this.nodeId, peerRoots: roots, lastSeenTs,
            });
            if (p?.catch) p.catch(() => {});
          }
        } else if (altK > 1 && typeof this.dht.findKClosest === 'function') {
          const targets = await this._findKClosest(topicId, altK);
          for (const target of targets) {
            if (target === this.nodeId) continue;
            const p = this.dht.sendDirect(target, 'pubsub:subscribe-k', {
              topicId, subscriberId: this.nodeId, peerRoots: targets, lastSeenTs,
            });
            if (p?.catch) p.catch(() => {});
          }
        } else {
          const p = this.dht.routeMessage(topicId, 'pubsub:subscribe',
                                          { topicId, subscriberId: this.nodeId, lastSeenTs });
          if (p?.catch) p.catch(() => {});
        }
        if (role) role.parentLastSent = now;
      } catch (err) {
        console.error('AxonManager refresh: subscribe issue failed:', err);
      }
    };

    for (const topicId of this.mySubscriptions.keys()) {
      const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
      issueSubscribe(topicId, lastSeenTs, null);    // fire-and-forget
    }
    for (const [topicId, role] of this.axonRoles) {
      if (role.children.size === 0) continue;
      const lastSeenTs = this._lastSeenTsByTopic.get(topicId);
      issueSubscribe(topicId, lastSeenTs, role);    // fire-and-forget
    }
  }

  // ── Post-level metrics ──────────────────────────────────────────────
  //
  // Counters are per (topicId, postHash) and held at every node that
  // performs a fan-out for that topic. Each event is counted by exactly
  // one relay (the one that actually forwarded the post to a direct
  // subscriber, fulfilled the Pull, or saw the reshare), so aggregating
  // is plain addition across responding relays.
  //
  // Held in `this._counters: Map<topicId, Map<postHash, RelayCounters>>`.

  /** Return (and lazily create) a RelayCounters record for this hash. */
  _counterFor(topicId, postHash) {
    let byTopic = this._counters.get(topicId);
    if (!byTopic) { byTopic = new Map(); this._counters.set(topicId, byTopic); }
    let c = byTopic.get(postHash);
    if (!c) {
      c = {
        post_hash:        postHash,
        topic_id:         topicId,
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

  /** Walk this role's replay cache (newest-first) looking for an entry
   *  matching `postHash`. Returns the cache entry or null. */
  _findInReplayCache(role, postHash) {
    const cache = role?.replayCache;
    if (!cache) return null;
    for (let i = cache.length - 1; i >= 0; i--) {
      if (cache[i].postHash === postHash) return cache[i];
    }
    return null;
  }

  /**
   * Common access-control + response shape for both metricsReq and
   * metricsBroadcast handlers. Returns true if we sent a response.
   */
  _maybeRespondMetrics(payload, role) {
    const { topicId, postHashes, requesterId, requestId } = payload;
    // §4.7 self-authenticating ownership check. Drop silently if the
    // requester isn't the topic's publisher — also avoids leaking
    // topic existence.
    const samplePost = role.replayCache?.[0];
    if (samplePost && samplePost.publisher && samplePost.publisher !== requesterId) {
      return false;
    }
    const byTopic = this._counters.get(topicId) || new Map();
    const wantedHashes = (postHashes && postHashes.length > 0)
      ? postHashes
      : [...byTopic.keys()];
    const entries = [];
    for (const h of wantedHashes) {
      const c = byTopic.get(h);
      if (c) entries.push({ ...c });
    }
    this.dht.sendDirect(requesterId, 'pubsub:metricsResp', {
      requestId,
      responderId: this.nodeId,
      entries,
      timestamp: this._now(),
    });
    return true;
  }

  /** Per-request dedup so a tree-broadcast hitting a node twice via
   *  different paths only triggers one response + one re-fan. */
  _markMetricsReqSeen(requestId) {
    if (this._seenMetricsReqs.has(requestId)) return true;
    this._seenMetricsReqs.add(requestId);
    if (this._seenMetricsReqs.size > this._seenMetricsReqCap) {
      // Drop the oldest half by re-iterating insertion order.
      const drop = this._seenMetricsReqCap / 2;
      let i = 0;
      for (const k of this._seenMetricsReqs) {
        if (i++ >= drop) break;
        this._seenMetricsReqs.delete(k);
      }
    }
    return false;
  }

  /**
   * ROUTED handler — the publisher's metricsReq lands at the closest
   * role-holder on the topic tree (typically the root). That node:
   *   1. Responds with its own counters.
   *   2. Broadcasts the request to its children via direct messages
   *      so sub-axons also report. Recursive: each sub-axon does the
   *      same. A request that doesn't hit any role-holder along the
   *      routed walk eventually drops at the terminal node — that's
   *      fine, it just means there are no relays for this topic.
   */
  async _onMetricsReq(payload, meta) {
    const { topicId, requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';
    if (this._markMetricsReqSeen(requestId)) return 'consumed';

    if (!this._maybeRespondMetrics(payload, role)) return 'consumed';

    // Fan out to all children. Leaf subscribers will silently ignore
    // (their _onMetricsBroadcast sees no role and returns early).
    // Sub-axons will respond + recurse.
    for (const [childId] of role.children) {
      if (childId === this.nodeId) continue;
      this.dht.sendDirect(childId, 'pubsub:metricsBroadcast', payload);
    }
    return 'consumed';
  }

  /**
   * DIRECT handler — tree-broadcast version of metricsReq. Reached
   * via the root's fan-out to its sub-axon children (and recursively
   * thereafter). Only role-bearing nodes act; leaves drop silently.
   */
  _onMetricsBroadcast(payload, meta) {
    const { topicId, requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return;
    if (this._markMetricsReqSeen(requestId)) return;

    if (!this._maybeRespondMetrics(payload, role)) return;

    // Re-fan to our own children (sub-axons of sub-axons, etc.). Skip
    // the upstream peer that sent us this so we don't bounce it back.
    for (const [childId] of role.children) {
      if (childId === this.nodeId) continue;
      if (childId === meta.fromId) continue;
      this.dht.sendDirect(childId, 'pubsub:metricsBroadcast', payload);
    }
  }

  /** Collect a metrics response into the pending request's accumulator. */
  _onMetricsResp(payload, meta) {
    const { requestId, responderId, entries } = payload;
    const pending = this._pendingMetricsReqs.get(requestId);
    if (!pending) return;  // late or unsolicited; drop
    pending.accumulated.push({ responderId, entries });
  }

  // ── Pull (on-demand fetch by post_hash) ─────────────────────────────

  /**
   * ROUTED handler — a Pull traveling through the relay tree toward
   * the post's topic_id. Any role-bearing node whose replay cache has
   * the post fulfills the request immediately and bumps pull_count.
   * Non-cache-holding role nodes can either NOT_FOUND-out (if they
   * are the root, the last hop) or forward (if they are a sub-axon
   * and the routed walk should continue upstream toward the root).
   *
   * Only the FULFILLING relay increments pull_count — forwarders do
   * not. This implements the single-count invariant for Pull metrics
   * (§4.5).
   */
  async _onPullReq(payload, meta) {
    const { topicId, postHash, requesterId, requestId } = payload;
    const role = this.axonRoles.get(topicId);
    if (!role) return 'forward';

    const cached = this._findInReplayCache(role, postHash);
    if (cached) {
      this.dht.sendDirect(requesterId, 'pubsub:pullResp', {
        requestId,
        postHash,
        status:      'FOUND',
        post:        cached.json,
        responderId: this.nodeId,
      });
      this._bumpPull(topicId, postHash);
      return 'consumed';
    }
    if (role.isRoot) {
      // Root is the topic's authoritative endpoint. If we don't have
      // it, no one in the tree does.
      this.dht.sendDirect(requesterId, 'pubsub:pullResp', {
        requestId,
        postHash,
        status:      'NOT_FOUND',
        responderId: this.nodeId,
      });
      return 'consumed';
    }
    // Sub-axon without a cache hit: let the routed walk continue
    // upstream toward the root, which has the canonical cache.
    return 'forward';
  }

  /** Resolve the pending pull() promise on first response. */
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
   * Publisher-/application-side: issue a Pull for a specific post.
   * Returns the parsed SignedPost or null (NOT_FOUND / timeout / parse
   * error). Best-effort: callers are expected to render `null` as
   * "post unavailable" rather than retry aggressively (§6.1).
   */
  requestPull(topicId, postHash, { timeoutMs = 1000 } = {}) {
    const requestId = `${this.nodeId}:pl${++this._pullCounter}`;
    return new Promise(resolve => {
      this._pendingPullReqs.set(requestId, { resolve });
      this.dht.routeMessage(topicId, 'pubsub:pullReq', {
        topicId, postHash, requesterId: this.nodeId, requestId,
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

  /**
   * ROUTED handler — a separate notification routed by the resharer's
   * AxonManager toward the referenced post's topic_id. The first
   * role-bearing node consumes it and bumps reshare_count. Forwarders
   * don't bump (single-count invariant); leaves can't bump because
   * they don't hold counter state for the topic.
   */
  _onReshareNotify(payload, meta) {
    const { refTopicId, refPostHash } = payload;
    const role = this.axonRoles.get(refTopicId);
    if (!role) return 'forward';
    this._bumpReshare(refTopicId, refPostHash);
    return 'consumed';
  }

  /**
   * Publisher-side: issue a MetricsRequest into a topic's relay tree
   * and collect responses for `timeoutMs`. Returns the raw per-relay
   * entries; aggregation (sum across relays) happens at the application
   * layer in AxonPubSub.
   *
   * @param {string|number} topicId
   * @param {string[]} [postHashes]  Empty/omitted = all posts the
   *                                 relay holds counters for.
   * @param {Object}   [opts]
   * @param {number}   [opts.timeoutMs=500]
   * @returns {Promise<Array<{responderId:string, entries:RelayCounters[]}>>}
   */
  requestMetrics(topicId, postHashes, { timeoutMs = 500 } = {}) {
    const requestId = `${this.nodeId}:m${++this._metricsCounter}`;
    return new Promise(resolve => {
      const pending = { accumulated: [] };
      this._pendingMetricsReqs.set(requestId, pending);
      this.dht.routeMessage(topicId, 'pubsub:metricsReq', {
        topicId,
        postHashes: postHashes ?? null,
        requesterId: this.nodeId,
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
   * wire. Useful for in-process unit tests and for the simulator's
   * cross-node oracle when ground-truth verification is needed.
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

  /** Snapshot for tests: list of (topicId, role) with serializable children. */
  inspectRoles() {
    const out = [];
    for (const [topicId, role] of this.axonRoles) {
      out.push({
        topicId,
        isRoot:         role.isRoot,
        parentId:       role.parentId,
        roleCreatedAt:  role.roleCreatedAt,
        emptiedAt:      role.emptiedAt,
        children: [...role.children.entries()].map(([id, e]) => ({
          id, createdAt: e.createdAt, lastRenewed: e.lastRenewed,
        })),
      });
    }
    return out;
  }

  /** Clean shutdown — stop the timer. Does not send unsubscribes; use
   *  pubsubUnsubscribe explicitly for a graceful departure. */
  destroy() {
    this.stop();
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._deliveryCallback = null;
  }
}
