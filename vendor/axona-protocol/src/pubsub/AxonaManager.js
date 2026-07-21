// =====================================================================
// AxonaManager.js — Axona pub/sub: the routing-only axonic tree.
//
// Design: axona-docs/architecture/Pubsub-Axon-Tree-v0.1.md
//
// CLEAN BREAK (kernel v3.15.0). Routing-only pub/sub. The one rule:
//
//     Axona pub/sub uses ONLY DHT message routing. There are no direct
//     connections. Every interaction is a routed message delivered, hop by
//     hop, to the single live node closest to a 264-bit target.
//
// A message published to a topic is ROUTED toward the topic id; the closest
// live node is the (emergent, never-elected) ROOT. The root assigns a single
// monotonic timestamp — the serialization point that gives the topic a total
// order — caches it, and fans it out to its subscribers by routing a deliver
// to each. Subscribers renew toward the topic id every minute; that renewal
// is at once the keepalive, the failure detector, the self-heal, and (with a
// `since` hint) the gap-recovery. A subscriber carries an ordered `via`
// waypoint list (its `upstream`) so it is pinned to its relay yet always falls
// back to the topic id if that waypoint is gone.
//
// PHASE 2 — THE TREE. When a relay exceeds MAX_DIRECT subscribers it delegates:
// it promotes one of its subscribers to a child relay and hands it a batch of
// the others. A child relay subscribes UP toward the topic id (pinned by its
// parent via), caches the feed, and re-fans each message DOWN to its own
// subscribers exactly once. Delegated subscribers receive their deliveries
// from the child, so they repin to it and renew toward it — the tree is stable
// — but a dead waypoint always falls through to the topic id and re-seats, so
// the tree is self-healing and re-roots if the root itself dies.
//
// (Implementation choice: a relay promotes one of its own SUBSCRIBERS — a
// known-alive participant it can already route to — as the child. The design's
// "one of its connections" is satisfied without the manager needing a
// synaptome/neighbour list; routing reaches the chosen node regardless.)
//
// PHASE 3 — DURABILITY. A SUBSCRIBE advertises the sender's cache high-water; a
// relay/root that is BEHIND a reattaching subscriber pulls its stamped history
// UP (PULLUP → REPLAYUP) and adopts it without re-stamping, advancing lastTs so
// new publishes continue monotonically above it. This carries the topic's recent
// history across abrupt root death (a fresh empty root recovers it from any
// surviving cache-bearing relay) and across graceful migration.
//
// The side functions (kill/unpub/touch/pull/metrics/host) remain thin —
// markers TODO(Phase 4). GONE for good: sendDirect, findKClosest, K-closest
// fan-out, root sets, the old recruit/adopt/promote/dissolve + msgsync/kill-sync.
// =====================================================================

import { extractS2Prefix }   from '../utils/hexid.js';
import { RootClaim, roleNature } from './rootClaim.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { isRegionLockEnforced as _regionLock,
         T, RENEW_MS, RENEW_FAST_MS, DROP_MS, ROOT_REPLICAS, CACHE_MAX,
         CACHE_BYTES, MAX_DIRECT, MAX_VIA, VIA_HOP_BUDGET, BEACON_MS,
         BEACON_FANOUT, BEACON_LAYERS, PENDING_PUB_TTL_MS, COLD_BURST_TRIES,
         COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES,
         COLD_BURST_SLOW_INTERVAL_MS, COLD_PEER_THRESHOLD,
         FIRST_PUBLISH_RESEND_MS, METRICS_LEASE_MS, METRICS_PUB_MS,
         METRICS_COALESCE_MS } from './constants.js';
import { topicStoreMethods }   from './topicStore.js';
import { rootElectionMethods } from './rootElection.js';
import { repairPlaneMethods }  from './repairPlane.js';
import { wireHandlersMethods }  from './wireHandlers.js';
import { syncEngineMethods }    from './syncEngine.js';

// Constants, wire types, and the region-lock switch live in constants.js
// (refactor Phase 2); the caps and region-lock functions are re-exported here
// unchanged — AxonaPeer, std/chunk, and src/index.js import them from this
// module as the stable surface.
export { MAX_PUBLISH_BYTES, MAX_RELIABLE_PUBLISH_BYTES,
         configureRegionLock, isRegionLockEnforced } from './constants.js';



// Role shape (makeRole) lives in rootClaim.js — the role's `isRoot` field is
// the state-machine's state, and EVERY flip of it goes through RootClaim.

export class AxonaManager {
  /**
   * @param {object} o
   * @param {object} o.dht  adapter: { getSelfId(), routeMessage(target,type,payload,opts?),
   *                         onRoutedMessage(type, handler) }. sendDirect/findKClosest unused.
   */
  constructor({
    dht,
    now = () => Date.now(),
    emitLog = null,
    renewMs = RENEW_MS,            // adaptive renewal CEILING (stable state)
    renewFastMs = RENEW_FAST_MS,   // adaptive renewal FLOOR (post-subscribe / post-re-home)
    dropMs = DROP_MS,
    refreshIntervalMs = 5_000,     // tick — must be ≤ renewFastMs so fast renewal can fire
    replayCacheSize = CACHE_MAX,
    replayCacheBytes = CACHE_BYTES,
    maxDirect = MAX_DIRECT,
    beaconFanout = BEACON_FANOUT,  // K XOR-closest neighbors per beacon layer (root-announce reach)
    beaconLayers = BEACON_LAYERS,  // recursive forward depth (reach ≈ K + K² + … + K^layers)
    rootReplicas = ROOT_REPLICAS,  // singleton-root durability: # of nearest backup roots holding the full cache
    ..._legacy   // accepted-and-ignored clean-break tunables (pickRelayPeer, rootSetSize, …)
  } = {}) {
    if (!dht || typeof dht.routeMessage !== 'function' || typeof dht.getSelfId !== 'function'
        || typeof dht.onRoutedMessage !== 'function') {
      throw new TypeError('AxonaManager: dht with routeMessage + getSelfId + onRoutedMessage required');
    }
    this.dht    = dht;
    this.nodeId = dht.getSelfId();          // bigint, 264-bit
    this._now   = now;
    this._logSink = (typeof emitLog === 'function') ? emitLog : null;

    this.renewMs     = renewMs;          // adaptive ceiling
    this.renewFastMs = renewFastMs;      // adaptive floor
    this.dropMs    = dropMs;
    this.maxDirect = maxDirect || MAX_DIRECT;
    this.refreshIntervalMs = refreshIntervalMs;
    this._cacheMax   = replayCacheSize || CACHE_MAX;
    this._cacheBytes = replayCacheBytes || CACHE_BYTES;
    this._rootReplicas = Number.isFinite(rootReplicas) ? Math.max(0, rootReplicas) : ROOT_REPLICAS;

    // Public/inspectable state (contract surface).
    this.axonRoles       = new Map();   // topicIdBig -> Role  (topics I host: root or relay)
    this.mySubscriptions = new Map();   // topicIdBig -> { since, lastRenewSent }
    this._hostedTopics   = new Set();   // topicIdBig hosted without app consumption
    this._backupTopics   = new Set();   // topicIdBig I hold a warm replica for → subscribe like a child relay (single-root election)
    this._lastSeenTsByTopic = new Map();// topicIdBig -> ts  (AxonaPeer seeds `since` here)

    // Internal.
    this._upstream        = new Map();  // topicIdBig -> [hex]  the relay we renew toward
    this._rootHint        = new Map();  // topicIdBig -> { via:hex|null, at }  cached iterative-lookup root
    this._unattachedSince = new Map();  // topicIdBig -> ts  first tick seen subscribed-but-unpinned (reachable-root fallback)
    this._rootBeacons     = new Map();  // topicIdBig -> { root:hex, at, exp }  inbound root advert (soft state)
    this._beaconSeen      = new Map();  // beaconId -> exp  (flood dedup)
    this._lastBeaconAt    = 0;
    this._beaconSeq       = 0;
    this._beaconFanout    = beaconFanout;   // tunable root-announce reach (see _emitRootBeacons)
    this._beaconLayers    = beaconLayers;
    this._appDelivered    = new Map();  // "topicHex:msgId" -> true (exactly-once LRU)
    this._deliveryCallback = null;
    this._hostKeyspace    = false;
    this._pending         = new Map();  // pull corrId -> { resolve, timer }
    this._pullSeq         = 0;
    this._timer           = null;
    this._burstTimers     = new Set();  // cold-publish burst + first-publish setTimeout handles (cleared on stop)
    this._publishedTopics = new Set();  // topics this node has published to (for the first-publish re-send)
    this.myMetricsRequests = new Map(); // dataTopicBig -> { lastSent }  topics THIS node wants metrics for (renewed like subscriptions)
    this._metricsWanted   = new Map();  // dataTopicBig -> exp   soft flag on a path node (short-circuit duplicate METRICSON)
    this._metricsFwdAt    = new Map();  // dataTopicBig -> ts    last upstream METRICSON forward (fan-in coalesce)
    this._metricsPublisher = null;      // (dataTopicIdHex, snapshot) => Promise  set by the peer; publishes to metricTopic(T)

    // The root-claim state machine: every isRoot transition + its guards
    // (claim / defer / demote / handoff decision table) live in rootClaim.js.
    this._rootClaim = new RootClaim(this, { beaconMs: BEACON_MS });

    this._registerHandlers();
  }

  // ── XOR-distance helper (264-bit ids as bigints) ────────────────────────
  _cmpXor(a, b, target) { const da = a ^ target, db = b ^ target; return da < db ? -1 : da > db ? 1 : 0; }

  // ── routing core ────────────────────────────────────────────────────
  // Route toward via[0] if present, else toward the topic id. The topic id is
  // authoritative; a dead waypoint is popped and routing continues. Never
  // orphaned by a stale via.
  _send(type, payload) {
    const via = Array.isArray(payload.via) ? payload.via : [];
    const target = via.length ? idBig(via[0]) : idBig(payload.topicId);
    this.dht.routeMessage(target, type, payload, { fromId: idHex(this.nodeId), viaHopBudget: VIA_HOP_BUDGET });
  }
  _route(targetBig, type, payload) {
    this.dht.routeMessage(targetBig, type, payload, { fromId: idHex(this.nodeId), viaHopBudget: VIA_HOP_BUDGET });
  }
  _reroute(type, payload) {
    payload.via = (Array.isArray(payload.via) ? payload.via : []).slice(1);
    this._send(type, payload);
  }

  // True iff a topic (or any id) shares this node's region byte (S2 prefix). The
  // region byte is the high byte of every 264-bit id; only same-region nodes may
  // form a topic's axon-tree infrastructure (root + child relays).
  _sameRegion(idBigVal) {
    try { return extractS2Prefix(idBigVal) === extractS2Prefix(this.nodeId); }
    catch { return false; }
  }

  // The region GATE used by every enforcement site. When the region lock is off
  // (default, pre-critical-mass) this is always true → an out-of-region node may
  // root/relay/host any topic (nearest node wins, pre-4.13.0 behavior). When on,
  // it collapses to the strict same-region check.
  _regionOk(idBigVal) {
    return !_regionLock() || this._sameRegion(idBigVal);
  }

  // I am the root for a topic iff I am the routing terminus for its bare id.
  // A non-root relay that becomes the closest node (e.g. after the old root dies)
  // is promoted here — without this it would reroute bare-topic publishes to
  // itself forever. Rules + defer gate live in the state machine.
  _maybePromoteRoot(role, payload, meta) {
    this._rootClaim.promote(role, payload, meta);
  }

  // Strictly-closer live-root defer gate — the decision table in rootClaim.js.
  _liveCloserRoot(topicBig, opts) {
    return this._rootClaim.liveCloserRoot(topicBig, opts);
  }

  // Defer a stranded terminal message to the beaconed root: demote any spurious
  // root claim I hold (and re-home under the true root so my subtree keeps
  // receiving), then forward the payload via-pinned to it.
  _deferToRoot(topicBig, type, payload, rootHex) {
    this._rootClaim.demote(topicBig, rootHex, 'defer-terminal');
    this._send(type, { ...payload, via: [rootHex] });
  }

  _becomeRoot(topicBig, why = 'terminal') {
    return this._rootClaim.become(topicBig, why);
  }

  // ── introspection (consumed by AxonaPeer.health()) ───────────────────
  // These were dropped in the v3.12 clean break, which left health().axonRoles
  // permanently empty — every relay reported roles=0 while actually rooting
  // topics, which masked the prod root-split for a full diagnosis cycle.
  // Observability surfaces must fail loudly or exist; these exist again.
  inspectRoles() {
    const out = [];
    for (const r of this.axonRoles.values()) {
      out.push({
        topicId: idHex(r.topicId),
        isRoot: !!r.isRoot,
        nature: roleNature(r),                           // ROOT | BACKUP | CHILD (Phase 7; I-6)
        holder: this._hostedTopics.has(r.topicId) || this.mySubscriptions.has(r.topicId),
        children: [...r.children],
        subscribers: r.subscribers.size,
        replayCacheSize: r.cache.length,
      });
    }
    return out;
  }

  inspectHosting() {
    return {
      topics: [...this._hostedTopics].map((t) => idHex(t)),
      subscriptions: this.mySubscriptions.size,
      backups: this._backupTopics.size,
    };
  }

  // Subscribe — always sent SYNCHRONOUSLY and immediately (fast path, never blocked
  // on the network). Pinned (steady state) → via the relay. Unpinned → the warm
  // root hint if we have one, else greedy ([]) toward the bare topic id; the
  // background lookup in _rootHint_ heals a greedy strand shortly after.
  _sendSubscribe(topicBig) {
    const pinned = this._upstream.get(topicBig) || [];
    let via = pinned;
    if (!via.length) { const hint = this._rootHint_(topicBig); via = hint ? [hint] : []; }
    this._emitSubscribe(topicBig, via.slice(0, MAX_VIA));
  }
  _emitSubscribe(topicBig, via) {
    const role = this.axonRoles.get(topicBig);
    const sub  = this.mySubscriptions.get(topicBig);
    const latest = !!(sub && sub.replayLatest);   // since:'latest' — newest entry rides this DELIVER, regardless of age
    this._send(T.SUB, {
      topicId: idHex(topicBig), via, subscriberId: idHex(this.nodeId),
      since: this._sinceFor(topicBig),
      hw: role ? this._highWater(role) : 0,   // a cache-bearing relay advertises its history (§6)
      lw: role ? this._lowWater(role) : 0,    // …and its OLDEST stamp, so a root missing the pre-transition half pulls it
      latest,
    });
    // One-shot: 'latest' delivers the current value once at subscribe, not on
    // every renewal — clear the flag after this first emit.
    if (latest) sub.replayLatest = false;
  }

  // ── public API (contract surface) ────────────────────────────────────
  // Route the UN-stamped publish toward the topic's root; root stamps it. Sent
  // SYNCHRONOUSLY and immediately: via the warm true-root hint if we have one (so
  // publisher + subscribers converge on the same root), else greedy ([]) toward the
  // bare topic id. _rootHint_ refreshes the hint in the background — never blocking
  // the publish on a slow live-mesh lookup.
  pubsubPublish(topicId, json, meta = {}) {
    const hint = this._rootHint_(topicId);
    // Retain briefly so a publish that stranded on the greedy walk (hint not yet
    // warm) is re-sent toward the true root the moment the background lookup
    // resolves — a one-shot publish never re-routes on its own, so a cold-hint
    // strand = a lost message. Idempotent: the root dedups by msgId.
    if (!this._pendingPub) this._pendingPub = new Map();
    // Keyed by msgId (not topic) so two quick publishes to the SAME topic don't
    // overwrite each other's pending retry — each message is independently retried.
    let pmsgId = null; try { pmsgId = JSON.parse(json)?.msgId ?? null; } catch { /* opaque body */ }
    if (pmsgId) this._pendingPub.set(pmsgId, { topicBig: topicId, json, at: this._now(), tries: 0 });
    this._send(T.PUB, { topicId: idHex(topicId), via: hint ? [hint] : [], json });
    // Early re-sends — ONE plan, ONE pump (v4.25.0, Phase 6): a cold publisher
    // (not yet integrated) front-loads burst waves while its table warms; a WARM
    // first publish to a topic gets one quick re-send so a just-formed tree still
    // catches it; subsequent warm publishes go back to a single send. Same quench
    // as the tick retry: the pending entry vanishing on observation (I-9).
    if (pmsgId) {
      const gaps = this._earlyResendPlan(this._isColdPublisher(), !this._publishedTopics.has(topicId));
      if (gaps.length) this._earlyResendPump(topicId, pmsgId, gaps);
      this._publishedTopics.add(topicId);
    }
    return meta.postHash || '';
  }

  pubsubSubscribe(topicId, opts = {}) {
    const seeded = this._lastSeenTsByTopic.get(topicId);
    const since  = Number.isFinite(seeded) ? seeded : this._now();
    // since:'latest' → carry a replayLatest flag so the root replays its newest
    // cache entry regardless of age (the ts-floor can't express "newest"). Sticky
    // across renewals (re-delivery is deduped); cleared by a later non-latest sub.
    this.mySubscriptions.set(topicId, {
      since, lastRenewSent: this._now(), interval: this.renewFastMs,
      replayLatest: !!opts.replayLatest,
    });
    // If this node already HOLDS the topic's cache (it is the root, or a
    // cache-bearing relay), no wire replay can serve it: the outgoing SUB
    // carries since=high-water (§6 — a holder never re-pulls history it already
    // stores), and a root's own SUB self-loops without seating. Replay the local
    // cache straight to the app against the app-level floor — without this, a
    // since:'all' subscriber that happens to be the topic's root receives zero
    // of the history it is itself storing. Idempotent (exactly-once app dedup).
    const role = this.axonRoles.get(topicId);
    if (role) this._replayLocal(role, since, !!opts.replayLatest);
    this._sendSubscribe(topicId);
  }

  pubsubUnsubscribe(topicId) {
    this.mySubscriptions.delete(topicId);
    const via = this._upstream.get(topicId) || [];
    this._send(T.UNSUB, { topicId: idHex(topicId), via, subscriberId: idHex(this.nodeId) });
    this.pubsubResetTopicConsumption(topicId);
  }

  // ── Demand-driven metrics ────────────────────────────────────────────
  // The peer registers a publisher that turns (dataTopicIdHex, snapshot) into a
  // publish to metricTopic(dataTopicId). Kept out of the kernel so the kernel
  // never needs an author key — the snapshot is published like any other message.
  setMetricsPublisher(fn) { this._metricsPublisher = (typeof fn === 'function') ? fn : null; }

  // Request metrics for a DATA topic: start a renewable lease toward its root.
  // Idempotent; renewed on the refresh tick. (The peer calls this when the app
  // subscribes to metricTopic(dataTopicId).)
  pubsubMetricsOn(dataTopicBig) {
    if (!this.myMetricsRequests.has(dataTopicBig)) this.myMetricsRequests.set(dataTopicBig, { lastSent: 0 });
    this._sendMetricsOn(dataTopicBig);
    this.myMetricsRequests.get(dataTopicBig).lastSent = this._now();
  }
  // Stop wanting metrics for a topic (lease lapses at the root → it stops publishing).
  pubsubMetricsOff(dataTopicBig) { this.myMetricsRequests.delete(dataTopicBig); }

  // Route a METRICSON toward the data topic's root (lookup-assisted, like SUB).
  _sendMetricsOn(dataTopicBig) {
    const hint = this._rootHint_(dataTopicBig);
    this._send(T.METRICSON, { topicId: idHex(dataTopicBig), via: hint ? [hint] : [], requesterId: idHex(this.nodeId) });
  }

  // Publish one metric snapshot for a rooted topic, throttled to METRICS_PUB_MS
  // (callers may fire on any trigger — the tick, or a just-armed lease — and the
  // throttle keeps the net cadence). Advisory: never throws.
  _publishMetricSnapshot(topicBig, role, now) {
    if (!this._metricsPublisher || !role.isRoot) return;
    if (now - role.metricsLastPub < METRICS_PUB_MS) return;
    role.metricsLastPub = now;
    const snap = { v: 1, topic: idHex(topicBig), ts: now, by: idHex(this.nodeId),
      current_count: role.cache.length, seq: role.seq,
      subscribers: role.subscribers.size, bytes: role.cacheBytes };
    try { const p = this._metricsPublisher(idHex(topicBig), snap); if (p && typeof p.catch === 'function') p.catch(() => {}); }
    catch { /* advisory — never let a metrics publish break the caller */ }
  }

  pubsubResetTopicConsumption(topicId) {
    // "Consumed nothing" → seed the since-floor to 0 so a following subscribe
    // replays the FULL history (since:'all'). MUST NOT delete the entry: a
    // missing _lastSeenTsByTopic makes pubsubSubscribe fall back to since=now()
    // (live tail), which silently defeats since:'all' (the live backlog/gap
    // recover-0% bug — the root then filters out everything before now).
    this._lastSeenTsByTopic.set(topicId, 0);
    this._upstream.delete(topicId);
    const prefix = topicId.toString(16) + ':';
    for (const k of this._appDelivered.keys()) if (k.startsWith(prefix)) this._appDelivered.delete(k);
  }

  pubsubHost(topicId) {
    // REGION RULE backstop (when enforced): a node hosts/roots only topics in its region.
    if (!this._regionOk(topicId)) {
      this._log('warn', 'host-refused-foreign-region', { topic: idHex(topicId).slice(0, 12) });
      return;
    }
    this._hostedTopics.add(topicId);
    // Participate so the node won't be torn down and can root the topic if closest.
    // Route the announce through _sendSubscribe (lookup-assisted → the true root, and
    // advertises our high-water for §6 PULLUP) rather than a bare greedy via:[] — the
    // bare send stranded the initial host announce until the next refreshTick healed it
    // (the tick already renews hosts via _sendSubscribe). v4.10.1.
    this._sendSubscribe(topicId);
  }
  pubsubUnhost(topicId) {
    this._hostedTopics.delete(topicId);
    const role = this.axonRoles.get(topicId);
    if (role) { const me = lc(idHex(this.nodeId)); role.subscribers.delete(me); role.children.delete(me); }
  }
  pubsubHostKeyspace(on = true) { this._hostKeyspace = !!on; }

  // Route the kill (tombstone) toward the topic's root EXACTLY like a publish:
  // via the warm true-root hint if we have one, else greedy. A kill is a one-shot
  // routed message — without the hint it strands on the greedy walk just as a cold
  // publish does, and (unlike a renewed subscribe) never re-routes on its own, so a
  // stranded kill = a tombstone that never reaches subscribers (the ~30% "kill not
  // received" flake). Retain it briefly so the background lookup re-sends it toward
  // the true root once resolved. Idempotent — the root dedups the tombstone by msgId.
  pubsubKill(topicId, kill) {
    const hint = this._rootHint_(topicId);
    if (!this._pendingKill) this._pendingKill = new Map();
    if (kill?.msgId) this._pendingKill.set(kill.msgId, { topicBig: topicId, kill, at: this._now(), tries: 0 });
    this._send(T.KILL, { topicId: idHex(topicId), via: hint ? [hint] : [], kill });
  }
  // pubsubUnpub() — REMOVED v4.3.0 (decision 2026-06-25: keep kill, drop unpub)
  pubsubTouch(topicId, touch) { this._send(T.TOUCH, { topicId: idHex(topicId), via: [], touch }); }

  requestPull(topicId, postHash = null, { timeoutMs = 1000 } = {}) {
    const corrId = idHex(this.nodeId).slice(0, 8) + ':' + (++this._pullSeq);
    // Route toward the true root via the warm lookup-assist hint (like publish/kill),
    // not a bare greedy via:[] — a pull that strands on a local minimum reaches a
    // non-cohort node and returns null (a false "no message") even though the cohort
    // holds it. The hint seeds the walk at the topic-closest node it can serve. v4.10.1.
    const hint = this._rootHint_(topicId);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this._pending.delete(corrId); resolve(null); }, timeoutMs);
      if (typeof timer.unref === 'function') timer.unref();
      this._pending.set(corrId, { resolve, timer });
      this._send(T.PULL, { topicId: idHex(topicId), via: hint ? [hint] : [], corrId, postHash: postHash || null, requesterId: idHex(this.nodeId) });
    });
  }
  // Enumerate the topics THIS node currently roots, each with a locally-computed
  // metric snapshot. The producer side of the derived-metric-topic convention: an
  // infrastructure root walks this on a timer and republishes each to metricTopic(T).
  // (Re-added in v4.10.1 — the routing-only clean break (v3.12) dropped it, silently
  // killing all metrics on the 4.x line.) Under the v4.10.0 cohort model EVERY
  // co-hosting root publishes its own snapshot; `seq` and `current_count` converge
  // across the cohort (anti-entropy), while `subscribers` is this member's local
  // subset — the reader (peer.metrics) aggregates across the cohort.
  rootedTopics() {
    const out = [];
    const now = this._now();
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot) continue;
      // Recover the signed topic descriptor from the newest cached envelope (the role
      // holds only the topic id as a bigint). No cache → nothing to describe → skip.
      let descriptor = null;
      for (let i = role.cache.length - 1; i >= 0; i--) {
        try { const env = JSON.parse(role.cache[i].json); if (env && env.topic) { descriptor = env.topic; break; } } catch { /* */ }
      }
      out.push({
        topicId:       idHex(t),
        descriptor,                          // { region, owner, name, write } | null
        current_count: role.cache.length,    // messages currently in cache (swept of expired/killed)
        seq:           role.seq,             // message counter — dense per-topic high-water (monotonic)
        subscribers:   role.subscribers.size,// this cohort member's local subscriber subset
        bytes:         role.cacheBytes,      // live cached envelope bytes
      });
    }
    return out;
  }

  onPubsubDelivery(cb) { this._deliveryCallback = cb; }
  setLogSink(fn) { this._logSink = (typeof fn === 'function') ? fn : null; }

  resetState() {
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
    this._backupTopics.clear();
    this._lastSeenTsByTopic.clear();
    this._upstream.clear();
    this._rootHint.clear();
    this._pendingPub?.clear();
    this._lookupInflight?.clear();
    this._rootBeacons.clear();
    this._beaconSeen.clear();
    this._lastAnnounce?.clear();
    this._lastBeaconAt = 0;
    this._appDelivered.clear();
    for (const p of this._pending.values()) clearTimeout(p.timer);
    this._pending.clear();
  }

  _log(level, event, ctx) {
    if (this._logSink) { try { this._logSink(level, 'pubsub:' + event, ctx); } catch { /* sink threw */ } }
  }
}

// ── Phase 2 assembly ────────────────────────────────────────────────────
// The four concern modules contribute their methods to the prototype; `this`
// is the manager everywhere, and all state stays on the manager façade (the
// same pattern as rootClaim.js, which owns the isRoot transitions).
Object.assign(
  AxonaManager.prototype,
  topicStoreMethods,     // cache, tombstones, exactly-once app delivery
  rootElectionMethods,   // beacons, hints, self-verification, liveness
  repairPlaneMethods,    // the tick scheduler, retries, replication, departure
  wireHandlersMethods,   // routed handlers + axon-tree mechanics
  syncEngineMethods,     // Phase 8: the ONE repair/durability sync operation + policy table
);

export default AxonaManager;
