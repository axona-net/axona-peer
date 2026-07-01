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

import { verifyEnvelope, checkFreshness } from './envelope.js';
import { verifyKill }                     from './kill.js';
import { deriveTopicIdBig }               from './post.js';

// ── Inbound caps (D-1: bound attacker-controlled payloads) ──────────────
// Re-exported unchanged — AxonaPeer and std/chunk import these as the
// publish-size contract; independent of the pub/sub mechanism.
export const MAX_PUBLISH_BYTES = 256 * 1024;         // absolute hard ceiling (chars)
export const MAX_RELIABLE_PUBLISH_BYTES = 15 * 1024; // WebRTC-interop reliable floor (O-5)

// ── Tunable constants (design §Appendix) ────────────────────────────────
const RENEW_MS        = 60_000;          // re-subscribe cadence CEILING (stable state)
// Adaptive renewal (churn re-home fix): a subscriber re-homes only when it renews,
// so the renewal interval IS the orphan window after its relay/root churns. A flat
// 60s is too slow for mobile churn; a flat 5s is 12× the steady-state traffic. So
// renew FAST right after subscribing or after a relay change (re-pin), backing off
// ×1.5 toward the ceiling while stable. Sustained churn keeps re-pinning → stays
// fast (measured ~82% delivery @30%/round vs 43% at 60s); calm → backs off → cheap.
const RENEW_FAST_MS   = 5_000;           // adaptive floor: initial + post-re-home interval
const RENEW_BACKOFF   = 1.5;             // multiply the interval each stable renewal, up to RENEW_MS
const DROP_MS         = 180_000;         // evict a subscriber after missed renewals (≥ 3× ceiling)
// Reachable-root fallback (cold-convergence fix). An unpinned subscriber whose
// iterative root hint names a node CLOSER in XOR but that never adopts it back
// (unreachable on the greedy data path / "broken-but-authentic") would otherwise
// defer forever and the topic never roots → total strand. After this confirmation
// window with no upstream pin, if we are the closest node among our REACHABLE
// neighbours we claim root locally — preferring a reachable root over a
// closer-but-unconfirmed one. Long enough (≥2 ticks) that a genuinely reachable
// multi-hop closer root has time to adopt us first; a wrongly-claimed farther root
// is self-corrected by the strictly-closer beacon demotion (_onRootBeacon).
const ROOT_CLAIM_MS   = 6_000;           // unconfirmed-deferral window before self-claiming root
// Singleton-root durability: a root with no sub-axon tree (the small-topic common
// case) has no cache-holding backup, so an abrupt root churn loses all history for
// future since:'all' joiners. The root proactively replicates its full cache to its
// REPLICAS nearest reachable neighbours (the natural successors); on churn the
// now-closest backup already holds everything and the reachable-root fallback
// promotes it with no gap. Backups are re-evaluated each tick (closer newcomers
// recruited, farther ones retired). 0 disables.
const ROOT_REPLICAS   = 2;
const REPLICA_STALE_MS = 65_000;         // a backup whose root stopped replicating this long is presumed gone
// ≈1 renewFastMs cycle: long enough that a genuinely reachable multi-hop closer
// root adopts us first (deliver-`from` pin), short enough that a cold topic roots
// within a couple of seconds instead of stranding. (12s was measured too slow —
// convergence was bottlenecked on the window: conv-strand 8/10 @7s wait vs 5/5
// @20s; dropping the window lets the would-be root claim ~1 tick after subscribe.)
const CACHE_MAX       = 1024;            // messages cached per relay
const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
const MAX_DIRECT      = 20;              // direct subscribers before a relay delegates
const DELEGATE_BATCH  = 8;               // subscribers handed off when promoting a child
const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side, Phase 2+)
const TTL_MS          = 24 * 60 * 60 * 1000;   // 24h message hold, keyed on the ROOT timestamp
const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
const PENDING_PUB_TTL_MS = 30_000;       // retain a recent publish/kill this long so refreshTick can RE-SEND it toward the true root until the publisher observes its own msgId (implicit ack) — covers a strand on the greedy walk AND packet loss; idempotent (root dedups by msgId)
const PENDING_PUB_MAX_TRIES = 6;         // cap re-sends so a never-confirmed publish (e.g. a non-subscribed publisher under loss) can't re-send unboundedly; loss^7 is negligible even at 30%
// Cold-publish burst (v4.11.0): a publish from a freshly-joined, not-yet-integrated
// node is the worst case for the one-shot greedy PUB — its routing table lacks the
// keyspace breadth to route to the true root, so the message strands and (unlike a
// renewing subscribe) never re-homes. Waiting for the table to warm is harmful: it's
// OUTBOUND traffic that integrates a newcomer (reachability to a node lives in its
// NEIGHBOURS' tables, healed by directed sends — same lesson as churn-in warmup). So
// while cold, re-send the SAME signed envelope a few times over the first second:
// idempotent (root dedups by msgId), each send both integrates us further AND gets a
// fresh shot at the true root as tables converge. Naturally disabled once warm (the
// gate is low neighbour count), and the slow refreshTick retry still backstops after.
const COLD_BURST_TRIES     = 5;          // extra fast re-sends on a cold publish
const COLD_BURST_INTERVAL_MS = 200;      // spacing of the burst (≈1s total)
const COLD_PEER_THRESHOLD  = 8;          // "cold" = fewer than this many neighbours (not yet integrated)
const REPLAY_CHUNK_BYTES = 96 * 1024;    // byte budget per replay deliver batch
const FUTURE_TOLERANCE_MS = 5 * 60 * 1000;   // §5 bad-clock rule: drop replayed stamps this far ahead

// ── Root beacon (soft-state root advertisement; Pubsub-Root-Beacon-v0.1) ────
// A root periodically announces "root for T was last at X" to its K XOR-closest
// neighbors (the topic's convergence basin), recursive 2 layers. Receivers cache
// it (verify-don't-trust) and consult it before greedy/lookup, fixing the
// last-mile divergence (publisher/subscriber resolving different roots).
const BEACON_MS       = 20_000;          // emission cadence (faster than RENEW_MS so churn heals quickly)
const BEACON_TTL_MS   = 50_000;          // inbound pointer validity (~2.5×BEACON_MS)
const BEACON_FANOUT   = 6;               // K closest neighbors per layer (fan-out ≤ K+K²)
const BEACON_LAYERS   = 2;               // recursive forward depth
const BEACON_SEEN_MS  = 60_000;          // flood-dedup retention

// ── Wire message types (all ROUTED) ─────────────────────────────────────
const T = {
  SUB:      'pubsub:sub',       // subscribe — routed toward topic id (or a via waypoint)
  UNSUB:    'pubsub:unsub',     // explicit unsubscribe (renewal lapse also drops)
  PUB:      'pubsub:pub',       // publish — routed toward topic id; NO timestamp (root stamps)
  DELIVER:  'pubsub:deliver',   // stamped messages — routed toward a subscriber id
  ADOPT:    'pubsub:adopt',     // delegate: "become my child relay + take these subscribers"
  PULLUP:   'pubsub:pullup',    // "I'm behind you — replay your stamped history up to me" (§6)
  REPLAYUP: 'pubsub:replayup',  // a relay's stamped cache delta, routed UP to a behind parent
  HANDOFF:  'pubsub:handoff',   // graceful-leave: a departing root pushes its cache to its heir
  KILL:     'pubsub:kill',      // retract a message (thin; TODO Phase 4)
  UNPUB:    'pubsub:unpub',     // RESERVED — removed v4.3.0 (no handler/sender); wire string kept so legacy frames are ignored, not misrouted
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
  ROOTBEACON: 'pubsub:rootbeacon', // soft-state root advertisement to the topic's neighborhood
  REPLICATE: 'pubsub:replicate', // singleton-root durability: root pushes cache+tombstones to its N nearest neighbours (warm backup roots)
};

// ── id helpers (264-bit ids ⇄ 66-char hex) ──────────────────────────────
const idHex = (big) => big.toString(16).padStart(66, '0');
const idBig = (hex) => (typeof hex === 'bigint' ? hex : BigInt('0x' + String(hex)));
const lc    = (s) => String(s ?? '').toLowerCase();
const isHexId = (s) => /^[0-9a-f]{1,66}$/.test(s);

/** A relay's per-topic state (root or non-root child relay). */
function makeRole(topicId, isRoot) {
  return {
    topicId,                         // bigint
    isRoot,                          // closest-to-topic node → true; delegated child → false
    subscribers: new Map(),          // subHex -> { since, lastRenewed }
    children: new Set(),             // subHex of subscribers that are themselves child relays
    cache: [],                       // [{ msgId, publishTs, json, bytes }] asc by publishTs
    cacheIds: new Set(),             // msgId set for O(1) dedup (root-stamp + relay re-fan)
    cacheBytes: 0,
    lastTs: 0,                       // highest stamp emitted (monotonic; root authority)
    seq: 0,                          // DENSE per-topic counter (root authority): ++ per emitted
                                     // message AND kill, so subscribers detect GAPS (env.seq jumps
                                     // ⇒ a message was missed). Distinct from the time-floored
                                     // publishTs (which is monotonic but sparse). Recovered to the
                                     // max seen seq on replay-up/handoff so a new root continues densely.
    tombstones: new Map(),           // msgId -> { exp, killTs, signer, seq }  (kill; thin)
    replicas: new Map(),             // (when ROOT) backupHex -> { at }  nodes holding a warm copy of our cache
    backupOf: null,                  // (when BACKUP) hex of the root replicating to us; null if we're not a backup
    lastReplicaAt: 0,                // (when BACKUP) _now() of the last replica push from our root (staleness → presume root gone)
  };
}

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
    this._burstTimers     = new Set();  // cold-publish burst setTimeout handles (cleared on stop)

    this._registerHandlers();
  }

  _registerHandlers() {
    const on = (type, fn) => this.dht.onRoutedMessage(type, (p, m) => fn.call(this, p, m));
    on(T.SUB,      this._onSub);
    on(T.UNSUB,    this._onUnsub);
    on(T.PUB,      this._onPub);
    on(T.DELIVER,  this._onDeliver);
    on(T.ADOPT,    this._onAdopt);
    on(T.PULLUP,   this._onPullUp);
    on(T.REPLAYUP, this._onReplayUp);
    on(T.HANDOFF,  this._onHandoff);
    on(T.REPLICATE, this._onReplicate);
    on(T.KILL,     this._onKill);
    on(T.TOUCH,    this._onTouch);   // no-op (peer.touch deprecated v4.3.0); kept for wire compat
    on(T.PULL,     this._onPull);
    on(T.PULLRESP, this._onPullResp);
    on(T.ROOTBEACON, this._onRootBeacon);
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

  // Decide what a topic-targeted message (SUB/PUB) should do at this node.
  //   'handle'  — this is the node that should act on it (the via waypoint, or,
  //               for a bare-topic message, the routing terminus = the root)
  //   'reroute' — a via waypoint is gone / consumed → pop it and route on
  //   'forward' — keep routing (return falsy so the kernel forwards)
  //
  // Root-ness is decided by ROUTING, not by "do I host it": the node that hosts
  // a topic but is no longer the closest must NOT intercept bare-topic traffic.
  _topicDecision(payload, meta) {
    const via = Array.isArray(payload.via) ? payload.via : [];
    if (via.length) {
      if (idBig(via[0]) === this.nodeId) return this.axonRoles.has(idBig(payload.topicId)) ? 'handle' : 'reroute';
      return meta.isTerminal ? 'reroute' : 'forward';      // waypoint dead; I'm just closest to it
    }
    return meta.isTerminal ? 'handle' : 'forward';         // bare topic id → only the terminus handles
  }

  // I am the root for a topic iff I am the routing terminus for its bare id.
  // A non-root relay that becomes the closest node (e.g. after the old root dies)
  // is promoted here — without this it would reroute bare-topic publishes to
  // itself forever.
  _maybePromoteRoot(role, payload, meta) {
    const viaEmpty = !(Array.isArray(payload.via) && payload.via.length);
    if (viaEmpty && meta.isTerminal && !role.isRoot) { role.isRoot = true; this._upstream.delete(role.topicId); this._announceRoot(role.topicId); }
  }

  // ── SUBSCRIBE ────────────────────────────────────────────────────────
  _onSub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.SUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    this._maybePromoteRoot(role, payload, meta);

    const subHex = lc(payload.subscriberId);
    if (!isHexId(subHex)) return 'consumed';
    const since = Number.isFinite(payload.since) ? payload.since : 0;

    // The root's own renewal self-loops here. Don't seat self as a subscriber
    // (no self-fan); just replay locally if the app subscribes.
    if (idBig(subHex) === this.nodeId) {
      if (this.mySubscriptions.has(topicBig)) this._replayLocal(role, since, !!payload.latest);
      return 'consumed';
    }
    // Durability (§6 stamped-replay-up): if this subscriber holds newer stamped
    // history than I do — e.g. I am a fresh root after the old one died, or a
    // displaced root reattaching — ask it to replay its cache UP to me.
    const myHw = this._highWater(role);
    if (Number.isFinite(payload.hw) && payload.hw > myHw) {
      this._route(idBig(subHex), T.PULLUP, { topicId: idHex(topicBig), sinceHw: myHw, parentId: idHex(this.nodeId) });
    }
    this._accept(role, subHex, since, !!payload.latest);
    return 'consumed';
  }

  // My cache high-water = the newest stamp I hold (or have emitted, as root).
  _highWater(role) {
    return Math.max(role.lastTs || 0, role.cache.length ? role.cache[role.cache.length - 1].publishTs : 0);
  }

  _onUnsub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.UNSUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) { const s = lc(payload.subscriberId); role.subscribers.delete(s); role.children.delete(s); }
    return 'consumed';
  }

  // Seat a subscriber on a relay, delegating to a child when over capacity.
  // `latest` (since:'latest') folds the newest cache entry into the replay
  // DELIVER regardless of age — served from here (we hold the cache) even when
  // the subscriber is then delegated downward for ongoing fan-out.
  _accept(role, subHex, since, latest = false) {
    const existing = role.subscribers.get(subHex);
    if (existing) {                                   // renewal of a current subscriber
      existing.lastRenewed = this._now();
      this._replayTo(role, subHex, since, false, latest);
      return;
    }
    if (role.subscribers.size >= this.maxDirect) {    // overloaded → delegate
      // WIDEN before DEEPEN: promote a new sibling child (offloading a batch)
      // so the tree grows bushy (depth ~log_MAX_DIRECT(S)), not into a chain.
      // Only when every direct is already a child do we deepen — forward the
      // newcomer down to the child XOR-closest to it.
      if (!this._promoteChild(role)) {
        const c = this._pickChild(role, subHex);
        if (c) {
          if (latest) this._replayTo(role, subHex, since, false, true);  // current value from here, then hand off
          this._delegateTo(c, role, [{ subscriberId: subHex, since }]);
          return;
        }
        // neither possible (no leaf to promote, no child) → seat over capacity
      }
    }
    role.subscribers.set(subHex, { since: Number.isFinite(since) ? since : 0, lastRenewed: this._now() });
    this._replayTo(role, subHex, since, true, latest);  // delta (+ newest if latest) + a via-repin ping
  }

  // Choose the child relay XOR-closest to a subscriber (keyspace locality).
  _pickChild(role, subHex) {
    const target = idBig(subHex);
    let best = null, bestD = null;
    for (const c of role.children) {
      if (!role.subscribers.has(c)) { role.children.delete(c); continue; }  // stale
      const dd = idBig(c) ^ target;
      if (bestD === null || dd < bestD) { bestD = dd; best = c; }
    }
    return best;
  }

  // Promote one leaf subscriber to a child relay and hand it a batch of OTHER
  // leaves. Only succeeds if it can actually free a slot — promoting the sole
  // remaining leaf would just re-label it a child and free nothing, so we need
  // ≥2 leaves. Returning false tells _accept to deepen (delegate to a child)
  // instead of seating the newcomer over capacity.
  _promoteChild(role) {
    const leaves = [];
    for (const s of role.subscribers.keys()) if (!role.children.has(s)) leaves.push(s);
    if (leaves.length < 2) return false;
    const leaf = leaves[0];
    role.children.add(leaf);
    const batch = [];
    for (let i = 1; i < leaves.length && batch.length < DELEGATE_BATCH; i++) {
      batch.push({ subscriberId: leaves[i], since: role.subscribers.get(leaves[i]).since });
    }
    for (const b of batch) role.subscribers.delete(b.subscriberId);
    this._delegateTo(leaf, role, batch);
    this._log('info', 'delegated', { child: leaf.slice(0, 12), moved: batch.length });
    return true;
  }

  _delegateTo(childHex, role, subs) {
    this._route(idBig(childHex), T.ADOPT, {
      topicId: idHex(role.topicId), parent: idHex(this.nodeId), subs,
    });
  }

  // A node is told to become a child relay and adopt a set of subscribers.
  _onAdopt(payload, meta) {
    if (meta.targetId !== this.nodeId) return;        // routed to me specifically
    const topicBig = idBig(payload.topicId);
    let role = this.axonRoles.get(topicBig);
    if (!role) { role = makeRole(topicBig, false); this.axonRoles.set(topicBig, role);
                 this._log('info', 'relay-formed', { topic: idHex(topicBig).slice(0, 12) }); }
    role.isRoot = false;
    this._upstream.set(topicBig, [lc(payload.parent)]);
    for (const s of (Array.isArray(payload.subs) ? payload.subs : [])) {
      const sh = lc(s.subscriberId);
      if (isHexId(sh) && idBig(sh) !== this.nodeId) this._accept(role, sh, s.since);
    }
    // Attach UP toward the parent so we receive the live feed + cache replay.
    this._sendSubscribe(topicBig);
    return 'consumed';
  }

  // ── PUBLISH ──────────────────────────────────────────────────────────
  async _onPub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.PUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction. At this point I'm the acting target for
    // the publish (bare-topic terminus, or via-pinned to me). If a fresh beacon
    // names a different root genuinely CLOSER to the topic than me, forward to it
    // and demote any spurious root I'd wrongly claimed at this near-miss node so
    // I stop intercepting. The strictly-closer test is a second verify-don't-trust
    // gate (never defer to a farther node). Fires regardless of the incoming via:
    // a node that wrongly became root also emits poisoning "root=me" beacons, so a
    // peer can arrive here via-pinned to me — the correction must still re-home it.
    {
      const b = this._rootBeacons.get(topicBig);
      const meHex = lc(idHex(this.nodeId));
      if (b && this._now() < b.exp && b.root !== meHex && (idBig(b.root) ^ topicBig) < (this.nodeId ^ topicBig)) {
        const spurious = this.axonRoles.get(topicBig);
        if (spurious && spurious.isRoot) spurious.isRoot = false;       // demote: a closer root exists
        this._send(T.PUB, { topicId: payload.topicId, via: [b.root], json: payload.json });
        return 'consumed';
      }
    }
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    this._maybePromoteRoot(role, payload, meta);
    // Only the root (the topic terminus) stamps. A non-root relay can only reach
    // here for a via-routed publish (a security waypoint) — pop the via and
    // continue toward the topic id. Bare-topic publishes always promote above.
    if (!role.isRoot) { this._reroute(T.PUB, payload); return 'consumed'; }

    await this._ingestPublish(role, payload.json);
    return 'consumed';
  }

  // Root ingress: authenticate, enforce write policy, stamp, cache, fan out.
  async _ingestPublish(role, json) {
    let env;
    try { env = JSON.parse(json); } catch { this._log('warn', 'drop-unparseable'); return; }

    const v = await verifyEnvelope(env);                                 // B-4 sig + msgId
    if (!v.ok) { this._log('warn', 'drop-bad-envelope', { reason: v.reason }); return; }
    const fr = checkFreshness(env, { now: this._now() });                 // C-2 freshness (live ingress)
    if (!fr.ok) { this._log('warn', 'drop-stale', { reason: fr.reason }); return; }

    const desc = env.topic;
    let tid;
    try { tid = await deriveTopicIdBig({ region: desc.region, owner: desc.owner, name: desc.name, write: desc.write }); }
    catch { this._log('warn', 'drop-bad-descriptor'); return; }
    if (tid !== role.topicId) { this._log('warn', 'drop-topic-mismatch'); return; }
    if (desc.write === 'owner' && (!env.signerPubkey || lc(env.signerPubkey) !== lc(desc.owner))) {
      this._log('warn', 'drop-write-policy', { topic: desc.name }); return;
    }

    if (role.cacheIds.has(env.msgId)) return;                            // idempotent re-publish
    if (this._tombstoned(role, env.msgId, json)) return;                 // killed (or republish-after-kill) → suppress

    // STAMP — single serialization point; strictly monotonic, floored at now.
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;
    const seq = ++role.seq;                                              // dense per-topic counter (gap detection)
    const msg = { json, publishTs: ts, msgId: env.msgId, seq };
    this._cachePush(role, { msgId: env.msgId, publishTs: ts, json, seq });
    this._confirmPending(role.topicId, env.msgId);                       // our own publish landed (we're its root) → stop retrying
    this._fanout(role, msg, null);                                       // to subscribers
    this._deliverToApp(role.topicId, json, env.msgId, ts, seq);          // local app (if subscribed)
    // EAGER cohort distribution: push the freshly-stamped message to the K-closest the
    // instant it's stamped — a kill is just a publish with a side effect, so a publish
    // must reach the whole cohort exactly as a kill must, else a subscriber landing on a
    // co-hosting root misses it (the post-churn loss). Fire-and-forget; the periodic tick
    // reconciles any miss.
    if (role.isRoot) {
      const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
      this._replicateRole(role.topicId, role, bridge, this._now()).catch(() => {});
    }
  }

  // ── stamped-replay-up durability (§6) ────────────────────────────────
  // A behind parent asked us to replay our stamped history up to it; send the
  // cache delta newer than its high-water, routed to that parent.
  _onPullUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (!role || !role.cache.length) return 'consumed';
    const sinceHw = Number.isFinite(payload.sinceHw) ? payload.sinceHw : 0;
    const msgs = role.cache.filter(c => c.publishTs > sinceHw)
                           .map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq }));
    // Carry tombstones UP too — else a behind parent adopting this history would
    // resurrect a message we've already killed (kill-leak via cache migration).
    const dels = this._activeDels(role);
    if ((msgs.length || dels.length) && isHexId(lc(payload.parentId))) {
      this._route(idBig(payload.parentId), T.REPLAYUP, { topicId: idHex(role.topicId), msgs, dels });
    }
    return 'consumed';
  }

  // Stamped history arriving from below — adopt it WITHOUT re-stamping (the
  // timestamp rule, §5: a timestamp already present is kept), advance lastTs so
  // new publishes continue monotonically above it, and propagate it down.
  async _onReplayUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    const topicBig = idBig(payload.topicId);
    const role = this.axonRoles.get(topicBig);
    if (!role) return 'consumed';
    this._applyDels(role, topicBig, payload.dels);   // tombstones FIRST → suppress any killed body in this batch
    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      if (m && typeof m.json === 'string' && Number.isFinite(m.publishTs)) await this._ingestStamped(role, m);
    }
    return 'consumed';
  }

  async _ingestStamped(role, m) {
    let env;
    try { env = JSON.parse(m.json); } catch { return; }
    const v = await verifyEnvelope(env);                                 // B-4 still applies
    if (!v.ok || env.msgId !== m.msgId) { this._log('warn', 'drop-bad-replayup', { reason: v.reason }); return; }
    if (m.publishTs > this._now() + FUTURE_TOLERANCE_MS) { this._log('warn', 'drop-future-replayup'); return; } // §5 bad-clock
    if (role.cacheIds.has(m.msgId)) return;                              // already have it
    if (this._tombstoned(role, m.msgId, m.json)) return;                 // killed → don't resurrect via replay-up
    this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json, seq: m.seq });
    if (m.publishTs > role.lastTs) role.lastTs = m.publishTs;            // continue stamping above recovered history
    if (Number.isFinite(m.seq) && m.seq > role.seq) role.seq = m.seq;   // recover dense counter → a new root continues it
    this._fanout(role, { json: m.json, publishTs: m.publishTs, msgId: m.msgId, seq: m.seq }, null);
    this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs, m.seq);
  }

  // ── graceful-leave cache handoff ─────────────────────────────────────
  // A departing root pushes its cache to its heir (the next-closest live node)
  // BEFORE it leaves, so a topic's history survives the root's departure even
  // when no relay/host held a copy. The heir adopts the cache and becomes the
  // root, so subscribers that re-home to it (or join after) still replay the
  // pre-departure history via since:'all'. This is the common-case durability fix
  // for single-root topics — nodes that leave gracefully (peer.leave()) hand off;
  // abrupt death still needs a replica/in-region host (separate work).
  async _onHandoff(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    const topicBig = idBig(payload.topicId);
    // Adopt as a root: _becomeRoot makes the role (isRoot) if we don't have one,
    // so we serve the inherited history; routing/beacons reconcile root-ness.
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    this._applyDels(role, topicBig, payload.dels);   // inherit the heir's tombstones, not just its bodies
    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      if (m && typeof m.json === 'string' && Number.isFinite(m.publishTs)) await this._ingestStamped(role, m);
    }
    return 'consumed';
  }

  // Called from AxonaPeer.leave() while the transport is still up: for every
  // topic we ROOT and hold cache for, push the cache to the heir (next-closest
  // live node) so the history isn't lost when we go. Best-effort; never throws.
  async pubsubLeaveHandoff() {
    if (typeof this.dht.findKClosest !== 'function') return;
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot || !role.cache.length) continue;
      let heir = null;
      try {
        const arr = await this.dht.findKClosest(t, 3);
        for (const id of (Array.isArray(arr) ? arr : [])) {
          const b = idBig(id);
          if (b !== this.nodeId) { heir = b; break; }   // closest node that isn't us
        }
      } catch { /* no heir resolvable → nothing we can do */ }
      if (heir === null) continue;
      const msgs = role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq }));
      const dels = this._activeDels(role);
      try { this._route(heir, T.HANDOFF, { topicId: idHex(t), msgs, dels }); } catch { /* best-effort */ }
    }
  }

  // ── singleton-root replication (warm backup roots) ───────────────────
  // The N reachable neighbours XOR-closest to a topic — its natural successors and
  // therefore the right backup roots (whoever routing re-converges on if the root
  // leaves). Excludes self and the bridge (signaling infra, never a root).
  // The active (non-expired) tombstones as a wire-shaped `dels` array. Every cache
  // MIGRATION (replicate, pull-up/replay-up, graceful handoff) must carry these
  // alongside `msgs`: a node that adopts cached history without the matching tombstones
  // would resurrect a killed message and serve it to a late joiner (the kill-leak class).
  _activeDels(role) {
    const now = this._now();
    const dels = [];
    for (const [tgt, tomb] of (role?.tombstones ?? [])) {
      if ((tomb?.exp ?? 0) > now) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, seq: tomb.seq, publishTs: tomb.killTs });
    }
    return dels;
  }

  // Apply a batch of migrated tombstones BEFORE ingesting the migrated bodies, so a
  // killed message in the same batch is suppressed (not briefly fanned/delivered then
  // retracted). Shared by the replay-up and handoff receive paths.
  _applyDels(role, topicBig, dels) {
    for (const d of (Array.isArray(dels) ? dels : [])) {
      if (d && d.msgId) this._applyKill(role, topicBig, d);
    }
  }

  _nearestReachable(tBig, n, bridge) {
    if (n <= 0 || typeof this.dht.neighbors !== 'function') return [];
    const cand = [];
    for (const nb of (this.dht.neighbors() || [])) {
      let b; try { b = idBig(nb); } catch { continue; }
      if (b === this.nodeId || (bridge != null && b === bridge)) continue;
      cand.push(b);
    }
    cand.sort((a, b) => (a ^ tBig) < (b ^ tBig) ? -1 : 1);
    return cand.slice(0, n).map(b => lc(idHex(b)));
  }

  // Replicate each SINGLETON root's cache to its N nearest neighbours as warm
  // backups, so an abrupt root churn doesn't lose the history (the dominant
  // post-churn since:'all' recovery failure). Only for roots with NO sub-axon tree
  // (children) — larger topics already have cache-holding relays. Idempotent: the
  // full cache+tombstones are (re)pushed each tick (singleton caches are small), which
  // also serves as a liveness heartbeat (refreshes the backup's lastReplicaAt) and
  // self-heals any miss. Backups track the closest-N: closer newcomers are recruited,
  // farther ones retired. On root churn the now-closest backup already holds everything
  // and promotes (via _onSub-terminal when a joiner routes to it, or the stale-promote
  // check below) with no gap.
  _replicateRoots() {
    if (!this._rootReplicas) return;
    const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
    const now = this._now();
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot) continue;
      this._replicateRole(t, role, bridge, now).catch(() => {});   // async (findKClosest); fire-and-forget
    }
  }

  // Replicate a root's full cache+tombstones to its K-closest COHORT — the set a
  // subscriber can actually land on. We target `findKClosest(topic, K)` (the same
  // local, non-probing nearest source subscribe resolves its root with), NOT just our
  // direct neighbours: a late subscriber attaches to the GLOBALLY closest node, which
  // may be several hops from us, so a neighbour-only push silently misses it (the
  // post-churn "message reaches one root but not the root the joiner picks" loss — a
  // KILL just makes that loss conspicuous). Because the cohort = the K closest and a
  // subscriber routes to the closest-1, the joiner's node is by construction in the
  // cohort. Idempotent FULL-state push (singleton caches are small) → also a liveness
  // heartbeat + anti-entropy: co-hosting roots converge to the union of cache+tombstones,
  // and tombstones keep killed bodies suppressed. Called every tick AND eagerly the
  // instant a message is stamped or a kill lands, so no holder lags the cohort.
  async _replicateRole(t, role, bridge, now) {
    if (!this._rootReplicas || !role || !role.isRoot) return;
    if (role.cache.length === 0 && role.tombstones.size === 0) return;         // nothing to preserve yet
    let want;
    if (typeof this.dht.findKClosest === 'function') {
      let arr = [];
      try { arr = await this.dht.findKClosest(t, this._rootReplicas + 1); } catch { arr = []; }  // +1: self is usually closest
      const seen = new Set(); want = [];
      for (const id of (Array.isArray(arr) ? arr : [])) {
        let b; try { b = idBig(id); } catch { continue; }
        if (b === this.nodeId || (bridge != null && b === bridge)) continue;   // never self / bridge
        const hex = lc(idHex(b)); if (seen.has(hex)) continue; seen.add(hex);
        want.push(hex); if (want.length >= this._rootReplicas) break;
      }
    } else {
      want = this._nearestReachable(t, this._rootReplicas, bridge);            // sim/fallback: neighbour-based
    }
    const wantSet = new Set(want);
    for (const hex of [...role.replicas.keys()]) if (!wantSet.has(hex)) role.replicas.delete(hex);   // retire those no longer in the cohort
    if (want.length === 0) return;
    const msgs = role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq }));
    const dels = this._activeDels(role);
    for (const hex of want) {
      try { this._route(idBig(hex), T.REPLICATE, { topicId: idHex(t), from: idHex(this.nodeId), msgs, dels }); } catch { /* best-effort */ }
      role.replicas.set(hex, { at: now });
    }
  }

  // Receive a replica push: become (or refresh) a passive BACKUP for this topic —
  // hold the full cache + tombstones without claiming root or fanning to subscribers
  // while the root keeps replicating. Verifies each message (don't-trust) via the
  // normal stamped-ingest. On root churn this cache is what makes instant takeover
  // gap-free (see the stale-promote in refreshTick + _onSub-terminal promotion).
  async _onReplicate(payload, meta) {
    if (meta.targetId !== this.nodeId) return;          // routed to me as the backup
    if (!this._rootReplicas) return 'consumed';         // replication disabled on this node
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return 'consumed'; }
    let from = null;
    if (payload.from && isHexId(lc(payload.from))) from = lc(payload.from);
    else if (meta.fromId != null) { try { from = lc(idHex(idBig(meta.fromId))); } catch { /* */ } }
    let role = this.axonRoles.get(topicBig);
    if (!role) { role = makeRole(topicBig, false); this.axonRoles.set(topicBig, role); }
    if (role.isRoot) return 'consumed';                 // I'm already (a closer) root — ignore a backup push
    role.backupOf = from;
    role.lastReplicaAt = this._now();
    this._applyDels(role, topicBig, payload.dels);   // tombstones FIRST → a killed body in the same push is suppressed
    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      if (m && typeof m.json === 'string' && Number.isFinite(m.publishTs)) await this._ingestStamped(role, m);
    }
    return 'consumed';
  }

  // ── DELIVER (parent → subscriber; a relay re-fans down the tree) ──────
  _onDeliver(payload, meta) {
    if (meta.targetId !== this.nodeId) return;        // forward (intermediate hop)
    const topicBig = idBig(payload.topicId);
    if (payload.from) {
      const fromHex = lc(payload.from);
      const prev = this._upstream.get(topicBig);
      // Re-pin to our relay. If the relay CHANGED (re-home after a churn), snap the
      // adaptive renewal interval back to fast — so we monitor the new attachment
      // closely and re-home quickly again if it too churns (the mobile common case).
      if ((!prev || prev[0] !== fromHex)) {
        const s = this.mySubscriptions.get(topicBig);
        if (s) s.interval = this.renewFastMs;
      }
      this._upstream.set(topicBig, [fromHex]);
    }

    const role = this.axonRoles.get(topicBig);        // set iff I'm a relay → re-fan
    for (const m of (Array.isArray(payload.msgs) ? payload.msgs : [])) {
      if (!m) continue;
      if (m.del) { this._applyKill(role, topicBig, m); continue; }   // del-marker carries killTs+signer
      if (this._tombstoned(role, m.msgId, m.json)) continue;          // killed → suppress (don't cache/deliver/re-fan)
      if (role && !role.cacheIds.has(m.msgId)) {       // relay: cache once + re-fan once
        this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json, seq: m.seq });
        if (Number.isFinite(m.seq) && m.seq > role.seq) role.seq = m.seq;   // keep counter ready if we're promoted to root
        this._fanout(role, m, lc(payload.from));       // exclude the sender (m carries seq)
      }
      this._deliverToApp(topicBig, m.json, m.msgId, m.publishTs, m.seq);
    }
    return 'consumed';
  }

  // Fan a stamped message to every subscriber (optionally excluding the sender).
  _fanout(role, msg, excludeHex) {
    const base = { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [msg] };
    for (const subHex of role.subscribers.keys()) {
      if (excludeHex && subHex === excludeHex) continue;
      this._route(idBig(subHex), T.DELIVER, { ...base });
    }
  }

  // Replay the cache delta (publishTs > since) to one subscriber, chunked by
  // bytes. `ping` forces a (possibly empty) deliver so a freshly-seated
  // subscriber repins to us even when the cache has nothing newer.
  // `latest` (since:'latest') also includes the single NEWEST retained entry
  // regardless of age — folded into this same DELIVER, no extra message. The
  // ts-floor delta can't express "newest regardless of age": a current value
  // published before the subscriber's now-anchored floor sits below it and is
  // filtered out (the "subscribe latest → no callback" bug). Receiver dedups.
  _replayTo(role, subHex, sinceTs, ping, latest = false) {
    const subBig = idBig(subHex);
    const isSelf = subBig === this.nodeId;
    let batch = [], bytes = 0, sent = false;
    const inBatch = new Set();
    const flush = () => {
      if (!batch.length) return;
      sent = true;
      if (isSelf) for (const m of batch) this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs, m.seq);
      else this._route(subBig, T.DELIVER,
        { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: batch });
      batch = []; bytes = 0;
    };
    for (const c of role.cache) {
      if (c.publishTs <= sinceTs) continue;
      if (bytes + c.bytes > REPLAY_CHUNK_BYTES) flush();
      batch.push({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq });
      inBatch.add(c.msgId);
      bytes += c.bytes;
    }
    if (latest && role.cache.length) {               // ensure the current value rides along
      const newest = role.cache[role.cache.length - 1];
      if (!inBatch.has(newest.msgId)) {
        if (bytes + newest.bytes > REPLAY_CHUNK_BYTES) flush();
        batch.push({ json: newest.json, publishTs: newest.publishTs, msgId: newest.msgId, seq: newest.seq });
      }
    }
    flush();
    // Self-heal kills: re-send EVERY active (non-expired) tombstone on each
    // renewal — NOT gated on `since`. A since-delta can't express "you're missing
    // an OLD deletion": a node that missed the kill but kept receiving newer
    // messages has a `since` already PAST killTs, so a killTs>since gate would
    // never backfill it → it keeps the killed body forever and serves it to late
    // subscribers (the v4.8.7 permanent-leak edge found by the soak). Replaying all
    // live tombstones guarantees convergence; the receiver dedups idempotently
    // (tombstone-set gate on re-fan + exactly-once kill key on app delivery), so a
    // sub that already has them does no extra work. Bounded by the TTL'd tombstone
    // set. Carries killTs+signer so the receiver's tombstone matches.
    const dels = [];
    for (const [tgt, tomb] of role.tombstones) {
      if ((tomb?.exp ?? 0) > this._now()) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, publishTs: tomb.killTs, seq: tomb.seq });
    }
    if (dels.length) {
      sent = true;
      if (isSelf) for (const dm of dels) this._deliverKillToApp(role.topicId, dm.msgId, dm.killTs, dm.seq);
      else this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: dels });
    }
    if (ping && !sent && !isSelf) {                    // repin even with no history
      this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [] });
    }
  }

  _replayLocal(role, sinceTs, latest = false) {
    const seen = new Set();
    for (const c of role.cache) if (c.publishTs > sinceTs) { this._deliverToApp(role.topicId, c.json, c.msgId, c.publishTs, c.seq); seen.add(c.msgId); }
    if (latest && role.cache.length) {               // since:'latest' — newest regardless of age
      const newest = role.cache[role.cache.length - 1];
      if (!seen.has(newest.msgId)) this._deliverToApp(role.topicId, newest.json, newest.msgId, newest.publishTs, newest.seq);
    }
    // kills replay alongside the cache (exactly-once on the kill-specific key).
    for (const [tgt, tomb] of role.tombstones) if ((tomb?.exp ?? 0) > this._now()) this._deliverKillToApp(role.topicId, tgt, tomb.killTs, tomb.seq);
  }

  // ── cache ────────────────────────────────────────────────────────────
  _cachePush(role, entry) {
    entry.bytes = (entry.json ? entry.json.length : 0) + 80;
    role.cache.push(entry);
    role.cacheIds.add(entry.msgId);
    role.cacheBytes += entry.bytes;
    while (role.cache.length > this._cacheMax || role.cacheBytes > this._cacheBytes) {
      const old = role.cache.shift();
      if (!old) break;
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  }
  _expireCache(role, now) {
    while (role.cache.length && (now - role.cache[0].publishTs) > TTL_MS) {
      const old = role.cache.shift();
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  }

  // ── app delivery (exactly-once) ──────────────────────────────────────
  _deliverToApp(topicBig, json, msgId, publishTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const key = topicBig.toString(16) + ':' + msgId;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (publishTs > prev) this._lastSeenTsByTopic.set(topicBig, publishTs);
    this._confirmPending(topicBig, msgId);             // a subscribed publisher saw its own msg → stop retrying
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, json, msgId, publishTs, seq); }
      catch (e) { this._log('warn', 'delivery-callback-threw', { err: e?.message }); }
    }
  }

  // Apply a (verified) kill: a kill is a publish-with-a-delete-side-effect. We
  // record a TOMBSTONE keyed by the target msgId — carrying the kill's stamp
  // (killTs) and its authorizing signer — drop the target from cache, and fan the
  // delete down ONCE. The tombstone is now first-class replayable state (see the
  // tombstone emit in _replayTo/_replayLocal): a subscriber that missed the live
  // delete re-acquires it on its next renewal, exactly like a missed publish
  // re-acquires from cache. Idempotent: the fan-out + cache-drop happen only the
  // first time we see the kill (tombstone-gated).
  _applyKill(role, topicBig, m) {
    const target = m.msgId;
    const killTs = m.killTs ?? this._now();
    const seq = m.seq;                                 // root-assigned dense counter for this kill
    if (role && Number.isFinite(seq) && seq > role.seq) role.seq = seq;   // recover counter (kill occupied a slot)
    if (role && !role.tombstones.has(target)) {
      role.tombstones.set(target, { exp: this._now() + TTL_MS, killTs, signer: m.signer ?? null, seq });
      const i = role.cache.findIndex(c => c.msgId === target);
      if (i >= 0) { role.cacheBytes -= role.cache[i].bytes; role.cache.splice(i, 1); }
      role.cacheIds.delete(target);
      // fan the delete down — carries killTs + signer + seq so each receiver records
      // an identical tombstone (consistent replay + ordering + provisional authorship).
      this._fanout(role, { del: true, msgId: target, killTs, signer: m.signer ?? null, publishTs: killTs, seq }, null);
      // Replicas/cohort aren't subscribers/children — they don't see the fan-out. Push the
      // new tombstone to the cohort EAGERLY (not on the next tick) so a co-hosting root or a
      // backup that promotes mid-window can't serve the killed body it already holds (the
      // kill-leak race). Same eager path a publish takes — a kill is just a publish + side effect.
      if (role.isRoot) {
        const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
        this._replicateRole(topicBig, role, bridge, this._now()).catch(() => {});
      }
    }
    this._confirmPending(topicBig, target);            // our own kill landed → stop retrying it
    this._deliverKillToApp(topicBig, target, killTs, seq);
  }

  // Deliver a kill to the local app (exactly-once, keyed distinctly from the
  // target's own delivery) and advance our since-floor to the kill's stamp so a
  // renewal stops re-pulling it once seen — while a MISSED kill (since still below
  // killTs) is re-pulled and self-heals.
  _deliverKillToApp(topicBig, target, killTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (killTs > prev) this._lastSeenTsByTopic.set(topicBig, killTs);
    const key = topicBig.toString(16) + ':kill:' + target;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    // A kill retracts a message the app is holding. If this app NEVER received the body
    // (a since:'all' joiner that arrived after the kill — the killed body is spliced from
    // cache and never replayed), there is nothing to retract: delivering a spurious
    // "deleted" event for a message it never saw is noise. Record the tombstone + advance
    // the floor (above) so we converge and suppress the body if it ever arrives, but only
    // CALL BACK when the body was actually delivered to this app. (Now reliably reproduced
    // once cohort anti-entropy made tombstone propagation dependable.)
    const bodyKey = topicBig.toString(16) + ':' + target;
    if (!this._appDelivered.has(bodyKey)) return;      // never had the body → nothing to retract
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, JSON.stringify({ deleted: true, msgId: target, topic: null }), target, killTs, seq); }
      catch (e) { this._log('warn', 'delete-callback-threw', { err: e?.message }); }
    }
  }

  // Implicit ACK for the persistent publish/kill retry: when this node OBSERVES a
  // msgId locally (it became root and cached it / it relayed it / it was delivered
  // to our app, or a kill tombstoned it), any pending publish/kill we hold for that
  // msgId has demonstrably reached a holder — stop re-sending it. Publishers that
  // never observe their own msg (non-subscribed, non-root) fall back to the bounded
  // maxTries/TTL in refreshTick.
  _confirmPending(_topicBig, msgId) {
    if (!msgId) return;                                // pending maps are keyed by msgId (globally unique)
    this._pendingPub?.delete(msgId);
    this._pendingKill?.delete(msgId);
  }

  // A target message arriving (publish / replay / fan-out) for which we hold a
  // tombstone: SUPPRESS it iff the kill was authorized by the message's own author
  // — this is where a PROVISIONAL kill (accepted before we held the target) is
  // enforced. A signer MISMATCH (or an unsigned/anonymous target a signed kill
  // can't own) means the kill was forged or unauthorized → REVOKE the tombstone
  // and accept the message. Returns true to drop the message.
  _tombstoned(role, msgId, json) {
    if (!role) return false;
    const tomb = role.tombstones.get(msgId);
    if (!tomb) return false;
    if (tomb.signer) {
      let author = null; try { author = JSON.parse(json)?.signerPubkey ?? null; } catch { /* */ }
      if (author && lc(author) === lc(tomb.signer)) return true;      // authorized kill → stays dead
      role.tombstones.delete(msgId);                                   // unauthorized/forged → revoke
      return false;
    }
    return true;                                                       // signer-less tombstone (defensive) → suppress
  }

  _becomeRoot(topicBig) {
    const role = makeRole(topicBig, true);
    this.axonRoles.set(topicBig, role);
    this._log('info', 'root-formed', { topic: idHex(topicBig).slice(0, 12) });
    this._announceRoot(topicBig);
    return role;
  }

  // Emit a root beacon IMMEDIATELY on becoming root, so a brand-new topic's
  // location is advertised at once instead of waiting up to BEACON_MS for the
  // throttled tick (closes the cold-publish timing gap: discovery 0% cases where
  // the publisher fires before the root's first periodic beacon). Lightly
  // rate-limited per topic so a flapping promotion can't storm the basin.
  _announceRoot(topicBig) {
    if (typeof this.dht.neighbors !== 'function') return;
    const now = this._now();
    if (!this._lastAnnounce) this._lastAnnounce = new Map();
    if (now - (this._lastAnnounce.get(topicBig) || 0) < BEACON_MS / 2) return;
    this._lastAnnounce.set(topicBig, now);
    this._emitRootBeacons();
  }

  // The `since` to renew with: max of our cache high-water (relay), last app
  // delivery, and the seeded subscription floor.
  _sinceFor(topicBig) {
    const role = this.axonRoles.get(topicBig);
    const relay = (role && role.cache.length) ? role.cache[role.cache.length - 1].publishTs : 0;
    const seen  = this._lastSeenTsByTopic.get(topicBig);
    const sub   = this.mySubscriptions.get(topicBig)?.since;
    return Math.max(relay, Number.isFinite(seen) ? seen : 0, Number.isFinite(sub) ? sub : 0);
  }

  // Non-blocking root hint. The iterative lookup (peer.findKClosest) escapes the
  // greedy-routing local minima that strand subscribers on a sparse mesh, BUT over
  // a real WebRTC mesh it can take many seconds (α-parallel rounds against peers
  // that may be slow to answer). We must NEVER block subscribe/publish on it — a
  // blocking lookup that doesn't finish inside the join window means the SUB/PUB is
  // never sent (observed live: scale 0%). So: return the cached true-root hint
  // immediately (or null), and refresh it in the BACKGROUND. When a fresh hint
  // lands and we're an unpinned subscriber not yet adopted (a greedy local-minimum
  // strand), re-subscribe toward it at once — healing within one lookup latency
  // rather than waiting for the next renewal. Steady state (pinned via the deliver
  // `from`) never consults this; renewals use the cheap via-pin.
  _rootHint_(topicBig) {
    // Highest priority: a fresh root beacon — the root announced its location
    // directly, so no per-node lookup (which can diverge on a gappy mesh) is
    // needed. This is the primary convergence aid (Pubsub-Root-Beacon-v0.1).
    const beacon = this._rootBeacons.get(topicBig);
    if (beacon && this._now() < beacon.exp) return beacon.root;
    // The true-root resolver MUST be the iterative K-closest search: it returns the
    // node XOR-closest to the (virtual) topic id, which is exactly the emergent root.
    // (Prior bug: this used `dht.lookup(topicBig)`, a find-NODE op that returns
    // `{ path, hops, found }` — NOT an id — so `idBig(result)` threw, the catch
    // swallowed it, the hint NEVER seeded, and SUB/PUB fell back to the single-pass
    // greedy walk forever → it strands before reaching the root → ~0% cross-peer
    // delivery on any non-trivial mesh. findKClosest(topicBig,1)[0] is the fix.)
    const resolveClosest =
      (typeof this.dht.findKClosest === 'function')
        ? () => this.dht.findKClosest(topicBig, 1).then(a => (a && a.length) ? a[0] : null)
      : (typeof this.dht.lookup === 'function')
        ? () => this.dht.lookup(topicBig).then(r => (r && Array.isArray(r.path) && r.path.length) ? r.path[r.path.length - 1] : null)
        : null;
    if (!resolveClosest) return null;
    const cached = this._rootHint.get(topicBig);
    // While we have no pin (a fresh subscriber, or one whose root just churned and
    // we dropped the pin), treat the cached hint as stale after only renewFastMs
    // so we re-resolve the CURRENT closest reachable root every few seconds —
    // instead of sitting on a 60s-cached hint that points at a dead/wrong node.
    const attached = (this._upstream.get(topicBig) || []).length > 0;
    const freshFor = attached ? this.renewMs : this.renewFastMs;
    const fresh = cached && (this._now() - cached.at) < freshFor;
    if (!fresh) {
      if (!this._lookupInflight) this._lookupInflight = new Set();
      if (!this._lookupInflight.has(topicBig)) {
        this._lookupInflight.add(topicBig);
        Promise.resolve()
          .then(resolveClosest)
          .then(id => {
            // Self-closest → leave the hint null so we route greedily toward the
            // bare topic id and become root as the terminus (don't via-pin to self).
            let hex = null;
            if (id != null) {
              const h = lc(idHex(idBig(id)));
              if (h !== lc(idHex(this.nodeId))) hex = h;
            }
            this._rootHint.set(topicBig, { via: hex, at: this._now() });
            // Heal (subscribe): subscribed, not yet pinned (no deliver `from`
            // adopted us), and the true root is someone else → re-home toward it.
            if (hex && this.mySubscriptions.has(topicBig) &&
                !(this._upstream.get(topicBig) || []).length) {
              this._emitSubscribe(topicBig, [hex]);
            }
            // Heal (publish/kill): a stranded publish/kill is RE-SENT toward the
            // true root by the persistent retry loop in refreshTick (until the
            // publisher observes its own msgId — implicit ack — or TTL/maxTries).
            // Single one-shot here was insufficient under packet loss: initial +
            // one retry both dropping = the message lost forever, the root never
            // gets it, no subscriber can ever receive it (the ~1/3 publish-strand
            // under 10-30% loss, captured by repro_lossy_restart.mjs).
          })
          .catch(() => { /* resolve failed → greedy stays in effect */ })
          .finally(() => this._lookupInflight.delete(topicBig));
      }
    }
    return cached ? cached.via : null;
  }

  // Bounded warm of the root hint: await the iterative closest-node resolve up to
  // timeoutMs and seed the hint, so the FIRST publish/subscribe routes straight to
  // the true root instead of stranding on the greedy walk (and, for a one-shot
  // publish, never re-routing). NEVER hangs: on timeout it returns and the caller
  // proceeds greedy, with the background _rootHint_ heal still applying. Cheap in
  // steady state — a fresh cached hint or a live beacon short-circuits immediately.
  async warmRootHint(topicBig, timeoutMs = 2500) {
    const beacon = this._rootBeacons.get(topicBig);
    if (beacon && this._now() < beacon.exp) return;
    const cached = this._rootHint.get(topicBig);
    if (cached && (this._now() - cached.at) < this.renewMs) return;
    if (typeof this.dht.findKClosest !== 'function') return;
    try {
      const arr = await Promise.race([
        this.dht.findKClosest(topicBig, 1),
        new Promise((res) => { const t = setTimeout(() => res(null), timeoutMs); if (t && typeof t.unref === 'function') t.unref(); }),
      ]);
      if (Array.isArray(arr) && arr.length) {
        const h = lc(idHex(idBig(arr[0])));
        this._rootHint.set(topicBig, { via: (h !== lc(idHex(this.nodeId))) ? h : null, at: this._now() });
      }
    } catch { /* greedy fallback; background heal still applies */ }
  }

  // ── Root beacon (Pubsub-Root-Beacon-v0.1) ───────────────────────────────
  // Emit: for every topic I root, announce {root: me} to my K XOR-closest
  // neighbors (the topics' convergence basin, since the root ≈ the topic ids),
  // aggregated into one beacon, recursive BEACON_LAYERS deep. No-op without a
  // neighbors() adapter (sim/fabric that don't model topology simply skip it).
  _emitRootBeacons() {
    if (typeof this.dht.neighbors !== 'function') return;
    const rooted = [];
    for (const [t, r] of this.axonRoles) if (r.isRoot) rooted.push(t);
    if (!rooted.length) return;
    const neigh = (this.dht.neighbors() || []).map(idBig).filter(n => n !== this.nodeId);
    if (!neigh.length) return;
    const basin = neigh.slice().sort((a, b) => this._cmpXor(a, b, this.nodeId)).slice(0, this._beaconFanout);
    const payload = {
      root: lc(idHex(this.nodeId)),
      topics: rooted.map(idHex),
      beaconId: `${idHex(this.nodeId).slice(0, 10)}-${this._now()}-${this._beaconSeq++}`,
      layer: this._beaconLayers,
    };
    this._beaconSeen.set(payload.beaconId, this._now() + BEACON_SEEN_MS);   // never re-forward my own
    for (const nb of basin) this._route(nb, T.ROOTBEACON, payload);
  }

  // Receive: cache the pointer (verify-don't-trust), then re-forward once within
  // the basin. The beacon is a HINT — accepted only if `root` is at least as
  // close to the topic as my own best-known node, so a liar cannot divert a
  // publish to a node FARTHER from the topic than honest routing would pick.
  _onRootBeacon(payload, meta) {
    if (!payload || typeof payload.root !== 'string' || !Array.isArray(payload.topics)) return;
    if (!payload.beaconId || this._beaconSeen.has(payload.beaconId)) return;
    this._beaconSeen.set(payload.beaconId, this._now() + BEACON_SEEN_MS);
    let rootBig; try { if (!isHexId(lc(payload.root))) return; rootBig = idBig(payload.root); } catch { return; }
    const now = this._now();
    for (const tHex of payload.topics.slice(0, 256)) {
      let tBig; try { if (!isHexId(lc(tHex))) continue; tBig = idBig(tHex); } catch { continue; }
      const mine = this._bestKnownClosest(tBig);                           // local-only
      if (mine != null && (rootBig ^ tBig) > (mine ^ tBig)) continue;       // verify-don't-trust
      this._rootBeacons.set(tBig, { root: lc(payload.root), at: now, exp: now + BEACON_TTL_MS });
      // If I'd wrongly become this topic's root but the beacon proves a strictly
      // closer root exists, demote NOW and renew toward it — so I stop claiming
      // the topic and stop emitting poisoning "root=me" beacons.
      if ((rootBig ^ tBig) < (this.nodeId ^ tBig)) {
        const role = this.axonRoles.get(tBig);
        if (role && role.isRoot && rootBig !== this.nodeId) {
          role.isRoot = false;
          this._upstream.set(tBig, [lc(payload.root)]);
          // Pinning upstream is not enough: the new root must REGISTER us as a
          // downstream child or it can't fan deliveries back down to us (and our
          // subtree). Every other _upstream write is paired with a confirming
          // subscribe-k (_onAdopt → _sendSubscribe; deliver-`from` is self-proving)
          // — this beacon-demotion path was the lone exception, leaving a
          // one-sided link: we renew toward the root, but the root never adopts
          // us, so _fanout (over role.subscribers) skips our branch entirely.
          // Symptom: chained sub→relay→root delivers 0 to the relay's subtree
          // while the root caches the message (root subs=0, cache>0). Emit the
          // subscribe-k now so the new root seats us and the tree fans down.
          this._sendSubscribe(tBig);
        }
      }
    }
    if (payload.layer > 1 && typeof this.dht.neighbors === 'function') {
      let from = null; try { if (meta && meta.fromId != null) from = idBig(meta.fromId); } catch { /* */ }
      const neigh = (this.dht.neighbors() || []).map(idBig).filter(n => n !== this.nodeId && n !== from);
      const fwd = { ...payload, layer: payload.layer - 1 };
      for (const nb of neigh.sort((a, b) => this._cmpXor(a, b, this.nodeId)).slice(0, this._beaconFanout)) {
        this._route(nb, T.ROOTBEACON, fwd);
      }
    }
  }

  // Nearest node to `tBig` among what I know LOCALLY: my neighbors, myself, and
  // any cached beacon root. Never triggers a network lookup (keeps the verify
  // step cheap and non-amplifying).
  _bestKnownClosest(tBig) {
    const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
    let best = this.nodeId, bestD = this.nodeId ^ tBig;
    if (typeof this.dht.neighbors === 'function') {
      for (const n of (this.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        if (bridge != null && nb === bridge) continue;   // bridge never a root → don't let it gate beacon acceptance
        const d = nb ^ tBig; if (d < bestD) { bestD = d; best = nb; }
      }
    }
    const b = this._rootBeacons.get(tBig);
    if (b) { try { const rb = idBig(b.root); const d = rb ^ tBig; if (d < bestD) { bestD = d; best = rb; } } catch { /* */ } }
    return best;
  }

  // True iff `self` is XOR-closest to `tBig` among the nodes we can ACTUALLY reach
  // (self + direct, authenticated neighbours), excluding the bridge (signaling infra,
  // never a topic root). The iterative findKClosest hint may name a node closer in
  // XOR than any of these, but if it never adopts us it is effectively unreachable;
  // when self beats every reachable neighbour, self is the best reachable root. Pure
  // local read — no network probe. Sim/fabric without neighbours() → trivially self.
  _selfClosestReachable(tBig) {
    const bridge = (typeof this.dht.bridgeId === 'function') ? this.dht.bridgeId() : null;
    let bestD = this.nodeId ^ tBig;
    if (typeof this.dht.neighbors === 'function') {
      for (const n of (this.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        if (nb === this.nodeId || (bridge != null && nb === bridge)) continue;
        if ((nb ^ tBig) < bestD) return false;     // a reachable neighbour is closer → route to it
      }
    }
    return true;
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
    // Cold-publish burst: if we're not yet integrated, front-load a few fast re-sends
    // (see COLD_BURST_* above) — idempotent, and each send also integrates us.
    if (pmsgId && this._isColdPublisher()) this._coldPublishBurst(topicId, pmsgId);
    return meta.postHash || '';
  }

  // "Cold" = this node hasn't accreted enough neighbours to route reliably to an
  // arbitrary topic root yet (a freshly-joined node). Cheap, and self-clearing:
  // once the synaptome fills past the threshold, publishes go back to a single send.
  _isColdPublisher() {
    if (typeof this.dht.neighbors !== 'function') return false;
    let n = 0; try { n = (this.dht.neighbors() || []).length; } catch { /* */ }
    return n < COLD_PEER_THRESHOLD;
  }

  // Re-send the SAME pending envelope a few times over the first ~second. Each tick
  // re-resolves the root hint (the background lookup that nudges integration) and
  // stops early the moment the publish is confirmed/expired (_pendingPub no longer
  // holds this msgId). Idempotent end-to-end: the root dedups by msgId.
  _coldPublishBurst(topicBig, msgId) {
    let i = 0;
    const schedule = () => {
      const h = setTimeout(() => {
        this._burstTimers.delete(h);
        const p = this._pendingPub?.get(msgId);
        if (!p) return;                                 // confirmed or aged out → done
        const hint = this._rootHint_(topicBig);
        this._send(T.PUB, { topicId: idHex(topicBig), via: hint ? [hint] : [], json: p.json });
        if (++i < COLD_BURST_TRIES) schedule();
      }, COLD_BURST_INTERVAL_MS);
      if (typeof h.unref === 'function') h.unref();
      this._burstTimers.add(h);
    };
    schedule();
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
    this._sendSubscribe(topicId);
  }

  pubsubUnsubscribe(topicId) {
    this.mySubscriptions.delete(topicId);
    const via = this._upstream.get(topicId) || [];
    this._send(T.UNSUB, { topicId: idHex(topicId), via, subscriberId: idHex(this.nodeId) });
    this.pubsubResetTopicConsumption(topicId);
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
  requestMetrics() { return Promise.resolve({ accumulated: [] }); }   // TODO(Phase 4)

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
  invalidateKClosestCache() { /* no K-closest cache in the routed model — no-op */ }

  resetState() {
    this.axonRoles.clear();
    this.mySubscriptions.clear();
    this._hostedTopics.clear();
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

  // ── side-function handlers (thin; TODO Phase 4) ──────────────────────
  async _onKill(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.KILL, payload); return 'consumed'; }
    const topicBig = idBig(payload.topicId);
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    const kill = payload.kill;
    const target = kill?.msgId;
    if (!target) return 'consumed';
    // AUTHZ: a kill must be signed, and only the AUTHOR of the target may kill it.
    // (1) signature self-valid (B-4 analog for kills).
    const v = await verifyKill(kill);
    if (!v.ok) { this._log('warn', 'drop-bad-kill', { reason: v.reason }); return 'consumed'; }
    // (2) authorship: if we hold the target, enforce signer===author NOW; if we
    // don't (kill races ahead of the publish, or we're a fresh root), accept
    // PROVISIONALLY — record the kill's signer and enforce when the target arrives
    // (ingest checks the tombstone's signer; a forged kill is revoked on mismatch).
    const cached = role.cache.find(c => c.msgId === target);
    if (cached) {
      let author = null; try { author = JSON.parse(cached.json)?.signerPubkey ?? null; } catch { /* */ }
      if (!author || lc(author) !== lc(kill.signerPubkey)) {
        this._log('warn', 'drop-unauthorized-kill', { target: String(target).slice(0, 12) });
        return 'consumed';
      }
    }
    // STAMP the kill like a publish (monotonic + dense counter) so it orders +
    // replays via `since` AND occupies a seq slot (a missed kill shows as a gap).
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;
    const seq = ++role.seq;
    this._applyKill(role, topicBig, { msgId: target, killTs: ts, signer: lc(kill.signerPubkey), seq });
    return 'consumed';
  }
  // _onUnpub — REMOVED v4.3.0 (keep kill, drop unpub)
  _onTouch(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.TOUCH, payload); return 'consumed'; }
    return 'consumed';
  }
  _onPull(payload, meta) {
    const role = this.axonRoles.get(idBig(payload.topicId));
    // Cache-hit early-answer (v4.11.1 by-msgId; v4.11.2 pull-latest too): a pull is a
    // read of replicated state — answer from the FIRST replica the routed PULL reaches
    // (a cohort member / child / host) instead of always driving it to the single root.
    //   • by-msgId    → exact: msgId = H(publisher‖message), so a nearer copy IS the copy.
    //   • pull-latest → served with whatever NEWEST this replica holds. Deliberately
    //     "recent, not necessarily THE newest": a hot read path (many pull-latest, e.g.
    //     polling current state) is spread across the cohort/children rather than
    //     hammering the root, which would otherwise be a read throughput bottleneck +
    //     SPOF. The small staleness window closes as the cohort converges (anti-entropy);
    //     a caller that needs the linearizable newest should pull a specific msgId.
    // A cached message is by definition NOT tombstoned here (a kill drops it), so neither
    // path can resurrect a killed message. A replica with an EMPTY cache does not
    // early-answer (nothing on hand) — it forwards so a populated node / the root answers.
    if (role && role.cache.length) {
      const hit = payload.postHash ? role.cache.find(c => c.msgId === payload.postHash)
                                   : role.cache[role.cache.length - 1];
      if (hit) { this._answerPull(payload, hit); return 'consumed'; }
    }
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.PULL, payload); return 'consumed'; }
    // Terminus (root/closest) fall-through: answer if we hold it, else a genuine null.
    const hit = role ? (payload.postHash ? role.cache.find(c => c.msgId === payload.postHash) : role.cache[role.cache.length - 1]) : null;
    this._answerPull(payload, hit || null);
    return 'consumed';
  }
  _answerPull(payload, hit) {
    const resp = { corrId: payload.corrId, json: hit ? hit.json : null, publishTs: hit ? hit.publishTs : null, requesterId: payload.requesterId };
    if (idBig(payload.requesterId) === this.nodeId) this._onPullResp(resp, { targetId: this.nodeId });
    else this._route(idBig(payload.requesterId), T.PULLRESP, resp);
  }
  _onPullResp(payload, meta) {
    if (meta.targetId !== this.nodeId && idBig(payload.requesterId) !== this.nodeId) return;
    const p = this._pending.get(payload.corrId);
    if (!p) return 'consumed';
    clearTimeout(p.timer);
    this._pending.delete(payload.corrId);
    let parsed = null;
    if (payload.json) { try { parsed = JSON.parse(payload.json); } catch { parsed = null; } }
    p.resolve(parsed ? (parsed.message ?? parsed) : null);
    return 'consumed';
  }

  // ── lifecycle: renewal + eviction + TTL sweep ────────────────────────
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => { this.refreshTick().catch(() => {}); }, this.refreshIntervalMs);
    if (typeof this._timer.unref === 'function') this._timer.unref();
  }
  stop() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    for (const h of this._burstTimers) clearTimeout(h);
    this._burstTimers.clear();
  }

  async refreshTick() {
    const now = this._now();

    // 1. Renew toward our upstream: app subscriptions + non-root relay roles
    //    (a root has no parent — its self-loop is a no-op, so we skip it).
    const toRenew = new Set(this.mySubscriptions.keys());
    for (const [t, role] of this.axonRoles) if (!role.isRoot && role.subscribers.size > 0) toRenew.add(t);
    for (const t of toRenew) {
      const role = this.axonRoles.get(t);
      if (role && role.isRoot) continue;
      const s = this.mySubscriptions.get(t);
      if (s) {
        // Stay at the fast floor while UNATTACHED (no upstream pin yet — a fresh
        // or stranded subscriber) so it retries + re-resolves quickly; back off
        // ×1.5 only once attached + stable. Paired with the unattached root-hint
        // re-resolve in _rootHint_, this turns a stranded subscriber's 60s-cached
        // dead-hint wait into a few-second re-home (the dead-pin case is already
        // covered by the route-via-dead-waypoint reroute).
        const attached = (this._upstream.get(t) || []).length > 0;
        // Reachable-root fallback: if we've been subscribed-but-unpinned past the
        // confirmation window (the iterative hint named a closer node that never
        // adopted us — unreachable / broken-but-authentic) AND no reachable
        // neighbour is closer to the topic than us, claim root locally rather than
        // defer forever to an unreachable node. Prefer a reachable root over a
        // closer-but-unconfirmed one. (A wrongly-claimed farther root self-corrects
        // via the strictly-closer beacon demotion in _onRootBeacon.)
        if (attached) {
          this._unattachedSince.delete(t);
        } else {
          if (!this._unattachedSince.has(t)) this._unattachedSince.set(t, now);
          if (now - this._unattachedSince.get(t) >= ROOT_CLAIM_MS && this._selfClosestReachable(t)) {
            const role2 = this.axonRoles.get(t) || this._becomeRoot(t);
            role2.isRoot = true;
            this._upstream.delete(t);
            this._rootHint.delete(t);          // stop deferring to the unreachable hint
            this._unattachedSince.delete(t);
            this._log('info', 'root-claimed-reachable', { topic: idHex(t).slice(0, 12) });
            continue;                          // we are root now — no upstream to renew toward
          }
        }
        const iv = attached ? (s.interval || this.renewFastMs) : this.renewFastMs;
        if (now - s.lastRenewSent < iv) continue;
        s.lastRenewSent = now;
        s.interval = attached
          ? Math.min(this.renewMs, Math.round((s.interval || this.renewFastMs) * RENEW_BACKOFF))
          : this.renewFastMs;
      }
      this._sendSubscribe(t);
    }
    for (const t of this._hostedTopics) {
      // Route hosted re-announce through _sendSubscribe so it (a) renews toward the
      // current root via _upstream and (b) advertises our high-water (§6). The old
      // raw send omitted `hw`, so a cache-bearing host never told a freshly-promoted
      // root it held history → the root never issued PULLUP and the cache stayed
      // stranded below an empty root (lost on the original root's departure).
      this._sendSubscribe(t);
    }
    // 1b. Cache-bearing-root re-announce (history-recovery durability) — A/B GATED OFF.
    // Generalising the hosted re-announce to ANY cache-bearing root measured BELOW
    // the deploy gate (Howard 25/30, regressing the kill/since:'all' recovery —
    // hypothesis: multiple cache-bearing roots dueling per tick). Disabled pending a
    // controlled A/B + a safer design (transient-non-host roots only, or rate-limited).
    if (this._reannounceCacheRoots) {
      for (const [t, role] of this.axonRoles) {
        if (!role.isRoot || role.cache.length === 0) continue;     // only roots holding history
        if (this._hostedTopics.has(t) || this.mySubscriptions.has(t)) continue;  // already re-announced above
        this._sendSubscribe(t);
      }
    }
    // 1b-rep. Singleton-root replication (warm backup roots) + backup promotion.
    this._replicateRoots();
    for (const [t, role] of this.axonRoles) {
      if (!role.backupOf) continue;
      if (now - role.lastReplicaAt <= REPLICA_STALE_MS) continue;   // root still replicating → stay a passive standby
      // Root stopped replicating ⇒ presumed gone. Stop being a passive backup; if we
      // are the closest reachable node, promote NOW with our full cache (gap-free
      // takeover). Otherwise drop the backup marker (a closer node should hold it).
      role.backupOf = null;
      if (this._selfClosestReachable(t)) {
        role.isRoot = true;
        this._announceRoot(t);
        this._log('info', 'backup-promoted-root', { topic: idHex(t).slice(0, 12) });
      }
    }

    // 1c. Persistent publish/kill retry (reliability under packet loss). A routed
    //     PUB/KILL is one-shot fire-and-forget; under loss the initial send +
    //     a single heal both dropping = the message never reaches the root and is
    //     lost for everyone. Re-send each tick toward the CURRENT root hint until
    //     the publisher observes its own msgId (implicit ack, _confirmPending) or
    //     maxTries/TTL — idempotent (the root dedups by msgId). Repro:
    //     test/repro_lossy_restart.mjs (root held full backlog ~20/30 → 30/30).
    for (const map of [this._pendingPub, this._pendingKill]) {
      if (!map) continue;
      const isKill = map === this._pendingKill;
      for (const [msgId, p] of map) {                  // keyed by msgId; p.topicBig is the topic
        if (now - p.at > PENDING_PUB_TTL_MS || (p.tries || 0) >= PENDING_PUB_MAX_TRIES) { map.delete(msgId); continue; }
        p.tries = (p.tries || 0) + 1;
        const tb = p.topicBig;
        const hint = this._rootHint_(tb);
        if (isKill) this._send(T.KILL, { topicId: idHex(tb), via: hint ? [hint] : [], kill: p.kill });
        else        this._send(T.PUB,  { topicId: idHex(tb), via: hint ? [hint] : [], json: p.json });
      }
    }

    // 2. Evict stale subscribers; expire cache + tombstones; tear down a role
    //    that is empty and not locally needed.
    for (const [t, role] of this.axonRoles) {
      for (const [subHex, sub] of role.subscribers) {
        if (now - sub.lastRenewed > this.dropMs) { role.subscribers.delete(subHex); role.children.delete(subHex); }
      }
      for (const [msgId, t] of role.tombstones) if ((t?.exp ?? 0) <= now) role.tombstones.delete(msgId);
      this._expireCache(role, now);
      // A ROOT holding non-expired cache MUST persist even with zero subscribers
      // — otherwise a message published before anyone subscribes (or after the
      // last subscriber leaves) is lost the moment refreshTick runs, breaking the
      // TTL hold + late-join replay. The cache itself ages out via _expireCache
      // (TTL), so the role naturally tears down once its history fully expires. A
      // non-root child relay with no subscribers carries only redundant cache (the
      // root has it) so it may tear down immediately.
      const holdsHistory = role.isRoot && role.cache.length > 0;
      // KEYSPACE HOSTING ("host whatever lands near me"): a node with keyspace
      // hosting on retains any topic it has become ROOT for — even with zero
      // current subscribers and an empty cache — so it stays an always-on,
      // durable home/convergence-anchor for topics in its keyspace neighborhood.
      // Without this the role is torn down the instant its cache empties, so the
      // no-arg host() volunteers nothing in the routing-only kernel (the relay
      // fleet's default mode). Root-ness is still decided by ROUTING (this only
      // protects roles the node legitimately won as terminus); the set is bounded
      // by the node's keyspace share of topics that actually see traffic.
      // TODO(Phase 4): age out keyspace-pinned empty roles after a long idle TTL.
      const keyspacePinned = this._hostKeyspace && role.isRoot;
      // A BACKUP holds a deliberate warm copy of another root's history — never tear
      // it down for being subscriber-less, or the durability replica vanishes.
      if (role.subscribers.size === 0 && !holdsHistory && !keyspacePinned && !role.backupOf && !this.mySubscriptions.has(t) && !this._hostedTopics.has(t)) {
        this.axonRoles.delete(t);
        this._upstream.delete(t);
      }
    }

    // 3. Root beacons — advertise where each topic I root lives, to my XOR-closest
    //    neighbors (last-mile convergence aid). Throttled to BEACON_MS; expire the
    //    inbound pointer + flood-dedup caches by their TTLs.
    if (now - this._lastBeaconAt >= BEACON_MS) { this._lastBeaconAt = now; this._emitRootBeacons(); }
    for (const [t, b] of this._rootBeacons) if (b.exp <= now) this._rootBeacons.delete(t);
    for (const [id, exp] of this._beaconSeen) if (exp <= now) this._beaconSeen.delete(id);
  }

  _log(level, event, ctx) {
    if (this._logSink) { try { this._logSink(level, 'pubsub:' + event, ctx); } catch { /* sink threw */ } }
  }
}

export default AxonaManager;
