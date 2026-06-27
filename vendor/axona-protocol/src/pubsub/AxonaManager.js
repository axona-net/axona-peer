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
const CACHE_MAX       = 1024;            // messages cached per relay
const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
const MAX_DIRECT      = 20;              // direct subscribers before a relay delegates
const DELEGATE_BATCH  = 8;               // subscribers handed off when promoting a child
const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side, Phase 2+)
const TTL_MS          = 24 * 60 * 60 * 1000;   // 24h message hold, keyed on the ROOT timestamp
const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
const PENDING_PUB_TTL_MS = 30_000;       // retain a recent publish this long so it can be re-sent toward the true root once a background lookup resolves (covers a stranded one-shot publish; idempotent — root dedups by msgId)
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
  KILL:     'pubsub:kill',      // retract a message (thin; TODO Phase 4)
  UNPUB:    'pubsub:unpub',     // RESERVED — removed v4.3.0 (no handler/sender); wire string kept so legacy frames are ignored, not misrouted
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
  ROOTBEACON: 'pubsub:rootbeacon', // soft-state root advertisement to the topic's neighborhood
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
    tombstones: new Map(),           // msgId -> expireTs (kill; thin)
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

    // Public/inspectable state (contract surface).
    this.axonRoles       = new Map();   // topicIdBig -> Role  (topics I host: root or relay)
    this.mySubscriptions = new Map();   // topicIdBig -> { since, lastRenewSent }
    this._hostedTopics   = new Set();   // topicIdBig hosted without app consumption
    this._lastSeenTsByTopic = new Map();// topicIdBig -> ts  (AxonaPeer seeds `since` here)

    // Internal.
    this._upstream        = new Map();  // topicIdBig -> [hex]  the relay we renew toward
    this._rootHint        = new Map();  // topicIdBig -> { via:hex|null, at }  cached iterative-lookup root
    this._rootBeacons     = new Map();  // topicIdBig -> { root:hex, at, exp }  inbound root advert (soft state)
    this._beaconSeen      = new Map();  // beaconId -> exp  (flood dedup)
    this._lastBeaconAt    = 0;
    this._beaconSeq       = 0;
    this._appDelivered    = new Map();  // "topicHex:msgId" -> true (exactly-once LRU)
    this._deliveryCallback = null;
    this._hostKeyspace    = false;
    this._pending         = new Map();  // pull corrId -> { resolve, timer }
    this._pullSeq         = 0;
    this._timer           = null;

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

    // STAMP — single serialization point; strictly monotonic, floored at now.
    const ts = Math.max(role.lastTs + 1, this._now());
    role.lastTs = ts;
    const msg = { json, publishTs: ts, msgId: env.msgId };
    this._cachePush(role, { msgId: env.msgId, publishTs: ts, json });
    this._fanout(role, msg, null);                                       // to subscribers
    this._deliverToApp(role.topicId, json, env.msgId, ts);              // local app (if subscribed)
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
                           .map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId }));
    if (msgs.length && isHexId(lc(payload.parentId))) {
      this._route(idBig(payload.parentId), T.REPLAYUP, { topicId: idHex(role.topicId), msgs });
    }
    return 'consumed';
  }

  // Stamped history arriving from below — adopt it WITHOUT re-stamping (the
  // timestamp rule, §5: a timestamp already present is kept), advance lastTs so
  // new publishes continue monotonically above it, and propagate it down.
  async _onReplayUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (!role) return 'consumed';
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
    this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json });
    if (m.publishTs > role.lastTs) role.lastTs = m.publishTs;            // continue stamping above recovered history
    this._fanout(role, { json: m.json, publishTs: m.publishTs, msgId: m.msgId }, null);
    this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs);
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
      if (m.del) { this._applyDelete(role, topicBig, m); continue; }
      if (role && !role.cacheIds.has(m.msgId)) {       // relay: cache once + re-fan once
        this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json });
        this._fanout(role, m, lc(payload.from));       // exclude the sender
      }
      this._deliverToApp(topicBig, m.json, m.msgId, m.publishTs);
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
      if (isSelf) for (const m of batch) this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs);
      else this._route(subBig, T.DELIVER,
        { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: batch });
      batch = []; bytes = 0;
    };
    for (const c of role.cache) {
      if (c.publishTs <= sinceTs) continue;
      if (bytes + c.bytes > REPLAY_CHUNK_BYTES) flush();
      batch.push({ json: c.json, publishTs: c.publishTs, msgId: c.msgId });
      inBatch.add(c.msgId);
      bytes += c.bytes;
    }
    if (latest && role.cache.length) {               // ensure the current value rides along
      const newest = role.cache[role.cache.length - 1];
      if (!inBatch.has(newest.msgId)) {
        if (bytes + newest.bytes > REPLAY_CHUNK_BYTES) flush();
        batch.push({ json: newest.json, publishTs: newest.publishTs, msgId: newest.msgId });
      }
    }
    flush();
    if (ping && !sent && !isSelf) {                    // repin even with no history
      this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [] });
    }
  }

  _replayLocal(role, sinceTs, latest = false) {
    const seen = new Set();
    for (const c of role.cache) if (c.publishTs > sinceTs) { this._deliverToApp(role.topicId, c.json, c.msgId, c.publishTs); seen.add(c.msgId); }
    if (latest && role.cache.length) {               // since:'latest' — newest regardless of age
      const newest = role.cache[role.cache.length - 1];
      if (!seen.has(newest.msgId)) this._deliverToApp(role.topicId, newest.json, newest.msgId, newest.publishTs);
    }
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
  _deliverToApp(topicBig, json, msgId, publishTs) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const key = topicBig.toString(16) + ':' + msgId;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (publishTs > prev) this._lastSeenTsByTopic.set(topicBig, publishTs);
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, json, msgId, publishTs); }
      catch (e) { this._log('warn', 'delivery-callback-threw', { err: e?.message }); }
    }
  }

  _applyDelete(role, topicBig, m) {
    if (role && !role.tombstones.has(m.msgId)) {       // relay: drop from cache + re-fan delete once
      role.tombstones.set(m.msgId, this._now() + TTL_MS);
      const i = role.cache.findIndex(c => c.msgId === m.msgId);
      if (i >= 0) { role.cacheBytes -= role.cache[i].bytes; role.cache.splice(i, 1); }
      role.cacheIds.delete(m.msgId);
      this._fanout(role, m, null);
    }
    if (this.mySubscriptions.has(topicBig) && this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, JSON.stringify({ deleted: true, msgId: m.msgId, topic: m.topic ?? null }), m.msgId, m.publishTs ?? this._now()); }
      catch (e) { this._log('warn', 'delete-callback-threw', { err: e?.message }); }
    }
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
    const fresh = cached && (this._now() - cached.at) < this.renewMs;
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
            // Heal (publish): a recent publish may have stranded on the greedy
            // walk → re-send it toward the now-known root, once. Idempotent
            // (root dedups by msgId); then drop it so we don't re-send forever.
            const pend = this._pendingPub && this._pendingPub.get(topicBig);
            if (hex && pend && (this._now() - pend.at) < PENDING_PUB_TTL_MS) {
              this._send(T.PUB, { topicId: idHex(topicBig), via: [hex], json: pend.json });
              this._pendingPub.delete(topicBig);
            }
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
    const basin = neigh.slice().sort((a, b) => this._cmpXor(a, b, this.nodeId)).slice(0, BEACON_FANOUT);
    const payload = {
      root: lc(idHex(this.nodeId)),
      topics: rooted.map(idHex),
      beaconId: `${idHex(this.nodeId).slice(0, 10)}-${this._now()}-${this._beaconSeq++}`,
      layer: BEACON_LAYERS,
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
      for (const nb of neigh.sort((a, b) => this._cmpXor(a, b, this.nodeId)).slice(0, BEACON_FANOUT)) {
        this._route(nb, T.ROOTBEACON, fwd);
      }
    }
  }

  // Nearest node to `tBig` among what I know LOCALLY: my neighbors, myself, and
  // any cached beacon root. Never triggers a network lookup (keeps the verify
  // step cheap and non-amplifying).
  _bestKnownClosest(tBig) {
    let best = this.nodeId, bestD = this.nodeId ^ tBig;
    if (typeof this.dht.neighbors === 'function') {
      for (const n of (this.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        const d = nb ^ tBig; if (d < bestD) { bestD = d; best = nb; }
      }
    }
    const b = this._rootBeacons.get(tBig);
    if (b) { try { const rb = idBig(b.root); const d = rb ^ tBig; if (d < bestD) { bestD = d; best = rb; } } catch { /* */ } }
    return best;
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
    this._pendingPub.set(topicId, { json, at: this._now() });
    this._send(T.PUB, { topicId: idHex(topicId), via: hint ? [hint] : [], json });
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
    // Participate so the node won't be torn down and can root the topic if
    // closest. TODO(Phase 4): proper host-as-durable-relay semantics.
    this._send(T.SUB, { topicId: idHex(topicId), via: [], subscriberId: idHex(this.nodeId), since: this._now() });
  }
  pubsubUnhost(topicId) {
    this._hostedTopics.delete(topicId);
    const role = this.axonRoles.get(topicId);
    if (role) { const me = lc(idHex(this.nodeId)); role.subscribers.delete(me); role.children.delete(me); }
  }
  pubsubHostKeyspace(on = true) { this._hostKeyspace = !!on; }

  pubsubKill(topicId, kill)   { this._send(T.KILL,  { topicId: idHex(topicId), via: [], kill }); }
  // pubsubUnpub() — REMOVED v4.3.0 (decision 2026-06-25: keep kill, drop unpub)
  pubsubTouch(topicId, touch) { this._send(T.TOUCH, { topicId: idHex(topicId), via: [], touch }); }

  requestPull(topicId, postHash = null, { timeoutMs = 1000 } = {}) {
    const corrId = idHex(this.nodeId).slice(0, 8) + ':' + (++this._pullSeq);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this._pending.delete(corrId); resolve(null); }, timeoutMs);
      if (typeof timer.unref === 'function') timer.unref();
      this._pending.set(corrId, { resolve, timer });
      this._send(T.PULL, { topicId: idHex(topicId), via: [], corrId, postHash: postHash || null, requesterId: idHex(this.nodeId) });
    });
  }
  requestMetrics() { return Promise.resolve({ accumulated: [] }); }   // TODO(Phase 4)

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
  _onKill(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.KILL, payload); return 'consumed'; }
    const topicBig = idBig(payload.topicId);
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig);
    const msgId = payload.kill?.msgId;
    if (msgId) this._applyDelete(role, topicBig, { del: true, msgId, topic: payload.kill?.topic ?? null, publishTs: this._now() });
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
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.PULL, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    let hit = null;
    if (role) hit = payload.postHash ? role.cache.find(c => c.msgId === payload.postHash) : role.cache[role.cache.length - 1];
    const reqBig = idBig(payload.requesterId);
    const resp = { corrId: payload.corrId, json: hit ? hit.json : null, publishTs: hit ? hit.publishTs : null, requesterId: payload.requesterId };
    if (reqBig === this.nodeId) this._onPullResp(resp, { targetId: this.nodeId });
    else this._route(reqBig, T.PULLRESP, resp);
    return 'consumed';
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
  stop() { if (this._timer) { clearInterval(this._timer); this._timer = null; } }

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
        const iv = s.interval || this.renewFastMs;
        if (now - s.lastRenewSent < iv) continue;
        s.lastRenewSent = now;
        s.interval = Math.min(this.renewMs, Math.round(iv * RENEW_BACKOFF));   // back off toward ceiling while stable
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

    // 2. Evict stale subscribers; expire cache + tombstones; tear down a role
    //    that is empty and not locally needed.
    for (const [t, role] of this.axonRoles) {
      for (const [subHex, sub] of role.subscribers) {
        if (now - sub.lastRenewed > this.dropMs) { role.subscribers.delete(subHex); role.children.delete(subHex); }
      }
      for (const [msgId, exp] of role.tombstones) if (exp <= now) role.tombstones.delete(msgId);
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
      if (role.subscribers.size === 0 && !holdsHistory && !keyspacePinned && !this.mySubscriptions.has(t) && !this._hostedTopics.has(t)) {
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
