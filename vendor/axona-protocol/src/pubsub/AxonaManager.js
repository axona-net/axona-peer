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
const RENEW_MS        = 60_000;          // re-subscribe cadence
const DROP_MS         = 180_000;         // evict a subscriber after 3 missed renewals
const CACHE_MAX       = 1024;            // messages cached per relay
const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
const MAX_DIRECT      = 20;              // direct subscribers before a relay delegates
const DELEGATE_BATCH  = 8;               // subscribers handed off when promoting a child
const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side, Phase 2+)
const TTL_MS          = 48 * 60 * 60 * 1000;   // 48h message hold, keyed on the ROOT timestamp
const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
const REPLAY_CHUNK_BYTES = 96 * 1024;    // byte budget per replay deliver batch
const FUTURE_TOLERANCE_MS = 5 * 60 * 1000;   // §5 bad-clock rule: drop replayed stamps this far ahead

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
  UNPUB:    'pubsub:unpub',     // retract a topic's feed (thin; TODO Phase 4)
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
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
    renewMs = RENEW_MS,
    dropMs = DROP_MS,
    refreshIntervalMs = 10_000,
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

    this.renewMs   = renewMs;
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
    on(T.UNPUB,    this._onUnpub);
    on(T.TOUCH,    this._onTouch);
    on(T.PULL,     this._onPull);
    on(T.PULLRESP, this._onPullResp);
  }

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
    if (viaEmpty && meta.isTerminal && !role.isRoot) { role.isRoot = true; this._upstream.delete(role.topicId); }
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
      if (this.mySubscriptions.has(topicBig)) this._replayLocal(role, since);
      return 'consumed';
    }
    // Durability (§6 stamped-replay-up): if this subscriber holds newer stamped
    // history than I do — e.g. I am a fresh root after the old one died, or a
    // displaced root reattaching — ask it to replay its cache UP to me.
    const myHw = this._highWater(role);
    if (Number.isFinite(payload.hw) && payload.hw > myHw) {
      this._route(idBig(subHex), T.PULLUP, { topicId: idHex(topicBig), sinceHw: myHw, parentId: idHex(this.nodeId) });
    }
    this._accept(role, subHex, since);
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
  _accept(role, subHex, since) {
    const existing = role.subscribers.get(subHex);
    if (existing) {                                   // renewal of a current subscriber
      existing.lastRenewed = this._now();
      this._replayTo(role, subHex, since, false);
      return;
    }
    if (role.subscribers.size >= this.maxDirect) {    // overloaded → delegate
      // WIDEN before DEEPEN: promote a new sibling child (offloading a batch)
      // so the tree grows bushy (depth ~log_MAX_DIRECT(S)), not into a chain.
      // Only when every direct is already a child do we deepen — forward the
      // newcomer down to the child XOR-closest to it.
      if (!this._promoteChild(role)) {
        const c = this._pickChild(role, subHex);
        if (c) { this._delegateTo(c, role, [{ subscriberId: subHex, since }]); return; }
        // neither possible (no leaf to promote, no child) → seat over capacity
      }
    }
    role.subscribers.set(subHex, { since: Number.isFinite(since) ? since : 0, lastRenewed: this._now() });
    this._replayTo(role, subHex, since, true);        // delta + a via-repin ping
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
    if (payload.from) this._upstream.set(topicBig, [lc(payload.from)]);  // pin to our relay

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
  _replayTo(role, subHex, sinceTs, ping) {
    const subBig = idBig(subHex);
    const isSelf = subBig === this.nodeId;
    let batch = [], bytes = 0, sent = false;
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
      bytes += c.bytes;
    }
    flush();
    if (ping && !sent && !isSelf) {                    // repin even with no history
      this._route(subBig, T.DELIVER, { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [] });
    }
  }

  _replayLocal(role, sinceTs) {
    for (const c of role.cache) if (c.publishTs > sinceTs) this._deliverToApp(role.topicId, c.json, c.msgId, c.publishTs);
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
    return role;
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

  // Find the topic's true root via the robust ITERATIVE lookup (escapes the
  // greedy-routing local minima that strand subscribers on a sparse mesh), and
  // cache it for renewMs so steady-state stays cheap. Returns a hex node id, or
  // null (no lookup adapter, or lookup failed) → caller falls back to greedy
  // routing toward the bare topic id.
  async _findRoot(topicBig) {
    if (typeof this.dht.lookup !== 'function') return null;
    const cached = this._rootHint.get(topicBig);
    if (cached && (this._now() - cached.at) < this.renewMs) return cached.via;
    let hex = null;
    try { const id = await this.dht.lookup(topicBig); if (id != null) hex = lc(idHex(idBig(id))); }
    catch { /* lookup failed → greedy fallback */ }
    this._rootHint.set(topicBig, { via: hex, at: this._now() });
    return hex;
  }

  // Lookup-assisted when UNPINNED (initial join, or healing a strand): find the
  // true root and route the subscribe toward it. Once pinned to a relay (via the
  // deliver `from`), renewals use the cheap via-pin and never lookup again.
  // Sync entry. Pinned (steady state) or no lookup adapter → send synchronously
  // (unchanged greedy behavior, keeps tests/mock adapters deterministic). Only
  // when UNPINNED *and* lookup is available do we go async to find the true root.
  _sendSubscribe(topicBig) {
    const pinned = this._upstream.get(topicBig) || [];
    if (pinned.length || typeof this.dht.lookup !== 'function') {
      this._emitSubscribe(topicBig, pinned.slice(0, MAX_VIA));
      return;
    }
    this._findRoot(topicBig).then(root => this._emitSubscribe(topicBig, root ? [root] : [])).catch(() => {});
  }
  _emitSubscribe(topicBig, via) {
    const role = this.axonRoles.get(topicBig);
    this._send(T.SUB, {
      topicId: idHex(topicBig), via, subscriberId: idHex(this.nodeId),
      since: this._sinceFor(topicBig),
      hw: role ? this._highWater(role) : 0,   // a cache-bearing relay advertises its history (§6)
    });
  }

  // ── public API (contract surface) ────────────────────────────────────
  // Route the UN-stamped publish toward the topic's TRUE root (iterative lookup,
  // cached) so publisher + subscribers converge on the same root; root stamps it.
  pubsubPublish(topicId, json, meta = {}) {
    if (typeof this.dht.lookup === 'function') {
      // lookup-assisted: find the TRUE root so publisher + subscribers converge
      // on the same root (async).
      this._findRoot(topicId)
        .then(root => this._send(T.PUB, { topicId: idHex(topicId), via: root ? [root] : [], json }))
        .catch(() => {});
    } else {
      this._send(T.PUB, { topicId: idHex(topicId), via: [], json });   // sync greedy (no lookup adapter)
    }
    return meta.postHash || '';
  }

  pubsubSubscribe(topicId) {
    const seeded = this._lastSeenTsByTopic.get(topicId);
    const since  = Number.isFinite(seeded) ? seeded : this._now();
    this.mySubscriptions.set(topicId, { since, lastRenewSent: this._now() });
    this._sendSubscribe(topicId);
  }

  pubsubUnsubscribe(topicId) {
    this.mySubscriptions.delete(topicId);
    const via = this._upstream.get(topicId) || [];
    this._send(T.UNSUB, { topicId: idHex(topicId), via, subscriberId: idHex(this.nodeId) });
    this.pubsubResetTopicConsumption(topicId);
  }

  pubsubResetTopicConsumption(topicId) {
    this._lastSeenTsByTopic.delete(topicId);
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
  pubsubUnpub(topicId, unpub) { this._send(T.UNPUB, { topicId: idHex(topicId), via: [], unpub }); }
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
  _onUnpub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reroute') { this._reroute(T.UNPUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) { role.cache = []; role.cacheIds.clear(); role.cacheBytes = 0; }
    return 'consumed';
  }
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
      if (s) { if (now - s.lastRenewSent < this.renewMs) continue; s.lastRenewSent = now; }
      this._sendSubscribe(t);
    }
    for (const t of this._hostedTopics) {
      this._send(T.SUB, { topicId: idHex(t), via: [], subscriberId: idHex(this.nodeId), since: now });
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
      // 48h hold + late-join replay. The cache itself ages out via _expireCache
      // (TTL), so the role naturally tears down once its history fully expires. A
      // non-root child relay with no subscribers carries only redundant cache (the
      // root has it) so it may tear down immediately.
      const holdsHistory = role.isRoot && role.cache.length > 0;
      if (role.subscribers.size === 0 && !holdsHistory && !this.mySubscriptions.has(t) && !this._hostedTopics.has(t)) {
        this.axonRoles.delete(t);
        this._upstream.delete(t);
      }
    }
  }

  _log(level, event, ctx) {
    if (this._logSink) { try { this._logSink(level, 'pubsub:' + event, ctx); } catch { /* sink threw */ } }
  }
}

export default AxonaManager;
