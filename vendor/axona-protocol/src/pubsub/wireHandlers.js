// wireHandlers.js — the WIRE plane (refactor Phase 2).
//
// Every routed-message handler and the axon-tree mechanics they drive:
// the topic-decision (handle/reroute/forward/reject), seating + delegation
// (widen-before-deepen), root ingress (verify → policy → stamp → fan out),
// stamped replay-up/handoff/replicate ingest, delivery + re-fan, replay,
// kills, pulls, and demand-driven metrics. Methods are mixed into
// AxonaManager.prototype; root-claim decisions delegate to rootClaim.js.

import {
  T, RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS, ROOT_CLAIM_MS,
  ROOT_REPLICAS, BACKUP_EVICT_MS, CACHE_MAX, CACHE_BYTES, MAX_DIRECT,
  DELEGATE_BATCH, MAX_VIA, VIA_HOP_BUDGET, TTL_MS, APP_DEDUP_MAX,
  PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES, COLD_BURST_TRIES,
  COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES, COLD_BURST_SLOW_INTERVAL_MS,
  COLD_PEER_THRESHOLD, FIRST_PUBLISH_RESEND_MS, REPLAY_CHUNK_BYTES,
  FUTURE_TOLERANCE_MS, BEACON_MS, BEACON_TTL_MS, BEACON_SEEN_MS,
  ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH, METRICS_LEASE_MS,
  METRICS_PUB_MS, METRICS_COALESCE_MS,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { verifyEnvelope, checkFreshness } from './envelope.js';
import { verifyKill } from './kill.js';
import { deriveTopicIdBig } from './post.js';
import { makeRole } from './rootClaim.js';

export const wireHandlersMethods = {
  _registerHandlers() {
    const on = (type, fn) => this.dht.onRoutedMessage(type, (p, m) => fn.call(this, p, m));
    on(T.SUB,      this._onSub);
    on(T.UNSUB,    this._onUnsub);
    on(T.PUB,      this._onPub);
    on(T.DELIVER,  this._onDeliver);
    on(T.ADOPT,    this._onAdopt);
    on(T.PULLUP,   this._onPullUp);
    on(T.HANDOFFACK, this._onHandoffAck);
    on(T.REPLAYUP, this._onReplayUp);
    on(T.HANDOFF,  this._onHandoff);
    on(T.REPLICATE, this._onReplicate);
    on(T.KILL,     this._onKill);
    on(T.TOUCH,    this._onTouch);   // no-op (peer.touch deprecated v4.3.0); kept for wire compat
    on(T.PULL,     this._onPull);
    on(T.PULLRESP, this._onPullResp);
    on(T.ROOTBEACON, this._onRootBeacon);
    on(T.METRICSON, this._onMetricsOn);
  },

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
    // Bare topic id → only the terminus handles. REGION RULE: a topic may only be
    // rooted by a node IN ITS REGION. If I'm the routing terminus but out-of-region,
    // the topic's region has no node — REFUSE rather than root it here (which would
    // pull a foreign region's traffic into mine and hotspot my region). The handlers
    // treat 'reject' as "drop, don't seat/store/root."
    if (!meta.isTerminal) return 'forward';
    return this._regionOk(idBig(payload.topicId)) ? 'handle' : 'reject';
  },

  // ── SUBSCRIBE ────────────────────────────────────────────────────────
  _onSub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.SUB, payload); return 'consumed'; }

    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction (SUB). A stranded subscribe must not
    // (re)root a near-miss node while a strictly-closer live NEIGHBOUR root is
    // beaconing — defer the seat to that root. Without this only PUB carried the
    // correction, and every stranded/renewing SUB re-rooted the just-demoted
    // relay → the ~20s root flap between same-region relays (split trees, 0%
    // fresh-subscriber delivery on prod). A node that already relays the topic
    // (non-root role with a live upstream) still seats subscribers below.
    if (!this.axonRoles.has(topicBig)) {
      const closer = this._liveCloserRoot(topicBig);
      if (closer) { this._deferToRoot(topicBig, T.SUB, payload, closer); return 'consumed'; }
      // Alone-in-the-dark guard (v4.19.2). A freshly-joined node subscribes
      // before its mesh has formed: with zero non-bridge neighbours its SUB
      // never leaves the node, terminates at self, and (no beacons heard yet)
      // it minted itself "root" — observed live as EVERY joining subscriber
      // creating a transient root for the topic, splitting the tree until
      // reconciliation caught up (which under churn load it often didn't
      // inside the delivery window). A node that can't reach anyone has no
      // business electing itself: hold the seat; mySubscriptions is already
      // set, so the renewFastMs renewal re-runs the election once meshed.
      // Publish-side is deliberately NOT gated — a genuinely solo node still
      // roots on its own publish and serves its local subscriber.
      if (idBig(lc(payload.subscriberId)) === this.nodeId && this._rootClaim.meshBare()) return 'consumed';
    }
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'sub-terminal');
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
    // Durability (§6 stamped-replay-up): if this subscriber holds stamped
    // history I lack, ask it to replay its cache UP to me. Two detectable shapes:
    //  · NEWER (hw above mine) — I am a fresh root after the old one died, or a
    //    displaced root reattaching → pull the delta above my hw.
    //  · OLDER (lw below mine) — the post-transition split: a demoted ex-root /
    //    heir re-homing under me holds the PRE-transition half, which sits
    //    entirely below my hw and is invisible to the hw rule (the cold-attach
    //    "exactly half the timeline" replay class) → pull its FULL range
    //    (sinceHw:0; ingest dedups by msgId, tombstones suppress killed bodies).
    //    ONE-SHOT per (child, lw) — when the union succeeds my lw drops to the
    //    child's and the condition quenches naturally, but when the pull is
    //    REFUSED (the child's oldest is tombstoned here, or beyond my retention
    //    window) the condition would hold forever and re-fire a full-cache
    //    replay-up on EVERY renewal (the 4.22.0 testnet relay storm: flapping
    //    roots, zero-delivery runs, leave() drains pinned at timeout). Remember
    //    the lw already pulled per child; re-arm only if its lw DECREASES
    //    (a deeper split — genuinely new history).
    const myHw = this._highWater(role);
    if (Number.isFinite(payload.hw) && payload.hw > myHw) {
      this._route(idBig(subHex), T.PULLUP, { topicId: idHex(topicBig), sinceHw: myHw, parentId: idHex(this.nodeId) });
    } else if (Number.isFinite(payload.lw) && payload.lw > 0 && role.cache.length && payload.lw < this._lowWater(role)) {
      const prev = role.sync.pulledLw.get(subHex);
      if (prev === undefined || payload.lw < prev) {
        role.sync.pulledLw.set(subHex, payload.lw);
        this._route(idBig(subHex), T.PULLUP, { topicId: idHex(topicBig), sinceHw: 0, parentId: idHex(this.nodeId) });
      }
    }
    this._accept(role, subHex, since, !!payload.latest);
    return 'consumed';
  },

  _onUnsub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.UNSUB, payload); return 'consumed'; }
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (role) { const s = lc(payload.subscriberId); role.subscribers.delete(s); role.children.delete(s); role.sync.pulledLw.delete(s); }
    return 'consumed';
  },

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
  },

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
  },

  // Promote one leaf subscriber to a child relay and hand it a batch of OTHER
  // leaves. Only succeeds if it can actually free a slot — promoting the sole
  // remaining leaf would just re-label it a child and free nothing, so we need
  // ≥2 leaves. Returning false tells _accept to deepen (delegate to a child)
  // instead of seating the newcomer over capacity.
  _promoteChild(role) {
    // REGION RULE: the tree's relay infrastructure must be IN-REGION. Only promote
    // an in-region leaf to a child relay; foreign leaves stay as direct leaves of
    // the root (they still receive, they just never relay for a region not theirs).
    const leaves = [];
    for (const s of role.subscribers.keys()) {
      if (role.children.has(s)) continue;
      if (!this._regionOk(idBig(s))) continue;   // out-of-region subscriber: never a relay child (when region lock on)
      leaves.push(s);
    }
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
  },

  _delegateTo(childHex, role, subs) {
    this._route(idBig(childHex), T.ADOPT, {
      topicId: idHex(role.topicId), parent: idHex(this.nodeId), subs,
    });
  },

  // A node is told to become a child relay and adopt a set of subscribers.
  _onAdopt(payload, meta) {
    if (meta.targetId !== this.nodeId) return;        // routed to me specifically
    const topicBig = idBig(payload.topicId);
    // REGION RULE: never become a child relay for a topic outside my region (the
    // tree infrastructure is region-homogeneous). A correct parent won't delegate
    // here, but refuse defensively so a stale/foreign ADOPT can't spill the tree.
    if (!this._regionOk(topicBig)) return 'consumed';
    const role = this._rootClaim.adoptChild(topicBig, lc(payload.parent));
    for (const s of (Array.isArray(payload.subs) ? payload.subs : [])) {
      const sh = lc(s.subscriberId);
      if (isHexId(sh) && idBig(sh) !== this.nodeId) this._accept(role, sh, s.since);
    }
    // Attach UP toward the parent so we receive the live feed + cache replay.
    this._sendSubscribe(topicBig);
    return 'consumed';
  },

  // ── PUBLISH ──────────────────────────────────────────────────────────
  async _onPub(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
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
      const closer = this._liveCloserRoot(topicBig, { requireReachable: false });
      if (closer) { this._deferToRoot(topicBig, T.PUB, payload, closer); return 'consumed'; }
    }
    let role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'pub-terminal');
    this._maybePromoteRoot(role, payload, meta);
    // Only the root (the topic terminus) stamps. A non-root relay can only reach
    // here for a via-routed publish (a security waypoint) — pop the via and
    // continue toward the topic id. Bare-topic publishes always promote above.
    if (!role.isRoot) { this._reroute(T.PUB, payload); return 'consumed'; }

    await this._ingestPublish(role, payload.json);
    return 'consumed';
  },

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
  },

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
  },

  // Stamped history arriving from below — adopt it WITHOUT re-stamping (the
  // timestamp rule, §5: a timestamp already present is kept), advance lastTs so
  // new publishes continue monotonically above it, and propagate it down.
  // Queued (task #332, I-11): the routing verdict ('consumed') is returned
  // immediately; the verify-heavy body drains through the time-sliced ingest
  // pump so a storm of pushes can't starve mesh liveness.
  async _onReplayUp(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    await this._ingestEnqueue(() => this._processReplayUp(payload));   // inline path completes here; queued path returns at once
    return 'consumed';
  },

  async _processReplayUp(payload) {
    const topicBig = idBig(payload.topicId);
    const role = this.axonRoles.get(topicBig);
    if (!role) return;
    this._applyDels(role, topicBig, payload.dels);   // tombstones FIRST → suppress any killed body in this batch
    await this._ingestStampedBatch(role, payload.msgs);
  },

  // Bulk-ingest with a macrotask yield (v4.24.1, #333/#332): verifying hundreds
  // of signatures in a microtask-chained await loop starves the macrotask queue —
  // heartbeats/pings go unanswered and the node's mesh peers evict it mid-ingest
  // (the join-storm collapse trigger: bulk role adoption → mass eviction →
  // state=stale). Yielding every 16 messages lets liveness traffic interleave
  // with history adoption; correctness is untouched (ingest is idempotent and
  // order-independent under msgId dedup).
  async _ingestStampedBatch(role, msgs) {
    let n = 0;
    for (const m of (Array.isArray(msgs) ? msgs : [])) {
      if (m && typeof m.json === 'string' && Number.isFinite(m.publishTs)) await this._ingestStamped(role, m);
      if ((++n & 15) === 0) {
        await new Promise(r => (typeof setImmediate === 'function' ? setImmediate(r) : setTimeout(r, 0)));
      }
    }
  },

  async _ingestStamped(role, m) {
    let env;
    try { env = JSON.parse(m.json); } catch { return; }
    const v = await verifyEnvelope(env);                                 // B-4 still applies
    if (!v.ok || env.msgId !== m.msgId) { this._log('warn', 'drop-bad-replayup', { reason: v.reason }); return; }
    if (m.publishTs > this._now() + FUTURE_TOLERANCE_MS) { this._log('warn', 'drop-future-replayup'); return; } // §5 bad-clock
    if (role.cacheIds.has(m.msgId)) return;                              // already have it
    if (this._tombstoned(role, m.msgId, m.json)) return;                 // killed → don't resurrect via replay-up
    this._cachePush(role, { msgId: m.msgId, publishTs: m.publishTs, json: m.json, seq: m.seq });
    // Seeing our own msgId arrive via ANY stamped path (cohort replicate,
    // replay-up, handoff) is proof it landed on a durable holder — confirm the
    // pending so the retry machinery (and leave()'s evidence-based drain)
    // doesn't keep waiting on a publish that already made it. A true ack is
    // impossible by design (a PUB carries no return address — publisher
    // location privacy), so echoes are the only confirmation signal.
    this._confirmPending(role.topicId, m.msgId);
    if (m.publishTs > role.lastTs) role.lastTs = m.publishTs;            // continue stamping above recovered history
    if (Number.isFinite(m.seq) && m.seq > role.seq) role.seq = m.seq;   // recover dense counter → a new root continues it
    this._fanout(role, { json: m.json, publishTs: m.publishTs, msgId: m.msgId, seq: m.seq }, null);
    this._deliverToApp(role.topicId, m.json, m.msgId, m.publishTs, m.seq);
  },

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
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'handoff-heir');
    this._applyDels(role, topicBig, payload.dels);   // inherit the heir's tombstones, not just its bodies
    await this._ingestStampedBatch(role, payload.msgs);
    // Departure rules — purge the leaver's ghost beacon, never defer back to
    // the leaver, yield only to a strictly-closer live root — live in the
    // state machine (rootClaim.handoffArrived).
    const leaver = typeof payload.from === 'string' ? lc(payload.from) : null;
    this._rootClaim.handoffArrived(topicBig, leaver);
    // Confirm receipt (v4.24.0): the leaver retries / cohort-sprays topics it
    // never hears an ack for — the old fire-and-forget silently dropped the
    // topic's last copy whenever this HANDOFF didn't land.
    if (leaver && isHexId(leaver)) {
      try { this._route(idBig(leaver), T.HANDOFFACK, { topicId: payload.topicId }); } catch { /* best-effort */ }
    }
    return 'consumed';
  },

  // Leaver side of the confirmed handoff: mark the topic acked so the
  // pubsubLeaveHandoff retry loop stops and skips the cohort-spray fallback.
  _onHandoffAck(payload, meta) {
    if (meta.targetId !== this.nodeId) return;
    if (typeof payload.topicId === 'string') this._handoffAcked?.add(lc(payload.topicId));
    return 'consumed';
  },

  // Receive a replica push: become (or refresh) a BACKUP for this topic — hold the
  // full cache + tombstones (don't-trust: each message re-verified via stamped-ingest)
  // AND join _backupTopics so refreshTick subscribes us toward the topic like an
  // ordinary child relay. While the root lives our SUB routes to it and we sit as a
  // warm child; when it churns the normal probe-protected subscribe machinery elects
  // ONE new root (the closest self-roots, we re-home under it) — no bespoke local-only
  // promotion to split. The prefetched cache makes whichever backup wins take over
  // gap-free (it already holds the history a since:'all' joiner will ask for).
  // Queued (task #332, I-11): a joining relay receives its whole region's role
  // mass as a burst of REPLICATEs; processed inline they starve the event loop
  // (missed keepalives → mesh eviction → the join-storm collapse). The handler
  // returns the routing verdict immediately and the body drains through the
  // time-sliced pump. Role state is read at PROCESSING time, not arrival time.
  async _onReplicate(payload, meta) {
    if (meta.targetId !== this.nodeId) return;          // routed to me as the backup
    await this._ingestEnqueue(() => this._processReplicate(payload, meta));   // inline path completes here; queued path returns at once
    return 'consumed';
  },

  async _processReplicate(payload, meta) {
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return; }
    const mine = this.axonRoles.get(topicBig);
    if (mine?.isRoot) {
      // UNION-INGEST at a root — the cohort anti-entropy contract _replicateRole
      // documents ("co-hosting roots converge to the union of cache+tombstones").
      // This used to be dropped, so two roots straddling a transition never
      // merged and the pushing ex-root's half stayed stranded (a fresh
      // since:'all' subscriber on the surviving root replayed exactly half the
      // timeline). No backup bookkeeping here — we keep our claim; every body
      // re-verifies through _ingestStamped (dedup, tombstone-suppress,
      // fanout + app delivery heal attached subscribers in place).
      this._applyDels(mine, topicBig, payload.dels);
      await this._ingestStampedBatch(mine, payload.msgs);
      return;
    }
    if (!this._rootReplicas) return;                    // backup duty disabled on this node
    let from = null;
    if (payload.from && isHexId(lc(payload.from))) from = lc(payload.from);
    else if (meta.fromId != null) { try { from = lc(idHex(idBig(meta.fromId))); } catch { /* */ } }
    let role = this.axonRoles.get(topicBig);
    if (!role) { role = makeRole(topicBig, false); this.axonRoles.set(topicBig, role); }
    this._rootClaim.becomeBackup(topicBig, role, from);   // nature transition (subscribing child relay; single-root election)
    this._applyDels(role, topicBig, payload.dels);   // tombstones FIRST → a killed body in the same push is suppressed
    await this._ingestStampedBatch(role, payload.msgs);
  },

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
  },

  // Fan a stamped message to every subscriber (optionally excluding the sender).
  _fanout(role, msg, excludeHex) {
    const base = { topicId: idHex(role.topicId), from: idHex(this.nodeId), msgs: [msg] };
    for (const subHex of role.subscribers.keys()) {
      if (excludeHex && subHex === excludeHex) continue;
      this._route(idBig(subHex), T.DELIVER, { ...base });
    }
  },

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
  },

  _replayLocal(role, sinceTs, latest = false) {
    const seen = new Set();
    for (const c of role.cache) if (c.publishTs > sinceTs) { this._deliverToApp(role.topicId, c.json, c.msgId, c.publishTs, c.seq); seen.add(c.msgId); }
    if (latest && role.cache.length) {               // since:'latest' — newest regardless of age
      const newest = role.cache[role.cache.length - 1];
      if (!seen.has(newest.msgId)) this._deliverToApp(role.topicId, newest.json, newest.msgId, newest.publishTs, newest.seq);
    }
    // kills replay alongside the cache (exactly-once on the kill-specific key).
    for (const [tgt, tomb] of role.tombstones) if ((tomb?.exp ?? 0) > this._now()) this._deliverKillToApp(role.topicId, tgt, tomb.killTs, tomb.seq);
  },

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
  },

  // ── side-function handlers (thin; TODO Phase 4) ──────────────────────
  async _onKill(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.KILL, payload); return 'consumed'; }
    const topicBig = idBig(payload.topicId);
    // Root-beacon last-mile correction (KILL) — same one-shot semantics as PUB:
    // a kill landing on a near-miss node must reach the true root, not mint a
    // competing root that the rest of the tree never consults.
    if (!this.axonRoles.has(topicBig)) {
      const closer = this._liveCloserRoot(topicBig, { requireReachable: false });
      if (closer) { this._deferToRoot(topicBig, T.KILL, payload, closer); return 'consumed'; }
    }
    const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'kill-terminal');
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
  },

  // _onUnpub — REMOVED v4.3.0 (keep kill, drop unpub)
  _onTouch(payload, meta) {
    const d = this._topicDecision(payload, meta);
    if (d === 'forward') return;
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.TOUCH, payload); return 'consumed'; }
    return 'consumed';
  },

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
    if (d === 'reject') return 'consumed';   // out-of-region terminus: topic's region has no node → don't root/seat/store here
    if (d === 'reroute') { this._reroute(T.PULL, payload); return 'consumed'; }
    // Terminus (root/closest) fall-through: answer if we hold it, else a genuine null.
    const hit = role ? (payload.postHash ? role.cache.find(c => c.msgId === payload.postHash) : role.cache[role.cache.length - 1]) : null;
    this._answerPull(payload, hit || null);
    return 'consumed';
  },

  _answerPull(payload, hit) {
    const resp = { corrId: payload.corrId, json: hit ? hit.json : null, publishTs: hit ? hit.publishTs : null, requesterId: payload.requesterId };
    if (idBig(payload.requesterId) === this.nodeId) this._onPullResp(resp, { targetId: this.nodeId });
    else this._route(idBig(payload.requesterId), T.PULLRESP, resp);
  },

  _onPullResp(payload, meta) {
    if (meta.targetId !== this.nodeId && idBig(payload.requesterId) !== this.nodeId) return;
    const p = this._pending.get(payload.corrId);
    if (!p) return 'consumed';
    clearTimeout(p.timer);
    this._pending.delete(payload.corrId);
    let parsed = null;
    if (payload.json) { try { parsed = JSON.parse(payload.json); } catch { parsed = null; } }
    // Resolve the FULL envelope (msgId/ts/signer/message …) — the same shape a
    // sub() callback delivers, and what peer.pull has always documented. The
    // previous `parsed.message ?? parsed` unwrap discarded the identity at the
    // last step (task #355): publish-confirm loops comparing env.msgId could
    // never succeed, and pull-then-act (kill/reply/verify by msgId) was
    // impossible even though the wire carried everything.
    p.resolve(parsed ?? null);
    return 'consumed';
  },

  // METRICSON routes toward the topic like a SUB. At the root (terminal / via
  // waypoint holding the role) it arms a renewable publish lease. On the path it
  // caches the flag so an inheriting root picks it up on promotion and a second
  // requester's METRICSON short-circuits (fan-in dedup), while still re-forwarding
  // periodically so the root's lease stays alive.
  _onMetricsOn(payload, meta) {
    const topicBig = idBig(payload.topicId);
    const now = this._now();
    const d = this._topicDecision(payload, meta);
    if (d === 'reroute') { this._reroute(T.METRICSON, payload); return 'consumed'; }
    if (d === 'reject') return 'consumed';   // out-of-region terminus: no in-region root to arm a metrics lease on
    if (d === 'handle') {
      // Root-beacon last-mile correction (METRICSON): a metrics lease must arm
      // the TRUE root, not mint a competing one at a near-miss node.
      if (!this.axonRoles.has(topicBig)) {
        const closer = this._liveCloserRoot(topicBig);
        if (closer) { this._deferToRoot(topicBig, T.METRICSON, payload, closer); return 'consumed'; }
      }
      const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'metricson-terminal');
      this._maybePromoteRoot(role, payload, meta);
      role.metricsOn = now + METRICS_LEASE_MS;                 // arm/renew the publish lease
      // Answer the demand NOW (v4.16.1): the first snapshot rides back at routing
      // latency instead of waiting for the next 5 s refresh tick, so a subscriber
      // that just turned metrics on hears a count roughly when its data-topic
      // replay lands — the difference between "0 alerts" and "cache is near its
      // rollover cap" must not wait on a poll cycle. The METRICS_PUB_MS throttle
      // inside the helper keeps renewals from turning this into a per-METRICSON
      // publish storm; the net cadence stays ~20 s.
      this._publishMetricSnapshot(topicBig, role, now);
      return 'consumed';
    }
    // forward: remember the flag (short-circuit + inheritance) and coalesce upstream.
    const hadFresh = (this._metricsWanted.get(topicBig) || 0) > now;
    this._metricsWanted.set(topicBig, now + METRICS_LEASE_MS);
    const lastFwd = this._metricsFwdAt.get(topicBig) || 0;
    if (hadFresh && now - lastFwd < METRICS_COALESCE_MS) return 'consumed';  // root already informed recently → drop
    this._metricsFwdAt.set(topicBig, now);
    return;   // falsy → kernel forwards toward the root
  },

};

export default wireHandlersMethods;
