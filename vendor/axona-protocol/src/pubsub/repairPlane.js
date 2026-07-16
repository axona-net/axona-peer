// repairPlane.js — the REPAIR plane (refactor Phase 2).
//
// The kernel's one periodic scheduler (refreshTick) and every repair loop it
// drives: adaptive renewal + re-home, backup renewal, cohort replication,
// the bounded publish/kill retry (observation-confirmed, I-9), metrics
// leases, role sweep, beacon/verify cadence — plus the departure paths
// (peer-died sweep, graceful-leave handoff) and lifecycle (start/stop).
// Methods are mixed into AxonaManager.prototype; state lives on the façade.

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
  EMPTY_ROOT_PROBE_DELAY_MS, EMPTY_ROOT_PROBE_MAX, EMPTY_ROOT_PROBE_INTERVAL_MS,
  EMPTY_ROOT_PROBE_FANOUT, HANDOFF_ACK_MS, HANDOFF_TRIES,
  ROOT_REPLICATE_FULL_MS,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';

export const repairPlaneMethods = {
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
          if (now - this._unattachedSince.get(t) >= ROOT_CLAIM_MS && this._regionOk(t) && this._rootClaim.selfClosestReachable(t)) {
            this._rootClaim.claimReachable(t);
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
    // 1b-rep. Singleton-root replication (warm backup roots) — push each root's full
    //         cache to its ROOT_REPLICAS nearest neighbours so a successor is always
    //         warm.
    this._replicateRoots();
    this._emptyRootProbeSweep(now);   // v4.24.0: empty self-roots re-pull the cohort (bounded)
    // 1b-bak. Backups are subscribing CHILD RELAYS. Renew each backup's subscribe
    //         every tick so root election runs through the SAME probe-protected path
    //         as any subscriber/host (_rootHint_'s iterative lookup → one globally-
    //         closest terminus), instead of the old bespoke local-only _selfClosest
    //         promotion that split when two backups couldn't see each other. While the
    //         root lives the SUB routes to it (we sit as a warm child); when it churns
    //         the closest backup self-roots via the _onSub terminal and the rest
    //         re-home under it — a single root, gap-free from the prefetched cache.
    for (const t of this._backupTopics) {
      const role = this.axonRoles.get(t);
      if (!role) { this._backupTopics.delete(t); continue; }
      if (role.isRoot) continue;                       // won the election — a root doesn't subscribe to itself
      // Cleanup ONLY when we're a redundant spare — never when we might need to promote:
      // we've re-homed as a child under a LIVE root (upstream set to a reachable node
      // that isn't us) and the root stopped replicating to us for a while. A backup
      // whose root vanished and hasn't re-homed stays subscribed so it can win the
      // election (that path must never be pruned, or a split-brain topic gets NO root).
      const up = this._upstream.get(t);
      const rehomed = Array.isArray(up) && up.length > 0 && up[0] !== lc(idHex(this.nodeId)) && this._isReachableId(up[0]);
      if (rehomed && role.subscribers.size === 0 && (now - (role.lastReplicaAt || 0)) > BACKUP_EVICT_MS) {
        this._backupTopics.delete(t); role.backupOf = null; continue;
      }
      this._sendSubscribe(t);
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

    // 1d. Metrics (demand-driven, ANY root). (a) Renew our own metrics requests
    //     toward the root so its lease stays alive; (b) if WE are a root with a
    //     fresh lease, publish a snapshot to metricTopic(T) each METRICS_PUB_MS via
    //     the peer's publisher hook; (c) expire stale path flags.
    for (const [t, r] of this.myMetricsRequests) {
      if (now - (r.lastSent || 0) >= METRICS_PUB_MS) { r.lastSent = now; this._sendMetricsOn(t); }
    }
    if (this._metricsPublisher) {
      for (const [t, role] of this.axonRoles) {
        if (role.metricsOn <= now) continue;
        this._publishMetricSnapshot(t, role, now);
      }
    }
    for (const [t, exp] of this._metricsWanted) if (exp <= now) this._metricsWanted.delete(t);

    // 2. Evict stale subscribers; expire cache + tombstones; tear down a role
    //    that is empty and not locally needed.
    for (const [t, role] of this.axonRoles) {
      for (const [subHex, sub] of role.subscribers) {
        if (now - sub.lastRenewed > this.dropMs) { role.subscribers.delete(subHex); role.children.delete(subHex); role.pulledLw?.delete(subHex); }
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
      // A root with a fresh metrics lease keeps publishing snapshots, so retain it
      // even with zero subscribers/cache — the lease self-expires (soft state), and
      // the role then tears down on a later tick like any other.
      const metricsLeased = role.isRoot && role.metricsOn > now;
      if (role.subscribers.size === 0 && !holdsHistory && !keyspacePinned && !role.backupOf && !this._backupTopics.has(t) && !metricsLeased && !this.mySubscriptions.has(t) && !this._hostedTopics.has(t)) {
        this.axonRoles.delete(t);
        this._upstream.delete(t);
      }
    }

    // 3. Root beacons — advertise where each topic I root lives, to my XOR-closest
    //    neighbors (last-mile convergence aid). Throttled to BEACON_MS; expire the
    //    inbound pointer + flood-dedup caches by their TTLs.
    if (now - this._lastBeaconAt >= BEACON_MS) { this._lastBeaconAt = now; this._emitRootBeacons(); }
    this._verifyRoots(now);   // root self-verification (non-blocking lookups; batched)
    for (const [t, b] of this._rootBeacons) if (b.exp <= now) this._rootBeacons.delete(t);
    for (const [id, exp] of this._beaconSeen) if (exp <= now) this._beaconSeen.delete(id);
  },

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
  },

  // Empty-self-root re-probe sweep (v4.24.0): a root STILL empty at renewal
  // time re-pulls the cohort (a holder may have joined/recovered since the
  // birth probe), bounded by EMPTY_ROOT_PROBE_MAX and rate-limited. Its own
  // sweep — deliberately NOT inside _replicateRoots, which no-ops when
  // replication is disabled (rootReplicas: 0) and would silently disable the
  // pull with it. Fire-and-forget — never await an iterative lookup inside
  // the tick (the 4.18.1 lesson).
  _emptyRootProbeSweep(now) {
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot || role.cache.length) continue;
      if (role.probeTries >= EMPTY_ROOT_PROBE_MAX) continue;
      if (now - role.probeAt < EMPTY_ROOT_PROBE_INTERVAL_MS) continue;
      this._emptyRootProbe(t).catch(() => {});
    }
  },

  // ── empty-self-root cohort pull (v4.24.0 — the alert-bot read-miss fix) ──
  // Field-captured mechanism: a cold subscriber's SUB terminates at itself →
  // it becomes the topic's root with an EMPTY cache while a live holder
  // (ex-root / backup / host) still has the history. Nothing tells the holder
  // about the new closer root, so the empty state is STICKY (82% of Howard's
  // misses; unrecovered at 600s). The new root PULLS instead of waiting to be
  // found: PULLUP(sinceHw:0) to the K-closest cohort plus the nodes on its own
  // iterative lookup path (the runner-up closest is usually the prior root).
  // Holders answer via the existing REPLAYUP → verified union-ingest (B-4
  // re-verify, msgId dedup, tombstone suppression), so the pull is idempotent
  // and needs no new wire verb. Quenches: probe only while empty, at most
  // EMPTY_ROOT_PROBE_MAX times.
  _scheduleEmptyRootProbe(topicBig) {
    const h = setTimeout(() => {
      this._burstTimers.delete(h);
      this._emptyRootProbe(topicBig).catch(() => {});
    }, EMPTY_ROOT_PROBE_DELAY_MS);
    if (typeof h.unref === 'function') h.unref();
    this._burstTimers.add(h);
  },

  async _emptyRootProbe(topicBig) {
    const pre = this.axonRoles.get(topicBig);
    if (!pre || !pre.isRoot || pre.cache.length) return;      // filled meanwhile / demoted / gone
    if (pre.probeTries >= EMPTY_ROOT_PROBE_MAX) return;
    pre.probeTries++; pre.probeAt = this._now();
    // Candidates — two complementary views, both excluding self:
    //  · local findKClosest: cheap, but a cold node's thin table may know nobody
    //  · iterative lookup PATH: the traversal's hops; its tail is the closest
    //    node the network routed through before us = the likely prior holder
    const cand = new Set();
    if (typeof this.dht.findKClosest === 'function') {
      try {
        for (const id of (await this.dht.findKClosest(topicBig, EMPTY_ROOT_PROBE_FANOUT + 1)) || []) {
          let b; try { b = idBig(id); } catch { continue; }
          if (b !== this.nodeId) cand.add(b);
        }
      } catch { /* thin table — the lookup path below still applies */ }
    }
    if (typeof this.dht.lookup === 'function') {
      try {
        const r = await this.dht.lookup(topicBig);
        for (const id of (r && Array.isArray(r.path) ? r.path : [])) {
          let b; try { b = idBig(id); } catch { continue; }
          if (b !== this.nodeId) cand.add(b);
        }
      } catch { /* best-effort */ }
    }
    // Re-check after the awaits: a REPLAYUP/HANDOFF may have landed meanwhile.
    const role = this.axonRoles.get(topicBig);
    if (!role || !role.isRoot || role.cache.length || !cand.size) return;
    let n = 0;
    for (const b of cand) {
      if (n >= EMPTY_ROOT_PROBE_FANOUT) break;
      try {
        this._route(b, T.PULLUP, { topicId: idHex(topicBig), sinceHw: 0, parentId: idHex(this.nodeId) });
        n++;
      } catch { /* best-effort */ }
    }
    if (n) this._log('info', 'empty-root-probe',
      { topic: idHex(topicBig).slice(0, 12), fanout: n, tries: role.probeTries });
  },

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
    // Delta gate (v4.24.1, #333): push the FULL state only when it changed, a
    // new cohort member needs seeding, or the anti-entropy backstop elapsed —
    // otherwise this tick's push is an empty KEEPALIVE (refreshes the backup's
    // lastReplicaAt; empty ingest is a no-op). The per-tick full-cache re-send
    // was the bandwidth fuel of the #332 role-bloat collapse. The signature is
    // cheap and captures every convergence-relevant change (count, high-water,
    // tombstones); any union-ingest bumps it and re-arms one full push.
    const sig = `${role.cache.length}:${this._highWater(role)}:${role.tombstones.size}`;
    const full = sig !== role.replSig
      || want.some((hex) => !role.replicas.has(hex))
      || (now - (role.replLastFull || 0)) >= ROOT_REPLICATE_FULL_MS;
    const msgs = full ? role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq })) : [];
    const dels = full ? this._activeDels(role) : [];
    for (const hex of want) {
      try { this._route(idBig(hex), T.REPLICATE, { topicId: idHex(t), from: idHex(this.nodeId), msgs, dels }); } catch { /* best-effort */ }
      role.replicas.set(hex, { at: now });
    }
    if (full) { role.replSig = sig; role.replLastFull = now; }
  },

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
  },

  // Re-send the SAME pending envelope a few times over the first ~second. Each tick
  // re-resolves the root hint (the background lookup that nudges integration) and
  // stops early the moment the publish is confirmed/expired (_pendingPub no longer
  // holds this msgId). Idempotent end-to-end: the root dedups by msgId.
  _coldPublishBurst(topicBig, msgId) {
    const total = COLD_BURST_TRIES + COLD_BURST_SLOW_TRIES;
    let i = 0;
    const schedule = () => {
      // Two phases: a fast wave (COLD_BURST_TRIES × 200ms ≈ 1s) front-loads re-sends
      // while the table warms, then a slower wave (COLD_BURST_SLOW_TRIES × 400ms ≈ 2s
      // more) keeps re-shooting at the true root as integration continues past the
      // first second. Idempotent throughout (the root dedups by msgId).
      const delay = (i < COLD_BURST_TRIES) ? COLD_BURST_INTERVAL_MS : COLD_BURST_SLOW_INTERVAL_MS;
      const h = setTimeout(() => {
        this._burstTimers.delete(h);
        const p = this._pendingPub?.get(msgId);
        if (!p) return;                                 // confirmed or aged out → done
        const hint = this._rootHint_(topicBig);
        this._send(T.PUB, { topicId: idHex(topicBig), via: hint ? [hint] : [], json: p.json });
        if (++i < total) schedule();
      }, delay);
      if (typeof h.unref === 'function') h.unref();
      this._burstTimers.add(h);
    };
    schedule();
  },

  // "Cold" = this node hasn't accreted enough neighbours to route reliably to an
  // arbitrary topic root yet (a freshly-joined node). Cheap, and self-clearing:
  // once the synaptome fills past the threshold, publishes go back to a single send.
  _isColdPublisher() {
    if (typeof this.dht.neighbors !== 'function') return false;
    let n = 0; try { n = (this.dht.neighbors() || []).length; } catch { /* */ }
    return n < COLD_PEER_THRESHOLD;
  },

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
  },

  // A peer died (channel closed / evicted) or announced its departure: every
  // root beacon naming it is now a ghost. Purge them so the defer gates
  // (SUB/PUB/promotion) stop steering topics at a corpse — otherwise, until
  // the 50s TTL, stranded traffic keeps deferring to a node that can never
  // serve, and promotions stay suppressed.
  //
  // The dead peer can't be anyone's UPSTREAM either. A pin on a corpse is not
  // a blackhole — the next renewal routed toward it is popped at the live
  // terminal ('reroute') and re-seats at the true root, which re-pins us via
  // the deliver `from` — but while pinned, `attached` stays true, so an app
  // subscriber's adaptive renewal can sit at the backed-off ceiling (up to
  // RENEW_MS = 60s of staleness) before that healing renewal fires, and the
  // reachable-root fallback stays gated off. Drop the pin NOW and reset the
  // renewal clock so the very next tick re-homes unpinned (external review
  // finding, validated 2026-07-13).
  pubsubPeerDied(deadHex) {
    if (typeof deadHex !== 'string') return;
    const dead = lc(deadHex);
    for (const [t, b] of this._rootBeacons) {
      if (b?.root === dead) this._rootBeacons.delete(t);
    }
    for (const [t, up] of this._upstream) {
      if (Array.isArray(up) && up[0] === dead) {
        this._upstream.delete(t);
        const s = this.mySubscriptions.get(t);
        if (s) { s.interval = this.renewFastMs; s.lastRenewSent = 0; }
      }
    }
  },

  // Called from AxonaPeer.leave() while the transport is still up: for every
  // topic we ROOT and hold cache for, push the cache to the heir (next-closest
  // live node) so the history isn't lost when we go. Best-effort; never throws.
  async pubsubLeaveHandoff() {
    if (typeof this.dht.findKClosest !== 'function') return;
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    this._handoffAcked = new Set();

    // Phase A — resolve heirs, PARALLEL with bounded concurrency. This used to
    // be one topic at a time — one iterative network lookup each — so a burst
    // publisher that had rooted a few dozen fresh topics (field case: an alert
    // bot left holding 25 roots) could not finish inside leave()'s time bound,
    // and every topic past the cutoff died with the departing node. Eight
    // lookups in flight turns 25 sequential round-trips into ~3 rounds.
    const jobs = [];
    for (const [t, role] of this.axonRoles) {
      if (!role.isRoot || !role.cache.length) continue;
      jobs.push({ t, role, heir: null, alt: null, key: lc(idHex(t)) });
    }
    let i = 0;
    const resolver = async () => {
      while (i < jobs.length) {
        const job = jobs[i++];
        try {
          const arr = await this.dht.findKClosest(job.t, 4);
          for (const id of (Array.isArray(arr) ? arr : [])) {
            const b = idBig(id);
            if (b === this.nodeId) continue;
            if (job.heir === null) job.heir = b;              // closest node that isn't us
            else if (b !== job.heir) { job.alt = b; break; }  // runner-up — the Phase C fallback target
          }
        } catch { /* fall through to the iterative probe */ }
        // findKClosest is LOCAL-only; a leaver with a thin table (fresh burst
        // publisher) can see nobody but itself even though the network is
        // populated. Mirror _rootHint_'s self-closest escape: probe with the
        // ITERATIVE lookup before giving up on the topic's history.
        if (job.heir === null && typeof this.dht.lookup === 'function') {
          try {
            const r = await this.dht.lookup(job.t);
            const id = (r && Array.isArray(r.path) && r.path.length) ? r.path[r.path.length - 1] : null;
            if (id != null) { const b = idBig(id); if (b !== this.nodeId) job.heir = b; }
          } catch { /* no heir resolvable → the cohort spray below still fires */ }
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(8, jobs.length) }, resolver));

    // Phase B — CONFIRMED handoff (v4.24.0), batch-phased. The old single
    // fire-and-forget _route silently transferred nothing whenever delivery
    // failed or no heir was resolvable (diagnosis: gone=40/40 at leaveMs≈11ms —
    // a confirmation failure, not a window failure). Send a whole ROUND to
    // every unacked heir, then wait ONE shared ack window (early-exit as acks
    // land), then retry the stragglers — total added latency is bounded by
    // ~HANDOFF_TRIES×HANDOFF_ACK_MS regardless of topic count, preserving the
    // parallelism that phase A bought.
    //
    // `from` names the DEPARTING root so the heir can (a) purge our stale root
    // beacon and (b) never defer its new claim back to us — without it, the
    // heir adopted the history and then immediately demoted toward our
    // still-fresh beacon, undoing the handoff.
    const sendable = jobs.filter(j => j.heir !== null);
    const unacked = () => sendable.filter(j => !this._handoffAcked.has(j.key));
    for (let round = 0; round < HANDOFF_TRIES && unacked().length; round++) {
      for (const j of unacked()) {
        const msgs = j.role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq }));
        const dels = this._activeDels(j.role);
        try { this._route(j.heir, T.HANDOFF, { topicId: idHex(j.t), msgs, dels, from: idHex(this.nodeId) }); } catch { /* best-effort */ }
      }
      const deadline = Date.now() + HANDOFF_ACK_MS;
      while (Date.now() < deadline && unacked().length) await sleep(25);
    }

    // Phase C — durability fallback (v4.24.1, #333). A departing node must
    // NEVER send REPLICATE: 4.24.0 sprayed every unacked topic's cache to the
    // K-closest cohort here, and each recipient became a BACKUP of a root that
    // was — by definition — about to be gone. Those orphan backups re-subscribe
    // toward the topic every tick; under load their SUBs strand into duplicate
    // sub-terminal roots, every duplicate replicates its cache to ITS cohort,
    // and the soak collapsed the backbone twice on exactly this loop (roles
    // >2000 → ingest storms → heartbeat evictions → mesh death). And because
    // the ack window is a race against load, "unacked" usually meant "acked
    // late" — the spray fired for topics whose heir already held the history.
    //
    // The fallback is now a single extra HANDOFF to the runner-up candidate:
    // the recipient adopts through the normal heir path (proper holder, purges
    // our beacon, never defers back to us) and the worst case plants TWO
    // holders that reconcile via union-ingest — the 4.22.1 footprint plus one
    // alternative, instead of an unbounded backup cascade. Heirless AND
    // alt-less topics get the honest warn (nothing routable exists to hold the
    // history — same terminal case as 4.22.1).
    const leftovers = jobs.filter(j => !this._handoffAcked.has(j.key));
    for (const j of leftovers) {
      const target = (j.alt !== null && j.alt !== j.heir) ? j.alt : j.heir;
      this._log('warn', 'handoff-unacked',
        { topic: idHex(j.t).slice(0, 12), heir: j.heir === null ? 'none' : idHex(j.heir).slice(0, 10),
          fallback: target === null ? 'none' : idHex(target).slice(0, 10) });
      if (target === null) continue;
      const msgs = j.role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq }));
      const dels = this._activeDels(j.role);
      try { this._route(target, T.HANDOFF, { topicId: idHex(j.t), msgs, dels, from: idHex(this.nodeId) }); } catch { /* best-effort */ }
    }
  },

  // ── lifecycle: renewal + eviction + TTL sweep ────────────────────────
  start() {
    if (this._timer) return;
    this._timer = setInterval(() => { this.refreshTick().catch(() => {}); }, this.refreshIntervalMs);
    if (typeof this._timer.unref === 'function') this._timer.unref();
  },

  stop() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    for (const h of this._burstTimers) clearTimeout(h);
    this._burstTimers.clear();
  },

};

export default repairPlaneMethods;
