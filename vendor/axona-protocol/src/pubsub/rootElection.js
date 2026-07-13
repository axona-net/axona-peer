// rootElection.js — the ROOT ELECTION plane (refactor Phase 2).
//
// "Where is the root, and who is alive?" — every where-is signal in one
// file: root beacons (emit, verify-don't-trust receive, immediate announce),
// the non-blocking root-hint resolver with its iterative-lookup escape,
// root self-verification, and channel liveness. The isRoot transitions these
// signals feed live in rootClaim.js (the state machine); the manager façade
// holds the state maps. Methods are mixed into AxonaManager.prototype.

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
import { extractS2Prefix } from '../utils/hexid.js';
import { isRegionLockEnforced } from './constants.js';

export const rootElectionMethods = {
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
  },

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
      // closer root exists, yield NOW — rootClaim.demote demotes, re-homes, and
      // sends the confirming subscribe (the new root must ADOPT us or our
      // subtree starves) — so I stop claiming the topic and stop emitting
      // poisoning "root=me" beacons.
      if ((rootBig ^ tBig) < (this.nodeId ^ tBig)) {
        this._rootClaim.demote(tBig, lc(payload.root), 'beacon-closer');
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
  },

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
  },

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
  },

  // Is `hex` a currently-reachable node (a direct neighbour in the synaptome)?
  // Used to distinguish a departed root (drop from the mesh → fast promotion) from
  // a live-but-quiet one (kept as a passive backup for the full stale window).
  // Conservative: with no neighbour introspection available, treat as reachable
  // (fall back to the slow silence timer rather than risk splitting a live root).
  _isReachableId(hex) {
    if (typeof this.dht.neighbors !== 'function') return true;
    let want; try { want = idBig(hex); } catch { return true; }
    for (const n of (this.dht.neighbors() || [])) {
      let nb; try { nb = idBig(n); } catch { continue; }
      if (nb === want) return true;
    }
    return false;
  },

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
          .then(async id => {
            // findKClosest is LOCAL-ONLY (never probes the network). Local knowledge
            // is region-bounded: a node in region A holds few/no synapses into
            // region B, and the bridge — the universal connector — is skipped as a
            // hop. So a foreign-region subscriber's local closest is often itself,
            // and it wrongly SELF-ROOTS the topic → a second root disjoint from the
            // publisher's → 0% cross-region delivery. Region is a PLACEMENT HINT,
            // not a routing wall: before self-rooting, probe the network with the
            // ITERATIVE lookup (which hops through the meshed relays and DOES cross
            // regions) to find the TRUE globally-closest reachable node. Only pay
            // this when about to self-root (rare — the real root, or a stranded
            // foreign node); the fast local path is unchanged for everyone else.
            const selfHex = lc(idHex(this.nodeId));
            const localHex = (id != null) ? lc(idHex(idBig(id))) : null;
            if ((localHex === null || localHex === selfHex) && typeof this.dht.lookup === 'function') {
              try {
                const r = await this.dht.lookup(topicBig);
                if (r && Array.isArray(r.path) && r.path.length) id = r.path[r.path.length - 1];
              } catch { /* keep the local result — greedy stays in effect */ }
            }
            // Self-closest → leave the hint null so we route greedily toward the
            // bare topic id and become root as the terminus (don't via-pin to self).
            let hex = null;
            if (id != null) {
              const h = lc(idHex(idBig(id)));
              if (h !== selfHex) hex = h;
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
  },

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
  },

  // Root self-verification (see the ROOT_VERIFY_* constants). Launch up to
  // BATCH iterative lookups per tick, NON-BLOCKING, for roots due a check. A
  // strictly-closer live node ⇒ this claim is spurious (or was overtaken):
  // seed a VERIFIED root pointer (honored by the promotion gates even where no
  // beacon reaches), demote to a subscribing child, and re-home — the SUB
  // carries our cache high-water so the true root PULLUPs anything only we
  // hold, and our seated subscribers keep receiving through us as a relay.
  _verifyRoots(now) {
    if (typeof this.dht.lookup !== 'function') return;
    if (!this._verifyInflight) this._verifyInflight = new Set();
    let launched = 0;
    for (const [t, role] of this.axonRoles) {
      if (launched >= ROOT_VERIFY_BATCH) break;
      if (!role.isRoot || this._verifyInflight.has(t)) continue;
      const due = role.lastVerify
        ? (now - role.lastVerify >= ROOT_VERIFY_MS)
        : (now - (role.formedAt || 0) >= ROOT_VERIFY_FIRST_MS);
      if (!due) continue;
      role.lastVerify = now; launched++;
      this._verifyInflight.add(t);
      Promise.resolve()
        .then(() => this.dht.lookup(t))
        .then((r) => {
          this._verifyInflight.delete(t);
          const id = (r && Array.isArray(r.path) && r.path.length) ? r.path[r.path.length - 1] : null;
          if (id == null) return;
          let cBig; try { cBig = idBig(id); } catch { return; }
          if (cBig === this.nodeId) return;                       // confirmed: I am the terminus
          if ((cBig ^ t) >= (this.nodeId ^ t)) return;            // not strictly closer → keep the claim
          if (isRegionLockEnforced() && extractS2Prefix(cBig) !== extractS2Prefix(t)) return;
          const live = this.axonRoles.get(t);
          if (!live || !live.isRoot) return;                      // already demoted meanwhile
          const hex = lc(idHex(cBig));
          this._rootBeacons.set(t, { root: hex, at: this._now(), exp: this._now() + 2 * ROOT_VERIFY_MS, verified: true });
          this._rootClaim.demote(t, hex, 'verify-closer');
          this._log('info', 'root-verify-demote', { topic: idHex(t).slice(0, 12), to: hex.slice(0, 10) });
        })
        .catch(() => this._verifyInflight.delete(t));
    }
  },

};

export default rootElectionMethods;
