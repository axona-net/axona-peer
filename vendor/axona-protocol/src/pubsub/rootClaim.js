// =====================================================================
// rootClaim.js — the root-claim state machine (refactor Phase 1).
//
// EVERY flip of `role.isRoot` in the kernel goes through this module: one
// transition function (`_set`), one decision table of guards, one structured
// `root-transition` log line per flip. Before this existed, root-ness was
// decided at ~8 independent sites in AxonaManager and every incident added a
// guard at another site (the patch-interaction bug class —
// axona-docs/architecture/Kernel-Refactor-Analysis-v0.1.md §2A). The rules
// themselves are unchanged; they now live in one place.
//
// The machine holds NO state of its own. Role objects stay in
// manager.axonRoles; beacons/upstream/hints stay on the manager (they move in
// Phase 2). What lives here is the AUTHORITY: who may claim, when a claim
// defers, and the enforced invariants (INVARIANTS.md):
//   I-1  a topic has exactly one root; wrong claims converge without flapping
//   I-2  never defer to a farther node, a ghost, or the node handing off
//
// Transitions (why-codes appear in the root-transition log):
//   become(t, why)            — create the role AS root (routing-terminal fallback)
//   promote(role, p, meta)    — terminal promotion of an existing non-root role
//   demote(t, toHex, why)     — yield the claim to a strictly-closer live root,
//                               pin upstream to it, and re-subscribe under it
//   claimReachable(t)         — reachable-root fallback (unconfirmed deferral
//                               window expired; self is closest REACHABLE)
//   adoptChild(t, parentHex)  — ADOPT: become a non-root child relay
//   handoffArrived(t, leaver) — departing root handed us its history: purge the
//                               leaver's ghost beacon, never defer back to it
// =====================================================================

import { asId } from '../utils/hexid.js';

const idHex = (big) => big.toString(16).padStart(66, '0');
const idBig = asId;
const lc    = (s) => String(s ?? '').toLowerCase();

// ── Role NATURES (v4.26.0, Phase 7 of the v0.2 program) ────────────────────
// A role acts in exactly one PRIMARY nature — ROOT, BACKUP, or CHILD — plus an
// orthogonal HOLDER flag (hosted / app-subscribed retention). The natures are
// DERIVED from the ground facts (isRoot, backupOf), never stored separately:
// a stored copy would drift, and drift here is the #333 bug class (a BACKUP
// whose principal was dead — a state nobody had modeled — self-perpetuated
// into the backbone collapse). Each nature carries obligations and a named
// eviction path; the table is normative in Axona-Architecture §VIII and
// enforced by smoke_role_natures.mjs.
//
//   ROOT    stamps, beacons, verifies, replicates, serves    → demote / idle sweep
//   BACKUP  holds warm copy; subscribes as election standby  → principal gone + re-homed + BACKUP_EVICT_MS
//   CHILD   renews upstream, re-fans down once               → subscriber loss + idle sweep
//   +HOLDER renews; advertises hw                            → unhost / unsub / TTL
export const NATURES = Object.freeze(['root', 'backup', 'child']);

export function roleNature(role) {
  if (role.isRoot) return 'root';
  if (role.backupOf !== null) return 'backup';
  return 'child';
}

/** A relay's per-topic state (root or non-root child relay). */
export function makeRole(topicId, isRoot) {
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
    // The per-topic CONVERGENCE LEDGER (v4.25.0, Phase 6): every guard the data
    // plane keeps about "what have I already exchanged, with whom" lives here —
    // one place, engine semantics (per-pair memory + quench), the seed of the
    // Phase-8 sync engine. Fields were previously scattered as five role-level
    // properties; the 4.22.0 lw-pull storm lived in exactly this kind of guard.
    sync: {
      sig: '',                       // (when ROOT) state signature at the last FULL replica push (4.24.1 delta gate)
      lastFullAt: 0,                 // (when ROOT) _now() of the last FULL push (backstop re-arms at ROOT_REPLICATE_FULL_MS)
      probeTries: 0,                 // empty-self-root cohort pulls fired (4.24.0; quenches at EMPTY_ROOT_PROBE_MAX)
      probeAt: 0,                    // _now() of the last cohort pull (rate-limits the refreshTick re-probe)
      pulledLw: new Map(),           // subHex -> lowest lw already PULLUP'd from that child (4.22.1:
                                     // a refused pull — e.g. the child's oldest is tombstoned here —
                                     // must not re-fire on every renewal; re-arm only if lw DECREASES)
    },
    replicas: new Map(),             // (when ROOT) backupHex -> { at }  nodes holding a warm copy of our cache
    backupOf: null,                  // (when BACKUP) hex of the root replicating to us; null if we're not a backup
    lastReplicaAt: 0,                // (when BACKUP) _now() of the last replica push from our root (staleness → presume root gone)
    metricsOn: 0,                    // (when ROOT) lease expiry ts; while > now, this root publishes snapshots to metricTopic(T)
    metricsLastPub: 0,               // _now() of the last metric snapshot we published (throttle to METRICS_PUB_MS)
  };
}

export class RootClaim {
  /** @param mgr the AxonaManager (state owner); beaconMs = BEACON_MS (corpse-freshness cut) */
  constructor(mgr, { beaconMs = 20_000 } = {}) {
    this.m = mgr;
    this._beaconMs = beaconMs;
  }

  // ────────────────────────── the decision table (guards) ──────────────────────────

  // A live root beacon naming a STRICTLY closer node than self means this node
  // must not (re)take the topic's root — stranded terminal traffic defers to that
  // root instead. This breaks the flap loop observed on the prod relay backbone
  // (4.19.0). `requireReachable` (the default) additionally demands the beaconed
  // root be a DIRECT, authenticated neighbour — when a root dies its channel
  // drops and the gate opens instantly, so backup promotion on churn is never
  // stalled by a stale beacon. The PUB/KILL last-mile correction uses the looser
  // TTL+freshness semantics (requireReachable:false): a publish is one
  // self-healing message, and that looser gate is what fixed cold-publish
  // discovery. Liveness evidence, strongest first: `verified` (network-confirmed
  // by an iterative lookup, 4.19.1), channel-verified neighbour, or a beacon
  // heard within 1.5×BEACON_MS (a DEAD root's beacon goes silent — without this
  // freshness cut a stranded publish ping-pongs toward the corpse until the full
  // TTL; the pre-4.19 latent loop, smoke_root_reconcile phase 4).
  liveCloserRoot(topicBig, { requireReachable = true } = {}) {
    const m = this.m;
    const b = m._rootBeacons.get(topicBig);
    if (!b || m._now() >= b.exp) return null;
    if (b.root === lc(idHex(m.nodeId))) return null;
    let rb; try { rb = idBig(b.root); } catch { return null; }
    if ((rb ^ topicBig) >= (m.nodeId ^ topicBig)) return null;   // never defer to a farther node (I-2)
    if (b.verified) return b.root;             // network-confirmed via iterative lookup
    if (m._isReachableId(b.root)) return b.root;                 // channel-verified live neighbour
    if (!requireReachable && (m._now() - b.at) < this._beaconMs * 1.5) return b.root;
    return null;
  }

  // True iff this node has NO non-bridge neighbours — it can't route to anyone,
  // so any "terminal at self" verdict is an artifact of isolation, not of
  // closeness (the alone-in-the-dark guard, 4.19.2). When the transport doesn't
  // expose neighbours, assume meshed (never block).
  meshBare() {
    const m = this.m;
    if (typeof m.dht.neighbors !== 'function') return false;
    let bridge = null;
    try { const b = (typeof m.dht.bridgeId === 'function') ? m.dht.bridgeId() : null; bridge = (b != null) ? idBig(b) : null; } catch { /* */ }
    for (const n of (m.dht.neighbors() || [])) {
      let nb; try { nb = idBig(n); } catch { continue; }
      if (bridge === null || nb !== bridge) return false;   // any routable non-bridge neighbour → meshed
    }
    return true;
  }

  // True iff `self` is XOR-closest to `tBig` among the nodes we can ACTUALLY
  // reach (self + direct neighbours), excluding the bridge (signaling infra,
  // never a topic root). The iterative hint may name a node closer in XOR, but
  // if it never adopts us it is effectively unreachable; when self beats every
  // reachable neighbour, self is the best reachable root. Pure local read.
  selfClosestReachable(tBig) {
    const m = this.m;
    const bridge = (typeof m.dht.bridgeId === 'function') ? m.dht.bridgeId() : null;
    const bestD = m.nodeId ^ tBig;
    if (typeof m.dht.neighbors === 'function') {
      for (const n of (m.dht.neighbors() || [])) {
        let nb; try { nb = idBig(n); } catch { continue; }
        if (nb === m.nodeId || (bridge != null && nb === bridge)) continue;
        if ((nb ^ tBig) < bestD) return false;     // a reachable neighbour is closer → route to it
      }
    }
    return true;
  }

  // ────────────────────────── the transition function ──────────────────────────

  // The ONLY place role.isRoot changes. One structured log per flip.
  _set(role, isRoot, why, ctx = {}) {
    if (role.isRoot === isRoot) return;
    // Nature hygiene (v4.26.0, Phase 7): a role flipping to ROOT sheds any
    // BACKUP residue in the same transition. Before this, promote() left
    // backupOf + the _backupTopics membership in place forever — a ROOT
    // wearing BACKUP state, exactly the unmodeled-residue class the natures
    // exist to make impossible. (If the root later demotes it re-enters
    // BACKUP only through a fresh REPLICATE from the new live principal.)
    if (isRoot && role.backupOf !== null) this.retireBackup(role.topicId, role, 'promoted');
    role.isRoot = isRoot;
    this.m._log('info', 'root-transition',
      { topic: idHex(role.topicId).slice(0, 12), isRoot, why, ...ctx });
  }

  // ── BACKUP nature transitions (v4.26.0, Phase 7) ──────────────────────────
  // Every entry to / exit from the BACKUP nature passes through these two
  // methods — the same single-transition-site discipline rootClaim gave
  // isRoot in Phase 1, extended to the nature whose unmodeled residue caused
  // #333. One structured `role-nature` log per genuine transition (a replica
  // refresh from the SAME principal is bookkeeping, not a transition).
  becomeBackup(topicBig, role, principalHex) {
    const m = this.m;
    const entering = role.backupOf === null;
    const rePrincipaled = !entering && role.backupOf !== principalHex;
    role.backupOf = principalHex;
    role.lastReplicaAt = m._now();
    m._backupTopics.add(topicBig);
    if (entering || rePrincipaled) {
      m._log('info', 'role-nature', {
        topic: idHex(topicBig).slice(0, 12), nature: 'backup',
        principal: String(principalHex).slice(0, 10),
        why: entering ? 'replicate' : 're-principaled',
      });
    }
  }

  retireBackup(topicBig, role, why) {
    const m = this.m;
    if (role.backupOf === null && !m._backupTopics.has(topicBig)) return;
    role.backupOf = null;
    m._backupTopics.delete(topicBig);
    m._log('info', 'role-nature', {
      topic: idHex(topicBig).slice(0, 12), nature: role.isRoot ? 'root' : 'child', why,
    });
  }

  // Create the role AS root — the routing-terminal fallback (the node closest
  // to the topic id acts as its emergent root). formedAt/lastVerify arm the
  // early self-verification (a fresh claim is checked at ROOT_VERIFY_FIRST_MS).
  become(topicBig, why = 'terminal') {
    const m = this.m;
    const role = makeRole(topicBig, true);
    role.formedAt = m._now(); role.lastVerify = 0;
    m.axonRoles.set(topicBig, role);
    m._log('info', 'root-formed', { topic: idHex(topicBig).slice(0, 12) });
    m._log('info', 'root-transition', { topic: idHex(topicBig).slice(0, 12), isRoot: true, why, born: true });
    m._announceRoot(topicBig);
    // Empty-self-root cohort pull (v4.24.0): a root born with no history must
    // PULL from whoever holds it — nothing reliably tells the holder about a
    // new closer root (the alert-bot read-miss mechanism: reader isRoot,
    // cacheSize:0, sticky). Delayed: a pub-terminal root's own publish fills
    // the cache before the probe fires, so a genuinely-fresh topic stays quiet.
    m._scheduleEmptyRootProbe?.(topicBig);
    return role;
  }

  // I am the root for a topic iff I am the routing terminus for its bare id.
  // A non-root relay that becomes the closest node (e.g. after the old root
  // dies) is promoted here — gated on the closer-live-root defer (a beaconing
  // closer root is never contested). Inherits an active metrics lease so a
  // METRICSON that passed through this node resumes publishing on promotion.
  promote(role, payload, meta) {
    const m = this.m;
    const viaEmpty = !(Array.isArray(payload.via) && payload.via.length);
    if (!(viaEmpty && meta.isTerminal && !role.isRoot)) return;
    if (this.liveCloserRoot(role.topicId)) return;   // a closer live root is beaconing — don't contest it
    this._set(role, true, 'terminal-promote');
    m._upstream.delete(role.topicId);
    m._announceRoot(role.topicId);
    role.formedAt = m._now(); role.lastVerify = 0;   // promoted claims get an early self-verify too
    const w = m._metricsWanted.get(role.topicId) || 0;
    if (w > m._now()) role.metricsOn = w;
  }

  // Yield the claim to a strictly-closer live root: demote, pin upstream to it,
  // and re-subscribe under it — the root must ADOPT us (register us as a
  // downstream child) or our subtree starves; every demotion is paired with a
  // confirming subscribe (the one-sided-link lesson: root subs=0, cache>0).
  // Returns false when there was no live root claim to yield (no-op).
  demote(topicBig, toHex, why) {
    const m = this.m;
    const role = m.axonRoles.get(topicBig);
    if (!role || !role.isRoot) return false;
    let to; try { to = idBig(toHex); } catch { return false; }
    if (to === m.nodeId) return false;               // never "demote toward self"
    this._set(role, false, why, { to: toHex.slice(0, 10) });
    m._upstream.set(topicBig, [lc(toHex)]);
    m._sendSubscribe(topicBig);
    return true;
  }

  // Reachable-root fallback (refreshTick): subscribed-but-unpinned past the
  // confirmation window (the iterative hint named a closer node that never
  // adopted us — unreachable / broken-but-authentic) AND no reachable neighbour
  // is closer → claim root locally rather than defer forever to an unreachable
  // node. A wrongly-claimed farther root self-corrects via the strictly-closer
  // beacon demotion.
  claimReachable(topicBig) {
    const m = this.m;
    const role = m.axonRoles.get(topicBig) || this.become(topicBig, 'reachable-fallback');
    this._set(role, true, 'reachable-fallback');
    m._upstream.delete(topicBig);
    m._rootHint.delete(topicBig);          // stop deferring to the unreachable hint
    m._unattachedSince.delete(topicBig);
    m._log('info', 'root-claimed-reachable', { topic: idHex(topicBig).slice(0, 12) });
    return role;
  }

  // ADOPT: become (or remain) a non-root child relay pinned to the parent.
  adoptChild(topicBig, parentHex) {
    const m = this.m;
    let role = m.axonRoles.get(topicBig);
    if (!role) {
      role = makeRole(topicBig, false);
      m.axonRoles.set(topicBig, role);
      m._log('info', 'relay-formed', { topic: idHex(topicBig).slice(0, 12) });
    }
    this._set(role, false, 'adopted-child', { to: parentHex.slice(0, 10) });
    m._upstream.set(topicBig, [lc(parentHex)]);
    return role;
  }

  // A departing root handed us its history. The sender's root beacon is a
  // ghost the moment the handoff arrives — purge it, or the defer gates would
  // keep pointing the topic at a corpse (observed: heirs adopting the history
  // and immediately demoting back toward the leaver, undoing the handoff).
  // If a strictly-closer live root OTHER than the leaver is beaconing, don't
  // hold a competing claim: demote and push the inherited history up (the SUB
  // carries our hw → the root PULLUPs it). Never defer back to the leaver (I-2).
  handoffArrived(topicBig, leaverHex) {
    const m = this.m;
    if (leaverHex && m._rootBeacons.get(topicBig)?.root === leaverHex) {
      m._rootBeacons.delete(topicBig);
    }
    const closer = this.liveCloserRoot(topicBig);
    if (closer && closer !== leaverHex) this.demote(topicBig, closer, 'handoff-better-heir');
  }
}

export default RootClaim;
