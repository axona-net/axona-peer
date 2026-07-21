// =====================================================================
// syncEngine.js — the sync engine, repair side (v0.2 refactor, Phase 8).
//
// ONE operation moves topic state between peers for repair/durability.
// Before this module the six repair policies each hand-rolled their own
// snapshot assembly, verb emission, and receiver composition across
// wireHandlers.js and repairPlane.js — the 4.22.0 lw-pull storm, the 4.24.0
// departure spray, and the #353 ephemeral-publisher strand all lived in the
// seams between those hand-rolled copies. Now:
//
//   · SYNC_POLICIES is the NORMATIVE policy table (Axona-Architecture §VIII).
//     Every row is typed by the role natures (Phase 7): it names the nature
//     it creates on the receiver and that nature's eviction path — the
//     principal-liveness rule (INVARIANTS I-10) made machine-checkable.
//   · _syncPull / _syncPush / _syncAnswerPull are the ONLY emission sites
//     for the repair verbs (PULLUP, REPLAYUP, REPLICATE, HANDOFF) — enforced
//     statically by test/smoke_emission_sites.mjs.
//   · _syncIngest is the ONE receiver composition (tombstones first, then
//     verified stamped ingest) with per-policy hooks.
//   · Per-pair quench ledgers (role.sync, Phase 6) are consulted INSIDE the
//     engine, not at call sites — a new policy cannot forget its quench.
//
// The hot delivery path (fan-out, replay-on-seat, DELIVER) is deliberately
// OUT (plan §7 Phase 8): it is high-rate, latency-sensitive, its quench
// machinery is already unified in topicStore, and no incident has lived
// there. It joins the engine only if it earns its way in.
// =====================================================================

import { T } from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';
import { makeRole } from './rootClaim.js';

// ── The typed policy table ──────────────────────────────────────────────
// Fields (all required — smoke_sync_engine.mjs asserts completeness):
//   mode              'pull' | 'push' | 'ingest' | 'gate'
//   verb              wire verb the policy emits (or rides on)
//   trigger           when the policy fires (prose, normative)
//   summary           what evidence the decision compares (hw/lw/sig/…)
//   createsOnReceiver role nature planted on the receiving node, or null
//   evictor           that nature's eviction path (principal-liveness, I-10)
//   ledger            the role.sync quench state the engine consults, or null
//   rateBound         what bounds the policy's send rate
export const SYNC_POLICIES = Object.freeze({
  REPLAY_UP: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: "renewal SUB advertises a child hw ABOVE mine (I hold less than my child)",
    summary: 'hw', createsOnReceiver: null,
    evictor: 'natural quench: my hw catches up to the child’s on union',
    ledger: null, rateBound: 'one pull per triggering renewal; delta-only answer',
  }),
  SPLIT_UNION: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: "renewal SUB advertises a child lw BELOW my lw (pre-transition half I never saw)",
    summary: 'lw', createsOnReceiver: null,
    evictor: 'natural quench: my lw drops to the child’s on union',
    ledger: 'pulledLw (one-shot per (child, lw); re-arms only if lw DECREASES — 4.22.1 storm guard)',
    rateBound: 'ledger one-shot',
  }),
  EMPTY_ROOT_PROBE: Object.freeze({
    mode: 'pull', verb: 'PULLUP',
    trigger: 'a root with an EMPTY cache (birth probe + renewal sweep) pulls the cohort',
    summary: 'empty-cache', createsOnReceiver: null,
    evictor: 'natural quench: probe only while empty',
    ledger: 'probeTries/probeAt (per-topic, max EMPTY_ROOT_PROBE_MAX, rate EMPTY_ROOT_PROBE_INTERVAL_MS — enforced at the trigger sweep)',
    rateBound: 'EMPTY_ROOT_PROBE_MAX tries, EMPTY_ROOT_PROBE_FANOUT targets',
  }),
  COHORT_REPLICATE: Object.freeze({
    mode: 'push', verb: 'REPLICATE',
    trigger: 'live root, every tick (keepalive) + eager on stamp/kill; full state on sig delta',
    summary: 'sig (count:hw:tombstones)', createsOnReceiver: 'backup',
    evictor: 'principal gone + re-homed + BACKUP_EVICT_MS (rootClaim.retireBackup)',
    ledger: 'sig/lastFullAt (delta gate) + replicas (cohort membership)',
    rateBound: 'REPLICATE_FULL_BUDGET full pushes per tick (round-robin cursor)',
  }),
  UNION_AT_ROOT: Object.freeze({
    mode: 'ingest', verb: 'REPLICATE',
    trigger: 'a REPLICATE arriving at a node that itself holds the ROOT claim',
    summary: 'none (full union)', createsOnReceiver: null,
    evictor: 'n/a — receiver keeps its claim; no backup bookkeeping (no usurpation)',
    ledger: null, rateBound: 'bounded time-sliced ingest queue (I-11)',
  }),
  HANDOFF: Object.freeze({
    mode: 'push', verb: 'HANDOFF',
    trigger: 'graceful leave: departing root pushes each rooted cache to its heir',
    summary: 'departure', createsOnReceiver: 'root (heir adopts the claim)',
    evictor: 'demote to a strictly-closer live root (rootClaim.handoffArrived); never back to the leaver (I-2)',
    ledger: 'handoffAcked (leaver-side ack set; HANDOFFACK)',
    rateBound: 'HANDOFF_TRIES rounds × HANDOFF_ACK_MS, heirs re-resolved per round (#340)',
  }),
  PUB_DURABLE: Object.freeze({
    mode: 'gate', verb: 'REPLICATE',
    trigger: 'a SELF-ROOTED stamp (publisher is the topic’s root) with a non-empty cohort',
    summary: 'dispatch of the eager cohort replicate', createsOnReceiver: 'backup',
    evictor: 'same as COHORT_REPLICATE',
    ledger: '_pendingPub (confirm deferred until ≥1 REPLICATE dispatched — #353: leave()’s evidence drain then holds an ephemeral publisher until its history has left the node)',
    rateBound: 'one gate per stamped publish',
  }),
});

export const syncEngineMethods = {
  // ── state assembly: ONE place a role's transferable state is serialized ──
  _syncSnapshot(role) {
    return {
      msgs: role.cache.map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq })),
      dels: this._activeDels(role),
    };
  },
  _syncDelta(role, sinceHw) {
    return {
      msgs: role.cache.filter(c => c.publishTs > sinceHw)
                      .map(c => ({ json: c.json, publishTs: c.publishTs, msgId: c.msgId, seq: c.seq })),
      dels: this._activeDels(role),
    };
  },

  // ── PULL side: ask `target` to replay its stamped history up to me ──────
  // The per-pair quench ledger is consulted HERE (not at call sites): a
  // SPLIT_UNION pull is one-shot per (child, lw) and re-arms only when the
  // child's lw decreases. Returns true iff the pull was sent.
  _syncPull(targetBig, topicBig, policyName, { sinceHw = 0, lw = null, role = null } = {}) {
    if (policyName === 'SPLIT_UNION') {
      const peerHex = lc(idHex(targetBig));
      const prev = role.sync.pulledLw.get(peerHex);
      if (!(prev === undefined || lw < prev)) return false;
      role.sync.pulledLw.set(peerHex, lw);
    }
    this._route(targetBig, T.PULLUP, { topicId: idHex(topicBig), sinceHw, parentId: idHex(this.nodeId) });
    return true;
  },

  // Answer a PULLUP: replay my cache delta (and active tombstones) UP to the
  // asking parent. The only REPLAYUP emission site.
  _syncAnswerPull(payload) {
    const role = this.axonRoles.get(idBig(payload.topicId));
    if (!role || !role.cache.length) return;
    const sinceHw = Number.isFinite(payload.sinceHw) ? payload.sinceHw : 0;
    const { msgs, dels } = this._syncDelta(role, sinceHw);
    if ((msgs.length || dels.length) && isHexId(lc(payload.parentId))) {
      this._route(idBig(payload.parentId), T.REPLAYUP, { topicId: idHex(role.topicId), msgs, dels });
    }
  },

  // ── PUSH side: move a role's full state to one peer ─────────────────────
  // The only REPLICATE / HANDOFF emission site. `full:false` sends an empty
  // KEEPALIVE (refreshes the backup's lastReplicaAt; empty ingest is a no-op).
  _syncPush(targetBig, topicBig, role, policyName, { full = true } = {}) {
    const { msgs, dels } = full ? this._syncSnapshot(role) : { msgs: [], dels: [] };
    const payload = { topicId: idHex(topicBig), from: idHex(this.nodeId), msgs, dels };
    if (policyName === 'HANDOFF') this._route(targetBig, T.HANDOFF, payload);
    else this._route(targetBig, T.REPLICATE, payload);
  },

  // ── INGEST side: the ONE receiver composition ───────────────────────────
  // Tombstones first (a killed body in the same batch must be suppressed),
  // then the verified stamped ingest (B-4 re-verify, msgId dedup, fanout +
  // app delivery). Policy hooks:
  //   REPLAY_UP  — role must already exist (I asked for this history)
  //   HANDOFF    — adopt as root (heir), then departure rules + confirming ack
  //   REPLICATE  — at a root: UNION_AT_ROOT (keep claim, no backup
  //                bookkeeping); else: enter the BACKUP nature (rootClaim)
  async _syncIngest(payload, meta, policyName) {
    let topicBig; try { topicBig = idBig(payload.topicId); } catch { return; }

    if (policyName === 'HANDOFF') {
      const role = this.axonRoles.get(topicBig) || this._becomeRoot(topicBig, 'handoff-heir');
      this._applyDels(role, topicBig, payload.dels);
      await this._ingestStampedBatch(role, payload.msgs);
      // Departure rules — purge the leaver's ghost beacon, never defer back to
      // the leaver, yield only to a strictly-closer live root (rootClaim).
      const leaver = typeof payload.from === 'string' ? lc(payload.from) : null;
      this._rootClaim.handoffArrived(topicBig, leaver);
      // Confirm receipt: the leaver retries topics it never hears an ack for.
      if (leaver && isHexId(leaver)) {
        try { this._route(idBig(leaver), T.HANDOFFACK, { topicId: payload.topicId }); } catch { /* best-effort */ }
      }
      // Diagnostic (#362): ingest + ack confirmation for the leaver's retry loop.
      this._log('debug', 'handoff-ingested',
        { topic: String(payload.topicId).slice(0, 12), msgs: Array.isArray(payload.msgs) ? payload.msgs.length : 0, acked: !!(leaver && isHexId(leaver)) });
      return;
    }

    if (policyName === 'REPLICATE') {
      const mine = this.axonRoles.get(topicBig);
      if (mine?.isRoot) {
        // UNION_AT_ROOT — the cohort anti-entropy contract: co-hosting roots
        // converge to the union of cache+tombstones. We keep our claim.
        this._applyDels(mine, topicBig, payload.dels);
        await this._ingestStampedBatch(mine, payload.msgs);
        return;
      }
      if (!this._rootReplicas) return;                  // backup duty disabled on this node
      let from = null;
      if (payload.from && isHexId(lc(payload.from))) from = lc(payload.from);
      else if (meta?.fromId != null) { try { from = lc(idHex(idBig(meta.fromId))); } catch { /* */ } }
      let role = this.axonRoles.get(topicBig);
      if (!role) { role = makeRole(topicBig, false); this.axonRoles.set(topicBig, role); }
      this._rootClaim.becomeBackup(topicBig, role, from);   // nature transition (I-10)
      this._applyDels(role, topicBig, payload.dels);
      await this._ingestStampedBatch(role, payload.msgs);
      return;
    }

    // REPLAY_UP — stamped history I asked for; only meaningful if the role lives.
    const role = this.axonRoles.get(topicBig);
    if (!role) return;
    this._applyDels(role, topicBig, payload.dels);
    await this._ingestStampedBatch(role, payload.msgs);
  },
};

export default syncEngineMethods;
