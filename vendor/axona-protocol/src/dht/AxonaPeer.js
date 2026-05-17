// =====================================================================
// AxonaPeer — per-node DHT contract implementation for NH-1.
//
// ── Migration status (Phase 1 of NH1-PerNode-Refactor-Plan-v0.71.md) ──
//
// Phase 1 — Skeleton + co-existence.
//
// This class exists to implement the per-node DHT contract at
// `src/contracts/DHT.js` against the existing multi-node
// `AxonaEngine` (the simulator's NH-1 engine).  In Phase 1 every
// method is a thin delegation to the engine, with the per-node
// `NeuronNode` reference passed through where the engine expects it.
//
// The intent is to validate the per-node API shape (matches the
// contract, can be constructed cleanly, can be observed via getMetrics
// and onEvent) before moving any actual protocol logic out of the
// engine.  Subsequent phases (2 → 3) progressively move read-only and
// then write operations into this class; Phase 4 then renames today's
// engine to `NHOneEngine` and finalises the split.
//
// During Phase 1 the simulator's behaviour is unchanged: the engine
// still owns all the routing logic; AxonaPeer is just an alternative
// API surface that production peers can use to exercise NH-1 through
// the contract.
//
// ── What this class IS ───────────────────────────────────────────────
//   - The DHT contract (src/contracts/DHT.js) implementation for one peer
//   - One instance per running node (in both sim and production)
//   - Owns: a reference to its NeuronNode (per-node state), a reference
//     to the engine (during Phase 1; later phases move logic here), a
//     reference to its transport, and a set of per-peer event listeners.
//
// ── What this class IS NOT ───────────────────────────────────────────
//   - A multi-node manager (that's the engine's job)
//   - A wrapper that hides the engine's existence from the simulator
//     (Phase 1 keeps the engine reachable so the simulator's tests and
//     Engine-cycle code continue to work)
// =====================================================================

import { DHT }      from '../contracts/DHT.js';
import { Synapse }  from './Synapse.js';
import { clz64 }    from '../utils/geo.js';

export class AxonaPeer extends DHT {
  /**
   * @param {object} opts
   * @param {import('./AxonaEngine.js').AxonaEngine} opts.engine
   *        The legacy multi-node engine (Phase 1: delegate target).
   * @param {import('./NeuronNode.js').NeuronNode} opts.node
   *        The NeuronNode this peer wraps.
   */
  constructor({ engine, node }) {
    super();
    if (!engine) throw new Error('AxonaPeer: engine is required');
    if (!node)   throw new Error('AxonaPeer: node is required');
    this._engine = engine;
    this._node   = node;
    this._started = false;
    /** @type {Set<(event: object) => void>} */
    this._eventListeners = new Set();
    /** @type {(event: object) => void | null} */
    this._engineListenerUnsub = null;
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────
  //
  // Phase 1: start/stop are mostly bookkeeping.  The underlying node
  // was already created and registered with the engine via
  // `engine.addNode()` before this AxonaPeer instance came into
  // existence.  We just need to wire up event forwarding so that
  // `onEvent` listeners on this peer receive events that the engine
  // emits about this node.
  //
  // In later phases, start() will move into the real lifecycle:
  // allocate the synaptome, register transport handlers, spin up the
  // decay tick.  For Phase 1 it's idempotent and lightweight.

  async start() {
    if (this._started) return;

    // Engine emits events to a single global listener set today
    // (engine._eventListeners).  We subscribe and filter to events
    // about THIS node, then forward to our per-peer listeners.  This
    // lets the production peer subscribe via AxonaPeer.onEvent without
    // seeing other nodes' events (which it can't, since production
    // only has one node).
    this._engineListenerUnsub = this._engine.onEvent((ev) => {
      // Most events carry a node identifier in one of several fields:
      //   nodeId, peerId, observerId, sourceId, …
      // The current set of event types and their id fields is
      // documented in src/contracts/types.js (ProtocolEvent union).
      // Phase 1 forwards events that mention this._node.id in any of
      // the documented locations; refinement happens when start() owns
      // the event-emit sites in Phase 3.
      if (this._eventMentionsSelf(ev)) {
        for (const cb of this._eventListeners) {
          try { cb(ev); }
          catch (err) {
            console.error(`AxonaPeer ${this._node.id} listener threw:`, err);
          }
        }
      }
    });

    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    if (this._engineListenerUnsub) {
      this._engineListenerUnsub();
      this._engineListenerUnsub = null;
    }
    this._started = false;
  }

  /**
   * Phase 1 stub.  The real production join() will:
   *   1. Use BootstrapService.bootstrap(sponsor) to open the first
   *      WebRTC channel.
   *   2. Run a self-lookup through that sponsor.
   *   3. Stratified-fill the synaptome via subsequent FIND_NODE-style
   *      RPCs over the new transport.
   * In Phase 1 we throw if called from the simulator path — the
   * simulator uses engine.bootstrapJoin instead, which is god's-eye
   * and not contract-shaped.  Production code paths arrive at this
   * stub once Phase 6 (integration) lands.
   */
  async join(_sponsor) {
    throw new Error(
      'AxonaPeer.join: not implemented in Phase 1. ' +
      'Simulator path uses engine.bootstrapJoin; ' +
      'production path lands in Phase 6 integration.'
    );
  }

  async leave() {
    // Phase 1: graceful shutdown is a no-op.  Real implementation
    // notifies known peers and closes channels; tracked in Phase 6.
    return;
  }

  // ─── DHT operations ────────────────────────────────────────────────
  //
  // Phase 4 (v0.71.4) — lookup() now owns the body.  No sourceId
  // parameter: the peer IS the source of its own lookups.  Internal
  // sim state (`simEpoch`, decay-tick interval, EMA hops/time) still
  // lives on the engine during the migration window; this peer's
  // lookup() reads/writes `this._engine.X` for those cross-peer
  // concerns.  Phase 5 splits the rest of the shared state.

  async lookup(targetKey) {
    const node   = this._node;
    const engine = this._engine;
    if (!node || !node.alive) return null;

    engine.simEpoch++;
    if (++engine.lookupsSinceDecay >= engine.DECAY_INTERVAL) {
      engine._tickDecay();                            // FORGET: periodic
      engine.lookupsSinceDecay = 0;
    }

    const result = await this._lookupStep({
      sourceId:    node.id,
      targetKey,
      hops:        0,
      path:        [node.id],
      trace:       [],
      queried:     new Set([node.id]),
      totalTimeMs: 0,
    });

    // ── LEARN: LTP reinforcement on fast paths ─────────────────────
    if (result.found && result.trace.length > 0) {
      const hopCount = result.trace.length;
      engine._emaHops = engine._emaHops === null
        ? hopCount : 0.9 * engine._emaHops + 0.1 * hopCount;
      engine._emaTime = engine._emaTime === null
        ? result.totalTimeMs : 0.9 * engine._emaTime + 0.1 * result.totalTimeMs;
      if (result.totalTimeMs <= engine._emaTime) {
        this._reinforceWave(result.trace);
      }
    }

    const hops = result.path.length - 1;
    engine._bumpLookupStats(node, result.found, hops, result.totalTimeMs);
    engine._emit({
      type: 'lookup-completed', timestamp: Date.now(),
      sourceId: node.id, targetKey,
      hops, time: result.totalTimeMs, found: result.found,
    });

    return {
      path:  result.path,
      hops,
      time:  result.totalTimeMs,
      found: result.found,
    };
  }

  async subscribe(topicName, handler) {
    // Phase 1: subscribe through the engine-owned AxonManager for
    // this node.  Future phases move this into the peer itself.
    const axon = this._engine.axonFor(this._node);
    return axon.subscribe(this._node.id, topicName, handler);
  }

  async unsubscribe(sub) {
    if (!sub) return;
    const axon = this._engine.axonFor(this._node);
    return axon.unsubscribe(sub);
  }

  async publish(topicName, payload) {
    const axon = this._engine.axonFor(this._node);
    return axon.publish(topicName, payload);
  }

  // ─── Identity & observability ──────────────────────────────────────

  getNodeId() {
    return this._node.id;
  }

  /**
   * Phase 2: own the synaptome-snapshot construction directly off the
   * local NeuronNode.  No engine round-trip.  Returns the per-node
   * snapshot — peer ids, weights, latencies, stratum indices.  The
   * application gets a frozen view; the protocol mutates the
   * underlying state independently.
   */
  getSynaptome() {
    if (!this._node) return [];
    return this._node.getSynaptomeSnapshot();
  }

  /**
   * Phase 2: own the metrics object construction.  Per-node lookup
   * counters still live in `engine._nodeStats` (a single Map keyed by
   * NeuronNode); Phase 3 will move those onto the peer.  Until then
   * we look up our entry there.
   */
  getMetrics() {
    const node = this._node;
    if (!node) return null;
    const stats = this._engine._nodeStats.get(node) ||
      { attempted: 0, succeeded: 0, sumHops: 0, sumLatency: 0 };
    const cycleStats = {
      lookupsAttempted: stats.attempted,
      lookupsSucceeded: stats.succeeded,
      avgHops:    stats.succeeded > 0 ? stats.sumHops    / stats.succeeded : 0,
      avgLatency: stats.succeeded > 0 ? stats.sumLatency / stats.succeeded : 0,
    };
    const traffic = {
      msgsSent:     node.msgsSent     | 0,
      msgsReceived: node.msgsReceived | 0,
      byType:       node.msgsByType ? { ...node.msgsByType } : {},
    };
    return {
      simEpoch:             this._engine.simEpoch,
      synaptomeSize:        node.synaptome.size,
      incomingSynapsesSize: node.incomingSynapses.size,
      temperature:          node.temperature ?? this._engine.T_INIT,
      cycleStats,
      traffic,
    };
  }

  // ─── Read-only candidate scoring (Phase 2) ─────────────────────────
  //
  // These methods are pure functions of this peer's local state
  // (synaptome, incomingSynapses) plus the routing target.  They take
  // no `node` parameter — `this._node` is the receiver.  The engine's
  // versions of the same names delegate here via `_peerFor(node)`.

  /**
   * Vitality score for a synapse.  weight × recency, where recency
   * decays exponentially from the synapse's last reinforcement epoch.
   * LTP-locked synapses (inertia > current epoch) get recency = 1.0.
   */
  _vitality(syn) {
    let recency;
    if (syn.inertia > this._engine.simEpoch) {
      recency = 1.0;
    } else {
      const elapsed = this._engine.simEpoch - syn.inertia;
      recency = Math.max(0.1, Math.exp(-elapsed / this._engine.RECENCY_HALF_LIFE));
    }
    return syn.weight * recency;
  }

  /**
   * Two-hop AP scoring with parallel `lookahead_probe` RPCs.  Body
   * matches AxonaEngine._bestByTwoHopAP byte-for-byte; only
   * the receiver changes (was `current` parameter, now `this._node`).
   * The engine method now delegates here.
   */
  async _bestByTwoHopAP(candidates, targetKey, currentDist) {
    const ranked = candidates.map(s => {
      const ap = Number(currentDist - (s.peerId ^ targetKey)) / s.latency;
      return { s, ap };
    }).sort((a, b) => b.ap - a.ap);

    const probeSet = ranked.slice(0, this._engine.LOOKAHEAD_ALPHA).map(x => x.s);

    // Short-circuit: any probe whose first-hop sits exactly on the
    // target wins outright (zero remaining XOR distance).
    for (const first of probeSet) {
      if ((first.peerId ^ targetKey) === 0n) return first;
    }

    // Parallel lookahead probes.  Each rejected probe is treated like
    // an empty-forward response — the source projects the second-hop
    // latency as 0 and distance as the first-hop's own distance to
    // target, the same fallback as `if (!fwd.length)` in the legacy
    // code path.
    const settled = await Promise.allSettled(
      probeSet.map(first =>
        this._node.transport.send(first.peerId, 'lookahead_probe', {
          target:   targetKey,
          fromDist: first.peerId ^ targetKey,
        })
      )
    );

    let bestSyn = null, bestAP2 = -Infinity;
    for (let i = 0; i < probeSet.length; i++) {
      const first = probeSet[i];
      const firstDist = first.peerId ^ targetKey;
      const r = settled[i];

      let twoHopDist, secondLat;
      if (r.status !== 'fulfilled' || !r.value || r.value.terminal) {
        twoHopDist = firstDist;
        secondLat  = 0;
      } else {
        twoHopDist = r.value.peerId ^ targetKey;
        secondLat  = r.value.latency;
      }

      const ap2 = Number(currentDist - twoHopDist) / (first.latency + secondLat);
      if (ap2 > bestAP2) { bestAP2 = ap2; bestSyn = first; }
    }
    return bestSyn ?? this._node.bestByAP(candidates, targetKey, 0);
  }

  /**
   * Pure synchronous greedy 1-hop nextHop selector.  Used by
   * `routeMessage` to find a first-hop closer to target than self.
   * Returns peerId or null if no synapse makes XOR progress.
   */
  _greedyNextHopToward(targetId) {
    if (!this._node?.alive) return null;
    const target = (typeof targetId === 'bigint')
      ? targetId
      : BigInt('0x' + targetId);
    let bestPeerId = null;
    let bestDist   = this._node.id ^ target;
    for (const syn of this._node.synaptome.values()) {
      const d = syn.peerId ^ target;
      if (d < bestDist) { bestDist = d; bestPeerId = syn.peerId; }
    }
    return bestPeerId;
  }

  /**
   * Bounded 2-hop "anyone closer than me?" check.  Parallel
   * `lookahead_probe` RPCs to each first-hop synapse; aggregates the
   * 2-hop responses + incomingSynapses-as-reverse-routing; returns
   * the globally-closest peer id strictly closer than self, or null
   * if this peer is a true 2-hop terminal.
   */
  async _findCloserInTwoHops(targetId) {
    const node = this._node;
    const target = (typeof targetId === 'bigint')
      ? targetId
      : BigInt('0x' + targetId);
    const myDist = node.id ^ target;
    let bestPeerId = null;
    let bestDist   = myDist;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length > 0) {
      const settled = await Promise.allSettled(
        probeTargets.map(peerId =>
          node.transport.send(peerId, 'lookahead_probe', { target, fromDist: myDist })
        )
      );
      for (const r of settled) {
        if (r.status !== 'fulfilled' || !r.value || r.value.terminal) continue;
        const d = r.value.peerId ^ target;
        if (d < bestDist) {
          bestDist   = d;
          bestPeerId = r.value.peerId;
        }
      }
    }
    for (const syn of node.incomingSynapses.values()) {
      const d = syn.peerId ^ target;
      if (d < bestDist) { bestDist = d; bestPeerId = syn.peerId; }
    }
    return bestPeerId;
  }

  onEvent(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('AxonaPeer.onEvent: handler must be a function');
    }
    this._eventListeners.add(handler);
    return () => this._eventListeners.delete(handler);
  }

  // ─── Internal: event filtering ────────────────────────────────────

  /**
   * @private
   * Decide whether a global engine event mentions this peer.  Phase 1
   * filter; refined in Phase 3 when the per-node event-emit sites
   * land directly on AxonaPeer.
   */
  _eventMentionsSelf(ev) {
    if (!ev || typeof ev !== 'object') return false;
    const me = this._node.id;
    // Common fields across the ProtocolEvent union.  Check all that
    // are documented in src/contracts/types.js; if the event doesn't
    // carry any of them it's a global event (cycle-snapshot) and
    // every per-peer instance receives it.
    return (
      ev.nodeId    === me ||
      ev.peerId    === me ||
      ev.observerId === me ||
      ev.sourceId  === me ||
      ev.type === 'cycle-snapshot'
    );
  }

  // ─── Write operations (Phase 3) ────────────────────────────────────
  //
  // Methods that mutate this peer's local state.  Bodies are copied
  // from AxonaEngine verbatim; `node` → `this._node`,
  // `this.X` (engine config) → `this._engine.X`, `this._vitality(node, s)`
  // → `this._vitality(s)` (peer's own method).  The engine retains
  // 1-line delegators for backward compat with internal callers.

  /** Admission gate.  Same logic as engine._addByVitality verbatim. */
  async _addByVitality(newSyn) {
    const node   = this._node;
    const engine = this._engine;
    const cap = node._maxSynaptome ?? engine.MAX_SYNAPTOME;

    let victim = null;
    if (node.synaptome.size >= cap) {
      let minV = Infinity, minVAny = Infinity, victimAny = null;
      for (const s of node.synaptome.values()) {
        if (s.inertia > engine.simEpoch) continue;
        const v = this._vitality(s);
        if (v < minVAny) { minVAny = v; victimAny = s; }
        if (!s.bootstrap && v < minV) { minV = v; victim = s; }
      }
      victim = victim ?? victimAny;
      if (!victim) return false;
    }

    const opened = await node.transport.openConnection(newSyn.peerId);
    if (!opened) return false;

    const measuredLat = node.transport.getLatency(newSyn.peerId);
    newSyn.latency = (measuredLat >= 0) ? measuredLat : 200;

    if (victim) {
      node.synaptome.delete(victim.peerId);
      node.connections?.delete(victim.peerId);
      await node.transport.closeConnection(victim.peerId);
    }
    node.addSynapse(newSyn);
    return true;
  }

  /** LTP reinforcement wave along a successful lookup trace. */
  _reinforceWave(trace) {
    for (let i = trace.length - 1; i >= 0; i--) {
      const { fromId, synapse } = trace[i];
      this._node.transport.notify(fromId, 'reinforce', { synapsePeerId: synapse.peerId })
        .catch(err => console.error('AxonaPeer: reinforce notify failed:', err));
    }
  }

  /**
   * Triadic-closure transit-counting.  After TRIADIC_THRESHOLD
   * observations of (origin→nextId) transiting through us, send the
   * origin a 'triadic_introduce' notification.
   */
  _recordTransit(originId, nextId) {
    const node = this._node;
    const key   = `${originId}_${nextId}`;
    const count = (node.transitCache.get(key) ?? 0) + 1;
    if (count >= this._engine.TRIADIC_THRESHOLD) {
      node.transitCache.delete(key);
      node.transport.notify(originId, 'triadic_introduce', { peerId: nextId })
        .catch(err => console.error('AxonaPeer: triadic_introduce notify failed:', err));
    } else {
      node.transitCache.set(key, count);
    }
  }

  /**
   * Anneal step — replace the weakest synapse with a candidate from
   * the under-represented stratum group.  Emits 'anneal-fired' via
   * the engine's event bus (Phase 3 retains shared bus; future phase
   * may split per-peer).
   */
  async _tryAnneal() {
    const node   = this._node;
    const engine = this._engine;
    if (!node.alive || node.synaptome.size === 0) return;

    let victim = null, weakW = Infinity;
    for (const s of node.synaptome.values()) {
      if (s.inertia > engine.simEpoch) continue;
      if (s.weight < weakW) { weakW = s.weight; victim = s; }
    }
    if (!victim) return;

    const counts = new Array(engine.STRATA_GROUPS).fill(0);
    for (const s of node.synaptome.values()) {
      counts[Math.min(engine.STRATA_GROUPS - 1, s.stratum >>> 2)]++;
    }
    let targetGroup = 0, minCount = Infinity;
    for (let g = 0; g < engine.STRATA_GROUPS; g++) {
      if (counts[g] < minCount) { minCount = counts[g]; targetGroup = g; }
    }

    const lo = targetGroup * 4, hi = lo + 3;
    const candidate = await this._localCandidate(lo, hi);
    if (!candidate || node.synaptome.has(candidate.id)) return;

    node.synaptome.delete(victim.peerId);
    node.connections?.delete(victim.peerId);
    await node.transport.closeConnection(victim.peerId);

    const opened = await node.transport.openConnection(candidate.id);
    if (!opened) return;

    const measuredLat = node.transport.getLatency(candidate.id);
    const latMs   = (measuredLat >= 0) ? measuredLat : 200;
    const stratum = clz64(node.id ^ candidate.id);
    const syn     = new Synapse({ peerId: candidate.id, latencyMs: latMs, stratum });
    syn.weight    = 0.1;
    syn._addedBy  = 'anneal';
    node.addSynapse(syn);
    engine._emit({
      type: 'anneal-fired', timestamp: Date.now(),
      observerId: node.id, evicted: victim.peerId, admitted: candidate.id,
    });
  }

  /**
   * Dead-synapse replacement.  Closes the dead channel, finds a
   * candidate in the same stratum group, opens a fresh channel.
   */
  async _evictAndReplace(deadSyn) {
    const node   = this._node;
    const engine = this._engine;

    node.synaptome.delete(deadSyn.peerId);
    node.connections?.delete(deadSyn.peerId);
    await node.transport.closeConnection(deadSyn.peerId);

    const group = Math.min(engine.STRATA_GROUPS - 1, deadSyn.stratum >>> 2);
    const candidate = await this._localCandidate(group * 4, group * 4 + 3);
    if (!candidate || node.synaptome.has(candidate.id)) return null;

    const opened = await node.transport.openConnection(candidate.id);
    if (!opened) return null;

    const weights = [];
    for (const s of node.synaptome.values()) weights.push(s.weight);
    weights.sort((a, b) => a - b);
    const medW = weights.length > 0 ? weights[weights.length >> 1] : engine.VITALITY_FLOOR;

    const measuredLat = node.transport.getLatency(candidate.id);
    const latMs   = (measuredLat >= 0) ? measuredLat : 200;
    const stratum = clz64(node.id ^ candidate.id);
    const syn     = new Synapse({ peerId: candidate.id, latencyMs: latMs, stratum });
    syn.weight    = medW;
    syn._addedBy  = 'evictReplace';
    node.addSynapse(syn);
    return syn;
  }

  /**
   * 2-hop neighbourhood scan via parallel `local_probe` RPCs.  Picks
   * a random candidate from the under-represented stratum group [lo, hi].
   * Returns `{id}` or null.
   */
  async _localCandidate(lo, hi) {
    const node   = this._node;
    const engine = this._engine;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length === 0) return null;

    const settled = await Promise.allSettled(
      probeTargets.map(peerId => node.transport.send(peerId, 'local_probe', null))
    );

    const candidates = [];
    outer:
    for (const r of settled) {
      if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
      for (const id of r.value) {
        if (id === node.id) continue;
        if (node.synaptome.has(id)) continue;
        const stratum = clz64(node.id ^ id);
        if (stratum < lo || stratum > hi) continue;
        candidates.push(id);
        if (candidates.length >= engine.ANNEAL_LOCAL_SAMPLE) break outer;
      }
    }
    if (candidates.length === 0) return null;

    const chosenId = candidates[Math.floor(Math.random() * candidates.length)];
    return { id: chosenId };
  }

  // ─── Routed messaging + pub/sub primitives (Phase 3d–f) ────────────
  //
  // These deliver AxonManager's pub/sub on top of NH-1's transport
  // contract.  Bodies are copied from the engine verbatim; `node` →
  // `this._node`; the per-peer handler tables continue to live on
  // `this._engine._routedHandlers` / `_directHandlers` until Phase 4
  // splits the storage too.  This is intentional: minimising changes
  // to handler-storage shape during Phase 3 keeps the gate strict.

  /**
   * K-closest iterative search.  Async; uses parallel
   * `find_closest_set` RPCs.  Returns BigInt peer ids sorted by XOR
   * distance to targetId.
   */
  async findKClosest(targetId, K = 5, { alpha = 3, maxRounds = 40 } = {}) {
    const src = this._node;
    if (!src) return [];
    const targetBig = topicToBigInt(targetId);

    const distances = new Map();
    const addCandidate = (peerId) => {
      if (typeof peerId !== 'bigint' || distances.has(peerId)) return;
      distances.set(peerId, peerId ^ targetBig);
    };

    addCandidate(src.id);
    for (const syn of src.synaptome.values())         addCandidate(syn.peerId);
    for (const syn of src.incomingSynapses.values())  addCandidate(syn.peerId);

    const visited = new Set();
    let lastPoolSize = 0;
    let stableRounds = 0;

    for (let round = 0; round < maxRounds; round++) {
      const sorted = [...distances.entries()]
        .sort((a, b) => a[1] < b[1] ? -1 : 1)
        .map(([peerId]) => peerId);
      const topK = sorted.slice(0, K);
      const topKAllVisited = topK.every(p => visited.has(p));

      let toQuery = topK.filter(p => !visited.has(p)).slice(0, alpha);
      if (toQuery.length < alpha) {
        const remaining = alpha - toQuery.length;
        const beyond = sorted
          .filter(p => !visited.has(p) && !topK.includes(p))
          .slice(0, remaining);
        toQuery = toQuery.concat(beyond);
      }
      if (toQuery.length === 0) break;

      const probes = toQuery.filter(p => p !== src.id);
      for (const p of toQuery) visited.add(p);

      if (probes.length > 0) {
        const settled = await Promise.allSettled(
          probes.map(peerId =>
            src.transport.send(peerId, 'find_closest_set',
              { target: targetBig, K: this._engine._k })
          )
        );
        for (const r of settled) {
          if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
          for (const peerId of r.value) addCandidate(peerId);
        }
      }

      const grew = distances.size > lastPoolSize;
      lastPoolSize = distances.size;
      stableRounds = grew ? 0 : stableRounds + 1;
      if (topKAllVisited && stableRounds >= 1) break;
    }

    return [...distances.entries()]
      .sort((a, b) => a[1] < b[1] ? -1 : 1)
      .slice(0, K)
      .map(([peerId]) => peerId);
  }

  /**
   * Send a routed message starting from this peer.  Greedy 1-hop or
   * 2-hop terminal check; dispatches local routed handler; if not
   * consumed AND not terminal, forwards via route_msg request chain.
   */
  async routeMessage(targetId, type, payload, opts = {}) {
    const originNode = this._node;
    const originId   = opts.fromId ?? nodeIdToHex(originNode.id);
    const targetBig  = topicToBigInt(targetId);

    let nextHopId = this._greedyNextHopToward(targetBig);
    let isTerminal = nextHopId === null;
    if (isTerminal) {
      const closer = await this._findCloserInTwoHops(targetBig);
      if (closer !== null && closer !== originNode.id) {
        nextHopId  = closer;
        isTerminal = false;
      }
    }

    const result = await this._deliverRouted(type, payload, {
      fromId:   originId,
      targetId: targetBig,
      hopCount: 0,
      isTerminal,
      node:     originNode,
    });

    if (result === 'consumed') {
      return { consumed: true, atNode: originNode.id, hops: 0 };
    }
    if (isTerminal) {
      return { consumed: false, atNode: originNode.id, hops: 0, terminal: true };
    }

    try {
      const downstream = await originNode.transport.send(nextHopId, 'route_msg', {
        type, payload, targetId: targetBig, hops: 1, originId,
      });
      return downstream;
    } catch {
      return { consumed: false, atNode: originNode.id, hops: 0, exhausted: true };
    }
  }

  /**
   * Dispatch a routed message to the local handler for `type`.
   * Returns the handler's return value (truthy = 'consumed' or other
   * meaningful response; falsy/throw → 'forward').
   */
  async _deliverRouted(type, payload, meta) {
    const node = this._node;
    const handlers = this._engine._routedHandlers.get(node);
    const handler = handlers?.get(type);
    if (!handler) return 'forward';
    try {
      const result = await handler(payload, meta);
      return result || 'forward';
    } catch (err) {
      console.error(`AxonaPeer routed handler error at ${node.id} for '${type}':`, err);
      return 'forward';
    }
  }

  /**
   * Fire-and-forget direct notification to one peer.  `type` is the
   * application name; the wire type is `direct_${type}`.
   */
  async sendDirect(peerId, type, payload) {
    const fromNode = this._node;
    if (!fromNode?.alive || !fromNode.transport) return false;
    const peerBig = topicToBigInt(peerId);
    try {
      const ok = await fromNode.transport.notify(peerBig, `direct_${type}`, payload);
      return ok !== false;
    } catch {
      return false;
    }
  }

  /**
   * Pick a child to promote as sub-axon — prefer existing high-weight
   * synaptome children; fall back to XOR-closest existing child.
   */
  _pickRecruitPeer(role, meta, subscriberId) {
    const node = this._node;
    if (role.children.size === 0) return null;
    const selfHex   = nodeIdToHex(node.id);
    const forwarder = meta.fromId;
    const dead      = node._deadPeers || new Set();

    const synapseWeights = new Map();
    for (const syn of node.synaptome.values()) {
      if (dead.has(syn.peerId)) continue;
      synapseWeights.set(nodeIdToHex(syn.peerId), {
        weight:  syn.weight,
        latency: syn.latency ?? syn.latencyMs ?? 0,
      });
    }

    let bestChildId = null;
    let bestScore = -Infinity;
    for (const childId of role.children.keys()) {
      if (childId === selfHex)   continue;
      if (childId === forwarder) continue;
      const s = synapseWeights.get(childId);
      if (!s) continue;
      const score = s.weight * 1_000_000 - s.latency;
      if (score > bestScore) { bestScore = score; bestChildId = childId; }
    }
    if (bestChildId) return bestChildId;

    const subBig = topicToBigInt(subscriberId);
    let best = null;
    let bestDist = null;
    for (const childId of role.children.keys()) {
      if (childId === selfHex)   continue;
      if (childId === forwarder) continue;
      const d = BigInt('0x' + childId) ^ subBig;
      if (bestDist === null || d < bestDist) { bestDist = d; best = childId; }
    }
    return best;
  }

  /**
   * Pick an external synaptome peer (not yet a child) to become a new
   * sub-axon — XOR-closest to the new subscriber's id.
   */
  _pickRelayPeer(role, subscriberId, forwarderId) {
    const node = this._node;
    if (!node?.alive) return null;
    const selfHex = nodeIdToHex(node.id);
    const subBig  = topicToBigInt(subscriberId);
    const dead    = node._deadPeers || new Set();

    const considered = new Map();
    for (const syn of node.synaptome.values()) {
      const peerId = syn.peerId;
      if (dead.has(peerId)) continue;
      const hex = nodeIdToHex(peerId);
      if (hex === selfHex)       continue;
      if (hex === forwarderId)   continue;
      if (hex === subscriberId)  continue;
      if (role.children.has(hex)) continue;
      if (considered.has(hex))    continue;
      considered.set(hex, { peerId, distToSub: peerId ^ subBig });
    }
    if (considered.size === 0) return null;

    let bestHex = null, bestDist = null;
    for (const [hex, rec] of considered) {
      if (bestDist === null || rec.distToSub < bestDist) {
        bestDist = rec.distToSub;
        bestHex  = hex;
      }
    }
    return bestHex;
  }

  /**
   * Register a routed-message handler for `type`.  Per-peer storage;
   * engine version still works because engine delegates here.
   */
  onRoutedMessage(type, handler) {
    const node = this._node;
    let table = this._engine._routedHandlers.get(node);
    if (!table) { table = new Map(); this._engine._routedHandlers.set(node, table); }
    table.set(type, handler);
  }

  /**
   * Register a direct-message handler for `type`.  Bridges to a
   * transport.onNotification listener on `direct_${type}`.
   */
  onDirectMessage(type, handler) {
    const node = this._node;
    let table = this._engine._directHandlers.get(node);
    if (!table) { table = new Map(); this._engine._directHandlers.set(node, table); }
    const wireType = `direct_${type}`;
    if (!table.has(type)) {
      node.transport.onNotification(wireType, (fromId, payload) => {
        const h = this._engine._directHandlers.get(node)?.get(type);
        if (!h) return;
        const fromHex = (typeof fromId === 'bigint') ? nodeIdToHex(fromId) : fromId;
        try {
          h(payload, { fromId: fromHex, type });
        } catch (err) {
          console.error(`AxonaPeer direct handler error at ${node.id} for '${type}':`, err);
        }
      });
    }
    table.set(type, handler);
  }

  // ─── Routing tick — _lookupStep + _lookupResult (Phase 3g) ─────────
  //
  // _lookupStep is NH-1's per-hop routing logic.  It runs on the
  // receiver of a 'lookup_step' request: collects forward-progress
  // candidates from local synaptome + incoming, evicts dead synapses
  // and replaces them, applies iterative-fallback if no candidate
  // makes XOR progress, selects a next hop (direct → epsilon → 2-hop
  // AP), applies LEARN side-effects (incoming promotion, hop caching,
  // triadic closure), bumps temperature + maybe triggers anneal, and
  // forwards via transport.send('lookup_step', ...).  Body copied
  // verbatim from AxonaEngine._lookupStep; `node` → `this._node`,
  // engine config via `this._engine.X`, internal method calls land on
  // peer methods (e.g. `this._addByVitality(syn)` instead of
  // `engine._addByVitality(node, syn)`).
  //
  async _lookupStep(ctx) {
    const node   = this._node;
    const engine = this._engine;
    if (!node || !node.alive) {
      return this._lookupResult(ctx, false);
    }

    const { sourceId, targetKey } = ctx;
    const currentDist = node.id ^ targetKey;
    if (currentDist === 0n) {
      return this._lookupResult(ctx, true);
    }
    if (ctx.hops >= engine.MAX_HOPS) {
      return this._lookupResult(ctx, false);
    }

    const dead = node._deadPeers || new Set();

    const deadSynapses = [];
    const candidates   = [];
    for (const s of node.synaptome.values()) {
      if ((s.peerId ^ targetKey) >= currentDist) continue;
      if (dead.has(s.peerId)) { deadSynapses.push(s); s.weight = 0; continue; }
      candidates.push(s);
    }
    for (const s of node.incomingSynapses.values()) {
      if ((s.peerId ^ targetKey) >= currentDist) continue;
      if (dead.has(s.peerId)) continue;
      candidates.push(s);
    }

    if (deadSynapses.length > 0) {
      node.temperature = Math.max(node.temperature, engine.T_REHEAT);
      for (const syn of deadSynapses) {
        const repl = await this._evictAndReplace(syn);
        if (repl && (repl.peerId ^ targetKey) < currentDist) candidates.push(repl);
      }
    }

    if (candidates.length === 0) {
      let bestSyn = null, bestDist = null;
      const scan = (s) => {
        if (ctx.queried.has(s.peerId)) return;
        if (dead.has(s.peerId)) return;
        const d = s.peerId ^ targetKey;
        if (bestDist === null || d < bestDist) { bestDist = d; bestSyn = s; }
      };
      for (const s of node.synaptome.values())         scan(s);
      for (const s of node.incomingSynapses.values()) scan(s);
      if (!bestSyn) return this._lookupResult(ctx, false);
      candidates.push(bestSyn);
    }

    let nextSyn;
    const direct = node.synaptome.get(targetKey)
                ?? node.incomingSynapses.get(targetKey);
    if (direct && !dead.has(targetKey)) nextSyn = direct;

    if (!nextSyn && node.id === sourceId
        && Math.random() < engine.EPSILON) {
      nextSyn = candidates[Math.floor(Math.random() * candidates.length)];
    }

    if (!nextSyn) {
      nextSyn = await this._bestByTwoHopAP(candidates, targetKey, currentDist);
    }

    const nextId = nextSyn.peerId;

    if (node.incomingSynapses.has(nextId) && !node.synaptome.has(nextId)) {
      const inc = node.incomingSynapses.get(nextId);
      inc.useCount = (inc.useCount ?? 0) + 1;
      if (inc.useCount >= engine.PROMOTE_THRESHOLD) {
        const syn = new Synapse({
          peerId: nextId, latencyMs: inc.latency, stratum: inc.stratum,
        });
        syn.weight   = 0.5;
        syn.inertia  = engine.simEpoch;
        syn._addedBy = 'promote';
        if (await this._addByVitality(syn)) {
          node.incomingSynapses.delete(nextId);
        }
      }
    }

    ctx.queried.add(nextId);
    ctx.path.push(nextId);
    ctx.trace.push({ fromId: node.id, synapse: nextSyn });
    ctx.totalTimeMs += nextSyn.latency;
    ctx.hops += 1;

    if (node.id !== targetKey && !node.synaptome.has(targetKey)) {
      const stratum = clz64(node.id ^ targetKey);
      const syn = new Synapse({
        peerId: targetKey, latencyMs: 0, stratum,
      });
      syn.weight   = 0.5;
      syn.inertia  = engine.simEpoch;
      syn._addedBy = 'hopCache';
      const added = await this._addByVitality(syn);
      if (added && engine.EN_LATERAL_SPREAD) {
        const nodeRegion = node.id >> BigInt(64 - engine.GEO_REGION_BITS);
        const regional   = [];
        for (const s of node.synaptome.values()) {
          if (s.peerId === targetKey) continue;
          if ((s.peerId >> BigInt(64 - engine.GEO_REGION_BITS)) === nodeRegion) {
            regional.push(s);
          }
        }
        regional.sort((a, b) => b.weight - a.weight);
        for (let i = 0; i < Math.min(engine.LATERAL_K, regional.length); i++) {
          node.transport.notify(regional[i].peerId, 'lateral_spread',
                                { target: targetKey, depth: 1 })
            .catch(err => console.error('AxonaPeer: lateral_spread notify failed:', err));
        }
      }
    }

    if (node.id !== sourceId) this._recordTransit(sourceId, nextId);

    node.temperature = Math.max(engine.T_MIN, node.temperature * engine.ANNEAL_COOLING);
    if (Math.random() < node.temperature * engine.ANNEAL_RATE_SCALE) {
      this._tryAnneal().catch(err =>
        console.error(`AxonaPeer: anneal failed at ${node.id.toString(16)}:`, err));
    }

    try {
      const downstream = await node.transport.send(nextId, 'lookup_step', {
        sourceId, targetKey,
        hops:        ctx.hops,
        path:        ctx.path,
        trace:       ctx.trace,
        queried:     ctx.queried,
        totalTimeMs: ctx.totalTimeMs,
      });
      return downstream;
    } catch {
      return this._lookupResult(ctx, false);
    }
  }

  _lookupResult(ctx, found) {
    return {
      found,
      path:        ctx.path,
      trace:       ctx.trace,
      totalTimeMs: ctx.totalTimeMs,
      hops:        ctx.hops,
    };
  }
}

// ─── Module-local helpers (mirror AxonaEngine) ────────────────

function topicToBigInt(v) {
  if (typeof v === 'bigint') return v;
  return BigInt('0x' + v);
}

function nodeIdToHex(id) {
  if (typeof id === 'string') return id;
  return id.toString(16).padStart(16, '0');
}
