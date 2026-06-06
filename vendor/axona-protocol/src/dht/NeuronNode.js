/**
 * NeuronNode – a Network Neuron in the Neuromorphic Geographic DHT.
 *
 * Unlike KademliaNode's fixed k-bucket array, a NeuronNode maintains a
 * dynamic, unbounded Synaptome (Map<peerId, Synapse>), governed by:
 *   - Experiential weight updates (LTP / LTD)
 *   - Inertia locks on recently-reinforced synapses
 *   - The Structural Survival Rule: never prune the last synapse in a stratum
 *
 * Routing decisions use the Activation Potential (AP) formula rather than
 * closest-XOR, weighting progress, latency, and reliability together.
 */
import { DHTNode } from './DHTNode.js';

export class NeuronNode extends DHTNode {
  constructor({ id, lat, lng }) {
    super({ id, lat, lng });

    /** @type {Map<number, import('./Synapse.js').Synapse>} */
    this.synaptome = new Map();

    /**
     * Reverse connection index: nodes that have an outgoing synapse pointing
     * TO this node.  Stored as lightweight objects so they can participate in
     * AP routing without full Synapse lifecycle (no LTP/LTD/pruning).
     * A fixed baseline weight of 0.1 lets them compete as routing candidates
     * without dominating over trained outgoing synapses.
     *
     * @type {Map<bigint, {peerId: bigint, latency: number, weight: number, stratum: number}>}
     */
    this.incomingSynapses = new Map();

    /**
     * Regional latency baselines: S2 prefix → historical average latency (ms).
     * Used by the neuromodulation wave to judge whether a route was "fast".
     */
    this.regionalBaselines = new Map();

    /**
     * Transit cache for triadic closure (Structural Plasticity).
     * Maps "fromId_toId" → count of times this node forwarded that pair.
     */
    this.transitCache = new Map();

    // v0.70.20 (refactor commit 14) — `_nodeMapRef` retired.  The
    // visualisation accessor `getRoutingTableEntries` no longer needs
    // a back-pointer into the protocol-level `nodeMap`; it derives
    // peer-id proxies directly from the local synaptome.  The legacy
    // assignment site in NeuromorphicDHT*.addNode is kept as a no-op
    // setter (assigning to an undeclared field is harmless) so we don't
    // need to touch every legacy NX-{3..11} class in this commit.
  }

  // ── Synaptome management ──────────────────────────────────────────────────

  addSynapse(synapse) {
    this.synaptome.set(synapse.peerId, synapse);
  }

  /**
   * Register that `peerId` has an outgoing synapse pointing TO this node.
   * Only stored if no outgoing synapse to that peer already exists (outgoing
   * synapses are always preferred as they carry trained weights).
   *
   * Bounded by the node's synaptome budget: the reverse index participates in
   * AP routing (see `progressCandidates`), so it MUST respect the same hard
   * limit as the outgoing synaptome — otherwise a popular node accrues an
   * unbounded reverse in-degree and routes through far more peers than a real
   * (connection-capped) node ever could, inflating measured performance. The
   * budget is SHARED: a real peer holds at most `_maxSynaptome` routing peers
   * total, regardless of who initiated each channel. When `_maxSynaptome` is
   * unset (no engine budget, e.g. a bare kernel node — production never
   * populates the reverse index), the cap is inactive.
   */
  addIncomingSynapse(peerId, latency, stratum) {
    if (this.synaptome.has(peerId)) return; // outgoing already covers this peer
    if (this.incomingSynapses.has(peerId)) return;
    const cap = this._maxSynaptome;
    if (cap != null && (this.synaptome.size + this.incomingSynapses.size) >= cap) {
      return;                                // routing-degree budget reached
    }
    this.incomingSynapses.set(peerId, { peerId, latency, weight: 0.1, stratum });
  }

  hasSynapse(peerId) {
    return this.synaptome.has(peerId);
  }

  /**
   * Structural Survival Rule: a synapse may only be pruned if another synapse
   * with the same stratum exists in the synaptome.  This guarantees that every
   * populated stratum always has at least one live route, preserving global
   * network navigability even after heavy decay.
   */
  canPrune(synapse) {
    for (const s of this.synaptome.values()) {
      if (s !== synapse && s.stratum === synapse.stratum) return true;
    }
    return false;
  }

  // ── Activation Potential routing ──────────────────────────────────────────

  /**
   * Return all synapses that make strict XOR progress toward targetId.
   * Strict progress (peerDist < myDist) is a mathematical loop prevention:
   * XOR distance can only decrease each hop, so no node can be revisited.
   */
  progressCandidates(targetId) {
    const myDist = this.id ^ targetId;  // BigInt XOR
    const result = [];
    for (const s of this.synaptome.values()) {
      if ((s.peerId ^ targetId) < myDist) result.push(s);
    }
    for (const s of this.incomingSynapses.values()) {
      if ((s.peerId ^ targetId) < myDist) result.push(s);
    }
    return result;
  }

  /**
   * Select the synapse with the highest Activation Potential.
   *
   *   AP_c = (ΔDistance_c / L_c) × (1 + weightScale × W_c)
   *
   * ΔDistance / L  — pure progress velocity (dominant term).
   * (1 + scale×W)  — mild preference boost for synapses that have historically
   *                  led to fast lookups (latency-quality LTP signal).
   *                  weightScale is kept small (default 0.15) so weight is a
   *                  tiebreaker, not a dominator.  Because weight only accrues
   *                  on at-or-below-average-latency paths, high-W synapses
   *                  genuinely represent fast routes, not just frequent ones.
   */
  bestByAP(candidates, targetId, weightScale = 0.15) {
    const myDist = this.id ^ targetId;  // BigInt XOR
    let best = null;
    let bestAP = -Infinity;
    for (const s of candidates) {
      const peerDist = s.peerId ^ targetId;  // BigInt XOR
      const delta    = Number(myDist - peerDist); // Convert for float arithmetic
      const ap       = (delta / s.latency) * (1 + weightScale * s.weight);
      if (ap > bestAP) { bestAP = ap; best = s; }
    }
    return best;
  }

  // ── Globe visualisation ───────────────────────────────────────────────────

  /**
   * Snapshot of the local synaptome — pure local-state read, no
   * cross-peer reach.  Returns an array of plain objects matching the
   * `SynapseSnapshot` typedef in `src/contracts/types.js`.  Production
   * observability (DHT.getSynaptome) and the simulator's globe-click
   * routing-table viewer both consume this.
   *
   * v0.70.20 (refactor commit 14) — replaces the legacy
   * `getRoutingTableEntries()` whose nodeMap-walk was the single V2
   * site in NeuronNode.  Visualisation can resolve peer ids to nodes
   * via the engine-side `dht.getNodes()` enumeration.
   */
  getSynaptomeSnapshot() {
    const snapshot = [];
    for (const s of this.synaptome.values()) {
      snapshot.push({
        peerId:   s.peerId,
        weight:   s.weight,
        latency:  s.latency ?? s.latencyMs ?? 0,
        stratum:  s.stratum,
        inertia:  s.inertia ?? 0,
        useCount: s.useCount ?? 0,
        addedBy:  s._addedBy ?? null,
      });
    }
    return snapshot;
  }

  /**
   * Back-compat shim for the simulator's globe-click viewer.  Returns
   * shallow `{ id: peerId }` proxies so callers that only consume `.id`
   * (eg. main.js's `entries.map(n => n.id)`) continue to work.  Pure
   * local-state read — no `_nodeMapRef`.
   *
   * @deprecated Prefer `getSynaptomeSnapshot()` for new code.
   */
  getRoutingTableEntries() {
    const entries = [];
    for (const s of this.synaptome.values()) entries.push({ id: s.peerId });
    return entries;
  }

  // ── Network message handler ───────────────────────────────────────────────

  handleMessage({ type }) {
    if (type === 'PING') return 'PONG';
    return null;
  }
}
