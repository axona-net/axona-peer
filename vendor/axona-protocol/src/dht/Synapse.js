/**
 * Synapse – a stateful connection record in a NeuronNode's Synaptome.
 *
 * Replaces the static k-bucket entry of standard Kademlia with a living
 * object that tracks reliability (Weight), measured speed (Latency), and
 * an LTP lock (Inertia) that prevents premature decay of recently-used routes.
 */
export class Synapse {
  /**
   * @param {object} opts
   * @param {number} opts.peerId    - G-ID of the connected peer
   * @param {number} opts.latencyMs - Initial RTT estimate (ms)
   * @param {number} opts.stratum   - Number of matching G-ID prefix bits
   */
  constructor({ peerId, latencyMs, stratum }) {
    this.peerId    = peerId;
    this.weight    = 0.5;      // reliability score [0, 1]
    this.latency   = latencyMs; // EMA of round-trip latency (ms)
    this.inertia   = 0;         // epoch lock: immune to decay while simEpoch < inertia
    this.stratum   = stratum;   // higher = more shared geographic prefix bits
    this.bootstrap = false;     // true for initial routing table synapses (slower decay)
    this._addedBy  = null;      // diagnostic: which rule introduced this synapse
                                // (e.g., 'bootstrap', 'hopCache', 'lateralSpread',
                                //  'triadic', 'promote', 'anneal', 'evictReplace').
                                // Set at the creation site for source-attribution
                                // analysis of K-set divergence in pub/sub.
  }

  /**
   * Long-Term Potentiation: called when a Positive Wave arrives.
   * Increases weight and sets an inertia lock for inertiaDuration epochs.
   */
  reinforce(currentEpoch, inertiaDuration) {
    this.weight  = Math.min(1.0, this.weight + 0.2);
    this.inertia = currentEpoch + inertiaDuration;
  }

  /**
   * Long-Term Depression: passive weight decay applied during Tick_Decay.
   */
  decay(gamma) {
    this.weight *= gamma;
  }

  /**
   * Update the latency EMA with a new RTT sample.
   * alpha=0.2 gives a smoothed, slowly-adapting estimate.
   */
  updateLatency(sampleMs, alpha = 0.2) {
    this.latency += alpha * (sampleMs - this.latency);
  }
}
