// =====================================================================
// DHT — the application-facing contract for one running node.
//
// The application (a chat client, a file-distribution layer, the
// simulator's Engine driving N nodes) creates a DHT instance, calls
// start() and join(), then uses lookup / subscribe / publish.
//
// Concrete implementations:
//   - AxonaEngine  — current SOTA (NH-1 generation)
//   - NeuromorphicDHTNX17 — reference benchmark
//   - KademliaDHT         — comparison baseline
//   - GeographicDHT       — comparison baseline (G-DHT)
//
// Every DHT implementation depends on a Transport (downward) and
// optionally a BootstrapService (for initial join). It exposes the
// methods below for the application layer above.
//
// Conformance rules — DHT implementations MUST:
//   1. Run as one node. The protocol does not enumerate "all nodes"
//      anywhere. (The simulator's Engine creates many DHT instances
//      and orchestrates them; that's a simulator concern.)
//   2. Use only the Transport contract for cross-peer interaction.
//      No direct field access on remote peers.
//   3. Emit observability events via `onEvent` rather than logging or
//      mutating shared state. The application listens.
//   4. Treat `getSynaptome()` as a read-only telemetry surface. The
//      application MUST NOT mutate it; the protocol MUST NOT consume
//      it for routing decisions (it has its own internal access).
// =====================================================================

/* eslint-disable no-unused-vars */

/**
 * @abstract
 */
export class DHT {

  // ─── Lifecycle ─────────────────────────────────────────────────────

  /**
   * Start the local node. Allocates the synaptome, spins up background
   * tasks (decay tick, refresh tick, ...). After `start()` the node is
   * "ready to join" but has no peers yet.
   *
   * @returns {Promise<void>}
   */
  async start() {
    throw new Error('DHT.start: not implemented');
  }

  /**
   * Stop the local node. Closes all peer connections, cancels pending
   * pub/sub, releases resources. Idempotent.
   *
   * @returns {Promise<void>}
   */
  async stop() {
    throw new Error('DHT.stop: not implemented');
  }

  // ─── Bootstrap ─────────────────────────────────────────────────────
  //
  // join() performs:
  //   1. BootstrapService.bootstrap() opens an initial channel to a
  //      sponsor.
  //   2. Self-lookup through the sponsor discovers peers near our ID.
  //   3. Stratified bootstrap populates the synaptome with one peer
  //      per XOR stratum, plus extra fill on the geographic strata.
  //
  // After join() resolves the synaptome is operational and the node
  // can route lookups and pub/sub.

  /**
   * @param {import('./types.js').BootstrapEndpoint} sponsor
   * @returns {Promise<void>}
   */
  async join(sponsor) {
    throw new Error('DHT.join: not implemented');
  }

  /**
   * Gracefully leave the network. Notifies known peers; closes
   * channels; stops heartbeats. After `leave()` the node is offline,
   * but the DHT instance can re-`join()`.
   *
   * @returns {Promise<void>}
   */
  async leave() {
    throw new Error('DHT.leave: not implemented');
  }

  // ─── DHT operations ────────────────────────────────────────────────

  /**
   * Look up a target key. Walks the routing layer toward the key via
   * AP scoring + two-hop lookahead + iterative fallback (the five
   * operations of the architecture, whitepaper Chapter 5).
   *
   * @param {bigint} targetKey
   * @returns {Promise<import('./types.js').LookupResult>}
   */
  async lookup(targetKey) {
    throw new Error('DHT.lookup: not implemented');
  }

  // ─── Pub/sub ───────────────────────────────────────────────────────
  //
  // subscribe / publish go through the axonal-tree primitive
  // (whitepaper Chapter 7). Subscribers attach during a routed
  // subscribe message; publishers deliver via fan-out from the topic
  // root. Self-healing under churn is built in via re-subscribe on the
  // refresh tick.

  /**
   * @param {string} topicName
   * @param {(payload: *) => void} handler
   * @returns {Promise<import('./types.js').Subscription>}
   */
  async subscribe(topicName, handler) {
    throw new Error('DHT.subscribe: not implemented');
  }

  /**
   * Cancel a subscription. Idempotent.
   *
   * @param {import('./types.js').Subscription} sub
   * @returns {Promise<void>}
   */
  async unsubscribe(sub) {
    throw new Error('DHT.unsubscribe: not implemented');
  }

  /**
   * Publish a message to a topic. Resolves once the publish reaches
   * the axon root — NOT once all subscribers have received it. (Real
   * delivery is a fan-out tree; only the root acks.)
   *
   * @param {string} topicName
   * @param {*}      payload
   * @returns {Promise<import('./types.js').PublishResult>}
   */
  async publish(topicName, payload) {
    throw new Error('DHT.publish: not implemented');
  }

  // ─── Identity & observability ──────────────────────────────────────

  /**
   * @returns {bigint} the local node's identifier
   */
  getNodeId() {
    throw new Error('DHT.getNodeId: not implemented');
  }

  /**
   * Read-only view of the current synaptome, for telemetry and
   * dashboards. Application MUST NOT use this for routing decisions.
   *
   * @returns {import('./types.js').SynapseSnapshot[]}
   */
  getSynaptome() {
    throw new Error('DHT.getSynaptome: not implemented');
  }

  /**
   * Aggregate metrics for this node — counts, hop histograms, vitality
   * stats, traffic counters. Updated continuously; safe to read at any
   * frequency.
   *
   * @returns {import('./types.js').Metrics}
   */
  getMetrics() {
    throw new Error('DHT.getMetrics: not implemented');
  }

  /**
   * Subscribe to protocol events. Multiple handlers allowed; each
   * receives every event.
   *
   * Used by the application for telemetry. The simulator's training
   * loop also consumes these events for per-cycle snapshots.
   *
   * @param {(event: import('./types.js').ProtocolEvent) => void} handler
   * @returns {() => void} function to unsubscribe this handler
   */
  onEvent(handler) {
    throw new Error('DHT.onEvent: not implemented');
  }
}
