// =====================================================================
// SimNetwork — shared registry binding Transport.sim() peers together.
//
// One SimNetwork instance, many peers.  Each peer's Transport.sim()
// registers itself by nodeId; sends and notifies look up the target
// transport by id and call into it directly.
//
// Latency is configurable:
//   - latencyFn(fromId, toId) returns ms of one-way delivery delay
//   - defaults to 0 (synchronous-under-the-hood, fastest possible)
//   - tests can plug a haversine-based fn to model real geography
//
// Used by:
//   - test/smoke_transport_sim.js (kernel)
//   - dht-sim's 'axona' protocol entry (task #33)
// =====================================================================

/**
 * Default latency function: zero delay.  Tests / sim runs that want
 * realistic delays pass their own.
 */
const ZERO_LATENCY = () => 0;

/**
 * Shared registry of sim transports.  Pass the same SimNetwork
 * instance to multiple Transport.sim({ network, identity }) calls
 * to make those peers reach each other.
 */
export class SimNetwork {
  /**
   * @param {object} [opts]
   * @param {(fromId: string, toId: string) => number} [opts.latencyFn]
   *        One-way latency in ms. Default 0.
   */
  constructor({ latencyFn = ZERO_LATENCY } = {}) {
    /** @type {Map<string, import('./transport.js').SimTransport>} */
    this._transports = new Map();
    this._latencyFn  = latencyFn;
  }

  /**
   * Called by SimTransport.start(). Throws if the same nodeId is
   * already registered (catches identity collisions in tests).
   *
   * @param {string} nodeId
   * @param {import('./transport.js').SimTransport} transport
   */
  _register(nodeId, transport) {
    if (this._transports.has(nodeId)) {
      throw new Error(`SimNetwork: nodeId ${nodeId} already registered`);
    }
    this._transports.set(nodeId, transport);
  }

  /**
   * Called by SimTransport.stop().
   * @param {string} nodeId
   */
  _unregister(nodeId) {
    this._transports.delete(nodeId);
  }

  /**
   * @param {string} nodeId
   * @returns {import('./transport.js').SimTransport | undefined}
   */
  _lookup(nodeId) {
    return this._transports.get(nodeId);
  }

  /**
   * @param {string} fromId
   * @param {string} toId
   * @returns {number}
   */
  _latencyMs(fromId, toId) {
    return this._latencyFn(fromId, toId);
  }

  /**
   * Diagnostic: list of all registered nodeIds.
   * @returns {string[]}
   */
  peers() {
    return [...this._transports.keys()];
  }

  /**
   * Diagnostic: how many peers are currently registered.
   * @returns {number}
   */
  size() {
    return this._transports.size;
  }
}
