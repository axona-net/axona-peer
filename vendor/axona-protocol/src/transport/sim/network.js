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
// Latency-simulation mode controls *whether* latency burns real
// wall-clock time:
//   - 'wall-clock' (default): SimTransport.send/notify `await sleep`
//     for the latencyFn-returned ms.  Smoke tests, integration runs,
//     anything that wants its time-domain behaviour to match a real
//     transport keeps this mode.
//   - 'instant': sends resolve in the same microtask; the geometric
//     latency is still recorded in `_latency` (so getLatency reports
//     correctly to the protocol's analytics) but no wall-clock blocking.
//     Designed for large-scale in-process simulators (e.g. dht-sim
//     25K-node sweeps) where the simulator drives the clock through
//     its own model and serial latency-sleeps would dominate wall
//     time without changing the measured outcome.
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
   * @param {'wall-clock'|'instant'} [opts.latencySimulation]
   *        How `send`/`notify` honour the latencyFn-returned ms.
   *        Default 'wall-clock' (await real-time sleep).  'instant'
   *        skips the wall-clock sleep but keeps the geometric value
   *        in each peer's `_latency` map (so `transport.getLatency`
   *        and analytics still report the right number).
   */
  constructor({
    latencyFn          = ZERO_LATENCY,
    latencySimulation  = 'wall-clock',
  } = {}) {
    if (latencySimulation !== 'wall-clock' && latencySimulation !== 'instant') {
      throw new Error(
        `SimNetwork: latencySimulation must be 'wall-clock' or 'instant', got ${latencySimulation}`
      );
    }
    /** @type {Map<string, import('./transport.js').SimTransport>} */
    this._transports        = new Map();
    this._latencyFn         = latencyFn;
    this._latencySimulation = latencySimulation;
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
   * Geometric one-way latency (analytics). Always returns the
   * latencyFn value — used by `_addConnection` to populate the
   * per-peer `_latency` map so the protocol's reported RTT numbers
   * remain accurate even in 'instant' simulation mode.
   *
   * @param {string} fromId
   * @param {string} toId
   * @returns {number}
   */
  _latencyMs(fromId, toId) {
    return this._latencyFn(fromId, toId);
  }

  /**
   * Wall-clock delay actually paid by `send`/`notify`.  In
   * 'instant' simulation mode this is always 0; the protocol still
   * receives the geometric RTT via `_addConnection` / getLatency.
   *
   * @param {string} fromId
   * @param {string} toId
   * @returns {number}
   */
  _wallClockDelayMs(fromId, toId) {
    if (this._latencySimulation === 'instant') return 0;
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
