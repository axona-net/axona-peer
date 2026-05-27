// =====================================================================
// Transport — the contract for moving typed messages between peers.
//
// One Transport instance per running node. The protocol layer
// (AxonaEngine, NeuronNode, AxonaManager, ...) calls into this
// abstraction; concrete transports implement it.
//
// Implementations:
//   - SimulatedNetwork — in-process, synchronous-under-the-hood (sim)
//   - WebRTCTransport  — production, real network: WebRTC data channels
//                        for peer-to-peer messaging, WebSocket signaling
//                        for connection establishment, 1 Hz ping/pong
//                        heartbeat for liveness and latency.
//
// Conformance rules — protocol code MUST:
//   1. Never reach around Transport to access peer state. No
//      `nodeMap.get(peerId)`, no `peer.synaptome.values()` reads, no
//      cross-peer field access. All cross-peer reads happen via
//      `send` / `notify`.
//   2. Treat every cross-peer interaction as async. No assumption that
//      the remote responds within the same tick.
//   3. Consume `getLatency()` for AP scoring rather than computing
//      latency itself. The transport is the single source of truth for
//      observed RTT.
//   4. Subscribe to `onPeerDied` for liveness rather than checking a
//      boolean field on a peer object. Peer death is asynchronous and
//      arrives via callback.
//
// Conformance rules — Transport implementations MUST:
//   1. Maintain persistent channels — channel setup happens once on
//      `openConnection`, not per message.
//   2. Run a 1 Hz ping/pong heartbeat on every open channel; expose
//      RTT via `getLatency`; emit `onPeerDied` on heartbeat timeout.
//   3. Respect bilateral cap semantics — `openConnection` returns
//      false if the remote refused.
//   4. Survive `stop()`/`start()` cycles cleanly. Calling `start()`
//      after `stop()` is supported.
// =====================================================================

/* eslint-disable no-unused-vars */

/**
 * @abstract
 */
export class Transport {

  // ─── Lifecycle ─────────────────────────────────────────────────────

  /**
   * Bring this transport up. Idempotent — safe to call after `stop()`.
   *
   * @param {bigint} localNodeId
   * @returns {Promise<void>}
   */
  async start(localNodeId) {
    throw new Error('Transport.start: not implemented');
  }

  /**
   * Tear down the transport — close all channels, stop heartbeats,
   * release resources. After `stop()` the transport is unusable until
   * `start()` is called again.
   *
   * @returns {Promise<void>}
   */
  async stop() {
    throw new Error('Transport.stop: not implemented');
  }

  /**
   * @returns {bigint} the local node ID this transport was started with
   */
  getLocalNodeId() {
    throw new Error('Transport.getLocalNodeId: not implemented');
  }

  // ─── Channel pool ──────────────────────────────────────────────────
  //
  // The protocol declares which peers it wants persistent channels to.
  // Maps cleanly to synaptome semantics:
  //
  //   protocol admits peer to synaptome → openConnection(peerId)
  //   protocol evicts peer from synaptome → closeConnection(peerId)
  //
  // Bilateral-cap enforcement lives inside the transport. The remote
  // may refuse a connection if its connection slots are full;
  // `openConnection` resolves with `false` in that case.

  /**
   * Open a persistent channel to peer. Resolves with `true` if the
   * channel is open and both sides accepted; `false` if the remote
   * refused (e.g., bilateral cap exceeded) or is unreachable.
   *
   * Connection-setup cost (WebRTC ICE/DTLS in production, ~free in
   * simulator) is paid here, not per-message.
   *
   * @param {bigint} peerId
   * @returns {Promise<boolean>}
   */
  async openConnection(peerId) {
    throw new Error('Transport.openConnection: not implemented');
  }

  /**
   * Close the channel to peer. Idempotent. The protocol calls this
   * when evicting a peer from its synaptome.
   *
   * @param {bigint} peerId
   * @returns {Promise<void>}
   */
  async closeConnection(peerId) {
    throw new Error('Transport.closeConnection: not implemented');
  }

  /**
   * @param {bigint} peerId
   * @returns {boolean} true if a channel is currently open to this peer
   */
  isConnected(peerId) {
    throw new Error('Transport.isConnected: not implemented');
  }

  // ─── Messaging ─────────────────────────────────────────────────────
  //
  // Two primitives: request/response (`send`) and one-way (`notify`).
  //
  // Use `send` when the caller needs the remote's return value (e.g.,
  // two-hop AP probe: "what is your closest peer to target X?").
  //
  // Use `notify` when the caller does not need a response (e.g., LTP
  // reinforcement walk, triadic introduction, hop-cache write). Notify
  // is fire-and-forget; the round-trip latency cost is avoided.
  //
  // For parallel probes, the canonical pattern is:
  //
  //   const results = await Promise.allSettled(
  //     peers.map(p => transport.send(p, 'find_closest', {target}))
  //   );
  //
  // `Promise.allSettled` (not `Promise.all`) is the right choice — a
  // slow or dead peer in one probe should not fail the whole batch.

  /**
   * Request/response over the open channel. Resolves with the remote's
   * handler return value. Rejects if the channel is closed, the remote
   * times out, or the remote handler throws.
   *
   * @param {bigint} peerId
   * @param {string} type       — application-level message type
   * @param {*}      payload    — JSON-serializable
   * @returns {Promise<*>}
   */
  async send(peerId, type, payload) {
    throw new Error('Transport.send: not implemented');
  }

  /**
   * Fire-and-forget notification — no response expected. Resolves once
   * the message is enqueued for transmission, NOT when it has been
   * delivered. The receiver's handler return value is discarded.
   *
   * @param {bigint} peerId
   * @param {string} type
   * @param {*}      payload
   * @returns {Promise<void>}
   */
  async notify(peerId, type, payload) {
    throw new Error('Transport.notify: not implemented');
  }

  // ─── Inbound dispatch ──────────────────────────────────────────────
  //
  // Protocol code registers handlers per message type. Only one handler
  // per (type, kind) — calling twice with the same type replaces the
  // previous handler.

  /**
   * Register a handler for inbound request/response messages of a
   * given type. The handler's return value is sent back to the caller
   * as the `send()` resolution.
   *
   * Throwing from the handler causes the caller's `send()` to reject
   * with the thrown error.
   *
   * @param {string} type
   * @param {(fromId: bigint, payload: *) => Promise<*>} handler
   * @returns {void}
   */
  onRequest(type, handler) {
    throw new Error('Transport.onRequest: not implemented');
  }

  /**
   * Register a handler for inbound one-way notifications of a given
   * type. The handler's return value is ignored. Throws inside the
   * handler are logged but do not propagate to the sender.
   *
   * @param {string} type
   * @param {(fromId: bigint, payload: *) => void} handler
   * @returns {void}
   */
  onNotification(type, handler) {
    throw new Error('Transport.onNotification: not implemented');
  }

  // ─── Liveness & latency ────────────────────────────────────────────
  //
  // The transport runs a 1 Hz ping/pong heartbeat on every open channel.
  // Each round-trip updates the channel's RTT measurement; missed pongs
  // (HEARTBEAT_TIMEOUT_MS, default 3000ms) trigger channel close +
  // `onPeerDied`.
  //
  // The protocol consumes:
  //   - `getLatency(peerId)` to update each Synapse's `latency` field
  //     each time it's used in AP scoring.
  //   - `onPeerDied(handler)` to fire `_evictAndReplace` and reheat
  //     temperature.

  /**
   * Register a callback for peer-died events. Multiple handlers
   * allowed; each receives every event.
   *
   * @param {(peerId: bigint) => void} handler
   * @returns {() => void} function to unsubscribe this handler
   */
  onPeerDied(handler) {
    throw new Error('Transport.onPeerDied: not implemented');
  }

  /**
   * Most recent observed RTT to peer in milliseconds, or -1 if no
   * measurement is available (channel not open, or no completed
   * heartbeat round-trip yet).
   *
   * @param {bigint} peerId
   * @returns {number} RTT in ms, or -1
   */
  getLatency(peerId) {
    throw new Error('Transport.getLatency: not implemented');
  }
}
