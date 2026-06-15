// =====================================================================
// composite_transport.js — fan-out Transport that delegates each
//                          peer to the appropriate sub-transport.
//
// In the browser peer the AxonaPeer has ONE transport reference.
// We need to address both:
//   - Mesh peers (other browsers): WebRTCTransport over RTCDataChannel
//   - The bridge (one peer): BridgeTransport over the bridge WebSocket
//
// CompositeTransport holds a list of sub-transports.  For
// send/notify/openConnection/isConnected/getLatency, it asks each
// sub-transport "do you own this peer?" via the optional `ownsPeer`
// method (or falls back to isConnected) and routes to the first
// hit.  Handler registrations (onRequest/onNotification/onPeerDied)
// fan out — when a handler is registered, we register it on every
// sub-transport so incoming messages from either channel reach it.
//
// The component sub-transports keep their own nodeId↔connId bindings
// internally.  The orchestrator (axona_node.js) calls bindPeer on
// the correct sub-transport directly when each handshake completes.
//
// nodeId convention: 264-bit BigInt throughout the public surface
// here.  The sub-transports likewise speak BigInt internally; hex is
// only on the wire (JSON payloads) and at user-facing display points.
// =====================================================================

import { Transport }       from '../../contracts/Transport.js';
import { TransportError, ErrorCodes } from '../../errors.js';

export class CompositeTransport extends Transport {
  /**
   * @param {Object} opts
   * @param {bigint} opts.localNodeId   264-bit BigInt nodeId
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor({ localNodeId, log }) {
    super();
    if (typeof localNodeId !== 'bigint') {
      throw new TypeError(`CompositeTransport: localNodeId must be bigint, got ${typeof localNodeId}`);
    }
    this._localNodeId = localNodeId;
    this._log         = log ?? (() => {});

    /** @type {Transport[]} */
    this._subs = [];

    // Track registered handlers so newly-added sub-transports
    // inherit them.
    /** @type {Map<string, Function>} */ this._reqHandlers = new Map();
    /** @type {Map<string, Function>} */ this._ntfHandlers = new Map();
    /** @type {Function[]}            */ this._peerDiedHandlers = [];
    // onPeerBound is registered per-handler via a registrar closure so a
    // sub-transport added AFTER onPeerBound() was called (e.g. an uplink added
    // post-start) still propagates its bound peers. Without this, late subs
    // never reach the routing layer and their mesh peers never enter the
    // synaptome.
    /** @type {Array<(t: Transport) => void>} */ this._peerBoundRegistrars = [];

    this._started = false;
  }

  /**
   * Add a sub-transport.  If start() has already been called, the
   * new sub-transport inherits the currently-registered handlers
   * (idempotent — register on the sub-transport).
   */
  addSubtransport(t) {
    this._subs.push(t);
    // Replay handler registrations to the new sub-transport.
    for (const [type, h] of this._reqHandlers) t.onRequest(type, h);
    for (const [type, h] of this._ntfHandlers) t.onNotification(type, h);
    for (const h of this._peerDiedHandlers)    t.onPeerDied(h);
    for (const reg of this._peerBoundRegistrars) reg(t);
  }

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    if (this._started) return;
    for (const t of this._subs) await t.start(this._localNodeId);
    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    for (const t of this._subs) await t.stop();
    this._started = false;
  }

  getLocalNodeId() { return this._localNodeId; }

  // ── Routing: pick the sub-transport that owns this peer ─────────────
  //
  // Two ways a sub-transport identifies "its" peer:
  //   - explicit `ownsPeer(nodeId)` method (BridgeTransport implements this)
  //   - isConnected(nodeId) === true (WebRTCTransport's natural answer)
  // We prefer the explicit method when present (cheaper for the
  // single-peer BridgeTransport) and fall back to isConnected.

  _routeFor(nodeId) {
    for (const t of this._subs) {
      if (typeof t.ownsPeer === 'function') {
        if (t.ownsPeer(nodeId)) return t;
      } else {
        if (t.isConnected(nodeId)) return t;
      }
    }
    return null;
  }

  /**
   * Aggregate boundPeers() across sub-transports.  Each sub may
   * implement `boundPeers()` (BridgeTransport, WebRTCTransport) to
   * report the BigInt nodeIds it has admitted via its own handshake.
   * AxonaPeer.start() consumes this to auto-admit peers into the
   * synaptome, so consumers don't have to wire the synapse by hand
   * after a webTransport handshake.
   *
   * Sub-transports without `boundPeers()` contribute nothing here;
   * the SimNetwork-only path (dht-sim, tests) keeps its existing
   * synaptome-seeding flow.
   *
   * @returns {bigint[]} deduplicated list of bound nodeIds
   */
  boundPeers() {
    const seen = new Set();
    for (const t of this._subs) {
      if (typeof t.boundPeers !== 'function') continue;
      for (const id of t.boundPeers()) {
        if (typeof id === 'bigint') seen.add(id);
      }
    }
    return [...seen];
  }

  /**
   * Subscribe to bind events across all sub-transports that emit them.
   * The composite's handler fires for every new peer bound on any sub,
   * deduplicated across sub-transports (a peer that gets bound on both
   * the bridge and the mesh fires once).
   *
   * @param {(nodeIdBig: bigint) => void} handler
   * @returns {() => void} unsubscribe
   */
  onPeerBound(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPeerBound: handler must be a function');
    }
    // Dedup the fan-out so `handler` fires once per peer even if more than one
    // sub-transport binds the same nodeId.  The dedup MUST be re-armed when the
    // peer dies — otherwise it is PERMANENT: a peer that drops and later
    // reconnects (churn, or a bridgeless relay reconnect) would never re-fire
    // onPeerBound, so the routing layer never re-admits it to the synaptome and
    // ignores a peer it is actually connected to.  Clearing the nodeId from
    // `seen` on peer-death lets the next bind re-fire.
    const seen = new Set();
    const wrapped = (nodeIdBig) => {
      if (typeof nodeIdBig !== 'bigint') return;
      if (seen.has(nodeIdBig)) return;
      seen.add(nodeIdBig);
      try { handler(nodeIdBig); }
      catch (err) { this._log?.('peer-bound-fanout-threw', { err: err.message }); }
    };
    const rearm = (nodeIdBig) => { if (typeof nodeIdBig === 'bigint') seen.delete(nodeIdBig); };
    const unsubs = [];
    // A registrar wires this handler onto one sub-transport. Stored so that
    // subs added later (addSubtransport) inherit it too — same `seen` set, so
    // dedup stays correct across all subs including late ones.
    const register = (t) => {
      if (typeof t.onPeerBound === 'function') unsubs.push(t.onPeerBound(wrapped));
      if (typeof t.onPeerDied  === 'function') unsubs.push(t.onPeerDied(rearm));
    };
    this._peerBoundRegistrars.push(register);
    for (const t of this._subs) register(t);
    return () => {
      const i = this._peerBoundRegistrars.indexOf(register);
      if (i >= 0) this._peerBoundRegistrars.splice(i, 1);
      for (const u of unsubs) try { u(); } catch { /* swallow */ }
    };
  }

  // ── Channel pool ────────────────────────────────────────────────────

  async openConnection(nodeId) {
    const t = this._routeFor(nodeId);
    if (!t) return false;
    return t.openConnection(nodeId);
  }

  async closeConnection(nodeId) {
    const t = this._routeFor(nodeId);
    if (t) await t.closeConnection(nodeId);
  }

  isConnected(nodeId) {
    const t = this._routeFor(nodeId);
    return t != null && t.isConnected(nodeId);
  }

  // ── Messaging ───────────────────────────────────────────────────────

  async send(nodeId, type, body) {
    const t = this._routeFor(nodeId);
    if (!t) {
      throw new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `CompositeTransport.send: no route to ${String(nodeId)}`,
        { context: { nodeId: String(nodeId), type } });
    }
    return t.send(nodeId, type, body);
  }

  async notify(nodeId, type, body) {
    const t = this._routeFor(nodeId);
    if (!t) {
      // Fire-and-forget but log: pubsub diagnostics correlate with
      // this when fan-out targets can't be reached.
      this._log('notify-no-route', { nodeId: String(nodeId), type });
      return;
    }
    return t.notify(nodeId, type, body);
  }

  onRequest(type, handler) {
    this._reqHandlers.set(type, handler);
    for (const t of this._subs) t.onRequest(type, handler);
  }

  onNotification(type, handler) {
    this._ntfHandlers.set(type, handler);
    for (const t of this._subs) t.onNotification(type, handler);
  }

  // ── Liveness & latency ─────────────────────────────────────────────

  onPeerDied(handler) {
    this._peerDiedHandlers.push(handler);
    const unsubs = this._subs.map(t => t.onPeerDied(handler));
    return () => {
      const i = this._peerDiedHandlers.indexOf(handler);
      if (i >= 0) this._peerDiedHandlers.splice(i, 1);
      for (const u of unsubs) try { u(); } catch {}
    };
  }

  /**
   * v2.0.2 — Aggregate onPingTraffic across sub-transports.  Only
   * sub-transports that implement it contribute; the rest silently
   * skip.  Returns an unsubscribe that detaches from all of them.
   */
  onPingTraffic(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPingTraffic: handler must be a function');
    }
    const unsubs = [];
    for (const t of this._subs) {
      if (typeof t.onPingTraffic === 'function') {
        unsubs.push(t.onPingTraffic(handler));
      }
    }
    return () => { for (const u of unsubs) try { u(); } catch {} };
  }

  getLatency(nodeId) {
    const t = this._routeFor(nodeId);
    return t ? t.getLatency(nodeId) : -1;
  }

  // ── Convenience: ask every sub-transport for its mapping ──────────

  /** Reverse-lookup BigInt nodeId from a mesh-layer connId / meshId.
   *  Tries every sub-transport that exposes the helper; returns the
   *  first hit, or null.
   *  @param {string} channelId
   *  @returns {bigint|null} */
  nodeIdFor(channelId) {
    for (const t of this._subs) {
      if (typeof t.nodeIdFor !== 'function') continue;
      const id = t.nodeIdFor(channelId);
      if (id != null) return id;
    }
    return null;
  }

  /** Forward-lookup the channel id (meshId or 'bridge') for a BigInt nodeId.
   *  @param {bigint} nodeId */
  channelIdFor(nodeId) {
    for (const t of this._subs) {
      if (typeof t.meshIdFor === 'function') {
        const id = t.meshIdFor(nodeId);
        if (id != null) return id;
      }
      if (typeof t.connIdFor === 'function') {
        const id = t.connIdFor(nodeId);
        if (id != null) return id;
      }
    }
    return null;
  }
}
