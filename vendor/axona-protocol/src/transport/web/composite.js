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
// Conformance: this class implements the same surface as the
// concrete transports.  Adding/removing sub-transports at runtime
// is allowed; new sub-transports inherit the currently-registered
// handlers.
// =====================================================================

import { Transport }       from '../../contracts/Transport.js';
import { isHexId }         from '../../utils/hexid.js';
import { TransportError, ErrorCodes } from '../../errors.js';

export class CompositeTransport extends Transport {
  /**
   * @param {Object} opts
   * @param {string} opts.localNodeId   66-char hex node ID
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor({ localNodeId, log }) {
    super();
    if (!isHexId(localNodeId)) {
      throw new TypeError(`CompositeTransport: localNodeId must be 66-char hex, got ${typeof localNodeId}`);
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
  }

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    if (this._started) return;
    for (const t of this._subs) await t.start(localNodeId);
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
        `CompositeTransport.send: no route to ${nodeId}`,
        { context: { nodeId, type } });
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

  getLatency(nodeId) {
    const t = this._routeFor(nodeId);
    return t ? t.getLatency(nodeId) : -1;
  }

  // ── Convenience: ask every sub-transport for its mapping ──────────

  /** Reverse-lookup nodeId from a mesh-layer connId / meshId.  Tries
   *  every sub-transport that exposes the helper; returns the first
   *  hit, or null. */
  nodeIdFor(channelId) {
    for (const t of this._subs) {
      if (typeof t.nodeIdFor !== 'function') continue;
      const id = t.nodeIdFor(channelId);
      if (id != null) return id;
    }
    return null;
  }

  /** Forward-lookup the channel id (meshId or 'bridge') for a nodeId. */
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
