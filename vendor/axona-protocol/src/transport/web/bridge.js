// =====================================================================
// bridge.js — BridgeTransport: client-side Transport implementation
//             that carries Axona wire frames over a single browser ↔
//             bridge WebSocket connection.
//
// Symmetric counterpart to axona-bridge/src/ws_transport.js.  Each
// browser has at most one bridge WebSocket, so this transport
// manages a single peer relationship (browser ↔ bridge) — unlike
// WebRTCTransport which manages many (one per mesh peer).
//
// Wire envelope (matches the bridge):
//
//     { type: 'axona', payload: { k: 'req'|'res'|'ntf', ... } }
//
// where the payload is a standard Axona wire frame.  Outbound frames
// are written via the constructor-provided `sendToBridge` hook;
// inbound frames arrive via `handleIncoming(payload)` called by the
// caller's WS message dispatcher.
//
// Lifecycle:
//   - Construct with { sendToBridge, isBridgeOpen, log }
//   - Start with the browser's local hex nodeId (converted internally
//     to BigInt; the contract surface is BigInt-only for nodeId).
//   - The webTransport orchestrator sends a `hello` notification to
//     the bridge; the bridge replies with `hello-ack` carrying its
//     nodeId (hex on the wire).  On hello-ack receipt the dispatcher
//     converts to BigInt and calls bindPeer(bridgeNodeIdBig, 'bridge').
//   - From there, send / notify / onRequest etc. work uniformly with
//     BigInt nodeIds in the kernel; hex appears only on the wire.
//
// nodeId convention: BigInt in memory; 66-char lowercase hex on the
// wire and as the user-facing display form.
// =====================================================================

import { Transport }            from '../../contracts/Transport.js';
import { TransportError, ErrorCodes } from '../../errors.js';

const REQUEST_TIMEOUT_MS = 5000;
const MAX_REQ_ID = 0x7fffffff;

const BRIDGE_CONN_ID = 'bridge';  // stable mesh-side id for the bridge

export class BridgeTransport extends Transport {
  /**
   * @param {Object} opts
   * @param {bigint} [opts.localNodeId]  BigInt nodeId; set via start() if omitted
   * @param {(msg: object) => boolean} opts.sendToBridge
   *        Synchronous send: serializes `msg` and writes to the
   *        bridge WebSocket.  Returns true if the socket accepted
   *        the frame.  Throws if the socket is closed.
   * @param {() => boolean} opts.isBridgeOpen
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor({ localNodeId = null, sendToBridge, isBridgeOpen, log }) {
    super();
    if (typeof sendToBridge !== 'function' || typeof isBridgeOpen !== 'function') {
      throw new TypeError('BridgeTransport: sendToBridge + isBridgeOpen required');
    }
    this._localNodeId  = localNodeId;
    this._sendToBridge = sendToBridge;
    this._isBridgeOpen = isBridgeOpen;
    this._log          = log ?? (() => {});

    this._reqHandlers = new Map();
    this._ntfHandlers = new Map();
    this._pending     = new Map();
    this._nextId      = 1;
    this._peerDiedHandlers = [];

    // v2.0.2 — Per-frame ping/pong traffic listeners.  The webTransport
    // factory drives the bridge ping loop and the bridge's pong replies
    // arrive on the WebSocket; it calls _emitPingTraffic on this
    // BridgeTransport so callers can subscribe to ping traffic via the
    // Transport contract instead of reaching into the factory.
    /** @type {Array<(nodeId: bigint, kind: 'sent'|'recv') => void>} */
    this._pingTrafficHandlers = [];

    // Single binding: the bridge's BigInt nodeId ↔ the fixed 'bridge'
    // connId.  Set by bindPeer once hello-ack arrives.
    /** @type {bigint | null} */
    this._bridgeNodeId = null;

    this._started = false;
  }

  /**
   * v2.0.2 — Subscribe to per-frame ping/pong traffic on the bridge
   * WebSocket.  Callback receives (bridgeNodeId, kind) where kind is
   * 'sent' on each outgoing ping and 'recv' on each incoming pong.
   * Fires only after the bridge has been bound (hello-ack received);
   * pre-bind ping traffic is silently dropped because there's no
   * stable nodeId to attribute it to yet.
   */
  onPingTraffic(callback) {
    this._pingTrafficHandlers.push(callback);
    return () => {
      const i = this._pingTrafficHandlers.indexOf(callback);
      if (i >= 0) this._pingTrafficHandlers.splice(i, 1);
    };
  }

  /** @internal — webTransport factory calls this on bridge ping send / pong recv. */
  _emitPingTraffic(kind) {
    if (this._bridgeNodeId === null) return;
    if (this._pingTrafficHandlers.length === 0) return;
    for (const cb of this._pingTrafficHandlers) {
      try { cb(this._bridgeNodeId, kind); }
      catch {}
    }
  }

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    this._started = true;
  }

  async stop() {
    for (const [, p] of this._pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        'BridgeTransport stopped'));
    }
    this._pending.clear();
    this._started = false;
  }

  getLocalNodeId() { return this._localNodeId; }

  // ── nodeId binding (single peer: the bridge) ──────────────────────

  /**
   * @param {bigint} nodeId  264-bit BigInt nodeId
   * @param {string} connId  must equal BRIDGE_CONN_ID ('bridge')
   */
  bindPeer(nodeId, connId) {
    if (typeof nodeId !== 'bigint') {
      throw new TypeError(`BridgeTransport.bindPeer: nodeId must be bigint, got ${typeof nodeId}`);
    }
    if (connId !== BRIDGE_CONN_ID) {
      throw new Error(`BridgeTransport bind expects connId='${BRIDGE_CONN_ID}', got ${connId}`);
    }
    const isNew = (this._bridgeNodeId !== nodeId);
    this._bridgeNodeId = nodeId;
    if (isNew && this._peerBoundHandlers) {
      for (const h of this._peerBoundHandlers) {
        try { h(nodeId); }
        catch (err) { this._log?.('peer-bound-handler-threw', { err: err.message }); }
      }
    }
  }

  unbindPeer(_connId) {
    this._bridgeNodeId = null;
  }

  /** @param {bigint} nodeId */
  connIdFor(nodeId) {
    if (this._bridgeNodeId === null) return null;
    return (this._bridgeNodeId === nodeId) ? BRIDGE_CONN_ID : null;
  }

  /** @param {string} connId @returns {bigint|null} */
  nodeIdFor(connId) {
    return (connId === BRIDGE_CONN_ID) ? this._bridgeNodeId : null;
  }

  /** True if this transport knows about this peer (i.e., it's the bridge). */
  ownsPeer(nodeId) {
    if (this._bridgeNodeId === null) return false;
    return this._bridgeNodeId === nodeId;
  }

  /**
   * Currently-bound peer node IDs.  At most one for a BridgeTransport:
   * the bridge's own embedded peer.  Empty until the hello-ack
   * admission completes.  Consumed by AxonaPeer.start() so peers known
   * to the transport are auto-admitted to the synaptome.
   *
   * @returns {bigint[]}
   */
  boundPeers() {
    return this._bridgeNodeId !== null ? [this._bridgeNodeId] : [];
  }

  /**
   * Register a callback that fires when a new peer is bound via
   * `bindPeer(nodeId, connId)`.  Used by AxonaPeer.start() to admit
   * synapses dynamically as the bridge handshake / mesh handshake
   * complete.
   *
   * @param {(nodeIdBig: bigint) => void} handler
   * @returns {() => void} unsubscribe
   */
  onPeerBound(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPeerBound: handler must be a function');
    }
    if (!this._peerBoundHandlers) this._peerBoundHandlers = new Set();
    this._peerBoundHandlers.add(handler);
    // Fire immediately for any peer already bound at subscribe time.
    if (this._bridgeNodeId !== null) {
      try { handler(this._bridgeNodeId); } catch { /* swallow */ }
    }
    return () => { this._peerBoundHandlers?.delete(handler); };
  }

  // ── Channel pool ──────────────────────────────────────────────────

  async openConnection(nodeId) {
    return this.ownsPeer(nodeId) && this._isBridgeOpen();
  }

  async closeConnection(_nodeId) {
    // Bridge channel lifecycle is owned by client.js; nothing to do.
  }

  isConnected(nodeId) {
    return this.ownsPeer(nodeId) && this._isBridgeOpen();
  }

  // ── Messaging ─────────────────────────────────────────────────────

  async send(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'BridgeTransport.send: not started');
    }
    if (!this.ownsPeer(nodeId) || !this._isBridgeOpen()) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        `BridgeTransport.send: peer ${String(nodeId)} not connected`,
        { context: { nodeId: String(nodeId), type } });
    }

    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_TIMEOUT,
          `BridgeTransport.send: timeout awaiting '${type}'`,
          { context: { nodeId: String(nodeId), type } }));
      }, REQUEST_TIMEOUT_MS);
      this._pending.set(id, { nodeId, resolve, reject, timer });

      try {
        this._sendToBridge({ type: 'axona', payload: { k: 'req', id, type, body } });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
          `BridgeTransport.send: bridge write failed (${err.message})`,
          { cause: err, context: { nodeId: String(nodeId), type } }));
      }
    });
  }

  async notify(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'BridgeTransport.notify: not started');
    }
    // Pre-bind hello path: the orchestrator may want to send a
    // notification BEFORE bindPeer happens (e.g. a hello reply targeting
    // BRIDGE_CONN_ID).  Allow the literal 'bridge' sentinel through.
    // Otherwise nodeId must be the bound BigInt bridge id.
    if (nodeId === BRIDGE_CONN_ID) {
      // sentinel — fall through
    } else if (typeof nodeId !== 'bigint') {
      throw new TypeError(`BridgeTransport.notify: nodeId must be bigint or BRIDGE_CONN_ID sentinel, got ${typeof nodeId}`);
    } else if (!this.ownsPeer(nodeId)) {
      this._log('bridge-notify-not-bridge-peer', { nodeId: String(nodeId), type });
      return;
    }
    if (!this._isBridgeOpen()) {
      this._log('bridge-notify-ws-closed', { type });
      return;
    }
    try {
      this._sendToBridge({ type: 'axona', payload: { k: 'ntf', type, body } });
    } catch (err) {
      this._log('notify-failed', { type, err: err.message });
    }
  }

  onRequest(type, handler) {
    if (typeof handler !== 'function') throw new TypeError('onRequest: handler must be a function');
    this._reqHandlers.set(type, handler);
  }

  onNotification(type, handler) {
    if (typeof handler !== 'function') throw new TypeError('onNotification: handler must be a function');
    this._ntfHandlers.set(type, handler);
  }

  onPeerDied(handler) {
    if (typeof handler !== 'function') throw new TypeError('onPeerDied: handler must be a function');
    this._peerDiedHandlers.push(handler);
    return () => {
      const i = this._peerDiedHandlers.indexOf(handler);
      if (i >= 0) this._peerDiedHandlers.splice(i, 1);
    };
  }

  /** Approximate RTT.  Future: thread through the application-ping
   *  rttBuffer from client.js's existing bridge ping loop. */
  getLatency(_nodeId) { return 50; }

  // ── Inbound dispatch ──────────────────────────────────────────────

  /**
   * Called from client.js when an `{type:'axona', payload:...}`
   * message arrives on the bridge WebSocket.
   */
  handleIncoming(payload) {
    if (!payload || typeof payload !== 'object') return;
    const fromNodeId = this._bridgeNodeId;   // BigInt or null (null until bindPeer)

    if (payload.k === 'req') {
      this._handleRequest(fromNodeId, payload);
    } else if (payload.k === 'res') {
      this._handleResponse(payload);
    } else if (payload.k === 'ntf') {
      this._handleNotification(fromNodeId, payload);
    }
  }

  async _handleRequest(fromNodeId, msg) {
    const handler = this._reqHandlers.get(msg.type);
    if (!handler) {
      this._reply(msg.id, false, { error: `no handler for '${msg.type}'` });
      return;
    }
    try {
      const result = await handler(fromNodeId, msg.body);
      this._reply(msg.id, true, result);
    } catch (err) {
      this._reply(msg.id, false, { error: err.message ?? String(err) });
    }
  }

  _reply(id, ok, body) {
    try {
      this._sendToBridge({ type: 'axona', payload: { k: 'res', id, ok, body } });
    } catch (err) {
      this._log('reply-failed', { id, err: err.message });
    }
  }

  _handleResponse(msg) {
    const pending = this._pending.get(msg.id);
    if (!pending) return;
    clearTimeout(pending.timer);
    this._pending.delete(msg.id);
    if (msg.ok) pending.resolve(msg.body);
    else {
      const errMsg = (msg.body && typeof msg.body === 'object')
        ? (msg.body.error ?? 'remote-error') : 'remote-error';
      pending.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `bridge handler error: ${errMsg}`,
        { context: { remoteError: errMsg } }));
    }
  }

  _handleNotification(fromNodeId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;
    try {
      // For pre-bind frames (hello before bindPeer), pass the sentinel
      // string so the orchestrator can recognise the unbound state.
      handler(fromNodeId ?? BRIDGE_CONN_ID, msg.body);
    } catch (err) {
      this._log('ntf-handler-threw', { type: msg.type, err: err.message });
    }
  }

  /** Called by client.js when the bridge WebSocket closes. */
  handleConnClosed() {
    const reported = this._bridgeNodeId ?? BRIDGE_CONN_ID;
    for (const h of this._peerDiedHandlers) {
      try { h(reported); }
      catch (err) { this._log('peer-died-handler-threw', { err: err.message }); }
    }
    for (const [id, p] of this._pending.entries()) {
      clearTimeout(p.timer);
      this._pending.delete(id);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        'bridge connection closed',
        { context: { nodeId: String(p.nodeId) } }));
    }
    this._bridgeNodeId = null;
  }
}

// Export the sentinel for orchestrators that need it (axona_node.js).
export const BRIDGE_CONN_ID_EXPORT = BRIDGE_CONN_ID;
