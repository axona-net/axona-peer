// =====================================================================
// bridge_transport.js — client-side Transport implementation that
//                       carries Axona wire frames over the existing
//                       browser ↔ bridge WebSocket connection.
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
// where the payload is a standard Axona wire frame.  The browser
// sends frames by writing this envelope to the bridge WebSocket;
// receives by routing inbound `axona`-typed messages to
// `handleIncoming(payload)` from client.js.
//
// Lifecycle:
//   - Construct with { sendToBridge, isBridgeOpen, log }
//   - Start with the browser's local nodeId
//   - The application sends `hello` notification to the bridge
//     (via the existing client.js wiring or a built-in helper);
//     the bridge replies with `hello-ack` carrying its nodeId.
//   - On hello-ack receipt, bindPeer(bridgeNodeId, 'bridge') is
//     called by the orchestrator (axona_node.js).
//   - From there, send/notify/onRequest etc. work uniformly.
// =====================================================================

import { Transport } from '../vendor/axona-protocol/src/index.js';

const REQUEST_TIMEOUT_MS = 5000;
const MAX_REQ_ID = 0x7fffffff;

const BRIDGE_CONN_ID = 'bridge';  // stable mesh-side id for the bridge

export class BridgeTransport extends Transport {
  /**
   * @param {Object} opts
   * @param {bigint} [opts.localNodeId]  set via start() if omitted
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

    // Single binding: the bridge's nodeId (BigInt) ↔ the fixed
    // 'bridge' connId.  Set by bindPeer once hello-ack arrives.
    /** @type {bigint | null} */
    this._bridgeNodeId = null;

    this._started = false;
  }

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    this._started = true;
  }

  async stop() {
    for (const [, p] of this._pending) {
      clearTimeout(p.timer);
      p.reject(new Error('transport-stopped'));
    }
    this._pending.clear();
    this._started = false;
  }

  getLocalNodeId() { return this._localNodeId; }

  // ── nodeId binding (single peer: the bridge) ──────────────────────

  bindPeer(nodeId, connId) {
    if (typeof nodeId !== 'bigint') throw new TypeError('nodeId must be bigint');
    if (connId !== BRIDGE_CONN_ID) {
      throw new Error(`BridgeTransport bind expects connId='${BRIDGE_CONN_ID}', got ${connId}`);
    }
    this._bridgeNodeId = nodeId;
  }

  unbindPeer(_connId) {
    this._bridgeNodeId = null;
  }

  connIdFor(nodeId) {
    return (this._bridgeNodeId !== null && this._bridgeNodeId === nodeId) ? BRIDGE_CONN_ID : null;
  }

  nodeIdFor(connId) {
    return (connId === BRIDGE_CONN_ID) ? this._bridgeNodeId : null;
  }

  /** True if this transport knows about this peer (i.e., it's the bridge). */
  ownsPeer(nodeId) {
    return this._bridgeNodeId !== null && this._bridgeNodeId === nodeId;
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
    if (!this._started) throw new Error('BridgeTransport.send: not started');
    if (!this.ownsPeer(nodeId) || !this._isBridgeOpen()) {
      throw new Error(`BridgeTransport.send: peer ${nodeId} not connected`);
    }

    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error('timeout'));
      }, REQUEST_TIMEOUT_MS);
      this._pending.set(id, { nodeId, resolve, reject, timer });

      try {
        this._sendToBridge({ type: 'axona', payload: { k: 'req', id, type, body } });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(err);
      }
    });
  }

  async notify(nodeId, type, body) {
    if (!this._started) throw new Error('BridgeTransport.notify: not started');
    // Hello / hello-ack flow special case: the orchestrator may want
    // to send a notification BEFORE bindPeer happens (e.g., a hello
    // reply targeting BRIDGE_CONN_ID).  Allow that by accepting the
    // sentinel.  Otherwise: peer must be the bound bridge.
    if (!this.ownsPeer(nodeId) && nodeId !== BRIDGE_CONN_ID) return;
    if (!this._isBridgeOpen()) return;
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
    const fromNodeId = this._bridgeNodeId;   // null until bindPeer

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
      pending.reject(new Error(errMsg));
    }
  }

  _handleNotification(fromNodeId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;
    try {
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
      p.reject(new Error('peer-died'));
    }
    this._bridgeNodeId = null;
  }
}

// Export the sentinel for orchestrators that need it (axona_node.js).
export const BRIDGE_CONN_ID_EXPORT = BRIDGE_CONN_ID;
