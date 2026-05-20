// =====================================================================
// wstransport.js — Node-side Transport contract over WebSocket(s).
//
// Operates in two modes:
//
//   - server mode: many peers (the bridge accepts inbound WSs from
//     browsers).  bindPeer(nodeId, connId) called per peer; nodeId↔
//     connId map tracks all of them.
//
//   - client mode: single-peer (a Node consumer connecting OUT to a
//     bridge).  Same surface, just one entry in the map.
//
// In both modes the constructor takes a `sendToConn(connId, msg)`
// hook and an `isConnOpen(connId)` predicate so the WebSocket plumbing
// itself (ws.WebSocketServer in the bridge; `ws` library client
// socket in a Node consumer) lives outside the transport.
//
// Wire envelope (symmetric with browser BridgeTransport):
//
//     { type: 'axona', payload: { k: 'req'|'res'|'ntf', ... } }
//
// nodeId convention: 66-char lowercase hex strings throughout the API
// surface.  Internal connId is opaque to the transport — strings.
// =====================================================================

import { Transport }                from '../../contracts/Transport.js';
import { isHexId }                  from '../../utils/hexid.js';
import { TransportError, ErrorCodes } from '../../errors.js';

const DEFAULT_REQUEST_TIMEOUT_MS = 5000;
const MAX_REQ_ID                 = 0x7fffffff;

export class WebSocketTransport extends Transport {
  /**
   * @param {object} opts
   * @param {string} [opts.localNodeId]   66-char hex; set via start() if omitted
   * @param {(connId: string, msg: object) => boolean} opts.sendToConn
   * @param {(connId: string) => boolean}              opts.isConnOpen
   * @param {(event: string, data?: object) => void}   [opts.log]
   * @param {number}                                    [opts.requestTimeoutMs]
   */
  constructor({
    localNodeId = null,
    sendToConn,
    isConnOpen,
    log = () => {},
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  } = {}) {
    super();
    if (typeof sendToConn !== 'function' || typeof isConnOpen !== 'function') {
      throw new TypeError('WebSocketTransport: sendToConn + isConnOpen required');
    }
    if (localNodeId !== null && !isHexId(localNodeId)) {
      throw new TypeError(`WebSocketTransport: localNodeId must be 66-char hex, got ${typeof localNodeId}`);
    }

    this._localNodeId      = localNodeId;
    this._sendToConn       = sendToConn;
    this._isConnOpen       = isConnOpen;
    this._log              = log;
    this._requestTimeoutMs = requestTimeoutMs;

    /** @type {Map<string, (fromId: string|null, payload: any) => Promise<any>>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, (fromId: string|null, payload: any) => void>} */
    this._ntfHandlers = new Map();

    /** @type {Map<number, { nodeId: string, resolve: Function, reject: Function, timer: any }>} */
    this._pending = new Map();
    this._nextId  = 1;

    /** @type {Array<(nodeId: string) => void>} */
    this._peerDiedHandlers = [];

    // nodeId ↔ connId binding (string ↔ string).
    /** @type {Map<string, string>} */ this._connIdByNodeId = new Map();
    /** @type {Map<string, string>} */ this._nodeIdByConnId = new Map();

    this._started = false;
  }

  // ─── Lifecycle ────────────────────────────────────────────────────

  async start(localNodeId) {
    if (localNodeId !== undefined) {
      if (!isHexId(localNodeId)) {
        throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
          `WebSocketTransport.start: localNodeId must be 66-char hex, got ${typeof localNodeId}`);
      }
      this._localNodeId = localNodeId;
    }
    this._started = true;
  }

  async stop() {
    for (const [, p] of this._pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        'WebSocketTransport stopped'));
    }
    this._pending.clear();
    this._started = false;
  }

  getLocalNodeId() { return this._localNodeId; }

  // ─── nodeId ↔ connId binding ──────────────────────────────────────

  bindPeer(nodeId, connId) {
    if (!isHexId(nodeId)) {
      throw new TypeError(`bindPeer: nodeId must be 66-char hex, got ${typeof nodeId}`);
    }
    if (typeof connId !== 'string') {
      throw new TypeError(`bindPeer: connId must be string, got ${typeof connId}`);
    }
    this._connIdByNodeId.set(nodeId, connId);
    this._nodeIdByConnId.set(connId, nodeId);
  }

  unbindPeer(connId) {
    const nodeId = this._nodeIdByConnId.get(connId);
    if (nodeId !== undefined) this._connIdByNodeId.delete(nodeId);
    this._nodeIdByConnId.delete(connId);
  }

  connIdFor(nodeId) { return this._connIdByNodeId.get(nodeId) ?? null; }
  nodeIdFor(connId) { return this._nodeIdByConnId.get(connId) ?? null; }

  // ─── Channel pool ─────────────────────────────────────────────────

  async openConnection(nodeId) {
    const connId = this._connIdByNodeId.get(nodeId);
    return connId != null && this._isConnOpen(connId);
  }

  async closeConnection(nodeId) {
    const connId = this._connIdByNodeId.get(nodeId);
    if (connId) this.unbindPeer(connId);
  }

  isConnected(nodeId) {
    const connId = this._connIdByNodeId.get(nodeId);
    return connId != null && this._isConnOpen(connId);
  }

  // ─── Messaging ────────────────────────────────────────────────────

  async send(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'WebSocketTransport.send: not started');
    }
    const connId = this._connIdByNodeId.get(nodeId);
    if (!connId || !this._isConnOpen(connId)) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        `WebSocketTransport.send: peer ${nodeId} not connected`,
        { context: { nodeId, type } });
    }
    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_TIMEOUT,
          `WebSocketTransport.send: timeout awaiting '${type}'`,
          { context: { nodeId, type } }));
      }, this._requestTimeoutMs);
      this._pending.set(id, { nodeId, resolve, reject, timer });

      try {
        this._sendToConn(connId, { type: 'axona', payload: { k: 'req', id, type, body } });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
          `WebSocketTransport.send: WS write failed (${err.message})`,
          { cause: err, context: { nodeId, type } }));
      }
    });
  }

  async notify(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'WebSocketTransport.notify: not started');
    }
    const connId = this._connIdByNodeId.get(nodeId);
    if (!connId) {
      this._log('ws-notify-no-binding', { nodeId, type });
      return;
    }
    if (!this._isConnOpen(connId)) {
      this._log('ws-notify-conn-closed', { nodeId, connId, type });
      return;
    }
    try {
      this._sendToConn(connId, { type: 'axona', payload: { k: 'ntf', type, body } });
    } catch (err) {
      this._log('notify-failed', { nodeId, type, err: err.message });
    }
  }

  onRequest(type, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onRequest: handler must be a function');
    }
    this._reqHandlers.set(type, handler);
  }

  onNotification(type, handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onNotification: handler must be a function');
    }
    this._ntfHandlers.set(type, handler);
  }

  // ─── Liveness & latency ───────────────────────────────────────────

  onPeerDied(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPeerDied: handler must be a function');
    }
    this._peerDiedHandlers.push(handler);
    return () => {
      const i = this._peerDiedHandlers.indexOf(handler);
      if (i >= 0) this._peerDiedHandlers.splice(i, 1);
    };
  }

  /** RTT in ms. Bridges sample RTT in their own ping loop; this is a
   *  sentinel until the orchestrator threads through measured RTT. */
  getLatency(_nodeId) { return 50; }

  // ─── Inbound dispatch ─────────────────────────────────────────────

  /**
   * Called by the orchestrator when an `{ type: 'axona', payload }`
   * message arrives on a WebSocket. `connId` is the per-WS id the
   * orchestrator assigned (e.g. the bridge's `welcome.connId`).
   *
   * @param {string} connId
   * @param {object} payload
   */
  handleIncoming(connId, payload) {
    if (!payload || typeof payload !== 'object') return;
    const fromNodeId = this._nodeIdByConnId.get(connId) ?? null;

    if (payload.k === 'req') {
      this._handleRequest(connId, fromNodeId, payload);
    } else if (payload.k === 'res') {
      this._handleResponse(payload);
    } else if (payload.k === 'ntf') {
      this._handleNotification(connId, fromNodeId, payload);
    }
  }

  async _handleRequest(connId, fromNodeId, msg) {
    const handler = this._reqHandlers.get(msg.type);
    if (!handler) {
      this._reply(connId, msg.id, false, { error: `no handler for '${msg.type}'` });
      return;
    }
    try {
      const result = await handler(fromNodeId, msg.body);
      this._reply(connId, msg.id, true, result);
    } catch (err) {
      this._reply(connId, msg.id, false, { error: err.message ?? String(err) });
    }
  }

  _reply(connId, id, ok, body) {
    try {
      this._sendToConn(connId, { type: 'axona', payload: { k: 'res', id, ok, body } });
    } catch (err) {
      this._log('reply-failed', { connId, id, err: err.message });
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
        `remote handler error: ${errMsg}`,
        { context: { remoteError: errMsg } }));
    }
  }

  _handleNotification(connId, fromNodeId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;
    try {
      // Pre-bind notifications carry connId in the fromId slot so the
      // orchestrator can bind on receipt.
      handler(fromNodeId ?? connId, msg.body);
    } catch (err) {
      this._log('ntf-handler-threw', { type: msg.type, err: err.message });
    }
  }

  /**
   * Called by the orchestrator when a WebSocket closes. If the conn
   * was bound to a nodeId, fire onPeerDied and unbind.
   *
   * @param {string} connId
   */
  handleConnClosed(connId) {
    const nodeId = this._nodeIdByConnId.get(connId);
    const reported = nodeId ?? connId;
    for (const h of this._peerDiedHandlers) {
      try { h(reported); }
      catch (err) { this._log('peer-died-handler-threw', { err: err.message }); }
    }
    for (const [id, p] of this._pending.entries()) {
      if (p.nodeId !== nodeId) continue;
      clearTimeout(p.timer);
      this._pending.delete(id);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `peer ${nodeId} died`, { context: { nodeId } }));
    }
    this.unbindPeer(connId);
  }
}
