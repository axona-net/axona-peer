// =====================================================================
// WebRTCTransport — Transport contract over WebRTC data channels.
//
// One instance per running peer.  Takes a MeshManager (the layer below
// that owns RTCPeerConnection + RTCDataChannel + ICE setup +
// application-level ping/pong heartbeat) and wraps it in the typed
// Transport contract: send(peerId, type, body), notify(peerId, …),
// onRequest, onNotification, onPeerDied, getLatency.
//
//     application (peer SDK)
//             │
//             │   DHT contract (lookup / publish / subscribe / …)
//             │
//     AxonaPeer (NH-1 routing + axonal pub/sub)
//             │
//             │   Transport contract
//             │
//     WebRTCTransport   ← this file
//             │
//             │   send(meshId, payload) / onMessage(cb) / onPeerLost(cb)
//             │
//     MeshManager (transport/web/mesh.js — moved in T1 part 2)
//             │
//     WebRTC (ICE/DTLS, TURN as fallback)
//
// At construction the MeshManager isn't required — pass it in via
// start(), or wire it via the webTransport() factory in index.js.
// This lets the transport be constructed before any signalling has
// happened.
//
// nodeId convention:  66-char lowercase hex strings throughout.  The
// Transport contract surface (`send`, `notify`, `openConnection`,
// `onPeerDied`, `getLatency`) all take and emit hex node IDs.  The
// internal nodeId↔meshId binding map is keyed on strings.
//
// MeshManager itself still speaks string `meshId`s (the bridge's
// UUID-ish connection IDs); the two-space binding is unchanged.
// =====================================================================

import { Transport }    from '../../contracts/Transport.js';
import { isHexId }      from '../../utils/hexid.js';
import {
  TransportError,
  ErrorCodes,
}                       from '../../errors.js';

/** Reject `send()` if the remote hasn't responded within this. */
const DEFAULT_REQUEST_TIMEOUT_MS = 5000;

/** Max correlation id before wrapping.  2^31 is plenty per session. */
const MAX_REQ_ID = 0x7fffffff;

/**
 * @typedef {object} MeshLike
 * @property {(meshId: string, msg: any) => void}     send
 * @property {(cb: (meshId: string, msg: any) => void) => () => void} onMessage
 * @property {(cb: (meshId: string) => void) => () => void}           onPeerLost
 * @property {(cb: (peers: any[]) => void) => () => void}              onChange
 * @property {(meshId: string) => boolean}                              isConnected
 * @property {(meshId: string) => number}                               getLatency
 */

export class WebRTCTransport extends Transport {
  /**
   * @param {object}   opts
   * @param {MeshLike} [opts.mesh]
   *        Optional at construction; can be supplied via start() or
   *        wired by the factory.
   * @param {string}   [opts.localNodeId]
   *        66-char hex nodeId.  Optional at construction; supplied
   *        via start() or factory.
   * @param {(event: string, data?: object) => void} [opts.log]
   * @param {number}   [opts.requestTimeoutMs]
   */
  constructor({
    mesh = null,
    localNodeId = null,
    log = () => {},
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  } = {}) {
    super();
    this._mesh             = mesh;
    this._localNodeId      = localNodeId;
    this._log              = log;
    this._requestTimeoutMs = requestTimeoutMs;

    /** @type {Map<string, (fromId: string|null, payload: any) => Promise<any>>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, (fromId: string|null, payload: any) => void>} */
    this._ntfHandlers = new Map();

    /**
     * Outstanding requests awaiting response.  Keyed by correlation id.
     * @type {Map<number, { nodeId: string, resolve: Function, reject: Function, timer: any }>}
     */
    this._pending = new Map();
    this._nextId  = 1;

    /** @type {Array<(nodeId: string) => void>} */
    this._peerDiedHandlers = [];

    // ── Two-space identifier translation ──────────────────────────────
    //
    // Mesh layer uses string `meshId`s (the bridge-assigned connId).
    // Axona protocol layer uses 66-char hex `nodeId`s.  The two coexist
    // until an Axona hello/hello-ack handshake exchanges nodeIds; the
    // application then calls bindPeer(nodeId, meshId) to teach the
    // transport.  After that, the protocol layer addresses peers by
    // their 66-char hex nodeId and the transport routes correctly.
    // Unbind on peer-left to keep maps clean.
    /** @type {Map<string, string>} nodeId(hex) → meshId */
    this._meshIdByNodeId = new Map();
    /** @type {Map<string, string>} meshId → nodeId(hex) */
    this._nodeIdByMeshId = new Map();

    this._started       = false;
    this._unsubMessage  = null;
    this._unsubPeerLost = null;
  }

  // ─── Lifecycle ───────────────────────────────────────────────────────

  /**
   * @param {string} [localNodeId]   66-char hex.  Overrides what was
   *                                 passed at construction time.
   * @param {MeshLike} [mesh]        Optional — overrides constructor mesh.
   */
  async start(localNodeId, mesh) {
    if (mesh !== undefined)        this._mesh        = mesh;
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    if (!this._mesh) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'WebRTCTransport.start: mesh is required');
    }
    if (this._started) return;
    if (this._localNodeId !== null && !isHexId(this._localNodeId)) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        `WebRTCTransport.start: localNodeId must be 66-char hex, got ${typeof this._localNodeId}`,
        { context: { localNodeId: this._localNodeId } });
    }
    this._unsubMessage  = this._mesh.onMessage((peerId, msg) => this._onMessage(peerId, msg));
    this._unsubPeerLost = this._mesh.onPeerLost((peerId)      => this._onPeerLost(peerId));
    this._started = true;
    this._log('transport-started', { localNodeId: this._localNodeId });
  }

  async stop() {
    if (!this._started) return;
    if (this._unsubMessage)  this._unsubMessage();
    if (this._unsubPeerLost) this._unsubPeerLost();
    this._unsubMessage  = null;
    this._unsubPeerLost = null;
    // Reject every outstanding request.
    for (const [, p] of this._pending) {
      clearTimeout(p.timer);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        'transport stopped'));
    }
    this._pending.clear();
    this._started = false;
    this._log('transport-stopped');
  }

  getLocalNodeId() {
    return this._localNodeId;
  }

  // ─── nodeId ↔ meshId binding ─────────────────────────────────────────
  //
  // The Axona protocol layer addresses peers by 66-char hex nodeId.
  // The mesh layer uses opaque string meshIds.  An external
  // orchestrator (the webTransport factory, once T1 part 2 lands)
  // runs the hello/hello-ack handshake on each fresh WebRTC channel;
  // once both sides know each other's nodeId, the orchestrator calls
  // bindPeer(nodeId, meshId).  Then send/notify by nodeId routes via
  // the mesh.

  /**
   * @param {string} nodeId  66-char hex
   * @param {string} meshId
   */
  bindPeer(nodeId, meshId) {
    if (!isHexId(nodeId)) {
      throw new TypeError(`bindPeer: nodeId must be 66-char hex, got ${typeof nodeId}`);
    }
    if (typeof meshId !== 'string') {
      throw new TypeError(`bindPeer: meshId must be string, got ${typeof meshId}`);
    }
    this._meshIdByNodeId.set(nodeId, meshId);
    this._nodeIdByMeshId.set(meshId, nodeId);
    this._log('bindPeer', { nodeId, meshId });
  }

  unbindPeer(meshId) {
    const nodeId = this._nodeIdByMeshId.get(meshId);
    if (nodeId !== undefined) this._meshIdByNodeId.delete(nodeId);
    this._nodeIdByMeshId.delete(meshId);
  }

  /** @param {string} nodeId @returns {string|null} */
  meshIdFor(nodeId) {
    return this._meshIdByNodeId.get(nodeId) ?? null;
  }

  /** @param {string} meshId @returns {string|null} */
  nodeIdFor(meshId) {
    return this._nodeIdByMeshId.get(meshId) ?? null;
  }

  // ─── Channel pool ────────────────────────────────────────────────────

  async openConnection(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId) return false;   // not yet handshaken
    if (this._mesh.isConnected(meshId)) return true;

    return new Promise((resolve) => {
      const unsub = this._mesh.onChange((peers) => {
        const p = peers.find(x => x.peerId === meshId);
        if (!p) {
          unsub(); clearTimeout(timer); resolve(false);
        } else if (p.state === 'open') {
          unsub(); clearTimeout(timer); resolve(true);
        } else if (p.state === 'failed' || p.state === 'closed') {
          unsub(); clearTimeout(timer); resolve(false);
        }
      });
      const timer = setTimeout(() => {
        unsub(); resolve(false);
      }, 15_000);
    });
  }

  async closeConnection(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (meshId) this.unbindPeer(meshId);
  }

  isConnected(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    return meshId != null && this._mesh.isConnected(meshId);
  }

  // ─── Messaging ───────────────────────────────────────────────────────

  async send(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'Transport.send: not started');
    }
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId || !this._mesh.isConnected(meshId)) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        `Transport.send: peer ${nodeId} not connected`,
        { context: { nodeId, type } });
    }

    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_TIMEOUT,
          `Transport.send: timeout awaiting '${type}' from ${nodeId}`,
          { context: { nodeId, type } }));
      }, this._requestTimeoutMs);

      this._pending.set(id, { nodeId, resolve, reject, timer });

      try {
        this._mesh.send(meshId, { k: 'req', id, type, body });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
          `Transport.send: mesh.send failed (${err.message})`,
          { cause: err, context: { nodeId, type } }));
      }
    });
  }

  async notify(nodeId, type, body) {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'Transport.notify: not started');
    }
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId) {
      this._log('notify-no-binding', { nodeId, type });
      return;
    }
    if (!this._mesh.isConnected(meshId)) {
      this._log('notify-not-connected', { nodeId, meshId, type });
      return;
    }
    try {
      this._mesh.send(meshId, { k: 'ntf', type, body });
    } catch (err) {
      this._log('notify-send-failed', { nodeId, type, err: err.message });
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

  // ─── Liveness & latency ──────────────────────────────────────────────

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

  getLatency(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    return meshId ? this._mesh.getLatency(meshId) : -1;
  }

  // ─── Internal: route incoming frames ─────────────────────────────────

  async _onMessage(fromMeshId, msg) {
    if (!msg || typeof msg !== 'object') return;

    // For frames addressed to the transport layer, fromMeshId is the
    // sender's meshId.  Translate to nodeId for the handler dispatch.
    // If a frame arrives from an unbound mesh peer (e.g. hello/hello-ack
    // BEFORE bindPeer is called), let it through with nodeId=null so the
    // orchestrator can intercept and bind.
    const fromNodeId = this._nodeIdByMeshId.get(fromMeshId) ?? null;

    if (msg.k === 'req') {
      await this._handleRequest(fromMeshId, fromNodeId, msg);
    } else if (msg.k === 'res') {
      this._handleResponse(msg);
    } else if (msg.k === 'ntf') {
      this._handleNotification(fromMeshId, fromNodeId, msg);
    }
  }

  async _handleRequest(fromMeshId, fromNodeId, msg) {
    const handler = this._reqHandlers.get(msg.type);
    if (!handler) {
      this._reply(fromMeshId, msg.id, false, { error: `no handler for '${msg.type}'` });
      return;
    }
    try {
      const result = await handler(fromNodeId, msg.body);
      this._reply(fromMeshId, msg.id, true, result);
    } catch (err) {
      this._reply(fromMeshId, msg.id, false, { error: err.message ?? String(err) });
    }
  }

  _reply(meshId, id, ok, body) {
    try {
      this._mesh.send(meshId, { k: 'res', id, ok, body });
    } catch (err) {
      this._log('reply-send-failed', { meshId, id, err: err.message });
    }
  }

  _handleResponse(msg) {
    const pending = this._pending.get(msg.id);
    if (!pending) return;     // late response — handler already timed out
    clearTimeout(pending.timer);
    this._pending.delete(msg.id);
    if (msg.ok) {
      pending.resolve(msg.body);
    } else {
      const errMsg = (msg.body && typeof msg.body === 'object')
        ? (msg.body.error ?? 'remote-error')
        : 'remote-error';
      pending.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `remote handler error: ${errMsg}`,
        { context: { remoteError: errMsg } }));
    }
  }

  _handleNotification(fromMeshId, fromNodeId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;
    try {
      // For pre-bind notifications (hello, hello-ack), fromNodeId is
      // null; handler receives fromMeshId in that slot so the
      // orchestrator can bind on receipt.
      handler(fromNodeId ?? fromMeshId, msg.body);
    } catch (err) {
      this._log('ntf-handler-threw', { type: msg.type, err: err.message });
    }
  }

  _onPeerLost(meshId) {
    const nodeId = this._nodeIdByMeshId.get(meshId);
    // Fan out to peer-died subscribers (translate to nodeId if bound).
    const reportedId = nodeId ?? meshId;
    for (const h of this._peerDiedHandlers) {
      try { h(reportedId); }
      catch (err) {
        this._log('peer-died-handler-threw', { reportedId, err: err.message });
      }
    }
    // Reject every pending request to this peer.
    for (const [id, p] of this._pending.entries()) {
      if (p.nodeId !== nodeId) continue;
      clearTimeout(p.timer);
      this._pending.delete(id);
      p.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `peer ${nodeId} died`, { context: { nodeId } }));
    }
    // Unbind the dead peer last so further sends fail fast.
    this.unbindPeer(meshId);
  }
}
