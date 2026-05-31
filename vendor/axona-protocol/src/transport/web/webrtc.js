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
// nodeId convention:  264-bit BigInt at every Transport-contract
// surface (`send`, `notify`, `openConnection`, `onPeerDied`,
// `getLatency`).  Hex appears only on the wire and at user-facing
// display surfaces.  Internally the nodeId↔meshId binding map is
// keyed by BigInt for nodeId and by string for meshId.
//
// MeshManager itself still speaks string `meshId`s (the bridge's
// UUID-ish 3-char connection IDs); the two-space binding is unchanged.
// =====================================================================

import { Transport }    from '../../contracts/Transport.js';
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
   * @param {bigint}   [opts.localNodeId]
   *        264-bit BigInt nodeId.  Optional at construction; supplied
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

    /** @type {Map<string, (fromId: bigint|string|null, payload: any) => Promise<any>>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, (fromId: bigint|string|null, payload: any) => void>} */
    this._ntfHandlers = new Map();

    /**
     * Outstanding requests awaiting response.  Keyed by correlation id.
     * @type {Map<number, { nodeId: bigint, resolve: Function, reject: Function, timer: any }>}
     */
    this._pending = new Map();
    this._nextId  = 1;

    /** @type {Array<(nodeId: bigint|string) => void>} */
    this._peerDiedHandlers = [];

    // ── Two-space identifier translation ──────────────────────────────
    //
    // Mesh layer uses string `meshId`s (3-char bridge-assigned connIds).
    // Axona protocol layer uses 264-bit BigInt `nodeId`s.  The two
    // coexist until an Axona hello/hello-ack handshake exchanges
    // nodeIds; the application then calls bindPeer(nodeIdBig, meshId)
    // to teach the transport.  After that, the protocol layer addresses
    // peers by BigInt nodeId and the transport routes correctly.
    // Unbind on peer-left to keep maps clean.
    /** @type {Map<bigint, string>} nodeId(BigInt) → meshId */
    this._meshIdByNodeId = new Map();
    /** @type {Map<string, bigint>} meshId → nodeId(BigInt) */
    this._nodeIdByMeshId = new Map();
    /** @type {Map<string, string>} meshId → symmetric channelKey.
     *  Used to deterministically pick the survivor when two channels bind
     *  the same nodeId (glare / reconnect-churn duplicate). */
    this._channelKeyByMeshId = new Map();

    this._started       = false;
    this._unsubMessage  = null;
    this._unsubPeerLost = null;
  }

  // ─── Lifecycle ───────────────────────────────────────────────────────

  /**
   * @param {bigint} [localNodeId]   264-bit BigInt.  Overrides what was
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
    if (this._localNodeId !== null && typeof this._localNodeId !== 'bigint') {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        `WebRTCTransport.start: localNodeId must be bigint, got ${typeof this._localNodeId}`,
        { context: { localNodeId: String(this._localNodeId) } });
    }
    this._unsubMessage  = this._mesh.onMessage((peerId, msg) => this._onMessage(peerId, msg));
    this._unsubPeerLost = this._mesh.onPeerLost((peerId)      => this._onPeerLost(peerId));
    this._started = true;
    this._log('transport-started', { localNodeId: String(this._localNodeId) });
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
  // The Axona protocol layer addresses peers by 264-bit BigInt nodeId.
  // The mesh layer uses opaque string meshIds.  An external orchestrator
  // (the webTransport factory) runs the hello/hello-ack handshake on
  // each fresh WebRTC channel; once both sides know each other's
  // nodeId, the orchestrator calls bindPeer(nodeIdBig, meshId).  Then
  // send/notify by nodeId routes via the mesh.

  /**
   * @param {bigint} nodeId      264-bit BigInt
   * @param {string} meshId
   * @param {string} [channelKey]  Symmetric per-channel identifier (the
   *        sorted axona/4 nonce pair) — IDENTICAL on both endpoints of a
   *        given channel.  Used to deterministically resolve a duplicate
   *        channel to an already-bound identity (see below).  Optional;
   *        when absent the dedup falls back to "keep the existing binding".
   */
  bindPeer(nodeId, meshId, channelKey = null) {
    if (typeof nodeId !== 'bigint') {
      throw new TypeError(`bindPeer: nodeId must be bigint, got ${typeof nodeId}`);
    }
    if (typeof meshId !== 'string') {
      throw new TypeError(`bindPeer: meshId must be string, got ${typeof meshId}`);
    }
    if (channelKey != null) this._channelKeyByMeshId.set(meshId, channelKey);

    const prevMeshId = this._meshIdByNodeId.get(nodeId);

    // ── Duplicate-identity dedup ──────────────────────────────────────
    // Two distinct channels are bound to the SAME peer identity.  This
    // happens on WebRTC "glare" (both sides dialed each other) and after a
    // bridge/signaling restart (the surviving peer-to-peer channel plus a
    // fresh post-reconnect channel both live).  Keep exactly one and tear
    // the other down, so the mesh shows one channel per peer.
    //
    // Survivor selection is by the SYMMETRIC channelKey (sorted nonce pair),
    // which is identical on both endpoints — so both sides pick the SAME
    // survivor without any coordinating message, and can't each kill a
    // different channel (which would drop the peer entirely).  Smaller key
    // wins.  Even one-sided dedup is safe: closing a channel closes it for
    // both ends, and the other end simply keeps the same survivor.
    if (prevMeshId !== undefined && prevMeshId !== meshId) {
      const prevKey = this._channelKeyByMeshId.get(prevMeshId);
      const newWins = (channelKey != null && prevKey != null)
        ? String(channelKey) < String(prevKey)
        : false;                       // no keys → prefer the existing binding
      const winnerMeshId = newWins ? meshId : prevMeshId;
      const loserMeshId  = newWins ? prevMeshId : meshId;
      this._log('mesh-duplicate-deduped', {
        nodeId: String(nodeId), winnerMeshId, loserMeshId,
      });
      // Point the binding at the winner and drop the loser's reverse
      // mapping BEFORE teardown, so the loser's onPeerLost can't unbind the
      // surviving winner or report a spurious peer-died for this identity.
      this._meshIdByNodeId.set(nodeId, winnerMeshId);
      this._nodeIdByMeshId.set(winnerMeshId, nodeId);
      this._nodeIdByMeshId.delete(loserMeshId);
      this._channelKeyByMeshId.delete(loserMeshId);
      try { this._mesh?.disconnect?.(loserMeshId, 'duplicate-nodeId'); }
      catch (err) { this._log('mesh-dedup-disconnect-threw', { loserMeshId, err: err.message }); }
      return;   // identity was already bound — not a new peer, no onPeerBound
    }

    const isNew = prevMeshId === undefined;
    this._meshIdByNodeId.set(nodeId, meshId);
    this._nodeIdByMeshId.set(meshId, nodeId);
    this._log('bindPeer', { nodeId: String(nodeId), meshId });
    if (isNew && this._peerBoundHandlers) {
      for (const h of this._peerBoundHandlers) {
        try { h(nodeId); }
        catch (err) { this._log('peer-bound-handler-threw', { err: err.message }); }
      }
    }
  }

  unbindPeer(meshId) {
    const nodeId = this._nodeIdByMeshId.get(meshId);
    this._nodeIdByMeshId.delete(meshId);
    this._channelKeyByMeshId.delete(meshId);
    // Only clear the forward mapping if THIS meshId is still the active
    // route for the nodeId.  A deduped-duplicate loser must not unbind the
    // surviving winner (which now owns the nodeId under a different meshId).
    if (nodeId !== undefined && this._meshIdByNodeId.get(nodeId) === meshId) {
      this._meshIdByNodeId.delete(nodeId);
    }
  }

  /**
   * Currently-bound mesh peer node IDs.  Consumed by AxonaPeer.start()
   * so peers admitted via the mesh handshake are auto-admitted to the
   * synaptome.
   *
   * @returns {bigint[]}
   */
  boundPeers() {
    return [...this._meshIdByNodeId.keys()];
  }

  /**
   * Register a callback that fires when a new peer is bound via
   * `bindPeer(nodeId, meshId)`.  Fires immediately for any peer
   * already bound at subscribe time.
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
    for (const nodeId of this._meshIdByNodeId.keys()) {
      try { handler(nodeId); } catch { /* swallow */ }
    }
    return () => { this._peerBoundHandlers?.delete(handler); };
  }

  /** @param {bigint} nodeId @returns {string|null} */
  meshIdFor(nodeId) {
    return this._meshIdByNodeId.get(nodeId) ?? null;
  }

  /** @param {string} meshId @returns {bigint|null} */
  nodeIdFor(meshId) {
    return this._nodeIdByMeshId.get(meshId) ?? null;
  }

  /** True if this transport owns a binding for `nodeId`. */
  ownsPeer(nodeId) {
    return this._meshIdByNodeId.has(nodeId);
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
        `Transport.send: peer ${String(nodeId)} not connected`,
        { context: { nodeId: String(nodeId), type } });
    }

    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_TIMEOUT,
          `Transport.send: timeout awaiting '${type}' from ${String(nodeId)}`,
          { context: { nodeId: String(nodeId), type } }));
      }, this._requestTimeoutMs);

      this._pending.set(id, { nodeId, resolve, reject, timer });

      try {
        this._mesh.send(meshId, { k: 'req', id, type, body });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
          `Transport.send: mesh.send failed (${err.message})`,
          { cause: err, context: { nodeId: String(nodeId), type } }));
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
      this._log('notify-no-binding', { nodeId: String(nodeId), type });
      return;
    }
    if (!this._mesh.isConnected(meshId)) {
      this._log('notify-not-connected', { nodeId: String(nodeId), meshId, type });
      return;
    }
    try {
      this._mesh.send(meshId, { k: 'ntf', type, body });
    } catch (err) {
      this._log('notify-send-failed', { nodeId: String(nodeId), type, err: err.message });
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
    // sender's meshId (a 3-char string).  Translate to BigInt nodeId for
    // the handler dispatch.  If a frame arrives from an unbound mesh
    // peer (e.g. hello/hello-ack BEFORE bindPeer is called), let it
    // through with fromNodeId=null so the orchestrator can intercept
    // and bind; in that case we pass the meshId string as the fromId
    // argument (which the hello-handler expects — typeof === 'string'
    // is the pre-bind sentinel).
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
      // Bound peers see BigInt fromId; pre-bind frames see the meshId
      // string (so the orchestrator's hello-handler can recognise the
      // pre-bind state).
      const result = await handler(fromNodeId ?? fromMeshId, msg.body);
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
      // orchestrator can bind on receipt.  After bindPeer, handler
      // receives the BigInt nodeId.
      handler(fromNodeId ?? fromMeshId, msg.body);
    } catch (err) {
      this._log('ntf-handler-threw', { type: msg.type, err: err.message });
    }
  }

  /**
   * v2.0.2 — Surface MeshManager's per-frame ping/pong traffic events
   * through the Transport.  Translates the mesh's peerId (hex string)
   * into the bound BigInt nodeId where available, matching the
   * convention of other Transport callbacks.  Callers can use this to
   * drive a "channel is moving bytes" UI indicator.
   *
   * @param {(nodeId: bigint|string, kind: 'sent'|'recv') => void} callback
   * @returns {() => void} unsubscribe
   */
  onPingTraffic(callback) {
    return this._mesh.onPingTraffic((meshId, kind) => {
      const reportedId = this._nodeIdByMeshId.get(meshId) ?? meshId;
      try { callback(reportedId, kind); }
      catch (err) {
        this._log('ping-traffic-handler-threw', {
          reportedId: String(reportedId), kind, err: err.message,
        });
      }
    });
  }

  _onPeerLost(meshId) {
    const nodeId = this._nodeIdByMeshId.get(meshId);
    // Only fire peer-died when this channel is the ACTIVE route for its
    // identity.  A deduped-duplicate loser had its reverse mapping cleared
    // before teardown (so nodeId is undefined here), and a channel whose
    // identity is now served by a different meshId is likewise not a death
    // of the peer — only the closure of a redundant channel.  Suppressing
    // those keeps the synaptome from dropping a peer that's still reachable.
    const isActiveRoute = nodeId !== undefined && this._meshIdByNodeId.get(nodeId) === meshId;
    if (isActiveRoute) {
      for (const h of this._peerDiedHandlers) {
        try { h(nodeId); }
        catch (err) {
          this._log('peer-died-handler-threw', { reportedId: String(nodeId), err: err.message });
        }
      }
      // Reject every pending request to this peer.
      for (const [id, p] of this._pending.entries()) {
        if (p.nodeId !== nodeId) continue;
        clearTimeout(p.timer);
        this._pending.delete(id);
        p.reject(new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
          `peer ${String(nodeId)} died`, { context: { nodeId: String(nodeId) } }));
      }
    } else {
      this._log('peer-lost-redundant-channel', { meshId, nodeId: nodeId === undefined ? null : String(nodeId) });
    }
    // Unbind last so further sends fail fast (meshId-aware: won't touch the
    // winner's mapping).
    this.unbindPeer(meshId);
  }
}
