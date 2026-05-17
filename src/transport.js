// =====================================================================
// WebRTCTransport — implements the Axona Transport contract on top of
// MeshManager.  One instance per running peer.
//
// The Transport contract (vendored at src/contracts/Transport.js) is
// the uniform interface every Axona DHT protocol consumes: send /
// notify / openConnection / onPeerDied / getLatency / etc.  See the
// wire-protocol spec at
// `dht-sim/documents/implementation/Axona-Wire-Protocol-v0.71.md`
// for the canonical frame shapes (req / res / ntf), version
// handshake, and message-type vocabulary.
//
// Architectural layering (Phase 3):
//
//     application (chat, social feed, agent SDK, …)
//             │
//             │   DHT contract (lookup / publish / subscribe / …)
//             │
//     protocol (NeuromorphicDHTNH1 — same code as runs in dht-sim)
//             │
//             │   Transport contract (send / notify / getLatency / …)
//             │
//     WebRTCTransport   ← this file
//             │
//             │   send(peerId, payload) / onMessage(cb) / onPeerLost(cb)
//             │   isConnected(peerId) / getLatency(peerId)
//             │
//     MeshManager (src/mesh.js)
//             │
//             │   RTCPeerConnection + RTCDataChannel
//             │
//     WebRTC wire (peer-to-peer, ICE/DTLS, TURN as fallback)
//
// MeshManager already owns: connection setup, ICE handling, 1 Hz
// application ping/pong (UI's liveness signal), peer-death detection,
// retry logic.  WebRTCTransport adds: typed request/response with
// correlation ids, fire-and-forget notifications, request timeout,
// dispatch-by-type, and the Transport-side onPeerDied bridge.
//
// The application ping/pong continues to drive the UI indicators
// (drives `state.rttBuffer` which `getLatency()` reads here).  We do
// not yet emit a separate Transport-owned `ping` request; that's a
// future spec-compliance step.  Until then, getLatency is sourced
// from the MeshManager's existing RTT samples.
// =====================================================================

import { Transport } from './contracts/Transport.js';

/** Reject `send()` if the remote hasn't responded within this. */
const REQUEST_TIMEOUT_MS = 5000;

/** Max correlation id before wrapping.  2^31 is plenty per session. */
const MAX_REQ_ID = 0x7fffffff;

export class WebRTCTransport extends Transport {
  /**
   * @param {object} opts
   * @param {import('./mesh.js').MeshManager} opts.mesh
   * @param {string} [opts.localNodeId]   — usually the bridge's `connId`
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor({ mesh, localNodeId = null, log }) {
    super();
    if (!mesh) throw new Error('WebRTCTransport: mesh is required');
    this._mesh        = mesh;
    this._localNodeId = localNodeId;
    this._log         = log ?? (() => {});

    /** @type {Map<string, (fromId:bigint, payload:any)=>Promise<any>>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, (fromId:bigint, payload:any)=>void>} */
    this._ntfHandlers = new Map();

    /**
     * Outstanding requests awaiting response.  Keyed by correlation id.
     * @type {Map<number, { nodeId:bigint, resolve:Function, reject:Function, timer:any }>}
     */
    this._pending = new Map();
    this._nextId  = 1;

    /** @type {Array<(nodeId:bigint) => void>} */
    this._peerDiedHandlers = [];

    // v0.6.0 — Two-space identifier translation.
    //
    // Mesh layer uses string `meshId`s (the bridge-assigned UUID-ish
    // connId).  Axona protocol layer uses 64-bit BigInt `nodeId`s
    // (S2 prefix + 56 bottom bits).  The two coexist: an Axona
    // hello/hello-ack handshake on each fresh WebRTC channel
    // exchanges nodeIds; the application calls bindPeer(nodeId,
    // meshId) to teach the transport.  After that, AxonaPeer can
    // address peers by their 64-bit nodeId and the transport
    // routes correctly.  Unbind on peer-left to keep maps clean.
    /** @type {Map<bigint, string>} */
    this._meshIdByNodeId = new Map();
    /** @type {Map<string, bigint>} */
    this._nodeIdByMeshId = new Map();

    this._started        = false;
    this._unsubMessage   = null;
    this._unsubPeerLost  = null;
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────

  async start(localNodeId) {
    if (localNodeId !== undefined) this._localNodeId = localNodeId;
    if (this._started) return;
    this._unsubMessage  = this._mesh.onMessage ((peerId, msg) => this._onMessage(peerId, msg));
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
      p.reject(new Error('transport-stopped'));
    }
    this._pending.clear();
    this._started = false;
    this._log('transport-stopped');
  }

  getLocalNodeId() {
    return this._localNodeId;
  }

  // ─── nodeId ↔ meshId binding (v0.6.0) ──────────────────────────────
  //
  // The Axona protocol layer addresses peers by 64-bit BigInt nodeId.
  // The mesh layer (MeshManager + the bridge's signaling) uses
  // opaque string meshIds (UUID-ish connIds).  An external
  // orchestrator runs the hello/hello-ack handshake on each fresh
  // WebRTC channel; once both sides know each other's nodeId, the
  // orchestrator calls bindPeer(nodeId, meshId) to teach the
  // transport.  After that, AxonaPeer can do transport.send(nodeId,
  // ...) and the transport routes via the mesh.
  //
  // unbindPeer is called when the underlying mesh channel closes;
  // any subsequent send/notify to that nodeId fails.

  bindPeer(nodeId, meshId) {
    if (typeof nodeId !== 'bigint') throw new TypeError('nodeId must be bigint');
    if (typeof meshId !== 'string') throw new TypeError('meshId must be string');
    this._meshIdByNodeId.set(nodeId, meshId);
    this._nodeIdByMeshId.set(meshId, nodeId);
    this._log('bindPeer', { nodeId: nodeId.toString(16), meshId });
  }

  unbindPeer(meshId) {
    const nodeId = this._nodeIdByMeshId.get(meshId);
    if (nodeId !== undefined) this._meshIdByNodeId.delete(nodeId);
    this._nodeIdByMeshId.delete(meshId);
  }

  /** Look up the meshId for a nodeId; null if not bound. */
  meshIdFor(nodeId) {
    return this._meshIdByNodeId.get(nodeId) ?? null;
  }

  /** Look up the nodeId for a meshId; null if not bound. */
  nodeIdFor(meshId) {
    return this._nodeIdByMeshId.get(meshId) ?? null;
  }

  // ─── Channel pool ──────────────────────────────────────────────────
  //
  // MeshManager owns connection setup; WebRTCTransport just observes.
  // `openConnection` resolves true if a channel is already open OR
  // becomes open within a 15-second window.  `closeConnection` is a
  // soft request that MeshManager doesn't currently honour (it tears
  // down on peer-left or pc-failed); the protocol's eviction-on-cap
  // semantics are still correct because the underlying mesh.send()
  // will throw on a peer that left.

  async openConnection(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId) return false;   // not yet handshook
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
    // Unbind the nodeId↔meshId mapping so further send/notify fail
    // fast.  MeshManager's WebRTC teardown is still driven by the
    // bridge's peer-left / pc-failed; we don't force it here.
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (meshId) this.unbindPeer(meshId);
  }

  isConnected(nodeId) {
    const meshId = this._meshIdByNodeId.get(nodeId);
    return meshId != null && this._mesh.isConnected(meshId);
  }

  // ─── Messaging ─────────────────────────────────────────────────────

  /**
   * Request/response.  Sends `{k:'req', id, type, body}` over the
   * data channel and resolves when the matching `{k:'res', id}`
   * arrives.  Rejects on timeout, transport stop, or handler error
   * propagated from the remote (`ok:false`).
   */
  async send(nodeId, type, body) {
    if (!this._started) throw new Error('Transport.send: not started');
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId || !this._mesh.isConnected(meshId)) {
      throw new Error(`Transport.send: peer ${nodeId} not connected`);
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
        this._mesh.send(meshId, { k: 'req', id, type, body });
      } catch (err) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(err);
      }
    });
  }

  /**
   * Fire-and-forget.  Silently dropped if the peer is not currently
   * connected — production transports treat notification delivery as
   * best-effort and surface liveness via the heartbeat channel.
   */
  async notify(nodeId, type, body) {
    if (!this._started) throw new Error('Transport.notify: not started');
    const meshId = this._meshIdByNodeId.get(nodeId);
    if (!meshId || !this._mesh.isConnected(meshId)) return;
    try {
      this._mesh.send(meshId, { k: 'ntf', type, body });
    } catch (err) {
      this._log('notify-send-failed', { nodeId: nodeId.toString(16), type, err: err.message });
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

  // ─── Liveness & latency ────────────────────────────────────────────

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

  // ─── Internal: route incoming frames ───────────────────────────────

  async _onMessage(fromMeshId, msg) {
    if (!msg || typeof msg !== 'object') return;

    // Frames addressed to the transport layer arrive here keyed by
    // the sender's meshId.  We translate to nodeId for the handler
    // dispatch.  If a frame arrives from an unbound mesh peer (e.g.
    // hello/hello-ack BEFORE bindPeer is called), let it through
    // with nodeId=null so the orchestrator can intercept and bind.
    const fromNodeId = this._nodeIdByMeshId.get(fromMeshId) ?? null;

    if (msg.k === 'req') {
      await this._handleRequest(fromMeshId, fromNodeId, msg);
    } else if (msg.k === 'res') {
      this._handleResponse(msg);
    } else if (msg.k === 'ntf') {
      this._handleNotification(fromMeshId, fromNodeId, msg);
    } else {
      // Unknown frame kind — drop silently.
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
      // Peer died mid-handle.  The originator's request will time out
      // on its own; we can't do anything useful here.
      this._log('reply-send-failed', { meshId, id, err: err.message });
    }
  }

  _handleResponse(msg) {
    const pending = this._pending.get(msg.id);
    if (!pending) {
      // Late response — handler already timed out and removed the
      // pending entry.  Drop.
      return;
    }
    clearTimeout(pending.timer);
    this._pending.delete(msg.id);
    if (msg.ok) {
      pending.resolve(msg.body);
    } else {
      const errMsg = (msg.body && typeof msg.body === 'object')
        ? (msg.body.error ?? 'remote-error')
        : 'remote-error';
      pending.reject(new Error(errMsg));
    }
  }

  _handleNotification(fromMeshId, fromNodeId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;   // unknown notification type — silent drop
    try {
      // For pre-bind notifications (hello, hello-ack), fromNodeId is
      // null; handler receives fromMeshId as a string in that slot so
      // the orchestrator can bind on receipt.
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
      p.reject(new Error('peer-died'));
    }
    // Unbind the dead peer last so further sends fail fast.
    this.unbindPeer(meshId);
  }
}
