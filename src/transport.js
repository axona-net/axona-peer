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

    /** @type {Map<string, (fromId:string, payload:any)=>Promise<any>>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, (fromId:string, payload:any)=>void>} */
    this._ntfHandlers = new Map();

    /**
     * Outstanding requests awaiting response.  Keyed by correlation id.
     * @type {Map<number, { peerId:string, resolve:Function, reject:Function, timer:any }>}
     */
    this._pending = new Map();
    this._nextId  = 1;

    /** @type {Array<(peerId:string) => void>} */
    this._peerDiedHandlers = [];

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

  // ─── Channel pool ──────────────────────────────────────────────────
  //
  // MeshManager owns connection setup; WebRTCTransport just observes.
  // `openConnection` resolves true if a channel is already open OR
  // becomes open within a 15-second window.  `closeConnection` is a
  // soft request that MeshManager doesn't currently honour (it tears
  // down on peer-left or pc-failed); the protocol's eviction-on-cap
  // semantics are still correct because the underlying mesh.send()
  // will throw on a peer that left.

  async openConnection(peerId) {
    if (this._mesh.isConnected(peerId)) return true;

    return new Promise((resolve) => {
      const unsub = this._mesh.onChange((peers) => {
        const p = peers.find(x => x.peerId === peerId);
        if (!p) {
          // Peer disappeared from the list — we never connected and
          // can't anymore.
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

  async closeConnection(_peerId) {
    // No-op: MeshManager's lifecycle is driven by the bridge's
    // peer-list / peer-left signals.  A protocol layer that wants to
    // "evict" a peer just stops sending to it; the channel persists
    // until the bridge announces peer-left or the connection fails.
    // A future revision may add an explicit teardown method on
    // MeshManager and call it here.
  }

  isConnected(peerId) {
    return this._mesh.isConnected(peerId);
  }

  // ─── Messaging ─────────────────────────────────────────────────────

  /**
   * Request/response.  Sends `{k:'req', id, type, body}` over the
   * data channel and resolves when the matching `{k:'res', id}`
   * arrives.  Rejects on timeout, transport stop, or handler error
   * propagated from the remote (`ok:false`).
   */
  async send(peerId, type, body) {
    if (!this._started) throw new Error('Transport.send: not started');
    if (!this._mesh.isConnected(peerId)) {
      throw new Error(`Transport.send: peer ${peerId} not connected`);
    }

    const id = this._nextId;
    this._nextId = (this._nextId >= MAX_REQ_ID) ? 1 : this._nextId + 1;

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error('timeout'));
      }, REQUEST_TIMEOUT_MS);

      this._pending.set(id, { peerId, resolve, reject, timer });

      try {
        this._mesh.send(peerId, { k: 'req', id, type, body });
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
  async notify(peerId, type, body) {
    if (!this._started) throw new Error('Transport.notify: not started');
    if (!this._mesh.isConnected(peerId)) return;
    try {
      this._mesh.send(peerId, { k: 'ntf', type, body });
    } catch (err) {
      this._log('notify-send-failed', { peerId, type, err: err.message });
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

  getLatency(peerId) {
    return this._mesh.getLatency(peerId);
  }

  // ─── Internal: route incoming frames ───────────────────────────────

  async _onMessage(fromPeerId, msg) {
    if (!msg || typeof msg !== 'object') return;

    if (msg.k === 'req') {
      await this._handleRequest(fromPeerId, msg);
    } else if (msg.k === 'res') {
      this._handleResponse(msg);
    } else if (msg.k === 'ntf') {
      this._handleNotification(fromPeerId, msg);
    } else {
      // Unknown frame kind — drop silently.  A future spec rev may
      // add new envelope kinds; current code ignores them rather than
      // emitting noise that legacy peers can't make sense of.
    }
  }

  async _handleRequest(fromPeerId, msg) {
    const handler = this._reqHandlers.get(msg.type);
    if (!handler) {
      this._reply(fromPeerId, msg.id, false, { error: `no handler for '${msg.type}'` });
      return;
    }
    try {
      const result = await handler(fromPeerId, msg.body);
      this._reply(fromPeerId, msg.id, true, result);
    } catch (err) {
      this._reply(fromPeerId, msg.id, false, { error: err.message ?? String(err) });
    }
  }

  _reply(peerId, id, ok, body) {
    try {
      this._mesh.send(peerId, { k: 'res', id, ok, body });
    } catch (err) {
      // Peer died mid-handle.  The originator's request will time out
      // on its own; we can't do anything useful here.
      this._log('reply-send-failed', { peerId, id, err: err.message });
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

  _handleNotification(fromPeerId, msg) {
    const handler = this._ntfHandlers.get(msg.type);
    if (!handler) return;   // unknown notification type — silent drop
    try {
      handler(fromPeerId, msg.body);
    } catch (err) {
      this._log('ntf-handler-threw', { type: msg.type, err: err.message });
    }
  }

  _onPeerLost(peerId) {
    // Fan out to peer-died subscribers.
    for (const h of this._peerDiedHandlers) {
      try { h(peerId); }
      catch (err) {
        this._log('peer-died-handler-threw', { peerId, err: err.message });
      }
    }
    // Reject every pending request to this peer.
    for (const [id, p] of this._pending.entries()) {
      if (p.peerId !== peerId) continue;
      clearTimeout(p.timer);
      this._pending.delete(id);
      p.reject(new Error('peer-died'));
    }
  }
}
