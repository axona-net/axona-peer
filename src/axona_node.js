// =====================================================================
// axona_node.js — orchestrates the Axona protocol layer for one
// browser peer.
//
// Glues together:
//   - identity.js's 64-bit nodeId (S2 prefix + persisted random bits)
//   - BrowserEngine (per-peer config + state stand-in for AxonaEngine)
//   - AxonaPeer (per-node DHT-contract implementation, from
//     @axona/protocol)
//   - WebRTCTransport (carries the wire protocol over MeshManager's
//     data channels)
//
// Also owns the application-protocol concerns NH-1 in the simulator
// hides inside AxonaEngine:
//   - Hello / hello-ack handshake on each new WebRTC channel, so the
//     transport learns each remote peer's nodeId and can bindPeer
//     before any AxonaPeer-level traffic.
//   - NH-1 transport handlers (ping, lookahead_probe, local_probe,
//     find_closest_set, lookup_step, reinforce, …) registered at
//     start time so incoming requests reach AxonaPeer methods.
//   - Admission: when a hello-ack arrives, install a Synapse for that
//     peer in AxonaPeer's synaptome (this is the equivalent of the
//     simulator's stratified bootstrap; production bootstrap via
//     BootstrapService is a follow-up).
//
// Single public class: AxonaNode.  Construct, call start(meshManager),
// then use node.peer for DHT operations (lookup, getMetrics, etc.).
// =====================================================================

import {
  AxonaPeer,
  NeuronNode,
  Synapse,
  clz64,
} from '../vendor/axona-protocol/src/index.js';

import { BrowserEngine }      from './browser_engine.js';
import { WebRTCTransport }    from './transport.js';
import { BridgeTransport,
         BRIDGE_CONN_ID_EXPORT as BRIDGE_CONN_ID }
                              from './bridge_transport.js';
import { CompositeTransport } from './composite_transport.js';
import { deriveIdentity, getCurrentIdentity } from './identity.js';

// ── ID encoding helpers ──────────────────────────────────────────────

function idToHex(id) { return id.toString(16).padStart(16, '0'); }
function hexToId(hex) { return BigInt('0x' + hex); }

// ── Public class ─────────────────────────────────────────────────────

export class AxonaNode {
  /**
   * @param {Object} opts
   * @param {Object} [opts.identity]  pre-derived Identity from
   *                                  identity.js.  If omitted, the
   *                                  caller MUST call start() with an
   *                                  identity argument.
   * @param {(event:string, data?:object) => void} [opts.log]
   */
  constructor(opts = {}) {
    this._log = opts.log ?? (() => {});
    this._identity = opts.identity ?? null;
    this._mesh = null;
    this._engine = null;
    this._node = null;
    this._transport = null;       // CompositeTransport
    this._meshTransport = null;   // WebRTCTransport sub
    this._bridgeTransport = null; // BridgeTransport sub (when configured)
    this._peer = null;
    this._meshUnsubs = [];
    this._helloByMeshId = new Map();  // meshId → 'pending'|'complete'
    this._meshOpenedAt = new Map();
    this._bridgeHelloState = 'idle'; // 'idle'|'pending'|'complete'
    this._bridgeAdapter = null;       // {sendToBridge, isBridgeOpen}
  }

  get peer()      { return this._peer; }
  get transport() { return this._transport; }
  get engine()    { return this._engine; }
  get identity()  { return this._identity; }
  get nodeId()    { return this._identity?.id ?? null; }

  /**
   * Bring the Axona protocol layer up.  Requires a MeshManager from
   * mesh.js.  Optionally accepts a `bridgeAdapter` for talking to
   * the embedded peer in axona-bridge over the existing WebSocket:
   *
   *     { sendToBridge: (msg) => void,   // serialize + send
   *       isBridgeOpen: () => boolean }
   *
   * If supplied, BridgeTransport is added alongside WebRTCTransport
   * in a CompositeTransport, and the bridge is admitted to the
   * synaptome via hello-ack just like any other peer.
   *
   * @param {import('./mesh.js').MeshManager} mesh
   * @param {Object} [bridgeAdapter]
   */
  async start(mesh, bridgeAdapter) {
    if (!mesh) throw new Error('AxonaNode.start: mesh is required');
    if (!this._identity) {
      this._identity = await deriveIdentity({});
    }
    this._mesh = mesh;
    this._bridgeAdapter = bridgeAdapter ?? null;

    this._engine = new BrowserEngine({ k: 20 });

    // NeuronNode wants lat / lng for geometry-aware features.  Use
    // the identity's region coords; they're the user's chosen point.
    this._node = new NeuronNode({
      id:  this._identity.id,
      lat: this._identity.region?.lat ?? 0,
      lng: this._identity.region?.lng ?? 0,
    });
    this._node.temperature = this._engine.T_INIT;
    this._engine.setTheNode(this._node);

    // ── Compose mesh + bridge transports under a single Transport ───
    this._transport = new CompositeTransport({
      localNodeId: this._identity.id,
      log: this._log,
    });

    this._meshTransport = new WebRTCTransport({
      mesh,
      localNodeId: this._identity.id,
      log: this._log,
    });
    this._transport.addSubtransport(this._meshTransport);

    if (this._bridgeAdapter) {
      this._bridgeTransport = new BridgeTransport({
        localNodeId:  this._identity.id,
        sendToBridge: this._bridgeAdapter.sendToBridge,
        isBridgeOpen: this._bridgeAdapter.isBridgeOpen,
        log: this._log,
      });
      this._transport.addSubtransport(this._bridgeTransport);
    }

    this._node.transport = this._transport;
    await this._transport.start(this._identity.id);

    this._registerNH1Handlers();

    this._peer = new AxonaPeer({ engine: this._engine, node: this._node });
    await this._peer.start();

    // Observe mesh state: send hello when a peer's channel reaches
    // open, install Synapse when its hello-ack arrives.
    this._meshUnsubs.push(mesh.onChange((peers) => this._onMeshChange(peers)));
    this._meshUnsubs.push(mesh.onPeerLost((meshId) => this._onMeshPeerLost(meshId)));

    this._log('axona-node-started', {
      nodeId: idToHex(this._identity.id),
      region: this._identity.region?.label ?? this._identity.region?.id,
      hasBridgeTransport: !!this._bridgeTransport,
    });
    return this;
  }

  // ── Public hooks for client.js bridge wiring ───────────────────────

  /**
   * Route an `{type:'axona', payload:...}` message from the bridge
   * WebSocket into the BridgeTransport.  Caller passes the inner
   * payload object.  No-op if BridgeTransport isn't configured.
   */
  handleBridgeAxonaFrame(payload) {
    if (!this._bridgeTransport) return;
    this._bridgeTransport.handleIncoming(payload);
  }

  /** Caller signals the bridge WebSocket has closed. */
  handleBridgeClosed() {
    if (!this._bridgeTransport) return;
    this._bridgeTransport.handleConnClosed();
    this._bridgeHelloState = 'idle';
  }

  async stop() {
    for (const unsub of this._meshUnsubs) try { unsub(); } catch {}
    this._meshUnsubs = [];
    if (this._peer)      await this._peer.stop();
    if (this._transport) await this._transport.stop();
    this._peer = null;
    this._transport = null;
    this._engine = null;
    this._node = null;
  }

  // ── Synaptome telemetry surfacing for the UI ───────────────────────
  getSynaptome() { return this._peer?.getSynaptome() ?? []; }
  getMetrics()   { return this._peer?.getMetrics()   ?? null; }

  // ── DHT operations passed through to AxonaPeer ─────────────────────
  async lookup(targetKey) { return this._peer?.lookup(targetKey); }

  // ─────────────────────────────────────────────────────────────────
  //   Internal: hello/hello-ack handshake + Synapse admission
  // ─────────────────────────────────────────────────────────────────

  _registerNH1Handlers() {
    const t = this._transport;
    const node = this._node;
    const engine = this._engine;
    const peer = () => this._peer;

    // Application hello/hello-ack — frame these as notifications.
    // The transport delivers pre-bind notifications with fromMeshId
    // as the first handler argument (string).  After we bind, later
    // notifications arrive with fromNodeId (BigInt).  We use the
    // arg's type to detect handshake state.

    t.onNotification('hello', (fromMeshIdOrNodeId, body) => {
      if (typeof fromMeshIdOrNodeId !== 'string') return;   // already bound
      const peerNodeId = hexToId(body.nodeId);
      this._completeHandshake(fromMeshIdOrNodeId, peerNodeId);
      // Reply with our hello-ack.  The reply path differs by
      // sub-transport kind: a mesh peer is reached via mesh.send,
      // the bridge via the bridge adapter's sendToBridge.
      const helloAck = {
        type: 'axona',  // bridge: wrapped envelope
        payload: {
          k: 'ntf', type: 'hello-ack',
          body: { proto: 'axona/3', nodeId: idToHex(this._identity.id) },
        },
      };
      try {
        if (fromMeshIdOrNodeId === BRIDGE_CONN_ID) {
          this._bridgeAdapter.sendToBridge(helloAck);
        } else {
          // Mesh: the data-channel carries the inner frame
          // directly (MeshManager.send wraps in JSON).
          this._mesh.send(fromMeshIdOrNodeId, helloAck.payload);
        }
      } catch (err) {
        this._log('hello-ack-send-failed', { err: err.message });
      }
    });

    t.onNotification('hello-ack', (fromMeshIdOrNodeId, body) => {
      if (typeof fromMeshIdOrNodeId !== 'string') return;
      const peerNodeId = hexToId(body.nodeId);
      this._completeHandshake(fromMeshIdOrNodeId, peerNodeId);
    });

    // ── NH-1 routing handlers ─────────────────────────────────────
    // Mirror AxonaEngine._registerNH1Handlers but on `this._node`
    // directly.  All fromId parameters are now BigInt nodeIds (the
    // transport translates from meshId before dispatch).

    t.onRequest('ping', async () => 'pong');

    t.onRequest('lookahead_probe', async (_fromId, payload) => {
      const target   = payload.target;
      const fromDist = payload.fromDist;
      const fwd = [];
      for (const syn of node.synaptome.values()) {
        if ((syn.peerId ^ target) < fromDist) fwd.push(syn);
      }
      if (fwd.length === 0) {
        return { peerId: node.id, latency: 0, terminal: true };
      }
      const best = node.bestByAP(fwd, target, 0);
      return { peerId: best.peerId, latency: best.latency, terminal: false };
    });

    t.onRequest('local_probe', async (fromId, _payload) => {
      const peerIds = [];
      for (const syn of node.synaptome.values()) {
        if (syn.peerId !== fromId) peerIds.push(syn.peerId);
      }
      return peerIds;
    });

    t.onRequest('find_closest_set', async (_fromId, payload) => {
      const targetBig = payload.target;
      const K = payload.K ?? 20;
      const top = [];
      for (const syn of node.synaptome.values()) {
        const d = syn.peerId ^ targetBig;
        if (top.length < K) {
          let i = 0;
          while (i < top.length && top[i].d < d) i++;
          top.splice(i, 0, { peerId: syn.peerId, d });
        } else if (d < top[K - 1].d) {
          let i = 0;
          while (i < top.length && top[i].d < d) i++;
          top.splice(i, 0, { peerId: syn.peerId, d });
          top.pop();
        }
      }
      return top.map(t => t.peerId);
    });

    t.onRequest('lookup_step', async (_fromId, payload) => {
      return await peer()._lookupStep({
        sourceId:    payload.sourceId,
        targetKey:   payload.targetKey,
        hops:        payload.hops,
        path:        payload.path,
        trace:       payload.trace,
        queried:     payload.queried instanceof Set
                       ? payload.queried
                       : Array.isArray(payload.queried)
                           ? new Set(payload.queried)
                           : new Set(),
        totalTimeMs: payload.totalTimeMs,
      });
    });

    t.onNotification('reinforce', (_fromId, payload) => {
      const syn = node.synaptome.get(payload.synapsePeerId);
      if (!syn) return;
      syn.reinforce(engine.simEpoch, engine.INERTIA_DURATION);
      syn.useCount = (syn.useCount ?? 0) + 1;
    });

    // LEARN side-effects: install hop-cache / triadic introductions
    // locally via the same admission gate AxonaPeer uses.
    t.onNotification('triadic_introduce', async (_fromId, payload) => {
      if (!payload?.peerId) return;
      if (node.synaptome.has(payload.peerId)) return;
      const stratum = clz64(node.id ^ payload.peerId);
      const syn = new Synapse({ peerId: payload.peerId, latencyMs: 0, stratum });
      syn.weight   = 0.5;
      syn.inertia  = engine.simEpoch;
      syn._addedBy = 'triadic';
      try { await peer()._addByVitality(syn); } catch {}
    });

    t.onNotification('hop_cache', async (_fromId, payload) => {
      if (!payload?.target) return;
      if (node.synaptome.has(payload.target)) return;
      const stratum = clz64(node.id ^ payload.target);
      const syn = new Synapse({ peerId: payload.target, latencyMs: 0, stratum });
      syn.weight   = 0.5;
      syn.inertia  = engine.simEpoch;
      syn._addedBy = 'hopCache';
      try { await peer()._addByVitality(syn); } catch {}
    });
    t.onNotification('lateral_spread', async () => { /* no-op in MVP */ });

    // Dead-peer callback already populates node._deadPeers via the
    // mesh-level onPeerLost path — the transport translates meshId
    // → nodeId before firing peer-died subscribers; we attach one
    // here that drops the dead peer from the synaptome.
    if (!node._deadPeers) node._deadPeers = new Set();
    t.onPeerDied((deadNodeId) => {
      if (typeof deadNodeId === 'bigint') node._deadPeers.add(deadNodeId);
    });
  }

  // ── Mesh observability handlers ────────────────────────────────────

  _onMeshChange(peers) {
    for (const p of peers) {
      if (p.state !== 'open') continue;
      if (this._helloByMeshId.has(p.peerId)) continue;
      // Newly-open channel — send hello.
      this._helloByMeshId.set(p.peerId, 'pending');
      this._meshOpenedAt.set(p.peerId, Date.now());
      try {
        this._mesh.send(p.peerId, {
          k: 'ntf', type: 'hello',
          body: {
            proto: 'axona/3',
            nodeId: idToHex(this._identity.id),
          },
        });
      } catch (err) {
        this._log('hello-send-failed', { peerId: p.peerId, err: err.message });
      }
    }
  }

  _onMeshPeerLost(meshId) {
    this._helloByMeshId.delete(meshId);
    this._meshOpenedAt.delete(meshId);
  }

  async _completeHandshake(channelId, peerNodeId) {
    // channelId is the BridgeTransport sentinel 'bridge' for the
    // bridge peer, or a string meshId from MeshManager for a mesh
    // peer.  Route bindPeer + dedup keys accordingly.
    const isBridge = (channelId === BRIDGE_CONN_ID);

    if (isBridge) {
      if (this._bridgeHelloState === 'complete') return;
      this._bridgeHelloState = 'complete';
      this._bridgeTransport.bindPeer(peerNodeId, BRIDGE_CONN_ID);
    } else {
      if (this._helloByMeshId.get(channelId) === 'complete') return;
      this._helloByMeshId.set(channelId, 'complete');
      this._meshTransport.bindPeer(peerNodeId, channelId);
    }

    // Admit this peer to our synaptome as a Synapse.  Production
    // bootstrap will replace this with a stratified-fill from the
    // bridge; for now, every successfully-handshook peer is admitted.
    if (!this._node.synaptome.has(peerNodeId)) {
      const stratum = clz64(this._node.id ^ peerNodeId);
      const latency = isBridge
        ? this._bridgeTransport.getLatency(peerNodeId)
        : this._mesh.getLatency(channelId);
      const syn = new Synapse({
        peerId: peerNodeId,
        latencyMs: latency > 0 ? latency : 200,
        stratum,
      });
      syn.weight   = 0.5;
      syn.inertia  = 0;
      syn._addedBy = isBridge ? 'bridge-handshake' : 'handshake';
      try { await this._peer._addByVitality(syn); } catch (err) {
        this._log('admit-failed', { peerNodeId: idToHex(peerNodeId), err: err.message });
      }
    }

    this._log('handshake-complete', {
      via:           isBridge ? 'bridge' : 'mesh',
      channelId,
      peerNodeId:    idToHex(peerNodeId),
      synaptomeSize: this._node.synaptome.size,
    });
  }
}
