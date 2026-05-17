// =====================================================================
// smoke_axona_peer.js — end-to-end smoke for the browser AxonaPeer
// integration (T4 / T6 MVP).
//
// Validates the chain that the production browser peer will run:
//   BrowserEngine + AxonaPeer + WebRTCTransport, with two peers
//   wired through a mocked MeshManager pair, exchange real
//   `lookup_step` request/response frames over the contract
//   surface, and the originator's lookup() returns a found result.
//
// Runs under Node directly: `node src/smoke_axona_peer.js`
// =====================================================================

import { AxonaPeer, NeuronNode } from '../vendor/axona-protocol/src/index.js';
import { BrowserEngine }         from './browser_engine.js';
import { WebRTCTransport }       from './transport.js';

// ── FakeMesh: same shape as MeshManager but no real WebRTC ──────────

class FakeMesh {
  constructor(myId) {
    this.myId = myId;
    this._peer = null;
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._connected = false;
  }
  linkTo(other) {
    this._peer = other;
    other._peer = this;
    this._connected = true;
    other._connected = true;
  }
  onMessage(cb)  { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange(_)    { return () => {}; }
  isConnected(peerId) {
    return this._connected && this._peer && this._peer.myId === peerId;
  }
  getLatency(peerId) {
    return this.isConnected(peerId) ? 42 : -1;
  }
  send(peerId, payload) {
    if (!this.isConnected(peerId)) {
      throw new Error(`FakeMesh.send: ${peerId} not connected`);
    }
    queueMicrotask(() => {
      for (const cb of this._peer._messageListeners) {
        cb(this.myId, payload);
      }
    });
  }
}

// ── Test runner ─────────────────────────────────────────────────────

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function makePeer(myIdBigInt, meshSide) {
  const engine = new BrowserEngine({ k: 20 });
  // The peer's NeuronNode.  In production it gets created with the
  // user's geographic prefix; in this smoke we synthesize one with
  // arbitrary lat/lng.
  const node = new NeuronNode({ id: myIdBigInt, lat: 0, lng: 0 });
  node.temperature = engine.T_INIT;
  engine.setTheNode(node);

  const transport = new WebRTCTransport({ mesh: meshSide, localNodeId: myIdBigInt });
  node.transport  = transport;

  const peer = new AxonaPeer({ engine, node });
  return { peer, engine, node, transport };
}

async function main() {
  console.log('AxonaPeer browser-integration smoke');

  // ── Build two linked fake-meshes ─────────────────────────────────
  const A_ID = 0xAAAAn;
  const B_ID = 0xBBBBn;
  const meshA = new FakeMesh(A_ID);
  const meshB = new FakeMesh(B_ID);
  meshA.linkTo(meshB);

  // ── Construct two AxonaPeers ──────────────────────────────────────
  const A = makePeer(A_ID, meshA);
  const B = makePeer(B_ID, meshB);

  await A.transport.start(A_ID);
  await B.transport.start(B_ID);

  // ── Wire NH-1 transport handlers on each peer ─────────────────────
  //
  // In the simulator this happens in AxonaEngine.addNode() via
  // _registerNH1Handlers(node).  In production the browser peer
  // wires them at AxonaPeer.start().  We mirror the registration
  // shape here so the lookup_step chain works end-to-end.

  function registerNH1Handlers(peer, transport, node, engine) {
    transport.onRequest('ping', async () => 'pong');

    transport.onRequest('lookahead_probe', async (_fromId, payload) => {
      const target = payload.target;
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

    transport.onRequest('local_probe', async (fromId, _payload) => {
      const peerIds = [];
      for (const syn of node.synaptome.values()) {
        if (syn.peerId !== fromId) peerIds.push(syn.peerId);
      }
      return peerIds;
    });

    transport.onRequest('find_closest_set', async (_fromId, payload) => {
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

    transport.onRequest('lookup_step', async (_fromId, payload) => {
      return await peer._lookupStep({
        sourceId:    payload.sourceId,
        targetKey:   payload.targetKey,
        hops:        payload.hops,
        path:        payload.path,
        trace:       payload.trace,
        queried:     payload.queried,
        totalTimeMs: payload.totalTimeMs,
      });
    });

    transport.onNotification('reinforce', (_fromId, payload) => {
      const syn = node.synaptome.get(payload.synapsePeerId);
      if (!syn) return;
      syn.reinforce(engine.simEpoch, engine.INERTIA_DURATION);
      syn.useCount = (syn.useCount ?? 0) + 1;
    });

    transport.onNotification('triadic_introduce', async () => { /* no-op in MVP */ });
    transport.onNotification('hop_cache',         async () => { /* no-op in MVP */ });
    transport.onNotification('lateral_spread',    async () => { /* no-op in MVP */ });

    if (!node._deadPeers) node._deadPeers = new Set();
    transport.onPeerDied((peerId) => { node._deadPeers.add(peerId); });
  }

  registerNH1Handlers(A.peer, A.transport, A.node, A.engine);
  registerNH1Handlers(B.peer, B.transport, B.node, B.engine);

  await A.peer.start();
  await B.peer.start();

  // ── Pre-populate A's synaptome with B (production: bootstrap does this)
  //    — give A a Synapse pointing at B with reasonable initial weight.
  const { Synapse } = await import('../vendor/axona-protocol/src/dht/Synapse.js');
  const { clz64 }   = await import('../vendor/axona-protocol/src/utils/geo.js');
  const stratum = clz64(A_ID ^ B_ID);
  const synAB = new Synapse({ peerId: B_ID, latencyMs: 42, stratum });
  synAB.weight = 0.7;
  synAB.inertia = 0;
  synAB._addedBy = 'bootstrap';
  await A.peer._addByVitality(synAB);

  check('A.getSynaptome() has B',
    A.peer.getSynaptome().some(s => s.peerId === B_ID));

  // ── A.lookup(B's id) → should find B in 1 hop ───────────────────
  const result = await A.peer.lookup(B_ID);
  check('A.lookup(B) returns a result',          result != null);
  check('A.lookup(B).found === true',            result?.found === true);
  check('A.lookup(B).hops === 1 (one-hop reach)', result?.hops === 1);
  check('A.lookup(B).path includes B',           result?.path.includes(B_ID));

  // ── A.lookup(self) → A is the target, should reach in 0 hops ────
  //
  // Actually with NH-1 routing, the source already IS the target —
  // no lookup_step fires.  Result should reflect "found at source".
  const selfRes = await A.peer.lookup(A_ID);
  check('A.lookup(self) returns a result',          selfRes != null);
  check('A.lookup(self).found === true',            selfRes?.found === true);
  check('A.lookup(self).hops === 0',                selfRes?.hops === 0);

  // ── A.getMetrics → reflects 2 lookups ────────────────────────────
  const m = A.peer.getMetrics();
  check('getMetrics reports lookupsAttempted >= 2',
    m?.cycleStats?.lookupsAttempted >= 2);
  check('getMetrics reports lookupsSucceeded >= 2',
    m?.cycleStats?.lookupsSucceeded >= 2);

  await A.peer.stop();
  await B.peer.stop();
  await A.transport.stop();
  await B.transport.stop();

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
