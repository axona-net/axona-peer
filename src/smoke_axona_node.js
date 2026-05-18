// =====================================================================
// smoke_axona_node.js — verify the AxonaNode orchestration end-to-end.
//
// Two AxonaNode instances are wired to a pair of FakeMeshes (one
// per side).  We verify:
//
//   1. Both nodes derive identities and instantiate their AxonaPeer
//      without error.
//   2. The hello/hello-ack handshake fires when the mesh transitions
//      to "open" — both transports gain a nodeId↔meshId binding.
//   3. The handshake handler installs a Synapse for the remote peer
//      via _addByVitality (so the synaptome grows by 1 on each side).
//   4. A.lookup(B's nodeId) returns {found, hops≥1, path includes B}.
//
// Runs under Node directly: `node src/smoke_axona_node.js`
// =====================================================================

// Stub localStorage so identity.js can persist between tests.
const store = new Map();
globalThis.sessionStorage = (() => { const m = new Map(); return { getItem(k){return m.has(k)?m.get(k):null;}, setItem(k,v){m.set(k,String(v));}, removeItem(k){m.delete(k);} }; })();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

import { AxonaNode } from './axona_node.js';
import { forgetIdentity, REGIONS } from './identity.js';

// ── FakeMesh: same shape as MeshManager but no real WebRTC ──────────
//
// Important: this matches what client.js gets from mesh.js.  Each
// FakeMesh emits onChange when state changes; AxonaNode listens
// there to detect peers reaching 'open' state and fires hello.

class FakeMesh {
  constructor(myMeshId) {
    this.myMeshId = myMeshId;
    this._peer = null;
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._changeListeners = new Set();
    this._connected = false;
  }
  linkTo(other) {
    this._peer = other;
    other._peer = this;
    this._connected = true;
    other._connected = true;
    // Fire onChange on both sides so the AxonaNodes notice and send hello.
    this._fireChange();
    other._fireChange();
  }
  killLink() {
    if (!this._connected) return;
    this._connected = false;
    if (this._peer) this._peer._connected = false;
    const otherMeshId = this.myMeshId;
    const myPeerMeshId = this._peer?.myMeshId;
    if (myPeerMeshId) {
      for (const cb of this._peerLostListeners) cb(myPeerMeshId);
    }
    if (this._peer) {
      for (const cb of this._peer._peerLostListeners) cb(otherMeshId);
    }
    this._fireChange();
    if (this._peer) this._peer._fireChange();
  }
  _fireChange() {
    const snap = this.getPeers();
    for (const cb of this._changeListeners) cb(snap);
  }
  // ── MeshManager-compatible API ────────────────────────────────────
  onMessage (cb) { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange  (cb) { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  isConnected(meshId) {
    return this._connected && this._peer && this._peer.myMeshId === meshId;
  }
  getLatency(meshId) { return this.isConnected(meshId) ? 42 : -1; }
  getPeers() {
    // Mimic MeshManager.getPeers — one row per known mesh peer.
    if (!this._peer) return [];
    return [{
      peerId: this._peer.myMeshId,
      state:  this._connected ? 'open' : 'failed',
    }];
  }
  send(meshId, payload) {
    if (!this.isConnected(meshId)) {
      throw new Error(`FakeMesh.send: ${meshId} not connected`);
    }
    queueMicrotask(() => {
      for (const cb of this._peer._messageListeners) cb(this.myMeshId, payload);
    });
  }
}

// ── Test runner ─────────────────────────────────────────────────────

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

async function buildNode(name, region) {
  // Reset localStorage so each AxonaNode gets a fresh identity.
  store.clear();
  forgetIdentity();
  const node = new AxonaNode({ log: () => {} });
  // deriveIdentity will run inside start() with the supplied region.
  // But we want a deterministic test region per side, so pre-derive.
  const { deriveIdentity } = await import('./identity.js');
  const identity = await deriveIdentity({ region });
  node._identity = identity;   // bypass the persisted check
  return node;
}

async function main() {
  console.log('AxonaNode orchestration smoke');

  const meshA = new FakeMesh('mesh-A');
  const meshB = new FakeMesh('mesh-B');

  const nodeA = await buildNode('A', REGIONS.find(r => r.id === 'us-east'));
  const nodeB = await buildNode('B', REGIONS.find(r => r.id === 'asia-east'));

  await nodeA.start(meshA);
  await nodeB.start(meshB);
  check('A starts with a valid identity', nodeA.nodeId != null);
  check('B starts with a valid identity', nodeB.nodeId != null);
  check('A and B have different ids',     nodeA.nodeId !== nodeB.nodeId);
  // Top 8 bits should differ (us-east vs asia-east are different S2 cells).
  check('A and B in different S2 cells',
    (nodeA.nodeId >> 56n) !== (nodeB.nodeId >> 56n));

  // Now link the meshes — fires the onChange path, AxonaNode sends hello.
  meshA.linkTo(meshB);

  // Wait long enough for the hello/hello-ack microtask round-trips.
  await new Promise(r => setTimeout(r, 50));

  check('A learned B nodeId',  nodeA.transport.nodeIdFor('mesh-B') === nodeB.nodeId);
  check('B learned A nodeId',  nodeB.transport.nodeIdFor('mesh-A') === nodeA.nodeId);
  check('A synaptome has B',   nodeA.getSynaptome().some(s => s.peerId === nodeB.nodeId));
  check('B synaptome has A',   nodeB.getSynaptome().some(s => s.peerId === nodeA.nodeId));

  // ── End-to-end lookup ────────────────────────────────────────────
  const result = await nodeA.lookup(nodeB.nodeId);
  check('A.lookup(B) returns a result', result != null);
  check('A.lookup(B).found',            result?.found === true);
  check('A.lookup(B).hops === 1',       result?.hops === 1);

  // Self-lookup short-circuit.
  const self = await nodeA.lookup(nodeA.nodeId);
  check('A.lookup(self).found',         self?.found === true);
  check('A.lookup(self).hops === 0',    self?.hops === 0);

  await nodeA.stop();
  await nodeB.stop();

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
