// =====================================================================
// smoke_three_node.js — verify Composite-Transport routing across a
//                       three-node Axona topology.
//
// Two fake browsers (A, B) are linked over a FakeMesh AND each
// connects to a spawned axona-bridge over a real WebSocket.  This
// is the first smoke that exercises both sub-transports of
// CompositeTransport simultaneously.  We verify:
//
//   - All three nodes admit the other two into their synaptomes
//     (browsers get peer + bridge; bridge gets both browsers)
//   - A.lookup(B) routes via the MESH sub-transport (hops=1)
//   - A.lookup(bridge) routes via the BRIDGE sub-transport (hops=1)
//   - B.lookup(A) routes via the MESH sub-transport (hops=1)
//   - Bridge's healthz reports synaptomeSize >= 2
//   - Tearing down the mesh link forces A.lookup(B) to route via
//     the bridge — found:true, hops:2 (A → Bridge → B).  This is the
//     interesting capability: the bridge's highway tier heals
//     mesh-only partitions.
//
// Topology:
//
//     ┌──────┐  WebRTC mesh  ┌──────┐
//     │  A   │ ◄───────────► │  B   │
//     └──┬───┘               └──┬───┘
//        │ WebSocket            │ WebSocket
//        └──────► Bridge ◄──────┘
//
// Run: `node src/smoke_three_node.js`
// =====================================================================

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { WebSocket } from 'ws';

// localStorage stub (identity.js reads it sync).
const store = new Map();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

import { AxonaNode } from './axona_node.js';
import { deriveIdentity, REGIONS, forgetIdentity } from './identity.js';
import { encode, decode } from './wire.js';

const BRIDGE_PORT = 19083;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const __dirname   = dirname(fileURLToPath(import.meta.url));
const BRIDGE_DIR  = resolve(__dirname, '../../axona-bridge');

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// ── Spawn helpers ───────────────────────────────────────────────────

function startBridge() {
  const identityPath = `/tmp/axona-bridge-three-${process.pid}.json`;
  try { require('node:fs').unlinkSync(identityPath); } catch {}
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_DIR,
    env: {
      ...process.env,
      PORT: String(BRIDGE_PORT),
      BRIDGE_IDENTITY_PATH: identityPath,
      BRIDGE_LAT: '51.5', BRIDGE_LNG: '-0.1',
      BRIDGE_REGION_LABEL: 'London',
      LOG_LEVEL: 'info',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let started = false;
  child.stdout.on('data', (chunk) => {
    if (chunk.toString().includes('"event":"listen"')) started = true;
    if (process.env.VERBOSE) process.stdout.write('[bridge] ' + chunk);
  });
  child.stderr.on('data', (chunk) => {
    if (process.env.VERBOSE) process.stderr.write('[bridge] ' + chunk);
  });
  return { child, identityPath, ready: () => started };
}

async function waitForReady(check, timeoutMs = 3000) {
  const start = Date.now();
  while (!check()) {
    if (Date.now() - start > timeoutMs) throw new Error('bridge did not start');
    await new Promise(r => setTimeout(r, 50));
  }
}

// ── FakeMesh — same shape as MeshManager (lifted from smoke_axona_node) ─

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
    this._fireChange();
    other._fireChange();
  }
  killLink() {
    if (!this._connected) return;
    this._connected = false;
    if (this._peer) this._peer._connected = false;
    const myMeshId        = this.myMeshId;
    const otherMeshId     = this._peer?.myMeshId;
    if (otherMeshId) {
      for (const cb of this._peerLostListeners) cb(otherMeshId);
    }
    if (this._peer) {
      for (const cb of this._peer._peerLostListeners) cb(myMeshId);
    }
    this._fireChange();
    if (this._peer) this._peer._fireChange();
  }
  _fireChange() {
    const snap = this.getPeers();
    for (const cb of this._changeListeners) cb(snap);
  }
  onMessage (cb) { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange  (cb) { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  isConnected(meshId) {
    return this._connected && this._peer && this._peer.myMeshId === meshId;
  }
  getLatency(meshId) { return this.isConnected(meshId) ? 42 : -1; }
  getPeers() {
    if (!this._peer) return [];
    return [{ peerId: this._peer.myMeshId, state: this._connected ? 'open' : 'failed' }];
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

// ── Browser harness — one WS to the bridge + one FakeMesh ───────────

async function buildBrowser(name, region, meshId) {
  store.clear();
  forgetIdentity();

  const ws = await new Promise((resolve, reject) => {
    const sock = new WebSocket(BRIDGE_URL);
    sock.on('open', () => resolve(sock));
    sock.on('error', reject);
  });

  const identity = await deriveIdentity({
    region: REGIONS.find(r => r.id === region),
  });

  const mesh = new FakeMesh(meshId);

  const bridgeAdapter = {
    sendToBridge(msg) {
      if (ws.readyState !== WebSocket.OPEN) return false;
      ws.send(encode(msg));
      return true;
    },
    isBridgeOpen() { return ws.readyState === WebSocket.OPEN; },
  };

  const node = new AxonaNode({ identity, log: () => {} });

  ws.on('message', (data) => {
    let msg;
    try { msg = decode(data.toString()); }
    catch { return; }
    if (msg.type === 'axona' && msg.payload) {
      node.handleBridgeAxonaFrame(msg.payload);
    }
  });
  ws.on('close', () => node.handleBridgeClosed());

  await node.start(mesh, bridgeAdapter);

  return { name, node, mesh, ws };
}

// ── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log('Three-node Axona topology smoke (browser ↔ browser ↔ bridge)');

  const { child, identityPath, ready } = startBridge();
  try {
    await waitForReady(ready);
  } catch (err) {
    child.kill('SIGKILL');
    console.error('FAIL:', err.message);
    process.exit(2);
  }

  let A, B;
  try {
    A = await buildBrowser('A', 'us-east',   'mesh-A');
    B = await buildBrowser('B', 'asia-east', 'mesh-B');

    // Link the two browsers' meshes so the WebRTCTransport handshake fires.
    A.mesh.linkTo(B.mesh);

    // Settle: hello round-trips on both mesh + WS.
    await new Promise(r => setTimeout(r, 250));

    const synA = A.node.getSynaptome();
    const synB = B.node.getSynaptome();
    check('A synaptome has 2 entries (B + bridge)', synA.length === 2);
    check('B synaptome has 2 entries (A + bridge)', synB.length === 2);
    check('A synaptome contains B',                  synA.some(s => s.peerId === B.node.nodeId));
    check('B synaptome contains A',                  synB.some(s => s.peerId === A.node.nodeId));

    const bridgeIdFromA = synA.find(s => s.addedBy === 'bridge-handshake')?.peerId;
    const bridgeIdFromB = synB.find(s => s.addedBy === 'bridge-handshake')?.peerId;
    check('A admitted bridge via bridge-handshake', typeof bridgeIdFromA === 'bigint');
    check('B admitted bridge via bridge-handshake', typeof bridgeIdFromB === 'bigint');
    check('A and B agree on bridge nodeId',          bridgeIdFromA === bridgeIdFromB);

    const peerSynA = synA.find(s => s.peerId === B.node.nodeId);
    const peerSynB = synB.find(s => s.peerId === A.node.nodeId);
    check('A admitted B via mesh-handshake',         peerSynA?.addedBy === 'handshake');
    check('B admitted A via mesh-handshake',         peerSynB?.addedBy === 'handshake');

    // ── lookups across both sub-transports ────────────────────────────
    const ab = await A.node.lookup(B.node.nodeId);
    check('A.lookup(B) found',         ab?.found === true);
    check('A.lookup(B).hops === 1',    ab?.hops === 1);

    const abr = await A.node.lookup(bridgeIdFromA);
    check('A.lookup(bridge) found',      abr?.found === true);
    check('A.lookup(bridge).hops === 1', abr?.hops === 1);

    const ba = await B.node.lookup(A.node.nodeId);
    check('B.lookup(A) found',         ba?.found === true);
    check('B.lookup(A).hops === 1',    ba?.hops === 1);

    // Bridge view: both browsers should be in its synaptome.
    const healthz = await fetch(`http://localhost:${BRIDGE_PORT}/healthz`).then(r => r.json());
    check('bridge sees 2 connections',         healthz.connections === 2);
    check('bridge synaptome >= 2 (both browsers admitted)',
      healthz.axona.synaptomeSize >= 2);

    // ── Mesh-failure / bridge-as-highway recovery ────────────────────
    // Kill the WebRTC link between A and B.  WebRTCTransport reports
    // peerDied → NH-1 marks B as dead in A's synaptome.  When A looks
    // B up again, the dead-synapse path triggers an eviction and
    // routes via the bridge (the only other peer in the synaptome).
    A.mesh.killLink();
    await new Promise(r => setTimeout(r, 50));

    const abRecover = await A.node.lookup(B.node.nodeId);
    check('A.lookup(B) after mesh-fail still found',
      abRecover?.found === true);
    check('A.lookup(B) after mesh-fail routes via bridge (hops === 2)',
      abRecover?.hops === 2);
    check('recovery path: A → bridge → B',
      abRecover?.path?.[1] === bridgeIdFromA &&
      abRecover?.path?.[2] === B.node.nodeId);

    // ── Tear down ───────────────────────────────────────────────────
    await A.node.stop();
    await B.node.stop();
    A.ws.close();
    B.ws.close();
  } finally {
    child.kill('SIGTERM');
    try { require('node:fs').unlinkSync(identityPath); } catch {}
  }

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
