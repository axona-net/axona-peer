// =====================================================================
// smoke_bridge_transport.js — end-to-end bridge ↔ browser axona flow.
//
// Starts axona-bridge as a child process, opens a WebSocket as a
// fake browser (real WS, no real WebRTC), runs the AxonaNode pieces
// (BrowserEngine + AxonaPeer + CompositeTransport with a BridgeTransport
// sub) against the bridge, and verifies:
//
//   1. Bridge sends hello (axona frame) on welcome
//   2. Browser BridgeTransport receives it, AxonaNode replies with
//      hello-ack
//   3. Both sides admit each other into their synaptomes
//   4. End-to-end ping over the bridge transport (req/res round-trip)
//   5. A.lookup(bridge nodeId) returns {found: true, hops: 1}
//
// Spawns axona-bridge via `node ../axona-bridge/src/server.js`.
//
// Run: `node src/smoke_bridge_transport.js`
// =====================================================================

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { WebSocket } from 'ws';

// localStorage stub (identity.js reads it sync).
const store = new Map();
globalThis.sessionStorage = (() => { const m = new Map(); return { getItem(k){return m.has(k)?m.get(k):null;}, setItem(k,v){m.set(k,String(v));}, removeItem(k){m.delete(k);} }; })();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

import { AxonaNode } from './axona_node.js';
import { deriveIdentity, REGIONS } from './identity.js';

const BRIDGE_PORT = 19082;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const __dirname   = dirname(fileURLToPath(import.meta.url));
const BRIDGE_DIR  = resolve(__dirname, '../../axona-bridge');

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function startBridge() {
  const identityPath = `/tmp/axona-bridge-smoke-${process.pid}.json`;
  try { require('node:fs').unlinkSync(identityPath); } catch {}
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_DIR,
    env: {
      ...process.env,
      PORT: String(BRIDGE_PORT),
      BRIDGE_IDENTITY_PATH: identityPath,
      BRIDGE_LAT: '51.5',  BRIDGE_LNG: '-0.1',
      BRIDGE_REGION_LABEL: 'London',
      LOG_LEVEL: 'info',
      // Pin the gate to the version the smoke client sends; keeps
      // smokes independent of whatever the production default is.
      MIN_PEER_VERSION: '0.14.0',
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

// ── Minimal MeshManager-shaped stub (the browser uses one for
//    WebRTCTransport; this smoke doesn't exercise mesh peers).
class StubMesh {
  constructor() {
    this._changeListeners = new Set();
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
  }
  onChange   (cb) { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  onMessage  (cb) { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost (cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  isConnected(_meshId) { return false; }
  getLatency (_meshId) { return -1; }
  getPeers   () { return []; }
  send       (_meshId, _payload) { throw new Error('StubMesh: no mesh peers'); }
}

async function main() {
  console.log('Bridge ↔ browser axona-transport end-to-end smoke');

  // ── Start the bridge ─────────────────────────────────────────────
  const { child, identityPath, ready } = startBridge();
  try {
    await waitForReady(ready);
  } catch (err) {
    child.kill('SIGKILL');
    console.error('FAIL:', err.message);
    process.exit(2);
  }

  // ── Derive identity FIRST (kernel-backed deriveIdentity is async
  //    via Web Crypto and can yield to the event loop; the bridge's
  //    initial 'hello' notification arrives ms after admission, so
  //    we want the message listener attached + node ready before
  //    those frames land).
  store.clear();
  const identity = await deriveIdentity({
    region: REGIONS.find(r => r.id === 'us-east'),
  });

  // ── Open a WebSocket to the bridge + send client-hello so the
  //    bridge admits us past its version gate.
  const ws = await new Promise((resolve, reject) => {
    const sock = new WebSocket(BRIDGE_URL);
    sock.on('open', () => {
      try { sock.send(JSON.stringify({ type: 'client-hello', version: '1.0.0' })); }
      catch (err) { reject(err); return; }
      resolve(sock);
    });
    sock.on('error', reject);
  });

  const mesh = new StubMesh();

  // The bridge adapter — writes axona-typed envelopes to the WebSocket
  // and reports isOpen.
  const bridgeAdapter = {
    sendToBridge(msg) {
      if (ws.readyState !== WebSocket.OPEN) return false;
      ws.send(JSON.stringify(msg, wireReplacer));
      return true;
    },
    isBridgeOpen() { return ws.readyState === WebSocket.OPEN; },
  };

  const node = new AxonaNode({ identity, log: () => {} });

  // Hook bridge WebSocket messages into the node BEFORE start() so
  // that any axona frames that arrive early aren't dropped.
  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString(), reviveBigInt); }
    catch { return; }
    if (msg.type === 'axona' && msg.payload) {
      node.handleBridgeAxonaFrame(msg.payload);
    }
  });
  ws.on('close', () => node.handleBridgeClosed());

  await node.start(mesh, bridgeAdapter);

  // ── Wait for the bridge's hello + our hello-ack to round-trip ────
  await new Promise(r => setTimeout(r, 200));

  // The browser's synaptome should now contain the bridge.
  const synaptome = node.getSynaptome();
  check('browser synaptome has 1 entry (the bridge)',
    synaptome.length === 1);

  const bridgeSynapse = synaptome[0];
  const bridgeNodeId = bridgeSynapse?.peerId;
  check('synapse._addedBy === bridge-handshake',
    bridgeSynapse?.addedBy === 'bridge-handshake');

  // ── End-to-end: ask the bridge for its `ping` handler over WS ───
  // (Uses the BridgeTransport.send path under the hood.)
  const pingResult = await node.transport.send(bridgeNodeId, 'ping', {});
  check('ping → pong via bridge transport', pingResult === 'pong');

  // ── End-to-end: real lookup ──────────────────────────────────────
  const lookup = await node.lookup(bridgeNodeId);
  check('lookup(bridge) returns a result', lookup != null);
  check('lookup(bridge).found',             lookup?.found === true);
  check('lookup(bridge).hops === 1',        lookup?.hops === 1);

  // ── Verify the bridge's view via /healthz ────────────────────────
  const healthz = await fetch(`http://localhost:${BRIDGE_PORT}/healthz`).then(r => r.json());
  check('bridge healthz reports our nodeId',
    healthz.axona.synaptomeSize >= 1);

  // ── Tear down ────────────────────────────────────────────────────
  await node.stop();
  ws.close();
  child.kill('SIGTERM');
  try { require('node:fs').unlinkSync(identityPath); } catch {}

  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

// Wire replacer: BigInts → "<digits>n", Sets → arrays.
function wireReplacer(_k, v) {
  if (typeof v === 'bigint') return v.toString() + 'n';
  if (v instanceof Set)      return [...v];
  return v;
}

// Reviver: turn "12345n" strings back into BigInts.
function reviveBigInt(_k, v) {
  if (typeof v === 'string' && /^-?\d+n$/.test(v)) {
    return BigInt(v.slice(0, -1));
  }
  return v;
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
