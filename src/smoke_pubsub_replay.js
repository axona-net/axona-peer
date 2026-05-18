// =====================================================================
// smoke_pubsub_replay.js — publish-before-subscribe + replay behaviour.
//
// Verifies the lazy-axon patch in AxonManager._onPublishDirect:
// publishes that land on a K-closest node which doesn't yet hold a
// role for the topic should still be cached, so subscribers arriving
// later get the missed messages via pubsub:replay-batch.
//
// Topology: 1 publisher P, 1 subscriber S, 1 bridge.  P publishes
// N messages, THEN S subscribes.  S should receive every published
// message in order.
//
// Run: `node src/smoke_pubsub_replay.js`
// =====================================================================

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { WebSocket } from 'ws';

const store = new Map();
globalThis.sessionStorage = (() => {
  const m = new Map();
  return {
    getItem(k)    { return m.has(k) ? m.get(k) : null; },
    setItem(k, v) { m.set(k, String(v)); },
    removeItem(k) { m.delete(k); },
  };
})();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

import { AxonaNode } from './axona_node.js';
import { deriveIdentity, REGIONS, forgetIdentity } from './identity.js';
import { encode, decode } from './wire.js';
import { deriveTopicKey } from './pubsub_topic.js';

const BRIDGE_PORT = 19086;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const __dirname   = dirname(fileURLToPath(import.meta.url));
const BRIDGE_DIR  = resolve(__dirname, '../../axona-bridge');

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function startBridge() {
  const identityPath = `/tmp/axona-bridge-replay-${process.pid}.json`;
  try { require('node:fs').unlinkSync(identityPath); } catch {}
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: BRIDGE_DIR,
    env: {
      ...process.env,
      PORT: String(BRIDGE_PORT),
      BRIDGE_IDENTITY_PATH: identityPath,
      BRIDGE_LAT: '38.0', BRIDGE_LNG: '-77.0',
      BRIDGE_REGION_LABEL: 'us-east',
      LOG_LEVEL: 'info',
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

class StubMesh {
  constructor() {
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._changeListeners = new Set();
  }
  onMessage(cb) { this._messageListeners.add(cb); return () => this._messageListeners.delete(cb); }
  onPeerLost(cb){ this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange(cb)  { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  isConnected(_id) { return false; }
  getLatency(_id) { return -1; }
  getPeers() { return []; }
  send(_id, _payload) { throw new Error('StubMesh: no mesh peers'); }
}

async function buildBrowser(name, region) {
  store.clear();
  forgetIdentity();
  const ws = await new Promise((resolve, reject) => {
    const s = new WebSocket(BRIDGE_URL);
    s.on('open', () => {
      try { s.send(JSON.stringify({ type: 'client-hello', version: '0.15.0' })); }
      catch (err) { reject(err); return; }
      resolve(s);
    });
    s.on('error', reject);
  });
  const identity = await deriveIdentity({ region: REGIONS.find(r => r.id === region) });
  const mesh = new StubMesh();
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
    let msg; try { msg = decode(data.toString()); } catch { return; }
    if (msg.type === 'axona' && msg.payload) node.handleBridgeAxonaFrame(msg.payload);
  });
  ws.on('close', () => node.handleBridgeClosed());
  await node.start(mesh, bridgeAdapter);
  return { name, node, ws, received: [] };
}

async function main() {
  console.log('Publish-before-subscribe replay smoke (lazy-axon patch)');
  const { child, identityPath, ready } = startBridge();
  try { await waitForReady(ready); }
  catch (err) { child.kill('SIGKILL'); console.error('FAIL:', err.message); process.exit(2); }

  let P, S;
  try {
    P = await buildBrowser('P', 'us-east');
    // Wait until P's bridge-handshake has completed so P's findKClosest
    // can actually reach beyond self.  Polling beats a fixed sleep.
    for (let i = 0; i < 30; i++) {
      if (P.node.getSynaptome().length >= 1) break;
      await new Promise(r => setTimeout(r, 100));
    }
    if (P.node.getSynaptome().length < 1) {
      throw new Error('P never handshook with bridge');
    }

    const T1 = await deriveTopicKey({ lat: 38.0, lng: -77.0 }, 'Topic Before Sub');

    // ── 1. Publish 3 messages from P with NO subscribers anywhere ───
    P.node.pubsubPublish(T1, 'M1');
    P.node.pubsubPublish(T1, 'M2');
    P.node.pubsubPublish(T1, 'M3');
    // Let the K-closest fan-out + lazy-axon promotion settle.
    await new Promise(r => setTimeout(r, 800));

    // ── 2. NOW create subscriber S and subscribe ────────────────────
    S = await buildBrowser('S', 'us-east');
    for (let i = 0; i < 30; i++) {
      if (S.node.getSynaptome().length >= 1) break;
      await new Promise(r => setTimeout(r, 100));
    }
    if (S.node.getSynaptome().length < 1) {
      throw new Error('S never handshook with bridge');
    }
    S.node.pubsubSubscribe(T1, ({ msg, publisher, ts }) => {
      S.received.push({ msg, publisher, ts });
    });
    // Allow the subscribe-k + replay-batch round-trip.
    await new Promise(r => setTimeout(r, 800));

    // ── 3. Assert S received all 3 missed messages ──────────────────
    check('S received M1 (replayed from cache)',
      S.received.some(r => r.msg === 'M1'));
    check('S received M2 (replayed)',
      S.received.some(r => r.msg === 'M2'));
    check('S received M3 (replayed)',
      S.received.some(r => r.msg === 'M3'));
    check('S received exactly 3 messages (no duplicates)',
      S.received.length === 3);
    check('all replayed messages list P as publisher',
      S.received.every(r => r.publisher === P.node.nodeId));

    // ── 4. Live publish AFTER subscribe still works ─────────────────
    P.node.pubsubPublish(T1, 'M4-live');
    await new Promise(r => setTimeout(r, 500));
    check('S received M4 (live, post-subscribe)',
      S.received.some(r => r.msg === 'M4-live'));
    check('total messages now 4',
      S.received.length === 4);

    // ── Tear down ───────────────────────────────────────────────────
    await P.node.stop();
    await S.node.stop();
    P.ws.close();
    S.ws.close();
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
