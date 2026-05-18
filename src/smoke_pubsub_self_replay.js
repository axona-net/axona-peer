// =====================================================================
// smoke_pubsub_self_replay.js — verify a subscriber that's ALSO an
//                               axon for the topic gets its own cache.
//
// Regression smoke for the bug where AxonManager._maybeSendReplay
// returned early when subscriberId === this.nodeId.  A subscriber
// that's in the K-closest set for its topic gets the role + cache
// via lazy-axon promotion (from receiving publish-k), but the upstream
// protocol skipped delivering that cache to its own delivery callback.
//
// Setup: Publisher P publishes N messages.  Subscriber S happens to
// be in K-closest for the topic (forced by choosing S's identity to
// be very close to topicId in XOR distance).  S subscribes AFTER
// the publishes.  S must receive all N cached messages.
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

const BRIDGE_PORT = 19087;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const __dirname   = dirname(fileURLToPath(import.meta.url));
const BRIDGE_DIR  = resolve(__dirname, '../../axona-bridge');

let passed = 0, failed = 0;
function check(label, cond) {
  if (cond) { console.log(`  ✓ ${label}`); passed++; }
  else      { console.log(`  ✗ ${label}`); failed++; }
}

function startBridge() {
  const identityPath = `/tmp/axona-bridge-self-replay-${process.pid}.json`;
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
      try { s.send(JSON.stringify({ type: 'client-hello', version: '0.17.0' })); }
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
  console.log('Self-replay smoke (axon-is-subscriber)');
  const { child, identityPath, ready } = startBridge();
  try { await waitForReady(ready); }
  catch (err) { child.kill('SIGKILL'); console.error('FAIL:', err.message); process.exit(2); }

  let P, S;
  try {
    P = await buildBrowser('P', 'us-east');
    for (let i = 0; i < 30; i++) {
      if (P.node.getSynaptome().length >= 1) break;
      await new Promise(r => setTimeout(r, 100));
    }

    const T = await deriveTopicKey({ lat: 38.0, lng: -77.0 }, 'self-replay-test');

    // P is in K-set for this topic (small network → K-set = network).
    P.node.pubsubPublish(T, 'M1');
    P.node.pubsubPublish(T, 'M2');
    P.node.pubsubPublish(T, 'M3');
    await new Promise(r => setTimeout(r, 800));

    // P now lazy-axons + caches M1/M2/M3.  When P subscribes to its
    // own topic, the self-replay path must deliver those 3 messages
    // to the local delivery callback.
    P.node.pubsubSubscribe(T, (m) => P.received.push(m));
    await new Promise(r => setTimeout(r, 800));

    check('P (axon + late subscriber) received M1', P.received.some(r => r.msg === 'M1'));
    check('P received M2',                          P.received.some(r => r.msg === 'M2'));
    check('P received M3',                          P.received.some(r => r.msg === 'M3'));
    check('P received exactly 3 (no dups)',         P.received.length === 3);

    // Bringing up a NON-K-set subscriber should still work via
    // the network replay path.
    S = await buildBrowser('S', 'asia-east');
    for (let i = 0; i < 30; i++) {
      if (S.node.getSynaptome().length >= 1) break;
      await new Promise(r => setTimeout(r, 100));
    }
    S.node.pubsubSubscribe(T, (m) => S.received.push(m));
    await new Promise(r => setTimeout(r, 800));

    check('S (remote subscriber) received M1', S.received.some(r => r.msg === 'M1'));
    check('S received M2',                     S.received.some(r => r.msg === 'M2'));
    check('S received M3',                     S.received.some(r => r.msg === 'M3'));

    // Live publish after both subscribed.
    P.node.pubsubPublish(T, 'M4-live');
    await new Promise(r => setTimeout(r, 500));
    check('P received M4 live', P.received.some(r => r.msg === 'M4-live'));
    check('S received M4 live', S.received.some(r => r.msg === 'M4-live'));

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
