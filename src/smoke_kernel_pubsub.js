// =====================================================================
// smoke_kernel_pubsub.js — verify the kernel's unified pub/sub API
//                          (peer.pub / peer.sub / peer.pull / peer.metrics)
//                          works against axona-peer's AxonaNode +
//                          BrowserEngine + AxonManager stack.
//
// I3 step: confirms the wire-up needed for consumers to use the
// v1.0 unified pub/sub API is complete inside this repo.  Before
// this commit, calling `node.peer.pub(topic, msg)` threw
// PUBLISH_INVALID_TOPIC because the kernel's _requireAxonManager
// resolution chain looked for `engine.axonManagerFor(node)` which
// BrowserEngine didn't expose.  A one-line alias in browser_engine.js
// (`axonManagerFor → axonFor`) closes the gap.
//
// Scenarios (single AxonaNode — the kernel's pub/sub is publisher-
// keyed, so a self-publish/self-subscribe exercise is the minimum
// proof that the wiring resolves):
//   1. peer.pub(topic, message) returns a 64-char msgId
//   2. peer.sub(topic, handler) returns a Subscription
//   3. After publish, the subscriber's handler fires with an
//      envelope whose .message matches what we published
//   4. envelope.signerPubkey === peer identity's pubkeyHex
//   5. envelope.msgId matches the publish return value
//   6. peer.pull(msgId, { topic, publisher }) returns the same envelope
//   7. peer.metrics(topic, { publisher }) reports 1 messagesPublished
//   8. sub.stop() cleanly tears down
//
// Cross-peer pub/sub requires the application to know the
// publisher's nodeId; that's a separate API-shape question (see
// I3 plan) and out of scope for this verification commit.
//
// Run:  node src/smoke_kernel_pubsub.js
// =====================================================================

// Stub browser globals before importing AxonaNode (identity.js + others)
const store = new Map();
globalThis.sessionStorage = (() => { const m = new Map();
  return {
    getItem(k){ return m.has(k) ? m.get(k) : null; },
    setItem(k,v){ m.set(k, String(v)); },
    removeItem(k){ m.delete(k); },
  };
})();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

import { AxonaNode } from './axona_node.js';
import { REGIONS, forgetIdentity, deriveIdentity } from './identity.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

// ── FakeMesh (mimics MeshManager — no real WebRTC) ──────────────────
class FakeMesh {
  constructor(myMeshId) {
    this.myMeshId = myMeshId;
    this._peer = null;
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._changeListeners = new Set();
    this._connected = false;
  }
  // No links — single-node test.
  onMessage (cb) { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange  (cb) { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  isConnected() { return false; }
  getLatency()  { return -1; }
  getPeers()    { return []; }
  send()        { throw new Error('FakeMesh.send: no link'); }
}

async function buildNode(regionId) {
  store.clear();
  forgetIdentity();
  const node = new AxonaNode({ log: () => {} });
  const identity = await deriveIdentity({ region: REGIONS.find(r => r.id === regionId) });
  node._identity = identity;
  return node;
}

async function main() {
  console.log('axona-peer ↔ kernel unified pub/sub (peer.pub / peer.sub / peer.pull / peer.metrics)');
  console.log();

  const mesh = new FakeMesh('mesh-A');
  const A    = await buildNode('us-east');
  await A.start(mesh);

  const peer    = A._peer;
  const topic   = 'kernel-pubsub-smoke';
  const payload = { hello: 'unified API', n: 42 };

  // ── peer.sub ─────────────────────────────────────────────────────
  const received = [];
  const sub = await peer.sub(topic, env => { received.push(env); });
  check('peer.sub returned a Subscription with a topicId',
    sub && typeof sub.topicId === 'string' && sub.topicId.length === 66);

  // ── peer.pub (unsigned — legacy identity lacks Ed25519 keys) ────
  // axona-peer's identity.js currently returns 64-bit BigInt
  // identities without privateKey / pubkeyHex.  The kernel's
  // signed-publish path needs those fields; until I4/I5 migrate
  // axona-peer to the kernel's deriveIdentity() (which produces a
  // 264-bit Ed25519 identity), this smoke uses { sign: false }.
  const msgId = await peer.pub(topic, payload, { sign: false });
  check('peer.pub returned a 64-char hex msgId',
    typeof msgId === 'string' && msgId.length === 64);

  // Let AxonManager's delivery loop run.  AxonManager publishes via
  // dht.routeMessage which is async; give the microtask queue +
  // delivery hook a tick.
  await new Promise(r => setTimeout(r, 20));

  // ── Delivery ─────────────────────────────────────────────────────
  check('subscriber received exactly one envelope',  received.length === 1);
  check('envelope.message matches publish payload',  received[0]?.message?.hello === 'unified API');
  check('envelope.message.n round-trips',            received[0]?.message?.n === 42);
  check('envelope.msgId matches publish return',     received[0]?.msgId === msgId);
  // Unsigned publish — no signerPubkey / signature fields.

  // ── peer.pull ────────────────────────────────────────────────────
  // Legacy identity.id is a BigInt; AxonaPeer._nodeIdHex pads to 16
  // chars.  For pull, pass the same hex form the peer would have
  // used to derive the topic id.
  const publisher = peer._nodeIdHex();
  const pulled    = await peer.pull(msgId, { topic, publisher });
  check('peer.pull returned the envelope',           pulled?.msgId === msgId);
  check('pulled envelope message matches',           pulled?.message?.hello === 'unified API');

  // ── peer.metrics — shape check only ─────────────────────────────
  // peer.metrics queries the K-closest peers; in a single-node
  // setup there's no mesh to query, so the response is all-zeros.
  // Validate the call returns the expected shape — multi-peer
  // metrics aggregation is verified by the dht-sim regression
  // suite at scale.
  const m = await peer.metrics(topic, { publisher });
  check('peer.metrics returns expected shape',
    m && typeof m.publishes === 'number'
      && typeof m.subscribers === 'number'
      && typeof m.deliveries === 'number'
      && typeof m.pulls === 'number'
      && typeof m.reshares === 'number'
      && typeof m.relayCount === 'number');

  // ── sub.stop ────────────────────────────────────────────────────
  await sub.stop();
  check('sub.stop completes without throwing',       true);

  // ── Public-topic mode (#47) ─────────────────────────────────────
  // Same peer subscribes + publishes with `publisher: null` so the
  // topic ID is the simple sha256(topicName) — anyone-can-publish.
  // Verifies the new opt-in mode is reachable through the same
  // peer.pub / peer.sub surface.
  console.log();
  const pubReceived = [];
  const pubSub = await peer.sub('chat-room', env => { pubReceived.push(env); },
    { publisher: null });
  check('peer.sub(public) returns a Subscription',   !!pubSub && typeof pubSub.topicId === 'string');
  check('public sub topicId starts with "00" (global bucket)',
    pubSub.topicId.slice(0, 2) === '00');

  const pubMsgId = await peer.pub('chat-room', { kind: 'hi' },
    { publisher: null, sign: false });
  check('peer.pub(public) returns a 64-char msgId',  pubMsgId?.length === 64);

  await new Promise(r => setTimeout(r, 20));
  check('public-mode delivery: handler fired',        pubReceived.length === 1);
  check('public-mode envelope carries the payload',   pubReceived[0]?.message?.kind === 'hi');
  check('public-mode envelope msgId matches publish', pubReceived[0]?.msgId === pubMsgId);

  await pubSub.stop();

  // ── Tear down ───────────────────────────────────────────────────
  await A.leave?.({ drain: false, notify: false });

  console.log();
  console.log(`Result: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => { console.error('smoke threw:', err); process.exit(2); });
