// =====================================================================
// smoke_stress_100.js — N-peer stress test for axonal pub/sub.
//
// Spins up N AxonaNodes in a single Node process, links them in a
// full mesh via an in-memory TestHub (no real WebSockets, no real
// WebRTC, no bridge process — just the AxonaPeer + AxonManager
// protocol logic exercised at scale).  Then runs three scenarios:
//
//   A. 1 publisher → (N-1) subscribers, single topic, basic delivery
//   B. Publish-before-subscribe replay with N subscribers joining late
//   C. Multi-topic — several topics, different K-closest sets,
//      different publishers/subscribers per topic
//
// Per-node telemetry captures the participation profile:
//
//   * published_count    — how many times this node called pubsubPublish
//   * subscribed_to      — list of topics this node subscribed to
//   * received_count     — handler fires (per-message delivery to UI)
//   * is_axon_for        — list of topics where this node holds an
//                          axonRole (and thus serves replay/relay)
//   * relay_forwards     — count of 'route_msg' requests this node
//                          forwarded on behalf of others
//
// At the end we print a histogram of node participation: how many
// pure-subscribers, pure-axons, pure-relays, etc.  This is the
// observability you wanted — every peer's actual role in the network.
//
// Run: `node src/smoke_stress_100.js`
// Env: NODES=100 (default), VERBOSE=1, ROUNDS=...
// =====================================================================

const N         = parseInt(process.env.NODES ?? '100', 10);
const VERBOSE   = !!process.env.VERBOSE;
function vlog(...a) { if (VERBOSE) console.log(...a); }

// ── Stub browser globals so identity.js works ────────────────────────
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
import { deriveTopicKey } from './pubsub_topic.js';

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── TestHub + BulkMesh: in-memory N-peer mesh ───────────────────────
//
// TestHub is the single in-process broadcast bus.  BulkMesh implements
// the MeshManager-compatible surface that AxonaNode expects.  Each
// peer has its own BulkMesh; pairs of peers `linkTo` each other to
// establish a logical mesh edge (state: 'open').  Messages dispatched
// via `send(targetMeshId, payload)` are deferred via queueMicrotask
// (mimics the async-but-fast semantics of an open WebRTC DataChannel)
// and delivered into the target's `onMessage` callbacks.

class TestHub {
  constructor() { this.meshes = new Map(); }
  register(mesh) { this.meshes.set(mesh.myMeshId, mesh); }
  deliver(fromMeshId, toMeshId, payload) {
    const target = this.meshes.get(toMeshId);
    if (!target) return;
    queueMicrotask(() => {
      for (const cb of target._messageListeners) cb(fromMeshId, payload);
    });
  }
}

class BulkMesh {
  constructor(hub, meshId) {
    this.hub = hub;
    this.myMeshId = meshId;
    this._peerLinks = new Map();
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._changeListeners = new Set();
    hub.register(this);
  }
  linkTo(other) {
    this._peerLinks.set(other.myMeshId, { state: 'open' });
    other._peerLinks.set(this.myMeshId, { state: 'open' });
    this._fireChange();
    other._fireChange();
  }
  _fireChange() {
    const snap = this.getPeers();
    for (const cb of this._changeListeners) cb(snap);
  }
  onMessage (cb) { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb) { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange  (cb) { this._changeListeners.add(cb);   return () => this._changeListeners.delete(cb); }
  isConnected(meshId) { return this._peerLinks.get(meshId)?.state === 'open'; }
  getLatency (meshId) { return this.isConnected(meshId) ? 5 : -1; }
  getPeers() {
    return [...this._peerLinks.entries()].map(([peerId, info]) => ({
      peerId, state: info.state,
    }));
  }
  send(meshId, payload) {
    if (!this.isConnected(meshId)) throw new Error(`BulkMesh.send: ${meshId} not connected`);
    this.hub.deliver(this.myMeshId, meshId, payload);
  }
}

// ── Per-peer telemetry ──────────────────────────────────────────────

function makeTelemetry(node) {
  return {
    node,
    nodeIdHex:        node.nodeId.toString(16).padStart(16, '0'),
    published:        [],        // [{ topic, msg }]
    subscribed:       new Set(), // topicId
    received:         new Map(), // topicId → [{publisher, msg}]
    relayForwards:    0,         // increment when this peer forwards a route_msg
  };
}

function trackedSubscribe(tele, topicKey, label) {
  tele.subscribed.add(topicKey);
  tele.received.set(topicKey, []);
  tele.node.pubsubSubscribe(topicKey, ({publisher, msg, ts}) => {
    tele.received.get(topicKey).push({ publisher, msg, ts });
  });
}

function trackedPublish(tele, topicKey, msg) {
  tele.published.push({ topic: topicKey, msg });
  tele.node.pubsubPublish(topicKey, msg);
}

// Wrap the route_msg request handler so we can count relay forwards.
// AxonaNode registers its handler via the composite transport; we
// intercept at the transport boundary by replacing the registered
// handler with a wrapper that increments the telemetry counter when
// the message is NOT terminal at this hop.
function instrumentRelay(tele) {
  const t = tele.node._transport;
  // Walk sub-transports and replace each one's 'route_msg' handler
  // with a wrapper.  CompositeTransport replays handlers to all subs,
  // so each sub has its own registered handler.
  for (const sub of (t?._subs ?? [])) {
    if (!sub._reqHandlers) continue;
    const orig = sub._reqHandlers.get('route_msg');
    if (!orig) continue;
    sub._reqHandlers.set('route_msg', async (fromId, body) => {
      const result = await orig(fromId, body);
      // result is the downstream return.  If `consumed: false` and
      // not terminal, we forwarded.  Either way, we ran a hop.
      tele.relayForwards++;
      return result;
    });
  }
}

// ── Setup ───────────────────────────────────────────────────────────

async function buildPeer(hub, idx) {
  store.clear();
  forgetIdentity();
  const region = REGIONS[idx % REGIONS.length];
  const identity = await deriveIdentity({ region });
  const mesh = new BulkMesh(hub, `m${idx}`);
  const node = new AxonaNode({ identity, log: () => {} });
  await node.start(mesh, null /* no bridgeAdapter — pure mesh */);
  return makeTelemetry(node);
}

async function setupNetwork(N) {
  console.log(`\nSpinning up ${N} peers...`);
  const t0 = Date.now();
  const hub = new TestHub();
  const peers = [];
  for (let i = 0; i < N; i++) {
    const p = await buildPeer(hub, i);
    peers.push(p);
  }
  console.log(`  ${N} peers built in ${Date.now() - t0}ms`);

  // Form full mesh.  100×99/2 = 4950 edges; each linkTo is O(1).
  console.log(`Linking full mesh (${N*(N-1)/2} edges)...`);
  const t1 = Date.now();
  for (let i = 0; i < N; i++) {
    for (let j = i + 1; j < N; j++) {
      peers[i].node._mesh.linkTo(peers[j].node._mesh);
    }
  }
  console.log(`  mesh linked in ${Date.now() - t1}ms`);

  // Wait for axona hello/hello-ack handshakes to settle across the
  // full mesh.  Each link fires two hello round-trips.  This is
  // potentially the slowest step.
  const settleMs = Math.max(1500, N * 20);
  console.log(`Waiting ${settleMs}ms for handshakes to settle...`);
  await sleep(settleMs);

  // Install relay instrumentation now that NH-1 handlers are wired.
  for (const p of peers) instrumentRelay(p);

  // Report synaptome saturation.
  const synSizes = peers.map(p => p.node.getSynaptome().length);
  const minSyn = Math.min(...synSizes);
  const maxSyn = Math.max(...synSizes);
  const avgSyn = (synSizes.reduce((a,b) => a+b, 0) / synSizes.length).toFixed(1);
  console.log(`  synaptome sizes: min=${minSyn} avg=${avgSyn} max=${maxSyn} (cap=50)`);

  return { hub, peers };
}

// ── Reporting ───────────────────────────────────────────────────────

function listAxonRoles(peer) {
  const axon = peer.node._axon;
  if (!axon?.axonRoles) return [];
  return [...axon.axonRoles.keys()];
}

function printParticipationProfile(peers, topicLabels = new Map()) {
  console.log(`\nPer-peer participation profile (showing first 20 + summary):`);
  console.log('  idx  nodeId            pub sub rec axon relay  role');
  console.log('  ---  ----------------  --- --- --- ---- -----  ----');
  const counts = { pubOnly: 0, subOnly: 0, axonOnly: 0, relayOnly: 0,
                   pubSub: 0, subAxon: 0, pubAxon: 0, all: 0, idle: 0 };

  for (let i = 0; i < peers.length; i++) {
    const p = peers[i];
    const totalRec = [...p.received.values()].reduce((a, arr) => a + arr.length, 0);
    const roles = listAxonRoles(p);
    const pub = p.published.length;
    const sub = p.subscribed.size;
    const axon = roles.length;

    const isPub  = pub > 0;
    const isSub  = sub > 0;
    const isAxon = axon > 0;
    const isRelay = p.relayForwards > 0;

    let role = '—';
    if (isPub && isSub && isAxon)        { role = 'pub+sub+axon';    counts.all++; }
    else if (isPub && isSub)             { role = 'pub+sub';         counts.pubSub++; }
    else if (isSub && isAxon)            { role = 'sub+axon';        counts.subAxon++; }
    else if (isPub && isAxon)            { role = 'pub+axon';        counts.pubAxon++; }
    else if (isPub)                      { role = 'publisher';       counts.pubOnly++; }
    else if (isSub)                      { role = 'subscriber';      counts.subOnly++; }
    else if (isAxon)                     { role = 'axon-only';       counts.axonOnly++; }
    else if (isRelay)                    { role = 'relay-only';      counts.relayOnly++; }
    else                                 { role = 'idle';            counts.idle++; }

    if (i < 20 || isPub || isAxon || axon > 0) {
      console.log(`  ${String(i).padStart(3)}  ${p.nodeIdHex}  ${
        String(pub).padStart(3)} ${String(sub).padStart(3)} ${
        String(totalRec).padStart(3)} ${String(axon).padStart(4)} ${
        String(p.relayForwards).padStart(5)}  ${role}`);
    }
  }

  console.log(`\nParticipation histogram:`);
  for (const [k, v] of Object.entries(counts)) {
    if (v > 0) console.log(`  ${k.padEnd(14)} ${v}`);
  }
}

// ── Scenarios ───────────────────────────────────────────────────────

async function runScenarioA(peers) {
  console.log(`\n══ Scenario A: 1 publisher, all others subscribe, 1 topic ══`);
  const T = await deriveTopicKey({ lat: 38.0, lng: -77.0 }, 'StressTopicA');
  console.log(`  topic key: ${T.toString(16).padStart(16,'0')}`);

  const publisher = peers[0];
  for (let i = 1; i < peers.length; i++) {
    trackedSubscribe(peers[i], T);
  }
  console.log(`  ${peers.length - 1} subscribers registered, waiting for subscribe-k to propagate...`);
  await sleep(1500);

  // Audit: who holds an axonRole for T?
  const axons = peers.filter(p => listAxonRoles(p).includes(T));
  console.log(`  K-closest axons (should be ~5): ${axons.length}`);
  console.log(`  axon nodeIds: ${axons.map(a => a.nodeIdHex.slice(-6)).join(', ')}`);

  trackedPublish(publisher, T, 'A1');
  trackedPublish(publisher, T, 'A2');
  trackedPublish(publisher, T, 'A3');
  await sleep(2000);

  let fullDelivery = 0, partial = 0, none = 0;
  for (let i = 1; i < peers.length; i++) {
    const got = peers[i].received.get(T)?.length ?? 0;
    if (got === 3)       fullDelivery++;
    else if (got > 0)    partial++;
    else                 none++;
  }
  console.log(`  delivery: ${fullDelivery} full / ${partial} partial / ${none} none / ${peers.length - 1} subscribers`);

  check(`A: ≥90% of subscribers got all 3 messages (full=${fullDelivery}/${peers.length-1})`,
    fullDelivery >= Math.floor((peers.length - 1) * 0.9));
  check(`A: zero subscribers got NOTHING (none=${none})`,
    none === 0);
  check(`A: axon count is bounded (≤10, ideally 5)`,
    axons.length >= 1 && axons.length <= 10);
}

async function runScenarioB(peers) {
  console.log(`\n══ Scenario B: publish-before-subscribe replay at scale ══`);
  const T = await deriveTopicKey({ lat: 51.5, lng: -0.1 }, 'StressTopicB-Replay');

  const publisher = peers[1];   // pick a different publisher
  trackedPublish(publisher, T, 'B1');
  trackedPublish(publisher, T, 'B2');
  trackedPublish(publisher, T, 'B3');
  trackedPublish(publisher, T, 'B4');
  trackedPublish(publisher, T, 'B5');
  console.log(`  publisher emitted 5 messages, no subscribers yet`);
  await sleep(1500);

  const lateSubs = Math.min(50, peers.length - 2);
  for (let i = 2; i < 2 + lateSubs; i++) {
    trackedSubscribe(peers[i], T);
  }
  console.log(`  ${lateSubs} subscribers joined LATE`);
  await sleep(2500);

  let got5 = 0, gotSome = 0, gotNone = 0;
  for (let i = 2; i < 2 + lateSubs; i++) {
    const n = peers[i].received.get(T)?.length ?? 0;
    if (n === 5)      got5++;
    else if (n > 0)   gotSome++;
    else              gotNone++;
  }
  console.log(`  replay delivery: ${got5} got-all / ${gotSome} partial / ${gotNone} none`);
  check(`B: ≥80% of late subscribers got all 5 replayed (got5=${got5}/${lateSubs})`,
    got5 >= Math.floor(lateSubs * 0.8));
}

async function runScenarioC(peers) {
  console.log(`\n══ Scenario C: 5 topics, 5 different publishers, mixed subscribers ══`);
  const topics = [];
  for (let i = 0; i < 5; i++) {
    const region = REGIONS[i * 3];
    const T = await deriveTopicKey({ lat: region.lat, lng: region.lng }, `Multi-${i}`);
    topics.push({ name: `Multi-${i}`, key: T });
  }

  // 5 publishers (peer[0..4]).  Pick ONE topic per remaining peer
  // (assign round-robin) instead of every peer subscribing to every
  // topic — at N=100 the all-to-all fan-out creates 475 concurrent
  // findKClosest chains and the event loop saturates.  Round-robin
  // keeps it to ~95 subscribes (1 per peer) and still gives every
  // topic a healthy subscriber set.
  const subStart = 5;
  const subsPerTopic = new Map(topics.map(tp => [tp.key, 0]));
  for (let i = subStart; i < peers.length; i++) {
    const tp = topics[i % 5];
    trackedSubscribe(peers[i], tp.key);
    subsPerTopic.set(tp.key, subsPerTopic.get(tp.key) + 1);
  }
  console.log(`  ${peers.length - subStart} subscribers distributed across 5 topics:`);
  for (const tp of topics) {
    console.log(`    ${tp.name}: ${subsPerTopic.get(tp.key)} subscribers`);
  }
  await sleep(Math.max(2000, peers.length * 30));

  for (let i = 0; i < 5; i++) {
    trackedPublish(peers[i], topics[i].key, `C-from-${i}`);
  }
  console.log(`  5 publishers each emitted 1 message`);
  await sleep(Math.max(2500, peers.length * 25));

  for (const tp of topics) {
    let got = 0, expected = 0;
    for (let i = subStart; i < peers.length; i++) {
      if (peers[i].subscribed.has(tp.key)) {
        expected++;
        if ((peers[i].received.get(tp.key)?.length ?? 0) >= 1) got++;
      }
    }
    console.log(`  topic ${tp.name} (${tp.key.toString(16).slice(-6)}): ${got}/${expected} received`);
    check(`C/${tp.name}: all subscribers received (${got}/${expected})`, got === expected);
  }

  const axonHistogram = new Map();
  for (const p of peers) {
    for (const t of listAxonRoles(p)) {
      axonHistogram.set(t, (axonHistogram.get(t) ?? 0) + 1);
    }
  }
  console.log(`  axons per topic (count of nodes holding role):`);
  for (const tp of topics) {
    console.log(`    ${tp.name}: ${axonHistogram.get(tp.key) ?? 0}`);
  }
}

// ── Main ────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n╔══════════════════════════════════════════════════════════╗`);
  console.log(`║  AXONA STRESS TEST                                       ║`);
  console.log(`║  N = ${String(N).padEnd(5)} peers, full in-process mesh             ║`);
  console.log(`╚══════════════════════════════════════════════════════════╝`);

  const { peers } = await setupNetwork(N);

  await runScenarioA(peers);
  await runScenarioB(peers);
  await runScenarioC(peers);

  printParticipationProfile(peers);

  for (const p of peers) await p.node.stop();

  console.log(`\n╔══════════════════════════════════════════════════════════╗`);
  console.log(`║  RESULT: ${passed} passed, ${failed} failed                            ║`);
  console.log(`╚══════════════════════════════════════════════════════════╝\n`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('stress test threw:', err);
  process.exit(2);
});
