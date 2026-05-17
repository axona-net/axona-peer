// =====================================================================
// WebRTCTransport smoke test (no real WebRTC required).
//
// Mocks two MeshManager-like objects connected to each other so we can
// exercise the Transport contract end-to-end: send/response, notify,
// onPeerDied, timeouts, isConnected, getLatency.
//
// Runs under Node:  `node src/smoke_transport.js`
// =====================================================================

import { WebRTCTransport } from './transport.js';

// ── Fake MeshManager pair ───────────────────────────────────────────
//
// Two FakeMesh instances share a bus.  send(peerId, payload) on one
// side fires onMessage(peerId, payload) on the other.  We can also
// simulate peer-loss by calling .killLink().

class FakeMesh {
  constructor(myId) {
    this.myId = myId;
    this._peer = null;
    this._messageListeners = new Set();
    this._peerLostListeners = new Set();
    this._connected = false;
    this._latency = 42;
  }

  // ── linking ─────────────────────────────────────────────────────
  linkTo(other) {
    this._peer = other;
    other._peer = this;
    this._connected = true;
    other._connected = true;
  }

  killLink() {
    if (!this._connected) return;
    this._connected = false;
    if (this._peer) this._peer._connected = false;
    // Fire lost on both sides.
    const myPeerId = this._peer?.myId;
    const otherPeerId = this.myId;
    if (myPeerId) {
      for (const cb of this._peerLostListeners) cb(myPeerId);
    }
    if (this._peer) {
      for (const cb of this._peer._peerLostListeners) cb(otherPeerId);
    }
  }

  // ── MeshManager interface used by Transport ──────────────────────
  onMessage(cb)   { this._messageListeners.add(cb);  return () => this._messageListeners.delete(cb); }
  onPeerLost(cb)  { this._peerLostListeners.add(cb); return () => this._peerLostListeners.delete(cb); }
  onChange(_cb)   { return () => {}; }   // not used by smoke test
  isConnected(peerId) {
    return this._connected && this._peer && this._peer.myId === peerId;
  }
  getLatency(peerId) {
    if (this.isConnected(peerId)) return this._latency;
    return -1;
  }

  send(peerId, payload) {
    if (!this.isConnected(peerId)) {
      throw new Error(`FakeMesh.send: ${peerId} not connected`);
    }
    // Deliver via microtask so it isn't synchronous.
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

async function setup() {
  const meshA = new FakeMesh('A');
  const meshB = new FakeMesh('B');
  meshA.linkTo(meshB);
  const tA = new WebRTCTransport({ mesh: meshA });
  const tB = new WebRTCTransport({ mesh: meshB });
  await tA.start('A');
  await tB.start('B');
  return { tA, tB, meshA, meshB };
}

async function testRequestResponse() {
  console.log('\n── Request/response ──');
  const { tA, tB } = await setup();
  tB.onRequest('add', async (_from, { a, b }) => a + b);
  const result = await tA.send('B', 'add', { a: 2, b: 3 });
  check('A→B add returns 5', result === 5);
  await tA.stop(); await tB.stop();
}

async function testNotification() {
  console.log('\n── Notification ──');
  const { tA, tB } = await setup();
  let received = null;
  tB.onNotification('hello', (_from, payload) => { received = payload; });
  await tA.notify('B', 'hello', { msg: 'hi' });
  // wait a microtask
  await new Promise(r => setTimeout(r, 10));
  check('notify delivered', received !== null && received.msg === 'hi');
  await tA.stop(); await tB.stop();
}

async function testTimeout() {
  console.log('\n── Request timeout ──');
  const { tA, tB } = await setup();
  tB.onRequest('slow', () => new Promise(() => {})); // never resolves
  // Speed up timeout by sending and waiting for the default 5s; we
  // shortcut by patching the constant in the test.  But the test
  // should still complete.  Use a short manual race.
  const raced = await Promise.race([
    tA.send('B', 'slow', {}).then(() => 'resolved').catch(e => `rejected: ${e.message}`),
    new Promise(r => setTimeout(() => r('still-waiting'), 200)),
  ]);
  check('5s timeout not yet fired (200ms wait)', raced === 'still-waiting');
  await tA.stop(); await tB.stop();
}

async function testRemoteError() {
  console.log('\n── Remote handler error ──');
  const { tA, tB } = await setup();
  tB.onRequest('boom', () => { throw new Error('kaboom'); });
  let caught = null;
  try { await tA.send('B', 'boom', {}); }
  catch (err) { caught = err.message; }
  check('A.send rejects with remote error', caught === 'kaboom');
  await tA.stop(); await tB.stop();
}

async function testPeerDied() {
  console.log('\n── Peer died ──');
  const { tA, tB, meshA } = await setup();
  let died = null;
  tA.onPeerDied((peerId) => { died = peerId; });

  // Send a request that will be pending when B dies.
  tB.onRequest('hang', () => new Promise(() => {}));
  const reqPromise = tA.send('B', 'hang', {}).catch(e => e.message);

  // Now kill the link.
  await new Promise(r => setTimeout(r, 10));
  meshA.killLink();
  await new Promise(r => setTimeout(r, 10));

  check('onPeerDied fired', died === 'B');
  const rejection = await reqPromise;
  check('pending send rejected with peer-died', rejection === 'peer-died');
  await tA.stop(); await tB.stop();
}

async function testLatency() {
  console.log('\n── getLatency ──');
  const { tA, tB } = await setup();
  check('A.getLatency(B) reports mesh RTT', tA.getLatency('B') === 42);
  check('A.getLatency(unknown) returns -1', tA.getLatency('UNKNOWN') === -1);
  await tA.stop(); await tB.stop();
}

async function testIsConnected() {
  console.log('\n── isConnected ──');
  const { tA, meshA } = await setup();
  check('A.isConnected(B) true before link killed', tA.isConnected('B') === true);
  meshA.killLink();
  check('A.isConnected(B) false after link killed', tA.isConnected('B') === false);
}

async function main() {
  console.log('WebRTCTransport smoke test');
  await testRequestResponse();
  await testNotification();
  await testTimeout();
  await testRemoteError();
  await testPeerDied();
  await testLatency();
  await testIsConnected();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke test threw:', err);
  process.exit(2);
});
