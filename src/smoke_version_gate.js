// =====================================================================
// smoke_version_gate.js — verify the bridge's client-hello version gate.
//
// Cases:
//   1. Client at the minimum version → admitted, gets welcome
//   2. Client below the minimum     → closed with code 4426 + reason
//   3. Client that never sends client-hello → closed on hello-timeout
//   4. Pre-admission ping is silently dropped (no pong leaks back)
//   5. /healthz reports `minPeerVersion` + admitted/pending counters
//
// Spawns the bridge with MIN_PEER_VERSION=0.12.0 and a tight
// HELLO_TIMEOUT_MS so the timeout case finishes promptly.
//
// Run: `node src/smoke_version_gate.js`
// =====================================================================

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { WebSocket } from 'ws';

const BRIDGE_PORT = 19085;
const BRIDGE_URL  = `ws://localhost:${BRIDGE_PORT}`;
const __dirname   = dirname(fileURLToPath(import.meta.url));
const BRIDGE_DIR  = resolve(__dirname, '../../axona-bridge');

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

function startBridge(env = {}) {
  const identityPath = `/tmp/axona-bridge-gate-${process.pid}.json`;
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
      MIN_PEER_VERSION: '0.14.0',
      // This test exercises the MIN_PEER_VERSION mechanic in isolation with
      // low (0.x) probe versions.  The bridge also enforces namespace-aware
      // flag-day floors (MIN_KERNEL_VERSION / MIN_PEER_APP_VERSION, default
      // 2.9.0 / 3.14.0 for the v2.9.0 envelope); neutralize them here so they
      // don't reject the 0.x probes before MIN_PEER_VERSION is even consulted.
      MIN_KERNEL_VERSION: '0.0.0',
      MIN_PEER_APP_VERSION: '0.0.0',
      HELLO_TIMEOUT_MS: '500',
      ...env,
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

async function waitForReady(checkFn, timeoutMs = 3000) {
  const start = Date.now();
  while (!checkFn()) {
    if (Date.now() - start > timeoutMs) throw new Error('bridge did not start');
    await new Promise(r => setTimeout(r, 50));
  }
}

// Open a WS + send (or skip) client-hello.  Resolves with
// { admitted, code, reason } when the connection either becomes
// admitted (welcome received) or closes.
function probe({ helloVersion /* undefined = skip */ }) {
  return new Promise((resolve) => {
    const ws = new WebSocket(BRIDGE_URL);
    let admitted = false, code = null, reason = null, messages = [];
    let resolved = false;
    function finish() {
      if (resolved) return;
      resolved = true;
      try { ws.close(); } catch {}
      resolve({ admitted, code, reason, messages });
    }
    ws.on('open', () => {
      if (helloVersion !== undefined) {
        // wireVersion '2.0' clears the bridge's wire-major partition gate so
        // this smoke exercises the VERSION floor in isolation (the real app
        // sends WIRE_VERSION from the kernel). Without it the gate rejects on
        // wire before version is ever checked.
        ws.send(JSON.stringify({ type: 'client-hello', version: helloVersion, wireVersion: '2.0' }));
      }
    });
    ws.on('message', (data) => {
      let msg; try { msg = JSON.parse(data.toString()); } catch { return; }
      messages.push(msg);
      if (msg.type === 'welcome') {
        admitted = true;
        finish();
      }
    });
    ws.on('close', (c, r) => {
      code = c;
      reason = r?.toString() ?? '';
      finish();
    });
    ws.on('error', () => {});
  });
}

async function main() {
  console.log('Version-gate smoke (client-hello required)');
  const { child, identityPath, ready } = startBridge();
  try { await waitForReady(ready); }
  catch (err) { child.kill('SIGKILL'); console.error('FAIL:', err.message); process.exit(2); }

  try {
    // ── 1. Compliant client: at min version ───────────────────────────
    const ok = await probe({ helloVersion: '0.14.0' });
    check('compliant peer admitted', ok.admitted === true);
    check('compliant peer saw welcome',
      ok.messages.some(m => m.type === 'welcome'));
    check('compliant peer saw version-gate announcement',
      ok.messages.some(m => m.type === 'version-gate' && m.minPeerVersion === '0.14.0'));

    // ── 2. Compliant client: ABOVE min version (forward-compatible) ──
    const newer = await probe({ helloVersion: '0.15.0' });
    check('newer-than-min peer admitted', newer.admitted === true);

    // ── 3. Old client below min ──────────────────────────────────────
    const old = await probe({ helloVersion: '0.13.0' });
    check('old peer rejected (not admitted)', old.admitted === false);
    check('old peer closed with 4426',         old.code === 4426);
    check('old peer reason mentions versions',
      typeof old.reason === 'string' && old.reason.includes('below minimum'));

    // ── 4. No client-hello at all → hello-timeout ───────────────────
    const silent = await probe({ helloVersion: undefined });
    check('silent peer rejected',         silent.admitted === false);
    check('silent peer closed with 4426', silent.code === 4426);
    check('silent peer reason mentions timeout',
      typeof silent.reason === 'string' && silent.reason.includes('not received'));

    // ── 5. /healthz reports gate config + admitted/pending counters ──
    const health = await fetch(`http://localhost:${BRIDGE_PORT}/healthz`).then(r => r.json());
    check('healthz reports minPeerVersion',
      health.minPeerVersion === '0.14.0');
    check('healthz reports admitted/pending counters',
      typeof health.admitted === 'number' && typeof health.pending === 'number');

    // ── 6. Garbage version string → rejected ─────────────────────────
    const garbage = await probe({ helloVersion: 'not-a-version' });
    check('garbage version rejected',         garbage.admitted === false);
    check('garbage version closed with 4426', garbage.code === 4426);

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
