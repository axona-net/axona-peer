// =====================================================================
// Axona Peer — Phase 1 browser client
//
// Connects to a single bridge over WebSocket, sends a `ping` every
// second, and renders connection state to a colored indicator:
//
//   red    — disconnected (solid)
//   yellow — connecting, OR connected but pongs have stopped (blinking)
//   green  — connected and receiving pongs on schedule (blinking)
//
// Bridge URL is configurable via ?bridge=ws://... in the query string;
// falls back to ws://localhost:8080 for local dev.
//
// Auto-reconnects with exponential backoff (1s → 16s).  Detects stale
// pongs (no pong for > 3 s) without waiting for the socket to close.
// =====================================================================

const PING_INTERVAL_MS   = 1000;
const STALE_PONG_MS      = 3000;
const RTT_WINDOW         = 10;
const BACKOFF_INITIAL_MS = 1000;
const BACKOFF_MAX_MS     = 16000;
const UPTIME_TICK_MS     = 1000;
const LOG_MAX_LINES      = 80;

// ── DOM handles ──────────────────────────────────────────────────────
const $indicator   = document.getElementById('indicator');
const $stateLabel  = document.getElementById('state-label');
const $bridgeUrl   = document.getElementById('bridge-url');
const $rttLast     = document.getElementById('rtt-last');
const $rttAvg      = document.getElementById('rtt-avg');
const $pingCount   = document.getElementById('ping-count');
const $pongCount   = document.getElementById('pong-count');
const $connId      = document.getElementById('conn-id');
const $connUptime  = document.getElementById('conn-uptime');
const $logList     = document.getElementById('log-list');
const $logClear    = document.getElementById('log-clear');

// ── Bridge URL resolution ────────────────────────────────────────────
function getBridgeUrl() {
  const fromQs = new URLSearchParams(location.search).get('bridge');
  if (fromQs) return fromQs;
  // Default for local development.  Override via ?bridge=wss://bridge.axona.net
  return 'ws://localhost:8080';
}
const bridgeUrl = getBridgeUrl();
$bridgeUrl.textContent = bridgeUrl;

// ── Runtime state ────────────────────────────────────────────────────
/** @type {WebSocket | null} */
let ws = null;
/** @type {'disconnected' | 'connecting' | 'connected' | 'stale'} */
let state = 'disconnected';

let pingTimer       = null;
let staleCheckTimer = null;
let uptimeTimer     = null;
let reconnectTimer  = null;

let backoffMs   = BACKOFF_INITIAL_MS;
let lastPongAt  = 0;
let pingCount   = 0;
let pongCount   = 0;
let connectedAt = 0;
let connId      = null;
const rttBuffer = [];

// ── State + UI helpers ───────────────────────────────────────────────
const STATE_TO_CLASS = {
  disconnected: 'indicator-red',
  connecting:   'indicator-yellow',
  stale:        'indicator-yellow',
  connected:    'indicator-green',
};

const STATE_TO_LABEL = {
  disconnected: 'Disconnected',
  connecting:   'Connecting…',
  stale:        'Stale (no pongs)',
  connected:    'Connected',
};

function setState(next, labelOverride) {
  if (state === next && !labelOverride) return;
  state = next;
  $indicator.className = `indicator ${STATE_TO_CLASS[next]}`;
  $stateLabel.textContent = labelOverride ?? STATE_TO_LABEL[next];
}

function appendLog(event, detail, kind) {
  const li = document.createElement('li');
  const ts = document.createElement('span');
  ts.className = 'ts';
  ts.textContent = new Date().toLocaleTimeString();
  const ev = document.createElement('span');
  ev.className = `ev${kind ? ' ev-' + kind : ''}`;
  ev.textContent = detail ? `${event} ${detail}` : event;
  li.appendChild(ts);
  li.appendChild(ev);
  $logList.insertBefore(li, $logList.firstChild);
  while ($logList.children.length > LOG_MAX_LINES) {
    $logList.removeChild($logList.lastChild);
  }
}

function fmtMs(ms) {
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const remS = s % 60;
  return `${m}m ${remS}s`;
}

// ── Connect / reconnect ──────────────────────────────────────────────
function connect() {
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  setState('connecting');
  appendLog('connecting', bridgeUrl);

  try {
    ws = new WebSocket(bridgeUrl);
  } catch (err) {
    appendLog('error', err.message, 'error');
    scheduleReconnect();
    return;
  }

  ws.addEventListener('open', onOpen);
  ws.addEventListener('message', onMessage);
  ws.addEventListener('close', onClose);
  ws.addEventListener('error', onError);
}

function onOpen() {
  appendLog('open', null, 'ok');
  // Don't go straight to green — we wait for the first pong to confirm
  // the bidirectional path.  Until then we render as yellow / connecting.
  setState('connecting', 'Awaiting first pong…');
  backoffMs   = BACKOFF_INITIAL_MS;
  pingCount   = 0;
  pongCount   = 0;
  connectedAt = Date.now();
  rttBuffer.length = 0;
  $pingCount.textContent = '0';
  $pongCount.textContent = '0';
  $rttLast.textContent   = '—';
  $rttAvg.textContent    = '—';
  $connId.textContent    = '—';
  $connUptime.textContent = '0s';

  startPingLoop();
  startStaleChecker();
  startUptimeTicker();
}

function onMessage(ev) {
  let msg;
  try { msg = JSON.parse(ev.data); }
  catch { appendLog('bad-json', null, 'error'); return; }

  if (msg.type === 'welcome') {
    connId = msg.connId;
    $connId.textContent = msg.connId;
    appendLog('welcome', `id=${msg.connId}`);
    return;
  }

  if (msg.type === 'pong') {
    const rtt = Date.now() - msg.t;
    pongCount++;
    $pongCount.textContent = String(pongCount);
    $rttLast.textContent   = `${rtt} ms`;
    rttBuffer.push(rtt);
    if (rttBuffer.length > RTT_WINDOW) rttBuffer.shift();
    const avg = rttBuffer.reduce((a, b) => a + b, 0) / rttBuffer.length;
    $rttAvg.textContent = `${avg.toFixed(1)} ms`;
    lastPongAt = Date.now();
    setState('connected');
    return;
  }
}

function onClose(ev) {
  appendLog('close', `code=${ev.code}${ev.reason ? ' reason=' + ev.reason : ''}`);
  cleanupTimers();
  ws = null;
  connId = null;
  setState('disconnected');
  scheduleReconnect();
}

function onError() {
  // The WebSocket spec hides error details; `close` will fire next with the code.
  appendLog('ws-error', null, 'error');
}

// ── Ping loop / liveness checks ──────────────────────────────────────
function startPingLoop() {
  clearInterval(pingTimer);
  pingTimer = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(JSON.stringify({ type: 'ping', t: Date.now() }));
      pingCount++;
      $pingCount.textContent = String(pingCount);
    } catch (err) {
      appendLog('ping-send-failed', err.message, 'error');
    }
  }, PING_INTERVAL_MS);
}

function startStaleChecker() {
  clearInterval(staleCheckTimer);
  staleCheckTimer = setInterval(() => {
    if (state !== 'connected' && state !== 'stale') return;
    if (lastPongAt === 0) return;   // pre-first-pong; onOpen handles that label
    const since = Date.now() - lastPongAt;
    if (since > STALE_PONG_MS && state !== 'stale') {
      setState('stale');
      appendLog('pong-stale', `${since}ms since last`, 'error');
    } else if (since <= STALE_PONG_MS && state === 'stale') {
      setState('connected');
    }
  }, 500);
}

function startUptimeTicker() {
  clearInterval(uptimeTimer);
  uptimeTimer = setInterval(() => {
    if (!connectedAt) return;
    $connUptime.textContent = fmtMs(Date.now() - connectedAt);
  }, UPTIME_TICK_MS);
}

function cleanupTimers() {
  clearInterval(pingTimer);       pingTimer = null;
  clearInterval(staleCheckTimer); staleCheckTimer = null;
  clearInterval(uptimeTimer);     uptimeTimer = null;
  connectedAt = 0;
  $connUptime.textContent = '—';
}

function scheduleReconnect() {
  appendLog('reconnect-in', fmtMs(backoffMs));
  reconnectTimer = setTimeout(() => {
    const usedBackoff = backoffMs;
    backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
    appendLog('reconnect-trying', `(prev wait ${fmtMs(usedBackoff)})`);
    connect();
  }, backoffMs);
}

// ── UI controls ──────────────────────────────────────────────────────
$logClear.addEventListener('click', () => {
  $logList.innerHTML = '';
});

// Clean shutdown so the bridge sees a normal close instead of a timeout.
window.addEventListener('beforeunload', () => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.close(1000, 'page unload'); } catch {}
  }
});

// ── Go ───────────────────────────────────────────────────────────────
appendLog('boot', `bridge=${bridgeUrl}`);
connect();
