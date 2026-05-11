// =====================================================================
// Axona Peer — Phase 2 browser client
//
// Connects to a single bridge over WebSocket (Phase 1 behavior), then
// uses the bridge's signaling messages to set up a WebRTC mesh with
// every other peer (Phase 2 behavior).
//
// The UI shows one row per connection.  The bridge appears as the
// first row (always present); WebRTC peers appear as additional rows
// as they come and go.  Each row has its own indicator:
//
//   red    — disconnected / failed
//   yellow — connecting, signaling, awaiting first pong, or stale
//   green  — open + pongs flowing on schedule
//
// Bridge URL: configurable via ?bridge=ws[s]://... query string.
// Default: ws://localhost:8080 for local dev.
// =====================================================================

import { MeshManager } from './mesh.js';

const BRIDGE_PING_INTERVAL_MS = 1000;
const BRIDGE_STALE_PONG_MS    = 3000;
const RTT_WINDOW              = 10;
const BACKOFF_INITIAL_MS      = 1000;
const BACKOFF_MAX_MS          = 16000;
const UPTIME_TICK_MS          = 1000;
const LOG_MAX_LINES           = 120;
const BRIDGE_ROW_ID           = '@bridge';   // synthetic id for the bridge row

// ── DOM handles ──────────────────────────────────────────────────────
const $bridgeUrl   = document.getElementById('bridge-url');
const $myId        = document.getElementById('my-id');
const $meshCount   = document.getElementById('mesh-count');
const $peerTable   = document.getElementById('peer-table-body');
const $logList     = document.getElementById('log-list');
const $logClear    = document.getElementById('log-clear');

// ── Bridge URL resolution ────────────────────────────────────────────
function getBridgeUrl() {
  const fromQs = new URLSearchParams(location.search).get('bridge');
  if (fromQs) return fromQs;
  return 'ws://localhost:8080';
}
const bridgeUrl = getBridgeUrl();
$bridgeUrl.textContent = bridgeUrl;

// ── Bridge connection state (one "peer" for rendering purposes) ──────
const bridge = {
  url:          bridgeUrl,
  ws:           null,
  state:        'disconnected', // 'disconnected' | 'connecting' | 'open' | 'stale'
  pings:        0,
  pongs:        0,
  lastPongAt:   0,
  rttBuffer:    [],
  connectedAt:  0,
  backoffMs:    BACKOFF_INITIAL_MS,
  myConnId:     null,
  // timers
  pingTimer:    null,
  staleTimer:   null,
  uptimeTimer:  null,
  reconnectTimer: null,
};

// ── Mesh manager ─────────────────────────────────────────────────────
const mesh = new MeshManager({
  sendSignal: (toPeerId, payload) => {
    if (!bridge.ws || bridge.ws.readyState !== WebSocket.OPEN) {
      appendLog('signal-drop-no-bridge', `to=${toPeerId}`, 'error');
      return;
    }
    try {
      bridge.ws.send(JSON.stringify({
        type: 'signal', to: toPeerId, payload,
      }));
    } catch (err) {
      appendLog('signal-send-failed', err.message, 'error');
    }
  },
  log: (event, extra) => {
    const detail = extra && Object.keys(extra).length
      ? Object.entries(extra).map(([k, v]) => `${k}=${v}`).join(' ')
      : '';
    appendLog(`mesh:${event}`, detail);
    render();   // most mesh events imply state change
  },
});

mesh.onChange(() => render());

// ── Event log ────────────────────────────────────────────────────────
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

// ── Renderer ─────────────────────────────────────────────────────────
// One render() pass rebuilds the peer table from the bridge state +
// the mesh snapshot.  This is cheap enough at our scale (≤ ~30 rows
// even in a small mesh) to not bother with diffing.

function fmtRelative(ms) {
  if (!ms) return '—';
  const delta = Date.now() - ms;
  if (delta < 1000) return `${delta} ms ago`;
  if (delta < 60_000) return `${(delta / 1000).toFixed(1)} s ago`;
  return `${Math.floor(delta / 60_000)} m ago`;
}

function fmtUptime(openedAt) {
  if (!openedAt) return '—';
  const s = Math.floor((Date.now() - openedAt) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

function renderRow({ peerId, label, kind, state, rttLast, rttAvg, pings, pongs, openedAt }) {
  const tr = document.createElement('tr');
  tr.className = `peer-row peer-row-${kind}`;
  tr.dataset.peerId = peerId;

  // ── Indicator cell ──
  const tdInd = document.createElement('td');
  const indClass = {
    disconnected:           'indicator-red',
    failed:                 'indicator-red',
    closed:                 'indicator-red',
    new:                    'indicator-yellow',
    connecting:             'indicator-yellow',
    signaling:              'indicator-yellow',
    'datachannel-opening':  'indicator-yellow',
    stale:                  'indicator-yellow',
    open:                   'indicator-green',
  }[state] ?? 'indicator-red';
  const dot = document.createElement('div');
  dot.className = `indicator ${indClass}`;
  tdInd.appendChild(dot);
  tr.appendChild(tdInd);

  // ── Peer cell ──
  const tdPeer = document.createElement('td');
  const lbl = document.createElement('div');
  lbl.className = 'peer-label';
  lbl.textContent = label;
  const sub = document.createElement('div');
  sub.className = 'peer-sub';
  sub.textContent = state;
  tdPeer.appendChild(lbl);
  tdPeer.appendChild(sub);
  tr.appendChild(tdPeer);

  // ── RTT cell ──
  const tdRtt = document.createElement('td');
  tdRtt.className = 'cell-num';
  if (rttLast != null) {
    tdRtt.innerHTML = `${rttLast.toFixed?.(0) ?? rttLast}<span class="cell-sub">ms · avg ${rttAvg.toFixed(1)}</span>`;
  } else {
    tdRtt.textContent = '—';
  }
  tr.appendChild(tdRtt);

  // ── Traffic cell ──
  const tdTraf = document.createElement('td');
  tdTraf.className = 'cell-num';
  tdTraf.innerHTML = `${pongs}<span class="cell-sub">${pings} sent</span>`;
  tr.appendChild(tdTraf);

  // ── Uptime cell ──
  const tdUp = document.createElement('td');
  tdUp.className = 'cell-num';
  tdUp.textContent = fmtUptime(openedAt);
  tr.appendChild(tdUp);

  return tr;
}

function render() {
  $peerTable.innerHTML = '';

  // Row 0: bridge (always present, even when disconnected)
  const bridgeRtt = bridge.rttBuffer.at(-1) ?? null;
  const bridgeAvg = bridge.rttBuffer.length
    ? bridge.rttBuffer.reduce((a, b) => a + b, 0) / bridge.rttBuffer.length
    : null;
  $peerTable.appendChild(renderRow({
    peerId:   BRIDGE_ROW_ID,
    label:    'bridge',
    kind:     'bridge',
    state:    bridge.state,
    rttLast:  bridgeRtt,
    rttAvg:   bridgeAvg,
    pings:    bridge.pings,
    pongs:    bridge.pongs,
    openedAt: bridge.connectedAt,
  }));

  // Rows 1..N: WebRTC peers, sorted by peerId for stability
  const peers = mesh.getPeers().sort((a, b) =>
    a.peerId < b.peerId ? -1 : a.peerId > b.peerId ? 1 : 0);
  for (const p of peers) {
    $peerTable.appendChild(renderRow({
      peerId:   p.peerId,
      label:    p.peerId,
      kind:     'rtc',
      state:    p.state,
      rttLast:  p.rttLast,
      rttAvg:   p.rttAvg,
      pings:    p.pings,
      pongs:    p.pongs,
      openedAt: p.openedAt,
    }));
  }

  // Header counts.
  const openPeers = peers.filter(p => p.state === 'open').length;
  $meshCount.textContent = `${openPeers} of ${peers.length}`;
  $myId.textContent = bridge.myConnId ?? '—';
}

// ── Bridge connection lifecycle ──────────────────────────────────────

function setBridgeState(next) {
  if (bridge.state === next) return;
  bridge.state = next;
  render();
}

function connectBridge() {
  clearTimeout(bridge.reconnectTimer);
  bridge.reconnectTimer = null;
  setBridgeState('connecting');
  appendLog('bridge:connecting', bridgeUrl);

  try {
    bridge.ws = new WebSocket(bridgeUrl);
  } catch (err) {
    appendLog('bridge:error', err.message, 'error');
    scheduleBridgeReconnect();
    return;
  }

  bridge.ws.addEventListener('open', onBridgeOpen);
  bridge.ws.addEventListener('message', onBridgeMessage);
  bridge.ws.addEventListener('close', onBridgeClose);
  bridge.ws.addEventListener('error', onBridgeError);
}

function onBridgeOpen() {
  appendLog('bridge:open', null, 'ok');
  setBridgeState('connecting');     // green only after first pong
  bridge.backoffMs = BACKOFF_INITIAL_MS;
  bridge.pings = 0;
  bridge.pongs = 0;
  bridge.rttBuffer = [];
  bridge.connectedAt = Date.now();
  bridge.lastPongAt = 0;
  startBridgePingLoop();
  startBridgeStaleChecker();
  startUptimeTicker();
  render();
}

function onBridgeMessage(ev) {
  let msg;
  try { msg = JSON.parse(ev.data); }
  catch { appendLog('bridge:bad-json', null, 'error'); return; }

  switch (msg.type) {
    case 'welcome':
      bridge.myConnId = msg.connId;
      mesh.setMyId(msg.connId);
      appendLog('bridge:welcome', `id=${msg.connId} v${msg.version ?? '?'}`, 'ok');
      render();
      break;

    case 'peer-list':
      appendLog('bridge:peer-list', `peers=${msg.peers.length}`);
      mesh.onPeerList(msg.peers);
      break;

    case 'peer-joined':
      appendLog('bridge:peer-joined', msg.peerId);
      mesh.onPeerJoined(msg.peerId);
      break;

    case 'peer-left':
      appendLog('bridge:peer-left', msg.peerId);
      mesh.onPeerLeft(msg.peerId);
      break;

    case 'signal':
      mesh.onSignal(msg.from, msg.payload);
      break;

    case 'pong': {
      const rtt = Date.now() - msg.t;
      bridge.pongs++;
      bridge.rttBuffer.push(rtt);
      if (bridge.rttBuffer.length > RTT_WINDOW) bridge.rttBuffer.shift();
      bridge.lastPongAt = Date.now();
      setBridgeState('open');
      // Don't render every pong — wait for state-change paths to render
      // OR for the uptime ticker to tick.  But we DO want RTT to update
      // visibly, so render here too.
      render();
      break;
    }

    default:
      // Unknown messages from a future bridge revision — just log.
      appendLog('bridge:unknown', msg.type);
  }
}

function onBridgeClose(ev) {
  appendLog('bridge:close', `code=${ev.code}${ev.reason ? ' reason=' + ev.reason : ''}`);
  cleanupBridgeTimers();
  bridge.ws = null;
  bridge.myConnId = null;
  // We deliberately do NOT tear down the mesh here.  Existing WebRTC
  // DataChannels are peer-to-peer; the bridge is not in the data path
  // for them once they're open.  If a peer dies, our PC's own
  // connectionstatechange will tell us — we don't need the bridge to
  // notify us via peer-left.  This lets the mesh survive a bridge
  // outage: peers keep ping/pong'ing each other while we reconnect.
  //
  // When the bridge comes back, the new `peer-list` is purely
  // additive: we initiate to any peers in it that we don't already
  // have.  Stale entries in our mesh (peers that died during the
  // outage) will already be cleaning themselves up via WebRTC's own
  // health monitoring.
  setBridgeState('disconnected');
  scheduleBridgeReconnect();
}

function onBridgeError() {
  appendLog('bridge:ws-error', null, 'error');
}

function startBridgePingLoop() {
  clearInterval(bridge.pingTimer);
  bridge.pingTimer = setInterval(() => {
    if (!bridge.ws || bridge.ws.readyState !== WebSocket.OPEN) return;
    try {
      bridge.ws.send(JSON.stringify({ type: 'ping', t: Date.now() }));
      bridge.pings++;
    } catch (err) {
      appendLog('bridge:ping-send-failed', err.message, 'error');
    }
  }, BRIDGE_PING_INTERVAL_MS);
}

function startBridgeStaleChecker() {
  clearInterval(bridge.staleTimer);
  bridge.staleTimer = setInterval(() => {
    if (bridge.state !== 'open' && bridge.state !== 'stale') return;
    if (bridge.lastPongAt === 0) return;
    const since = Date.now() - bridge.lastPongAt;
    if (since > BRIDGE_STALE_PONG_MS && bridge.state !== 'stale') {
      setBridgeState('stale');
      appendLog('bridge:pong-stale', `${since}ms`, 'error');
    } else if (since <= BRIDGE_STALE_PONG_MS && bridge.state === 'stale') {
      setBridgeState('open');
    }
  }, 500);
}

function startUptimeTicker() {
  clearInterval(bridge.uptimeTimer);
  bridge.uptimeTimer = setInterval(() => render(), UPTIME_TICK_MS);
}

function cleanupBridgeTimers() {
  clearInterval(bridge.pingTimer);    bridge.pingTimer = null;
  clearInterval(bridge.staleTimer);   bridge.staleTimer = null;
  clearInterval(bridge.uptimeTimer);  bridge.uptimeTimer = null;
  bridge.connectedAt = 0;
}

function scheduleBridgeReconnect() {
  appendLog('bridge:reconnect-in', `${bridge.backoffMs}ms`);
  bridge.reconnectTimer = setTimeout(() => {
    bridge.backoffMs = Math.min(bridge.backoffMs * 2, BACKOFF_MAX_MS);
    connectBridge();
  }, bridge.backoffMs);
}

// ── UI controls ──────────────────────────────────────────────────────
$logClear.addEventListener('click', () => { $logList.innerHTML = ''; });

window.addEventListener('beforeunload', () => {
  mesh.dispose();
  if (bridge.ws && bridge.ws.readyState === WebSocket.OPEN) {
    try { bridge.ws.close(1000, 'page unload'); } catch {}
  }
});

// ── Go ───────────────────────────────────────────────────────────────
appendLog('boot', `bridge=${bridgeUrl}`);
render();
connectBridge();
