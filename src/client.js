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
//   orange — connecting, signaling, awaiting first pong, or stale
//   green  — open + pongs flowing on schedule
//
// Bridge URL: configurable via ?bridge=ws[s]://... query string.
// Default: wss://bridge.axona.net (production).  Override with
// ?bridge=ws://localhost:8080 for local development.
// =====================================================================

import { MeshManager } from './mesh.js';
import { renderQR }    from './qr.js';

// Peer build number.  Bump the middle digit on every code change so
// you can confirm a redeployed page actually loaded (esp. on iOS,
// where the bfcache can serve a stale module set for ages).  The
// bridge version arrives separately in its `welcome` message; the
// "version" row in the me panel shows both side by side.
const PEER_VERSION = '0.4.0';

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
const $version     = document.getElementById('version');
const $qrLauncher  = document.getElementById('qr-launcher');
const $qrOverlay   = document.getElementById('qr-overlay');
const $qrOverlayImg = document.getElementById('qr-overlay-image');
const $qrOverlayUrl = document.getElementById('qr-overlay-url');
const $qrOverlayClose = document.getElementById('qr-overlay-close');

// ── Bridge URL resolution ────────────────────────────────────────────
function getBridgeUrl() {
  const fromQs = new URLSearchParams(location.search).get('bridge');
  if (fromQs) return fromQs;
  // Default bridge: the project-operated public introducer.  Override
  // via ?bridge=ws://localhost:8080 for local development against a
  // bridge running on your own machine.
  return 'wss://bridge.axona.net';
}
const bridgeUrl = getBridgeUrl();
$bridgeUrl.textContent = bridgeUrl;

// ── Share QR ─────────────────────────────────────────────────────────
// Encode the current full URL — including any ?bridge= override — so a
// phone that scans it lands on the same mesh.  We don't reconstruct
// the URL; location.href is the canonical source of truth for "where
// am I right now."
//
// Two renderings of the same QR:
//   - Small thumbnail in the floating top-right launcher button.
//   - Large version inside the overlay, rendered once at load so the
//     overlay opens instantly on first click.
renderQR($qrLauncher,   location.href);
renderQR($qrOverlayImg, location.href);
$qrOverlayUrl.textContent = location.href;

// Open / close the overlay.  Per the brief: tapping anywhere on the
// overlay — backdrop, card, or the × button — dismisses it.  We also
// honour ESC because it's the universal modal-close keystroke and
// costs nothing.
function openQrOverlay() {
  $qrOverlay.hidden = false;
  // Defer the focus shift so the click event that opened us doesn't
  // immediately trigger the close handler bound to the overlay.
  requestAnimationFrame(() => $qrOverlayClose.focus());
}
function closeQrOverlay() {
  if ($qrOverlay.hidden) return;
  $qrOverlay.hidden = true;
  $qrLauncher.focus();
}
$qrLauncher.addEventListener('click', openQrOverlay);
$qrOverlay  .addEventListener('click', closeQrOverlay);
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeQrOverlay();
});

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
  version:      null,           // populated by `welcome`
  // timers
  pingTimer:    null,
  staleTimer:   null,
  uptimeTimer:  null,
  reconnectTimer: null,
};

// "peer v0.1.0 · bridge v0.3.0" once the bridge has welcomed us.
// Bridge half stays "—" until welcome arrives — that gap is itself
// useful UX since it signals "WS still negotiating."
function renderVersion() {
  const bridgeStr = bridge.version ? `v${bridge.version}` : '—';
  $version.textContent = `peer v${PEER_VERSION} · bridge ${bridgeStr}`;
}
renderVersion();

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
    // No render() here — mesh state changes already trigger render via
    // mesh.onChange().  Log-only events (ice-candidate-local, stats,
    // pc-state transitions that don't change our `state` field) should
    // not cause re-renders, since render() unnecessarily often makes
    // the CSS blink animation invisible.
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
//
// Diffing renderer.  Each row's <tr> is created once and kept alive
// across renders; only its text content and indicator class change in
// place.  This is what makes the CSS blink animation continue smoothly:
// removing-and-reinserting an element restarts its CSS animation, so a
// naive "rebuild tbody on every render" approach with the previous
// renderer caused the indicator to never visibly blink under high
// render frequency (every pong fires a render).
//
// Note that we deliberately allow the indicator class to swap between
// `.indicator-green` and `.indicator-orange` — both declare the same
// `animation: blink 1s step-end infinite`, so the browser keeps the
// running animation rather than restarting it.  Only transitions to
// `.indicator-red` (which has no animation) reset the cycle.

const INDICATOR_CLASS = {
  disconnected:           'indicator-red',
  failed:                 'indicator-red',
  closed:                 'indicator-red',
  new:                    'indicator-orange',
  connecting:             'indicator-orange',
  signaling:              'indicator-orange',
  'datachannel-opening':  'indicator-orange',
  stale:                  'indicator-orange',
  open:                   'indicator-green',
};

/** Cached row references — peerId → DOM nodes we update directly. */
const rowElements = new Map();

function fmtUptime(openedAt) {
  if (!openedAt) return '—';
  const s = Math.floor((Date.now() - openedAt) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

function setText(node, text) {
  if (node.nodeValue !== text) node.nodeValue = text;
}
function setClass(el, cls) {
  if (el.className !== cls) el.className = cls;
}

function buildRow(peerId) {
  const tr = document.createElement('tr');
  tr.dataset.peerId = peerId;

  // Indicator cell — the only animated element.
  const tdInd = document.createElement('td');
  const indicator = document.createElement('div');
  indicator.className = 'indicator';
  tdInd.appendChild(indicator);
  tr.appendChild(tdInd);

  // Peer cell — label + state subtext.
  // The state subtext gets an optional " · via TURN" badge appended
  // as a child span; we keep them as separate nodes so the badge can
  // be styled distinctly and hidden when the path is direct.
  const tdPeer = document.createElement('td');
  const labelEl = document.createElement('div');
  labelEl.className = 'peer-label';
  const labelText = document.createTextNode('');
  labelEl.appendChild(labelText);
  const stateEl = document.createElement('div');
  stateEl.className = 'peer-sub';
  const stateText = document.createTextNode('');
  stateEl.appendChild(stateText);
  const pathBadge = document.createElement('span');
  pathBadge.className = 'path-badge';
  const pathBadgeText = document.createTextNode('');
  pathBadge.appendChild(pathBadgeText);
  stateEl.appendChild(pathBadge);
  tdPeer.appendChild(labelEl);
  tdPeer.appendChild(stateEl);
  tr.appendChild(tdPeer);

  // RTT cell — primary value + " ms · avg N.N" sub.
  const tdRtt = document.createElement('td');
  tdRtt.className = 'cell-num';
  const rttPrimary = document.createTextNode('—');
  const rttSub = document.createElement('span');
  rttSub.className = 'cell-sub';
  const rttSubText = document.createTextNode('');
  rttSub.appendChild(rttSubText);
  tdRtt.appendChild(rttPrimary);
  tdRtt.appendChild(rttSub);
  tr.appendChild(tdRtt);

  // Traffic cell — pongs / sent.
  const tdTraf = document.createElement('td');
  tdTraf.className = 'cell-num';
  const trafPrimary = document.createTextNode('0');
  const trafSub = document.createElement('span');
  trafSub.className = 'cell-sub';
  const trafSubText = document.createTextNode('');
  trafSub.appendChild(trafSubText);
  tdTraf.appendChild(trafPrimary);
  tdTraf.appendChild(trafSub);
  tr.appendChild(tdTraf);

  // Uptime cell.
  const tdUp = document.createElement('td');
  tdUp.className = 'cell-num';
  const upText = document.createTextNode('—');
  tdUp.appendChild(upText);
  tr.appendChild(tdUp);

  return {
    tr,
    indicator,
    labelText, stateText,
    pathBadgeText,
    rttPrimary, rttSubText,
    trafPrimary, trafSubText,
    upText,
  };
}

function updateRow(refs, data) {
  setClass(refs.tr, `peer-row peer-row-${data.kind}`);
  setClass(refs.indicator, `indicator ${INDICATOR_CLASS[data.state] ?? 'indicator-red'}`);
  setText(refs.labelText, data.label);
  setText(refs.stateText, data.state);

  // Path badge: empty by default, " · via TURN" when either end of the
  // nominated candidate pair is a TURN relay.  Hidden visually when
  // empty so spacing collapses cleanly.  Tooltip on the row carries
  // the precise pair (e.g. "path: srflx ↔ relay") for the curious.
  const viaRelay = data.localCand === 'relay' || data.remoteCand === 'relay';
  setText(refs.pathBadgeText, viaRelay ? ' · TURN' : '');
  const title = (data.localCand && data.remoteCand)
    ? `path: ${data.localCand} ↔ ${data.remoteCand}`
    : '';
  if (refs.tr.title !== title) refs.tr.title = title;

  if (data.rttLast != null) {
    setText(refs.rttPrimary, String(Math.round(data.rttLast)));
    setText(refs.rttSubText, ` ms · avg ${data.rttAvg.toFixed(1)}`);
  } else {
    setText(refs.rttPrimary, '—');
    setText(refs.rttSubText, '');
  }

  setText(refs.trafPrimary, String(data.pongs));
  setText(refs.trafSubText, ` ${data.pings} sent`);
  setText(refs.upText, fmtUptime(data.openedAt));
}

function render() {
  // Build the desired ordered list of row data.
  const peers = mesh.getPeers().sort((a, b) =>
    a.peerId < b.peerId ? -1 : a.peerId > b.peerId ? 1 : 0);

  const bridgeRtt = bridge.rttBuffer.at(-1) ?? null;
  const bridgeAvg = bridge.rttBuffer.length
    ? bridge.rttBuffer.reduce((a, b) => a + b, 0) / bridge.rttBuffer.length
    : null;

  const ordered = [
    {
      peerId:     BRIDGE_ROW_ID,
      label:      'bridge',
      kind:       'bridge',
      state:      bridge.state,
      rttLast:    bridgeRtt,
      rttAvg:     bridgeAvg,
      pings:      bridge.pings,
      pongs:      bridge.pongs,
      openedAt:   bridge.connectedAt,
      // Bridge connection is plain WebSocket, no ICE involved.  Null
      // here means "no path badge, no tooltip" — bridges aren't relayed.
      localCand:  null,
      remoteCand: null,
    },
    ...peers.map(p => ({
      peerId:     p.peerId,
      label:      p.peerId,
      kind:       'rtc',
      state:      p.state,
      rttLast:    p.rttLast,
      rttAvg:     p.rttAvg,
      pings:      p.pings,
      pongs:      p.pongs,
      openedAt:   p.openedAt,
      localCand:  p.localCand,
      remoteCand: p.remoteCand,
    })),
  ];

  // Remove rows for peers that have left.
  const wanted = new Set(ordered.map(d => d.peerId));
  for (const [pid, refs] of rowElements) {
    if (!wanted.has(pid)) {
      refs.tr.remove();
      rowElements.delete(pid);
    }
  }

  // Walk the desired order, reconciling DOM position with cached refs.
  // We only call insertBefore/appendChild when the DOM is actually out
  // of sync — in steady state this is a no-op, which is what keeps the
  // CSS animations running.
  let domCursor = $peerTable.firstElementChild;
  for (const data of ordered) {
    let refs = rowElements.get(data.peerId);
    if (!refs) {
      refs = buildRow(data.peerId);
      rowElements.set(data.peerId, refs);
    }
    updateRow(refs, data);

    if (refs.tr !== domCursor) {
      // Either refs.tr isn't in DOM yet, or it's in DOM but at the
      // wrong position.  insertBefore handles both cases.
      $peerTable.insertBefore(refs.tr, domCursor);
      // domCursor is unchanged — refs.tr was inserted *before* it.
    } else {
      // Already in the right slot; advance.
      domCursor = domCursor.nextElementSibling;
    }
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
      bridge.version  = msg.version ?? null;
      mesh.setMyId(msg.connId);
      // Hand any bridge-supplied TURN credential to the mesh BEFORE
      // peer-list arrives.  peer-list will trigger _initiateTo, which
      // builds RTCPeerConnections using mesh._iceConfig() — by then
      // we want the TURN entry in place so the new PCs can relay.
      mesh.setTurnConfig(msg.turn ?? null);
      appendLog('bridge:welcome', `id=${msg.connId} v${msg.version ?? '?'}${msg.turn ? ' turn=on' : ''}`, 'ok');
      renderVersion();
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
  bridge.version  = null;
  // Drop the cached TURN credential.  When we reconnect, the bridge
  // hands us a fresh one in welcome — minted right then with a full
  // 2h TTL.  Holding the old one risks racing the expiry boundary on
  // a long-running tab that reconnects right before its credential
  // would have expired.
  mesh.setTurnConfig(null);
  renderVersion();
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

// ── Page-resume reset ────────────────────────────────────────────────
//
// When a phone backgrounds the tab (lock screen, app switcher) or the
// laptop sleeps, mobile browsers heavily throttle our timers and the
// OS often kills the WebSocket.  By the time we come back the remote
// side has long since seen us peer-leave; our RTCPeerConnections are
// zombie shells that won't recover on their own and the indicators
// sit orange indefinitely.
//
// The simplest fix that always works: when the page becomes visible
// again after a non-trivial absence, tear down every WebRTC peer and
// force-reconnect the bridge.  The fresh peer-list rebuilds the mesh
// from scratch.  Cheap and correct — there's no per-peer state worth
// salvaging once the underlying transport has died.

const RESUME_HIDDEN_THRESHOLD_MS = 5000;   // ignore brief tab switches
let hiddenAt = 0;

function resetMesh(reason) {
  appendLog('resume', reason, 'ok');
  mesh.reset();
  forceReconnectBridge();
}

function forceReconnectBridge() {
  bridge.backoffMs = BACKOFF_INITIAL_MS;
  clearTimeout(bridge.reconnectTimer);
  bridge.reconnectTimer = null;
  if (bridge.ws && bridge.ws.readyState !== WebSocket.CLOSED) {
    // Closing here triggers onclose → scheduleBridgeReconnect, which
    // honours the backoff we just reset.  No need to call connectBridge
    // directly; that would race the close-handler's reconnect.
    try { bridge.ws.close(1000, 'resume'); } catch {}
  } else {
    bridge.ws = null;
    cleanupBridgeTimers();
    setBridgeState('disconnected');
    connectBridge();
  }
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') {
    hiddenAt = Date.now();
    return;
  }
  // visibility === 'visible'
  if (!hiddenAt) return;
  const wasHiddenFor = Date.now() - hiddenAt;
  hiddenAt = 0;
  if (wasHiddenFor >= RESUME_HIDDEN_THRESHOLD_MS) {
    resetMesh(`visible after ${(wasHiddenFor / 1000).toFixed(1)}s`);
  }
});

// Network came back from offline.  Almost certainly a phone leaving
// airplane mode or a laptop reattaching to Wi-Fi.  Always reset.
window.addEventListener('online', () => {
  resetMesh('network online');
});

// Restoration from the back-forward cache.  iOS Safari is aggressive
// about putting backgrounded pages here.  When we come back from
// bfcache, every timer and socket has been frozen — reset to be safe.
window.addEventListener('pageshow', (ev) => {
  if (ev.persisted) resetMesh('pageshow from bfcache');
});

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
