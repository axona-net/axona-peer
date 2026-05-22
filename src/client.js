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
import { AxonaNode }   from './axona_node.js';
import {
  REGIONS,
  getCurrentIdentity,
  deriveIdentity,
} from './identity.js';
// v1.1: region-keyed topics use a synthetic publisher whose top 8
// bits match the chosen region's S2 cell, so deriveTopicId emits a
// topic ID with that S2 prefix and us-east peers (S2='df') become
// naturally XOR-closest to us-east topics.
import { geoCellId }   from '../vendor/axona-protocol/src/utils/s2.js';
// Shared codec: same BigInt + Set conventions as mesh.js and the
// bridge's server.js — keeps every channel on one wire format.
import { encode, decode } from './wire.js';
// I3: pub/sub now uses the kernel's unified API via axonaNode.pub /
// axonaNode.sub.  Topics are public-mode strings (anyone-can-publish,
// anyone-can-subscribe) shaped as `${regionId}/${eventName}` so the
// (region, event) addressing UX is preserved without forcing a
// publisher-keyed model.  The application chooses the hashing mode
// per call; `{ publisher: null }` selects the simple-name hash.

// Peer build number.  Bump the middle digit on every code change so
// you can confirm a redeployed page actually loaded (esp. on iOS,
// where the bfcache can serve a stale module set for ages).  The
// bridge version arrives separately in its `welcome` message; the
// "version" row in the me panel shows both side by side.
const PEER_VERSION = '1.1.3';

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

// Phase 3 elements (region picker + axona id + lookup harness)
const $regionPicker   = document.getElementById('region-picker');
const $regionSelect   = document.getElementById('region-select');
const $regionSave     = document.getElementById('region-save');
const $axonaId        = document.getElementById('axona-id');
const $axonaRegion    = document.getElementById('axona-region');
const $synaptomeSize  = document.getElementById('synaptome-size');
const $lookupTarget   = document.getElementById('lookup-target');
const $lookupGo       = document.getElementById('lookup-go');
const $lookupResult   = document.getElementById('lookup-result');

// Pub/Sub harness
const $pubAdd         = document.getElementById('pub-add');
const $pubList        = document.getElementById('pub-list');
const $subAdd         = document.getElementById('sub-add');
const $subForm        = document.getElementById('sub-form');
const $subRegion      = document.getElementById('sub-region');
const $subEvent       = document.getElementById('sub-event');
const $subSave        = document.getElementById('sub-save');
const $subCancel      = document.getElementById('sub-cancel');
const $subList        = document.getElementById('sub-list');

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
      bridge.ws.send(encode({
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

// Traffic-driven indicator pulse.  Every real ping-out / pong-in
// briefly brightens that row's dot, which then fades back to its
// dim resting opacity (320 ms transition in CSS).  The visual
// effect mirrors actual bytes-on-the-wire: healthy peers pulse ~2
// Hz (ping + pong), stale peers pulse ~1 Hz (ping only, pong
// absent), and peers stuck in `connecting` / `signaling` do not
// pulse at all — their indicator sits dim and steady, which is the
// honest signal that nothing is actually happening on that
// connection.  Replaces the prior continuous `animation: blink 1s
// step-end infinite` CSS animation that ran off a clock regardless
// of traffic.  Used both for the WebRTC peer rows (driven by
// mesh.onPingTraffic) and the bridge row (driven by the bridge
// ping/pong loop in this file).
const PULSE_HOLD_MS = 80;
function pulseIndicator(peerId) {
  const refs = rowElements.get(peerId);
  if (!refs) return;
  // Already pulsing — restart the hold timer rather than stacking.
  refs.indicator.classList.add('indicator--pulse');
  clearTimeout(refs._pulseTimer);
  refs._pulseTimer = setTimeout(() => {
    refs.indicator.classList.remove('indicator--pulse');
  }, PULSE_HOLD_MS);
}
mesh.onPingTraffic((peerId, _kind) => pulseIndicator(peerId));

// ── Event log ────────────────────────────────────────────────────────
function appendLog(event, detail, kind) {
  const li = document.createElement('li');
  const ts = document.createElement('span');
  ts.className = 'ts';
  ts.textContent = new Date().toLocaleTimeString();
  const ev = document.createElement('span');
  ev.className = `ev${kind ? ' ev-' + kind : ''}`;
  // detail can be a string, number, or an object — stringify objects
  // as compact JSON (BigInt → "<hex>n", arrays inline) so pubsub /
  // mesh traces are actually readable in the log.
  let detailText = '';
  if (detail != null) {
    if (typeof detail === 'object') {
      try {
        detailText = ' ' + JSON.stringify(detail, (_k, v) =>
          typeof v === 'bigint' ? v.toString(16) + 'n' : v);
      } catch { detailText = ' ' + String(detail); }
    } else {
      detailText = ' ' + String(detail);
    }
  }
  ev.textContent = event + detailText;
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
// place.  Reusing the DOM nodes is also what lets the indicator's
// `.indicator--pulse` class survive across renders — the renderer
// edits class names in place rather than rebuilding the tbody, so a
// pulse applied by mesh.onPingTraffic isn't yanked out from under
// the CSS opacity transition by the next render tick.

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
  // Indicator: state-color class via classList so a concurrently-
  // applied `.indicator--pulse` (from mesh.onPingTraffic) isn't
  // overwritten by the next render.  setClass's bulk assignment
  // would wipe it; classList lets the pulse and the state class
  // coexist.
  const stateClass = INDICATOR_CLASS[data.state] ?? 'indicator-red';
  if (!refs.indicator.classList.contains(stateClass)) {
    refs.indicator.classList.remove(
      'indicator-red', 'indicator-orange', 'indicator-green',
    );
    refs.indicator.classList.add(stateClass);
  }
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

  // Identify ourselves so the bridge's version gate can decide whether
  // to admit us.  This MUST be the first message we send — the bridge
  // drops anything else from un-admitted connections.  If we're too
  // old (or this message is missing), the bridge closes with
  // CLOSE_UPGRADE_REQUIRED (4426) and onBridgeClose surfaces a banner.
  try {
    bridge.ws.send(encode({ type: 'client-hello', version: PEER_VERSION }));
  } catch (err) {
    appendLog('bridge:client-hello-send-failed', err.message, 'error');
  }

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
  try { msg = decode(ev.data); }
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
      pulseIndicator(BRIDGE_ROW_ID);
      break;
    }

    case 'version-gate':
      // The bridge announces the minimum peer version it'll admit
      // BEFORE waiting for client-hello.  Pure informational here —
      // our own version is fixed at PEER_VERSION; if we're below
      // the gate, the bridge will close us with code 4426 next.
      appendLog('bridge:version-gate', `min=${msg.minPeerVersion}`);
      break;

    case 'axona':
      // Axona protocol frame (hello / hello-ack / req / res / ntf).
      // Hand off to the AxonaNode's BridgeTransport.  Frames can arrive
      // before bootAxonaNode() completes (the bridge's `hello` lands
      // right after `welcome`); in that case the node-init path will
      // pull the buffered frame out of `pendingAxonaFrames` below.
      if (axonaNode) {
        axonaNode.handleBridgeAxonaFrame(msg.payload);
      } else {
        pendingAxonaFrames.push(msg.payload);
      }
      break;

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
  // Tell the AxonaNode the bridge channel is gone so its BridgeTransport
  // unbinds the bridge peer + rejects any in-flight requests.  When the
  // WebSocket reconnects, the new welcome → hello flow will re-admit
  // the bridge as a fresh synapse via `bridge-handshake`.
  if (axonaNode) {
    try { axonaNode.handleBridgeClosed(); }
    catch (err) { appendLog('axona:bridge-close-handler', err.message, 'error'); }
  }
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

  // The bridge enforces a version gate.  If our peer build is below
  // its minimum, it closes with CLOSE_UPGRADE_REQUIRED (4426) and a
  // reason string.  Reconnecting would just fail again the same way,
  // so we stop the retry loop and surface a banner instructing the
  // user to hard-reload the page (Pages may need a cache-bust).
  if (ev.code === 4426) {
    showUpgradeBanner(ev.reason || 'Your client is out of date.');
    return;
  }

  scheduleBridgeReconnect();
}

/**
 * Render a visible, persistent banner when the bridge rejects this
 * client as too old.  The user needs to hard-reload (or close & re-
 * open) to pick up the new peer build.  No automatic reload — that
 * could disrupt typing or in-flight work.
 */
function showUpgradeBanner(reason) {
  if (document.getElementById('upgrade-banner')) return;   // idempotent
  const div = document.createElement('div');
  div.id = 'upgrade-banner';
  div.className = 'upgrade-banner';
  div.innerHTML =
    '<strong>Client out of date</strong><br>' +
    `<span class="upgrade-reason"></span><br><br>` +
    'Reload the page (<code>Cmd&nbsp;+&nbsp;Shift&nbsp;+&nbsp;R</code> on macOS, ' +
    '<code>Ctrl&nbsp;+&nbsp;Shift&nbsp;+&nbsp;R</code> elsewhere) to upgrade.';
  div.querySelector('.upgrade-reason').textContent = reason;
  document.body.insertBefore(div, document.body.firstChild);
}

function onBridgeError() {
  appendLog('bridge:ws-error', null, 'error');
}

function startBridgePingLoop() {
  clearInterval(bridge.pingTimer);
  bridge.pingTimer = setInterval(() => {
    if (!bridge.ws || bridge.ws.readyState !== WebSocket.OPEN) return;
    try {
      bridge.ws.send(encode({ type: 'ping', t: Date.now() }));
      bridge.pings++;
      pulseIndicator(BRIDGE_ROW_ID);
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
  if (axonaNode) try { axonaNode.stop(); } catch {}
  if (bridge.ws && bridge.ws.readyState === WebSocket.OPEN) {
    try { bridge.ws.close(1000, 'page unload'); } catch {}
  }
});

// ── Axona protocol layer (Phase 3) ──────────────────────────────────
//
// On boot: if a persisted identity exists, instantiate AxonaNode
// immediately.  Otherwise show the region picker and wait for the
// user to choose; on save, derive an identity and instantiate.

let axonaNode = null;

// Axona frames that arrive on the bridge WebSocket BEFORE the
// AxonaNode is instantiated (e.g., the bridge's `hello` lands within
// milliseconds of welcome, but bootAxonaNode is async because identity
// derivation needs Web Crypto).  We buffer them here and drain into
// the node as soon as it's ready, so the bridge-handshake completes on
// the very first hello rather than waiting for a retry.
const pendingAxonaFrames = [];

// Bridge adapter shape consumed by AxonaNode → BridgeTransport.
// Reads `bridge.ws` from the surrounding closure so the same adapter
// keeps working across reconnects (we never replace the adapter, just
// the underlying WebSocket).
const bridgeAdapter = {
  sendToBridge(msg) {
    if (!bridge.ws || bridge.ws.readyState !== WebSocket.OPEN) return false;
    try {
      bridge.ws.send(encode(msg));
      return true;
    } catch (err) {
      appendLog('axona:bridge-send-failed', err.message, 'error');
      return false;
    }
  },
  isBridgeOpen() {
    return !!bridge.ws && bridge.ws.readyState === WebSocket.OPEN;
  },
};

function populateRegionDropdown() {
  for (const r of REGIONS) {
    const opt = document.createElement('option');
    opt.value = r.id;
    opt.textContent = r.label;
    $regionSelect.appendChild(opt);
  }
}

function showRegionPicker() {
  populateRegionDropdown();
  $regionPicker.hidden = false;
  $regionSave.addEventListener('click', async () => {
    const choice = REGIONS.find(r => r.id === $regionSelect.value) ?? REGIONS[0];
    $regionPicker.hidden = true;
    await bootAxonaNode({ region: choice });
  }, { once: true });
}

function fmtNodeId(id) {
  if (typeof id !== 'bigint') return '—';
  // v1.1: 264-bit (66-char hex) IDs.  Display in full so users can
  // verify the S2 prefix and pubkey-derived hash match what their
  // tools expect; the mono class keeps it inline.
  return id.toString(16).padStart(66, '0');
}

async function bootAxonaNode(opts = {}) {
  const identity = await deriveIdentity(opts);
  $axonaId.textContent     = fmtNodeId(identity.id);
  $axonaRegion.textContent = identity.region?.label
    ?? identity.region?.id
    ?? identity.region?.source
    ?? '—';

  axonaNode = new AxonaNode({
    identity,
    log: (event, detail) => appendLog(`axona:${event}`, detail),
  });
  await axonaNode.start(mesh, bridgeAdapter);

  // Debug surface for DevTools.  Exposed on window so operators can
  // introspect live state without rebuilding — `window.axona.synaptome()`,
  // `window.axona.subs()`, etc.  All read-only — no setters that could
  // accidentally tamper with running state.
  const hex = (id) => typeof id === 'bigint'
    ? id.toString(16).padStart(66, '0')
    : String(id);

  window.axona = {
    /** Raw node reference — for poking around when needed. */
    node:        axonaNode,
    /** Identity object (nodeId + region + geoBits). */
    identity,
    /** Snapshot of every Synapse currently in our routing table. */
    synaptome:   () => axonaNode.getSynaptome().map(s => ({
                   ...s,
                   peerId: hex(s.peerId),
                 })),
    /** Active subscriptions (UI-side state) with their topic keys + msg counts. */
    subs:        () => subscriptions.map(s => ({
                   regionId: s.regionId,
                   eventName: s.eventName,
                   key: hex(s.key),
                   messageCount: s.messages.length,
                 })),
    /** Configured publish cards. */
    publishForms: () => publishForms.slice(),
    /** Live bridge connection state — useful when handshake stalls. */
    bridge:      () => ({
                   state:    bridge.state,
                   myConnId: bridge.myConnId,
                   version:  bridge.version,
                   url:      bridge.url,
                   wsReady:  bridge.ws?.readyState,
                 }),
    /** Mesh peers with their connection states (the ones reachable via WebRTC). */
    mesh:        () => mesh.getPeers(),
    /** Which peer nodeIds is the underlying transport bound to right now?
     *  This is the set of peers we can reach via sendDirect.  Useful when
     *  an axon supposedly has a subscriber in role.children but
     *  transport.notify silently drops because the binding doesn't exist. */
    boundPeers:  () => {
      const t = axonaNode._transport;
      const out = [];
      // CompositeTransport: walk each sub-transport's connId↔nodeId maps.
      for (const sub of (t?._subs ?? [])) {
        if (sub._meshIdByNodeId) {
          for (const [nodeId] of sub._meshIdByNodeId) out.push({ via: 'mesh', nodeId: hex(nodeId) });
        }
        if (sub._bridgeNodeId) out.push({ via: 'bridge', nodeId: hex(sub._bridgeNodeId) });
      }
      return out;
    },
    /** Resolve a (regionId, eventName) to its 264-bit hex topic ID
     *  (public-mode: '00' + sha256(`${regionId}/${eventName}`)). */
    topicKey: async (regionId, eventName) => {
      const { deriveTopicId } = await import(
        '../vendor/axona-protocol/src/pubsub/post.js'
      );
      // v1.1: region-keyed — same synth publisher addSubscription /
      // publishFromForm use, so DevTools shows the topic ID that's
      // actually routed.
      return deriveTopicId(regionSynthPublisher(regionId), `${regionId}/${eventName}`);
    },
    /** Run findKClosest for a topic.  Reveals WHICH K peers this node
     *  thinks are the topic's axon set — the most important diagnostic
     *  when subscribes and publishes seem to land on different roots. */
    findKClosest: async (topicHex, K = 5) => {
      const t = typeof topicHex === 'string' ? BigInt('0x' + topicHex.replace(/^0x/, '')) : topicHex;
      const ids = await axonaNode._peer.findKClosest(t, K);
      return ids.map(hex);
    },
    /** Topics we're an axon role-holder for, with our children (subscribers).
     *  If we expected to be an axon and this is empty, our subscribe didn't
     *  propagate.  If a subscriber we expected to deliver to ISN'T listed
     *  here, they never subscribed at us. */
    axonRoles:   () => {
      const axon = axonaNode._axon;
      if (!axon?.axonRoles) return [];
      return [...axon.axonRoles.entries()].map(([topicId, role]) => ({
        topic:    hex(topicId),
        isRoot:   role.isRoot,
        children: [...role.children.keys()].map(hex),
        peerRoots: role.peerRoots ? [...role.peerRoots].map(hex) : [],
      }));
    },
    /** Topics we're subscribed to (from AxonManager's POV, not just the
     *  UI's `subs()`).  Should match — if it doesn't, we have a sync bug. */
    mySubscriptions: () => {
      const axon = axonaNode._axon;
      if (!axon?.mySubscriptions) return [];
      return [...axon.mySubscriptions.keys()].map(hex);
    },
  };

  // Drain any axona frames that arrived during the async boot.  The
  // bridge sends `hello` right after `welcome`; identity derivation
  // (post.js + Web Crypto) takes ~1 frame, so the hello almost always
  // lands before this point in single-tab cold-loads.
  while (pendingAxonaFrames.length) {
    const payload = pendingAxonaFrames.shift();
    try { axonaNode.handleBridgeAxonaFrame(payload); }
    catch (err) { appendLog('axona:drain-failed', err.message, 'error'); }
  }

  appendLog('axona:ready', { nodeId: fmtNodeId(identity.id) });
  $lookupGo.disabled = false;

  // Pub/Sub: populate dropdowns + restore persisted publish forms and
  // subscriptions, then enable the controls.
  const homeRegionId = identity.region?.id ?? REGIONS[0].id;
  populateSubRegionDropdown(homeRegionId);

  publishForms = loadPersistedPublishForms();
  if (publishForms.length === 0) {
    publishForms = [newPublishForm(homeRegionId, '')];
    persistPublishForms();
  }
  renderPublishForms();
  $pubAdd.disabled = false;
  $subAdd.disabled = false;

  const persisted = loadPersistedSubscriptions();
  for (const s of persisted) {
    try { await addSubscription(s.regionId, s.eventName); }
    catch (err) { appendLog('pubsub:restore-failed', err.message, 'error'); }
  }

  // Refresh synaptome counter on any mesh change (cheap proxy).
  const updateSynaptomeBadge = () => {
    if (!axonaNode) return;
    $synaptomeSize.textContent = String(axonaNode.getSynaptome().length);
  };
  mesh.onChange(updateSynaptomeBadge);
  setInterval(updateSynaptomeBadge, 1000);
}

// ── Pub/Sub UI ────────────────────────────────────────────────────────
//
// State shape — kept entirely in client.js memory + localStorage:
//
//   subscriptions: [
//     { regionId, eventName, topic, handle, messages: [{publisher, msg, ts, msgId}, ...] }
//   ]
//
// `topic` is the public-mode topic string (`${regionId}/${eventName}`).
// `handle` is the kernel Subscription returned by axonaNode.sub; it
// exposes `.topicId` (66-char hex) for diagnostics and `.stop()` for
// teardown.  `messages` is ephemeral — a fresh inbox on every reload.

const SUBSCRIPTIONS_LS_KEY = 'axona-peer:subscriptions:v1';
const MAX_MESSAGES_PER_SUB = 50;

// Pub/Sub state persistence is intentionally OFF for now.  localStorage
// is shared across all tabs on the same origin, so a tab restoring
// subs / publish-forms from a sibling tab's state was creating
// "dead" UI cards (visible but never actually registered on this
// tab's AxonaNode).  Until we have proper per-tab persistence
// (sessionStorage, or per-tab namespaced keys), every fresh load
// starts with one empty publish card and no subscriptions.
const PERSIST_PUBSUB_STATE = false;

// One-shot cleanup: when persistence is OFF, wipe any keys left over
// from earlier 0.11.0–0.13.1 builds so they don't sit around stale
// in localStorage.  Cheap; happens once per page load.
if (!PERSIST_PUBSUB_STATE) {
  try {
    localStorage.removeItem(SUBSCRIPTIONS_LS_KEY);
    localStorage.removeItem('axona-peer:publishForms:v1');
  } catch {}
}

/** @type {Array<{regionId:string, eventName:string, key:bigint, messages:Array}>} */
let subscriptions = [];

function populateRegionSelect($sel, selectedRegionId) {
  $sel.replaceChildren();
  for (const r of REGIONS) {
    const opt = document.createElement('option');
    opt.value = r.id;
    opt.textContent = r.label;
    if (r.id === selectedRegionId) opt.selected = true;
    $sel.appendChild(opt);
  }
}

function populateSubRegionDropdown(homeRegionId) {
  populateRegionSelect($subRegion, homeRegionId);
}

function loadPersistedSubscriptions() {
  if (!PERSIST_PUBSUB_STATE) return [];
  try {
    const raw = localStorage.getItem(SUBSCRIPTIONS_LS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(s =>
      s && typeof s.regionId === 'string' && typeof s.eventName === 'string');
  } catch { return []; }
}

function persistSubscriptions() {
  if (!PERSIST_PUBSUB_STATE) return;
  // Store only the durable bits (regionId + eventName).  Keys are
  // derived; inboxes are session-scoped.
  const serial = subscriptions.map(s => ({
    regionId: s.regionId, eventName: s.eventName,
  }));
  try { localStorage.setItem(SUBSCRIPTIONS_LS_KEY, JSON.stringify(serial)); }
  catch (err) { appendLog('pubsub:persist-failed', err.message, 'error'); }
}

/** Render the entire subscription list (idempotent — wipes + rebuilds). */
function renderSubscriptions() {
  $subList.replaceChildren();
  for (let idx = 0; idx < subscriptions.length; idx++) {
    const sub = subscriptions[idx];
    const li = document.createElement('li');
    li.className = 'pubsub-sub';
    li.dataset.idx = String(idx);

    const region = REGIONS.find(r => r.id === sub.regionId);

    const head = document.createElement('div');
    head.className = 'pubsub-sub-head';
    const title = document.createElement('div');
    title.className = 'pubsub-sub-title';
    title.innerHTML =
      `<span class="region-tag">${region?.id ?? sub.regionId}</span>` +
      escapeHtml(sub.eventName);
    const key = document.createElement('span');
    key.className = 'pubsub-sub-key';
    key.textContent = sub.handle?.topicId ?? '—';
    const rm = document.createElement('button');
    rm.className = 'pubsub-sub-remove';
    rm.type = 'button';
    rm.title = 'remove subscription';
    rm.textContent = '×';
    rm.addEventListener('click', () => removeSubscription(idx));

    head.appendChild(title);
    head.appendChild(key);
    head.appendChild(rm);
    li.appendChild(head);

    if (sub.messages.length === 0) {
      const empty = document.createElement('p');
      empty.className = 'pubsub-sub-empty';
      empty.textContent = 'no messages yet — waiting for the network…';
      li.appendChild(empty);
    } else {
      const ul = document.createElement('ul');
      ul.className = 'pubsub-sub-msgs';
      // Newest first.
      for (let i = sub.messages.length - 1; i >= 0; i--) {
        const m = sub.messages[i];
        const mli = document.createElement('li');
        const meta = document.createElement('span');
        meta.className = 'msg-meta';
        const time = new Date(m.ts).toLocaleTimeString();
        meta.textContent = `${time} · from ${fmtNodeId(m.publisher)}`;
        const body = document.createElement('span');
        body.textContent = m.msg;
        mli.appendChild(meta);
        mli.appendChild(body);
        ul.appendChild(mli);
      }
      li.appendChild(ul);
    }
    $subList.appendChild(li);
  }
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

/**
 * Build a 66-char synthetic publisher ID for `regionId` so the
 * topic ID derives with the region's 8-bit S2 prefix.  All peers
 * pick the same synth ID for the same region (geoCellId is
 * deterministic from lat/lng), so publisher + subscriber produce
 * identical topicIds and land on the same K-closest axon set.
 *
 * Shape: `<2 hex chars: regional S2 prefix> + <64 zero hex chars>`.
 * Not a real peer's nodeId — it never signs anything; the envelope
 * is signed by the calling peer's actual identity.privateKey, and
 * `opts.publisher` here only controls deriveTopicId's prefix.
 */
function regionSynthPublisher(regionId) {
  const region = REGIONS.find(r => r.id === regionId);
  if (!region) return null;
  const s2 = geoCellId(region.lat, region.lng, 8);
  return s2.toString(16).padStart(2, '0') + '0'.repeat(64);
}

async function addSubscription(regionId, eventName) {
  if (!axonaNode) throw new Error('axona node not started yet');
  if (!REGIONS.find(r => r.id === regionId)) {
    throw new Error('unknown region: ' + regionId);
  }

  // De-dup: same regionId + same eventName is already there.
  if (subscriptions.some(s => s.regionId === regionId && s.eventName === eventName)) {
    return;
  }
  const topic = `${regionId}/${eventName}`;
  const sub = { regionId, eventName, topic, handle: null, messages: [] };
  subscriptions.push(sub);

  // v1.1: region-keyed topic.  We pass a synthetic publisher ID
  // whose top 8 bits = the region's S2 cell, so the derived
  // topic ID has the region's S2 prefix and us-east peers (S2
  // prefix matches) become the natural axon set.  Previously
  // {publisher: null} produced a `00`-prefix global-bucket topic,
  // which broke F1's geographic locality entirely.
  //
  // since: 'all' replays everything in the axon's pubsub cache for
  // this topic before live-tailing.  Matches the demo UX expectation
  // that "publish first, subscribe second" still delivers the past
  // message — without this the new subscriber starts at a live tail
  // and never sees publishes that happened before its sub envelope
  // reached the K-closest axon.
  const publisher = regionSynthPublisher(regionId);
  sub.handle = await axonaNode.sub(topic, (envelope) => {
    sub.messages.push({
      publisher: envelope.signerPubkey ?? null,
      msg:       envelope.message,
      ts:        envelope.ts,
      msgId:     envelope.msgId,
    });
    if (sub.messages.length > MAX_MESSAGES_PER_SUB) {
      sub.messages.shift();
    }
    renderSubscriptions();
  }, { publisher, since: 'all' });

  persistSubscriptions();
  renderSubscriptions();
  appendLog('pubsub:subscribed', `${regionId}/${eventName} → ${sub.handle.topicId}`);
}

async function removeSubscription(idx) {
  const sub = subscriptions[idx];
  if (!sub) return;
  if (sub.handle) { try { await sub.handle.stop(); } catch { /* ignore */ } }
  subscriptions.splice(idx, 1);
  persistSubscriptions();
  renderSubscriptions();
  appendLog('pubsub:unsubscribed', `${sub.regionId}/${sub.eventName}`);
}

// ── Publish forms (multi-card) ───────────────────────────────────────
//
// State: an array of { id, regionId, eventName }.  Each card is a
// separate `<div class="pubsub-publish">` rendered by renderPublishForms.
// The message body is per-card and ephemeral (held in the DOM input);
// regionId + eventName persist so users keep their topic configs
// across reloads.

const PUBLISH_FORMS_LS_KEY = 'axona-peer:publishForms:v1';

/** @type {Array<{id:string, regionId:string, eventName:string}>} */
let publishForms = [];

function loadPersistedPublishForms() {
  if (!PERSIST_PUBSUB_STATE) return [];
  try {
    const raw = localStorage.getItem(PUBLISH_FORMS_LS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(f => f && typeof f.regionId === 'string' && typeof f.eventName === 'string')
      .map(f => ({
        id: typeof f.id === 'string' ? f.id : crypto.randomUUID(),
        regionId: f.regionId, eventName: f.eventName,
      }));
  } catch { return []; }
}

function persistPublishForms() {
  if (!PERSIST_PUBSUB_STATE) return;
  try {
    localStorage.setItem(PUBLISH_FORMS_LS_KEY,
      JSON.stringify(publishForms));
  } catch (err) {
    appendLog('pubsub:persist-failed', err.message, 'error');
  }
}

function newPublishForm(homeRegionId, eventName = '') {
  return {
    id: crypto.randomUUID(),
    regionId: homeRegionId ?? REGIONS[0].id,
    eventName,
  };
}

async function recomputeKeyLabel(form, $keyEl) {
  if (!form.eventName) { $keyEl.textContent = '—'; return; }
  if (!REGIONS.find(r => r.id === form.regionId)) { $keyEl.textContent = '—'; return; }
  try {
    const { deriveTopicId } = await import(
      '../vendor/axona-protocol/src/pubsub/post.js'
    );
    const topicId = await deriveTopicId(
      regionSynthPublisher(form.regionId),
      `${form.regionId}/${form.eventName}`,
    );
    $keyEl.textContent = `topic id: ${topicId}`;
  } catch { $keyEl.textContent = '—'; }
}

async function publishFromForm(form, $messageEl) {
  console.log('[axona] publishFromForm fired', { form, value: $messageEl?.value });
  if (!axonaNode) { console.warn('[axona] publish aborted: axonaNode not ready'); return; }
  const eventName = form.eventName.trim();
  const message   = $messageEl.value;
  if (!eventName) {
    console.warn('[axona] publish skipped: topic required');
    appendLog('pubsub:publish-skip', 'topic required', 'error');
    return;
  }
  if (!message) {
    console.warn('[axona] publish skipped: message body empty');
    appendLog('pubsub:publish-skip', 'message required', 'error');
    return;
  }
  if (!REGIONS.find(r => r.id === form.regionId)) {
    console.warn('[axona] publish skipped: unknown region', form.regionId);
    appendLog('pubsub:publish-skip', 'unknown region', 'error');
    return;
  }
  const topic = `${form.regionId}/${eventName}`;
  try {
    // v1.1: region-keyed publish — `opts.publisher` is a synthetic
    // ID whose top 8 bits carry the region's S2 prefix, matching
    // what addSubscription does so the derived topic ID lands in
    // the region's address space (us-east peers ↔ us-east topics).
    // Signed by default (sign:true is the kernel default); the
    // envelope's signerPubkey is the real publisher's pubkey —
    // the synthetic ID only steers deriveTopicId's S2 prefix.
    const publisher = regionSynthPublisher(form.regionId);
    const msgId = await axonaNode.pub(topic, message, { publisher });
    console.log('[axona] published', { topic, msgId });
    appendLog('pubsub:published', `${form.regionId}/${eventName} → ${msgId}`);
    $messageEl.value = '';
  } catch (err) {
    console.error('[axona] publish failed', { topic, message, err });
    appendLog('pubsub:publish-failed', err.message ?? String(err), 'error');
  }
}

/**
 * Build a publish card.  The card holds its own input refs and a
 * focus-friendly render: editing the region or event name updates
 * `form` in place + persists, without re-rendering the whole list
 * (which would steal focus mid-typing).
 */
function buildPublishCard(form) {
  const card = document.createElement('div');
  card.className = 'pubsub-publish';
  card.dataset.id = form.id;

  // Remove (×) — hidden if this is the only card.
  const rm = document.createElement('button');
  rm.className = 'pubsub-sub-remove';
  rm.type = 'button';
  rm.title = 'remove publish card';
  rm.textContent = '×';
  rm.addEventListener('click', () => removePublishForm(form.id));
  card.appendChild(rm);

  // Row 1: prefix + region select
  const row1 = document.createElement('div');
  row1.className = 'pubsub-row';
  const lbl = document.createElement('label');
  lbl.className = 'me-label';
  lbl.textContent = 'prefix';
  const $region = document.createElement('select');
  $region.className = 'pubsub-region';
  populateRegionSelect($region, form.regionId);
  $region.addEventListener('change', () => {
    form.regionId = $region.value;
    persistPublishForms();
    recomputeKeyLabel(form, $key);
  });
  row1.appendChild(lbl);
  row1.appendChild($region);
  card.appendChild(row1);

  // Row 2: event name
  const row2 = document.createElement('div');
  row2.className = 'pubsub-row';
  const $event = document.createElement('input');
  $event.type = 'text';
  $event.className = 'pubsub-input';
  $event.placeholder = 'topic (e.g. News of the Day)';
  $event.spellcheck = false;
  $event.autocomplete = 'off';
  $event.value = form.eventName;
  $event.addEventListener('input', () => {
    form.eventName = $event.value;
    persistPublishForms();
    recomputeKeyLabel(form, $key);
  });
  row2.appendChild($event);
  card.appendChild(row2);

  // Row 3: message body + send
  const row3 = document.createElement('div');
  row3.className = 'pubsub-row';
  const $message = document.createElement('input');
  $message.type = 'text';
  $message.className = 'pubsub-input';
  $message.placeholder = 'message body — Enter or Send to publish';
  $message.spellcheck = false;
  $message.autocomplete = 'off';
  const $send = document.createElement('button');
  $send.className = 'pubsub-send';
  $send.type = 'button';
  $send.textContent = 'send';
  $send.disabled = !axonaNode;
  $send.addEventListener('click', () => publishFromForm(form, $message));
  $message.addEventListener('keydown', (ev) => {
    if (ev.key === 'Enter' && !ev.shiftKey) {
      ev.preventDefault();
      publishFromForm(form, $message);
    }
  });
  row3.appendChild($message);
  row3.appendChild($send);
  card.appendChild(row3);

  // Key preview
  const $key = document.createElement('div');
  $key.className = 'pubsub-key';
  $key.textContent = '—';
  card.appendChild($key);
  recomputeKeyLabel(form, $key);

  return card;
}

function renderPublishForms() {
  $pubList.replaceChildren();
  for (const form of publishForms) {
    $pubList.appendChild(buildPublishCard(form));
  }
  // Hide the × button if there's only one card (avoid the "removed
  // everything, can't publish" trap).
  const removeBtns = $pubList.querySelectorAll('.pubsub-sub-remove');
  if (publishForms.length === 1) {
    removeBtns[0].style.display = 'none';
  }
}

function addPublishForm() {
  const home = axonaNode?._identity?.region?.id ?? REGIONS[0].id;
  publishForms.push(newPublishForm(home, ''));
  persistPublishForms();
  renderPublishForms();
}

function removePublishForm(id) {
  const idx = publishForms.findIndex(f => f.id === id);
  if (idx < 0) return;
  publishForms.splice(idx, 1);
  if (publishForms.length === 0) {
    // Keep at least one form on screen.
    const home = axonaNode?._identity?.region?.id ?? REGIONS[0].id;
    publishForms.push(newPublishForm(home, ''));
  }
  persistPublishForms();
  renderPublishForms();
}

$pubAdd.addEventListener('click', addPublishForm);

// Subscription form: + → reveal → fill → subscribe / cancel
$subAdd.addEventListener('click', () => {
  $subEvent.value = '';
  $subForm.hidden = false;
  $subEvent.focus();
});
$subCancel.addEventListener('click', () => { $subForm.hidden = true; });
$subSave.addEventListener('click', async () => {
  const regionId  = $subRegion.value;
  const eventName = $subEvent.value.trim();
  if (!eventName) return;
  try {
    await addSubscription(regionId, eventName);
    $subForm.hidden = true;
  } catch (err) {
    appendLog('pubsub:sub-failed', err.message, 'error');
  }
});
$subEvent.addEventListener('keydown', (ev) => {
  if (ev.key === 'Enter') { ev.preventDefault(); $subSave.click(); }
  if (ev.key === 'Escape'){ ev.preventDefault(); $subCancel.click(); }
});

// ── Lookup UI ────────────────────────────────────────────────────────
$lookupGo.addEventListener('click', async () => {
  if (!axonaNode) return;
  const raw = $lookupTarget.value.trim().replace(/^0x/i, '');
  // v1.1: 264-bit node IDs are 66 hex chars; allow any prefix from
  // 1 char (handy for "find anything close to this S2 cell") up to
  // a full 66-char ID.
  if (!/^[0-9a-fA-F]{1,66}$/.test(raw)) {
    $lookupResult.textContent = 'target must be 1-66 hex chars';
    return;
  }
  const target = BigInt('0x' + raw);
  $lookupResult.textContent = `looking up ${target.toString(16)}…`;
  $lookupGo.disabled = true;
  try {
    const result = await axonaNode.lookup(target);
    $lookupResult.textContent = JSON.stringify({
      found:  result?.found,
      hops:   result?.hops,
      time:   result?.time,
      path:   (result?.path ?? []).map(id => id.toString(16).padStart(66, '0')),
    }, null, 2);
  } catch (err) {
    $lookupResult.textContent = 'error: ' + err.message;
  }
  $lookupGo.disabled = false;
});

// ── Go ───────────────────────────────────────────────────────────────
appendLog('boot', `bridge=${bridgeUrl}`);
render();
connectBridge();

// Boot the Axona layer: existing identity → use it; else show picker.
(() => {
  const existing = getCurrentIdentity();
  if (existing) {
    bootAxonaNode().catch(err =>
      appendLog('axona:boot-failed', { err: err.message }, 'error'));
  } else {
    showRegionPicker();
  }
})();
