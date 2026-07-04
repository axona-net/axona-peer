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

import { renderQR }    from './qr.js';
// Orchestration now rides directly on the kernel.  webTransport() owns
// the bridge WebSocket, reconnect/backoff, ping/pong, stale detection,
// and the version-gate + hello/hello-ack handshake; AxonaPeer +
// NeuronNode + AxonaDomain own routing and pub/sub.  This replaces the
// hand-rolled mesh / node / wire codec the prior build wired together
// in this file.
import { AxonaPeer, AxonaDomain, NeuronNode, ANONYMOUS } from '../vendor/axona-protocol/src/index.js';
import { webTransport } from '../vendor/axona-protocol/src/transport/web/index.js';
import { makeMessage, readMessage } from '../vendor/axona-protocol/std/message.js';
import {
  REGIONS,
  getCurrentIdentity,
  deriveIdentity,
  getAuthorIdentity,
} from './identity.js';
// v0.3: topics are STRUCTURED descriptors, not strings.  An open topic
// is `{ region, name }`, where `region` is a kernel region NAME (e.g.
// 'useast') or numeric S2 code.  We map the app's REGIONS entries
// (ids like 'us-east', lat/lng) to kernel region names via
// `regionNameForLatLng(lat, lng)` so the structured topic's region
// carries the same S2 cell the old synthetic-publisher scheme used —
// us-east peers stay XOR-closest to us-east topics.
import { regionNameForLatLng } from '../vendor/axona-protocol/src/index.js';
// Bridge directory: discover alternate bridges + fail over to a saved one
// when the configured primary is unreachable (see bridgeBook.js).
import { BRIDGE_DIRECTORY_TOPIC } from '../vendor/axona-protocol/src/bridgeDirectory.js';
import * as bridgeBook from './bridgeBook.js';

// v0.3: open topics are structured descriptors and REQUIRE a region —
// there's no global region byte anymore.  The bridge-directory topic is
// a single well-known, globally-discoverable topic, so every client must
// agree on ONE fixed region for it.  We pin it to 'useast' (0x89).
//
// Cross-repo invariant: axona-bridge/src/bridge_directory.js publishes to
// the IDENTICAL descriptor `{ region: 'useast', name: BRIDGE_DIRECTORY_TOPIC }`
// (both repos migrated together in the v3.0.0 flag-day), so the published
// and subscribed topic ids match.  If either side's region or name changes,
// directory discovery silently stops interoperating — keep them in lockstep.
const BRIDGE_DIRECTORY_DESC = Object.freeze({
  region: 'useast',
  name:   BRIDGE_DIRECTORY_TOPIC,
});
// v0.3: pub/sub uses the kernel's structured-topic API via peer.pub /
// peer.sub.  An open topic is the descriptor `{ region, name }`
// (anyone-can-publish, anyone-can-subscribe); `region` is a kernel
// region name carrying the S2 cell.  The (region, event) addressing
// UX is preserved: the app's `eventName` becomes the topic `name` and
// the app's `regionId` maps to a kernel region name (appRegionName()).
// Publishes are signed with the durable author key (signWith:).

// Peer build number.  Bump the middle digit on every code change so
// you can confirm a redeployed page actually loaded (esp. on iOS,
// where the bfcache can serve a stale module set for ages).  The
// bridge version arrives separately in its `welcome` message; the
// "version" row in the me panel shows both side by side.
const PEER_VERSION = '3.46.3';

// webTransport() now owns the bridge WebSocket lifecycle (ping cadence,
// stale window, reconnect backoff, uptime), so those constants moved
// into the kernel.  The app keeps only what the UI itself uses.
const LOG_MAX_LINES           = 120;
const BRIDGE_ROW_ID           = '@bridge';   // synthetic id for the bridge row
const UPTIME_TICK_MS          = 1000;        // re-render cadence for the uptime column

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
  // The SF testnet droplet serves this app AND fronts its bridge at the same
  // origin (testnet.axona.net), so default to the same-origin bridge there.
  // This keeps release/2.28.0 dual-purpose: served from testnet.axona.net it
  // targets the testnet; served from axona.net (the prod flag-day) it targets
  // the production introducer. Override either with ?bridge=ws://localhost:8080
  // for local dev.
  if (location.hostname === 'testnet.axona.net') return 'wss://testnet.axona.net';
  return 'wss://bridge.axona.net';
}
// The configured primary (root) — `?bridge=`, testnet, or production.
// Failover only ever ADDS alternates from the saved directory; the primary
// is never auto-replaced. The testnet host runs an independent fleet, so it
// skips the directory mechanism entirely.
const PRIMARY_BRIDGE = getBridgeUrl();
const IS_TESTNET = location.hostname === 'testnet.axona.net' || location.hostname.includes('testnet');
let bridgeUrl = PRIMARY_BRIDGE;          // updated by selectBridge() at boot
$bridgeUrl.textContent = bridgeUrl;

// Lightweight reachability probe: can we open a WebSocket to `url` at all?
// (We don't run the auth handshake here — "the socket opens" is a good
// proxy for "this bridge is up," enough to decide whether to fail over.)
function probeBridge(url, timeoutMs = 3500) {
  return new Promise((resolve) => {
    let done = false, ws;
    const finish = (ok) => { if (done) return; done = true; try { ws && ws.close(); } catch {} resolve(ok); };
    try { ws = new WebSocket(url); } catch { resolve(false); return; }
    const t = setTimeout(() => finish(false), timeoutMs);
    ws.onopen  = () => { clearTimeout(t); finish(true); };
    ws.onerror = () => { clearTimeout(t); finish(false); };
    ws.onclose = () => { clearTimeout(t); finish(false); };
  });
}

// Pick a reachable bridge: try the primary first, then saved alternates in
// ranked order. Returns the primary unchanged on the testnet host or when
// nothing probes reachable (let the transport's own backoff keep retrying).
async function selectBridge(self) {
  if (IS_TESTNET) return PRIMARY_BRIDGE;
  const cands = bridgeBook.candidates({ roots: [PRIMARY_BRIDGE], self });
  for (const url of cands) {
    if (await probeBridge(url)) {
      if (url !== PRIMARY_BRIDGE) appendLog('bridge:failover', `primary unreachable → ${url}`, 'warn');
      return url;
    }
    bridgeBook.recordOutcome(url, { ok: false });
    appendLog('bridge:unreachable', url, 'warn');
  }
  return PRIMARY_BRIDGE;
}

// One-shot directory refresh: after the mesh is ready, subscribe to the
// public bridge-directory topic, collect current entries for a few seconds,
// merge them into the local book, then unsubscribe. We do NOT renew — the
// saved list is consulted on the NEXT launch.
async function refreshBridgeDirectory(connectedUrl, timeToMeshMs) {
  if (IS_TESTNET) return;
  bridgeBook.recordOutcome(connectedUrl, { ok: true, timeToMeshMs });
  const collected = [];
  let sub = null;
  try {
    sub = await peer.sub(BRIDGE_DIRECTORY_DESC, (env) => {
      if (!env || env.deleted || !env.signerPubkey) return;
      collected.push({ entry: env.message, signerKey: env.signerPubkey });
    }, { since: 'all' });
    await new Promise((r) => setTimeout(r, 5000));
  } catch (err) {
    appendLog('directory:refresh-failed', err.message ?? String(err), 'warn');
  } finally {
    try { await sub?.stop?.(); } catch {}
  }
  if (collected.length) {
    const entries = bridgeBook.mergeDirectory(collected);
    appendLog('directory:saved', `${Object.keys(entries).length} bridge(s) known`, 'ok');
  }
}

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

// ── Bridge welcome snapshot (for the version row) ────────────────────
//
// webTransport surfaces the bridge's `welcome` frame via
// transport.onWelcome / transport.bridgeInfo.  We cache the last one
// here so renderVersion() can read the bridge's package + kernel
// version without reaching into the transport on every paint.
let bridgeWelcome = null;   // { connId, version, kernelVersion, turn } | null

// Bridge-row traffic counters for the peer table.  webTransport owns
// the actual ping/pong loop and RTT; it doesn't tally cumulative
// counts, so we increment these off transport.onPingTraffic (filtered
// to the bridge nodeId) and reset connectedAt whenever the bridge
// transitions back to 'open'.  Mirrors the old bridge.pings/pongs/
// connectedAt the peer table used to read.
const bridgeTraffic = { pings: 0, pongs: 0, connectedAt: 0 };

// Pull the kernel version this peer is running.  Imported via the
// vendored kernel path so it's exactly the build we shipped, never
// stale relative to PEER_VERSION.  Kernel-version mismatch between
// peer and bridge is the kind of bug surfaced here by the matching
// "kernel vX" segments in the version row.
import { KERNEL_VERSION as PEER_KERNEL_VERSION } from '../vendor/axona-protocol/src/transport/handshake.js';

// "peer v1.1.5 · kernel v1.5.0 · bridge v1.1.3 (kernel v1.5.0)" once
// welcome has arrived.  The bridge halves stay "—" until then — that
// gap is itself useful UX since it signals "WS still negotiating."
function renderVersion() {
  const bridgeStr       = bridgeWelcome?.version
    ? `v${bridgeWelcome.version}` : '—';
  const bridgeKernelStr = bridgeWelcome?.kernelVersion
    ? `kernel v${bridgeWelcome.kernelVersion}` : 'kernel —';
  $version.textContent =
    `peer v${PEER_VERSION} · kernel v${PEER_KERNEL_VERSION} · ` +
    `bridge ${bridgeStr} (${bridgeKernelStr})`;
}
renderVersion();

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
// mesh + bridge ping-traffic → indicator pulse is wired in
// bootAxonaNode() once `transport` exists: mesh rows pulse off
// transport.mesh.onPingTraffic (string meshId), the bridge row off
// transport.onPingTraffic filtered to the bridge's nodeId.

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

/** Tracks the last meshDegraded value so we log the transition once,
 *  not on every render tick. */
let _meshDegradedShown = false;

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

// Map webTransport's bridgeState vocabulary onto the INDICATOR_CLASS
// states the peer table renders.  'open'/'stale'/'connecting'/
// 'disconnected' pass through; 'upgrade-required' shows red ('failed').
const BRIDGE_STATE_MAP = {
  'open':             'open',
  'stale':            'stale',
  'connecting':       'connecting',
  'disconnected':     'disconnected',
  'upgrade-required': 'failed',
};

function render() {
  // No transport yet (region picker still up) — render an empty table
  // with just the placeholder bridge row so the UI isn't blank.
  const peers = transport
    ? transport.mesh.getPeers().sort((a, b) =>
        a.peerId < b.peerId ? -1 : a.peerId > b.peerId ? 1 : 0)
    : [];

  const bridgeState = transport
    ? (BRIDGE_STATE_MAP[transport.bridgeState] ?? 'disconnected')
    : 'disconnected';

  const ordered = [
    {
      peerId:     BRIDGE_ROW_ID,
      label:      'bridge',
      kind:       'bridge',
      state:      bridgeState,
      rttLast:    transport ? transport.bridgeRtt : null,
      rttAvg:     transport ? transport.bridgeRttAvg : null,
      pings:      bridgeTraffic.pings,
      pongs:      bridgeTraffic.pongs,
      openedAt:   bridgeTraffic.connectedAt,
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

  // Header counts.  Open data channels of known — PLUS the count that
  // actually matters for routing: how many of those channels have
  // completed the axona/5 handshake and bound into the synaptome.  The
  // demo bug had every dot green while NOTHING was bound; surfacing the
  // bound count here means the Peer UI can no longer hide that.
  const openPeers = peers.filter(p => p.state === 'open').length;
  let boundLabel = '';
  let degraded   = false;
  try {
    if (peer && typeof peer.health === 'function') {
      const h  = peer.health();
      const mb = h.transport?.meshBound;
      if (typeof mb === 'number') boundLabel = ` · ${mb} bound`;
      degraded = h.meshDegraded === true;
    }
  } catch { /* best-effort — never let diagnostics break render */ }
  $meshCount.textContent = `${openPeers} of ${peers.length}${boundLabel}`;
  $meshCount.classList.toggle('mesh-degraded', degraded);
  $meshCount.title = degraded
    ? 'Mesh degraded: data channels are open but the axona/5 handshake '
      + 'has not bound them — no authenticated routing is flowing.'
    : '';
  if (degraded !== _meshDegradedShown) {
    _meshDegradedShown = degraded;
    if (degraded) appendLog('mesh:degraded', `${openPeers} open, only ${boundLabel.trim() || '0 bound'} — channels open but unbound`, 'warn');
    else          appendLog('mesh:recovered', 'all open channels authenticated', 'info');
  }
  $myId.textContent = bridgeWelcome?.connId ?? '—';
}

// ── Bridge connection lifecycle ──────────────────────────────────────
//
// The WebSocket lifecycle (open, version-gate client-hello, bridge
// hello/hello-ack, ping/pong, stale detection, reconnect/backoff) now
// lives entirely inside webTransport().  The app subscribes to
// transport.onBridgeState / onWelcome / onPingTraffic in bootAxonaNode()
// and drives the version row, upgrade banner, and peer-table bridge row
// from those callbacks.  Only the upgrade-banner renderer remains here.

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

// ── Uptime ticker ────────────────────────────────────────────────────
// webTransport drives ping/pong + RTT internally, but the peer table's
// uptime column counts wall-clock seconds, so we re-render on a 1 Hz
// tick regardless of traffic.  Armed once at module load.
setInterval(() => render(), UPTIME_TICK_MS);

// ── Page-resume reset ────────────────────────────────────────────────
//
// When a phone backgrounds the tab (lock screen, app switcher) or the
// laptop sleeps, mobile browsers heavily throttle our timers and the
// OS often kills the WebSocket.  By the time we come back the remote
// side has long since seen us peer-leave; the bridge socket is a
// zombie that won't recover on its own and the indicators sit orange
// indefinitely.
//
// webTransport().reconnectNow() is the fix: it closes the live socket
// (or re-opens a dead one) with the backoff reset to zero, re-running
// the version-gate + hello/hello-ack handshake.  The mesh re-fills
// from the fresh peer-list the bridge sends on reconnect.  Cheap and
// correct — there's no per-peer state worth salvaging once the
// underlying transport has died.

const RESUME_HIDDEN_THRESHOLD_MS = 5000;   // ignore brief tab switches
let hiddenAt = 0;

// Coalesce reset storms.  A genuine resume fires several events almost at
// once (visibilitychange→visible + online + pageshow), and a *flapping*
// network (spotty mobile, Wi-Fi roaming) fires `online` repeatedly within
// seconds.  Each resetMesh() blows away the whole synaptome and forces a
// full bridge reconnect + mesh rebuild, so firing it back-to-back amplifies
// disruption on exactly the links that can least afford it.  Collapse any
// resets within this window into the first one; the first reset already
// re-runs the handshake and refills the mesh, so the rest are redundant.
const RESET_MIN_INTERVAL_MS = 5000;
let lastResetAt = 0;

function resetMesh(reason) {
  const sinceLast = Date.now() - lastResetAt;
  if (lastResetAt && sinceLast < RESET_MIN_INTERVAL_MS) {
    appendLog('resume', `${reason} — coalesced (mesh reset ` +
      `${(sinceLast / 1000).toFixed(1)}s ago; skipping redundant teardown)`, 'ok');
    return;
  }
  lastResetAt = Date.now();
  appendLog('resume', `${reason} — re-establishing mesh + bridge; any ` +
    `RTCDataChannel/WebSocket errors in the browser console just now are ` +
    `expected (zombie channels after sleep) and self-heal`, 'ok');
  // Tear down every WebRTC peer outright: after a genuine resume (lid
  // open, long tab-background) the channels are almost always dead, and
  // mesh.reset() fires onPeerLost for each so the synaptome clears too.
  // This restores the teardown the pre-v3.3.0 resume path did (the
  // v3.3.0 rewrite dropped it).  The kernel's per-peer pong-timeout /
  // send-failure eviction (mesh v2.1.1) is the backstop for the cases
  // where these resume events don't fire at all — notably a macOS
  // screensaver, where the tab never goes hidden and the network
  // interface may not toggle.
  try { transport?.mesh?.reset?.(); } catch { /* mesh may not exist yet */ }
  transport?.reconnectNow();   // reconnect the bridge; fresh peer-list rebuilds the mesh
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
  transport?.stop();
});

// ── Axona protocol layer (Phase 3) ──────────────────────────────────
//
// On boot: if a persisted identity exists, instantiate the kernel peer
// immediately.  Otherwise show the region picker and wait for the user
// to choose; on save, derive an identity and instantiate.
//
// The kernel now owns the whole connection: webTransport() builds the
// bridge WebSocket + WebRTC mesh and drives the version-gate +
// hello/hello-ack handshake, NeuronNode holds the synaptome,
// AxonaDomain is the routing domain, and AxonaPeer is the pub/sub +
// lookup surface this UI talks to.
let peer = null;        // AxonaPeer
let transport = null;   // CompositeTransport from webTransport()
let node = null;        // NeuronNode
let authorIdentity = null;  // durable AUTHORSHIP key (signWith: for every publish)

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

// Mesh-ready gate: wait until the mesh is wide enough to subscribe. Delegates to
// the kernel's peer.ready() (v4.8.2) — which resolves on synaptome>=minPeers OR a
// stable (no-longer-growing) synaptome OR timeout. The stable case is the win
// over the old hand-rolled poll: a solo/small-mesh tab resolves in ~1.5s instead
// of burning the full timeout waiting for a count it can never reach.
const READY_SYNAPSE_COUNT = 4;
const READY_TIMEOUT_MS    = 10_000;
async function waitForMeshReady() {
  const r = await peer.ready({ minPeers: READY_SYNAPSE_COUNT, timeoutMs: READY_TIMEOUT_MS });
  appendLog(r.ready ? 'mesh:ready' : 'mesh:ready-timeout',
    `synaptome=${r.peers} (${r.reason}, ${r.ms}ms)${r.ready ? '' : ' — proceeding anyway'}`);
  return r.peers;
}

async function bootAxonaNode(opts = {}) {
  const bootStartedAt = Date.now();
  const identity = await deriveIdentity(opts);
  // Key separation (design v0.3): publishes are signed by the durable
  // AUTHORSHIP key, never the transport/node `identity`.  The author key
  // is a keypair-only Author ID (no location, no nodeId) and persists to
  // localStorage so a returning visitor keeps a recognizable identity and
  // can retract (kill) their own messages.  It is passed per-publish
  // as `{ signWith: authorIdentity }` — there is no default signer on the
  // AxonaPeer anymore.
  authorIdentity = await getAuthorIdentity();
  $axonaId.textContent     = fmtNodeId(identity.id);
  $axonaRegion.textContent = identity.region?.label
    ?? identity.region?.id
    ?? identity.region?.source
    ?? '—';

  // Choose a reachable bridge: the configured primary first, then any saved
  // alternate from the directory if the primary won't open. Updates the
  // module `bridgeUrl` the transport build below reads.
  bridgeUrl = await selectBridge({ lat: identity.region?.lat, lng: identity.region?.lng });
  $bridgeUrl.textContent = bridgeUrl;

  // ── Build the kernel stack ───────────────────────────────────────
  //
  // webTransport() validates `identity.id` as a 66-char hex string, but
  // this repo's deriveIdentity returns `id` as a 264-bit BigInt with
  // the hex form on `idHex`.  Hand the transport an identity whose
  // `id` is the hex form (plus the kernel key fields so the bridge
  // handshake's hello/hello-ack carry the right nodeId).  NeuronNode,
  // by contrast, wants the BigInt id, and AxonaPeer wants the full
  // kernel identity (privateKey + pubkeyHex) so peer.pub can sign.
  transport = webTransport({
    bridgeUrl,
    identity: { ...identity, id: identity.idHex },
    peerVersion: PEER_VERSION,
    log: (e, d) => appendLog('tx:' + e, d),
    reconnect: true,
  });

  node = new NeuronNode({
    id:  identity.id,                 // BigInt (264-bit) — kernel form
    lat: identity.region?.lat ?? 0,
    lng: identity.region?.lng ?? 0,
  });
  node.transport = transport;

  const domain = new AxonaDomain({ k: 20 });
  // synaptomeMaintain REVERTED to off (2026-06-29) — enabling near-quota maintenance
  // regressed Howard's suite; re-enable only behind a fix + Howard gate.
  peer = new AxonaPeer({ domain, node, nodeIdentity: identity, transport });   // publishes are signed per-call via { signWith: authorIdentity }

  // ── Wire UI events BEFORE start so we don't miss the first welcome
  //    / state transition that lands during the handshake. ──────────
  const hex = (id) => typeof id === 'bigint'
    ? id.toString(16).padStart(66, '0')
    : String(id);

  transport.onBridgeState((state, detail) => {
    appendLog('bridge:state', `${state}${detail ? ' ' + detail : ''}`);
    if (state === 'open') bridgeTraffic.connectedAt = Date.now();
    if (state === 'connecting' || state === 'disconnected') {
      bridgeTraffic.connectedAt = 0;
    }
    if (state === 'upgrade-required') {
      showUpgradeBanner(transport.upgradeReason || 'Your client is out of date.');
    }
    renderVersion();
    render();
  });

  transport.onWelcome((info) => {
    bridgeWelcome = info;
    appendLog('bridge:welcome',
      `id=${info.connId ?? '?'} v${info.version ?? '?'}${info.turn ? ' turn=on' : ''}`, 'ok');
    renderVersion();
    render();
  });

  // Bridge ping/pong → pulse the bridge row + tally counts.  Mesh-peer
  // ping traffic comes from the mesh layer's own onPingTraffic (string
  // meshIds, wired below); transport.onPingTraffic reports BigInt
  // nodeIds, of which only the bridge has a clean match here.
  transport.onPingTraffic((peerIdBig, kind) => {
    if (transport.bridgeNodeIdBig != null && peerIdBig === transport.bridgeNodeIdBig) {
      if (kind === 'sent') bridgeTraffic.pings++;
      else                 bridgeTraffic.pongs++;
      pulseIndicator(BRIDGE_ROW_ID);
    }
  });

  // WebRTC mesh rows pulse on their own ping/pong frames (string meshId).
  transport.mesh.onPingTraffic((meshId, _kind) => pulseIndicator(meshId));

  transport.onPeerBound(() => render());
  transport.onPeerDied(() => render());
  transport.mesh.onChange(() => render());

  // ── Start the connection ─────────────────────────────────────────
  // transport.start() drives the version-gate + bridge hello/hello-ack
  // and resolves once the bridge handshake completes (or rejects on
  // timeout / upgrade-required).  On failure we still leave the UI
  // usable — the transport's own reconnect loop keeps retrying in the
  // background, and onBridgeState will surface the upgrade banner if
  // it's a version-gate rejection.
  try {
    await transport.start();
  } catch (err) {
    appendLog('bridge:start-failed', err.message ?? String(err), 'error');
  }
  await peer.start();

  // Self-integration (kernel v4.7.0): proactively weave ourselves into our
  // keyspace neighbourhood — findKClosest(ownId) + open authenticated channels
  // so neighbours adopt us and we become reachable, instead of waiting on
  // ambient annealing. Non-blocking: bootstrap (and the upgrade banner) must
  // not wait on it; the iterative lookup + ~K channel opens run in background.
  if (typeof peer.integrate === 'function') {
    peer.integrate().catch((err) => appendLog('peer:integrate-failed', err?.message ?? String(err), 'warn'));
  }

  // Debug surface for DevTools.  Exposed on window so operators can
  // introspect live state without rebuilding — `window.axona.synaptome()`,
  // `window.axona.subs()`, etc.  All read-only — no setters that could
  // accidentally tamper with running state.
  window.axona = {
    /** Raw peer reference — for poking around when needed. */
    node:        peer,
    /** Identity object (nodeId + region + geoBits). */
    identity,
    /** Snapshot of every Synapse currently in our routing table. */
    synaptome:   () => peer.getSynaptome().map(s => ({
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
                   state:   transport.bridgeState,
                   connId:  transport.bridgeInfo?.connId,
                   version: transport.bridgeInfo?.version,
                   wsReady: transport.socket?.readyState,
                 }),
    /** Mesh peers with their connection states (the ones reachable via WebRTC). */
    mesh:        () => transport.mesh.getPeers(),
    /** Which peer nodeIds is the underlying transport bound to right now?
     *  This is the set of peers we can reach via sendDirect.  Useful when
     *  an axon supposedly has a subscriber in role.children but
     *  transport.notify silently drops because the binding doesn't exist. */
    boundPeers:  () => transport.boundPeers().map(hex),
    /** Resolve an (appRegionId, eventName) to its 264-bit hex topic ID
     *  (v0.3 structured open topic: <region S2 prefix> + sha256(descriptor)). */
    topicKey: async (regionId, eventName) => {
      const { deriveTopicId } = await import(
        '../vendor/axona-protocol/src/pubsub/post.js'
      );
      // v0.3: structured open topic descriptor — same one
      // addSubscription / publishFromForm use, so DevTools shows the
      // topic ID that's actually routed.
      return deriveTopicId(openTopicDesc(regionId, eventName));
    },
    /** Run findKClosest for a topic.  Reveals WHICH K peers this node
     *  thinks are the topic's axon set — the most important diagnostic
     *  when subscribes and publishes seem to land on different roots. */
    findKClosest: async (topicHex, K = 5) => {
      const t = typeof topicHex === 'string' ? BigInt('0x' + topicHex.replace(/^0x/, '')) : topicHex;
      const ids = await peer.findKClosest(t, K);
      return ids.map(hex);
    },
    /** Topics we're an axon role-holder for, with our children (subscribers).
     *  If we expected to be an axon and this is empty, our subscribe didn't
     *  propagate.  If a subscriber we expected to deliver to ISN'T listed
     *  here, they never subscribed at us. */
    axonRoles:   () => {
      const am = peer._axonaManager;
      if (!am?.axonRoles) return [];
      return [...am.axonRoles.entries()].map(([topicId, role]) => ({
        topic:    hex(topicId),
        isRoot:   role.isRoot,
        children: [...role.children.keys()].map(hex),
        peerRoots: role.peerRoots ? [...role.peerRoots].map(hex) : [],
      }));
    },
    /** Topics we're subscribed to (from AxonaManager's POV, not just the
     *  UI's `subs()`).  Should match — if it doesn't, we have a sync bug. */
    mySubscriptions: () => {
      const am = peer._axonaManager;
      if (!am?.mySubscriptions) return [];
      return [...am.mySubscriptions.keys()].map(hex);
    },
  };

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

  // Wait for the mesh to stabilise before issuing any subscribes.
  // Mirrors the kernel demo's pattern: subscribing while
  // synaptome.size === 1 (just the bridge) anchors the subscription
  // at a tiny K-closest set; when the mesh grows, publishes computed
  // against the larger K-closest can miss the stale subscription —
  // including the publisher's own self-subscription.  The kernel
  // exposes findKClosest at the AxonaManager layer with a one-shot
  // cache that's never invalidated, so the FIRST sub call locks the
  // axon set forever.  Waiting until we know multiple peers makes
  // that first cache populate with a wide, stable set.
  await waitForMeshReady();

  // One-shot: record this bootstrap's outcome and learn/save alternate
  // bridges for next-launch failover. Fire-and-forget — never blocks subs.
  refreshBridgeDirectory(bridgeUrl, Date.now() - bootStartedAt).catch(() => {});

  const persisted = loadPersistedSubscriptions();
  for (const s of persisted) {
    try { await addSubscription(s.regionId, s.eventName); }
    catch (err) { appendLog('pubsub:restore-failed', err.message, 'error'); }
  }

  // Refresh synaptome counter on any mesh change (cheap proxy).
  const updateSynaptomeBadge = () => {
    if (!peer) return;
    $synaptomeSize.textContent = String(peer.getSynaptome().length);
  };
  transport.mesh.onChange(updateSynaptomeBadge);
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
// `handle` is the kernel Subscription returned by peer.sub; it
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
        body.textContent = readMessage(m.msg);   // canonical std/message convention
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
 * Map an app `regionId` (e.g. 'us-east' from REGIONS) to a KERNEL region
 * NAME (e.g. 'useast') for a structured topic descriptor.  The kernel
 * region name carries the same 8-bit S2 cell the old synthetic-publisher
 * scheme used: it's `regionNameForLatLng(lat, lng)` of the app region's
 * canonical coordinates, so publisher and subscriber compute the same
 * topic id and land on the same K-closest axon set, and us-east peers
 * stay XOR-closest to us-east topics (replaces regionSynthPublisher).
 *
 * Returns null for an unknown regionId.
 */
function appRegionName(regionId) {
  const region = REGIONS.find(r => r.id === regionId);
  if (!region) return null;
  return regionNameForLatLng(region.lat, region.lng);
}

/** Structured OPEN-topic descriptor for an (appRegionId, eventName) pair. */
function openTopicDesc(regionId, eventName) {
  return { region: appRegionName(regionId), name: eventName };
}

async function addSubscription(regionId, eventName) {
  if (!peer) throw new Error('axona peer not started yet');
  if (!REGIONS.find(r => r.id === regionId)) {
    throw new Error('unknown region: ' + regionId);
  }

  // De-dup: same regionId + same eventName is already there.
  if (subscriptions.some(s => s.regionId === regionId && s.eventName === eventName)) {
    return;
  }
  const topic = openTopicDesc(regionId, eventName);
  const sub = { regionId, eventName, topic, handle: null, messages: [] };
  subscriptions.push(sub);

  // v0.3: structured open topic { region, name }.  `region` is a kernel
  // region name carrying the chosen region's S2 cell, so the derived
  // topic id has that region's prefix and same-region peers become the
  // natural axon set — same geographic locality the old synthetic-
  // publisher scheme gave, now expressed in the descriptor.
  //
  // since: 'all' replays everything in the axon's pubsub cache for
  // this topic before live-tailing.  Matches the demo UX expectation
  // that "publish first, subscribe second" still delivers the past
  // message — without this the new subscriber starts at a live tail
  // and never sees publishes that happened before its sub envelope
  // reached the K-closest axon.
  sub.handle = await peer.sub(topic, (envelope) => {
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
  }, { since: 'all' });

  // Arm the kernel's AxonaManager periodic refresh timer.  This is
  // exactly what the kernel's minimal-pubsub-browser demo does after
  // peer.sub (see examples/minimal-pubsub-browser/index.html ~line
  // 546).  By default AxonaPeer._buildDefaultAxonaManager() does NOT
  // call am.start() — the assumption is that applications subscribe
  // after the mesh has stabilised, so the initial K-closest set is
  // already wide.  That's only true in static simulator runs, NOT in
  // a browser mesh where WebRTC peers come and go on every page
  // reload.  Without this arming, a subscription anchored at the
  // initial K-closest set (just self + bridge at sub time) becomes
  // stale as the mesh grows, and self-published messages routed
  // through the same stale set silently drop on the publisher's own
  // handler — the symptom users see as "publish to a topic I'm
  // subscribed to, message reaches everyone else but not me."
  // start() is idempotent; the timer won't double-arm.
  peer._axonaManager?.start?.();

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
    // v0.3: deriveTopicId takes a structured descriptor.
    const topicId = await deriveTopicId(openTopicDesc(form.regionId, form.eventName));
    $keyEl.textContent = `topic id: ${topicId}`;
  } catch { $keyEl.textContent = '—'; }
}

async function publishFromForm(form, $messageEl) {
  console.log('[axona] publishFromForm fired', { form, value: $messageEl?.value });
  if (!peer) { console.warn('[axona] publish aborted: peer not ready'); return; }
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
  const topic = openTopicDesc(form.regionId, eventName);
  try {
    // v0.3: structured open topic { region, name } + a REQUIRED signer.
    // `region` carries the chosen region's S2 cell so the derived topic
    // id lands in that region's address space (us-east peers ↔ us-east
    // topics), matching addSubscription.  The envelope's signerPubkey is
    // the durable author key's Author ID, passed as { signWith }.
    const msgId = await peer.pub(topic, makeMessage(message), { signWith: authorIdentity });
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
  $send.disabled = !peer;
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
  const home = peer?._identity?.region?.id ?? REGIONS[0].id;
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
    const home = peer?._identity?.region?.id ?? REGIONS[0].id;
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
  if (!peer) return;
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
    const result = await peer.lookup(target);
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
//
// The bridge WebSocket is no longer connected here — webTransport()
// (built inside bootAxonaNode) owns the socket and its handshake.  Boot
// the Axona layer: existing identity → use it; else show the region
// picker and connect once the user chooses.
appendLog('boot', `bridge=${bridgeUrl}`);
render();

(() => {
  const existing = getCurrentIdentity();
  if (existing) {
    bootAxonaNode().catch(err =>
      appendLog('axona:boot-failed', { err: err.message }, 'error'));
  } else {
    showRegionPicker();
  }
})();
