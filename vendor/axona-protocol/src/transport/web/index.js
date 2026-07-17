// =====================================================================
// transport/web/index.js — browser-side Transport implementations.
//
// Three classes ship out of this directory:
//
//   MeshManager        — RTCPeerConnection + RTCDataChannel + ICE,
//                        driven by signaling relayed through a bridge.
//                        Internal peer IDs are string `meshId`s.
//
//   WebRTCTransport    — Transport contract wrapping MeshManager; the
//                        per-channel Axona protocol layer rides on
//                        these.  nodeId↔meshId binding is internal.
//
//   BridgeTransport    — Transport contract that carries Axona wire
//                        frames over the browser ↔ bridge WebSocket.
//                        Used as the route for peers we haven't yet
//                        opened a WebRTC channel to (most importantly,
//                        the bridge's own embedded peer).
//
//   CompositeTransport — fans Transport-contract calls between the
//                        WebRTC and Bridge sub-transports based on
//                        which one owns each nodeId.
//
// The webTransport({...}) factory below ties them together for the
// common case (browser peer connecting to bridge.axona.net + opening
// WebRTC channels to other browsers it meets through that bridge).
//
// nodeIds at every Transport-contract surface are 264-bit BigInts.
// Hex strings (66-char) appear on the JSON wire (hello/hello-ack
// body.nodeId fields) and at user-facing display surfaces — converted
// at the dispatcher boundary inside this factory.
// =====================================================================

import { MeshManager }       from './mesh.js';
import { MeshAuth }          from './mesh-auth.js';
import { WebRTCTransport }   from './webrtc.js';
import { BridgeTransport, BRIDGE_CONN_ID_EXPORT as BRIDGE_CONN_ID } from './bridge.js';
import { CompositeTransport } from './composite.js';
import { isHexId, toHex, fromHex } from '../../utils/hexid.js';
import { TransportError, ErrorCodes, UpgradeRequiredError } from '../../errors.js';
import { KERNEL_VERSION, WIRE_VERSION } from '../handshake.js';
import {
  buildAuthHello, verifyAuthHello, cbvFromNonces, AUTH_PROTO,
} from '../handshake-auth.js';

export { MeshManager, WebRTCTransport, BridgeTransport, CompositeTransport };

/**
 * @typedef {object} WebTransportConfig
 * @property {string} bridgeUrl    e.g. 'wss://bridge.axona.net'
 * @property {object} identity     Identity envelope from `deriveIdentity`
 *                                 (or any object with `id` = 66-char hex).
 * @property {(event:string, data?:object) => void} [log]
 * @property {WebSocket}           [WebSocketImpl]
 *           Constructor for the WebSocket class.  Defaults to
 *           globalThis.WebSocket (browser).  Tests inject a fake.
 * @property {boolean}             [autoHandshake=true]
 *           When true (default), the transport drives the full bridge
 *           admission sequence as part of `start()`:
 *             (a) sends `{type:'client-hello', version}` as the first
 *                 raw frame on the socket (satisfies the bridge's
 *                 WebSocket-level version gate);
 *             (b) registers a notification handler for the bridge's
 *                 `hello`, calls `bridge.bindPeer(bridgeNodeIdBig, 'bridge')`
 *                 on receipt, replies with our own `hello-ack`;
 *             (c) `transport.start()` resolves only after the bridge
 *                 has been bound, OR rejects on timeout / WS close.
 *           Set to `false` for advanced consumers (axona-peer,
 *           dht-sim, smoke tests) that drive the handshake themselves.
 * @property {string}              [peerVersion]
 *           Semver string sent in `client-hello`.  Defaults to the
 *           kernel's KERNEL_VERSION.
 * @property {number}              [handshakeTimeoutMs=15000]
 *           How long to wait for the bridge's `hello` before rejecting
 *           start().  Ignored when autoHandshake is false.
 */

/**
 * Build a CompositeTransport whose two sub-transports are:
 *   - a WebRTCTransport over a MeshManager wired to the bridge's
 *     signaling channel
 *   - a BridgeTransport that talks Axona wire frames directly to the
 *     bridge over the same WebSocket
 *
 * With `autoHandshake: true` (default), `await transport.start()`
 * also completes the bridge's WebSocket-level version gate AND the
 * application-level hello / hello-ack admission.  After start, the
 * bridge is bound in `transport.bridge` and reachable as a peer.
 *
 * - `transport.bridgeNodeId`   — bridge's 66-char hex nodeId (display surface)
 * - `transport.bridgeNodeIdBig`— bridge's BigInt nodeId (kernel form)
 * - `transport.bridgeReady`    — Promise resolving to the BigInt bridge nodeId
 *
 * @param {WebTransportConfig} config
 * @returns {CompositeTransport & { mesh: MeshManager, webrtc: WebRTCTransport, bridge: BridgeTransport, socket: WebSocket | null, bridgeReady: Promise<bigint|null>, bridgeNodeId: string | null, bridgeNodeIdBig: bigint | null }}
 */
/** Bridge ping cadence — matches axona-peer's BRIDGE_PING_INTERVAL_MS. */
const BRIDGE_PING_INTERVAL_MS = 1000;
/** No pong within this window ⇒ bridge state goes 'stale'. */
const BRIDGE_STALE_PONG_MS    = 3000;
/** Reconnect backoff bounds (exponential, doubling per attempt). */
const RECONNECT_BACKOFF_INITIAL_MS = 1000;
const RECONNECT_BACKOFF_MAX_MS     = 16000;
/** WebSocket close code the bridge uses for version-gate rejection. */
const UPGRADE_CLOSE_CODE = 4426;
/** Window of recent RTT samples kept for the average. */
const RTT_WINDOW = 10;
// Ceiling on concurrent in-flight relay negotiations a node will START. The
// autonomous bridgeless-connect path (connectViaRelay, fired from peer
// discovery) is otherwise unbounded: a peer that sprays gossip introductions
// (triadic_introduce / hop_cache / lateral_spread) with distinct fabricated
// nodeIds could drive an arbitrary number of concurrent RTCPeerConnection
// negotiations. We throttle on the mesh's never-opened count, which the
// negotiation watchdog reaps on its own — so the cap frees up without any
// completion bookkeeping. Generous: normal nodes open channels in seconds and
// sit far below this; legitimate relay connects past the cap simply retry on
// the next discovery tick.
const MAX_PENDING_RELAY_NEGOTIATIONS = 64;

export function webTransport({
  bridgeUrl,
  identity,
  log = () => {},
  WebSocketImpl,
  autoHandshake = true,
  peerVersion,
  handshakeTimeoutMs = 15000,
  pingIntervalMs = BRIDGE_PING_INTERVAL_MS,
  // v2.1 — auto-reconnect with exponential backoff.  Only active when
  // autoHandshake is true (reconnect re-runs the version-gate +
  // hello/hello-ack the factory owns; an autoHandshake:false consumer
  // drives its own socket lifecycle).  Triggers on socket *close*
  // other than a 4426 version-gate rejection, so the first-attempt
  // handshake-timeout contract (start() rejects) is unchanged.
  reconnect = true,
  reconnectInitialMs = RECONNECT_BACKOFF_INITIAL_MS,
  reconnectMaxMs     = RECONNECT_BACKOFF_MAX_MS,
  // Peer-relayed signaling (bridgeless connect).  When true (the default as of
  // kernel v2.19.0, after the end-to-end verification in Peer-Relayed-Signaling
  // §8d), sendSignal prefers routing SDP/ICE through the mesh (via an AxonaPeer
  // relay registered with setSignalRelay) over the bridge, and connectViaRelay()
  // forms a new WebRTC edge to a nodeId without the bridge — driven autonomously
  // by AxonaPeer._considerCandidate on peer discovery.  Pass `false` to pin the
  // legacy bridge-only behaviour (the bridge bootstrap path is unaffected either
  // way: it signals by 3-char connId, which is not a hex nodeId, so the relay
  // sink never intercepts it).  Design:
  // axona-docs/implementation/Peer-Relayed-Signaling-v0.1.md.
  meshRelay = true,
} = {}) {
  if (typeof bridgeUrl !== 'string' || !/^wss?:\/\//.test(bridgeUrl)) {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'webTransport: bridgeUrl must be a ws:// or wss:// URL',
      { context: { bridgeUrl } });
  }
  if (!identity || !isHexId(identity.id)) {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'webTransport: identity must have a 66-char hex id',
      { context: { hasId: !!identity?.id } });
  }
  // axona/4 — the authenticated handshake signs with the identity's
  // key, so when autoHandshake is on we need a usable signer + pubkey.
  // Fail fast and clearly rather than silently producing unauthenticable
  // hellos that the network will reject.
  if (autoHandshake) {
    if (typeof identity.sign !== 'function' || typeof identity.pubkeyHex !== 'string'
        || identity.pubkeyHex.length !== 64) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'webTransport: identity must expose sign() + pubkeyHex (64-hex) for the ' +
        'authenticated handshake (axona/4); pass the full deriveIdentity() result',
        { context: { hasSign: typeof identity?.sign, pubkeyLen: identity?.pubkeyHex?.length } });
    }
  }
  const WSImpl = WebSocketImpl ?? globalThis.WebSocket;
  if (typeof WSImpl !== 'function') {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'webTransport: no WebSocket implementation available',
      { context: {} });
  }

  // Internal canonical form is BigInt; identity.id stays hex (user-facing
  // display + wire form).  `localNodeIdHex` is used only for hello/hello-ack
  // wire payloads and for the bridge's setMyId signaling-channel id.
  const localNodeIdHex = identity.id;
  const localNodeIdBig = fromHex(identity.id);

  // ── 1. Bridge WebSocket connection ───────────────────────────────
  //
  // The WebSocket carries:
  //   (a) signaling frames (peer-list, peer-joined, peer-left,
  //       opaque `signal` payloads relaying SDP / ICE between
  //       browser peers) — consumed by MeshManager
  //   (b) Axona wire frames addressed to the bridge's own embedded
  //       peer — consumed by BridgeTransport
  //
  // We construct the socket here and route inbound messages to the
  // appropriate sub-transport based on the frame's `type` field.

  let socket = null;
  let socketOpen = false;
  const socketEvents = {
    open:  new Set(),
    close: new Set(),
  };

  function openSocket() {
    if (socket) return;
    try {
      socket = new WSImpl(bridgeUrl);
    } catch (err) {
      // Synchronous construction failure (bad state, resource exhaustion):
      // treat like a failed attempt and keep the backoff chain alive.
      socket = null;
      log('bridge-socket-create-failed', { err: String(err?.message || err).slice(0, 120) });
      setBridgeState('disconnected');
      scheduleReconnect();
      return;
    }
    const sock = socket;
    // A failed HTTP upgrade — e.g. a proxy answering 502 while the bridge
    // container is still booting — surfaces as an 'error' EVENT, not a close.
    // In Node's `ws` an unlistened 'error' THROWS out of the emitter; that
    // escaped here as an uncaughtException, aborted before 'close' could fire,
    // and left `socket` non-null — so scheduleReconnect never ran and every
    // prod relay wedged in state=connecting after a bridge-only restart
    // (2026-07-09). Listening is the fix: ws then proceeds to its normal
    // 'close', which drives the backoff chain. The delayed fallback below
    // covers any implementation that fires 'error' without a 'close' for a
    // never-opened socket (guards make it a no-op when close did arrive).
    socket.addEventListener('error', (ev) => {
      const msg = ev?.message || ev?.error?.message || 'socket error';
      log('bridge-socket-error', { err: String(msg).slice(0, 120) });
      if (!socketOpen) {
        const fb = setTimeout(() => {
          if (socket === sock && !socketOpen && !stopped) {
            socket = null;
            setBridgeState('disconnected');
            scheduleReconnect();
          }
        }, 1000);
        if (typeof fb?.unref === 'function') fb.unref();   // never hold the process open
      }
    });
    socket.addEventListener('open', () => {
      socketOpen = true;
      log('bridge-socket-open', { bridgeUrl });
      setBridgeState('connecting');
      if (autoHandshake) {
        // (a) WebSocket-level version gate: the bridge requires
        // {type:'client-hello', version} as the FIRST raw frame, before
        // any axona payloads.  Sent here on every (re)open so reconnect
        // re-clears the gate without bespoke caller logic.
        try {
          socket.send(JSON.stringify({
            type:          'client-hello',
            version:       peerVersion || KERNEL_VERSION,
            wireVersion:   WIRE_VERSION,   // major-compat axis; the bridge gate
                                           // rejects a mismatched major (4426)
            kernelVersion: KERNEL_VERSION, // exact kernel build; lets a bridge
                                           // enforce a MIN_KERNEL_VERSION floor
                                           // (STRICT_VERSION island) independent
                                           // of the app's own `version`
            ...(meshRelay ? { capabilities: ['mesh-relay'] } : {}),
          }));
        } catch (err) {
          log('auto-handshake-client-hello-failed', { err: err.message });
        }
        // (c) Bridge ping/pong heartbeat + stale detection.
        startBridgePingLoop();
        startStaleChecker();
      }
      for (const h of socketEvents.open) try { h(); } catch (e) { log('open-handler-threw', { err: e.message }); }
    });
    socket.addEventListener('close', (ev) => {
      socketOpen = false;
      stopBridgePingLoop();
      const code = ev && typeof ev.code === 'number' ? ev.code : null;
      log('bridge-socket-close', { code });
      bridge.handleConnClosed();
      // Allow the persistent hello handler to re-bind on the next open.
      bridgeNodeIdBig   = null;
      bridgeServerNonce = null;   // fresh nonce per (re)connection
      if (code === UPGRADE_CLOSE_CODE) {
        // Version-gate rejection — reconnecting would just fail again.
        stopped = true;
        stopStaleChecker();
        setBridgeState('upgrade-required', (ev && ev.reason) || 'client out of date');
      } else if (!stopped && reconnect && autoHandshake) {
        setBridgeState('disconnected');
        scheduleReconnect();
      } else {
        stopStaleChecker();
        setBridgeState('disconnected');
      }
      // Drop the dead socket reference so openSocket() (called by the
      // reconnect path) can create a fresh one — its `if (socket) return`
      // guard would otherwise block reconnection.
      socket = null;
      for (const h of socketEvents.close) try { h(ev); } catch (e) { log('close-handler-threw', { err: e.message }); }
    });
    socket.addEventListener('message', (ev) => {
      let frame;
      try { frame = JSON.parse(ev.data); }
      catch (err) {
        log('bridge-frame-parse-failed', { err: err.message });
        return;
      }
      // Two upstream consumers:
      if (frame && frame.type === 'axona') {
        bridge.handleIncoming(frame.payload);
      } else {
        // Everything else (peer-list, peer-joined, signal, welcome, …)
        // is signaling — feed MeshManager.  The MeshManager's existing
        // surface uses callbacks rather than a single ingest entrypoint,
        // so the orchestrator below installs the relevant handlers.
        signaling.dispatch(frame);
      }
    });
  }

  function sendToBridge(msg) {
    if (!socket || !socketOpen) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        'webTransport: bridge socket not open');
    }
    socket.send(JSON.stringify(msg));
    return true;
  }

  // ── 2. MeshManager (handles WebRTC + signaling) ──────────────────

  // MeshManager calls sendSignal(toPeerId, payload) with two args when
  // it has SDP offers, answers, or ICE candidates for a remote peer.
  // Wrap them in the bridge's `signal` envelope so the bridge can
  // route the payload to the destination peer.  Pattern matches
  // axona-peer/src/client.js's MeshManager setup verbatim.
  // Peer-relayed signaling: an AxonaPeer registers its routed-delivery sink
  // here via composite.setSignalRelay().  When meshRelay is enabled and the
  // destination is a nodeId (hex meshId — the bridgeless connectViaRelay
  // path uses nodeIds as meshIds, whereas the bridge path uses 3-char
  // connIds), we offer the frame to the relay first; it returns true if it
  // took ownership (will route through the mesh), else we fall back to the
  // bridge.  Pure bridge behaviour is preserved when meshRelay is off.
  let signalRelay = null;
  // Signaling-path telemetry (W1): how often WebRTC signaling rides the mesh
  // (peer-relayed, bridge untouched) vs falls back to the bridge. Message-level
  // counts show load split; the distinct-peer sets approximate how many *links*
  // were established each way (a healthy dense mesh should signal almost every
  // post-bootstrap edge peer-to-peer, leaving the bridge only genuinely new
  // joiners + NAT/ICE failures). Pure measurement — no behaviour change.
  const signalStats = { meshMsgs: 0, bridgeMsgs: 0, dropMsgs: 0, meshPeers: new Set(), bridgePeers: new Set() };
  const mesh = new MeshManager({
    sendSignal: (toPeerId, payload) => {
      if (meshRelay && typeof signalRelay === 'function' && isHexId(toPeerId)) {
        let took = false;
        try { took = signalRelay(toPeerId, payload) === true; }
        catch (err) { log('signal-relay-threw', { to: toPeerId, err: err.message }); }
        if (took) { signalStats.meshMsgs++; signalStats.meshPeers.add(toPeerId); return; }
      }
      if (!socketOpen) {
        signalStats.dropMsgs++;
        log('signal-drop-no-bridge', { to: toPeerId });
        return;
      }
      try {
        sendToBridge({ type: 'signal', to: toPeerId, payload });
        signalStats.bridgeMsgs++; signalStats.bridgePeers.add(toPeerId);
      } catch (err) {
        log('signal-send-failed', { to: toPeerId, err: err.message });
      }
    },
    log,
  });

  // Signaling-frame dispatcher.  Bridge frames carry payloads addressed
  // to the local node's MeshManager so it can drive the WebRTC layer
  // (peer discovery + SDP/ICE relay).  The mapping from bridge frame
  // type → MeshManager method mirrors axona-peer/src/client.js's
  // onBridgeMessage switch — keep these in sync or peers will never
  // negotiate a data channel.
  const signaling = {
    dispatch(frame) {
      if (!frame || typeof frame !== 'object') return;
      const t = frame.type;
      switch (t) {
        case 'welcome':
          // Bridge greeting (myConnId, server version, optional TURN
          // credentials).  composite.start has already called
          // mesh.setMyId(localNodeIdHex); here we just thread the TURN
          // config through to the mesh BEFORE peer-list arrives so the
          // RTCPeerConnections built by _initiateTo can relay through
          // it.  Mirrors axona-peer/src/client.js's `case 'welcome'`.
          if (typeof mesh.setTurnConfig === 'function') {
            try { mesh.setTurnConfig(frame.turn ?? null); }
            catch (err) { log('turn-config-failed', { err: err.message }); }
          }
          // Capture welcome for observability (consumers read it via
          // transport.bridgeInfo + onWelcome) — connId, the bridge's
          // package version, and its kernel version for the UI's
          // version row.
          bridgeInfo = {
            connId:        frame.connId ?? null,
            version:       frame.version ?? null,
            kernelVersion: frame.kernelVersion ?? null,
            turn:          !!frame.turn,
          };
          // axona/4 — the bridge mints a fresh per-connection nonce in
          // welcome; it (with the connId) is the bridge-link channel
          // binding value both sides fold into their signed hello.
          bridgeServerNonce = (typeof frame.serverNonce === 'string') ? frame.serverNonce : null;
          for (const h of welcomeHandlers) {
            try { h(bridgeInfo); } catch (e) { log('welcome-handler-threw', { err: e.message }); }
          }
          log('bridge-welcome', {
            connId:  frame.connId,
            version: frame.version,
            turn:    !!frame.turn,
          });
          return;
        case 'peer-list':
          if (typeof mesh.onPeerList === 'function') {
            return mesh.onPeerList(Array.isArray(frame.peers) ? frame.peers : []);
          }
          break;
        case 'peer-joined':
          if (typeof mesh.onPeerJoined === 'function' && typeof frame.peerId === 'string') {
            return mesh.onPeerJoined(frame.peerId);
          }
          break;
        case 'peer-left':
          if (typeof mesh.onPeerLeft === 'function' && typeof frame.peerId === 'string') {
            return mesh.onPeerLeft(frame.peerId);
          }
          break;
        case 'signal':
          if (typeof mesh.onSignal === 'function' && typeof frame.from === 'string') {
            return mesh.onSignal(frame.from, frame.payload);
          }
          break;
        case 'pong':
          bridge._emitPingTraffic('recv');
          // RTT + liveness: the bridge echoes the ping's `t` timestamp.
          recordPong(frame.t);
          return;
        case 'version-gate':
          // Version-gate announcement — no action needed.
          return;
      }
      log('bridge-frame-unhandled', { type: t });
    },
  };

  // ── 3. WebRTCTransport over the mesh ─────────────────────────────

  const webrtc = new WebRTCTransport({
    mesh,
    localNodeId: localNodeIdBig,
    log,
  });

  // ── 4. BridgeTransport over the WebSocket ────────────────────────

  const bridge = new BridgeTransport({
    localNodeId: localNodeIdBig,
    sendToBridge: (msg) => sendToBridge(msg),
    isBridgeOpen: () => socketOpen,
    log,
  });

  // ── 5. CompositeTransport — public surface ───────────────────────

  const composite = new CompositeTransport({ localNodeId: localNodeIdBig, log });
  composite.addSubtransport(bridge);   // bridge is the single-peer fast-path
  composite.addSubtransport(webrtc);   // WebRTC for everyone else

  // ── Bridge handshake state (auto-handshake path) ─────────────────
  //
  // The kernel's webTransport optionally drives the full bridge
  // admission sequence so consumers don't have to re-discover it.
  // Two layers:
  //
  //   (a) WebSocket-level version gate.  The bridge requires
  //       `{type:'client-hello', version}` as the FIRST raw frame on
  //       the socket — before any axona payloads.  Send it once on
  //       open.
  //
  //   (b) Application-level hello / hello-ack.  After admission the
  //       bridge sends an `axona`-framed `hello` carrying its own
  //       nodeId (hex on the wire).  On receipt: convert to BigInt,
  //       bridge.bindPeer(nodeIdBig, 'bridge') + reply with hello-ack
  //       carrying our own hex nodeId.
  //
  // composite.start() awaits both layers when autoHandshake is true.
  /** @type {bigint|null} */
  let bridgeNodeIdBig = null;
  let bridgeReadyResolve = null;
  let bridgeReadyReject  = null;
  const bridgeReady = new Promise((resolve, reject) => {
    bridgeReadyResolve = resolve;
    bridgeReadyReject  = reject;
  });
  // Suppress unhandled-rejection warnings for the no-op case
  // (autoHandshake === false → we resolve immediately below).
  bridgeReady.catch(() => {});

  // ── Connection state machine (v2.1 — reconnect + observability) ──
  //
  // bridgeState transitions, surfaced via onBridgeState(cb):
  //   'connecting'       socket opening / handshake in flight
  //   'open'             handshake complete + pongs flowing
  //   'stale'            open but no pong within BRIDGE_STALE_PONG_MS
  //   'disconnected'     socket closed, reconnect pending
  //   'upgrade-required' bridge rejected us with 4426 (no reconnect)
  let bridgeState      = 'disconnected';
  let bridgeInfo       = null;   // last welcome: { connId, version, kernelVersion, turn }
  let bridgeServerNonce = null;  // axona/4 — per-connection nonce from welcome
  let upgradeReason    = null;   // set when state === 'upgrade-required'
  let lastPongAt       = 0;
  let lastRtt          = null;
  const rttBuffer      = [];
  let staleTimer       = null;
  let reconnectTimer   = null;
  let reconnectAttempt = 0;
  let stopped          = false;  // composite.stop() sets this — suppresses reconnect
  const stateHandlers   = new Set();
  const welcomeHandlers = new Set();

  function setBridgeState(next, detail) {
    if (next === 'upgrade-required') {
      upgradeReason = detail ?? upgradeReason;
      logUpgradeRequired(upgradeReason);
    }
    if (bridgeState === next) return;
    bridgeState = next;
    for (const h of stateHandlers) {
      try { h(next, detail); } catch (e) { log('bridge-state-handler-threw', { err: e.message }); }
    }
  }

  // axona/4 — surface "you must upgrade" loudly to the DEVELOPER CONSOLE
  // by default, not only through an app-wired onBridgeState handler.
  // The whole point of the gate is to help developers of apps we don't
  // control: when their build speaks an older protocol than the network
  // requires, the kernel itself prints an actionable, branded line so
  // it's obvious in DevTools without any app cooperation.  Fires once
  // per distinct reason.
  let _lastUpgradeLogged = null;
  function logUpgradeRequired(reason) {
    if (reason && reason === _lastUpgradeLogged) return;
    _lastUpgradeLogged = reason;
    const msg =
      `[axona] UPGRADE REQUIRED — this client could not join the network. ` +
      `It speaks protocol ${AUTH_PROTO} / kernel ${KERNEL_VERSION}, but the ` +
      `bridge rejected it${reason ? ` (${reason})` : ''}. ` +
      `Update @axona/protocol to the current release and reload.`;
    try {
      if (typeof console !== 'undefined' && console.error) console.error(msg);
    } catch { /* no console — ignore */ }
  }

  function recordPong(t) {
    lastPongAt = Date.now();
    if (typeof t === 'number') {
      lastRtt = Math.max(0, Date.now() - t);
      rttBuffer.push(lastRtt);
      if (rttBuffer.length > RTT_WINDOW) rttBuffer.shift();
    }
    if (bridgeState === 'stale') setBridgeState('open');
  }

  function startStaleChecker() {
    if (staleTimer != null) return;
    staleTimer = setInterval(() => {
      if (bridgeState !== 'open' && bridgeState !== 'stale') return;
      if (lastPongAt === 0) return;
      const since = Date.now() - lastPongAt;
      if (since > BRIDGE_STALE_PONG_MS && bridgeState === 'open') {
        setBridgeState('stale');
      }
    }, 500);
    if (typeof staleTimer?.unref === 'function') staleTimer.unref();
  }
  function stopStaleChecker() {
    if (staleTimer != null) { clearInterval(staleTimer); staleTimer = null; }
  }

  function scheduleReconnect() {
    if (stopped || !reconnect || !autoHandshake) return;
    if (reconnectTimer != null) return;
    const delay = Math.min(
      reconnectInitialMs * (2 ** reconnectAttempt),
      reconnectMaxMs,
    );
    reconnectAttempt++;
    log('bridge-reconnect-scheduled', { delay, attempt: reconnectAttempt });
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (stopped) return;
      setBridgeState('connecting');
      openSocket();          // re-open; the 'open' handler re-runs client-hello,
                             // the persistent hello handler re-binds the bridge.
    }, delay);
    if (typeof reconnectTimer?.unref === 'function') reconnectTimer.unref();
  }

  if (!autoHandshake) {
    bridgeReadyResolve(null);
  } else {
    // ════════════════════════════════════════════════════════════════
    // axona/4 AUTHENTICATED HANDSHAKE
    //
    // Every bind is now gated on proof: the peer must present its
    // pubkey, that pubkey must hash to the 256-bit suffix of the nodeId
    // it claims, and it must sign a per-connection channel-binding value
    // (CBV).  An unproven nodeId is never bound — closing the root
    // impersonation / eclipse gap.  See transport/handshake-auth.js.
    // ════════════════════════════════════════════════════════════════

    // ── Bridge link ───────────────────────────────────────────────
    // CBV = the bridge's per-connection serverNonce (from welcome) +
    // the connId.  Both are unpredictable per connection, so a hello
    // captured on one connection can't be replayed onto another.  Two
    // messages suffice because welcome pre-seeds the nonce before either
    // side signs.
    function bridgeCbv() {
      if (!bridgeServerNonce) return null;
      return cbvFromNonces(bridgeServerNonce, bridgeInfo?.connId ?? '', 'bridge');
    }

    const onBridgeAuthHello = async (fromConnId, body, label) => {
      if (typeof fromConnId !== 'string') return;     // already bound
      if (label === 'hello-ack' && bridgeNodeIdBig !== null) return;
      const cbv = bridgeCbv();
      if (!cbv) { log('auth-bridge-no-cbv', { label }); return; }
      const res = await verifyAuthHello(body, { cbv });
      if (!res.ok) {
        log('auth-bridge-rejected', { label, reason: res.reason });
        // A proto mismatch means the bridge speaks a version this
        // client doesn't — surface the upgrade prompt to the console.
        if (res.reason === 'proto_mismatch') setBridgeState('upgrade-required', 'bridge_proto_newer');
        return;
      }
      const nodeIdBig = fromHex(res.nodeId);
      try { bridge.bindPeer(nodeIdBig, BRIDGE_CONN_ID); }
      catch (err) { log('auth-bridge-bind-failed', { err: err.message }); bridgeReadyReject(err); return; }

      // Reply with OUR authenticated hello-ack over the same CBV (only
      // on the inbound 'hello' — the bridge's reply to our ack would
      // loop).
      if (label === 'hello') {
        try {
          const ack = await buildAuthHello({ identity, cbv });
          bridge.notify(BRIDGE_CONN_ID, 'hello-ack', ack)
            .catch(err => log('auth-bridge-ack-send-failed', { err: err.message }));
        } catch (err) {
          log('auth-bridge-ack-build-failed', { err: err.message });
        }
      }

      bridgeNodeIdBig  = nodeIdBig;
      reconnectAttempt = 0;
      lastPongAt       = Date.now();
      log('auth-bridge-complete', { bridgeNodeId: res.nodeId });
      setBridgeState('open');
      bridgeReadyResolve(nodeIdBig);
    };
    bridge.onNotification('hello',     (c, b) => onBridgeAuthHello(c, b, 'hello'));
    bridge.onNotification('hello-ack', (c, b) => onBridgeAuthHello(c, b, 'hello-ack'));

    socketEvents.close.add(() => {
      if (bridgeNodeIdBig === null) {
        bridgeReadyReject(new UpgradeRequiredError(
          'bridge closed socket before handshake completed',
          { context: { reason: 'socket_closed_pre_handshake', bridgeUrl } }));
      }
    });

    // ── Mesh (peer ↔ peer over WebRTC) ────────────────────────────
    // Symmetric 3-message mutual handshake, owned by MeshAuth
    // (mesh-auth.js) so the orchestration is unit-testable without real
    // WebRTC.  The CBV folds a fresh nonce pair (freshness) AND each
    // side's DTLS certificate fingerprint (channel binding, finding A-1):
    // a bridge that terminates DTLS to MITM the mesh must present a
    // different cert on each leg, so the two endpoints derive divergent
    // fingerprints and the mutual signature fails.
    const meshAuth = new MeshAuth({
      identity,
      send:         (meshId, frame)     => mesh.send(meshId, frame),
      bindPeer:     (nodeIdHex, meshId, channelKey) => webrtc.bindPeer(fromHex(nodeIdHex), meshId, channelKey),
      fingerprints: (meshId)            => mesh.fingerprintsFor(meshId),
      log,
    });

    if (typeof mesh.onChange === 'function') {
      mesh.onChange((peers) => {
        const list = Array.isArray(peers) ? peers : [];
        for (const p of list) {
          if (!p || p.state !== 'open') continue;
          const meshId = p.peerId ?? p.id;
          if (typeof meshId === 'string') meshAuth.onChannelOpen(meshId);
        }
      });
    }
    if (typeof mesh.onPeerLost === 'function') {
      mesh.onPeerLost((meshId) => meshAuth.onChannelLost(meshId));
    }
    webrtc.onNotification('hello',     (fromConnId, body) => meshAuth.onHello(fromConnId, body));
    webrtc.onNotification('hello-sig', (fromConnId, body) => meshAuth.onHelloSig(fromConnId, body));
  }

  // Wire start() so calling composite.start() opens the socket and
  // starts the sub-transports in order.  Stop reverses the chain.
  const origStart = composite.start.bind(composite);
  composite.start = async () => {
    openSocket();
    // Wait for socket open before starting BridgeTransport (so its
    // notify/send don't fail-fast against a not-yet-open socket).
    if (!socketOpen) {
      await new Promise((resolve, reject) => {
        const onOpen  = () => { socketEvents.open.delete(onOpen); socketEvents.close.delete(onClose); resolve(); };
        const onClose = () => { socketEvents.open.delete(onOpen); socketEvents.close.delete(onClose); reject(new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED, 'bridge socket closed before open')); };
        socketEvents.open.add(onOpen);
        socketEvents.close.add(onClose);
      });
    }
    // mesh.setMyId is the signaling-channel identifier the mesh layer
    // compares against to skip self in peer-list iteration.  The mesh
    // layer uses bridge connIds (3-char strings) as peerIds; passing
    // our hex nodeId here is a no-op skip (it never matches a 3-char
    // connId in the peer-list) — preserved as-is for compatibility
    // with axona-peer's wiring.  Pass hex (the wire-form of our id).
    if (typeof mesh.setMyId === 'function') mesh.setMyId(localNodeIdHex);
    await origStart(localNodeIdBig);

    // The socket 'open' handler already sent the client-hello version
    // gate and armed the ping/pong + stale heartbeat (so reconnect
    // re-runs them on every re-open).  start() just awaits the FIRST
    // application-level hello / hello-ack to land.
    if (autoHandshake) {
      const timer = setTimeout(() => {
        if (bridgeNodeIdBig === null) {
          bridgeReadyReject(new UpgradeRequiredError(
            `bridge handshake timed out after ${handshakeTimeoutMs}ms`,
            { context: { reason: 'handshake_timeout', bridgeUrl } }));
        }
      }, handshakeTimeoutMs);
      try {
        await bridgeReady;
      } finally {
        clearTimeout(timer);
      }
    }
  };
  const origStop = composite.stop.bind(composite);
  composite.stop = async () => {
    stopped = true;                     // suppress any pending reconnect
    if (reconnectTimer != null) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    stopStaleChecker();
    stopBridgePingLoop();
    await origStop();
    if (socket) {
      try { socket.close(); } catch { /* ignore */ }
      socket = null;
      socketOpen = false;
    }
    setBridgeState('disconnected');
    if (typeof mesh.dispose === 'function') mesh.dispose();
  };

  // ── Bridge ping/pong heartbeat ──────────────────────────────────
  // The live bridge drops idle clients after a short timeout.  Send a
  // raw `{type:'ping', t}` over the WebSocket every pingIntervalMs;
  // the bridge replies with `{type:'pong', t}` which the signaling
  // dispatcher logs as bridge-frame-unhandled (harmless).  Future
  // enhancement: surface RTT to consumers via transport.getLatency.
  let pingTimer = null;
  function startBridgePingLoop() {
    if (pingTimer != null) return;
    pingTimer = setInterval(() => {
      if (!socket || !socketOpen) return;
      try {
        socket.send(JSON.stringify({ type: 'ping', t: Date.now() }));
        bridge._emitPingTraffic('sent');
      } catch (err) {
        log('bridge-ping-send-failed', { err: err.message });
      }
    }, pingIntervalMs);
    if (typeof pingTimer?.unref === 'function') pingTimer.unref();
  }
  function stopBridgePingLoop() {
    if (pingTimer != null) {
      clearInterval(pingTimer);
      pingTimer = null;
    }
  }
  // Also stop the ping loop if the socket dies for any reason.
  socketEvents.close.add(() => stopBridgePingLoop());

  // Expose the sub-transports + raw mesh for orchestrators that need
  // direct access (hello/hello-ack wiring before W1 lands, smoke
  // tests, dht-sim integration).
  composite.mesh    = mesh;
  composite.webrtc  = webrtc;
  composite.bridge  = bridge;

  // ── Peer-relayed signaling surface (bridgeless connect) ──────────────
  // Only meaningful when meshRelay is enabled; an AxonaPeer detects these
  // methods on its transport and wires itself up on start().
  //
  // setSignalRelay(fn): register the outbound relay sink.  fn(toHexId,
  //   payload) → boolean ("took ownership").  Consumed by the sendSignal
  //   closure above.
  composite.setSignalRelay = (fn) => {
    if (fn !== null && typeof fn !== 'function') {
      throw new TypeError('setSignalRelay: fn must be a function or null');
    }
    signalRelay = fn;
  };
  // deliverMeshSignal(fromHex, payload): terminal ingress — a relayed
  //   `mesh:signal` reached us as its target; feed it into the SAME mesh
  //   signaling state machine the bridge path drives (offerer/responder/ICE).
  composite.deliverMeshSignal = (fromHex, payload) => {
    if (typeof mesh.onSignal !== 'function') return;
    try { return mesh.onSignal(fromHex, payload); }
    catch (err) { log('mesh-signal-deliver-threw', { from: fromHex, err: err.message }); }
  };
  // connectViaRelay(toHex): initiate a new direct WebRTC channel to a nodeId
  //   we hold no binding for, using the nodeId as the meshId.  The offer's
  //   SDP/ICE then rides the relay sink above.  No-op when meshRelay is off,
  //   when we already own a binding/channel to the target, or for self.
  composite.connectViaRelay = (toHex) => {
    if (!meshRelay) { log('relay-connect-disabled', { to: toHex }); return false; }
    if (typeof toHex !== 'string' || !isHexId(toHex)) return false;
    if (toHex === localNodeIdHex) return false;
    try {
      const toBig = fromHex(toHex);
      // No-op if we already own a binding, an open channel, OR an in-flight
      // negotiation to this peer.  The last guard is essential: peer discovery
      // (triadic_introduce etc.) fires connectViaRelay repeatedly, and without
      // it each call would re-run _initiateTo and overwrite the in-progress
      // RTCPeerConnection, restarting ICE so the channel never opens.
      if (webrtc.ownsPeer(toBig) || mesh.isConnected(toHex) || mesh.hasPeer(toHex)) return false;
    } catch { return false; }
    // Backpressure: cap concurrent in-flight relay negotiations so a flood of
    // gossip-introduced fake peerIds can't drive unbounded RTCPeerConnection
    // setup. The watchdog reaps stuck never-opened negotiations, so the cap
    // self-frees; a throttled connect retries on the next discovery tick.
    const pending = mesh.pendingNegotiations();
    if (pending >= MAX_PENDING_RELAY_NEGOTIATIONS) {
      log('relay-connect-throttled', { to: toHex, pending });
      return false;
    }
    log('relay-connect-initiate', { to: toHex });
    mesh._initiateTo(toHex);
    return true;
  };
  // Advisory capability surface (forward-compat; functional gate is the flag).
  composite.capabilities = () => (meshRelay ? ['mesh-relay'] : []);
  composite.hasCapability = (cap) => composite.capabilities().includes(cap);
  // Signaling-path telemetry (W1): snapshot of mesh-relayed vs bridge-fallback
  // signaling. bridgeMsgFraction near 0 on a healthy mesh = the bridge is only
  // carrying genuine new-joiner + NAT/ICE-failure signaling, as intended.
  composite.signalStats = () => {
    const m = signalStats.meshMsgs, b = signalStats.bridgeMsgs;
    return {
      meshMsgs: m, bridgeMsgs: b, dropMsgs: signalStats.dropMsgs,
      meshPeers: signalStats.meshPeers.size, bridgePeers: signalStats.bridgePeers.size,
      bridgeMsgFraction: (m + b) ? +(b / (m + b)).toFixed(3) : 0,
    };
  };
  Object.defineProperty(composite, 'socket',          { get() { return socket; } });
  Object.defineProperty(composite, 'bridgeReady',     { get() { return bridgeReady; } });
  // Display surface: hex (derived from BigInt).  External UI / log
  // consumers read this for human-readable bridge nodeId.
  Object.defineProperty(composite, 'bridgeNodeId',    {
    get() { return bridgeNodeIdBig === null ? null : toHex(bridgeNodeIdBig); },
  });
  // Kernel-internal form: BigInt.
  Object.defineProperty(composite, 'bridgeNodeIdBig', {
    get() { return bridgeNodeIdBig; },
  });

  // ── v2.1 observability surface ───────────────────────────────────
  // Current bridge connection state (see setBridgeState transitions).
  Object.defineProperty(composite, 'bridgeState', { get() { return bridgeState; } });
  // Last `welcome` frame: { connId, version, kernelVersion, turn } or null.
  Object.defineProperty(composite, 'bridgeInfo',  { get() { return bridgeInfo; } });
  // Reason string when state === 'upgrade-required'.
  Object.defineProperty(composite, 'upgradeReason', { get() { return upgradeReason; } });
  // Most recent bridge ping→pong RTT in ms (null until first pong).
  Object.defineProperty(composite, 'bridgeRtt',   { get() { return lastRtt; } });
  // Mean of the recent RTT window, or null.
  Object.defineProperty(composite, 'bridgeRttAvg', {
    get() {
      return rttBuffer.length
        ? rttBuffer.reduce((a, b) => a + b, 0) / rttBuffer.length
        : null;
    },
  });
  /**
   * Ask the bridge to resend its admitted peer-list (task #332, facet 2).
   * The join-time peer-list is a one-shot; a peer whose WebRTC mesh later
   * dissolved has an empty routing table, so self-integration can't find
   * anyone to dial. On the resent list, mesh.onPeerList re-initiates to
   * every peer it lacks — the same path that formed the mesh at join.
   * Rate-limit at the caller (the kernel's mesh-rewarm cooldown does).
   * Returns true if the request was sent (bridge socket open).
   */
  composite.requestPeerIntroductions = () => {
    try { sendToBridge({ type: 'peer-list-request' }); return true; }
    catch { return false; }
  };
  /** Subscribe to bridge-state transitions.  cb(state, detail). Returns unsub. */
  composite.onBridgeState = (cb) => {
    if (typeof cb !== 'function') throw new TypeError('onBridgeState: cb must be a function');
    stateHandlers.add(cb);
    return () => stateHandlers.delete(cb);
  };
  /** Subscribe to bridge welcome frames.  cb({connId,version,kernelVersion,turn}). Returns unsub. */
  composite.onWelcome = (cb) => {
    if (typeof cb !== 'function') throw new TypeError('onWelcome: cb must be a function');
    welcomeHandlers.add(cb);
    // Replay the last welcome so late subscribers aren't left blank.
    if (bridgeInfo) { try { cb(bridgeInfo); } catch { /* ignore */ } }
    return () => welcomeHandlers.delete(cb);
  };
  /** Force an immediate reconnect now (e.g. on tab resume / network online).
   *  No-op if stopped or reconnect disabled.  Closes the live socket so the
   *  close handler's reconnect path runs with a reset backoff. */
  composite.reconnectNow = () => {
    if (stopped || !reconnect || !autoHandshake) return;
    reconnectAttempt = 0;
    if (reconnectTimer != null) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    if (socket && socketOpen) {
      try { socket.close(); } catch { /* close handler schedules reconnect */ }
    } else if (!socket) {
      setBridgeState('connecting');
      openSocket();
    }
  };

  return composite;
}
