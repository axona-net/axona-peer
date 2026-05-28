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
import { WebRTCTransport }   from './webrtc.js';
import { BridgeTransport, BRIDGE_CONN_ID_EXPORT as BRIDGE_CONN_ID } from './bridge.js';
import { CompositeTransport } from './composite.js';
import { isHexId, toHex, fromHex } from '../../utils/hexid.js';
import { TransportError, ErrorCodes, UpgradeRequiredError } from '../../errors.js';
import { KERNEL_VERSION }    from '../handshake.js';

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
    socket = new WSImpl(bridgeUrl);
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
            type:    'client-hello',
            version: peerVersion || KERNEL_VERSION,
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
      bridgeNodeIdBig = null;
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
  const mesh = new MeshManager({
    sendSignal: (toPeerId, payload) => {
      if (!socketOpen) {
        log('signal-drop-no-bridge', { to: toPeerId });
        return;
      }
      try {
        sendToBridge({ type: 'signal', to: toPeerId, payload });
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
    if (next === 'upgrade-required') upgradeReason = detail ?? upgradeReason;
    if (bridgeState === next) return;
    bridgeState = next;
    for (const h of stateHandlers) {
      try { h(next, detail); } catch (e) { log('bridge-state-handler-threw', { err: e.message }); }
    }
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
    // ── Bridge hello / hello-ack ─────────────────────────────────
    // Register BEFORE the socket opens so we don't miss the bridge's
    // first hello (which arrives on the same tick as version-gate /
    // welcome).
    //
    // Wire form: body.nodeId is hex (66-char).  Convert to BigInt at
    // the boundary; the rest of the kernel sees BigInt.
    bridge.onNotification('hello', (fromConnId, body) => {
      // Pre-bind state: fromConnId === BRIDGE_CONN_ID sentinel string.
      if (typeof fromConnId !== 'string') return;     // already bound
      if (!body || !isHexId(body.nodeId)) return;
      const nodeIdBig = fromHex(body.nodeId);
      try {
        bridge.bindPeer(nodeIdBig, BRIDGE_CONN_ID);
      } catch (err) {
        log('auto-handshake-bind-failed', { err: err.message });
        bridgeReadyReject(err);
        return;
      }
      // Reply with hello-ack so the bridge knows our nodeId.
      // Outbound wire: hex.
      bridge.notify(BRIDGE_CONN_ID, 'hello-ack', {
        proto:  'axona/3',
        nodeId: localNodeIdHex,
      }).catch(err => log('auto-handshake-ack-failed', { err: err.message }));
      bridgeNodeIdBig = nodeIdBig;
      reconnectAttempt = 0;          // good connection — reset backoff
      lastPongAt = Date.now();
      log('auto-handshake-complete', { bridgeNodeId: body.nodeId });
      setBridgeState('open');
      bridgeReadyResolve(nodeIdBig);
    });
    bridge.onNotification('hello-ack', (fromConnId, body) => {
      if (typeof fromConnId !== 'string') return;
      if (!body || !isHexId(body.nodeId)) return;
      if (bridgeNodeIdBig !== null) return;           // already done
      const nodeIdBig = fromHex(body.nodeId);
      try {
        bridge.bindPeer(nodeIdBig, BRIDGE_CONN_ID);
      } catch (err) {
        log('auto-handshake-bind-failed', { err: err.message });
        bridgeReadyReject(err);
        return;
      }
      bridgeNodeIdBig = nodeIdBig;
      reconnectAttempt = 0;          // good connection — reset backoff
      lastPongAt = Date.now();
      log('auto-handshake-complete', { bridgeNodeId: body.nodeId });
      setBridgeState('open');
      bridgeReadyResolve(nodeIdBig);
    });
    socketEvents.close.add(() => {
      if (bridgeNodeIdBig === null) {
        bridgeReadyReject(new UpgradeRequiredError(
          'bridge closed socket before handshake completed',
          { context: { reason: 'socket_closed_pre_handshake', bridgeUrl } }));
      }
    });

    // ── Mesh hello / hello-ack ────────────────────────────────────
    // When a WebRTC DataChannel reaches 'open' state, send hello to
    // the remote.  When their hello (or hello-ack) arrives, bindPeer
    // in WebRTCTransport so subsequent transport.send / notify by
    // BigInt nodeId routes via the mesh.  AxonaPeer's onPeerBound
    // subscriber then admits the new peer into the synaptome — the
    // kernel now handles the full multi-peer mesh admission
    // automatically.
    const helloSentToMeshId = new Set();
    if (typeof mesh.onChange === 'function') {
      mesh.onChange((peers) => {
        const list = Array.isArray(peers) ? peers : [];
        for (const p of list) {
          if (!p || p.state !== 'open') continue;
          const meshId = p.peerId ?? p.id;
          if (typeof meshId !== 'string') continue;
          if (helloSentToMeshId.has(meshId)) continue;
          helloSentToMeshId.add(meshId);
          try {
            mesh.send(meshId, {
              k: 'ntf', type: 'hello',
              body: { proto: 'axona/3', nodeId: localNodeIdHex },
            });
            log('mesh-hello-sent', { meshId });
          } catch (err) {
            log('mesh-hello-send-failed', { meshId, err: err.message });
          }
        }
      });
    }
    if (typeof mesh.onPeerLost === 'function') {
      mesh.onPeerLost((meshId) => helloSentToMeshId.delete(meshId));
    }
    webrtc.onNotification('hello', (fromConnId, body) => {
      // Pre-bind: fromConnId is the meshId string.  Once bound it
      // would be a BigInt — but the hello path is the binding event,
      // so we're always in the pre-bind branch here.
      if (typeof fromConnId !== 'string') return;
      if (!body || !isHexId(body.nodeId)) return;
      const meshId  = fromConnId;
      const peerBig = fromHex(body.nodeId);
      try {
        webrtc.bindPeer(peerBig, meshId);
      } catch (err) {
        log('mesh-bind-failed', { meshId, err: err.message });
        return;
      }
      // Reply with hello-ack on the SAME data channel (mesh.send,
      // not webrtc.notify — the latter requires bindPeer to have run
      // on the SENDING side too, which is the case here, but mesh.send
      // is the direct path that mirrors what axona-peer uses).
      try {
        mesh.send(meshId, {
          k: 'ntf', type: 'hello-ack',
          body: { proto: 'axona/3', nodeId: localNodeIdHex },
        });
      } catch (err) {
        log('mesh-hello-ack-failed', { meshId, err: err.message });
      }
      log('mesh-handshake-complete', { meshId, peer: body.nodeId });
    });
    webrtc.onNotification('hello-ack', (fromConnId, body) => {
      if (typeof fromConnId !== 'string') return;
      if (!body || !isHexId(body.nodeId)) return;
      const meshId  = fromConnId;
      const peerBig = fromHex(body.nodeId);
      try {
        webrtc.bindPeer(peerBig, meshId);
      } catch (err) {
        log('mesh-bind-failed', { meshId, err: err.message });
        return;
      }
      log('mesh-handshake-complete', { meshId, peer: body.nodeId });
    });
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
