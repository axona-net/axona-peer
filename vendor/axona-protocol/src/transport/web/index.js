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
// nodeIds at every Transport-contract surface are 66-char lowercase
// hex strings (matches the kernel's 264-bit address space).
//
// Hello/hello-ack admission — the handshake that exchanges nodeIds on
// each fresh channel and calls bindPeer() — lands as part of the W1
// task (#22) since it's the version-gated entry point.  Until then,
// callers wire bindPeer themselves; the existing axona-peer code base
// has the reference orchestration.
// =====================================================================

import { MeshManager }       from './mesh.js';
import { WebRTCTransport }   from './webrtc.js';
import { BridgeTransport, BRIDGE_CONN_ID_EXPORT as BRIDGE_CONN_ID } from './bridge.js';
import { CompositeTransport } from './composite.js';
import { isHexId }            from '../../utils/hexid.js';
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
 *                 `hello`, calls `bridge.bindPeer(bridgeNodeId, 'bridge')`
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
 * Read `transport.bridgeNodeId` for the bound bridge's 66-char hex id.
 *
 * @param {WebTransportConfig} config
 * @returns {CompositeTransport & { mesh: MeshManager, webrtc: WebRTCTransport, bridge: BridgeTransport, socket: WebSocket | null, bridgeReady: Promise<string|null>, bridgeNodeId: string | null }}
 */
/** Bridge ping cadence — matches axona-peer's BRIDGE_PING_INTERVAL_MS. */
const BRIDGE_PING_INTERVAL_MS = 1000;

export function webTransport({
  bridgeUrl,
  identity,
  log = () => {},
  WebSocketImpl,
  autoHandshake = true,
  peerVersion,
  handshakeTimeoutMs = 15000,
  pingIntervalMs = BRIDGE_PING_INTERVAL_MS,
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

  const localNodeId = identity.id;

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
      for (const h of socketEvents.open) try { h(); } catch (e) { log('open-handler-threw', { err: e.message }); }
    });
    socket.addEventListener('close', () => {
      socketOpen = false;
      log('bridge-socket-close');
      bridge.handleConnClosed();
      for (const h of socketEvents.close) try { h(); } catch (e) { log('close-handler-threw', { err: e.message }); }
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
          // mesh.setMyId(localNodeId); here we just thread the TURN
          // config through to the mesh BEFORE peer-list arrives so the
          // RTCPeerConnections built by _initiateTo can relay through
          // it.  Mirrors axona-peer/src/client.js's `case 'welcome'`.
          if (typeof mesh.setTurnConfig === 'function') {
            try { mesh.setTurnConfig(frame.turn ?? null); }
            catch (err) { log('turn-config-failed', { err: err.message }); }
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
        case 'version-gate':
          // Heartbeat reply / version-gate announcement — no action needed.
          return;
      }
      log('bridge-frame-unhandled', { type: t });
    },
  };

  // ── 3. WebRTCTransport over the mesh ─────────────────────────────

  const webrtc = new WebRTCTransport({
    mesh,
    localNodeId,
    log,
  });

  // ── 4. BridgeTransport over the WebSocket ────────────────────────

  const bridge = new BridgeTransport({
    localNodeId,
    sendToBridge: (msg) => sendToBridge(msg),
    isBridgeOpen: () => socketOpen,
    log,
  });

  // ── 5. CompositeTransport — public surface ───────────────────────

  const composite = new CompositeTransport({ localNodeId, log });
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
  //       nodeId.  On receipt: bridge.bindPeer(nodeId, 'bridge') +
  //       reply with hello-ack carrying our nodeId.
  //
  // composite.start() awaits both layers when autoHandshake is true.
  let bridgeNodeId = null;
  let bridgeReadyResolve = null;
  let bridgeReadyReject  = null;
  const bridgeReady = new Promise((resolve, reject) => {
    bridgeReadyResolve = resolve;
    bridgeReadyReject  = reject;
  });
  // Suppress unhandled-rejection warnings for the no-op case
  // (autoHandshake === false → we resolve immediately below).
  bridgeReady.catch(() => {});

  if (!autoHandshake) {
    bridgeReadyResolve(null);
  } else {
    // ── Bridge hello / hello-ack ─────────────────────────────────
    // Register BEFORE the socket opens so we don't miss the bridge's
    // first hello (which arrives on the same tick as version-gate /
    // welcome).
    bridge.onNotification('hello', (fromConnId, body) => {
      if (typeof fromConnId !== 'string') return;     // already bound
      if (!body || !isHexId(body.nodeId)) return;
      const nodeIdHex = body.nodeId;
      try {
        bridge.bindPeer(nodeIdHex, BRIDGE_CONN_ID);
      } catch (err) {
        log('auto-handshake-bind-failed', { err: err.message });
        bridgeReadyReject(err);
        return;
      }
      // Reply with hello-ack so the bridge knows our nodeId.
      bridge.notify(BRIDGE_CONN_ID, 'hello-ack', {
        proto:  'axona/3',
        nodeId: localNodeId,
      }).catch(err => log('auto-handshake-ack-failed', { err: err.message }));
      bridgeNodeId = nodeIdHex;
      log('auto-handshake-complete', { bridgeNodeId: nodeIdHex });
      bridgeReadyResolve(nodeIdHex);
    });
    bridge.onNotification('hello-ack', (fromConnId, body) => {
      if (typeof fromConnId !== 'string') return;
      if (!body || !isHexId(body.nodeId)) return;
      if (bridgeNodeId) return;                       // already done
      const nodeIdHex = body.nodeId;
      try {
        bridge.bindPeer(nodeIdHex, BRIDGE_CONN_ID);
      } catch (err) {
        log('auto-handshake-bind-failed', { err: err.message });
        bridgeReadyReject(err);
        return;
      }
      bridgeNodeId = nodeIdHex;
      log('auto-handshake-complete', { bridgeNodeId: nodeIdHex });
      bridgeReadyResolve(nodeIdHex);
    });
    socketEvents.close.add(() => {
      if (!bridgeNodeId) {
        bridgeReadyReject(new UpgradeRequiredError(
          'bridge closed socket before handshake completed',
          { context: { reason: 'socket_closed_pre_handshake', bridgeUrl } }));
      }
    });

    // ── Mesh hello / hello-ack ────────────────────────────────────
    // When a WebRTC DataChannel reaches 'open' state, send hello to
    // the remote.  When their hello (or hello-ack) arrives, bindPeer
    // in WebRTCTransport so subsequent transport.send / notify by
    // nodeId routes via the mesh.  AxonaPeer's onPeerBound subscriber
    // then admits the new peer into the synaptome — the kernel now
    // handles the full multi-peer mesh admission automatically.
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
              body: { proto: 'axona/3', nodeId: localNodeId },
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
      if (typeof fromConnId !== 'string') return;     // already bound
      if (!body || !isHexId(body.nodeId)) return;
      const meshId  = fromConnId;
      const peerHex = body.nodeId;
      try {
        webrtc.bindPeer(peerHex, meshId);
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
          body: { proto: 'axona/3', nodeId: localNodeId },
        });
      } catch (err) {
        log('mesh-hello-ack-failed', { meshId, err: err.message });
      }
      log('mesh-handshake-complete', { meshId, peer: peerHex });
    });
    webrtc.onNotification('hello-ack', (fromConnId, body) => {
      if (typeof fromConnId !== 'string') return;
      if (!body || !isHexId(body.nodeId)) return;
      const meshId  = fromConnId;
      const peerHex = body.nodeId;
      try {
        webrtc.bindPeer(peerHex, meshId);
      } catch (err) {
        log('mesh-bind-failed', { meshId, err: err.message });
        return;
      }
      log('mesh-handshake-complete', { meshId, peer: peerHex });
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
    if (typeof mesh.setMyId === 'function') mesh.setMyId(localNodeId);
    await origStart(localNodeId);

    if (autoHandshake) {
      // (a) WebSocket-level version gate: send the raw client-hello
      // frame the bridge waits for.  Must precede any axona payloads.
      try {
        sendToBridge({
          type:    'client-hello',
          version: peerVersion || KERNEL_VERSION,
        });
      } catch (err) {
        log('auto-handshake-client-hello-failed', { err: err.message });
      }
      // (b) Wait for the application-level hello / hello-ack to land.
      const timer = setTimeout(() => {
        if (!bridgeNodeId) {
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

      // (c) Start the bridge ping/pong heartbeat.  The live bridge
      // closes idle sockets after ~15s without a ping; axona-peer
      // sends one every 1s.  We do the same so apps stay connected.
      startBridgePingLoop();
    }
  };
  const origStop = composite.stop.bind(composite);
  composite.stop = async () => {
    stopBridgePingLoop();
    await origStop();
    if (socket) {
      try { socket.close(); } catch { /* ignore */ }
      socket = null;
      socketOpen = false;
    }
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
      } catch (err) {
        log('bridge-ping-send-failed', { err: err.message });
      }
    }, pingIntervalMs);
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
  Object.defineProperty(composite, 'socket',       { get() { return socket; } });
  Object.defineProperty(composite, 'bridgeReady',  { get() { return bridgeReady; } });
  Object.defineProperty(composite, 'bridgeNodeId', { get() { return bridgeNodeId; } });

  return composite;
}
