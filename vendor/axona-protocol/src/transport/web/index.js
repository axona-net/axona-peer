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
import { BridgeTransport }   from './bridge.js';
import { CompositeTransport } from './composite.js';
import { isHexId }            from '../../utils/hexid.js';
import { TransportError, ErrorCodes } from '../../errors.js';

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
 */

/**
 * Build a CompositeTransport whose two sub-transports are:
 *   - a WebRTCTransport over a MeshManager wired to the bridge's
 *     signaling channel
 *   - a BridgeTransport that talks Axona wire frames directly to the
 *     bridge over the same WebSocket
 *
 * Returns the CompositeTransport (which implements the full Transport
 * contract).  Call `await transport.start()` before use.
 *
 * @param {WebTransportConfig} config
 * @returns {CompositeTransport & { mesh: MeshManager, webrtc: WebRTCTransport, bridge: BridgeTransport, socket: WebSocket | null }}
 */
export function webTransport({ bridgeUrl, identity, log = () => {}, WebSocketImpl } = {}) {
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

  const mesh = new MeshManager({
    sendSignal: (msg) => sendToBridge(msg),
    log,
  });

  // Minimal signaling-frame dispatcher.  MeshManager's existing API
  // exposes the handlers it expects to be called when these frames
  // arrive (handleWelcome, handlePeerList, handlePeerJoined, etc).
  // Different builds of MeshManager have slightly different surface
  // names; we keep the dispatch defensive.
  const signaling = {
    dispatch(frame) {
      if (!frame || typeof frame !== 'object') return;
      const t = frame.type;
      if (t === 'welcome'     && typeof mesh.handleWelcome     === 'function') return mesh.handleWelcome(frame);
      if (t === 'peer-list'   && typeof mesh.handlePeerList    === 'function') return mesh.handlePeerList(frame);
      if (t === 'peer-joined' && typeof mesh.handlePeerJoined  === 'function') return mesh.handlePeerJoined(frame);
      if (t === 'peer-left'   && typeof mesh.handlePeerLeft    === 'function') return mesh.handlePeerLeft(frame);
      if (t === 'signal'      && typeof mesh.handleSignal      === 'function') return mesh.handleSignal(frame);
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
  };
  const origStop = composite.stop.bind(composite);
  composite.stop = async () => {
    await origStop();
    if (socket) {
      try { socket.close(); } catch { /* ignore */ }
      socket = null;
      socketOpen = false;
    }
    if (typeof mesh.dispose === 'function') mesh.dispose();
  };

  // Expose the sub-transports + raw mesh for orchestrators that need
  // direct access (hello/hello-ack wiring before W1 lands, smoke
  // tests, dht-sim integration).
  composite.mesh    = mesh;
  composite.webrtc  = webrtc;
  composite.bridge  = bridge;
  Object.defineProperty(composite, 'socket', { get() { return socket; } });

  return composite;
}
