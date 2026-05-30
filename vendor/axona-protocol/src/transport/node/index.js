// =====================================================================
// transport/node/index.js — Node-side Transport implementations.
//
// Two factories:
//
//   nodeTransport.server({ identity, attach, log })
//     Server mode — for a Node process accepting inbound WebSocket
//     connections (the bridge).  `attach` returns a tiny binding API
//     the orchestrator uses to feed connection lifecycle into the
//     transport:
//       attach.added(connId)        — call when a new WS opens
//       attach.message(connId, frame)  — call on every WS frame
//       attach.closed(connId)       — call when WS closes
//
//   nodeTransport.client({ identity, wsUrl, WebSocketImpl, log })
//     Client mode — opens a single outbound WebSocket to `wsUrl` (a
//     bridge) and exposes the Transport contract against the
//     embedded bridge peer.  Used by Node CLI tools, the simulator's
//     out-of-process integration, and tests.
//
// The shared core is WebSocketTransport (wstransport.js).
// nodeId convention: 66-char hex strings.
// =====================================================================

import { WebSocketTransport }                from './wstransport.js';
import { isHexId }                           from '../../utils/hexid.js';
import { TransportError, ErrorCodes }        from '../../errors.js';

export { WebSocketTransport };

/**
 * Build a Node-side WebSocketTransport in server mode. The caller
 * (typically axona-bridge/src/server.js) hooks the returned `attach`
 * methods into its WebSocketServer's `connection` / `message` /
 * `close` events.
 *
 * @param {object} opts
 * @param {object} opts.identity                Identity envelope with .id
 * @param {(connId: string, msg: object) => boolean} opts.sendToConn
 *        Synchronously writes `msg` (after JSON.stringify) to the
 *        WS identified by `connId`.  Returns true if the socket
 *        accepted the frame.  Throws if the socket is closed.
 * @param {(connId: string) => boolean}              opts.isConnOpen
 * @param {(event: string, data?: object) => void}   [opts.log]
 * @param {number}                                    [opts.requestTimeoutMs]
 * @returns {{
 *   transport: WebSocketTransport,
 *   attach: {
 *     added:   (connId: string) => void,
 *     message: (connId: string, frame: object) => void,
 *     closed:  (connId: string) => void,
 *   },
 * }}
 */
export function serverTransport({
  identity,
  sendToConn,
  isConnOpen,
  log = () => {},
  requestTimeoutMs,
  // axona/4 — opt-in mutual authentication.  When true, every inbound
  // connection must complete the handshake before it is bound; identity
  // needs {id, pubkeyHex, sign}.  `closeConn(connId, code, reason)` lets
  // the transport reject a legacy/forged peer (4426 Upgrade Required).
  authenticate = false,
  closeConn    = null,
  onAuthReject = null,
  onAuthBound  = null,
} = {}) {
  if (!identity || !isHexId(identity.id)) {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'serverTransport: identity must have a 66-char hex id',
      { context: { hasId: !!identity?.id } });
  }
  const transport = new WebSocketTransport({
    localNodeId: identity.id,
    sendToConn,
    isConnOpen,
    log,
    requestTimeoutMs,
    authenticate,
    identity,
    closeConn,
    onAuthReject,
    onAuthBound,
  });

  const attach = {
    added(connId) {
      log('ws-added', { connId });
      // Open the authenticated handshake as soon as the socket is up.
      if (authenticate) transport.beginAuth(connId);
    },
    message(connId, frame) {
      if (!frame || typeof frame !== 'object') return;
      if (frame.type !== 'axona') return;   // not for us — orchestrator handles signaling
      transport.handleIncoming(connId, frame.payload);
    },
    closed(connId) {
      transport.handleConnClosed(connId);
    },
  };
  return { transport, attach };
}

/**
 * Build a Node-side WebSocketTransport in client mode. Opens a single
 * outbound WS to `wsUrl`.
 *
 * @param {object} opts
 * @param {object} opts.identity              Identity envelope with .id
 * @param {string} opts.wsUrl                 e.g. 'wss://bridge.axona.net'
 * @param {Function} [opts.WebSocketImpl]     Defaults to globalThis.WebSocket
 *                                            (Node ≥21 has it built-in;
 *                                            earlier Node needs the `ws`
 *                                            library passed in).
 * @param {(event: string, data?: object) => void} [opts.log]
 * @param {number} [opts.requestTimeoutMs]
 * @returns {WebSocketTransport & { socket: WebSocket | null }}
 */
export function clientTransport({
  identity,
  wsUrl,
  WebSocketImpl,
  log = () => {},
  requestTimeoutMs,
  // axona/4 — opt-in mutual authentication against the server peer.
  // `expectServerNodeId`, when set, asserts the server proved that exact
  // nodeId (defends against a hostile relay swapping the far end).
  authenticate       = false,
  expectServerNodeId = null,
  onAuthReject       = null,
  onAuthBound        = null,
} = {}) {
  if (!identity || !isHexId(identity.id)) {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'clientTransport: identity must have a 66-char hex id');
  }
  if (typeof wsUrl !== 'string' || !/^wss?:\/\//.test(wsUrl)) {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'clientTransport: wsUrl must be a ws:// or wss:// URL',
      { context: { wsUrl } });
  }
  const WSImpl = WebSocketImpl ?? globalThis.WebSocket;
  if (typeof WSImpl !== 'function') {
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'clientTransport: no WebSocket implementation available; ' +
      'pass WebSocketImpl from the `ws` library on older Node');
  }

  const FIXED_CONN_ID = 'bridge';

  let socket = null;
  let socketOpen = false;

  function sendToConn(connId, msg) {
    if (connId !== FIXED_CONN_ID) return false;
    if (!socket || !socketOpen) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        'clientTransport: socket not open');
    }
    socket.send(JSON.stringify(msg));
    return true;
  }
  function isConnOpen(connId) {
    return connId === FIXED_CONN_ID && socketOpen;
  }
  // On auth failure, close the single outbound socket with the code.
  function closeConn(connId, code, reason) {
    if (connId !== FIXED_CONN_ID || !socket) return;
    try { socket.close(code, reason); }
    catch { try { socket.close(); } catch { /* ignore */ } }
  }

  const transport = new WebSocketTransport({
    localNodeId: identity.id,
    sendToConn,
    isConnOpen,
    log,
    requestTimeoutMs,
    authenticate,
    identity,
    closeConn:    authenticate ? closeConn : null,
    onAuthReject,
    onAuthBound,
  });

  // Override start/stop to manage the socket lifecycle.
  const origStart = transport.start.bind(transport);
  transport.start = async () => {
    if (!socket) {
      await new Promise((resolve, reject) => {
        socket = new WSImpl(wsUrl);
        socket.addEventListener('open', () => {
          socketOpen = true;
          log('client-socket-open', { wsUrl });
          resolve();
        });
        socket.addEventListener('close', () => {
          socketOpen = false;
          log('client-socket-close');
          transport.handleConnClosed(FIXED_CONN_ID);
        });
        socket.addEventListener('error', (err) => {
          log('client-socket-error', { err: err?.message ?? String(err) });
          if (!socketOpen) reject(new TransportError(
            ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
            `clientTransport: socket error before open`,
            { cause: err }));
        });
        socket.addEventListener('message', (ev) => {
          let frame;
          try { frame = JSON.parse(ev.data); }
          catch (err) {
            log('frame-parse-failed', { err: err.message });
            return;
          }
          if (frame && frame.type === 'axona') {
            transport.handleIncoming(FIXED_CONN_ID, frame.payload);
          }
        });
      });
    }
    await origStart(identity.id);
    // Once the socket is up, open the authenticated handshake against
    // the server peer.
    if (authenticate) {
      transport.beginAuth(FIXED_CONN_ID, { expectNodeId: expectServerNodeId });
    }
  };
  const origStop = transport.stop.bind(transport);
  transport.stop = async () => {
    await origStop();
    if (socket) {
      try { socket.close(); } catch { /* ignore */ }
      socket = null;
      socketOpen = false;
    }
  };

  Object.defineProperty(transport, 'socket', { get() { return socket; } });

  // Expose FIXED_CONN_ID so the consumer can bindPeer after hello-ack.
  transport.bridgeConnId = FIXED_CONN_ID;

  return transport;
}

// Convenience namespace export so consumers can do
// `Transport.node.server(...)` after a future top-level alias.
export const nodeTransport = {
  server: serverTransport,
  client: clientTransport,
};
