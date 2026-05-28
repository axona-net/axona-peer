// =====================================================================
// bridge_transport.js — re-export of the kernel's BridgeTransport.
//
// As of axona-peer v3.0.0 the bridge-WebSocket Transport
// implementation is owned by the kernel (@axona/protocol vendored
// under vendor/axona-protocol/).  This file exists only to preserve
// the local-import path `./bridge_transport.js` that pre-v3 callers
// in this repo still use.
//
// Gains relative to the previous peer fork:
//   - `onPingTraffic(cb)`   — per-frame pulse hook for UI indicators
//   - Typed TransportError on bad-nodeId / no-route paths
//   - Stricter bindPeer signature checking
//
// `BRIDGE_CONN_ID_EXPORT` is re-exported under the same name (and is
// imported as `BRIDGE_CONN_ID` by axona_node.js).
//
// See vendor/axona-protocol/src/transport/web/bridge.js for the
// implementation.
// =====================================================================

export {
  BridgeTransport,
  BRIDGE_CONN_ID_EXPORT,
} from '../vendor/axona-protocol/src/transport/web/bridge.js';
