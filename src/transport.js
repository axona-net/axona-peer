// =====================================================================
// transport.js — re-export of the kernel's WebRTCTransport.
//
// As of axona-peer v3.0.0 the WebRTC-backed Transport implementation
// is owned by the kernel (@axona/protocol vendored under
// vendor/axona-protocol/).  This file exists only to preserve the
// local-import path `./transport.js` that pre-v3 callers in this repo
// still use.
//
// Gains relative to the previous peer fork:
//   - `boundPeers()`         — read the live nodeId↔meshId binding map
//   - `onPeerBound(handler)` — subscribe to bind/unbind events
//   - `ownsPeer(nodeId)`     — true if this transport is the one bound
//                              to a given nodeId
//   - `onPingTraffic(cb)`    — per-frame pulse hook for UI indicators
//
// See vendor/axona-protocol/src/transport/web/webrtc.js for the
// implementation.
// =====================================================================

export { WebRTCTransport } from '../vendor/axona-protocol/src/transport/web/webrtc.js';
