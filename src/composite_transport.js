// =====================================================================
// composite_transport.js — re-export of the kernel's CompositeTransport.
//
// As of axona-peer v3.0.0 the composite (bridge ⊎ WebRTC) Transport
// is owned by the kernel (@axona/protocol vendored under
// vendor/axona-protocol/).  This file exists only to preserve the
// local-import path `./composite_transport.js` that pre-v3 callers
// in this repo still use.
//
// Gains relative to the previous peer fork:
//   - `boundPeers()`        — aggregated nodeId list across sub-transports
//   - `onPeerBound(cb)`     — fan-out of bind events, deduplicated
//   - `onPingTraffic(cb)`   — fan-out of ping-pulse traffic
//   - Typed TransportError on "no route to peer"
//
// See vendor/axona-protocol/src/transport/web/composite.js for the
// implementation.
// =====================================================================

export { CompositeTransport } from '../vendor/axona-protocol/src/transport/web/composite.js';
