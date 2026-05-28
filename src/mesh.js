// =====================================================================
// mesh.js — re-export of the kernel's MeshManager.
//
// As of axona-peer v3.0.0 the WebRTC mesh manager is owned by the
// kernel (@axona/protocol vendored under vendor/axona-protocol/).  This
// file exists only to preserve the local-import path `./mesh.js` that
// pre-v3 callers in this repo still use.
//
// Behaviours included with the kernel version that the previous peer
// fork lacked:
//   - `onPingTraffic(callback)` — per-frame pulse hook the demo's dot
//     strip uses to blink in time with real bytes-on-the-wire.
//   - The `wasOpen = openedAt > 0` teardown fix that prevents missed
//     onPeerLost notifications when the data channel flips to 'closing'
//     before our cleanup runs.
//   - Naming aligned with the rest of the kernel (`_emitPingTraffic`).
//
// See vendor/axona-protocol/src/transport/web/mesh.js for the
// implementation.
// =====================================================================

export { MeshManager } from '../vendor/axona-protocol/src/transport/web/mesh.js';
