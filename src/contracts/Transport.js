// =====================================================================
// contracts/Transport.js — re-export of the kernel's Transport contract.
//
// As of axona-peer v3.0.0 the transport contract is owned by the
// kernel (@axona/protocol vendored under vendor/axona-protocol/).  This
// file exists only to preserve the local-import path
// `./contracts/Transport.js` that pre-v3 callers in this repo still
// use.  All conformance rules and the abstract class itself live in
// vendor/axona-protocol/src/contracts/Transport.js.
// =====================================================================

export { Transport } from '../../vendor/axona-protocol/src/contracts/Transport.js';
