// =====================================================================
// wire.js — re-export of the kernel's wire codec.
//
// As of axona-peer v3.0.0 the transport stack is the kernel
// (@axona/protocol vendored under vendor/axona-protocol/).  This file
// exists only to preserve the local-import path `./wire.js` that
// pre-v3 callers in this repo still use.  All behaviour is defined in
// the kernel — see vendor/axona-protocol/src/transport/wire.js.
// =====================================================================

export {
  bigintReplacer,
  bigintReviver,
  encode,
  decode,
} from '../vendor/axona-protocol/src/transport/wire.js';
