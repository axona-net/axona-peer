// =====================================================================
// wire.js — shared JSON codec for Axona wire frames.
//
// Axona protocol values include `bigint` (nodeIds, XOR distances) and
// `Set` (per-lookup `queried`) — neither survives a vanilla
// `JSON.stringify`.  We work around with a string-suffix convention:
//
//   BigInt 0xabc        →  "2748n"   (decimal digits + "n" sentinel)
//   Set([id1, id2, …])  →  [id1, id2, …]
//
// The bridge mirrors these conventions (axona-bridge/src/server.js)
// so every WS / DC channel uses the same wire format.  The Axona
// protocol layer is responsible for wrapping incoming arrays back
// into a `Set` where it expects one (e.g., the `lookup_step` handler
// re-coerces `payload.queried`).
//
// One module, two callers: mesh.js (WebRTC data channel) and
// client.js (bridge WebSocket).  Don't fork — keep the codec
// canonical so wire-format bugs only need fixing once.
// =====================================================================

/** JSON.stringify replacer.  Emits BigInt as "<digits>n", Set as array. */
export function bigintReplacer(_key, value) {
  if (typeof value === 'bigint') return value.toString() + 'n';
  if (value instanceof Set)      return [...value];
  return value;
}

/** JSON.parse reviver.  Inverts the "<digits>n" suffix back to BigInt. */
export function bigintReviver(_key, value) {
  if (typeof value === 'string' && /^-?\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
}

/** Convenience: `JSON.stringify` with the Axona replacer. */
export function encode(msg) {
  return JSON.stringify(msg, bigintReplacer);
}

/** Convenience: `JSON.parse` with the Axona reviver. */
export function decode(text) {
  return JSON.parse(text, bigintReviver);
}
