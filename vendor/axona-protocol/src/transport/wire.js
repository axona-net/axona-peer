// =====================================================================
// wire.js — shared JSON codec for Axona wire frames.
//
// Axona protocol values include `bigint` (XOR distances, low-level
// routing math) and `Set` (per-lookup `queried`) — neither survives a
// vanilla `JSON.stringify`.  We work around with a string-suffix
// convention:
//
//   BigInt 0xabc        →  "2748n"   (decimal digits + "n" sentinel)
//   Set([id1, id2, …])  →  [id1, id2, …]
//
// At the API boundary node IDs are 66-char hex strings — they need no
// special encoding.  The codec is for the internal payload types the
// protocol carries (XOR-distance bigints inside routing-decision
// envelopes, queried-set membership maps).
//
// Used by:
//   - src/transport/web/  (WebRTC data channels + bridge WebSocket)
//   - src/transport/node/ (WebSocket transport in the bridge)
//
// The bridge mirrors these conventions so every WS / DC channel uses
// the same wire format.  The protocol layer is responsible for
// wrapping incoming arrays back into a `Set` where it expects one
// (e.g., the `lookup_step` handler re-coerces `payload.queried`).
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
