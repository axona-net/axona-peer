// =====================================================================
// hexid.js — 264-bit identifier math for the Axona keyspace.
//
// Identifier layout:
//
//   nodeId   = [8-bit S2 prefix from geo] || [256-bit SHA-256(pubkey)]
//   topicId  = [8-bit S2 prefix from publisher.nodeId]
//                                       || [256-bit SHA-256(publisher.nodeId || ":" || topicName)]
//
// Total width: 264 bits. Encoded as 66-char lowercase hex at API
// boundaries (JSON-safe, sortable, comparable). BigInt internally for
// XOR distance math.
//
// The hash component is the full SHA-256 output (256 bits, 124-bit
// collision resistance against pubkey forgery). The S2 prefix is a
// routing hint that anchors a topic to its publisher's region.
//
// The keyspace is intentionally not byte-aligned (264 bits = 33 bytes);
// the security-over-tidiness trade is deliberate.
// =====================================================================

// ─── Constants ───────────────────────────────────────────────────────

export const ID_BITS      = 264;                  // total address width
export const HASH_BITS    = 256;                  // SHA-256 component
export const S2_BITS      = 8;                    // S2 prefix component
export const HEX_CHARS    = 66;                   // 264 / 4

export const MAX_ID       = (1n << 264n) - 1n;
export const MAX_HASH     = (1n << 256n) - 1n;
export const MAX_S2       = 255;                  // 2^8 - 1
export const HASH_MASK    = MAX_HASH;
export const S2_SHIFT     = 256n;                 // S2 prefix lives at top 8 bits

// ─── Encoding ────────────────────────────────────────────────────────

/**
 * Encode a 264-bit BigInt as a 66-char lowercase hex string.
 * Pads on the left with zeros so output width is stable for sorting
 * and exact-match equality across the JSON wire.
 *
 * @param {bigint} id
 * @returns {string} 66 lowercase hex chars
 */
export function toHex(id) {
  if (typeof id !== 'bigint') {
    throw new TypeError(`toHex expects bigint, got ${typeof id}`);
  }
  if (id < 0n || id > MAX_ID) {
    throw new RangeError(`id out of range [0, 2^264): ${id}`);
  }
  return id.toString(16).padStart(HEX_CHARS, '0');
}

/**
 * Decode a 66-char hex string back to a 264-bit BigInt.
 * Accepts mixed case; rejects anything that isn't exactly 66 hex chars.
 *
 * @param {string} hex
 * @returns {bigint}
 */
export function fromHex(hex) {
  if (typeof hex !== 'string') {
    throw new TypeError(`fromHex expects string, got ${typeof hex}`);
  }
  if (hex.length !== HEX_CHARS) {
    throw new RangeError(`hex id must be ${HEX_CHARS} chars, got ${hex.length}`);
  }
  if (!/^[0-9a-fA-F]+$/.test(hex)) {
    throw new RangeError(`hex id contains non-hex chars`);
  }
  return BigInt('0x' + hex);
}

/**
 * True iff `hex` is a syntactically valid 66-char node/topic ID.
 * @param {unknown} hex
 * @returns {boolean}
 */
export function isHexId(hex) {
  return typeof hex === 'string' &&
         hex.length === HEX_CHARS &&
         /^[0-9a-fA-F]+$/.test(hex);
}

// ─── Random ──────────────────────────────────────────────────────────

/**
 * Generate a cryptographically random 256-bit unsigned BigInt
 * via Web Crypto (works in browsers and Node ≥18).
 *
 * Use as the hash component of a placeholder node ID when no real
 * pubkey is available yet; the identity module (F2) replaces this
 * with `SHA-256(pubkey)`.
 *
 * @returns {bigint}
 */
export function randomU256() {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  let n = 0n;
  for (let i = 0; i < 32; i++) {
    n = (n << 8n) | BigInt(bytes[i]);
  }
  return n;
}

// ─── Composition / decomposition ─────────────────────────────────────

/**
 * Compose a 264-bit ID from its 8-bit S2 prefix and 256-bit hash.
 *
 * @param {number}   s2Prefix  0..255
 * @param {bigint}   hash256   0..2^256 - 1
 * @returns {bigint}           264-bit ID
 */
export function assembleId(s2Prefix, hash256) {
  if (!Number.isInteger(s2Prefix) || s2Prefix < 0 || s2Prefix > MAX_S2) {
    throw new RangeError(`s2Prefix out of range [0, 255]: ${s2Prefix}`);
  }
  if (typeof hash256 !== 'bigint' || hash256 < 0n || hash256 > MAX_HASH) {
    throw new RangeError(`hash256 out of range [0, 2^256): ${hash256}`);
  }
  return (BigInt(s2Prefix) << S2_SHIFT) | hash256;
}

/**
 * Extract the 8-bit S2 prefix from a 264-bit ID.
 * @param {bigint} id
 * @returns {number} 0..255
 */
export function extractS2Prefix(id) {
  if (typeof id !== 'bigint') {
    throw new TypeError(`extractS2Prefix expects bigint, got ${typeof id}`);
  }
  return Number(id >> S2_SHIFT);
}

/**
 * Extract the 256-bit hash component from a 264-bit ID.
 * @param {bigint} id
 * @returns {bigint} 0..2^256 - 1
 */
export function extractHash(id) {
  if (typeof id !== 'bigint') {
    throw new TypeError(`extractHash expects bigint, got ${typeof id}`);
  }
  return id & HASH_MASK;
}

/**
 * Read the 8-bit S2 prefix directly from a hex-encoded ID (cheap;
 * avoids the BigInt round-trip when only the prefix matters).
 *
 * @param {string} hex  66-char hex id
 * @returns {number}    0..255
 */
export function s2PrefixOfHex(hex) {
  if (!isHexId(hex)) {
    throw new RangeError(`not a valid hex id: ${hex}`);
  }
  return parseInt(hex.slice(0, 2), 16);
}

// ─── Distance / stratum ──────────────────────────────────────────────

/**
 * XOR distance between two 264-bit IDs. Symmetric, satisfies the
 * triangle inequality in the XOR metric. Used by Kademlia-style
 * routing.
 *
 * @param {bigint} a
 * @param {bigint} b
 * @returns {bigint}
 */
export function xorDistance(a, b) {
  return a ^ b;
}

/**
 * Count leading zeros of a 264-bit BigInt. Returns 264 for 0n;
 * otherwise returns the bit index (from the top) of the first 1.
 *
 * Used to compute the XOR stratum: `clz264(self ^ peer)` is the
 * number of leading bits self and peer share — equivalent to the
 * Kademlia bucket index `K - 1 - msb(self ^ peer)` for a K-bit
 * keyspace.
 *
 * Walks the BigInt in 32-bit chunks from the top using Math.clz32
 * for each chunk; O(8) work for 264 bits.
 *
 * @param {bigint} n  in [0, 2^264)
 * @returns {number}  0..264
 */
export function clz264(n) {
  if (typeof n !== 'bigint') {
    throw new TypeError(`clz264 expects bigint, got ${typeof n}`);
  }
  if (n === 0n) return ID_BITS;

  // Walk 32 bits at a time from the top. 264 bits = 8 full u32s + 8 bits.
  // Top chunk (bits 256..263) is 8 bits wide; bits 0..255 are 8 × 32 = 256 bits.
  const top8 = Number(n >> 256n) & 0xff;
  if (top8 !== 0) {
    // top8 is at most 0xff (8 bits). Math.clz32 treats input as u32, so
    // for an 8-bit value we get 24 + clz of an 8-bit-wide value. Subtract
    // the unused top 24 bits to get clz of the 8-bit slot.
    return Math.clz32(top8) - 24;
  }
  // Bits 0..255: 8 × u32. Index 7 holds bits 224..255, index 0 holds bits 0..31.
  for (let i = 7; i >= 0; i--) {
    const chunk = Number((n >> BigInt(i * 32)) & 0xFFFFFFFFn);
    if (chunk !== 0) {
      const skipped = (7 - i) * 32;
      return 8 + skipped + Math.clz32(chunk);
    }
  }
  // Should be unreachable (n !== 0n guarantees a non-zero chunk somewhere).
  return ID_BITS;
}

/**
 * Stratum index for the XOR distance between `selfId` and `peerId`.
 * Bounded to [0, ID_BITS - 1]. Two identical IDs would return
 * ID_BITS, which we clamp to ID_BITS - 1 so it remains a valid
 * bucket index.
 *
 * @param {bigint} selfId
 * @param {bigint} peerId
 * @returns {number} 0..263
 */
export function stratumOf(selfId, peerId) {
  return Math.min(ID_BITS - 1, clz264(selfId ^ peerId));
}
