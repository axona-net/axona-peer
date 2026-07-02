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

// ─── Keyspace profile (configurable; default = production 264-bit) ────
//
// The width constants below are EXPORTED LET bindings so every consumer that
// `import { ID_BITS, ... }`s them sees the live value. They default to the
// production profile (8-bit region + 256-bit SHA-256 hash = 264-bit node/topic
// ids, 256-bit author ids). The SIMULATOR — and only the simulator — calls
// `configureKeyspace({ hashBits })` ONCE at startup, before any identity or
// peer is minted, to shrink the hash component (e.g. hashBits:64 → 72-bit
// node/topic ids, 64-bit author ids) so churn tests scale to many nodes.
// Production never calls it. Region width stays 8 bits unless overridden.

export let ID_BITS;          // total node/topic address width (region + hash)
export let HASH_BITS;        // hash component (SHA-256 in prod; truncated in sim)
export let S2_BITS;          // region/S2 prefix width
export let HEX_CHARS;        // ceil(ID_BITS/4) — node/topic hex width
export let AUTHOR_ID_BITS;   // author id width (= HASH_BITS; carries no region)
export let AUTHOR_HEX_CHARS; // ceil(AUTHOR_ID_BITS/4) — author id hex width
export let MAX_ID;           // 2^ID_BITS - 1
export let MAX_HASH;         // 2^HASH_BITS - 1
export let MAX_S2;           // 2^S2_BITS - 1
export let HASH_MASK;        // = MAX_HASH (mask the hash slot)
export let S2_SHIFT;         // region lives above the hash → BigInt(HASH_BITS)
export let AUTH_VERIFY_RELAXED; // true when authorId is shrunk below the real pubkey
                                // width (sim only) → signerPubkey can no longer BE the
                                // Ed25519 pubkey, so envelope verify SKIPS the crypto
                                // signature check. Production (256-bit) keeps full verify.

let _hashBits   = 256;
let _regionBits = 8;
let _configured = false;

function _recompute() {
  HASH_BITS       = _hashBits;
  S2_BITS         = _regionBits;
  ID_BITS         = _regionBits + _hashBits;
  HEX_CHARS       = Math.ceil(ID_BITS / 4);
  AUTHOR_ID_BITS  = _hashBits;
  AUTHOR_HEX_CHARS = Math.ceil(AUTHOR_ID_BITS / 4);
  MAX_HASH        = (1n << BigInt(_hashBits)) - 1n;
  MAX_ID          = (1n << BigInt(ID_BITS)) - 1n;
  MAX_S2          = (1 << _regionBits) - 1;
  HASH_MASK       = MAX_HASH;
  S2_SHIFT        = BigInt(_hashBits);
  // A sub-256-bit author id cannot also be a verifiable 256-bit Ed25519 pubkey;
  // in that (sim-only) profile the envelope verifier skips the crypto check.
  AUTH_VERIFY_RELAXED = _hashBits < 256;
}
_recompute();   // production default

/**
 * Shrink the keyspace for in-simulator runs. SET ONCE, before any identity or
 * peer is created. PRODUCTION MUST NEVER CALL THIS — the default 264-bit profile
 * is load-bearing for the live network. Intended caller: the dht-sim transport.
 *
 * @param {{ hashBits?: number, regionBits?: number }} opts
 */
export function configureKeyspace({ hashBits, regionBits } = {}) {
  if (typeof hashBits === 'number') {
    if (!Number.isInteger(hashBits) || hashBits < 8 || hashBits > 256) {
      throw new RangeError(`configureKeyspace: hashBits must be an integer in [8, 256], got ${hashBits}`);
    }
    _hashBits = hashBits;
  }
  if (typeof regionBits === 'number') {
    if (!Number.isInteger(regionBits) || regionBits < 0 || regionBits > 16) {
      throw new RangeError(`configureKeyspace: regionBits must be an integer in [0, 16], got ${regionBits}`);
    }
    _regionBits = regionBits;
  }
  _recompute();
  _configured = true;
  // Loud, so a non-default keyspace can never go unnoticed (e.g. a sim build
  // accidentally shipped). Stderr — never the JSON wire.
  try {
    (globalThis.process?.stderr?.write ?? ((s) => console.warn(s.trim())))(
      `[axona] KEYSPACE CONFIGURED → region=${_regionBits}b hash=${_hashBits}b ` +
      `nodeId=${ID_BITS}b authorId=${AUTHOR_ID_BITS}b (NON-PRODUCTION unless 8/256)\n`);
  } catch { /* logging is best-effort */ }
}

/** Current keyspace profile (introspection / tests). */
export function getKeyspace() {
  return {
    regionBits: S2_BITS, hashBits: HASH_BITS, idBits: ID_BITS,
    authorIdBits: AUTHOR_ID_BITS, hexChars: HEX_CHARS, authorHexChars: AUTHOR_HEX_CHARS,
    configured: _configured, isProductionDefault: _hashBits === 256 && _regionBits === 8,
  };
}

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
    throw new RangeError(`id out of range [0, 2^${ID_BITS}): ${id}`);
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
// Stable marker on every "this isn't a valid id" error fromHex throws, so a
// caller (e.g. a transport dispatch boundary) can classify a malformed-frame
// drop — an expected, low-severity condition during churn/shutdown — apart from
// a genuine bug, WITHOUT string-matching the message text.
export const BAD_ID_CODE = 'AXONA_BAD_ID';
const badId = (err) => { err.code = BAD_ID_CODE; return err; };

export function fromHex(hex) {
  if (typeof hex !== 'string') {
    throw badId(new TypeError(`fromHex expects string, got ${typeof hex}`));
  }
  if (hex.length !== HEX_CHARS) {
    throw badId(new RangeError(`hex id must be ${HEX_CHARS} chars, got ${hex.length}`));
  }
  if (!/^[0-9a-fA-F]+$/.test(hex)) {
    throw badId(new RangeError(`hex id contains non-hex chars`));
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

/**
 * asId — THE canonical coercion gate for Axona addresses.
 *
 * The rule: an address (nodeId / peerId / topicId / targetId / via / subscriberId)
 * is a BigInt everywhere inside the system. A hex string is legitimate ONLY inside
 * a JSON wire frame or as an identity object's serialized `.id`/`.authorId`. Every
 * value crossing INTO the internal world passes through here; every value crossing
 * OUT to JSON passes through `toHex()`/`idHex()`. Internal code never has to ask
 * "is this a string or a bigint?" — it can assume bigint, because construction and
 * wire ingress both run through this gate.
 *
 * Accepts a bigint (idempotent) or a hex string (optionally `0x`-prefixed). Both are
 * range-checked to [0, MAX_ID]. Anything else throws a BAD_ID_CODE-tagged error, so a
 * transport dispatch boundary can classify a malformed-frame drop apart from a bug.
 *
 * Deliberately width-LENIENT (unlike `fromHex`): it validates the numeric range, not
 * the hex length. `idHex` pads to 66 chars even in a shrunk sim profile (HEX_CHARS<66),
 * and zero-padding never changes the value, so a strict-width check would wrongly reject
 * valid sim ids. Use `fromHex` where an exact-width parse of an untrusted frame is wanted.
 *
 * @param {bigint|string} v
 * @returns {bigint}
 */
export function asId(v) {
  if (typeof v === 'bigint') {
    if (v < 0n || v > MAX_ID) {
      throw badId(new RangeError(`asId: id out of range [0, 2^${ID_BITS}): ${v}`));
    }
    return v;
  }
  if (typeof v === 'string') {
    const h = (v.startsWith('0x') || v.startsWith('0X')) ? v.slice(2) : v;
    if (h.length === 0 || !/^[0-9a-fA-F]+$/.test(h)) {
      throw badId(new TypeError(`asId: not a hex id string: ${JSON.stringify(v)}`));
    }
    const n = BigInt('0x' + h);
    if (n > MAX_ID) {
      throw badId(new RangeError(`asId: id out of range [0, 2^${ID_BITS}): ${v}`));
    }
    return n;
  }
  throw badId(new TypeError(`asId expects bigint or hex string, got ${typeof v}`));
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
  return n & HASH_MASK;   // profile-aware: full 256 bits in prod, truncated in a shrunk sim profile
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
  // Width-generic: find the most-significant set bit by walking 32-bit chunks
  // from the top of the active ID_BITS-wide keyspace. clz = ID_BITS - 1 - msb.
  const chunks = Math.ceil(ID_BITS / 32);
  for (let i = chunks - 1; i >= 0; i--) {
    const chunk = Number((n >> BigInt(i * 32)) & 0xFFFFFFFFn);
    if (chunk !== 0) {
      const msb = i * 32 + (31 - Math.clz32(chunk));   // 0-indexed bit position from LSB
      return ID_BITS - 1 - msb;
    }
  }
  return ID_BITS;   // unreachable (n !== 0n)
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
