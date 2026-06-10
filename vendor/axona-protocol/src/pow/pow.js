// =====================================================================
// pow/pow.js — proof-of-work scaffolding (E-1 placement defense /
//              publish anti-flood anchor).
//
// SHIPPED AT DIFFICULTY 0 — BEHAVIOURALLY INERT. The `pow` (transport) and
// `signerPow` (publish) fields and this verifier travel the wire NOW so the
// difficulty can later be raised by a PARAMETER change, not an identity/
// envelope-format flag-day. (E-1 decision record §5; remediation Stage 2.)
//
// ⚠️ SCAFFOLDING HASH = SHA-256 (fast, ASIC-friendly). At difficulty 0 it never
// gates anything — `powVerify` short-circuits to true (0 required leading-zero
// bits ⇒ any nonce, including an absent one, passes). BEFORE difficulty is
// raised above 0 (Stage 4), this MUST be replaced with a MEMORY-HARD function
// (Argon2id / scrypt) per the E-1 decision — that swap rides the same
// coordinated release that raises difficulty, so it is not a separate flag-day.
//
// PER-ROLE difficulty is intentional: a transport identity can be ground into a
// root position (eclipse) and warrants higher difficulty; a publish key cannot
// route or root — it can only flood — so it needs only enough to make
// quota-evasion uneconomic. The model: mint cost ≈ 2^difficulty work-hashes;
// verify cost = 1. Honest peers pay it ONCE; a grinder pays it per attempt.
//
// Zero runtime dependencies (WebCrypto only), browser + Node.
// =====================================================================

const _enc = new TextEncoder();

export const POW_DOMAIN = 'axona:pow:v1';

// Required leading-zero BITS, by role. 0 ⇒ no-op (inert, shipped state). Raising
// either is a protocol-parameter change gated by a coordinated version bump
// (Stage 4a transport / 4b publish), at which point the scaffolding hash above
// must already be memory-hard.
export const POW_DIFFICULTY = Object.freeze({ transport: 0, publish: 0 });

export function powDifficulty(role) {
  const d = POW_DIFFICULTY[role];
  return Number.isInteger(d) && d > 0 ? d : 0;
}

/** Count leading zero bits of a byte array (the realized difficulty). */
function leadingZeroBits(bytes) {
  let bits = 0;
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i];
    if (b === 0) { bits += 8; continue; }
    let mask = 0x80;
    while (mask && (b & mask) === 0) { bits++; mask >>= 1; }
    break;
  }
  return bits;
}

async function workHash(pubkeyHex, role, nonce) {
  // SCAFFOLDING HASH — see file header. Bound to (domain, role, pubkey, nonce):
  // a nonce minted for one pubkey/role can't be reused for another.
  const buf = await crypto.subtle.digest('SHA-256', _enc.encode(`${POW_DOMAIN}:${role}:${pubkeyHex}:${nonce}`));
  return new Uint8Array(buf);
}

/** The realized difficulty (leading zero bits) of a (pubkey, role, nonce). */
export async function powBits({ pubkeyHex, nonce, role = 'transport' }) {
  return leadingZeroBits(await workHash(pubkeyHex, role, String(nonce)));
}

/**
 * Verify a PoW nonce against a pubkey at the given role's difficulty.
 * At difficulty 0 (the shipped default) returns true for ANY nonce — including
 * an absent/empty one — so peers and envelopes that predate the field pass.
 */
export async function powVerify({ pubkeyHex, nonce, role = 'transport', difficulty } = {}) {
  const d = Number.isInteger(difficulty) ? difficulty : powDifficulty(role);
  if (d <= 0) return true;                                   // inert / no-op
  if (typeof pubkeyHex !== 'string' || typeof nonce !== 'string' || nonce.length === 0) return false;
  return leadingZeroBits(await workHash(pubkeyHex, role, nonce)) >= d;
}

/**
 * Mint a PoW nonce for a pubkey at the given role's difficulty.
 * At difficulty 0 returns '' instantly (no search). Otherwise searches nonces
 * until the work hash carries >= difficulty leading zero bits.
 */
export async function powMint({ pubkeyHex, role = 'transport', difficulty, maxTries = 50_000_000 } = {}) {
  const d = Number.isInteger(difficulty) ? difficulty : powDifficulty(role);
  if (d <= 0) return '';                                     // inert
  if (typeof pubkeyHex !== 'string' || pubkeyHex.length === 0) {
    throw new TypeError('powMint: pubkeyHex required');
  }
  for (let i = 0; i < maxTries; i++) {
    const nonce = i.toString(36);
    if (leadingZeroBits(await workHash(pubkeyHex, role, nonce)) >= d) return nonce;
  }
  throw new Error(`powMint: no nonce within ${maxTries} tries at difficulty ${d}`);
}

/**
 * Device calibration: measure the work-hash rate (hashes/sec) so an operator can
 * pick a difficulty target. Gathered WHILE difficulty is 0 — pure measurement,
 * no protocol effect. `estMintMs[D]` ≈ expected one-time mint cost at D bits.
 */
export async function powCalibrate({ ms = 400 } = {}) {
  const t0 = Date.now();
  let n = 0;
  while (Date.now() - t0 < ms) { await workHash('00', 'cal', String(n)); n++; }
  const seconds = (Date.now() - t0) / 1000 || 1e-3;
  const hashesPerSec = Math.round(n / seconds);
  const est = (D) => (hashesPerSec > 0 ? Math.round((2 ** D) / hashesPerSec * 1000) : Infinity);
  return { hashesPerSec, sampled: n, seconds, estMintMs: { 12: est(12), 16: est(16), 20: est(20), 24: est(24) } };
}
