// =====================================================================
// ed25519.js — Ed25519 helpers with a software fallback.
//
// Prefers native Web Crypto Ed25519 (Chrome ~137+, Safari 17+, Firefox
// 130+, Node 20+) — fast and, for the private key, NON-EXTRACTABLE
// (finding H4). On runtimes WITHOUT native Ed25519 (older Chrome /
// Samsung Internet / many in-app WebViews) it transparently falls back to
// a vendored pure-JS implementation (@noble/ed25519, see src/crypto/), so
// those devices can still derive an identity, sign, and verify — i.e. join
// the network at all. Before this fallback, deriveIdentity threw
// "generateKey: Unrecognized name" and the device never connected.
//
// Key material:
//   - native:   CryptoKey handles (private key non-extractable when asked).
//   - software: { __sw:true, kind:'private'|'public', secret?, raw } —
//               raw bytes; INHERENTLY extractable (it lives in JS memory),
//               so the H4 hardening is a native-only property. The fallback
//               is strictly better than "can't connect."
//
// Signatures interoperate both ways (it's the same RFC 8032 Ed25519): a
// software-signed publish verifies under a native verifier and vice versa.
//
// All functions are async. Public keys export to 32 raw bytes; private
// keys persist as standard Ed25519 PKCS#8 (interoperable with native).
// =====================================================================

import * as noble from '../crypto/noble-ed25519.js';

const ALGORITHM = { name: 'Ed25519' };

// Standard Ed25519 PKCS#8 v1 prefix (RFC 8410); the 32-byte seed follows.
// Web Crypto exports exactly this, so software ⇄ native persistence interops.
const PKCS8_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06,
  0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
]);

// ── native-Ed25519 detection (cached; overridable for tests) ─────────
let _native = null;            // null = unknown, true/false once probed
let _forceSoftware = false;    // test hook

/** TEST ONLY: force the software path on a runtime that has native Ed25519. */
export function __setForceSoftware(v) { _forceSoftware = !!v; _native = v ? false : null; }

/** Whether native Web Crypto Ed25519 is usable here. Probes once, caches. */
export async function nativeEd25519Available() {
  if (_forceSoftware) return false;
  if (_native !== null) return _native;
  try {
    await crypto.subtle.generateKey(ALGORITHM, true, ['sign', 'verify']);
    _native = true;
  } catch {
    _native = false;   // e.g. "generateKey: Unrecognized name" on old Chrome
  }
  return _native;
}

const isSwPriv = (k) => !!(k && k.__sw && k.kind === 'private');
const isSwPub  = (k) => !!(k && k.__sw && k.kind === 'public');
export const isSoftwareKey = (k) => !!(k && k.__sw);

// ── keygen ──────────────────────────────────────────────────────────
/**
 * Generate a fresh Ed25519 keypair. Native → CryptoKey handles (private
 * non-extractable when `extractable:false`, finding H4). Software → byte
 * handles (the `extractable` flag is moot; software keys are always
 * extractable — documented tradeoff, only when native is unavailable).
 * @param {{extractable?: boolean}} [opts]
 * @returns {Promise<{publicKey: any, privateKey: any}>}
 */
export async function generateKeyPair({ extractable = true } = {}) {
  if (await nativeEd25519Available()) {
    const pair = await crypto.subtle.generateKey(ALGORITHM, true, ['sign', 'verify']);
    if (extractable) return pair;
    const pkcs8 = await crypto.subtle.exportKey('pkcs8', pair.privateKey);
    const privateKey = await crypto.subtle.importKey('pkcs8', pkcs8, ALGORITHM, false, ['sign']);
    return { publicKey: pair.publicKey, privateKey };
  }
  const secret = noble.utils.randomPrivateKey();          // 32-byte seed
  const raw    = await noble.getPublicKeyAsync(secret);   // 32-byte pubkey
  return {
    publicKey:  { __sw: true, kind: 'public',  raw },
    privateKey: { __sw: true, kind: 'private', secret, raw },
  };
}

// ── public-key export / import ──────────────────────────────────────
/** Export a public key to its 32-byte raw Ed25519 encoding. */
export async function exportPublicKey(publicKey) {
  if (isSwPub(publicKey)) return publicKey.raw;
  const buf = await crypto.subtle.exportKey('raw', publicKey);
  return new Uint8Array(buf);
}

/** Import 32 raw bytes into a verify-capable key handle (native or software). */
export async function importPublicKey(rawBytes) {
  const raw = rawBytes instanceof Uint8Array ? rawBytes : new Uint8Array(rawBytes);
  if (await nativeEd25519Available()) {
    return crypto.subtle.importKey('raw', raw, ALGORITHM, true, ['verify']);
  }
  return { __sw: true, kind: 'public', raw };
}

// ── private-key persistence (PKCS#8; interoperable native ⇄ software) ─
/** Export a private key to PKCS#8 bytes (for dumpIdentity). */
export async function exportPrivateKeyPkcs8(privateKey) {
  if (isSwPriv(privateKey)) {
    const out = new Uint8Array(PKCS8_PREFIX.length + 32);
    out.set(PKCS8_PREFIX, 0);
    out.set(privateKey.secret, PKCS8_PREFIX.length);
    return out;
  }
  const buf = await crypto.subtle.exportKey('pkcs8', privateKey);
  return new Uint8Array(buf);
}

/** Import a PKCS#8 private key (from loadIdentity) → signing key handle. */
export async function importPrivateKey(pkcs8Bytes) {
  const bytes = pkcs8Bytes instanceof Uint8Array ? pkcs8Bytes : new Uint8Array(pkcs8Bytes);
  if (await nativeEd25519Available()) {
    return crypto.subtle.importKey('pkcs8', bytes, ALGORITHM, true, ['sign']);
  }
  const secret = bytes.slice(-32);                        // seed is the trailing 32 bytes (v1 encoding)
  const raw    = await noble.getPublicKeyAsync(secret);
  return { __sw: true, kind: 'private', secret, raw };
}

// ── sign / verify ───────────────────────────────────────────────────
/** Sign `message` (Uint8Array) → 64-byte Ed25519 signature. */
export async function sign(privateKey, message) {
  if (isSwPriv(privateKey)) return noble.signAsync(message, privateKey.secret);
  const sig = await crypto.subtle.sign(ALGORITHM, privateKey, message);
  return new Uint8Array(sig);
}

/** Verify a 64-byte signature. `publicKey` may be a CryptoKey, a software
 *  handle, or 32 raw bytes. Cross-impl: a software-signed message verifies
 *  natively and vice versa. */
export async function verify(publicKey, message, signature) {
  if (isSwPub(publicKey)) return noble.verifyAsync(signature, message, publicKey.raw);
  if (publicKey instanceof Uint8Array) {
    if (!(await nativeEd25519Available())) return noble.verifyAsync(signature, message, publicKey);
    const key = await importPublicKey(publicKey);
    return verify(key, message, signature);
  }
  // native CryptoKey
  return crypto.subtle.verify(ALGORITHM, publicKey, signature, message);
}

/** Convenience signer for post.js makePost(..., { signer }). */
export function makeSigner(privateKey) {
  return (canonicalBytes) => sign(privateKey, canonicalBytes);
}

/** Convenience verifier for post.js verifySignature. */
export function makeVerifier() {
  return (publisherKey, msg, sig) => verify(publisherKey, msg, sig);
}
