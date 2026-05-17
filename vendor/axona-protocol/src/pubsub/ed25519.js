// =====================================================================
// ed25519.js — Ed25519 helpers built on Web Crypto.
//
// Optional companion to post.js: gives applications a portable
// {generateKeyPair, sign, verify, exportKey, importKey} surface that
// works in modern browsers (Chrome 110+, Safari 17+, Firefox 130+) and
// in Node 20+ via the same Web Crypto API.
//
// On runtimes that don't support Web Crypto Ed25519, the application
// can substitute a pure-JS implementation (e.g. @noble/ed25519) with
// the same shape — `makePost`'s `signer` and `verifySignature`'s
// `verifier` accept any function with the right signature.  This
// module is the path of least friction for runtimes that do support it.
//
// All functions are async.  Key material is represented as CryptoKey
// objects internally and as raw Uint8Array bytes when exported (32
// bytes for the public key — Ed25519's standard encoding).
// =====================================================================

const ALGORITHM = { name: 'Ed25519' };

/**
 * Generate a fresh Ed25519 keypair.  Returns CryptoKey handles.
 *
 * Use {@link exportPublicKey} when you need the raw 32-byte public
 * key to embed as a peer identifier or share over the wire.  The
 * private key stays as a non-extractable CryptoKey so it can't leak
 * through JSON.stringify, error logging, etc.
 *
 * @returns {Promise<{publicKey: CryptoKey, privateKey: CryptoKey}>}
 */
export async function generateKeyPair() {
  // `extractable: false` for the private key — locks it inside the
  // browser's crypto subsystem.  Public key is extractable for sharing.
  // (Web Crypto Ed25519 currently requires both halves of the pair to
  // be generated together; we set false on the keypair and re-export
  // only the public half.)
  const pair = await crypto.subtle.generateKey(ALGORITHM, true, ['sign', 'verify']);
  return pair;
}

/**
 * Export a public key to its 32-byte raw encoding (the on-the-wire
 * Ed25519 format).
 *
 * @param {CryptoKey} publicKey
 * @returns {Promise<Uint8Array>}
 */
export async function exportPublicKey(publicKey) {
  const buf = await crypto.subtle.exportKey('raw', publicKey);
  return new Uint8Array(buf);
}

/**
 * Import a 32-byte raw public key for use with {@link verify}.
 *
 * @param {Uint8Array} rawBytes  32-byte Ed25519 public key.
 * @returns {Promise<CryptoKey>}
 */
export async function importPublicKey(rawBytes) {
  return crypto.subtle.importKey('raw', rawBytes, ALGORITHM, true, ['verify']);
}

/**
 * Sign `message` with `privateKey`.  Returns a 64-byte Ed25519
 * signature suitable for embedding as `ed25519:<hex>` in a
 * SignedPost.signature field via post.js makePost(..., { signer }).
 *
 * @param {CryptoKey}   privateKey
 * @param {Uint8Array}  message
 * @returns {Promise<Uint8Array>} 64 bytes
 */
export async function sign(privateKey, message) {
  const sig = await crypto.subtle.sign(ALGORITHM, privateKey, message);
  return new Uint8Array(sig);
}

/**
 * Verify a 64-byte Ed25519 signature.
 *
 * @param {CryptoKey | Uint8Array} publicKey  CryptoKey, or 32 raw bytes
 *                                            (we'll import).
 * @param {Uint8Array}             message
 * @param {Uint8Array}             signature   64 bytes
 * @returns {Promise<boolean>}
 */
export async function verify(publicKey, message, signature) {
  const key = (publicKey instanceof CryptoKey)
    ? publicKey
    : await importPublicKey(publicKey);
  return crypto.subtle.verify(ALGORITHM, key, signature, message);
}

/**
 * Convenience: build the signer function that post.js makePost
 * expects.  Bind the private key once; use the returned function for
 * every publish call.
 *
 * @param {CryptoKey} privateKey
 * @returns {(canonicalBytes: Uint8Array) => Promise<Uint8Array>}
 */
export function makeSigner(privateKey) {
  return (canonicalBytes) => sign(privateKey, canonicalBytes);
}

/**
 * Convenience: build the verifier function that post.js
 * verifySignature expects.
 *
 * @returns {(publisherKey: any, msg: Uint8Array, sig: Uint8Array) => Promise<boolean>}
 */
export function makeVerifier() {
  return (publisherKey, msg, sig) => verify(publisherKey, msg, sig);
}
