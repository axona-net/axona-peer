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
 * Security (finding H4): the private key can be made **non-extractable**
 * so it cannot be exported (exfiltrated) by later XSS or a malicious
 * dependency in the same context.  Web Crypto's `generateKey` sets
 * extractability for *both* halves at once, and the public key must be
 * extractable (we export it to raw bytes for the nodeId / wire), so when
 * `extractable: false` we generate extractable, export the public half,
 * then re-import the private half as non-extractable — leaving a private
 * key handle that signs but never exports.
 *
 * `extractable` defaults to `true` for backward compatibility: callers
 * that persist the identity (`dumpIdentity` → PKCS#8) require it.  An
 * ephemeral / browser identity that is never persisted should pass
 * `{ extractable: false }` — the genuinely XSS-exposed surface.
 *
 * @param {object} [opts]
 * @param {boolean} [opts.extractable=true]  Whether the PRIVATE key may
 *        be exported.  The public key is always extractable.
 * @returns {Promise<{publicKey: CryptoKey, privateKey: CryptoKey}>}
 */
export async function generateKeyPair({ extractable = true } = {}) {
  const pair = await crypto.subtle.generateKey(ALGORITHM, true, ['sign', 'verify']);
  if (extractable) return pair;

  // Re-import the private key as non-extractable.  The public key keeps
  // its extractable handle so exportPublicKey() still works.
  const pkcs8 = await crypto.subtle.exportKey('pkcs8', pair.privateKey);
  const privateKey = await crypto.subtle.importKey('pkcs8', pkcs8, ALGORITHM, false, ['sign']);
  return { publicKey: pair.publicKey, privateKey };
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
