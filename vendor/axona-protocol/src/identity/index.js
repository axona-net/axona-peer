// =====================================================================
// identity/index.js — Axona node identity: keypair + nodeId + region.
//
// An identity binds a peer to:
//   - an Ed25519 keypair (signs every publish, verifies every receive)
//   - a 264-bit nodeId derived from the public key and an S2-prefix
//     anchor for the peer's geographic region
//   - the geographic region itself (lat, lng), so the nodeId can be
//     recomputed and verified by anyone
//
// Identity is persistent.  Apps store the envelope (id + pubkey +
// privkey + region) via PersistenceAdapter, and load it again on
// startup so the same nodeId survives reloads, restarts, and process
// migration.
//
// Sign / verify helpers live in src/pubsub/ed25519.js; identity
// re-exports the most-used surface (sign, verify, exportPublicKey)
// for convenience.
// =====================================================================

import {
  generateKeyPair,
  exportPublicKey,
  exportPrivateKeyPkcs8,
  importPrivateKey,
  sign,
  verify,
}                                       from '../pubsub/ed25519.js';
import { computeNodeId }                from './nodeid.js';
import { IdentityError, ErrorCodes }    from '../errors.js';
import { powMint, powVerify }           from '../pow/pow.js';

const ALGORITHM = { name: 'Ed25519' };

/**
 * A constructed Identity. NOT JSON-serializable directly — call
 * `dumpIdentity()` to get a persistence envelope.
 *
 * @typedef {object} Identity
 * @property {string}     id          66-char hex nodeId.
 * @property {Uint8Array} pubkey      32 raw bytes (Ed25519 public key).
 * @property {string}     pubkeyHex   64-char hex of pubkey (convenience).
 * @property {CryptoKey}  privateKey  Web Crypto signing key.
 * @property {{lat: number, lng: number}} region
 * @property {number}     createdAt   ms since epoch.
 * @property {(message: Uint8Array) => Promise<Uint8Array>} sign
 *           Sign with this identity's private key.
 * @property {(message: Uint8Array, signature: Uint8Array) => Promise<boolean>} verify
 *           Verify a signature against this identity's public key.
 */

/**
 * A persistence envelope — what apps store / load.  All fields are
 * JSON-serializable strings or numbers.
 *
 * @typedef {object} IdentityEnvelope
 * @property {string} id          66-char hex nodeId.
 * @property {string} pubkey      64-char hex (32 raw bytes).
 * @property {string} privkey     base64 PKCS#8 encoding of the private key.
 * @property {{lat: number, lng: number}} region
 * @property {number} createdAt
 */

/**
 * Create a fresh identity: generate a new Ed25519 keypair and derive
 * the 264-bit nodeId from the public key + region.
 *
 * @param {object} opts
 * @param {number} opts.lat
 * @param {number} opts.lng
 * @param {boolean} [opts.extractable=true]  Whether the private key may be
 *        exported.  Defaults to `true` because `dumpIdentity` (persistence)
 *        needs it.  An ephemeral / browser identity that is never persisted
 *        should pass `false` so XSS can't exfiltrate the signing key (H4).
 * @returns {Promise<Identity>}
 */
export async function deriveIdentity({ lat, lng, extractable = true }) {
  if (typeof lat !== 'number' || typeof lng !== 'number') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'deriveIdentity: region must be { lat: number, lng: number }');
  }

  let pair;
  try {
    pair = await generateKeyPair({ extractable });
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_KEYGEN_FAILED,
      `deriveIdentity: Web Crypto Ed25519 generateKey failed (${cause.message})`,
      { cause });
  }

  const pubkey  = await exportPublicKey(pair.publicKey);
  const id      = await computeNodeId(pubkey, lat, lng);

  const identity = buildIdentity({
    id,
    pubkey,
    privateKey: pair.privateKey,
    region:     { lat, lng },
    createdAt:  Date.now(),
  });
  // Stage 2: mint the transport PoW (inert at difficulty 0 ⇒ ''). Presented in
  // the auth hello; raising difficulty later needs no identity-format change.
  identity.pow = await powMint({ pubkeyHex: identity.pubkeyHex, role: 'transport' });
  return identity;
}

/**
 * Dump an identity to its persistence envelope.  Exports the private
 * key as PKCS#8 (base64-encoded) — works in both browser and Node
 * Web Crypto.  Loses the in-memory CryptoKey handle; reconstruct via
 * loadIdentity().
 *
 * @param {Identity} identity
 * @returns {Promise<IdentityEnvelope>}
 */
export async function dumpIdentity(identity) {
  let pkcs8;
  try {
    pkcs8 = await exportPrivateKeyPkcs8(identity.privateKey);   // native or software key
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_LOAD_FAILED,
      `dumpIdentity: privateKey export failed (${cause.message})`,
      { cause });
  }
  return {
    id:        identity.id,
    pubkey:    identity.pubkeyHex,
    privkey:   bytesToBase64(new Uint8Array(pkcs8)),
    region:    { ...identity.region },
    createdAt: identity.createdAt,
    pow:       typeof identity.pow === 'string' ? identity.pow : '',   // Stage 2: persist the transport PoW nonce
  };
}

/**
 * Reconstruct an Identity from a persistence envelope.  Verifies that
 * the stored nodeId matches the freshly-derived one (catches corruption
 * or mismatched pubkey/region pairs).
 *
 * @param {IdentityEnvelope} envelope
 * @returns {Promise<Identity>}
 */
export async function loadIdentity(envelope) {
  if (!envelope || typeof envelope !== 'object') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'loadIdentity: envelope must be an object');
  }
  const { id, pubkey, privkey, region, createdAt } = envelope;
  if (typeof id !== 'string' || id.length !== 66) {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      `loadIdentity: id must be 66-char hex, got ${typeof id} length ${id?.length}`);
  }
  if (typeof pubkey !== 'string' || pubkey.length !== 64) {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      `loadIdentity: pubkey must be 64-char hex, got length ${pubkey?.length}`);
  }
  if (typeof privkey !== 'string') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'loadIdentity: privkey must be base64 string');
  }
  if (!region || typeof region.lat !== 'number' || typeof region.lng !== 'number') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'loadIdentity: region must be { lat, lng }');
  }

  const pubkeyBytes = hexToBytes(pubkey);
  let privateKey;
  try {
    privateKey = await importPrivateKey(bytesToBase64.decode(privkey));   // native or software key
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_LOAD_FAILED,
      `loadIdentity: privateKey import failed (${cause.message})`,
      { cause });
  }

  // Verify the stored id is internally consistent.
  const expected = await computeNodeId(pubkeyBytes, region.lat, region.lng);
  if (expected !== id) {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      `loadIdentity: stored id ${id} does not match derived id ${expected}`);
  }

  // M5: verify the private key actually corresponds to the public key.
  // The nodeId check above only proves pubkey↔region↔id consistency; a
  // corrupted or mismatched `privkey` blob would otherwise load cleanly
  // and then silently produce signatures that no one can verify.  A
  // sign→verify round-trip over a fixed probe catches it at load time.
  try {
    const probe = new TextEncoder().encode('axona-identity-keypair-probe');
    const probeSig = await sign(privateKey, probe);
    const matches  = await verify(pubkeyBytes, probe, probeSig);
    if (!matches) {
      throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
        'loadIdentity: private key does not correspond to the stored public key');
    }
  } catch (cause) {
    if (cause instanceof IdentityError) throw cause;
    throw new IdentityError(ErrorCodes.IDENTITY_LOAD_FAILED,
      `loadIdentity: private/public key correspondence check failed (${cause.message})`,
      { cause });
  }

  const identity = buildIdentity({
    id,
    pubkey: pubkeyBytes,
    privateKey,
    region: { lat: region.lat, lng: region.lng },
    createdAt: typeof createdAt === 'number' ? createdAt : Date.now(),
  });
  // Stage 2: reuse the PERSISTED transport PoW nonce if it still satisfies the
  // current difficulty — avoids re-solving the puzzle on every load once
  // difficulty > 0; re-mint only if absent or now-insufficient. At difficulty 0
  // the persisted '' (or absent) verifies trivially, so this is a no-op.
  const storedPow = typeof envelope.pow === 'string' ? envelope.pow : '';
  identity.pow = (await powVerify({ pubkeyHex: identity.pubkeyHex, nonce: storedPow, role: 'transport' }))
    ? storedPow
    : await powMint({ pubkeyHex: identity.pubkeyHex, role: 'transport' });
  return identity;
}

// ── internal: shared Identity constructor ────────────────────────────

function buildIdentity({ id, pubkey, privateKey, region, createdAt }) {
  const pubkeyHex = bytesToHex(pubkey);
  return {
    id,
    pubkey,
    pubkeyHex,
    privateKey,
    region,
    createdAt,
    pow: '',                          // Stage 2: transport PoW nonce (deriveIdentity/loadIdentity overwrite; '' = inert)
    sign: (message) => sign(privateKey, message),
    // Verify against this identity's own public key — used for
    // round-trip sanity checks. To verify a different signer's
    // signature, import their pubkey via importPublicKey and call
    // verify() from ed25519.js directly.
    verify: async (message, signature) => verify(pubkey, message, signature),
  };
}

// ── re-exports for convenience ───────────────────────────────────────

export { computeNodeId, computeNodeIdBigInt }
  from './nodeid.js';

export {
  exportPublicKey,
  importPublicKey,
  sign,
  verify,
  generateKeyPair,
  makeSigner,
  makeVerifier,
}                              from '../pubsub/ed25519.js';

// ── internal: hex / base64 codecs ────────────────────────────────────

function bytesToHex(bytes) {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}

function hexToBytes(hex) {
  if (hex.length % 2 !== 0) {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      `hexToBytes: odd length ${hex.length}`);
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return bytes;
}

function bytesToBase64(bytes) {
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}
bytesToBase64.decode = function base64ToBytes(b64) {
  const binary = atob(b64);
  const bytes  = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
};
