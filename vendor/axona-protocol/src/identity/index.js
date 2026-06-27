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
import { AUTHOR_ID_BITS, AUTHOR_HEX_CHARS, getKeyspace } from '../utils/hexid.js';
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
export async function createNodeIdentity({ lat, lng, extractable = true, fast = false }) {
  if (typeof lat !== 'number' || typeof lng !== 'number') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'createNodeIdentity: region must be { lat: number, lng: number }');
  }

  // ── Fast (SIM-ONLY) path: skip the Ed25519 keygen ──────────────────────────
  // A node identity is never signature-verified by the protocol (the sim
  // transport doesn't sign; web-mesh DTLS-fingerprint binding is a browser-only
  // concern), so for large in-simulator builds we can mint a routable identity
  // from random bytes — same nodeId shape (region byte ‖ truncated SHA-256), a
  // synthetic pubkey, and NO private key — at a fraction of the cost (keygen,
  // not hashing, is the bottleneck at 50k nodes). Guarded to the shrunk,
  // NON-PRODUCTION keyspace so a fast (unverifiable) identity can never ship:
  // production (264-bit default) refuses it.
  if (fast) {
    if (getKeyspace().isProductionDefault) {
      throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
        'createNodeIdentity({ fast }) is sim-only and refused on the production 264-bit keyspace — ' +
        'shrink the keyspace with configureKeyspace() first');
    }
    const rand = new Uint8Array(32);
    crypto.getRandomValues(rand);
    const id = await computeNodeId(rand, lat, lng);   // region byte ‖ truncated SHA-256(rand)
    const identity = buildIdentity({
      id, pubkey: rand, privateKey: null, region: { lat, lng }, createdAt: Date.now(),
    });
    identity.fast = true;   // marker: no real keypair; never persist or sign with this
    return identity;
  }

  let pair;
  try {
    pair = await generateKeyPair({ extractable });
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_KEYGEN_FAILED,
      `createNodeIdentity: Web Crypto Ed25519 generateKey failed (${cause.message})`,
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
 * Create an AUTHOR identity — the keypair that signs publishes (its public key
 * is the Author ID, i.e. `signerPubkey`). Unlike a node identity it has **no
 * location and no nodeId**: authorship is not a place (design D3). Only the
 * keypair matters.
 *
 * Durability is the only real choice (design D5): mint a fresh one (ephemeral,
 * unlinkable) or persist it (a recognizable author across sessions, able to
 * retract). Persistence is an *option here*, not a separate function:
 *   createAuthorIdentity()                         → ephemeral
 *   createAuthorIdentity({ persistAs: 'me' })       → durable (browser localStorage)
 *   createAuthorIdentity({ persistAs: 'me', store }) → durable (custom {get,set})
 *
 * @param {object}   [opts]
 * @param {string}   [opts.persistAs]    storage key; load-or-create + save.
 * @param {{get,set}}[opts.store]        custom store (else browser localStorage).
 * @param {boolean}  [opts.extractable]  forced true when persistAs is set.
 * @returns {Promise<AuthorIdentity>} { authorId, pubkey, pubkeyHex, privateKey, sign, verify }
 */
export async function createAuthorIdentity({ persistAs = null, store = null, extractable = true } = {}) {
  const st = persistAs ? (store || defaultAuthorStore()) : null;
  if (persistAs && st) {
    try {
      const saved = await st.get(persistAs);
      if (saved) return await loadAuthorIdentity(typeof saved === 'string' ? JSON.parse(saved) : saved);
    } catch { /* corrupt / absent → mint fresh below */ }
  }
  // A persisted key must be exportable; an ephemeral one defaults non-persistable-safe.
  const id = await mintAuthorIdentity({ extractable: persistAs ? true : extractable });
  if (persistAs && st) {
    try { await st.set(persistAs, JSON.stringify(await dumpAuthorIdentity(id))); } catch { /* best-effort */ }
  }
  return id;
}

async function mintAuthorIdentity({ extractable }) {
  let pair;
  try {
    pair = await generateKeyPair({ extractable });
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_KEYGEN_FAILED,
      `createAuthorIdentity: Ed25519 generateKey failed (${cause.message})`, { cause });
  }
  const pubkey = await exportPublicKey(pair.publicKey);
  return buildAuthorIdentity({ pubkey, privateKey: pair.privateKey, createdAt: Date.now() });
}

/** Author identities have a keypair only — no nodeId, no region. */
function buildAuthorIdentity({ pubkey, privateKey, createdAt }) {
  const pubkeyHex = bytesToHex(pubkey);
  // The Author ID is the raw 256-bit pubkey in production (== signerPubkey, the
  // self-authenticating credential). Under a shrunk sim keyspace profile it is the
  // pubkey TRUNCATED to the author width (e.g. 64-bit / 16-hex); the sim relaxes
  // signature verification (decision B), so the id need not be the full key.
  const authorId  = (AUTHOR_ID_BITS === 256) ? pubkeyHex : pubkeyHex.slice(0, AUTHOR_HEX_CHARS);
  return {
    kind:      'author',
    authorId,                        // the public Author ID (== signerPubkey on the wire)
    pubkey,
    pubkeyHex,
    privateKey,
    createdAt,
    pow:       '',                   // publish-role PoW nonce (minted by the publish path; '' = inert)
    sign:   (message)            => sign(privateKey, message),
    verify: (message, signature) => verify(pubkey, message, signature),
  };
}

/** Author persistence envelope — keypair only (no id/region). */
async function dumpAuthorIdentity(identity) {
  let pkcs8;
  try {
    pkcs8 = await exportPrivateKeyPkcs8(identity.privateKey);
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_LOAD_FAILED,
      `createAuthorIdentity: privateKey export failed (${cause.message})`, { cause });
  }
  return { kind: 'author', pubkey: identity.pubkeyHex, privkey: bytesToBase64(new Uint8Array(pkcs8)), createdAt: identity.createdAt };
}

async function loadAuthorIdentity(env) {
  if (!env || typeof env.pubkey !== 'string' || env.pubkey.length !== 64 || typeof env.privkey !== 'string') {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT, 'loadAuthorIdentity: bad envelope');
  }
  const pubkeyBytes = hexToBytes(env.pubkey);
  let privateKey;
  try {
    privateKey = await importPrivateKey(bytesToBase64.decode(env.privkey));
  } catch (cause) {
    throw new IdentityError(ErrorCodes.IDENTITY_LOAD_FAILED,
      `loadAuthorIdentity: privateKey import failed (${cause.message})`, { cause });
  }
  // Same private↔public correspondence probe as loadIdentity (M5).
  const probe = new TextEncoder().encode('axona-identity-keypair-probe');
  if (!(await verify(pubkeyBytes, probe, await sign(privateKey, probe)))) {
    throw new IdentityError(ErrorCodes.IDENTITY_INVALID_FORMAT,
      'loadAuthorIdentity: private key does not correspond to the stored public key');
  }
  return buildAuthorIdentity({ pubkey: pubkeyBytes, privateKey, createdAt: typeof env.createdAt === 'number' ? env.createdAt : Date.now() });
}

/** Browser localStorage as a {get,set} store, or null when unavailable. */
function defaultAuthorStore() {
  try {
    if (typeof localStorage !== 'undefined') {
      return { get: (k) => localStorage.getItem(k), set: (k, v) => localStorage.setItem(k, v) };
    }
  } catch { /* sandboxed / denied */ }
  return null;
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
    pow: '',                          // Stage 2: transport PoW nonce (createNodeIdentity/loadIdentity overwrite; '' = inert)
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
