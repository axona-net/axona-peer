// =====================================================================
// unpub.js — the signed "unpub" object: owner-only removal of a topic's
//            message queue (Phase A #3).
//
// Where `kill` retracts ONE message (authorized by the message's signer),
// `unpub` clears (or destroys) a whole topic — authorized by the topic
// OWNER.  The owner is the identity whose nodeId seeds the topic id:
//
//   topicId = [owner.s2Prefix(8 bits)] || sha256(owner.nodeId || ':' || name)
//
// A topic's root axons authorize an unpub self-authenticatingly, with no
// central registry, by checking BOTH:
//   (1) the signer's pubkey binds to the claimed ownerNodeId —
//       sha256(signerPubkey) === ownerNodeId[8:]  (the 256-bit suffix; the
//       8-bit geo prefix is the owner's own choice), and
//   (2) deriveTopicId(ownerNodeId, topicName) === topicId.
// Only the genuine owner satisfies both (see AxonaManager._handleUnpub).
//
// The signature covers a DOMAIN-TAGGED core (E-4); `ts`+`seq` give the
// unpub its own C-2 freshness/replay protection.
//
//   signedBytes = utf8( canonical({
//     d: UNPUB_DOMAIN, topicId, topicName, ownerNodeId, destroy, ts, seq }) )
//
// Public (ownerless) topics — topicId = '00'||sha256(name) — have no owner
// and can never be unpubbed: no ownerNodeId satisfies (2).
// =====================================================================

import { canonical }                        from './post.js';
import { sign, verify, importPublicKey }    from './ed25519.js';

/** Domain-separation tag folded into the signed unpub core (E-4). */
export const UNPUB_DOMAIN = 'axona:pubsub-unpub:v1';

const _enc = new TextEncoder();

function bytesToHex(bytes) {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}
function hexToBytes(hex) {
  if (hex.length % 2 !== 0) throw new Error('hexToBytes: odd-length hex');
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) out[i / 2] = parseInt(hex.substr(i, 2), 16);
  return out;
}

/**
 * Build (and sign) an unpub object.
 *
 * @param {object} opts
 * @param {string}  opts.topicId      66-char hex topic id
 * @param {string}  opts.topicName    the topic name (needed for the owner check)
 * @param {string}  opts.ownerNodeId  66-char hex owner nodeId (seeds topicId)
 * @param {boolean} [opts.destroy=false]  true → total removal (config/ACL too)
 * @param {number}  [opts.ts]         ms timestamp; defaults to Date.now()
 * @param {number}  [opts.seq=0]      per-publisher monotonic seq (freshness)
 * @param {object}  opts.identity     { privateKey (CryptoKey), pubkeyHex }
 * @returns {Promise<object>} signed unpub object
 */
export async function buildUnpub({ topicId, topicName, ownerNodeId, destroy = false, ts = Date.now(), seq = 0, identity }) {
  if (!identity || !identity.privateKey || typeof identity.pubkeyHex !== 'string') {
    throw new TypeError('buildUnpub: identity with privateKey + pubkeyHex required');
  }
  if (typeof topicId !== 'string' || typeof topicName !== 'string' || typeof ownerNodeId !== 'string') {
    throw new TypeError('buildUnpub: topicId, topicName and ownerNodeId must be strings');
  }
  const flag     = destroy === true;
  const bytes    = _enc.encode(canonical({ d: UNPUB_DOMAIN, topicId, topicName, ownerNodeId, destroy: flag, ts, seq }));
  const sigBytes = await sign(identity.privateKey, bytes);
  return {
    kind:         UNPUB_DOMAIN,
    topicId,
    topicName,
    ownerNodeId,
    destroy:      flag,
    ts,
    seq,
    signerPubkey: identity.pubkeyHex,
    signature:    'ed25519:' + bytesToHex(sigBytes),
  };
}

/**
 * Verify an unpub's signature against its `signerPubkey`.  Does NOT decide
 * ownership — the caller still checks pubkey↔ownerNodeId binding and that
 * the topicId derives from ownerNodeId (AxonaManager._handleUnpub).
 *
 * @param {object} unpub
 * @returns {Promise<{ok: boolean, reason?: string, signerPubkey?: string}>}
 */
export async function verifyUnpub(unpub) {
  if (!unpub || typeof unpub !== 'object')        return { ok: false, reason: 'not_an_object' };
  if (typeof unpub.topicId !== 'string')          return { ok: false, reason: 'missing_topicId' };
  if (typeof unpub.topicName !== 'string')        return { ok: false, reason: 'missing_topicName' };
  if (typeof unpub.ownerNodeId !== 'string')      return { ok: false, reason: 'missing_ownerNodeId' };
  if (typeof unpub.destroy !== 'boolean')         return { ok: false, reason: 'missing_destroy' };
  if (typeof unpub.ts !== 'number')               return { ok: false, reason: 'missing_ts' };
  if (typeof unpub.seq !== 'number')              return { ok: false, reason: 'missing_seq' };
  if (typeof unpub.signerPubkey !== 'string')     return { ok: false, reason: 'missing_signerPubkey' };
  if (typeof unpub.signature !== 'string' ||
      !unpub.signature.startsWith('ed25519:'))    return { ok: false, reason: 'unknown_signature_scheme' };

  const sigBytes = hexToBytes(unpub.signature.slice('ed25519:'.length));
  const pkBytes  = hexToBytes(unpub.signerPubkey);
  if (sigBytes.length !== 64 || pkBytes.length !== 32) {
    return { ok: false, reason: 'wrong_key_or_signature_length' };
  }
  const bytes  = _enc.encode(canonical({
    d: UNPUB_DOMAIN, topicId: unpub.topicId, topicName: unpub.topicName,
    ownerNodeId: unpub.ownerNodeId, destroy: unpub.destroy, ts: unpub.ts, seq: unpub.seq,
  }));
  const pubKey = await importPublicKey(pkBytes);
  const sigOk  = await verify(pubKey, bytes, sigBytes);
  if (!sigOk) return { ok: false, reason: 'bad_signature' };
  return { ok: true, signerPubkey: unpub.signerPubkey };
}
