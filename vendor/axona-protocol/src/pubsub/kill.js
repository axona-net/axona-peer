// =====================================================================
// kill.js — the signed "kill" object: creator-only message retraction
//           (Phase A #2).
//
// A kill names a single published message (topicId + msgId) and is signed
// by the SAME key that signed the original message.  A topic's root axons
// verify the kill's signature, then check `signerPubkey` matches the
// signer of the cached message — so only the original creator can retract
// it.  No central authority: the right to kill is proven by the same
// keypair that proved authorship.
//
// The signature covers a DOMAIN-TAGGED core (E-4 discipline — a kill
// signature can never be replayed as an envelope/post signature):
//
//   signedBytes = utf8( canonical({
//     d: KILL_DOMAIN, topicId, msgId, ts, seq }) )
//   signature   = 'ed25519:' + hex( ed25519.sign(privkey, signedBytes) )
//
// `ts` + `seq` give the kill its own freshness / replay protection (the
// same C-2 treatment publishes get), so a captured kill can't be re-injected
// later.  `signerPubkey` is the 64-char-hex Ed25519 public key.
//
// Anonymous (unsigned) messages have no provable creator, so they cannot be
// killed — there is no signerPubkey to match.  That's correct: nobody can
// prove the right to retract a message nobody signed.
// =====================================================================

import { canonical }                        from './post.js';
import { sign, verify, importPublicKey }    from './ed25519.js';

/** Domain-separation tag folded into the signed kill core (E-4). */
export const KILL_DOMAIN = 'axona:pubsub-kill:v1';

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
 * Build (and sign) a kill object.  Requires an identity (kills are always
 * signed — an unsigned kill carries no proof of the right to retract).
 *
 * @param {object} opts
 * @param {string} opts.topicId   66-char hex topic ID
 * @param {string} opts.msgId     64-char hex message ID to retract
 * @param {number} [opts.ts]      ms timestamp; defaults to Date.now()
 * @param {number} [opts.seq=0]   per-publisher monotonic seq (freshness)
 * @param {object} opts.identity  { privateKey (CryptoKey), pubkeyHex }
 * @returns {Promise<object>} signed kill object
 */
export async function buildKill({ topicId, msgId, ts = Date.now(), seq = 0, identity }) {
  if (!identity || !identity.privateKey || typeof identity.pubkeyHex !== 'string') {
    throw new TypeError('buildKill: identity with privateKey + pubkeyHex required');
  }
  if (typeof topicId !== 'string' || typeof msgId !== 'string') {
    throw new TypeError('buildKill: topicId and msgId must be strings');
  }
  const bytes    = _enc.encode(canonical({ d: KILL_DOMAIN, topicId, msgId, ts, seq }));
  const sigBytes = await sign(identity.privateKey, bytes);
  return {
    kind:         KILL_DOMAIN,
    topicId,
    msgId,
    ts,
    seq,
    signerPubkey: identity.pubkeyHex,
    signature:    'ed25519:' + bytesToHex(sigBytes),
  };
}

/**
 * Verify a kill object's signature against its `signerPubkey`.  Returns
 * `{ ok, reason?, signerPubkey? }`.  Does NOT decide authorization — the
 * caller still has to match `signerPubkey` against the killed message's
 * signer (that's the creator-only check, done at the root axon).
 *
 * @param {object} kill
 * @returns {Promise<{ok: boolean, reason?: string, signerPubkey?: string}>}
 */
export async function verifyKill(kill) {
  if (!kill || typeof kill !== 'object')            return { ok: false, reason: 'not_an_object' };
  if (typeof kill.topicId !== 'string')             return { ok: false, reason: 'missing_topicId' };
  if (typeof kill.msgId !== 'string')               return { ok: false, reason: 'missing_msgId' };
  if (typeof kill.ts !== 'number')                  return { ok: false, reason: 'missing_ts' };
  if (typeof kill.seq !== 'number')                 return { ok: false, reason: 'missing_seq' };
  if (typeof kill.signerPubkey !== 'string')        return { ok: false, reason: 'missing_signerPubkey' };
  if (typeof kill.signature !== 'string' ||
      !kill.signature.startsWith('ed25519:'))       return { ok: false, reason: 'unknown_signature_scheme' };

  const sigBytes = hexToBytes(kill.signature.slice('ed25519:'.length));
  const pkBytes  = hexToBytes(kill.signerPubkey);
  if (sigBytes.length !== 64 || pkBytes.length !== 32) {
    return { ok: false, reason: 'wrong_key_or_signature_length' };
  }
  const bytes  = _enc.encode(canonical({
    d: KILL_DOMAIN, topicId: kill.topicId, msgId: kill.msgId, ts: kill.ts, seq: kill.seq,
  }));
  const pubKey = await importPublicKey(pkBytes);
  const sigOk  = await verify(pubKey, bytes, sigBytes);
  if (!sigOk) return { ok: false, reason: 'bad_signature' };
  return { ok: true, signerPubkey: kill.signerPubkey };
}
