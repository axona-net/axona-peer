// =====================================================================
// touch.js — the signed "touch" object: keep-alive, gated by TOPIC OWNERSHIP
//            (Phase A #7).
//
// A touch names a single published message (topicId + msgId) and is always
// signed (for freshness / replay protection).  A topic's root axons verify
// the signature, then authorize by **topic ownership** (see
// AxonaManager._handleTouch):
//   · OPEN topic (ownerless — a public topic, or a synthetic regional anchor
//     `prefix‖0^256`): ANYONE may touch — any valid, fresh, signed touch is
//     accepted.
//   · OWNED topic (anchored at a real identity): only the OWNER may — the
//     touch signer's pubkey must hash to the owner anchor's 256-bit suffix
//     (the same pubkey↔nodeId bind `unpub` uses; the 8-bit geo prefix is the
//     owner's own choice, so only the suffix is checked).
// No central authority: the right to keep a message alive is proven by the
// owning keypair (owned topics) or open to all (ownerless topics).
//
// Applying a touch on a root that holds the message:
//   · resets the message's hold-time expiry to `now + hold`, BOUNDED by its
//     absolute 48h ceiling (a touch can extend life but never past the cap a
//     pull also respects);
//   · moves the entry to the HEAD of the replay queue and bumps a recency
//     (`touchedTs`) that dominates eviction ordering, so a touched message is
//     evicted LAST.
//
// The signature covers a DOMAIN-TAGGED core (E-4 discipline — a touch
// signature can never be replayed as an envelope/kill signature):
//
//   signedBytes = utf8( canonical({
//     d: TOUCH_DOMAIN, topicId, msgId, ts, seq }) )
//   signature   = 'ed25519:' + hex( ed25519.sign(privkey, signedBytes) )
//
// `ts` + `seq` give the touch its own freshness / replay protection (the same
// C-2 treatment publishes and kills get), so a captured touch can't be
// re-injected later, and `ts` is the recency stamped onto the entry.
// `signerPubkey` is the 64-char-hex Ed25519 public key.
//
// Authorization is by topic, not message authorship: a message published
// anonymously to an OPEN topic is still touchable (anyone may), while on an
// OWNED topic only the owner may touch — regardless of who authored the
// individual message.
// =====================================================================

import { canonical }                        from './post.js';
import { sign, verify, importPublicKey }    from './ed25519.js';

/** Domain-separation tag folded into the signed touch core (E-4). */
export const TOUCH_DOMAIN = 'axona:pubsub-touch:v1';

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
 * Build (and sign) a touch object.  Requires an identity (touches are always
 * signed — an unsigned touch carries no proof of the right to keep alive).
 *
 * @param {object} opts
 * @param {string} opts.topicId   66-char hex topic ID
 * @param {string} opts.msgId     64-char hex message ID to refresh
 * @param {number} [opts.ts]      ms timestamp; defaults to Date.now()
 * @param {number} [opts.seq=0]   per-publisher monotonic seq (freshness)
 * @param {object} opts.identity  { privateKey (CryptoKey), pubkeyHex }
 * @returns {Promise<object>} signed touch object
 */
export async function buildTouch({ topicId, msgId, ts = Date.now(), seq = 0, identity }) {
  if (!identity || !identity.privateKey || typeof identity.pubkeyHex !== 'string') {
    throw new TypeError('buildTouch: identity with privateKey + pubkeyHex required');
  }
  if (typeof topicId !== 'string' || typeof msgId !== 'string') {
    throw new TypeError('buildTouch: topicId and msgId must be strings');
  }
  const bytes    = _enc.encode(canonical({ d: TOUCH_DOMAIN, topicId, msgId, ts, seq }));
  const sigBytes = await sign(identity.privateKey, bytes);
  return {
    kind:         TOUCH_DOMAIN,
    topicId,
    msgId,
    ts,
    seq,
    signerPubkey: identity.pubkeyHex,
    signature:    'ed25519:' + bytesToHex(sigBytes),
  };
}

/**
 * Verify a touch object's signature against its `signerPubkey`.  Returns
 * `{ ok, reason?, signerPubkey? }`.  Does NOT decide authorization — the
 * caller still matches `signerPubkey` against the touched message's signer
 * (the creator-only check, done at the root axon).
 *
 * @param {object} touch
 * @returns {Promise<{ok: boolean, reason?: string, signerPubkey?: string}>}
 */
export async function verifyTouch(touch) {
  if (!touch || typeof touch !== 'object')           return { ok: false, reason: 'not_an_object' };
  if (typeof touch.topicId !== 'string')             return { ok: false, reason: 'missing_topicId' };
  if (typeof touch.msgId !== 'string')               return { ok: false, reason: 'missing_msgId' };
  if (typeof touch.ts !== 'number')                  return { ok: false, reason: 'missing_ts' };
  if (typeof touch.seq !== 'number')                 return { ok: false, reason: 'missing_seq' };
  if (typeof touch.signerPubkey !== 'string')        return { ok: false, reason: 'missing_signerPubkey' };
  if (typeof touch.signature !== 'string' ||
      !touch.signature.startsWith('ed25519:'))       return { ok: false, reason: 'unknown_signature_scheme' };

  const sigBytes = hexToBytes(touch.signature.slice('ed25519:'.length));
  const pkBytes  = hexToBytes(touch.signerPubkey);
  if (sigBytes.length !== 64 || pkBytes.length !== 32) {
    return { ok: false, reason: 'wrong_key_or_signature_length' };
  }
  const bytes  = _enc.encode(canonical({
    d: TOUCH_DOMAIN, topicId: touch.topicId, msgId: touch.msgId, ts: touch.ts, seq: touch.seq,
  }));
  const pubKey = await importPublicKey(pkBytes);
  const sigOk  = await verify(pubKey, bytes, sigBytes);
  if (!sigOk) return { ok: false, reason: 'bad_signature' };
  return { ok: true, signerPubkey: touch.signerPubkey };
}
