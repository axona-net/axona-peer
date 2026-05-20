// =====================================================================
// envelope.js — Axona pub/sub envelope: build, sign, verify, hash.
//
// Every message that flows through `peer.pub()` carries an envelope:
//
//     {
//       msgId:        '<64-char hex>',     // content-derived
//       ts:           1716210000000,
//       topic:        'cats',
//       message:      <any JSON-serializable>,
//       signature?:   'ed25519:<128-hex>',  // present when signed
//       signerPubkey?:'<64-char hex>',      // present when signed
//     }
//
// `msgId` is content-derived so any receiver can verify a message
// matches a claimed identity without trusting the sender:
//
//   When signed:
//     msgId = sha256( canonicalize({ signature, ts, topic, message }) )
//   When unsigned:
//     msgId = sha256( canonicalize({ ts, topic, message }) )
//
// The signature itself covers the unsigned core only:
//
//     signedBytes = utf8( canonicalize({ ts, topic, message }) )
//     signature   = 'ed25519:' + hex( ed25519.sign(privkey, signedBytes) )
//
// Receivers verify the signature against signerPubkey and recompute
// msgId — any tamper to ts/topic/message breaks the signature AND the
// hash; any tamper to signature breaks the msgId.  pull(msgId) (A3)
// therefore returns an envelope that the caller can independently
// authenticate.
//
// `signerPubkey` is the raw 32-byte Ed25519 public key as 64-char hex
// — the same encoding `identity.pubkeyHex` uses.
// =====================================================================

import { canonical, sha256Hex }              from './post.js';
import { sign, verify, importPublicKey }     from './ed25519.js';

const _enc = new TextEncoder();

/**
 * Compute the content-derived msgId for an envelope.
 *
 * @param {object} core  { ts, topic, message } plus optional `signature`
 * @returns {Promise<string>} 64-char hex sha256
 */
export async function computeMsgId(core) {
  return sha256Hex(canonical(core));
}

/**
 * Build (and optionally sign) an envelope for a pub call.
 *
 * @param {object} opts
 * @param {string} opts.topic
 * @param {*}      opts.message
 * @param {number} [opts.ts]      ms timestamp; defaults to Date.now()
 * @param {object} [opts.identity]
 *        Identity with `.privateKey` (CryptoKey) + `.pubkeyHex` (64-char hex).
 *        Required when `sign: true` (the default).
 * @param {boolean} [opts.sign=true]
 * @returns {Promise<object>} envelope
 */
export async function buildEnvelope({ topic, message, ts = Date.now(), identity, sign: doSign = true }) {
  if (doSign && (!identity || !identity.privateKey || typeof identity.pubkeyHex !== 'string')) {
    throw new TypeError('buildEnvelope: identity with privateKey + pubkeyHex required when sign=true');
  }

  const core = { ts, topic, message };

  let signature = undefined;
  let signerPubkey = undefined;
  if (doSign) {
    const bytes = _enc.encode(canonical(core));
    const sigBytes = await sign(identity.privateKey, bytes);
    signature    = 'ed25519:' + bytesToHex(sigBytes);
    signerPubkey = identity.pubkeyHex;
  }

  const msgId = signature
    ? await computeMsgId({ signature, ts, topic, message })
    : await computeMsgId({          ts, topic, message });

  const envelope = { msgId, ts, topic, message };
  if (signature) {
    envelope.signature    = signature;
    envelope.signerPubkey = signerPubkey;
  }
  return envelope;
}

/**
 * Verify an envelope: recompute the msgId from the core (+ signature)
 * and (if signed) check the Ed25519 signature against signerPubkey.
 * Returns a result object instead of throwing so callers can
 * distinguish "missing field" from "bad signature" cleanly.
 *
 * @param {object} envelope
 * @returns {Promise<{ok: boolean, reason?: string, signed: boolean}>}
 */
export async function verifyEnvelope(envelope) {
  if (!envelope || typeof envelope !== 'object') {
    return { ok: false, reason: 'not_an_object', signed: false };
  }
  if (typeof envelope.msgId !== 'string') {
    return { ok: false, reason: 'missing_msgId', signed: false };
  }
  if (typeof envelope.ts !== 'number') {
    return { ok: false, reason: 'missing_ts', signed: false };
  }
  if (typeof envelope.topic !== 'string') {
    return { ok: false, reason: 'missing_topic', signed: false };
  }
  if (!('message' in envelope)) {
    return { ok: false, reason: 'missing_message', signed: false };
  }

  const signed = typeof envelope.signature === 'string';
  if (signed) {
    if (typeof envelope.signerPubkey !== 'string') {
      return { ok: false, reason: 'missing_signerPubkey', signed: true };
    }
    if (!envelope.signature.startsWith('ed25519:')) {
      return { ok: false, reason: 'unknown_signature_scheme', signed: true };
    }
    const sigBytes = hexToBytes(envelope.signature.slice('ed25519:'.length));
    const pkBytes  = hexToBytes(envelope.signerPubkey);
    if (sigBytes.length !== 64 || pkBytes.length !== 32) {
      return { ok: false, reason: 'wrong_key_or_signature_length', signed: true };
    }
    const core    = { ts: envelope.ts, topic: envelope.topic, message: envelope.message };
    const bytes   = _enc.encode(canonical(core));
    const pubKey  = await importPublicKey(pkBytes);
    const sigOk   = await verify(pubKey, bytes, sigBytes);
    if (!sigOk) return { ok: false, reason: 'bad_signature', signed: true };
  }

  // Recompute msgId.
  const expected = signed
    ? await computeMsgId({ signature: envelope.signature, ts: envelope.ts, topic: envelope.topic, message: envelope.message })
    : await computeMsgId({                                 ts: envelope.ts, topic: envelope.topic, message: envelope.message });
  if (expected !== envelope.msgId) {
    return { ok: false, reason: 'bad_msgid', signed };
  }
  return { ok: true, signed };
}

// ── internal byte/hex helpers ────────────────────────────────────────

function bytesToHex(bytes) {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}

function hexToBytes(hex) {
  if (hex.length % 2 !== 0) throw new Error('hexToBytes: odd-length hex');
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    out[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return out;
}
