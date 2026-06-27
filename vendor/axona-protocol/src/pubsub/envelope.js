// =====================================================================
// envelope.js — Axona pub/sub envelope: build, sign, verify, hash.
//
// Every message that flows through `peer.pub()` carries an envelope
// (envelope format v2 — finding C-2):
//
//     {
//       msgId:        '<64-char hex>',     // content-derived
//       seq:          1716210000123,       // per-publisher monotonic
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
//     msgId = sha256( canonicalize({ publisher, message }) )
//
//   where `publisher` is the signer's pubkey (`signerPubkey`) for a signed
//   envelope, or `null` for an unsigned/anonymous one — i.e. exactly the
//   author identity a receiver can read off the envelope and re-verify.
//
// The id is therefore a stable CONTENT ADDRESS of (author, message): the
// SAME (publisher, message) always hashes to the SAME msgId. It deliberately
// does NOT fold in `ts`, `seq`, `topic`, or the `signature` — so a message's
// id is independent of WHEN it was sent or its position in the publisher's
// stream. A publisher that needs two otherwise-identical messages to have
// distinct ids includes a nonce in `message`. (Anti-replay and ordering do
// not rely on the msgId — they use the signed `seq`/`ts`, see below.)
//
// The signature covers a DOMAIN-TAGGED core (finding E-4 — explicit
// domain separation so an envelope signature can never be replayed as a
// post signature or a channel-binding value, and there is no ':'-
// delimiter ambiguity):
//
//     signedBytes = utf8( canonicalize({
//                     d: ENVELOPE_DOMAIN, seq, ts, topic, message }) )
//     signature   = 'ed25519:' + hex( ed25519.sign(privkey, signedBytes) )
//
// `seq` (finding C-2) is a per-publisher MONOTONIC sequence number,
// folded under the signature.  Two things rely on it:
//   1. Freshness / anti-replay — combined with `ts`, the routing layer
//      (root-axon ingress) rejects a captured envelope re-injected later
//      as a live publish.  The signed `ts` is what's checked, NOT the
//      unsigned wire `publishTs` (a replayer controls the latter).
//   2. Ordering — a single publisher's stream is totally ordered by seq,
//      so "most recent" and bounded-queue eviction are deterministic.
// Publishers seed `seq` from the wall clock (see AxonaPeer._nextPubSeq)
// so it stays monotonic across restarts without persisted state.
//
// Receivers verify the signature against signerPubkey AND recompute the
// msgId: a tamper to `message` breaks both the signature and the msgId; a
// tamper to seq/ts/topic breaks the signature; a swap of signerPubkey
// breaks the recomputed msgId (and the signature). pull(msgId) (A3)
// therefore returns an envelope that the caller can independently
// authenticate.
//
// `signerPubkey` is the raw 32-byte Ed25519 public key as 64-char hex
// — the same encoding `identity.pubkeyHex` uses.
// =====================================================================

import { canonical, sha256Hex }              from './post.js';
import { sign, verify, importPublicKey }     from './ed25519.js';
import { powMint }                           from '../pow/pow.js';
import { AUTH_VERIFY_RELAXED }               from '../utils/hexid.js';

const _enc = new TextEncoder();

/**
 * Domain-separation tag folded into the signed core (finding E-4).
 * Distinct from post signing and from channel-binding-value construction,
 * so a signature minted in one context can never be presented as valid in
 * another.  Versioned: a future envelope revision bumps the trailing tag.
 */
export const ENVELOPE_DOMAIN = 'axona:pubsub-envelope:v2';

/**
 * Default freshness window (finding C-2).  At live-publish ingress the
 * routing layer rejects any signed envelope whose signed `ts` differs
 * from local time by more than this.  Generous enough to absorb clock
 * skew + propagation; tight enough that a captured envelope can't be
 * re-injected as live once it ages out.  Does NOT apply to the replay
 * path (root axons deliberately serve cached history older than this to
 * late subscribers).
 */
export const MAX_PUBLISH_SKEW_MS = 300_000;

/**
 * Compute the content-derived msgId for an envelope:
 *
 *     msgId = sha256( canonicalize({ publisher, message }) )
 *
 * `publisher` is the author identity (the signer's pubkey, or null when
 * unsigned).  The id is a stable content address of (author, message) — it
 * intentionally does NOT depend on time, sequence, topic, or signature, so a
 * publisher that wants distinct ids for otherwise-identical content adds a
 * nonce to `message`.
 *
 * @param {object} core  { publisher, message }
 * @returns {Promise<string>} 64-char hex sha256
 */
export async function computeMsgId({ publisher = null, message }) {
  return sha256Hex(canonical({ publisher, message }));
}

/**
 * Build (and optionally sign) an envelope for a pub call.
 *
 * @param {object} opts
 * @param {string} opts.topic
 * @param {*}      opts.message
 * @param {number} [opts.ts]      ms timestamp; defaults to Date.now()
 * @param {number} [opts.seq=0]   per-publisher monotonic sequence (C-2).
 *        Callers that want replay/ordering guarantees pass a strictly
 *        increasing value (see AxonaPeer._nextPubSeq); 0 is acceptable
 *        for one-off / test envelopes.
 * @param {object} [opts.identity]
 *        Identity with `.privateKey` (CryptoKey) + `.pubkeyHex` (64-char hex).
 *        Required when `sign: true` (the default).
 * @param {boolean} [opts.sign=true]
 * @returns {Promise<object>} envelope
 */
export async function buildEnvelope({ topic, message, ts = Date.now(), seq = 0, identity, sign: doSign = true }) {
  if (doSign && (!identity || !identity.privateKey || typeof identity.pubkeyHex !== 'string')) {
    throw new TypeError('buildEnvelope: identity with privateKey + pubkeyHex required when sign=true');
  }

  let signature = undefined;
  let signerPubkey = undefined;
  if (doSign) {
    // Sign the DOMAIN-TAGGED core (E-4) — never the bare core.
    const bytes = _enc.encode(canonical({ d: ENVELOPE_DOMAIN, seq, ts, topic, message }));
    const sigBytes = await sign(identity.privateKey, bytes);
    signature    = 'ed25519:' + bytesToHex(sigBytes);
    // The carried publisher identity is the author's PUBLIC id. In production
    // `authorId === pubkeyHex` (the raw Ed25519 key), so this is the verifiable
    // key. In a shrunk sim profile `authorId` is the truncated routing id (the
    // same value owner-topic descriptors key off), keeping signerPubkey, the
    // owner-write check, and the msgId author all consistent at the small width.
    signerPubkey = identity.authorId ?? identity.pubkeyHex;
  }

  // msgId = hash(publisher + message). `publisher` is the signer's pubkey
  // (recoverable from the envelope as signerPubkey, so a receiver can
  // recompute and verify); null for an unsigned/anonymous envelope.  Time
  // and sequence are deliberately NOT folded in — see the header.
  const msgId = await computeMsgId({ publisher: signerPubkey ?? null, message });

  const envelope = { msgId, seq, ts, topic, message };
  if (signature) {
    envelope.signature    = signature;
    envelope.signerPubkey = signerPubkey;
    // DELIBERATELY NOT INCLUDED: the publisher's node-id / S2 region. The
    // envelope authenticates WHO signed a message (signerPubkey, the Ed25519
    // verification key) but never WHERE they are. The S2 region cell lives only
    // in the top byte of the node-id, which is NOT derivable from the public key
    // — so by carrying just the key, a signed publish discloses identity without
    // disclosing the publisher's geography. This is a publisher-privacy property,
    // not an oversight: do not "helpfully" add publisherNodeId/region here. An
    // app that WANTS to surface a sender's region opts in by putting it in its
    // own message payload (see apps/axona-minimal), keeping the choice — and the
    // disclosure — at the application layer where the user can see it.
    // Stage 2: publish-role PoW nonce — a SIBLING field (outside the signed
    // core, like signerPubkey/signature), self-binding to signerPubkey via the
    // PoW relation. Inert at difficulty 0 (''); a root verifies it at ingress
    // before granting a per-publisher quota slot once publish difficulty > 0.
    envelope.signerPow    = await powMint({ pubkeyHex: signerPubkey, role: 'publish' });
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
  // Envelope v3: `topic` is the topic DESCRIPTOR { region, owner, name, write }
  // (signed, so the signature binds the exact topic + write policy — a root
  // recomputes the topic id from it and enforces owner-only writes).
  if (!envelope.topic || typeof envelope.topic !== 'object' || typeof envelope.topic.name !== 'string') {
    return { ok: false, reason: 'missing_topic', signed: false };
  }
  if (!('message' in envelope)) {
    return { ok: false, reason: 'missing_message', signed: false };
  }
  // Envelope format v2 (C-2): seq is mandatory and folded under the
  // signature.  A v1 envelope (no seq) fails here — this is the
  // intended flag-day boundary.
  if (typeof envelope.seq !== 'number') {
    return { ok: false, reason: 'missing_seq', signed: false };
  }

  const signed = typeof envelope.signature === 'string';
  if (signed) {
    if (typeof envelope.signerPubkey !== 'string') {
      return { ok: false, reason: 'missing_signerPubkey', signed: true };
    }
    if (!envelope.signature.startsWith('ed25519:')) {
      return { ok: false, reason: 'unknown_signature_scheme', signed: true };
    }
    // Sim-only relaxed profile: when the author id is shrunk below the full
    // 256-bit Ed25519 pubkey width, `signerPubkey` is a truncated routing id —
    // not a key the crypto can verify against. The simulator tests routing and
    // delivery, not auth (the B-4 sig-verify path stays exercised only at the
    // production 256-bit profile). Skip the crypto check but KEEP the structural
    // checks above and the msgId recompute below (signerPubkey still binds the
    // author id into the content address consistently with buildEnvelope).
    if (!AUTH_VERIFY_RELAXED) {
      const sigBytes = hexToBytes(envelope.signature.slice('ed25519:'.length));
      const pkBytes  = hexToBytes(envelope.signerPubkey);
      if (sigBytes.length !== 64 || pkBytes.length !== 32) {
        return { ok: false, reason: 'wrong_key_or_signature_length', signed: true };
      }
      // Verify against the SAME domain-tagged core buildEnvelope signed (E-4).
      const core    = { d: ENVELOPE_DOMAIN, seq: envelope.seq, ts: envelope.ts, topic: envelope.topic, message: envelope.message };
      const bytes   = _enc.encode(canonical(core));
      const pubKey  = await importPublicKey(pkBytes);
      const sigOk   = await verify(pubKey, bytes, sigBytes);
      if (!sigOk) return { ok: false, reason: 'bad_signature', signed: true };
    }
  }

  // Recompute msgId = hash(publisher + message), where publisher is the
  // signer's pubkey for a signed envelope (null otherwise) — the same author
  // identity buildEnvelope used.  Binds the message to its author; a swapped
  // signerPubkey changes the id (and would already have failed the signature).
  const expected = await computeMsgId({
    publisher: signed ? envelope.signerPubkey : null,
    message:   envelope.message,
  });
  if (expected !== envelope.msgId) {
    return { ok: false, reason: 'bad_msgid', signed };
  }
  return { ok: true, signed };
}

/**
 * Freshness check (finding C-2) — used ONLY at live-publish ingress, not
 * on the replay path.  Returns `{ ok, reason }`.  Checks the SIGNED `ts`
 * (the unsigned wire `publishTs` is attacker-controlled on a replay, so
 * it is never trusted here).  A caller that has already run
 * `verifyEnvelope` knows `ts` is genuine for signed envelopes.
 *
 * Stateless: pass `now` from the caller's clock.  Replay *detection*
 * across the publisher's stream is the seq high-water check in
 * AxonaManager; this is the absolute time-window half.
 *
 * @param {object} envelope
 * @param {object} [opts]
 * @param {number} [opts.now]        local time (ms); defaults to Date.now()
 * @param {number} [opts.maxSkewMs]  window; defaults to MAX_PUBLISH_SKEW_MS
 * @returns {{ok: boolean, reason?: string}}
 */
export function checkFreshness(envelope, { now = Date.now(), maxSkewMs = MAX_PUBLISH_SKEW_MS } = {}) {
  if (!envelope || typeof envelope.ts !== 'number') {
    return { ok: false, reason: 'missing_ts' };
  }
  const drift = Math.abs(envelope.ts - now);
  if (drift > maxSkewMs) {
    return { ok: false, reason: 'stale', drift };
  }
  return { ok: true };
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
