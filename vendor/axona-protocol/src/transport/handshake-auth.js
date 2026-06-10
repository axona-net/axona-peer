// =====================================================================
// handshake-auth.js — authenticated peer-identity handshake (axona/5).
//
// Closes the root authentication gap: before v4, a peer's routing
// identity (nodeId) was self-asserted — the hello carried only the
// claimed nodeId, so anyone could claim to be any node.  This module
// makes a peer *prove* its nodeId at connect time, via a three-part
// gate:
//
//   1. BIND     — the presented pubkey must hash to the bottom 256
//                 bits of the claimed nodeId.  nodeId is
//                 [8-bit S2 prefix] || [SHA-256(pubkey)]; we verify the
//                 256-bit suffix only.
//   2. POSSESS  — an Ed25519 signature, under that pubkey, over a
//                 transcript containing a fresh per-connection
//                 "channel binding value" (CBV).  THIS is the
//                 load-bearing part: pubkeys are public, so presenting
//                 a pubkey proves nothing — only a signature over a
//                 value the attacker could not precompute proves
//                 possession of the private key.
//   3. CHANNEL  — the CBV is supplied by the transport and is unique to
//                 the live connection (WebRTC: the DTLS fingerprint
//                 pair; bridge/node WS: a fresh nonce pair; sim: a
//                 per-link token).  Because the CBV is folded into the
//                 signed transcript, a captured hello cannot be
//                 replayed onto a different channel, and a man-in-the-
//                 middle that substitutes the channel (e.g. a hostile
//                 signaling relay) produces a CBV the signature can't
//                 cover.
//
// THE 8-BIT GEO PREFIX IS NOT AUTHENTICATED — BY DESIGN.  It is an
// "area code": a performance hint that keeps a peer's traffic local to
// its region (and degrades gracefully, like phone roaming, when a user
// travels).  It is not covered by the pubkey hash and nothing
// security-relevant depends on it.  Only the 256-bit key-derived
// suffix is bound.
//
// Transport-agnostic: this module knows nothing about WebRTC / WS /
// sim.  Each transport computes its own CBV (a string both endpoints
// derive identically from the live channel) and passes it in.  The
// verify path reconstructs the transcript with the VERIFIER's own view
// of the CBV — so the CBV is never trusted from the wire; a mismatch
// simply fails the signature.
// =====================================================================

import { canonical }              from '../pubsub/post.js';
import { sign, verify, importPublicKey } from '../pubsub/ed25519.js';
import { powVerify }              from '../pow/pow.js';

/**
 * The wire proto tag for the authenticated handshake — ALSO the network
 * partition key. It is both hard-checked (`proto_mismatch`) and folded into the
 * SIGNED transcript (transcriptBytes), so two peers on different proto tags can
 * never form an authenticated channel: the verify either rejects the tag
 * outright or the signature fails to cover the verifier's transcript. Bumping
 * this tag is therefore a hermetic flag-day partition — it severs BOTH the
 * peer↔peer mesh auth and the peer↔bridge-embedded-node auth in one move,
 * regardless of rendezvous. The auth MECHANICS are unchanged across the bump;
 * the rev is the partition epoch.
 *   axona/4 → axona/5: 2026-06 wire flag-day (msgId v2.18 split + this kernel
 *   family). Pre-bump (axona/4, kernel ≤2.16) and post-bump nodes cannot
 *   interoperate at any authenticated layer.
 */
export const AUTH_PROTO = 'axona/5';

const _enc = new TextEncoder();
const MASK_256 = (1n << 256n) - 1n;

// ── transcript ───────────────────────────────────────────────────────

/**
 * The exact bytes both sides sign / verify.  Canonical (sorted-key)
 * JSON over the claimer's own identity fields plus the shared CBV.
 * The CBV is the only freshness source; everything else is the
 * claimer's stable identity.
 */
function transcriptBytes({ proto, nodeId, pubkey, cbv }) {
  return _enc.encode(canonical({ proto, nodeId, pubkey, cbv }));
}

// ── build ──────────────────────────────────────────────────────────

/**
 * Build an authenticated hello frame.
 *
 * @param {object} opts
 * @param {object} opts.identity  Identity with `.id` (66-hex nodeId),
 *                                `.pubkeyHex` (64-hex), and `.sign`
 *                                (CryptoKey-backed signer).
 * @param {string} opts.cbv       Channel binding value — a string both
 *                                endpoints derive identically from the
 *                                live connection.
 * @param {string} [opts.proto]   Defaults to AUTH_PROTO.
 * @returns {Promise<{proto:string, nodeId:string, pubkey:string, sig:string}>}
 */
export async function buildAuthHello({ identity, cbv, proto = AUTH_PROTO }) {
  if (!identity || typeof identity.id !== 'string' || typeof identity.pubkeyHex !== 'string'
      || typeof identity.sign !== 'function') {
    throw new TypeError('buildAuthHello: identity with {id, pubkeyHex, sign} required');
  }
  if (typeof cbv !== 'string' || cbv.length === 0) {
    throw new TypeError('buildAuthHello: cbv must be a non-empty string');
  }
  const core = { proto, nodeId: identity.id, pubkey: identity.pubkeyHex, cbv };
  const sigBytes = await identity.sign(transcriptBytes(core));
  return {
    proto,
    nodeId: identity.id,
    pubkey: identity.pubkeyHex,
    sig:    'ed25519:' + bytesToHex(sigBytes),
    // Stage 2: transport PoW nonce — a SIBLING field, NOT folded into the signed
    // transcript (it self-binds to `pubkey` via the PoW relation, and keeping it
    // out of the transcript preserves interop with peers that predate the field:
    // their verify covers the same core). Inert at difficulty 0 (''):
    pow:    typeof identity.pow === 'string' ? identity.pow : '',
  };
}

// ── verify ─────────────────────────────────────────────────────────

/**
 * Verify an authenticated hello frame against the verifier's own view
 * of the channel binding value.
 *
 * Returns a result object (never throws on a bad frame) so callers can
 * log the precise reason and tear the channel down.
 *
 * @param {object} frame   The received {proto, nodeId, pubkey, sig}.
 * @param {object} opts
 * @param {string} opts.cbv          The VERIFIER's view of the channel
 *                                   binding value.  Never taken from
 *                                   the frame.
 * @param {string} [opts.proto]      Expected proto (default AUTH_PROTO).
 * @returns {Promise<{ok:boolean, nodeId?:string, pubkey?:string, reason?:string}>}
 */
export async function verifyAuthHello(frame, { cbv, proto = AUTH_PROTO } = {}) {
  if (!frame || typeof frame !== 'object')           return fail('not_an_object');
  if (frame.proto !== proto)                         return fail('proto_mismatch');
  if (typeof frame.nodeId !== 'string' || frame.nodeId.length !== 66 || !isHex(frame.nodeId))
    return fail('bad_nodeId');
  if (typeof frame.pubkey !== 'string' || frame.pubkey.length !== 64 || !isHex(frame.pubkey))
    return fail('bad_pubkey');
  if (typeof frame.sig !== 'string' || !frame.sig.startsWith('ed25519:'))
    return fail('bad_sig_scheme');
  if (typeof cbv !== 'string' || cbv.length === 0)   return fail('missing_cbv');

  const sigHex = frame.sig.slice('ed25519:'.length);
  if (sigHex.length !== 128 || !isHex(sigHex))       return fail('bad_sig_length');

  // (1) BIND — pubkey must hash to the 256-bit suffix of nodeId.
  const pubkeyBytes = hexToBytes(frame.pubkey);
  const ok256 = await pubkeyMatchesNodeId(pubkeyBytes, frame.nodeId);
  if (!ok256)                                        return fail('pubkey_nodeid_mismatch');

  // (2)+(3) POSSESS + CHANNEL — signature over the transcript built
  // with the VERIFIER's CBV.  A wrong/replayed channel ⇒ wrong CBV ⇒
  // signature fails here.
  let sigOk = false;
  try {
    const pubKey = await importPublicKey(pubkeyBytes);
    sigOk = await verify(
      pubKey,
      transcriptBytes({ proto, nodeId: frame.nodeId, pubkey: frame.pubkey, cbv }),
      hexToBytes(sigHex),
    );
  } catch {
    return fail('verify_threw');
  }
  if (!sigOk)                                        return fail('bad_signature');

  // Stage 2: transport PoW gate. Self-binding to the (already BIND-checked)
  // pubkey, so it is verified independently of the channel-binding signature.
  // INERT at difficulty 0 — `powVerify` returns true for any/absent nonce, so a
  // peer that predates the `pow` field still passes. Raising transport
  // difficulty (Stage 4a) turns this into the anti-eclipse mint cost.
  const powOk = await powVerify({
    pubkeyHex: frame.pubkey,
    nonce:     typeof frame.pow === 'string' ? frame.pow : '',
    role:      'transport',
  });
  if (!powOk)                                        return fail('bad_pow');

  return { ok: true, nodeId: frame.nodeId, pubkey: frame.pubkey };
}

/**
 * BIND check in isolation: does SHA-256(pubkey) equal the bottom 256
 * bits of nodeId?  The top 8 bits (geo "area code" prefix) are NOT
 * checked — they're an unauthenticated routing hint by design.
 *
 * @param {Uint8Array} pubkeyBytes 32 raw bytes.
 * @param {string}     nodeIdHex   66-char hex.
 * @returns {Promise<boolean>}
 */
export async function pubkeyMatchesNodeId(pubkeyBytes, nodeIdHex) {
  if (!(pubkeyBytes instanceof Uint8Array) || pubkeyBytes.length !== 32) return false;
  if (typeof nodeIdHex !== 'string' || nodeIdHex.length !== 66) return false;
  const buf     = await crypto.subtle.digest('SHA-256', pubkeyBytes);
  const hash256 = BigInt('0x' + bytesToHex(new Uint8Array(buf)));
  const suffix  = BigInt('0x' + nodeIdHex) & MASK_256;
  return suffix === hash256;
}

// ── channel-binding-value helpers ────────────────────────────────────

/** Fresh random hex string for nonce-based CBVs (bridge / node / sim). */
export function makeNonce(bytes = 16) {
  const a = new Uint8Array(bytes);
  crypto.getRandomValues(a);
  return bytesToHex(a);
}

/**
 * Deterministic CBV from a pair of nonces (+ optional extra context
 * like a connId).  Order-independent so both endpoints agree
 * regardless of who is "client" vs "server".
 */
export function cbvFromNonces(nonceA, nonceB, extra = '') {
  const [lo, hi] = [String(nonceA), String(nonceB)].sort();
  return `n:${lo}:${hi}${extra ? ':' + extra : ''}`;
}

/**
 * Deterministic CBV from a pair of channel fingerprints (WebRTC DTLS).
 * Order-independent for the same reason.
 */
export function cbvFromFingerprints(fpA, fpB) {
  const [lo, hi] = [String(fpA), String(fpB)].sort();
  return `fp:${lo}:${hi}`;
}

// ── internal byte/hex ────────────────────────────────────────────────

function fail(reason) { return { ok: false, reason }; }

function isHex(s) { return /^[0-9a-fA-F]*$/.test(s); }

function bytesToHex(bytes) {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}

function hexToBytes(hex) {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) out[i / 2] = parseInt(hex.substr(i, 2), 16);
  return out;
}
