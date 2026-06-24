// authorClass.js — voluntary, signed human/agent provenance bound to an AUTHOR key.
//
// A small, optional attestation an author may publish about ITSELF — "this
// publisher is an agent" (or "human") — so subscribers/filters can choose how to
// treat machine-tempo traffic. It is NOT a gate: the kernel never reads it before
// moving a message, it is never in the routing envelope or the address, and its
// absence means UNSTATED (never silently "human"). See
// axona-docs/implementation/Author-Class-Attestation-v0.1.md and the
// agent-legibility design note.
//
// Carrier: each author's own owner-only profile topic on a PINNED region
// (`authorClassTopic`). The shipped kernel deliberately does NOT derive a topic
// region from the author key (that would hotspot — see post.js), so a fixed,
// well-known region is what makes the claim discoverable from the Author ID alone
// (the same pattern the bridge directory uses). owner-only write means only the
// author can set its own class — enforced by the existing WRITE_POLICY_VIOLATION
// check at root ingress.
//
// The object is ALSO self-signed (domain-tagged via `kind`, E-4 discipline) so it
// verifies standalone — e.g. when echoed inline alongside a message rather than
// pulled from the profile topic.

import { canonical }                       from './post.js';
import { sign, verify, importPublicKey }   from './ed25519.js';

export const AUTHOR_CLASS_KIND   = 'axona:author-class:v1';   // domain tag (signed)
export const AUTHOR_CLASS_NAME   = 'axona:author-class';      // profile-topic name
export const AUTHOR_CLASS_REGION = 'useast';                  // pinned, well-known region
export const AUTHOR_CLASS_OPERATOR_DOMAIN = 'axona:author-class-operator:v1';   // operator countersignature domain

const _enc = new TextEncoder();
// Self-asserted publisher kind. Two conceptual axes share one flat set:
//   principal — who/what is behind the message:  'human' | 'agent' | 'service'
//               ('service' = an automated app/feed, not an AI agent or a person)
//   infra     — a node self-identifying its role: 'bridge' | 'relay'
// Finer detail (app name, sensor id) goes in the free-text `label`, NOT here —
// the set stays coarse and stable. Adding a value is backward-TOLERANT: an older
// verifier that doesn't know it returns bad_class → the caller treats it as
// UNSTATED (never a wrong default), so no flag day. Absence is always UNSTATED.
const CLASSES = new Set(['agent', 'human', 'service', 'bridge', 'relay']);

function bytesToHex(bytes) {
  let s = ''; for (const b of bytes) s += b.toString(16).padStart(2, '0'); return s;
}
function hexToBytes(hex) {
  if (hex.length % 2 !== 0) throw new Error('hexToBytes: odd-length hex');
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

/**
 * The author's owner-only, pinned-region class profile topic — derivable from the
 * Author ID alone. Pass to peer.pub/pull/sub like any descriptor.
 * @param {string} authorId 64-hex Author ID (== signerPubkey)
 */
export function authorClassTopic(authorId) {
  if (typeof authorId !== 'string' || authorId.length !== 64) {
    throw new RangeError(`authorClassTopic: authorId must be 64-hex, got length ${authorId?.length}`);
  }
  return { region: AUTHOR_CLASS_REGION, owner: authorId.toLowerCase(), name: AUTHOR_CLASS_NAME };
}

/** Bytes the author's signature covers: the attestation core (no `signature`/`operatorProof`), domain-tagged via `kind`. */
function signedBytes(obj) {
  const core = { kind: AUTHOR_CLASS_KIND, class: obj.class, ts: obj.ts, author: obj.author };
  if (obj.operator != null) core.operator = obj.operator;
  if (obj.label != null)    core.label    = obj.label;
  return _enc.encode(canonical(core));   // canonical() is a total key order, so insertion order is irrelevant
}

/** Bytes the OPERATOR's countersignature covers — binds the operator key to THIS author (own domain tag). */
function operatorSignedBytes(author, operator) {
  return _enc.encode(canonical({ d: AUTHOR_CLASS_OPERATOR_DOMAIN, author, operator }));
}

/**
 * Build + sign an author-class attestation.
 * @param {object} o
 * @param {'agent'|'human'|'service'|'bridge'|'relay'} o.class
 * @param {string}  [o.operator]  who runs this author (pubkey/handle); self-asserted in v1
 * @param {string}  [o.label]     short opaque human-readable label
 * @param {number}  [o.ts]        ms timestamp (defaults to now); latest-valid wins
 * @param {object}  o.signWith    an author identity ({ pubkeyHex, privateKey })
 * @param {object}  [o.operatorSignWith]  an operator identity ({ pubkeyHex, privateKey }); if given,
 *                  `operator` is set to its pubkey and a countersignature (v1.1) is attached so the
 *                  operator vouches BACK for the author — verifyAuthorClass reports operatorVerified:true.
 * @returns {Promise<object>} the signed attestation object
 */
export async function buildAuthorClass({ class: cls, operator = null, label = null, ts = Date.now(), signWith, operatorSignWith = null } = {}) {
  if (!CLASSES.has(cls)) throw new RangeError(`buildAuthorClass: class must be one of ${[...CLASSES].map(c => `"${c}"`).join(', ')}, got "${cls}"`);
  if (!signWith || typeof signWith.pubkeyHex !== 'string' || !signWith.privateKey) {
    throw new TypeError('buildAuthorClass: signWith must be an author identity ({ pubkeyHex, privateKey })');
  }
  // A countersigning operator's pubkey IS the operator field (the proof binds to it).
  if (operatorSignWith) {
    if (typeof operatorSignWith.pubkeyHex !== 'string' || !operatorSignWith.privateKey) {
      throw new TypeError('buildAuthorClass: operatorSignWith must be an operator identity ({ pubkeyHex, privateKey })');
    }
    operator = operatorSignWith.pubkeyHex.toLowerCase();
  }
  const author = signWith.pubkeyHex.toLowerCase();
  const obj = { kind: AUTHOR_CLASS_KIND, class: cls, ts, author };
  if (operator != null) obj.operator = operator;
  if (label != null)    obj.label    = label;
  obj.signature = 'ed25519:' + bytesToHex(await sign(signWith.privateKey, signedBytes(obj)));
  if (operatorSignWith) {
    obj.operatorProof = 'ed25519:' + bytesToHex(await sign(operatorSignWith.privateKey, operatorSignedBytes(author, operator)));
  }
  return obj;
}

/**
 * Verify an author-class attestation standalone (signature + binding). Does NOT
 * decide trust — `operator` is only as good as §3.1 of the spec. Returns the
 * verdict; callers treat any failure as UNSTATED, never as a default class.
 * @param {object} obj
 * @param {object} [opts]
 * @param {string} [opts.expectedAuthor] require obj.author to equal this (e.g. an inline echo's enclosing signerPubkey)
 * `operatorVerified` is true only when a valid operator countersignature (v1.1) is present; a
 * self-asserted `operator` string (no proof) returns the operator but operatorVerified:false.
 * @returns {Promise<{ok:true,class,operator,operatorVerified,label,ts,author}|{ok:false,reason:string}>}
 */
export async function verifyAuthorClass(obj, { expectedAuthor = null } = {}) {
  if (!obj || typeof obj !== 'object')                      return { ok: false, reason: 'not_object' };
  if (obj.kind !== AUTHOR_CLASS_KIND)                       return { ok: false, reason: 'wrong_kind' };
  if (!CLASSES.has(obj.class))                              return { ok: false, reason: 'bad_class' };
  if (typeof obj.author !== 'string' || obj.author.length !== 64) return { ok: false, reason: 'bad_author' };
  if (typeof obj.signature !== 'string' || !obj.signature.startsWith('ed25519:')) return { ok: false, reason: 'unsigned' };
  if (expectedAuthor && obj.author.toLowerCase() !== expectedAuthor.toLowerCase()) return { ok: false, reason: 'author_mismatch' };
  let sigBytes, pkBytes;
  try { sigBytes = hexToBytes(obj.signature.slice('ed25519:'.length)); pkBytes = hexToBytes(obj.author); }
  catch { return { ok: false, reason: 'bad_hex' }; }
  if (sigBytes.length !== 64 || pkBytes.length !== 32)      return { ok: false, reason: 'bad_sig_or_key_length' };
  let sigOk;
  try { sigOk = await verify(await importPublicKey(pkBytes), signedBytes(obj), sigBytes); }
  catch { return { ok: false, reason: 'verify_error' }; }
  if (!sigOk)                                               return { ok: false, reason: 'bad_signature' };

  // Optional operator countersignature (v1.1): a present proof MUST verify, or the whole
  // attestation is rejected (a claimed countersignature that doesn't check is a forgery).
  let operatorVerified = false;
  if (obj.operatorProof != null) {
    if (typeof obj.operatorProof !== 'string' || !obj.operatorProof.startsWith('ed25519:')) return { ok: false, reason: 'bad_operator_proof' };
    if (typeof obj.operator !== 'string' || obj.operator.length !== 64) return { ok: false, reason: 'operator_not_pubkey' };
    let opSig, opPk;
    try { opSig = hexToBytes(obj.operatorProof.slice('ed25519:'.length)); opPk = hexToBytes(obj.operator); }
    catch { return { ok: false, reason: 'bad_operator_hex' }; }
    if (opSig.length !== 64 || opPk.length !== 32) return { ok: false, reason: 'bad_operator_proof' };
    let opOk;
    try { opOk = await verify(await importPublicKey(opPk), operatorSignedBytes(obj.author.toLowerCase(), obj.operator.toLowerCase()), opSig); }
    catch { return { ok: false, reason: 'operator_verify_error' }; }
    if (!opOk) return { ok: false, reason: 'bad_operator_proof' };
    operatorVerified = true;
  }
  return { ok: true, class: obj.class, operator: obj.operator ?? null, operatorVerified, label: obj.label ?? null, ts: obj.ts ?? null, author: obj.author.toLowerCase() };
}
