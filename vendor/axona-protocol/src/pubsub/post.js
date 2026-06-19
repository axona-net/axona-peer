// =====================================================================
// post.js — Axona post structure: hashing, topic-id derivation, refs,
//          Ed25519 signing & verification.
//
// A "post" is the unit of pub/sub traffic.  It carries provenance via
// a publisher identifier and a content hash; the protocol layer
// treats the `content` field as opaque bytes (end-to-end argument —
// any encryption / structure lives inside the application).
//
// Topic identity is self-authenticating and anchored to the publisher's
// region in the keyspace:
//
//     topic_id = [publisher.s2Prefix(8 bits)]
//             || [sha256(publisher.nodeId || ':' || topic_name) (256 bits)]
//
// Total: 264 bits = 66 hex chars.  The S2-prefix anchor means routing
// to a topic_id naturally lands near the publisher's region in the
// K-closest neighborhood, without any extra mechanism.  Anyone with
// the publisher's nodeId and the topic name can recompute the topic_id;
// relays verify by recomputation alone, no central registry.
//
// Post identity is content-addressed:
//
//     post_hash = sha256(canonical({
//       publisher, topic_id, topic_name, timestamp, content, references
//     }))
//
// where canonical() is JSON.stringify with sorted keys.  Same post
// always hashes to the same value, regardless of platform.
//
// v1.0.0 — async, Web Crypto:
//   All hashing / signing / verification uses `crypto.subtle` (browser
//   Web Crypto API, also available in Node 19+).  Every function below
//   is async.  The synchronous v0 API was based on node:crypto, which
//   doesn't exist in browsers; the async port unblocks browser
//   pub/sub.  Callers (AxonPubSub) await accordingly.
//
//   Signatures use Ed25519 via Web Crypto where supported (Chrome 110+,
//   Safari 17+, Firefox 130+).  On older runtimes the caller passes in
//   a signer/verifier that can use @noble/ed25519 as a pure-JS
//   fallback — see signPost / verifySignature for the contract.
// =====================================================================

import { resolveRegion } from '../utils/region-names.js';

/**
 * Stable, total, JSON-valid canonical encoding (finding C-1).
 *
 * Object keys are sorted at every level so two semantically-identical
 * values hash/sign to the same bytes across runs and implementations.
 * The value semantics match `JSON.stringify` EXACTLY — which is also
 * what the wire does to every frame — so a value canonicalized at the
 * signer and the same value canonicalized at the verifier (after a
 * JSON round-trip on the wire) always agree:
 *
 *   - undefined / function / symbol as an OBJECT VALUE  → key omitted
 *   - undefined / function / symbol as an ARRAY ELEMENT → `null`
 *   - NaN / ±Infinity                                   → `null`
 *   - -0                                                → `0`
 *
 * The previous implementation recursed into object values unconditionally,
 * so an `undefined`-valued key emitted the literal token `undefined` —
 * invalid JSON, and a signer/verifier mismatch (the wire drops such keys),
 * meaning any message containing one silently failed verification. This
 * version omits them exactly as `JSON.stringify` does, so output is always
 * valid JSON and identical to the wire-observed shape. Output is unchanged
 * for every value that does not contain `undefined`/function/symbol — i.e.
 * for everything that verified before — so this is not a wire/flag-day
 * change.
 *
 * Note: special numerics (NaN/Infinity) follow JSON's `null` coercion on
 * both the signing and wire paths; applications that must preserve them
 * should encode them as strings.
 */
export function canonical(value) {
  if (value === null || typeof value !== 'object') {
    // Primitives: defer to JSON.stringify (string, finite number → as-is;
    // NaN/±Infinity → "null"; -0 → "0").  A bare undefined/function/symbol
    // has no JSON form (JSON.stringify returns the JS value `undefined`);
    // emit a valid-JSON "null" so the function is total and never yields
    // the literal token `undefined`.
    const s = JSON.stringify(value);
    return s === undefined ? 'null' : s;
  }
  if (Array.isArray(value)) {
    // canonical() already maps undefined/function/symbol elements to the
    // string "null" (via the primitive branch), matching JSON.stringify.
    return '[' + value.map(canonical).join(',') + ']';
  }
  const keys = Object.keys(value).sort();
  const parts = [];
  for (const k of keys) {
    const v  = value[k];
    const tv = typeof v;
    // Omit keys with no JSON form, exactly as JSON.stringify does.
    if (v === undefined || tv === 'function' || tv === 'symbol') continue;
    parts.push(JSON.stringify(k) + ':' + canonical(v));
  }
  return '{' + parts.join(',') + '}';
}

const _enc = new TextEncoder();

/** sha256 → hex.  Web Crypto in both Node 19+ and modern browsers. */
export async function sha256Hex(input) {
  const bytes  = (typeof input === 'string') ? _enc.encode(input) : input;
  const buf    = await crypto.subtle.digest('SHA-256', bytes);
  const arr    = new Uint8Array(buf);
  let hex = '';
  for (let i = 0; i < arr.length; i++) hex += arr[i].toString(16).padStart(2, '0');
  return hex;
}

/**
 * Derive a topic identifier from a publisher's nodeId and a topic name.
 *
 *     topic_id = [publisher.s2Prefix (2 hex chars / 8 bits)]
 *             || [sha256(publisher.nodeId || ':' || topic_name)
 *                  (64 hex chars / 256 bits)]
 *
 * The publisher's S2 prefix is prepended so the resulting topic_id
 * lands in the publisher's region of the keyspace — K-closest routing
 * to a topic naturally finds its publisher's neighborhood without
 * extra mechanism.
 *
 * Two publishers with the same topic_name produce DIFFERENT topic_ids;
 * names are scoped to publishers.
 *
 * Async (uses sha256Hex which uses Web Crypto).
 *
 * Two modes, application's choice:
 *
 *   1. Publisher-keyed (default).  `publisherNodeId` is a 66-char hex
 *      node ID.  Topic ID =
 *
 *          [publisher's 8-bit S2 prefix] || sha256(publisher + ':' + topicName)
 *
 *      Two publishers with the same topic_name produce DIFFERENT topic
 *      IDs; names are scoped to publishers.  Signed envelopes give
 *      verifiable provenance.  K-closest routing naturally lands at
 *      the publisher's geographic neighborhood.
 *
 *   2. Public (`publisherNodeId === null` or `''`).  Anyone-can-
 *      publish, anyone-can-subscribe.  Topic ID =
 *
 *          '00' || sha256(topicName)
 *
 *      8-bit S2 prefix is 0x00 — global bucket, no geographic anchor.
 *      Signed envelopes still carry signerPubkey when sign=true, so a
 *      public-topic subscriber can still verify who sent a particular
 *      message — but the topic itself isn't scoped to a single
 *      publisher.  Useful for chat rooms / news boards / well-known
 *      protocol topics.
 *
 * Async (uses sha256Hex which uses Web Crypto).
 *
 * @param {string|null} publisherNodeId  66-char lowercase hex node ID,
 *                                       OR null/'' for public mode.
 * @param {string}      topicName        Application-chosen topic name.
 * @returns {Promise<string>}            66-char lowercase hex topic ID.
 */
export async function resolveTopic({ region = null, owner = null, name, write } = {}, selfRegion = null) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new TypeError('resolveTopic: name must be a non-empty string');
  }
  const ownerLc = owner == null ? null : String(owner).toLowerCase();
  if (ownerLc != null && !/^[0-9a-f]{64}$/.test(ownerLc)) {
    throw new RangeError('resolveTopic: owner must be a 64-hex Author ID or null');
  }
  // Write policy is keyed on whether an owner is named:
  //   no owner   → the topic is necessarily 'open' (there's no owner to restrict
  //                to); any passed `write` is ignored.
  //   has owner  → defaults to 'owner' (naming an owner means owner-only — the safe
  //                default, no footgun); pass write:'open' explicitly for an
  //                owner-namespaced open topic (an inbox/wall anyone may post to).
  // `write` is folded into the hash, so { owner, name } and { owner, name,
  // write:'owner' } resolve to the SAME id, and { owner, name, write:'open' } to a
  // different (open) one.
  let w;
  if (ownerLc == null) {
    w = 'open';
  } else {
    w = (write == null) ? 'owner' : write;
    if (w !== 'open' && w !== 'owner') {
      throw new RangeError(`resolveTopic: write must be 'open' or 'owner', got '${write}'`);
    }
  }

  // Region byte (top byte of the topic id) — ALWAYS a real, routable region;
  // never global, never derived from the author key (the author has no region,
  // and a hashed region would dump every author's topics into one arbitrary
  // cell, creating a hotspot in whatever populated region sits closest).
  //   region given   → that region (explicit, app-chosen placement)
  //   region omitted  → the publisher's own node region (`selfRegion`, the top
  //                     byte of its node/transport ID) supplied by the caller
  //   neither         → error (the caller must name a region or pass selfRegion)
  let code;
  if (region !== null && region !== undefined) {
    code = resolveRegion(region);
    if (code === null) throw new RangeError(`resolveTopic: unknown region '${region}'`);
  } else if (selfRegion !== null && selfRegion !== undefined) {
    code = resolveRegion(selfRegion);
    if (code === null) throw new RangeError(`resolveTopic: invalid selfRegion '${selfRegion}'`);
  } else {
    throw new RangeError(
      'resolveTopic: region is required (no global region; not derived from the author) — ' +
      'name a region or publish from a peer that supplies its node region');
  }

  const prefix  = code.toString(16).padStart(2, '0');
  // owner + write are folded into the hash so a root can recompute the id from the
  // SIGNED descriptor and enforce write authorization statelessly. region is the
  // resolved code so the descriptor is self-contained (recomputable without
  // re-running key-derivation).
  const hash256 = await sha256Hex(canonical({ owner: ownerLc, name, write: w }));
  return { region: code, owner: ownerLc, name, write: w, topicId: prefix + hash256 };
}

/** Convenience: just the 66-hex topic id for a topic descriptor. `selfRegion`
 *  (optional) is the region-omitted fallback — the publisher's node region. */
export async function deriveTopicId(descriptor, selfRegion = null) {
  return (await resolveTopic(descriptor, selfRegion)).topicId;
}

/**
 * BigInt-returning variant of `deriveTopicId`.  Kernel internals
 * (AxonaPeer.pub / sub, AxonaManager state) hold topic IDs as 264-bit
 * BigInts; this helper is the canonical entrypoint for that form.
 *
 * The `publisher` argument is either:
 *   - a 264-bit BigInt nodeId (canonical), or
 *   - `null` for public mode (anyone-can-publish topics).
 *
 * Returns the same value as `fromHex(await deriveTopicId(toHex(publisher), topic))`
 * but expresses the contract in BigInt terms for the kernel.
 *
 * @param {bigint|null} publisherBig  264-bit BigInt nodeId, or null for public mode.
 * @param {string}      topicName     Application-chosen topic name.
 * @returns {Promise<bigint>}         264-bit BigInt topic ID.
 */
export async function deriveTopicIdBig(descriptor, selfRegion = null) {
  const hex = await deriveTopicId(descriptor, selfRegion);
  return BigInt('0x' + hex);
}
