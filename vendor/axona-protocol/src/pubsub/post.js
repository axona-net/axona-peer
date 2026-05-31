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
export async function deriveTopicId(publisherNodeId, topicName) {
  if (typeof topicName !== 'string' || topicName.length === 0) {
    throw new TypeError('deriveTopicId: topicName must be a non-empty string');
  }
  // Public mode: 0x00 prefix + sha256(topicName).
  if (publisherNodeId === null || publisherNodeId === undefined || publisherNodeId === '') {
    const hash256 = await sha256Hex(topicName);
    return '00' + hash256;
  }
  // Publisher-keyed mode (default).
  if (typeof publisherNodeId !== 'string' || publisherNodeId.length !== 66) {
    throw new RangeError(
      `deriveTopicId: publisherNodeId must be a 66-char hex string or null, got length ${publisherNodeId?.length}`,
    );
  }
  if (!/^[0-9a-fA-F]+$/.test(publisherNodeId)) {
    throw new RangeError('deriveTopicId: publisherNodeId contains non-hex characters');
  }
  const s2Prefix = publisherNodeId.slice(0, 2).toLowerCase();
  const hash256  = await sha256Hex(publisherNodeId + ':' + topicName);
  return s2Prefix + hash256;
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
export async function deriveTopicIdBig(publisherBig, topicName) {
  if (publisherBig === null || publisherBig === undefined) {
    const hex = await deriveTopicId(null, topicName);
    return BigInt('0x' + hex);
  }
  if (typeof publisherBig !== 'bigint') {
    throw new TypeError(
      `deriveTopicIdBig: publisher must be bigint or null, got ${typeof publisherBig}`,
    );
  }
  const publisherHex = publisherBig.toString(16).padStart(66, '0');
  const hex = await deriveTopicId(publisherHex, topicName);
  return BigInt('0x' + hex);
}

/**
 * @typedef {Object} PostRef
 * @property {string} topic_id
 * @property {string} post_hash
 */

/**
 * @typedef {Object} SignedPost
 * @property {string}    post_hash
 * @property {string}    publisher
 * @property {string}    topic_id
 * @property {string}    topic_name
 * @property {number}    timestamp     Unix ms
 * @property {any}       content       Application-defined payload
 * @property {PostRef[]} references    Empty for original posts;
 *                                     non-empty for reshares
 * @property {string}    signature     "ed25519:<hex>" when signed,
 *                                     "stub:<publisher>" when not
 */

/**
 * Construct a `SignedPost`.  Async.
 *
 * The signing step is optional — if `args.signer` is omitted, the
 * post carries a `stub:<publisher>` placeholder signature (the same
 * v0 behavior).  Callers that DO want real signatures pass in a
 * `signer` function that returns a Promise<Uint8Array> over the
 * `canonical(draft)` bytes; we hex-encode and prefix with `ed25519:`.
 *
 * @param {Object}   args
 * @param {string}   args.publisher
 * @param {string}   args.topicName
 * @param {any}      args.content
 * @param {PostRef[]} [args.references]
 * @param {number}   [args.timestamp]
 * @param {(canonicalBytes: Uint8Array) => Promise<Uint8Array>} [args.signer]
 *                   Optional Ed25519 signer.  Receives the canonical-
 *                   encoded draft bytes; returns the 64-byte signature.
 *                   Without a signer, the post is unsigned (stub:).
 * @returns {Promise<SignedPost>}
 */
export async function makePost({
  publisher, topicName, content, references = [], timestamp, signer,
}) {
  const topic_id = await deriveTopicId(publisher, topicName);
  const ts = timestamp ?? Date.now();
  const draft = {
    publisher,
    topic_id,
    topic_name: topicName,
    timestamp:  ts,
    content,
    references,
  };
  const canonicalStr   = canonical(draft);
  const canonicalBytes = _enc.encode(canonicalStr);
  const post_hash      = await sha256Hex(canonicalBytes);

  let signature = 'stub:' + publisher;
  if (typeof signer === 'function') {
    const sigBytes = await signer(canonicalBytes);
    let hex = '';
    for (let i = 0; i < sigBytes.length; i++) hex += sigBytes[i].toString(16).padStart(2, '0');
    signature = 'ed25519:' + hex;
  }

  return {
    post_hash,
    ...draft,
    signature,
  };
}

/**
 * Recompute a post's hash and verify it matches the stored field.
 * Async.
 *
 * Failed verification means the relay layer mangled (or forged) the
 * post — every conforming receiver drops it silently, no error
 * surfaced to the user.  This is the §3.5 spec rule applied at the
 * receiving edge.
 *
 * @param {SignedPost} post
 * @returns {Promise<boolean>}
 */
export async function verifyPostHash(post) {
  if (!post || typeof post !== 'object') return false;
  const recomputed = await sha256Hex(canonical({
    publisher:  post.publisher,
    topic_id:   post.topic_id,
    topic_name: post.topic_name,
    timestamp:  post.timestamp,
    content:    post.content,
    references: post.references,
  }));
  return recomputed === post.post_hash;
}

/**
 * Verify topic_id was derived from this publisher + topic_name.
 * The cheap half of authenticity, needs only the hash.
 * Async.
 *
 * @param {SignedPost} post
 * @returns {Promise<boolean>}
 */
export async function verifyTopicOwnership(post) {
  const expected = await deriveTopicId(post.publisher, post.topic_name);
  return post.topic_id === expected;
}

/**
 * Verify an Ed25519 signature on a SignedPost.  Answers exactly one
 * question: "is this post's signature cryptographically valid?"
 *
 * An unsigned post (legacy `stub:` prefix) carries NO proof of origin,
 * so it returns `false` — never `true`.  (Returning `true` here was a
 * forgery hole: a `stub:<any-publisher>` placeholder would pass as
 * authentic.  Security finding M4.)  Callers that legitimately accept
 * unsigned posts in a trusted/sim context must check
 * `post.signature.startsWith('stub:')` themselves and decide — this
 * function does not conflate "unsigned" with "valid".
 *
 * For real signatures (`ed25519:<hex>`), call the provided `verifier`
 * with (publisherKeyBytes, canonicalBytes, signatureBytes) — return
 * true/false.
 *
 * The verifier function is the caller's responsibility because Ed25519
 * support differs between Web Crypto (Chrome 110+, Safari 17+,
 * Firefox 130+) and pure-JS (@noble/ed25519).  Decoupling lets each
 * runtime pick its implementation.
 *
 * @param {SignedPost} post
 * @param {(publisherKey: any, canonicalBytes: Uint8Array, sig: Uint8Array) => Promise<boolean>} verifier
 * @param {any} publisherKey   Caller-supplied; format depends on
 *                             the verifier (CryptoKey, hex string,
 *                             Uint8Array).
 * @returns {Promise<boolean>}
 */
export async function verifySignature(post, verifier, publisherKey) {
  if (!post?.signature || typeof post.signature !== 'string') return false;
  if (post.signature.startsWith('stub:')) return false;  // unsigned ⇒ not authenticated (M4)
  if (!post.signature.startsWith('ed25519:')) return false;
  if (typeof verifier !== 'function') return false;

  const hex = post.signature.slice('ed25519:'.length);
  if (hex.length !== 128) return false;  // 64 bytes = 128 hex chars
  const sig = new Uint8Array(64);
  for (let i = 0; i < 64; i++) sig[i] = parseInt(hex.substr(i * 2, 2), 16);

  const canonicalBytes = _enc.encode(canonical({
    publisher:  post.publisher,
    topic_id:   post.topic_id,
    topic_name: post.topic_name,
    timestamp:  post.timestamp,
    content:    post.content,
    references: post.references,
  }));

  try { return await verifier(publisherKey, canonicalBytes, sig); }
  catch { return false; }
}
