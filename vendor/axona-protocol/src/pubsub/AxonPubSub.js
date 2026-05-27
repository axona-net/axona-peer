// =====================================================================
// AxonPubSub — feed-style pub/sub API on top of AxonaManager.
//
// This is the application-facing layer for the Axona protocol. It
// builds SignedPost envelopes, threads post_hash metadata through the
// existing AxonaManager fan-out, and translates raw per-relay
// MetricsResponse fragments into aggregate AggregateMetrics that a
// publisher's UI can display.
//
// Distinct from PubSubAdapter (Croquet-style event bus). PubSubAdapter
// is a small set of named events; AxonPubSub is named topics holding
// histories of signed posts that publishers can later query for reach.
//
// API surface (PR 1):
//   publish(topicName, content)        → SignedPost
//   subscribe(publisher, topicName, fn) → Subscription handle
//   metrics(topicName, postHashes?)    → Promise<Map<postHash, AggregateMetrics>>
//
// PR 2 adds: pull(), reshare(), reference resolution, pull_count and
// reshare_count semantics. The structure here is deliberately ready
// for them — no API churn expected.
// =====================================================================

import { makePost, deriveTopicId, verifyPostHash, verifyTopicOwnership } from './post.js';

/**
 * Lightweight LRU for posts already resolved via Pull. Lets the
 * application call pull() repeatedly without re-routing requests
 * for the same hash, and (more importantly) keeps pull_count from
 * inflating just because the UI re-rendered. The cache lives at the
 * subscriber; ground-truth pull_count on the publisher side reflects
 * only the first fetch per (subscriber, post). PR 2 default is small;
 * applications that want stricter behavior can override.
 */
const DEFAULT_PULL_CACHE_SIZE = 512;

/** @typedef {import('./post.js').SignedPost} SignedPost */
/** @typedef {import('./post.js').PostRef}    PostRef */

/**
 * @typedef {Object} AggregateMetrics
 * @property {number} delivery_count   Total deliveries summed across relays.
 * @property {number} pull_count       PR 2.
 * @property {number} reshare_count    PR 2.
 * @property {number} reach_estimate   delivery_count + pull_count.
 * @property {number} coverage         Relays that responded / 1.0 baseline.
 *                                     v1 reports raw responder count;
 *                                     "expected" denominator added later.
 */

export class AxonPubSub {
  /**
   * @param {Object} opts
   * @param {import('./AxonaManager.js').AxonaManager} opts.axon
   *        The underlying AxonaManager (already wired to a MockDHTNode).
   */
  constructor({ axon, pullCacheSize = DEFAULT_PULL_CACHE_SIZE } = {}) {
    if (!axon) throw new Error('AxonPubSub: axon is required');
    this.axon     = axon;
    this.nodeId   = axon.nodeId;

    /**
     * Per-(publisher, topicName) subscriber callbacks. The same node
     * can hold multiple subscriptions; this map lets us dispatch each
     * incoming delivery to every registered listener for that topicId.
     * @type {Map<string, Set<(post:SignedPost) => void>>}
     */
    this._listeners = new Map();

    /** Locally-cached Pull results, keyed by post_hash. Bounded ring. */
    this._pullCache    = new Map();
    this._pullCacheCap = pullCacheSize;

    // Register a single delivery callback with AxonaManager that
    // fans out to our local listeners. Idempotent — if another
    // AxonPubSub on the same node has registered first, this would
    // replace it; one AxonPubSub per node is the expected pattern.
    axon.onPubsubDelivery((topicId, json /*, publishId, publishTs */) => {
      this._onDelivery(topicId, json);
    });
  }

  // ── publish ─────────────────────────────────────────────────────────

  /**
   * Publish a new post to one of MY topics.
   *
   * @param {string} topicName  A topic owned by this node.
   * @param {any}    content    Application-defined payload (JSON-stringifiable).
   * @param {Object} [opts]
   * @param {PostRef[]} [opts.references]  Reshare references (PR 2).
   * @returns {SignedPost}
   */
  async publish(topicName, content, { references = [], signer } = {}) {
    // v1.0.0 — post.js is now async (Web Crypto).  publish() returns
    // the SignedPost via a Promise.  Callers must await.
    // The optional `signer` argument lets the application supply an
    // Ed25519 signing function; without one, the post carries the
    // legacy `stub:<publisher>` placeholder signature.
    const post = await makePost({
      publisher: this.nodeId,
      topicName,
      content,
      references,
      signer,
    });
    const json = JSON.stringify(post);
    this.axon.pubsubPublish(post.topic_id, json, {
      postHash:   post.post_hash,
      publisher:  post.publisher,
      references: post.references,
    });
    return post;
  }

  // ── subscribe ───────────────────────────────────────────────────────

  /**
   * Subscribe to another publisher's topic.
   *
   * @param {string} publisher   Publisher identifier (nodeId in sim).
   * @param {string} topicName   Topic owned by `publisher`.
   * @param {(post:SignedPost) => void} callback  Fires per delivered post
   *        once signature/hash/ownership checks pass.
   * @returns {{ topic_id: string, unsubscribe: () => void }}
   */
  async subscribe(publisher, topicName, callback) {
    // v1.0.0 — async (post.js is now async).  Callers must await.
    const topic_id = await deriveTopicId(publisher, topicName);

    let set = this._listeners.get(topic_id);
    if (!set) {
      set = new Set();
      this._listeners.set(topic_id, set);
      this.axon.pubsubSubscribe(topic_id);
    }
    set.add(callback);

    return {
      topic_id,
      unsubscribe: () => {
        const s = this._listeners.get(topic_id);
        if (!s) return;
        s.delete(callback);
        if (s.size === 0) {
          this._listeners.delete(topic_id);
          this.axon.pubsubUnsubscribe(topic_id);
        }
      },
    };
  }

  /** Internal: a delivery has arrived from the axon. Parse + verify
   *  + dispatch to local listeners. Failed verification = silent drop
   *  per §3.5.
   *
   *  v1.0.0 — async (Web Crypto verifies are async). */
  async _onDelivery(topicId, json) {
    const set = this._listeners.get(topicId);
    if (!set || set.size === 0) return;

    let post;
    try { post = JSON.parse(json); }
    catch { return; }
    if (!(await verifyPostHash(post)))      return;
    if (!(await verifyTopicOwnership(post))) return;
    if (post.topic_id !== topicId)   return;

    for (const cb of set) {
      try { cb(post); } catch (err) { console.error('AxonPubSub: listener threw', err); }
    }
  }

  // ── pull ────────────────────────────────────────────────────────────

  /**
   * Fetch a specific post by content hash from its topic's relay tree.
   *
   * Returns the verified SignedPost or null. Null covers all the
   * legitimate cases the application has to handle as "post
   * unavailable" — never seen, aged out of all relays' caches,
   * routing timed out — and the application is expected to surface
   * them identically (silent skip in the feed, §6.1).
   *
   * Local cache: a successful Pull is cached so re-displaying the
   * same post doesn't generate new wire traffic AND doesn't inflate
   * pull_count at the origin. Application reload clears the cache.
   *
   * @param {string|PostRef} ref       Either a {topic_id, post_hash}
   *                                   pair or two positional args.
   * @param {string}         [postHash]
   * @param {Object}         [opts]
   * @param {number}         [opts.timeoutMs=1000]
   * @returns {Promise<SignedPost|null>}
   */
  async pull(ref, postHash, { timeoutMs = 1000 } = {}) {
    let topic_id, post_hash;
    if (typeof ref === 'object' && ref !== null) {
      topic_id  = ref.topic_id;
      post_hash = ref.post_hash;
    } else {
      topic_id  = ref;
      post_hash = postHash;
    }
    if (!topic_id || !post_hash) return null;

    // Cache hit → no wire traffic.
    if (this._pullCache.has(post_hash)) {
      return this._pullCache.get(post_hash);
    }

    const post = await this.axon.requestPull(topic_id, post_hash, { timeoutMs });
    if (!post) return null;
    // Verify everything the protocol gave us before caching or
    // returning to the application. Failed verification = null.
    if (!(await verifyPostHash(post)))      return null;
    if (!(await verifyTopicOwnership(post))) return null;
    if (post.topic_id !== topic_id || post.post_hash !== post_hash) return null;

    this._cachePost(post);
    return post;
  }

  _cachePost(post) {
    this._pullCache.set(post.post_hash, post);
    while (this._pullCache.size > this._pullCacheCap) {
      const oldestKey = this._pullCache.keys().next().value;
      this._pullCache.delete(oldestKey);
    }
  }

  // ── reshare ─────────────────────────────────────────────────────────

  /**
   * Reshare an upstream post on one of MY topics, optionally with
   * commentary. Semantically equivalent to publish() with a single-
   * element references[]; named separately so call sites read
   * intentfully and so the analytics layer can attribute the event
   * cleanly when it lands as reshare_count++ at the origin.
   *
   * @param {string}    myTopicName   A topic owned by THIS node.
   * @param {PostRef}   originalRef   {topic_id, post_hash} of the post
   *                                  being reshared.
   * @param {any}       [commentary]  Optional payload — null means
   *                                  "pure forward, no comment."
   * @returns {SignedPost}            The new reshare post.
   */
  reshare(myTopicName, originalRef, commentary = null) {
    return this.publish(myTopicName, commentary, { references: [originalRef] });
  }

  // ── metrics ─────────────────────────────────────────────────────────

  /**
   * Query metrics for one or more of MY posts. The publisher half of
   * the self-authentication is enforced by the relay (it drops requests
   * whose requesterId doesn't match the publisher embedded in cached
   * posts), so we just route the request and aggregate.
   *
   * @param {string}   topicName    A topic owned by THIS node.
   * @param {string[]} [postHashes] Empty/omitted → all hashes the
   *                                relays know about.
   * @param {Object}   [opts]
   * @param {number}   [opts.timeoutMs=500]
   * @returns {Promise<Map<string, AggregateMetrics>>}
   */
  async metrics(topicName, postHashes, { timeoutMs = 500 } = {}) {
    const topic_id = await deriveTopicId(this.nodeId, topicName);
    const responses = await this.axon.requestMetrics(
      topic_id,
      postHashes ?? null,
      { timeoutMs }
    );

    const byHash = new Map();
    for (const { entries } of responses) {
      for (const e of entries) {
        let agg = byHash.get(e.post_hash);
        if (!agg) {
          agg = {
            delivery_count: 0,
            pull_count:     0,
            reshare_count:  0,
            reach_estimate: 0,
            coverage:       0,
          };
          byHash.set(e.post_hash, agg);
        }
        agg.delivery_count += e.delivery_count;
        agg.pull_count     += e.pull_count;
        agg.reshare_count  += e.reshare_count;
      }
    }
    // Reach + coverage finalisation.
    for (const agg of byHash.values()) {
      agg.reach_estimate = agg.delivery_count + agg.pull_count;
      agg.coverage       = responses.length;   // raw responder count
    }
    return byHash;
  }
}
