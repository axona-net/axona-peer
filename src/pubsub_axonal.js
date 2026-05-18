// =====================================================================
// pubsub_axonal.js — application API on top of the protocol's
//                    AxonManager (real DHT-based pub/sub).
//
// AxonManager handles the wire protocol: K-closest replication of
// subscriptions, axon recruitment, direct deliver, refresh, etc.
// It exposes a single delivery callback `onPubsubDelivery((topicId,
// json, publishId, publishTs) => ...)` for the application layer.
//
// This module wraps that single callback with a per-topic handler
// map and serialises our `{msg, publisher}` payload as JSON.  The
// shape AxonaNode exposes (pubsubPublish / pubsubSubscribe /
// pubsubUnsubscribe) is unchanged so client.js doesn't notice.
//
// The bridge is just a peer here — there is NO central forwarding.
// Publishes route to the K nodes closest to the topicId via the
// DHT.  Subscribers register at the same K nodes.  If the bridge
// happens to be one of those K (often will be in a small network)
// it forwards as a DHT participant, not as a relay.
// =====================================================================

// Inline hex converter — small enough to avoid the dependency, and
// mirrors the convention used elsewhere in this codebase (16-char
// zero-padded lowercase hex).
function idToHex(id) {
  return typeof id === 'bigint' ? id.toString(16).padStart(16, '0') : String(id);
}

/**
 * @param {object} axon  AxonManager instance (from engine.axonFor(node))
 * @param {object} peer  AxonaPeer (for publisher nodeId)
 * @param {object} [opts]
 * @param {(event:string, data?:object) => void} [opts.log]
 */
export function mountAxonalPubsub({ axon, peer, log }) {
  const trace = log ?? (() => {});

  /** topicKey (bigint) → handler({publisher: bigint, msg: string, ts: number}) */
  const handlers = new Map();

  // Coerce whatever AxonManager hands us (bigint or hex string) into a
  // bigint we can match against our local map keys.
  function normalizeTopicKey(topicId) {
    if (typeof topicId === 'bigint') return topicId;
    if (typeof topicId === 'string') {
      const stripped = topicId.startsWith('0x') ? topicId.slice(2) : topicId;
      // dht.findKClosest etc. pass either 16-hex-char ids or BigInt;
      // AxonManager tracks topicIds as whatever we give it, so as
      // long as we ALWAYS pass bigints in, we get bigints back.
      return BigInt('0x' + stripped);
    }
    if (typeof topicId === 'number') return BigInt(topicId);
    return null;
  }

  axon.onPubsubDelivery((topicId, json, publishId, publishTs) => {
    const key = normalizeTopicKey(topicId);
    trace('pubsub:axonal-rx', {
      topic: key ? idToHex(key) : String(topicId),
      publishId,
      hasHandler: handlers.has(key),
    });
    if (key == null) return;
    const handler = handlers.get(key);
    if (!handler) return;
    let payload;
    try { payload = JSON.parse(json); }
    catch (err) {
      trace('pubsub:axonal-rx-bad-json', { publishId, err: err.message });
      return;
    }
    const publisher = typeof payload.publisher === 'string'
      ? BigInt('0x' + payload.publisher)
      : payload.publisher;
    try {
      handler({
        publisher,
        msg: payload.msg ?? '',
        ts:  publishTs ?? Date.now(),
      });
    } catch (err) {
      trace('pubsub:axonal-handler-threw', { publishId, err: err.message });
    }
  });

  // Accept either a bigint or a 16-char hex string for topic keys.
  // The debug surface (window.axona.topicKey) returns hex for readable
  // cross-tab comparison; user code calling pubsubPublish directly
  // with that hex shouldn't fail with a cryptic TypeError.
  function asBigInt(k, label) {
    if (typeof k === 'bigint') return k;
    if (typeof k === 'string') {
      const stripped = k.startsWith('0x') ? k.slice(2) : k;
      if (/^[0-9a-fA-F]{1,16}$/.test(stripped)) return BigInt('0x' + stripped);
    }
    throw new TypeError(`${label} must be bigint or 16-char hex string`);
  }

  return {
    subscribe(topicKey, handler) {
      const key = asBigInt(topicKey, 'topicKey');
      if (typeof handler !== 'function') throw new TypeError('handler must be a function');
      handlers.set(key, handler);
      trace('pubsub:axonal-sub', { topic: idToHex(key) });
      axon.pubsubSubscribe(key);
    },

    unsubscribe(topicKey) {
      let key;
      try { key = asBigInt(topicKey, 'topicKey'); } catch { return; }
      handlers.delete(key);
      trace('pubsub:axonal-unsub', { topic: idToHex(key) });
      axon.pubsubUnsubscribe(key);
    },

    publish(topicKey, msg) {
      const key = asBigInt(topicKey, 'topicKey');
      if (typeof msg !== 'string')      throw new TypeError('msg must be a string');
      // Embed our own nodeId as the publisher so receivers can show
      // who sent the message even though AxonManager's wire payload
      // is opaque from its POV.
      const json = JSON.stringify({ msg, publisher: idToHex(peer.getNodeId()) });
      const publishId = axon.pubsubPublish(key, json);
      trace('pubsub:axonal-tx', {
        topic: idToHex(key),
        publishId,
        msgLen: msg.length,
      });
      // Note: AxonManager's K-closest publish loop sendDirects 'pubsub:publish-k'
      // to every root including self.  When self holds an axonRole for this
      // topic (which is true if we're subscribed to it — pubsubSubscribe
      // also includes self in K-closest), the self-loop dispatches into
      // _onPublishDirect locally, which fires our delivery callback.  We
      // therefore do NOT self-deliver synchronously here; that would
      // double-fire the handler.
      return publishId;
    },
  };
}
