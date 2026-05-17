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

  return {
    subscribe(topicKey, handler) {
      if (typeof topicKey !== 'bigint') throw new TypeError('topicKey must be bigint');
      if (typeof handler !== 'function') throw new TypeError('handler must be a function');
      handlers.set(topicKey, handler);
      trace('pubsub:axonal-sub', { topic: idToHex(topicKey) });
      axon.pubsubSubscribe(topicKey);
    },

    unsubscribe(topicKey) {
      if (typeof topicKey !== 'bigint') return;
      handlers.delete(topicKey);
      trace('pubsub:axonal-unsub', { topic: idToHex(topicKey) });
      axon.pubsubUnsubscribe(topicKey);
    },

    publish(topicKey, msg) {
      if (typeof topicKey !== 'bigint') throw new TypeError('topicKey must be bigint');
      if (typeof msg !== 'string')      throw new TypeError('msg must be a string');
      // Embed our own nodeId as the publisher so receivers can show
      // who sent the message even though AxonManager's wire payload
      // is opaque from its POV.
      const json = JSON.stringify({ msg, publisher: idToHex(peer.getNodeId()) });
      const publishId = axon.pubsubPublish(topicKey, json);
      trace('pubsub:axonal-tx', {
        topic: idToHex(topicKey),
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
