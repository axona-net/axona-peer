// =====================================================================
// pubsub.js — Axona application-layer pub/sub.
//
// Flood-publish with dedup, on top of the existing transport-level
// notification machinery.  No AxonManager / Scribe-style rendezvous;
// just gossip every published message to every direct synapse, and
// have receivers forward to their own synapses (suppressing the
// sender) once.  Publish IDs in a bounded LRU prevent cycles and
// duplicate delivery.
//
// Topic addresses live in the same 64-bit space as nodeIds:
//
//     ┌──────────────┬─────────────────────────────────────────┐
//     │ 8-bit prefix │ 56-bit hash(event-name)                 │
//     └──────────────┴─────────────────────────────────────────┘
//
// The prefix is an S2 cell id (same as identity.js' nodeId encoding),
// so geographically-co-located publishers and subscribers naturally
// collide.  The 56 low bits are SHA-256(event-name)[0..7] reinterpreted
// as a big-endian unsigned integer.
//
// `mountPubsub(node, transport, options)` wires the 'pubsub:deliver'
// notification handler onto `transport` and returns a small API:
//
//   subscribe(topicKey, handler)   — register a topic; handler({publisher, msg, ts})
//   unsubscribe(topicKey)          — remove a subscription
//   publish(topicKey, msg)         — fan out to every synapse + self
//
// `node` is the protocol's NeuronNode (used for .id + .synaptome).
// `transport` is the CompositeTransport (used to notify by nodeId).
//
// The handler arg shape mirrors the wire frame body without the
// transport-internal publishId/topicKey; consumers don't care which
// peer relayed the message, only who published it.
// =====================================================================

const SEEN_LRU_CAP = 256;

/**
 * @param {object} node       protocol NeuronNode (has .id + .synaptome)
 * @param {object} transport  CompositeTransport
 * @param {object} [opts]
 * @param {(event:string, data?:object) => void} [opts.log]
 */
export function mountPubsub(node, transport, opts = {}) {
  const log = opts.log ?? (() => {});

  /** Topics this node subscribes to.  Map<bigint, handler>. */
  const subs = new Map();

  /** LRU of publishIds we've already processed (Set preserves insertion order). */
  const seen = new Set();
  function markSeen(id) {
    if (seen.has(id)) return false;
    seen.add(id);
    if (seen.size > SEEN_LRU_CAP) {
      // Drop the oldest insertion.  Set.values() iterates in insertion order.
      const oldest = seen.values().next().value;
      seen.delete(oldest);
    }
    return true;
  }

  /** Per-publisher monotonic counter used to mint publishIds. */
  let publishCounter = 0;

  /**
   * Fan out a 'pubsub:deliver' notification to every direct synapse
   * EXCEPT `exceptPeerId` (which avoids echoing back to the relayer).
   * Errors are swallowed with a log — a single dead peer mustn't
   * abort the broadcast.
   */
  function fanOut(frame, exceptPeerId) {
    for (const syn of node.synaptome.values()) {
      if (exceptPeerId !== undefined && syn.peerId === exceptPeerId) continue;
      transport.notify(syn.peerId, 'pubsub:deliver', frame)
        .catch(err => log('pubsub-fanout-failed', {
          to: syn.peerId.toString(16),
          err: err.message,
        }));
    }
  }

  transport.onNotification('pubsub:deliver', (fromNodeIdOrChannel, body) => {
    if (!body || typeof body !== 'object')   return;
    const { topicKey, publishId, publisher, msg, ts } = body;
    if (typeof topicKey !== 'bigint')        return;
    if (typeof publishId !== 'string')       return;
    if (typeof publisher !== 'bigint')       return;
    if (typeof msg !== 'string')             return;
    if (!markSeen(publishId))                return;  // already handled

    // Deliver locally if I'm subscribed to this topic.
    const handler = subs.get(topicKey);
    if (handler) {
      try { handler({ publisher, msg, ts }); }
      catch (err) { log('pubsub-handler-threw', { err: err.message }); }
    }

    // Forward to all my synapses EXCEPT the peer who sent me this
    // (when we know who it was).  Pre-bind channels send a string
    // fromMeshId/connId, which we just skip — those paths shouldn't
    // ever carry pubsub frames in practice, but be defensive.
    const exceptPeerId = (typeof fromNodeIdOrChannel === 'bigint')
      ? fromNodeIdOrChannel : undefined;
    fanOut(body, exceptPeerId);
  });

  return {
    subscribe(topicKey, handler) {
      if (typeof topicKey !== 'bigint') throw new TypeError('topicKey must be bigint');
      if (typeof handler !== 'function') throw new TypeError('handler must be a function');
      subs.set(topicKey, handler);
    },

    unsubscribe(topicKey) {
      subs.delete(topicKey);
    },

    publish(topicKey, msg) {
      if (typeof topicKey !== 'bigint') throw new TypeError('topicKey must be bigint');
      if (typeof msg !== 'string')      throw new TypeError('msg must be a string');

      const publisher = node.id;
      const publishId = publisher.toString(16) + ':' + (++publishCounter);
      const ts        = Date.now();

      const frame = { topicKey, publishId, publisher, msg, ts };
      markSeen(publishId);     // we'll see our own fan-out echoes; dedup them

      // Self-deliver synchronously so the publisher's UI shows the
      // message they just sent without a network round-trip.
      const handler = subs.get(topicKey);
      if (handler) {
        try { handler({ publisher, msg, ts }); }
        catch (err) { log('pubsub-self-deliver-threw', { err: err.message }); }
      }

      fanOut(frame, undefined);
    },

    /** Debug surface — exposed for smokes. */
    _debug() {
      return {
        subs:  [...subs.keys()],
        seen:  seen.size,
      };
    },
  };
}
