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
// Tracing
// ───────
// Every observable decision is logged via the `opts.log(event, data)`
// callback (defaults to a no-op).  Events are tagged with the
// publishId so a single message can be correlated across nodes:
//
//   pubsub:tx-start    publisher about to fan out — { topic, publishId,
//                                                    msgLen, targets, synSize }
//   pubsub:tx          per-target send — { to, publishId }
//   pubsub:tx-error    transport error during fan-out — { to, publishId, err }
//   pubsub:tx-skip     fan-out target skipped — { reason, to?, publishId }
//   pubsub:rx          incoming 'pubsub:deliver' — { publishId, from }
//   pubsub:rx-bad      malformed frame — { reason, body }
//   pubsub:rx-dup      publishId already seen — { publishId }
//   pubsub:rx-deliver  matched local subscription — { publishId, topic }
//   pubsub:rx-forward  forwarding (per target) — { publishId, to }
//   pubsub:rx-noop     no local sub + no forward targets — { publishId }
//
// The browser appendLog renders these in the event-log section, and
// the bridge's structured logger writes them to journalctl.  Filter
// by 'pubsub:' to see only pubsub events.
// =====================================================================

const SEEN_LRU_CAP = 256;

function idToHex(id) {
  return typeof id === 'bigint' ? id.toString(16).padStart(16, '0') : String(id);
}

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
  function fanOut(frame, exceptPeerId, kind /* 'tx' | 'rx-forward' */) {
    const targets = [];
    let skippedSelf = 0;
    for (const syn of node.synaptome.values()) {
      if (exceptPeerId !== undefined && syn.peerId === exceptPeerId) {
        skippedSelf++;
        continue;
      }
      targets.push(syn.peerId);
    }

    if (skippedSelf > 0) {
      log('pubsub:tx-skip', {
        reason: 'is-sender',
        publishId: frame.publishId,
        skipped: skippedSelf,
      });
    }

    for (const peerId of targets) {
      const toHex = idToHex(peerId);
      log(kind === 'tx' ? 'pubsub:tx' : 'pubsub:rx-forward', {
        to: toHex,
        publishId: frame.publishId,
      });
      transport.notify(peerId, 'pubsub:deliver', frame)
        .catch(err => log(
          kind === 'tx' ? 'pubsub:tx-error' : 'pubsub:rx-forward-error',
          { to: toHex, publishId: frame.publishId, err: err.message }
        ));
    }

    return targets.length;
  }

  transport.onNotification('pubsub:deliver', (fromNodeIdOrChannel, body) => {
    // Defensive shape check + per-field validation; if anything is off,
    // log once and drop (don't blow up the dispatcher).
    if (!body || typeof body !== 'object') {
      log('pubsub:rx-bad', { reason: 'no-body' });
      return;
    }
    const { topicKey, publishId, publisher, msg, ts } = body;
    if (typeof topicKey  !== 'bigint') { log('pubsub:rx-bad', { reason: 'topicKey-not-bigint',  publishId }); return; }
    if (typeof publishId !== 'string') { log('pubsub:rx-bad', { reason: 'publishId-not-string', publishId }); return; }
    if (typeof publisher !== 'bigint') { log('pubsub:rx-bad', { reason: 'publisher-not-bigint', publishId }); return; }
    if (typeof msg       !== 'string') { log('pubsub:rx-bad', { reason: 'msg-not-string',       publishId }); return; }

    const fromHex = (typeof fromNodeIdOrChannel === 'bigint')
      ? idToHex(fromNodeIdOrChannel)
      : String(fromNodeIdOrChannel ?? 'unknown');

    log('pubsub:rx', {
      publishId,
      from: fromHex,
      publisher: idToHex(publisher),
      topic: idToHex(topicKey),
      msgLen: msg.length,
    });

    if (!markSeen(publishId)) {
      log('pubsub:rx-dup', { publishId, from: fromHex });
      return;
    }

    // Deliver locally if I'm subscribed to this topic.
    const handler = subs.get(topicKey);
    if (handler) {
      log('pubsub:rx-deliver', { publishId, topic: idToHex(topicKey) });
      try { handler({ publisher, msg, ts }); }
      catch (err) { log('pubsub:rx-deliver-error', { publishId, err: err.message }); }
    }

    // Forward to all my synapses EXCEPT the peer who sent me this
    // (when we know who it was).  Pre-bind channels send a string
    // fromMeshId/connId, which we just skip — those paths shouldn't
    // ever carry pubsub frames in practice, but be defensive.
    const exceptPeerId = (typeof fromNodeIdOrChannel === 'bigint')
      ? fromNodeIdOrChannel : undefined;
    const forwarded = fanOut(body, exceptPeerId, 'rx-forward');
    if (forwarded === 0 && !handler) {
      log('pubsub:rx-noop', { publishId });
    }
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

      const synSize   = node.synaptome.size;
      const targetIds = [];
      for (const syn of node.synaptome.values()) targetIds.push(idToHex(syn.peerId));

      log('pubsub:tx-start', {
        topic:     idToHex(topicKey),
        publishId,
        msgLen:    msg.length,
        synSize,
        targets:   targetIds,
        localSub:  subs.has(topicKey),
      });

      const frame = { topicKey, publishId, publisher, msg, ts };
      markSeen(publishId);     // we'll see our own fan-out echoes; dedup them

      // Self-deliver synchronously so the publisher's UI shows the
      // message they just sent without a network round-trip.
      const handler = subs.get(topicKey);
      if (handler) {
        log('pubsub:tx-self-deliver', { publishId, topic: idToHex(topicKey) });
        try { handler({ publisher, msg, ts }); }
        catch (err) { log('pubsub:tx-self-deliver-error', { publishId, err: err.message }); }
      }

      const sent = fanOut(frame, undefined, 'tx');
      if (sent === 0 && !handler) {
        log('pubsub:tx-isolated', { publishId, synSize });
      }
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
