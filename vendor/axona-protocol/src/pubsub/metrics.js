// =====================================================================
// src/pubsub/metrics.js — derived metric-topic convention.
//
// Lives in CORE (not std) on purpose: the derivation and the recursion guard
// are a PROTOCOL convention that clients AND infrastructure roots must compute
// byte-for-byte identically — exactly like deriveTopicId (its sibling in
// post.js), and unlike the optional app-layer helpers in std/. Every vendored
// kernel (relay, peer, dht-sim all vendor src/) therefore gets this for free
// through the normal sync; the metric-publish loop in a relay imports it from
// here, with no dependency on std.
//
// Problem it solves: peer.metrics(topic) is a scatter-gather to the K closest
// roots (pubsub:metricsReq, ~500 ms, O(K) per call). Fine for an occasional
// admin probe; ruinous if every client polls it for every topic on a timer.
//
// Convention: the metrics for a data topic are PUBLISHED, by the topic's primary
// root, to a SEPARATE topic derived from the data topic's id. Anyone who knows
// the data topic computes the metric topic and sub()s it — they receive the
// latest snapshot via replay-on-subscribe plus every subsequent update, paying
// one subscription instead of a scatter-gather per poll. Snapshots accumulate in
// the replay cache and age out at the 48 h hold ceiling, so a subscriber also
// gets a rolling history to watch trends — no replace/overwrite machinery, the
// cache TTL is the window.
//
// The derivation uses a self-identifying reserved namespace ("axona:metric:")
// rather than a bare hash so a root can recognise a metric topic and REFUSE to
// compute metrics-of-metrics (the recursion guard, isMetricTopic below).
//
// Pure and synchronous. metricTopic() takes an already-resolved 66-hex data-topic
// id and returns a topic descriptor you pass straight to peer.sub()/peer.pub().
// Resolve the data topic first with the public deriveTopicId():
//
//   import { deriveTopicId, metricTopic } from '@axona/protocol';
//   const dataId = await deriveTopicId({ region, name: 'lobby' });
//   await peer.sub(metricTopic(dataId), (env) => render(JSON.parse(env.message)),
//                  { since: 'all' });   // since:'all' → latest snapshot + history
// =====================================================================

// Reserved name prefix that marks a topic as carrying another topic's metrics.
// Part of the wire-visible topic descriptor (the signed { region, owner, name,
// write }); a root checks it to enforce the recursion guard. Changing this string
// is a flag-day for metric discovery, so it is frozen as a named constant.
export const METRIC_NAMESPACE = 'axona:metric:';

const TOPIC_ID_RE = /^[0-9a-f]{66}$/;   // regionByte(2) ‖ SHA-256(264-bit id) = 66 hex

/**
 * Derive the metric topic for a data topic.
 *
 * @param {string} dataTopicId  The data topic's RESOLVED id — 66 lowercase hex
 *                              chars from `deriveTopicId(descriptor)`. (Resolve
 *                              the descriptor first; this helper is pure/sync.)
 * @returns {{region:number, name:string}} An OPEN topic descriptor (no owner →
 *   write defaults to 'open'). Region byte is inherited from the data topic id so
 *   the metric topic lives in the same regional keyspace band as its data topic.
 *
 * The metric topic is intentionally open + advisory: anyone can publish to it, so
 * a subscriber must treat the snapshot as a hint and check env.signerPubkey if it
 * wants to trust only the rooting relay. The protocol does not (and cannot,
 * statelessly) prove a metric snapshot is authoritative.
 */
export function metricTopic(dataTopicId) {
  const id = String(dataTopicId).trim().toLowerCase();
  if (!TOPIC_ID_RE.test(id)) {
    throw new Error(`metricTopic: expected a 66-hex topic id, got ${JSON.stringify(dataTopicId)}`);
  }
  // Inherit the region byte so deriveTopicId({region, name}) keeps the metric
  // topic in the same regional band (and resolveRegion accepts a numeric code).
  const region = parseInt(id.slice(0, 2), 16);
  return { region, name: METRIC_NAMESPACE + id };
}

/** True if `name` is in the reserved metric namespace. */
export function isMetricTopicName(name) {
  return typeof name === 'string' && name.startsWith(METRIC_NAMESPACE);
}

/**
 * Recursion guard. True if `descriptor` is a metric topic — a root MUST NOT
 * compute and publish metrics for these (metrics-of-metrics never terminates).
 * Accepts a { name } descriptor (preferred — the wire form) or a bare name string.
 */
export function isMetricTopic(descriptor) {
  if (typeof descriptor === 'string') return isMetricTopicName(descriptor);
  return !!descriptor && isMetricTopicName(descriptor.name);
}

/**
 * Recover the data-topic id a metric topic was derived from, or null if the name
 * is not in the metric namespace. The inverse of metricTopic()'s name rule —
 * handy for a root that wants to log "metrics for <dataId>".
 */
export function dataTopicIdOf(descriptor) {
  const name = typeof descriptor === 'string' ? descriptor : descriptor?.name;
  if (!isMetricTopicName(name)) return null;
  const id = name.slice(METRIC_NAMESPACE.length).toLowerCase();
  return TOPIC_ID_RE.test(id) ? id : null;
}
