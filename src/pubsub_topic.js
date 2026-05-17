// =====================================================================
// pubsub_topic.js — derive a 64-bit Axona topic address from a
//                   user-friendly (region, event-name) pair.
//
// Layout matches identity.js' nodeId encoding so topics share the
// same key space as nodes and routing distances are comparable:
//
//     ┌──────────────┬─────────────────────────────────────────┐
//     │ 8-bit prefix │ 56-bit hash(event-name)                 │
//     └──────────────┴─────────────────────────────────────────┘
//
// prefix:    geoCellId(region.lat, region.lng, 8) — same Hilbert-curve
//            S2 cell used for nodeIds.  Geographically-close
//            publishers and subscribers collide on the prefix.
// hash:      first 7 bytes of SHA-256(event-name) interpreted as a
//            big-endian 56-bit unsigned integer.
//
// Web Crypto subtle.digest is async; this module exports an async
// helper.  Cache hot topics on the call site if the cost matters
// (one SHA-256 ≈ 0.1ms, fine for interactive UI usage).
// =====================================================================

import { geoCellId } from '../vendor/axona-protocol/src/utils/s2.js';

const GEO_BITS = 8;
const MASK_56  = (1n << 56n) - 1n;

/**
 * @param {{lat:number, lng:number}} region  any object with .lat/.lng
 * @param {string} eventName                 human-friendly topic name
 * @returns {Promise<bigint>}                64-bit topic key
 */
export async function deriveTopicKey(region, eventName) {
  if (!region || typeof region.lat !== 'number' || typeof region.lng !== 'number') {
    throw new TypeError('deriveTopicKey: region.lat + region.lng required');
  }
  if (typeof eventName !== 'string' || eventName.length === 0) {
    throw new TypeError('deriveTopicKey: eventName must be a non-empty string');
  }

  const prefix = geoCellId(region.lat, region.lng, GEO_BITS);    // 0..255

  const enc = new TextEncoder();
  const buf = await crypto.subtle.digest('SHA-256', enc.encode(eventName));
  const bytes = new Uint8Array(buf);

  // First 7 bytes → 56-bit BigInt (big-endian).
  let bottom = 0n;
  for (let i = 0; i < 7; i++) bottom = (bottom << 8n) | BigInt(bytes[i]);

  return (BigInt(prefix) << 56n) | (bottom & MASK_56);
}

/**
 * Format a topic key as a 16-char lowercase hex string — same
 * convention as idToHex() for nodeIds.
 */
export function topicKeyToHex(key) {
  return key.toString(16).padStart(16, '0');
}

/** Extract the 8-bit prefix from a topic key. */
export function topicPrefix(key) {
  return Number((key >> 56n) & 0xFFn);
}
