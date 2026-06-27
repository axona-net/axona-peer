// =====================================================================
// nodeid.js — derive a 264-bit nodeId from (pubkey, lat, lng).
//
//     nodeId = [8-bit S2 prefix from geoCellId(lat, lng, 8)]
//           || [256-bit SHA-256(pubkey raw bytes)]
//
// Deterministic: same (pubkey, lat, lng) always produces the same id.
// Apps that need a stable identity across reloads load the persisted
// pubkey + region and recompute the id.
// =====================================================================

import { geoCellId }        from '../utils/s2.js';
import { assembleId, toHex, HASH_MASK } from '../utils/hexid.js';

/**
 * Compute the 264-bit nodeId for a given Ed25519 public key + region.
 *
 * @param {Uint8Array} pubkeyBytes  32 raw bytes (Ed25519 public key).
 * @param {number}     lat          latitude in degrees, [-90, 90].
 * @param {number}     lng          longitude in degrees, [-180, 180].
 * @returns {Promise<bigint>}       264-bit nodeId.
 */
export async function computeNodeIdBigInt(pubkeyBytes, lat, lng) {
  if (!(pubkeyBytes instanceof Uint8Array) || pubkeyBytes.length !== 32) {
    throw new TypeError('computeNodeIdBigInt: pubkeyBytes must be 32-byte Uint8Array');
  }
  const s2Prefix = geoCellId(lat, lng, 8);
  const buf      = await crypto.subtle.digest('SHA-256', pubkeyBytes);
  const hashHex  = bytesToHex(new Uint8Array(buf));
  // Mask to the active hash width: full 256 bits in production, truncated in a
  // shrunk sim keyspace profile (e.g. 64-bit → 72-bit nodeId).
  const hash     = BigInt('0x' + hashHex) & HASH_MASK;
  return assembleId(s2Prefix, hash);
}

/**
 * Compute the 66-char hex nodeId.  Convenience wrapper around
 * computeNodeIdBigInt + toHex; what apps see at API boundaries.
 *
 * @param {Uint8Array} pubkeyBytes
 * @param {number}     lat
 * @param {number}     lng
 * @returns {Promise<string>}  66-char lowercase hex.
 */
export async function computeNodeId(pubkeyBytes, lat, lng) {
  const big = await computeNodeIdBigInt(pubkeyBytes, lat, lng);
  return toHex(big);
}

// ── internal ─────────────────────────────────────────────────────────

function bytesToHex(bytes) {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) hex += bytes[i].toString(16).padStart(2, '0');
  return hex;
}
