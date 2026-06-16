// =====================================================================
// bridgeDirectory.js — the bridge directory: how bridges advertise
// themselves and how a client ranks them for failover.
//
// WHY THIS EXISTS — making the network hard to stop. The bridge is the
// network's one semi-centralized touchpoint: browsers can't accept inbound
// connections, so a new peer needs *some* rendezvous to reach the mesh. A
// single hard-coded bridge is a single point of failure — block its
// domain/IP or take down its host and new peers can't bootstrap. The
// directory turns that single point into a MOVING TARGET: every bridge
// advertises its access point here and every node collects the set on
// launch, so there's no one address to block, a downed bridge is failed
// over transparently, and a newly stood-up bridge is discovered without a
// client update. Because every bridge also federates into the mesh as a
// node, the directory lives IN the mesh — there's no central registry to
// seize. The goal is resilience: the network stays reachable even at its
// weakest point, the bridge.
//
// WHAT IT DOES NOT DO. The directory does NOT by itself prevent bridge
// SYBIL attacks or FALSE ADVERTISING: anyone can sign + publish an entry
// for a bridge that doesn't exist, or flood it with many identities. The
// signature proves a stable pseudonymous publisher, not that the endpoint
// is real or honest. What bounds the damage is the CLIENT-SIDE RANKING
// below, not the directory: the configured primary is never auto-replaced,
// first-party (personally-observed) bridges outrank unknown ones, and a
// fake/dead endpoint just fails on connect and sinks to last resort — so a
// false entry costs at most one wasted failover attempt, never a hijack.
// Stronger admission (bridge-identity PoW, gossiped reputation, trusted-
// root attestation) is deferred hardening, not provided here.
//
// A bridge is only a rendezvous / WebRTC-signaling broker — it cannot
// impersonate a peer or read mesh content (mutual auth + channel binding
// + E2E DTLS). So a client can safely *discover* alternate bridges from
// an open, signed directory and fall over to one when its configured
// primary is unreachable.
//
// The directory is a PUBLIC pub/sub topic (`publisher: null`) every
// participant derives identically. A bridge publishes a SIGNED entry on
// launch and once a day. Clients dedup + rank + build reputation on the
// entry's `url` (the stable handle), NOT the signer: a bridge's transport id
// is EPHEMERAL (re-minted every restart, Phase 2), so the signer rotates — the
// signature still proves the entry wasn't tampered in transit, but identity/
// trust is URL- and first-party-experience-based. mergeDirectory keeps the
// latest entry per url, so a restarted bridge cleanly replaces its own.
//
// THE TRUST MODEL IS LAYERED + FIRST-PARTY (see rankBridges): a small set
// of configured/trusted roots first, then bridges this client has
// personally bootstrapped through (recency + latency), then fresh signed
// third-party entries by proximity (+ tenure). The configured primary is
// NEVER auto-replaced — the directory only adds fallbacks. Reputation is
// the client's OWN observed outcomes (unforgeable); network-gossiped
// reputation and a bridge-role PoW are deferred.
//
// This module is pure and env-neutral: the bridge uses buildBridgeEntry +
// the topic name to publish; a client uses validateBridgeEntry + rankBridges
// (apps wrap it with their own persistence, e.g. axona-peer/src/bridgeBook.js).
// =====================================================================

/** Public topic name every bridge publishes to and every client reads. */
export const BRIDGE_DIRECTORY_TOPIC = 'axona:bridge-directory';

/** Directory entries older than this are treated as dead (a live bridge
 *  republishes daily, so 48h tolerates one missed cycle). Matches the
 *  pub/sub replay-cache MAX_HOLD_MS ceiling. */
export const BRIDGE_ENTRY_MAX_AGE_MS = 48 * 60 * 60 * 1000;

/**
 * Build a directory entry payload (the `message` a bridge publishes).
 * Location here is the bridge voluntarily advertising itself — infra, not
 * an end-user post; the publisher-location-privacy property governs the
 * latter, not this.
 *
 * @param {object} o
 * @param {string} o.url    advertised endpoint, MUST be wss://
 * @param {number} o.lat
 * @param {number} o.lng
 * @param {string} [o.label]
 * @param {string} [o.ver]  bridge version string
 * @param {string|string[]} [o.turn]  the bridge's TURN endpoint(s), e.g.
 *        'turn:host:3478' / 'turns:host:5349' — advertised so a client that
 *        discovers this bridge also learns its TURN relay. Credentials are NOT
 *        carried here (they're short-lived; the bridge mints them in its welcome
 *        on connect) — only the endpoint URL(s).
 * @param {number} [o.ts]   ms; defaults to now
 * @returns {{url,lat,lng,label,ver,ts,turn?}}
 */
export function buildBridgeEntry({ url, lat, lng, label = '', ver = '', turn, ts = Date.now() }) {
  const entry = { url, lat, lng, label, ver, ts };
  const turns = normalizeTurn(turn);
  if (turns.length) entry.turn = turns;
  return entry;
}

/** Normalize a turn spec (string|array) to a clean array of turn(s):// URLs. */
function normalizeTurn(turn) {
  const arr = Array.isArray(turn) ? turn : (typeof turn === 'string' ? turn.split(',') : []);
  return arr.map((s) => String(s).trim()).filter((s) => /^turns?:[^\s]+$/.test(s)).slice(0, 4);
}

/**
 * Validate + normalize a received entry. Returns the normalized entry or
 * `null` if malformed. Only `wss://` endpoints are accepted (a directory
 * entry must never downgrade a client to an unencrypted bridge).
 *
 * @param {*} msg
 * @returns {{url,lat,lng,label,ver,ts}|null}
 */
export function validateBridgeEntry(msg) {
  if (!msg || typeof msg !== 'object') return null;
  const { url, lat, lng } = msg;
  if (typeof url !== 'string' || !/^wss:\/\/[^\s]+$/.test(url)) return null;
  if (typeof lat !== 'number' || !Number.isFinite(lat) || lat < -90 || lat > 90) return null;
  if (typeof lng !== 'number' || !Number.isFinite(lng) || lng < -180 || lng > 180) return null;
  const ts = typeof msg.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : 0;
  const entry = {
    url,
    lat,
    lng,
    label: typeof msg.label === 'string' ? msg.label.slice(0, 64) : '',
    ver:   typeof msg.ver === 'string' ? msg.ver.slice(0, 32) : '',
    ts,
  };
  const turns = normalizeTurn(msg.turn);     // advertised TURN endpoint(s), if any
  if (turns.length) entry.turn = turns;
  return entry;
}

/**
 * Great-circle distance in km between two {lat,lng} points (Haversine).
 * Used only to bias ranking toward nearby bridges; precision is irrelevant.
 */
export function haversineKm(a, b) {
  if (!a || !b || typeof a.lat !== 'number' || typeof b.lat !== 'number') return Infinity;
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const s = Math.sin(dLat / 2) ** 2 +
            Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(s)));
}

/**
 * Rank bridge URLs for a failover attempt — the layered, first-party model.
 *
 * Order:
 *   1. `roots` — configured/trusted bridges (e.g. the app's primary),
 *      verbatim and first. Never displaced by the directory.
 *   2. KNOWN-GOOD directory bridges — `reputation[url].okCount > 0`, i.e.
 *      ones THIS client has personally bootstrapped through; ranked by most
 *      recent success, then lower observed latency (time-to-mesh / RTT).
 *   3. FRESH directory bridges — never tried; ranked by proximity to `self`,
 *      tie-broken by tenure (older firstSeen first).
 * Bridges that have only ever failed for this client sink to the end.
 * Stale entries (older than `maxAgeMs`) are dropped. URLs already present as
 * a root are not repeated.
 *
 * @param {object}   o
 * @param {string[]} [o.roots=[]]        configured primary/trusted urls, in order
 * @param {Array}    [o.entries=[]]      normalized directory entries (validateBridgeEntry)
 * @param {object}   [o.reputation={}]   url -> { okCount, failCount, lastOkAt, lastRttMs, lastTimeToMeshMs, firstSeen }
 * @param {{lat,lng}}[o.self=null]       this client's location (for proximity)
 * @param {number}   [o.now=Date.now()]
 * @param {number}   [o.maxAgeMs=BRIDGE_ENTRY_MAX_AGE_MS]
 * @returns {Array<{url,source,reason}>} ordered candidates; source ∈ root|known|fresh
 */
export function rankBridges({
  roots = [],
  entries = [],
  reputation = {},
  self = null,
  now = Date.now(),
  maxAgeMs = BRIDGE_ENTRY_MAX_AGE_MS,
} = {}) {
  const out = [];
  const seen = new Set();

  for (const url of roots) {
    if (typeof url === 'string' && url && !seen.has(url)) {
      seen.add(url);
      out.push({ url, source: 'root', reason: 'configured' });
    }
  }

  const fresh = [];
  const known = [];
  const tried = [];   // tried-and-only-failed → last resort
  for (const e of entries) {
    if (!e || typeof e.url !== 'string' || seen.has(e.url)) continue;
    if (e.ts && now - e.ts > maxAgeMs) continue;            // stale → drop
    const rep = reputation[e.url] || {};
    const dist = haversineKm(self, e);
    if (rep.okCount > 0) {
      known.push({ e, rep, dist });
    } else if (rep.failCount > 0) {
      tried.push({ e, rep, dist });
    } else {
      fresh.push({ e, rep, dist });
    }
  }

  // Known-good: most recently successful first, then lower latency.
  known.sort((a, b) =>
    (b.rep.lastOkAt || 0) - (a.rep.lastOkAt || 0) ||
    (latency(a.rep) - latency(b.rep)));
  // Fresh: nearest first, then oldest tenure (longest-standing) first.
  fresh.sort((a, b) =>
    a.dist - b.dist ||
    ((a.rep.firstSeen || a.e.ts || Infinity) - (b.rep.firstSeen || b.e.ts || Infinity)));
  // Previously-failed: nearest first, but after everything else.
  tried.sort((a, b) => a.dist - b.dist);

  for (const { e } of known) { seen.add(e.url); out.push({ url: e.url, source: 'known', reason: 'prior-success' }); }
  for (const { e } of fresh) { seen.add(e.url); out.push({ url: e.url, source: 'fresh', reason: 'directory' }); }
  for (const { e } of tried) { seen.add(e.url); out.push({ url: e.url, source: 'fresh', reason: 'prior-failure' }); }

  return out;
}

function latency(rep) {
  const v = rep.lastTimeToMeshMs ?? rep.lastRttMs;
  return typeof v === 'number' && Number.isFinite(v) ? v : Infinity;
}
