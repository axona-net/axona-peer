// =====================================================================
// bridgeBook.js — local, first-party bridge directory + reputation store.
//
// Persists, in localStorage: directory entries this peer has seen (with
// tenure = firstSeen) and this peer's OWN observed connect outcomes per
// bridge (okCount/failCount/recency/latency). Ranking is delegated to the
// kernel's rankBridges (the layered model: roots → personally-succeeded →
// fresh by proximity+tenure). Reputation is first-party and therefore
// unforgeable — it's this client's lived experience, not anything a bridge
// claims. See @axona/protocol/bridgeDirectory.
// =====================================================================

import {
  validateBridgeEntry,
  rankBridges,
  BRIDGE_ENTRY_MAX_AGE_MS,
} from '../vendor/axona-protocol/src/bridgeDirectory.js';

const KEY = 'axona:bridgeBook:v1';

function load() {
  try { return JSON.parse(localStorage.getItem(KEY)) || {}; }
  catch { return {}; }
}
function save(s) {
  try { localStorage.setItem(KEY, JSON.stringify(s)); } catch { /* private mode / quota */ }
}

/**
 * Merge a batch of received directory messages.
 * @param {Array<{entry:object, signerKey:string}>} received
 * @returns {object} the current entries map (url -> entry)
 */
export function mergeDirectory(received, now = Date.now()) {
  const s = load();
  s.entries = s.entries || {};
  for (const { entry, signerKey } of received) {
    const v = validateBridgeEntry(entry);
    if (!v) continue;
    const prev = s.entries[v.url];
    if (prev && (prev.ts || 0) >= v.ts) continue;        // keep the latest per url
    s.entries[v.url] = {
      ...v,
      signerKey: signerKey || prev?.signerKey || null,
      firstSeen: prev?.firstSeen || now,                 // tenure: first time we ever saw it
    };
  }
  for (const url of Object.keys(s.entries)) {            // drop dead bridges
    if (now - (s.entries[url].ts || 0) > BRIDGE_ENTRY_MAX_AGE_MS) delete s.entries[url];
  }
  save(s);
  return s.entries;
}

/**
 * Record THIS client's outcome connecting to `url`.
 * @param {string} url
 * @param {{ok:boolean, timeToMeshMs?:number, rttMs?:number}} o
 */
export function recordOutcome(url, { ok, timeToMeshMs, rttMs } = {}, now = Date.now()) {
  if (!url) return;
  const s = load();
  s.rep = s.rep || {};
  const r = s.rep[url] || { okCount: 0, failCount: 0 };
  if (ok) { r.okCount = (r.okCount || 0) + 1; r.lastOkAt = now; }
  else    { r.failCount = (r.failCount || 0) + 1; }
  if (typeof timeToMeshMs === 'number') r.lastTimeToMeshMs = timeToMeshMs;
  if (typeof rttMs === 'number')        r.lastRttMs = rttMs;
  s.rep[url] = r;
  save(s);
}

/**
 * Ranked failover candidate URLs (best first). `roots` (the configured
 * primary) always lead; the directory only adds fallbacks.
 * @param {{roots?:string[], self?:{lat:number,lng:number}|null}} o
 * @returns {string[]}
 */
export function candidates({ roots = [], self = null, now = Date.now() } = {}) {
  const s = load();
  const entries = Object.values(s.entries || {});
  const reputation = {};
  for (const [url, r] of Object.entries(s.rep || {})) reputation[url] = { ...r };
  // Fold tenure (firstSeen) into the reputation map so rankBridges can use it.
  for (const e of entries) reputation[e.url] = { ...(reputation[e.url] || {}), firstSeen: e.firstSeen };
  return rankBridges({ roots, entries, reputation, self, now }).map((c) => c.url);
}
