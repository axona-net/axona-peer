// =====================================================================
// identity.js — derive (and persist) this peer's 64-bit node ID.
//
// Production node-ID layout (NH-1):
//
//     ┌────────────────────────┬───────────────────────────────────────┐
//     │  S2 cell prefix        │  pubkey-derived random bits            │
//     │  8 bits  (GEO_BITS=8)  │  56 bits                               │
//     └────────────────────────┴───────────────────────────────────────┘
//
// The S2 prefix is `geoCellId(lat, lng, 8)` — Google's S2
// Hilbert-curve cell, truncated to 8 bits (≈256 cells covering
// Earth).  It makes XOR distance approximately track geographic
// distance, which is what NH-1's locality routing exploits.
//
// The bottom 56 bits are stable per browser profile: generated once
// on first visit, persisted in localStorage, reused thereafter.  A
// returning user lands on the same peer ID, which is what makes
// reputation / known-node fast-paths possible.  (Future: those 56
// bits become a truncated hash of the user's Ed25519 public key,
// arriving with the Web-Crypto port of post.js.)
//
// Region fallback: if geolocation is denied / unavailable, the
// caller picks a region from `REGIONS` (a hand-curated list of 15
// canonical points covering the populated world).  The chosen
// region's lat/lng feeds geoCellId for the prefix.
//
// API:
//   getCurrentIdentity()      → null | Identity   (synchronous read of localStorage)
//   deriveIdentity(opts)      → Promise<Identity> (creates + persists)
//   rotateIdentity(opts)      → Promise<Identity> (forces fresh bits)
//   forgetIdentity()          → void              (clears localStorage)
//
// Where Identity = { id: bigint, geoBits: number, region: {...} }
// =====================================================================

import { geoCellId } from '../vendor/axona-protocol/src/utils/s2.js';
import { randomU64 } from '../vendor/axona-protocol/src/utils/geo.js';

const STORAGE_KEY = 'axona.identity.v1';
const GEO_BITS    = 8;

// Identity store.  We use sessionStorage (per-tab) by default instead
// of localStorage (per-origin) so multiple tabs of the same browser
// can be distinct Axona nodes — critical for pubsub testing, where
// shared identity collapses N tabs into 1 logical node and the bridge's
// WSTransport binds only the most-recently-handshook tab.
//
// Tradeoff: a returning visitor who closes & reopens the tab gets a
// fresh identity.  That's the right call during the "testing era"; we
// can move back to localStorage (or offer the user a checkbox) when we
// have a proper returning-user UX story.
//
// `IDENTITY_STORE_KIND === 'local'` (override via the URL query string
// `?identityStore=local`) reverts to the old behaviour for users who
// want returning-visitor persistence.
function identityStore() {
  if (typeof sessionStorage === 'undefined') return null;
  if (typeof location === 'undefined')       return sessionStorage;
  const qs = new URLSearchParams(location.search);
  return qs.get('identityStore') === 'local' && typeof localStorage !== 'undefined'
    ? localStorage
    : sessionStorage;
}

// One-shot cleanup: tabs upgrading from 0.14.0 → 0.14.2 have a stale
// localStorage identity that the old code was reading.  Wipe it so the
// new per-tab sessionStorage path actually creates a fresh identity
// instead of falling back to the shared-across-tabs localStorage one.
if (typeof localStorage !== 'undefined') {
  try { localStorage.removeItem(STORAGE_KEY); } catch {}
}

/**
 * Hand-curated region list for the geolocation fallback.  Fifteen
 * canonical lat/lng points spread across populated continents.  The
 * user picks one when geolocation is unavailable; it sets only the
 * top 8 ID bits (the S2 cell), so picking "wrong" lands you in a
 * different routing-table neighborhood but doesn't break routing.
 */
export const REGIONS = Object.freeze([
  // ── North America ────────────────────────────────────────────────
  { id: 'us-east',   label: 'US East (Virginia)',         lat:   38.0, lng:  -77.0 },
  { id: 'us-central',label: 'US Central (Dallas)',        lat:   32.8, lng:  -96.8 },
  { id: 'us-west',   label: 'US West (California)',       lat:   37.0, lng: -122.0 },
  { id: 'canada',    label: 'Canada (Toronto)',            lat:   43.7, lng:  -79.4 },
  { id: 'mexico',    label: 'Mexico (Mexico City)',        lat:   19.4, lng:  -99.1 },
  // ── Europe ────────────────────────────────────────────────────────
  { id: 'uk',        label: 'UK (London)',                 lat:   51.5, lng:   -0.1 },
  { id: 'eu-west',   label: 'EU West (Paris)',             lat:   48.9, lng:    2.3 },
  { id: 'eu-central',label: 'EU Central (Frankfurt)',      lat:   50.1, lng:    8.7 },
  { id: 'eu-north',  label: 'EU North (Stockholm)',        lat:   59.3, lng:   18.1 },
  // ── Asia ──────────────────────────────────────────────────────────
  { id: 'asia-east',     label: 'Asia East (Tokyo)',       lat:   35.7, lng:  139.7 },
  { id: 'asia-southeast',label: 'Asia SE (Singapore)',     lat:    1.3, lng:  103.8 },
  { id: 'asia-south',    label: 'Asia South (Mumbai)',     lat:   19.1, lng:   72.9 },
  // ── Southern hemisphere ──────────────────────────────────────────
  { id: 'sa-east',   label: 'SA East (São Paulo)',         lat:  -23.5, lng:  -46.6 },
  { id: 'africa',    label: 'Africa (Johannesburg)',       lat:  -26.2, lng:   28.0 },
  { id: 'oceania',   label: 'Oceania (Sydney)',            lat:  -33.9, lng:  151.2 },
]);

/**
 * @typedef {Object} Identity
 * @property {bigint}  id          full 64-bit node ID
 * @property {number}  geoBits     prefix width (8)
 * @property {Object}  region      { id, label, lat, lng, source }
 * @property {number}  createdAt   epoch ms when persisted
 */

/**
 * Synchronously read the stored identity, if any.  Returns null when
 * localStorage is empty / corrupted / unavailable.  Useful for the
 * "do we need to prompt the user?" check at startup.
 */
export function getCurrentIdentity() {
  const store = identityStore();
  if (!store) return null;
  let raw;
  try { raw = store.getItem(STORAGE_KEY); }
  catch { return null; }
  if (!raw) return null;
  try {
    const stored = JSON.parse(raw);
    if (typeof stored.id !== 'string') return null;
    return {
      id:        BigInt('0x' + stored.id),
      geoBits:   stored.geoBits ?? GEO_BITS,
      region:    stored.region  ?? { source: 'unknown' },
      createdAt: stored.createdAt ?? 0,
    };
  } catch {
    return null;
  }
}

/**
 * Get (or create) this peer's identity.  Order of operations:
 *   1. If localStorage has an existing identity AND !opts.rotate,
 *      return it.  Stable IDs are the default.
 *   2. Resolve the geographic region:
 *      a. If opts.region is provided (the UI's dropdown choice),
 *         use it.
 *      b. Else try navigator.geolocation, with a 5-second timeout.
 *      c. Else fall back to opts.regionFallback or REGIONS[0].
 *   3. Compute the S2 prefix from the resolved lat/lng.
 *   4. Generate 56 random bits via crypto.getRandomValues.
 *   5. Persist + return.
 *
 * @param {Object}   opts
 * @param {Object}   [opts.region]          {lat, lng, id?, label?} — when set,
 *                                          skips the geolocation prompt
 * @param {Object}   [opts.regionFallback]  same shape, used when geolocation fails
 * @param {boolean}  [opts.rotate]          force fresh bits even if persisted
 * @returns {Promise<Identity>}
 */
export async function deriveIdentity(opts = {}) {
  if (!opts.rotate) {
    const existing = getCurrentIdentity();
    if (existing) return existing;
  }

  const region = await resolveRegion(opts);
  const prefix = geoCellId(region.lat, region.lng, GEO_BITS);
  const shift  = 64n - BigInt(GEO_BITS);
  const bottom = randomU64() & ((1n << shift) - 1n);
  const id     = (BigInt(prefix) << shift) | bottom;

  const identity = {
    id,
    geoBits:   GEO_BITS,
    region,
    createdAt: Date.now(),
  };

  persist(identity);
  return identity;
}

/**
 * Force a fresh identity — discards localStorage and generates new
 * bottom-56-bits.  The region is re-resolved (geolocation prompt
 * again unless `opts.region` is supplied).  Used by the UI's
 * "rotate identity" affordance when the user wants per-session
 * anonymity.
 */
export async function rotateIdentity(opts = {}) {
  return deriveIdentity({ ...opts, rotate: true });
}

/** Clear stored identity.  Subsequent `deriveIdentity()` starts fresh.
 *  Wipes both stores so the cleanup is unambiguous (e.g. callers
 *  toggling between session + local). */
export function forgetIdentity() {
  for (const store of [
    typeof sessionStorage !== 'undefined' ? sessionStorage : null,
    typeof localStorage   !== 'undefined' ? localStorage   : null,
  ]) {
    if (!store) continue;
    try { store.removeItem(STORAGE_KEY); } catch { /* quota or disabled */ }
  }
}

// ── Internal ────────────────────────────────────────────────────────

async function resolveRegion(opts) {
  // (a) Explicit region from caller (dropdown selection)
  if (opts.region && Number.isFinite(opts.region.lat) && Number.isFinite(opts.region.lng)) {
    return { ...opts.region, source: 'user-selected' };
  }
  // (b) Browser geolocation
  if (typeof navigator !== 'undefined' && navigator.geolocation) {
    try {
      const pos = await new Promise((resolve, reject) => {
        navigator.geolocation.getCurrentPosition(resolve, reject, {
          timeout:    5000,
          maximumAge: 24 * 60 * 60 * 1000,
        });
      });
      return {
        id:     'auto-geolocation',
        label:  `Auto (${pos.coords.latitude.toFixed(2)}, ${pos.coords.longitude.toFixed(2)})`,
        lat:    pos.coords.latitude,
        lng:    pos.coords.longitude,
        source: 'geolocation',
      };
    } catch { /* fall through */ }
  }
  // (c) Caller-supplied fallback
  if (opts.regionFallback && Number.isFinite(opts.regionFallback.lat)) {
    return { ...opts.regionFallback, source: 'fallback' };
  }
  // (d) Final fallback: first region in the table
  return { ...REGIONS[0], source: 'default' };
}

function persist(identity) {
  if (typeof localStorage === 'undefined') return;
  const serialized = JSON.stringify({
    id:        identity.id.toString(16).padStart(16, '0'),
    geoBits:   identity.geoBits,
    region:    identity.region,
    createdAt: identity.createdAt,
  });
  const store = identityStore();
  if (!store) return;
  try { store.setItem(STORAGE_KEY, serialized); }
  catch { /* quota exceeded or storage disabled — silent fail */ }
}
