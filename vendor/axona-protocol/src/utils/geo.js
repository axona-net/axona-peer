// Geographic utilities for the DHT simulator
// All distances in km, times in ms

export const EARTH_RADIUS_KM = 6371;

// Antipodal distance = half the great circle circumference
export const MAX_GREAT_CIRCLE_KM = Math.PI * EARTH_RADIUS_KM; // ~20,015 km

// Propagation delay constants (configurable via setLatencyParams)
let MAX_PROPAGATION_MS = 150;  // one-way propagation ms for antipodal nodes (~20,015 km)
                                // Real antipodal RTT ≈ 300 ms; divide by 2 for one-way.
                                // roundTripLatency() doubles this, so antipodal RTT = 2*(150+10) = 320 ms.
let HOP_COST_MS = 10;          // ms processing overhead per one-way message

export function setLatencyParams(maxProp, hopCost) {
  MAX_PROPAGATION_MS = maxProp;
  HOP_COST_MS = hopCost;
}

export function getLatencyParams() {
  return { maxPropagation: MAX_PROPAGATION_MS, hopCost: HOP_COST_MS };
}

/**
 * Haversine great-circle distance between two lat/lng points.
 * @returns {number} Distance in km
 */
export function haversine(lat1, lng1, lat2, lng2) {
  const toRad = x => x * Math.PI / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * One-way propagation delay based on great-circle distance.
 * Antipodal nodes (max distance) → MAX_PROPAGATION_MS.
 * @returns {number} Propagation delay in ms
 */
export function propagationDelay(node1, node2) {
  const dist = haversine(node1.lat, node1.lng, node2.lat, node2.lng);
  return (dist / MAX_GREAT_CIRCLE_KM) * MAX_PROPAGATION_MS;
}

/**
 * Total one-way message latency: propagation + hop processing cost.
 */
export function messageLatency(node1, node2) {
  return propagationDelay(node1, node2) + HOP_COST_MS;
}

/**
 * Round-trip latency (send + receive) between two nodes.
 */
export function roundTripLatency(node1, node2) {
  return 2 * messageLatency(node1, node2);
}

/**
 * Convert lat/lng to Three.js-compatible XYZ on a unit sphere.
 * Convention: Y-up, north pole at (0,1,0).
 */
export function latLngToXYZ(lat, lng, radius = 1) {
  const phi = (90 - lat) * Math.PI / 180;   // polar angle from north pole
  const theta = (lng + 180) * Math.PI / 180; // azimuthal angle
  return {
    x: -radius * Math.sin(phi) * Math.cos(theta),
    y:  radius * Math.cos(phi),
    z:  radius * Math.sin(phi) * Math.sin(theta),
  };
}

/**
 * Convert a unit XYZ vector back to lat/lng.
 */
export function xyzToLatLng(x, y, z) {
  const lat = 90 - Math.acos(Math.max(-1, Math.min(1, y))) * 180 / Math.PI;
  const lng = Math.atan2(z, -x) * 180 / Math.PI - 180;
  return { lat, lng: ((lng + 540) % 360) - 180 };
}

/**
 * Generate a cryptographically random 32-bit unsigned integer.
 */
export function randomU32() {
  return crypto.getRandomValues(new Uint32Array(1))[0];
}

/**
 * Generate a cryptographically random 64-bit unsigned BigInt.
 */
export function randomU64() {
  const arr = crypto.getRandomValues(new Uint32Array(2));
  return (BigInt(arr[0]) << 32n) | BigInt(arr[1]);
}

/**
 * Count leading zeros of a 64-bit BigInt (0n returns 64).
 */
export function clz64(n) {
  if (n === 0n) return 64;
  const hi = Number(n >> 32n);
  if (hi !== 0) return Math.clz32(hi);
  return 32 + Math.clz32(Number(n & 0xFFFFFFFFn));
}

/**
 * Collect up to k nodes from XOR-bucket b relative to selfId.
 * Internal helper — extracts the binary-search + range scan used by
 * buildXorRoutingTable so it can be called per-bucket during stratified
 * allocation.
 *
 * @param {BigInt}   selfId  Source node ID (64-bit BigInt).
 * @param {object[]} sorted  All nodes sorted ascending by .id.
 * @param {number}   b       Bucket index 0–63 (highest differing bit).
 * @param {number}   k       Max nodes to return.
 * @returns {object[]}       Up to k peers whose XOR distance puts them in bucket b.
 */
export function _collectBucket(selfId, sorted, b, k) {
  const bBig = BigInt(b);
  let rangeStart, rangeEnd;

  if (b < 63) {
    const highBits    = selfId >> (bBig + 1n);
    const flippedBitB = ((selfId >> bBig) & 1n) ^ 1n;
    const peerPfx     = (highBits << 1n) | flippedBitB;
    rangeStart        = peerPfx << bBig;
    rangeEnd          = rangeStart | ((1n << bBig) - 1n);
  } else {
    // b = 63: MSB differs — peers live in the opposite half of the ID space.
    rangeStart = (selfId >> 63n) === 0n ? (1n << 63n) : 0n;
    rangeEnd   = (selfId >> 63n) === 0n ? 0xFFFFFFFFFFFFFFFFn : ((1n << 63n) - 1n);
  }

  // Binary search for the first index >= rangeStart.
  let lo = 0, hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (sorted[mid].id < rangeStart) lo = mid + 1; else hi = mid;
  }

  // Reservoir-sample up to k peers from [rangeStart, rangeEnd]. Reservoir
  // sampling (instead of a deterministic "first k") is critical under a
  // bilateral connection cap: if every node that falls in the same bucket
  // range were to always pick the same first-k candidates, those candidates
  // saturate instantly and the rest of the range's capacity is wasted.
  // Random sampling spreads connection demand across the full stratum.
  const reservoir = [];
  let count = 0;
  for (let i = lo; i < sorted.length; i++) {
    if (sorted[i].id > rangeEnd) break;
    if (count < k) {
      reservoir.push(sorted[i]);
    } else {
      const j = Math.floor(Math.random() * (count + 1));
      if (j < k) reservoir[j] = sorted[i];
    }
    count++;
  }
  return reservoir;
}

/**
 * O(n log n) XOR-bucket routing table builder for 64-bit BigInt node IDs.
 *
 * WITHOUT a connection budget (maxTotal = Infinity):
 *   Sequential fill — up to k peers per bucket, b=0 through b=63.
 *   All protocols use this path when no web-limit is set.
 *
 * WITH a connection budget (maxTotal finite, i.e. web-limit mode):
 *   Stratified two-phase allocation applied to ALL protocols.
 *
 *   Why sequential fill fails under a budget cap:
 *
 *   For random-ID protocols (Kademlia): buckets b=0–55 are sparse but b=56–63
 *   each have ~N/2^(64-b) nodes.  With N=5000 and maxTotal=50, sequential fill
 *   stops around b=57, leaving b=58–63 completely empty.  Lookups then fail to
 *   reach nodes whose IDs differ in the top bits, which — for a random network —
 *   is essentially everyone.  The new exact-match `found` metric exposes this
 *   as a ~2% success rate.
 *
 *   For geo-prefix protocols (geo8, geo16): buckets b=0–55 are ALL within the
 *   same geographic cell, so sequential fill exhausts the entire budget on
 *   local nodes before reaching the inter-cell buckets (b=56–63) that provide
 *   continental and global connectivity.
 *
 *   Stratified fix — two phases:
 *   Phase 1 — breadth: 1 peer per non-empty bucket (full XOR-range coverage,
 *                       guarantees at least one connection at every occupied
 *                       XOR-distance level including global b=63 buckets).
 *   Phase 2 — depth:   remaining budget to the highest-b buckets first (b=63
 *                       down), maximising global-reach diversity.
 *
 * @param {BigInt}   selfId              64-bit unsigned BigInt node ID.
 * @param {object[]} sorted              Nodes sorted ascending by .id (BigInt).
 * @param {number}   k                   Max peers per bucket.
 * @param {number}   [maxTotal=Infinity] Hard cap on total connections (web-limit).
 * @returns {object[]}                   Peer nodes to add (never includes selfId).
 */
export function buildXorRoutingTable(selfId, sorted, k, maxTotal = Infinity) {

  // ── No budget cap: sequential fill (unchanged behaviour) ─────────────────
  if (!isFinite(maxTotal)) {
    const result = [];
    for (let b = 0; b <= 63; b++) {
      result.push(..._collectBucket(selfId, sorted, b, k));
    }
    return result;
  }

  // ── Budget-capped: stratified allocation for all protocols ────────────────
  // Step 1: collect up to k candidates per bucket (O(n) total via binary search).
  const buckets = [];
  for (let b = 0; b <= 63; b++) {
    buckets.push(_collectBucket(selfId, sorted, b, k));
  }

  const allotted = new Array(64).fill(0);
  let remaining = maxTotal;

  // Phase 1: 1 per non-empty bucket — guarantees at least one connection at
  // every XOR-distance level including the global high-b buckets.
  for (let b = 0; b <= 63 && remaining > 0; b++) {
    if (buckets[b].length > 0) {
      allotted[b] = 1;
      remaining--;
    }
  }

  // Phase 2: fill highest-b buckets first with remaining budget.
  // Prioritises global-reach diversity (large XOR distance) over local
  // redundancy (small XOR distance).
  for (let b = 63; b >= 0 && remaining > 0; b--) {
    const canAdd = Math.min(buckets[b].length - allotted[b], k - allotted[b], remaining);
    if (canAdd > 0) {
      allotted[b] += canAdd;
      remaining   -= canAdd;
    }
  }

  // Step 3: assemble in bucket order (b=0 first, b=63 last).
  const result = [];
  for (let b = 0; b <= 63; b++) {
    for (let i = 0; i < allotted[b]; i++) result.push(buckets[b][i]);
  }
  return result;
}

/**
 * Collect peers from the intra-cell XOR buckets b=0 through b<numBuckets.
 * These are nodes that share the same geographic prefix in their IDs
 * (same S2 cell for G-DHT), giving locally clustered connections.
 *
 * @param {BigInt}   selfId      64-bit unsigned BigInt node ID.
 * @param {object[]} sorted      All nodes sorted ascending by .id.
 * @param {number}   k           Max peers per bucket.
 * @param {number}   numBuckets  Number of low-order XOR buckets to collect
 *                               (geo8 → 56, geo16 → 48).
 * @returns {object[]}           Peers from buckets 0 through numBuckets-1.
 */
export function buildIntraCellTable(selfId, sorted, k, numBuckets) {
  const result = [];
  for (let b = 0; b < numBuckets; b++) {
    result.push(..._collectBucket(selfId, sorted, b, k));
  }
  return result;
}

/**
 * Collect peers from the inter-cell XOR buckets b=startBucket through b=63.
 * These buckets cover nodes whose geographic prefix differs from selfId, giving
 * one representative per geographic-prefix bit — the Kademlia halving guarantee
 * applied to the inter-cell key space.
 *
 * With k=1 this guarantees exactly one peer per bucket (8 peers for geo8),
 * enough to ensure every target in the global key space is reachable.
 *
 * @param {BigInt}   selfId       64-bit unsigned BigInt node ID.
 * @param {object[]} sorted       All nodes sorted ascending by .id.
 * @param {number}   k            Max peers per bucket.
 * @param {number}   startBucket  First inter-cell bucket index (64 - geoBits).
 *                                geo8 → 56, geo16 → 48.
 * @returns {object[]}            Peers from buckets startBucket through 63.
 */
export function buildInterCellTable(selfId, sorted, k, startBucket) {
  const result = [];
  for (let b = startBucket; b <= 63; b++) {
    result.push(..._collectBucket(selfId, sorted, b, k));
  }
  return result;
}

/**
 * Uniform random sample from a pool of nodes, excluding any IDs in excludeIds.
 * Uses a partial Fisher-Yates shuffle — O(count) time, O(pool.length) space.
 *
 * @param {object[]} pool        All candidate nodes.
 * @param {number}   count       How many to select.
 * @param {Set}      excludeIds  Node IDs to skip (already selected or self).
 * @returns {object[]}           Up to count randomly chosen live nodes.
 */
export function reservoirSample(pool, count, excludeIds = new Set()) {
  const available = pool.filter(n => n.alive && !excludeIds.has(n.id));
  const take = Math.min(count, available.length);
  for (let i = 0; i < take; i++) {
    const j = i + Math.floor(Math.random() * (available.length - i));
    const tmp = available[i]; available[i] = available[j]; available[j] = tmp;
  }
  return available.slice(0, take);
}

// ── Continent classification ──────────────────────────────────────────────

/**
 * Bounding-box continent classification.
 * Order matters: OC before AS so Australia/NZ nodes aren't absorbed by Asia.
 */
const CONTINENT_BOXES = [
  { id: 'NA', minLat: 15,  maxLat: 85,  minLng: -170, maxLng: -50  },
  { id: 'SA', minLat: -60, maxLat: 15,  minLng: -90,  maxLng: -30  },
  { id: 'EU', minLat: 35,  maxLat: 72,  minLng: -25,  maxLng: 45   },
  { id: 'AF', minLat: -40, maxLat: 40,  minLng: -20,  maxLng: 55   },
  { id: 'OC', minLat: -50, maxLat: 10,  minLng: 110,  maxLng: 180  },
  { id: 'AS', minLat: 5,   maxLat: 80,  minLng: 45,   maxLng: 180  },
];

export const CONTINENT_NAMES = {
  NA: 'N.Am.', SA: 'S.Am.', EU: 'Europe',
  AF: 'Africa', AS: 'Asia',  OC: 'Oceania',
};

/**
 * Return the continent code ('NA', 'SA', 'EU', 'AF', 'AS', 'OC') for a
 * lat/lng point, or null if unclassified (open ocean, polar regions).
 */
export function continentOf(lat, lng) {
  for (const b of CONTINENT_BOXES) {
    if (lat >= b.minLat && lat <= b.maxLat &&
        lng >= b.minLng && lng <= b.maxLng) return b.id;
  }
  return null;
}

/**
 * Compute statistics over an array of numbers.
 */
export function computeStats(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    count: n,
    mean: sum / n,
    median: sorted[Math.floor(n / 2)],
    p25: sorted[Math.floor(n * 0.25)],
    p75: sorted[Math.floor(n * 0.75)],
    p95: sorted[Math.floor(n * 0.95)],
    p99: sorted[Math.floor(n * 0.99)],
    min: sorted[0],
    max: sorted[n - 1],
  };
}
