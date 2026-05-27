/**
 * s2.js — Geographic cell ID via Google's S2 cube projection.
 *
 * Maps (lat, lng) to an 8-bit cell ID in [0, 192) using the standard
 * S2 unit-sphere-on-cube geometry.  Unlike a flat lat/lng partition,
 * S2 cells have near-equal area across the globe (within ~2× of each
 * other) including at the poles — which matters both for routing
 * locality and for the privacy property that no peer should be
 * uniquely identifiable by its cell prefix.
 *
 * Implementation summary:
 *
 *   1. (lat, lng)               →  unit-sphere (x, y, z)
 *   2. xyz                      →  face (0..5)  +  face-local (u, v) ∈ [-1,+1]²
 *   3. quadratic UV-to-ST       →  (s, t)       ∈ [0, 1]²            (equal-area)
 *   4. bin (s, t) into 4×8      →  (sBin, tBin)
 *   5. cellId = face·32 + sBin·8 + tBin                              ∈ [0, 192)
 *
 * The 4×8 partition is "S2 level 2.5" — one bit short of S2 level 3
 * (8×8 squares per face).  Cells are rectangular on each face but
 * 192 fits within 8 bits with 64 byte values left over (192..255)
 * reserved for system topics, future address-space extensions, etc.
 *
 * Face axes follow the Google S2 convention.  ST quadratic transform
 * follows S2 default ("quadratic" projection — equal-area within
 * ~1.7× across the globe, vs. ~3× for the linear projection).
 *
 * No external dependencies.  Reverses cleanly: cellId → lat/lng-center.
 */

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/** Number of cube faces (S2's hierarchical foundation). */
export const S2_FACES = 6;

/** Cell subdivisions per face axis at S2 level 3 (real S2). */
const S_BINS = 8;
const T_BINS = 8;

/** Cells per face at level 3. */
const CELLS_PER_FACE_L3 = S_BINS * T_BINS;            // 64

/** Cells per face after truncating level-3 Hilbert by one bit. */
const CELLS_PER_FACE = CELLS_PER_FACE_L3 >> 1;        // 32

/** Total valid 8-bit cell IDs. */
export const S2_CELL_COUNT = S2_FACES * CELLS_PER_FACE;   // 192

/** First reserved (invalid) 8-bit cell ID. */
export const S2_RESERVED_FROM = S2_CELL_COUNT;            // 192

/**
 * Face metadata.  For each face k, defines the three sphere-axis
 * roles: which axis is the face normal, which is the u-axis, which
 * is the v-axis, each with a sign.  Matches Google S2 conventions.
 *
 *   Format: [ [normalAxis, normalSign], [uAxis, uSign], [vAxis, vSign] ]
 *   where axis is 0=X, 1=Y, 2=Z and sign is ±1.
 */
const FACE_AXES = [
  // Face 0: normal=+X, u=+Y, v=+Z
  [[0, +1], [1, +1], [2, +1]],
  // Face 1: normal=+Y, u=-X, v=+Z
  [[1, +1], [0, -1], [2, +1]],
  // Face 2: normal=+Z, u=-X, v=-Y
  [[2, +1], [0, -1], [1, -1]],
  // Face 3: normal=-X, u=-Z, v=-Y
  [[0, -1], [2, -1], [1, -1]],
  // Face 4: normal=-Y, u=-Z, v=+X
  [[1, -1], [2, -1], [0, +1]],
  // Face 5: normal=-Z, u=+Y, v=+X
  [[2, -1], [1, +1], [0, +1]],
];

// ─────────────────────────────────────────────────────────────────────────────
// Coordinate conversions
// ─────────────────────────────────────────────────────────────────────────────

const DEG = Math.PI / 180;
const RAD = 180 / Math.PI;

function latLngToXYZ(lat, lng) {
  const latR = lat * DEG;
  const lngR = lng * DEG;
  const c = Math.cos(latR);
  return { x: c * Math.cos(lngR), y: c * Math.sin(lngR), z: Math.sin(latR) };
}

function xyzToLatLng(x, y, z) {
  return {
    lat: Math.asin(Math.max(-1, Math.min(1, z))) * RAD,
    lng: Math.atan2(y, x) * RAD,
  };
}

/** Which of the 6 cube faces does this unit-sphere point sit on? */
function xyzToFace(x, y, z) {
  const ax = Math.abs(x), ay = Math.abs(y), az = Math.abs(z);
  if (ax >= ay && ax >= az) return x >= 0 ? 0 : 3;
  if (ay >= az)             return y >= 0 ? 1 : 4;
  return                          z >= 0 ? 2 : 5;
}

/** Project xyz onto the chosen face's (u, v) plane.  u, v ∈ [-1, +1]. */
function xyzToFaceUV(face, x, y, z) {
  const xyz = [x, y, z];
  const [[nAx, nSign], [uAx, uSign], [vAx, vSign]] = FACE_AXES[face];
  const n = nSign * xyz[nAx];        // > 0 by face-selection
  return { u: (uSign * xyz[uAx]) / n, v: (vSign * xyz[vAx]) / n };
}

/** Inverse of xyzToFaceUV: unproject from face-(u,v) back onto unit sphere. */
function faceUVToXYZ(face, u, v) {
  const xyz = [0, 0, 0];
  const [[nAx, nSign], [uAx, uSign], [vAx, vSign]] = FACE_AXES[face];
  xyz[nAx] = nSign;
  xyz[uAx] = uSign * u;
  xyz[vAx] = vSign * v;
  const len = Math.hypot(xyz[0], xyz[1], xyz[2]);
  return { x: xyz[0] / len, y: xyz[1] / len, z: xyz[2] / len };
}

// ─────────────────────────────────────────────────────────────────────────────
// Hilbert curve for cell numbering within a face — real S2 level-3
//
// Real S2 at level 3 partitions each face into an 8×8 grid (64 cells)
// numbered along a Hilbert curve.  The Google S2 64-bit cell ID
// encodes face in the top 3 bits and the level-3 Hilbert position in
// the next 6 bits.  Truncating to 8 bits keeps the top 3 (face) + the
// top 5 of the Hilbert position — i.e. pairs of consecutive level-3
// cells along the Hilbert curve.  6 × 32 = 192 truncated cells total.
//
// This guarantees that anyone using a standard S2 library at level 3
// can recover our 8-bit cell ID by computing the level-3 ID and
// shifting right by 1 — full interop.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Hilbert 8×8 xy → d.  Used at S2 level 3 (8×8 grid per face).
 * (x, y) ∈ [0..7]², returns d ∈ [0..63].
 *
 * Wikipedia xy2d rotation uses `n-1 - x` (full-grid size) to keep
 * intermediate values in range.
 */
function hilbert8x8_xy2d(x, y) {
  const N = 8, NM1 = N - 1;
  let d = 0;
  for (let s = N >> 1; s > 0; s >>= 1) {
    const rx = (x & s) > 0 ? 1 : 0;
    const ry = (y & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    if (ry === 0) {
      if (rx === 1) { x = NM1 - x; y = NM1 - y; }
      const tmp = x; x = y; y = tmp;
    }
  }
  return d;
}

/**
 * Hilbert 8×8 d → xy.  d ∈ [0..63], returns {x, y} ∈ [0..7]².
 * Wikipedia d2xy rotation uses `s-1 - x` (iteration size).
 */
function hilbert8x8_d2xy(d) {
  let x = 0, y = 0, t = d;
  for (let s = 1; s < 8; s <<= 1) {
    const rx = 1 & (t >> 1);
    const ry = 1 & (t ^ rx);
    if (ry === 0) {
      if (rx === 1) { x = s - 1 - x; y = s - 1 - y; }
      const tmp = x; x = y; y = tmp;
    }
    x += s * rx;
    y += s * ry;
    t >>= 2;
  }
  return { x, y };
}

// ─────────────────────────────────────────────────────────────────────────────
// Quadratic UV↔ST transform (Google S2 default — for equal-area cells)
// ─────────────────────────────────────────────────────────────────────────────

/** UV → ST.  u ∈ [-1, +1]  →  s ∈ [0, 1]. */
function uvToST(u) {
  return u >= 0
    ?         0.5 * Math.sqrt(1 + 3 * u)
    : 1.0   - 0.5 * Math.sqrt(1 - 3 * u);
}

/** ST → UV.  s ∈ [0, 1]  →  u ∈ [-1, +1]. */
function stToUV(s) {
  return s >= 0.5
    ?         (4 * s * s - 1) / 3
    : -((4 * (1 - s) * (1 - s) - 1) / 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Compute the 8-bit S2 cell ID for a (lat, lng) coordinate.
 *
 * @param {number} lat   – latitude in degrees, [-90, +90]
 * @param {number} lng   – longitude in degrees, [-180, +180]
 * @param {number} [bits] – ignored (kept for API back-compat); always returns 8 bits.
 * @returns {number}     – integer in [0, 192).  Never returns 192..255.
 */
export function geoCellId(lat, lng, bits) {
  // bits parameter ignored; kept in the signature so callers that
  // previously passed `8` don't need patching.  All Axona usage is
  // 8-bit at this level; deeper hierarchies are a future addition.
  void bits;

  const { x, y, z } = latLngToXYZ(lat, lng);
  const face = xyzToFace(x, y, z);
  const { u, v } = xyzToFaceUV(face, x, y, z);
  const s = uvToST(u);
  const t = uvToST(v);
  const sBin = clamp(Math.floor(s * S_BINS), 0, S_BINS - 1);
  const tBin = clamp(Math.floor(t * T_BINS), 0, T_BINS - 1);
  const h3 = hilbert8x8_xy2d(sBin, tBin);        // 0..63  (level 3)
  // Top 8 bits of the S2 cell ID: face (3) || h3 >> 1 (5).
  return (face << 5) | (h3 >> 1);
}

/**
 * Compute the (lat, lng) at the center of a given 8-bit S2 cell.
 *
 * Useful for visualization and for system topics that need a well-
 * defined coordinate per cell.  Returns null if the cell ID is in
 * the reserved 192..255 range.
 *
 * @param {number} cellId – 8-bit cell ID in [0, 192).
 * @returns {{lat: number, lng: number} | null}
 */
export function geoCellCenter(cellId) {
  if (!isValidCellId(cellId)) return null;
  const face = cellId >> 5;
  const trunc = cellId & 0x1F;                    // 5-bit truncated Hilbert
  // Two level-3 cells share this truncated index: positions 2*trunc and 2*trunc+1.
  const a = hilbert8x8_d2xy(trunc << 1);
  const b = hilbert8x8_d2xy((trunc << 1) | 1);
  // Midpoint in face-local (sBin, tBin), then to ST → UV → xyz → lat/lng.
  const sMid = (a.x + 0.5 + b.x + 0.5) / 2;       // in [0, 8]
  const tMid = (a.y + 0.5 + b.y + 0.5) / 2;
  const s = sMid / S_BINS;
  const t = tMid / T_BINS;
  const u = stToUV(s);
  const v = stToUV(t);
  const { x, y, z } = faceUVToXYZ(face, u, v);
  return xyzToLatLng(x, y, z);
}

/**
 * Compute the four (lat, lng) corners of an 8-bit S2 cell, in
 * face-local order.  The cell boundary on the sphere is the great-
 * circle arc between consecutive corners.  Useful for the visualizer
 * to draw curved cell outlines.
 *
 * Returns null for reserved cell IDs.
 *
 * @param {number} cellId – 8-bit cell ID in [0, 192).
 * @returns {Array<{lat: number, lng: number}> | null} 4-element array.
 */
export function geoCellCorners(cellId) {
  if (!isValidCellId(cellId)) return null;
  const face = cellId >> 5;
  const trunc = cellId & 0x1F;
  const a = hilbert8x8_d2xy(trunc << 1);
  const b = hilbert8x8_d2xy((trunc << 1) | 1);
  // Union of two adjacent level-3 cells is always a 1×2 or 2×1
  // rectangle in the 8×8 face grid.  Find its bounding box in
  // (sBin, tBin) coordinates.
  const sLo = Math.min(a.x, b.x), sHi = Math.max(a.x, b.x);
  const tLo = Math.min(a.y, b.y), tHi = Math.max(a.y, b.y);
  const s0 = sLo / S_BINS,        s1 = (sHi + 1) / S_BINS;
  const t0 = tLo / T_BINS,        t1 = (tHi + 1) / T_BINS;
  const ll = (s, t) => {
    const { x, y, z } = faceUVToXYZ(face, stToUV(s), stToUV(t));
    return xyzToLatLng(x, y, z);
  };
  return [ll(s0, t0), ll(s1, t0), ll(s1, t1), ll(s0, t1)];
}

/**
 * Compute the face (0..5) for an 8-bit S2 cell ID.
 * Reserved IDs (≥192) return -1.
 */
export function geoCellFace(cellId) {
  if (!isValidCellId(cellId)) return -1;
  return cellId >> 5;
}

/** True iff cellId is in the valid [0, 192) range. */
export function isValidCellId(cellId) {
  return Number.isInteger(cellId) && cellId >= 0 && cellId < S2_CELL_COUNT;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}
