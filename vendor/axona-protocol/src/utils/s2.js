/**
 * s2.js – S2-inspired geographic cell ID utilities.
 *
 * Converts a (lat, lng) coordinate to an N-bit unsigned integer whose value
 * preserves geographic locality: nearby coordinates produce nearby integers.
 *
 * Algorithm:
 *   1. Quantise lat/lng into a 2^(N/2) × 2^(N/2) grid.
 *   2. Apply a 2D Hilbert-curve ordering to flatten the grid to 1D.
 *
 * The Hilbert curve is superior to simple row/column or Morton (Z-order)
 * ordering because it minimises the maximum distance between any cell and
 * its neighbours in the 1D index — a property critical for DHT locality.
 *
 * Supported bit widths: any even number (8, 16, 32 …).
 */

// ─────────────────────────────────────────────────────────────────────────────
// Hilbert curve  (xy → d)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Convert 2D grid coordinates (x, y) to a Hilbert-curve distance d.
 *
 * @param {number} n  – grid side length; must be a power of 2
 * @param {number} x  – column in [0, n)
 * @param {number} y  – row    in [0, n)
 * @returns {number}  Hilbert distance in [0, n²)
 */
function hilbertXY2D(n, x, y) {
  let d = 0;
  for (let s = n >> 1; s > 0; s >>= 1) {
    const rx = (x & s) > 0 ? 1 : 0;
    const ry = (y & s) > 0 ? 1 : 0;
    d += s * s * ((3 * rx) ^ ry);
    // Rotate / reflect the current quadrant in-place
    if (ry === 0) {
      if (rx === 1) { x = s - 1 - x; y = s - 1 - y; }
      const tmp = x; x = y; y = tmp; // swap
    }
  }
  return d;
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Compute an N-bit geographic cell ID for (lat, lng).
 *
 * The sphere is divided into a 2^(N/2) × 2^(N/2) grid of lat/lng cells.
 * The Hilbert curve maps each grid cell to a unique integer in [0, 2^N),
 * such that geographically adjacent cells always have close indices.
 *
 * @param {number} lat   – latitude  in degrees, [-90, +90]
 * @param {number} lng   – longitude in degrees, [-180, +180]
 * @param {number} bits  – even number of bits for the cell ID (8 / 16 / 32)
 * @returns {number}     – unsigned integer in [0, 2^bits)
 */
export function geoCellId(lat, lng, bits) {
  const halfBits = bits >> 1;
  const gridSize = 1 << halfBits; // 2^halfBits cells per axis

  // Clamp and quantise to grid coordinates
  const x = Math.min(gridSize - 1, Math.floor((lat  +  90) / 180 * gridSize));
  const y = Math.min(gridSize - 1, Math.floor((lng  + 180) / 360 * gridSize));

  return hilbertXY2D(gridSize, x, y);
}
