// =====================================================================
// BrowserEngine — minimal browser-side engine stub for AxonaPeer.
//
// AxonaPeer's per-node logic (the per-peer DHT contract implementation
// in @axona/protocol/dht/AxonaPeer.js) still reads several pieces of
// "engine" state through `this._engine.X`: routing constants
// (MAX_SYNAPTOME, LOOKAHEAD_ALPHA, …), shared counters (simEpoch,
// lookupsSinceDecay), per-node stats, event listener set, and
// per-node handler tables.  In dht-sim that engine is AxonaEngine,
// the multi-node simulator orchestrator.  In a real browser peer
// there's only one peer, so we give it a minimal stand-in that
// satisfies AxonaPeer's read/write surface.
//
// The per-node refactor's eventual Phase 6 will move most of this
// state onto AxonaPeer directly and the engine reference disappears.
// Until then, BrowserEngine fills the role.
//
// What this class does NOT do:
//   - Manage multiple nodes (no `nodeMap` — production peer is one)
//   - Pub/sub (no AxonManager wiring — axonFor() throws clearly).
//     Pub/sub support in the browser awaits a Web-Crypto port of
//     post.js or an esbuild bundling step.
//   - Drive a benchmark cycle (no `snapshotMetrics`, no `_tickDecay`
//     called from outside).  `_tickDecay` here is fired only via
//     AxonaPeer.lookup()'s periodic decay trigger.
// =====================================================================

export class BrowserEngine {
  constructor(config = {}) {
    const r = config.rules ?? {};

    // ── Routing / structural constants (NH-1 defaults) ───────────────
    this._k                  = config.k                  ?? 20;
    this.MAX_SYNAPTOME       = r.maxSynaptome            ?? 50;
    this.LOOKAHEAD_ALPHA     = r.lookaheadAlpha          ?? 5;
    this.MAX_HOPS            = r.maxGreedyHops           ?? 40;
    this.EPSILON             = r.explorationEpsilon      ?? 0.05;
    this.WEIGHT_SCALE        = r.weightScale             ?? 0.40;
    this.ANNEAL_COOLING      = r.annealCooling           ?? 0.9997;
    this.DECAY_GAMMA         = r.decayGamma              ?? 0.995;
    this.DECAY_GAMMA_MIN     = r.decayGammaMin           ?? 0.990;
    this.DECAY_GAMMA_MAX     = r.decayGammaMax           ?? 0.9998;
    this.USE_SATURATION      = r.useSaturation           ?? 20;
    this.INERTIA_DURATION    = r.inertiaDuration         ?? 20;
    this.PROMOTE_THRESHOLD   = r.promoteThreshold        ?? 2;
    this.TRIADIC_THRESHOLD   = r.triadicThreshold        ?? 2;
    this.EN_LATERAL_SPREAD   = r.lateralSpread           ?? true;
    this.EN_ADAPTIVE_DECAY   = r.adaptiveDecay           ?? false;
    this.LATERAL_K           = r.lateralK                ?? 2;
    this.ANNEAL_LOCAL_SAMPLE = r.annealLocalSample       ?? 50;
    this.ANNEAL_RATE_SCALE   = r.annealRateScale         ?? 1.0;
    this.GEO_BITS            = config.geoBits ?? 8;
    this.GEO_REGION_BITS     = r.geoRegionBits ?? Math.min(4, this.GEO_BITS);
    this.STRATA_GROUPS       = 16;
    this.RECENCY_HALF_LIFE   = r.recencyHalfLife         ?? 50;
    this.DECAY_INTERVAL      = 100;
    this.T_INIT              = 1.0;
    this.T_MIN               = 0.05;
    this.T_REHEAT            = 0.5;
    this.VITALITY_FLOOR      = 0.05;

    // ── Shared counters AxonaPeer reads/writes ───────────────────────
    this.simEpoch          = 0;
    this.lookupsSinceDecay = 0;
    this._emaHops          = null;
    this._emaTime          = null;

    // ── Per-node stats (keyed by NeuronNode) ─────────────────────────
    /** @type {Map<object, {attempted: number, succeeded: number, sumHops: number, sumLatency: number}>} */
    this._nodeStats = new Map();

    // ── Event listeners (engine-global ProtocolEvent stream) ─────────
    /** @type {Set<(event: object) => void>} */
    this._eventListeners = new Set();

    // ── Per-node routed/direct handler tables ───────────────────────
    // AxonaPeer.onRoutedMessage / onDirectMessage write here; the
    // route_msg request handler (registered by AxonaEngine in the sim
    // and by BrowserPeer.start in production) reads them.
    /** @type {Map<object, Map<string, Function>>} */
    this._routedHandlers = new Map();
    /** @type {Map<object, Map<string, Function>>} */
    this._directHandlers = new Map();

    // ── The one and only peer reference (production: one per tab) ────
    /** @type {object | null} */  this._theNode = null;
  }

  // ── Engine surface AxonaPeer relies on ─────────────────────────────

  _emit(event) {
    if (this._eventListeners.size === 0) return;
    for (const h of this._eventListeners) {
      try { h(event); }
      catch (err) { console.error('BrowserEngine: event listener threw:', err); }
    }
  }

  onEvent(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('BrowserEngine.onEvent: handler must be a function');
    }
    this._eventListeners.add(handler);
    return () => this._eventListeners.delete(handler);
  }

  _tickDecay() {
    // Browser peer has one node; iterate its synaptome.  Triggered by
    // AxonaPeer.lookup() every DECAY_INTERVAL successful lookups.
    const node = this._theNode;
    if (!node || !node.alive) return;
    for (const syn of node.synaptome.values()) {
      if (syn.inertia > this.simEpoch) continue;  // LTP-locked: skip
      let gamma;
      if (this.EN_ADAPTIVE_DECAY) {
        const useFrac = Math.min(1, (syn.useCount ?? 0) / this.USE_SATURATION);
        gamma = this.DECAY_GAMMA_MIN
              + (this.DECAY_GAMMA_MAX - this.DECAY_GAMMA_MIN) * useFrac;
        if (syn.bootstrap) gamma = gamma + (this.DECAY_GAMMA_MAX - gamma) * 0.5;
      } else {
        gamma = this.DECAY_GAMMA;
      }
      syn.decay(gamma);
    }
  }

  _bumpLookupStats(node, found, hops, latency) {
    let s = this._nodeStats.get(node);
    if (!s) {
      s = { attempted: 0, succeeded: 0, sumHops: 0, sumLatency: 0 };
      this._nodeStats.set(node, s);
    }
    s.attempted++;
    if (found) {
      s.succeeded++;
      s.sumHops    += hops;
      s.sumLatency += latency;
    }
  }

  // ── Pub/sub: not supported in the browser MVP ──────────────────────
  //
  // AxonaPeer.subscribe / publish / unsubscribe call
  // `engine.axonFor(node)`.  We throw with a clear message so the
  // application sees what's missing.  Pub/sub support arrives with
  // the Web-Crypto port of post.js or an esbuild bundle.

  axonFor(_node) {
    throw new Error(
      'BrowserEngine.axonFor: pub/sub not supported in the browser ' +
      'MVP.  Requires Web-Crypto port of post.js or bundled build.'
    );
  }

  // ── Register the one node this engine wraps ────────────────────────
  setTheNode(node) {
    this._theNode = node;
  }
}
