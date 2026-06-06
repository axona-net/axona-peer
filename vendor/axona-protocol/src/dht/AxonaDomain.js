// =====================================================================
// AxonaDomain — shared mesh state + config for a group of AxonaPeers.
//
// Phase 5 of the engine→peer migration.  Before this class existed,
// the kernel's AxonaPeer reached into `this._engine.X` for ~25
// different config knobs (T_INIT, EPSILON, MAX_HOPS, ...) and shared
// mutable state (simEpoch, _emaHops, _emaTime, event listeners).
// dht-sim's AxonaEngine provided those; standalone peers had nowhere
// to read them from.
//
// AxonaDomain consolidates that surface so:
//
//   · A group of kernel peers running standalone over Transport.sim
//     can share a `new AxonaDomain()` and have peer.lookup() work
//     without an engine at all (Phase 5e).
//
//   · dht-sim's AxonaEngine can continue to be the source of truth
//     for its own benchmarking world — Phase 5d wires AxonaPeer to
//     read from `this._domain`, which initially aliases the engine
//     (engine === domain, behaviour unchanged).
//
//   · Migration is gradual: engine consumers don't have to change;
//     the engine just keeps owning the fields, and peers read them
//     via the domain handle.  Engine and domain are conceptually the
//     same object until standalone peers explicitly construct a
//     dedicated AxonaDomain.
//
// AxonaDomain is NOT responsible for:
//   · Per-peer state — that lives on each AxonaPeer (5a + 5b).
//   · Cross-peer routing — that's the transport's job.
//   · Bootstrap — dht-sim's AxonaEngine still owns that path; a
//     standalone-peer bootstrap is a separate concern handled by
//     peer.join(sponsor).
//
// =====================================================================

/** Default tuning constants.  All overrideable via constructor opts. */
const DEFAULTS = Object.freeze({
  // Routing / Kademlia
  _k:                  20,
  // Upper bound on hops before a lookup/route gives up. The vast majority of
  // lookups converge in ~7 hops (p95 ~11 at 25k under a 100-connection cap);
  // 40 is a generous ceiling for the rare long tail. Under a hard connection
  // cap the sparser graph occasionally needs >16 hops to converge, so a 16-hop
  // ceiling silently dropped ~0.5% of lookups that a higher ceiling completes
  // — verified in dht-sim (16 → ~99.4%, 40 → 100% at 25k, matching NH-1, whose
  // engine has long used 40). 40-hop lookups are exceedingly rare, so the
  // worst-case latency cost is paid by almost no one.
  MAX_HOPS:            40,
  EPSILON:             0.05,
  LOOKAHEAD_ALPHA:     5,
  GEO_REGION_BITS:     8,

  // Annealing / temperature
  T_INIT:              1.0,
  T_REHEAT:            0.5,
  T_MIN:               0.05,
  ANNEAL_COOLING:      0.995,
  ANNEAL_RATE_SCALE:   0.05,
  ANNEAL_LOCAL_SAMPLE: 8,

  // LEARN: per-hop reinforcement / promotion / cache
  PROMOTE_THRESHOLD:   3,
  RECENCY_HALF_LIFE:   1000,
  TRIADIC_THRESHOLD:   3,
  EN_LATERAL_SPREAD:   true,
  LATERAL_K:           3,
  MAX_SYNAPTOME:       50,
  VITALITY_FLOOR:      0.1,

  // FORGET: periodic decay tick
  DECAY_INTERVAL:      500,

  // Misc.
  STRATA_GROUPS:       4,
});

export class AxonaDomain {
  constructor(opts = {}) {
    // Config — defaults overridable per-instance.
    for (const [k, v] of Object.entries(DEFAULTS)) {
      this[k] = opts[k] ?? v;
    }

    // ── Shared mutable state ───────────────────────────────────────
    /** Monotonic event counter incremented on every lookup attempt. */
    this.simEpoch = 0;
    /** Lookups since the last `_tickDecay()`.  Threshold = DECAY_INTERVAL. */
    this.lookupsSinceDecay = 0;
    /** Rolling EMA of successful-lookup hop counts (LTP gating). */
    this._emaHops = null;
    /** Rolling EMA of successful-lookup wall-clock latency. */
    this._emaTime = null;

    // ── Event bus ──────────────────────────────────────────────────
    // Peers and external observers subscribe via `onEvent(cb)`.  The
    // engine and the peer both call `_emit(event)` to publish.
    /** @type {Set<Function>} */
    this._eventListeners = new Set();

    // ── Peer registry ──────────────────────────────────────────────
    // Tracks the set of peers attached to this domain so `_tickDecay()`
    // can walk them.  AxonaPeer.start() registers itself.
    /** @type {Set<import('./AxonaPeer.js').AxonaPeer>} */
    this._peers = new Set();
  }

  // ── Event bus ────────────────────────────────────────────────────

  /** Subscribe to all events.  Returns an unsubscribe handle. */
  onEvent(cb) {
    this._eventListeners.add(cb);
    return () => this._eventListeners.delete(cb);
  }

  /** Emit an event to every subscriber.  Errors in listeners are logged
   *  and isolated so one bad handler can't poison the rest. */
  _emit(event) {
    if (this._eventListeners.size === 0) return;
    for (const h of this._eventListeners) {
      try { h(event); }
      catch (err) { console.error('AxonaDomain: event listener threw:', err); }
    }
  }

  // ── Peer registry ────────────────────────────────────────────────

  _registerPeer(peer)   { this._peers.add(peer); }
  _unregisterPeer(peer) { this._peers.delete(peer); }

  /** Periodic decay: walk each attached peer and apply synapse decay. */
  _tickDecay() {
    for (const peer of this._peers) {
      peer._tickDecay?.();
    }
  }
}
