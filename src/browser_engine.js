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
// What this class does (Phase 3+ — pub/sub on):
//   - `axonFor(node)` returns a cached per-node AxonManager wired to
//     the AxonaPeer's DHT primitives.  AxonaPeer.publish/subscribe now
//     reach the real protocol pubsub layer.  Caller passes the peer
//     reference via `setPeerForNode(node, peer)` after constructing
//     the AxonaPeer (since the engine is built first by AxonaNode).
//
// What this class does NOT do:
//   - Manage multiple nodes (no `nodeMap` — production peer is one)
//   - Drive a benchmark cycle (no `snapshotMetrics`, no `_tickDecay`
//     called from outside).  `_tickDecay` here is fired only via
//     AxonaPeer.lookup()'s periodic decay trigger.
// =====================================================================

import { AxonManager } from '../vendor/axona-protocol/src/pubsub/AxonManager.js';

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

    // ── Per-node AxonManager + AxonaPeer back-ref ────────────────────
    // `setPeerForNode(node, peer)` is called by AxonaNode after it
    // constructs the AxonaPeer (engine is built first, peer second).
    // axonFor(node) reads _peerByNode to build the dht adapter the
    // AxonManager constructor needs.
    /** @type {Map<object, object>}  node → AxonaPeer        */
    this._peerByNode = new Map();
    /** @type {Map<object, object>}  node → AxonManager       */
    this._axonByNode = new Map();
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

  // ── Pub/sub: real AxonManager wiring ───────────────────────────────
  //
  // `axonFor(node)` returns a cached AxonManager for this node.  The
  // AxonManager talks to a `dht` adapter that fronts the AxonaPeer's
  // DHT primitives (findKClosest, sendDirect, routeMessage, …).
  // AxonaPeer's onDirectMessage / onRoutedMessage installs the wire
  // listeners via transport.onNotification, so we don't have to wire
  // those here.

  /**
   * Set by AxonaNode after it constructs the AxonaPeer.  Without this,
   * `axonFor` can't build the dht adapter (it needs the peer's DHT
   * primitives).  Idempotent.
   */
  setPeerForNode(node, peer) {
    if (!node || !peer) throw new Error('setPeerForNode: node and peer required');
    this._peerByNode.set(node, peer);
  }

  axonFor(node) {
    if (!node) throw new Error('axonFor: node required');
    const cached = this._axonByNode.get(node);
    if (cached) return cached;

    const peer = this._peerByNode.get(node);
    if (!peer) {
      throw new Error(
        'BrowserEngine.axonFor: AxonaPeer not registered for this node. ' +
        'Call engine.setPeerForNode(node, peer) after constructing the peer.'
      );
    }

    // The dht adapter the AxonManager talks to.  AxonaPeer exposes
    // every primitive; we add the missing alias (getSelfId vs
    // getNodeId), intercept self-targets, and add a routed fallback
    // to sendDirect.
    //
    // sendDirect cases:
    //   self        → local dispatch (skip the wire entirely; matches
    //                 SimulatedNetwork's in-process semantics)
    //   directly    → peer.sendDirect (1-hop transport.notify)
    //     bound
    //   else        → peer.routeMessage with a '__tunneled_direct__'
    //                 envelope.  The receiver's routed handler (below)
    //                 unwraps and dispatches into its direct-handler
    //                 table.  This lets pubsub reach K-closest axons
    //                 the local transport can't directly notify (the
    //                 common case in our sparse-mesh deployment, where
    //                 only the bridge is a universal neighbour).  The
    //                 bridge ends up being a routing hop, not a
    //                 protocol-privileged relay.
    const self = peer.getNodeId();
    const engine = this;

    const dht = {
      getSelfId:       () => peer.getNodeId(),
      findKClosest:    (...args) => peer.findKClosest(...args),
      routeMessage:    (...args) => peer.routeMessage(...args),
      sendDirect:      async (peerId, type, payload) => {
        if (peerId === self) {
          const table = engine._directHandlers.get(node);
          const h = table?.get(type);
          if (!h) return false;
          try {
            await h(payload, { fromId: self.toString(16).padStart(16, '0'), type });
            return true;
          } catch (err) {
            console.error('BrowserEngine self-sendDirect handler threw:', err);
            return false;
          }
        }
        // Probe: is the target directly reachable on any sub-transport?
        // CompositeTransport.isConnected returns true iff some sub-
        // transport currently owns + has an open channel to peerId.
        if (node.transport?.isConnected?.(peerId)) {
          return peer.sendDirect(peerId, type, payload);
        }
        // Not bound — fall back to routed delivery via the DHT.
        // Fire-and-forget; the routed walk will time out on its own if
        // it can't reach the target.  Return true optimistically so
        // AxonManager's child-dead-detection doesn't false-positive.
        peer.routeMessage(peerId, '__tunneled_direct__', {
          targetId:  peerId.toString(16).padStart(16, '0'),
          innerType: type,
          innerPayload: payload,
        }).catch(err => console.error('BrowserEngine routed sendDirect failed:', err));
        return true;
      },
      onRoutedMessage: (type, h) => peer.onRoutedMessage(type, h),
      onDirectMessage: (type, h) => peer.onDirectMessage(type, h),
    };

    // Register the tunneled-direct routed handler ONCE per node.  This
    // is what receives at the target end of the routed-fallback path
    // above: it checks meta.targetId, unwraps the envelope, and
    // dispatches into the local direct handler table.
    peer.onRoutedMessage('__tunneled_direct__', async (payload, meta) => {
      const targetBig = typeof meta.targetId === 'bigint'
        ? meta.targetId
        : (typeof meta.targetId === 'string'
            ? BigInt('0x' + meta.targetId.replace(/^0x/, ''))
            : null);
      if (targetBig == null) return 'forward';
      if (targetBig !== self) return 'forward';  // not for us — keep routing
      const handler = engine._directHandlers.get(node)?.get(payload.innerType);
      if (!handler) return 'consumed';  // unknown type; drop here, don't forward
      try {
        await handler(payload.innerPayload, {
          fromId: meta.fromId,
          type:   payload.innerType,
        });
      } catch (err) {
        console.error('BrowserEngine tunneled-direct dispatch threw:', err);
      }
      return 'consumed';
    });

    const axon = new AxonManager({ dht });
    this._axonByNode.set(node, axon);
    return axon;
  }

  // ── Register the one node this engine wraps ────────────────────────
  setTheNode(node) {
    this._theNode = node;
  }
}
