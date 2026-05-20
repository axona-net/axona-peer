// =====================================================================
// AxonaPeer — per-node DHT contract implementation for NH-1.
//
// ── Migration status (Phase 1 of NH1-PerNode-Refactor-Plan-v0.71.md) ──
//
// Phase 1 — Skeleton + co-existence.
//
// This class exists to implement the per-node DHT contract at
// `src/contracts/DHT.js` against the existing multi-node
// `AxonaEngine` (the simulator's NH-1 engine).  In Phase 1 every
// method is a thin delegation to the engine, with the per-node
// `NeuronNode` reference passed through where the engine expects it.
//
// The intent is to validate the per-node API shape (matches the
// contract, can be constructed cleanly, can be observed via getMetrics
// and onEvent) before moving any actual protocol logic out of the
// engine.  Subsequent phases (2 → 3) progressively move read-only and
// then write operations into this class; Phase 4 then renames today's
// engine to `NHOneEngine` and finalises the split.
//
// During Phase 1 the simulator's behaviour is unchanged: the engine
// still owns all the routing logic; AxonaPeer is just an alternative
// API surface that production peers can use to exercise NH-1 through
// the contract.
//
// ── What this class IS ───────────────────────────────────────────────
//   - The DHT contract (src/contracts/DHT.js) implementation for one peer
//   - One instance per running node (in both sim and production)
//   - Owns: a reference to its NeuronNode (per-node state), a reference
//     to the engine (during Phase 1; later phases move logic here), a
//     reference to its transport, and a set of per-peer event listeners.
//
// ── What this class IS NOT ───────────────────────────────────────────
//   - A multi-node manager (that's the engine's job)
//   - A wrapper that hides the engine's existence from the simulator
//     (Phase 1 keeps the engine reachable so the simulator's tests and
//     Engine-cycle code continue to work)
// =====================================================================

import { DHT }            from '../contracts/DHT.js';
import { Synapse }        from './Synapse.js';
import { Subscription }   from './Subscription.js';
import { clz264, toHex, isHexId } from '../utils/hexid.js';
import { deriveTopicId }  from '../pubsub/post.js';
import { buildEnvelope }  from '../pubsub/envelope.js';
import { PublishError, SubscribeError, PullError, MetricsError, ErrorCodes } from '../errors.js';

export class AxonaPeer extends DHT {
  /**
   * @param {object} opts
   * @param {import('./AxonaEngine.js').AxonaEngine} opts.engine
   *        The legacy multi-node engine (Phase 1: delegate target).
   * @param {import('./NeuronNode.js').NeuronNode} opts.node
   *        The NeuronNode this peer wraps.
   * @param {object} [opts.axonManager]
   *        Optional explicit AxonManager instance to use for the
   *        unified pub()/sub() API.  When omitted, pub/sub fall back
   *        to the engine's per-node AxonManager (engine.axonManagerFor
   *        if present, else throws).
   * @param {object} [opts.identity]
   *        Identity envelope from `deriveIdentity()` — required for
   *        signed publishes (the default).  Apps that only call
   *        `peer.pub(topic, message, { sign: false })` can omit it.
   */
  constructor({ engine, node, axonManager = null, identity = null, transport = null, persist = null }) {
    super();
    if (!engine) throw new Error('AxonaPeer: engine is required');
    if (!node)   throw new Error('AxonaPeer: node is required');
    this._engine = engine;
    /**
     * Phase 5c (kernel cleanup): the peer reads its shared mesh
     * state + config from a "domain" handle.  Today the domain IS
     * the engine — dht-sim's AxonaEngine provides simEpoch, _emaHops,
     * MAX_HOPS, ... directly on itself, so engine === domain and
     * behaviour is unchanged.  Phase 5d gives the peer a way to
     * accept a standalone AxonaDomain so it can run without an
     * engine at all.  Phase 5e then ships the smoke that exercises
     * that path through Transport.sim end-to-end.
     */
    this._domain = engine;
    this._node   = node;
    this._axonManager = axonManager;
    this._identity = identity;
    this._transport = transport;
    this._persist  = persist;
    this._started = false;

    // ─── Persistence state ────────────────────────────────────────
    this._persistDirty   = new Set();  // namespaces with pending writes
    this._persistTimer   = null;
    this._persistFlushMs = 5000;
    /** @type {Set<(event: object) => void>} */
    this._eventListeners = new Set();
    /** @type {(event: object) => void | null} */
    this._engineListenerUnsub = null;

    // ─── Unified pub/sub state ────────────────────────────────────
    /** @type {Map<string, Set<Subscription>>} topicId(hex) → handles */
    this._subscriptions = new Map();
    /** True once we've installed the AxonManager-side delivery hook. */
    this._deliveryHookInstalled = false;

    // ─── Direct messaging state ───────────────────────────────────
    /** Application handler set by peer.onMessage().  At most one. */
    this._directMessageHandler = null;
    /** True once we've installed the transport-side req/ntf handlers. */
    this._directHandlersInstalled = false;

    // ─── Wire-handler tables (Phase 5a — own them) ────────────────
    // Before Phase 5a these lived as engine-keyed-by-node Maps in
    // `engine._routedHandlers` / `engine._directHandlers`.  They never
    // had any cross-peer relevance — every read site looked up the
    // entry for THIS peer's own node.  Owning them on the peer
    // shrinks the engine API surface and is a step toward letting
    // peers run without an engine at all (see Phase 5 plan).
    /** @type {Map<string, Function>} routed-message type → handler */
    this._routedHandlers = new Map();
    /** @type {Map<string, Function>} direct-message type → handler */
    this._directHandlers = new Map();

    // ─── Per-peer lookup stats (Phase 5b — own them) ──────────────
    // Before Phase 5b these lived as engine._nodeStats — one entry per
    // node, keyed by the NeuronNode.  Read sites (peer.getMetrics) and
    // write sites (peer's _bumpLookupStats at the end of lookup()) all
    // resolve to THIS peer's own entry — nothing cross-peer.  Moving
    // it onto the peer matches where the data conceptually belongs
    // and gets us one step closer to peer.lookup() running without an
    // engine.
    this._stats = { attempted: 0, succeeded: 0, sumHops: 0, sumLatency: 0 };
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────
  //
  // Phase 1: start/stop are mostly bookkeeping.  The underlying node
  // was already created and registered with the engine via
  // `engine.addNode()` before this AxonaPeer instance came into
  // existence.  We just need to wire up event forwarding so that
  // `onEvent` listeners on this peer receive events that the engine
  // emits about this node.
  //
  // In later phases, start() will move into the real lifecycle:
  // allocate the synaptome, register transport handlers, spin up the
  // decay tick.  For Phase 1 it's idempotent and lightweight.

  async start() {
    if (this._started) return;

    // ─── Persistence load (P4) ──────────────────────────────────────
    // If a PersistenceAdapter was provided AND we don't already have
    // an identity, try to load one.  Same for the synaptome seed and
    // the subscriptions list (which becomes pendingSubscriptions for
    // the app to re-register handlers).
    if (this._persist) {
      await this._loadFromPersist();
    }

    // Engine emits events to a single global listener set today
    // (domain._eventListeners).  We subscribe and filter to events
    // about THIS node, then forward to our per-peer listeners.  This
    // lets the production peer subscribe via AxonaPeer.onEvent without
    // seeing other nodes' events (which it can't, since production
    // only has one node).
    this._engineListenerUnsub = this._domain.onEvent((ev) => {
      // Most events carry a node identifier in one of several fields:
      //   nodeId, peerId, observerId, sourceId, …
      // The current set of event types and their id fields is
      // documented in src/contracts/types.js (ProtocolEvent union).
      // Phase 1 forwards events that mention this._node.id in any of
      // the documented locations; refinement happens when start() owns
      // the event-emit sites in Phase 3.
      if (this._eventMentionsSelf(ev)) {
        for (const cb of this._eventListeners) {
          try { cb(ev); }
          catch (err) {
            console.error(`AxonaPeer ${this._node.id} listener threw:`, err);
          }
        }
      }
    });

    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    if (this._engineListenerUnsub) {
      this._engineListenerUnsub();
      this._engineListenerUnsub = null;
    }
    this._started = false;
  }

  /**
   * Bootstrap into the Axona mesh.
   *
   *   await peer.join()           — start standalone; wait for inbound
   *                                 connections.
   *   await peer.join(sponsorId)  — open a channel to a known sponsor
   *                                 (66-char hex node ID) and seed
   *                                 the synaptome from it.
   *
   * Pre-conditions: peer.start() has been called.  If a transport was
   * passed to the constructor, the transport is brought up here
   * (transport.start) and admission is established with the sponsor
   * (transport.openConnection).  The sponsor must already be reachable
   * via the transport — for the web transport that means the bridge's
   * signaling has delivered the sponsor's meshId binding; for the sim
   * transport, the sponsor must be registered in the same SimNetwork.
   *
   * Resolves once the synaptome has been seeded (best-effort) or
   * immediately if no sponsor was given.  Throws if the transport
   * can't reach the sponsor.
   *
   * @param {string} [sponsor]  66-char hex node ID
   * @returns {Promise<void>}
   */
  async join(sponsor) {
    if (!this._started) {
      // The engine-event filter chain must be in place before we
      // start touching the synaptome — peer-joined events fired
      // during the bootstrap walk need to reach our listeners.
      await this.start();
    }

    // Bring up the transport if one was wired in.  Idempotent — the
    // sim and node/web transports all handle a second start() cleanly.
    if (this._transport && typeof this._transport.start === 'function') {
      try { await this._transport.start(this._nodeIdHex()); }
      catch (cause) {
        throw new (await import('../errors.js')).TransportError(
          'TRANSPORT_NOT_STARTED',
          `AxonaPeer.join: transport.start failed (${cause.message})`,
          { cause });
      }
    }

    // No sponsor → standalone start.  We're "joined" but isolated;
    // inbound connections from other peers will populate our
    // synaptome via the usual handshake + bindPeer flow.
    if (sponsor === undefined || sponsor === null) return;

    if (!isHexId(sponsor)) {
      throw new (await import('../errors.js')).TransportError(
        'TRANSPORT_NOT_STARTED',
        `AxonaPeer.join: sponsor must be 66-char hex, got ${typeof sponsor}`,
        { context: { sponsor } });
    }

    // Open a channel to the sponsor.  The transport's openConnection
    // returns false if the sponsor isn't reachable (not registered in
    // the SimNetwork, mesh signaling not delivered, etc).
    if (this._transport && typeof this._transport.openConnection === 'function') {
      const ok = await this._transport.openConnection(sponsor);
      if (!ok) {
        throw new (await import('../errors.js')).TransportError(
          'TRANSPORT_PEER_UNREACHABLE',
          `AxonaPeer.join: sponsor ${sponsor} not reachable`,
          { context: { sponsor } });
      }
    }

    // Seed the synaptome with the sponsor.  Without a real self-lookup
    // (which would need wiring through the engine), this is the minimum
    // viable bootstrap: one channel open, one synapse known.  Future
    // enhancement: walk K-closest via transport.send + the
    // find_closest_set RPC and stratified-fill from results.
    this._seedSynaptomeWithSponsor(sponsor);
  }

  /**
   * Leave the network gracefully.
   *
   *   await peer.leave()
   *   await peer.leave({ drain: true, notify: true, timeoutMs: 5000 })
   *
   * If `notify`, sends a `peer-leaving` notification to every peer in
   * the synaptome so they can drop us proactively (instead of waiting
   * for heartbeat timeouts).  If `drain`, waits up to `timeoutMs` ms
   * for in-flight publishes to settle before closing.  Closes the
   * transport last.  Stops event listeners.
   *
   * Persistence-side snapshot of final state lands in P4 (#32).
   *
   * @param {{drain?: boolean, notify?: boolean, timeoutMs?: number}} [opts]
   * @returns {Promise<void>}
   */
  async leave({ drain = true, notify = true, timeoutMs = 5000 } = {}) {
    if (!this._started) return;
    const selfId = this._nodeIdHex();

    // (1) notify peers (fire-and-forget, bounded by drain window)
    if (notify && this._transport && typeof this._transport.notify === 'function') {
      const peers = this.peers();
      for (const peerId of peers) {
        // Use the transport's notify directly (not peer.notify) so we
        // don't tunnel through 'axona:direct'; this is a transport-
        // level signal, not an application message.
        try {
          await this._transport.notify(peerId, 'peer-leaving', { from: selfId });
        } catch { /* swallow — best-effort */ }
      }
    }

    // (2) optional drain — pause for in-flight publishes / pulls
    if (drain && timeoutMs > 0) {
      // Without a per-publish ack stream we can only bound by time.
      // Apps that want stronger guarantees should await their own
      // pub() promises before calling leave().
      await new Promise(r => setTimeout(r, Math.min(timeoutMs, 50)));
    }

    // (3) force-flush persistence (P4)
    try { await this._flushAllToPersist(); } catch { /* swallow */ }

    // (4) stop event listeners (mirrors stop() from Phase 1)
    if (this._engineListenerUnsub) {
      try { this._engineListenerUnsub(); } catch { /* swallow */ }
      this._engineListenerUnsub = null;
    }

    // (5) close transport
    if (this._transport && typeof this._transport.stop === 'function') {
      try { await this._transport.stop(); }
      catch { /* swallow — we're shutting down */ }
    }

    this._started = false;
  }

  /**
   * @private — best-effort initial synaptome seed.
   * Tries the engine's add-synapse path if available; otherwise sets
   * the synaptome entry directly so peer.peers() and onPeerJoin fire.
   */
  // ─── Persistence wiring (P4) ──────────────────────────────────────
  //
  // Four namespaces, one PersistenceAdapter key each:
  //   'identity'       — IdentityEnvelope from dumpIdentity()
  //   'synaptome'      — [{peerId, weight, latency, stratum, addedBy}]
  //   'subscriptions'  — [{topic, since}]
  //   'wireVersion'    — string (the kernel build that wrote this)
  //
  // On start(): all four loaded if persist is wired and the
  // constructor didn't already supply identity / synaptome.
  // On sub() / sub.stop() / synapse-add: namespace marked dirty,
  // debounced flush scheduled (~5s).  On leave(): force flush.
  //
  // Axon-role state is owned by AxonManager and persisted at that
  // layer (deferred to AxonManager P4-followup).

  async _loadFromPersist() {
    const p = this._persist;
    if (!p) return;

    // Identity — only if constructor didn't supply one.
    if (!this._identity) {
      try {
        const env = await p.load('identity');
        if (env && typeof env === 'object') {
          const { loadIdentity } = await import('../identity/index.js');
          this._identity = await loadIdentity(env);
        }
      } catch (err) {
        this._emitLog?.('warn', 'persist-identity-load-failed', { err: err.message });
      }
    }

    // Synaptome — only if it's currently empty.
    if (this._node?.synaptome && this._node.synaptome.size === 0) {
      try {
        const entries = await p.load('synaptome');
        if (Array.isArray(entries)) {
          for (const s of entries) {
            if (!s?.peerId) continue;
            this._node.synaptome.set(s.peerId, {
              peerId:  s.peerId,
              weight:  s.weight,
              latency: s.latency,
              stratum: s.stratum,
              addedBy: s.addedBy ?? 'persist',
            });
          }
        }
      } catch (err) {
        this._emitLog?.('warn', 'persist-synaptome-load-failed', { err: err.message });
      }
    }

    // Subscriptions — expose as pendingSubscriptions for apps to
    // re-register handlers (functions don't serialize).
    try {
      const subs = await p.load('subscriptions');
      if (Array.isArray(subs)) {
        this.pendingSubscriptions = subs.map(s => ({ ...s }));
      }
    } catch (err) {
      this._emitLog?.('warn', 'persist-subscriptions-load-failed', { err: err.message });
    }
  }

  /** Mark a namespace dirty and schedule a debounced flush. */
  _markPersistDirty(namespace) {
    if (!this._persist) return;
    this._persistDirty.add(namespace);
    if (this._persistTimer) return;
    this._persistTimer = setTimeout(() => {
      this._persistTimer = null;
      this._flushDirtyToPersist().catch(err => {
        this._emitLog?.('warn', 'persist-flush-failed', { err: err.message });
      });
    }, this._persistFlushMs);
    if (typeof this._persistTimer.unref === 'function') this._persistTimer.unref();
  }

  async _flushDirtyToPersist() {
    if (!this._persist || this._persistDirty.size === 0) return;
    const namespaces = [...this._persistDirty];
    this._persistDirty.clear();
    for (const ns of namespaces) {
      try { await this._writeNamespace(ns); }
      catch (err) {
        this._emitLog?.('warn', `persist-write-${ns}-failed`, { err: err.message });
        // Re-queue on failure so the next debounce retries.
        this._persistDirty.add(ns);
      }
    }
  }

  async _writeNamespace(ns) {
    const p = this._persist;
    if (!p) return;
    if (ns === 'identity') {
      if (!this._identity) return;
      const { dumpIdentity } = await import('../identity/index.js');
      await p.save('identity', await dumpIdentity(this._identity));
      return;
    }
    if (ns === 'synaptome') {
      const snap = await this.snapshot();
      await p.save('synaptome', snap.synaptome);
      return;
    }
    if (ns === 'subscriptions') {
      const snap = await this.snapshot();
      await p.save('subscriptions', snap.subscriptions);
      return;
    }
    if (ns === 'wireVersion') {
      const { WIRE_VERSION } = await import('../transport/handshake.js');
      await p.save('wireVersion', WIRE_VERSION);
      return;
    }
  }

  /** Force-flush every dirty namespace immediately. Called on leave. */
  async _flushAllToPersist() {
    if (!this._persist) return;
    if (this._persistTimer) {
      clearTimeout(this._persistTimer);
      this._persistTimer = null;
    }
    // Make sure identity gets written at least once even if it wasn't
    // explicitly marked dirty (first-run case).
    if (this._identity) this._persistDirty.add('identity');
    this._persistDirty.add('wireVersion');
    await this._flushDirtyToPersist();
  }

  // ─── Snapshot / restore (v1.0 escape hatch — A9) ───────────────────
  //
  // Apps that want to manage state outside the bundled
  // PersistenceAdapter can dump a fully-serializable snapshot of this
  // peer's state, store it however they want (encrypted, synced
  // through a different channel, written to a custom database), and
  // reconstruct a peer from it via Peer.fromSnapshot(state, opts).
  //
  // The snapshot carries:
  //   - formatVersion: '1.0'
  //   - identity envelope (id + pubkey hex + privkey base64 + region + createdAt)
  //   - synaptome (list of {peerId, weight, latency, stratum, addedBy})
  //   - subscriptions ([{ topic, lastSeenTs, opts }])
  //   - wireVersion (the kernel build that produced this snapshot)
  //   - snapshotAt (ms timestamp)
  //
  // Restoration is intentionally lazy — fromSnapshot returns a peer
  // that's constructed and the snapshot pre-loaded; the caller still
  // calls peer.join() to bring the transport up.  This keeps
  // snapshot/restore decoupled from network state.

  /**
   * Serialize this peer's state to a JSON-safe envelope.
   *
   * @returns {Promise<object>}
   */
  async snapshot() {
    const { dumpIdentity } = await import('../identity/index.js');
    const { WIRE_VERSION } = await import('../transport/handshake.js');

    const identityEnv = this._identity
      ? await dumpIdentity(this._identity)
      : null;

    const syn = this._node?.synaptome;
    const synaptome = [];
    if (syn) {
      for (const [k, v] of syn.entries()) {
        const peerId =
          (typeof k === 'string' && isHexId(k)) ? k :
          (typeof k === 'bigint')               ? toHex(k) :
          null;
        if (peerId === null) continue;
        synaptome.push({
          peerId,
          weight:   v?.weight   ?? null,
          latency:  v?.latency  ?? null,
          stratum:  v?.stratum  ?? null,
          addedBy:  v?.addedBy  ?? null,
        });
      }
    }

    const subscriptions = [];
    for (const set of this._subscriptions.values()) {
      for (const sub of set) {
        subscriptions.push({
          topic:       sub.topicName,
          since:       sub._opts?.since ?? null,
        });
      }
    }

    return {
      formatVersion: '1.0',
      snapshotAt:    Date.now(),
      wireVersion:   WIRE_VERSION,
      identity:      identityEnv,
      synaptome,
      subscriptions,
    };
  }

  /**
   * Reconstruct a peer from a snapshot envelope.  The returned peer
   * is constructed and its identity / synaptome / subscriptions are
   * pre-loaded, but the transport is NOT started.  Call
   * `await peer.join(sponsor?)` to bring the network connection up.
   *
   * Subscription handlers are NOT restored — they're application
   * state (functions can't be serialized).  Callers must re-register
   * handlers via peer.sub(topic, ...) for each restored subscription;
   * the returned peer exposes the list at `peer.pendingSubscriptions`
   * so apps can iterate.
   *
   * @param {object} state          snapshot envelope from .snapshot()
   * @param {object} opts           AxonaPeer constructor args
   * @param {object} opts.engine
   * @param {object} opts.node
   * @param {object} [opts.axonManager]
   * @param {object} [opts.transport]
   * @returns {Promise<AxonaPeer>}
   */
  static async fromSnapshot(state, { engine, node, axonManager, transport } = {}) {
    if (!state || typeof state !== 'object') {
      throw new TypeError('AxonaPeer.fromSnapshot: state must be a snapshot object');
    }
    if (state.formatVersion !== '1.0') {
      throw new RangeError(`AxonaPeer.fromSnapshot: unsupported formatVersion ${state.formatVersion}`);
    }

    let identity = null;
    if (state.identity) {
      const { loadIdentity } = await import('../identity/index.js');
      identity = await loadIdentity(state.identity);
    }

    // Reconstitute the node + synaptome.  If the caller passed a node
    // we honour it (and skip our own construction), otherwise build a
    // bare node with the identity's id.
    const finalNode = node ?? {
      id:        identity?.id,
      alive:     true,
      synaptome: new Map(),
    };
    if (!finalNode.synaptome) finalNode.synaptome = new Map();
    if (Array.isArray(state.synaptome)) {
      for (const s of state.synaptome) {
        if (!s?.peerId) continue;
        // Default to hex string keys (kernel native).
        finalNode.synaptome.set(s.peerId, {
          peerId:  s.peerId,
          weight:  s.weight,
          latency: s.latency,
          stratum: s.stratum,
          addedBy: s.addedBy ?? 'snapshot',
        });
      }
    }

    const peer = new AxonaPeer({
      engine: engine ?? { onEvent: () => () => {} },
      node:   finalNode,
      axonManager,
      identity,
      transport,
    });
    peer.pendingSubscriptions = Array.isArray(state.subscriptions)
      ? state.subscriptions.map(s => ({ ...s }))
      : [];
    return peer;
  }

  _seedSynaptomeWithSponsor(sponsor) {
    const syn = this._node?.synaptome;
    if (!syn) return;
    if (syn.has?.(sponsor) || syn.has?.(BigInt('0x' + sponsor))) return;

    // Engine-managed path: if the engine exposes addSynapse, use it
    // so its bookkeeping (stratum, decay, anneal pool) stays consistent.
    const engine = this._engine;
    const domain = this._domain;
    if (engine && typeof engine.addSynapse === 'function') {
      try { engine.addSynapse(this._node, sponsor, { addedBy: 'bootstrap' }); return; }
      catch { /* fall through to direct insert */ }
    }

    // Direct insert: the simulator-facing path uses BigInt synaptome
    // keys; the kernel-only path uses hex strings.  Detect by checking
    // an existing entry's key shape, defaulting to hex.
    let firstKey = null;
    for (const k of syn.keys()) { firstKey = k; break; }
    const useBigInt = (typeof firstKey === 'bigint');
    const key = useBigInt ? BigInt('0x' + sponsor) : sponsor;
    syn.set(key, {
      peerId: key, weight: 0.5, latency: 50, stratum: 0,
      addedBy: 'bootstrap',
    });
  }

  // ─── DHT operations ────────────────────────────────────────────────
  //
  // Phase 4 (v0.71.4) — lookup() now owns the body.  No sourceId
  // parameter: the peer IS the source of its own lookups.  Internal
  // sim state (`simEpoch`, decay-tick interval, EMA hops/time) still
  // lives on the engine during the migration window; this peer's
  // lookup() reads/writes `this._engine.X` for those cross-peer
  // concerns.  Phase 5 splits the rest of the shared state.

  async lookup(targetKey) {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;
    if (!node || !node.alive) return null;

    domain.simEpoch++;
    if (++domain.lookupsSinceDecay >= domain.DECAY_INTERVAL) {
      domain._tickDecay();                            // FORGET: periodic
      domain.lookupsSinceDecay = 0;
    }

    const result = await this._lookupStep({
      sourceId:    node.id,
      targetKey,
      hops:        0,
      path:        [node.id],
      trace:       [],
      queried:     new Set([node.id]),
      totalTimeMs: 0,
    });

    // ── LEARN: LTP reinforcement on fast paths ─────────────────────
    if (result.found && result.trace.length > 0) {
      const hopCount = result.trace.length;
      domain._emaHops = domain._emaHops === null
        ? hopCount : 0.9 * domain._emaHops + 0.1 * hopCount;
      domain._emaTime = domain._emaTime === null
        ? result.totalTimeMs : 0.9 * domain._emaTime + 0.1 * result.totalTimeMs;
      if (result.totalTimeMs <= domain._emaTime) {
        this._reinforceWave(result.trace);
      }
    }

    const hops = result.path.length - 1;
    this._bumpLookupStats(result.found, hops, result.totalTimeMs);
    domain._emit({
      type: 'lookup-completed', timestamp: Date.now(),
      sourceId: node.id, targetKey,
      hops, time: result.totalTimeMs, found: result.found,
    });

    return {
      path:  result.path,
      hops,
      time:  result.totalTimeMs,
      found: result.found,
    };
  }

  async subscribe(topicName, handler) {
    // Phase 1: subscribe through the engine-owned AxonManager for
    // this node.  Future phases move this into the peer itself.
    const axon = this._engine.axonFor(this._node);
    return axon.subscribe(this._node.id, topicName, handler);
  }

  async unsubscribe(sub) {
    if (!sub) return;
    const axon = this._engine.axonFor(this._node);
    return axon.unsubscribe(sub);
  }

  async publish(topicName, payload) {
    const axon = this._engine.axonFor(this._node);
    return axon.publish(topicName, payload);
  }

  // ─── Unified pub/sub (v1.0 API) ────────────────────────────────────
  //
  // Replaces the legacy AxonManager.pubsubPublish(bigintTopicKey, json)
  // and AxonManager.pubsubSubscribe(bigintTopicKey) entrypoints with a
  // string-topic API:
  //
  //   const msgId = await peer.pub(topic, message);
  //   const sub   = await peer.sub(topic, envelope => …, { since });
  //   await sub.stop();
  //
  // topic is a string at the API boundary; we hash it via
  // deriveTopicId(peer.nodeIdHex, topic) → 66-char hex topic ID, which
  // is what flows through AxonManager.  Apps don't see the topic ID
  // unless they introspect it on the subscription handle.
  //
  // The envelope shape (delivered to subscribers) is:
  //   { msgId, ts, topic, message, publisher }
  // A2 (#24) extends this with signature + signerPubkey once signing
  // is wired through pub().

  /**
   * Publish a message on `topic`.  Resolves with the content-derived
   * msgId once the publish has been handed to the K-closest replica
   * set (today's AxonManager semantics).
   *
   * Signed by default with the peer's identity; opt-out via
   * `{ sign: false }` for anonymous broadcast.
   *
   * @param {string}  topic     application-level topic name
   * @param {*}       message   JSON-serializable payload
   * @param {object}  [opts]
   * @param {boolean} [opts.sign=true]
   * @returns {Promise<string>} msgId — sha256 of the canonical envelope.
   */
  async pub(topic, message, { sign = true } = {}) {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new PublishError(ErrorCodes.PUBLISH_INVALID_TOPIC,
        `peer.pub: topic must be a non-empty string, got ${typeof topic}`,
        { context: { topic } });
    }
    const am          = this._requireAxonManager('pub');
    const publisherId = this._nodeIdHex();
    const topicId     = await deriveTopicId(publisherId, topic);

    if (sign && !this._identity) {
      throw new PublishError(ErrorCodes.PUBLISH_SIGN_FAILED,
        'peer.pub: identity required for signed publish; pass {identity} to AxonaPeer ' +
        'or call peer.pub(topic, msg, { sign: false }) for anonymous publish',
        { context: { topic } });
    }

    let envelope;
    try {
      envelope = await buildEnvelope({
        topic, message,
        identity: this._identity,
        sign,
      });
    } catch (cause) {
      throw new PublishError(ErrorCodes.PUBLISH_SIGN_FAILED,
        `peer.pub: building envelope failed (${cause.message})`,
        { cause, context: { topic, sign } });
    }

    let json;
    try { json = JSON.stringify(envelope); }
    catch (cause) {
      throw new PublishError(ErrorCodes.PUBLISH_PAYLOAD_TOO_LARGE,
        `peer.pub: envelope not JSON-serializable (${cause.message})`,
        { cause, context: { topic } });
    }

    // AxonManager generates its own publishId for network routing/
    // cache keying.  We pass postHash = envelope.msgId so the relay's
    // replay cache becomes searchable by content-hash for peer.pull()
    // (A3).  publisher is recorded for the metrics counters.
    am.pubsubPublish(topicId, json, {
      publisher: publisherId,
      postHash:  envelope.msgId,
    });
    return envelope.msgId;
  }

  /**
   * Subscribe to `topic`.  Handler is invoked with the full envelope
   * `{ msgId, ts, topic, message, publisher }` for each delivery.
   *
   * @param {string}                       topic
   * @param {(envelope: object) => void}   handler
   * @param {object}                       [opts]
   * @param {'all'|'latest'|number}        [opts.since]  replay control:
   *   - omitted/undefined → live tail (future messages only)
   *   - 'latest'          → most recent cached message + future
   *   - 'all'             → everything in replay cache + future
   *   - timestamp (number) → messages newer than the timestamp + future
   * @returns {Promise<Subscription>}
   */
  async sub(topic, handler, opts = {}) {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new SubscribeError(ErrorCodes.SUBSCRIBE_INVALID_TOPIC,
        `peer.sub: topic must be a non-empty string, got ${typeof topic}`,
        { context: { topic } });
    }
    if (typeof handler !== 'function') {
      throw new SubscribeError(ErrorCodes.SUBSCRIBE_HANDLER_MISSING,
        'peer.sub: handler must be a function',
        { context: { topic } });
    }

    const am          = this._requireAxonManager('sub');
    const publisherId = this._nodeIdHex();
    const topicId     = await deriveTopicId(publisherId, topic);

    // Apply `since` mode by seeding AxonManager's per-topic lastSeenTs
    // BEFORE the subscribe call.  AxonManager passes lastSeenTs in the
    // subscribe envelope; the receiving axon's replay cache filters
    // strictly above it.
    this._applySince(am, topicId, opts.since);

    // Register the handler and the dispatch hook before the network
    // call so deliveries that arrive between submit and resolve are
    // routed correctly.
    const sub = new Subscription({
      peer: this, topicId, topicName: topic, handler, opts,
    });
    if (!this._subscriptions.has(topicId)) this._subscriptions.set(topicId, new Set());
    this._subscriptions.get(topicId).add(sub);
    this._installDeliveryHook(am);

    am.pubsubSubscribe(topicId);
    this._markPersistDirty('subscriptions');
    return sub;
  }

  /** @internal — called by Subscription.stop() */
  async _unsubscribeInternal(sub) {
    const set = this._subscriptions.get(sub.topicId);
    if (set) {
      set.delete(sub);
      if (set.size === 0) {
        this._subscriptions.delete(sub.topicId);
        try {
          this._requireAxonManager('unsubscribe').pubsubUnsubscribe(sub.topicId);
        } catch { /* unsubscribe is best-effort */ }
      }
      this._markPersistDirty('subscriptions');
    }
  }

  /**
   * Pull a specific message by content hash.  The msgId is what
   * peer.pub() returned to the publisher and what subscribers receive
   * as `envelope.msgId`.
   *
   * Bounded by the K-closest set's replay cache window (~100 messages
   * per topic, ~60s grace).  Older messages return null and that's
   * expected — pull is for "did I miss this one?" not durable storage.
   *
   * Because msgId is content-derived and the topic is publisher-
   * scoped, the caller passes `{ topic, publisher }` so we can route
   * the request to the right K-closest set.
   *
   * @param {string} msgId
   * @param {object} opts
   * @param {string} opts.topic       application topic name
   * @param {string} opts.publisher   66-char hex node ID of the topic owner
   * @param {number} [opts.timeoutMs=1000]
   * @returns {Promise<object | null>} envelope or null
   */
  async pull(msgId, { topic, publisher, timeoutMs = 1000 } = {}) {
    if (typeof msgId !== 'string' || msgId.length !== 64) {
      throw new PullError(ErrorCodes.PULL_INVALID_MSGID,
        `peer.pull: msgId must be 64-char hex, got length ${msgId?.length}`,
        { context: { msgId } });
    }
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new PullError(ErrorCodes.PULL_INVALID_MSGID,
        'peer.pull: { topic } is required',
        { context: { msgId } });
    }
    if (!isHexId(publisher)) {
      throw new PullError(ErrorCodes.PULL_INVALID_MSGID,
        'peer.pull: { publisher } must be a 66-char hex node ID',
        { context: { msgId, publisher } });
    }
    const am = this._requireAxonManager('pull');
    if (typeof am.requestPull !== 'function') {
      throw new PullError(ErrorCodes.PULL_AXONS_UNREACHABLE,
        'peer.pull: AxonManager does not support requestPull',
        { context: {} });
    }
    const topicId = await deriveTopicId(publisher, topic);
    const result = await am.requestPull(topicId, msgId, { timeoutMs });
    if (!result) return null;

    // requestPull returns the parsed payload — which is the JSON we
    // wrote in pub(): the envelope itself.  Some legacy AxonManagers
    // return a SignedPost shape; we surface either, leaving
    // verification to the caller via verifyEnvelope().
    if (result && typeof result === 'object' &&
        typeof result.msgId === 'string' &&
        typeof result.ts === 'number') {
      return result;
    }
    // Legacy / unknown shape — return as-is so caller can inspect.
    return result;
  }

  /**
   * Aggregate counters for a topic across the K-closest relay tree.
   *
   * Returns an object `{ publishes, subscribers, deliveries, pulls,
   * reshares, relayCount }`.  Sums per-post counters across all relays
   * that respond; `relayCount` is the number of distinct responding
   * relays so callers can sanity-check coverage.
   *
   * Note: today's AxonManager enforces a publisher-only ownership
   * check on metrics requests — only the topic's publisher gets a
   * non-empty result.  Removing that check (so any peer can audit)
   * is queued as a kernel-side cleanup.
   *
   * @param {string} topic
   * @param {object} opts
   * @param {string} opts.publisher  66-char hex node ID of the topic owner
   * @param {number} [opts.timeoutMs=500]
   * @returns {Promise<{ publishes: number, subscribers: number, deliveries: number, pulls: number, reshares: number, relayCount: number }>}
   */
  async metrics(topic, { publisher, timeoutMs = 500 } = {}) {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new MetricsError(ErrorCodes.METRICS_AXONS_UNREACHABLE,
        'peer.metrics: topic must be a non-empty string',
        { context: { topic } });
    }
    if (!isHexId(publisher)) {
      throw new MetricsError(ErrorCodes.METRICS_AXONS_UNREACHABLE,
        'peer.metrics: { publisher } must be a 66-char hex node ID',
        { context: { topic, publisher } });
    }
    const am = this._requireAxonManager('metrics');
    if (typeof am.requestMetrics !== 'function') {
      return { publishes: 0, subscribers: 0, deliveries: 0, pulls: 0, reshares: 0, relayCount: 0 };
    }
    const topicId = await deriveTopicId(publisher, topic);
    const responses = await am.requestMetrics(topicId, null, { timeoutMs });

    let deliveries = 0, pulls = 0, reshares = 0, publishes = 0;
    let subscribers = 0;
    const relayIds = new Set();
    for (const resp of (responses ?? [])) {
      if (resp?.responderId) relayIds.add(resp.responderId);
      const entries = resp?.entries ?? [];
      publishes = Math.max(publishes, entries.length);    // distinct post hashes seen
      for (const c of entries) {
        deliveries += c.delivery_count ?? 0;
        pulls      += c.pull_count     ?? 0;
        reshares   += c.reshare_count  ?? 0;
      }
      if (typeof resp?.subscribers === 'number') {
        subscribers = Math.max(subscribers, resp.subscribers);
      }
    }
    return {
      publishes,
      subscribers,
      deliveries,
      pulls,
      reshares,
      relayCount: relayIds.size,
    };
  }

  // ─── Direct messaging (v1.0 API) ──────────────────────────────────
  //
  // Three primitives that ride directly on the underlying Transport
  // contract without going through pub/sub:
  //
  //   await peer.send(targetId, message)    — RPC; awaits reply
  //   peer.notify(targetId, message)        — fire-and-forget
  //   peer.onMessage(handler)               — receive direct msgs
  //
  // `targetId` is the 66-char hex node ID of the peer. The peer must
  // already be in the synaptome (transport.openConnection completed)
  // — direct messaging assumes a working channel.  Routing to peers
  // we haven't established a channel with is the responsibility of
  // higher layers (e.g. AxonaPeer.lookup); that's not in scope here.
  //
  // Wire type used between peers is 'axona:direct' so it doesn't
  // collide with the existing typed transport surfaces (lookup_step,
  // reinforce, pubsub:*, etc.).

  /**
   * Send a direct message to `targetId` and await the remote handler's
   * return value (RPC-style).
   *
   * @param {string} targetId 66-char hex node ID
   * @param {*}      message  JSON-serializable
   * @returns {Promise<*>}     remote handler's return value
   */
  async send(targetId, message) {
    if (!isHexId(targetId)) {
      throw new TypeError(`peer.send: targetId must be 66-char hex, got ${typeof targetId}`);
    }
    const t = this._requireTransport('send');
    return t.send(targetId, 'axona:direct', { from: this._nodeIdHex(), message });
  }

  /**
   * Fire-and-forget direct message.  Resolves once enqueued, NOT
   * when delivered.
   *
   * @param {string} targetId
   * @param {*}      message
   * @returns {Promise<void>}
   */
  async notify(targetId, message) {
    if (!isHexId(targetId)) {
      throw new TypeError(`peer.notify: targetId must be 66-char hex, got ${typeof targetId}`);
    }
    const t = this._requireTransport('notify');
    return t.notify(targetId, 'axona:direct', { from: this._nodeIdHex(), message });
  }

  /**
   * Register a handler for inbound direct messages.  At most one
   * handler — calling onMessage again replaces the previous handler.
   *
   * The handler signature is `(senderId, message) => reply | void`:
   *   - For peer.send() callers: any value returned (or its promise)
   *     becomes the resolution of the caller's send().
   *   - For peer.notify() callers: the return value is discarded.
   *
   * @param {(senderId: string, message: any) => any} handler
   */
  onMessage(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onMessage: handler must be a function');
    }
    this._directMessageHandler = handler;
    this._installDirectHandlers();
  }

  _installDirectHandlers() {
    if (this._directHandlersInstalled) return;
    const t = this._transport;
    if (!t || typeof t.onRequest !== 'function') return;

    t.onRequest('axona:direct', async (fromId, payload) => {
      const h = this._directMessageHandler;
      if (!h) return undefined;
      // The wire fromId is the transport's notion (typically the hex
      // nodeId once bindPeer has happened).  payload.from is the
      // sender's self-reported hex id — we prefer the transport's
      // since it's bound at handshake time.
      const senderId = (typeof fromId === 'string' && isHexId(fromId))
        ? fromId : (payload?.from ?? null);
      return await h(senderId, payload?.message);
    });
    t.onNotification('axona:direct', (fromId, payload) => {
      const h = this._directMessageHandler;
      if (!h) return;
      const senderId = (typeof fromId === 'string' && isHexId(fromId))
        ? fromId : (payload?.from ?? null);
      try { h(senderId, payload?.message); }
      catch { /* notification handler errors swallow */ }
    });
    this._directHandlersInstalled = true;
  }

  // ─── Diagnostics + log/error/upgrade event surfaces (A6) ──────────
  //
  // Apps consume these for observability:
  //
  //   peer.health()         → snapshot of synaptome / axon / connections
  //                            / replay-cache / wireVersion / uptime
  //   peer.onLog(level, h)  → 'debug' | 'info' | 'warn' | 'error'
  //   peer.onError(h)       → fires on background AxonaError emissions
  //   peer.onUpgradeRequired(h) → fires on version-handshake mismatch
  //
  // The underlying log/error/upgrade events come from the transport
  // layer (today via the `log` callback we passed to the factory).
  // The peer offers a typed event surface on top so apps don't need
  // to wire transport-specific callbacks themselves.

  /**
   * Synchronous diagnostic snapshot.  Stable shape:
   *
   *   {
   *     nodeId:           '<hex>',
   *     synaptomeSize:    number,
   *     peers:            string[],
   *     subscriptions:    number,
   *     axonRoles:        Array<{topic, isRoot, children, cacheSize}>,
   *     wireVersion:      string | null,
   *     started:          boolean,
   *   }
   *
   * Heavy implementations (per-replay-cache byte sizes, traffic
   * counters) can be added later.  This is intentionally cheap so
   * apps can poll it on a UI tick.
   *
   * @returns {object}
   */
  health() {
    const am = this._axonManager
            ?? (this._engine?.axonManagerFor?.(this._node))
            ?? this._engine?._axonManagers?.get?.(this._node.id)
            ?? null;
    const axonRoles = [];
    if (am && typeof am.inspectRoles === 'function') {
      try {
        for (const r of am.inspectRoles()) {
          axonRoles.push({
            topic:      r.topicId,
            isRoot:     !!r.isRoot,
            children:   Array.isArray(r.children) ? r.children.length : 0,
            cacheSize:  r.replayCacheSize ?? r.cacheSize ?? 0,
          });
        }
      } catch { /* best-effort */ }
    }
    return {
      nodeId:        this._nodeIdHex(),
      synaptomeSize: this._node?.synaptome?.size ?? 0,
      peers:         this.peers(),
      subscriptions: this._subscriptions.size,
      axonRoles,
      wireVersion:   this._transport?.wireVersion ?? null,
      started:       this._started === true,
    };
  }

  /**
   * Subscribe to log-level events.
   * @param {'debug'|'info'|'warn'|'error'} level
   * @param {(msg: string, context?: object) => void} handler
   * @returns {() => void} unsubscribe
   */
  onLog(level, handler) {
    if (!['debug', 'info', 'warn', 'error'].includes(level)) {
      throw new TypeError(`peer.onLog: level must be one of debug|info|warn|error, got ${String(level)}`);
    }
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onLog: handler must be a function');
    }
    if (!this._logHandlers) this._logHandlers = new Map();
    if (!this._logHandlers.has(level)) this._logHandlers.set(level, new Set());
    const set = this._logHandlers.get(level);
    set.add(handler);
    this._installTransportLogHook();
    return () => set.delete(handler);
  }

  /**
   * Subscribe to background AxonaError emissions (things the kernel
   * surfaces asynchronously rather than throwing — e.g. transport
   * failures during heartbeat, persistence-layer warnings).
   *
   * @param {(err: AxonaError) => void} handler
   * @returns {() => void} unsubscribe
   */
  onError(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onError: handler must be a function');
    }
    if (!this._errorHandlers) this._errorHandlers = new Set();
    this._errorHandlers.add(handler);
    return () => this._errorHandlers.delete(handler);
  }

  /**
   * Subscribe to wire-version handshake mismatches.  Handler receives
   * the UpgradeRequiredError with full context (reason, server
   * version, client version, downloadUrl).
   *
   * @param {(err: AxonaError) => void} handler
   * @returns {() => void} unsubscribe
   */
  onUpgradeRequired(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onUpgradeRequired: handler must be a function');
    }
    if (!this._upgradeHandlers) this._upgradeHandlers = new Set();
    this._upgradeHandlers.add(handler);
    return () => this._upgradeHandlers.delete(handler);
  }

  // ── internal: emit helpers + transport log hook ────────────────────

  /** @internal — kernel/transport modules call this. */
  _emitLog(level, msg, context) {
    const set = this._logHandlers?.get(level);
    if (!set || set.size === 0) return;
    for (const h of set) { try { h(msg, context); } catch { /* swallow */ } }
  }

  /** @internal — kernel modules call this on background errors. */
  _emitError(err) {
    if (!this._errorHandlers || this._errorHandlers.size === 0) return;
    for (const h of this._errorHandlers) { try { h(err); } catch { /* swallow */ } }
    // UpgradeRequired errors fan to their own channel too.
    if (err?.code === 'UPGRADE_REQUIRED' && this._upgradeHandlers) {
      for (const h of this._upgradeHandlers) { try { h(err); } catch { /* swallow */ } }
    }
  }

  _installTransportLogHook() {
    if (this._transportLogHooked) return;
    const t = this._transport;
    if (!t) return;
    // Transports accept a log(event, data) callback at construction.
    // For the kernel-side event surface we wrap any pre-existing log
    // hook (preserved via t._log if present) so both consumers still
    // get fed.  Falls back gracefully on transports without _log.
    const orig = (typeof t._log === 'function') ? t._log : (() => {});
    t._log = (event, data) => {
      try { orig(event, data); } catch { /* keep going */ }
      // Heuristic: events containing 'failed' or 'error' route to warn.
      const level = (event.includes('failed') || event.includes('error'))
        ? 'warn' : 'debug';
      this._emitLog(level, event, data);
    };
    this._transportLogHooked = true;
  }

  _requireTransport(callerName) {
    const t = this._transport ?? this._engine?.transport ?? null;
    if (!t) {
      throw new Error(`peer.${callerName}: no transport available; ` +
        'pass {transport} to the AxonaPeer constructor');
    }
    return t;
  }

  // ── AxonManager glue ────────────────────────────────────────────

  _requireAxonManager(callerName) {
    if (this._axonManager) return this._axonManager;
    // Fallback: ask the engine for this node's AxonManager.  Different
    // engine builds expose this differently; we probe in priority
    // order and cache the result.
    const engine = this._engine;
    const domain = this._domain;
    let am = null;
    if (typeof engine?.axonManagerFor === 'function') {
      am = engine.axonManagerFor(this._node);
    } else if (engine?._axonManagers instanceof Map) {
      am = engine._axonManagers.get(this._node.id);
    }
    if (!am) {
      throw new PublishError(ErrorCodes.PUBLISH_INVALID_TOPIC,
        `peer.${callerName}: no AxonManager available; ` +
        'pass {axonManager} to the AxonaPeer constructor or wire engine.axonManagerFor()',
      );
    }
    this._axonManager = am;
    return am;
  }

  _installDeliveryHook(am) {
    if (this._deliveryHookInstalled) return;
    if (typeof am.onPubsubDelivery !== 'function') return;
    am.onPubsubDelivery((topicId, json, publishId, publishTs) => {
      this._dispatchDelivery(topicId, json, publishId, publishTs);
    });
    this._deliveryHookInstalled = true;
  }

  _dispatchDelivery(topicId, json, publishId, publishTs) {
    const set = this._subscriptions.get(topicId);
    if (!set || set.size === 0) return;
    let envelope;
    try {
      envelope = JSON.parse(json);
      // Defence: enforce envelope shape so apps always see consistent
      // fields even if a malformed peer sends garbage.
      if (!envelope || typeof envelope !== 'object' ||
          typeof envelope.msgId !== 'string' ||
          typeof envelope.ts !== 'number' ||
          typeof envelope.topic !== 'string' ||
          !('message' in envelope)) {
        throw new Error('malformed envelope');
      }
    } catch {
      // Fall back to a synthetic envelope carrying the raw json as
      // message and the AxonManager's publishId as msgId — at least
      // the handler still fires with something it can inspect.
      envelope = {
        msgId:    publishId,
        ts:       publishTs,
        topic:    null,
        message:  json,
      };
    }
    for (const sub of set) sub._deliver(envelope);
  }

  _applySince(am, topicId, since) {
    // The AxonManager tracks lastSeenTs per topic in _lastSeenTsByTopic.
    // The subscribe call reads this and includes it in the outbound
    // subscribe envelope; the axon's replay filter applies it strictly.
    // We seed it here based on the `since` mode.
    if (!am._lastSeenTsByTopic) return;     // unknown AxonManager build
    if (since === undefined) {
      // Live tail: only future messages.  Seed with a sentinel just
      // below the current time so cached messages are filtered out.
      am._lastSeenTsByTopic.set(topicId, Date.now());
      return;
    }
    if (since === 'all') {
      am._lastSeenTsByTopic.set(topicId, 0);
      return;
    }
    if (since === 'latest') {
      // Approximate: ask for the most recent ~1s of cache + future.
      am._lastSeenTsByTopic.set(topicId, Date.now() - 1000);
      return;
    }
    if (typeof since === 'number') {
      am._lastSeenTsByTopic.set(topicId, since);
      return;
    }
    throw new SubscribeError(ErrorCodes.SUBSCRIBE_INVALID_TOPIC,
      `peer.sub: invalid since value: ${String(since)}`,
      { context: { since } });
  }

  _nodeIdHex() {
    const id = this._node.id;
    if (typeof id === 'string' && isHexId(id)) return id;
    if (typeof id === 'bigint') return toHex(id);
    throw new PublishError(ErrorCodes.PUBLISH_INVALID_TOPIC,
      `peer.pub: node.id must be 66-char hex or bigint, got ${typeof id}`,
      { context: { id } });
  }

  // ─── Identity & observability ──────────────────────────────────────

  getNodeId() {
    return this._node.id;
  }

  /**
   * Phase 2: own the synaptome-snapshot construction directly off the
   * local NeuronNode.  No engine round-trip.  Returns the per-node
   * snapshot — peer ids, weights, latencies, stratum indices.  The
   * application gets a frozen view; the protocol mutates the
   * underlying state independently.
   */
  getSynaptome() {
    if (!this._node) return [];
    return this._node.getSynaptomeSnapshot();
  }

  /**
   * Phase 5b: bump this peer's lookup-stat accumulators.  Called at the
   * end of lookup() with the outcome.  Replaces the engine-side
   * `_bumpLookupStats(node, ...)` Map write — same shape, but data
   * lives on the peer where the read site (getMetrics) consumes it.
   */
  _bumpLookupStats(found, hops, latency) {
    const s = this._stats;
    s.attempted++;
    if (found) {
      s.succeeded++;
      s.sumHops    += hops;
      s.sumLatency += latency;
    }
  }

  /**
   * Phase 5b: reset stat accumulators.  Called by the engine's cycle
   * snapshot (snapshotMetrics with reset=true) on each tick.
   */
  _resetStats() {
    const s = this._stats;
    s.attempted = 0; s.succeeded = 0; s.sumHops = 0; s.sumLatency = 0;
  }

  /**
   * Phase 5b — lookup stats are now peer-owned in `this._stats`.  The
   * engine's `_nodeStats` Map is vestigial (no kernel reader).
   * `snapshotMetrics` on the engine resets via the peer's own
   * `_resetStats()` instead of writing to the map.
   */
  getMetrics() {
    const node = this._node;
    if (!node) return null;
    const stats = this._stats;
    const cycleStats = {
      lookupsAttempted: stats.attempted,
      lookupsSucceeded: stats.succeeded,
      avgHops:    stats.succeeded > 0 ? stats.sumHops    / stats.succeeded : 0,
      avgLatency: stats.succeeded > 0 ? stats.sumLatency / stats.succeeded : 0,
    };
    const traffic = {
      msgsSent:     node.msgsSent     | 0,
      msgsReceived: node.msgsReceived | 0,
      byType:       node.msgsByType ? { ...node.msgsByType } : {},
    };
    return {
      simEpoch:             this._domain.simEpoch,
      synaptomeSize:        node.synaptome.size,
      incomingSynapsesSize: node.incomingSynapses.size,
      temperature:          node.temperature ?? this._domain.T_INIT,
      cycleStats,
      traffic,
    };
  }

  // ─── Read-only candidate scoring (Phase 2) ─────────────────────────
  //
  // These methods are pure functions of this peer's local state
  // (synaptome, incomingSynapses) plus the routing target.  They take
  // no `node` parameter — `this._node` is the receiver.  The engine's
  // versions of the same names delegate here via `_peerFor(node)`.

  /**
   * Vitality score for a synapse.  weight × recency, where recency
   * decays exponentially from the synapse's last reinforcement epoch.
   * LTP-locked synapses (inertia > current epoch) get recency = 1.0.
   */
  _vitality(syn) {
    let recency;
    if (syn.inertia > this._domain.simEpoch) {
      recency = 1.0;
    } else {
      const elapsed = this._domain.simEpoch - syn.inertia;
      recency = Math.max(0.1, Math.exp(-elapsed / this._domain.RECENCY_HALF_LIFE));
    }
    return syn.weight * recency;
  }

  /**
   * Two-hop AP scoring with parallel `lookahead_probe` RPCs.  Body
   * matches AxonaEngine._bestByTwoHopAP byte-for-byte; only
   * the receiver changes (was `current` parameter, now `this._node`).
   * The engine method now delegates here.
   */
  async _bestByTwoHopAP(candidates, targetKey, currentDist) {
    const ranked = candidates.map(s => {
      const ap = Number(currentDist - (s.peerId ^ targetKey)) / s.latency;
      return { s, ap };
    }).sort((a, b) => b.ap - a.ap);

    const probeSet = ranked.slice(0, this._domain.LOOKAHEAD_ALPHA).map(x => x.s);

    // Short-circuit: any probe whose first-hop sits exactly on the
    // target wins outright (zero remaining XOR distance).
    for (const first of probeSet) {
      if ((first.peerId ^ targetKey) === 0n) return first;
    }

    // Parallel lookahead probes.  Each rejected probe is treated like
    // an empty-forward response — the source projects the second-hop
    // latency as 0 and distance as the first-hop's own distance to
    // target, the same fallback as `if (!fwd.length)` in the legacy
    // code path.
    const settled = await Promise.allSettled(
      probeSet.map(first =>
        this._node.transport.send(first.peerId, 'lookahead_probe', {
          target:   targetKey,
          fromDist: first.peerId ^ targetKey,
        })
      )
    );

    let bestSyn = null, bestAP2 = -Infinity;
    for (let i = 0; i < probeSet.length; i++) {
      const first = probeSet[i];
      const firstDist = first.peerId ^ targetKey;
      const r = settled[i];

      let twoHopDist, secondLat;
      if (r.status !== 'fulfilled' || !r.value || r.value.terminal) {
        twoHopDist = firstDist;
        secondLat  = 0;
      } else {
        twoHopDist = r.value.peerId ^ targetKey;
        secondLat  = r.value.latency;
      }

      const ap2 = Number(currentDist - twoHopDist) / (first.latency + secondLat);
      if (ap2 > bestAP2) { bestAP2 = ap2; bestSyn = first; }
    }
    return bestSyn ?? this._node.bestByAP(candidates, targetKey, 0);
  }

  /**
   * Pure synchronous greedy 1-hop nextHop selector.  Used by
   * `routeMessage` to find a first-hop closer to target than self.
   * Returns peerId or null if no synapse makes XOR progress.
   */
  _greedyNextHopToward(targetId) {
    if (!this._node?.alive) return null;
    const target = (typeof targetId === 'bigint')
      ? targetId
      : BigInt('0x' + targetId);
    let bestPeerId = null;
    let bestDist   = this._node.id ^ target;
    for (const syn of this._node.synaptome.values()) {
      const d = syn.peerId ^ target;
      if (d < bestDist) { bestDist = d; bestPeerId = syn.peerId; }
    }
    return bestPeerId;
  }

  /**
   * Bounded 2-hop "anyone closer than me?" check.  Parallel
   * `lookahead_probe` RPCs to each first-hop synapse; aggregates the
   * 2-hop responses + incomingSynapses-as-reverse-routing; returns
   * the globally-closest peer id strictly closer than self, or null
   * if this peer is a true 2-hop terminal.
   */
  async _findCloserInTwoHops(targetId) {
    const node = this._node;
    const target = (typeof targetId === 'bigint')
      ? targetId
      : BigInt('0x' + targetId);
    const myDist = node.id ^ target;
    let bestPeerId = null;
    let bestDist   = myDist;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length > 0) {
      const settled = await Promise.allSettled(
        probeTargets.map(peerId =>
          node.transport.send(peerId, 'lookahead_probe', { target, fromDist: myDist })
        )
      );
      for (const r of settled) {
        if (r.status !== 'fulfilled' || !r.value || r.value.terminal) continue;
        const d = r.value.peerId ^ target;
        if (d < bestDist) {
          bestDist   = d;
          bestPeerId = r.value.peerId;
        }
      }
    }
    for (const syn of node.incomingSynapses.values()) {
      const d = syn.peerId ^ target;
      if (d < bestDist) { bestDist = d; bestPeerId = syn.peerId; }
    }
    return bestPeerId;
  }

  onEvent(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('AxonaPeer.onEvent: handler must be a function');
    }
    this._eventListeners.add(handler);
    return () => this._eventListeners.delete(handler);
  }

  // ─── Mesh introspection (v1.0 API) ─────────────────────────────────
  //
  // Three primitives apps use to track who's in their synaptome:
  //
  //   peer.peers()        → string[] of 66-char hex nodeIds
  //   peer.onPeerJoin(cb) → fires (peerId, ctx) on synapse admission
  //   peer.onPeerLeave(cb)→ fires (peerId, ctx) on synapse eviction
  //
  // Both event helpers return an unsubscribe function.  `ctx` is the
  // underlying event object so callers can inspect the addedBy /
  // reason without going to the lower-level onEvent stream.

  /**
   * Current synaptome membership as hex node IDs.
   * @returns {string[]}
   */
  peers() {
    const syn = this._node?.synaptome;
    if (!syn || typeof syn.keys !== 'function') return [];
    const out = [];
    for (const id of syn.keys()) {
      if (typeof id === 'string' && isHexId(id))      out.push(id);
      else if (typeof id === 'bigint')                out.push(toHex(id));
    }
    return out;
  }

  /**
   * @param {(peerId: string, event?: object) => void} handler
   * @returns {() => void} unsubscribe
   */
  onPeerJoin(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onPeerJoin: handler must be a function');
    }
    return this._onPeerLifecycleEvent('peer-joined', handler);
  }

  /**
   * @param {(peerId: string, event?: object) => void} handler
   * @returns {() => void} unsubscribe
   */
  onPeerLeave(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('peer.onPeerLeave: handler must be a function');
    }
    return this._onPeerLifecycleEvent('peer-left', handler);
  }

  _onPeerLifecycleEvent(eventType, handler) {
    const filter = (ev) => {
      if (!ev || ev.type !== eventType) return;
      const peerId = ev.peerId;
      const hex =
        (typeof peerId === 'string' && isHexId(peerId)) ? peerId :
        (typeof peerId === 'bigint')                    ? toHex(peerId) :
        null;
      if (hex === null) return;
      try { handler(hex, ev); }
      catch { /* listener errors are app-level; swallow */ }
    };
    this._eventListeners.add(filter);
    return () => this._eventListeners.delete(filter);
  }

  // ─── Internal: event filtering ────────────────────────────────────

  /**
   * @private
   * Decide whether a global engine event mentions this peer.  Phase 1
   * filter; refined in Phase 3 when the per-node event-emit sites
   * land directly on AxonaPeer.
   */
  _eventMentionsSelf(ev) {
    if (!ev || typeof ev !== 'object') return false;
    const me = this._node.id;
    // Common fields across the ProtocolEvent union.  Check all that
    // are documented in src/contracts/types.js; if the event doesn't
    // carry any of them it's a global event (cycle-snapshot) and
    // every per-peer instance receives it.
    return (
      ev.nodeId    === me ||
      ev.peerId    === me ||
      ev.observerId === me ||
      ev.sourceId  === me ||
      ev.type === 'cycle-snapshot'
    );
  }

  // ─── Write operations (Phase 3) ────────────────────────────────────
  //
  // Methods that mutate this peer's local state.  Bodies are copied
  // from AxonaEngine verbatim; `node` → `this._node`,
  // `this.X` (engine config) → `this._engine.X`, `this._vitality(node, s)`
  // → `this._vitality(s)` (peer's own method).  The engine retains
  // 1-line delegators for backward compat with internal callers.

  /** Admission gate.  Same logic as engine._addByVitality verbatim. */
  async _addByVitality(newSyn) {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;
    const cap = node._maxSynaptome ?? domain.MAX_SYNAPTOME;

    let victim = null;
    if (node.synaptome.size >= cap) {
      let minV = Infinity, minVAny = Infinity, victimAny = null;
      for (const s of node.synaptome.values()) {
        if (s.inertia > domain.simEpoch) continue;
        const v = this._vitality(s);
        if (v < minVAny) { minVAny = v; victimAny = s; }
        if (!s.bootstrap && v < minV) { minV = v; victim = s; }
      }
      victim = victim ?? victimAny;
      if (!victim) return false;
    }

    const opened = await node.transport.openConnection(newSyn.peerId);
    if (!opened) return false;

    const measuredLat = node.transport.getLatency(newSyn.peerId);
    newSyn.latency = (measuredLat >= 0) ? measuredLat : 200;

    if (victim) {
      node.synaptome.delete(victim.peerId);
      node.connections?.delete(victim.peerId);
      await node.transport.closeConnection(victim.peerId);
    }
    node.addSynapse(newSyn);
    return true;
  }

  /** LTP reinforcement wave along a successful lookup trace. */
  _reinforceWave(trace) {
    for (let i = trace.length - 1; i >= 0; i--) {
      const { fromId, synapse } = trace[i];
      this._node.transport.notify(fromId, 'reinforce', { synapsePeerId: synapse.peerId })
        .catch(err => console.error('AxonaPeer: reinforce notify failed:', err));
    }
  }

  /**
   * Triadic-closure transit-counting.  After TRIADIC_THRESHOLD
   * observations of (origin→nextId) transiting through us, send the
   * origin a 'triadic_introduce' notification.
   */
  _recordTransit(originId, nextId) {
    const node = this._node;
    const key   = `${originId}_${nextId}`;
    const count = (node.transitCache.get(key) ?? 0) + 1;
    if (count >= this._domain.TRIADIC_THRESHOLD) {
      node.transitCache.delete(key);
      node.transport.notify(originId, 'triadic_introduce', { peerId: nextId })
        .catch(err => console.error('AxonaPeer: triadic_introduce notify failed:', err));
    } else {
      node.transitCache.set(key, count);
    }
  }

  /**
   * Anneal step — replace the weakest synapse with a candidate from
   * the under-represented stratum group.  Emits 'anneal-fired' via
   * the engine's event bus (Phase 3 retains shared bus; future phase
   * may split per-peer).
   */
  async _tryAnneal() {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;
    if (!node.alive || node.synaptome.size === 0) return;

    let victim = null, weakW = Infinity;
    for (const s of node.synaptome.values()) {
      if (s.inertia > domain.simEpoch) continue;
      if (s.weight < weakW) { weakW = s.weight; victim = s; }
    }
    if (!victim) return;

    const counts = new Array(domain.STRATA_GROUPS).fill(0);
    for (const s of node.synaptome.values()) {
      counts[Math.min(domain.STRATA_GROUPS - 1, s.stratum >>> 2)]++;
    }
    let targetGroup = 0, minCount = Infinity;
    for (let g = 0; g < domain.STRATA_GROUPS; g++) {
      if (counts[g] < minCount) { minCount = counts[g]; targetGroup = g; }
    }

    const lo = targetGroup * 4, hi = lo + 3;
    const candidate = await this._localCandidate(lo, hi);
    if (!candidate || node.synaptome.has(candidate.id)) return;

    node.synaptome.delete(victim.peerId);
    node.connections?.delete(victim.peerId);
    await node.transport.closeConnection(victim.peerId);

    const opened = await node.transport.openConnection(candidate.id);
    if (!opened) return;

    const measuredLat = node.transport.getLatency(candidate.id);
    const latMs   = (measuredLat >= 0) ? measuredLat : 200;
    const stratum = clz264(node.id ^ candidate.id);
    const syn     = new Synapse({ peerId: candidate.id, latencyMs: latMs, stratum });
    syn.weight    = 0.1;
    syn._addedBy  = 'anneal';
    node.addSynapse(syn);
    domain._emit({
      type: 'anneal-fired', timestamp: Date.now(),
      observerId: node.id, evicted: victim.peerId, admitted: candidate.id,
    });
  }

  /**
   * Dead-synapse replacement.  Closes the dead channel, finds a
   * candidate in the same stratum group, opens a fresh channel.
   */
  async _evictAndReplace(deadSyn) {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;

    node.synaptome.delete(deadSyn.peerId);
    node.connections?.delete(deadSyn.peerId);
    await node.transport.closeConnection(deadSyn.peerId);

    const group = Math.min(domain.STRATA_GROUPS - 1, deadSyn.stratum >>> 2);
    const candidate = await this._localCandidate(group * 4, group * 4 + 3);
    if (!candidate || node.synaptome.has(candidate.id)) return null;

    const opened = await node.transport.openConnection(candidate.id);
    if (!opened) return null;

    const weights = [];
    for (const s of node.synaptome.values()) weights.push(s.weight);
    weights.sort((a, b) => a - b);
    const medW = weights.length > 0 ? weights[weights.length >> 1] : domain.VITALITY_FLOOR;

    const measuredLat = node.transport.getLatency(candidate.id);
    const latMs   = (measuredLat >= 0) ? measuredLat : 200;
    const stratum = clz264(node.id ^ candidate.id);
    const syn     = new Synapse({ peerId: candidate.id, latencyMs: latMs, stratum });
    syn.weight    = medW;
    syn._addedBy  = 'evictReplace';
    node.addSynapse(syn);
    return syn;
  }

  /**
   * 2-hop neighbourhood scan via parallel `local_probe` RPCs.  Picks
   * a random candidate from the under-represented stratum group [lo, hi].
   * Returns `{id}` or null.
   */
  async _localCandidate(lo, hi) {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length === 0) return null;

    const settled = await Promise.allSettled(
      probeTargets.map(peerId => node.transport.send(peerId, 'local_probe', null))
    );

    const candidates = [];
    outer:
    for (const r of settled) {
      if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
      for (const id of r.value) {
        if (id === node.id) continue;
        if (node.synaptome.has(id)) continue;
        const stratum = clz264(node.id ^ id);
        if (stratum < lo || stratum > hi) continue;
        candidates.push(id);
        if (candidates.length >= domain.ANNEAL_LOCAL_SAMPLE) break outer;
      }
    }
    if (candidates.length === 0) return null;

    const chosenId = candidates[Math.floor(Math.random() * candidates.length)];
    return { id: chosenId };
  }

  // ─── Routed messaging + pub/sub primitives (Phase 3d–f) ────────────
  //
  // These deliver AxonManager's pub/sub on top of NH-1's transport
  // contract.  Bodies are copied from the engine verbatim; `node` →
  // `this._node`; the per-peer handler tables continue to live on
  // `this._engine._routedHandlers` / `_directHandlers` until Phase 4
  // splits the storage too.  This is intentional: minimising changes
  // to handler-storage shape during Phase 3 keeps the gate strict.

  /**
   * K-closest iterative search.  Async; uses parallel
   * `find_closest_set` RPCs.  Returns BigInt peer ids sorted by XOR
   * distance to targetId.
   */
  async findKClosest(targetId, K = 5, { alpha = 3, maxRounds = 40 } = {}) {
    const src = this._node;
    if (!src) return [];
    const targetBig = topicToBigInt(targetId);

    const distances = new Map();
    const addCandidate = (peerId) => {
      if (typeof peerId !== 'bigint' || distances.has(peerId)) return;
      distances.set(peerId, peerId ^ targetBig);
    };

    addCandidate(src.id);
    for (const syn of src.synaptome.values())         addCandidate(syn.peerId);
    for (const syn of src.incomingSynapses.values())  addCandidate(syn.peerId);

    const visited = new Set();
    let lastPoolSize = 0;
    let stableRounds = 0;

    for (let round = 0; round < maxRounds; round++) {
      const sorted = [...distances.entries()]
        .sort((a, b) => a[1] < b[1] ? -1 : 1)
        .map(([peerId]) => peerId);
      const topK = sorted.slice(0, K);
      const topKAllVisited = topK.every(p => visited.has(p));

      let toQuery = topK.filter(p => !visited.has(p)).slice(0, alpha);
      if (toQuery.length < alpha) {
        const remaining = alpha - toQuery.length;
        const beyond = sorted
          .filter(p => !visited.has(p) && !topK.includes(p))
          .slice(0, remaining);
        toQuery = toQuery.concat(beyond);
      }
      if (toQuery.length === 0) break;

      const probes = toQuery.filter(p => p !== src.id);
      for (const p of toQuery) visited.add(p);

      if (probes.length > 0) {
        const settled = await Promise.allSettled(
          probes.map(peerId =>
            src.transport.send(peerId, 'find_closest_set',
              { target: targetBig, K: this._domain._k })
          )
        );
        for (const r of settled) {
          if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
          for (const peerId of r.value) addCandidate(peerId);
        }
      }

      const grew = distances.size > lastPoolSize;
      lastPoolSize = distances.size;
      stableRounds = grew ? 0 : stableRounds + 1;
      if (topKAllVisited && stableRounds >= 1) break;
    }

    return [...distances.entries()]
      .sort((a, b) => a[1] < b[1] ? -1 : 1)
      .slice(0, K)
      .map(([peerId]) => peerId);
  }

  /**
   * Send a routed message starting from this peer.  Greedy 1-hop or
   * 2-hop terminal check; dispatches local routed handler; if not
   * consumed AND not terminal, forwards via route_msg request chain.
   */
  async routeMessage(targetId, type, payload, opts = {}) {
    const originNode = this._node;
    const originId   = opts.fromId ?? nodeIdToHex(originNode.id);
    const targetBig  = topicToBigInt(targetId);

    let nextHopId = this._greedyNextHopToward(targetBig);
    let isTerminal = nextHopId === null;
    if (isTerminal) {
      const closer = await this._findCloserInTwoHops(targetBig);
      if (closer !== null && closer !== originNode.id) {
        nextHopId  = closer;
        isTerminal = false;
      }
    }

    const result = await this._deliverRouted(type, payload, {
      fromId:   originId,
      targetId: targetBig,
      hopCount: 0,
      isTerminal,
      node:     originNode,
    });

    if (result === 'consumed') {
      return { consumed: true, atNode: originNode.id, hops: 0 };
    }
    if (isTerminal) {
      return { consumed: false, atNode: originNode.id, hops: 0, terminal: true };
    }

    try {
      const downstream = await originNode.transport.send(nextHopId, 'route_msg', {
        type, payload, targetId: targetBig, hops: 1, originId,
      });
      return downstream;
    } catch {
      return { consumed: false, atNode: originNode.id, hops: 0, exhausted: true };
    }
  }

  /**
   * Dispatch a routed message to the local handler for `type`.
   * Returns the handler's return value (truthy = 'consumed' or other
   * meaningful response; falsy/throw → 'forward').
   */
  async _deliverRouted(type, payload, meta) {
    const node = this._node;
    const handler = this._routedHandlers.get(type);
    if (!handler) return 'forward';
    try {
      const result = await handler(payload, meta);
      return result || 'forward';
    } catch (err) {
      console.error(`AxonaPeer routed handler error at ${node.id} for '${type}':`, err);
      return 'forward';
    }
  }

  /**
   * Fire-and-forget direct notification to one peer.  `type` is the
   * application name; the wire type is `direct_${type}`.
   */
  async sendDirect(peerId, type, payload) {
    const fromNode = this._node;
    if (!fromNode?.alive || !fromNode.transport) return false;
    const peerBig = topicToBigInt(peerId);
    try {
      const ok = await fromNode.transport.notify(peerBig, `direct_${type}`, payload);
      return ok !== false;
    } catch {
      return false;
    }
  }

  /**
   * Pick a child to promote as sub-axon — prefer existing high-weight
   * synaptome children; fall back to XOR-closest existing child.
   */
  _pickRecruitPeer(role, meta, subscriberId) {
    const node = this._node;
    if (role.children.size === 0) return null;
    const selfHex   = nodeIdToHex(node.id);
    const forwarder = meta.fromId;
    const dead      = node._deadPeers || new Set();

    const synapseWeights = new Map();
    for (const syn of node.synaptome.values()) {
      if (dead.has(syn.peerId)) continue;
      synapseWeights.set(nodeIdToHex(syn.peerId), {
        weight:  syn.weight,
        latency: syn.latency ?? syn.latencyMs ?? 0,
      });
    }

    let bestChildId = null;
    let bestScore = -Infinity;
    for (const childId of role.children.keys()) {
      if (childId === selfHex)   continue;
      if (childId === forwarder) continue;
      const s = synapseWeights.get(childId);
      if (!s) continue;
      const score = s.weight * 1_000_000 - s.latency;
      if (score > bestScore) { bestScore = score; bestChildId = childId; }
    }
    if (bestChildId) return bestChildId;

    const subBig = topicToBigInt(subscriberId);
    let best = null;
    let bestDist = null;
    for (const childId of role.children.keys()) {
      if (childId === selfHex)   continue;
      if (childId === forwarder) continue;
      const d = BigInt('0x' + childId) ^ subBig;
      if (bestDist === null || d < bestDist) { bestDist = d; best = childId; }
    }
    return best;
  }

  /**
   * Pick an external synaptome peer (not yet a child) to become a new
   * sub-axon — XOR-closest to the new subscriber's id.
   */
  _pickRelayPeer(role, subscriberId, forwarderId) {
    const node = this._node;
    if (!node?.alive) return null;
    const selfHex = nodeIdToHex(node.id);
    const subBig  = topicToBigInt(subscriberId);
    const dead    = node._deadPeers || new Set();

    const considered = new Map();
    for (const syn of node.synaptome.values()) {
      const peerId = syn.peerId;
      if (dead.has(peerId)) continue;
      const hex = nodeIdToHex(peerId);
      if (hex === selfHex)       continue;
      if (hex === forwarderId)   continue;
      if (hex === subscriberId)  continue;
      if (role.children.has(hex)) continue;
      if (considered.has(hex))    continue;
      considered.set(hex, { peerId, distToSub: peerId ^ subBig });
    }
    if (considered.size === 0) return null;

    let bestHex = null, bestDist = null;
    for (const [hex, rec] of considered) {
      if (bestDist === null || rec.distToSub < bestDist) {
        bestDist = rec.distToSub;
        bestHex  = hex;
      }
    }
    return bestHex;
  }

  /**
   * Register a routed-message handler for `type`.  Per-peer storage;
   * engine version still works because engine delegates here.
   */
  onRoutedMessage(type, handler) {
    this._routedHandlers.set(type, handler);
  }

  /**
   * Register a direct-message handler for `type`.  Bridges to a
   * transport.onNotification listener on `direct_${type}`.
   */
  onDirectMessage(type, handler) {
    const node = this._node;
    const wireType = `direct_${type}`;
    if (!this._directHandlers.has(type)) {
      node.transport.onNotification(wireType, (fromId, payload) => {
        const h = this._directHandlers.get(type);
        if (!h) return;
        const fromHex = (typeof fromId === 'bigint') ? nodeIdToHex(fromId) : fromId;
        try {
          h(payload, { fromId: fromHex, type });
        } catch (err) {
          console.error(`AxonaPeer direct handler error at ${node.id} for '${type}':`, err);
        }
      });
    }
    this._directHandlers.set(type, handler);
  }

  // ─── Routing tick — _lookupStep + _lookupResult (Phase 3g) ─────────
  //
  // _lookupStep is NH-1's per-hop routing logic.  It runs on the
  // receiver of a 'lookup_step' request: collects forward-progress
  // candidates from local synaptome + incoming, evicts dead synapses
  // and replaces them, applies iterative-fallback if no candidate
  // makes XOR progress, selects a next hop (direct → epsilon → 2-hop
  // AP), applies LEARN side-effects (incoming promotion, hop caching,
  // triadic closure), bumps temperature + maybe triggers anneal, and
  // forwards via transport.send('lookup_step', ...).  Body copied
  // verbatim from AxonaEngine._lookupStep; `node` → `this._node`,
  // engine config via `this._engine.X`, internal method calls land on
  // peer methods (e.g. `this._addByVitality(syn)` instead of
  // `engine._addByVitality(node, syn)`).
  //
  async _lookupStep(ctx) {
    const node   = this._node;
    const engine = this._engine;
    const domain = this._domain;
    if (!node || !node.alive) {
      return this._lookupResult(ctx, false);
    }

    const { sourceId, targetKey } = ctx;
    const currentDist = node.id ^ targetKey;
    if (currentDist === 0n) {
      return this._lookupResult(ctx, true);
    }
    if (ctx.hops >= domain.MAX_HOPS) {
      return this._lookupResult(ctx, false);
    }

    const dead = node._deadPeers || new Set();

    const deadSynapses = [];
    const candidates   = [];
    for (const s of node.synaptome.values()) {
      if ((s.peerId ^ targetKey) >= currentDist) continue;
      if (dead.has(s.peerId)) { deadSynapses.push(s); s.weight = 0; continue; }
      candidates.push(s);
    }
    for (const s of node.incomingSynapses.values()) {
      if ((s.peerId ^ targetKey) >= currentDist) continue;
      if (dead.has(s.peerId)) continue;
      candidates.push(s);
    }

    if (deadSynapses.length > 0) {
      node.temperature = Math.max(node.temperature, domain.T_REHEAT);
      for (const syn of deadSynapses) {
        const repl = await this._evictAndReplace(syn);
        if (repl && (repl.peerId ^ targetKey) < currentDist) candidates.push(repl);
      }
    }

    if (candidates.length === 0) {
      let bestSyn = null, bestDist = null;
      const scan = (s) => {
        if (ctx.queried.has(s.peerId)) return;
        if (dead.has(s.peerId)) return;
        const d = s.peerId ^ targetKey;
        if (bestDist === null || d < bestDist) { bestDist = d; bestSyn = s; }
      };
      for (const s of node.synaptome.values())         scan(s);
      for (const s of node.incomingSynapses.values()) scan(s);
      if (!bestSyn) return this._lookupResult(ctx, false);
      candidates.push(bestSyn);
    }

    let nextSyn;
    const direct = node.synaptome.get(targetKey)
                ?? node.incomingSynapses.get(targetKey);
    if (direct && !dead.has(targetKey)) nextSyn = direct;

    if (!nextSyn && node.id === sourceId
        && Math.random() < domain.EPSILON) {
      nextSyn = candidates[Math.floor(Math.random() * candidates.length)];
    }

    if (!nextSyn) {
      nextSyn = await this._bestByTwoHopAP(candidates, targetKey, currentDist);
    }

    const nextId = nextSyn.peerId;

    if (node.incomingSynapses.has(nextId) && !node.synaptome.has(nextId)) {
      const inc = node.incomingSynapses.get(nextId);
      inc.useCount = (inc.useCount ?? 0) + 1;
      if (inc.useCount >= domain.PROMOTE_THRESHOLD) {
        const syn = new Synapse({
          peerId: nextId, latencyMs: inc.latency, stratum: inc.stratum,
        });
        syn.weight   = 0.5;
        syn.inertia  = domain.simEpoch;
        syn._addedBy = 'promote';
        if (await this._addByVitality(syn)) {
          node.incomingSynapses.delete(nextId);
        }
      }
    }

    ctx.queried.add(nextId);
    ctx.path.push(nextId);
    ctx.trace.push({ fromId: node.id, synapse: nextSyn });
    ctx.totalTimeMs += nextSyn.latency;
    ctx.hops += 1;

    if (node.id !== targetKey && !node.synaptome.has(targetKey)) {
      const stratum = clz264(node.id ^ targetKey);
      const syn = new Synapse({
        peerId: targetKey, latencyMs: 0, stratum,
      });
      syn.weight   = 0.5;
      syn.inertia  = domain.simEpoch;
      syn._addedBy = 'hopCache';
      const added = await this._addByVitality(syn);
      if (added && domain.EN_LATERAL_SPREAD) {
        const nodeRegion = node.id >> BigInt(64 - domain.GEO_REGION_BITS);
        const regional   = [];
        for (const s of node.synaptome.values()) {
          if (s.peerId === targetKey) continue;
          if ((s.peerId >> BigInt(64 - domain.GEO_REGION_BITS)) === nodeRegion) {
            regional.push(s);
          }
        }
        regional.sort((a, b) => b.weight - a.weight);
        for (let i = 0; i < Math.min(domain.LATERAL_K, regional.length); i++) {
          node.transport.notify(regional[i].peerId, 'lateral_spread',
                                { target: targetKey, depth: 1 })
            .catch(err => console.error('AxonaPeer: lateral_spread notify failed:', err));
        }
      }
    }

    if (node.id !== sourceId) this._recordTransit(sourceId, nextId);

    node.temperature = Math.max(domain.T_MIN, node.temperature * domain.ANNEAL_COOLING);
    if (Math.random() < node.temperature * domain.ANNEAL_RATE_SCALE) {
      this._tryAnneal().catch(err =>
        console.error(`AxonaPeer: anneal failed at ${node.id.toString(16)}:`, err));
    }

    try {
      const downstream = await node.transport.send(nextId, 'lookup_step', {
        sourceId, targetKey,
        hops:        ctx.hops,
        path:        ctx.path,
        trace:       ctx.trace,
        queried:     ctx.queried,
        totalTimeMs: ctx.totalTimeMs,
      });
      return downstream;
    } catch {
      return this._lookupResult(ctx, false);
    }
  }

  _lookupResult(ctx, found) {
    return {
      found,
      path:        ctx.path,
      trace:       ctx.trace,
      totalTimeMs: ctx.totalTimeMs,
      hops:        ctx.hops,
    };
  }
}

// ─── Module-local helpers (mirror AxonaEngine) ────────────────

function topicToBigInt(v) {
  if (typeof v === 'bigint') return v;
  return BigInt('0x' + v);
}

function nodeIdToHex(id) {
  if (typeof id === 'string') return id;
  return id.toString(16).padStart(16, '0');
}
