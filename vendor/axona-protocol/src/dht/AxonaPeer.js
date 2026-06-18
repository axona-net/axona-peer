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
import { clz264, toHex, fromHex, isHexId, extractS2Prefix, BAD_ID_CODE } from '../utils/hexid.js';
import { resolveTopic, deriveTopicId, deriveTopicIdBig } from '../pubsub/post.js';

/**
 * Sentinel for an intentionally UNSIGNED (anonymous) publish:
 *   peer.pub(topic, msg, { signWith: ANONYMOUS })
 * Anonymity must be explicit — omitting a signer is an error, never silent
 * anonymity (design v0.3 §6). Importable from '@axona/protocol'.
 */
export const ANONYMOUS = Symbol.for('axona.publish.anonymous');
import { buildEnvelope }  from '../pubsub/envelope.js';
import { buildKill }      from '../pubsub/kill.js';
import { buildTouch }     from '../pubsub/touch.js';
import { buildUnpub }     from '../pubsub/unpub.js';
import { AxonaManager, MAX_PUBLISH_BYTES, MAX_RELIABLE_PUBLISH_BYTES } from '../pubsub/AxonaManager.js';
import { PublishError, SubscribeError, KillError, UnpubError, TouchError, PullError, MetricsError, ErrorCodes } from '../errors.js';

// ── B-3 (eclipse prevention) tunables ───────────────────────────────
// Max concurrent verification probes triggered by gossip introductions —
// bounds the connection load a flood of triadic/hop_cache/lateral_spread
// notifications can induce.
const MAX_VERIFY_PROBES = 8;
// Max peers disclosed by a single local_probe reply (D-4): enough for an
// honest annealing/dead-replace pick, too few to cheaply map the mesh.
const LOCAL_PROBE_MAX   = 8;
// Peer-relayed signaling: how long a "target is reachable over the mesh"
// verdict (from the iterative lookup, per Peer-Relayed-Signaling §8b
// finding 6) stays cached, so per-ICE-candidate signal frames within one
// negotiation don't each pay a full lookup.
const RELAY_REACH_TTL_MS = 5000;
// Memory bounds for two caches that the TTL/threshold logic alone does NOT
// bound by entry COUNT: the relay-reachability verdict cache keeps one entry
// per distinct peer-id ever checked (TTL only gates freshness, never evicts),
// and the triadic transit cache keeps one entry per (origin,next) pair that
// never reaches TRIADIC_THRESHOLD. Both leak slowly on a churny/large mesh.
const RELAY_REACH_CAP   = 1024;
const TRANSIT_CACHE_CAP = 4096;

/** Evict the oldest-inserted half of a Map once it exceeds `cap` (cheap FIFO
 *  bound for caches whose entries are individually cheap to recompute). */
function capOldest(map, cap) {
  if (map.size <= cap) return;
  const drop = cap / 2;
  let i = 0;
  for (const k of map.keys()) {
    if (i++ >= drop) break;
    map.delete(k);
  }
}

export class AxonaPeer extends DHT {
  /**
   * @param {object} opts
   * @param {import('./AxonaEngine.js').AxonaEngine} opts.engine
   *        The legacy multi-node engine (Phase 1: delegate target).
   * @param {import('./NeuronNode.js').NeuronNode} opts.node
   *        The NeuronNode this peer wraps.
   * @param {object} [opts.axonaManager]
   *        Optional explicit AxonaManager instance to use for the
   *        unified pub()/sub() API.  When omitted, pub/sub fall back
   *        to the engine's per-node AxonaManager (engine.axonaManagerFor
   *        if present, else throws).
   * @param {object} [opts.identity]
   *        Identity envelope from `deriveIdentity()` — required for
   *        signed publishes (the default).  Apps that only call
   *        `peer.pub(topic, message, { sign: false })` can omit it.
   */
  constructor({ engine = null, domain = null, node, axonaManager = null, nodeIdentity = null, transport = null, persist = null, maxPublishBytes = null }) {
    super();
    if (!node) throw new Error('AxonaPeer: node is required');
    // O-5: a publish must be RECEIVABLE by any peer on any browser across any
    // path → default the per-publish limit to the WebRTC-interop floor (16 KiB),
    // never above the absolute ingress cap. Override only for controlled,
    // known-homogeneous deployments (e.g. node-only relay fleets).
    this._maxPublishBytes = Math.min(maxPublishBytes ?? MAX_RELIABLE_PUBLISH_BYTES, MAX_PUBLISH_BYTES);

    // Phase 5d (kernel cleanup): engine is optional now.  A peer can
    // be constructed against:
    //
    //   · { engine }                  — legacy simulator path.  The
    //                                   engine doubles as the domain
    //                                   (it carries simEpoch, _emaHops,
    //                                   the config constants, etc.).
    //                                   `this._domain = engine`.
    //
    //   · { engine, domain }          — explicit dual handle.  Useful
    //                                   for tests that want to swap
    //                                   the domain without rebuilding
    //                                   the engine.
    //
    //   · { domain }                  — standalone.  No engine; the
    //                                   peer runs on Transport.sim
    //                                   (or another transport) and
    //                                   shares state with sibling
    //                                   peers via this AxonaDomain.
    //                                   Engine-specific calls
    //                                   (legacy `subscribe`/`publish`
    //                                   /`unsubscribe`, sponsor-
    //                                   bootstrap fallback) throw if
    //                                   reached in this mode.
    //
    //   · {}                          — invalid.  We need at least
    //                                   one of engine or domain to
    //                                   know where to read simEpoch
    //                                   etc. from.
    //
    if (!engine && !domain) {
      throw new Error('AxonaPeer: engine or domain is required');
    }
    this._engine = engine;
    this._domain = domain ?? engine;
    this._node   = node;
    this._axonaManager = axonaManager;
    // The NODE identity — the connection/transport keypair (its pubkey forms the
    // nodeId). Used for the handshake, routing, subscribing, and signing kill/unpub
    // of the node's OWN node-level actions. It NEVER signs a publish (key
    // separation): authorship is supplied per-publish via { signWith } (an author
    // identity), and a peer holds no default author.
    this._identity = nodeIdentity;
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
    /** @type {Map<bigint, Set<Subscription>>} topicId(BigInt) → handles */
    this._subscriptions = new Map();
    /** True once we've installed the AxonaManager-side delivery hook. */
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

    // ─── Routing-handler install flag (Phase 5e follow-up) ───────
    /** True once start() has called transport.onRequest('lookup_step') etc. */
    this._routingHandlersInstalled = false;

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

    // Phase 5e follow-up: wire the receiver-side routing handlers
    // when a transport is available.  Standalone peers (constructed
    // with just { domain, node, transport }) need this so the
    // kernel's _lookupStep recursion through transport.send finds
    // a registered handler on each forwarder.
    //
    // In the simulator path the engine wires these via
    // _registerNH1Handlers BEFORE we get here.  Re-registering is
    // safe — transport handler maps overwrite by `type`, and the
    // handler we install delegates to the same peer._lookupStep
    // method the engine's wrapper would call.  Skipped entirely
    // when no transport is attached (the legacy engine-driven path
    // in dht-sim sets node.transport from network.makeTransport
    // before constructing the peer, so the receive path is wired
    // either way).
    if (this._node?.transport && typeof this._node.transport.onRequest === 'function') {
      this._installRoutingHandlers();
    }

    // Peer-relayed signaling (bridgeless connect): if the transport exposes
    // a signal-relay hook (the web transport, when meshRelay is enabled),
    // register our routed delivery as the relay sink.  The transport's
    // sendSignal then prefers routing SDP/ICE through the mesh over the
    // bridge.  Transports without this hook (sim/node) are unaffected.
    const relayTransport = this._node?.transport;
    if (relayTransport && typeof relayTransport.setSignalRelay === 'function') {
      relayTransport.setSignalRelay((toHexId, signal) => this._relaySignalSink(toHexId, signal));
    }

    // Auto-admit any peers the transport has already bound for us
    // (e.g., the bridge from webTransport's autoHandshake).  Sub-
    // transports that don't expose boundPeers() (SimTransport,
    // dht-sim's engine-driven path) contribute nothing here — the
    // existing synaptome-seeding flow stays intact for them.
    //
    // boundPeers() is contractually BigInt[] now — the web transport
    // (composite + bridge + webrtc) speaks BigInt throughout.
    const transport = this._node?.transport;
    if (transport && typeof transport.boundPeers === 'function') {
      try {
        for (const peerBig of transport.boundPeers()) {
          this._seedSynaptomeWithSponsor(peerBig);
        }
      } catch (err) {
        if (typeof console !== 'undefined') {
          console.warn('AxonaPeer.start: auto-admit failed', err);
        }
      }
    }
    // Subscribe to ongoing bind events so peers admitted to the
    // transport AFTER start() — typically other browser peers that
    // join the mesh — are also auto-admitted to the synaptome.
    // This mirrors axona-peer/src/axona_node.js's _completeHandshake:
    // admit-to-synaptome only.  Pub/sub state (K-closest cache,
    // subscription targets) is left alone — applications subscribe
    // after the mesh has stabilised, so the K-closest computed at
    // sub time is already wide.  Subscribing before mesh
    // stabilisation is an application-level mistake (the demo waits
    // for synaptome convergence via a "ready" gate before calling
    // peer.sub), not a kernel bug to paper over here.
    //
    // onPeerBound handler receives BigInt (contract).
    if (transport && typeof transport.onPeerBound === 'function') {
      this._onPeerBoundUnsub = transport.onPeerBound((peerBig) => {
        // A (re)bound peer is alive — clear any dead-mark from a prior drop,
        // or it would stay shadow-banned: routing skips _deadPeers, and the
        // synaptome-seed below would re-add a synapse the router then ignores.
        // Symmetric counterpart to the onPeerDied eviction.
        this._node?._deadPeers?.delete(peerBig);
        try { this._seedSynaptomeWithSponsor(peerBig); }
        catch (err) {
          if (typeof console !== 'undefined') {
            console.warn('AxonaPeer.onPeerBound: admission failed', err);
          }
        }
        // The synaptome just changed — drop any cached K-closest set so
        // the next pub/sub recomputes the topic's axon set against the
        // newly-wider mesh.  Without this the relay set a peer chose
        // while the mesh was sparse stays frozen, and publishes from
        // peers that joined later route to axons this peer never
        // registered at (the cross-app delivery asymmetry).  refreshTick
        // also flushes on its 10 s cadence; this makes convergence
        // immediate on each new binding.
        this._axonaManager?.invalidateKClosestCache?.();
      });
    }

    // Symmetric counterpart to onPeerBound: when a peer's channel dies
    // (heartbeat timeout / send-fail eviction at the transport, or a bridge
    // socket close), EVICT it from the synaptome immediately.  Until this
    // existed, AxonaPeer only ever *admitted* peers; a dead synapse lingered
    // until lazy anneal cleanup, and routing (greedy lookup_step / route_msg)
    // would still pick that dead peer when it was XOR-near a target — the send
    // failed and the route died one hop short.  This is acutely fatal for
    // bridgeless peer-relay right after the central bridge drops: the dead
    // bridge synapse poisons lookup()/routeMessage toward many targets, so a
    // relayed answer/ICE never finds its way back.  Eager eviction keeps the
    // routing table honest (every synapse is a live channel) the moment a peer
    // goes; the synapse re-admits via onPeerBound if the channel re-forms.
    if (transport && typeof transport.onPeerDied === 'function') {
      this._onPeerDiedUnsub = transport.onPeerDied((peerBig) => {
        try {
          const dead = (typeof peerBig === 'bigint') ? peerBig
            : (typeof peerBig === 'string' && isHexId(peerBig)) ? fromHex(peerBig) : null;
          if (dead === null) return;
          const node = this._node;
          if (!node) return;
          node.synaptome?.delete(dead);
          node.incomingSynapses?.delete(dead);
          node.connections?.delete(dead);
          (node._deadPeers ??= new Set()).add(dead);
          this._axonaManager?.invalidateKClosestCache?.();
          this._emitLog?.('info', 'peer-died-evicted', { peer: toHex(dead) });
        } catch (err) {
          if (typeof console !== 'undefined') console.warn('AxonaPeer.onPeerDied: eviction failed', err);
        }
      });
    }

    this._started = true;
  }

  /**
   * Install transport-side handlers for routed messages the peer
   * understands.  Phase 6 wires the LEARN/FORGET handlers so a
   * group of peers driving lookups through Transport.sim converge
   * to NH-1-quality success rate without an engine.
   *
   * Wired today:
   *   · lookup_step       — the per-hop routing tick
   *   · lookahead_probe   — answers "what's your AP-best forward
   *                          synapse to target X?"
   *   · reinforce         — LTP weight bump on a used synapse
   *   · triadic_introduce — install a new synapse based on a
   *                          transit-observer's recommendation
   *   · hop_cache /
   *     lateral_spread    — install a direct hop-cache synapse
   *                          to a peer that just completed a
   *                          successful lookup through us
   *
   * Not yet wired (low-impact for cold lookup success, queued):
   *   · local_probe       — needed by _tryAnneal (anneal not run
   *                          in the kernel-driven loop yet)
   *   · route_msg         — needed by peer.routeMessage()
   *   · find_closest_set  — needed by AxonaManager K-closest queries
   *
   * Bodies are 1:1 mirrors of dht-sim/.../AxonaEngine.js's
   * _registerNH1Handlers — the engine version uses
   * `_addByVitality(node, syn)` (2 args); the peer version uses
   * `this._addByVitality(syn)` (1 arg — node is self).
   */
  _installRoutingHandlers() {
    const node      = this._node;
    const transport = node.transport;
    const domain    = this._domain;
    if (this._routingHandlersInstalled) return;

    // ── lookup_step — chain forward ─────────────────────────────────
    //
    // The wire codec serialises Set → array (see transport/wire.js);
    // the receiver is responsible for re-coercing payload.queried
    // back to a Set before passing it through _lookupStep, which
    // does ctx.queried.add(nextId) on every hop.  Without this
    // coercion the second hop in any multi-hop walk throws
    // "ctx.queried.add is not a function" and the lookup short-
    // circuits to found=false.
    transport.onRequest('lookup_step', async (_fromId, payload) => {
      const queried = payload?.queried instanceof Set
        ? payload.queried
        : Array.isArray(payload?.queried)
            ? new Set(payload.queried)
            : new Set();
      return await this._lookupStep({
        sourceId:    payload.sourceId,
        targetKey:   payload.targetKey,
        hops:        payload.hops,
        path:        payload.path,
        trace:       payload.trace,
        queried,
        totalTimeMs: payload.totalTimeMs,
      });
    });

    // ── lookahead_probe — AP-best forward synapse to target ─────────
    transport.onRequest('lookahead_probe', async (_fromId, payload) => {
      const target   = payload.target;
      const fromDist = payload.fromDist;
      const fwd = [];
      for (const syn of node.synaptome.values()) {
        if ((syn.peerId ^ target) < fromDist) fwd.push(syn);
      }
      if (fwd.length === 0) {
        return { peerId: node.id, latency: 0, terminal: true };
      }
      const best = node.bestByAP(fwd, target, 0);
      return { peerId: best.peerId, latency: best.latency, terminal: false };
    });

    // ── reinforce — LTP weight bump on a used synapse ───────────────
    transport.onNotification('reinforce', (_fromId, payload) => {
      const syn = node.synaptome.get(payload.synapsePeerId);
      if (!syn) return;
      // B-3: on identity-binding transports, only reinforce a synapse whose
      // peer is currently bound (identity-verified).  Otherwise an
      // unauthenticated `reinforce` could refresh the eviction-protection
      // (inertia) of a stale / unverified entry to keep it pinned in the
      // table (eclipse persistence).  Weight itself is already clamped to
      // ≤1.0 in Synapse.reinforce, so inertia is the only lever to gate.
      if (typeof transport.boundPeers === 'function') {
        let bound = false;
        try { bound = transport.boundPeers().some(p => p === payload.synapsePeerId); } catch { /* ignore */ }
        if (!bound) return;
      }
      // INERTIA_DURATION lives on the engine in the simulator path;
      // the kernel uses simEpoch alone (Synapse.reinforce reads
      // currentEpoch + inertiaDuration to set syn.inertia).  Pass
      // a small inertia window so the synapse becomes immediately
      // eligible for vitality-based eviction protection.
      syn.reinforce(domain.simEpoch, domain.INERTIA_DURATION ?? 8);
      syn.useCount = (syn.useCount ?? 0) + 1;
    });

    // ── triadic_introduce — observer-driven candidate ──────────────
    // B-3: an introduced peer is a *candidate*, not a table entry.  On
    // identity-binding transports it is admitted only after first-party
    // verification (see _considerCandidate); a forged introduction can no
    // longer poison the synaptome.
    transport.onNotification('triadic_introduce', async (_fromId, payload) => {
      await this._considerCandidate(payload.peerId, 'triadic');
    });

    // ── hop_cache + lateral_spread — observed-path candidates ──────
    const hopCacheHandler = async (_fromId, payload) => {
      const source = (payload.depth ?? 0) === 0 ? 'hopCache' : 'lateralSpread';
      await this._considerCandidate(payload.target, source);
    };
    transport.onNotification('hop_cache',      hopCacheHandler);
    transport.onNotification('lateral_spread', hopCacheHandler);

    // ── peer-leaving — graceful-departure fast path ─────────────────
    // A peer (e.g. the bridge on a `systemctl restart`) announces that
    // it is shutting down cleanly.  Today recovery from any departure is
    // purely *reactive*: the transport close is detected, the synapse is
    // evicted, the K-closest cache is invalidated — but existing
    // subscriptions only re-anchor on the next refreshTick (≤10 s).  For
    // a super-central node like the bridge (in every synaptome, root for
    // every us-east/* topic) that 10 s window is when pub/sub visibly
    // stalls across the mesh.
    //
    // Acting on the announcement turns that into a *proactive* sub-second
    // handoff: drop the departing peer now and immediately re-anchor our
    // subscriptions/roles onto the converged set that excludes it, a beat
    // before its socket actually closes.
    //
    // Security: the subject of the eviction is `fromId` — the
    // transport-AUTHENTICATED origin of the notification (the bridge
    // transport delivers its bound nodeId; mesh delivers the bound peer
    // id).  A peer can therefore only announce *its own* departure; it
    // cannot spoof `peer-leaving` for a third party to force-evict it
    // (payload.from is advisory and deliberately ignored).  The handler
    // is also idempotent — once the subject is gone the repeat path
    // early-returns before re-anchoring, so it can't be used as a
    // refreshTick-amplification lever.  Additive + backward-compatible:
    // peers that never receive this behave exactly as before.
    transport.onNotification('peer-leaving', (fromId, _payload) => {
      try {
        let leaving =
          (typeof fromId === 'bigint')                  ? fromId :
          (typeof fromId === 'string' && isHexId(fromId)) ? fromHex(fromId) : null;
        if (leaving === null && typeof transport.nodeIdFor === 'function') {
          try { const r = transport.nodeIdFor(fromId); if (typeof r === 'bigint') leaving = r; }
          catch { /* unresolved channel → ignore */ }
        }
        if (leaving === null) return;                 // can't authenticate subject
        const node = this._node;
        if (!node?.synaptome?.has(leaving)) return;   // not (or no longer) our peer
        node.synaptome.delete(leaving);
        node.connections?.delete(leaving);
        try { node.transport?.closeConnection?.(leaving); } catch { /* dying channel */ }
        this._emitLog?.('info', 'peer-leaving', { from: toHex(leaving) });
        // Re-anchor now rather than waiting for the 10 s refreshTick.
        this._axonaManager?.invalidateKClosestCache?.();
        Promise.resolve(this._axonaManager?.refreshTick?.()).catch(() => {});
      } catch { /* best-effort resilience path */ }
    });

    // ── Phase 7 handlers ────────────────────────────────────────────

    // ── local_probe — 2-hop neighbourhood for anneal / dead-replace ─
    // Source asks "what peers do you know?" so it can pick one for
    // its own annealing exploration or as a replacement candidate
    // when a synapse goes dead.  Reply: the synaptome peerIds,
    // excluding the requestor itself (otherwise they'd see themselves
    // as a candidate — useless).
    transport.onRequest('local_probe', async (fromId, _payload) => {
      const fromBig = (typeof fromId === 'bigint')
        ? fromId
        : BigInt('0x' + fromId);
      const peerIds = [];
      for (const syn of node.synaptome.values()) {
        if (syn.peerId !== fromBig) peerIds.push(syn.peerId);
      }
      // B-3/D-4: don't hand the full synaptome to an arbitrary caller —
      // that's a cheap map of our neighbourhood for eclipse targeting.
      // Return a bounded sample, closest-to-caller (the useful subset for
      // an honest annealing / dead-peer-replacement pick).
      if (peerIds.length > LOCAL_PROBE_MAX) {
        peerIds.sort((a, b) => {
          const da = a ^ fromBig, db = b ^ fromBig;
          return da < db ? -1 : da > db ? 1 : 0;
        });
        peerIds.length = LOCAL_PROBE_MAX;
      }
      return peerIds;
    });

    // ── find_closest_set — top-K closest peers from local synaptome
    // Used by AxonaManager's findKClosest (pub/sub) and by iterative
    // discovery.  Insertion-sorted scan; cheap because synaptome is
    // bounded by MAX_SYNAPTOME.  Caller merges results across rounds.
    transport.onRequest('find_closest_set', async (_fromId, payload) => {
      const targetBig = (typeof payload.target === 'bigint')
        ? payload.target
        : BigInt('0x' + String(payload.target));
      const K = payload.K ?? domain._k;
      const top = [];
      for (const syn of node.synaptome.values()) {
        const d = syn.peerId ^ targetBig;
        if (top.length < K) {
          let i = 0;
          while (i < top.length && top[i].d < d) i++;
          top.splice(i, 0, { peerId: syn.peerId, d });
        } else if (d < top[K - 1].d) {
          let i = 0;
          while (i < top.length && top[i].d < d) i++;
          top.splice(i, 0, { peerId: syn.peerId, d });
          top.pop();
        }
      }
      return top.map(t => t.peerId);
    });

    // ── route_msg — recursive routed-message forwarder ──────────────
    // Receiver runs greedy 1-hop scan over its own synaptome (closer
    // than self?), falls back to 2-hop terminal check, dispatches the
    // local routed handler for `type` (if any).  Returns 'consumed' /
    // 'terminal' / 'exhausted', or forwards to nextHop via another
    // route_msg request and bubbles the downstream reply unchanged.
    transport.onRequest('route_msg', async (fromId, msg) => {
      const { type, payload, targetId, hops, originId } = msg;
      const targetBig = (typeof targetId === 'bigint')
        ? targetId
        : BigInt('0x' + String(targetId));

      // Greedy 1-hop forward — only over synapses we are actually connected
      // to (skip dead/unbound entries, e.g. the bridge after it drops; see
      // _greedyNextHopToward).  Without this a dead synapse that is XOR-near
      // the target is picked, the send throws, and the relay dies one hop
      // short — breaking bridgeless peer-relay right when it's needed.
      const connOk = (typeof node.transport?.isConnected === 'function')
        ? node.transport.isConnected.bind(node.transport) : null;
      const deadSet = node._deadPeers;
      let nextHopId = null;
      let bestDist  = node.id ^ targetBig;
      for (const syn of node.synaptome.values()) {
        if (deadSet && deadSet.has(syn.peerId)) continue;
        if (connOk && !connOk(syn.peerId)) continue;
        const d = syn.peerId ^ targetBig;
        if (d < bestDist) { bestDist = d; nextHopId = syn.peerId; }
      }

      let isTerminal = nextHopId === null;
      if (isTerminal) {
        const closer = await this._findCloserInTwoHops(targetBig);
        if (closer !== null && closer !== node.id) {
          nextHopId  = closer;
          isTerminal = false;
        }
      }

      const meId = node.id;
      const result = await this._deliverRouted(type, payload, {
        fromId,
        targetId: targetBig,
        hopCount: hops,
        isTerminal,
      });

      if (result === 'consumed') {
        return { consumed: true, atNode: meId, hops };
      }
      if (isTerminal) {
        return { consumed: false, atNode: meId, hops, terminal: true };
      }
      if (hops + 1 >= domain.MAX_HOPS) {
        return { consumed: false, atNode: meId, hops, exhausted: true };
      }

      // Same lazy channel-open as _lookupStep — route_msg can hop
      // through cache synapses installed mid-walk.
      if (typeof node.transport.isConnected === 'function'
          && !node.transport.isConnected(nextHopId)
          && typeof node.transport.openConnection === 'function') {
        try { await node.transport.openConnection(nextHopId); }
        catch { /* fall through */ }
      }

      try {
        // Wire payload targetId is hex (v1.5 contract).
        const downstream = await node.transport.send(nextHopId, 'route_msg', {
          type, payload, targetId: toHex(targetBig), hops: hops + 1, originId,
        });
        return downstream;
      } catch {
        return { consumed: false, atNode: meId, hops, exhausted: true };
      }
    });

    // ── mesh:signal — peer-relayed WebRTC signaling (bridgeless connect) ──
    // A routed message carrying an opaque SDP/ICE payload toward a target
    // nodeId the originator has no direct channel to.  Intermediaries
    // forward (return falsy); only the terminal node (we ARE the target)
    // consumes, handing the payload to the transport's mesh-signal ingress
    // (transport.deliverMeshSignal → MeshManager.onSignal), which drives the
    // SAME offerer/responder/ICE state machine the bridge path uses — only
    // the transport of the signaling bytes differs.  The resulting WebRTC
    // channel is still authenticated end-to-end (axona/4 + DTLS-fingerprint
    // binding), so a relay can drop/observe but never MITM.  Design:
    // axona-docs/implementation/Peer-Relayed-Signaling-v0.1.md §3.1.
    this.onRoutedMessage('mesh:signal', async (payload, meta) => {
      if (meta.targetId !== node.id) return null;       // not us — forward
      const t = node.transport;
      if (t && typeof t.deliverMeshSignal === 'function'
          && payload && typeof payload.from === 'string') {
        try { await t.deliverMeshSignal(payload.from, payload.signal); }
        catch (err) {
          this._domain?._emit?.({ type: 'mesh-signal-deliver-failed', err: err?.message });
        }
      }
      return 'consumed';
    });

    this._routingHandlersInstalled = true;
  }

  /** clz over node.id ^ targetId — picks the right width based on
   *  whether we're on the legacy 64-bit BigInt id path or the
   *  264-bit hex id path.  Bootstrap synapses created in handlers
   *  above need stratum=clz(...), and a clz64-vs-clz264 mismatch
   *  here would put new synapses in the wrong bucket. */
  _clz(xor) {
    if (xor === 0n) return 64;
    const hi = Number((xor >> 32n) & 0xFFFFFFFFn);
    if (hi !== 0) return Math.clz32(hi);
    const lo = Number(xor & 0xFFFFFFFFn);
    return 32 + Math.clz32(lo);
  }

  async stop() {
    if (!this._started) return;
    if (this._engineListenerUnsub) {
      this._engineListenerUnsub();
      this._engineListenerUnsub = null;
    }
    if (this._onPeerBoundUnsub) {
      this._onPeerBoundUnsub();
      this._onPeerBoundUnsub = null;
    }
    if (this._onPeerDiedUnsub) {
      this._onPeerDiedUnsub();
      this._onPeerDiedUnsub = null;
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
    // join(sponsor) takes hex (user-facing API); _seedSynaptomeWithSponsor
    // is BigInt-only (kernel-internal).
    this._seedSynaptomeWithSponsor(fromHex(sponsor));
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

    // (1) notify peers (fire-and-forget, bounded by drain window).
    // Resolve the transport the same way the routing path does: prefer
    // the constructor-supplied transport, else node.transport.  Hosts
    // like the bridge wire their transport onto node.transport (not the
    // constructor opt), so without this fallback leave() would silently
    // skip the announcement and the graceful-departure handoff would
    // never fire on a bridge restart.
    const announceVia = this._transport ?? this._node?.transport;
    if (notify && announceVia && typeof announceVia.notify === 'function') {
      // peers() returns hex (display); convert to BigInt for the
      // transport contract.  The wire `from` field stays hex.
      const peers = this.peers();
      for (const peerHex of peers) {
        // Use the transport's notify directly (not peer.notify) so we
        // don't tunnel through 'axona:direct'; this is a transport-
        // level signal, not an application message.
        try {
          await announceVia.notify(fromHex(peerHex), 'peer-leaving', { from: selfId });
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
  // Axon-role state is owned by AxonaManager and persisted at that
  // layer (deferred to AxonaManager P4-followup).

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
   * @param {object} [opts.axonaManager]
   * @param {object} [opts.transport]
   * @returns {Promise<AxonaPeer>}
   */
  static async fromSnapshot(state, { engine, node, axonaManager, transport } = {}) {
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
      axonaManager,
      nodeIdentity: identity,
      transport,
    });
    peer.pendingSubscriptions = Array.isArray(state.subscriptions)
      ? state.subscriptions.map(s => ({ ...s }))
      : [];
    return peer;
  }

  _seedSynaptomeWithSponsor(sponsor) {
    if (typeof sponsor !== 'bigint') {
      throw new TypeError(
        `AxonaPeer._seedSynaptomeWithSponsor: sponsor must be bigint, got ${typeof sponsor}`,
      );
    }
    const syn = this._node?.synaptome;
    if (!syn) return;
    if (syn.has?.(sponsor)) return;
    // Belt-and-braces: dedupe against any pre-existing hex-string key
    // from an older session before we standardise on BigInt.
    const sponsorHex = toHex(sponsor);
    if (syn.has?.(sponsorHex)) return;

    // Engine-managed path: if the engine exposes addSynapse, use it
    // so its bookkeeping (stratum, decay, anneal pool) stays consistent.
    const engine = this._engine;
    if (engine && typeof engine.addSynapse === 'function') {
      try { engine.addSynapse(this._node, sponsor, { addedBy: 'bootstrap' }); return; }
      catch { /* fall through to direct insert */ }
    }

    // Direct insert: real Synapse instance with the BigInt peerId.
    // Stratum = number of leading zero bits in (self ^ peer), matching
    // axona-peer/src/axona_node.js's _completeHandshake.
    const selfId = this._node.id;
    const stratum = (typeof selfId === 'bigint')
      ? this._clz(selfId ^ sponsor)
      : 0;
    syn.set(sponsor, new Synapse({
      peerId:    sponsor,
      latencyMs: 50,
      stratum,
    }));
    const inserted = syn.get(sponsor);
    if (inserted) {
      inserted.weight   = 0.5;
      inserted.inertia  = 0;
      inserted._addedBy = 'bootstrap';
    }
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
    // Phase 1: subscribe through the engine-owned AxonaManager for
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
  // Replaces the legacy AxonaManager.pubsubPublish(bigintTopicKey, json)
  // and AxonaManager.pubsubSubscribe(bigintTopicKey) entrypoints with a
  // string-topic API:
  //
  //   const msgId = await peer.pub(topic, message);
  //   const sub   = await peer.sub(topic, envelope => …, { since });
  //   await sub.stop();
  //
  // topic is a string at the API boundary; we hash it via
  // deriveTopicId(peer.nodeIdHex, topic) → 66-char hex topic ID, which
  // is what flows through AxonaManager.  Apps don't see the topic ID
  // unless they introspect it on the subscription handle.
  //
  // The envelope shape (delivered to subscribers) is:
  //   { msgId, ts, topic, message, publisher }
  // A2 (#24) extends this with signature + signerPubkey once signing
  // is wired through pub().

  /**
   * Next per-publisher monotonic sequence number (finding C-2).
   *
   * Seeded from the wall clock and never decreasing, so the publisher's
   * stream stays monotonic ACROSS process restarts without persisting a
   * counter: after a restart, `Date.now()` is already past every seq this
   * identity emitted before (assuming the clock didn't move backwards), so
   * root axons' per-publisher high-water marks still advance.  The
   * `+1`/`Math.max` guarantees strict monotonicity even for multiple
   * publishes within the same millisecond.
   *
   * @returns {number}
   */
  _nextPubSeq() {
    const next = Math.max((this._pubSeq || 0) + 1, Date.now());
    this._pubSeq = next;
    return next;
  }

  /**
   * Publish a message on `topic`.  Resolves with the content-derived
   * msgId once the publish has been handed to the K-closest replica
   * set (today's AxonaManager semantics).
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
  /**
   * Resolve a topic descriptor { region?, owner?, name, write? } → the canonical
   * resolved descriptor + topic id (hex + BigInt). The single entrypoint every
   * pub/sub/pull/kill/unpub/host/unhost uses, so they all address identically and
   * a root can recompute the same id from the signed descriptor. @internal
   */
  async _resolveTopicOrThrow(topic, op) {
    if (!topic || typeof topic !== 'object' || typeof topic.name !== 'string' || topic.name.length === 0) {
      throw new PublishError(ErrorCodes.PUBLISH_INVALID_TOPIC,
        `peer.${op}: topic must be an object { name, region?, owner?, write? }`,
        { context: { topic } });
    }
    // When the app omits the region, default it to THIS peer's own node region
    // (the top byte of its node/transport ID) — a real, routable cell. Never
    // derived from the author. selfRegion is ignored when topic.region is given.
    const selfRegion = (this._node && typeof this._node.id === 'bigint')
      ? extractS2Prefix(this._node.id) : null;
    let r;
    try {
      r = await resolveTopic(topic, selfRegion);
    } catch (cause) {
      const code = /region is required/.test(cause.message)
        ? ErrorCodes.TOPIC_REGION_REQUIRED : ErrorCodes.PUBLISH_INVALID_TOPIC;
      throw new PublishError(code, `peer.${op}: ${cause.message}`, { cause, context: { topic } });
    }
    r.topicIdBig = BigInt('0x' + r.topicId);
    return r;
  }

  async pub(topic, message, opts = {}) {
    const desc = await this._resolveTopicOrThrow(topic, 'pub');
    const am   = this._requireAxonaManager('pub');

    // Signer (design v0.3 §5/§6): opts.signWith is an AUTHOR identity, or the
    // ANONYMOUS sentinel for a deliberately unsigned publish. There is NO default
    // author and NO fallback to the node key — omitting a signer is an error, never
    // silent anonymity. Run many personas through one peer by varying { signWith }.
    const anon   = opts.signWith === ANONYMOUS;
    const signId = anon ? null : (opts.signWith ?? null);
    if (!anon && !signId) {
      throw new PublishError(ErrorCodes.PUBLISH_NO_PUBLISH_IDENTITY,
        'peer.pub: name a signer — pass { signWith: <authorIdentity> }, or { signWith: ANONYMOUS } to ' +
        'publish unsigned. There is no default author, and the node key never signs publishes (key separation).',
        { context: { topic: desc.name } });
    }
    if (signId && (!signId.privateKey || typeof signId.pubkeyHex !== 'string')) {
      throw new PublishError(ErrorCodes.PUBLISH_SIGN_FAILED,
        'peer.pub: { signWith } must be an author identity exposing privateKey + pubkeyHex',
        { context: { topic: desc.name } });
    }
    // Owner-only topic: only the owner key may publish. Fail fast here (the root
    // enforces the same at ingress, so this just turns a silent drop into an error).
    if (desc.write === 'owner' && signId && signId.pubkeyHex.toLowerCase() !== desc.owner) {
      throw new PublishError(ErrorCodes.WRITE_POLICY_VIOLATION,
        `peer.pub: owner-only topic '${desc.name}' — only the owner key may publish ` +
        `(signer ${signId.pubkeyHex.slice(0, 12)}… ≠ owner ${desc.owner.slice(0, 12)}…)`,
        { context: { topic: desc.name } });
    }

    let envelope;
    try {
      envelope = await buildEnvelope({
        topic:    { region: desc.region, owner: desc.owner, name: desc.name, write: desc.write },
        message,
        seq:      this._nextPubSeq(),
        identity: signId,
        sign:     !anon,
      });
    } catch (cause) {
      throw new PublishError(ErrorCodes.PUBLISH_SIGN_FAILED,
        `peer.pub: building envelope failed (${cause.message})`,
        { cause, context: { topic: desc.name } });
    }

    let json;
    try { json = JSON.stringify(envelope); }
    catch (cause) {
      throw new PublishError(ErrorCodes.PUBLISH_INVALID_MESSAGE,
        `peer.pub: message is not JSON-serializable (${cause.message})`,
        { cause, context: { topic: desc.name } });
    }
    if (json.length > this._maxPublishBytes) {
      throw new PublishError(ErrorCodes.PUBLISH_PAYLOAD_TOO_LARGE,
        `peer.pub: enveloped message ${json.length}B exceeds the reliable-delivery limit ${this._maxPublishBytes}B ` +
        `(WebRTC-interoperable floor). Chunk large payloads with @axona/protocol/std/chunk (publishChunkedBytes).`,
        { context: { topic: desc.name, size: json.length, max: this._maxPublishBytes } });
    }

    // postHash = envelope.msgId makes the replay cache searchable by content hash
    // for peer.pull (A3). v0.3: no publisher anchor, no publishId — dedup is the
    // content-addressed msgId; placement is the topic id's region byte.
    am.pubsubPublish(desc.topicIdBig, json, { postHash: envelope.msgId });
    return envelope.msgId;
  }

  /**
   * Retract a previously-published message (Phase A #2) — "unsend".
   *
   * Only the ORIGINAL creator can kill a message: the kill is signed with
   * this peer's identity, and the topic's root axons accept it only if the
   * signing key matches the signer of the cached message. So you can only
   * kill messages you yourself signed. The kill is routed to the topic's
   * K-closest root axons, which drop it from their replay cache, record a
   * short-lived tombstone (so a lagging replica can't resurrect it), and
   * forward a delete marker to current subscribers — whose `sub` handlers
   * receive `{ topic, msgId, deleted: true }` so they can drop their local
   * copy.
   *
   * Best-effort, not a cryptographic unsend: a subscriber that already has
   * the plaintext can keep it; an offline subscriber may never see the
   * purge. And an anonymous (`sign:false`) message can't be killed — it has
   * no provable creator.
   *
   * @param {string} topic    the topic the message was published to
   * @param {string} msgId    the msgId returned by `peer.pub` (64-char hex)
   * @param {object} [opts]
   * @param {string|null} [opts.publisher]  topic-id mode; MUST match `pub`
   * @returns {Promise<{ ok: boolean }>}  ok:true once the kill is dispatched
   */
  async kill(topic, msgId, opts = {}) {
    if (typeof msgId !== 'string' || !/^[0-9a-f]{64}$/.test(msgId)) {
      throw new KillError(ErrorCodes.KILL_INVALID_MSGID,
        `peer.kill: msgId must be a 64-char hex string (the value peer.pub returned)`,
        { context: { topic, msgId } });
    }
    // A kill is authorized by AUTHORSHIP: the root accepts it only if its signer
    // matches the signer of the cached message. So a kill is signed by the SAME
    // author key that published the message — pass it as { signWith } (v0.3 §5).
    const author = opts.signWith;
    if (!author || !author.privateKey || typeof author.pubkeyHex !== 'string') {
      throw new KillError(ErrorCodes.KILL_SIGN_FAILED,
        'peer.kill: a kill must be signed by the author key that published the message — pass { signWith }',
        { context: { topic } });
    }
    const desc = await this._resolveTopicOrThrow(topic, 'kill');
    const am   = this._requireAxonaManager('kill');
    let kill;
    try {
      kill = await buildKill({ topicId: desc.topicId, msgId, seq: this._nextPubSeq(), identity: author });
    } catch (cause) {
      throw new KillError(ErrorCodes.KILL_SIGN_FAILED,
        `peer.kill: signing the kill failed (${cause.message})`,
        { cause, context: { topic, msgId } });
    }
    am.pubsubKill(desc.topicIdBig, kill);
    return { ok: true };
  }

  /**
   * Touch a message (Phase A #7) — a keep-alive gated by TOPIC OWNERSHIP.
   * Always signed (for freshness); routed to the topic's K-closest roots,
   * each of which (if it holds the message) resets the message's hold-time
   * expiry to `now + hold` (bounded by its absolute 48h ceiling), moves it to
   * the head of the replay queue, and makes it the last entry to be evicted.
   * Use it to keep a still-relevant message (a pinned status, a current value)
   * alive past its default hold without re-publishing.
   *
   * Authority is self-authenticating and by topic, not by message authorship:
   * on an **open** topic (public, or a synthetic regional anchor) **anyone**
   * may touch; on an **owned** topic only the **owner** may (the touch signer's
   * pubkey must hash to the owner's nodeId suffix). Pass the same `publisher`
   * you published under so the right topic id is derived.
   *
   * @param {string} topic   the topic the message was published to
   * @param {string} msgId   64-char hex (the value `pub` returned)
   * @param {object} [opts]
   * @param {string|null} [opts.publisher]  same addressing mode used for pub
   * @returns {Promise<{ ok: true }>}
   */
  async touch(topic, msgId, opts = {}) {
    if (typeof msgId !== 'string' || !/^[0-9a-f]{64}$/.test(msgId)) {
      throw new TouchError(ErrorCodes.TOUCH_INVALID_MSGID,
        `peer.touch: msgId must be a 64-char hex string (the value peer.pub returned)`,
        { context: { topic, msgId } });
    }
    // Signed for freshness; authority is by topic — anyone may touch an OPEN topic,
    // only the owner an OWNED one (verified at the root). Pass the author as { signWith }.
    const author = opts.signWith;
    if (!author || !author.privateKey || typeof author.pubkeyHex !== 'string') {
      throw new TouchError(ErrorCodes.TOUCH_SIGN_FAILED,
        'peer.touch: a touch must be signed — pass { signWith } (an author key; on an owned topic it must be the owner)',
        { context: { topic } });
    }
    const desc = await this._resolveTopicOrThrow(topic, 'touch');
    const am   = this._requireAxonaManager('touch');
    let touch;
    try {
      touch = await buildTouch({ topicId: desc.topicId, msgId, seq: this._nextPubSeq(), identity: author });
    } catch (cause) {
      throw new TouchError(ErrorCodes.TOUCH_SIGN_FAILED,
        `peer.touch: signing the touch failed (${cause.message})`,
        { cause, context: { topic, msgId } });
    }
    am.pubsubTouch(desc.topicIdBig, touch);
    return { ok: true };
  }

  /**
   * Remove a topic's message queue (Phase A #3) — owner-only.
   *
   * Only the topic OWNER (the identity whose nodeId seeds the topic id) can
   * unpub.  The topic's root axons verify ownership self-authenticatingly:
   * the signer's pubkey must bind to the owner nodeId, and that nodeId must
   * derive the topicId.  Two modes:
   *   - default            → drop the message queue (tombstone the msgIds so
   *                          a lagging replica can't resurrect them); any
   *                          topic config/ACL is kept so the owner can keep
   *                          publishing.
   *   - `{ destroy: true }`→ TOTAL removal: messages AND config/ACL AND the
   *                          hosting role state. The topicId can be
   *                          re-derived and the topic re-created later, but
   *                          it comes back with defaults, not its old state.
   *
   * Ownerless (public) topics have no owner key and cannot be unpubbed.
   *
   * @param {string} topic
   * @param {object} [opts]
   * @param {boolean} [opts.destroy=false]
   * @param {string|null} [opts.publisher]  owner selector; default = this peer
   * @returns {Promise<{ ok: boolean }>}
   */
  async unpub(topic, opts = {}) {
    const desc = await this._resolveTopicOrThrow(topic, 'unpub');
    // Only an OWNED topic can be unpublished, and only by its owner key.
    if (!desc.owner || desc.write !== 'owner') {
      throw new UnpubError(ErrorCodes.UNPUB_PUBLIC_TOPIC,
        'peer.unpub: only an owned topic can be unpublished ({ owner, write: \'owner\' }); open topics have no owner',
        { context: { topic: desc.name } });
    }
    const author = opts.signWith;
    if (!author || !author.privateKey || typeof author.pubkeyHex !== 'string') {
      throw new UnpubError(ErrorCodes.UNPUB_SIGN_FAILED,
        'peer.unpub: an unpub must be signed by the topic owner — pass { signWith } (the owner author key)',
        { context: { topic: desc.name } });
    }
    if (author.pubkeyHex.toLowerCase() !== desc.owner) {
      throw new UnpubError(ErrorCodes.UNPUB_SIGN_FAILED,
        'peer.unpub: signer is not the topic owner',
        { context: { topic: desc.name } });
    }
    const am = this._requireAxonaManager('unpub');
    let unpub;
    try {
      unpub = await buildUnpub({
        topicId:     desc.topicId,
        topicName:   desc.name,
        ownerNodeId: desc.owner,         // v0.3: the owner is the Author ID (public key)
        destroy:     opts.destroy === true,
        seq:         this._nextPubSeq(),
        identity:    author,
      });
    } catch (cause) {
      throw new UnpubError(ErrorCodes.UNPUB_SIGN_FAILED,
        `peer.unpub: signing the unpub failed (${cause.message})`,
        { cause, context: { topic: desc.name } });
    }
    am.pubsubUnpub(desc.topicIdBig, unpub);
    return { ok: true };
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
    if (typeof handler !== 'function') {
      throw new SubscribeError(ErrorCodes.SUBSCRIBE_HANDLER_MISSING,
        'peer.sub: handler must be a function', { context: { topic } });
    }
    // Structured topic { region?, owner?, name, write? } — same addressing as pub.
    // To read someone's feed/profile pass their owner Author ID; key-derived
    // placement (region omitted + owner) makes it discoverable from the Author ID.
    const desc       = await this._resolveTopicOrThrow(topic, 'sub');
    const am         = this._requireAxonaManager('sub');
    const topicIdBig = desc.topicIdBig;

    // Apply `since` mode by seeding AxonaManager's per-topic lastSeenTs
    // BEFORE the subscribe call.  AxonaManager passes lastSeenTs in the
    // subscribe envelope; the receiving axon's replay cache filters
    // strictly above it.
    this._applySince(am, topicIdBig, opts.since);

    // Register the handler and the dispatch hook before the network
    // call so deliveries that arrive between submit and resolve are
    // routed correctly.  Subscription's internal `_topicId` is BigInt
    // (kernel form); the public `sub.topicId` getter returns hex.
    const sub = new Subscription({
      peer: this, topicId: topicIdBig, topicName: desc.name, handler, opts,
    });
    if (!this._subscriptions.has(topicIdBig)) this._subscriptions.set(topicIdBig, new Set());
    this._subscriptions.get(topicIdBig).add(sub);
    this._installDeliveryHook(am);

    am.pubsubSubscribe(topicIdBig);
    this._markPersistDirty('subscriptions');
    return sub;
  }

  /**
   * Unsubscribe from `topic` by name — the counterpart to `peer.sub`.
   *
   * Convenience over `subscription.stop()`: stops EVERY local subscription
   * this peer holds for the topic (you don't need to have kept the handle),
   * and — once the last one goes — sends the network unsubscribe so the
   * topic's root axons drop this peer from their subscriber set.  That
   * routed/​direct unsubscribe is self-only by construction: a peer may only
   * remove its OWN subscriberId (the B-1 invariant enforced at ingress), so
   * `unsub` can never be used to silence another peer.
   *
   * Idempotent: unsubscribing a topic you're not subscribed to is a no-op
   * that returns `{ ok: true, removed: 0 }`.
   *
   * `opts.publisher` selects the topic-id derivation mode — it MUST match
   * what you passed to `sub` (default = this peer's own feed, `null` =
   * public topic, a hex id = someone else's feed), or the derived topicId
   * won't match your subscription.
   *
   * @param {string} topic
   * @param {object} [opts]
   * @param {string|null} [opts.publisher]
   * @returns {Promise<{ ok: boolean, removed: number }>}
   */
  async unsub(topic, opts = {}) {
    // Derive the topicId exactly as sub() does so we target the same feed.
    const desc       = await this._resolveTopicOrThrow(topic, 'unsub');
    const topicIdBig = desc.topicIdBig;

    const set = this._subscriptions.get(topicIdBig);
    if (!set || set.size === 0) return { ok: true, removed: 0 };
    // Snapshot first — sub.stop() → _unsubscribeInternal mutates the set,
    // and the final removal triggers the network-level pubsubUnsubscribe.
    const subs = [...set];
    for (const sub of subs) await sub.stop();
    return { ok: true, removed: subs.length };
  }

  /**
   * Host a topic — store and serve it for other peers WITHOUT subscribing as
   * a consumer. This is the relay/infrastructure primitive: it makes the node
   * a willing root/replica so publishes land on it and subscribers can pull
   * replays from it, but it registers NO handler and delivers nothing to a
   * local application. Decoupled from `sub()` on purpose — hosting is "I'll
   * serve this for others," subscribing is "I want to receive this."
   *
   * Two forms:
   *   • `host()`           — host this node's own keyspace neighborhood: get
   *                          recruited as a root for whatever topics land near
   *                          this node's id ("host whatever lands near me").
   *   • `host(topic, opts)` — host one specific topic. `opts.publisher` selects
   *                          the topic-id derivation exactly like `sub()`
   *                          (default = this node's feed, `null` = public
   *                          topic, hex = someone else's feed).
   *
   * Wire-compatible with every existing kernel (reuses `subscribe-k`), so it
   * needs no flag day. Idempotent.
   *
   * @param {string} [topic]
   * @param {object} [opts]
   * @param {string|null} [opts.publisher]
   * @returns {Promise<{ ok: boolean, scope: 'keyspace'|'topic', topicId?: string }>}
   */
  async host(topic, opts = {}) {
    const am = this._requireAxonaManager('host');
    if (topic === undefined) {
      am.pubsubHostKeyspace(true);
      this._markPersistDirty('hosting');
      return { ok: true, scope: 'keyspace' };
    }
    const desc = await this._resolveTopicOrThrow(topic, 'host');
    this._applySince(am, desc.topicIdBig, opts.since);
    am.pubsubHost(desc.topicIdBig);
    this._markPersistDirty('hosting');
    return { ok: true, scope: 'topic', topicId: desc.topicId };
  }

  /**
   * Stop hosting — the counterpart to `host()`. `unhost()` with no topic
   * turns off keyspace hosting; `unhost(topic)` drops one hosted topic.
   * Does NOT touch your subscriptions. Idempotent.
   *
   * @param {string} [topic]
   * @param {object} [opts]
   * @param {string|null} [opts.publisher]
   * @returns {Promise<{ ok: boolean, scope: 'keyspace'|'topic' }>}
   */
  async unhost(topic, opts = {}) {
    const am = this._requireAxonaManager('unhost');
    if (topic === undefined) {
      am.pubsubHostKeyspace(false);
      this._markPersistDirty('hosting');
      return { ok: true, scope: 'keyspace' };
    }
    const desc = await this._resolveTopicOrThrow(topic, 'unhost');
    am.pubsubUnhost(desc.topicIdBig);
    this._markPersistDirty('hosting');
    return { ok: true, scope: 'topic' };
  }

  /** @internal — called by Subscription.stop() */
  async _unsubscribeInternal(sub) {
    // sub._topicId is the BigInt key (kernel form); sub.topicId getter
    // returns hex (display form).  Use BigInt for Map lookup.
    const key = sub._topicId;
    const set = this._subscriptions.get(key);
    if (set) {
      set.delete(sub);
      if (set.size === 0) {
        this._subscriptions.delete(key);
        try {
          this._requireAxonaManager('unsubscribe').pubsubUnsubscribe(key);
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
  async pull(msgId, { topic, timeoutMs = 1000 } = {}) {
    // Phase A #6: msgId is OPTIONAL — pass null (or omit) to fetch the topic's
    // most-recent message; pass a 64-char hex msgId for a specific one.
    const wantsLatest = msgId === null || msgId === undefined;
    if (!wantsLatest && (typeof msgId !== 'string' || msgId.length !== 64)) {
      throw new PullError(ErrorCodes.PULL_INVALID_MSGID,
        `peer.pull: msgId must be a 64-char hex string, or null/omitted for the latest message`,
        { context: { msgId } });
    }
    const am = this._requireAxonaManager('pull');
    if (typeof am.requestPull !== 'function') {
      throw new PullError(ErrorCodes.PULL_AXONS_UNREACHABLE,
        'peer.pull: AxonaManager does not support requestPull',
        { context: {} });
    }
    const desc       = await this._resolveTopicOrThrow(topic, 'pull');
    const topicIdBig = desc.topicIdBig;
    const result = await am.requestPull(topicIdBig, wantsLatest ? null : msgId, { timeoutMs });
    if (!result) return null;

    // requestPull returns the parsed payload — which is the JSON we
    // wrote in pub(): the envelope itself.  Some legacy AxonaManagers
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
   * Note: today's AxonaManager enforces a publisher-only ownership
   * check on metrics requests — only the topic's publisher gets a
   * non-empty result.  Removing that check (so any peer can audit)
   * is queued as a kernel-side cleanup.
   *
   * @param {string} topic
   * @param {object} opts
   * @param {string} opts.publisher  66-char hex node ID of the topic owner
   * @param {number} [opts.timeoutMs=500]
   * @returns {Promise<{ publishes: number, current_count: number, subscribers: number, deliveries: number, pulls: number, reshares: number, relayCount: number }>}
   *   `current_count` is the number of published events currently retained
   *   (live, non-expired, non-killed) in the topic's tree — the max reported
   *   across responding root relays.  `subscribers` is the max direct-child
   *   count reported by any single responding relay — exact for an unsplit
   *   topic (single root), a lower bound once the tree has split into sub-axons.
   */
  async metrics(topic, { timeoutMs = 500 } = {}) {
    const desc = await this._resolveTopicOrThrow(topic, 'metrics');
    const am = this._requireAxonaManager('metrics');
    if (typeof am.requestMetrics !== 'function') {
      return { publishes: 0, subscribers: 0, deliveries: 0, pulls: 0, reshares: 0, relayCount: 0 };
    }
    const topicIdBig = desc.topicIdBig;
    const responses = await am.requestMetrics(topicIdBig, null, { timeoutMs });

    let deliveries = 0, pulls = 0, reshares = 0, publishes = 0;
    let subscribers = 0, current_count = 0;
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
      // current_count: live (non-expired, non-killed) messages a relay is
      // holding for this topic right now.  Each root replica holds the same
      // queue, so the max across responders is the tree's current count.
      if (typeof resp?.current_count === 'number') {
        current_count = Math.max(current_count, resp.current_count);
      }
    }
    return {
      publishes,
      current_count,
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
    // Public API: hex.  Transport contract: BigInt.  Convert at boundary.
    return t.send(fromHex(targetId), 'axona:direct', { from: this._nodeIdHex(), message });
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
    // Public API: hex.  Transport contract: BigInt.  Convert at boundary.
    return t.notify(fromHex(targetId), 'axona:direct', { from: this._nodeIdHex(), message });
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
      // The wire fromId is the transport's notion:
      //   - web transport (bound): BigInt nodeId
      //   - web transport (pre-bind): meshId string
      //   - sim transport: hex string
      // The user-facing senderId surface is always hex.  payload.from
      // is the sender's self-reported hex id — we prefer the transport's
      // since it's bound at handshake time.
      const senderId =
        (typeof fromId === 'bigint')                  ? toHex(fromId) :
        (typeof fromId === 'string' && isHexId(fromId)) ? fromId :
        (payload?.from ?? null);
      return await h(senderId, payload?.message);
    });
    t.onNotification('axona:direct', (fromId, payload) => {
      const h = this._directMessageHandler;
      if (!h) return;
      const senderId =
        (typeof fromId === 'bigint')                  ? toHex(fromId) :
        (typeof fromId === 'string' && isHexId(fromId)) ? fromId :
        (payload?.from ?? null);
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
   *     transport:        { boundCount, meshChannels, meshOpen,
   *                         meshBound, bridgeState } | null,
   *     meshDegraded:     boolean,
   *   }
   *
   * `transport` is populated only for transports that expose the web
   * observability surface (boundPeers / .mesh / .webrtc); it is null
   * for the sim/node transports.  `meshDegraded` is the routing-truth
   * invariant: data channels are OPEN but the axona/4 handshake has not
   * bound them into the synaptome — i.e. the mesh looks connected at the
   * WebRTC layer while carrying no authenticated routing.  This is the
   * exact condition the v2.4.0 demo bug hid behind a healthy-looking dot
   * grid.  A single true tick can be a normal mid-handshake transient;
   * consumers should treat a value that stays true across several polls
   * as the real signal.
   *
   * Heavy implementations (per-replay-cache byte sizes, traffic
   * counters) can be added later.  This is intentionally cheap so
   * apps can poll it on a UI tick.
   *
   * @returns {object}
   */
  health() {
    const am = this._axonaManager
            ?? (this._engine?.axonaManagerFor?.(this._node))
            ?? this._engine?._axonaManagers?.get?.(this._node.id)
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
    let hosting = null;
    if (am && typeof am.inspectHosting === 'function') {
      try { hosting = am.inspectHosting(); } catch { /* best-effort */ }
    }
    // ── transport / routing-truth observability ──────────────────────
    // Web transport exposes boundPeers() (authenticated nodeIds), .mesh
    // (DC-level peer snapshot), and .webrtc (mesh-only bind set).  Sim
    // and node transports lack these — `transport` stays null for them.
    let transport = null;
    let meshDegraded = false;
    const t = this._transport;
    if (t) {
      let boundCount = null, meshChannels = null, meshOpen = null, meshBound = null;
      try {
        if (typeof t.boundPeers === 'function') boundCount = t.boundPeers().length;
      } catch { /* best-effort */ }
      try {
        if (t.mesh && typeof t.mesh.getPeers === 'function') {
          const mp = t.mesh.getPeers();
          meshChannels = mp.length;
          meshOpen     = mp.filter(p => p && p.state === 'open').length;
        }
      } catch { /* best-effort */ }
      try {
        if (t.webrtc && typeof t.webrtc.boundPeers === 'function') {
          meshBound = t.webrtc.boundPeers().length;
        }
      } catch { /* best-effort */ }
      if (meshChannels !== null || boundCount !== null) {
        transport = {
          boundCount, meshChannels, meshOpen, meshBound,
          bridgeState: t.bridgeState ?? null,
        };
        // Open data channels with materially fewer authenticated binds
        // ⇒ routing is not flowing despite a connected-looking mesh.
        // Require a gap of ≥2 so a single in-flight handshake doesn't
        // trip the flag.
        if (meshOpen !== null && meshBound !== null) {
          meshDegraded = meshOpen >= 2 && (meshOpen - meshBound) >= 2;
        }
      }
    }

    return {
      nodeId:        this._nodeIdHex(),
      synaptomeSize: this._node?.synaptome?.size ?? 0,
      peers:         this.peers(),
      subscriptions: this._subscriptions.size,
      axonRoles,
      hosting,
      wireVersion:   this._transport?.wireVersion ?? null,
      started:       this._started === true,
      transport,
      meshDegraded,
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

  // ── AxonaManager glue ────────────────────────────────────────────

  // Forward AxonaManager's 24 security drop-path logs (bad-signature, stale,
  // oversize, posthash-mismatch, unauthorized kill/touch/unpub, …) to this
  // peer's onLog surface. Idempotent per manager instance; defensively
  // optional so an older vendored AxonaManager without setLogSink is a no-op.
  _wireManagerLog(am) {
    if (am && typeof am.setLogSink === 'function' && this._managerLogWired !== am) {
      am.setLogSink((level, msg, context) => this._emitLog(level, msg, context));
      this._managerLogWired = am;
    }
    return am;
  }

  _requireAxonaManager(callerName) {
    if (this._axonaManager) return this._wireManagerLog(this._axonaManager);
    // Fallback 1: ask the engine for this node's AxonaManager.  Different
    // engine builds expose this differently; we probe in priority
    // order and cache the result.
    const engine = this._engine;
    let am = null;
    if (typeof engine?.axonaManagerFor === 'function') {
      am = engine.axonaManagerFor(this._node);
    } else if (engine?._axonaManagers instanceof Map) {
      am = engine._axonaManagers.get(this._node.id);
    }
    // Fallback 2: build one ourselves.  Standalone consumers (the
    // browser pub/sub demo, kernel smoke tests, anyone constructing
    // an AxonaPeer with just { domain, node, identity, transport })
    // shouldn't have to hand-wire a dht adapter — peer.pub / peer.sub
    // should just work after peer.start().  The adapter we build here
    // mirrors browser_engine.axonFor in axona-peer (the proven
    // production wiring): reachable-only findKClosest to dodge
    // ghost-peer drops, and sendDirect with a routed __tunneled_direct__
    // fallback for K-closest axons we don't have a direct channel to.
    if (!am) {
      am = this._buildDefaultAxonaManager();
    }
    if (!am) {
      throw new PublishError(ErrorCodes.PUBLISH_INVALID_TOPIC,
        `peer.${callerName}: no AxonaManager available; ` +
        'pass {axonaManager} to the AxonaPeer constructor or wire engine.axonaManagerFor()',
      );
    }
    this._axonaManager = am;
    return this._wireManagerLog(am);
  }

  /**
   * Construct an AxonaManager wired to a dht adapter that uses this
   * peer's reachable peer set (self + bound transport peers + learned
   * synaptome).  Used as the default when no explicit AxonaManager and
   * no engine.axonaManagerFor are available — typically browser apps
   * that talk to bridge.axona.net directly via webTransport.
   *
   * The two production hardenings this adapter ships with:
   *
   *   findKClosest — local-only, never probes the network.  Network
   *     probes return ghost IDs from prior tab sessions still cached
   *     in remote synaptomes; pub/sub messages routed at a ghost
   *     terminate at a live peer with no role for the topic, so
   *     deliveries silently drop.  Using only locally-known peers
   *     guarantees publisher + subscriber land on the same axon set.
   *
   *   sendDirect — directly-bound peers go through peer.sendDirect
   *     (one transport.notify hop).  Anyone else falls back to a
   *     routed `__tunneled_direct__` envelope that the receiver
   *     unwraps into its own direct-handler table.  Makes K-closest
   *     axons reachable even when the local transport only has a
   *     channel to the bridge.
   *
   * @returns {AxonaManager}
   */
  _buildDefaultAxonaManager() {
    const peer = this;
    const node = this._node;
    if (!node) return null;
    // Only auto-build when there's a transport to route over.  Smoke
    // tests that construct an AxonaPeer with a mock node (no transport,
    // no synaptome) keep getting the explicit "no AxonaManager" error
    // — they're exercising the validation surface, not the runtime.
    if (!node.transport) return null;
    const selfId = peer.getNodeId();

    const dht = {
      getSelfId:    () => peer.getNodeId(),
      findKClosest: async (targetIdBig, K = 5) => {
        // AxonaManager now passes BigInt targetId; the adapter is
        // BigInt-throughout.  No hex conversion needed.
        if (typeof targetIdBig !== 'bigint') {
          throw new TypeError(
            `default-dht.findKClosest: targetId must be bigint, got ${typeof targetIdBig}`,
          );
        }
        const dist = new Map();
        if (typeof selfId === 'bigint') {
          dist.set(selfId, selfId ^ targetIdBig);
        }
        for (const syn of node.synaptome?.values?.() ?? []) {
          const pid = syn.peerId;
          if (typeof pid === 'bigint' && !dist.has(pid)) {
            dist.set(pid, pid ^ targetIdBig);
          }
        }
        for (const syn of node.incomingSynapses?.values?.() ?? []) {
          const pid = syn.peerId;
          if (typeof pid === 'bigint' && !dist.has(pid)) {
            dist.set(pid, pid ^ targetIdBig);
          }
        }
        return [...dist.entries()]
          .sort((a, b) => a[1] < b[1] ? -1 : 1)
          .slice(0, K)
          .map(([pid]) => pid);
      },
      routeMessage: (...args) => peer.routeMessage(...args),
      sendDirect: async (peerIdBig, type, payload) => {
        // AxonaManager calls with BigInt peerId.
        if (peerIdBig === selfId) {
          const h = peer._directHandlers?.get(type);
          if (!h) return false;
          try {
            await h(payload, {
              // Routed-handler meta carries hex fromId (display surface).
              fromId: toHex(selfId),
              type,
            });
            return true;
          } catch (err) {
            if (typeof console !== 'undefined') {
              console.error('AxonaPeer default-dht self-sendDirect threw:', err);
            }
            return false;
          }
        }
        if (node.transport?.isConnected?.(peerIdBig)) {
          return peer.sendDirect(peerIdBig, type, payload);
        }
        // Tunnel via routed delivery — fire-and-forget; report
        // success so AxonaManager's child-dead detection doesn't
        // false-positive while the walk is in flight.  The
        // `targetId` wire field is hex (wire form).
        peer.routeMessage(peerIdBig, '__tunneled_direct__', {
          targetId:     toHex(peerIdBig),
          innerType:    type,
          innerPayload: payload,
        }).catch(err => {
          if (typeof console !== 'undefined') {
            console.error('AxonaPeer default-dht routed sendDirect failed:', err);
          }
        });
        return true;
      },
      onRoutedMessage: (type, h) => peer.onRoutedMessage(type, h),
      onDirectMessage: (type, h) => peer.onDirectMessage(type, h),
    };

    // Receiver end of the routed fallback.  Mirrors browser_engine.
    // meta.targetId arrives over the wire as hex; convert to BigInt
    // before comparing to selfId.
    peer.onRoutedMessage('__tunneled_direct__', async (payload, meta) => {
      const targetBig =
        (typeof meta?.targetId === 'bigint')   ? meta.targetId :
        (typeof meta?.targetId === 'string' && isHexId(meta.targetId))
                                               ? fromHex(meta.targetId) :
        null;
      if (targetBig == null) return 'forward';
      if (targetBig !== selfId) return 'forward';
      const handler = peer._directHandlers?.get(payload.innerType);
      if (!handler) return 'consumed';
      try {
        await handler(payload.innerPayload, {
          fromId: meta?.fromId,
          type:   payload.innerType,
        });
      } catch (err) {
        if (typeof console !== 'undefined') {
          console.error('AxonaPeer default-dht tunneled-direct dispatch threw:', err);
        }
      }
      return 'consumed';
    });

    // Match axona-peer's wiring: do NOT call am.start() here.
    // axona-peer constructs AxonaManager via engine.axonFor(node)
    // and never arms the 10s refreshTick interval.  Applications
    // call peer.sub after the mesh has stabilised, so the
    // initial K-closest is already wide and refresh isn't needed
    // to recover from a stale boot-time target set.
    return new AxonaManager({ dht });
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
      // Kill / retraction (Phase A #2): a delete marker is delivered on the
      // same handler so apps can drop their local copy.  It carries no
      // message/ts — deliver it as-is, { msgId, topic, deleted: true }, and
      // skip the normal envelope shape check.
      if (envelope && typeof envelope === 'object' && envelope.deleted === true &&
          typeof envelope.msgId === 'string') {
        for (const sub of set) sub._deliver({ msgId: envelope.msgId, topic: envelope.topic ?? null, deleted: true });
        return;
      }
      // Defence: enforce envelope shape so apps always see consistent
      // fields even if a malformed peer sends garbage.
      // v0.3: the envelope's topic is the signed descriptor object
      // { region, owner, name, write } — not a bare string. Require the
      // descriptor's name to be a string so apps see a consistent shape.
      if (!envelope || typeof envelope !== 'object' ||
          typeof envelope.msgId !== 'string' ||
          typeof envelope.ts !== 'number' ||
          typeof envelope.topic !== 'object' || envelope.topic === null ||
          typeof envelope.topic.name !== 'string' ||
          !('message' in envelope)) {
        throw new Error('malformed envelope');
      }
    } catch {
      // Fall back to a synthetic envelope carrying the raw json as
      // message and the AxonaManager's publishId as msgId — at least
      // the handler still fires with something it can inspect.
      envelope = {
        msgId:    publishId,
        ts:       publishTs,
        topic:    null,
        message:  json,
      };
    }
    // Kernel keeps loopback semantics — a publisher's own publishes
    // do bounce back through the K-closest tree and deliver to its
    // own subscriptions.  Tests rely on this for single-peer e2e
    // verification, and applications that want to hide self-
    // publishes in the UI can filter on envelope.signerPubkey ===
    // identity.pubkeyHex in their own handler.
    for (const sub of set) sub._deliver(envelope);
  }

  _applySince(am, topicId, since) {
    // The AxonaManager tracks lastSeenTs per topic in _lastSeenTsByTopic.
    // The subscribe call reads this and includes it in the outbound
    // subscribe envelope; the axon's replay filter applies it strictly.
    // We seed it here based on the `since` mode.
    if (!am._lastSeenTsByTopic) return;     // unknown AxonaManager build
    if (since === undefined) {
      // Live tail: only future messages.  Seed with a sentinel just
      // below the current time so cached messages are filtered out.
      am._lastSeenTsByTopic.set(topicId, Date.now());
      return;
    }
    if (since === 'all') {
      // Full replay: forget ALL retained per-topic consumption state, not just
      // the ts floor. Zeroing lastSeenTs alone is silently overridden by a
      // retained `have` digest (roots then replay nothing) and by the
      // _appDelivered dedup (replayed messages dropped before the handler) —
      // the "re-subscribed topic never re-delivers" / "missed alert until
      // reload" bug. pubsubResetTopicConsumption clears have + ts + this
      // topic's app-dedup together.
      if (typeof am.pubsubResetTopicConsumption === 'function') {
        am.pubsubResetTopicConsumption(topicId);
      } else {
        am._lastSeenTsByTopic.set(topicId, 0);     // older kernel: best-effort
      }
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
    // Only forward to a synapse we are ACTUALLY connected to.  A dead synapse
    // (e.g. the bridge after it dies — peers keep the synapse until anneal
    // cleans it) is XOR-near many targets and would be picked as the greedy
    // best, then transport.send() throws and the single-path forward gives up
    // one hop short.  Skipping unconnected synapses lets routing pick the
    // next-best LIVE hop and route around the dead node — essential for
    // bridgeless peer-relay right after the central bridge drops.
    const t = this._node.transport;
    const connOk = (typeof t?.isConnected === 'function') ? t.isConnected.bind(t) : null;
    const dead   = this._node._deadPeers;
    let bestPeerId = null;
    let bestDist   = this._node.id ^ target;
    for (const syn of this._node.synaptome.values()) {
      if (dead && dead.has(syn.peerId)) continue;
      if (connOk && !connOk(syn.peerId)) continue;
      const d = syn.peerId ^ target;
      if (d < bestDist) { bestDist = d; bestPeerId = syn.peerId; }
    }
    return bestPeerId;
  }

  /**
   * Bounded 2-hop "anyone closer than me?" check.  Parallel
   * `lookahead_probe` RPCs to each first-hop synapse; aggregates the
   * 2-hop responses + incomingSynapses-as-reverse-routing.
   *
   * Returns the **first-hop synapse** (a peer we are DIRECTLY connected to)
   * that leads to the closest 2-hop node strictly closer than self — i.e. the
   * NEXT HOP to forward to, NOT the 2-hop destination.  This is the routed-
   * message forwarder's fallback when greedy finds no 1-hop progress; the
   * caller does `transport.send(<return>, 'route_msg', …)`, so it MUST be an
   * adjacent peer.  Returning the 2-hop node here (the old behaviour) made the
   * forwarder send route_msg to a peer it has no channel to → the send threw
   * and routing died one hop short — breaking peer-relayed signaling whenever
   * greedy fell through to the 2-hop path.  Each candidate first hop is one
   * that just ANSWERED a probe, so the channel to it is proven live.
   * Returns null if this peer is a true 2-hop terminal.
   */
  async _findCloserInTwoHops(targetId) {
    const node = this._node;
    const target = (typeof targetId === 'bigint')
      ? targetId
      : BigInt('0x' + targetId);
    const myDist = node.id ^ target;
    let bestPeerId = null;        // the FIRST-HOP (adjacent) peer to forward to
    let bestDist   = myDist;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length > 0) {
      const settled = await Promise.allSettled(
        probeTargets.map(peerId =>
          node.transport.send(peerId, 'lookahead_probe', { target, fromDist: myDist })
        )
      );
      // settled[i] corresponds to probeTargets[i] (Promise.allSettled preserves
      // order).  r.value.peerId is the 2-hop node that first hop would forward
      // to; we score by ITS distance but forward to the FIRST HOP (probeTargets[i]).
      for (let i = 0; i < settled.length; i++) {
        const r = settled[i];
        if (r.status !== 'fulfilled' || !r.value || r.value.terminal) continue;
        const d = r.value.peerId ^ target;
        if (d < bestDist) {
          bestDist   = d;
          bestPeerId = probeTargets[i];   // adjacent next hop, not the 2-hop node
        }
      }
    }
    // incomingSynapses are reverse channels — the peer IS directly connected,
    // so the peer id itself is a valid (adjacent) next hop.
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

  /**
   * B-3: route a gossip-introduced peer through FIRST-PARTY verification
   * before it can become a synapse.
   *
   * On identity-binding transports (web/node: boundPeers + onPeerBound +
   * openConnection) a peer named in triadic_introduce / hop_cache /
   * lateral_spread is NOT inserted from the message.  If we already hold an
   * authenticated channel to it, we admit it via the verified path; else we
   * open a connection (budgeted) and let the axona/4 handshake bind its
   * identity — `onPeerBound` then admits it.  A peer that can't prove the
   * claimed nodeId never binds and is never admitted, so forged gossip
   * cannot poison the routing table (eclipse).
   *
   * On transports without an identity-binding layer (the in-process sim /
   * benchmark engine) the prior vitality-based direct admission is preserved
   * unchanged — those environments have no identity to verify and are not a
   * security boundary.
   *
   * @param {bigint} peerId
   * @param {string} source  provenance tag ('triadic'|'hopCache'|'lateralSpread')
   */
  async _considerCandidate(peerId, source) {
    const node = this._node;
    if (!node?.synaptome || typeof peerId !== 'bigint') return;
    if (peerId === node.id || node.synaptome.has(peerId)) return;
    const t = node.transport;
    const bindingCapable = t
      && typeof t.onPeerBound   === 'function'
      && typeof t.boundPeers    === 'function'
      && typeof t.openConnection === 'function';

    if (bindingCapable) {
      // Already authenticated? admit through the verified path immediately.
      let bound = false;
      try { bound = t.boundPeers().some(p => p === peerId); } catch { /* ignore */ }
      if (bound) { this._seedSynaptomeWithSponsor(peerId); return; }
      // Budgeted probe: trigger a connection; the handshake binds identity
      // and onPeerBound admits on success. Never binds ⇒ never admitted.
      if ((this._verifyProbes ?? 0) >= MAX_VERIFY_PROBES) return;
      this._verifyProbes = (this._verifyProbes ?? 0) + 1;
      let opened = false;
      try { opened = await t.openConnection(peerId); }
      catch { /* unverifiable → not admitted */ }
      finally { this._verifyProbes = Math.max(0, (this._verifyProbes ?? 1) - 1); }
      // AUTONOMOUS BRIDGELESS CONNECT.  openConnection only succeeds for a
      // peer the transport already has a (bridge-assigned) binding for; a peer
      // discovered purely peer-to-peer (triadic_introduce / hop_cache /
      // lateral_spread) has none, so without the bridge it could never be
      // connected.  When the transport supports peer-relay (web transport with
      // meshRelay on), fall back to forming the edge by relaying the WebRTC
      // signaling THROUGH the mesh — no bridge required.  The axona/4 handshake
      // on the resulting channel binds the identity and onPeerBound admits it,
      // so a forged introduction still can't poison the table.  This is what
      // makes new-connection formation independent of the bridge in steady
      // state; connectViaRelay itself no-ops when meshRelay is disabled, when
      // we're not yet meshed (cold bootstrap still needs the rendezvous), or
      // when a channel/binding to the peer already exists.
      if (!opened && typeof t.connectViaRelay === 'function') {
        try { t.connectViaRelay(toHex(peerId)); }
        catch { /* best-effort; falls back to bridge if relay can't route */ }
      }
      return;
    }

    // Non-binding transport: preserve prior vitality-based direct admit.
    const stratum = this._clz(node.id ^ peerId);
    const syn = new Synapse({ peerId, latencyMs: 0, stratum });
    syn.weight   = 0.5;
    syn.inertia  = this._domain.simEpoch;
    syn._addedBy = source;
    await this._addByVitality(syn);
  }

  /** Admission gate.  Same logic as engine._addByVitality verbatim. */
  async _addByVitality(newSyn) {
    const node   = this._node;
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

  /** LTP reinforcement wave along a successful lookup trace.  The
   *  first trace entry's `fromId` is this peer itself (we're the
   *  lookup source); skip the self-notify — transport.notify doesn't
   *  support self-loops, and any local LTP for our own synapse is
   *  handled inline by _lookupStep without going through the wire. */
  _reinforceWave(trace) {
    const selfId = this._node.id;
    for (let i = trace.length - 1; i >= 0; i--) {
      const { fromId, synapse } = trace[i];
      if (fromId === selfId) continue;
      // LTP reinforcement is opportunistic.  When the trace hop's
      // channel isn't open (common when lookup_step used routed
      // delivery rather than a direct send to that hop) the notify
      // throws and we silently skip — the lookup itself succeeded.
      // Without silencing, at scale (25K × 5000 warmup lookups) the
      // synchronous console.error spam becomes the wall-clock
      // bottleneck.
      this._node.transport.notify(fromId, 'reinforce', { synapsePeerId: synapse.peerId })
        .catch(() => { /* opportunistic — see comment */ });
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
        .catch(() => { /* opportunistic — see _reinforceWave comment */ });
    } else {
      node.transitCache.set(key, count);
      capOldest(node.transitCache, TRANSIT_CACHE_CAP);
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
    const domain = this._domain;

    const probeTargets = [...node.synaptome.values()].map(s => s.peerId);
    if (probeTargets.length === 0) return null;

    const settled = await Promise.allSettled(
      probeTargets.map(peerId => node.transport.send(peerId, 'local_probe', null))
    );

    // Dead-peer filter (#48): _localCandidate used to return any peer
    // a probe-target advertised even if WE had just marked that peer
    // dead.  The _evictAndReplace caller would then admit the same
    // dead peer back into the synaptome via _addByVitality, undoing
    // the eviction.  Filter dead ids at assembly time.
    const dead = node._deadPeers || new Set();
    const candidates = [];
    outer:
    for (const r of settled) {
      if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
      for (const id of r.value) {
        if (id === node.id) continue;
        if (dead.has(id)) continue;
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
  // These deliver AxonaManager's pub/sub on top of NH-1's transport
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
    // Accept BigInt (canonical kernel form).  No hex conversion needed.
    const targetBig = (typeof targetId === 'bigint')
      ? targetId
      : (() => { throw new TypeError(`findKClosest: targetId must be bigint, got ${typeof targetId}`); })();

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
    if (typeof targetId !== 'bigint') {
      throw new TypeError(`peer.routeMessage: targetId must be bigint, got ${typeof targetId}`);
    }
    const originNode = this._node;
    const originId   = opts.fromId ?? nodeIdToHex(originNode.id);

    let nextHopId = this._greedyNextHopToward(targetId);
    let isTerminal = nextHopId === null;
    if (isTerminal) {
      const closer = await this._findCloserInTwoHops(targetId);
      if (closer !== null && closer !== originNode.id) {
        nextHopId  = closer;
        isTerminal = false;
      }
    }

    const result = await this._deliverRouted(type, payload, {
      fromId:   originId,
      targetId,
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
      // Wire payload `targetId` is hex (per the v1.5 contract; the
      // receiver handles either form, but hex is the canonical wire
      // shape so this also works over JSON-serialising transports).
      const downstream = await originNode.transport.send(nextHopId, 'route_msg', {
        type, payload, targetId: toHex(targetId), hops: 1, originId,
      });
      return downstream;
    } catch {
      return { consumed: false, atNode: originNode.id, hops: 0, exhausted: true };
    }
  }

  // ── Peer-relayed signaling sink (bridgeless connect) ────────────────
  //
  // Registered as the web transport's `setSignalRelay` hook on start().
  // The transport's sendSignal calls this with (toNodeIdHex, signalPayload)
  // when it wants to deliver an SDP/ICE frame; we route it through the mesh
  // as a `mesh:signal` to the target.  Synchronous "took ownership" return:
  //   true  → we will deliver via the mesh (the sink skips the bridge)
  //   false → we can't (not meshed / bad id) — the sink falls back to the
  //           bridge, the cold-bootstrap rendezvous path (design §3.3).
  _relaySignalSink(toHexId, signal) {
    if (!this._started || !this._node?.alive) return false;
    if (typeof toHexId !== 'string') return false;
    if (this._node.synaptome.size === 0) return false;   // not meshed → bridge
    let toBig;
    try { toBig = fromHex(toHexId); } catch { return false; }
    if (toBig === this._node.id) return false;
    // Fire-and-forget; the negotiation's own retry/timeout (mesh layer)
    // re-drives if a frame is lost (design §6).
    this._relayMeshSignal(toBig, toHexId, signal).catch(() => {});
    return true;
  }

  /** Deliver one signaling frame to `toBig` over the mesh as a routed
   *  `mesh:signal`.  Reachability is gated on the iterative lookup
   *  (alpha-parallel, dead-peer-aware — Peer-Relayed-Signaling §8b
   *  finding 6), cached briefly; delivery is route_msg with one retry. */
  async _relayMeshSignal(toBig, toHexId, signal) {
    if (!(await this._relayReachable(toBig))) {
      this._domain?._emit?.({ type: 'mesh-signal-unreachable', to: toHexId });
      return;
    }
    // Canonical 66-char hex (the web transport's meshId form) so the
    // responder's answer routes back via fromHex() without a width mismatch.
    const body = { from: toHex(this._node.id), signal };
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const r = await this.routeMessage(toBig, 'mesh:signal', body);
        if (r && r.consumed) return;
      } catch { /* retry */ }
    }
    this._domain?._emit?.({ type: 'mesh-signal-relay-failed', to: toHexId });
  }

  /** Is `toBig` reachable over the mesh right now?  Verdict cached for
   *  RELAY_REACH_TTL_MS so per-ICE-candidate frames don't each lookup. */
  async _relayReachable(toBig) {
    const key = toBig.toString(16);
    if (!this._relayReach) this._relayReach = new Map();
    const cached = this._relayReach.get(key);
    const now = Date.now();
    if (cached && (now - cached.ts) < RELAY_REACH_TTL_MS) return cached.ok;
    let ok = false;
    try { const lk = await this.lookup(toBig); ok = !!(lk && lk.found); }
    catch { ok = false; }
    this._relayReach.set(key, { ok, ts: Date.now() });
    capOldest(this._relayReach, RELAY_REACH_CAP);
    return ok;
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
      this._onHandlerError('routed', type, err);
      return 'forward';
    }
  }

  /**
   * Single sink for any error a pub/sub message handler throws (sync or async,
   * direct or routed). A malformed-id error (`BAD_ID_CODE` — a truncated id from
   * a peer mid-teardown reaching a field the transport-level fromId check didn't
   * cover) is an EXPECTED churn-time drop, logged at debug; anything else is a
   * genuine bug, logged loudly. This is the one place that makes "a malformed
   * frame can never crash or spam a node" true for every handler and every id
   * field — no per-handler or per-registration guard required.
   */
  _onHandlerError(kind, type, err) {
    if (err && err.code === BAD_ID_CODE) {
      this._emitLog?.('debug', 'drop-malformed-frame', { kind, type, reason: err.message });
      return;
    }
    console.error(`AxonaPeer ${kind} handler error at ${this._node?.id} for '${type}':`, err);
  }

  /**
   * Fire-and-forget direct notification to one peer.  `type` is the
   * application name; the wire type is `direct_${type}`.
   */
  async sendDirect(peerId, type, payload) {
    if (typeof peerId !== 'bigint') {
      throw new TypeError(`peer.sendDirect: peerId must be bigint, got ${typeof peerId}`);
    }
    const fromNode = this._node;
    if (!fromNode?.alive || !fromNode.transport) return false;
    try {
      const ok = await fromNode.transport.notify(peerId, `direct_${type}`, payload);
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
    // role.children keys are BigInt (kernel form).  subscriberId is
    // BigInt.  This hook is consumed by external orchestrators that
    // override AxonaManager.pickRecruitPeer; the signature mirrors the
    // AxonaManager-internal _pickExistingChildForRecruit but with the
    // additional synapse-weight scoring.
    const node = this._node;
    if (role.children.size === 0) return null;
    const selfBig   = (typeof node.id === 'bigint') ? node.id : fromHex(node.id);
    const forwarder = meta.fromId;     // BigInt (kernel form)
    const dead      = node._deadPeers || new Set();

    const synapseWeights = new Map();
    for (const syn of node.synaptome.values()) {
      if (dead.has(syn.peerId)) continue;
      synapseWeights.set(syn.peerId, {
        weight:  syn.weight,
        latency: syn.latency ?? syn.latencyMs ?? 0,
      });
    }

    let bestChildId = null;
    let bestScore = -Infinity;
    for (const childId of role.children.keys()) {
      if (childId === selfBig)   continue;
      if (childId === forwarder) continue;
      const s = synapseWeights.get(childId);
      if (!s) continue;
      const score = s.weight * 1_000_000 - s.latency;
      if (score > bestScore) { bestScore = score; bestChildId = childId; }
    }
    if (bestChildId) return bestChildId;

    let best = null;
    let bestDist = null;
    for (const childId of role.children.keys()) {
      if (childId === selfBig)   continue;
      if (childId === forwarder) continue;
      const d = childId ^ subscriberId;
      if (bestDist === null || d < bestDist) { bestDist = d; best = childId; }
    }
    return best;
  }

  /**
   * Pick an external synaptome peer (not yet a child) to become a new
   * sub-axon — XOR-closest to the new subscriber's id.  All IDs BigInt.
   */
  _pickRelayPeer(role, subscriberId, forwarderId) {
    const node = this._node;
    if (!node?.alive) return null;
    const selfBig = (typeof node.id === 'bigint') ? node.id : fromHex(node.id);
    const dead    = node._deadPeers || new Set();

    const considered = new Map();
    for (const syn of node.synaptome.values()) {
      const peerId = syn.peerId;
      if (dead.has(peerId)) continue;
      if (peerId === selfBig)       continue;
      if (peerId === forwarderId)   continue;
      if (peerId === subscriberId)  continue;
      if (role.children.has(peerId)) continue;
      if (considered.has(peerId))    continue;
      considered.set(peerId, { peerId, distToSub: peerId ^ subscriberId });
    }
    if (considered.size === 0) return null;

    let bestId = null, bestDist = null;
    for (const [id, rec] of considered) {
      if (bestDist === null || rec.distToSub < bestDist) {
        bestDist = rec.distToSub;
        bestId   = id;
      }
    }
    return bestId;
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
        // A node id is ALWAYS 66 hex chars. A present-but-malformed sender id
        // (e.g. a 3-char `fromId` from a peer tearing down mid-shutdown) is a
        // corrupt frame for every subsystem, not just pub/sub — drop it once,
        // here, rather than letting each handler re-discover it by throwing.
        // (null/undefined fromId = locally-originated ⇒ allowed.)
        if (typeof fromHex === 'string' && fromHex.length > 0 && !isHexId(fromHex)) {
          this._emitLog?.('debug', 'drop-malformed-frame', { type, reason: 'bad-fromId' });
          return;
        }
        try {
          // Handlers are frequently async: a *synchronous* throw inside one (e.g.
          // parsing a malformed id from a frame) becomes a REJECTED PROMISE that
          // this synchronous try/catch cannot see — on Node that escalates to a
          // process-killing unhandledRejection. Catch both the sync throw and the
          // async rejection (as _deliverRouted does), and treat a malformed-id
          // error as an expected drop, not a loud bug.
          const r = h(payload, { fromId: fromHex, type });
          if (r && typeof r.then === 'function') r.catch((err) => this._onHandlerError('direct', type, err));
        } catch (err) {
          this._onHandlerError('direct', type, err);
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
    // v1.1.2: prefer the transport's live RTT measurement over the
    // synapse's stamped latency.  syn.latency is set once at handshake
    // admission (often before the WebRTC ping buffer is populated) and
    // is never refreshed; on browser peers it's almost always the
    // 200-ms fallback.  Query getLatency now so `lookup().time` reflects
    // current network conditions.  Fall back to the stored value when
    // the transport reports -1 (no measurement yet) or doesn't
    // implement getLatency.
    const liveLatency = (typeof node.transport?.getLatency === 'function')
      ? node.transport.getLatency(nextId)
      : -1;
    ctx.totalTimeMs += (liveLatency > 0 ? liveLatency : nextSyn.latency);
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
            .catch(() => { /* opportunistic — see _reinforceWave comment */ });
        }
      }
    }

    if (node.id !== sourceId) this._recordTransit(sourceId, nextId);

    node.temperature = Math.max(domain.T_MIN, node.temperature * domain.ANNEAL_COOLING);
    if (Math.random() < node.temperature * domain.ANNEAL_RATE_SCALE) {
      this._tryAnneal().catch(err =>
        console.error(`AxonaPeer: anneal failed at ${node.id.toString(16)}:`, err));
    }

    // Lazy channel-open: synapses added by hop_cache / lateral_spread /
    // triadic_introduce point at peers we may not have opened a
    // channel to during bootstrap.  In a real WebRTC deployment the
    // first-use path triggers connection setup; here we do the same
    // on simTransport.  If open fails (peer gone / admission denied)
    // the subsequent send() throws and the lookup terminates with
    // found=false, same as before.
    if (typeof node.transport.isConnected === 'function'
        && !node.transport.isConnected(nextId)
        && typeof node.transport.openConnection === 'function') {
      try { await node.transport.openConnection(nextId); }
      catch { /* fall through — send() will fail and we return false */ }
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

// ─── Module-local helpers ─────────────────────────────────────
//
// Post-v1.5: nodeIds are 264-bit BigInts canonically; the public
// hex form is 66 chars.  The engine-driven sim path may still pass
// legacy short ids; nodeIdToHex pads accordingly.

function nodeIdToHex(id) {
  if (typeof id === 'string') return id;
  if (typeof id === 'bigint') {
    const hex = id.toString(16);
    // Legacy 64-bit sim ids pad to 16 chars; full 264-bit kernel
    // ids pad to 66.  The split mirrors the engine's
    // `padStart(16, '0')` for sim-compat with the 264-bit production
    // path.
    return hex.padStart(hex.length > 16 ? 66 : 16, '0');
  }
  return String(id);
}
