// =====================================================================
// Shared type definitions for the N-DHT contract surface.
//
// JSDoc typedefs — no runtime code. Files that import from this module
// receive nothing executable. The typedefs provide IDE autocomplete and
// contract documentation; runtime conformance is enforced by the
// abstract base classes in Transport.js, DHT.js, and BootstrapService.js.
//
// All identifiers are 264-bit BigInts (the Axona keyspace):
//   [8-bit S2 prefix] || [256-bit hash]
// Encoded as 66-char lowercase hex at API boundaries.  Internal
// XOR / distance math uses BigInt.
// =====================================================================

/**
 * 264-bit node identifier. The high 8 bits encode the node's S2
 * geographic cell; the remaining 256 bits are the SHA-256 of the
 * node's Ed25519 public key. See whitepaper Chapter 6 for
 * construction details.
 *
 * Wire / API representation: 66-char lowercase hex string.
 * Internal math: BigInt.
 *
 * @typedef {bigint} NodeId
 */

/**
 * 264-bit publish/subscribe topic identifier. Anchored to the
 * publisher's region:
 *
 *     TopicId = [publisher.nodeId top 8 bits (S2 prefix)]
 *            || [SHA-256(publisher.nodeId || ':' || topicName)]
 *
 * Two publishers with the same topicName produce different TopicIds;
 * topic names are scoped to publishers.
 *
 * @typedef {bigint} TopicId
 */

// ─── DHT operation result types ─────────────────────────────────────

/**
 * Outcome of a `DHT.lookup()` call.
 *
 * @typedef {object} LookupResult
 * @property {boolean}  found   — true if the target was reached
 * @property {NodeId[]} path    — IDs traversed, including source and
 *                                terminal node (`path[0]` is the source,
 *                                `path[hops]` is the terminal)
 * @property {number}   hops    — `path.length - 1`
 * @property {number}   time    — cumulative observed latency in ms
 */

/**
 * Outcome of a `DHT.publish()` call.
 *
 * @typedef {object} PublishResult
 * @property {boolean} ok                  — message accepted by an axon root
 * @property {NodeId}  rootId              — ID of the axon root that accepted
 * @property {number}  treeDepth           — observed tree depth at publish time
 * @property {number}  estimatedDelivery   — heuristic subscriber-count estimate;
 *                                           NOT an authoritative delivery count
 */

/**
 * Handle returned by `DHT.subscribe()`. Pass back to
 * `DHT.unsubscribe()` to cancel.
 *
 * @typedef {object} Subscription
 * @property {string}  id         — opaque subscription ID
 * @property {string}  topicName  — the topic this subscription belongs to
 * @property {NodeId}  attachedTo — the axon node this subscriber is currently
 *                                  attached to (may change under churn as the
 *                                  re-subscribe path lands on a new ancestor)
 */

// ─── Observability types ────────────────────────────────────────────

/**
 * Read-only snapshot of one synapse, for telemetry and dashboards. The
 * application MUST NOT use this to drive routing — the protocol owns
 * its routing table.
 *
 * @typedef {object} SynapseSnapshot
 * @property {NodeId}  peerId
 * @property {number}  weight     — vitality weight in [0, 1]
 * @property {number}  latency    — observed RTT in ms
 * @property {number}  stratum    — XOR distance bucket index
 * @property {number}  useCount
 * @property {number}  inertia    — sim epoch of last reinforcement
 * @property {boolean} bootstrap  — added by initial network join
 * @property {string}  addedBy    — origin tag: bootstrap, hopCache,
 *                                  lateralSpread, triadic, evictReplace,
 *                                  anneal, promote
 */

/**
 * Aggregate metrics for one node. Updated continuously by the
 * protocol; safe to read at any frequency.
 *
 * @typedef {object} Metrics
 * @property {number}  simEpoch
 * @property {number}  synaptomeSize
 * @property {number}  incomingSynapsesSize
 * @property {number}  temperature
 * @property {object}  cycleStats
 * @property {number}  cycleStats.lookupsAttempted
 * @property {number}  cycleStats.lookupsSucceeded
 * @property {number}  cycleStats.avgHops
 * @property {number}  cycleStats.avgLatency
 * @property {object}  traffic
 * @property {number}  traffic.msgsSent
 * @property {number}  traffic.msgsReceived
 * @property {Object<string,number>} traffic.byType
 */

// ─── Protocol events ────────────────────────────────────────────────

/**
 * Discriminated union of events emitted via `DHT.onEvent()`.
 *
 * Event consumers should switch on `event.type` and ignore unknown
 * types (forward-compatible).
 *
 * @typedef {object} BaseEvent
 * @property {string}  type
 * @property {number}  timestamp  — `Date.now()` at emit
 *
 * @typedef {BaseEvent & {type: 'peer-joined',         peerId: NodeId, addedBy: string}} PeerJoinedEvent
 * @typedef {BaseEvent & {type: 'peer-left',           peerId: NodeId, reason: string}}  PeerLeftEvent
 * @typedef {BaseEvent & {type: 'lookup-completed',    targetKey: NodeId, hops: number, time: number, found: boolean}} LookupCompletedEvent
 * @typedef {BaseEvent & {type: 'pubsub-published',    topicName: string, treeDepth: number}} PubsubPublishedEvent
 * @typedef {BaseEvent & {type: 'pubsub-delivered',    topicName: string}} PubsubDeliveredEvent
 * @typedef {BaseEvent & {type: 'anneal-fired',        evicted: NodeId, admitted: NodeId}} AnnealFiredEvent
 * @typedef {BaseEvent & {type: 'dead-peer-detected',  peerId: NodeId}} DeadPeerDetectedEvent
 * @typedef {BaseEvent & {type: 'cycle-snapshot',      metrics: Metrics}} CycleSnapshotEvent
 *
 * @typedef {PeerJoinedEvent | PeerLeftEvent | LookupCompletedEvent
 *         | PubsubPublishedEvent | PubsubDeliveredEvent
 *         | AnnealFiredEvent | DeadPeerDetectedEvent | CycleSnapshotEvent} ProtocolEvent
 */

// ─── Bootstrap endpoint variants ────────────────────────────────────

/**
 * Discriminated union describing how to bootstrap a new node into the
 * network. Implementations of `BootstrapService` switch on `kind`.
 *
 * Simulator: in-process pointer to a peer.
 * Production: signed rendezvous URL, or QR-code-pasted sponsor info.
 *
 * @typedef {object} SimulatorEndpoint
 * @property {'simulator'} kind
 * @property {object}      sim          — opaque ref to the simulator's world
 * @property {NodeId}      sponsorId    — pre-existing in-process node to use as sponsor
 *
 * @typedef {object} RendezvousEndpoint
 * @property {'rendezvous'} kind
 * @property {string}       url          — wss:// signaling endpoint
 * @property {string}       manifestSig  — base64 signature of the rendezvous
 *                                         manifest, verified against a known key
 *
 * @typedef {object} QREndpoint
 * @property {'qr'} kind
 * @property {string} sponsorAddr        — opaque pairing string from QR scan
 *
 * @typedef {SimulatorEndpoint | RendezvousEndpoint | QREndpoint} BootstrapEndpoint
 */

// This module exports nothing at runtime; it exists to host JSDoc
// typedefs that other modules reference.
export {};
