/**
 * DHTNode – base class representing a physical node in the DHT network.
 *
 * Design intent: this class is network-agnostic. In simulation mode, the
 * SimulatedNetwork wires nodes together directly. In a real deployment, a
 * RealNetwork implementation would replace SimulatedNetwork, intercepting
 * send/receive at the same interface without touching this class.
 */
import { geoCellId }       from '../utils/s2.js';
import { clz264, ID_BITS, asId } from '../utils/hexid.js';

/**
 * Number of bits used for the S2 geographic cell stored on every node.
 * 8 bits → 256 cells worldwide, radius ≈ 800 km each – fine-grained enough
 * for the 2000 km regional radius while still being coarser than node IDs.
 */
export const GEO_CELL_BITS = 8;

export class DHTNode {
  /**
   * @param {object} opts
   * @param {number} opts.id    - Unique identifier in the DHT key space
   * @param {number} opts.lat   - Geographic latitude  (-90 … 90)
   * @param {number} opts.lng   - Geographic longitude (-180 … 180)
   */
  constructor({ id, lat, lng }) {
    // Address invariant: node ids are BigInt internally, always. Coerce at the
    // one construction gate so no downstream code has to guess the type — a hex
    // string (e.g. an identity's serialized `.id`) or a BigInt both land as BigInt.
    this.id = asId(id);
    this.lat = lat;
    this.lng = lng;
    this.alive = true;
    this.joinedAt = Date.now();

    /**
     * Per-cycle traffic counters (v0.70.00).
     *
     * The simulator until now has had no model of bandwidth — every node
     * can handle an arbitrary number of messages per unit time. The
     * hypothesis (red-team Tier 1 "bandwidth saturation" deploy blocker)
     * is that real workloads will produce a heavy-tailed distribution: a
     * small number of nodes — highway hubs, hot pub/sub roots,
     * over-trained synapse targets — will accumulate physically impossible
     * message rates while the median node sees almost nothing.
     *
     * Every message through SimulatedNetwork.send() bumps a counter on
     * both endpoints and a per-type subtotal.
     * Engine.snapshotTrafficLoad() reads and resets them after each
     * training cycle, producing a per-cycle delta we can plot, summarize,
     * and use to set load-balancing goals.
     *
     * Initialized to 0; reset to 0 after each snapshot. msgsByType is a
     * null-prototype object to avoid hash-key collisions with method
     * names.
     */
    this.msgsSent = 0;
    this.msgsReceived = 0;
    this.msgsByType = Object.create(null);

    /**
     * S2 geographic cell ID (GEO_CELL_BITS wide).
     * Computed for every node regardless of protocol so that regional lookups
     * can route by geographic cell XOR even in plain Kademlia.
     */
    this.s2Cell = geoCellId(lat, lng, GEO_CELL_BITS);

    /**
     * Physical-transport connection budget.
     *
     * The real bottleneck for WebRTC-based P2P is the number of simultaneous
     * RTCPeerConnections a browser can hold (~50–100 before degrading,
     * Chromium hard-caps at 500). That budget is bilateral and symmetric:
     * a "connection" consumes one slot on each side regardless of which
     * party initiated it.
     *
     * `connections` is the set of peer IDs with a live physical link to me.
     * It's a superset of whatever routing structure (bucket / synaptome /
     * reverse-index) references a given peer — the transport must exist
     * before any routing table can use it.
     *
     * `tryConnect(other)` is the gate: both sides must have capacity,
     * otherwise the connection is refused. Routing code treats a refused
     * connection as "this peer is unavailable" and moves on to its next
     * candidate.
     */
    this.connections    = new Set();     // bigint peerIds with a live link
    this.maxConnections = Infinity;      // set by DHT.buildRoutingTables

    /**
     * Directional sub-caps on top of the bilateral total (Interpretation B,
     * v0.67.02). `_outboundConns` is the subset of `connections` that this
     * node initiated via tryConnect(). The complement (connections −
     * _outboundConns) is the inbound subset.
     *
     * - maxOutgoing : cap on how many connections THIS node may initiate
     * - maxIncoming : cap on how many connections THIS node may accept
     *
     * Both default to Infinity so the existing single-cap behavior is
     * preserved when these are not configured. Highway nodes also get
     * Infinity/Infinity so the "server backbone" semantics carry over.
     *
     * Models a P2P deployment where browser-class nodes voluntarily limit
     * outbound dial concurrency (CPU/bandwidth cost of NAT-traversal) but
     * keep more inbound headroom (reachability / pull from popular peers).
     */
    this._outboundConns = new Set();
    this.maxOutgoing    = Infinity;
    this.maxIncoming    = Infinity;

    // Injected by the DHT implementation
    this.routingTable = null;

    // Injected by the Network layer
    this._network = null;
  }

  /**
   * Attempt to establish a bilateral physical link to `other`.
   *
   * Returns true if the link was already present or was newly established
   * (both sides now have each other in their `connections` set).
   * Returns false if either side is at capacity AND neither can free a
   * slot via the swap policy.
   *
   * Under a bilateral cap, naïve first-come-first-served refusal blocks
   * every learning/growth mechanism: new routes, dead-peer replacement,
   * incoming promotion, pub/sub child recruitment, and annealing all
   * stop the moment caps saturate. To let the graph keep growing, we
   * let a full node evict its LEAST-VALUABLE existing peer in favour
   * of the incoming candidate — if and only if the candidate genuinely
   * improves structural coverage (fills an under-represented XOR stratum).
   *
   * The default value function (stratum coverage) is protocol-agnostic.
   * Subclasses can override `_chooseVictim` to incorporate richer signals
   * (LTP weight for N-DHT, bucket recency for Kademlia, etc.).
   *
   * @param {DHTNode} other
   */
  tryConnect(other) {
    if (other === this) return false;
    if (this.connections.has(other.id)) return true;

    // Bilateral total cap (always enforced).
    const myFull    = this.connections.size  >= this.maxConnections;
    const theirFull = other.connections.size >= other.maxConnections;

    // Directional sub-caps (Interpretation B, v0.67.02). When both default
    // to Infinity these are no-ops and behavior is identical to the
    // single-cap model. When set, they enforce in/out balance per node.
    const myOutFull   = this._outboundConns.size                              >= this.maxOutgoing;
    const otherInSize = other.connections.size - other._outboundConns.size;
    const otherInFull = otherInSize                                            >= other.maxIncoming;

    // Fast path: every gate has room.
    if (!myFull && !theirFull && !myOutFull && !otherInFull) {
      this.connections.add(other.id);
      this._outboundConns.add(other.id);
      other.connections.add(this.id);
      return true;
    }

    // Directional caps don't have a swap policy — once you've initiated 45
    // connections, you can't initiate a 46th regardless of how many you'd
    // evict (the cap is on dial concurrency, not on stored peers). Refuse
    // outright. The total-cap swap policy still kicks in below.
    if (myOutFull || otherInFull) return false;

    // My total full — attempt a value-based swap to free a slot.
    if (myFull && !this._trySwapIn(other)) return false;

    // Other total full — they run the same policy from their perspective.
    if (theirFull && !other._trySwapIn(this)) return false;

    this.connections.add(other.id);
    this._outboundConns.add(other.id);
    other.connections.add(this.id);
    return true;
  }

  /**
   * Try to evict a less-valuable existing peer in favour of `candidate`.
   * Default policy: find the most over-represented XOR stratum (ignoring
   * `candidate`'s stratum); only swap if the candidate's stratum is
   * strictly less-represented. This is a conservative Pareto improvement
   * — we never swap if it would worsen stratum coverage.
   *
   * Returns true if a slot was freed (caller can then add `candidate`),
   * false if no swap improves coverage.
   *
   * @param {DHTNode} candidate
   */
  _trySwapIn(candidate) {
    const TOP_STRATUM = ID_BITS - 1;
    const candidateStratum = Math.min(TOP_STRATUM, clz264(this.id ^ candidate.id));
    const counts = new Array(ID_BITS).fill(0);
    for (const pid of this.connections) {
      const s = Math.min(TOP_STRATUM, clz264(this.id ^ pid));
      counts[s]++;
    }
    // Find the most over-represented stratum OTHER than the candidate's.
    let evictStratum = -1;
    let maxCount    = 0;
    for (let s = 0; s < ID_BITS; s++) {
      if (s === candidateStratum) continue;
      if (counts[s] > maxCount) { maxCount = counts[s]; evictStratum = s; }
    }
    // Only swap if the candidate fills a stratum with strictly fewer peers
    // than the evict-source. This is the "net structural improvement"
    // guard: it prevents thrashing (evicting and re-adding similar peers).
    if (maxCount <= counts[candidateStratum] + 1) return false;

    const victimId = this._chooseVictim(evictStratum);
    if (victimId == null) return false;

    // Free the slot on both sides. The victim's routing-table entry (if
    // any) becomes stale; future traffic through the victim will simply
    // find no live path and trigger protocol-specific cleanup (e.g.
    // dead-peer eviction in N-DHT).
    this.connections.delete(victimId);
    this._outboundConns.delete(victimId);  // safe even if victim was inbound
    const victimNode = this._network?.nodes?.get(victimId);
    if (victimNode) {
      victimNode.connections.delete(this.id);
      victimNode._outboundConns.delete(this.id);
    }
    return true;
  }

  /**
   * Choose a specific peer to evict from a given stratum.
   *
   * Base implementation: first peer found in the target stratum. Subclasses
   * can override to prefer low-LTP-weight peers (N-DHT) or
   * least-recently-used peers (Kademlia bucket tail).
   *
   * @param {number} stratum
   * @returns {bigint|null} peerId to evict, or null if no peer is in this stratum
   */
  _chooseVictim(stratum) {
    const TOP_STRATUM = ID_BITS - 1;
    for (const pid of this.connections) {
      const s = Math.min(TOP_STRATUM, clz264(this.id ^ pid));
      if (s === stratum) return pid;
    }
    return null;
  }

  /**
   * Tear down the physical link to `other` (both sides).
   * Called when a routing table drops the peer and no other structure
   * still wants it; also called automatically during dead-peer eviction.
   */
  disconnect(other) {
    if (!other) return;
    this.connections.delete(other.id);
    this._outboundConns?.delete(other.id);    // safe — only fires if outbound
    other.connections?.delete(this.id);
    other._outboundConns?.delete(this.id);    // safe — only fires if outbound
  }

  /**
   * Handle an incoming message.  Implemented by subclasses (e.g. KademliaNode).
   * @param {{ type: string, from: number, data: any }} msg
   * @returns {any}
   */
  handleMessage(msg) {
    throw new Error(`${this.constructor.name}.handleMessage() not implemented`);
  }

  /**
   * Send a message to another node via the network layer.
   * Returns { response, latency } where latency is in ms.
   * In simulation this is synchronous + instant; latency is computed, not waited.
   *
   * @param {number}  targetId
   * @param {string}  type
   * @param {any}     data
   */
  async send(targetId, type, data) {
    if (!this._network) throw new Error('Node not connected to a network');
    return this._network.send(this, targetId, type, data);
  }

  toJSON() {
    return { id: this.id, lat: this.lat, lng: this.lng, alive: this.alive };
  }
}
