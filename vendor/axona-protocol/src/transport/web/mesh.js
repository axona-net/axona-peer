// =====================================================================
// Axona Peer — WebRTC mesh manager (Phase 2 / B)
//
// Maintains an RTCPeerConnection + RTCDataChannel to every other peer
// in the mesh.  Driven by signaling messages relayed through the
// bridge: `peer-list`, `peer-joined`, `peer-left`, and the opaque
// `signal` payloads that carry SDP offers / answers and ICE
// candidates.
//
// Once a DataChannel opens we ping the peer directly at 1 Hz; the
// bridge is no longer in the data path for that pair.
//
// Initiation rule (matches the bridge's protocol):
//   - Peers in our `peer-list` → we initiate (createOffer)
//   - Peers announced via `peer-joined` → we wait for their offer
//
// State machine per peer:
//
//     new ──peer-joined──► awaiting-offer ──offer──► signaling
//      │                                                 │
//      └──peer-list──► signaling ───setLocal───► signaling ───dc-open───► open
//                                                                          │
//                                          peer-left / pc-failed / pc-closed
//                                                                          ▼
//                                                                      [removed]
// =====================================================================

const PING_INTERVAL_MS = 1000;
const STALE_PONG_MS    = 3000;
// No pong for this long ⇒ the channel is dead and the peer is evicted
// (onPeerLost fires), even if dc.readyState still lies 'open'.  This is
// the heartbeat-timeout the Transport contract requires; without it a
// channel that goes silent (laptop sleep / screensaver, where Safari
// keeps readyState 'open' on a dead dc) is stuck at 'stale' forever and
// the mesh never heals.  Must be > STALE_PONG_MS so there's a visible
// stale window first.
const DEAD_PONG_MS     = 10000;
// Consecutive dc.send() throws that mean "this channel is dead now" —
// a throwing send is definitive proof the channel is gone even when
// readyState lies 'open', so we don't wait the full DEAD_PONG_MS.
const SEND_FAIL_LIMIT  = 3;
const RTT_WINDOW       = 10;
const DC_LABEL         = 'axona';
const RETRY_AFTER_MS   = 5000;   // single retry after pc-failed (B10)
// Absolute ceiling on how long a peer may stay in negotiation WITHOUT ever
// opening a data channel. A PeerConnection that fails ICE does NOT autonomously
// reach 'closed' (only an explicit close does), and the ping/stale/send-fail
// eviction timers run ONLY on already-open channels — so a never-opened peer
// stuck in 'new'/'signaling'/'failed' (e.g. a responder, which gets no retry)
// would otherwise sit in _peers forever, keeping hasPeer() true and no-op'ing
// connectViaRelay's idempotency guard permanently (it could never reconnect
// bridgeless). This watchdog tears such a peer down, freeing the slot so
// discovery can re-drive, and bounds the offerer retry loop. Generous so it is
// a safety net, not a primary mechanism: healthy channels open in well under it.
const NEGOTIATION_DEADLINE_MS = 30000;
// One reaper interval drives EVERY per-peer liveness decision (see _reapTick):
// while never-opened it enforces NEGOTIATION_DEADLINE_MS; once open it folds the
// pong-timeout (DEAD_PONG_MS), send-fail (SEND_FAIL_LIMIT) and stale-display
// transitions that used to live in three separate timers/methods. Must be ≤
// STALE_PONG_MS so the stale→open display flip is observed promptly.
const REAP_INTERVAL_MS = 500;

/**
 * Pull the DTLS certificate fingerprint out of an SDP blob.  WebRTC SDP
 * carries `a=fingerprint:<hash-alg> <HEX:HEX:…>` (session- or media-level);
 * this is the value the DTLS handshake actually authenticates the channel
 * with.  Returns a normalized `"<alg> <hexnocolons>"` string (alg + bytes,
 * lowercased, colons stripped) so it can be folded into the axona/4 CBV —
 * a bridge that terminates DTLS to MITM the mesh must present a DIFFERENT
 * cert on each leg, so the two endpoints derive divergent fingerprints and
 * the mutual signature fails.  Returns null if no fingerprint is present.
 */
function extractFingerprint(sdp) {
  if (typeof sdp !== 'string') return null;
  const m = sdp.match(/^a=fingerprint:(\S+)\s+([0-9A-Fa-f:]+)\s*$/m);
  if (!m) return null;
  return `${m[1].toLowerCase()} ${m[2].replace(/:/g, '').toLowerCase()}`;
}

// BigInt-aware JSON codec — shared by every channel so the wire
// format stays consistent across WebRTC data channels and the bridge
// WebSocket.
import { bigintReplacer, bigintReviver } from '../wire.js';

// ── ICE configuration ───────────────────────────────────────────────
//
// STUN servers let each peer discover its public-facing (post-NAT)
// IP and offer that as a candidate.  Cross-NAT pairs (e.g., a phone
// on cellular talking to a laptop on home Wi-Fi) need STUN — without
// it, peers only see each other's private host IPs and can't reach
// each other.
//
// We use Google's free public STUN servers — stable, ubiquitous for
// WebRTC dev.  For production we'd consider self-hosting to avoid
// the soft dependency.
//
// TURN (relay) entries are NOT hardcoded.  The bridge hands us an
// HMAC-signed short-lived (2h) credential in its `welcome` message;
// we cache it via `setTurnConfig()` and splice it into the iceServers
// list whenever we build a fresh RTCPeerConnection.  This means the
// shipped peer JS contains zero long-term TURN credentials — anyone
// View-Source-ing axona.net learns nothing useful about how to use
// our relay.  If the bridge ever omits TURN config (e.g. its
// TURN_AUTH_SECRET env isn't set), we gracefully degrade to
// STUN-only — direct ICE still works for most pairs.
const STUN_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302'  },
  { urls: 'stun:stun1.l.google.com:19302' },
];

/**
 * @typedef {object} PeerState
 * @property {string} peerId
 * @property {'offerer' | 'responder'} role
 * @property {'new' | 'signaling' | 'datachannel-opening' | 'open' | 'stale' | 'failed' | 'closed'} state
 * @property {RTCPeerConnection | null} pc
 * @property {RTCDataChannel    | null} dc
 * @property {number} since        — epoch ms when state row first created
 * @property {number} openedAt     — epoch ms when DC opened, 0 otherwise
 * @property {number} pings
 * @property {number} pongs
 * @property {number} lastPongAt
 * @property {number[]} rttBuffer
 * @property {RTCIceCandidateInit[]} pendingCandidates  — queued before remote desc
 * @property {number | null} pingTimer
 * @property {number | null} reaperTimer  — single liveness reaper (see _reapTick)
 * @property {number} sendFailures        — consecutive dc.send() throws
 * @property {number | null} retryTimer
 * @property {boolean} retryUsed
 * @property {string | null} localCand   — 'host'|'srflx'|'prflx'|'relay'|null
 * @property {string | null} remoteCand  — same; nominated candidate pair types
 * @property {number | null} pathPollTimer
 */

export class MeshManager {
  constructor({ sendSignal, log }) {
    this._sendSignal = sendSignal;
    this._log = log ?? (() => {});
    /** @type {Map<string, PeerState>} */
    this._peers = new Map();
    /** Absolute negotiation deadline (ms) per peerId, set on the FIRST
     *  not-yet-connected negotiation and preserved across retries, so a peer
     *  that never opens is bounded in total negotiation time — not per-attempt.
     *  @type {Map<string, number>} */
    this._negotiationDeadline = new Map();
    /** @type {Set<(peers: PeerState[]) => void>} */
    this._listeners = new Set();
    /** @type {string | null} */
    this._myId = null;
    /** @type {{urls: string[]|string, username: string, credential: string} | null} */
    this._turn = null;

    // ── v0.4.0 — Transport-contract hooks ──────────────────────────────
    //
    // These let a WebRTCTransport instance (src/transport.js) ride on
    // top of MeshManager without touching its existing UI-driving
    // state machine.  Two new event channels:
    //
    //   onMessage(cb)  — fired for every non-ping/non-pong frame that
    //                    arrives on a data channel.  The Transport's
    //                    req/res/ntf envelopes flow through here.
    //
    //   onPeerLost(cb) — fired when a peer's data channel goes from
    //                    open to closed/failed.  Used by Transport to
    //                    fire its `onPeerDied` listeners and reject
    //                    outstanding requests to that peer.
    //
    // The existing application-level ping/pong remains the UI's source
    // of liveness signal (drives indicator colors).  WebRTCTransport
    // reads `getLatency(peerId)` for AP scoring and `isConnected(peerId)`
    // for cap-admission checks; both derive from existing per-peer state.
    /** @type {Set<(peerId: string, message: any) => void>} */
    this._messageListeners = new Set();
    /** @type {Set<(peerId: string) => void>} */
    this._peerLostListeners = new Set();
    // v2.0.2 — per-frame ping/pong traffic notifications.  Without
    // this, application UIs that want a "channel is actually moving
    // bytes" indicator have to roll their own (see axona-peer's
    // standalone mesh.js).  Subscribers get callback(peerId, kind)
    // where kind is 'sent' on each ping-out and 'recv' on each
    // pong-in.  Used by the demo's dot strip to drive a per-peer
    // pulse animation that fades out within a second if the channel
    // stops moving bytes.
    /** @type {Set<(peerId: string, kind: 'sent'|'recv') => void>} */
    this._pingTrafficListeners = new Set();
  }

  // ── External lifecycle ────────────────────────────────────────────

  setMyId(id) {
    this._myId = id;
  }

  /** Cache the bridge-supplied TURN credential (or null to clear).
   *  Future PCs use this; existing PCs keep their original config. */
  setTurnConfig(turn) {
    this._turn = turn ?? null;
    this._log('turn-config', {
      hasTurn: !!turn,
      username: turn?.username ?? null,
      urlCount: Array.isArray(turn?.urls) ? turn.urls.length : (turn ? 1 : 0),
    });
  }

  /** Build a fresh RTCConfiguration from STUN + the cached TURN. */
  _iceConfig() {
    const iceServers = [...STUN_SERVERS];
    if (this._turn) iceServers.push(this._maybeEncodeTurn(this._turn));
    return { iceServers };
  }

  // TURN REST credentials under Node (#343). The bridge-minted username is
  // `<expiry>:<token>` — a colon BY DESIGN (RFC-style TURN REST). Browsers
  // pass {urls, username, credential} to the native stack intact, but
  // node-datachannel's polyfill flattens them into a URL string
  // `turn:USERNAME:CREDENTIAL@host:port`, and libdatachannel re-parses that
  // by splitting on colons — the embedded colon truncates the username to
  // the timestamp half, the HMAC (computed over the FULL username) can never
  // match, coturn 401s, and no relay candidate is ever gathered. This is why
  // every NAT↔NAT node-process pair was an island (three-way experiment
  // 2026-07-21: native-struct control ✓, raw-colon polyfill ✗, pct-encoded ✓
  // — libdatachannel DOES percent-decode userinfo). Percent-encode the
  // credentials before they reach the flattener — but ONLY under Node: a
  // browser's native stack sends the fields literally, and an encoded
  // username would break its HMAC instead.
  _maybeEncodeTurn(turn) {
    const isBrowser = typeof window !== 'undefined' && typeof window.document !== 'undefined';
    if (isBrowser || !turn || typeof turn.username !== 'string') return turn;
    return {
      ...turn,
      username: encodeURIComponent(turn.username),
      credential: typeof turn.credential === 'string' ? encodeURIComponent(turn.credential) : turn.credential,
    };
  }

  /** Subscribe to mesh-state changes.  Returns an unsubscribe fn. */
  onChange(callback) {
    this._listeners.add(callback);
    return () => this._listeners.delete(callback);
  }

  /**
   * v0.4.0 — Subscribe to incoming Transport-level frames from peers.
   * The callback fires with `(peerId, parsedMessage)` for every JSON
   * message that's NOT an internal ping/pong (those drive the UI's
   * indicator state and stay private to MeshManager).
   * Returns an unsubscribe fn.
   */
  onMessage(callback) {
    this._messageListeners.add(callback);
    return () => this._messageListeners.delete(callback);
  }

  /**
   * v0.4.0 — Subscribe to peer-death events.  Fires once when a peer's
   * data channel transitions from `open` to a teardown state
   * (peer-left, pc-failed, pc-closed, or local dispose/reset).  The
   * Transport contract's `onPeerDied` semantics ride this signal.
   * Returns an unsubscribe fn.
   */
  onPeerLost(callback) {
    this._peerLostListeners.add(callback);
    return () => this._peerLostListeners.delete(callback);
  }

  /**
   * v2.0.2 — Subscribe to per-frame ping/pong traffic on each peer
   * data channel.  Fires `callback(peerId, 'sent')` immediately after
   * each outgoing ping and `callback(peerId, 'recv')` on each incoming
   * pong.  The application-level ping loop runs at 1 Hz, so a healthy
   * peer triggers two callbacks per second.  Use this to drive a
   * "channel is moving bytes" UI indicator that decouples from the
   * state machine — a peer stuck in a half-open state stops blinking
   * within ~1 s of the underlying socket going dark.
   * Returns an unsubscribe fn.
   *
   * @param {(peerId: string, kind: 'sent'|'recv') => void} callback
   */
  onPingTraffic(callback) {
    this._pingTrafficListeners.add(callback);
    return () => this._pingTrafficListeners.delete(callback);
  }

  /** @private — internal fan-out helper for onPingTraffic. */
  _emitPingTraffic(peerId, kind) {
    if (this._pingTrafficListeners.size === 0) return;
    for (const cb of this._pingTrafficListeners) {
      try { cb(peerId, kind); }
      catch (err) {
        this._log('ping-traffic-listener-threw', {
          peerId, kind, err: err.message,
        });
      }
    }
  }

  /**
   * v0.4.0 — Send a JSON-serializable payload to a peer's data channel.
   * Synchronous; throws if the peer isn't currently open.  Used by
   * WebRTCTransport to write req / res / ntf envelopes onto the wire.
   * The existing ping/pong loop continues independently of this.
   */
  send(peerId, payload) {
    const state = this._peers.get(peerId);
    if (!state || state.dc?.readyState !== 'open') {
      throw new Error(`mesh.send: peer ${peerId} not open`);
    }
    // BigInt-aware replacer: the Axona wire protocol carries BigInt
    // node IDs through req/res/ntf bodies; native JSON.stringify
    // throws on BigInts.  Serialise as "<digits>n" suffixed strings;
    // the receiver's dc.onmessage parses with the inverse reviver.
    state.dc.send(JSON.stringify(payload, bigintReplacer));
  }

  /**
   * v0.4.0 — Whether a data channel to peer is currently open.
   * Cheap O(1) check used by Transport.isConnected.
   */
  isConnected(peerId) {
    return this._peers.get(peerId)?.dc?.readyState === 'open';
  }

  /**
   * Whether we hold ANY peer state for `peerId` — including an in-progress
   * negotiation (new / signaling / datachannel-opening), not just an open
   * channel.  connectViaRelay uses this so a repeated connect request (peer
   * discovery fires more than once) does NOT call `_initiateTo` again and
   * overwrite the in-flight RTCPeerConnection, which would restart ICE and
   * prevent the negotiation from ever completing.  A failed/timed-out
   * negotiation is torn down (`_retire` deletes the entry), so this frees up
   * for a genuine retry.
   */
  hasPeer(peerId) {
    return this._peers.has(peerId);
  }

  /**
   * Count of peers still NEGOTIATING — a channel is forming but has never
   * opened (`openedAt === 0`).  Each holds a live RTCPeerConnection, so this is
   * the in-flight connection-setup load.  Used to throttle new relay connects
   * (connectViaRelay): an attacker spraying gossip introductions with distinct
   * fabricated peerIds would otherwise drive unbounded concurrent negotiations.
   * Self-healing: the negotiation watchdog reaps a stuck never-opened entry
   * after NEGOTIATION_DEADLINE_MS, so the count falls again on its own — no
   * external completion signal needed (a never-opened teardown fires no
   * onPeerLost, so a counter maintained elsewhere could not be decremented).
   */
  pendingNegotiations() {
    let n = 0;
    for (const st of this._peers.values()) if (!(st.openedAt > 0)) n++;
    return n;
  }

  /**
   * v0.4.0 — Most recent RTT to peer in milliseconds, or -1 if not
   * known yet (no completed ping/pong round-trip).  Equivalent to the
   * Transport contract's `getLatency`.
   */
  getLatency(peerId) {
    const state = this._peers.get(peerId);
    if (!state) return -1;
    const last = state.rttBuffer.at(-1);
    return (typeof last === 'number') ? last : -1;
  }

  /** Snapshot of all known peers, suitable for rendering. */
  getPeers() {
    return [...this._peers.values()].map(p => ({
      peerId:     p.peerId,
      role:       p.role,
      state:      p.state,
      since:      p.since,
      openedAt:   p.openedAt,
      pings:      p.pings,
      pongs:      p.pongs,
      lastPongAt: p.lastPongAt,
      rttLast:    p.rttBuffer.at(-1) ?? null,
      rttAvg:     p.rttBuffer.length
                    ? p.rttBuffer.reduce((a, b) => a + b, 0) / p.rttBuffer.length
                    : null,
      // Nominated candidate-pair types — null until the DC opens and
      // we've had a chance to inspect getStats().  Either end being
      // 'relay' means this specific connection is going through TURN.
      localCand:  p.localCand,
      remoteCand: p.remoteCand,
    }));
  }

  /**
   * DTLS fingerprints for the link to `peerId`, parsed from the local
   * and remote session descriptions.  Returns `{ local, remote }`
   * (normalized `"<alg> <hex>"` strings) once both descriptions are in
   * place — which, by the time any data-channel frame can arrive, they
   * always are (the DTLS handshake that opened the channel consumed
   * them).  Returns null before then, or if a fingerprint line is
   * missing.  The mesh axona/4 handshake folds these into its CBV so the
   * signed transcript is bound to the actual DTLS channel (finding A-1).
   */
  fingerprintsFor(peerId) {
    const st = this._peers.get(peerId);
    if (!st || !st.pc) return null;
    const local  = extractFingerprint(st.pc.localDescription?.sdp);
    const remote = extractFingerprint(st.pc.remoteDescription?.sdp);
    if (!local || !remote) return null;
    return { local, remote };
  }

  /** Disconnect from everyone and stop all timers. */
  dispose() {
    for (const id of [...this._peers.keys()]) {
      this._retire(id, 'dispose');
    }
    this._negotiationDeadline.clear();
    this._listeners.clear();
  }

  /**
   * Tear down every WebRTC peer but keep listeners + myId intact.
   * Used by the page-resume handler: when the device wakes from sleep
   * or the tab returns from background, every PC and DataChannel is
   * likely zombie — the remote side has long since seen us go away.
   * Easier to nuke and let the bridge's peer-list rebuild than to try
   * to detect-and-renegotiate per peer.
   */
  reset() {
    for (const id of [...this._peers.keys()]) {
      this._retire(id, 'reset');
    }
    this._negotiationDeadline.clear();
    this._notify();
  }

  /**
   * Public: tear down a single channel by meshId.  Used to close a
   * REDUNDANT channel to a peer we're already connected to under a
   * different meshId (duplicate-identity dedup after glare / reconnect
   * churn — see WebRTCTransport.bindPeer).  Fires the normal onPeerLost
   * path for that meshId and re-renders.  No-op if the meshId is unknown.
   *
   * @param {string} meshId
   * @param {string} [reason]
   */
  disconnect(meshId, reason = 'disconnect') {
    if (!this._peers.has(meshId)) return;
    this._retire(meshId, reason);
    this._notify();
  }

  _notify() {
    const snap = this.getPeers();
    for (const cb of this._listeners) {
      try { cb(snap); } catch (err) { console.error('mesh listener threw', err); }
    }
  }

  // ── Inbound events from the bridge layer ──────────────────────────

  onPeerList(peerIds) {
    for (const id of peerIds) {
      if (id === this._myId)   continue;
      if (this._peers.has(id)) continue;
      this._initiateTo(id);
    }
    this._notify();
  }

  onPeerJoined(peerId) {
    if (peerId === this._myId)   return;
    if (this._peers.has(peerId)) return;
    this._acceptFrom(peerId);
    this._notify();
  }

  onPeerLeft(peerId) {
    // A node drops a peer for exactly two reasons in this architecture:
    // the peer authentically announces its OWN departure (the `peer-leaving`
    // fast path — subject is the transport-authenticated origin, so a peer
    // can only retire itself, never a third party), or the direct channel's
    // own vitality reaches zero (the ping/pong reaper + pc-state machine that
    // this transport runs per channel). A `peer-left` from the bridge is
    // NEITHER: it is a third party (the signaling bridge) asserting that
    // SOMEONE ELSE departed — the same third-party force-eviction the
    // `peer-leaving` handler deliberately refuses. It only tells us that peer
    // left the BRIDGE (a bootstrap-nursery graduation 4200, or a transient
    // peer↔bridge blip), which says nothing about our first-party channel to
    // it. So if that channel is live (open), we keep it and let its own
    // vitality govern teardown — a peer that truly vanished is still reaped
    // within seconds by the reaper/ICE machinery, with no bridge involved.
    //
    // Without this, graduation was self-defeating: a graduate kept its own
    // mesh, but every witness took the bridge's word for it, slammed its
    // healthy channel shut, and stranded the graduate (boundPeers → 0),
    // forcing an immediate re-dial — the open→graduate→peers=0→open churn. (#374)
    const st = this._peers.get(peerId);
    if (st && st.state === 'open') {
      this._log('peer-left-ignored-live', { peerId, state: st.state });
      return;
    }
    this._retire(peerId, 'peer-left');
    this._notify();
  }

  async onSignal(from, payload) {
    if (!payload || typeof payload !== 'object') {
      this._log('signal-bad-payload', { from });
      return;
    }
    try {
      if (payload.kind === 'sdp-offer') {
        // We're the responder.  Build (or reuse) the PC and answer.
        const state = this._peers.get(from) ?? this._initResponderState(from);
        await this._handleOffer(state, payload.sdp);
      } else if (payload.kind === 'sdp-answer') {
        const peer = this._peers.get(from);
        if (!peer || !peer.pc) {
          this._log('answer-for-unknown', { from });
          return;
        }
        await peer.pc.setRemoteDescription({ type: 'answer', sdp: payload.sdp });
        await this._flushPendingCandidates(peer);
      } else if (payload.kind === 'ice') {
        const peer = this._peers.get(from);
        if (!peer) {
          this._log('ice-for-unknown', { from });
          return;
        }
        if (peer.pc && peer.pc.remoteDescription) {
          await peer.pc.addIceCandidate(payload.candidate);
        } else {
          // PC not ready (responder hasn't received offer yet) — queue.
          peer.pendingCandidates.push(payload.candidate);
        }
      } else {
        this._log('signal-unknown-kind', { from, kind: payload.kind });
      }
    } catch (err) {
      this._log('signal-handler-failed', {
        from, kind: payload.kind, err: err.message,
      });
    }
    this._notify();
  }

  // ── Peer state construction ──────────────────────────────────────

  _newPeerState(peerId, role) {
    return {
      peerId, role,
      state: 'new',
      pc: null, dc: null,
      since: Date.now(),
      openedAt: 0,
      pings: 0, pongs: 0,
      lastPongAt: 0,
      rttBuffer: [],
      pendingCandidates: [],
      pingTimer: null,
      reaperTimer: null,
      sendFailures: 0,
      retryTimer: null,
      retryUsed: false,
      localCand:  null,
      remoteCand: null,
      pathPollTimer: null,
    };
  }

  /** Build a PC for either role and wire its common event handlers. */
  _attachPc(state) {
    const pc = new RTCPeerConnection(this._iceConfig());
    state.pc = pc;
    state.state = 'signaling';

    pc.onicecandidate = (ev) => {
      if (!ev.candidate) return;
      // Log every candidate we generate locally; helps diagnose which
      // address family / interface is being offered.
      this._log('ice-candidate-local', {
        peerId: state.peerId,
        type:   ev.candidate.type,                  // 'host' | 'srflx' | 'prflx' | 'relay'
        proto:  ev.candidate.protocol,              // 'udp' | 'tcp'
        addr:   ev.candidate.address,
        port:   ev.candidate.port,
      });
      this._sendSignal(state.peerId, {
        kind: 'ice',
        candidate: ev.candidate.toJSON(),
      });
    };

    pc.onconnectionstatechange = () => this._onConnState(state, pc.connectionState);

    // ICE-level state transitions: 'new' → 'checking' → 'connected' →
    // 'completed' → 'disconnected' → 'failed' → 'closed'.  More
    // granular than connectionState; in particular it shows the
    // 'disconnected' phase, where consent freshness has started to
    // fail but the PC hasn't given up yet.
    pc.oniceconnectionstatechange = () => {
      this._log('ice-state', {
        peerId: state.peerId,
        ice:    pc.iceConnectionState,
      });
      if (pc.iceConnectionState === 'disconnected' ||
          pc.iceConnectionState === 'failed') {
        this._refreshPath(state, `ice-${pc.iceConnectionState}`);
      }
    };

    return pc;
  }

  /**
   * Look up the nominated candidate pair, cache its `candidateType`s on
   * the peer state (`localCand`, `remoteCand`), and log the details.
   *
   * Called at:
   *   - dc-open                 — initial path discovery
   *   - ice-disconnected/failed — what was the path when it died?
   *   - periodic (every 10s)    — catch ICE renegotiations silently
   *
   * If `silent` is true, the log line is suppressed UNLESS the path
   * actually changed.  The periodic poll uses silent=true so the event
   * log doesn't get spammed once per peer per ten seconds.  Every call
   * still updates the cache and triggers a UI re-render when the path
   * changes — that's how the "via TURN" badge appears/disappears.
   */
  async _refreshPath(state, when, { silent = false } = {}) {
    if (!state.pc) return;
    let stats;
    try {
      stats = await state.pc.getStats();
    } catch (err) {
      this._log('stats-failed', { peerId: state.peerId, err: err.message });
      return;
    }

    let pair = null, local = null, remote = null;
    stats.forEach(s => {
      if (s.type === 'candidate-pair' && s.nominated) pair = s;
    });
    if (pair) {
      stats.forEach(s => {
        if (s.id === pair.localCandidateId)  local  = s;
        if (s.id === pair.remoteCandidateId) remote = s;
      });
    }

    const newLocal  = local?.candidateType  ?? null;
    const newRemote = remote?.candidateType ?? null;
    const changed = (state.localCand !== newLocal) ||
                    (state.remoteCand !== newRemote);

    state.localCand  = newLocal;
    state.remoteCand = newRemote;

    if (!silent || changed) {
      this._log('stats', {
        peerId: state.peerId,
        when,
        pairState:    pair?.state,
        bytesSent:    pair?.bytesSent,
        bytesRecv:    pair?.bytesReceived,
        local:  local
          ? `${local.candidateType}/${local.protocol} ${local.ip ?? local.address}:${local.port}`
          : 'unknown',
        remote: remote
          ? `${remote.candidateType}/${remote.protocol} ${remote.ip ?? remote.address}:${remote.port}`
          : 'unknown',
      });
    }

    if (changed) this._notify();
  }

  /**
   * Re-poll getStats() every 10s once the DC is open.  ICE can
   * renegotiate underneath us — e.g. a network change moves us from
   * srflx to relay, or our laptop wakes from sleep on a new IP — and
   * we want the UI to reflect the new path.  Silent unless something
   * actually shifts.
   */
  _startPathPoll(state) {
    if (state.pathPollTimer) clearInterval(state.pathPollTimer);
    state.pathPollTimer = setInterval(() => {
      this._refreshPath(state, 'periodic', { silent: true });
    }, 10_000);
  }

  async _initiateTo(peerId) {
    this._log('initiate', { peerId });
    const state = this._newPeerState(peerId, 'offerer');
    this._peers.set(peerId, state);
    this._armReaper(state);
    this._attachPc(state);

    // Offerer creates the DataChannel up front.
    const dc = state.pc.createDataChannel(DC_LABEL, { ordered: true });
    state.dc = dc;
    this._wireDataChannel(state, dc);

    try {
      const offer = await state.pc.createOffer();
      await state.pc.setLocalDescription(offer);
      this._sendSignal(peerId, { kind: 'sdp-offer', sdp: offer.sdp });
    } catch (err) {
      this._log('offer-create-failed', { peerId, err: err.message });
      state.state = 'failed';
      this._scheduleRetry(state);
      this._notify();
    }
  }

  _acceptFrom(peerId) {
    this._log('accept', { peerId });
    // We don't build the PC yet — we wait for the offer to arrive.
    // Recording the peer here just gives the UI a row to show.
    const state = this._newPeerState(peerId, 'responder');
    this._peers.set(peerId, state);
    this._armReaper(state);
  }

  /** Build the PC for a responder that just got an offer (no prior row). */
  _initResponderState(peerId) {
    const state = this._newPeerState(peerId, 'responder');
    this._peers.set(peerId, state);
    this._armReaper(state);
    return state;
  }

  async _handleOffer(state, sdp) {
    if (!state.pc) {
      this._attachPc(state);
      state.pc.ondatachannel = (ev) => {
        state.dc = ev.channel;
        this._wireDataChannel(state, ev.channel);
      };
    }
    await state.pc.setRemoteDescription({ type: 'offer', sdp });
    await this._flushPendingCandidates(state);
    const answer = await state.pc.createAnswer();
    await state.pc.setLocalDescription(answer);
    this._sendSignal(state.peerId, { kind: 'sdp-answer', sdp: answer.sdp });
  }

  async _flushPendingCandidates(state) {
    while (state.pendingCandidates.length > 0) {
      const c = state.pendingCandidates.shift();
      try { await state.pc.addIceCandidate(c); }
      catch (err) {
        this._log('flush-ice-failed', {
          peerId: state.peerId, err: err.message,
        });
      }
    }
  }

  // ── DataChannel + ping/pong ──────────────────────────────────────

  _wireDataChannel(state, dc) {
    state.state = 'datachannel-opening';

    dc.onopen = () => {
      state.state = 'open';
      state.openedAt = Date.now();
      // Cancel any pending retry — we've connected successfully.
      if (state.retryTimer) {
        clearTimeout(state.retryTimer);
        state.retryTimer = null;
      }
      // Opened in time: drop the absolute negotiation deadline so a future
      // re-drive of this peer starts with a fresh window. The reaper itself
      // keeps running — it self-transitions from its never-opened branch to
      // the open-channel (pong/send-fail/stale) branch now that openedAt is set.
      this._negotiationDeadline.delete(state.peerId);
      state.retryUsed = false;
      this._log('dc-open', { peerId: state.peerId, role: state.role });
      // Dump the nominated candidate pair so we can see what
      // address family / protocol the data path is actually using —
      // and keep polling so we notice ICE renegotiations later on.
      this._refreshPath(state, 'dc-open');
      this._startPathPoll(state);
      this._startPingLoop(state);
      this._notify();
    };

    dc.onclose = () => {
      this._log('dc-close', { peerId: state.peerId });
      // Don't tear down here — onconnectionstatechange / peer-left will
      // arrive shortly with the canonical cleanup signal.  If we tore
      // down here too we'd risk double-cleanup races.
    };

    dc.onerror = (ev) => {
      this._log('dc-error', {
        peerId: state.peerId,
        err: ev.error?.message ?? 'unknown',
      });
    };

    dc.onmessage = (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data, bigintReviver); }
      catch { this._log('dc-bad-json', { peerId: state.peerId }); return; }

      if (msg.type === 'ping') {
        // Echo the timestamp back.
        if (state.dc?.readyState === 'open') {
          try {
            state.dc.send(JSON.stringify({
              type: 'pong', t: msg.t, peerT: Date.now(),
            }));
          } catch (err) {
            this._log('pong-send-failed', {
              peerId: state.peerId, err: err.message,
            });
          }
        }
      } else if (msg.type === 'pong') {
        const rtt = Date.now() - msg.t;
        state.pongs++;
        state.lastPongAt = Date.now();
        state.rttBuffer.push(rtt);
        if (state.rttBuffer.length > RTT_WINDOW) state.rttBuffer.shift();
        if (state.state === 'stale') state.state = 'open';
        this._emitPingTraffic(state.peerId, 'recv');
        this._notify();
      } else {
        // v0.4.0 — non-ping/pong frame: forward to Transport listeners.
        // WebRTCTransport routes by msg.k (req / res / ntf) downstream;
        // MeshManager itself doesn't care about the envelope shape.
        for (const cb of this._messageListeners) {
          try { cb(state.peerId, msg); }
          catch (err) {
            this._log('msg-listener-threw', {
              peerId: state.peerId, err: err.message,
            });
          }
        }
      }
    };
  }

  _startPingLoop(state) {
    if (state.pingTimer) clearInterval(state.pingTimer);
    state.pingTimer = setInterval(() => this._pingTick(state), PING_INTERVAL_MS);
  }

  /** One heartbeat-ping send. This is an ACTION (keepalive), not a death
   *  detector: it only records the consecutive-failure streak on
   *  state.sendFailures; the reaper (_reapTick) reads that streak and is the
   *  single place that evicts. Returns 'sent' | 'skip' | 'fail' | 'fail-limit'
   *  ('fail-limit' = the streak has reached SEND_FAIL_LIMIT and the next reap
   *  will evict). */
  _pingTick(state) {
    if (state.dc?.readyState !== 'open') return 'skip';
    try {
      state.dc.send(JSON.stringify({ type: 'ping', t: Date.now() }));
      state.pings++;
      state.sendFailures = 0;          // a successful send clears the streak
      this._emitPingTraffic(state.peerId, 'sent');
      return 'sent';
    } catch (err) {
      this._log('ping-send-failed', {
        peerId: state.peerId, err: err.message,
      });
      // A throwing send proves the channel is dead even though readyState
      // still reads 'open' (Safari after sleep). Record the streak; the
      // reaper evicts once it crosses SEND_FAIL_LIMIT so the mesh heals.
      state.sendFailures = (state.sendFailures || 0) + 1;
      return state.sendFailures >= SEND_FAIL_LIMIT ? 'fail-limit' : 'fail';
    }
  }

  // Arm (or re-arm, across retries) the single per-peer liveness reaper. The
  // reaper is the ONE timer that makes every death decision for this peer; it
  // runs from creation (covering the never-opened negotiation window) straight
  // through the open channel's life. The absolute negotiation deadline is kept
  // separately in _negotiationDeadline — set once on the first not-yet-connected
  // negotiation and preserved through retries — so total negotiation time is
  // bounded regardless of how many fresh PCs the retry path creates.
  _armReaper(state) {
    const peerId = state.peerId;
    if (!this._negotiationDeadline.has(peerId)) {
      this._negotiationDeadline.set(peerId, Date.now() + NEGOTIATION_DEADLINE_MS);
    }
    if (state.reaperTimer) clearInterval(state.reaperTimer);
    state.reaperTimer = setInterval(() => this._reapTick(state), REAP_INTERVAL_MS);
  }

  /** The single liveness verdict for one peer, run by the reaper interval from
   *  creation through death. This is the ONE place a peer dies for a
   *  liveness reason — it folds the three open-channel death detectors that
   *  used to be separate timers/methods (negotiation-timeout while never
   *  opened, pong-timeout and send-fail once open) plus the stale/recovered
   *  DISPLAY transition. Returns a verdict string for unit-testing:
   *  'gone' | 'negotiating' | 'reaped-negotiation' | 'reaped-send' |
   *  'reaped-pong' | 'stale' | 'recovered' | 'live'. */
  _reapTick(state) {
    if (this._peers.get(state.peerId) !== state) return 'gone';   // replaced/removed
    const now = Date.now();

    // ── never opened: bounded by the absolute negotiation deadline ──
    // A PC that fails ICE never autonomously reaches 'closed', so without this
    // a peer stuck in new/signaling/failed would wedge hasPeer() true forever
    // and no-op connectViaRelay's idempotency guard (it could never reconnect
    // bridgeless). Frees the slot; onPeerLost does NOT fire (no one used it).
    if (state.openedAt === 0) {
      const deadline = this._negotiationDeadline.get(state.peerId);
      if (deadline != null && now >= deadline) {
        this._log('negotiation-timeout', {
          peerId: state.peerId, role: state.role, state: state.state,
        });
        this._retire(state.peerId, 'negotiation-timeout');
        return 'reaped-negotiation';
      }
      return 'negotiating';
    }

    // ── open: a throwing send is definitive proof the channel is dead even
    //    when readyState lies 'open' (Safari after sleep). _pingTick records
    //    the streak; we evict one reap after it crosses the limit. ──
    if (state.sendFailures >= SEND_FAIL_LIMIT) {
      this._retire(state.peerId, 'send-failed');
      return 'reaped-send';
    }

    // ── open: heartbeat timeout (hard death) + stale↔open display flip ──
    if (state.lastPongAt > 0) {                    // after the first pong
      const since = now - state.lastPongAt;
      if (since > DEAD_PONG_MS) {
        // Pongs stopped long enough that the channel is dead. Evict + fire
        // onPeerLost so upper layers route around and the mesh rebuilds.
        this._retire(state.peerId, 'pong-timeout');
        return 'reaped-pong';
      }
      if (since > STALE_PONG_MS && state.state !== 'stale') {
        state.state = 'stale'; this._notify(); return 'stale';
      }
      if (since <= STALE_PONG_MS && state.state === 'stale') {
        state.state = 'open';  this._notify(); return 'recovered';
      }
    }
    return 'live';
  }

  // Extracted from pc.onconnectionstatechange so the terminal-state policy is
  // unit-testable (see smoke_mesh_closed_teardown.js).
  //   failed → mark + refresh path + one scheduled retry (PC may recover via a
  //            fresh negotiation).
  //   closed → terminal; the PC can never be reused. If THIS entry is still the
  //            live one (i.e. we did NOT initiate the close via _retire, which
  //            already deleted it), the channel died out from under us (remote
  //            close / abrupt drop) — tear it down so the slot frees: hasPeer →
  //            false, onPeerLost fires for upper layers to route around, and
  //            discovery (_considerCandidate → connectViaRelay) can re-drive a
  //            FRESH negotiation. Without this the peer wedges in 'closed'
  //            forever and connectViaRelay's idempotency guard no-ops
  //            permanently — a peer discovered after such a drop could never
  //            reconnect bridgeless.
  _onConnState(state, connectionState) {
    this._log('pc-state', { peerId: state.peerId, pc: connectionState });
    if (connectionState === 'failed') {
      state.state = 'failed';
      this._refreshPath(state, 'on-failed');
      this._scheduleRetry(state);
      this._notify();
    } else if (connectionState === 'closed') {
      state.state = 'closed';
      if (this._peers.get(state.peerId) === state) {
        this._retire(state.peerId, 'pc-closed');
      }
      this._notify();
    }
  }

  _scheduleRetry(state) {
    if (state.retryUsed) return;            // we get one retry per peer
    if (state.retryTimer) return;
    if (state.role !== 'offerer') return;   // only offerers retry; responders wait
    // Don't retry past the absolute negotiation deadline — the reaper will
    // tear the peer down so a LATER fresh discovery can re-drive cleanly.
    // Without this, each retry's fresh state resets retryUsed and the offerer
    // would re-offer every RETRY_AFTER_MS forever for an unreachable peer.
    const deadline = this._negotiationDeadline.get(state.peerId);
    if (deadline != null && Date.now() >= deadline) return;
    state.retryUsed = true;
    state.retryTimer = setTimeout(() => {
      state.retryTimer = null;
      // Has peer-left arrived in the meantime?  If so, _retire removed us.
      if (!this._peers.has(state.peerId)) return;
      this._log('retry', { peerId: state.peerId });
      // Retire the failed PC but KEEP the absolute deadline (so the fresh
      // negotiation honours the original window) and DON'T fire onPeerLost
      // (we're immediately re-initiating), then re-offer.
      this._retire(state.peerId, 'retry', { keepDeadline: true, notifyLost: false });
      this._initiateTo(state.peerId);
    }, RETRY_AFTER_MS);
  }

  // ── Cleanup ──────────────────────────────────────────────────────

  /**
   * The SINGLE teardown for a peer — every death path funnels through here
   * (dispose/reset/disconnect, peer-left, pc-closed, and the reaper's
   * negotiation-/pong-/send-fail evictions). Clears all per-peer timers,
   * closes the dc/pc, drops the map entry, and (by default) fires onPeerLost
   * for a channel that had actually opened.
   *
   * @param {string} peerId
   * @param {string} reason
   * @param {object} [opts]
   * @param {boolean} [opts.keepDeadline=false]  Keep _negotiationDeadline[peerId]
   *        — the retry path re-creates the state via _initiateTo, whose
   *        _armReaper must honour the ORIGINAL absolute deadline so total
   *        negotiation time stays bounded across retries. Default clears it so a
   *        future legitimate re-drive starts with a fresh window.
   * @param {boolean} [opts.notifyLost=true]  Fire onPeerLost iff the channel had
   *        opened. The retry path passes false: it's immediately re-initiating,
   *        so the peer isn't "lost" from the Transport's perspective.
   */
  _retire(peerId, reason, { keepDeadline = false, notifyLost = true } = {}) {
    const state = this._peers.get(peerId);
    if (!state) return;
    // "Was this channel ever open?" — check the openedAt timestamp set when
    // dc.onopen first fired, NOT the dc's current readyState. By the time
    // _retire runs the browser may already have flipped the dc from 'open' to
    // 'closing'/'closed', which would mask the real open-history and skip
    // onPeerLost — leaving stale bindings (unbindPeer never ran) and ghost
    // children in AxonaManager roles across the network.
    const wasOpen = state.openedAt > 0;
    this._log('teardown', {
      peerId, reason,
      role:    state.role,
      state:   state.state,
      hadDc:   !!state.dc,
      pings:   state.pings,
      pongs:   state.pongs,
    });
    if (state.pingTimer)     clearInterval(state.pingTimer);
    if (state.reaperTimer)   clearInterval(state.reaperTimer);
    if (state.retryTimer)    clearTimeout (state.retryTimer);
    if (state.pathPollTimer) clearInterval(state.pathPollTimer);
    if (state.dc) try { state.dc.close(); } catch {}
    if (state.pc) try { state.pc.close(); } catch {}
    this._peers.delete(peerId);
    if (!keepDeadline) this._negotiationDeadline.delete(peerId);
    // Fire onPeerLost for Transport listeners ONLY when the channel actually
    // went open → closed; retiring a never-opened peer (failed ICE) does not
    // count as a peer death because no one was using it.
    if (notifyLost && wasOpen) {
      for (const cb of this._peerLostListeners) {
        try { cb(peerId); }
        catch (err) {
          this._log('peer-lost-listener-threw', {
            peerId, err: err.message,
          });
        }
      }
    }
  }
}
