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
const RTT_WINDOW       = 10;
const DC_LABEL         = 'axona';
const RETRY_AFTER_MS   = 5000;   // single retry after pc-failed (B10)

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
 * @property {number | null} staleTimer
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
    if (this._turn) iceServers.push(this._turn);
    return { iceServers };
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

  /** Disconnect from everyone and stop all timers. */
  dispose() {
    for (const id of [...this._peers.keys()]) {
      this._teardown(id, 'dispose');
    }
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
      this._teardown(id, 'reset');
    }
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
    this._teardown(peerId, 'peer-left');
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
      staleTimer: null,
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

    pc.onconnectionstatechange = () => {
      this._log('pc-state', {
        peerId: state.peerId,
        pc:     pc.connectionState,
      });
      if (pc.connectionState === 'failed') {
        state.state = 'failed';
        this._refreshPath(state, 'on-failed');
        this._scheduleRetry(state);
        this._notify();
      } else if (pc.connectionState === 'closed') {
        state.state = 'closed';
        this._notify();
      }
    };

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
  }

  /** Build the PC for a responder that just got an offer (no prior row). */
  _initResponderState(peerId) {
    const state = this._newPeerState(peerId, 'responder');
    this._peers.set(peerId, state);
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
      state.retryUsed = false;
      this._log('dc-open', { peerId: state.peerId, role: state.role });
      // Dump the nominated candidate pair so we can see what
      // address family / protocol the data path is actually using —
      // and keep polling so we notice ICE renegotiations later on.
      this._refreshPath(state, 'dc-open');
      this._startPathPoll(state);
      this._startPingLoop(state);
      this._startStaleChecker(state);
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
    state.pingTimer = setInterval(() => {
      if (state.dc?.readyState !== 'open') return;
      try {
        state.dc.send(JSON.stringify({ type: 'ping', t: Date.now() }));
        state.pings++;
      } catch (err) {
        this._log('ping-send-failed', {
          peerId: state.peerId, err: err.message,
        });
      }
    }, PING_INTERVAL_MS);
  }

  _startStaleChecker(state) {
    if (state.staleTimer) clearInterval(state.staleTimer);
    state.staleTimer = setInterval(() => {
      if (state.state !== 'open' && state.state !== 'stale') return;
      if (state.lastPongAt === 0) return;   // pre-first-pong
      const since = Date.now() - state.lastPongAt;
      if (since > STALE_PONG_MS && state.state !== 'stale') {
        state.state = 'stale';
        this._notify();
      } else if (since <= STALE_PONG_MS && state.state === 'stale') {
        state.state = 'open';
        this._notify();
      }
    }, 500);
  }

  _scheduleRetry(state) {
    if (state.retryUsed) return;            // we get one retry per peer
    if (state.retryTimer) return;
    if (state.role !== 'offerer') return;   // only offerers retry; responders wait
    state.retryUsed = true;
    state.retryTimer = setTimeout(() => {
      state.retryTimer = null;
      // Has peer-left arrived in the meantime?  If so, _teardown removed us.
      if (!this._peers.has(state.peerId)) return;
      this._log('retry', { peerId: state.peerId });
      // Clean up the failed PC, then re-initiate.
      this._teardownButKeep(state.peerId);
      this._initiateTo(state.peerId);
    }, RETRY_AFTER_MS);
  }

  // ── Cleanup ──────────────────────────────────────────────────────

  _teardown(peerId, reason) {
    const state = this._peers.get(peerId);
    if (!state) return;
    // "Was this channel ever open?" — check the openedAt timestamp set
    // when dc.onopen first fired, NOT the dc's current readyState.  By
    // the time _teardown runs the browser may already have flipped the
    // dc from 'open' to 'closing'/'closed', which masked the real
    // open-history and skipped onPeerLost notifications — that left
    // stale bindings (unbindPeer never ran) and ghost children in
    // AxonManager roles across the network.
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
    if (state.staleTimer)    clearInterval(state.staleTimer);
    if (state.retryTimer)    clearTimeout (state.retryTimer);
    if (state.pathPollTimer) clearInterval(state.pathPollTimer);
    if (state.dc) try { state.dc.close(); } catch {}
    if (state.pc) try { state.pc.close(); } catch {}
    this._peers.delete(peerId);
    // v0.4.0 — fire onPeerLost for Transport listeners.  Only when the
    // channel actually went from open → closed; teardown of a never-
    // opened peer (e.g. failed ICE) does NOT count as a peer death
    // from the Transport's perspective because no one was using it.
    if (wasOpen) {
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

  /** Like _teardown but used by the retry path: keep the map entry
   *  removed so _initiateTo can build a fresh one. */
  _teardownButKeep(peerId) {
    const state = this._peers.get(peerId);
    if (!state) return;
    if (state.pingTimer)     clearInterval(state.pingTimer);
    if (state.staleTimer)    clearInterval(state.staleTimer);
    if (state.pathPollTimer) clearInterval(state.pathPollTimer);
    if (state.dc) try { state.dc.close(); } catch {}
    if (state.pc) try { state.pc.close(); } catch {}
    this._peers.delete(peerId);
  }
}
