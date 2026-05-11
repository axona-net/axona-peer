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

// ── ICE configuration ───────────────────────────────────────────────
//
// STUN servers let each peer discover its public-facing (post-NAT)
// IP and offer that as a candidate.  Cross-NAT pairs (e.g., a phone
// on cellular talking to a laptop on home Wi-Fi) need this — without
// STUN, peers only see each other's private host IPs and can't
// reach each other.
//
// We use Google's free public STUN servers.  They're stable and
// widely used for WebRTC dev; for production we'd consider running
// our own to avoid the soft dependency.
//
// TURN is the next escalation, used when direct peer-to-peer is
// blocked by symmetric NATs or strict firewalls (cellular carriers
// typically, double-NATs on tethered hotspots).  We run a coturn
// instance on the same droplet as the bridge.  Credentials are a
// static long-term pair for now; later we'll switch to short-lived
// HMAC-signed credentials handed out by the bridge so the password
// can't be skimmed from the page source.
//
// The TURN URL is keyed by IP because turn.axona.net DNS + TLS cert
// aren't provisioned yet.  Once they are, the `turn:` entry becomes
// `turn:turn.axona.net:3478` and we add a `turns:turn.axona.net:5349`
// entry alongside it for clients on networks that block UDP 3478.
const RTC_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302'  },
    { urls: 'stun:stun1.l.google.com:19302' },
    {
      urls: 'turn:64.227.2.28:3478',
      username:   'axonademo',
      credential: 'a0f479670d8f533d2c5a5449c1c3bbaa',
    },
  ],
};

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
  }

  // ── External lifecycle ────────────────────────────────────────────

  setMyId(id) {
    this._myId = id;
  }

  /** Subscribe to mesh-state changes.  Returns an unsubscribe fn. */
  onChange(callback) {
    this._listeners.add(callback);
    return () => this._listeners.delete(callback);
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
    }));
  }

  /** Disconnect from everyone and stop all timers. */
  dispose() {
    for (const id of [...this._peers.keys()]) {
      this._teardown(id, 'dispose');
    }
    this._listeners.clear();
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
    };
  }

  /** Build a PC for either role and wire its common event handlers. */
  _attachPc(state) {
    const pc = new RTCPeerConnection(RTC_CONFIG);
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
        this._dumpStats(state, 'on-failed');
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
        this._dumpStats(state, `ice-${pc.iceConnectionState}`);
      }
    };

    return pc;
  }

  /** Dump the nominated candidate pair + transport stats. */
  async _dumpStats(state, when) {
    if (!state.pc) return;
    try {
      const stats = await state.pc.getStats();
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
    } catch (err) {
      this._log('stats-failed', { peerId: state.peerId, err: err.message });
    }
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
      // address family / protocol the data path is actually using.
      this._dumpStats(state, 'dc-open');
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
      try { msg = JSON.parse(ev.data); }
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
    this._log('teardown', {
      peerId, reason,
      role:    state.role,
      state:   state.state,
      hadDc:   !!state.dc,
      pings:   state.pings,
      pongs:   state.pongs,
    });
    if (state.pingTimer)  clearInterval(state.pingTimer);
    if (state.staleTimer) clearInterval(state.staleTimer);
    if (state.retryTimer) clearTimeout(state.retryTimer);
    if (state.dc) try { state.dc.close(); } catch {}
    if (state.pc) try { state.pc.close(); } catch {}
    this._peers.delete(peerId);
  }

  /** Like _teardown but used by the retry path: keep the map entry
   *  removed so _initiateTo can build a fresh one. */
  _teardownButKeep(peerId) {
    const state = this._peers.get(peerId);
    if (!state) return;
    if (state.pingTimer)  clearInterval(state.pingTimer);
    if (state.staleTimer) clearInterval(state.staleTimer);
    if (state.dc) try { state.dc.close(); } catch {}
    if (state.pc) try { state.pc.close(); } catch {}
    this._peers.delete(peerId);
  }
}
