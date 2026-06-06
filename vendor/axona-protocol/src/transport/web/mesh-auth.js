// =====================================================================
// mesh-auth.js — the axona/4 authenticated-identity handshake for the
// WebRTC mesh, extracted from webTransport() so it is unit-testable
// without a browser or real RTCPeerConnection.
//
// Symmetric 3-message mutual handshake, one per peer ("meshId"):
//
//     A ──hello{nonce}──► B          B ──hello{nonce}──► A
//     A ──hello-sig{proof over CBV}──► B   (and vice-versa)
//     each side verifies the other's proof, then bindPeer()s it.
//
// The channel-binding value (CBV) has two parts:
//
//   (1) a fresh per-link nonce pair — cbvFromNonces(myNonce, peerNonce,
//       MESH_CBV_TAG).  Provides freshness/replay-resistance.  The nonce
//       pair is sorted (order-independent) and the TAG MUST be a constant
//       both sides share — an earlier version folded the peer's bridge
//       connId in as the tag, but that connId differs per side (each holds
//       the OTHER peer's), so the two endpoints derived different CBVs and
//       EVERY signature failed → the mesh never bound a single peer.  The
//       constant tag now only provides cross-transport domain separation
//       (mesh vs 'bridge' vs 'ws' vs sim).
//
//   (2) the DTLS channel fingerprints — cbvFromFingerprints(localFp,
//       remoteFp), folded in when a `fingerprints(meshId)` callback is
//       supplied (the real WebRTC mesh).  This is finding A-1: nonces
//       alone travel as cleartext through the bridge-relayed signaling,
//       so a malicious bridge could terminate DTLS on both legs, forward
//       the nonce/proof frames verbatim, and MITM "direct" peer traffic
//       while both signatures still verified.  Binding the CBV to each
//       side's actual DTLS cert means a fingerprint-rewriting bridge
//       produces divergent CBVs and the mutual signature fails — the
//       untrusted-bridge premise is restored.
//
// When no `fingerprints` callback is provided (sim transport, unit-test
// loopback) the CBV is nonce-only — those paths have no DTLS channel to
// bind to.  Callback presence is a local, non-negotiated decision, so a
// peer can't be downgraded to nonce-only by a remote attacker.
//
// Transport-agnostic by construction: it receives `send` / `bindPeer` /
// `fingerprints` callbacks and is driven by onChannelOpen / onHello /
// onHelloSig / onChannelLost.  webTransport wires those to the
// MeshManager; a test wires two instances over an in-memory loopback.
// =====================================================================

import { buildAuthHello, verifyAuthHello, makeNonce, cbvFromNonces, cbvFromFingerprints, AUTH_PROTO } from '../handshake-auth.js';

/** Symmetric domain-separation tag for the WebRTC-mesh CBV.  MUST be a
 *  constant both endpoints share (see header). */
export const MESH_CBV_TAG = 'mesh';

export class MeshAuth {
  /**
   * @param {object} opts
   * @param {object}   opts.identity   {id, pubkeyHex, sign}
   * @param {(meshId: string, frame: object) => void} opts.send
   *        Deliver a {k:'ntf', type, body} frame to the peer on `meshId`.
   * @param {(nodeIdHex: string, meshId: string) => void} opts.bindPeer
   *        Called once, after the peer's proof verifies, with the proven
   *        nodeId (hex) and the meshId it authenticated on.
   * @param {(meshId: string) => ({local:string, remote:string}|null)} [opts.fingerprints]
   *        Optional.  Returns the DTLS fingerprints for the link, folded
   *        into the CBV for channel binding (finding A-1).  When omitted
   *        the CBV is nonce-only (sim / unit-test paths with no DTLS).
   * @param {(event: string, data?: object) => void} [opts.log]
   */
  constructor({ identity, send, bindPeer, fingerprints = null, log = () => {} }) {
    if (!identity || typeof identity.sign !== 'function') {
      throw new TypeError('MeshAuth: identity with sign() required');
    }
    if (typeof send !== 'function' || typeof bindPeer !== 'function') {
      throw new TypeError('MeshAuth: send + bindPeer callbacks required');
    }
    if (fingerprints !== null && typeof fingerprints !== 'function') {
      throw new TypeError('MeshAuth: fingerprints must be a function or null');
    }
    this._identity     = identity;
    this._send         = send;
    this._bindPeer     = bindPeer;
    this._fingerprints = fingerprints;
    this._log          = log;
    /** @type {Map<string, {myNonce:string, peerNonce?:string, peerNodeId?:string, pendingSig?:object, sigSent:boolean, bound:boolean}>} */
    this._state = new Map();
  }

  /** Has the handshake on `meshId` completed (peer bound)? */
  isBound(meshId) { return this._state.get(meshId)?.bound === true; }

  /** Number of peers currently bound through this MeshAuth. */
  boundCount() {
    let n = 0;
    for (const st of this._state.values()) if (st.bound) n++;
    return n;
  }

  _ensure(meshId) {
    let st = this._state.get(meshId);
    if (!st) {
      st = { myNonce: makeNonce(), sigSent: false, bound: false };
      this._state.set(meshId, st);
      this._sendHello(meshId, st);
    }
    return st;
  }

  _cbv(meshId, st) {
    if (!st?.myNonce || !st?.peerNonce) return null;
    const nonceCbv = cbvFromNonces(st.myNonce, st.peerNonce, MESH_CBV_TAG);
    // No fingerprints callback (sim / unit-test) ⇒ nonce-only CBV.
    if (!this._fingerprints) return nonceCbv;
    // Real mesh: bind to the DTLS channel.  Fail CLOSED — if the
    // fingerprints aren't available yet, return null so _progress defers
    // rather than silently downgrading to nonce-only (which a MITM could
    // otherwise rely on).  In practice both descriptions are always in
    // place by the time a data-channel frame arrives, so this only
    // defers across a transient.
    const fp = this._fingerprints(meshId);
    if (!fp || typeof fp.local !== 'string' || typeof fp.remote !== 'string') {
      this._log('auth-mesh-awaiting-fingerprints', { meshId });
      return null;
    }
    return `${nonceCbv}|${cbvFromFingerprints(fp.local, fp.remote)}`;
  }

  _sendHello(meshId, st) {
    try {
      this._send(meshId, { k: 'ntf', type: 'hello', body: { proto: AUTH_PROTO, nonce: st.myNonce } });
    } catch (err) { this._log('mesh-hello-send-failed', { meshId, err: err.message }); }
  }

  /** A data channel to `meshId` opened — mint our nonce + send hello. */
  onChannelOpen(meshId) {
    if (typeof meshId !== 'string') return;
    if (this._state.has(meshId)) return;
    this._ensure(meshId);
  }

  /** The channel to `meshId` is gone — drop auth state. */
  onChannelLost(meshId) { this._state.delete(meshId); }

  /** Peer's hello: their nonce (no proof yet). */
  async onHello(meshId, body) {
    if (typeof meshId !== 'string') return;
    if (!body || typeof body.nonce !== 'string') { this._log('auth-mesh-bad-hello', { meshId }); return; }
    const st = this._ensure(meshId);
    st.peerNonce = body.nonce;
    await this._progress(meshId);
  }

  /** Peer's proof: the authenticated hello {proto,nodeId,pubkey,sig}. */
  async onHelloSig(meshId, body) {
    if (typeof meshId !== 'string') return;
    const st = this._ensure(meshId);
    st.pendingSig = body;
    st.peerNodeId = (body && typeof body.nodeId === 'string') ? body.nodeId : st.peerNodeId;
    await this._progress(meshId);
  }

  // (a) send our signed proof once we know the peer's nonce; (b) verify +
  // bind once the peer's proof is in hand.  Idempotent.
  async _progress(meshId) {
    const st = this._state.get(meshId);
    if (!st || st.bound) return;
    const cbv = this._cbv(meshId, st);
    if (!cbv) return;                         // missing a nonce or fingerprints

    if (!st.sigSent) {
      st.sigSent = true;
      try {
        const proof = await buildAuthHello({ identity: this._identity, cbv });
        this._send(meshId, { k: 'ntf', type: 'hello-sig', body: proof });
      } catch (err) { this._log('mesh-sig-send-failed', { meshId, err: err.message }); }
    }

    if (st.pendingSig && !st.verifying && !st.bound) {
      // Guard against re-entrancy: _progress is driven from both onHello
      // and onHelloSig and awaits verifyAuthHello, so without this flag
      // two concurrent calls could both verify and bind the same peer.
      st.verifying = true;
      // `verifying` MUST be cleared on EVERY non-success exit, or the channel
      // wedges: the guard above is `!st.verifying && !st.bound`, so any path
      // that leaves verifying=true with bound=false (the id-mismatch return, or
      // a throw from _bindPeer) blocks every future _progress retry forever —
      // the channel stays authenticated-but-never-bound (absent from
      // boundPeers, never admitted to routing) until it dies. A `finally`
      // guarantees the reset on all paths; on success bound=true blocks
      // re-entry regardless, so clearing it there too is harmless. (Verified by
      // smoke_mesh_auth_loopback testTransientBindThrowRecovers, which fails
      // without this.)
      try {
        const res = await verifyAuthHello(st.pendingSig, { cbv });
        if (!res.ok) { this._log('auth-mesh-rejected', { meshId, reason: res.reason }); return; }
        if (st.peerNodeId && res.nodeId !== st.peerNodeId) {
          this._log('auth-mesh-id-mismatch', { meshId }); return;
        }
        // Symmetric per-channel key = the sorted nonce pair.  Both endpoints
        // hold the same two nonces, so this string is IDENTICAL on each side
        // of a given channel — letting the transport deterministically pick
        // the same survivor when deduping a duplicate channel to one peer.
        const channelKey = [st.myNonce, st.peerNonce].sort().join(':');
        this._bindPeer(res.nodeId, meshId, channelKey);
        st.bound = true;
        this._log('auth-mesh-complete', { meshId, peer: res.nodeId });
      } catch (err) {
        this._log('mesh-bind-failed', { meshId, err: err.message });
      } finally {
        st.verifying = false;
      }
    }
  }
}
