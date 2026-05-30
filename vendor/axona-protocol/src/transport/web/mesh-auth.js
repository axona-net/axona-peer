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
// The channel-binding value (CBV) is cbvFromNonces(myNonce, peerNonce,
// MESH_CBV_TAG).  cbvFromNonces sorts the nonce pair, so the nonces are
// symmetric — and the TAG MUST be a constant too.  An earlier version
// folded the peer's bridge connId in as the tag; that connId differs per
// side (each holds the OTHER peer's), so the two endpoints derived
// different CBVs and EVERY signature failed → the mesh never bound a
// single peer.  The fresh per-link nonce pair already makes the CBV
// unique to this connection; the constant tag only provides cross-
// transport domain separation (mesh vs 'bridge' vs 'ws' vs sim).
//
// Transport-agnostic by construction: it receives `send` / `bindPeer`
// callbacks and is driven by onChannelOpen / onHello / onHelloSig /
// onChannelLost.  webTransport wires those to the MeshManager; a test
// wires two instances to each other over an in-memory loopback.
// =====================================================================

import { buildAuthHello, verifyAuthHello, makeNonce, cbvFromNonces, AUTH_PROTO } from '../handshake-auth.js';

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
   * @param {(event: string, data?: object) => void} [opts.log]
   */
  constructor({ identity, send, bindPeer, log = () => {} }) {
    if (!identity || typeof identity.sign !== 'function') {
      throw new TypeError('MeshAuth: identity with sign() required');
    }
    if (typeof send !== 'function' || typeof bindPeer !== 'function') {
      throw new TypeError('MeshAuth: send + bindPeer callbacks required');
    }
    this._identity = identity;
    this._send     = send;
    this._bindPeer = bindPeer;
    this._log      = log;
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

  _cbv(st) {
    if (!st?.myNonce || !st?.peerNonce) return null;
    return cbvFromNonces(st.myNonce, st.peerNonce, MESH_CBV_TAG);
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
    const cbv = this._cbv(st);
    if (!cbv) return;                         // still missing a nonce

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
      const res = await verifyAuthHello(st.pendingSig, { cbv });
      if (!res.ok) { st.verifying = false; this._log('auth-mesh-rejected', { meshId, reason: res.reason }); return; }
      if (st.peerNodeId && res.nodeId !== st.peerNodeId) {
        this._log('auth-mesh-id-mismatch', { meshId }); return;
      }
      try {
        this._bindPeer(res.nodeId, meshId);
        st.bound = true;
        this._log('auth-mesh-complete', { meshId, peer: res.nodeId });
      } catch (err) { this._log('mesh-bind-failed', { meshId, err: err.message }); }
    }
  }
}
