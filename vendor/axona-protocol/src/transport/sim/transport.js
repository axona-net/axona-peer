// =====================================================================
// SimTransport — in-process Transport.sim() implementation.
//
// Every method of the Transport contract is implemented end-to-end so
// kernel code can run against the sim transport identically to how it
// runs against Transport.web() / Transport.node() in production.
//
// - openConnection: peer lookup in SimNetwork + bilateral admission
//   (each side accepts; either side may refuse via accept callback)
// - send: look up target transport, deliver to its registered handler,
//   await the handler's return value, deliver back to caller.
//   Latency from SimNetwork's latencyFn is honoured both ways.
// - notify: same lookup, fire-and-forget.
// - heartbeat: 1 Hz ping/pong loop per open channel (configurable);
//   updates RTT (= 2 × one-way latency from the SimNetwork), fires
//   onPeerDied if the target unregisters or closes.
//
// Identity-wise, the SimTransport is keyed on the hex nodeId string
// (66-char), not the BigInt. This matches the wire format and avoids
// BigInt-as-Map-key pitfalls.
// =====================================================================

import { AxonaError, TransportError, ErrorCodes } from '../../errors.js';
import { toHex }                                  from '../../utils/hexid.js';
import { Transport }                              from '../../contracts/Transport.js';

const HEARTBEAT_MS      = 1000;
const HEARTBEAT_TIMEOUT = 3000;
const SEND_TIMEOUT_MS   = 5000;

/**
 * @typedef {(fromId: string, payload: any) => any | Promise<any>} RequestHandler
 * @typedef {(fromId: string, payload: any) => void}                NotificationHandler
 */

export class SimTransport extends Transport {
  /**
   * @param {object} opts
   * @param {import('./network.js').SimNetwork} opts.network
   * @param {object|null} [opts.identity]  Optional Identity — start()
   *        will pull its 66-char hex nodeId from `identity.id` if no
   *        localNodeId is passed in explicitly.
   * @param {(peerId: string) => boolean} [opts.acceptConnection]
   *        Bilateral cap hook. Return false to refuse a peer opening
   *        a channel into us. Default: accept everyone.
   * @param {number} [opts.heartbeatMs]
   * @param {number} [opts.heartbeatTimeoutMs]
   * @param {number} [opts.sendTimeoutMs]
   */
  constructor({
    network,
    identity = null,
    acceptConnection = () => true,
    heartbeatMs        = HEARTBEAT_MS,
    heartbeatTimeoutMs = HEARTBEAT_TIMEOUT,
    sendTimeoutMs      = SEND_TIMEOUT_MS,
  } = {}) {
    super();
    if (!network) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'SimTransport: network is required');
    }
    this._network            = network;
    this._identity           = identity;
    this._acceptConnection   = acceptConnection;
    this._heartbeatMs        = heartbeatMs;
    this._heartbeatTimeoutMs = heartbeatTimeoutMs;
    this._sendTimeoutMs      = sendTimeoutMs;

    /** @type {string|null} */
    this._localId = null;
    this._started = false;

    /** @type {Set<string>} peers we have an open channel to */
    this._openTo = new Set();

    /** @type {Map<string, number>} per-peer RTT in ms */
    this._latency = new Map();

    /** @type {Map<string, RequestHandler>} */
    this._reqHandlers = new Map();
    /** @type {Map<string, NotificationHandler>} */
    this._ntfHandlers = new Map();
    /** @type {Set<(peerId: string) => void>} */
    this._diedHandlers = new Set();

    /** @type {Map<string, ReturnType<typeof setInterval>>} */
    this._heartbeats = new Map();
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────

  async start(localNodeId) {
    if (this._started) return;
    const idHex = this._resolveLocalId(localNodeId);
    this._localId = idHex;
    this._network._register(idHex, this);
    this._started = true;
  }

  async stop() {
    if (!this._started) return;
    // Close every open channel cleanly; let other peers learn we left.
    for (const peerId of [...this._openTo]) {
      await this._closeChannel(peerId, /* notify */ true);
    }
    for (const t of this._heartbeats.values()) clearInterval(t);
    this._heartbeats.clear();
    this._network._unregister(this._localId);
    this._localId = null;
    this._started = false;
  }

  getLocalNodeId() {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'SimTransport: getLocalNodeId before start()');
    }
    return this._localId;
  }

  _resolveLocalId(arg) {
    if (typeof arg === 'string') return arg;
    if (typeof arg === 'bigint') return toHex(arg);
    if (this._identity?.id)      return this._identity.id;
    throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
      'SimTransport.start: localNodeId or constructor identity required');
  }

  // ─── Channel pool ──────────────────────────────────────────────────

  async openConnection(peerId) {
    this._assertStarted();
    const target = this._network._lookup(peerId);
    if (!target) return false;
    if (peerId === this._localId) return false;

    // Bilateral admission: ask target if it'll accept us.
    if (!target._acceptConnection(this._localId)) return false;

    // Both sides record the channel.
    this._openTo.add(peerId);
    target._openTo.add(this._localId);

    // RTT seed = 2 × one-way latency (we'll keep this accurate via heartbeats).
    const rtt = this._network._latencyMs(this._localId, peerId) +
                this._network._latencyMs(peerId, this._localId);
    this._latency.set(peerId, rtt);
    target._latency.set(this._localId, rtt);

    this._startHeartbeat(peerId);
    target._startHeartbeat(this._localId);
    return true;
  }

  async closeConnection(peerId) {
    if (!this._started) return;
    await this._closeChannel(peerId, /* notify */ true);
  }

  isConnected(peerId) {
    return this._openTo.has(peerId);
  }

  async _closeChannel(peerId, notify) {
    if (!this._openTo.has(peerId)) return;
    this._openTo.delete(peerId);
    this._latency.delete(peerId);
    const hb = this._heartbeats.get(peerId);
    if (hb) { clearInterval(hb); this._heartbeats.delete(peerId); }

    if (notify) {
      const target = this._network._lookup(peerId);
      if (target && target._openTo.has(this._localId)) {
        target._openTo.delete(this._localId);
        target._latency.delete(this._localId);
        const thb = target._heartbeats.get(this._localId);
        if (thb) { clearInterval(thb); target._heartbeats.delete(this._localId); }
        target._fireDied(this._localId);
      }
    }
  }

  // ─── Messaging ─────────────────────────────────────────────────────

  async send(peerId, type, payload) {
    this._assertStarted();
    if (!this._openTo.has(peerId)) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        `SimTransport.send: no open channel to ${peerId}`,
        { context: { peerId, type } });
    }
    const target = this._network._lookup(peerId);
    if (!target) {
      // Peer unregistered between channel-open and now.
      await this._closeChannel(peerId, /* notify */ false);
      this._fireDied(peerId);
      throw new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `SimTransport.send: peer ${peerId} not registered`,
        { context: { peerId, type } });
    }
    const handler = target._reqHandlers.get(type);
    if (!handler) {
      throw new TransportError(ErrorCodes.TRANSPORT_PEER_UNREACHABLE,
        `SimTransport.send: peer ${peerId} has no handler for '${type}'`,
        { context: { peerId, type } });
    }

    // Round-trip latency simulation. The forward-trip delays handler
    // invocation; the return-trip delays the resolution.
    const fwd = this._network._latencyMs(this._localId, peerId);
    const back = this._network._latencyMs(peerId, this._localId);
    await sleep(fwd);

    const fromId = this._localId;
    return withTimeout(
      (async () => {
        const result = await handler(fromId, payload);
        await sleep(back);
        return result;
      })(),
      this._sendTimeoutMs,
      `SimTransport.send: timed out waiting for '${type}' from ${peerId}`,
    );
  }

  async notify(peerId, type, payload) {
    this._assertStarted();
    if (!this._openTo.has(peerId)) {
      throw new TransportError(ErrorCodes.TRANSPORT_CHANNEL_CLOSED,
        `SimTransport.notify: no open channel to ${peerId}`,
        { context: { peerId, type } });
    }
    const target = this._network._lookup(peerId);
    if (!target) return;            // silent — fire and forget
    const handler = target._ntfHandlers.get(type);
    if (!handler) return;

    // Schedule on the next tick so notify() returns immediately
    // regardless of latency.  Real transports do the same — the
    // round-trip cost is absorbed by the receiver, not the sender.
    const fromId  = this._localId;
    const fwd     = this._network._latencyMs(this._localId, peerId);
    setTimeout(() => {
      try { handler(fromId, payload); }
      catch (err) { /* notifications swallow handler errors */ }
    }, fwd);
  }

  onRequest(type, handler) {
    if (typeof type !== 'string' || typeof handler !== 'function') {
      throw new TypeError('onRequest: (type: string, handler: function) required');
    }
    this._reqHandlers.set(type, handler);
  }

  onNotification(type, handler) {
    if (typeof type !== 'string' || typeof handler !== 'function') {
      throw new TypeError('onNotification: (type: string, handler: function) required');
    }
    this._ntfHandlers.set(type, handler);
  }

  // ─── Liveness ──────────────────────────────────────────────────────

  onPeerDied(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('onPeerDied: handler must be a function');
    }
    this._diedHandlers.add(handler);
    return () => this._diedHandlers.delete(handler);
  }

  getLatency(peerId) {
    return this._latency.get(peerId) ?? -1;
  }

  // ─── Heartbeat ─────────────────────────────────────────────────────

  _startHeartbeat(peerId) {
    if (this._heartbeats.has(peerId)) return;
    if (this._heartbeatMs <= 0) return;       // disabled (e.g. tests)

    const interval = setInterval(async () => {
      const start = Date.now();
      const target = this._network._lookup(peerId);
      if (!target || !target._openTo.has(this._localId)) {
        await this._closeChannel(peerId, /* notify */ false);
        this._fireDied(peerId);
        return;
      }
      // Simulated heartbeat — just refresh the RTT from the latencyFn.
      // No real timeout possible in this transport since calls are sync.
      const rtt = this._network._latencyMs(this._localId, peerId) +
                  this._network._latencyMs(peerId, this._localId);
      this._latency.set(peerId, rtt);
      // Touch start so eslint doesn't think it's unused; in a real
      // transport this would be the heartbeat-RTT measurement.
      void start;
    }, this._heartbeatMs);

    // Unref so the interval doesn't keep Node alive in tests.
    if (typeof interval.unref === 'function') interval.unref();
    this._heartbeats.set(peerId, interval);
  }

  _fireDied(peerId) {
    for (const h of this._diedHandlers) {
      try { h(peerId); }
      catch (err) { /* swallow */ }
    }
  }

  _assertStarted() {
    if (!this._started) {
      throw new TransportError(ErrorCodes.TRANSPORT_NOT_STARTED,
        'SimTransport not started');
    }
  }
}

// ── helpers ──────────────────────────────────────────────────────────

function sleep(ms) {
  if (ms <= 0) return Promise.resolve();
  return new Promise(r => setTimeout(r, ms));
}

function withTimeout(p, ms, message) {
  if (ms <= 0) return p;
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      reject(new TransportError(ErrorCodes.TRANSPORT_TIMEOUT, message));
    }, ms);
    if (typeof t.unref === 'function') t.unref();
    p.then(v => { clearTimeout(t); resolve(v); },
           e => { clearTimeout(t); reject(e); });
  });
}
