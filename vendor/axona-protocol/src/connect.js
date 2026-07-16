// =====================================================================
// connect.js — the one-call bootstrap.
//
//   import { connect } from '@axona/protocol/connect.js';
//   const { peer, author } = await connect({
//     bridge:   'wss://testnet.axona.net',
//     location: { lat: 38.0, lng: -77.0 },
//   });
//   await peer.sub(topic, handler);
//   await peer.pub(topic, msg, { signWith: author });
//
// Everything an application previously assembled by hand — two identity
// factories, the web transport, a NeuronNode (with the hex→BigInt id
// conversion), an AxonaDomain, the AxonaPeer, transport.start, peer.start,
// and the peer.ready() mesh warm-up — collapses into one awaited call.
// The primitives all remain public and unchanged; connect() is sugar,
// not a new layer. Apps that need custom wiring (an explicit transport,
// a persistence adapter, several peers on one page) keep using the
// constructors directly.
//
// This module lives OUTSIDE the main barrel on purpose: the default
// transport is the browser/web stack, and importing that from the
// environment-neutral barrel would drag WebRTC-specific code into sim
// and server contexts. The web transport is loaded lazily, only when no
// `transport` is injected.
// =====================================================================

import { createNodeIdentity, createAuthorIdentity } from './identity/index.js';
import { NeuronNode }  from './dht/NeuronNode.js';
import { AxonaDomain } from './dht/AxonaDomain.js';
import { AxonaPeer }   from './dht/AxonaPeer.js';

/**
 * Bring up a ready-to-use Axona peer in one call.
 *
 * @param {object}  opts
 * @param {string}  [opts.bridge]        wss:// bridge URL. Required unless
 *                                       `transport` is injected.
 * @param {{lat:number, lng:number}} [opts.location]
 *                                       The node's real location (sets its
 *                                       region byte). Required unless
 *                                       `nodeIdentity` is injected.
 * @param {boolean|string|object} [opts.author=true]
 *                                       Authorship key handling:
 *                                       `true` → mint a fresh (ephemeral)
 *                                       author identity; a `string` → mint a
 *                                       DURABLE author persisted under that
 *                                       key (`createAuthorIdentity({ persistAs })`);
 *                                       an author-identity object → use it
 *                                       as-is; `false` → no author (you must
 *                                       pass your own `signWith` on publish).
 * @param {number}  [opts.k=20]          Routing closest-set size (AxonaDomain).
 * @param {object|false} [opts.ready]    Options forwarded to `peer.ready()`
 *                                       (minPeers, timeoutMs, …), or `false`
 *                                       to skip the mesh warm-up wait.
 * @param {object}  [opts.transport]     Inject a pre-built Transport (tests,
 *                                       sim, custom stacks). Skips webTransport.
 * @param {object}  [opts.nodeIdentity]  Inject a pre-minted node identity.
 * @param {object}  [opts.web]           Extra options forwarded to the
 *                                       webTransport factory (log, reconnect…).
 *
 * @returns {Promise<{
 *   peer: AxonaPeer,
 *   author: object|null,
 *   nodeIdentity: object,
 *   transport: object,
 *   status: {ready:boolean, peers:number, ms:number, reason:string}|null,
 *   disconnect: () => Promise<void>,
 * }>}
 */
export async function connect({
  bridge,
  location,
  author = true,
  k = 20,
  ready = {},
  transport,
  nodeIdentity,
  web = {},
} = {}) {
  // 1. Identities — connection (place-anchored) + authorship (place-free).
  if (!nodeIdentity) {
    if (!location || typeof location.lat !== 'number' || typeof location.lng !== 'number') {
      throw new TypeError("connect: pass { location: { lat, lng } } (or inject a nodeIdentity)");
    }
    nodeIdentity = await createNodeIdentity(location);
  }
  let authorIdentity = null;
  if (author === true)               authorIdentity = await createAuthorIdentity();
  else if (typeof author === 'string') authorIdentity = await createAuthorIdentity({ persistAs: author });
  else if (author && typeof author === 'object') authorIdentity = author;   // pre-minted, pass through

  // 2. Transport — the browser/Node web stack by default, loaded lazily so
  //    this module stays importable in sim/server contexts.
  if (!transport) {
    if (!bridge) throw new TypeError("connect: pass { bridge: 'wss://…' } (or inject a transport)");
    // Node has no WebRTC globals; the mesh reads globalThis.RTCPeerConnection
    // at CONNECTION time, so without this the failure surfaced as a
    // ReferenceError thrown inside the bridge's first relayed signal — which
    // callers experienced as connect() "hanging at transport.start" (field
    // incident, 2026-07-16). Polyfill up front from node-datachannel (the
    // same stack the relay fleet runs), or fail HERE with instructions.
    if (typeof globalThis.RTCPeerConnection === 'undefined' &&
        typeof process !== 'undefined' && process.versions?.node) {
      try {
        const ndc = await import('node-datachannel/polyfill');
        globalThis.RTCPeerConnection    ??= ndc.RTCPeerConnection;
        globalThis.RTCSessionDescription ??= ndc.RTCSessionDescription;
        globalThis.RTCIceCandidate       ??= ndc.RTCIceCandidate;
      } catch {
        throw new TypeError(
          "connect: Node needs a WebRTC implementation for the peer mesh — " +
          "run `npm install node-datachannel`, or set " +
          "globalThis.RTCPeerConnection yourself before calling connect()");
      }
    }
    const { webTransport } = await import('./transport/web/index.js');
    transport = webTransport({ bridgeUrl: bridge, identity: nodeIdentity, ...web });
  }

  // 3. Assemble the peer. NeuronNode's id gate (asId) accepts the identity's
  //    hex id directly — BigInt internally, hex only on the wire.
  const node = new NeuronNode({
    id: nodeIdentity.id,
    lat: nodeIdentity.lat ?? location?.lat ?? 0,
    lng: nodeIdentity.lng ?? location?.lng ?? 0,
  });
  node.transport = transport;
  const peer = new AxonaPeer({
    domain: new AxonaDomain({ k }),
    node,
    nodeIdentity,
    transport,
  });

  // 4. Lifecycle: start the wire, start the peer, wait for the mesh.
  await transport.start(nodeIdentity.id);
  await peer.start();
  const status = (ready === false) ? null : await peer.ready(ready);

  // 5. One-call teardown, mirror of the one-call bring-up.
  const disconnect = async () => {
    try { await peer.leave(); } catch { /* best-effort */ }
    try { await peer.stop?.(); } catch { /* best-effort */ }
    try { await transport.stop?.(); } catch { /* best-effort */ }
  };

  return { peer, author: authorIdentity, nodeIdentity, transport, status, disconnect };
}
