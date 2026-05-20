// =====================================================================
// transport/sim/index.js — public entry for the in-process sim transport.
//
// Usage:
//
//   import { SimNetwork, simTransport } from '@axona/protocol/transport/sim';
//
//   const network = new SimNetwork();
//   const alice = simTransport({ network, identity: aliceIdentity });
//   const bob   = simTransport({ network, identity: bobIdentity });
//   await alice.start();
//   await bob.start();
//   await alice.openConnection(bob.getLocalNodeId());
//
//   alice.onRequest('ping', (from, payload) => ({ pong: payload }));
//   const reply = await bob.send(alice.getLocalNodeId(), 'ping', { hi: 1 });
//   // reply === { pong: { hi: 1 } }
//
// One SimNetwork per simulated mesh.  Tests and dht-sim's 'axona'
// protocol entry consume this.
// =====================================================================

export { SimNetwork }  from './network.js';
export { SimTransport } from './transport.js';

import { SimTransport } from './transport.js';

/**
 * Factory: construct a SimTransport bound to a SimNetwork + identity.
 *
 * @param {ConstructorParameters<typeof SimTransport>[0]} opts
 * @returns {SimTransport}
 */
export function simTransport(opts) {
  return new SimTransport(opts);
}
