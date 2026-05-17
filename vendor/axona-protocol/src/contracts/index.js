// =====================================================================
// N-DHT contract surface — barrel export.
//
// Every other file in the codebase that depends on the contract should
// import from here, not from individual contract files. This gives us
// one stable import path even if individual files are split or merged.
//
// Example:
//   import { Transport, DHT, BootstrapService } from '../contracts/index.js';
//
// The contract files describe behavior; concrete implementations live
// elsewhere:
//
//   src/dht/SimulatedNetwork.js          implements Transport (sim)
//   src/transport/WebRTCTransport.js     implements Transport (production, future)
//   src/dht/neuromorphic/...DHT*.js      implement DHT
//   src/bootstrap/SimulatorBootstrap.js  implements BootstrapService (sim, future)
//   src/bootstrap/ProductionBootstrap.js implements BootstrapService (production, future)
// =====================================================================

export { Transport }        from './Transport.js';
export { DHT }              from './DHT.js';
export { BootstrapService } from './BootstrapService.js';
// types.js exports nothing at runtime — it carries JSDoc typedefs only,
// which other files import via /** @type {import('./types.js').X} */
// annotations.
