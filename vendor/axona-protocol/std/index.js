// =====================================================================
// @axona/protocol/std — Axona's standard library of app-layer helpers.
//
// Utilities built ONLY on the public AxonaPeer API (no kernel internals),
// shipped with the package so every consumer shares one tested implementation
// instead of re-inventing them. Loosely the role C's stdio/stdlib plays:
// common, optional, sits beside the core rather than inside it.
//
//   import { chunkBytes, receiveChunkedBytes } from '@axona/protocol/std';
//   import { chunkBytes } from '@axona/protocol/std/chunk';   // or per-module
//
// Modules:
//   chunk     — reliable large-payload chunking + reassembly over pub/sub.
//   message   — canonical pub/sub message body convention (makeMessage /
//               readMessage / readSender) so every app renders every app's
//               messages. ALL apps publish + render through these.
//   (image downsampling and other helpers will land here as sibling modules.)
//
// Note: the derived metric-topic helpers (metricTopic/isMetricTopic) live in
// CORE (`@axona/protocol`, src/pubsub/metrics.js), not here — they are a protocol
// convention that infrastructure roots must compute identically, and infra
// vendors src/ only. Import them from '@axona/protocol'.
// =====================================================================
export * from './chunk.js';
