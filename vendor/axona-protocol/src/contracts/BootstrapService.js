// =====================================================================
// BootstrapService — provides the very first peer connection when a
// node joins the network.
//
// Production implementations handle:
//   - WebSocket signaling against a known rendezvous server
//   - Manifest fetch and signature verification
//   - WebRTC offer/answer negotiation
//   - QR-code-based pairing
//
// Simulator implementation hands back an in-process reference.
//
// After `bootstrap()` returns, the protocol's normal Transport-mediated
// routing takes over. The BootstrapService is not consulted again until
// the node leaves and rejoins.
// =====================================================================

/* eslint-disable no-unused-vars */

/**
 * @abstract
 */
export class BootstrapService {
  /**
   * Open the initial peer connection through the given sponsor
   * endpoint. Resolves with the sponsor's NodeId and a started
   * Transport whose channel pool already contains a live connection
   * to the sponsor.
   *
   * After this resolves, the protocol's `bootstrapJoin` (or
   * equivalent) issues normal `transport.send` calls to discover more
   * peers and populate the synaptome via stratified bootstrap.
   *
   * @param {import('./types.js').BootstrapEndpoint} sponsor
   * @returns {Promise<{
   *   sponsorId: bigint,
   *   transport: import('./Transport.js').Transport
   * }>}
   */
  async bootstrap(sponsor) {
    throw new Error('BootstrapService.bootstrap: not implemented');
  }
}
