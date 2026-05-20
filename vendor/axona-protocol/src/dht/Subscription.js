// =====================================================================
// Subscription — handle returned by AxonaPeer.sub().
//
// Apps treat this as opaque: store it, call .stop() when done.  The
// handle carries enough state for the peer to route incoming
// deliveries to the right handler and to clean up cleanly when the
// subscription is cancelled.
// =====================================================================

let _nextSubId = 1;

export class Subscription {
  /**
   * @param {object}   opts
   * @param {object}   opts.peer       the AxonaPeer that owns this sub
   * @param {string}   opts.topicId    66-char hex topic ID
   * @param {string}   opts.topicName  the application-level topic string
   * @param {(envelope: object) => void} opts.handler
   * @param {object}   [opts.opts]     the original {since, ...} options
   */
  constructor({ peer, topicId, topicName, handler, opts = {} }) {
    this._peer       = peer;
    this._topicId    = topicId;
    this._topicName  = topicName;
    this._handler    = handler;
    this._opts       = opts;
    this._id         = `sub-${_nextSubId++}`;
    this._stopped    = false;
  }

  get id()        { return this._id; }
  get topicId()   { return this._topicId; }
  get topicName() { return this._topicName; }
  get stopped()   { return this._stopped; }

  /**
   * Cancel this subscription.  Idempotent.
   * @returns {Promise<void>}
   */
  async stop() {
    if (this._stopped) return;
    this._stopped = true;
    await this._peer._unsubscribeInternal(this);
  }

  /** @internal — invoked by AxonaPeer's dispatch */
  _deliver(envelope) {
    if (this._stopped) return;
    try { this._handler(envelope); }
    catch { /* handler errors are app-level; don't propagate to dispatch */ }
  }
}
