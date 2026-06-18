// =====================================================================
// errors.js — Typed error classes for the Axona protocol kernel.
//
// Every kernel throw site uses one of these classes (or a subclass).
// Callers can switch on `err.code` for programmatic handling without
// parsing error messages.  The full hierarchy is rooted at AxonaError
// so apps can `catch (err) { if (err instanceof AxonaError) ... }`.
//
// Codes are stable identifiers, not human-readable strings.  Messages
// are human-readable.  Context is an optional dictionary carrying
// machine-readable details (e.g. { peerId, topic } for routing errors,
// { expected, actual } for version mismatches).
//
// Wire compatibility: typed errors survive a roundtrip via
// `toWire(err)` / `fromWire(obj)` so a remote peer can throw the same
// class the local peer would have thrown, with the same code and
// context.  This matters for `send()` RPCs where the remote handler
// may signal failure via an error rather than a value.
// =====================================================================

/**
 * Base class for all errors thrown by @axona/protocol.
 *
 * @property {string} code     Stable identifier (UPPER_SNAKE_CASE).
 *                             Apps switch on this, not on message text.
 * @property {Error?} cause    Original error if this wraps another.
 * @property {object} context  Machine-readable details (optional).
 */
export class AxonaError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {object} [opts]
   * @param {Error}  [opts.cause]
   * @param {object} [opts.context]
   */
  constructor(code, message, opts = {}) {
    super(message);
    this.name    = this.constructor.name;
    this.code    = code;
    this.cause   = opts.cause   ?? undefined;
    this.context = opts.context ?? {};
  }

  /**
   * Serialize for the wire (e.g. a `send()` RPC reply).
   * Loses the .cause chain — only the immediate error is sent.
   */
  toWire() {
    return {
      __axonaError: true,
      class:   this.name,
      code:    this.code,
      message: this.message,
      context: this.context,
    };
  }
}

/**
 * Identity creation or load failed (key generation, persistence,
 * malformed nodeId, etc).
 */
export class IdentityError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Transport failure — channel didn't open, peer unreachable, message
 * timed out, etc.  Subclass of AxonaError so consumers can catch
 * specifically or fall through to AxonaError.
 */
export class TransportError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Publish failed — replication didn't reach K-closest, topic ID was
 * invalid, payload too large, signing failed, etc.
 */
export class PublishError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Subscribe failed — topic ID invalid, handler missing, attach to
 * axon role failed, etc.
 */
export class SubscribeError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Kill (creator-only message retraction) failed — no identity to sign the
 * kill, malformed msgId/topic, etc.  Note: a kill the network simply can't
 * authorize (the message isn't ours, or already gone) is NOT an error —
 * `peer.kill()` resolves with `{ ok: false }` in that case.  KillError is
 * for local/programmer faults only.
 */
export class KillError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Unpub (owner-only topic-queue removal) failed — no identity to sign, an
 * ownerless (public) topic that has no owner to prove, etc.  As with kill,
 * a network that simply can't authorize the unpub is not a local error;
 * UnpubError is for local/programmer faults only.
 */
export class UnpubError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Touch (creator-only keep-alive) failed — no identity to sign, a malformed
 * msgId, etc.  Like kill, a touch must be signed by the message creator; a
 * network that simply can't authorize the touch is not a local error.
 * TouchError is for local/programmer faults only.
 */
export class TouchError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Pull failed — msgId not in cache window, K-closest set unreachable,
 * malformed msgId, etc.  Note: cache-miss for a msgId older than the
 * replay window is NOT an error — `pull()` returns null in that case.
 * PullError is for unexpected failures only.
 */
export class PullError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Metrics query failed — K-closest unreachable, topic unknown, etc.
 */
export class MetricsError extends AxonaError {
  constructor(code, message, opts) { super(code, message, opts); }
}

/**
 * Wire-version handshake mismatch.  The bridge requires a newer peer
 * than this client, or vice versa.  context carries:
 *   { reason, serverVersion, clientVersion, downloadUrl? }
 * Apps surface this to the user (e.g. "please update axona.net").
 */
export class UpgradeRequiredError extends AxonaError {
  constructor(message, opts) {
    super('UPGRADE_REQUIRED', message, opts);
  }
}

// ── Stable error codes ──────────────────────────────────────────────
// Listing these as named constants lets apps reference them without
// stringly-typed lookups, and surfaces the full taxonomy in one place.

export const ErrorCodes = Object.freeze({
  // Identity
  IDENTITY_KEYGEN_FAILED:    'IDENTITY_KEYGEN_FAILED',
  IDENTITY_LOAD_FAILED:      'IDENTITY_LOAD_FAILED',
  IDENTITY_INVALID_FORMAT:   'IDENTITY_INVALID_FORMAT',

  // Transport
  TRANSPORT_NOT_STARTED:     'TRANSPORT_NOT_STARTED',
  TRANSPORT_PEER_UNREACHABLE:'TRANSPORT_PEER_UNREACHABLE',
  TRANSPORT_TIMEOUT:         'TRANSPORT_TIMEOUT',
  TRANSPORT_CHANNEL_CLOSED:  'TRANSPORT_CHANNEL_CLOSED',
  TRANSPORT_HELLO_FAILED:    'TRANSPORT_HELLO_FAILED',

  // Publish
  PUBLISH_INVALID_TOPIC:     'PUBLISH_INVALID_TOPIC',
  PUBLISH_SIGN_FAILED:       'PUBLISH_SIGN_FAILED',
  PUBLISH_NO_PUBLISH_IDENTITY:'PUBLISH_NO_PUBLISH_IDENTITY',   // signed publish with no signer named (transport key must not sign)
  TOPIC_REGION_REQUIRED:     'TOPIC_REGION_REQUIRED',          // open topic with no region (no global region exists)
  WRITE_POLICY_VIOLATION:    'WRITE_POLICY_VIOLATION',         // publish to an owner-only topic by a non-owner key
  PUBLISH_REPLICATION_FAILED:'PUBLISH_REPLICATION_FAILED',
  PUBLISH_PAYLOAD_TOO_LARGE: 'PUBLISH_PAYLOAD_TOO_LARGE',
  PUBLISH_INVALID_MESSAGE:   'PUBLISH_INVALID_MESSAGE',

  // Subscribe
  SUBSCRIBE_INVALID_TOPIC:   'SUBSCRIBE_INVALID_TOPIC',
  SUBSCRIBE_ATTACH_FAILED:   'SUBSCRIBE_ATTACH_FAILED',
  SUBSCRIBE_HANDLER_MISSING: 'SUBSCRIBE_HANDLER_MISSING',

  // Kill (creator-only retraction)
  KILL_INVALID_TOPIC:        'KILL_INVALID_TOPIC',
  KILL_INVALID_MSGID:        'KILL_INVALID_MSGID',
  KILL_SIGN_FAILED:          'KILL_SIGN_FAILED',

  // Unpub (owner-only queue removal)
  UNPUB_INVALID_TOPIC:       'UNPUB_INVALID_TOPIC',
  UNPUB_PUBLIC_TOPIC:        'UNPUB_PUBLIC_TOPIC',
  UNPUB_SIGN_FAILED:         'UNPUB_SIGN_FAILED',

  TOUCH_INVALID_TOPIC:       'TOUCH_INVALID_TOPIC',
  TOUCH_INVALID_MSGID:       'TOUCH_INVALID_MSGID',
  TOUCH_SIGN_FAILED:         'TOUCH_SIGN_FAILED',

  // Pull
  PULL_INVALID_MSGID:        'PULL_INVALID_MSGID',
  PULL_AXONS_UNREACHABLE:    'PULL_AXONS_UNREACHABLE',

  // Metrics
  METRICS_AXONS_UNREACHABLE: 'METRICS_AXONS_UNREACHABLE',

  // Upgrade
  UPGRADE_REQUIRED:          'UPGRADE_REQUIRED',
});

// ── Wire round-trip helpers ──────────────────────────────────────────

const CLASS_REGISTRY = {
  AxonaError,
  IdentityError,
  TransportError,
  PublishError,
  SubscribeError,
  KillError,
  UnpubError,
  PullError,
  MetricsError,
  UpgradeRequiredError,
};

/**
 * Detect a wire-encoded AxonaError envelope.
 * @param {unknown} obj
 * @returns {boolean}
 */
export function isWireError(obj) {
  return obj && typeof obj === 'object' && obj.__axonaError === true &&
         typeof obj.class === 'string' && typeof obj.code === 'string';
}

/**
 * Reconstruct a typed error from its wire envelope. Unknown error
 * classes fall back to plain AxonaError so a forward-compatible peer
 * still sees the code and context.
 *
 * @param {object} obj  Output of `err.toWire()`.
 * @returns {AxonaError}
 */
export function fromWire(obj) {
  if (!isWireError(obj)) {
    throw new TypeError('fromWire: not a wire-encoded AxonaError');
  }
  const Ctor = CLASS_REGISTRY[obj.class] ?? AxonaError;
  // UpgradeRequiredError has a fixed code; other classes take (code, message).
  if (Ctor === UpgradeRequiredError) {
    return new UpgradeRequiredError(obj.message, { context: obj.context });
  }
  return new Ctor(obj.code, obj.message, { context: obj.context });
}
