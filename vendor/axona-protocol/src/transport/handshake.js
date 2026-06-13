// =====================================================================
// handshake.js — bidirectional version handshake for transport channels.
//
// Wire frames (JSON, sent on the signaling channel before any Axona
// payload):
//
//   peer → bridge:
//     { type: 'client-hello',
//       version:      '1.0.0-rc.0',     // semver of the peer/bridge code
//       wireVersion:  '1.0',            // semver-ish of the wire format
//       capabilities: ['pubsub', ...]   // optional list of features
//     }
//
//   bridge → peer:
//     { type: 'server-hello',
//       version:        '1.0.0-rc.0',
//       wireVersion:    '1.0',
//       minPeerVersion: '1.0.0-rc.0',   // peer must be at least this
//       downloadUrl:    'https://axona.net'  // optional, surfaced
//                                            // to apps via the
//                                            // UpgradeRequired event
//     }
//
// Either side closes the WebSocket with code 4426 ("Upgrade Required")
// if the versions are incompatible and throws/emits an
// UpgradeRequiredError carrying { reason, serverVersion, clientVersion,
// minPeerVersion, downloadUrl }.
//
// Compatibility rule:
//   - wireVersion must match at the major level (peer.wire major ==
//     server.wire major).  Minor differences are accepted — newer
//     side downgrades to the older minor.
//   - peer.version must be >= server.minPeerVersion.
//
// This module is transport-agnostic.  The actual sending / receiving
// is handed in via callbacks so the same code runs over a Node
// WebSocket, a browser WebSocket, an in-process pipe, etc.
// =====================================================================

import { UpgradeRequiredError } from '../errors.js';

// ── Constants ──────────────────────────────────────────────────────

/** The wire-format version this build of the kernel speaks. Major is the
 *  hard-compat axis (see wireCompatible): a major bump partitions the
 *  peer↔bridge handshake. Bumped 1.0 → 2.0 for the 2026-06 wire flag-day
 *  (paired with the AUTH_PROTO axona/4 → axona/5 bump, which is the
 *  load-bearing peer-to-peer partition). The web client now sends this in its
 *  client-hello so the bridge gate can reject a mismatched major early. */
export const WIRE_VERSION = '2.0';

/** The kernel's own peer-version string.  Apps wrapping the kernel
 *  pass their own version through; this is just the default.  Kept on the 2.x
 *  line (NOT bumped to 3.x) so the bridge's major-version namespace gate
 *  (kernel-2.x vs peer-app-3.x in flagDayFloor) still classifies it correctly;
 *  the partition rides on WIRE_VERSION + AUTH_PROTO, not this semver. */
export const KERNEL_VERSION = '2.40.1';

/** WebSocket close code for version mismatches (custom, in the
 *  application-specific 4000-4999 range). */
export const UPGRADE_CLOSE_CODE = 4426;

const HANDSHAKE_TIMEOUT_MS = 10_000;

// ── Frame builders ─────────────────────────────────────────────────

/**
 * @param {{ version: string, wireVersion?: string, capabilities?: string[] }} opts
 */
export function buildClientHello({ version, wireVersion = WIRE_VERSION, capabilities = [] }) {
  return {
    type: 'client-hello',
    version,
    wireVersion,
    capabilities: [...capabilities],
  };
}

/**
 * @param {{ version: string, wireVersion?: string, minPeerVersion: string, downloadUrl?: string }} opts
 */
export function buildServerHello({ version, wireVersion = WIRE_VERSION, minPeerVersion, downloadUrl }) {
  const out = {
    type: 'server-hello',
    version,
    wireVersion,
    minPeerVersion,
  };
  if (downloadUrl) out.downloadUrl = downloadUrl;
  return out;
}

/**
 * Recognise a hello frame. Returns the frame if its `type` matches one
 * of `client-hello` / `server-hello`, otherwise null.
 * @param {unknown} frame
 * @returns {object | null}
 */
export function parseHello(frame) {
  if (!frame || typeof frame !== 'object') return null;
  if (frame.type !== 'client-hello' && frame.type !== 'server-hello') return null;
  if (typeof frame.version !== 'string')     return null;
  if (typeof frame.wireVersion !== 'string') return null;
  return frame;
}

// ── Compatibility checks ───────────────────────────────────────────

/**
 * Parse a semver-ish version string into a comparable tuple.  Accepts
 * `1.2.3`, `1.2.3-rc.0`, `1.2.3-beta.4+meta`. Prerelease tags compare
 * before the corresponding stable release.
 *
 * @param {string} s
 * @returns {{ major: number, minor: number, patch: number, pre: string }}
 */
export function parseVersion(s) {
  if (typeof s !== 'string') {
    throw new TypeError(`parseVersion: expected string, got ${typeof s}`);
  }
  const m = s.match(/^(\d+)\.(\d+)\.(\d+)(?:[-+](.+))?$/);
  if (!m) throw new RangeError(`parseVersion: not a semver: ${s}`);
  return {
    major: parseInt(m[1], 10),
    minor: parseInt(m[2], 10),
    patch: parseInt(m[3], 10),
    pre:   m[4] ?? '',
  };
}

/**
 * Compare two semver strings.  Returns -1 / 0 / +1 in the
 * traditional sense.  Prereleases compare LESS than the same
 * major.minor.patch without a prerelease tag.
 *
 * @param {string} a
 * @param {string} b
 * @returns {number}
 */
export function compareVersions(a, b) {
  const A = parseVersion(a);
  const B = parseVersion(b);
  if (A.major !== B.major) return A.major - B.major < 0 ? -1 : 1;
  if (A.minor !== B.minor) return A.minor - B.minor < 0 ? -1 : 1;
  if (A.patch !== B.patch) return A.patch - B.patch < 0 ? -1 : 1;
  // Stable beats prerelease.
  if (A.pre === '' && B.pre !== '') return  1;
  if (A.pre !== '' && B.pre === '') return -1;
  if (A.pre < B.pre) return -1;
  if (A.pre > B.pre) return  1;
  return 0;
}

/**
 * Wire-version compatibility: same major.  '1.0' and '1.7' are
 * compatible; '1.x' and '2.x' are not.
 *
 * @param {string} a
 * @param {string} b
 * @returns {boolean}
 */
export function wireCompatible(a, b) {
  // wireVersion is major.minor (not full semver — no patch field).
  const am = String(a).split('.')[0];
  const bm = String(b).split('.')[0];
  return am === bm;
}

// ── Handshake runners ─────────────────────────────────────────────

/**
 * Run the client side of the handshake: send `client-hello`, await
 * the matching `server-hello`, validate compatibility.
 *
 * @param {object} opts
 * @param {string} opts.version             this peer's semver
 * @param {(frame: object) => void}    opts.sendFrame
 *        Synchronous send hook.  Throws if the channel is closed.
 * @param {() => Promise<object>}      opts.awaitServerHello
 *        Resolve with the server-hello frame when it arrives on the
 *        same channel; reject if the channel closes first.
 * @param {string[]}                   [opts.capabilities]
 * @param {string}                     [opts.wireVersion]
 * @param {number}                     [opts.timeoutMs]
 * @returns {Promise<{ serverHello: object }>}
 *
 * @throws {UpgradeRequiredError}
 */
export async function performClientHandshake(opts) {
  const {
    version,
    sendFrame,
    awaitServerHello,
    capabilities = [],
    wireVersion = WIRE_VERSION,
    timeoutMs   = HANDSHAKE_TIMEOUT_MS,
  } = opts;

  sendFrame(buildClientHello({ version, wireVersion, capabilities }));

  const serverHello = await withTimeout(awaitServerHello(), timeoutMs, 'server-hello timeout');
  const parsed = parseHello(serverHello);
  if (!parsed || parsed.type !== 'server-hello') {
    throw new UpgradeRequiredError(
      'expected server-hello, got something else',
      { context: { reason: 'malformed_server_hello', received: serverHello } },
    );
  }

  // Wire version mismatch.
  if (!wireCompatible(wireVersion, parsed.wireVersion)) {
    throw new UpgradeRequiredError(
      `wire-version mismatch: client=${wireVersion} server=${parsed.wireVersion}`,
      { context: {
          reason:         'wire_version_mismatch',
          clientVersion:  version,
          serverVersion:  parsed.version,
          downloadUrl:    parsed.downloadUrl,
        } },
    );
  }

  // Peer too old for this server.
  if (compareVersions(version, parsed.minPeerVersion) < 0) {
    throw new UpgradeRequiredError(
      `peer version ${version} is older than server minPeerVersion ${parsed.minPeerVersion}`,
      { context: {
          reason:         'peer_too_old',
          clientVersion:  version,
          serverVersion:  parsed.version,
          minPeerVersion: parsed.minPeerVersion,
          downloadUrl:    parsed.downloadUrl,
        } },
    );
  }

  return { serverHello: parsed };
}

/**
 * Run the server side of the handshake: await `client-hello`, decide
 * whether to admit, send `server-hello` either way.  Returns the
 * client-hello if accepted, throws UpgradeRequiredError if rejected.
 *
 * The caller decides what to do with the rejection (typically: close
 * the underlying WS with UPGRADE_CLOSE_CODE).
 *
 * @param {object} opts
 * @param {string} opts.version             this server's semver
 * @param {string} opts.minPeerVersion      minimum peer semver
 * @param {(frame: object) => void}    opts.sendFrame
 * @param {() => Promise<object>}      opts.awaitClientHello
 * @param {string}                     [opts.wireVersion]
 * @param {string}                     [opts.downloadUrl]
 * @param {number}                     [opts.timeoutMs]
 * @returns {Promise<{ clientHello: object }>}
 *
 * @throws {UpgradeRequiredError}
 */
export async function performServerHandshake(opts) {
  const {
    version,
    minPeerVersion,
    sendFrame,
    awaitClientHello,
    wireVersion = WIRE_VERSION,
    downloadUrl,
    timeoutMs   = HANDSHAKE_TIMEOUT_MS,
  } = opts;

  const clientHello = await withTimeout(awaitClientHello(), timeoutMs, 'client-hello timeout');
  const parsed = parseHello(clientHello);
  if (!parsed || parsed.type !== 'client-hello') {
    // Still send a server-hello + close — the caller closes the
    // underlying socket; we just signal what we expect.
    const reject = buildServerHello({ version, wireVersion, minPeerVersion, downloadUrl });
    try { sendFrame(reject); } catch { /* socket dying */ }
    throw new UpgradeRequiredError(
      'expected client-hello, got something else',
      { context: { reason: 'malformed_client_hello', received: clientHello, downloadUrl } },
    );
  }

  // Build the server-hello — sent regardless of admit/reject so the
  // client knows what it should be running.
  const reply = buildServerHello({ version, wireVersion, minPeerVersion, downloadUrl });
  sendFrame(reply);

  if (!wireCompatible(wireVersion, parsed.wireVersion)) {
    throw new UpgradeRequiredError(
      `wire-version mismatch: server=${wireVersion} client=${parsed.wireVersion}`,
      { context: {
          reason:         'wire_version_mismatch',
          clientVersion:  parsed.version,
          serverVersion:  version,
          downloadUrl,
        } },
    );
  }
  if (compareVersions(parsed.version, minPeerVersion) < 0) {
    throw new UpgradeRequiredError(
      `peer version ${parsed.version} is older than minPeerVersion ${minPeerVersion}`,
      { context: {
          reason:         'peer_too_old',
          clientVersion:  parsed.version,
          serverVersion:  version,
          minPeerVersion,
          downloadUrl,
        } },
    );
  }

  return { clientHello: parsed };
}

// ── helpers ────────────────────────────────────────────────────────

function withTimeout(promise, ms, message) {
  if (ms <= 0) return promise;
  return new Promise((resolve, reject) => {
    // Note: NOT unref'd — the handshake timer is the only thing
    // keeping the event loop alive while we wait for the remote
    // hello.  An unref'd timer would let the process exit cleanly
    // before the timeout fires, which silently masks the error.
    const t = setTimeout(() => {
      reject(new UpgradeRequiredError(message,
        { context: { reason: 'handshake_timeout', timeoutMs: ms } }));
    }, ms);
    promise.then(v => { clearTimeout(t); resolve(v); },
                 e => { clearTimeout(t); reject(e); });
  });
}
