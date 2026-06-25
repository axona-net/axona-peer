// =====================================================================
// std/message.js — the canonical application-message convention for Axona
// pub/sub bodies.
//
// `peer.pub(topic, body, { signWith })` accepts ANY JSON-serialisable `body`,
// and `peer.sub(topic, env => …)` hands it back verbatim as `env.message`. That
// flexibility is good for the kernel but bad for INTEROP: if every app invents
// its own body shape, App A renders App B's message as "[object Object]" (or
// crashes string-parsing an object). Every Axona reference/example app — and,
// by convention, every app built on the protocol — uses THESE two helpers so
// any app renders any other app's messages.
//
//   publish:  peer.pub(topic, makeMessage(text, { …extra }), { signWith })
//   render:   const text = readMessage(env.message)
//
// CANONICAL BODY SHAPE
//   { v: 1, text: <string>, ...appExtras }
//   - `text` is the human-readable body (the one field every app displays).
//   - `v` is a format marker for forward-compat.
//   - apps MAY attach extra fields (e.g. a node id for region display); other
//     apps ignore them.
//   The SENDER is NOT carried in the body — it is the envelope's authenticated
//   `signerPubkey` (use readSender(env)). Don't encode the sender into `text`.
//
// readMessage() is deliberately TOLERANT (Postel's law): it also accepts a bare
// string, a `{ message }` object, or any other object (shown as JSON) so legacy
// or third-party publishers still display, never as "[object Object]".
// =====================================================================

export const MESSAGE_FORMAT = 1;

/**
 * Build a canonical pub/sub message body.
 * @param {string} text                 human-readable body
 * @param {object} [extra]              app-specific extra fields (merged in)
 * @returns {{ v: number, text: string }}
 */
export function makeMessage(text, extra = {}) {
  return { v: MESSAGE_FORMAT, text: String(text ?? ''), ...extra };
}

/**
 * Extract the display text from ANY received message body — canonical object,
 * a `{ message }` object, a bare string, or an arbitrary object (→ JSON). Never
 * returns "[object Object]".
 * @param {unknown} body  env.message as delivered
 * @returns {string}
 */
export function readMessage(body) {
  if (body == null) return '';
  if (typeof body === 'string') return body;
  if (typeof body === 'object') {
    if (typeof body.text === 'string')    return body.text;
    if (typeof body.message === 'string') return body.message;
    try { return JSON.stringify(body); } catch { return String(body); }
  }
  return String(body);
}

/**
 * Canonical short sender label from a delivered envelope. The authenticated
 * author is `env.signerPubkey` (location-free Author ID); fall back to a body
 * `node`/`from` hint, then "(unknown)". Apps decide how to present it.
 * @param {{ signerPubkey?: string, message?: any }} env
 * @param {number} [len=8]  hex chars to show
 * @returns {string}
 */
export function readSender(env, len = 8) {
  const key = env?.signerPubkey;
  if (typeof key === 'string' && key.length) return key.slice(0, len);
  const hint = env?.message && typeof env.message === 'object'
    ? (env.message.node ?? env.message.from) : null;
  if (typeof hint === 'string' && hint.length) return hint.slice(0, len);
  return '(unknown)';
}

export default { MESSAGE_FORMAT, makeMessage, readMessage, readSender };
