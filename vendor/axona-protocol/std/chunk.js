// =====================================================================
// @axona/protocol/std/chunk — reliable large-payload chunking over pub/sub.
//
// Part of `std`, Axona's standard library of app-layer helpers built ONLY on
// the public AxonaPeer API (pub/sub/pull) — no kernel internals. This module
// turns a byte array too large for a single publish into a set of self-
// describing messages and reassembles them byte-exactly, tolerating
// duplicates, reordering, and late/replay joiners.
//
// Design notes (informed by the stress campaign + a civildefense.io audit):
//   • SIZE (finding O-5): a single publish only reliably traverses the mesh
//     below the WebRTC-interoperable SCTP max message (~16 KB). So chunks are
//     sized so the ENVELOPED message stays under `maxMessageBytes` (default
//     16 KiB) — not the 256 KB kernel hard cap, which silently fails to deliver
//     on many paths.
//   • COMPLETION: done iff every distinct index 0..n-1 is present — NOT a count
//     of messages received. (civildefense's reassembler counted receipts, so a
//     duplicate could fire "done" over a hole → silent truncation.)
//   • RELIABLE PUBLISH (the reload-timeout fix): peer.pub is fire-and-forget into
//     the transport buffer, so a fast burst drops chunks before they cache and a
//     reload subscriber never reassembles. publishChunkedBytes() paces by default
//     AND verifies what the mesh cached, re-publishing gaps — so the durable set
//     is complete without the app guessing a throttle.
//   • NO SILENT HANG: receiveChunkedBytes() REJECTS on timeout (reporting the
//     missing indices) instead of awaiting forever. A lost chunk surfaces as an
//     error, never an indefinite wait.
//   • COUNT CAP (finding O-1): a topic's replay cache holds ~100 messages, so a
//     late/reload joiner can only reassemble files of ≲ (cacheSize-1) chunks.
//     At the default chunk size that's ~1.1 MB. publishChunkedBytes() throws if
//     a file would exceed that ceiling rather than produce an unrecoverable
//     transfer. (Larger files need a bigger per-topic cache or a live publisher.)
//   • GARBAGE: every message is tagged with the format marker + fileId; the
//     reassembler ignores anything that doesn't match the file it's collecting.
//
//   import { chunkBytes, createReassembler,
//            publishChunkedBytes, receiveChunkedBytes } from '@axona/protocol/std';
// =====================================================================

export const CHUNK_FORMAT = 1;                         // message format/version marker
export const DEFAULT_MAX_MESSAGE_BYTES = 15 * 1024;    // = kernel MAX_RELIABLE_PUBLISH_BYTES (interop floor − wrapper headroom, O-5)
const ENVELOPE_RESERVE = 1024;                         // headroom for the kernel signed envelope (sig/pubkey/msgId/topic)
                                                       // so the ENVELOPED frame stays < 16 KiB and never trips peer.pub's guard
const DEFAULT_REPLAY_CACHE = 1024;                     // kernel DEFAULT_REPLAY_CACHE_SIZE (O-1; raised 100→1024 in v2.47.0)

// ── base64 (browser AND node) ────────────────────────────────────────
const hasBuffer = typeof Buffer !== 'undefined';
function bytesToB64(u8) {
  if (hasBuffer) return Buffer.from(u8).toString('base64');
  let s = ''; const CH = 0x8000;
  for (let i = 0; i < u8.length; i += CH) s += String.fromCharCode.apply(null, u8.subarray(i, i + CH));
  return btoa(s);
}
function b64ToBytes(b64) {
  if (hasBuffer) return new Uint8Array(Buffer.from(b64, 'base64'));
  const s = atob(b64); const u8 = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) u8[i] = s.charCodeAt(i);
  return u8;
}
// Deterministic-enough id without crypto dependency (collision-irrelevant: it
// only namespaces one transfer's chunks on its own topic).
function randomFileId() {
  const c = (typeof crypto !== 'undefined') ? crypto : null;
  if (c?.randomUUID) return c.randomUUID();
  const b = (c?.getRandomValues) ? c.getRandomValues(new Uint8Array(16)) : null;
  let s = '';
  for (let i = 0; i < 16; i++) s += ((b ? b[i] : (i * 37 + 11)) & 0xff).toString(16).padStart(2, '0');
  return s;
}

/** Raw bytes per chunk so the base64 + JSON message stays under maxMessageBytes. */
export function rawChunkSize(maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES) {
  return Math.max(1, Math.floor((maxMessageBytes - ENVELOPE_RESERVE) * 3 / 4));
}

/**
 * Split bytes into self-describing messages: ONE manifest then N data chunks.
 *   manifest: { f:1, k:'m', id, n, size, name, mime, meta? }
 *   chunk:    { f:1, k:'c', id, i, n, d:<base64> }
 * Each carries `n` so completion is detectable even if the manifest is lost.
 * @param {Uint8Array} bytes
 * @returns {{ messages: object[], fileId: string, n: number }}
 */
export function chunkBytes(bytes, {
  name = 'file', mime = 'application/octet-stream', meta = null,
  maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES, fileId = null,
} = {}) {
  if (!(bytes instanceof Uint8Array)) bytes = new Uint8Array(bytes);
  const id = fileId || randomFileId();
  const raw = rawChunkSize(maxMessageBytes);
  const n = Math.max(1, Math.ceil(bytes.length / raw));
  const manifest = { f: CHUNK_FORMAT, k: 'm', id, n, size: bytes.length, name, mime };
  if (meta != null) manifest.meta = meta;
  const messages = [manifest];
  for (let i = 0; i < n; i++) {
    const slice = bytes.subarray(i * raw, Math.min(bytes.length, (i + 1) * raw));
    messages.push({ f: CHUNK_FORMAT, k: 'c', id, i, n, d: bytesToB64(slice) });
  }
  return { messages, fileId: id, n };
}

/** Encode a string to bytes (UTF-8) for chunking; pair with bytesToString. */
export function stringToBytes(str) { return new TextEncoder().encode(str); }
export function bytesToString(u8) { return new TextDecoder().decode(u8); }

/**
 * Collect chunk messages (any order, duplicates tolerated, garbage ignored).
 * Fires onComplete({ id, name, mime, size, bytes, meta }) ONCE per file when
 * every distinct index of that file is present.
 *
 * MULTI-FILE: a single topic commonly carries a STREAM of files over time — an
 * image channel, a chat with attachments. So one reassembler tracks every file
 * it sees, keyed by `fileId`, and fires onComplete once for EACH that completes.
 * (Earlier this locked onto the first file's id and silently dropped every later
 * file — a second image on a channel never reassembled for receivers.) Each
 * file's chunks are kept apart by id, so a foreign/garbage file can't corrupt
 * the one you want, and indices are matched per-file. Pass `fileId` to restrict
 * to a single file (receiveChunkedBytes does this implicitly via its own scope).
 *
 * Returns { accept(msg)->bool, missing(id?), have(id?), total(id?) }; the
 * introspectors default to the most-recently-touched file when no id is given.
 */
export function createReassembler(onComplete, { onProgress = null, fileId = null } = {}) {
  const only = fileId;                 // if set, accept ONLY this file id
  const files = new Map();             // id -> { n, meta, name, mime, size, slots:Map, fired }
  let last = null;                     // most-recently-touched id (for arg-less introspectors)
  const mk = () => ({ n: null, meta: null, name: 'file', mime: 'application/octet-stream', size: null, slots: new Map(), fired: false });
  function complete(id, f) {
    if (f.fired || f.n == null || f.slots.size !== f.n) return;
    let len = 0; for (let i = 0; i < f.n; i++) len += f.slots.get(i).length;
    const bytes = new Uint8Array(f.size != null ? f.size : len);
    let off = 0; for (let i = 0; i < f.n; i++) { const c = f.slots.get(i); bytes.set(c, off); off += c.length; }
    f.fired = true; f.slots = new Map();    // delivered → free the chunk bytes; keep n+fired so dups/replays are ignored
    onComplete({ id, name: f.name, mime: f.mime, size: bytes.length, bytes, meta: f.meta });
  }
  return {
    accept(msg) {
      if (!msg || msg.f !== CHUNK_FORMAT || typeof msg.id !== 'string') return false;
      if (only != null && msg.id !== only) return false;     // single-file scope
      let f = files.get(msg.id);
      if (!f) { f = mk(); files.set(msg.id, f); }
      last = msg.id;
      if (f.fired) return true;                              // already delivered this file → ignore further copies
      if (msg.k === 'm') {                                    // manifest: authoritative metadata for THIS file
        f.name = msg.name ?? f.name; f.mime = msg.mime ?? f.mime; f.size = msg.size ?? f.size; f.meta = msg.meta ?? f.meta;
        if (typeof msg.n === 'number') f.n = msg.n;
      } else if (msg.k === 'c' && typeof msg.i === 'number' && typeof msg.d === 'string') {
        if (typeof msg.n === 'number') f.n ??= msg.n;          // learn count even if the manifest never arrives
        if (!f.slots.has(msg.i)) f.slots.set(msg.i, b64ToBytes(msg.d));
      } else return false;
      if (onProgress) onProgress({ id: msg.id, have: f.slots.size, total: f.n });
      complete(msg.id, f);
      return true;
    },
    missing(id) { const f = files.get(id ?? last); return (!f || f.n == null) ? null : Array.from({ length: f.n }, (_, i) => i).filter(i => !f.slots.has(i)); },
    have(id)    { const f = files.get(id ?? last); return f ? f.slots.size : 0; },
    total(id)   { const f = files.get(id ?? last); return f ? f.n : null; },
  };
}

// ── peer-bound high-level helpers ────────────────────────────────────
const delay = (ms) => new Promise(r => setTimeout(r, ms));

// Default inter-chunk pacing. peer.pub is FIRE-AND-FORGET into the transport
// send buffer (it does not await the WebRTC/bridge drain), so publishing N chunks
// in a tight loop bursts the buffer and the mesh silently drops some before they
// reach the topic's replay cache — which a reload subscriber can then never
// reassemble (it times out). A small pace lets the buffer drain between chunks;
// the verify+repair pass below is the actual guarantee, this just cuts the work.
const DEFAULT_PUBLISH_THROTTLE_MS = 12;

/**
 * Chunk `bytes` and publish every message to a fresh topic, then VERIFY the mesh
 * actually cached them and REPAIR any gaps. Refuses files that would exceed the
 * replay-cache ceiling (O-1) so you never create a transfer a reload subscriber
 * can't reassemble.
 *
 * Reliability (the reason verify exists): peer.pub is fire-and-forget into the
 * transport buffer, so a fast burst can drop chunks before they durably cache —
 * leaving a hole only a reload subscriber discovers (it never reassembles). The
 * old default `throttleMs: 0` made this routine for any non-trivial chunk count,
 * with no value an app author could reliably pick. So we now (a) pace by default
 * and (b) read back what the mesh holds and re-publish the missing indices until
 * the cache is complete (or progress stalls). Re-publishing identical content is
 * free — the cache upserts by msgId and never double-delivers (kernel ≥3.3.1).
 *
 * `verify` defaults true and is BEST-EFFORT: it never throws and never makes a
 * transfer worse (it only ever re-publishes gaps it positively observes). It
 * briefly subscribes to `topic` to read it back and stops ONLY that read-back
 * handle when done, so it is SAFE to pre-subscribe the publishing peer to the
 * same topic — a persistent app subscription on this topic (e.g. a streaming
 * channel's reassembler) survives the verify pass untouched. (Set `verify:false`
 * only to skip the read-back entirely, e.g. a one-shot loopback test.)
 *
 * @returns {Promise<{ topic, fileId, n, msgIds: string[], repaired: number }>}
 */
export async function publishChunkedBytes(peer, bytes, {
  topic, signWith, name, mime, meta,
  maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES, throttleMs = DEFAULT_PUBLISH_THROTTLE_MS,
  cacheSize = DEFAULT_REPLAY_CACHE,
  verify = true, verifyRounds = 4, verifyWaitMs = 1500,
} = {}) {
  const { messages, fileId, n } = chunkBytes(bytes, { name, mime, meta, maxMessageBytes });
  if (messages.length > cacheSize) {
    throw new Error(`std/chunk: transfer needs ${messages.length} messages but the topic replay cache holds ~${cacheSize} ` +
      `(O-1). ${bytes.length} bytes at ~${rawChunkSize(maxMessageBytes)}B/chunk exceeds the reload-safe ceiling ` +
      `(~${((cacheSize - 1) * rawChunkSize(maxMessageBytes) / 1024 / 1024).toFixed(1)} MB). Downsample, or keep the publisher online.`);
  }
  const msgIds = [];
  const pub = async (m) => { const id = await peer.pub(topic, m, { signWith }); msgIds.push(id); if (throttleMs) await delay(throttleMs); return id; };
  for (const m of messages) await pub(m);              // manifest [0] first, then chunks [1..n]

  // Verify + repair: read back what actually cached, re-publish gaps. Best-effort.
  let repaired = 0;
  if (verify && typeof peer.sub === 'function') {
    for (let round = 0; round < verifyRounds; round++) {
      let have;
      try { have = await _cachedChunkSet(peer, topic, fileId, n, verifyWaitMs); }
      catch { break; }                                  // read-back failed → leave the initial publish as-is
      const gaps = [];
      if (!have.has('m')) gaps.push(0);                 // manifest is messages[0]
      for (let i = 0; i < n; i++) if (!have.has(i)) gaps.push(i + 1);  // chunk i is messages[i+1]
      if (gaps.length === 0) break;                     // fully cached — done
      if (round === verifyRounds - 1) break;            // last look was for detection only; don't loop forever
      for (const gi of gaps) { await pub(messages[gi]); repaired++; }
    }
  }
  return { topic, fileId, n, msgIds, repaired };
}

/**
 * Briefly subscribe to `topic` (since:'all') and collect which chunk indices of
 * `fileId` the mesh currently holds — 'm' for the manifest, integers for chunks.
 * Used by publishChunkedBytes to find gaps to repair. Always unsubscribes.
 */
async function _cachedChunkSet(peer, topic, fileId, n, waitMs) {
  const have = new Set();
  const complete = () => { if (!have.has('m')) return false; for (let i = 0; i < n; i++) if (!have.has(i)) return false; return true; };
  let handle = null;
  try {
    handle = await peer.sub(topic, (env) => {
      if (!env || env.deleted) return;
      const m = env.message;
      if (!m || m.f !== CHUNK_FORMAT || m.id !== fileId) return;
      if (m.k === 'm') have.add('m');
      else if (m.k === 'c' && typeof m.i === 'number') have.add(m.i);
    }, { since: 'all' });
    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline && !complete()) await delay(60);
  } finally {
    // Stop ONLY this read-back subscription's own handle — NOT peer.unsub(topic),
    // which stops EVERY subscription on the topic and would tear down a caller's
    // pre-existing persistent subscription (e.g. an app holding a live channel
    // reassembler on the same topic it publishes to — axona-share). sub.stop()
    // removes just this handle; the network unsubscribe only fires if it was the
    // last sub on the topic. Fall back to unsub only if no handle was returned.
    try { if (handle && typeof handle.stop === 'function') await handle.stop(); else await peer.unsub?.(topic); } catch { /* */ }
  }
  return have;
}

/**
 * Subscribe to a chunk topic and reassemble. NEVER hangs: resolves with the file
 * on completion, else REJECTS after timeoutMs with the missing indices. Always
 * unsubscribes before settling. (Completeness of the durable set is ensured on
 * the PUBLISH side by publishChunkedBytes's verify+repair — a reload subscriber
 * relies on every chunk being cached; the receiver cannot conjure a chunk the
 * mesh never stored.)
 * @returns {Promise<{ bytes, name, mime, size, meta, id }>}
 */
export async function receiveChunkedBytes(peer, topic, {
  timeoutMs = 30000, onProgress = null,
} = {}) {
  return new Promise((resolve, reject) => {
    let settled = false, timer = null, handle = null;
    const reassembler = createReassembler((file) => finish(null, file), { onProgress });
    const finish = async (err, file) => {
      if (settled) return; settled = true;
      if (timer) clearTimeout(timer);
      // Stop only our own handle, never peer.unsub(topic) — see _cachedChunkSet.
      try { if (handle && typeof handle.stop === 'function') await handle.stop(); else await peer.unsub?.(topic); } catch { /* */ }
      err ? reject(err) : resolve(file);
    };
    timer = setTimeout(() => {
      const miss = reassembler.missing();
      finish(new Error(`std/chunk: reassembly timed out after ${timeoutMs}ms — ` +
        `have ${reassembler.have()}/${reassembler.total() ?? '?'}, missing indices [${(miss || ['unknown']).slice(0, 20).join(',')}]`));
    }, timeoutMs);
    peer.sub(topic, (envelope) => {
      if (!envelope || envelope.deleted) return;
      reassembler.accept(envelope.message);
    }, { since: 'all' }).then((h) => { handle = h; if (settled) { try { h?.stop?.(); } catch { /* */ } } }).catch((e) => finish(e));
  });
}

export const _internals = { bytesToB64, b64ToBytes, rawChunkSize, ENVELOPE_RESERVE };
