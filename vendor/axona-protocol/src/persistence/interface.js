// =====================================================================
// PersistenceAdapter — abstract interface for state persistence.
//
// Three concrete implementations ship with the kernel:
//   - InMemoryPersistence   (this file)              — for tests
//   - IndexedDBPersistence  (src/persistence/indexeddb.js) — for browsers
//   - FilePersistence       (src/persistence/file.js)      — for Node
//
// Apps can also supply their own adapter (e.g. encrypted-at-rest,
// synced via a different channel) — the kernel only depends on the
// five methods defined here.
//
// All methods are async.  Values are JSON-serializable; the wire
// codec for BigInts (`"<digits>n"` strings) and Sets (arrays) is
// applied by the kernel before calling save().
// =====================================================================

import { AxonaError, ErrorCodes } from '../errors.js';

/**
 * Abstract persistence adapter.  Subclasses must override every
 * method; calling a base method throws.  The five methods cover the
 * full lifecycle a kernel module needs:
 *
 *   load(key)               → snapshot at start
 *   save(key, value)        → checkpoint as state changes
 *   delete(key)             → forget a namespace (e.g. on identity reset)
 *   transaction(keys, fn)   → atomic multi-key read/modify/write
 *   close()                 → final flush + cleanup on shutdown
 *
 * @abstract
 */
export class PersistenceAdapter {
  /**
   * Load the value for `key`, or `undefined` if not present.
   *
   * @param {string} _key
   * @returns {Promise<unknown>}
   */
  async load(_key) {
    throw new AxonaError('NOT_IMPLEMENTED',
      'PersistenceAdapter.load: subclass must override');
  }

  /**
   * Save `value` under `key`.  Overwrites any existing value.
   * Implementations should be atomic — a crash mid-save must not
   * leave a partial value visible to a future load().
   *
   * @param {string}  _key
   * @param {unknown} _value
   * @returns {Promise<void>}
   */
  async save(_key, _value) {
    throw new AxonaError('NOT_IMPLEMENTED',
      'PersistenceAdapter.save: subclass must override');
  }

  /**
   * Delete the value for `key`.  No-op if not present.
   *
   * @param {string} _key
   * @returns {Promise<void>}
   */
  async delete(_key) {
    throw new AxonaError('NOT_IMPLEMENTED',
      'PersistenceAdapter.delete: subclass must override');
  }

  /**
   * Atomic read-modify-write across multiple keys.  Implementations
   * call `fn` with an object mapping key→currentValue; the returned
   * object (same keys, new values) is saved atomically.  A returned
   * `undefined` for a key deletes it.
   *
   * In InMemoryPersistence this is implemented as a synchronous lock
   * around load/save.  In IndexedDB it uses a readwrite transaction.
   * In File it uses a per-namespace lock plus the atomic-rename
   * pattern.
   *
   * @param {string[]} _keys
   * @param {(entries: Record<string, unknown>) => Promise<Record<string, unknown>> | Record<string, unknown>} _fn
   * @returns {Promise<void>}
   */
  async transaction(_keys, _fn) {
    throw new AxonaError('NOT_IMPLEMENTED',
      'PersistenceAdapter.transaction: subclass must override');
  }

  /**
   * Final flush + cleanup. After close(), all other methods throw.
   * Idempotent — calling close() twice is safe.
   *
   * @returns {Promise<void>}
   */
  async close() {
    throw new AxonaError('NOT_IMPLEMENTED',
      'PersistenceAdapter.close: subclass must override');
  }
}

// ─── Reference implementation: in-memory ──────────────────────────────

/**
 * In-memory PersistenceAdapter.  Values live in a Map for the lifetime
 * of the instance; close() drops the Map.  Used by tests and by the
 * `persist: false` Peer configuration.
 *
 * Transactions are serialised through a single internal lock so
 * concurrent transaction() calls don't interleave.  Outside a
 * transaction, load/save/delete are not serialised — callers are
 * expected to await each call.
 */
export class InMemoryPersistence extends PersistenceAdapter {
  constructor() {
    super();
    /** @type {Map<string, unknown>} */
    this._store = new Map();
    this._closed = false;
    /** @type {Promise<void>} chained transaction queue */
    this._txTail = Promise.resolve();
  }

  _check() {
    if (this._closed) {
      throw new AxonaError('NOT_IMPLEMENTED',
        'PersistenceAdapter is closed');
    }
  }

  async load(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`load: key must be string, got ${typeof key}`);
    }
    // Return a structured clone so callers can mutate without
    // affecting stored state.  Web Crypto / Node both provide
    // structuredClone since v17.
    const v = this._store.get(key);
    return v === undefined ? undefined : structuredClone(v);
  }

  async save(key, value) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`save: key must be string, got ${typeof key}`);
    }
    if (value === undefined) {
      this._store.delete(key);
    } else {
      this._store.set(key, structuredClone(value));
    }
  }

  async delete(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`delete: key must be string, got ${typeof key}`);
    }
    this._store.delete(key);
  }

  async transaction(keys, fn) {
    this._check();
    if (!Array.isArray(keys)) {
      throw new TypeError('transaction: keys must be an array');
    }
    if (typeof fn !== 'function') {
      throw new TypeError('transaction: fn must be a function');
    }
    // Serialise transactions through a promise chain.
    const next = this._txTail.then(async () => {
      this._check();
      const entries = {};
      for (const k of keys) {
        const v = this._store.get(k);
        entries[k] = v === undefined ? undefined : structuredClone(v);
      }
      const result = await fn(entries);
      if (result === undefined || result === null) return;
      for (const [k, v] of Object.entries(result)) {
        if (v === undefined) this._store.delete(k);
        else                 this._store.set(k, structuredClone(v));
      }
    });
    // Even if fn() throws, future transactions should still proceed.
    this._txTail = next.catch(() => {});
    return next;
  }

  async close() {
    this._closed = true;
    this._store.clear();
  }
}
