// =====================================================================
// indexeddb.js — IndexedDBPersistence adapter for browsers.
//
// One IndexedDB database per peer; a single object store inside it
// keyed by namespace.  Transactions use IndexedDB's native readwrite
// transactions, which atomically commit or roll back if the
// browser's locking is interrupted.
//
// On quota errors (browser refuses to store any more data),
// IndexedDBPersistence catches the failure and emits a warn-level
// onLog event (if a logger was provided); the operation that
// triggered quota throws.  Apps that hit this should reduce what
// they cache or expire old entries.
//
// Runs in real browsers and in Node tests under `fake-indexeddb`
// (which provides `indexedDB`, `IDBKeyRange`, etc. on globalThis).
// =====================================================================

import { AxonaError }         from '../errors.js';
import { PersistenceAdapter } from './interface.js';

const DEFAULT_DB    = 'axona';
const STORE_NAME    = 'kv';
const SCHEMA_VERSION = 1;

/**
 * IndexedDBPersistence — concrete PersistenceAdapter using IndexedDB.
 */
export class IndexedDBPersistence extends PersistenceAdapter {
  /**
   * @param {object}  [opts]
   * @param {string}  [opts.dbName='axona']  IndexedDB database name.
   * @param {(level: string, msg: string, ctx?: object) => void} [opts.log]
   *        Optional log sink for quota warnings.
   */
  constructor({ dbName = DEFAULT_DB, log = null } = {}) {
    super();
    this._dbName     = dbName;
    this._log        = log;
    /** @type {IDBDatabase | null} */
    this._db         = null;
    this._closed     = false;
    this._initPromise = null;
    /** @type {Promise<void>} chained transaction queue (in-process) */
    this._txTail     = Promise.resolve();
  }

  // ── Lifecycle ──────────────────────────────────────────────────────

  _check() {
    if (this._closed) {
      throw new AxonaError('PERSIST_CLOSED', 'IndexedDBPersistence is closed');
    }
  }

  async _init() {
    if (this._initPromise) return this._initPromise;
    this._initPromise = new Promise((resolve, reject) => {
      const idb = globalThis.indexedDB;
      if (!idb) {
        return reject(new AxonaError('PERSIST_NO_INDEXEDDB',
          'IndexedDBPersistence: globalThis.indexedDB not available'));
      }
      const req = idb.open(this._dbName, SCHEMA_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME);
        }
      };
      req.onsuccess = () => {
        this._db = req.result;
        this._db.onversionchange = () => {
          // Another tab is upgrading or deleting the DB.  Close so
          // they don't block.  Subsequent calls will throw closed.
          this._closed = true;
          try { this._db?.close(); } catch { /* ignore */ }
        };
        resolve();
      };
      req.onerror = () => reject(new AxonaError('PERSIST_OPEN_FAILED',
        `IndexedDBPersistence: open '${this._dbName}' failed (${req.error?.message ?? 'unknown'})`,
        { cause: req.error }));
    });
    return this._initPromise;
  }

  // ── load / save / delete ───────────────────────────────────────────

  async load(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`load: key must be string, got ${typeof key}`);
    }
    await this._init();
    return await this._withStore('readonly', (store) => req(store.get(key)));
  }

  async save(key, value) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`save: key must be string, got ${typeof key}`);
    }
    await this._init();
    if (value === undefined) {
      await this._withStore('readwrite', (store) => req(store.delete(key)));
    } else {
      // structuredClone is what IndexedDB does internally — pass the
      // pre-cloned value so caller mutations after save() don't affect
      // what gets persisted.  (Belt-and-braces; IDB clones anyway.)
      await this._withStore('readwrite',
        (store) => req(store.put(structuredClone(value), key)),
        { catchQuota: true });
    }
  }

  async delete(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`delete: key must be string, got ${typeof key}`);
    }
    await this._init();
    await this._withStore('readwrite', (store) => req(store.delete(key)));
  }

  // ── transaction ────────────────────────────────────────────────────

  async transaction(keys, fn) {
    this._check();
    if (!Array.isArray(keys)) {
      throw new TypeError('transaction: keys must be an array');
    }
    if (typeof fn !== 'function') {
      throw new TypeError('transaction: fn must be a function');
    }
    await this._init();

    // Serialise transactions through a promise chain.  IndexedDB does
    // its own intra-DB locking, but we still need to ensure that the
    // (load → fn() → save) sequence in `this` doesn't interleave with
    // other transactions issued from this same adapter.
    const next = this._txTail.then(async () => {
      this._check();
      // First pass: read current values in a readonly transaction.
      const entries = {};
      await this._withStore('readonly', async (store) => {
        for (const k of keys) entries[k] = await req(store.get(k));
      });
      // User fn runs outside the transaction — IndexedDB transactions
      // auto-commit when there's no pending request, so we can't safely
      // await user code inside one.  Atomic commit comes from the
      // readwrite tx below: all writes happen in one IndexedDB tx,
      // which either commits or rolls back as a unit.
      const result = await fn(entries);
      if (result === undefined || result === null) return;

      await this._withStore('readwrite', async (store) => {
        for (const [k, v] of Object.entries(result)) {
          if (v === undefined) await req(store.delete(k));
          else                  await req(store.put(structuredClone(v), k));
        }
      }, { catchQuota: true });
    });
    this._txTail = next.catch(() => {});
    return next;
  }

  // ── close ──────────────────────────────────────────────────────────

  async close() {
    if (this._closed) return;
    this._closed = true;
    try { await this._txTail; } catch { /* surfaced elsewhere */ }
    try { this._db?.close(); } catch { /* ignore */ }
    this._db = null;
  }

  // ── internals ──────────────────────────────────────────────────────

  async _withStore(mode, work, { catchQuota = false } = {}) {
    const tx = this._db.transaction(STORE_NAME, mode);
    const store = tx.objectStore(STORE_NAME);
    let result;
    try {
      result = await work(store);
    } catch (err) {
      if (catchQuota && err && err.name === 'QuotaExceededError') {
        this._log?.('warn', 'IndexedDBPersistence: quota exceeded', { dbName: this._dbName });
        throw new AxonaError('PERSIST_QUOTA_EXCEEDED',
          'IndexedDBPersistence: quota exceeded', { cause: err });
      }
      throw err;
    }
    await new Promise((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onerror    = () => reject(tx.error);
      tx.onabort    = () => reject(tx.error ?? new Error('transaction aborted'));
    });
    return result;
  }
}

// ── Promise wrapper for IDBRequest ───────────────────────────────────

function req(idbRequest) {
  return new Promise((resolve, reject) => {
    idbRequest.onsuccess = () => resolve(idbRequest.result);
    idbRequest.onerror   = () => reject(idbRequest.error);
  });
}
