// =====================================================================
// file.js — FilePersistence adapter for Node.
//
// JSON-per-key files under a configurable directory.  Atomic writes
// via temp-file + rename + fsync.  Per-instance in-memory mutex
// serialises transactions; cross-process safety via a PID lock file.
//
// Layout under `dir/`:
//   <key>.json        — one file per key (identity, synaptome, …).
//   .lock             — PID of the process currently holding the dir.
//
// The cross-process lock is advisory: it prevents the kernel from
// running two concurrent peers against the same persistence dir, but
// doesn't defend against external corruption (an editor opening the
// JSON file, an out-of-band rm, etc).  Apps that need stronger
// guarantees should wrap FilePersistence behind an OS-level mechanism.
//
// Node-only — uses `node:fs/promises` and `node:path` and
// `node:process`.  Browsers should use IndexedDBPersistence instead.
// =====================================================================

import { readFile, writeFile, unlink, mkdir, rename, open as fsOpen, rm }
  from 'node:fs/promises';
import { join, resolve }
  from 'node:path';
import { AxonaError } from '../errors.js';

import { PersistenceAdapter } from './interface.js';

const LOCK_FILE = '.lock';

/**
 * FilePersistence — concrete PersistenceAdapter using one JSON file
 * per key under a directory.
 */
export class FilePersistence extends PersistenceAdapter {
  /**
   * @param {object}  opts
   * @param {string}  opts.dir         Directory to read/write under. Created
   *                                   if missing.  Defaults to `./.axona/`.
   * @param {boolean} [opts.lock=true] Acquire a PID lock file at startup.
   *                                   Set to false in tests that need
   *                                   multiple instances against the same
   *                                   directory.
   */
  constructor({ dir = './.axona', lock = true } = {}) {
    super();
    this._dir          = resolve(dir);
    this._lockEnabled  = lock;
    this._closed       = false;
    this._initPromise  = null;
    /** @type {Promise<void>} chained transaction queue */
    this._txTail       = Promise.resolve();
  }

  // ── Lifecycle ──────────────────────────────────────────────────────

  async _init() {
    if (this._initPromise) return this._initPromise;
    this._initPromise = (async () => {
      await mkdir(this._dir, { recursive: true });
      if (this._lockEnabled) await this._acquireLock();
    })();
    return this._initPromise;
  }

  async _acquireLock() {
    const lockPath = join(this._dir, LOCK_FILE);
    const existing = await tryRead(lockPath);
    if (existing) {
      const pid = parseInt(existing.trim(), 10);
      if (Number.isFinite(pid) && isPidAlive(pid)) {
        throw new AxonaError('PERSIST_LOCKED',
          `FilePersistence: directory ${this._dir} is locked by pid ${pid}`,
          { context: { dir: this._dir, pid } });
      }
      // Stale lock from a crashed previous run — overwrite.
    }
    await writeFile(lockPath, String(process.pid), 'utf8');
  }

  _check() {
    if (this._closed) {
      throw new AxonaError('PERSIST_CLOSED',
        'FilePersistence is closed');
    }
  }

  // ── load / save / delete ───────────────────────────────────────────

  async load(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`load: key must be string, got ${typeof key}`);
    }
    await this._init();
    return this._readKey(key);
  }

  async save(key, value) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`save: key must be string, got ${typeof key}`);
    }
    await this._init();
    if (value === undefined) {
      await this._deleteKey(key);
    } else {
      await this._writeKey(key, value);
    }
  }

  async delete(key) {
    this._check();
    if (typeof key !== 'string') {
      throw new TypeError(`delete: key must be string, got ${typeof key}`);
    }
    await this._init();
    await this._deleteKey(key);
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

    const next = this._txTail.then(async () => {
      this._check();
      const entries = {};
      for (const k of keys) entries[k] = await this._readKey(k);

      const result = await fn(entries);
      if (result === undefined || result === null) return;

      for (const [k, v] of Object.entries(result)) {
        if (v === undefined) await this._deleteKey(k);
        else                  await this._writeKey(k, v);
      }
    });
    this._txTail = next.catch(() => {});
    return next;
  }

  // ── close ──────────────────────────────────────────────────────────

  async close() {
    if (this._closed) return;
    this._closed = true;
    // Wait for any in-flight transactions before releasing the lock.
    try { await this._txTail; } catch { /* surfaced elsewhere */ }
    if (this._lockEnabled) {
      try { await unlink(join(this._dir, LOCK_FILE)); } catch { /* already gone */ }
    }
  }

  // ── internals ──────────────────────────────────────────────────────

  async _readKey(key) {
    const p = join(this._dir, this._fileFor(key));
    const text = await tryRead(p);
    if (text === null) return undefined;
    try {
      return JSON.parse(text);
    } catch (cause) {
      throw new AxonaError('PERSIST_CORRUPT',
        `FilePersistence: ${p} is not valid JSON (${cause.message})`,
        { cause, context: { key, path: p } });
    }
  }

  async _writeKey(key, value) {
    const dst  = join(this._dir, this._fileFor(key));
    const tmp  = dst + '.tmp';
    const text = JSON.stringify(value);

    // Atomic rename pattern: write tmp, fsync tmp, rename → dst.
    const fh = await fsOpen(tmp, 'w');
    try {
      await fh.writeFile(text, 'utf8');
      await fh.sync();
    } finally {
      await fh.close();
    }
    await rename(tmp, dst);
  }

  async _deleteKey(key) {
    const p = join(this._dir, this._fileFor(key));
    try { await rm(p); } catch { /* already gone */ }
  }

  _fileFor(key) {
    // Sanitise — file system separators and dot-prefix would be unsafe.
    if (key.includes('/') || key.includes('\\') || key.startsWith('.')) {
      throw new AxonaError('PERSIST_INVALID_KEY',
        `FilePersistence: key '${key}' contains separator or leading dot`,
        { context: { key } });
    }
    return `${key}.json`;
  }
}

// ── module helpers ──────────────────────────────────────────────────

async function tryRead(p) {
  try {
    return await readFile(p, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') return null;
    throw err;
  }
}

function isPidAlive(pid) {
  try {
    // POSIX: signal 0 tests existence without delivering a signal.
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // ESRCH = no such process; EPERM = exists but we can't signal it.
    return err.code === 'EPERM';
  }
}
