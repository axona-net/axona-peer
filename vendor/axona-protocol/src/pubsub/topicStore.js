// topicStore.js — the TOPIC STORE (refactor Phase 2).
//
// One implementation of per-topic storage semantics: the bounded cache
// (count+byte caps, TTL expiry), the tombstone set and its authorization
// rule, the migrated-deletion wire shape (every cache migration carries
// dels — invariant I-8), the exactly-once app-delivery LRU, and the
// since-floor bookkeeping. Methods are mixed into AxonaManager.prototype
// (`this` is the manager); state lives on the manager façade.

import {
  T, RENEW_MS, RENEW_FAST_MS, RENEW_BACKOFF, DROP_MS, ROOT_CLAIM_MS,
  ROOT_REPLICAS, BACKUP_EVICT_MS, CACHE_MAX, CACHE_BYTES, MAX_DIRECT,
  DELEGATE_BATCH, MAX_VIA, VIA_HOP_BUDGET, TTL_MS, APP_DEDUP_MAX,
  PENDING_PUB_TTL_MS, PENDING_PUB_MAX_TRIES, COLD_BURST_TRIES,
  COLD_BURST_INTERVAL_MS, COLD_BURST_SLOW_TRIES, COLD_BURST_SLOW_INTERVAL_MS,
  COLD_PEER_THRESHOLD, FIRST_PUBLISH_RESEND_MS, REPLAY_CHUNK_BYTES,
  FUTURE_TOLERANCE_MS, BEACON_MS, BEACON_TTL_MS, BEACON_SEEN_MS,
  ROOT_VERIFY_FIRST_MS, ROOT_VERIFY_MS, ROOT_VERIFY_BATCH, METRICS_LEASE_MS,
  METRICS_PUB_MS, METRICS_COALESCE_MS,
} from './constants.js';
import { idHex, idBig, lc, isHexId } from './ids.js';

export const topicStoreMethods = {
  // ── cache ────────────────────────────────────────────────────────────
  _cachePush(role, entry) {
    entry.bytes = (entry.json ? entry.json.length : 0) + 80;
    role.cache.push(entry);
    role.cacheIds.add(entry.msgId);
    role.cacheBytes += entry.bytes;
    while (role.cache.length > this._cacheMax || role.cacheBytes > this._cacheBytes) {
      const old = role.cache.shift();
      if (!old) break;
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  },

  _expireCache(role, now) {
    while (role.cache.length && (now - role.cache[0].publishTs) > TTL_MS) {
      const old = role.cache.shift();
      role.cacheIds.delete(old.msgId);
      role.cacheBytes -= old.bytes;
    }
  },

  // My cache high-water = the newest stamp I hold (or have emitted, as root).
  _highWater(role) {
    return Math.max(role.lastTs || 0, role.cache.length ? role.cache[role.cache.length - 1].publishTs : 0);
  },

  // A target message arriving (publish / replay / fan-out) for which we hold a
  // tombstone: SUPPRESS it iff the kill was authorized by the message's own author
  // — this is where a PROVISIONAL kill (accepted before we held the target) is
  // enforced. A signer MISMATCH (or an unsigned/anonymous target a signed kill
  // can't own) means the kill was forged or unauthorized → REVOKE the tombstone
  // and accept the message. Returns true to drop the message.
  _tombstoned(role, msgId, json) {
    if (!role) return false;
    const tomb = role.tombstones.get(msgId);
    if (!tomb) return false;
    if (tomb.signer) {
      let author = null; try { author = JSON.parse(json)?.signerPubkey ?? null; } catch { /* */ }
      if (author && lc(author) === lc(tomb.signer)) return true;      // authorized kill → stays dead
      role.tombstones.delete(msgId);                                   // unauthorized/forged → revoke
      return false;
    }
    return true;                                                       // signer-less tombstone (defensive) → suppress
  },

  // ── singleton-root replication (warm backup roots) ───────────────────
  // The N reachable neighbours XOR-closest to a topic — its natural successors and
  // therefore the right backup roots (whoever routing re-converges on if the root
  // leaves). Excludes self and the bridge (signaling infra, never a root).
  // The active (non-expired) tombstones as a wire-shaped `dels` array. Every cache
  // MIGRATION (replicate, pull-up/replay-up, graceful handoff) must carry these
  // alongside `msgs`: a node that adopts cached history without the matching tombstones
  // would resurrect a killed message and serve it to a late joiner (the kill-leak class).
  _activeDels(role) {
    const now = this._now();
    const dels = [];
    for (const [tgt, tomb] of (role?.tombstones ?? [])) {
      if ((tomb?.exp ?? 0) > now) dels.push({ del: true, msgId: tgt, killTs: tomb.killTs, signer: tomb.signer ?? null, seq: tomb.seq, publishTs: tomb.killTs });
    }
    return dels;
  },

  // Apply a batch of migrated tombstones BEFORE ingesting the migrated bodies, so a
  // killed message in the same batch is suppressed (not briefly fanned/delivered then
  // retracted). Shared by the replay-up and handoff receive paths.
  _applyDels(role, topicBig, dels) {
    for (const d of (Array.isArray(dels) ? dels : [])) {
      if (d && d.msgId) this._applyKill(role, topicBig, d);
    }
  },

  // ── app delivery (exactly-once) ──────────────────────────────────────
  _deliverToApp(topicBig, json, msgId, publishTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const key = topicBig.toString(16) + ':' + msgId;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (publishTs > prev) this._lastSeenTsByTopic.set(topicBig, publishTs);
    this._confirmPending(topicBig, msgId);             // a subscribed publisher saw its own msg → stop retrying
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, json, msgId, publishTs, seq); }
      catch (e) { this._log('warn', 'delivery-callback-threw', { err: e?.message }); }
    }
  },

  // Deliver a kill to the local app (exactly-once, keyed distinctly from the
  // target's own delivery) and advance our since-floor to the kill's stamp so a
  // renewal stops re-pulling it once seen — while a MISSED kill (since still below
  // killTs) is re-pulled and self-heals.
  _deliverKillToApp(topicBig, target, killTs, seq) {
    if (!this.mySubscriptions.has(topicBig)) return;   // pure relay stores+forwards, doesn't consume
    const prev = this._lastSeenTsByTopic.get(topicBig) || 0;
    if (killTs > prev) this._lastSeenTsByTopic.set(topicBig, killTs);
    const key = topicBig.toString(16) + ':kill:' + target;
    if (this._appDelivered.has(key)) return;           // exactly-once
    this._appDelivered.set(key, true);
    if (this._appDelivered.size > APP_DEDUP_MAX) this._appDelivered.delete(this._appDelivered.keys().next().value);
    // A kill retracts a message the app is holding. If this app NEVER received the body
    // (a since:'all' joiner that arrived after the kill — the killed body is spliced from
    // cache and never replayed), there is nothing to retract: delivering a spurious
    // "deleted" event for a message it never saw is noise. Record the tombstone + advance
    // the floor (above) so we converge and suppress the body if it ever arrives, but only
    // CALL BACK when the body was actually delivered to this app. (Now reliably reproduced
    // once cohort anti-entropy made tombstone propagation dependable.)
    const bodyKey = topicBig.toString(16) + ':' + target;
    if (!this._appDelivered.has(bodyKey)) return;      // never had the body → nothing to retract
    if (this._deliveryCallback) {
      try { this._deliveryCallback(topicBig, JSON.stringify({ deleted: true, msgId: target, topic: null }), target, killTs, seq); }
      catch (e) { this._log('warn', 'delete-callback-threw', { err: e?.message }); }
    }
  },

  // The `since` to renew with: max of our cache high-water (relay), last app
  // delivery, and the seeded subscription floor.
  _sinceFor(topicBig) {
    const role = this.axonRoles.get(topicBig);
    const relay = (role && role.cache.length) ? role.cache[role.cache.length - 1].publishTs : 0;
    const seen  = this._lastSeenTsByTopic.get(topicBig);
    const sub   = this.mySubscriptions.get(topicBig)?.since;
    return Math.max(relay, Number.isFinite(seen) ? seen : 0, Number.isFinite(sub) ? sub : 0);
  },

};

export default topicStoreMethods;
