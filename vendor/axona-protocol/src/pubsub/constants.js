// =====================================================================
// constants.js — the pub/sub kernel's tunables, wire types, and gated
// policy switches, in one place (refactor Phase 2; audited in
// INVARIANTS.md Appendix A — every constant there is live with a test or
// incident behind it). Grouped by concern; Phase 4 turns these groups
// into documented policy objects.
// =====================================================================

// ── Inbound caps (D-1: bound attacker-controlled payloads) ──────────────
// Re-exported via AxonaManager.js unchanged — AxonaPeer and std/chunk import
// these as the publish-size contract; independent of the pub/sub mechanism.
export const MAX_PUBLISH_BYTES = 256 * 1024;         // absolute hard ceiling (chars)
export const MAX_RELIABLE_PUBLISH_BYTES = 15 * 1024; // WebRTC-interop reliable floor (O-5)

// ── Region-occupancy enforcement (v4.13.0), gated (v4.15.0) ─────────────
// The region rule — a topic may only be rooted by a node IN ITS REGION, and a
// pub/sub to a region with no reachable in-region node is refused — is correct
// long-term (it prevents cross-region hotspots). But PRE-critical-mass most
// regions, even populated ones, have no nodes yet, so enforcing it would refuse
// nearly every real pub/sub. So it is OFF BY DEFAULT: when disabled, an
// out-of-region node may root a topic (pre-4.13.0 behavior — nearest node wins),
// and the peer-side REGION_UNPOPULATED pre-send guard is a no-op. Flip it on with
// configureRegionLock({ enforce: true }) once the network has enough regional
// coverage. Gates BOTH layers (manager _regionOk + peer _assertRegionUsable) so
// disabling it never leaves an empty-region topic silently un-rooted.
let _regionLockEnforced = false;
export function configureRegionLock({ enforce } = {}) {
  _regionLockEnforced = !!enforce;
  return _regionLockEnforced;
}
export function isRegionLockEnforced() { return _regionLockEnforced; }

// ── Renewal / attachment (the adaptive-renewal policy) ──────────────────
export const RENEW_MS        = 60_000;          // re-subscribe cadence CEILING (stable state)
// Adaptive renewal (churn re-home fix): a subscriber re-homes only when it renews,
// so the renewal interval IS the orphan window after its relay/root churns. A flat
// 60s is too slow for mobile churn; a flat 5s is 12× the steady-state traffic. So
// renew FAST right after subscribing or after a relay change (re-pin), backing off
// ×1.5 toward the ceiling while stable. Sustained churn keeps re-pinning → stays
// fast (measured ~82% delivery @30%/round vs 43% at 60s); calm → backs off → cheap.
export const RENEW_FAST_MS   = 5_000;           // adaptive floor: initial + post-re-home interval
export const RENEW_BACKOFF   = 1.5;             // multiply the interval each stable renewal, up to RENEW_MS
export const DROP_MS         = 180_000;         // evict a subscriber after missed renewals (≥ 3× ceiling)

// ── Root election / convergence ─────────────────────────────────────────
// Reachable-root fallback (cold-convergence fix). An unpinned subscriber whose
// iterative root hint names a node CLOSER in XOR but that never adopts it back
// (unreachable on the greedy data path / "broken-but-authentic") would otherwise
// defer forever and the topic never roots → total strand. After this confirmation
// window with no upstream pin, if we are the closest node among our REACHABLE
// neighbours we claim root locally — preferring a reachable root over a
// closer-but-unconfirmed one. Long enough (≥2 ticks) that a genuinely reachable
// multi-hop closer root has time to adopt us first; a wrongly-claimed farther root
// is self-corrected by the strictly-closer beacon demotion (_onRootBeacon).
export const ROOT_CLAIM_MS   = 6_000;           // unconfirmed-deferral window before self-claiming root
// Singleton-root durability: a root with no sub-axon tree (the small-topic common
// case) has no cache-holding backup, so an abrupt root churn loses all history for
// future since:'all' joiners. The root proactively replicates its full cache to its
// REPLICAS nearest reachable neighbours (the natural successors); on churn the
// now-closest backup already holds everything and the reachable-root fallback
// promotes it with no gap. Backups are re-evaluated each tick (closer newcomers
// recruited, farther ones retired). 0 disables.
export const ROOT_REPLICAS   = 2;
// A backup is a subscribing CHILD RELAY that also prefetches the root's full cache
// (see _onReplicate + the _backupTopics subscribe loop in refreshTick). Promotion is
// NOT a bespoke, local-only decision anymore (that split roots when two backups
// couldn't see each other): a backup renews its subscribe every tick, so on root
// churn the SAME probe-protected machinery every subscriber/host uses — _rootHint_'s
// iterative lookup() → single globally-closest terminus — elects exactly ONE new root
// (the closest self-roots, the rest re-home under it), gap-free from the warm cache.
// This constant only bounds set growth: a backup whose root stopped replicating to it
// this long (root departed & we've since re-homed/promoted, or the root retired us) is
// dropped from _backupTopics as cleanup — never used to trigger promotion.
export const BACKUP_EVICT_MS = 60_000;
// Delta-aware replication (v4.24.1, task #333): the periodic _replicateRoots sweep
// pushes a root's FULL cache+tombstones only when its state changed since the last
// push, a new cohort member was recruited, or this anti-entropy backstop elapsed —
// otherwise it sends an empty REPLICATE as a liveness KEEPALIVE (the recipient
// refreshes lastReplicaAt; ingest of an empty batch is a no-op). Before this, every
// root re-sent its entire cache to its whole cohort EVERY tick; during the 4.24.0
// soak's root-transition storms that per-tick × per-role full-state traffic was the
// bandwidth fuel of the role-bloat collapse (#332 feedback loop: 2.7 GB relay RSS,
// >1M log lines/night). Convergence is unaffected: any divergence changes the
// diverged root's own state signature and triggers one full push (union-ingest at
// the receiving root), then quenches back to keepalives.
export const ROOT_REPLICATE_FULL_MS = 60_000;
// ── Join-storm pacing (task #332, invariant I-11: bulk work never starves ──
// liveness). Two failure facets, both live-reproduced on testnet 2026-07-17:
// a node holding N roles that gains a new cohort member (a joining relay)
// fires N full-state pushes at it within ONE tick, and the recipient's event
// loop drowns in per-message signature verifies — its mesh keepalives miss,
// peers evict it, and the mesh dissolves without self-healing.
//
// Sender leg: at most this many FULL replications per tick (round-robin cursor
// across roles, so deferred roles are next tick's first served). Keepalives are
// unbudgeted (empty, cheap). Seeding a newcomer into a 1,000-role region takes
// ~1000/32 ≈ 31 ticks ≈ 2.6 min — degraded convergence instead of a dead relay.
// Under sustained load the 60s anti-entropy backstop stretches proportionally;
// that is I-11 working as intended, not a bug.
export const REPLICATE_FULL_BUDGET = 32;
// Receiver leg (defense in depth — also bounds a malicious flood): REPLICATE /
// REPLAYUP ingest is queued and drained in time-boxed slices so verification
// CPU can never monopolize the loop. Queue overflow drops the NEWEST payload
// (logged); anti-entropy re-delivers within ROOT_REPLICATE_FULL_MS.
export const INGEST_QUEUE_MAX = 4096;           // queued ingest payloads before overflow-drop
export const INGEST_SLICE_MS  = 8;              // ingest CPU per slice; keeps ping RTT << mesh eviction timeout
// Mesh re-warm (facet 2: relays never re-initiated their inter-mesh after a
// mass departure — peers=1..2 forever, systemd-green but backbone-dead). If the
// mesh stays below MIN for TICKS consecutive ticks, re-run self-integration
// (findKClosest(self) + open channels — idempotent, never throws), at most once
// per cooldown.
export const MESH_REWARM_MIN         = 3;       // mesh peers below this = starved
export const MESH_REWARM_TICKS       = 3;       // consecutive starved ticks before acting (~15s: outlasts a blip)
export const MESH_REWARM_COOLDOWN_MS = 60_000;  // min gap between re-warm attempts (integration takes seconds)

// ── Cache / tree shape / wire sanity ────────────────────────────────────
export const CACHE_MAX       = 1024;            // messages cached per relay
export const CACHE_BYTES     = 16 * 1024 * 1024;// byte ceiling on a relay's cache
export const MAX_DIRECT      = 20;              // direct subscribers before a relay delegates
export const DELEGATE_BATCH  = 8;               // subscribers handed off when promoting a child
export const MAX_VIA         = 8;               // ordered-waypoint list length cap (wire sanity)
export const VIA_HOP_BUDGET  = 8;               // hops per via leg (enforced kernel-side, Phase 2+)
export const TTL_MS          = 24 * 60 * 60 * 1000;   // 24h message hold, keyed on the ROOT timestamp
export const APP_DEDUP_MAX   = 8192;            // exactly-once app-delivery LRU
export const REPLAY_CHUNK_BYTES = 96 * 1024;    // byte budget per replay deliver batch
export const FUTURE_TOLERANCE_MS = 5 * 60 * 1000;   // §5 bad-clock rule: drop replayed stamps this far ahead

// ── Publish/kill reliability (observation-confirmed retry, I-9) ─────────
export const PENDING_PUB_TTL_MS = 30_000;       // retain a recent publish/kill this long so refreshTick can RE-SEND it toward the true root until the publisher observes its own msgId (implicit ack) — covers a strand on the greedy walk AND packet loss; idempotent (root dedups by msgId)
export const PENDING_PUB_MAX_TRIES = 6;         // cap re-sends so a never-confirmed publish (e.g. a non-subscribed publisher under loss) can't re-send unboundedly; loss^7 is negligible even at 30%
// Cold-publish burst (v4.11.0): a publish from a freshly-joined, not-yet-integrated
// node is the worst case for the one-shot greedy PUB — its routing table lacks the
// keyspace breadth to route to the true root, so the message strands and (unlike a
// renewing subscribe) never re-homes. Waiting for the table to warm is harmful: it's
// OUTBOUND traffic that integrates a newcomer (reachability to a node lives in its
// NEIGHBOURS' tables, healed by directed sends — same lesson as churn-in warmup). So
// while cold, re-send the SAME signed envelope a few times over the first second:
// idempotent (root dedups by msgId), each send both integrates us further AND gets a
// fresh shot at the true root as tables converge. Naturally disabled once warm (the
// gate is low neighbour count), and the slow refreshTick retry still backstops after.
export const COLD_BURST_TRIES     = 5;          // extra fast re-sends on a cold publish
export const COLD_BURST_INTERVAL_MS = 200;      // spacing of the fast burst (≈1s total)
export const COLD_BURST_SLOW_TRIES    = 5;      // then a second, slower wave as tables keep warming
export const COLD_BURST_SLOW_INTERVAL_MS = 400; // 5 × 400ms ≈ 2s more coverage past the first second
export const COLD_PEER_THRESHOLD  = 8;          // "cold" = fewer than this many neighbours (not yet integrated)
// First publish to a topic — even when WARM — is re-sent once this long after, so a
// tree that formed microseconds before the send (a just-arrived subscriber, a root
// that just rooted) still catches it. Cold publishers already re-send via the burst.
export const FIRST_PUBLISH_RESEND_MS = 200;

// ── Root beacon (soft-state root advertisement; Pubsub-Root-Beacon-v0.1) ────
// A root periodically announces "root for T was last at X" to its K XOR-closest
// neighbors (the topic's convergence basin), recursive 2 layers. Receivers cache
// it (verify-don't-trust) and consult it before greedy/lookup, fixing the
// last-mile divergence (publisher/subscriber resolving different roots).
export const BEACON_MS       = 20_000;          // emission cadence (faster than RENEW_MS so churn heals quickly)
export const BEACON_TTL_MS   = 50_000;          // inbound pointer validity (~2.5×BEACON_MS)
export const BEACON_FANOUT   = 6;               // K closest neighbors per layer (fan-out ≤ K+K²)
export const BEACON_LAYERS   = 2;               // recursive forward depth
export const BEACON_SEEN_MS  = 60_000;          // flood-dedup retention

// Root self-verification (v4.19.1). Beacon-gated reconciliation is reach-limited:
// a spurious root minted by a stranded SUB on a FRESH topic (no beacon anywhere
// yet) can sit outside the true root's beacon basin (fanout^layers) and never
// hear the demotion — its via-pinned subscribers are then permanently orphaned
// (observed on prod: binary 0-of-N subscribers inside an otherwise-perfect
// tree). So every root verifies its own claim with the SAME iterative
// closest-node lookup subscribers use: once shortly after forming (the
// fresh-topic race window), then periodically. Finding a strictly-closer live
// node ⇒ seed a VERIFIED root pointer + demote + re-home + push cache up.
// Batched per tick so a many-rooted relay doesn't storm lookups; each lookup is
// fired non-blocking (NEVER awaited in the tick — the 4.18.1 lesson).
export const ROOT_VERIFY_FIRST_MS = 6_000;      // first verify after root-formed
export const ROOT_VERIFY_MS       = 45_000;     // steady-state re-verify cadence
export const ROOT_VERIFY_BATCH    = 3;          // max verify lookups launched per tick

// ── Demand-driven metrics (any root, no special nodes) ───────────────────
// Metrics are NOT a relay feature — any node that is root for a topic publishes
// its snapshots WHEN and ONLY WHEN a metrics lease is active. A subscriber to
// metricTopic(T) emits a METRICSON toward T (routed like a SUB); the root (and
// every inheriting root) sets a renewable lease and, while fresh, publishes a
// snapshot to metricTopic(T) each METRICS_PUB_MS. Path nodes cache the flag so a
// second requester's METRICSON short-circuits (the root already knows). The lease
// self-expires when the last metric subscriber stops renewing → metrics turn off,
// no orphan load.
export const METRICS_LEASE_MS = 70_000;   // root keeps publishing this long after the last METRICSON (~= dropMs; renewed by the subscriber's refresh tick)
export const METRICS_PUB_MS   = 20_000;   // snapshot cadence at the root while the lease is fresh
export const METRICS_COALESCE_MS = 8_000; // a path node re-forwards METRICSON upstream at most this often (fan-in dedup; still keeps the root's lease alive)

// ── Wire message types (all ROUTED) ─────────────────────────────────────
export const T = {
  SUB:      'pubsub:sub',       // subscribe — routed toward topic id (or a via waypoint)
  UNSUB:    'pubsub:unsub',     // explicit unsubscribe (renewal lapse also drops)
  PUB:      'pubsub:pub',       // publish — routed toward topic id; NO timestamp (root stamps)
  DELIVER:  'pubsub:deliver',   // stamped messages — routed toward a subscriber id
  ADOPT:    'pubsub:adopt',     // delegate: "become my child relay + take these subscribers"
  PULLUP:   'pubsub:pullup',    // "I'm behind you — replay your stamped history up to me" (§6)
  REPLAYUP: 'pubsub:replayup',  // a relay's stamped cache delta, routed UP to a behind parent
  HANDOFF:  'pubsub:handoff',   // graceful-leave: a departing root pushes its cache to its heir
  KILL:     'pubsub:kill',      // retract a message (thin; TODO Phase 4)
  UNPUB:    'pubsub:unpub',     // RESERVED — removed v4.3.0 (no handler/sender); wire string kept so legacy frames are ignored, not misrouted
  TOUCH:    'pubsub:touch',     // extend TTL (thin; TODO Phase 4)
  PULL:     'pubsub:pull',      // on-demand fetch request — routed toward topic id
  PULLRESP: 'pubsub:pullresp',  // pull response — routed back toward the requester id
  ROOTBEACON: 'pubsub:rootbeacon', // soft-state root advertisement to the topic's neighborhood
  REPLICATE: 'pubsub:replicate', // singleton-root durability: root pushes cache+tombstones to its N nearest neighbours (warm backup roots)
  METRICSON: 'pubsub:metricson', // demand-driven metrics: routed toward the topic id like SUB; marks the path + root so ANY root publishes snapshots while the lease is fresh
  HANDOFFACK: 'pubsub:handoffack', // heir confirms a HANDOFF landed (v4.24.0) — the leaver retries / cohort-sprays unacked topics instead of silently dropping the last copy
};

// ── Empty-self-root cohort pull (v4.24.0 — the alert-bot read-miss fix) ──────
// A cold subscriber whose SUB terminates at itself becomes the topic's root with
// an EMPTY cache while a live holder (ex-root / backup / host) still has the
// history — and nothing tells the holder about the new closer root, so the
// state is STICKY (field: 82% of misses, unrecovered at 600s). The new root
// therefore PULLS: probe the K-closest cohort + the nodes on its own iterative
// lookup path (the runner-up closest is usually the prior root) with a
// PULLUP(sinceHw:0); any holder replays via the existing REPLAYUP → verified
// union-ingest. Delayed so a pub-terminal root (whose own publish fills the
// cache immediately) never probes; bounded tries so a genuinely-fresh topic
// quenches after finding nobody.
export const EMPTY_ROOT_PROBE_DELAY_MS    = 800;    // wait before first probe (skip if cache filled)
export const EMPTY_ROOT_PROBE_MAX         = 3;      // total probes per role while empty
export const EMPTY_ROOT_PROBE_INTERVAL_MS = 5_000;  // min gap between probes (via refreshTick)
export const EMPTY_ROOT_PROBE_FANOUT      = 4;      // candidates contacted per probe

// ── Leave-handoff confirmation (v4.24.0) ─────────────────────────────────────
// The graceful-leave HANDOFF was fire-and-forget: a failed heir delivery
// silently transferred nothing (diagnosis: gone=40/40 with leaveMs≈11ms — a
// CONFIRMATION failure, not a window failure). The leaver now waits briefly for
// HANDOFFACK, retries once, and finally sprays the cache to the K-closest
// cohort as REPLICATE (idempotent; backups store it, roots union-ingest it).
export const HANDOFF_ACK_MS  = 700;   // per-attempt ack wait
export const HANDOFF_TRIES   = 2;     // sends before falling back to cohort spray
