// =====================================================================
// smoke_identity.js — verify the identity module's contract.
//
// Runs in Node with a mocked localStorage.  Browser-only paths
// (navigator.geolocation) are skipped by passing an explicit region.
// =====================================================================

// ── Mock localStorage in Node ───────────────────────────────────────
const store = new Map();
globalThis.localStorage = {
  getItem(k)    { return store.has(k) ? store.get(k) : null; },
  setItem(k, v) { store.set(k, String(v)); },
  removeItem(k) { store.delete(k); },
};

const {
  REGIONS,
  getCurrentIdentity,
  deriveIdentity,
  rotateIdentity,
  forgetIdentity,
} = await import('./identity.js');

let passed = 0, failed = 0;
function check(label, condition) {
  if (condition) { console.log(`  ✓ ${label}`); passed++; }
  else           { console.log(`  ✗ ${label}`); failed++; }
}

async function testFreshIdentity() {
  console.log('\n── Fresh identity from a region pick ──');
  forgetIdentity();

  const region = REGIONS.find(r => r.id === 'us-east');
  const ident = await deriveIdentity({ region });

  check('id is a BigInt',                typeof ident.id === 'bigint');
  check('id is 64-bit (< 2^64)',          ident.id < (1n << 64n));
  check('geoBits === 8',                  ident.geoBits === 8);
  check('region matches input',           ident.region.id === 'us-east');
  check('region.source = user-selected',  ident.region.source === 'user-selected');
  check('createdAt is a recent timestamp', ident.createdAt > Date.now() - 1000);

  // Top 8 bits should encode the S2 cell for the picked lat/lng.
  // We don't recompute geoCellId here, but we can verify the top
  // bits are stable: deriving twice for the same region gives the
  // same top-8-bits (different bottom bits if rotated).
  const ident2 = await rotateIdentity({ region });
  check('rotated id has same top 8 bits',
    (ident.id >> 56n) === (ident2.id >> 56n));
  check('rotated id has different bottom 56 bits',
    (ident.id & ((1n << 56n) - 1n)) !== (ident2.id & ((1n << 56n) - 1n)));
}

async function testPersistence() {
  console.log('\n── Persistence + reload ──');
  forgetIdentity();

  const region = REGIONS.find(r => r.id === 'asia-east');
  const created = await deriveIdentity({ region });

  // Simulate a page reload: getCurrentIdentity must return what was
  // persisted (without re-prompting / re-generating).
  const loaded = getCurrentIdentity();
  check('persisted identity can be read back',     loaded != null);
  check('persisted id matches created',             loaded.id === created.id);
  check('persisted region matches created',         loaded.region.id === 'asia-east');
  check('persisted geoBits matches',                loaded.geoBits === created.geoBits);

  // deriveIdentity called again returns the same id (does not
  // generate fresh).  This is the "known node fast path" the user
  // asked for.
  const reDerived = await deriveIdentity({ region });
  check('deriveIdentity returns persisted id (fast path)',
    reDerived.id === created.id);
}

async function testRotation() {
  console.log('\n── Rotate identity ──');
  forgetIdentity();

  const region = REGIONS.find(r => r.id === 'eu-west');
  const a = await deriveIdentity({ region });
  const b = await rotateIdentity({ region });

  check('rotate replaces persisted id',
    a.id !== b.id);
  check('after rotate, getCurrentIdentity returns new id',
    getCurrentIdentity()?.id === b.id);
}

async function testForget() {
  console.log('\n── Forget identity ──');
  await deriveIdentity({ region: REGIONS[0] });
  check('identity exists before forget', getCurrentIdentity() != null);
  forgetIdentity();
  check('identity gone after forget',    getCurrentIdentity() == null);
}

async function testFallback() {
  console.log('\n── Fallback when no region + no geolocation ──');
  forgetIdentity();
  // No navigator.geolocation in Node; resolveRegion falls through
  // to REGIONS[0] (us-east) by default.
  const ident = await deriveIdentity();
  check('falls back to a real region',
    Number.isFinite(ident.region.lat) && Number.isFinite(ident.region.lng));
  check('region.source = default (last-resort)',
    ident.region.source === 'default');
}

async function main() {
  console.log('Identity module smoke');
  await testFreshIdentity();
  await testPersistence();
  await testRotation();
  await testForget();
  await testFallback();
  console.log(`\nResult: ${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch(err => {
  console.error('smoke threw:', err);
  process.exit(2);
});
