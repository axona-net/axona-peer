<!-- axona-peer (axona.net reference app). Quick checklist before merge. -->

## Summary

<!-- What changed and why. -->

## Checklist

- [ ] **Security-relevant change?** If this touches auth, crypto, key handling, CSP/identity exposure, or anything that changes *what's protected* — add/update an entry in [`axona-docs/SECURITY-CHANGELOG.md`](https://github.com/axona-net/axona-docs/blob/main/SECURITY-CHANGELOG.md). Resolved items only: describe what's protected, **never enumerate still-open findings**.
- [ ] Smoke tests pass (`npm run smoke && npm run smoke:transport && npm run smoke:identity && npm run smoke:gate && npm run smoke:kernel`).
- [ ] Middle version bumped in `index.html` **and** `PEER_VERSION` in `src/client.js` for any code change.
- [ ] Kernel re-vendored via `./scripts/sync-protocol.sh` if pulling a new `@axona/protocol` (verify the vendored `KERNEL_VERSION`).
