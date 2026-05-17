#!/usr/bin/env bash
# =====================================================================
# sync-protocol.sh — refresh vendor/axona-protocol/ from the canonical
# @axona/protocol source at github.com/axona-net/axona-protocol.
#
# Run from the repo root:  ./scripts/sync-protocol.sh
#
# What gets copied:
#   contracts/  (all 5 contract files)
#   dht/        (AxonaPeer, DHTNode, NeuronNode, Synapse)
#   utils/      (geo.js, s2.js)
#
# What stays vendored slim:
#   index.js   — DELIBERATELY NOT OVERWRITTEN.  The vendored version
#                omits the `./pubsub/*` re-exports (post.js uses
#                node:crypto which doesn't exist in browsers).  If
#                you change the upstream `index.js` non-pubsub
#                exports, manually reconcile.
#
# What gets skipped:
#   pubsub/    — post.js imports node:crypto and won't run in a
#                browser without a Web-Crypto port or an esbuild
#                bundling step.  Re-vendor pubsub once that lands.
#
# After running, commit the changed files.
# =====================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROTOCOL_SRC="${PROTOCOL_SRC:-${REPO_ROOT}/../axona-protocol/src}"
VENDOR_DST="${REPO_ROOT}/vendor/axona-protocol/src"

if [ ! -d "${PROTOCOL_SRC}" ]; then
  echo "✗ Source not found at ${PROTOCOL_SRC}"
  echo "  Either clone github.com/axona-net/axona-protocol as a"
  echo "  sibling directory, or set PROTOCOL_SRC=path/to/src"
  exit 1
fi

echo "→ Syncing from ${PROTOCOL_SRC}"
echo "  to            ${VENDOR_DST}"

mkdir -p "${VENDOR_DST}"
for dir in contracts dht utils; do
  if [ -d "${PROTOCOL_SRC}/${dir}" ]; then
    rm -rf "${VENDOR_DST}/${dir}"
    cp -r "${PROTOCOL_SRC}/${dir}" "${VENDOR_DST}/${dir}"
    echo "  ✓ ${dir}/"
  fi
done

echo
echo "→ Skipped (browser-incompatible):"
echo "  ✗ pubsub/  (post.js uses node:crypto)"
echo
echo "→ Preserved (slim subset, manual reconcile if needed):"
echo "  ◇ index.js"
echo
echo "Diff:"
( cd "${REPO_ROOT}" && git status --porcelain vendor/axona-protocol/ )
