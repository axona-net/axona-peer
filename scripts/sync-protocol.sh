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
#   pubsub/     (AxonManager, AxonPubSub, post.js (Web Crypto), ed25519.js)
#   utils/      (geo.js, s2.js)
#   index.js    (barrel)
#
# v0.6.0 — pubsub is now browser-safe.  post.js was ported to Web
# Crypto (async sha256 + Ed25519); ed25519.js is the new Web Crypto
# Ed25519 helper.  See axona-protocol v1.0.0 changelog.
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
for dir in contracts dht pubsub utils; do
  if [ -d "${PROTOCOL_SRC}/${dir}" ]; then
    rm -rf "${VENDOR_DST}/${dir}"
    cp -r "${PROTOCOL_SRC}/${dir}" "${VENDOR_DST}/${dir}"
    echo "  ✓ ${dir}/"
  fi
done

cp "${PROTOCOL_SRC}/index.js" "${VENDOR_DST}/index.js"
echo "  ✓ index.js"
echo
echo "Diff:"
( cd "${REPO_ROOT}" && git status --porcelain vendor/axona-protocol/ )
