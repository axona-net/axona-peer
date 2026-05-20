#!/usr/bin/env bash
# =====================================================================
# sync-protocol.sh — refresh vendor/axona-protocol/ from the canonical
# @axona/protocol source at github.com/axona-net/axona-protocol.
#
# Run from the repo root:  ./scripts/sync-protocol.sh
#
# What gets copied:
#   contracts/    (all 5 contract files)
#   dht/          (AxonaPeer, DHTNode, NeuronNode, Synapse, Subscription)
#   identity/     (deriveIdentity, dumpIdentity, loadIdentity, nodeId math)
#   persistence/  (PersistenceAdapter + InMemory/IndexedDB/File impls)
#   pubsub/       (AxonManager, AxonPubSub, post.js (Web Crypto), ed25519.js,
#                  envelope.js)
#   transport/    (handshake.js, wire.js, sim/, web/, node/)
#   utils/        (geo.js, hexid.js, s2.js)
#   errors.js     (typed error hierarchy)
#   index.js      (barrel)
#
# v1.0.0-rc.0 — full kernel.  Adds 264-bit hex IDs (utils/hexid.js),
# typed error hierarchy (errors.js), pubkey-derived identities
# (identity/), pluggable persistence (persistence/), and the three
# transport flavors (transport/{sim,web,node}/).  AxonaPeer surface
# now includes the unified pub/sub/pull/metrics/direct-messaging API.
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
for dir in contracts dht identity persistence pubsub transport utils; do
  if [ -d "${PROTOCOL_SRC}/${dir}" ]; then
    rm -rf "${VENDOR_DST}/${dir}"
    cp -r "${PROTOCOL_SRC}/${dir}" "${VENDOR_DST}/${dir}"
    echo "  ✓ ${dir}/"
  fi
done

for f in index.js errors.js; do
  if [ -f "${PROTOCOL_SRC}/${f}" ]; then
    cp "${PROTOCOL_SRC}/${f}" "${VENDOR_DST}/${f}"
    echo "  ✓ ${f}"
  fi
done
echo
echo "Diff:"
( cd "${REPO_ROOT}" && git status --porcelain vendor/axona-protocol/ )
