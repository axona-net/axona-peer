#!/usr/bin/env bash
# =====================================================================
# sync-protocol.sh — refresh vendor/axona-protocol/ from the canonical
# @axona/protocol source (github.com/axona-net/axona-protocol), with the
# peer's smoke suite as a hard gate.
#
# The kernel src/ tree (and std/, the app-layer standard library the
# client uses for std/message) are copied WHOLE. No hand-maintained file
# lists — the previous per-directory/per-file lists silently dropped
# connect.js when the kernel grew a new top-level module; a full-tree
# copy plus the diff -r completeness checks below makes that class of
# rot structurally impossible.
#
# Run from the repo root:  ./scripts/sync-protocol.sh
# Override the source:     PROTOCOL_SRC=/path/to/axona-protocol/src ./scripts/sync-protocol.sh
#
# After a green run, commit the changed vendor/ files (+ version bumps:
# package.json, PEER_VERSION in src/client.js, middle version in index.html).
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROTOCOL_SRC="${PROTOCOL_SRC:-${REPO_ROOT}/../axona-protocol/src}"
VENDOR_DST="${REPO_ROOT}/vendor/axona-protocol/src"
STD_SRC="$(dirname "${PROTOCOL_SRC}")/std"
STD_DST="$(dirname "${VENDOR_DST}")/std"

if [ ! -d "${PROTOCOL_SRC}" ]; then
  echo "✗ Source not found at ${PROTOCOL_SRC}"
  echo "  Clone github.com/axona-net/axona-protocol as a sibling directory,"
  echo "  or set PROTOCOL_SRC=path/to/src"
  exit 1
fi

# Vendoring copies whatever is on disk — including another session's
# uncommitted kernel work. Warn loudly when the source tree is dirty; to vendor
# a clean release state, extract it first:
#   git -C ../axona-protocol archive HEAD src std | tar -x -C /tmp/kernel-head
#   PROTOCOL_SRC=/tmp/kernel-head/src ./scripts/sync-protocol.sh
KERNEL_REPO="$(dirname "${PROTOCOL_SRC}")"
if git -C "${KERNEL_REPO}" rev-parse --is-inside-work-tree > /dev/null 2>&1; then
  if ! git -C "${KERNEL_REPO}" diff --quiet -- src std 2>/dev/null; then
    echo "⚠ WARNING: kernel working tree at ${KERNEL_REPO} has UNCOMMITTED changes"
    echo "  under src/ or std/ — you are about to vendor in-flight work:"
    git -C "${KERNEL_REPO}" status --porcelain src std | head -10
    echo "  (extract a committed state with git archive if that isn't intended)"
  fi
fi

echo "→ Syncing from ${PROTOCOL_SRC}"
echo "  to            ${VENDOR_DST}"

rm -rf "${VENDOR_DST}"
mkdir -p "$(dirname "${VENDOR_DST}")"
cp -R "${PROTOCOL_SRC}" "${VENDOR_DST}"

if [ -d "${STD_SRC}" ]; then
  rm -rf "${STD_DST}"
  cp -R "${STD_SRC}" "${STD_DST}"
fi

echo "→ Completeness check (vendored trees must mirror the kernel exactly)"
if ! diff -rq "${PROTOCOL_SRC}" "${VENDOR_DST}" > /dev/null; then
  echo "✗ vendored src/ differs from the kernel source after copy:"
  diff -rq "${PROTOCOL_SRC}" "${VENDOR_DST}" | head -20
  exit 1
fi
if [ -d "${STD_SRC}" ] && ! diff -rq "${STD_SRC}" "${STD_DST}" > /dev/null; then
  echo "✗ vendored std/ differs from the kernel source after copy:"
  diff -rq "${STD_SRC}" "${STD_DST}" | head -20
  exit 1
fi
echo "  ✓ trees identical"

KV=$(grep -oE "KERNEL_VERSION = '[^']+'" "${VENDOR_DST}/transport/handshake.js" || true)
echo "  ✓ vendored ${KV:-(kernel version unknown)}"

echo "→ npm test (re-vendor gate: transport/identity/version-gate/kernel-path smokes)"
if ! (cd "${REPO_ROOT}" && npm test); then
  echo ""
  echo "✗ SMOKE FAILED against the freshly vendored kernel — do NOT commit."
  echo "  vendor/ is left in the failing state for inspection; restore with:"
  echo "    git checkout vendor/"
  exit 1
fi

echo ""
echo "✓ sync + gate green. Commit the vendor/ changes (+ peer version bumps):"
( cd "${REPO_ROOT}" && git status --porcelain vendor/ 2>/dev/null | head -20 || true )
