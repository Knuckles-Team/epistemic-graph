#!/usr/bin/env bash
# The base a differential gate compares against: origin/main (MEDULLA-CONTRACT-R039).
# The gate measures the full accumulated change set against main, the same base
# the release workflow checks, so a duplicate pair split across incremental
# pushes cannot escape by comparing against the branch's own pushed tip.
#   1. CX_DUP_BASE_REF, when the caller names it explicitly;
#   2. origin/main.
# Comparing against HEAD (no base) makes a committed change invisible.
set -euo pipefail
for candidate in "${CX_DUP_BASE_REF:-}" "origin/main"; do
  [ -n "$candidate" ] || continue
  if sha="$(git rev-parse --verify --quiet "${candidate}^{commit}")"; then
    printf '%s\n' "$sha"
    exit 0
  fi
done
echo "gate-base-ref: no base found (CX_DUP_BASE_REF, origin/main)" >&2
exit 2
