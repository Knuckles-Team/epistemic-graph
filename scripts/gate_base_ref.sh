#!/usr/bin/env bash
# The base a differential gate compares against: the remote tip a push would
# replace, i.e. what hosted CI sees as `github.event.before`.
#   1. CX_DUP_BASE_REF, when the caller names it;
#   2. PRE_COMMIT_FROM_REF (pre-push: the remote ref's current tip), unless it is
#      the all-zero id of a new remote branch;
#   3. @{upstream} of the current branch;
#   4. origin/main.
# Comparing against HEAD (no base) makes a committed change invisible: after a
# commit the before and after trees are identical (train 2: 740 NEW pairs in CI,
# 0 locally).
set -euo pipefail
for candidate in "${CX_DUP_BASE_REF:-}" "${PRE_COMMIT_FROM_REF:-}" "@{upstream}" "origin/main"; do
  case "$candidate" in "" | 0000000000000000000000000000000000000000) continue ;; esac
  if sha="$(git rev-parse --verify --quiet "${candidate}^{commit}")"; then
    printf '%s\n' "$sha"
    exit 0
  fi
done
echo "gate-base-ref: no base found (CX_DUP_BASE_REF, pre-push ref, @{upstream}, origin/main)" >&2
exit 2
