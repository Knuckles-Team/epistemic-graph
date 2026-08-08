#!/usr/bin/env bash
# Run EVERY gate CI runs, including the heavy ones.
#
# `pre-commit run --all-files` runs only the COMMIT stage. Five hooks here are
# staged pre-push/manual (cargo-test-full, cargo-clippy-all-features,
# replay-determinism, wheel-smoke, pytest) — precisely the expensive ones that
# mirror CI's slowest, most failure-prone jobs. So a green `--all-files` run
# says nothing about `Test (facade full)`, which is where CI actually breaks.
#
# This script is the one command that answers "would CI pass?".
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

rc=0
echo "── commit-stage gates ─────────────────────────────────────────────"
pre-commit run --all-files || rc=1
echo
echo "── pre-push-stage gates (the heavy CI mirrors) ────────────────────"
pre-commit run --all-files --hook-stage pre-push || rc=1

echo
if [ "$rc" -eq 0 ]; then
  echo "CI PARITY: PASS — both stages clean."
else
  echo "CI PARITY: FAIL — see the failing hook(s) above."
fi
exit "$rc"
