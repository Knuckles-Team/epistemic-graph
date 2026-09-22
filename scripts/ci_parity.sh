#!/usr/bin/env bash
# Run the complete local validation profile, including manual-only heavy gates.
#
# `pre-commit run --config .config/pre-commit.yaml --all-files` runs only the COMMIT stage. The bounded
# pre-push tier and exhaustive manual tier are separate; this script runs all
# three explicitly. Hosted CI remains the exhaustive publication authority.
#
# This script is the one command that answers "would CI pass?".
set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

rc=0
echo "── commit-stage gates ─────────────────────────────────────────────"
pre-commit run --config .config/pre-commit.yaml --all-files || rc=1
echo
echo "── bounded pre-push gates ─────────────────────────────────────────"
pre-commit run --config .config/pre-commit.yaml --all-files --hook-stage pre-push || rc=1
echo
echo "── exhaustive manual gates ────────────────────────────────────────"
pre-commit run --config .config/pre-commit.yaml --all-files --hook-stage manual || rc=1

echo
if [ "$rc" -eq 0 ]; then
  echo "CI PARITY: PASS — all three validation tiers clean."
else
  echo "CI PARITY: FAIL — see the failing hook(s) above."
fi
exit "$rc"
