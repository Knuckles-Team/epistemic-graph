#!/usr/bin/env bash
# Thin wrapper around ci_gate_replica.py — see that file's module docstring
# for the design (parses .github/workflows/release.yml, derives which steps
# to run vs. skip, never hand-copies the step list).
#
# Registered as the manual-only `ci-gate-replica` hook in
# .config/pre-commit.yaml. Run it directly with:
#   scripts/ci_gate_replica.sh                # full run (heavy)
#   scripts/ci_gate_replica.sh --dry-run       # print the plan only
#   scripts/ci_gate_replica.sh --consistency-check   # anti-drift check only
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 90
exec python3 scripts/ci_gate_replica.py "$@"
