#!/usr/bin/env bash
# Bounded R820/manual all-feature workspace validation.
#
# This is intentionally a thin command-preserving adapter: cargo still owns
# test selection and all failures remain failures. The Python runner supplies
# the suite/test deadlines, private child process group, bounded
# TERM/grace/KILL cleanup, and machine-readable lifecycle evidence.
set -euo pipefail
cd "$(dirname "$0")/.."

SUITE_TIMEOUT="${EG_ALL_FEATURE_SUITE_TIMEOUT:-5400}"
TEST_TIMEOUT="${EG_ALL_FEATURE_TEST_TIMEOUT:-900}"
TERM_GRACE="${EG_ALL_FEATURE_TERM_GRACE:-30}"
KILL_GRACE="${EG_ALL_FEATURE_KILL_GRACE:-10}"

exec python3 scripts/bounded_test_runner.py \
  --suite-name "r820-all-features-workspace" \
  --suite-timeout "$SUITE_TIMEOUT" \
  --test-timeout "$TEST_TIMEOUT" \
  --term-grace "$TERM_GRACE" \
  --kill-grace "$KILL_GRACE" \
  -- cargo test --workspace --all-features --no-fail-fast "$@"
