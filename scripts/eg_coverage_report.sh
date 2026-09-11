#!/usr/bin/env bash
# Standing coverage-instrumentation runner for epistemic-graph (CONCEPT: eg
# coverage instrument, one of four measurement lanes alongside wiring gates,
# contract conformance, and mutation testing -- see the task that created
# this script). Produces a region/line/branch-ish (function-granularity)
# baseline via `cargo-llvm-cov` and a ranked "what never executed" report
# via `scripts/eg_coverage_zero_report.py`.
#
# This is a MEASUREMENT tool, not a gate: it is deliberately NOT wired into
# pre-commit and carries no --fail-under threshold. A bare coverage-percent
# ratchet is against this project's rules (CLAUDE.md "NO RATCHETS"); the
# useful signal is the zero-coverage list this script produces, reviewed by
# a human, not a number a hook can silently game.
#
# WHERE TO RUN: cargo-llvm-cov instrumented builds are substantially larger
# and slower than normal ones, and this workspace's own convention is to
# run heavy Rust builds on the R820 build host, never the interactive dev
# host (see AGENTS.md "constrained-parallelism gate" / CLAUDE.md build-host
# note). This script does not fail if run elsewhere, but the default
# CARGO_TARGET_DIR below assumes R820's local scratch disk layout -- override
# it if that assumption is wrong for your host.
#
# REQUIREMENTS: `cargo install cargo-llvm-cov` on the toolchain in
# rust-toolchain.toml, plus that toolchain's `llvm-tools-preview` component
# (`rustup component add llvm-tools-preview --toolchain <pinned>`).
#
# USAGE:
#   scripts/eg_coverage_report.sh [lane ...]
#     lane in {contract-core, epistemic-graph-lib, workspace}, default:
#     "contract-core epistemic-graph-lib"
#
# ENV:
#   EG_COV_OUT        output directory (default: /var/tmp/l9/eg-coverage-<date>)
#   EG_COV_JOBS        cargo -j (default: 4 -- see AGENTS.md "count cargo
#                       lanes not lanes"; NEVER raise this on a shared build
#                       host without checking `uptime` first)
#   EG_COV_TOP         how many ranked zero-coverage entries to list (default: 30)
#
# Each lane is collected with `--no-report` (one test run, no report yet)
# then reported three ways (lcov, json, html) from the SAME cached profile
# data via `cargo llvm-cov report`, so multi-format output never re-runs
# the suite. `--ignore-run-fail` is used deliberately: main is known to
# carry red tests independent of this instrument (see
# merge-queue-differential-gating note) and a red test must not silently
# suppress the coverage report for the code around it.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  cat >&2 <<'EOF'
FAIL: cargo-llvm-cov is not installed.

Install it on the build host, against the pinned toolchain (see
rust-toolchain.toml):
  rustup component add llvm-tools-preview --toolchain <pinned-channel>
  cargo install cargo-llvm-cov --locked

Then re-run this script.
EOF
  exit 2
fi

JOBS="${EG_COV_JOBS:-4}"
TOP="${EG_COV_TOP:-30}"
OUT="${EG_COV_OUT:-/var/tmp/l9/eg-coverage-$(date +%Y%m%d)}"
mkdir -p "$OUT/raw"

# NEVER silently proceed on a starved fd table -- see AGENTS.md /
# CLAUDE.md "ulimit -n 65536 is mandatory on R820" note; the default 1024
# produces ~86 phantom "Too many open files" failures across this suite.
CURRENT_NOFILE="$(ulimit -n)"
if [[ "$CURRENT_NOFILE" -lt 65536 ]]; then
  ulimit -n 65536 2>/dev/null || echo "WARN: could not raise ulimit -n above $CURRENT_NOFILE; expect possible phantom fd-exhaustion failures" >&2
fi

LANES=("$@")
if [[ ${#LANES[@]} -eq 0 ]]; then
  LANES=(contract-core epistemic-graph-lib)
fi

LCOV_FILES=()

run_lane() {
  # $1 = lane name, $2 = space-separated PACKAGE-SELECTION args (-p foo -p
  # bar, or --workspace) -- these must be repeated on every `cargo llvm-cov
  # report` call too, not just the test run, because `report` invoked
  # standalone otherwise falls back to the current directory's default
  # package and silently reports zero files (confirmed empirically: the
  # first run of this script reported TOTAL=0/0 everywhere until the -p
  # flags were repeated on `report`). $3.. = TEST-ONLY args (--lib,
  # --all-targets, ...), which `report` does not accept.
  local name="$1"
  local pkg_args_str="$2"
  shift 2
  local test_only_args=("$@")
  # shellcheck disable=SC2206 -- intentional word-splitting of a flag string
  local pkg_args=($pkg_args_str)
  echo "=== lane: $name -- $(date -Is) ==="
  cargo llvm-cov --locked -j"$JOBS" "${pkg_args[@]}" "${test_only_args[@]}" --ignore-run-fail --no-report
  cargo llvm-cov report "${pkg_args[@]}" --lcov --output-path "$OUT/raw/$name.lcov"
  cargo llvm-cov report "${pkg_args[@]}" --json --output-path "$OUT/raw/$name.json"
  cargo llvm-cov report "${pkg_args[@]}" --html --output-dir "$OUT/raw/$name-html"
  cargo llvm-cov report "${pkg_args[@]}" > "$OUT/raw/$name-summary.txt"
  LCOV_FILES+=("$OUT/raw/$name.lcov")
  echo "=== lane: $name DONE -- $(date -Is) ==="
}

for lane in "${LANES[@]}"; do
  case "$lane" in
    contract-core)
      # The crates the contract freeze cares about most: eg-types (owns the
      # agent_library/agent_graph/agent_component/agent_template/delegation
      # wire types), eg-storage, eg-capabilities (the method-policy ledger).
      run_lane contract-core "-p eg-types -p eg-storage -p eg-capabilities"
      ;;
    epistemic-graph-lib)
      # The root package's own agent/delegation modules live under
      # src/server/persistence/{agent_graph,agent_library,agent_component,
      # agent_template}.rs and src/server/handlers/delegation.rs, all
      # covered by inline `#[cfg(test)]` unit tests reachable via `--lib`.
      # Deliberately scoped to `--lib` (not `--tests`, which would also
      # build+run the ~41 separate integration-test binaries under tests/
      # -- each its own from-scratch link of the full `full`-featured
      # crate, the dominant cost in the "226 test binaries" scale warning)
      # to keep this lane tractable as a routine, repeatable run.
      run_lane epistemic-graph-lib "-p epistemic-graph" --lib
      ;;
    workspace)
      # Full workspace, every target. Expensive -- see the scale warning
      # in the task/report that created this script. Run explicitly, not
      # by default.
      run_lane workspace "--workspace" --all-targets
      ;;
    *)
      echo "unknown lane: $lane (expected contract-core|epistemic-graph-lib|workspace)" >&2
      exit 2
      ;;
  esac
done

LCOV_ARGS=()
for f in "${LCOV_FILES[@]}"; do
  LCOV_ARGS+=(--lcov "$f")
done

python3 scripts/eg_coverage_zero_report.py \
  "${LCOV_ARGS[@]}" \
  --root crates/eg-types --root crates/eg-storage --root crates/eg-capabilities --root crates/eg-core --root src \
  --top "$TOP" \
  --out "$OUT/ZERO-COVERAGE-REPORT.md"

echo "Report written under $OUT"
echo "DONE_rc=0"
