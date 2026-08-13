#!/usr/bin/env bash
# GOC-70 constrained-parallelism verification gate.
#
# WHY THIS EXISTS: epistemic-graph 2.25.0 shipped with all six gates verified
# green on a 64-core dev host, and CI (a 2-core runner) failed anyway.
# `server::tests::dispatch_coalesces_concurrent_writes_to_one_graph` spawned
# 400 concurrent writes and asserted every one landed through a specific
# coalescer counter path -- true when the scheduler genuinely overlaps 400
# tasks across many cores, false when a handful of tokio workers contend for
# 2 CPUs and uncontended writes take a different (still correct) path. CI
# runners are the REFERENCE environment, not a degraded one: our build hosts
# are the outlier, and because we verify on them, they systematically hide
# this entire defect class (timing/scheduling/core-count-dependent
# assertions). See plans/graph-os-completion-program/GOC-59-67-EXPANSION-
# TRACKS.md, GOC-70.
#
# WHAT THIS DOES: restricts CPU affinity to 2 cores (`taskset -c 0,1` by
# default, override with EG_CONSTRAINED_CORES) and runs the lib test suite
# under that constraint -- the same mechanism (fewer schedulable cores,
# tokio workers genuinely contending) that broke 2.25.0 in CI, reproducible
# on any dev box regardless of its real core count. This is NOT a substitute
# for a real small CI runner (GOC-70 also requires keeping one in the CI
# matrix) -- it is the cheap, routine, pre-push-tier check that catches the
# class BEFORE a push, at a cost of minutes rather than a failed release.
#
# WHY THE BUILD ISN'T ALSO CONSTRAINED: the defect class is about RUNTIME
# scheduling (tokio workers contending for cores while a test executes), not
# compilation. Restricting `rustc`/the linker to 2 cores as well would turn a
# multi-minute compile into a much longer one for no additional signal, which
# would price this gate out of "routine". So: build once, unconstrained (full
# parallelism, warms/reuses the incremental cache), then re-invoke `cargo
# test` under the CPU-affinity restriction -- cargo's own up-to-date check
# means the second invocation does no meaningful recompilation, only linking
# as needed and running the already-built test binaries under the constraint.
#
# SCOPE: `--lib` runs every unit test under src/ -- which is where BOTH known
# instances of this defect class live (`write_coalescer::tests::
# concurrent_writes_coalesce_into_fewer_lock_acquisitions`,
# `server::tests::dispatch_coalesces_concurrent_writes_to_one_graph`) and
# where every `#[tokio::test(flavor = "multi_thread", worker_threads = N)]`
# test in this crate's own module tree is defined. It is the same "full lib
# suite" (1024 tests) the fix report for this lane measures as its
# regression baseline.
#
# CHANGED (fix/ambient-env-test-dependency): the concurrency-sensitive
# integration-test binaries under tests/ (pgwire/mysql/mssql roundtrip,
# advanced_crossmodal_roundtrip, etc.) now run BY DEFAULT, in this same
# pre-push-tier invocation -- not behind an opt-in flag. That opt-in
# (`EG_CONSTRAINED_EXTRA_TESTS=1`, "periodic/CI-only deeper sweep rather than
# every push") is exactly why a real defect in this class reached CI instead
# of being caught before the push: `advanced_crossmodal_roundtrip.rs`'s
# `plan_writeback_stages_and_commits_inferred_edges_atomically_d7` depended on
# ambient process-global env state another test in the same binary mutated
# (`EPISTEMIC_GRAPH_ENCRYPTION_KEY` -- see that file's `state()` doc), which
# only manifests under exactly the kind of low-core scheduling this gate
# exists to reproduce -- and this gate never ran it by default. Measured cost
# of including these binaries (`--features full`, `--test <name>` x7, build +
# `taskset -c 0,1` run) on a 24-core host: **the extra tier adds ~2-3 minutes
# on top of the `--lib` tier's ~4-5 minutes** (see the fix report for exact
# numbers) -- well inside "routine pre-push", not the rare "periodic/CI-only"
# cost the old comment assumed. Opt OUT with `EG_CONSTRAINED_EXTRA_TESTS=0` if
# a narrower, faster local iteration loop is needed; CI and the merge queue
# must never set that.
#
# USAGE:
#   scripts/constrained_parallelism_gate.sh                # default: 2 cores, --lib + extra integration binaries
#   EG_CONSTRAINED_CORES=0,1,2,3 scripts/constrained_parallelism_gate.sh
#   EG_CONSTRAINED_EXTRA_TESTS=0 scripts/constrained_parallelism_gate.sh   # --lib ONLY (fast local loop, not for CI/pre-push)
#   EG_CONSTRAINED_FILTER='write_coalescer::' scripts/constrained_parallelism_gate.sh  # scope to one area
#
# Coordinate with `rm_gates(action=run, stage=heavy)` (feat/rm-gates,
# in-flight sibling lane): this belongs in the heavy tier as one more
# pre-push check, not as a second, parallel enforcement mechanism.
set -uo pipefail
cd "$(dirname "$0")/.."

CORES="${EG_CONSTRAINED_CORES:-0,1}"
TARGET_DIR="${CARGO_TARGET_DIR:-target-isolated}"
export CARGO_TARGET_DIR="$TARGET_DIR"

if ! command -v taskset >/dev/null 2>&1; then
  cat >&2 <<EOF
FAIL: taskset is not installed -- constrained-parallelism verification cannot run.

This gate exists because epistemic-graph 2.25.0 shipped with all gates green
on a 64-core host and CI (2 cores) failed. Skipping it silently would recreate
exactly that gap. Install taskset (util-linux; on Debian/Ubuntu:
'apt-get install -y util-linux') or run this on a Linux host, then re-run:
  scripts/constrained_parallelism_gate.sh

This is a hard failure, not a skip -- we never report a pass we didn't verify.
EOF
  exit 2
fi

n_cores=$(($(echo "$CORES" | tr ',' '\n' | wc -l)))
echo "== GOC-70 constrained-parallelism gate =="
echo "== environment: CPU affinity restricted to cores [$CORES] ($n_cores logical cores) =="
echo "== host reports $(nproc) cores total; this run deliberately does not use them all =="

echo "-- step 1/2: unconstrained build (full host parallelism; compile cost must not be paid under 2-core affinity) --"
if ! cargo test --no-run -p epistemic-graph --features full --lib; then
  echo "FAIL: build failed before constrained execution even started." >&2
  exit 1
fi

echo "-- step 2/2: running --lib suite under taskset -c $CORES --"
# --no-fail-fast: a fail-fast run stops at the first failure across binaries,
# which would under-report exactly the class this gate exists to find.
if taskset -c "$CORES" cargo test -p epistemic-graph --features full --lib --no-fail-fast; then
  constrained_rc=0
else
  constrained_rc=$?
fi

if [ "$constrained_rc" -ne 0 ]; then
  cat >&2 <<EOF

FAIL: the lib suite failed under $n_cores-core CPU affinity but (presumably)
passes on this host's full core count. Per GOC-70: a test that requires a
large machine is a DEFECTIVE TEST, not a machine requirement. Do not:
  - mark it #[ignore]
  - raise timeouts blindly without justifying the new value
  - delete or weaken the assertion just to make it pass

Instead: read the failure, identify whether it asserts a timing/scheduling/
core-count-dependent property (rewrite to assert what's true regardless of
scheduling), needs deterministically-constructed contention (barrier/lock/
gate instead of spawn-and-hope), or has a resource assertion contaminated by
a process-wide/absolute reading (switch to a per-test baseline delta). See
plans/graph-os-completion-program/GOC-59-67-EXPANSION-TRACKS.md, GOC-70.
EOF
  exit "$constrained_rc"
fi

# Default ON (see the header comment: this used to be an opt-in
# `EG_CONSTRAINED_EXTRA_TESTS=1` "periodic/CI-only" sweep, which is exactly why
# a real ambient-process-state defect in one of these binaries
# (`advanced_crossmodal_roundtrip.rs`) reached CI instead of failing pre-push).
# Only skip this tier with an explicit `EG_CONSTRAINED_EXTRA_TESTS=0` for a
# fast local iteration loop -- never in CI or the merge queue.
if [ "${EG_CONSTRAINED_EXTRA_TESTS:-1}" != "0" ]; then
  echo "-- extra: concurrency-sensitive integration test binaries under taskset -c $CORES --"
  EXTRA_TESTS="pgwire_roundtrip mysql_roundtrip mssql_roundtrip advanced_crossmodal_roundtrip incremental_server_indexes txn_recovery_key_decoupled_d_orc_50 external_compute_e2e"
  build_args=()
  for t in $EXTRA_TESTS; do build_args+=(--test "$t"); done
  if ! cargo test --no-run -p epistemic-graph --features full "${build_args[@]}"; then
    echo "FAIL: extra-target build failed." >&2
    exit 1
  fi
  if ! taskset -c "$CORES" cargo test -p epistemic-graph --features full "${build_args[@]}" --no-fail-fast; then
    cat >&2 <<EOF

FAIL: an integration-test binary failed under $n_cores-core CPU affinity but
(presumably) passes on this host's full core count. Same GOC-70 edict as the
--lib tier above: a test that requires a large machine is a DEFECTIVE TEST.
If the failure is a process-global env var (or other ambient process state)
one test mutates and another test in the SAME binary reads without holding
the same lock, that is exactly the class this gate was widened to catch --
see \`tests/advanced_crossmodal_roundtrip.rs\`'s \`ENC_ENV_LOCK\` for the
serialization pattern to copy, not \`--test-threads=1\` (that would mask the
defect class, not fix it).
EOF
    exit 1
  fi
  echo "== PASS: full lib suite + extra integration binaries green under $n_cores-core CPU affinity (cores $CORES) =="
else
  echo "== PASS: full lib suite green under $n_cores-core CPU affinity (cores $CORES) [EG_CONSTRAINED_EXTRA_TESTS=0: integration binaries SKIPPED, not for CI/pre-push] =="
fi
