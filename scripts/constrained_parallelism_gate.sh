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
#   scripts/constrained_parallelism_gate.sh                # default: 2 same-NUMA-node cores, --lib + extra integration binaries
#   EG_CONSTRAINED_CORES=0,1,2,3 scripts/constrained_parallelism_gate.sh
#   EG_CONSTRAINED_TIMEOUT=2400 scripts/constrained_parallelism_gate.sh    # raise the suite deadline (default 1200s)
#   EG_CONSTRAINED_TEST_TIMEOUT=1200 scripts/constrained_parallelism_gate.sh # raise one-test watchdog (default 900s)
#   EG_CONSTRAINED_TERM_GRACE=45 EG_CONSTRAINED_KILL_GRACE=15 scripts/constrained_parallelism_gate.sh
#   EG_CONSTRAINED_EXTRA_TESTS=0 scripts/constrained_parallelism_gate.sh   # --lib ONLY (fast local loop, not for CI/pre-push)
#   EG_CONSTRAINED_FILTER='write_coalescer::' scripts/constrained_parallelism_gate.sh  # scope to one area
#
# Coordinate with `rm_gates(action=run, stage=heavy)` (feat/rm-gates,
# in-flight sibling lane): this belongs in the heavy tier as one more
# pre-push check, not as a second, parallel enforcement mechanism.
#
# CORE SELECTION (fix/txn-snapshot-phantom-deadlock, 2026-08-15): the old
# hardcoded default `EG_CONSTRAINED_CORES=0,1` picks logical CPUs 0 and 1
# LITERALLY, with no regard for topology. On a single-socket box (a real
# 2-vCPU CI runner, most laptops) that is harmless -- CPU 0 and CPU 1 are the
# only two CPUs there is. On a multi-socket dev/build host it is NOT: e.g. a
# 4-socket Xeon E5-4620 (`lscpu -p=CPU,NODE`) puts CPU 0 on NUMA node 0 and
# CPU 1 on NUMA node 1 -- literally different sockets, different memory
# controllers, cache coherence over QPI. That is a materially different (and
# strictly harsher) scheduling regime than "2 vCPUs of a small VM", which is
# what this gate exists to emulate (see the file header). It was the prime
# suspect after a real-world hang was observed running this gate on exactly
# such a host (`server::tests::txn_snapshot_allows_phantom`, stuck ~48min,
# all threads parked in `futex_do_wait`/`ep_poll`, zero forward progress) --
# investigated on `fix/txn-snapshot-phantom-deadlock`: the SAME command
# (`taskset -c 0,1 cargo test -p epistemic-graph --features full --lib
# --no-fail-fast`) was re-run 6 times on the same host, 3 uncontended and 3
# while 4 extra `taskset -c 0,1`-pinned busy-loops deliberately hammered
# those exact 2 cores, plus the single hanging test in isolation 5 more
# times (--test-threads=1) -- 0/11 reproductions. The test itself is fully
# sequential (no spawned/concurrent tasks; GOC-70 rule 3 does not apply --
# there is no scheduler-dependent race to construct deterministically), so a
# genuine lock-ordering defect reachable through its one linear call chain
# would be expected to reproduce on EVERY run, not 0 of 11 under deliberate
# adversarial contention on the very cores in question. That -- plus the
# confirmed cross-socket pinning this host's "0,1" produces -- points at a
# host-level anomaly coincident with (not caused by) this test, most likely
# transient memory/swap pressure or scheduler pathology from the OTHER heavy
# cargo/rustc jobs that regularly co-run on shared build hosts here (see
# workspace CLAUDE.md / `systemd-oomd-kills-tmux-scope`), not a product
# defect -- so the test is UNCHANGED. What we can and must fix is the gate:
# (1) default to two CPUs confirmed to share one NUMA node, so "2-core
# affinity" actually means what the header comment says it means, and (2)
# never let this gate block a host indefinitely again, regardless of root
# cause -- see the bounded process-group runner below. The runner also has a
# per-libtest-case watchdog and emits deterministic thread/fd teardown
# evidence, so a hung test cannot leave cargo descendants behind.
set -uo pipefail
cd "$(dirname "$0")/.."

# Pick two logical CPUs that share a NUMA node when topology is discoverable
# (same-socket, the faithful analog of a small single-socket CI VM);
# otherwise fall back to the literal "0,1" default. On a single-node host
# (or one `lscpu` can't introspect) this is a no-op -- 0,1 already satisfies
# it or there is nothing better to compute.
default_cores() {
  if command -v lscpu >/dev/null 2>&1; then
    same_node_pair=$(lscpu -p=CPU,NODE 2>/dev/null | grep -v '^#' | awk -F, '
      { if (!($2 in seen)) { seen[$2] = $1 } else if (!done[$2]) { print seen[$2] "," $1; done[$2] = 1; exit } }
    ')
    if [ -n "$same_node_pair" ]; then
      echo "$same_node_pair"
      return
    fi
  fi
  echo "0,1"
}

CORES="${EG_CONSTRAINED_CORES:-$(default_cores)}"
TARGET_DIR="${CARGO_TARGET_DIR:-target-isolated}"
export CARGO_TARGET_DIR="$TARGET_DIR"

# Bound EVERY constrained `cargo test` invocation below with a hard wall-clock
# ceiling. A gate that can hang forever is a gate people stop running (see
# this file's header investigation): whatever the cause -- a real product
# deadlock, or the host-level contention this program's evidence points to --
# this script must fail loudly and release the host within a bounded time,
# never sit silent for 48 minutes. 1200s (20min) is chosen against the
# measured baselines on the reference host during that investigation: ~24-27s
# uncontended, ~47-73s under deliberate 4-way same-core contention -- 20min is
# a >15x margin over the worst measured figure, and still far below the
# point where a genuinely wedged run would be indistinguishable from "just
# slow." Override with EG_CONSTRAINED_TIMEOUT for a slower/smaller runner;
# never remove the bound.
TIMEOUT_SECS="${EG_CONSTRAINED_TIMEOUT:-1200}"
TEST_TIMEOUT_SECS="${EG_CONSTRAINED_TEST_TIMEOUT:-900}"
TERM_GRACE_SECS="${EG_CONSTRAINED_TERM_GRACE:-30}"
KILL_GRACE_SECS="${EG_CONSTRAINED_KILL_GRACE:-10}"
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 is not installed -- bounded lifecycle gate refuses to run unbounded." >&2
  exit 2
fi

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
echo "== lifecycle bounds: suite=${TIMEOUT_SECS}s test=${TEST_TIMEOUT_SECS}s TERM=${TERM_GRACE_SECS}s KILL=${KILL_GRACE_SECS}s =="

# Keep the test command unchanged while giving every invocation a private
# process group, an absolute suite deadline, a per-test deadline, and bounded
# TERM/grace/KILL containment. The wrapper preserves cargo's return code and
# never retries, ignores, or rewrites an assertion failure.
bounded_test() {
  local suite_name="$1"
  shift
  python3 scripts/bounded_test_runner.py \
    --suite-name "$suite_name" \
    --suite-timeout "$TIMEOUT_SECS" \
    --test-timeout "$TEST_TIMEOUT_SECS" \
    --term-grace "$TERM_GRACE_SECS" \
    --kill-grace "$KILL_GRACE_SECS" \
    -- "$@"
}

echo "-- step 1/2: unconstrained build (full host parallelism; compile cost must not be paid under 2-core affinity) --"
if ! bounded_test "constrained-lib-build" cargo test --no-run -p epistemic-graph --features full --lib; then
  echo "FAIL: build failed before constrained execution even started." >&2
  exit 1
fi

echo "-- step 2/2: running --lib suite under taskset -c $CORES (bounded to ${TIMEOUT_SECS}s) --"
# --no-fail-fast: a fail-fast run stops at the first failure across binaries,
# which would under-report exactly the class this gate exists to find.
# The runner sends TERM at the deadline and KILL after the configured grace if
# a genuinely wedged process (all threads parked, for example) ignores TERM.
if bounded_test "constrained-lib" taskset -c "$CORES" cargo test -p epistemic-graph --features full --lib --no-fail-fast; then
  constrained_rc=0
else
  constrained_rc=$?
fi

if [ "$constrained_rc" -eq 124 ] || [ "$constrained_rc" -eq 137 ]; then
  cat >&2 <<EOF

FAIL: the lib suite did not finish within ${TIMEOUT_SECS}s under $n_cores-core
CPU affinity (cores $CORES) -- contained in its private process group, not a
test assertion failure. This
gate used to be able to hang silently and indefinitely instead of failing;
see this file's header (fix/txn-snapshot-phantom-deadlock investigation) for
why that changed. A timeout here means one of:
  - a genuine product deadlock reachable only under this constraint -- get a
    thread backtrace of the NEXT hang before it's killed: run this script's
    inner command by hand, and while it's stuck, \`sudo gdb -p <pid> -batch
    -ex "thread apply all bt"\` (or temporarily
    \`sudo sysctl -w kernel.yama.ptrace_scope=0\`, then restore it);
  - host-level contention/pressure (co-running heavy builds, swap) -- retry
    on a quieter host/time before assuming a product defect;
  - this host's CORES selection is unrepresentative -- check \`lscpu
    -p=CPU,NODE\` for the cores in \`$CORES\`; they should share a NUMA node.
Do NOT raise EG_CONSTRAINED_TIMEOUT to make this go away without first
establishing which of the above it is -- that masks the same class of defect
this gate exists to catch.
EOF
  exit "$constrained_rc"
elif [ "$constrained_rc" -ne 0 ]; then
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
  if ! bounded_test "constrained-extra-build" cargo test --no-run -p epistemic-graph --features full "${build_args[@]}"; then
    echo "FAIL: extra-target build failed." >&2
    exit 1
  fi
  if bounded_test "constrained-extra" taskset -c "$CORES" cargo test -p epistemic-graph --features full "${build_args[@]}" --no-fail-fast; then
    extra_rc=0
  else
    extra_rc=$?
  fi
  if [ "$extra_rc" -eq 124 ] || [ "$extra_rc" -eq 137 ]; then
    cat >&2 <<EOF

FAIL: the extra integration-test binaries did not finish within
${TIMEOUT_SECS}s under $n_cores-core CPU affinity (cores $CORES) -- contained,
not a test assertion failure. The bounded runner emitted process-group,
thread, fd, and teardown evidence above. See the --lib tier's identical-shaped timeout
message above for how to get a backtrace from the next hang and how to tell
a host-contention artifact from a real product deadlock before touching
EG_CONSTRAINED_TIMEOUT.
EOF
    exit "$extra_rc"
  elif [ "$extra_rc" -ne 0 ]; then
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
    exit "$extra_rc"
  fi
  echo "== PASS: full lib suite + extra integration binaries green under $n_cores-core CPU affinity (cores $CORES) =="
else
  echo "== PASS: full lib suite green under $n_cores-core CPU affinity (cores $CORES) [EG_CONSTRAINED_EXTRA_TESTS=0: integration binaries SKIPPED, not for CI/pre-push] =="
fi
