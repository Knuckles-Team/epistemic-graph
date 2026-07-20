#!/usr/bin/env bash
# ============================================================================
# validate-raft-cluster.sh — prove the Raft cluster MECHANISM (CONCEPT:KG-2.273)
# ============================================================================
# Runs the openraft-0.10 Raft cluster proofs on FRESH, THROWAWAY persist dirs and
# loopback TCP listeners — it never touches any configured deployment or authoritative
# data. The proofs are in-process multi-node clusters (each node a real
# `MultiRaft` with its own redb under a fresh temp dir, talking real TCP on 127.0.0.1),
# which is exactly the cluster mechanism the production deploy uses — just co-located.
#
# What it proves:
#   1. FORMATION + REPLICATION + FAILOVER — a 3-node cluster forms, a write to the
#      leader replicates to followers (read back on a survivor), and killing the leader
#      elects a new one with NO data loss.
#   2. MEMBERSHIP JOIN + GRACEFUL TRANSFER — a group grows from 1→3 nodes
#      (add_learner → change_membership) and the leader balancer hands leadership to the
#      round-robin target via the NATIVE openraft-0.10 `trigger().transfer_leader()`.
#   3. DURABLE LOG — the redb-backed Raft log replays its tail after a restart.
#   4. MULTI-GROUP ISOLATION + SCOPED SNAPSHOTS — groups on one shared redb don't
#      collide; per-group snapshots are tenant-scoped.
#
# Usage:  scripts/validate-raft-cluster.sh
# Env:    CARGO_FEATURES (default "full,cluster"); RUST_LOG for engine traces.
# Exit:   0 = all proofs green; non-zero = a proof failed (see output).
# ----------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

FEATURES="${CARGO_FEATURES:-full,cluster}"

# The cluster proofs, grouped by the property each demonstrates. All run under
# `--lib raft` with `--features full,cluster` (so `--features raft,harness` extras stay
# out of the default set).
TESTS=(
  raft::tests::three_node_cluster_replicates_and_survives_leader_failover
  raft::tests::multi_node_group_join_then_leader_rebalance
  raft::tests::durable_log_replays_from_redb_after_restart
  raft::tests::fault_injection_no_committed_log_entry_lost_on_restart
  raft::tests::multi_group_logs_isolate_on_shared_redb
  raft::tests::two_groups_one_node_commit_independently
  raft::tests::group_snapshot_is_scoped_to_its_tenant_range
)

echo "== Raft cluster validation (openraft 0.10, CONCEPT:KG-2.273) =="
echo "features: ${FEATURES}"
echo "fresh temp redb dirs + loopback TCP — configured deployment data is not touched."
echo

# Build the test binary once (so timing the proofs is not dominated by compile).
echo "-- building test harness --"
cargo test --features "${FEATURES}" --lib raft --no-run

echo
echo "-- running cluster proofs (single-threaded: each binds its own loopback ports) --"
# `--test-threads=1` keeps the in-process clusters from contending for ephemeral ports
# and CPU; the failover/transfer proofs have real election timeouts.
set +e
cargo test --features "${FEATURES}" --lib raft -- --test-threads=1 --nocapture "${TESTS[@]}"
rc=$?
set -e

echo
if [ "${rc}" -eq 0 ]; then
  echo "== RESULT: PASS — cluster forms, replicates, fails over, transfers leadership, and the log is durable =="
else
  echo "== RESULT: FAIL (rc=${rc}) — a cluster proof did not pass; see output above =="
fi
exit "${rc}"
