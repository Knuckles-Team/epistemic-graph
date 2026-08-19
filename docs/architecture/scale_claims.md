# Scale claim register

This is the machine-readable status register for the M1/M2/M3 scaling docs. A
claim has exactly one evidence state:

| Status | Meaning |
|---|---|
| `DESIGNED` | The contract or future behavior is specified, but the current source does not provide the capability. |
| `IMPLEMENTED` | The current `main` source contains the capability; no execution result is implied. |
| `UNIT-PROVEN` | A focused repository fixture is the stated evidence for the current source path. |
| `LAB-PROVEN` | A throwaway, bounded lab/loopback run has produced evidence for the exact artifact. |
| `LIVE` | A deployed runtime observation identifies the artifact, environment, and observation time. |
| `1M-CERTIFIED` | The exact one-million-user/agent workload contract and certification report identify the artifact and results. |

`LIVE` and `1M-CERTIFIED` are intentionally absent until deployment evidence and
the exact workload report exist. `IMPLEMENTED` and `UNIT-PROVEN` are not claims
that a feature is deployed, horizontally proven, or capable of the one-million
target.

| ID | Status | Source anchor | Required source text | Evidence / scope |
|---|---|---|---|---|
| `m1.memory-budget-zero` | `IMPLEMENTED` | `src/cost.rs#EPISTEMIC_GRAPH_MEMORY_BUDGET` | `must be positive` | Startup configuration rejects zero, negative, and malformed values; there is no memory-budget disable value. |
| `m1.nonraft-default-shards` | `IMPLEMENTED` | `src/server/persistence/redb_backend.rs#resolve_shard_count` | `(cpus / 2).clamp(1, 8)` | Non-Raft production auto-sizing remains capped at eight; the 64-shard layout ceiling is for explicit migration values and Raft group counts. |
| `m1.raft-default-groups` | `IMPLEMENTED` | `src/raft/config.rs#default_raft_group_count` | `(cpus / 2).clamp(1, crate::redb_layout::MAX_SHARD_COUNT as u64)` | The active Raft default is effective-cgroup CPU derived and bounded by the 64-shard layout ceiling. |
| `m1.raft-group-parser` | `UNIT-PROVEN` | `src/raft/config.rs#parse_groups_uses_default_when_absent_or_empty_or_zero` | `parse_groups(None, 4)` | Focused parser fixtures cover absent, empty, zero, malformed, and ceiling-clamped values. |
| `m3.online-reshard-delta` | `UNIT-PROVEN` | `src/server/persistence/online_reshard.rs#delta_flip_purge` | `import(delta) committed -> catalog flip durable -> purge(src)` | Current source and the focused no-loss reshard fixture cover the snapshot-plus-delta quiesce and crash-ordering path. |
| `m3.rebalance-planner` | `UNIT-PROVEN` | `src/server/persistence/rebalance.rs#plan_rebalance` | `pub fn plan_rebalance` | Pure planner fixtures cover deterministic balancing, indivisible hot graphs, and no-op cases. |
| `m3.admin-execution` | `IMPLEMENTED` | `src/server/handlers/admin.rs#RebalanceExecute` | `Method::RebalanceExecute` | The current wire handler plans and executes catalog-backed moves; deployment is separate evidence. |
| `m3.cold-offload-sweep` | `IMPLEMENTED` | `src/main.rs#offload_cold_tenants` | `offload_cold_tenants` | The configured interval starts the durable-gated whole-graph offload sweep. |
| `m2.loopback-harness` | `IMPLEMENTED` | `scripts/validate-raft-cluster.sh#cargo test` | `cargo test` | A bounded throwaway loopback harness exists; this register does not claim that it was run here or that it proves live multi-host behavior. |
| `m3.cross-node-distribution` | `DESIGNED` | `docs/architecture/m3_resharding.md#R2 — Cross-NODE tenant distribution` | `Cross-NODE tenant distribution` | Cross-node row movement remains a follow-on behind multi-node deployment and consensus integration. |
| `m3.object-tier-cold-arm` | `DESIGNED` | `docs/architecture/m3_resharding.md#object-store arm` | `object-store` | The colder-than-redb object-store arm remains future work; redb offload is a separate implemented path. |
| `scale.capacity-arithmetic` | `DESIGNED` | `src/cost.rs#capacity_estimate` | `resident_fraction` | Capacity examples are planning arithmetic, not a stress result or 1M certification. |
| `m2.live-cutover` | `DESIGNED` | `docs/architecture/cluster_deployment.md#does NOT perform the live cutover` | `does NOT perform the live cutover` | The runbook is deployable guidance, but an operator must supply backup, artifact, runtime, and observation evidence. |
