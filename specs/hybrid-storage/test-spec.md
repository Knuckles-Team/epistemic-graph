# EG-HYBRID-STORAGE — Test specification

Governing spec: [spec.md](spec.md).

| Requirement | Name | Proving test |
|---|---|---|
| `EG-HYBRID-STORAGE-R001` | Capability partition taxonomy | `crates/eg-types/tests/capability_partition.rs::matrix_round_trips_and_control_plane_is_engine_only` |
| `EG-HYBRID-STORAGE-R002` | Plan-level query router | `crates/eg-query/tests/route_decision.rs::matrix_routes_and_cache_reuses` |
| `EG-HYBRID-STORAGE-R003` | Placement catalog and single DDL authority | `tests/test_hybrid_placement_served.py::test_placement_commits_with_catalog` |
| `EG-HYBRID-STORAGE-R004` | Single admission authority for row writes | `tests/test_hybrid_row_write_served.py::test_row_write_admitted_once` |
| `EG-HYBRID-STORAGE-R005` | Fence record and cross-engine consistency | `crates/eg-rowstore/tests/fence.rs::visibility_and_reconcile` |
| `EG-HYBRID-STORAGE-R006` | Secondary-index contract | `tests/test_hybrid_index_served.py::test_probe_routes_and_digest_falls_back` |
| `EG-HYBRID-STORAGE-R007` | Projection pipeline over the committed stream | `tests/test_hybrid_projection_served.py::test_projection_catches_up_to_digest` |
| `EG-HYBRID-STORAGE-R008` | Backfill and row-ready gate | `tests/test_hybrid_backfill_served.py::test_backfill_resumes_and_gates` |
| `EG-HYBRID-STORAGE-R009` | Shadow-read parity before cutover | `crates/eg-query/tests/shadow_window.rs::mismatch_resets_and_error_is_not_match` |
| `EG-HYBRID-STORAGE-R010` | Degradation and failover semantics | `tests/test_hybrid_failover_served.py::test_degrade_refuse_and_recover` |
| `EG-HYBRID-STORAGE-R011` | Workload-class extension and per-listener route gate | `crates/eg-query/tests/workload_class.rs::oltp_on_row_table_allocates_no_session` |
| `EG-HYBRID-STORAGE-R012` | Mixed-batch boundary: typed refusal and coordinated-commit seam | `tests/test_hybrid_mixed_batch_served.py::test_mixed_batch_refuses_pre_effect` |
| `EG-HYBRID-STORAGE-R013` | Storage-provider capability negotiation (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R013.1` | Model | `crates/eg-types/tests/storage_provider.rs::capabilities_round_trip` |
| `EG-HYBRID-STORAGE-R013.2` | Behavior | `crates/eg-core/tests/storage_provider.rs::negotiation_is_intersection` |
| `EG-HYBRID-STORAGE-R013.3` | Wiring | `tests/test_hybrid_provider_served.py::test_status_reports_negotiated_capabilities` |
| `EG-HYBRID-STORAGE-R013.4` | Refusal | `tests/test_hybrid_provider_served.py::test_missing_capability_refuses_placement` |
| `EG-HYBRID-STORAGE-R013.5` | Evidence | `tests/test_hybrid_provider_served.py::test_negotiation_record_reads_back` |
| `EG-HYBRID-STORAGE-R013.6` | Hot-path proof | `tests/test_hybrid_provider_served.py::test_negotiation_latency` |
| `EG-HYBRID-STORAGE-R014` | Placement policy by record class (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R014.1` | Model | `crates/eg-types/tests/placement_policy.rs::policy_round_trips` |
| `EG-HYBRID-STORAGE-R014.2` | Behavior | `crates/eg-types/tests/placement_policy.rs::engine_only_classes_refuse_other_placements` |
| `EG-HYBRID-STORAGE-R014.3` | Wiring | `tests/test_hybrid_policy_served.py::test_policy_gates_placement` |
| `EG-HYBRID-STORAGE-R014.4` | Refusal | `tests/test_hybrid_policy_served.py::test_policy_refusals` |
| `EG-HYBRID-STORAGE-R014.5` | Evidence | `tests/test_hybrid_policy_served.py::test_receipt_names_policy` |
| `EG-HYBRID-STORAGE-R014.6` | Hot-path proof | `cargo bench --bench placement_policy` |
| `EG-HYBRID-STORAGE-R015` | Own-write reads, idempotency and replay across engines (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R015.1` | Model | `crates/eg-types/tests/storage_provider.rs::token_round_trips` |
| `EG-HYBRID-STORAGE-R015.2` | Behavior | `crates/eg-rowstore/tests/consistency.rs::token_read_waits_for_fence` |
| `EG-HYBRID-STORAGE-R015.3` | Wiring | `tests/test_hybrid_consistency_served.py::test_read_own_writes_over_sql_wire` |
| `EG-HYBRID-STORAGE-R015.4` | Refusal | `tests/test_hybrid_consistency_served.py::test_token_refusals` |
| `EG-HYBRID-STORAGE-R015.5` | Evidence | `crates/eg-rowstore/tests/consistency.rs::replay_converges_to_live_digest` |
| `EG-HYBRID-STORAGE-R015.6` | Hot-path proof | `cargo bench -p eg-rowstore --bench apply_committed` |
| `EG-HYBRID-STORAGE-R016` | Federated plans with pushdown to the row engine (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R016.1` | Model | `crates/eg-rowstore/tests/provider.rs::pushdown_support_is_declared` |
| `EG-HYBRID-STORAGE-R016.2` | Behavior | `crates/eg-rowstore/tests/provider.rs::row_join_pushes_as_one_statement` |
| `EG-HYBRID-STORAGE-R016.3` | Wiring | `tests/test_hybrid_federated_served.py::test_mixed_join_pushes_filters` |
| `EG-HYBRID-STORAGE-R016.4` | Refusal | `tests/test_hybrid_federated_served.py::test_pushdown_bounds_refuse` |
| `EG-HYBRID-STORAGE-R016.5` | Evidence | `tests/test_hybrid_federated_served.py::test_explain_lists_fragments` |
| `EG-HYBRID-STORAGE-R016.6` | Hot-path proof | `cargo bench --bench hybrid_federated_join` |
| `EG-HYBRID-STORAGE-R017` | Tenancy and row-level security on the row engine (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R017.1` | Model | `crates/eg-types/tests/storage_provider.rs::security_mapping_round_trips` |
| `EG-HYBRID-STORAGE-R017.2` | Behavior | `crates/eg-rowstore/tests/security.rs::compiled_policy_matches_engine_predicate` |
| `EG-HYBRID-STORAGE-R017.3` | Wiring | `tests/test_hybrid_rls_served.py::test_row_engine_rls_equals_engine_rls` |
| `EG-HYBRID-STORAGE-R017.4` | Refusal | `tests/test_hybrid_rls_served.py::test_rls_refusals` |
| `EG-HYBRID-STORAGE-R017.5` | Evidence | `tests/test_hybrid_rls_served.py::test_rls_differential` |
| `EG-HYBRID-STORAGE-R017.6` | Hot-path proof | `tests/test_hybrid_rls_served.py::test_role_switch_latency` |
| `EG-HYBRID-STORAGE-R018` | Backup, restore and placement migration (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R018.1` | Model | `crates/eg-types/tests/storage_provider.rs::backup_record_round_trips` |
| `EG-HYBRID-STORAGE-R018.2` | Behavior | `crates/eg-rowstore/tests/backup.rs::restore_reaches_pinned_fence` |
| `EG-HYBRID-STORAGE-R018.3` | Wiring | `tests/test_hybrid_backup_served.py::test_backup_restore_round_trip` |
| `EG-HYBRID-STORAGE-R018.4` | Refusal | `tests/test_hybrid_backup_served.py::test_backup_refusals` |
| `EG-HYBRID-STORAGE-R018.5` | Evidence | `tests/test_hybrid_backup_served.py::test_restore_drill_records_digests` |
| `EG-HYBRID-STORAGE-R018.6` | Hot-path proof | `tests/test_hybrid_backup_served.py::test_restore_drill_budget` |
| `EG-HYBRID-STORAGE-R019` | Performance budget and placement benchmarks (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R019.1` | Model | `crates/eg-types/tests/storage_provider.rs::benchmark_record_round_trips` |
| `EG-HYBRID-STORAGE-R019.2` | Behavior | `benches/hybrid_placement.rs` |
| `EG-HYBRID-STORAGE-R019.3` | Wiring | `tests/test_hybrid_benchmark_gate.py::test_policy_defaults_cite_runs` |
| `EG-HYBRID-STORAGE-R019.4` | Refusal | `tests/test_hybrid_benchmark_gate.py::test_unmeasured_default_refuses` |
| `EG-HYBRID-STORAGE-R019.5` | Evidence | `tests/test_hybrid_benchmark_gate.py::test_run_records_provenance` |
| `EG-HYBRID-STORAGE-R019.6` | Hot-path proof | `cargo bench --bench hybrid_placement` |
| `EG-HYBRID-STORAGE-R020` | Row-engine deployment contract (rollup) | See each child. |
| `EG-HYBRID-STORAGE-R020.1` | Model | `crates/eg-types/tests/storage_provider.rs::deployment_contract_round_trips` |
| `EG-HYBRID-STORAGE-R020.2` | Behavior | `crates/eg-rowstore/tests/contract.rs::mismatch_marks_unready` |
| `EG-HYBRID-STORAGE-R020.3` | Wiring | `tests/test_hybrid_deploy_served.py::test_status_reports_contract_fields` |
| `EG-HYBRID-STORAGE-R020.4` | Refusal | `tests/test_hybrid_deploy_served.py::test_contract_refusals` |
| `EG-HYBRID-STORAGE-R020.5` | Evidence | `tests/test_hybrid_deploy_served.py::test_open_record_reads_back` |
| `EG-HYBRID-STORAGE-R020.6` | Hot-path proof | `tests/test_hybrid_deploy_served.py::test_probe_latency` |

## Negative and boundary cases

- A placement of an engine-only record class is refused.
- A placement that needs an absent provider capability is refused before data moves.
- A row write before row-ready is refused.
- A cutover on an incomplete or mismatched shadow window is refused.
- A mixed-placement batch is refused before effect.
- A token from another graph, or ahead of the commit sequence, is refused.
- A routed statement without a mapped role is refused.
- A restore with a row base newer than the engine snapshot is refused.

## Integration

- Write through the native method; read through the SQL wire listener with the token.
- Stop the row engine; observe degraded reads and refused row writes; restart; observe reconcile first.
- Join a row table with a lake table; read the pushed fragments in EXPLAIN.
- Read one table as three principals through both engines; compare row sets.
- Back up, write more, restore; compare digests at the pinned fence.

## Quality and release proof

- Clippy, tests, jscpd, CCCC, dupehound and KISS pass on `crates/eg-rowstore`.
- The no-duplicate-authority check confirms one commit authority and one apply function.
- Each benchmark gates a 10 percent regression against its stored baseline.
