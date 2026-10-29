# MEDULLA-ENGINE-PARITY — Test specification

Governing spec: [spec.md](spec.md).

| Requirement | Name | Proving test |
|---|---|---|
| `MEDULLA-ENGINE-PARITY-R001` | Single committer with group commit and batched fsync (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R001.1` | Model and behavior | `crates/eg-storage/tests/group_commit.rs::batch_shares_one_fsync` |
| `MEDULLA-ENGINE-PARITY-R001.2` | Wiring and refusal | `crates/eg-storage/tests/group_commit.rs::one_failure_does_not_fail_batch` |
| `MEDULLA-ENGINE-PARITY-R001.3` | Hot-path proof | `cargo bench -p eg-storage --bench group_commit` |
| `MEDULLA-ENGINE-PARITY-R002` | Optimistic concurrency with read and write sets (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R002.1` | Model and behavior | `crates/eg-transaction/tests/occ.rs::write_skew_is_detected` |
| `MEDULLA-ENGINE-PARITY-R002.2` | Wiring and refusal | `crates/eg-transaction/tests/occ.rs::conflict_refuses_and_writes_nothing` |
| `MEDULLA-ENGINE-PARITY-R002.3` | Hot-path proof | `cargo bench -p eg-transaction --bench occ` |
| `MEDULLA-ENGINE-PARITY-R003` | Partitioned store files for parallel writers (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R003.1` | Model and behavior | `crates/eg-storage/tests/partition.rs::two_partitions_commit_in_parallel` |
| `MEDULLA-ENGINE-PARITY-R003.2` | Wiring and refusal | `crates/eg-storage/tests/partition.rs::cross_partition_batch_refuses_or_coordinates` |
| `MEDULLA-ENGINE-PARITY-R003.3` | Hot-path proof | `cargo bench -p eg-storage --bench partition` |
| `MEDULLA-ENGINE-PARITY-R004` | Lock-free acceptance for disjoint keys (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R004.1` | Model and behavior | `crates/eg-transaction/tests/occ.rs::disjoint_footprints_never_wait` |
| `MEDULLA-ENGINE-PARITY-R004.2` | Wiring and refusal | `crates/eg-transaction/tests/occ.rs::retry_bound_refuses` |
| `MEDULLA-ENGINE-PARITY-R004.3` | Hot-path proof | `cargo bench -p eg-transaction --bench disjoint` |
| `MEDULLA-ENGINE-PARITY-R005` | Write throughput benchmark and targets (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R005.1` | Model and behavior | `benches/engine_writes.rs` |
| `MEDULLA-ENGINE-PARITY-R005.2` | Wiring and refusal | `tests/test_engine_bench_gate.py::test_missing_workload_fails` |
| `MEDULLA-ENGINE-PARITY-R005.3` | Hot-path proof | `cargo bench --bench engine_writes` |
| `MEDULLA-ENGINE-PARITY-R006` | PostgreSQL wire protocol front end (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R006.1` | Model and behavior | `tests/pgwire_roundtrip.rs::extended_query_prepared_and_copy` |
| `MEDULLA-ENGINE-PARITY-R006.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::unsupported_message_and_plaintext_refuse` |
| `MEDULLA-ENGINE-PARITY-R006.3` | Hot-path proof | `tests/pgwire_roundtrip.rs::prepared_point_read_latency` |
| `MEDULLA-ENGINE-PARITY-R007` | Wire authentication mapped to engine principals and row security (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R007.1` | Model and behavior | `tests/pgwire_roundtrip.rs::login_maps_to_principal` |
| `MEDULLA-ENGINE-PARITY-R007.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::wire_rls_equals_native_rls` |
| `MEDULLA-ENGINE-PARITY-R007.3` | Hot-path proof | `tests/pgwire_roundtrip.rs::login_latency` |
| `MEDULLA-ENGINE-PARITY-R008` | Catalog views for outside tools (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R008.1` | Model and behavior | `crates/eg-query/tests/pg_catalog.rs::views_list_engine_tables` |
| `MEDULLA-ENGINE-PARITY-R008.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::bi_tool_introspection_queries_pass` |
| `MEDULLA-ENGINE-PARITY-R008.3` | Hot-path proof | `tests/pgwire_roundtrip.rs::introspection_latency` |
| `MEDULLA-ENGINE-PARITY-R009` | SQL dialect compatibility matrix (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R009.1` | Model and behavior | `crates/eg-query/tests/compat.rs::matrix_loads_and_is_closed` |
| `MEDULLA-ENGINE-PARITY-R009.2` | Wiring and refusal | `crates/eg-query/tests/compat.rs::every_supported_entry_has_a_test` |
| `MEDULLA-ENGINE-PARITY-R009.3` | Hot-path proof | `tests/test_sql_compat_conformance.py::test_supported_entries_pass` |
| `MEDULLA-ENGINE-PARITY-R010` | Secondary, partial and expression indexes (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R010.1` | Model and behavior | `crates/eg-query/tests/index.rs::partial_index_holds_only_matching_rows` |
| `MEDULLA-ENGINE-PARITY-R010.2` | Wiring and refusal | `crates/eg-query/tests/index.rs::planner_uses_partial_index` |
| `MEDULLA-ENGINE-PARITY-R010.3` | Hot-path proof | `cargo bench -p eg-query --bench index` |
| `MEDULLA-ENGINE-PARITY-R011` | Constraints: unique, foreign key, check and shapes (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R011.1` | Model and behavior | `crates/eg-query/tests/constraint.rs::deferred_foreign_key_checks_at_commit` |
| `MEDULLA-ENGINE-PARITY-R011.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::constraint_violations_return_sqlstate` |
| `MEDULLA-ENGINE-PARITY-R011.3` | Hot-path proof | `cargo bench --bench engine_writes` |
| `MEDULLA-ENGINE-PARITY-R012` | Triggers as reactive rules (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R012.1` | Model and behavior | `crates/eg-query/tests/trigger.rs::before_insert_rewrites_row` |
| `MEDULLA-ENGINE-PARITY-R012.2` | Wiring and refusal | `crates/eg-query/tests/trigger.rs::depth_bound_refuses` |
| `MEDULLA-ENGINE-PARITY-R012.3` | Hot-path proof | `cargo bench -p eg-query --bench trigger` |
| `MEDULLA-ENGINE-PARITY-R013` | Sequences (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R013.1` | Model and behavior | `crates/eg-query/tests/sequence.rs::values_never_repeat_after_crash` |
| `MEDULLA-ENGINE-PARITY-R013.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::serial_column_uses_sequence` |
| `MEDULLA-ENGINE-PARITY-R013.3` | Hot-path proof | `cargo bench -p eg-query --bench sequence` |
| `MEDULLA-ENGINE-PARITY-R014` | Views and materialized views as derived series (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R014.1` | Model and behavior | `crates/eg-query/tests/matview.rs::refresh_on_change_tracks_lineage` |
| `MEDULLA-ENGINE-PARITY-R014.2` | Wiring and refusal | `tests/pgwire_roundtrip.rs::view_and_matview_over_wire` |
| `MEDULLA-ENGINE-PARITY-R014.3` | Hot-path proof | `cargo bench -p eg-query --bench matview` |
| `MEDULLA-ENGINE-PARITY-R015` | Change data capture by logical decoding (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R015.1` | Model and behavior | `crates/eg-storage/tests/decode.rs::decode_matches_commits` |
| `MEDULLA-ENGINE-PARITY-R015.2` | Wiring and refusal | `tests/test_change_subscribe_served.py::test_subscriber_resumes_from_cursor` |
| `MEDULLA-ENGINE-PARITY-R015.3` | Hot-path proof | `cargo bench -p eg-storage --bench decode` |
| `MEDULLA-ENGINE-PARITY-R016` | Point-in-time recovery from snapshots and the log (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R016.1` | Model and behavior | `crates/eg-storage/tests/archive.rs::restore_reaches_target_seq` |
| `MEDULLA-ENGINE-PARITY-R016.2` | Wiring and refusal | `tests/test_pitr_served.py::test_restore_to_time` |
| `MEDULLA-ENGINE-PARITY-R016.3` | Hot-path proof | `tests/test_pitr_served.py::test_restore_drill_budget` |
| `MEDULLA-ENGINE-PARITY-R017` | Read replicas that follow the log (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R017.1` | Model and behavior | `crates/eg-storage/tests/replica.rs::replica_equals_primary_digest` |
| `MEDULLA-ENGINE-PARITY-R017.2` | Wiring and refusal | `tests/test_replica_served.py::test_token_read_and_write_refusal` |
| `MEDULLA-ENGINE-PARITY-R017.3` | Hot-path proof | `tests/test_replica_served.py::test_replica_lag_budget` |
| `MEDULLA-ENGINE-PARITY-R018` | Columnar materialization of hot record classes (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R018.1` | Model and behavior | `crates/eg-query/tests/columnar_cache.rs::copy_tracks_log_within_staleness` |
| `MEDULLA-ENGINE-PARITY-R018.2` | Wiring and refusal | `crates/eg-query/tests/columnar_cache.rs::stale_copy_falls_back` |
| `MEDULLA-ENGINE-PARITY-R018.3` | Hot-path proof | `cargo bench -p eg-query --bench columnar_cache` |
| `MEDULLA-ENGINE-PARITY-R019` | Outside PostgreSQL databases are sources only (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R019.1` | Model and behavior | `crates/eg-storage/tests/provider.rs::provider_set_is_closed` |
| `MEDULLA-ENGINE-PARITY-R019.2` | Wiring and refusal | `tests/test_external_source_boundary.py::test_engine_data_never_syncs_out` |
| `MEDULLA-ENGINE-PARITY-R019.3` | Hot-path proof | `cargo bench --bench external_source_join` |
| `MEDULLA-ENGINE-PARITY-R020` | Demand gate, capability report and parity benchmark suite (rollup) | See each child. |
| `MEDULLA-ENGINE-PARITY-R020.1` | Model and behavior | `crates/eg-types/tests/engine_capabilities.rs::report_round_trips` |
| `MEDULLA-ENGINE-PARITY-R020.2` | Wiring and refusal | `tests/test_engine_capabilities_served.py::test_report_matches_features` |
| `MEDULLA-ENGINE-PARITY-R020.3` | Hot-path proof | `cargo bench --bench engine_parity` |

## Negative and boundary cases

- A conflicting transaction refuses with `TXN_CONFLICT` and writes nothing.
- A batch over two partitions coordinates or refuses with `CROSS_PARTITION_BATCH`.
- An unsupported wire message refuses with SQLSTATE `0A000`.
- A non-loopback wire listener without TLS refuses to start.
- A constraint violation returns its SQLSTATE and the constraint name.
- A trigger chain deeper than 16 refuses.
- A change cursor below the retained log refuses.
- A recovery target past the archived log refuses.
- A write to a read replica refuses with SQLSTATE `25006`.
- A request to place engine data in an outside database refuses.

## Integration

- A standard PostgreSQL client connects over TLS, introspects the catalog, runs prepared statements and COPY, and reads under row security.
- 64 concurrent writers commit through group commit; disjoint writers never wait.
- A subscriber follows the change stream, stops and resumes from its cursor.
- A restore to a chosen sequence matches the digest recorded at that sequence.

## Quality and release proof

- Clippy, tests, jscpd, CCCC, dupehound and KISS pass on the changed crates.
- The no-duplicate-authority check confirms one committer per store file and one change stream.
- Each benchmark gates a 10 percent regression against its stored baseline.
