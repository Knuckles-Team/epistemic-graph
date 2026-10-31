# Test specification

All fixtures run against a disposable local data directory and generated credentials. Fault injection is deterministic and records seed, revision, platform, command and artifact path. Tests may use local multi-process nodes and disposable containers; no private cluster, inventory, credentials or always-on service is needed.

| Scenario | Requirements | Positive fixture and expected result | Negative/failure fixture and expected result |
|---|---|---|---|
| T-KG-01 batch terminality | KG-01, KG-02 | Graph+table+KV batch commits once. Readback and retry return same terminal result | Inject failure at every commit phase. Restart yields all-or-none. Lost-ack retry does not duplicate |
| T-KG-02 table-only path | KG-02 | Concurrent table-only writes use the current atomic path and match the reference corpus. | Mixed unsupported member sets refuse before writes. |
| T-KG-03 owner lineage | KG-03 | Load each supported golden manifest. One validation per transaction | Unknown predecessor or extra physical table returns typed upgrade/ownership error. Stale cached binding never crosses transactions |
| T-KG-04 scrub | KG-04 | Corrupt one persisted node. The scrub reports a typed finding and resumes the cursor after restart | Rate cap prevents writer starvation. The scrub hides unreadable payloads and never silently repairs them |
| T-KG-05 outbox | KG-05 | Healthy successors deliver after a failing head is rejected/rewound with ordered receipts | Retry one head 16 times. Successors keep their own zero-attempt count. Reasoning skip without rebuild is refused |
| T-KG-06 CDC/mirror | KG-06 | Commit 100 ordered graph/table changes, disconnect target, reconnect, replay and match digest | Crash after target apply before cursor; replay is idempotent; empty default target guard and schema drift fail visibly |
| T-KG-07 scope | KG-07 | Owner and explicitly granted principal can access allowed data | Cross-tenant/principal probes deny SQL, UQL, TSDB and KV access. UDF, foreign-source and cache probes also deny access. Revoked grants deny the next use |
| T-KG-07a auth modes | KG-07, KG-09 | `none`, `local` and `external` each resolve to the same internal principal/RBAC evaluator | Spoofed name in `none`, stale local session after mode switch, wrong external issuer/audience/tenant/policy version and generic elevation lease all deny |
| T-KG-08 audit | KG-08 | Reservation precedes the governed effect and links its truthful outcome. | An unavailable audit writer, missing class, wrong tenant or duplicate ID refuses. A crash after effect reconciles without false success. |
| T-KG-09 approval | KG-09 | Two distinct principals approve exact digest before expiry. | Self-approval, generic lease, altered digest, expiry and replay all deny and audit refusal |
| T-KG-10 Raft | KG-10 | Repeated 3-node multi-group writes/failover preserve independent progress and one committed result | Inject slow peer/leader loss. Unaffected groups progress. Failed assertion emits causal ring-buffer trace |
| T-KG-11 durability | KG-11, KG-12 | Sync survives kill. Async oldest-unflushed age stays ≤100 ms. Hot structures rebuild from log | Ephemeral disappears on restart. Forced flush lag yields explicit degraded/refusal, never silent class change. Concurrent primitive history linearizes |
| T-KG-11a control-lease expiry | MEDULLA-DURABLE-KERNEL-R039.2 | `native_record_consumption_checks_commit_time_and_preserves_expired_rows` enters native-record dispatch over a real shard and consumes strictly before expiry. Existing row-applier tests prove single use; the new regression also proves revocation after expiry. | At or after either deadline, consumption returns `Conflict`. It reports no changed IDs. The persisted lease and revision remain unchanged. Time is the engine-owned mutation timestamp, not caller input or physical commit wall-clock. Runtime execution is required; source presence alone is not acceptance. |
| T-KG-12 wire batches | KG-13, KG-14 | RESP and pgwire batches return N ordered replies after one commit. Point route avoids DataFusion session | Middle operation fails. No earlier reply promises a commit. Schema/policy change invalidates cached SQL plan |
| T-KG-13 parity and benchmark | KG-15 | Differential corpus, restore drill and benchmark report include exact config, hardware class, code revision and digest | Accepted deviation is named/reviewed; a regression fails instead of rewriting baseline; missing benchmark row blocks superiority claim |

## Commands and quality evidence

Use focused package tests for `medulla-types`, `medulla-transaction`, `medulla-storage`, `medulla-stream`, `medulla-tsdb`, and affected server integration tests. Then run the full pre-commit and hosted parity commands in `plan.md`. Run `python3 scripts/check_universal_read_rls.py` and `python3 scripts/check_persisted_mutation_contract.py` when their paths are affected. The repository's CCCC changed-function gate rejects new cyclomatic >10 or cognitive >15. Run Dupehound changed-function and jscpd differential gates without new suppressions. Also run KISS, Rust architecture lint and clippy. Record scanner versions from the checked-in scanner contract. A benchmark is evidence only for its measured hardware/workload/durability class. No live environment is an implicit prerequisite to unit or contract acceptance.

## Evidence record

For each scenario, add a dated table row in the PR or an evidence file with exact main commit, command, CI URL, test count, result, fixture digest and known limitation. A passed local branch test means `BUILT`; merged-head tests and reviewed negative cases are required for `ACCEPTED`. Do not infer acceptance from the presence of these documents.

## Worker registry cases

R074.1: register a 16-core, 64-GiB worker supporting general/hydration, serde round-trip its record, heartbeat to extend expiry, and expire exactly at the new deadline. Refuse invalid capacity, blank identity, missing lanes, zero/overflowing TTL, duplicate live registration, unknown/expired heartbeats and backward clocks with unchanged state. Re-registration is allowed after expiry. Tests are native in worker_registry.rs.

R074.2 remains pending: exercise these transitions through real authenticated dispatch and generated contracts, including cross-tenant and cross-principal refusals. Pure tests are not served acceptance.

## T-KG-14 scheduler configuration producer

R076.1 and MEDULLA-PLACEMENT-R002.2 bind native tests in `crates/medulla-types/src/scheduler_config.rs`: real-serde default and override round trips. Exact floor sum. Over-subscription. U32::MAX floor overflow. Zero disabling bounds. CPU >1000, memory=0 and blank-lane refusals. Missing/unknown/negative/out-of-range wire fields. Run `cargo test -p medulla-types scheduler_config::tests` on a warm build, or compile that exact source with real serde/serde_json dependencies for isolated producer evidence. The latter is not full-crate or server validation. R076.2 still requires the served settings/admission scenario.

## T-KG-15 association submission schema

R082.2 binds the table-driven tests in `src/server/handlers/jobs/graph_tests.rs`.
Fifteen malformed outer-list, transaction-list and item shapes go through the
actual native request-body decoder and return `INVALID_ARGUMENT`, correlation
id 82, a bounded encoding detail and no result. The decoder submits a matching valid full request to the real governor.
It uses support 0.1, confidence 0.5 and fpgrowth defaults. Governor tables cover NaN, both infinities, thresholds
outside [0, 1], unknown algorithms, all five accepted names/aliases and inclusive
0/1 boundaries. The production reader uses the same decoder; no second refusal
implementation is introduced for tests.

2026-10-11 focused evidence: the exact complete submission-test module plus the
two existing hash tests passed (6 tests), compiled against real serde,
rmp-serde, sha2 and the actual jobs-enabled medulla-types crate. The harness includes
the exact production decoder, governor, submit validator and authority/hash
functions. This proves schema admission and decoding, not socket serving,
authentication, persistence or the executor lifecycle. The broader R082 rollup
and its other children remain open. Normal warm-tree reproduction is
`cargo test --lib --features jobs mine_associate_submission_tests`.

## R082.6 historical association parity

| Requirement | Proving tests | Evidence |
|---|---|---|
| MEDULLA-DURABLE-KERNEL-R082.6 | `deleted_au_six_transaction_order_and_thresholds`; `minimum_count_and_zero_consequent_lift_boundaries` | Six historical transaction cases cover all three engines, stable ordering, thresholds, metrics, and zero lift. |

Run `cargo test -p medulla-compute mining::association`.
Isolated compilation includes the complete production association and math modules with real serde_json.
This proves native kernel behavior, not server transport or durable execution.

## R011 and R014 runtime acceptance

These rows separate declaration evidence from runtime acceptance. Planned test names do not claim passing results.
The producer children retain their current evidence. Neither producer proves runtime execution.

| Requirement | Proving tests and source | Acceptance |
|---|---|---|
| MEDULLA-DURABLE-KERNEL-R011.2 | `crates/medulla-types/src/pre_merge_test_module.rs`: `required_manifest_covers_runtime_categories`, `required_manifest_rejects_empty_selection`, and `required_manifest_anchors_exist_in_source`. | Eight source-module tests passed with Rust 1.96.0. Runtime acceptance remains open under R011.3 and R011.4. |
| MEDULLA-DURABLE-KERNEL-R011.3 | Planned tests in `tests/test_required_runtime_gate.py`: `test_pull_request_executes_required_categories` and `test_failed_or_empty_category_blocks_acceptance`. | Pending runtime acceptance. |
| MEDULLA-DURABLE-KERNEL-R011.4 | CI receipts must name `published-full`, `workspace-all-features`, and every selected runtime test. R011.2 and R011.3 test names define the gate checks. | Pending runtime acceptance. |
| MEDULLA-DURABLE-KERNEL-R014.2 | Process tests in `tests/test_validate_raft_cluster.py`: `test_cpu_load_runs_during_cluster_test`, `test_zero_load_workers_refused`, and `test_load_workers_stop_on_failure`. | Process lifecycle tested; loaded Rust acceptance remains in R014.4 and R014.5. |
| MEDULLA-DURABLE-KERNEL-R014.3 | Planned tests in `tests/test_validate_raft_cluster.py`: `test_repeats_both_cluster_tests`, `test_retry_budget_is_bounded`, and `test_retry_preserves_failure_and_trace`. | Pending runtime acceptance. |
| MEDULLA-DURABLE-KERNEL-R014.4 | `src/raft/tests.rs::multi_node_group_join_then_leader_rebalance` must pass every recorded loaded repetition without flaky failures. The record must include at least two repetitions. | Pending runtime acceptance. |
| MEDULLA-DURABLE-KERNEL-R014.5 | `src/raft/tests.rs::placement_admin_wire_rpc::placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster` must pass every recorded loaded repetition without flaky failures. The record must include at least two repetitions. | Pending runtime acceptance. |

R011.2 must list each selected module and its feature flags before R011.3 execution.
Existing runtime anchors include `src/raft/tests.rs::durable_log_replays_from_redb_after_restart`.
Backup anchors include `crates/medulla-transaction/src/tests/backup_replay.rs::a_coordinator_backup_preserves_every_consumed_nonce_and_receipt`.
Persistence anchors include `src/raft/tests.rs::fault_injection_no_committed_log_entry_lost_on_restart`.
These anchors do not replace the complete three-category manifest.
R011.4 receipts must link passing logs for every manifest entry and both clippy profiles.
R014.4 and R014.5 depend on R014.2 and R014.3.
The loaded evidence must record every attempt, including retries and failed first attempts.
A successful retry does not satisfy the parent's requirement for no flaky failures.

### R011.2 manifest evidence (2026-10-11)

The existing typed declaration now records feature flags and builds Cargo arguments.
Each command uses `test --locked --lib --no-default-features`.

| Category | Package | Filter | Features |
|---|---|---|---|
| raft | Medulla | `raft::` | `cluster` |
| backup | medulla-transaction | `tests::backup_replay::` | none |
| persistence | Medulla | `server::persistence::` | `full` |

Source inspection confirmed the module paths, package names, and feature gates.
The source-binding test checks existing runtime test names in each selected module.
The discovery-refusal test uses fixtures; it is not a compiled runtime inventory.
Portable reproduction from the checkout root: `mkdir -p target && rustc --edition 2021 --test crates/medulla-types/src/pre_merge_test_module.rs -o target/required-manifest-tests && target/required-manifest-tests`.
Result: eight tests passed, zero failed, zero ignored.
No raft, backup, persistence, or clippy runtime gate ran for this slice.
R011.3 must obtain test names from compiled library targets and run every selected test.
R011.4 must record the exact-head runtime and clippy receipts.

### R014.2 process evidence (2026-10-11)

Run `python -m pytest -q tests/test_validate_raft_cluster.py`.
Twenty-one tests exercise the real shell entry point and CPU worker processes.
A controlled Cargo fixture checks that compilation precedes load and workers remain alive.
Linux fixtures also check increasing worker CPU time during the command.
Cases cover count and duration refusals, command failure, SIGINT, and SIGTERM.
The suite also covers timeout, unexpected worker exit, build failure, Cargo child cleanup, and unchanged unloaded runs.
This is process-harness evidence, not a passing Rust cluster run.
R014, R014.3, R014.4, and R014.5 remain open.

### R014.3.1 process evidence (2026-10-11)

Run `python -m pytest -q tests/test_validate_raft_cluster.py`. Controlled Cargo subprocesses check both exact test names over two loaded repetitions. They check separate traces and result records, invalid repeat refusal, empty-selection refusal, and failure retention despite later successes.

Use `RAFT_LOAD_WORKERS=2 RAFT_LOAD_REPEATS=2 scripts/validate-raft-cluster.sh` on a Rust-capable host. Each execution creates a fresh directory under `target/raft-cluster-runs` with `results.jsonl` and separate trace logs. `RAFT_LOAD_OUTPUT` changes that parent directory. The harness performs no diagnostic retries. Process fixtures do not prove Rust cluster execution; R014.3.2, R014.4 and R014.5 remain open.

### Lifecycle and fixture validation

Published source: [cf7010728682663f735516ee276f423476d3690b](https://github.com/Knuckles-Team/medulla/commit/cf7010728682663f735516ee276f423476d3690b).
Review: [PR 445](https://github.com/Knuckles-Team/medulla/pull/445).

`control_maintenance_reused_label_admits_at_the_current_version` checks fresh control admission for a reused label.
`delete_purges_receipts_of_a_prior_incarnation` checks receipt removal, restart, fresh recreation, and the new incarnation's stable replay.
Graph replay completes its fresh control owner phase without opening replayed graph owner rows.
Native facade execution remains pending; this record does not establish R049 acceptance.
The original EG318 database-lock failure remains unreproduced.

Run `mkdir -p target-isolated/tmp` from the checkout.
Run `TMPDIR="$PWD/target-isolated/tmp" rustc --edition 2021 --test src/test_support.rs -o target-isolated/fixture-path-tests`.
Run `target-isolated/fixture-path-tests`.
The standalone helper test checks 2,048 distinct paths across eight threads.
This helper test does not establish native facade acceptance.

### Clippy profile declaration coverage

`test_required_clippy_profiles_and_command_are_declared` checks both required profiles, warning enforcement, and Kafka build headers.
`test_required_clippy_declaration_drift_is_refused` rejects configuration changes that remove those obligations.
Run `pytest --noconftest -o addopts='' tests/test_release_feature_matrix.py -q`.
The suite passed 19 tests on the published source; Ruff lint and formatting checks also passed.
The official generator retains SPECIFIED for R011, R011.3, and R011.4.
Only the configuration child R011.5 gains its own generated evidence.
The parent binding records partial declaration coverage; it does not prove native runtime or exact-head acceptance.

Shared KV-cache tier (R083): two consumers share one content-addressed block. A model-version mismatch refuses. An over-budget insert evicts the coldest block by recency and graph importance.

## MEDULLA-DURABLE-KERNEL-R084 acceptance

| Scenario | Requirement | Fixture |
|---|---|---|
| Two writers, one version | R084.6 | Both write against version N. One commits. The other gets `WRITE_CONFLICT`. |
| Strategy merge | R084.2 | A `FieldUnion` scope merges two disjoint field edits. |


## R085 planned acceptance

All test names below are proposed. None ran in this specification batch.
Use the real server binary, authenticated public client and disposable durable directory.
Transport, graph authorization, mutation dispatch and persistence must remain real.
Read durable state through an independent authorized client after restart.

- `MEDULLA-DURABLE-KERNEL-R085.1`: Planned: tests/memory_sync_contract.rs::bounded_contract_round_trips_and_refuses_invalid_fields.
- `MEDULLA-DURABLE-KERNEL-R085.2`: Planned: tests/memory_sync_served.rs::authorized_read_list_and_cursor_isolation.
- `MEDULLA-DURABLE-KERNEL-R085.3`: Planned: tests/memory_sync_served.rs::competing_saves_retain_both_and_resolve_once.
- `MEDULLA-DURABLE-KERNEL-R085.4`: Planned: tests/memory_sync_served.rs::wire_methods_reach_durable_gateway.
- `MEDULLA-DURABLE-KERNEL-R085.5`: Planned: tests/memory_sync_served.rs::denied_and_malformed_requests_have_no_effects.
- `MEDULLA-DURABLE-KERNEL-R085.6`: Planned: tests/memory_sync_served.rs::provenance_and_materialization_ack_survive_restart.
- `MEDULLA-DURABLE-KERNEL-R085.7`: Planned: tests/memory_sync_served.rs::conflicts_survive_gc_and_retention_refusal.
- `MEDULLA-DURABLE-KERNEL-R085.8`: Planned: tests/memory_sync_served.rs::complete_memory_sync_journey.

The complete journey creates a document, reads an exact revision and lists it across pages.
First observe a host edit and check file_ahead without changing the accepted head.
Capture an acknowledgment for the accepted revision and observation generation before the host edit.
Retain the host edit without advancing the accepted revision, then deliver that delayed acknowledgment.
Check a generation-mismatch refusal and unchanged pending content, receipt and file_ahead state.
A fresh acknowledgment also cannot clear file_ahead while that observed edit remains pending.
Two clients submit distinct contents from one base. Exactly one advances the accepted head.
The other receives a durable conflict reference containing both readable versions and hashes.
Retry both requests and check stable receipts without duplicate content or holder effects.
Restart the engine and run eligible blob collection. Both conflicting versions remain readable.
Resolve with the current revision, materialize the result, then acknowledge its exact digest.
Only the final acknowledgment may report in_sync. A stale acknowledgment must not clear newer state.

Negative cases include another tenant, wrong graph incarnation and a principal without write authority.
Denied calls leave graph heads, content records, conflict records, holders and outbox rows unchanged.
Their responses reveal no existence, revision, digest, conflict reference or stored receipt.
Compare denied reads, saves and creation attempts against both existing and absent documents.
Check equal error codes and response shapes after removing request-local correlation identities.
An authorized but refused creation also uses Refused without synthetic head metadata.
Round-trip every tagged outcome and reject head fields attached to Refused.
Also test tampered cursors, policy changes, mismatched digests, oversized content and revision overflow.
A stale cursor or absent producer must produce an explicit refusal, never fabricated empty success.
Retention exhaustion must refuse before partial retention or an accepted-head change.
Inject failure before and after durable commit. Recovery must expose one complete outcome or none.
Record source SHA, binary digest, generated contract digest and authenticated operation receipts.
Client mocks and producer unit tests cannot close R085.8.

## R086 planned safe-eviction acceptance

All names below are proposed tests, not completed tests or new bindings to existing receipts.
Implement them in tests/safe_eviction_read_through.rs using existing production helpers.
Each child is focused; the rollup waits for all children and the complete served journey.

| Requirement | Planned proving test |
|---|---|
| MEDULLA-DURABLE-KERNEL-R086 | `served_safe_eviction_read_through_journey` |
| MEDULLA-DURABLE-KERNEL-R086.1 | `read_through_model_outcomes` |
| MEDULLA-DURABLE-KERNEL-R086.2 | `selected_residents_leave_durable_rows_unchanged` |
| MEDULLA-DURABLE-KERNEL-R086.3 | `evicted_blob_reads_without_resident_growth` |
| MEDULLA-DURABLE-KERNEL-R086.4 | `startup_attaches_graph_scoped_read_through` |
| MEDULLA-DURABLE-KERNEL-R086.5 | `served_evicted_reads_preserve_scope` |
| MEDULLA-DURABLE-KERNEL-R086.6 | `production_eviction_retains_unconfirmed_candidates` |
| MEDULLA-DURABLE-KERNEL-R086.7 | `backend_failure_is_not_absence_evidence` |
| MEDULLA-DURABLE-KERNEL-R086.8 | `restarted_engine_reads_an_evicted_node` |
| MEDULLA-DURABLE-KERNEL-R086.9 | `served_safe_eviction_read_through_journey` |

The served journey uses a real authenticated engine, disposable durable directory and independent clients.
Acknowledge distinct node-property writes before requesting EvictLRU with a small cap.
Record selected identities and resident counts before and after the call.
Prove the target is absent from raw-core resident properties and topology before GetNodeProperties.
Observe the actual core seam, facade adapter and scoped redb lookup without replacing their results.
Assert exact committed bytes, unchanged durable node rows and no read-driven resident growth.
Also read a resident node and observe that it avoids the durable read-through seam.
Capture the served admission receipt separately from memory-only projection removal.

Exercise periodic-cap and tenant-budget callers as separate focused wiring cases.
At each eviction caller, test absent persistence, failed presence, wrong vector length and false entries.
A narrowly injected presence fault can exercise that branch but cannot replace the real-store journey.
Check retained residents and unchanged durable node rows after each refusal.
For read failure, use an unavailable real backend or an unreadable durable row.
Record the backend error, adapter warning and missing bytes; do not label this successful absence.
For genuine absence, separately establish a successful scoped backend lookup returning no row.

Use the same node ID with distinct properties in two authorized graphs.
Read each after eviction, then repeat with a principal denied access to one graph or row.
No hidden property may escape through the raw-core fallback or diagnostic evidence.
Include an incomplete Bloom filter whose missing resident node is nevertheless durable.
Test a complete-filter negative separately from unavailable storage.

Restart the process on the same directory through normal startup wiring.
Use a fresh authorized client. If eager load makes the target resident, evict it again first.
Prove a new cache miss and durable read after restart; compare the acknowledged properties.
Opening an index or serving an eagerly restored resident does not prove this path.
Record source SHA, binary digest, enabled features, exact command, graph/node scope,
request correlation, commit acknowledgment, residency observations and terminal result.
Preserve all existing owner receipts. New results must identify the exact source exercised.
This specification batch makes no runtime execution or implementation-complete claim.

<!-- spec-definition-patch:owner-batch-20261012-durable-graph-kernel -->
## Durable-kernel owner acceptance targets — 2026-10-12

Planned cases only. None have run. Medulla producer acceptance stops at its authenticated durable memory contract. Cross-owner cases remain blocked where called out.

- **`MEDULLA-DURABLE-KERNEL-R028.1` / AU:21:** Write a checkpoint and both referenced memory revisions through R085. Restart and read back the same revision and digests. Refuse wrong tenant/graph, stale head or stale WorkItem fence. Also refuse a crash/race between snapshot write and WorkItem pointer update. Refuse digest mismatch, changed-body replay identity or unavailable storage without advancing the head. Meristem must resume the same run and fence. Arbora must continue from the exact existing host-issued stream cursor without duplicate or skipped output. Those cases remain blocked on the atomic snapshot/pointer transaction or crash-recovery contract. Short-/long-term kind and retention decisions, Meristem resume-binding and host cursor/replay semantics also remain open. Preserve `MEDULLA-DURABLE-KERNEL-R028`, `MEDULLA-IDENTITY-R006`, `ARBORA-A2A-R001`, `ARBORA-A2A-R001.1`, `ARBORA-OPS-R001`. Source hash `d89bb9c69e2fdef6a5c3fd428ede3f91868539056c2b949700867d8d8087d1e7`.
- **`MEDULLA-DURABLE-KERNEL-R085.9` / AU:48:** Store all required search-state fields and evict context. Restart or use an independent client. Read back byte-identical exact UTF-8 JSON content at its bound revision. Validate the envelope field types and the equality of state revision and snapshot revision. Refuse missing/malformed fields, oversize content, wrong authority, stale graph, digest mismatch, and unavailable storage without changing the head. Stale expected revision preserves the accepted head and retains the candidate conflict. Meristem continuation must restore all four fields using the exact returned document ID/revision before compacted context is used. That last case is blocked on Meristem’s per-entry schema/source identity and continuation receipt decision. Preserve Meristem R013.13/R020.1 and Medulla R085.1/.4/.7. Source hash `8c76fa1637e0cc2082a1cce16b9d637f12b310ae96d1fe9a50ec9c64f8e23944`.
- **`MEDULLA-DURABLE-KERNEL-R085.10` / AU:49:** Write/read an authorized note across restart with source revision, digest, and provenance intact. Ensure cross-tenant/revoked reads are redacted. Stale writes must retain conflict without replacing the accepted head. Capacity exhaustion must refuse before effects. Meristem must separately test the decided capture trigger, relevance/staleness rule, and conflict presentation without using unresolved content. That acceptance is blocked on Meristem defining those semantics. Preserve Meristem R022.3 and Medulla R085.2/.5/.7. Source hash `2362fd76bf40223aeb813866392670b65eae13d02a725b1af9a2e6711542dd12`.

- **`MEDULLA-DURABLE-KERNEL-R028.1.1`:** Run `checkpoint_native_authenticated_cas_replay_and_owner_isolation` through authenticated dispatch and MutationBatch. Exact nonce reuse returns `REPLAY_NONCE_CONSUMED`. Live-claim and expected-head refusals return `CONFLICT`, not `INTERNAL`. Closed error-routing tests preserve only the declared method codes. A fresh nonce with identical canonical retry identity returns the original receipt, including after expiry/reopen, without another snapshot/version. Changed content/references/fence conflict; wrong owner/context/tenant/run refuse before ledger replay. Run `run_checkpoint` DTO tests, native `work_item/checkpoint/tests.rs` for immutable admitted-context authority despite metadata replacement, wrong graph/tenant/run and missing admission/node-type refusals, CAS/digest/fence and rollback tests. Run graph predecessor/offline upgrade/reopen tests and regenerate owner layout/public contracts from the exact candidate. Python `tests/test_run_checkpoint_client.py` requires those fresh generated methods. Hosted job 120276448287 passes the full authenticated checkpoint fixture after the refusal fixes. Replay, expiry, owner isolation, claim-bearing read refusal and reopen now have native evidence for head `c065ad9a`. The integrated final source still requires a fresh complete gate run. Hosted storage predecessor and migration-preservation tests pass, as recorded in the plan. Both Python facade tests pass with the hosted generated send methods. Final composed-head export, parity and complete gate qualification remain pending.
- **`MEDULLA-DURABLE-KERNEL-R028.1.2` / AU:21:** Resolve and read back both referenced memory revisions after restart. AU must resume the same run and current fence, preferring its current-run checkpoint. Shared served paths must enforce durable context ordering and continue the exact host-issued cursor without duplicate or skipped output. Retention/release, memory existence and host replay authority remain open. Preserve `MEDULLA-DURABLE-KERNEL-R028`, `MEDULLA-IDENTITY-R006`, `ARBORA-A2A-R001`, `ARBORA-A2A-R001.1`, `ARBORA-OPS-R001`. Source hash `d89bb9c69e2fdef6a5c3fd428ede3f91868539056c2b949700867d8d8087d1e7`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run. They describe required future acceptance, not evidence.

- **`MEDULLA-DURABLE-KERNEL-R038.100` / EG:60. Source `b36ed0f40742d2f455b4eae67f2fb64edfb7a6ea87b93cbb8ac0046970728458`:** Start a fresh embedded profile without an external service and read/write an explicitly ephemeral namespace. Confirm no persistent namespace is silently downgraded. Start served mode against its configured endpoint and confirm its declared sync/async class and acknowledgment boundary. Refuse an unknown profile and an ephemeral-to-durable class mismatch. Read back profile and namespace declarations. Producer/consumer: Medulla owns profile and storage selection. Transport mode is separate from namespace DurabilityClass. Preserve R038 durability classes, sync default, redb authority and authorization/HMAC isolation. Parent `MEDULLA-DURABLE-KERNEL-R038` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — graph change feed

No test in this section has run. It is a future contract.

- `MEDULLA-DURABLE-KERNEL-R043.100`: Use two authenticated served GraphView clients to take a consistent snapshot. Commit concurrent `ApplyChangeEnvelope` mutations and consume ordered events without refresh. Reconnect from the opaque cursor and independently read back committed state. Exercise stable dedup ids, tombstones, expired cursor recovery, stale graph incarnation and revoked permissions without denied counts/edges. Measure Medulla commit-to-visible p95 only through first authorized feed read. Arbora/Overstory acceptance must separately set and confirm a versioned browser commit-to-visible p95. It must also set explicit numeric readable-layout limits for visible node/edge budget, interaction and label bounds. Medulla feed latency is not end-to-end evidence.
- Exercise tombstones, expired cursor recovery, graph-incarnation mismatch and authorization revocation. Denied edges/counts remain invisible. Measure p95 commit acknowledgement to first authorized read ≤1000 ms using the recorded latency-profile revision. Overstory R030.8 separately proves concurrent layout remains readable.
<!-- /spec-definition-patch -->

## Planned served acceptance — benchmark measurement

- `MEDULLA-DURABLE-KERNEL-R042.100`: read back benchmark input and measured-result bindings for at least two releases. Independently recompute input-token TPS and currency-rounded per-query cost, proving startup is excluded. Refuse missing full input-token count/tokenizer/price, zero or invalid inference time, invalid currency exponent and different workload/profile settings. This does not assert that any performance comparison passes.

## MEDULLA-DURABLE-KERNEL-R099 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-DURABLE-KERNEL-R099.1` | A serde round trip keeps every field. |
| Behavior | `MEDULLA-DURABLE-KERNEL-R099.2` | A unit test with four graphs on four shards observes overlapping commit windows. |
| Wiring | `MEDULLA-DURABLE-KERNEL-R099.3` | A dispatch test asserts one report per sub-batch from the served arm. |
| Refusal | `MEDULLA-DURABLE-KERNEL-R099.4` | Tests cover the size refusal and one failed sub-batch beside committed ones. |
| Evidence | `MEDULLA-DURABLE-KERNEL-R099.5` | A test reads the reports and scrapes the two metrics. |
| Hot-path proof | `MEDULLA-DURABLE-KERNEL-R099.6` | The served test passes, and the benchmark receipt meets the budget. |

Hot-path proof for `MEDULLA-DURABLE-KERNEL-R099`: `tests/shard_write_fanout.rs::cross_graph_writes_fan_across_shard_writers`.

## MEDULLA-DURABLE-KERNEL-R100 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Model | `MEDULLA-DURABLE-KERNEL-R100.1` | A serde round trip keeps every field. An inline credential is rejected. |
| Behavior | `MEDULLA-DURABLE-KERNEL-R100.2` | Unit tests replay one batch twice and observe one target state. |
| Wiring | `MEDULLA-DURABLE-KERNEL-R100.3` | A served test registers each kind and observes outbox delivery. |
| Refusal | `MEDULLA-DURABLE-KERNEL-R100.4` | Tests cover both refusals and an unchanged cursor. |
| Evidence | `MEDULLA-DURABLE-KERNEL-R100.5` | A test reads the receipts and one digest comparison. |
| Hot-path proof | `MEDULLA-DURABLE-KERNEL-R100.6` | The served test passes against real targets, and the benchmark receipt meets the budget. |

Hot-path proof for `MEDULLA-DURABLE-KERNEL-R100`: `tests/mirror_graph_sinks_served.rs::each_sink_replays_after_outage`.

- MEDULLA-DURABLE-KERNEL-R028.2: terminal event readback, restart identity, tenant isolation, missing/corrupt binding refusal and legacy unavailability. Receipt 2026-10-19 UTC on main `ed1f25ba023c29a8da6734819005c38bb34f18b9`: the five wired native tests from `2835f06de` (PR #622) ran and all passed (5 passed, 0 failed): `redb_store::mutation_batch_tests::terminal_extension_commits_rows_outbox_and_replays_after_reopen`, `redb_store::work_item::read::tests::an_outcome_read_returns_only_digest_verified_provenance_of_the_tenants_item`, `redb_store::work_item::read::tests::event_bindings_cannot_be_forged_and_missing_bound_event_is_not_legacy`, `redb_store::work_item::read::tests::a_bound_event_read_refuses_a_committed_batch_without_its_event`, `redb_store::work_item::read::tests::present_event_payload_corruption_is_refused`; `tests/test_work_item_read_client.py` passed 14/14, including `test_get_outcome_returns_the_verified_provenance_view`. Every acceptance bullet of the row is proven by these tests, so the pending condition is cleared.
## MEDULLA-DURABLE-KERNEL-R101 acceptance

| Case | Row | Proving test and observation |
|---|---|---|

| Model | `MEDULLA-DURABLE-KERNEL-R101.1` | `crates/medulla-jobs/src/downtime.rs::tests::job_type_round_trips`: A serde round trip keeps every field, and the shape refuses a job type without a budget. |

| Behavior | `MEDULLA-DURABLE-KERNEL-R101.2` | `crates/medulla-jobs/src/downtime.rs::tests::priority_then_budget_fit`: Three job types run in priority order, and one with a false precondition is skipped. |

| Wiring | `MEDULLA-DURABLE-KERNEL-R101.3` | `crates/medulla-jobs/src/downtime.rs::tests::tick_enqueues_leased_jobs`: A served tick enqueues the selected jobs under background leases. |

| Refusal | `MEDULLA-DURABLE-KERNEL-R101.4` | `crates/medulla-jobs/src/downtime.rs::tests::downtime_refusals`: Each case returns its code, and no foreground lane runs downtime work. |

| Evidence | `MEDULLA-DURABLE-KERNEL-R101.5` | `crates/medulla-jobs/src/downtime.rs::tests::selection_recorded`: A served read finds the selection and the budget use of each job. |

| Hot-path proof | `MEDULLA-DURABLE-KERNEL-R101.6` | `tests/scheduler/test_downtime_scheduler_served.py::test_idle_window_runs_priority_jobs_and_preempts`: Jobs run in priority order, and the foreground query preempts them within 250 ms. |


Hot-path proof for `MEDULLA-DURABLE-KERNEL-R101`: `tests/scheduler/test_downtime_scheduler_served.py::test_idle_window_runs_priority_jobs_and_preempts`.

## MEDULLA-DURABLE-KERNEL-R102 acceptance

| Case | Row | Proving test and observation |
|---|---|---|
| Served contract | `MEDULLA-DURABLE-KERNEL-R102.1` | `src/server/kv/dispatch_tests.rs::kv_dispatch_put_get_scan_delete_cas`: every KV method answers its typed result through the real dispatch. |
| Scope and authorization | `MEDULLA-DURABLE-KERNEL-R102.2` | `src/server/kv/dispatch_tests.rs::kv_keys_are_scoped_by_graph_and_writes_need_kv_write`: another graph sees nothing; a `kv:read` caller is refused a write. |
| CAS and scan | `MEDULLA-DURABLE-KERNEL-R102.3` | `src/server/kv/dispatch_tests.rs::kv_scan_pages_are_bounded_and_resume_from_the_cursor`: 25 keys arrive in bounded pages in order; a foreign cursor refuses. |
| Durable mutations | `MEDULLA-DURABLE-KERNEL-R102.4` | `src/server/kv/dispatch_tests.rs::kv_fresh_nonce_replays_stable_operation_without_duplicate_write`: the retried CAS returns the stored verdict and writes once. |
| Hot-path proof | `MEDULLA-DURABLE-KERNEL-R102.5` | `src/server/kv/dispatch_tests.rs::kv_served_hot_path_meets_latency_budgets`: measured p50/p99 within the budgets. |
| Shared namespace | `MEDULLA-DURABLE-KERNEL-R102.6` | `src/server/kv/dispatch_tests.rs::kv_shared_namespace_spans_principals_and_needs_kv_shared_write`: private keys never cross principals; a shared key does, and a coarse-write principal is refused. |
- **MEDULLA-DURABLE-KERNEL-R028.3:** `src/redb_store/work_item/lease/tests.rs` tests live, expired and missing worker lease expiry against engine time, unchanged refused rows and unfenced compatibility. Runtime execution pending hosted CI.

## Pending envelope storage

`MEDULLA-DURABLE-KERNEL-R039.3`: use a native shard to register an immutable envelope and consume it with the exact WorkItem preimage. Check native provenance, stale fence/revision, expiry, changed metadata/envelope, duplicate registration and replay. Refuse a plausible NODES-only lease tuple without native claim authority. Every refusal preserves both rows. This is storage acceptance, not human approval or execution acceptance.
`MEDULLA-DURABLE-KERNEL-R039.4`: register an exact same-tenant active plan preimage with a pending envelope. Consume both leases and clear the WorkItem reference atomically. Refuse omitted or changed pairs, expired or revoked plans, and replay without partial writes. Check that paired served requests refuse before dispatch; ordinary unpaired requests retain their policy checks. This storage proof does not authenticate human approval or task hierarchy. Check the inherited 64 KiB preimage, 512-byte identifier and 24-hour lifetime bounds. The transaction changes three rows. Extend `benches/redb_write_latency_bench.rs` with a paired-consumption scenario before reporting performance evidence. That scenario and its measurements remain pending.

`MEDULLA-DURABLE-KERNEL-R039.4.1`: run the existing typed preimage tests, native three-row consumption and unchanged-row refusal tests, and paired served-entry refusal test. Preserve the original replay, tenant, lifetime and unpaired compatibility assertions. These tests do not prove performance or authorized served execution.

`MEDULLA-DURABLE-KERNEL-R039.4.2`: pending benchmark coverage in `benches/redb_write_latency_bench.rs` must exercise paired consumption through the durable path. Retain workload configuration and commit-phase measurements with the source revision. Generic write results are not paired-call evidence.

- **MEDULLA-DURABLE-KERNEL-R028.4:** Native admission tests cover immutable caller attribution, duplicate run refusal, and generic-write refusal. Native execution remains pending.

- **MEDULLA-DURABLE-KERNEL-R028.5:** `current_plan_publication_readback`, `current_plan_refusals_preserve_row`, `renewal_does_not_extend_snapshot_and_generic_nodes_carry_no_plan` exercise private storage; `current_plan_native_publish_read_and_receipt_replay` exercises authenticated admission, native claim, ordered publication, foreign-owner refusal, replay and reopen. Native acceptance remains pending.

## Agent-memory substrate claims (2026-10-22)

| Case | Requirement | Proving test |
|---|---|---|
| Tier ladder from accelerator memory to durable storage | `MEDULLA-DURABLE-KERNEL-R083.5` | `crates/medulla-kvcache/tests/tier_ladder.rs::demote_then_promote_keeps_bytes` |
| Graph-guided paging policy | `MEDULLA-DURABLE-KERNEL-R083.6` | `crates/medulla-kvcache/tests/graph_paging.rs::central_blocks_stay_hot_and_neighbors_prefetch` |
| Long-context coherence across tiers | `MEDULLA-DURABLE-KERNEL-R083.7` | `crates/medulla-kvcache/tests/long_context.rs::ten_million_token_context_pages_in_coherent` |
| Tier refusals | `MEDULLA-DURABLE-KERNEL-R083.8` | `crates/medulla-kvcache/tests/tier_ladder.rs::cold_write_failure_and_digest_mismatch_refuse` |
| Cross-modal query latency benchmark | `MEDULLA-DURABLE-KERNEL-R042.2` | `crates/medulla-plan/benches/hybrid_queries.rs` |
| KV-cache tiering benchmark | `MEDULLA-DURABLE-KERNEL-R042.3` | `crates/medulla-kvcache/benches/tiering.rs` |
| Ingest throughput benchmark | `MEDULLA-DURABLE-KERNEL-R042.4` | `benches/ingest_throughput.rs` |
| Wire protocol benchmark | `MEDULLA-DURABLE-KERNEL-R042.5` | `benches/wire_protocols.rs` |
| Benchmark publication and regression gate | `MEDULLA-DURABLE-KERNEL-R042.6` | `tests/test_benchmark_receipts.py::test_regression_over_ten_percent_fails` |
| Served wire protocol listeners (rollup) | `MEDULLA-DURABLE-KERNEL-R860` | Rollup: satisfied when the test of every child passes. |
| PostgreSQL wire listener | `MEDULLA-DURABLE-KERNEL-R860.1` | `tests/pgwire_roundtrip.rs` |
| Bolt listener | `MEDULLA-DURABLE-KERNEL-R860.2` | `tests/bolt_roundtrip.rs::run_returns_records` |
| RESP listener | `MEDULLA-DURABLE-KERNEL-R860.3` | `tests/resp_roundtrip.rs::set_get_maps_to_kv_methods` |
| S3-compatible object API | `MEDULLA-DURABLE-KERNEL-R860.4` | `tests/s3_roundtrip.rs::put_get_list_delete` |
| Listener refusals | `MEDULLA-DURABLE-KERNEL-R860.5` | `tests/wire_listeners_one_store.rs::unauthenticated_sessions_refuse` |
| Hot-path proof: one store behind four protocols | `MEDULLA-DURABLE-KERNEL-R860.6` | `tests/wire_listeners_one_store.rs::write_one_protocol_read_all` |
| Lifecycle replay and listing survive a patch upgrade (rollup) | `MEDULLA-DURABLE-KERNEL-R861` | Rollup: satisfied when the test of every child passes. |
| Lifecycle replay compares the stored identity | `MEDULLA-DURABLE-KERNEL-R861.1` | `src/server/mutation_batch/lifecycle_tests.rs::lifecycle_replay_compares_identity_not_recompiled_bytes` |
| Graph listing survives a restart and a patch upgrade | `MEDULLA-DURABLE-KERNEL-R861.2` | `tests/test_upgrade_store_listing.py::test_reopened_store_lists_graphs_for_the_stored_policy` |
| Engine migrates an older supported store at open | `MEDULLA-DURABLE-KERNEL-R070.1` | `src/server/persistence/store_upgrade/auto_migrate_tests.rs::an_interrupted_migration_resumes_at_the_next_open` and the other tests of that file |
| Previous-patch store migrates and serves at open | `MEDULLA-DURABLE-KERNEL-R070.2` | `tests/test_store_auto_migrate.py::test_open_migrates_a_previous_patch_store_and_serves` |
| Copied store or bundle restores onto new files (rollup) | `MEDULLA-DURABLE-KERNEL-R862` | Rollup: satisfied when the test of every child passes. |
| Restore reads bundle sources by content | `MEDULLA-DURABLE-KERNEL-R862.1` | Copied-bundle restore unit test (not yet written; see #709). |
| Explicit rebind of a copied data directory | `MEDULLA-DURABLE-KERNEL-R862.2` | `store-upgrade rebind` command tests (not yet written; see #709). |
| Copied bundle restores and serves | `MEDULLA-DURABLE-KERNEL-R862.3` | `tests/test_store_copy_restore.py` (not yet written; see #709). |
- **MEDULLA-DURABLE-KERNEL-R028.5.1:** `current_plan_task_plan_validation_and_legacy_roundtrip` checks JSON and named MessagePack compatibility. `current_plan_task_plan_rejects_unpaired_or_changed_content` refuses unpaired fields, tampered executable fields, policy, digests and ordering. The original proposal must reproduce every TaskPlan field.
- **MEDULLA-DURABLE-KERNEL-R028.5.2:** `current_plan_attachment_is_immutable_and_scaffold_updates_preserve_it` checks initial and later attachment. `current_plan_attachment_refusals_preserve_rows` checks conflicting plans, malformed digests, foreign owners, stale fences and expiry. Refusals leave both private snapshot and owner rows unchanged.
- **MEDULLA-DURABLE-KERNEL-R028.5.3:** `publication_attachment_changes_content_not_retry_identity` checks stable identity and changed canonical content. `current_plan_native_publish_read_and_receipt_replay` exercises authenticated attachment, unchanged replay receipts, conflict and exact restart readback. Expired replay returns historical evidence only.
- **MEDULLA-DURABLE-KERNEL-R028.5.4:** Planned benchmark `current_plan_publication_readback` must measure attached-plan latency and transient memory against the parent budgets. No benchmark implementation or measurement receipt is claimed by the functional attachment tests.

- **MEDULLA-DURABLE-KERNEL-R028.4.1:** `run_admission_recovery_preserves_original_request`, `run_admission_recovery_checks_complete_owner`, `run_admission_recovery_refuses_missing_evidence` and `run_admission_snapshot_cannot_be_planted_or_removed` check durable records and refusal. Served and client tests check typed owner-only reads.
- **MEDULLA-DURABLE-KERNEL-R028.4.2:** Record a dedicated native recovery benchmark; acceptance remains pending.
## Ordinary terminal completion acceptance (MEDULLA-DURABLE-KERNEL-R028.6)

- Commit success with observed counters. Preserve the exact input and receipt.
- Commit failure with observed counters and with empty usage. Empty stays empty.
- Refuse negative, fractional, boolean, unsafe-sized or inconsistent counters.
- Refuse foreign run, tenant, graph, worker, lease epoch and fencing token.
- Refuse expired lease without changing the row, receipt or outbox, even with
  backdated caller time. Bind successful completion time to the engine commit.
- Refuse an ordinary completion with a delegated extension or failed answer.
- Replay with the same and a new key. Preserve the first terminal receipt.
- Reopen storage and read the same scope, sequence, ordinal and payload digest.
- Refuse missing, corrupt, duplicated or mismatched ledger/outbox evidence.
- Refuse forged bindings through generic row writes, replacements and removal.
- Keep current-source native tests, generated freshness and served tests separate.
  No historical test receipt establishes acceptance for a new source revision.

- **`MEDULLA-DURABLE-KERNEL-R028.1.3`:** Record exact-source hosted native, migration, fresh layout/export and generated Python client results for the R028.1.1 fixtures. No local native build or test was run for this slice; no broad successful conversation continuation is claimed.

- **`MEDULLA-DURABLE-KERNEL-R028.1.3.1`:** Validate the hosted source, recipe and artifact hashes against the import. Run `tests/test_run_checkpoint_client.py` and the neighboring read and retry facade suites from the plan.
- **`MEDULLA-DURABLE-KERNEL-R028.1.3.2`:** Run the R028.1.1 native fixtures and predecessor migration against the final reviewed source. Regenerate the layout and public contracts. Attach full repository gate receipts. Pending, not satisfied by the export or Python facade checks.

## Admission retry replay (MEDULLA-DURABLE-KERNEL-R028.4.4)

Tests live beside `tests/adopt_workitem_authenticated_replay.rs`.
They drive the served MutationBatch path with authenticated requests.

- Repeat an admission with a refreshed request context. Require the original receipt.
- Require no new outbox event and an unchanged WorkItem version, also after a restart.
- Refuse a changed policy, input or command.
- Refuse missing or corrupt retained evidence with `RUN_ADMISSION_UNAVAILABLE`.
- Open a store written by the server before this change and replay one of its admissions.

## Ordered conversation admission (MEDULLA-DURABLE-KERNEL-R028.4.3)

Source tests live in `src/redb_store/work_item/conversation/tests.rs`.
They use the existing native row applier inside a maintenance transaction.
They do not prove the server MutationBatch replay path or model continuation.
Each distinct fixture write uses a distinct maintenance operation ID. Reusing a
committed operation tests replay refusal, not a fresh state transition or guard.

- Admit revision zero. Refuse a second writer with the same expected head.
- Refuse a foreign owner and an active preceding run without a new WorkItem.
- Finish the first run, then admit its exact successor. Ignore clock order.
- Refuse failed, cancelled, dead-letter, missing and unknown preceding state.
- Reopen the shard and retain its head revision and original predecessor.
- Refuse generic create, replacement, compare-and-set and removal of a head.
- Preserve omitted-field legacy serialization. Refuse unknown typed fields and
  disagreement with the original A2A context. A registered context cannot bypass
  its head through metadata-only admission.

Before acceptance, run these tests on the exact source and add server tests for
same-key replay with a fresh nonce and changed expected revision or predecessor.
Check original receipt recovery, full rollback and graph-incarnation isolation.
Regenerate request contracts and check installed-client compatibility.
Keep the existing `medulla-types/tests/contract_wave.rs` Method-size regression test
unchanged. Box the optional admission so it does not enlarge every Method value.
The first native run measured 640 bytes against the existing 616-byte bound.
Qualify the correction on fresh source rather than raising that bound.

A complete continuation test must restart Medulla, Arbora and the client between
turns, inspect the actual model request for prior user, assistant and tool
messages, and prove no repeated tool effect. It must also prove fresh new-turn
budgets, exact same-run checkpoint precedence, bounded context and explicit
conflict handling. Those checks remain open under the existing parent rows.

- **MEDULLA-DURABLE-KERNEL-R030.1:** Typed host-start tests cover bounded bindings,
  expiry clamping and caller provenance injection. Native shard tests cover
  absent issuer refusal, immutable issuer stamping, collision, tenant read
  isolation, revocation and generic consumption refusal. The signed dispatch
  test `signed_issuer_is_stamped_and_retry_retains_attribution` must prove
  exact-scope refusal and stable native principal fingerprint across a retry.
  These Rust tests remain unexecuted. Python provenance and existing lease-client
  checks passed; dedicated issue transport runtime validation remains pending.
  Session liveness, external execution reservation, restart and latency qualification
  remain separate pending acceptance; fixture objects do not prove live policy.

## Current-main host-start qualification boundaries

R030.1.1 binds the typed host-start tests and `tests/test_host_start_client.py`. R030.1.2 binds the native redb host-start tests. R030.1.3 binds the signed-dispatch host-start tests. The Rust fixtures remain unexecuted in this reconciliation.

R030.1.4 retains the complete parent qualification. Old canonical outputs do not qualify current-main source. Fresh hosted generation, native suites, installed consumer and benchmark evidence remain required. No issuer enrollment, policy grant or infrastructure start is performed.

The authored R030.1.3 signed negative fixture sends a foreign body tenant through the signed issuer request path. It also attempts generic host-start issuance with forged provenance inside grant JSON. Each must refuse with no grant or graph-version change. A subsequent legitimate dedicated issue must still succeed with native issuer attribution. This test remains unexecuted locally and does not complete R030.1.3 or R030.1.4.


### Original context binding bridge (MEDULLA-DURABLE-KERNEL-R028.1.1)

- Publish and read under a typed-only original conversation admission.
- Accept matching typed and original A2A metadata bindings.
- Retain legacy original-metadata coverage and reject mutable metadata rebinding.
- Refuse foreign owners and conflicting, null or non-string original metadata.
- Compare native snapshot count and WorkItem state before and after refusals.
- Run the signed typed-only checkpoint fixture through MutationBatch, then reopen.

These composed-source fixtures require fresh native execution. Prior green
checkpoint and admission runs do not qualify this new integration.
