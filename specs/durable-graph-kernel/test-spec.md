# Test specification

All fixtures run against a disposable local data directory and generated credentials. Fault injection is deterministic and records seed, revision, platform, command and artifact path. Tests may use local multi-process nodes and disposable containers; no private cluster, inventory, credentials or always-on service is needed.

| Scenario | Requirements | Positive fixture and expected result | Negative/failure fixture and expected result |
|---|---|---|---|
| T-KG-01 batch terminality | KG-01, KG-02 | Graph+table+KV batch commits once. Readback and retry return same terminal result | Inject failure at every commit phase. Restart yields all-or-none. Lost-ack retry does not duplicate |
| T-KG-02 table-only path | KG-02 | Concurrent table-only writes use current atomic path and match reference corpus | Mixed unsupported member set refuses before any table or graph write |
| T-KG-03 owner lineage | KG-03 | Load each supported golden manifest. One validation per transaction | Unknown predecessor or extra physical table returns typed upgrade/ownership error. Stale cached binding never crosses transactions |
| T-KG-04 scrub | KG-04 | Corrupt one persisted node. The scrub reports a typed finding and resumes the cursor after restart | Rate cap prevents writer starvation. The scrub hides unreadable payloads and never silently repairs them |
| T-KG-05 outbox | KG-05 | Healthy successors deliver after a failing head is rejected/rewound with ordered receipts | Retry one head 16 times. Successors keep their own zero-attempt count. Reasoning skip without rebuild is refused |
| T-KG-06 CDC/mirror | KG-06 | Commit 100 ordered graph/table changes, disconnect target, reconnect, replay and match digest | Crash after target apply before cursor; replay is idempotent; empty default target guard and schema drift fail visibly |
| T-KG-07 scope | KG-07 | Owner and explicitly granted principal can access allowed data | Cross-tenant/principal probes deny SQL, UQL, TSDB and KV access. UDF, foreign-source and cache probes also deny access. Revoked grants deny the next use |
| T-KG-07a auth modes | KG-07, KG-09 | `none`, `local` and `external` each resolve to the same internal principal/RBAC evaluator | Spoofed name in `none`, stale local session after mode switch, wrong external issuer/audience/tenant/policy version and generic elevation lease all deny |
| T-KG-08 audit | KG-08 | Reservation precedes governed effect and links truthful outcome | Audit writer unavailable, missing class, wrong tenant, duplicate ID and crash after effect all fail or reconcile without false success |
| T-KG-09 approval | KG-09 | Two distinct principals approve exact digest before expiry | Self-approval, generic lease, altered digest, expiry and replay all deny and audit refusal |
| T-KG-10 Raft | KG-10 | Repeated 3-node multi-group writes/failover preserve independent progress and one committed result | Inject slow peer/leader loss. Unaffected groups progress. Failed assertion emits causal ring-buffer trace |
| T-KG-11 durability | KG-11, KG-12 | Sync survives kill. Async oldest-unflushed age stays ≤100 ms. Hot structures rebuild from log | Ephemeral disappears on restart. Forced flush lag yields explicit degraded/refusal, never silent class change. Concurrent primitive history linearizes |
| T-KG-12 wire batches | KG-13, KG-14 | RESP and pgwire batches return N ordered replies after one commit. Point route avoids DataFusion session | Middle operation fails. No earlier reply promises a commit. Schema/policy change invalidates cached SQL plan |
| T-KG-13 parity and benchmark | KG-15 | Differential corpus, restore drill and benchmark report include exact config, hardware class, code revision and digest | Accepted deviation is named/reviewed; a regression fails instead of rewriting baseline; missing benchmark row blocks superiority claim |

## Commands and quality evidence

Use focused package tests for `eg-types`, `eg-transaction`, `eg-storage`, `eg-stream`, `eg-tsdb`, and affected server integration tests, followed by the configured full pre-commit and hosted parity commands in `plan.md`. Run `python3 scripts/check_universal_read_rls.py` and `python3 scripts/check_persisted_mutation_contract.py` when their paths are affected. The repository's CCCC changed-function gate rejects new cyclomatic >10 or cognitive >15; run the configured Dupehound changed-function and jscpd differential gates without new suppressions, plus KISS, Rust architecture lint and clippy. Record scanner versions from the checked-in scanner contract. A benchmark is evidence only for its measured hardware/workload/durability class; no live environment is an implicit prerequisite to unit or contract acceptance.

## Evidence record

For each scenario, add a dated table row in the PR or an evidence file with exact main commit, command, CI URL, test count, result, fixture digest and known limitation. A passed local branch test means `BUILT`; merged-head tests and reviewed negative cases are required for `ACCEPTED`. Do not infer acceptance from the presence of these documents.

## Worker registry cases

R074.1: register a 16-core, 64-GiB worker supporting general/hydration, serde round-trip its record, heartbeat to extend expiry, and expire exactly at the new deadline. Refuse invalid capacity, blank identity, missing lanes, zero/overflowing TTL, duplicate live registration, unknown/expired heartbeats and backward clocks with unchanged state. Re-registration is allowed after expiry. Tests are native in worker_registry.rs.

R074.2 remains pending: exercise these transitions through real authenticated dispatch and generated contracts, including cross-tenant and cross-principal refusals. Pure tests are not served acceptance.

## T-KG-14 scheduler configuration producer

R076.1 and EG-PLACEMENT-R002.2 bind native tests in `crates/eg-types/src/scheduler_config.rs`: real-serde default and override round trips. Exact floor sum. Over-subscription. U32::MAX floor overflow. Zero disabling bounds. CPU >1000, memory=0 and blank-lane refusals. Missing/unknown/negative/out-of-range wire fields. Run `cargo test -p eg-types scheduler_config::tests` on a warm build, or compile that exact source with real serde/serde_json dependencies for isolated producer evidence. The latter is not full-crate or server validation. R076.2 still requires the served settings/admission scenario.

## T-KG-15 association submission schema

R082.2 binds the table-driven tests in `src/server/handlers/jobs/graph_tests.rs`.
Fifteen malformed outer-list, transaction-list and item shapes go through the
actual native request-body decoder and return `INVALID_ARGUMENT`, correlation
id 82, a bounded encoding detail and no result. A matching valid full request
is decoded and submitted to the real governor with support 0.1, confidence 0.5
and fpgrowth defaults. Governor tables cover NaN, both infinities, thresholds
outside [0, 1], unknown algorithms, all five accepted names/aliases and inclusive
0/1 boundaries. The production reader uses the same decoder; no second refusal
implementation is introduced for tests.

2026-10-11 focused evidence: the exact complete submission-test module plus the
two existing hash tests passed (6 tests), compiled against real serde,
rmp-serde, sha2 and the actual jobs-enabled eg-types crate. The harness includes
the exact production decoder, governor, submit validator and authority/hash
functions. This proves schema admission and decoding, not socket serving,
authentication, persistence or the executor lifecycle. The broader R082 rollup
and its other children remain open. Normal warm-tree reproduction is
`cargo test --lib --features jobs mine_associate_submission_tests`.

## R082.6 historical association parity

| Requirement | Proving tests | Evidence |
|---|---|---|
| EG-DURABLE-KERNEL-R082.6 | `deleted_au_six_transaction_order_and_thresholds`; `minimum_count_and_zero_consequent_lift_boundaries` | Six historical transaction cases cover all three engines, stable ordering, thresholds, metrics, and zero lift. |

Run `cargo test -p eg-compute mining::association`.
Isolated compilation includes the complete production association and math modules with real serde_json.
This proves native kernel behavior, not server transport or durable execution.

## R011 and R014 runtime acceptance

These rows separate declaration evidence from runtime acceptance. Planned test names do not claim passing results.
The producer children retain their current evidence. Neither producer proves runtime execution.

| Requirement | Proving tests and source | Acceptance |
|---|---|---|
| EG-DURABLE-KERNEL-R011.2 | `crates/eg-types/src/pre_merge_test_module.rs`: `required_manifest_covers_runtime_categories`, `required_manifest_rejects_empty_selection`, and `required_manifest_anchors_exist_in_source`. | Eight source-module tests passed with Rust 1.96.0. Runtime acceptance remains open under R011.3 and R011.4. |
| EG-DURABLE-KERNEL-R011.3 | Planned tests in `tests/test_required_runtime_gate.py`: `test_pull_request_executes_required_categories` and `test_failed_or_empty_category_blocks_acceptance`. | Pending runtime acceptance. |
| EG-DURABLE-KERNEL-R011.4 | CI receipts must name `published-full`, `workspace-all-features`, and every selected runtime test. R011.2 and R011.3 test names define the gate checks. | Pending runtime acceptance. |
| EG-DURABLE-KERNEL-R014.2 | Process tests in `tests/test_validate_raft_cluster.py`: `test_cpu_load_runs_during_cluster_test`, `test_zero_load_workers_refused`, and `test_load_workers_stop_on_failure`. | Process lifecycle tested; loaded Rust acceptance remains in R014.4 and R014.5. |
| EG-DURABLE-KERNEL-R014.3 | Planned tests in `tests/test_validate_raft_cluster.py`: `test_repeats_both_cluster_tests`, `test_retry_budget_is_bounded`, and `test_retry_preserves_failure_and_trace`. | Pending runtime acceptance. |
| EG-DURABLE-KERNEL-R014.4 | `src/raft/tests.rs::multi_node_group_join_then_leader_rebalance` must pass every recorded loaded repetition without flaky failures. The record must include at least two repetitions. | Pending runtime acceptance. |
| EG-DURABLE-KERNEL-R014.5 | `src/raft/tests.rs::placement_admin_wire_rpc::placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster` must pass every recorded loaded repetition without flaky failures. The record must include at least two repetitions. | Pending runtime acceptance. |

R011.2 must list each selected module and its feature flags before R011.3 execution.
Existing runtime anchors include `src/raft/tests.rs::durable_log_replays_from_redb_after_restart`.
Backup anchors include `crates/eg-transaction/src/tests/backup_replay.rs::a_coordinator_backup_preserves_every_consumed_nonce_and_receipt`.
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
| raft | epistemic-graph | `raft::` | `cluster` |
| backup | eg-transaction | `tests::backup_replay::` | none |
| persistence | epistemic-graph | `server::persistence::` | `full` |

Source inspection confirmed the module paths, package names, and feature gates.
The source-binding test checks existing runtime test names in each selected module.
The discovery-refusal test uses fixtures; it is not a compiled runtime inventory.
Portable reproduction from the checkout root: `mkdir -p target && rustc --edition 2021 --test crates/eg-types/src/pre_merge_test_module.rs -o target/required-manifest-tests && target/required-manifest-tests`.
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

Published source: [cf7010728682663f735516ee276f423476d3690b](https://github.com/Knuckles-Team/epistemic-graph/commit/cf7010728682663f735516ee276f423476d3690b).
Review: [PR 445](https://github.com/Knuckles-Team/epistemic-graph/pull/445).

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

## EG-DURABLE-KERNEL-R084 acceptance

| Scenario | Requirement | Fixture |
|---|---|---|
| Two writers, one version | R084.6 | Both write against version N. One commits. The other gets `WRITE_CONFLICT`. |
| Strategy merge | R084.2 | A `FieldUnion` scope merges two disjoint field edits. |


## R085 planned acceptance

All test names below are proposed. None ran in this specification batch.
Use the real server binary, authenticated public client and disposable durable directory.
Transport, graph authorization, mutation dispatch and persistence must remain real.
Read durable state through an independent authorized client after restart.

- `EG-DURABLE-KERNEL-R085.1`: Planned: tests/memory_sync_contract.rs::bounded_contract_round_trips_and_refuses_invalid_fields.
- `EG-DURABLE-KERNEL-R085.2`: Planned: tests/memory_sync_served.rs::authorized_read_list_and_cursor_isolation.
- `EG-DURABLE-KERNEL-R085.3`: Planned: tests/memory_sync_served.rs::competing_saves_retain_both_and_resolve_once.
- `EG-DURABLE-KERNEL-R085.4`: Planned: tests/memory_sync_served.rs::wire_methods_reach_durable_gateway.
- `EG-DURABLE-KERNEL-R085.5`: Planned: tests/memory_sync_served.rs::denied_and_malformed_requests_have_no_effects.
- `EG-DURABLE-KERNEL-R085.6`: Planned: tests/memory_sync_served.rs::provenance_and_materialization_ack_survive_restart.
- `EG-DURABLE-KERNEL-R085.7`: Planned: tests/memory_sync_served.rs::conflicts_survive_gc_and_retention_refusal.
- `EG-DURABLE-KERNEL-R085.8`: Planned: tests/memory_sync_served.rs::complete_memory_sync_journey.

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
| EG-DURABLE-KERNEL-R086 | `served_safe_eviction_read_through_journey` |
| EG-DURABLE-KERNEL-R086.1 | `read_through_model_outcomes` |
| EG-DURABLE-KERNEL-R086.2 | `selected_residents_leave_durable_rows_unchanged` |
| EG-DURABLE-KERNEL-R086.3 | `evicted_blob_reads_without_resident_growth` |
| EG-DURABLE-KERNEL-R086.4 | `startup_attaches_graph_scoped_read_through` |
| EG-DURABLE-KERNEL-R086.5 | `served_evicted_reads_preserve_scope` |
| EG-DURABLE-KERNEL-R086.6 | `production_eviction_retains_unconfirmed_candidates` |
| EG-DURABLE-KERNEL-R086.7 | `backend_failure_is_not_absence_evidence` |
| EG-DURABLE-KERNEL-R086.8 | `restarted_engine_reads_an_evicted_node` |
| EG-DURABLE-KERNEL-R086.9 | `served_safe_eviction_read_through_journey` |

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
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-DURABLE-KERNEL-R028.1` / AU:21:** Retain partial; preserve existing host streaming IDs. Define EG durable checkpoint/memory state and retention, AU resume application, and exact host stream cursor/run-state binding with explicit dependency edges. Do not replace the existing streaming definition with a new rollup. Producer/consumer path: EG WorkItem lifecycle and isolation -> AU run execution -> existing GRAPHOS-A2A-R001/GRAPHOS-OPS-R001 streaming facade. The reviewed packet has no explicit dependency joining a complete run-checkpoint/memory contract to the resume and stream consumers. Source review `d89bb9c69e2fdef6a5c3fd428ede3f91868539056c2b949700867d8d8087d1e7`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DURABLE-KERNEL-R085.9` / AU:48:** Extend existing EG memory/search state and AU continuation interfaces with all four fields and context-eviction readback acceptance; reuse the durable memory authority. Producer/consumer path: AU summary/workspace consumers and EG authenticated durable memory dispatch exist as definitions. Search continuation has no named binding to a complete search-state revision. Source review `8c76fa1637e0cc2082a1cce16b9d637f12b310ae96d1fe9a50ec9c64f8e23944`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DURABLE-KERNEL-R085.10` / AU:49:** Extend the existing memory/context consumer contract with automatic session-note capture, relevance and stale/conflict behavior, preserving tenant isolation and authoritative versions. Producer/consumer path: EG authorized memory read/list -> AU context consumer; AU experience retrieval handles reusable run records. Automatic research-note capture/relevance/application is not wired by a definition. Source review `2362fd76bf40223aeb813866392670b65eae13d02a725b1af9a2e6711542dd12`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DURABLE-KERNEL-R084.7` / EG:28:** Clarify which backends are writable projections/mirrors and define deterministic tenant-scoped coordination under the existing durable authority. Extend R084 with a residual covering multi-backend ordering, uncoordinatable-write refusal and no partial authoritative effects; use two tenants and multiple target failure/replay cases. Preserve the complete existing one-graph conflict acceptance. Producer/consumer path: Served ApplyMutation -> version check/resolve_conflict -> durable mutation authority -> declared mirror targets where supported. R024.1 is read-only; R036 supplies mirror replay/cursors, not distributed transaction coordination. Source review `96c3079859b0e63ec3e1174e25e7a4ab107b23ef6863ae9b54ff0e921a88e4b0`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run; they describe required future acceptance, not evidence.

- **`EG-DURABLE-KERNEL-R038.100` / EG:60; source `b36ed0f40742d2f455b4eae67f2fb64edfb7a6ea87b93cbb8ac0046970728458`:** Start a fresh embedded profile without an external service and read/write an explicitly ephemeral namespace; verify no persistent namespace is silently downgraded. Start served mode against its configured endpoint and verify its declared sync/async class and acknowledgment boundary. Refuse an unknown profile and an ephemeral-to-durable class mismatch; read back profile and namespace declarations. Producer/consumer: EG owns profile and storage selection; transport mode is separate from namespace DurabilityClass. Preserve R038 durability classes, sync default, redb authority and authorization/HMAC isolation. Parent `EG-DURABLE-KERNEL-R038` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — graph change feed

No test in this section has run; it is a future contract.

- `EG-DURABLE-KERNEL-R043.100`: use two authenticated served GraphView clients to take a consistent snapshot, commit concurrent `ApplyChangeEnvelope` mutations, consume ordered events without refresh, reconnect from the opaque cursor and independently read back committed state. Test stable dedup ids, tombstones, expired cursor recovery, stale graph incarnation and revoked permissions without denied counts/edges; measure EG commit-to-visible p95 only through first authorized feed read. GraphOS/WebUI acceptance must separately set and verify a versioned browser commit-to-visible p95 and explicit numeric readable-layout limits (visible node/edge budget plus interaction and label bounds); EG feed latency is not end-to-end evidence.
- Verify tombstones, expired cursor recovery, graph-incarnation mismatch and authorization revocation. Denied edges/counts remain invisible. Measure p95 commit acknowledgement to first authorized read ≤1000 ms using the recorded latency-profile revision; WebUI R030.8 separately proves concurrent layout remains readable.
<!-- /spec-definition-patch -->

## Planned served acceptance — benchmark measurement

- `EG-DURABLE-KERNEL-R042.100`: read back benchmark input and measured-result bindings for at least two releases; independently recompute input-token TPS and currency-rounded per-query cost, proving startup is excluded. Refuse missing full input-token count/tokenizer/price, zero or invalid inference time, invalid currency exponent and different workload/profile settings. This does not assert that any performance comparison passes.
