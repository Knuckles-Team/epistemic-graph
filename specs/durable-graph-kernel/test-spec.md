# Test specification

All fixtures run against a disposable local data directory and generated credentials. Fault injection is deterministic and records seed, revision, platform, command and artifact path. Tests may use local multi-process nodes and disposable containers; no private cluster, inventory, credentials or always-on service is needed.

| Scenario | Requirements | Positive fixture and expected result | Negative/failure fixture and expected result |
|---|---|---|---|
| T-KG-01 batch terminality | KG-01, KG-02 | Graph+table+KV batch commits once. Readback and retry return same terminal result | Inject failure at every commit phase. Restart yields all-or-none. Lost-ack retry does not duplicate |
| T-KG-02 table-only path | KG-02 | Concurrent table-only writes use current atomic path and match reference corpus | Mixed unsupported member set refuses before any table or graph write |
| T-KG-03 owner lineage | KG-03 | Load each supported golden manifest. One validation per transaction | Unknown predecessor or extra physical table returns typed upgrade/ownership error. Stale cached binding never crosses transactions |
| T-KG-04 scrub | KG-04 | Corrupt one persisted node, scrub reports typed finding and resumes cursor after restart | Rate cap prevents writer starvation; unreadable payload is not exposed; no silent repair |
| T-KG-05 outbox | KG-05 | Healthy successors deliver after a failing head is rejected/rewound with ordered receipts | Retry one head 16 times. Successors keep their own zero-attempt count. Reasoning skip without rebuild is refused |
| T-KG-06 CDC/mirror | KG-06 | Commit 100 ordered graph/table changes, disconnect target, reconnect, replay and match digest | Crash after target apply before cursor; replay is idempotent; empty default target guard and schema drift fail visibly |
| T-KG-07 scope | KG-07 | Owner and explicitly granted principal can access allowed data | Cross-tenant/principal SQL, UQL, TSDB, KV, UDF, foreign-source and cache probes deny. Revoked grant denies next use |
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
Validation command: `rustc --edition 2021 --test crates/eg-types/src/pre_merge_test_module.rs -o /tmp/required-manifest-tests && /tmp/required-manifest-tests`.
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
