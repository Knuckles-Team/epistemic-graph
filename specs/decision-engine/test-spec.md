# Test specification

Tests are keyed to the published commit and record command, result, fixture version and workflow URL in an acceptance record. The baseline suite starts a disposable local EG instance and creates its own tenant, pack and graph fixtures. External services are optional deployment qualification, not a PR prerequisite.

## A. Contract and deterministic records

| Case | Setup and assertion |
|---|---|
| A1 method/profile parity | Generate Rust/Python/contract descriptors. `AgentAssemble`, `DecisionCommit`, `Decide`, `DecisionFit`, `DecisionEval`, pack import and content reads have exactly one reachable handler, stated scope, stability and feature profile. Generated files are fresh. |
| A2 canonical replay | Commit v1, v2 and v3 records with golden byte vectors on all supported release targets. Decode, re-encode, verify and compare digest/answer bytes. Unknown future version fails closed. Replaying stored inputs is stable even when live catalog changes. |
| A3 forged record | Mutate a candidate fact, premise class, solver bound, objective, policy digest or outcome while retaining an old digest. `DecisionCommit` refuses; no record/component/outbox write occurs. |
| A4 snapshot race | Publish or withdraw a candidate after evaluate and before commit. Expected catalog digest mismatch returns stale-catalog; unrelated publish outside the candidate universe does not. Retry same committed record is idempotent. |
| A5 limits | 64 KiB/64 candidate/32 capability limits accept boundary values and return typed `RecordTooLarge` or bounded refusal above them. Check no partial write. |

## B. Solver, entailment and explanation

Generate small 0–1 models with covering, implication, exactly-one, knapsack, compatibility and cardinality constraints; compare optimum and infeasibility to exhaustive enumeration. Repeat with shuffled input order and assert byte-identical answer/certificate. Corrupt each certificate field and confirm the independent verifier rejects it. Exhaust work budget with and without a policy-approved gap; confirm incumbent/gap reporting or typed abstention. Test no artificial result-count cutoff. Plant unknown cost and missing capability facts; neither may be treated as zero or proven true. Construct `is_a` paths whose weakest premise is claim or observation; derivation class remains proof while conclusion class weakens. A why-not re-solve must name a visible violation or worse objective and must not leak invisible alternatives. Fail a post-solve template validator and verify no-good-cut re-solve or budget abstention.

## C. Visibility, policy and statistics

Use two principals in one tenant plus a separate tenant. Seed an invisible candidate that would dominate a visible one; verify it changes neither shortlist, candidate-local BM25/vector score, count, digest, explanation, aggregate nor abstention reason. Test policy tightening accepted and each relaxation field rejected. A fitted head may reorder only legal candidates and may abstain; it cannot restore a denied candidate or authorize an irreversible action. With synthetic full-label and bandit datasets, assert exact propensities, zero-support refusal, clipped/SWITCH-IPS/doubly-robust calculations, ESS threshold, minimum support, censoring and drift behavior. A self-reported outcome, low-fidelity trace or unapproved commit principal cannot enter training. `DecisionFit` yields a draft; promotion without a matching `DecisionEval` receipt, or with changed head/policy/schema/dataset, is refused. Check HMAC exploration unpredictability to callers, exact logged propensity, budget enforcement and forbidden question classes. Walk-forward folds must purge and embargo as configured; a shared cap may not be exceeded across concurrent options.

## D. Served decision journey

Against a locally started server, publish a typed component with claim-class capabilities, call Python `AgentAssemble`, inspect record/certificate/why-not, call `DecisionCommit`, read it back, and pin it as graph synthesis evidence. Repeat through the generated Python method wrapper and a wire client. Call `DecideText` with quoted candidate blocks, typed parameters and malformed spans. Run a fit/evaluate/publish/decide cycle with an independently evaluated outcome; assert receipt and record lineage. A second tenant sees no record. For topology, verify capacity-premise inputs, bounded shape variables, fail-closed template shape, proposed graph and no capacity lease before separate acquisition. For entity resolution and finance, verify proposals/analysis only and no write or trade side effect.

## D2. Task-plan facts

Publish two templates and one A2A agent card with overlapping capabilities. Make one template invisible to the caller. Call the coverage query for two task IRIs. Expect the closure, both visible covering components and claim class on the agent card. Expect no trace of the invisible template in rows, counts or errors. Call the guardrail query for a subclassed task and an unrelated task. Expect the entailed rules with paths for the first task and none for the second. Restart the server and repeat both calls. Expect byte-identical premises. (EG-DECISION-ENGINE-R126, EG-DECISION-ENGINE-R127)

Drive `GuardrailEntailment` through the dispatcher with a published parent-class rule and exact-class rule. Verify the complete subclass path, source generation and ontology digest, and byte-identical response premises after reopening the owner store. An unrelated task returns no rules. A foreign-tenant request refuses before the read; foreign rules, non-rule components, retired heads and withdrawn heads never enter the result. Empty task input refuses rather than answering an empty rule set. The method remains a read-only snapshot under `agent:assemble-read`. (EG-DECISION-ENGINE-R127.2.2)

## E. Pack facts at the decision boundary

Publish one pack-provided component with native and foreign capability annotations, cost, latency, modality, schema digest, model facts and `contract_pin`. Assemble from that component and assert every such premise remains a publisher claim in the record and explanation. The native capability may participate in an `is_a` derivation whose conclusion class is still claim. The foreign IRI remains an exact unentailed claim. Omit the member in a later valid pack and assert `Withdrawn` makes it ineligible. Re-import byte-identical entry content and assert its candidate digest and decision result do not churn. A stale projection marker must prevent a projected ontology or shape fact from entering a decision. Plant `DecisionRecord`, `DecisionHead` and `DecisionPolicy` pack entries and assert refusal before any decision-owned state changes. These probes use the pack API as a fixture. Pack parser, body and crash recovery have their own acceptance suite.

Check metric labels omit raw facts and errors omit invisible component identifiers. Every negative probe asserts no durable state change. A served test uses disposable credentials and a local policy fixture.

## H. Quality and acceptance matrix

### EG-DECISION-ENGINE-R116.2: generated-client runtime admission

Use the published Python contract against a disposable served fixture. Exercise an engine read and a decision call without importing `epistemic_graph.engine` or constructing a bespoke facade. Prove that the existing Rust `admit_call_path` runs before dispatch. Check the full generated method inventory, operation-specific adapters and `send_by_id` so an extra sender cannot bypass that boundary.

Pass an empty contract version and a bespoke call path through the real runtime boundary. Both must fail before transport dispatch or any durable side effect. Attempt an arbitrary `_send` adapter and a caller-asserted `Contract` label; neither may substitute for the owned admission-to-dispatch binding. Include a mutation fixture that removes admission from one generated sender and proves the coverage check fails. Re-run generated artifact freshness after changing the generator. Direct gate unit tests, source-text checks and mocked successful dispatch do not replace the served acceptance fixture.

### EG-DECISION-ENGINE-R118 and EG-DECISION-ENGINE-R119 focused acceptance

| ID | Positive fixture | Refusal and boundary fixture |
|---|---|---|
| EG-DECISION-ENGINE-R118 | Two source pools with independently labeled successes/failures, one sparse pool and two tenant scopes yield reproducible posterior, Brier/log/decomposition/calibration scores; the governed value agrees in decision policy, retrieval rank and UQL. | Self-reported labels, invisible outcomes and missing pool history cannot improve another tenant's score; a caller-supplied weight cannot override the materialization; sparse history abstains. |
| EG-DECISION-ENGINE-R119 | A fixed bitemporal series produces exact purged/embargoed walk-forward folds, proportional shared-cap allocations, real trial-count DSR, split PBO, incumbent DM, option contributions and byte-identical sealed replay after checkpoint resume. | Future-known rows, overlap, changed source/head/policy digest, over-cap allocation, policy-dependent environment, bad supersedes digest and malformed `REPLAY WALK FORWARD` refuse without a sealed run or head promotion. |

For each changed code path run focused Rust/Python/client tests, `cargo fmt --check`, workspace Clippy with applicable features, generated contract freshness, CCCC, KISS, Dupehound and jscpd differential with zero new pairs. Hosted CI runs build/package tests for affected features and the locally provisioned served journey. Optional cloud/cluster/identity-provider tests run in a separate deployment qualification and cannot block normal external PRs merely because no live environment exists. An acceptance record is complete only when every relevant A–E case and gate has a result tied to the same published commit; `SOURCE_LANDED` stays distinct from `ACCEPTED`.

### EG-DECISION-ENGINE-R113.1: capacity commit type

On 2026-10-11, all six tests in `capacity_commit.rs` passed with Rust 1.96.0.
The test harness compiled the production module directly.
It linked the real cached `eg-types` library for `CapacityDecision`.
No dependency stub or replacement constructor was used.

The tests cover all 13 capacity decisions and both commit parts.
Accepted and replayed decisions must retain the full lease list and publish evidence.
Each other decision must retain its reason and grant exactly one retry.
A denied decision with either commit part must fail.
An accepted or replayed decision without either commit part must fail.

To repeat this focused check, set `DEPS` to the cached dependency directory.
Set `EG_TYPES` to its compiled `libeg_types-*.rlib` file.
Use the Rust version that built that library:

```bash
root="$PWD"
work="$(mktemp -d)"
printf 'pub use eg_types::native_control;\n#[path = "%s/crates/eg-types/src/decision/topology/capacity_commit.rs"]\nmod capacity_commit;\n' "$root" > "$work/harness.rs"
rustc --edition=2021 --test "$work/harness.rs" \
  --extern "eg_types=$EG_TYPES" -L "dependency=$DEPS" -o "$work/tests"
"$work/tests" --nocapture
```

The normal workspace command is `cargo test -p eg-types capacity_commit`.
The direct-module run verifies this leaf's constructor contract only.
It does not verify a durable transaction or enforce retry use at runtime.
EG-DECISION-ENGINE-R113.2 tracks the atomic transaction.
EG-DECISION-ENGINE-R113.3 tracks retry enforcement.

### Planned runtime checks for EG-DECISION-ENGINE-R113

The refusal handler fixture exists; native execution remains pending. Other transaction fixtures remain planned and unexecuted.
Planned test names do not identify existing tests.
The transaction and durable runtime rows remain SPECIFIED. The local retry model has direct native proof.
Each fixture is a bounded acceptance slice. It does not include the missing transaction implementation.

On 2026-10-11, source inspection found separate write owners:

- `src/server/persistence/agent_library.rs` opens `agent_library.redb` with `AgentLibraryOwner`.
- `src/server/persistence/decision_record.rs::commit_decision_record` writes through that owner.
- `src/server/persistence/agent_graph/write.rs` publishes the graph and its synthesis evidence separately.
- `src/redb_store/capacity_lease.rs::commit` uses a graph-scoped `ShardWrite` and a separate commit.
- `src/server/handlers/decide/commit.rs::commit` calls only the record commit path.

No current call joins these owners into the requested transaction.
A shared transaction and recovery design is a prerequisite for `.2.2` to `.2.4`.
That design must preserve the existing owner boundaries and avoid a second capacity authority.
The implementation exceeds the five-minute fixture slices. It needs further decomposition after that design decision.
The refusal guard in `.2.1` must remain until the transaction path exists.
The durable denial record and consumption operation for `.3.2` are also missing.
The request fixture in `.3.3` depends on those operations.

| Requirement | Intended source or test file | Planned proving test | State |
|---|---|---|---|
| EG-DECISION-ENGINE-R113.2.1 | `src/server/handlers/decide/commit.rs` | `tests::topology_commit_refuses_uncoordinated_owners` | Handler fixture added; native execution pending |
| EG-DECISION-ENGINE-R113.2.2 | `src/server/persistence/decision_record/tests.rs` | `topology_commit_publishes_capacity_and_evidence` | SPECIFIED; not run |
| EG-DECISION-ENGINE-R113.2.3 | `src/server/persistence/decision_record/tests.rs` | `topology_capacity_denial_leaves_no_commit_parts` | SPECIFIED; not run |
| EG-DECISION-ENGINE-R113.2.4 | `src/server/persistence/decision_record/tests.rs` | `topology_commit_recovery_preserves_atomic_outcome` | SPECIFIED; not run |
| EG-DECISION-ENGINE-R113.3.1 | `crates/eg-types/src/decision/topology/retry_allowance.rs` | `tests::retry_allowance_consumes_once`, `tests::invalid_allowances_fail_without_mutation` | Direct native module tests pass; workspace integration not run |
| EG-DECISION-ENGINE-R113.3.2 | `src/server/persistence/decision_record/tests.rs` | `topology_denial_allowance_consumption_survives_restart` | SPECIFIED; not run |
| EG-DECISION-ENGINE-R113.3.3 | `src/server/handlers/decide/commit.rs` | `tests::topology_denial_accepts_one_followup_request` | SPECIFIED; not run |

R113.2 requires all four transaction tests. R113.3 requires all three retry tests.
Neither rollup is runtime acceptance evidence.

### R090.1 native acceptance receipt (2026-10-11)

`crates/eg-types/src/decision/statistical/scorer.rs::budget_tests` passed all six native tests with Rust 1.96.0.
`a_budget_at_the_ceiling_is_declared` proves ceiling acceptance.
The parameter, byte, and latency refusal tests prove independent limit enforcement.
`each_budget_axis_accepts_its_boundary_and_refuses_overflow` covers zero, exact independent ceilings, and `u32::MAX` refusals.
The harness imports the unchanged production module with real cached `eg-types` and `serde` dependencies; no substitutes were used.
The reproducible Cargo command is `cargo test -p eg-types decision::statistical::scorer::budget_tests`.
The equivalent direct native harness ran; the full Cargo crate build and hosted CI were not run locally.
This receipt covers typed declarations only, not measured latency or the parent R090 benchmark obligation.

The latency boundary accepts 9,999 microseconds and refuses 10,000 microseconds.

### Remaining R090 acceptance

EG-DECISION-ENGINE-R090.2 requires `tests/test_decision_resident_scorer.py::test_compiled_engine_scores_without_external_runtime`.
EG-DECISION-ENGINE-R090.3 requires `crates/eg-numeric/benches/decision_resident_scorer.rs::resident_scorer_meets_declared_budget`.
R090.2 passed in the [Python suite job](https://github.com/Knuckles-Team/epistemic-graph/actions/runs/38148086774/job/114499779006)
of hosted Release run 38148086774 at `e5190222b38d628b7d0f268ce4c46b184a170ee1`.
That job passed all 3,702 tests, including the resident runtime tracing case.
The Python job succeeded; the parent Release run failed overall. This is the
scoped compiled-runtime acceptance, not a claim that the parent workflow or all
served decision paths passed. R090.3 has paired native measurements below:
the Cargo launch is inconclusive; the standalone run meets the measured limits.
Budget declarations alone do not establish either result.

### R090.3 native release measurement (2026-10-11)

The [structured receipt](../../crates/eg-numeric/benches/decision_resident_scorer.receipt.json)
binds measured source `41cce5dd08d7f27aeacf23b6f01d177dfcbf7c59` to the executable
SHA-256, file length, compiler identity, release flags and native benchmark JSON.
The run used Rust 1.96.0 on `x86_64-unknown-linux-gnu`, optimization 3, thin LTO,
one codegen unit, no debug assertions, and at most three build jobs. The initial
build took 503 seconds within its 900-second bound; total command time was
504.766 seconds. There was no package-profile override or facade build.

| Case | Parameters | p99 thread CPU (microseconds) |
| --- | ---: | ---: |
| Fitted promotion recipe | 234 | 11.183 |
| Maximum legal head dimensions, 256-row fixture | 1360 | 1412.071 |

Both cases meet the 700,000-parameter and 9,999-microsecond ceilings over 1,000
calls after 100 warmups. The 637,136-byte runner produced a whole-runner peak RSS
of 124,788,736 bytes, above the 8,388,608-byte ceiling. Its scorer-specific resident
memory remains unknown. The native result is **inconclusive**, with exit code 2;
Neither serialized model size nor runner file length resolves this above-ceiling
RSS result. It remains preserved in the first receipt.

### R090.3 standalone corroboration (2026-10-11)

The [standalone receipt](../../crates/eg-numeric/benches/decision_resident_scorer.standalone.receipt.json)
records the exact same executable, with no rebuild, fixture changes, reduced
samples or baseline subtraction. A separate shell ran it under `/usr/bin/time -v`.
Its SHA-256 remains `b648cd00dc5dae3d5648b269f5245db0175c3c5ae3d9f51611e4dea501c260a5`.

The native peak RSS was 2,850,816 bytes; external time reported exactly 2,784 KiB.
Both are below 8 MiB. The fitted and 256-row fixture p99 CPU measurements were
7.713 and 1205.546 microseconds; parameters remained 234 and 1360. The run exited
0 in 0.99 seconds after the same 100 warmups and 1,000 samples per case.
The standalone native fixture therefore meets all measured resource limits with
verified release provenance. This does not establish served parent acceptance.
The difference between launch methods is observed; startup or inherited
high-water effects remain hypotheses, not an established causal attribution.

### R093.2 planned served acceptance

These tests are planned and have not run.

| Row | Proving test | Required observation |
|---|---|---|
| EG-DECISION-ENGINE-R093.2.1 | `tests/synthetic/test_synthetic_decision_export.py::test_corpus_collects_seeded_served_results` | Exported candidates match recorded served-client results. |
| EG-DECISION-ENGINE-R093.2.2 | `tests/synthetic/test_synthetic_decision_export.py::test_served_labels_require_collected_results` | Invalid provenance and labels fail closed. |
| EG-DECISION-ENGINE-R093.2.3 | `tests/synthetic/test_served_corpus_training.py::test_training_consumes_served_ground_truth` | Training consumes episodes collected from a real server. |

## Static contract checks

Run `python -m pytest -q tests/test_decision_static_contracts.py` without a native engine build.
R074 checks the full component enum and agent-slot references against the closure documentation.
R123 scans Rust sources under `src` and `crates` for duplicate solve DTO declarations.
The scan permits explicitly listed, unrelated domain homonyms.
Negative fixtures add duplicate declarations, remove documented kinds, and introduce an undocumented slot reference.
These checks prove source and documentation consistency only. They do not execute solver behavior.

## Local retry model verification

Run `rustc --edition=2021 --test crates/eg-types/src/decision/topology/retry_allowance.rs -o /path/to/retry-tests`, then run that binary. This compiles the exact production module without a substitute dependency. The tests cover the 1-to-0 transition, repeated refusal, zero, and all 254 malformed counts. The module remains re-exported through `capacity_commit` for existing callers. This proves only EG-DECISION-ENGINE-R113.3.1; copies do not share state. Durable and handler enforcement remain required by R113.3.2 and R113.3.3.

## Ingestion and schema integration acceptance

The following fixtures are planned and unexecuted. Their producer-only tests remain valid for the narrower producer children. They do not prove these integration requirements.

| Requirement | Planned fixture | Acceptance state |
|---|---|---|
| EG-DECISION-ENGINE-R079.2 | `tests/test_decision_ingestion_acceptance.py::test_ingestion_embedding_admission_and_cheaper_index_skip` | SPECIFIED; no executed integration proof |
| EG-DECISION-ENGINE-R079.3 | `tests/test_decision_ingestion_acceptance.py::test_duplicate_and_oversized_units_do_not_embed` | SPECIFIED; no executed integration proof |
| EG-DECISION-ENGINE-R079.4 | `tests/test_decision_ingestion_acceptance.py::test_sql_column_kinds_and_cardinality_control_embedding` | SPECIFIED; no executed integration proof |
| EG-DECISION-ENGINE-R079.5 | `tests/test_decision_ingestion_acceptance.py::test_retrieval_feedback_demotes_unused_content` | SPECIFIED; no executed integration proof |
| EG-DECISION-ENGINE-R080.2 | `tests/test_decision_ingestion_acceptance.py::test_real_extractors_follow_the_lowest_sufficient_rung` | SPECIFIED; no executed integration proof |
| EG-DECISION-ENGINE-R102.2 | `src/server/graph_schema/approved/tests.rs::schema_repair_requires_real_shadow_ingest_before_activation` | SPECIFIED; no executed integration proof |

## Retention acceptance

The following tests use the served decision path and persistent stores.
Their bindings establish coverage. Native execution remains unrun in this delivery slice.

| Requirement | Proving test | Scope |
|---|---|---|
| EG-DECISION-ENGINE-R066.1 | `src/server/handlers/decide/stat_tests/retention_tests.rs::library_records_survive_every_retention_window` | Zero windows and maximum time preserve complete library record bytes. |
| EG-DECISION-ENGINE-R066.2 | `src/server/handlers/decide/stat_tests/retention_tests.rs::graph_inputs_expire_at_the_configured_boundaries` | Graph inputs compact at 100 milliseconds and retire 200 milliseconds later. Summaries retain their digests. |
| EG-DECISION-ENGINE-R066.3 | `src/server/handlers/decide/stat_tests/retention_tests.rs::evaluate_only_leaves_no_committed_records` | Sixteen evaluations leave statistical and library record stores empty. |
| EG-DECISION-ENGINE-R066.4 | Planned: `graph_summaries_expire_on_policy_window` | No implementation or test exists. Graph summaries currently remain indefinitely. |

Run `cargo test --lib --features decide,query,blob retention_tests` for the new native fixtures.
The existing graph-log fixture also checks CAS restoration, forged blobs, and retirement.
Run `cargo test --lib --features decide,query,blob graph_sourced_records_are_principal_scoped_not_tenant_scoped_on_log_read` for that fixture.
Local Rust formatting and 12 Python decision-contract checks passed. Those checks do not execute retention behavior.

## Bounded topology oracle

| Requirement | Proving test |
|---|---|
| EG-DECISION-ENGINE-R110.1 | `crates/eg-compute/src/assemble/tests/topology_oracle.rs::property_oracle_agrees_with_the_solver_for_instances_up_to_twenty_variables` checks 66 fixtures and twenty actual variables. |

Native execution remains pending after dependency compilation exceeded local resources.
The isolated fixture arithmetic check does not execute the assembly or solver.

### R113.2.1 refusal boundary

The handler rejects solved topology plans with positive per-cell lease demand.
The handler preserves tenant, size, replay, and shape checks before this refusal.
The guard runs before Agent Library initialization or mutation.
The fixture compares both durable owner files and capacity status after refusal.
It also checks foreign-tenant refusal and successful ordinary record publication.
Advisory assembly remains unchanged. The shared transaction and recovery paths remain absent.
Native command: `cargo test --lib --features decide topology_commit_refuses_uncoordinated_owners`.
This command has not run for this change.

## Synthetic aggregate separation

| Requirement | Proving test |
|---|---|
| EG-DECISION-ENGINE-R122 | `src/server/handlers/decide/stat_tests.rs::fit_evaluate_publish_and_decide_end_to_end` exercises both origins through served handlers. |

The fixture excludes synthetic labels above minimum support and preserves real counts in mixed-origin aggregates.
It also checks synthetic-only logged fitting and the inline provenance guard.
Native execution remains pending.

## Typed provenance acceptance

- R121.1: `crates/eg-compute/src/assemble/tests/evidence.rs::assembly_evidence_uses_typed_premises_not_labels` exercises actual assembly and replay.
- R121.2: planned `src/server/handlers/decide/stat_tests/classes_tests.rs::graph_claim_and_evidence_control_answer_class` covers graph-source provenance.

With `query`, the same fixture creates enough synthetic forecasts to reach the score support floor.
It checks absent synthetic Brier, log-loss, and ECE rows, plus exact real reputation and learned-reliability counts.

## R118.1.2 cross-surface acceptance prerequisites

`src/server/handlers/decide/stat_tests/reputation_tests.rs` already contains the served decision-log fixture.
`below_the_support_floor_a_reputation_has_no_numbers` covers sparse SQL output.
`source_reliability_is_learned_from_the_visible_log` covers dense UQL learning and invisible-history fallback.
Neither test proves agreement across authority policy, retrieval ranking, and UQL.

Extend this fixture through the actual governed consumers after the plan's source-identity adapter and result contract exist.
A visible sparse subject and a dense peer must share one pool and one materialization.
All three consumers must preserve the sparse subject's typed abstention without producing a numeric reliability.
All three consumers must use the dense subject's same materialized estimate, allowing only documented wire precision differences.
Vary the graph prior and caller-supplied retrieval weight; neither may replace the governed estimate or suppress abstention.
An absent subject must remain distinguishable from a visible subject below the support floor.
Invisible outcomes and another tenant's dense history must not change either result or disclose hidden counts.
The fixture must exercise real served adapters rather than manually assign equal scalar values.

These acceptance tests require the consumer prerequisites described in the plan.

## Harness configuration evidence

- `EG-DECISION-ENGINE-R128`: A served test submits one `harness_config` question and asserts the committed record names all three parts with premises.
- `EG-DECISION-ENGINE-R128.1`: `crates/eg-types/src/decision/statistical.rs::tests::harness_config_kind_serialises_snake_case`.
- `EG-DECISION-ENGINE-R128.2`: `crates/eg-types/src/decision/statistical/harness_config.rs::tests::body_requires_every_input`.
- `EG-DECISION-ENGINE-R128.3`: `crates/eg-types/src/decision/statistical/harness_config.rs::tests::answer_round_trips_through_serde`.
- `EG-DECISION-ENGINE-R128.4`: `crates/eg-types/src/decision/statistical/harness_config.rs::tests::policy_override_replaces_default`.
- `EG-DECISION-ENGINE-R128.5`: `crates/eg-types/src/decision/statistical/harness_config/context_policy/tests.rs::context_policy_none_only_when_overflow_impossible`.
- `EG-DECISION-ENGINE-R128.6`: `crates/eg-types/src/decision/statistical/harness_config.rs::tests::plan_scaffold_rules`.
- `EG-DECISION-ENGINE-R128.7`: `crates/eg-types/src/decision/statistical/harness_config.rs::tests::shell_only_needs_both_facts`.
- `EG-DECISION-ENGINE-R128.8`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::unknown_profile_is_refused`.
- `EG-DECISION-ENGINE-R128.9`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::tool_set_reuses_covering_subset`.
- `EG-DECISION-ENGINE-R128.10`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::decide_routes_harness_config`.
- `EG-DECISION-ENGINE-R128.11`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::premises_and_why_not_cover_each_part`.
- `EG-DECISION-ENGINE-R128.12`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::all_parts_abstained_seals_abstention`.
- `EG-DECISION-ENGINE-R128.13`: `src/server/handlers/decide/stat_tests/harness_config_tests.rs::harness_config_record_commits`.
- `EG-DECISION-ENGINE-R128.14`: `crates/eg-capabilities/tests/contract_schema.rs::question_kind_lists_harness_config`.
- `EG-DECISION-ENGINE-R128.15`: `tests/decide/test_harness_config_served.py::test_harness_config_end_to_end`.

## Committed pack receipt binding

`storage_acceptance::receipt::committed_multi_intent_receipt_binds_all_effects_and_survives_replay` covers EG-DECISION-ENGINE-R073.4.
The fixture signs served imports and reads the typed receipt, batch and physical outbox from the owner.
It checks three component intents and one pack intent against an independent digest oracle.
Each omission, adjacent swap, topic change, key change and payload change must alter the digest.
Each header name, value, removal and addition must alter the digest.
A fresh nonce replays through `commit_pack_write`; any new owner application panics.
The receipt and batch remain equal, and nonce reuse fails.
The fixture separately checks the served catalog shortcut and unchanged owner rows.

Run `cargo test --lib --features blob committed_multi_intent_receipt_binds_all_effects_and_survives_replay`.
Then run `cargo test --lib --features blob storage_acceptance` for the adjacent served storage fixtures.

## R073.3 and R073.5 production acceptance

| Requirement | Proving tests | Expected evidence |
|---|---|---|
| `EG-DECISION-ENGINE-R073.3` | `src/server/persistence/agent_library/open_tests.rs::fresh_open_initializes_all_pack_tables_and_reopens_empty` | Production open exposes five empty pack tables and an empty catalog before and after reopen. The registry contains 21 owner tables. |
| `EG-DECISION-ENGINE-R073.3` | `src/server/persistence/agent_library/open_tests.rs::production_open_refuses_each_genuine_predecessor_without_writing` | Every declared Agent Library predecessor fails twice by name. Both failures preserve the exact file bytes. |
| `EG-DECISION-ENGINE-R073.5` | `src/server/handlers/admin/connector_pack/tests/content_model.rs::a_changed_leaf_revises_transitive_reference_pins_once` and `a_shared_descendant_diamond_revises_each_component_once` | Transitive and diamond references pin the committed revisions once. Repeated imports preserve revisions. |
| `EG-DECISION-ENGINE-R073.5` | `src/server/handlers/admin/connector_pack/tests/withdrawal.rs::withdrawn_tool_leaves_search_but_keeps_historical_content` | Signed Search initially returns both tools. Withdrawal removes one; the published control remains. Signed historical Content returns exact bytes. A new pin refuses with `COMPONENT_WITHDRAWN` and creates no component. |

The existing lineage tests retain their independent file-digest and manifest-order assertions.
The existing admin holder test retains its historical Content assertions.
- `EG-DECISION-ENGINE-R129`: `crates/eg-types/src/decision/guardrail.rs::tests::promotion_obeys_invariants`.
- `EG-DECISION-ENGINE-R129.1`: `src/server/handlers/decide/stat_tests/rule_promotion_tests.rs::attach_point_is_reached`.
- `EG-DECISION-ENGINE-R129.2`: `crates/eg-types/src/decision/guardrail.rs::tests::verdict_serialises_snake_case` roundtrips both exact snake_case wire variants and preserves the invariant IRI; `verdict_rejects_unknown_variants_and_malformed_payloads` refuses wrong-case/unknown variants, missing or non-string IRIs, and unknown payload fields. These type tests do not prove the invariant gate.
- `EG-DECISION-ENGINE-R129.3`: `crates/eg-types/src/decision/guardrail.rs::tests::first_contradicted_invariant_is_named`.
- `EG-DECISION-ENGINE-R129.4`: `crates/eg-types/src/decision/guardrail.rs::tests::full_consensus_does_not_override`.
- `EG-DECISION-ENGINE-R129.5`: `src/server/handlers/decide/stat_tests/rule_promotion_tests.rs::rejected_rule_is_audited_not_active`.
- `EG-DECISION-ENGINE-R129.6`: `tests/decide/test_rule_promotion_served.py::test_invariant_blocks_promotion`.

### EG-DECISION-ENGINE-R130 tests

- `EG-DECISION-ENGINE-R130.1`: `crates/eg-epistemic/src/ranking.rs::tests::ranked_candidate_round_trips`.
- `EG-DECISION-ENGINE-R130.2`: `crates/eg-epistemic/src/ranking.rs::tests::equal_codes_order_by_item_score`.
- `EG-DECISION-ENGINE-R130.3`: `tests/decide/test_ranked_decide_served.py::test_decide_orders_by_calibrated_score`.
- `EG-DECISION-ENGINE-R130.4`: `crates/eg-epistemic/src/ranking.rs::tests::uncalibrated_candidate_abstains`.
- `EG-DECISION-ENGINE-R130.5`: `crates/eg-compute/src/assemble/tests/ranking_premises.rs::premises_name_tie_break`.
- `EG-DECISION-ENGINE-R130.6`: `tests/decide/test_ranked_decide_served.py::test_tied_codes_resolve_by_item_score`.

### EG-DECISION-ENGINE-R131 tests

- `EG-DECISION-ENGINE-R131.1`: `crates/eg-types/src/decision/limit_guard.rs::tests::limit_types_round_trip`.
- `EG-DECISION-ENGINE-R131.2`: `crates/eg-types/src/decision/limit_guard.rs::tests::check_returns_all_breaches`.
- `EG-DECISION-ENGINE-R131.3`: `tests/decide/test_limit_guard_served.py::test_breach_refuses`.
- `EG-DECISION-ENGINE-R131.4`: `crates/eg-compute/src/assemble/tests/limit_refusal.rs::breach_abstains_with_rule_id`.
- `EG-DECISION-ENGINE-R131.5`: `crates/eg-compute/src/assemble/tests/limit_refusal.rs::breach_observation_links_signal`.
- `EG-DECISION-ENGINE-R131.6`: `tests/decide/test_limit_guard_served.py::test_oversize_order_refused_and_recorded`.

### R090.3 reproducible build-then-standalone protocol

Build in the preserved native workspace, then measure the exact emitted
executable in a separate shell. Do not rebuild between the provenance capture
and standalone run:

```bash
CARGO_BUILD_JOBS=3 cargo bench --locked -p eg-numeric --features decision \
  --bench decision_resident_scorer --profile release --no-run
```

Use the executable path emitted by Cargo; do not guess a filename or select a
stale binary by glob. Record its SHA-256 and file length, the source commit,
Rust version, target, host CPU/OS, command and effective profile. In a separate
shell, run `/usr/bin/time -v <exact-emitted-executable> --bench`, retain the native
JSON and structured external RSS/exit/timing values, and confirm the hash again.
All measured scorer code must use optimization 3. No opt0 substitute, fixture
reduction, baseline subtraction or rebuild is permitted for this comparison.
Debug assertions are refused; the external provenance receipt also verifies
optimization. Both historical launch receipts above remain preserved.

The harness emits schema-version 1 JSON (schema: `crates/eg-numeric/benches/decision_resident_scorer.schema.json`): `cases` contains fixture shape, actual trainable scalar count (linear weights plus seven attention tensors), calibration scalar count, warmup/sample counts, nearest-rank p99 nanoseconds and microseconds, and parameter/latency acceptance. Both fixtures execute 100 warmups and 1,000 individually clocked production `forward::read` calls. Fit/construction, validation, and result destruction are outside each timing window. The fitted case reuses the 200-label, seed-0, 60-iteration promotion recipe and asserts nonzero trained self weights; the second case covers maximum legal head dimensions (width 16, features 32, shortlist 64) at 256 rows. LegalSet permits varying row counts, so this does not cover every legal workload.

`memory.bytes` is Linux whole-runner lifetime peak RSS, including fitting and allocator overhead. At or below 8,388,608 bytes it proves a conservative upper bound; above that it is inconclusive for scorer-specific residency and leaves R090.3 unverified. No baseline subtraction or serialized-payload proxy is used. `runner_binary_bytes` measures the executable actually running; `server_binary_bytes` and scorer-specific bytes are null. Exact parameter/latency limits, one-unit excesses, RSS inconclusiveness, nearest-rank indexing, invalid-head refusal and out-of-distribution refusal are checked before measurement. Exit 0 means the measured upper bounds pass but build provenance still needs review; exit 1 means parameter/CPU failure; exit 2 means inconclusive memory or unsupported OS. The recorded standalone run meets these measured limits; its receipt provides the required release provenance. This does not establish served parent acceptance.

### R090.3 production dependency comparison after main integration

Current main `40fefdd6cb66c3c432e73da5966a1aea6f9a9c4e` was merged normally.
Against measured source `41cce5dd08d7f27aeacf23b6f01d177dfcbf7c59`, all 214
files compared across numeric production and test fixtures, shared decision
and contract types, shared test support, both crate manifests, root manifest,
lockfile, toolchain and Cargo configuration are byte-identical. The benchmark
changes only its literal spec-binding comment. The shared types crate also has
new agent-component model facts outside those unchanged roots; its whole crate
is therefore not byte-identical. This is a scoped scorer/fixture input comparison,
not a claim that a newly linked final-HEAD executable would have the same hash.
No unrelated rebuild was substituted for the exact measured-source receipts.

### EG-DECISION-ENGINE-R132 tests

- `EG-DECISION-ENGINE-R132.1`: `crates/eg-types/src/decision/entry_trust.rs::tests::entry_trust_round_trips`.
- `EG-DECISION-ENGINE-R132.2`: `crates/eg-types/src/decision/entry_trust.rs::tests::lower_tier_never_overrides_higher`.
- `EG-DECISION-ENGINE-R132.3`: `crates/eg-compute/src/assemble/tests/entry_trust_wiring.rs::admit_runs_before_premises`.
- `EG-DECISION-ENGINE-R132.4`: `crates/eg-compute/src/assemble/tests/entry_trust_refusal.rs::poisoned_only_premise_abstains`.
- `EG-DECISION-ENGINE-R132.5`: `crates/eg-compute/src/assemble/tests/entry_trust_refusal.rs::refused_entries_are_recorded`.
- `EG-DECISION-ENGINE-R132.6`: `tests/decide/test_entry_trust_served.py::test_unverified_entry_cannot_override_verified`.

### EG-DECISION-ENGINE-R133 tests

- `EG-DECISION-ENGINE-R133.1`: `crates/eg-types/src/decision/run_governance.rs::tests::quota_round_trips`.
- `EG-DECISION-ENGINE-R133.2`: `crates/eg-types/src/decision/run_governance.rs::tests::request_only_tightens_quota`: omission uses D; tightening uses R; D < R <= C uses D; R > C refuses before clamping.
- `EG-DECISION-ENGINE-R133.3`: `src/server/handlers/decide/stat_tests/runtime_quota_tests.rs::decide_routes_runtime_quota`.
- `EG-DECISION-ENGINE-R133.4`: `src/server/handlers/decide/stat_tests/runtime_quota_tests.rs::missing_task_class_abstains_to_default`.
- `EG-DECISION-ENGINE-R133.5`: `src/redb_store/work_item/commit.rs::tests::run_trace_is_linked_to_unit`.
- `EG-DECISION-ENGINE-R133.6`: `tests/decide/test_runtime_quota_served.py::test_quota_and_trace_end_to_end`: query quotas through read-only Decide, commit the trace through CommitWorkItemResult, then query the produced link; an invalid fence creates neither trace nor link.

### EG-DECISION-ENGINE-R134 tests

- `EG-DECISION-ENGINE-R134.1`: `crates/eg-types/src/decision/task_plan.rs::tests::task_plan_round_trips`.
- `EG-DECISION-ENGINE-R134.2`: `crates/eg-types/src/decision/task_plan.rs::tests::relevant_tools_fit_budget`: floor/top-k filtering precedes R045 coverage-per-token selection; include cost/id ties, a lower-relevance coverage winner, and uncovered-capability refusal.
- `EG-DECISION-ENGINE-R134.3`: `crates/eg-types/src/decision/task_plan.rs::tests::independent_subtasks_set_flag_and_operator`.
- `EG-DECISION-ENGINE-R134.4`: `crates/eg-types/src/decision/task_plan.rs::tests::plan_respects_dependencies`.
- `EG-DECISION-ENGINE-R134.5`: `src/server/handlers/decide/stat_tests/task_plan_tests.rs::decide_routes_task_plan`.
- `EG-DECISION-ENGINE-R134.6`: `src/server/handlers/decide/stat_tests/task_plan_tests.rs::uncovered_subtask_abstains`.
- `EG-DECISION-ENGINE-R134.7`: `src/server/handlers/decide/stat_tests/task_plan_tests.rs::premises_name_skill_and_flag`.
- `EG-DECISION-ENGINE-R134.8`: `tests/decide/test_task_plan_served.py::test_plan_end_to_end`.

### EG-DECISION-ENGINE-R135 tests

- `EG-DECISION-ENGINE-R135.1`: `crates/eg-types/src/decision/program_store.rs::tests::program_round_trips`.
- `EG-DECISION-ENGINE-R135.2`: `crates/eg-types/src/decision/program_store.rs::tests::literals_become_parameters`.
- `EG-DECISION-ENGINE-R135.3`: `src/server/handlers/decide/stat_tests/program_store_tests.rs::decide_routes_program_lookup`.
- `EG-DECISION-ENGINE-R135.4`: `src/server/handlers/decide/stat_tests/program_store_tests.rs::overlong_trace_is_refused`.
- `EG-DECISION-ENGINE-R135.5`: `src/server/handlers/decide/stat_tests/program_store_tests.rs::program_links_source_trace`.
- `EG-DECISION-ENGINE-R135.6`: `tests/decide/test_program_store_served.py::test_compile_then_lookup`.

### EG-DECISION-ENGINE-R136 tests

- `EG-DECISION-ENGINE-R136.1`: `crates/eg-types/src/decision/evolution_round.rs::tests::round_types_round_trip`.
- `EG-DECISION-ENGINE-R136.2`: `crates/eg-types/src/decision/evolution_round.rs::tests::verdict_follows_flip_fraction`, including five declared-direction and five opposite flips at 0.5 yielding `unverified`; add `crates/eg-types/src/decision/evolution_round.rs::tests::simultaneous_thresholds_are_unverified`.
- `EG-DECISION-ENGINE-R136.3`: `crates/eg-types/src/decision/evolution_round.rs::tests::stronger_models_go_to_high_impact_stages`.
- `EG-DECISION-ENGINE-R136.4`: `src/server/handlers/decide/stat_tests/evolution_round_tests.rs::decide_routes_round_kinds`.
- `EG-DECISION-ENGINE-R136.5`: `src/server/handlers/decide/stat_tests/evolution_round_tests.rs::empty_prediction_abstains`.
- `EG-DECISION-ENGINE-R136.6`: `src/server/handlers/decide/stat_tests/evolution_round_tests.rs::premises_name_counts_and_budget`.
- `EG-DECISION-ENGINE-R136.7`: `tests/decide/test_evolution_round_served.py::test_verdict_and_allocation`.

### EG-DECISION-ENGINE-R137 tests

- `EG-DECISION-ENGINE-R137.1`: `crates/eg-types/src/decision/context_budget.rs::tests::context_budget_round_trips`.
- `EG-DECISION-ENGINE-R137.2`: `crates/eg-types/src/decision/context_budget.rs::tests::over_window_prompt_is_split_within_limits`.
- `EG-DECISION-ENGINE-R137.3`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::decide_routes_context_budget`.
- `EG-DECISION-ENGINE-R137.4`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::too_many_items_is_refused`.
- `EG-DECISION-ENGINE-R137.5`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::premises_name_window_and_limits`.
- `EG-DECISION-ENGINE-R137.6`: `tests/decide/test_context_budget_served.py::test_over_window_plan_end_to_end`.

### EG-DECISION-ENGINE-R138 tests

- `EG-DECISION-ENGINE-R138.1`: `crates/eg-types/src/decision/red_team.rs::tests::red_team_records_round_trip`.
- `EG-DECISION-ENGINE-R138.2`: `crates/eg-types/src/decision/red_team.rs::tests::one_passing_scorer_compromises`.
- `EG-DECISION-ENGINE-R138.3`: `src/server/handlers/decide/stat_tests/red_team_tests.rs::decide_routes_probe_verdict`.
- `EG-DECISION-ENGINE-R138.4`: `src/server/handlers/decide/stat_tests/red_team_tests.rs::missing_scorer_abstains`.
- `EG-DECISION-ENGINE-R138.5`: `src/server/handlers/decide/stat_tests/red_team_tests.rs::premises_name_scorers_and_scores`.
- `EG-DECISION-ENGINE-R138.6`: `tests/decide/test_red_team_served.py::test_probe_verdict_end_to_end`.

### EG-DECISION-ENGINE-R137 package tests (planned)

These are acceptance targets, not execution evidence. Retain every existing R137.1–.6 test.

- `EG-DECISION-ENGINE-R137.7`: `crates/eg-types/src/decision/context_budget.rs::tests::package_records_round_trip`. Round trips preserve every constraint, source/range/revision and measurement identity. Validation rejects duplicate candidate ids, inconsistent resolution groups and malformed ranges. Negative/nonfinite measurements and arithmetic overflow refuse before solving.
- `EG-DECISION-ENGINE-R137.8`: `crates/eg-compute/src/solve/tests/context_package.rs::matches_tiny_multiresolution_oracle` (planned). Exhaustive tiny models match the optimum and fewer-token tie-break. Independent window, output reservation, caller, money and latency cases each become the tightest bound; fractional and zero-capacity boundaries never overrun. Changing candidate order must not change objective or totals; remaining ties use the recorded stable order.
- `EG-DECISION-ENGINE-R137.9`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::decide_routes_package_selection` (planned). An authenticated served `Decide` request reaches package lowering, solver, independent verifier and sealed typed answer. The test rejects an unwired helper or local selection that replaces any step. Package selection, whole-prompt fit and R137.1–.6 recursive decomposition remain distinct operations.
- `EG-DECISION-ENGINE-R137.10`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::package_refusals_are_typed` (planned). Negative cases cover invalid/missing measurements, no fitting nonempty package, empty/zero-capacity results and stale revisions. Work exhaustion tests cover no incumbent and unacceptable gaps. Certificate tests corrupt the proof, digests and selection bindings. No refusal silently relaxes a ceiling or returns a certified selection.
- `EG-DECISION-ENGINE-R137.11`: `src/server/handlers/decide/stat_tests/context_budget_tests.rs::package_evidence_binds_inputs` (planned). Changes to unchosen candidates, token weights, tokenizer, cited ranges or revisions invalidate reuse. So do changes to active rates, output reservation, ceilings or objective. Independent recomputation checks selection, totals and certificate against the bound model. The record states the weakest evidence class and all defaults used.
- `EG-DECISION-ENGINE-R137.12`: `tests/decide/test_context_package_served.py::test_certified_package_end_to_end` (planned). The EG served receipt records source SHA, features/configuration, exact command, input/model/certificate digests and results. The linked AU receipt records its SHA, served caller and applied package/citations. Cover reserved output, monetary/latency ceilings, caller/window limits, fractional rounding, zero capacity, unknown rates, stale sources and rejected certificates. Unit-only solves, mock transports, greedy fits, planned test names and documentation commits do not establish this proof.

<!-- spec-definition-patch:comparative-analysis-r139 -->
### EG-DECISION-ENGINE-R139 tests (planned)

These are acceptance targets only, not implementation or execution evidence. Retain R137/R138 tests unchanged.

- `EG-DECISION-ENGINE-R139.1`: `crates/eg-types/src/decision/comparative_analysis.rs::tests::plan_round_trips_and_binds_all_stages`; preserve every revision, digest, candidate/order, threshold, budget, refusal and question kind; reject duplicate/over-limit candidates.
- `EG-DECISION-ENGINE-R139.2`: `crates/eg-types/src/decision/comparative_analysis.rs::tests::thresholds_and_budgets_select_only_admissible_stages`; cover inclusive 0.50/0.80, one/two synthesis calls, stable ties, zero discovery calls, extraction exclusion and total equality/overflow.
- `EG-DECISION-ENGINE-R139.3`: `src/server/handlers/decide/stat_tests/comparative_analysis_tests.rs::decide_returns_plan_without_model_execution`; run actual Decide dispatch and assert pinned inputs and zero discovery calls.
- `EG-DECISION-ENGINE-R139.4`: `src/server/handlers/decide/stat_tests/comparative_analysis_tests.rs::stale_inputs_and_stage_budgets_refuse_before_calls`; cover stale query/vector/universe, invalid weights/thresholds, stage and total exhaustion with typed refusals and no model call.
- `EG-DECISION-ENGINE-R139.5`: `crates/eg-types/src/decision/comparative_analysis.rs::tests::plan_digest_binds_inputs_and_budgets`; every bound mutation changes digest and AU receipt validation rejects a different plan or over-budget calls.
- `EG-DECISION-ENGINE-R139.6`: EG served target `tests/decide/test_comparative_analysis_served.py::test_plan_and_refusal_through_served_decide`; AU consumer target `tests/decide/test_comparative_analysis_consumer.py::test_served_consumer_obeys_plan_and_binds_invocation_receipt`. Require exact source SHAs and separate receipts; an EG-only test cannot establish AU invocation.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:owner-batch-20261012-decision-engine -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-DECISION-ENGINE-R135.7` / AU:18:** Retain partial. Define the AU pre-planning test execution/result handoff and verifier scoring fields, plus the EG per-successful-cycle pattern persistence obligation; extend existing harness/run-reuse owners and explicitly connect the producer/consumer IDs. Do not close from R135 or R022 names alone. Producer/consumer path: AU run_agent/record_outcome and SkillGraphPipeline produce run experience; EG compile_program -> AgentLibraryStore -> handle_decide ProgramLookup defines reuse. The pre-planning test-result producer and verifier consumer contract are absent; no explicit cross-owner dependency connects those missing obligations. Source review `90828a09b388b9910d870ce944aa2baeb561af14d8f7bf88b48a4d51c4da877f`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R106.1` / AU:19:** Retain partial. Extend the existing council decision/topology contract with position inputs, coordination behavior and recorded output, then name the AU execution-to-EG decision dependency. No new topology implementation is required by this finding. Producer/consumer path: AU decide/topology/templates.py supplies topology facts to EG AgentGraph validation; swarm:loopsBackTo describes council structure. No reviewed contract connects persona-position execution in AU to an EG coordination/recorded-decision result. Source review `0bce81c137b521ea446644d2880498e23d67c3ab5701f09ba3f67a4b2c0e00fc`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R137.13` / AU:25:** Retain partial. Extend existing R137/AU-HARNESS-R026 interfaces with the two measured signals, authoritative recursive-mode selection and an explicit AU observation-producer/decision-consumer binding; preserve R137.7-.12 package semantics. Producer/consumer path: AU _router_impl.py:811 -> run_full_rlm executes the EG ContextBudget decision wired beside tool_subset_outcome in stat_decide.rs:134. R137.12 explicitly depends on AU-CONTEXT-R004 for package application; that edge does not bind tool-output/node-count signals to recursive-mode selection. Source review `9724fcb9db21705fe9ac6a4b26d6d27280f55bf8a22bf11907b3ad9449383290`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.9` / AU:34:** EG emits a ranked `TaskPlan` tree with each skill id, parent skill id, context-match evidence, prior outcome refs, rank and plan revision; feedback may change the next breadth only through the versioned policy and pinned outcomes in `EG-DECISION-ENGINE-R140.102`. AU application of EG rank is a separate pending consumer dependency until its owner contract is reviewed; do not state that AU already applies the ranking. Preserve multi-step skill selection and existing AU authority. Producer/consumer path: EG plan_over_skills -> AgentLibraryStore -> served TaskPlan; AU experience feedback lacks a named EG selection-policy update consumer. Source review `ced4d0bb75fa260e4291aea5f71c28b407a95fc3e06047666e1278b043a3cdbd`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R137.14` / AU:37:** Extend this existing replacement gate with held-out question identity, full/compressed answer comparison and declared tolerance; retain raw context when the paired verdict fails. Producer/consumer path: AU ContextManager.before_model_request -> run_agent replaces context only after its summary gate; no held-out paired-answer verdict is a named prerequisite. Source review `7e1405981aa8b697a73eb7e8c3c3170be8edefc674c23beef740884450fc1add`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.10` / AU:39:** Extend the existing fan-out result and trace contract with summary fields, parent budget enforcement and decision/child/summary links; preserve EG planning authority. Producer/consumer path: AU _fork_fanout -> FanoutResult and parallel workspace synthesis; EG decides plans. The bounded child-summary producer/parent consumer boundary is absent. Source review `742906721351687d032991b4e9c0255baf7a6c480e21ac7a616975d40e38ce84`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R137.15` / AU:40:** Extend existing recursive request/result contracts with input/output and admitted tool signatures, validation at every call boundary and signature/model/tool trace binding. Producer/consumer path: AU run_full_rlm -> RLMEnvironment.run_parallel_sub_calls executes EG limits; span instrumentation records calls. No recursive-call signature producer/validator is named. Source review `4edb15328650ebf084f0ab1d073b092e5b684aafad9b7b1ca7f412859390b923`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.11` / AU:45:** Extend existing TaskPlan and runtime budget contracts with step-horizon splitting before launch and durable sub-horizon credit; do not substitute finance forecast horizons or solver budgets. Producer/consumer path: EG decompose_task/TaskPlan -> AU bounded map primitives; the horizon partition and durable credit receipt are not a named producer/consumer interface. Source review `c1bfd61241a0fd02ada2816f173a3b9568da5b48e490e76058fd740b356886da`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R059.1` / AU:57:** Extend existing skill and independent-outcome contracts with cross-model candidate identity/tree merge and per-backbone evaluation/transferability evidence; reuse the independent-evaluation gate. Producer/consumer path: AU experience/skill candidates -> EG admission-linked outcome aggregate; the shared tree and backbone-bound per-skill evaluation join remain unnamed. Source review `0c7c2eed61e3cfe6fe6c863c75fb66092f3a8bea97e3942b635c7b69f715d3a9`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.12` / AU:85:** Extend the existing plan/aggregation definition with multiple alternative trajectories for the same task followed by explicit summarization. Bind chosen width/depth to the committed decision and require paired remeasurement against a named single-trajectory baseline through R018. Producer/consumer path: R134 plan explicitly connects Decide TaskPlan to AU-CONTROL-R042/R043; R137 plan connects ContextBudget to AU-CONTEXT-R004/AU-HARNESS-R026. AU-HARNESS-R018 has run_agent paired arms. These dependencies lack the alternative-trajectory/summarization/baseline binding. Source review `a28929a50cbfcb691664403ce456fbf9dc38d6bd64b34424932e4fa22f9cbda1`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.13` / AU:91:** Extend the existing aggregation/result contract with cited analyst findings and one decision role. Require explicit abstention for unresolved conflict or missing citations, with served worker-to-decision positive and negative cases. Producer/consumer path: EG R134.5 plans TaskPlan through served Decide; AU R020.5 constructs worker slots through _dispatch_parallel_batch. The selected aggregation consumer lacks a cited-finding/conflict abstention contract. Source review `f5ddb0ac9b0329ac031c01c56c49ee03c922a991dbf2d0e29c111515424d3455`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R029.1` / AU:93:** Extend the retrieval route with a provider identity/revision separate from the reasoning model, then require exact provider execution and refusal on unavailable committed provider. Test provider/model independence and zero alternate-provider calls. Producer/consumer path: Preserve GRAPHOS-OPS-R021.2.1 retrieval.search and its tenant-scoped retriever. EG must produce the committed provider binding and the SDK/GraphOS consumer must execute that exact provider; that edge is missing. Source review `c9552392eb813763a83fc96fb48ad1bd18fd805ef608736f1dae5d48ab12c984`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.14` / AU:105:** Keep this R134.14 branch-fusion/every-outcome persistence contract distinct from breadth adaptation in `EG-DECISION-ENGINE-R140.102`. Bind each branch id, parent plan digest, outcome revision/digest and fusion input order; persist every branch outcome and record which later branch retrieved each outcome. Do not claim R140.102 supplies branch fusion. Test exact per-branch readback and a later retrieval of the pinned outcome. Producer/consumer path: Use the same missing branch-experience producer and committed planning consumer identified for AU:84. Existing attachment points are R134.5 Decide, R020.5 worker execution and R022.4 run outcome distillation. Source review `4fd31dd504f158f43bcf77d81a39c2419303a538b32553a77a39f6b5a575e9d2`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R020.1` / AU:110:** Extend experience-guided EG candidate selection with typed per-agent exploitation/exploration pools and a defined context-match test. Refuse or exclude exploration when a matching past trajectory exists; prove match, no-match and cross-agent isolation cases. Producer/consumer path: EG owns candidate selection and orchestration evidence; AU R022 supplies run experience. No per-agent two-pool producer or context-miss-only selection contract exists. Source review `dafa8396b5a7c2a168253c7be219718c78186266610e5a91e7dc318d35fc552b`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R133.7` / EG:13:** Extend existing traces and name the critique read path, retaining fencing/projection. Producer/consumer path: Terminal CommitWorkItemResult adapts traces after fenced CAS; AddNode validates traces; graph queries read them. Recursive hierarchy production and critique consumption are absent. Source review `9e0be988c182da3c7f440c14d2fc8e56f60426e543e3f53ba39f250f235e6c8d`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R100.1` / EG:16:** Add feedback ingestion/fitting linkage, retaining adapter/evaluation requirements. Producer/consumer path: R100 persists examples, R101 applies promoted post-RLS/pre-ANN query adapters, R040 gates publish. Signal production and example-to-fit linkage are absent. Source review `eca485ffdff9cea5394130b53e12a0edc235e342ab01accc36ad9d06f496daa8`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R088.2` / EG:17:** Add event-driven replay/harness semantics reusing AS OF and retaining policy-dependent refusal. Producer/consumer path: DecisionEval owns AS OF and fold replay. A real-world event simulator feeding an AU adaptive harness is undefined; policy-dependent environments must respect R119 off-policy routing. Source review `d01db99df0c6c22b37f82cd815e129956fa151e7da2b670b73348ca5058e629e`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R137.16` / EG:26:** Update the ledger links to R137.7-.12 and R128.5. Add only the remaining complexity-to-package policy and remaining-budget derivation/consumer binding, with served tests varying complexity at fixed cost and independently exhausting the current remaining-token budget. Preserve all package solver/certificate and old recursive-plan acceptance. Producer/consumer path: Planned served Decide(ContextBudget) -> existing solver -> independent certificate verifier -> sealed ContextPackageSelection -> AU-CONTEXT-R004 context-compile consumer. R137.11/.12 explicitly name the consumer; current dispatch absence is stated in R137.9. Source review `ccbc35ba3595763696c824434868079aeb401ce8b601041e913b323a2d2fb575`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R029.2` / EG:30:** Credit R137.7-.12 and link AU-CONTEXT-R004 rather than repeat package design. Define available-source enumeration and a reported selected/omitted partition with reasons, preserving authorization. Test relevant, irrelevant, over-budget and authority-displaced sources in one served context-compile journey. Producer/consumer path: Served Decide(ContextBudget) -> certified package -> AU-CONTEXT-R004 context compile; ontology source selection R072 and source-authority conflict handling supply distinct inputs. Source review `05fcbe47e588141000ec329927406270abd30b590d2624f22e9239079f829d38`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R134.15` / EG:36:** Each DAG step carries a positive estimated duration in integer milliseconds, a nonnegative versioned priority weight, and source evidence. Compute each path total as the sum of `duration_ms * weight`; return ordered `critical_chain_step_ids`, `total_weighted_duration_ms`, tie-break rule (lexicographically smallest step-id sequence), plan revision and input digest. Refuse cycles, missing duration/weight or overflow before emitting TaskPlan. Add a branched weighted DAG with a known longest path, deterministic ties, and a cyclic negative through served TaskPlan. Producer/consumer path: Served Decide(TaskPlan) -> plan_over_skills -> typed TaskPlan -> execution consumer; critical-chain fields and computation remain to be defined. Source review `6e1f0c3ab1626a00c8392e8a304b4c4a28a86d0aee8fff1933317bbdb4893966`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-DECISION-ENGINE-R135.8` / EG:52:** Retain R134/R135 coverage and define only the workflow-source compiler input, stored agent configuration/reuse binding and missing-step compile diagnostic. Prove equivalent NL/graph workflow compilation and reuse; a missing step must refuse without publishing a partial program. Producer/consumer path: Served Decide(TaskPlan) and ProgramLookup -> AgentLibraryStore/handle_decision_commit -> harness; SourceIngest is the workflow ingestion authority. A definition-to-program compiler between those surfaces remains unspecified. Source review `1ee5f2b5bd5bc45c9b9ff8ccaab8de944ed41eceab17a8c1e88cc86d4725e2dc`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run; names describe acceptance obligations, not evidence.

- **`EG-DECISION-ENGINE-R042.100` (AU:28; source `9dac8e5f41f6948885215836733ce3116b8246ca5c9c4725a12a62bdb11cb400`):** Prove with served Decide cases that a sufficiently sampled higher-success path is preferred, threshold-qualified repeated failure is skipped, and missing/stale/undersampled history preserves declared order. Verify the advisory cannot trigger write-back and every score recomputes from its bound counts. Producer/consumer boundary: EG R042 remains evaluate-only connector tool-choice advisory; SDK owns ordered transport fallback and current attempt outcomes. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R042.101` (AU:86; source `38315c5f9a088c33c4b81b88b813ed851f97f454d788c9fb07185c88c752f979`):** For each paired fixture, show necessary/not-necessary/unknown results; at the AU call boundary prove every disagreement creates a linked trace event and matching decisions create none. Stale or insufficient evidence must not become a necessity verdict. Served EG and AU receipts remain separate and linked by decision/measurement ids. Producer/consumer boundary: R134.2 relevance is only a candidate prefilter and AU-HARNESS-R018.5 generic profile metrics do not measure tool necessity; AU run_agent per-tool spans are the consumer boundary. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R129.100` (AU:38; source `1d54abebd7cbc92bd3d6c397f334b42d602b4c3b9f77e0091f1c683774701d72`):** Served tests cover correction/output mismatch refusal, proposal-only state, approval plus invariant pass, invariant rejection, version replacement, revocation and a future invocation using the approved version only. Every state transition reads back with actor, version and source-output lineage; existing R129 invariant behavior stays mandatory. Producer/consumer boundary: Extend R129 promotion authority and AU future-invocation application; R034 contradiction-retraction remains separate. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R134.100` (AU:30; source `8f2ff3030a71d67b20cd19301f0ed8cef75e975403595cec4060abf9fa8952c1`):** At the actual served `agents.run` -> AU `_fork_fanout` -> generated EG aggregation call, cover all-success results, named failed gaps, missing/duplicate/stale receipts, refused concurrent-budget admission, and independent artifact readback. The output must bind the same selected operation/version and exact AU/GraphOS receipt set; no extrapolated capacity claim is added. Producer/consumer boundary: AU owns FanoutResult production; GraphOS `agents.run` owns durable instance receipts and capacity/refusal. EG owns selected operator and public application/readback contract only. Preserve existing R134.1/.3 and GRAPHOS-FLEET-R024.5-.9. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R134.101` (AU:94; source `19331af6d24dacefccebddcae637919b39cb111b800e3bd239a68b42b8ad10c7`):** Served fan-out test sends several outputs and a named failed gap through the designated coordinator, then reads one structured artifact back. Refuse absent coordinator, missing/duplicate child accounting, schema-invalid artifact or mismatched digest; verify a non-coordinator cannot finalize and every child is accounted for. Producer/consumer boundary: EG-selected TaskPlan/AggregationOp -> AU `_fork_fanout` FanoutResult -> named coordinator -> one final artifact; retains AU-CONTROL-R043.2/.4 named gaps. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R137.100` (AU:59; source `0b55041f0517baa9de449e3d712356a0e252cd194365e09ae559e9067cc8eb1f`):** Through served Decide and AU run-loop consumer, prove initial instructions precede execution, a runtime refinement is withheld before its trigger and disclosed at the trigger, and every disclosure is trace-linked. Stale or unauthorized instruction revisions refuse without application; verify distinct initial/runtime tags and exact version readback. Producer/consumer boundary: Existing EG context/decision authority -> AU run loop; general context selection, reminder and trace counters do not cover disclosure. AU sole writer owns runtime application. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R134.102` (AU:24; source `7cbecf865c6185e23ec202d541c5f8d33388338af413ac06209709c619fbe200`):** Served Decide matrix proves below-threshold and unstable/incomplete windows refuse, while a complete above-threshold window admits. Mutate a sample, threshold, metric or policy revision and require a new verdict. AU consumer demonstrates no delegation effect on refusal. Producer/consumer boundary: EG owns authoritative TaskPlan/delegation admission; AU executes the admitted plan. Existing retrieval quality-saturation logic is adjacent only. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R136.100` (AU:32; source `1d12449ecff9c30c5999933324f1f705fd826539eba6182f3e6d2773c263fec3`):** Positive paired test uses identical test digest and environment across before/after revisions and proves fail then pass. Negative cases alter either revision/test digest, omit a receipt, pass before or fail after; each refuses admission. Read back the admitted proposal with both receipt links. Producer/consumer boundary: AU CorrectionProposal/study_admit supplies trace and benchmark; EG decides admission. Generic benchmark lift does not replace paired proof. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R031.100` (AU:41; source `8c69b66d62c833b4da482e38a51efa624c3e3fc924d6fd6d3e2ef114378a8527`):** For served EG admission, use a paper relevant to both goals and graph, one relevant to only one, below-threshold, and duplicate/stale snapshots. Only the both-relevant accepted record yields a selected-claim ingestion request; read back the decision and source bindings. Do not claim six-hour schedule or provider fetching is implemented. Producer/consumer boundary: GraphOS owns six-hour durable scheduler; SDK owns paper fetching/provider receipts; EG owns relevance scoring/admission and selected-claim contract. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R036.100` (AU:46; source `04556089bf2790a52c59ede225b6212ccd90c0f7ca1dfad07b869ac9de6a19a1`):** Test substitution independently for all four primitive classes, compatible and incompatible schema/effect changes, and trace-derived proposal creation that remains inactive until normal admission. Served assembly receipt names each primitive/version and compatibility proof. Producer/consumer boundary: EG graph.assemble is composition authority; AU HarnessPort executes selected composition. Existing prompt-only optimization proposal path remains and typed HarnessPort is preserved. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R136.101` (AU:47; source `9f72c0e6cb8d476050ffde54043ba0e37a063bfda05eb3e27020312cd9eeb69b`):** Served revision sequence starts with satisfied and unsatisfied criteria, applies process-level feedback, then proves a nonregressing revision accepted and a single previously satisfied criterion regression refused. Mutating rubric or prior report digest invalidates the comparison. Producer/consumer boundary: AU ContinuousEvaluationEngine produces per-criterion evidence/report revisions; EG evaluates the bound round verdict. Aggregate divergence and self-reflection are not criterion-level nonregression. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R121.100` (AU:54; source `78c1b58ee2c97f19ad4834981d1b75adeb89c82881710aed055a0cc6f5c36760`):** Served AU/EG chain verifies a supported step, rejects an under-evidenced step and proves downstream conclusions are absent on failed/uncertain dependencies. Alter proof digest, evidence revision or uncertainty bounds and require re-verification. Trace readback shows the entire dependency chain. Producer/consumer boundary: EG owns proof/evidence/verdict; AU run execution gates continuation on the returned verdict. Existing Claim/Evidence and ordered trace digest semantics remain intact. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R138.100` (AU:55; source `3d07d9e40d19b8d4502fcbf3fc2e682f67bcdeaa0730d5d698f7af32767bb785`):** Served admission accepts only a complete receipt whose artifact digest matches the proposed code and whose edge cases are all resolved and retested. Negative cases include skipped verifier, unresolved edge case, stale artifact, changed tests and failed recheck; none can activate/write code. Producer/consumer boundary: AU generates/proposes code and verifier evidence; EG admission consumes it; existing governed write path retains materialization authority. ProbeVerdict R138.1-.6 is a separate red-team probe capability. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R136.102` (AU:56; source `712793000145f8e6b1c3ac1de647e91256312eb601def299653a9671de2757ba`):** Serve one fully graded report admitted against a proposal, then refuse missing outcome, missing rubric, ungraded criterion, wrong artifact digest, stale rubric and absent grade evidence. Read back the proposal and prove no admission side effect on refusal. Producer/consumer boundary: AU study_admit/ContinuousEvaluationEngine supplies the report; EG decision verdict controls admission. Preserve R019/R021 evidence and per-criterion R136.101 behavior. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R138.101` (AU:83; source `0c5f2631be262ba69472eb94841b7acfbcfd6d0cb379382c528ccd0efb4d22e7`):** Served review fixtures include one finding from each class, one evidence-backed low-impact false positive, and an injected high-impact finding. Verify five-class coverage, changed-range binding, evidence retention and filtering disposition; reject missing revision, unsupported class, absent proof or silently omitted high-impact findings. Producer/consumer boundary: EG owns review verdict/schema; named GraphOS authenticated changeset-review producer/consumer must be served and linked separately. Existing R138.3 probe endpoint is not that consumer. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R036.101` (AU:107; source `d179fc0eb33039df3a87efe8a59ce2a635f0e39f360c58f925011448d0401cdc`):** Served EG assembly accepts verified agent/skill facts and returns a plan with exact revisions and transformation order. Refuse absent/stale discovery receipts, incompatible transformation schemas or changed artifact digest. Separate AU discovery receipt and WebUI sandbox receipt must be linked for end-to-end closure; no runtime implementation is claimed. Producer/consumer boundary: EG composition authority -> AU PATH discovery and skill execution -> WebUI isolated generated-artifact frame. Existing DESIGN guidance does not provide these producer/consumer receipts. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-DECISION-ENGINE-R014.100` (AU:108; source `2dfb2ab7469f19faf03d91782fa276ed2c613dec2e1d1fce2c629e5b8974b476`):** Tiny exhaustive candidates prove the selected result is the smallest/cost-optimal eligible composition under the declared objective and constraints; test equal-size/cost ties, unknown facts, inadequate smaller candidate and illegal candidate refusal. The output preserves separate size and cost measurements. No test imposes or removes shortcut comments. Producer/consumer boundary: Reuse R014 exact bounded solver/graph.assemble authority. AU records execution cost; code-size measurement and candidate adequacy feed EG. Preserve legal/policy constraints and hold comment-style conflict. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run; they describe required future acceptance, not evidence.

- **`EG-DECISION-ENGINE-R139.100` / AU:58; source `f13717cacb9e87ff76591467e17b5f9d76d44577f62d71b57d57c42bc89d37ba`:** Serve one graph paper, one graph codebase and one filesystem source in the same comparison; assert exact ordered references, revisions and digests in readback. Refuse stale, unauthorized and mismatched graph refs before comparison, while filesystem-only intake remains valid. AU consumer receipts remain a separate dependency. Producer/consumer: EG owns authorized graph resolution and returns pinned source evidence; AU owns StudySource intake, comparison execution and proposal generation. Preserve AU-HARNESS-R019.2/.3/.5/.6 and do not treat capability lookup as graph-source intake. Parent `EG-DECISION-ENGINE-R139` remains intact.

- **`EG-DECISION-ENGINE-R019.100` / GRAPHOS:17; source `0e4858352b73b18661ed21ef503cc4c4f36e72045c3ffeff8d2094d9f8f8e50a`:** Serve a decision with all three evidence streams and independently read back exact provenance. Refuse a recommendation commit with missing, stale, wrong-reviewer-scope or digest-mismatched approval and prove no decision revision/effect changed. Approve the exact revision, commit once, and verify reviewer provenance and idempotent retry under R019. Producer/consumer: EG produces decision/provenance and approval-binding inputs; GraphOS owns shared invoke/A2A approval admission; SDK owns governed source effects and effect receipts. R047 remains the related producer reference. Parent `EG-DECISION-ENGINE-R019` remains intact.

- **`EG-DECISION-ENGINE-R029.100` / PIPE:22; source `0d3964cba3359a3b454f2fd7b61f2b893b3aec0d58613062b859c2201344980c`:** Through the real retrieval command, load a valid file and assert its config/mode/plan digests are bound in the run record. Refuse missing file, invalid typed field, unknown mode and stale/mismatched mode digest before command effects. Repeat the same command/input and verify identical config, mode and decision bindings; do not assert result determinism beyond declared source behavior. Producer/consumer: EG owns the typed plan/mode decision and digest; AU owns command-file loading and execution. Do not conflate plan selection with file loading or add a pipeline runtime orchestrator. Parent `EG-DECISION-ENGINE-R029` remains intact.

- **`EG-DECISION-ENGINE-R140.100` / AU:44; source `74416937af9a4934ae1f5d1a02e30336deb1535189a07f53fb81fb352f81779f`:** Run served Decide with pinned simple and complex cases: simple selects one trajectory and zero deliberation calls; complex declares K and returns only a plan for AU. Refuse missing/stale policy and invalid K before execution. Bind the AU execution receipt to the exact decision/policy and K. Producer/consumer: EG owns score/policy/branch decision; AU owns trajectory generation and deliberation execution. AU-CONTROL-R043 independent-subtask fan-out remains distinct. Parent `EG-DECISION-ENGINE-R140` remains intact.

- **`EG-DECISION-ENGINE-R140.101` / AU:51; source `be286cbd079c08493be1dcc0512aa562b6e2f2fec8d3566437837d91b5f36ba0`:** Use two trajectories with identifiable reasoning-only and evidence spans; verify the projection removes only declared spans, shuffled order is reproducible from the recorded seed, and all source digests remain readable. Verify the deliberation output is appended as a new linked candidate. Refuse a missing source digest or unlinked output without mutating source experiences. Producer/consumer: AU record_outcome/SkillGraphPipeline owns experience production and deliberation; EG owns the refinement decision/result binding. Preserve AU-HARNESS-R022 experience identities and source trace provenance. Parent `EG-DECISION-ENGINE-R140` remains intact.

- **`EG-DECISION-ENGINE-R140.102` / AU:84; source `b56fbf074624786b4a8c2b499f985ecc2d14d1e8349d08781cf28aba07c3abef`:** Serve two sequential branches: record an earlier outcome, retrieve it at the next branch start and prove the pinned policy changes the breadth decision. Verify decision and execution receipts share the outcome digests. Refuse stale/cross-tenant history; with no history, use only the policy’s explicitly declared initial breadth and mark history absent. Producer/consumer: EG TaskPlan/Decide owns committed breadth decisions; AU owns branch execution and attempt outcomes. Preserve AU-HARNESS-R020 worker slots and R022 outcome storage; this child covers long-horizon candidate branches only. Parent `EG-DECISION-ENGINE-R140` remains intact.

- **`EG-DECISION-ENGINE-R140.103` / AU:87; source `6f67238075c0bf262188d97a1813b18bfb0c71e13c6689a583c788d956287612`:** Serve a no-verifier fixture with a known pairwise outcome matrix; assert the exact ranking, tie-break, retained partition and revisions only for non-retained candidates. Refuse incomplete pair coverage, mismatched candidate digests or absent policy. With a verifier present, assert the pairwise fallback is not selected. Read back all bound evidence. Producer/consumer: EG owns pairwise evidence aggregation, ranking and retention decision; AU owns comparison execution and revisions. Preserve calibrated-ranking constraints in R130 and AU generation ownership. Parent `EG-DECISION-ENGINE-R140` remains intact.

- **`EG-DECISION-ENGINE-R141.100` / EG:23; source `ac1b5df0212beaade03e78502a494148692e1a0007c3fa68ee37a2bb6dbe8cce`:** Serve a passing rewrite and independently read back the candidate as active with metric, source and policy bindings. Serve below-threshold fidelity, degraded history, invalid signal/token counts and stale source revision; each refuses, discards the candidate and proves the original active revision/digest remains unchanged. Producer/consumer: EG owns the rewrite admission decision and active-memory revision; AU harness consumes the decision and may generate candidate rewrites. Retain the original as the rollback source. Parent `EG-DECISION-ENGINE-R141` remains intact.

- **`EG-DECISION-ENGINE-R141.101` / AU:125; source `27a6a74820b2b646163d8f7bd58b94167b9f0a3e423b8bf5210fe6e758deb440`:** Serve a candidate with both metrics passing and read back the admitted candidate plus benchmark receipt. Independently fail recall and answer accuracy; each must block every mutation type and leave the baseline active. Refuse mismatched dataset/snapshot digest or missing denominator before activation. Producer/consumer: AU executes the repeatable benchmark and returns its receipt; EG owns memory-change admission and refuses activation unless the receipt matches candidate and baseline. Reuse existing evaluation/activation mechanisms without substituting R060. Parent `EG-DECISION-ENGINE-R141` remains intact.

- **`EG-DECISION-ENGINE-R142.100` / GRAPHOS:8; source `5df3f5a45d486aeba5765a31b6c6283637adf2bd8ce63a87938eddd28b90f082`:** Run a local-model atomic evaluation and verify EG attestation readback, then certify a dependent workflow and validate it in GraphOS. Refuse workflow certification with an uncertified, stale or failed atomic dependency and prove no invocation begins. Mutating skill, model, dataset or policy digest invalidates the attestation. Producer/consumer: Extend existing GRAPHOS-FLEET-R034.7-.12 identity bindings; do not add host scoring authority or infer certification from host prerequisites alone. Parent `EG-DECISION-ENGINE-R142` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — retrieval evaluation

No test in this section has run.

- `EG-DECISION-ENGINE-R100.100`: run two registered algorithms on the same served fixed corpus/query set and independently labeled queries; verify exact hits-at-k numerator/denominator, input/result digest, DecisionEval receipt readback and shared comparison identity.
- Verify `RetrievalContextSearchV1` offsets against the exact NFC UTF-8 source text, 100-span/8-KiB bounds and tenant filters. Missing labels, stale corpus, mismatched settings, incomplete outputs, expired cursor and denied context refuse without fabricated metric or protected-count leakage.
<!-- /spec-definition-patch -->
