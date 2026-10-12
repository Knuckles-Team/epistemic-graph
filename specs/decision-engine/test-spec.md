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
