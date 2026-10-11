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

## E. Pack facts at the decision boundary

Publish one pack-provided component with native and foreign capability annotations, cost, latency, modality, schema digest, model facts and `contract_pin`. Assemble from that component and assert every such premise remains a publisher claim in the record and explanation. The native capability may participate in an `is_a` derivation whose conclusion class is still claim. The foreign IRI remains an exact unentailed claim. Omit the member in a later valid pack and assert `Withdrawn` makes it ineligible. Re-import byte-identical entry content and assert its candidate digest and decision result do not churn. A stale projection marker must prevent a projected ontology or shape fact from entering a decision. Plant `DecisionRecord`, `DecisionHead` and `DecisionPolicy` pack entries and assert refusal before any decision-owned state changes. These probes use the pack API as a fixture. Pack parser, body and crash recovery have their own acceptance suite.

Check metric labels omit raw facts and errors omit invisible component identifiers. Every negative probe asserts no durable state change. A served test uses disposable credentials and a local policy fixture.

## H. Quality and acceptance matrix

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

Except for the local retry model noted below, these tests are planned and unexecuted.
Those planned test names do not identify existing tests.
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
| EG-DECISION-ENGINE-R113.2.1 | `src/server/handlers/decide/commit.rs` | `tests::topology_commit_refuses_uncoordinated_owners` | SPECIFIED; not run |
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

EG-DECISION-ENGINE-R090.2 requires `tests/decision_resident_scorer.rs::compiled_engine_scores_without_external_runtime`.
EG-DECISION-ENGINE-R090.3 requires `benches/decision_resident_scorer.rs::resident_scorer_meets_declared_budget`.
Both checks remain planned and unrun. Budget declarations do not establish either result.

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

## Bounded topology oracle

| Requirement | Proving test |
|---|---|
| EG-DECISION-ENGINE-R110.1 | `crates/eg-compute/src/assemble/tests/topology_oracle.rs::property_oracle_agrees_with_the_solver_for_instances_up_to_twenty_variables` checks 66 fixtures and twenty actual variables. |

Native execution remains pending after dependency compilation exceeded local resources.
The isolated fixture arithmetic check does not execute the assembly or solver.

## Typed provenance acceptance

- R121.1: `crates/eg-compute/src/assemble/tests/evidence.rs::assembly_evidence_uses_typed_premises_not_labels` exercises actual assembly and replay.
- R121.2: planned `src/server/handlers/decide/stat_tests/classes_tests.rs::graph_claim_and_evidence_control_answer_class` covers graph-source provenance.
