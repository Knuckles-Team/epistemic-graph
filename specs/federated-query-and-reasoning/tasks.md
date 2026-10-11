# Delivery tasks

Check boxes describe work remaining for this combined contract. They do not certify acceptance. Assign one or more stable IDs to each PR, keep implementation and acceptance states separate, and record evidence in the PR and this spec before closing the task.

## UQL and served query

- [x] Audit the public head for the single read-only UQL method, typed parameters and generated Python sender. Remove any remaining duplicate query-text route. **EG-FEDERATED-QUERY-R020, EG-FEDERATED-QUERY-R025, EG-DURABLE-KERNEL-R023**
- [x] Close predicate/lexer/source/modality/traversal parity and canonical round-trip exhaustiveness, including generated grammar and feature-gated refusal. **EG-FEDERATED-QUERY-R021 and related rows (see requirements.md), EG-TYPED-PACKS-R080, EG-FEDERATED-QUERY-R061**
- [x] Complete named subplans, score channels, budgets, proof, explain/profile, KnowledgeSet and shared DecideText lexing; prove served wire parity. **EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R028, EG-FEDERATED-QUERY-R065, EG-FEDERATED-QUERY-R067, EG-FEDERATED-QUERY-R068, EG-FEDERATED-QUERY-R070, EG-DECISION-ENGINE-R104**
- [x] Verify all executable UQL examples and stable error diagnostics against the current grammar. **EG-FEDERATED-QUERY-R064, EG-FEDERATED-QUERY-R026**
- [x] Verify `VALIDATE SHAPE` and graph-free SQL read-only guards with positive and mutation-negative fixtures. **EG-FEDERATED-QUERY-R005, EG-FEDERATED-QUERY-R015**

## Federation and virtual graphs

- [x] Qualify capability model, shared renderer, typed budgets and optimized/full-fetch oracle on the exact public head. **EG-FEDERATED-QUERY-R038, EG-FEDERATED-QUERY-R041–EG-FEDERATED-QUERY-R043**
- [x] Add a typed `ForeignSourceCapability`/cost-model struct (cardinality estimate, network-budget cost, which of projection/filter/limit-order/aggregate/same-source-join/batched-lookup pushdown kinds the source's declared capability proves sound) plus construction and refusal tests. **EG-FEDERATED-QUERY-R038.1**
- [x] Expose redacted trace and caller-lowered budget in served UQL `EXPLAIN`/`PROFILE`; verify no URL/DSN leakage. **EG-FEDERATED-QUERY-R044**
- [x] Qualify capability model, shared renderer, typed budgets and optimized/full-fetch oracle on the exact public head. **EG-FEDERATED-QUERY-R038, EG-FEDERATED-QUERY-R041–EG-FEDERATED-QUERY-R043**
- [x] Expose redacted trace and caller-lowered budget in served UQL `EXPLAIN`/`PROFILE`; verify no URL/DSN leakage. **EG-FEDERATED-QUERY-R044**
- [x] Add typed column-carrying foreign rows and exact/inexact/unsupported residual handling. Validate foreign-only predicates, projection dependencies and schema drift against a full-fetch oracle. **EG-FEDERATED-QUERY-R045**
- [x] Execute approved OBDA virtual-graph solutions with typed ORDER/LIMIT/aggregate/join pushdown and sound fallback. **EG-FEDERATED-QUERY-R046, EG-FEDERATED-QUERY-R055**
- [x] Bound source concurrency, learn/persist provenance-tagged stats, and cache only with fresh owner-bound watermarks. **EG-FEDERATED-QUERY-R018, EG-FEDERATED-QUERY-R047–EG-FEDERATED-QUERY-R049**
- [x] Define `JoinCardinalityStatistic`/`StatisticsProvenance` in `crates/eg-plan/src/federation_stats.rs`, refusing a statistic built with no provenance. **EG-FEDERATED-QUERY-R048.1**
- [x] Qualify SPARQL SERVICE bind join, Iceberg pruning, RemoteEngine pushdown and the SSRF guard under the one outbound gate. **EG-FEDERATED-QUERY-R050, EG-FEDERATED-QUERY-R051, EG-FEDERATED-QUERY-R052, EG-FEDERATED-QUERY-R054**
- [x] Register Trino, Cypher and Spark as typed `ForeignSourceSpec` kinds with per-kind capability sets; Spark is batch-only. **EG-FEDERATED-QUERY-R053**
- [x] Prove RemoteEngine key/LIMIT UQL pushdown against a recording second engine, including signed owner context, exact residual, pagination and bounded fallback. **EG-FEDERATED-QUERY-R052**
- [ ] Integrate the local [unified-data-plane](../unified-data-plane/spec.md) source identity and approved mapping contract without duplicating its registry or dialect work. **EG-UNIFIED-DATA-PLANE-R002, EG-UNIFIED-DATA-PLANE-R005–EG-UNIFIED-DATA-PLANE-R007 interface only**

## Search, reasoning and numerics

- [x] Validate semantic and edge index lifecycle, filtered BM25/ANN correctness, mixed traversal top-k/shortfall, generation replay and RLS. **EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008, EG-FEDERATED-QUERY-R009, EG-FEDERATED-QUERY-R035, EG-FEDERATED-QUERY-R039, EG-FEDERATED-QUERY-R040**
- [x] Restore index state on graph reopen within the bounded performance profile, and refuse reads while materialization is incomplete. **EG-FEDERATED-QUERY-R036**
- [x] Validate current corpus compose, EL/RL/tableau consistency, disjointness, bounded derivation, proof and skipped-event rebuild. **EG-FEDERATED-QUERY-R001, EG-DURABLE-KERNEL-R002, EG-FEDERATED-QUERY-R004, EG-FEDERATED-QUERY-R006, EG-TYPED-PACKS-R071, EG-FEDERATED-QUERY-R010, EG-FEDERATED-QUERY-R011, EG-FEDERATED-QUERY-R012**
- [x] Implement and validate shared series/attribution/impact/barrier/motif kernels with exact or seeded oracles and typed budgets. Remove duplicate probability math. **EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031, EG-FEDERATED-QUERY-R032, EG-FEDERATED-QUERY-R033, EG-FEDERATED-QUERY-R034, EG-FEDERATED-QUERY-R037, EG-FEDERATED-QUERY-R056**

## Additional coverage

- [x] Close the remaining requirements no task above names: community-detection quality function and naming/connectivity (**EG-FEDERATED-QUERY-R060, EG-FEDERATED-QUERY-R063**), streaming memory test isolation (**EG-FEDERATED-QUERY-R062**), SPARQL evaluator file-size split (**EG-FEDERATED-QUERY-R066**), and SQL/HTTP pushdown batching and pagination (**EG-FEDERATED-QUERY-R069, EG-FEDERATED-QUERY-R071**); see `requirements.md` for each ID's definition.

- [x] Serve ontology source selection with premises and add operation-bound API, MCP, A2A and GraphQL source kinds; run F-07 and F-08. **EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073**
  - [x] Typed `CoverageResult`/`SourceSelectionHop`/`Premise` model in `eg-rdf::source_selection`, proved against a fixture (direct mapping, subclass-serves-superclass, uncovered, disconnected). **EG-FEDERATED-QUERY-R072.1**
  - [x] Typed `ForeignSourceSpec` variants (`Api`/`Mcp`/`A2a`/`GraphQl`) with declared `ForeignOperationCapabilities`, plus unbound/ungranted and budget-exceeded refusal tests. **EG-FEDERATED-QUERY-R073.1**

## Acceptance

- [ ] Run all applicable [test cases](test-spec.md) and quality gates on the final public head; record one evidence line per accepted ID.
- [ ] Reconcile the spec state legend and delivery crosswalk against public merged commits and hosted CI. Do not mark a historical `BUILT` row `ACCEPTED` without the stated proof.
- [x] Keep the SQL provider module within the configured KISS file cap with behavior-preserving extraction. **EG-FEDERATED-QUERY-R057**
- [x] Add the typed `NlQueryResult`/`NlQueryError`/`NlQueryBudget` disclosure model (`plan_only`, `plan_and_execute_typed`) to `eg-plan`, with refusal and plan-only tests. **EG-FEDERATED-QUERY-R058.1**
- [ ] Wire `Method::NlQuery`'s dispatch handler and result contract to the typed disclosure model so the served result carries the executed UQL, and expose a plan-only entry point to callers. **EG-FEDERATED-QUERY-R058.2**
  - [x] Enforce the typed NL budget at the dispatch handler before parsing. **EG-FEDERATED-QUERY-R058.2.1**
  - [ ] Wire the result contract to carry the executed UQL and expose a plan-only entry point. **EG-FEDERATED-QUERY-R058.2.2**
- [ ] Confirm natural-language results are never cached on the NL text and that no mutating/side-effecting operation is reachable from the NL surface. **EG-FEDERATED-QUERY-R058.3**
  - [x] Guard that NL results are never cached on the NL text. **EG-FEDERATED-QUERY-R058.3.1**
  - [ ] Confirm no mutating operation is reachable from the NL surface. **EG-FEDERATED-QUERY-R058.3.2**

## Decomposition children (tracked)

- [x] **EG-FEDERATED-QUERY-R048:** Join ordering across foreign sources uses learned, provenanced statistics
- [x] **EG-FEDERATED-QUERY-R058:** Natural language runs only as disclosed, read-only typed UQL
- [ ] **EG-FEDERATED-QUERY-R074:** Served `select_topology` reasoning-policy op
- [ ] **EG-FEDERATED-QUERY-R075:** Generated client method for `select_topology`
- [ ] **EG-FEDERATED-QUERY-R076:** Canonical ontology hash for a Turtle payload

- [x] EG-FEDERATED-QUERY-R056: execute the exact causal-impact and shared numeric
  source modules, confirm all five tests pass, and bind the independent literal
  far-tail regression to the existing requirement. Full-crate and hosted gates
  remain separate from this pure-module proof.
- [ ] **EG-FEDERATED-QUERY-R077 (rollup):** Vectorized SQL text columns (rollup).
  - [ ] **EG-FEDERATED-QUERY-R077.1:** `ColumnVectorBinding` typed model.
  - [ ] **EG-FEDERATED-QUERY-R077.2:** Bind and unbind surface.
  - [ ] **EG-FEDERATED-QUERY-R077.3:** Row-write re-embedding hook.
  - [ ] **EG-FEDERATED-QUERY-R077.4:** Vectorization recommendation.
  - [ ] **EG-FEDERATED-QUERY-R077.5:** End-to-end column vector search.
  - [ ] **EG-FEDERATED-QUERY-R078:** Tenant and clearance predicates are part of the query plan (rollup).
  - [ ] **EG-FEDERATED-QUERY-R078.1:** `PlanScope` typed model
  - [ ] **EG-FEDERATED-QUERY-R078.2:** Predicate injection as a replaceable default
  - [ ] **EG-FEDERATED-QUERY-R078.3:** Attach at the UQL statement handler
  - [ ] **EG-FEDERATED-QUERY-R078.4:** Unverified caller refusal
  - [ ] **EG-FEDERATED-QUERY-R078.5:** Scope premise in the trace
  - [ ] **EG-FEDERATED-QUERY-R078.6:** Out-of-tenant read proof
  - [ ] **EG-FEDERATED-QUERY-R079:** Lake catalog schema evolution and time travel (rollup).
  - [ ] **EG-FEDERATED-QUERY-R079.1:** `ReorderColumn` kind and classification
  - [ ] **EG-FEDERATED-QUERY-R079.2:** `evolve_column` for four change kinds
  - [ ] **EG-FEDERATED-QUERY-R079.3:** Attach to the CommitTable route
  - [ ] **EG-FEDERATED-QUERY-R079.4:** Unapproved destructive change refusal
  - [ ] **EG-FEDERATED-QUERY-R079.5:** Schema version in the operation record
  - [ ] **EG-FEDERATED-QUERY-R079.6:** Commit, time-travel and manifest proof
  - [ ] **EG-FEDERATED-QUERY-R080:** Graph is the sole write boundary; projections are read-only (rollup).
  - [ ] **EG-FEDERATED-QUERY-R080.1:** `ProjectionAccess` typed model
  - [ ] **EG-FEDERATED-QUERY-R080.2:** `refuse_projection_write` classification
  - [ ] **EG-FEDERATED-QUERY-R080.3:** Attach at the listener dispatch
  - [ ] **EG-FEDERATED-QUERY-R080.4:** `PROJECTION_READ_ONLY` refusal
  - [ ] **EG-FEDERATED-QUERY-R080.5:** Refusal and read-version evidence
  - [ ] **EG-FEDERATED-QUERY-R080.6:** Write-then-read projection proof
- [ ] **EG-FEDERATED-QUERY-R081:** Retrieval sufficiency verdict with repair step, fallback policy and round limit.
- [ ] **EG-FEDERATED-QUERY-R081.1:** Model. Add `SufficiencyVerdict{verdict, evidence_count, reason, repair, fallback, round, round_limit}` in `crates/eg-types/src/retrieval_verdict.rs`. The `verdict` set is closed: `sufficient`, `insufficient`. `repair` is `BoostK{k}`, `SwitchMode{mode}` or `Stop`. `fallback` is `Lenient` or `Strict`. No type of this shape exists today. Proving test: `crates/eg-types/src/retrieval_verdict.rs::tests::verdict_round_trips_and_rejects_unknown`.
- [ ] **EG-FEDERATED-QUERY-R081.2:** Behavior. Add `judge_sufficiency(rows, rule, call) -> SufficiencyVerdict`. The default `rule` is `min_evidence_count`, a named replaceable value with default 3. The default repair order is `BoostK` (k doubles, capped at 64), then `SwitchMode`, then `Stop`. The call carries `round` and `round_limit`. At `round_limit` the repair must be `Stop`. Proving test: `crates/eg-plan/src/retrieval_verdict.rs::tests::repair_order_ends_at_round_limit`.
- [ ] **EG-FEDERATED-QUERY-R081.3:** Wiring. `judge_sufficiency` attaches at the end of `execute` in `crates/eg-plan/src/exec.rs:536`, after `fuse_rrf` at `crates/eg-plan/src/exec.rs:848` returns its `RowSet`. A plan that carries a `SufficiencyRule` gets the verdict beside its rows. The production caller is the served query path: `eg_plan::execute` is called from `crates/eg-graphql/src/crossmodal.rs:429`. This row creates the call site in `execute`. No production caller of a sufficiency check exists today. Proving test: `crates/eg-plan/src/exec.rs::tests::execute_returns_verdict_for_rule_plan`.
- [ ] **EG-FEDERATED-QUERY-R081.4:** Refusal. A verdict call without `round_limit` must refuse with `RETRIEVAL_ROUND_LIMIT_REQUIRED`. A `Strict` fallback with an `insufficient` verdict must return `abstain` and no extractive answer. The refusal names the missing field or the fallback. Proving test: `crates/eg-plan/src/retrieval_verdict.rs::tests::strict_insufficient_abstains`.
- [ ] **EG-FEDERATED-QUERY-R081.5:** Evidence. Each verdict must be recorded as a premise on the retrieval `DecisionRecord` of `EG-DECISION-ENGINE-R100`. The premise lists `verdict`, `evidence_count`, `reason`, `repair`, `fallback` and `round`. Proving test: `crates/eg-plan/src/retrieval_verdict.rs::tests::verdict_premise_lists_all_fields`.
- [ ] **EG-FEDERATED-QUERY-R081.6:** Hot-path proof. The entry point is the served cross-modal query operation. The test sends a query that returns two rows against a rule of three. The call passes through `execute` at `crates/eg-plan/src/exec.rs:536`. It observes an `insufficient` verdict, repair `BoostK`, and the premise on the decision record. Proving test: `tests/retrieval/test_sufficiency_served.py::test_insufficient_returns_boost_k`.
- [ ] **EG-FEDERATED-QUERY-R082:** Iterative retrieval stop rule with a calibrated confidence margin.
- [ ] **EG-FEDERATED-QUERY-R082.1:** Model. Add `CalibrationTable{rounds: Vec<RoundCalibration>}` and `StopVerdict{stop, reason, round, margin}` in `crates/eg-types/src/retrieval_stop.rs`. `RoundCalibration` holds `round`, `baseline_confidence` and `margin_threshold`. `reason` is closed: `answer_repeated_margin_met`, `round_limit`, `continue_answer_changed`, `continue_margin_low`. Search found no stop type in the engine. Proving test: `crates/eg-types/src/retrieval_stop.rs::tests::table_and_verdict_round_trip` [arXiv:2606.13814].
- [ ] **EG-FEDERATED-QUERY-R082.2:** Behavior. Add `decide_stop(table, history) -> StopVerdict`. The rule stops when the latest answer equals the previous answer and `confidence - baseline_confidence` exceeds `margin_threshold`. The default table is a named replaceable value. The paper values are starting defaults that EG measurement replaces. The rule has no learned part. Proving test: `crates/eg-plan/src/retrieval_stop.rs::tests::repeat_and_margin_stops` [arXiv:2606.13814].
- [ ] **EG-FEDERATED-QUERY-R082.3:** Wiring. `decide_stop` attaches beside `judge_sufficiency` of `EG-FEDERATED-QUERY-R081.3` at the end of `execute` in `crates/eg-plan/src/exec.rs:536`. The caller sends the round history in the query call. The production caller of `execute` is `crates/eg-graphql/src/crossmodal.rs:429`. This row creates the call site. No production caller of a stop rule exists today. Proving test: `crates/eg-plan/src/exec.rs::tests::execute_returns_stop_verdict_with_history`.
- [ ] **EG-FEDERATED-QUERY-R082.4:** Refusal. A round with no row in the table must not stop the loop early. `decide_stop` must refuse with `ROUND_NOT_CALIBRATED` and name the round. The harness then continues to `round_limit`. Proving test: `crates/eg-plan/src/retrieval_stop.rs::tests::uncalibrated_round_refuses`.
- [ ] **EG-FEDERATED-QUERY-R082.5:** Evidence. Each `StopVerdict` must be recorded as a premise on the retrieval `DecisionRecord`. The premise lists `round`, `margin`, `margin_threshold` and `reason`. The stop reason is returned to the caller. Proving test: `crates/eg-plan/src/retrieval_stop.rs::tests::stop_premise_lists_margin_and_reason`.
- [ ] **EG-FEDERATED-QUERY-R082.6:** Hot-path proof. The entry point is the served cross-modal query operation. The test sends three rounds of history in which the answer repeats with a margin above the threshold. The call passes through `execute` at `crates/eg-plan/src/exec.rs:536`. It observes `stop` true with reason `answer_repeated_margin_met`. Proving test: `tests/retrieval/test_stop_rule_served.py::test_repeat_with_margin_stops`.
- [ ] **EG-FEDERATED-QUERY-R083:** Arbitration between model prior and evidence.
- [ ] **EG-FEDERATED-QUERY-R083.1:** Model. Add `Candidate{answer, confidence, evidence_class}` and `ArbitrationVerdict{choice, margin, prior, retrieved}` in `crates/eg-types/src/retrieval_arbitration.rs`. `evidence_class` is closed: `parametric`, `retrieved`. `choice` is `Prior`, `Retrieved` or `Abstain`. Both candidates ride on every verdict. Search found no such type. Proving test: `crates/eg-types/src/retrieval_arbitration.rs::tests::verdict_carries_both_candidates` [arXiv:2606.08397].
- [ ] **EG-FEDERATED-QUERY-R083.2:** Behavior. Add `arbitrate(prior, retrieved, margin) -> ArbitrationVerdict`. The default `margin` is a named replaceable value of 0.10. A candidate wins when its confidence exceeds the other by more than `margin`. Otherwise the choice is `Abstain`. The rule has no learned part. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::margin_picks_or_abstains` [arXiv:2606.08397].
- [ ] **EG-FEDERATED-QUERY-R083.3:** Wiring. `arbitrate` attaches at the end of `execute` in `crates/eg-plan/src/exec.rs:536`. The harness runs the prior probe, the answer without retrieval, and sends it as a `Candidate` in the query call. EG scores the evidence side from the same call. The production caller of `execute` is `crates/eg-graphql/src/crossmodal.rs:429`. This row creates the call site. No production caller of an arbitration exists today. Proving test: `crates/eg-plan/src/exec.rs::tests::execute_returns_arbitration_for_prior_candidate`.
- [ ] **EG-FEDERATED-QUERY-R083.4:** Refusal. A verdict that abstains must return the typed code `ARBITRATION_ABSTAIN` with both candidates and no chosen answer. A call whose prior candidate lacks `evidence_class` must refuse with `CANDIDATE_CLASS_REQUIRED`. The typed abstention follows the pattern at `crates/eg-compute/src/assemble/conclude.rs:69`. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::abstain_returns_both_candidates`.
- [ ] **EG-FEDERATED-QUERY-R083.5:** Evidence. Each verdict must be recorded as a premise on the retrieval `DecisionRecord`. The premise lists both candidates, their `evidence_class`, the margin and the choice. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::arbitration_premise_lists_both_classes`.
- [ ] **EG-FEDERATED-QUERY-R083.6:** Hot-path proof. The entry point is the served cross-modal query operation. The test sends a prior candidate and a retrieved set whose confidence gap is below the margin. The call passes through `execute` at `crates/eg-plan/src/exec.rs:536`. It observes `ARBITRATION_ABSTAIN` with both candidates. Proving test: `tests/retrieval/test_arbitration_served.py::test_small_gap_abstains_with_both`.
