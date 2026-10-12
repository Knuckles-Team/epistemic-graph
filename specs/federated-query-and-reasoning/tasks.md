# Delivery tasks

Check boxes describe work remaining for this combined contract. They do not certify acceptance. Assign one or more stable IDs to each PR. Record implementation details and acceptance evidence before closing each task.

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
- [ ] Verify generated delivery records against public merged commits and hosted CI. Require the stated proof for every acceptance claim.
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
  - [ ] **EG-FEDERATED-QUERY-R079:** Iceberg v2 catalog and manifest interoperability (rollup).
  - [ ] **EG-FEDERATED-QUERY-R079.1:** Model. Add version-bound `ManifestPublication` and complete schema-change kinds.
  - [ ] **EG-FEDERATED-QUERY-R079.2:** Behavior. Publish durable v2 manifests and preserve retained snapshot/schema compatibility.
  - [ ] **EG-FEDERATED-QUERY-R079.3:** Wiring. Bind the production graph-to-lake publisher and serve committed catalog metadata.
  - [ ] **EG-FEDERATED-QUERY-R079.4:** Refusal. Reject invalid publication, unapproved destructive evolution and projected mutation.
  - [ ] **EG-FEDERATED-QUERY-R079.5:** Evidence. Correlate graph version, schema, snapshot and artifact digests with reader acceptance.
  - [ ] **EG-FEDERATED-QUERY-R079.6:** Hot-path proof. Exercise served mutations and REST reads with an independent Iceberg reader.
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
- [ ] **EG-FEDERATED-QUERY-R082:** **Iterative retrieval stop rule with calibrated confidence and bounded termination (rollup).** EG applies one precedence rule for invalid feedback, exhausted limits, measured retrieval progress and calibrated confidence. Only the confidence branch can claim `answer_repeated_margin_met`. No component is learned. Children `.1` to `.6` deliver it. [arXiv:2606.13814]
- [ ] **EG-FEDERATED-QUERY-R082.1:** **Model.** Add `CalibrationTable`, `RoundCalibration`, `StopContext` and `StopVerdict` in `crates/eg-types/src/retrieval_stop.rs`. Calibration rows hold round, baseline confidence and margin threshold. Context carries validated limits, completed-round progress and policy revision. Verdict carries stop, reason, round, optional margin/threshold, optional calibration refusal and exhausted dimension. Closed reasons: `round_limit`, `budget_exhausted`, `no_new_evidence`, `quality_saturated`, `answer_repeated_margin_met`, `continue_uncalibrated`, `continue_answer_changed`, `continue_margin_low`. Missing calibration retains `ROUND_NOT_CALIBRATED` and absent margin/threshold.
- [ ] **EG-FEDERATED-QUERY-R082.2:** **Behavior.** Add `decide_stop(table, history, context) -> StopVerdict`. After feedback validation, precedence is round limit, other exhausted budget, no new evidence, declared quality saturation, then calibrated confidence. A confidence stop requires repeated answers and `confidence - baseline_confidence > margin_threshold`. Missing calibration selects `continue_uncalibrated` only if no earlier terminal condition applies. Other continuations use answer-changed or low-margin reasons. Stop alone does not assert R081 sufficiency.
- [ ] **EG-FEDERATED-QUERY-R082.3:** **Wiring.** `decide_stop` attaches beside `judge_sufficiency` at `crates/eg-plan/src/exec.rs:536`, reached through `crates/eg-graphql/src/crossmodal.rs:429`. Apply continuation validation and limit checks before dispatch; evaluate completed-round progress before issuing another continuation. EG derives context from validated evidence and counters. This row creates the call site. The harness cannot bypass a terminal verdict.
- [ ] **EG-FEDERATED-QUERY-R082.4:** **Refusal.** A missing calibration row prohibits a confidence-based early stop and records `ROUND_NOT_CALIBRATED` with the round. It never overrides an exhausted limit, no new evidence or declared saturation. Such terminal outcomes retain the calibration refusal; only a valid progressing round with remaining budgets may continue. Exhausted limits return `RETRIEVAL_BUDGET_EXCEEDED` with the terminal verdict. Invalid feedback refuses before continuation.
- [ ] **EG-FEDERATED-QUERY-R082.5:** **Evidence.** Record each `StopVerdict` as a retrieval `DecisionRecord` premise. Include round, reason, optional margin/threshold, calibration refusal, policy revision, completed-round progress and exhausted dimension. Return the same typed outcome to the caller. Retain R081 insufficiency where present.
- [ ] **EG-FEDERATED-QUERY-R082.6:** **Hot-path proof.** Entry point: the served cross-modal query through `execute` at `crates/eg-plan/src/exec.rs:536`. `tests/retrieval/test_stop_rule_served.py::test_repeat_with_margin_stops` proves the calibrated branch. `tests/retrieval/test_feedback_served.py::test_uncalibrated_continuation_precedence` sends real successive continuations through production retrieval and inference operators with pinned artifacts.
- [ ] **EG-FEDERATED-QUERY-R083:** Arbitration between model prior and evidence.
- [ ] **EG-FEDERATED-QUERY-R083.1:** Model. Add `Candidate{answer, confidence, evidence_class}` and `ArbitrationVerdict{choice, margin, prior, retrieved}` in `crates/eg-types/src/retrieval_arbitration.rs`. `evidence_class` is closed: `parametric`, `retrieved`. `choice` is `Prior`, `Retrieved` or `Abstain`. Both candidates ride on every verdict. Search found no such type. Proving test: `crates/eg-types/src/retrieval_arbitration.rs::tests::verdict_carries_both_candidates` [arXiv:2606.08397].
- [ ] **EG-FEDERATED-QUERY-R083.2:** Behavior. Add `arbitrate(prior, retrieved, margin) -> ArbitrationVerdict`. The default `margin` is a named replaceable value of 0.10. A candidate wins when its confidence exceeds the other by more than `margin`. Otherwise the choice is `Abstain`. The rule has no learned part. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::margin_picks_or_abstains` [arXiv:2606.08397].
- [ ] **EG-FEDERATED-QUERY-R083.3:** Wiring. `arbitrate` attaches at the end of `execute` in `crates/eg-plan/src/exec.rs:536`. The harness runs the prior probe, the answer without retrieval, and sends it as a `Candidate` in the query call. EG scores the evidence side from the same call. The production caller of `execute` is `crates/eg-graphql/src/crossmodal.rs:429`. This row creates the call site. No production caller of an arbitration exists today. Proving test: `crates/eg-plan/src/exec.rs::tests::execute_returns_arbitration_for_prior_candidate`.
- [ ] **EG-FEDERATED-QUERY-R083.4:** Refusal. A verdict that abstains must return the typed code `ARBITRATION_ABSTAIN` with both candidates and no chosen answer. A call whose prior candidate lacks `evidence_class` must refuse with `CANDIDATE_CLASS_REQUIRED`. The typed abstention follows the pattern at `crates/eg-compute/src/assemble/conclude.rs:69`. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::abstain_returns_both_candidates`.
- [ ] **EG-FEDERATED-QUERY-R083.5:** Evidence. Each verdict must be recorded as a premise on the retrieval `DecisionRecord`. The premise lists both candidates, their `evidence_class`, the margin and the choice. Proving test: `crates/eg-plan/src/retrieval_arbitration.rs::tests::arbitration_premise_lists_both_classes`.
- [ ] **EG-FEDERATED-QUERY-R083.6:** Hot-path proof. The entry point is the served cross-modal query operation. The test sends a prior candidate and an evidence set whose confidence gap is below the margin. The call passes through `execute` at `crates/eg-plan/src/exec.rs:536`. It observes `ARBITRATION_ABSTAIN` with both candidates. Proving test: `tests/retrieval/test_arbitration_served.py::test_small_gap_abstains_with_both`.

## R081 retrieval synthesis extensions

- [ ] **EG-FEDERATED-QUERY-R081.7:** **Adaptive dual-score chunk selection (rollup).** Extend R081 with children `.1` through `.6`. EG owns retrieval decisions and evidence; applications consume or execute its bounded decisions.
- [ ] **EG-FEDERATED-QUERY-R081.7.1:** **Model.** Define `ChunkScorePair`, `SelectionPolicy` and `SelectionReceipt` in `crates/eg-types/src/retrieval_verdict.rs`. Bind chunk IDs, corpus revision, embedding and cross-encoder scores, model revisions, normalization, thresholds, fusion weights and maximum count. Carry an activation benchmark digest.
- [ ] **EG-FEDERATED-QUERY-R081.7.2:** **Behavior.** Use already computed embedding and cross-encoder scores to classify four score regions. Retain dual-high candidates; reject dual-low candidates. Rescue low-embedding/high-relevance candidates, including vocabulary-mismatch results. Apply the declared fusion threshold to the remaining region. Compute query-dependent count, capped by `max_chunks`, with stable ID tie-breaking. Selection itself performs no model inference.
- [ ] **EG-FEDERATED-QUERY-R081.7.3:** **Wiring.** Add the selection operator to `crates/eg-plan/src/exec.rs:536` after candidate cross-encoder scoring and before final truncation. Retain per-chunk score pairs through `fuse_rrf` at line 848; rank-fusion output alone is insufficient. Its production caller is `crates/eg-graphql/src/crossmodal.rs:429`. Cross-encoder binding and this selector remain missing production work.
- [ ] **EG-FEDERATED-QUERY-R081.7.4:** **Refusal.** Return `RETRIEVAL_SCORE_PAIR_REQUIRED` for missing or stale scores and `RETRIEVAL_NORMALIZATION_INVALID` for undefined normalization. Explicitly define empty and constant-score normalization in the policy. Return `RETRIEVAL_POLICY_UNVERIFIED` when activation lacks the matching benchmark receipt. Reject dual-low chunks even when fewer than the requested count remain.
- [ ] **EG-FEDERATED-QUERY-R081.7.5:** **Evidence.** Record input scores, normalization, region, fused score, selected IDs, exclusions, count and policy digest. Count embedding, reranker and selection inference calls separately. Benchmark against a declared fixed-count baseline using identical candidates, models, corpus and tenant predicates. Activation requires zero extra selection inference and declared quality/cost acceptance.
- [ ] **EG-FEDERATED-QUERY-R081.7.6:** **Hot-path proof.** Entry point: the authenticated served cross-modal query. `tests/retrieval/test_adaptive_selection_served.py` uses a deterministic corpus and pinned production embedding/reranker artifacts. Compare adaptive and fixed-count results through the actual execution path.
- [ ] **EG-FEDERATED-QUERY-R081.8:** **Bounded retrieval feedback and critique (rollup).** Extend R081 with children `.1` through `.6`. EG owns retrieval decisions and evidence; applications consume or execute its bounded decisions.
- [ ] **EG-FEDERATED-QUERY-R081.8.1:** **Model.** Extend `crates/eg-types/src/retrieval_verdict.rs` with `RetrievalFeedback`, `PerspectiveQuery`, `TermFeedback` and `RetrievalCritique`. Bind original query, round, corpus/model revisions, document IDs, measured relevance, term weights and critique provenance. Feedback tiers are `Reinforce`, `Missing`, `Suppress`. Carry round, query, term, token and inference budgets. Use R082 `StopContext` and `StopVerdict` for termination; do not add a second stop authority.
- [ ] **EG-FEDERATED-QUERY-R081.8.2:** **Behavior.** Generate bounded diverse semantic-perspective expansions from the original query. Assess returned documents against that original query, then reinforce useful terms, record missing aspects and suppress misleading terms. Use the latest accepted expansion and revision-bound feedback for the next round. Critique may propose a reformulation but never replace measured relevance.
- [ ] **EG-FEDERATED-QUERY-R081.8.3:** **Wiring.** Add the feedback operator beside R081 sufficiency evaluation in `crates/eg-plan/src/exec.rs:536`. The caller remains `crates/eg-graphql/src/crossmodal.rs:429`. EG validates continuation evidence and budgets, then applies R082 precedence before dispatching any next query. The harness only executes an authorized bounded continuation. This row creates the missing feedback call site.
- [ ] **EG-FEDERATED-QUERY-R081.8.4:** **Refusal.** Return `RETRIEVAL_FEEDBACK_INVALID` for forged documents, incompatible revisions or evidence outside tenant scope. R082 checks round and other budget limits before progress or confidence stops. Exhaustion returns `RETRIEVAL_BUDGET_EXCEEDED` with its terminal verdict and dimension. Valid completed-round evidence can stop with `no_new_evidence` or `quality_saturated` even without calibration. Preserve `ROUND_NOT_CALIBRATED` and never promote such termination to calibrated sufficiency. Reject critiques that alter scope, scores, limits or activation policy.
- [ ] **EG-FEDERATED-QUERY-R081.8.5:** **Evidence.** Record expansion text, semantic perspective, term reinforcement/suppression, document tiers, critique text and applied or rejected proposal. Bind each item to measured relevance, original query, round, policy and corpus revision. Record R082 reason, calibration refusal, exhausted dimension, progress evidence and inference/token costs. Treat critique text as untrusted data.
- [ ] **EG-FEDERATED-QUERY-R081.8.6:** **Hot-path proof.** Entry point: the authenticated served cross-modal query with successive EG continuations. `tests/retrieval/test_feedback_served.py` reads a deterministic corpus through production operators and pinned inference artifacts. Its uncalibrated continuation matrix exercises R082 precedence and observes production dispatch counters.
- [ ] **EG-FEDERATED-QUERY-R081.9:** **Bounded typed hierarchy retrieval (rollup).** Children `.1` through `.6` retain community-to-entity retrieval. Child `.7` adds distinct document-section retrieval. EG owns retrieval decisions and evidence; applications consume or execute its bounded decisions.
- [ ] **EG-FEDERATED-QUERY-R081.9.1:** **Model.** Add `HierarchyRetrievalPolicy` and `HierarchyStageReceipt` to `crates/eg-types/src/retrieval_verdict.rs`. Carry hierarchy revision, community/summary/entity IDs, parent links, stage, score provenance, beam width and depth. Bound visited communities, entity candidates, reranker calls, tokens and total cost.
- [ ] **EG-FEDERATED-QUERY-R081.9.2:** **Behavior.** Read community summaries, refine selected communities, then descend to entity evidence. Rerank at each declared stage and prune with the declared beam and stable ID tie-breaks. Deduplicate entities and retain their community paths. Apply tenant predicates before every stage and candidate truncation.
- [ ] **EG-FEDERATED-QUERY-R081.9.3:** **Wiring.** Attach the hierarchy operator before terminal fusion in `crates/eg-plan/src/exec.rs:536`. Bind graph community summaries and entity reads through `PlanCtx`; the served caller is `crates/eg-graphql/src/crossmodal.rs:429`. Reuse `fuse_rrf` only for its existing rank-combination role. Revision-aware descent and stage reranking remain missing production calls.
- [ ] **EG-FEDERATED-QUERY-R081.9.4:** **Refusal.** Return `RETRIEVAL_HIERARCHY_INVALID` for cycles, stale summaries or dangling parents/entities. Exhausted breadth, depth, scoring or cost bounds return `RETRIEVAL_BUDGET_EXCEEDED` with completed-stage evidence. Never silently replace failed descent with unbounded global retrieval.
- [ ] **EG-FEDERATED-QUERY-R081.9.5:** **Evidence.** Record each visited community, pruning decision, entity path, stage score, reranker invocation and cumulative cost. Bind summary generation and graph/corpus revisions to the receipt. Compare quality and measured work against a declared flat-retrieval baseline.
- [ ] **EG-FEDERATED-QUERY-R081.9.6:** **Hot-path proof.** Entry point: the authenticated served cross-modal query. `tests/retrieval/test_hierarchy_served.py` loads two tenant hierarchies with deterministic summaries and entity evidence. It uses production graph reads and scoring.

## Retrieval policy extensions

- [ ] **EG-FEDERATED-QUERY-R039.1:** Shared multimodal space and missing-embedding shortfall (rollup).
  - [ ] **EG-FEDERATED-QUERY-R039.1.1:** **Model.** The engine must extend EmbeddingSpaceRef in `crates/eg-types/src/embedding.rs:47` with a modality contract. It must declare text, image, audio and video bindings within one pinned coordinate space.
  - [ ] **EG-FEDERATED-QUERY-R039.1.2:** **Behavior.** The planner must rank each query modality against every target modality using the same compatible space. The named compatible_space_only policy must preserve R039 candidate replenishment and R040 exclusions.
  - [ ] **EG-FEDERATED-QUERY-R039.1.3:** **Wiring.** The server must extend run_unified_bind_embedder at `src/server/handlers/query/planning.rs:611`, called by run_unified_with at `src/server/handlers/query/planning.rs:424`. It must bind approved multimodal encoders before rank_top at `crates/eg-plan/src/exec/vector_rank.rs:65`. No production caller today supplies this complete multimodal contract.
  - [ ] **EG-FEDERATED-QUERY-R039.1.4:** **Refusal.** The planner must return missing_embedding shortfall when eligible target items lack embeddings. It must report requested, returned and authorized missing counts without padding. Incompatible spaces and unavailable query encoders must refuse before ranking.
  - [ ] **EG-FEDERATED-QUERY-R039.1.5:** **Evidence.** The retrieval receipt must record modality direction, space identity, model revisions, index generation and authorized shortfall counts. It must identify every actual encoding call.
  - [ ] **EG-FEDERATED-QUERY-R039.1.6:** **Hot-path proof.** Entry point: authenticated UnifiedQuery. `tests/retrieval/test_multimodal_served.py` must use production encoders and persistent indexes for all sixteen pairs.
- [ ] **EG-FEDERATED-QUERY-R040.1:** Hard entity and relationship constraints (rollup).
  - [ ] **EG-FEDERATED-QUERY-R040.1.1:** **Model.** The engine must add RetrievalConstraints beside Pred at `crates/eg-types/src/wire_query_core.rs:27`. It must bind typed entity predicates, directed relationship predicates, quantifiers and graph revision.
  - [ ] **EG-FEDERATED-QUERY-R040.1.2:** **Behavior.** The constraint compiler must apply every declared constraint before ranking or truncation. The named require_all policy must intersect authorized entity and relationship matches. Contradictory constraints must yield an empty eligible set.
  - [ ] **EG-FEDERATED-QUERY-R040.1.3:** **Wiring.** The planner must attach constraint compilation at `crates/eg-plan/src/exec.rs:536` before rank_limit at `crates/eg-plan/src/exec/vector_rank.rs:56`. Its caller is execute_rows at `src/server/handlers/query/planning.rs:411`. Generic filters exist; no production caller today compiles this declared constraint contract.
  - [ ] **EG-FEDERATED-QUERY-R040.1.4:** **Refusal.** The planner must refuse invalid predicates, stale graph revisions and unevaluable required constraints. It must never replace hard exclusion with a score penalty. An unsatisfiable valid conjunction must return R039 typed shortfall.
  - [ ] **EG-FEDERATED-QUERY-R040.1.5:** **Evidence.** The trace must record constraint digest, graph revision, authorized exclusion counts and per-predicate outcomes. It must retain the same constraint binding across replenishment and fusion.
  - [ ] **EG-FEDERATED-QUERY-R040.1.6:** **Hot-path proof.** Entry point: authenticated UnifiedQuery. `tests/retrieval/test_constraints_served.py` must traverse real entity relationships before production vector and text ranking.
- [ ] **EG-FEDERATED-QUERY-R081.9.7:** Vector-free document-section traversal (rollup).
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.1:** **Model.** The engine must extend HierarchyRetrievalPolicy in `crates/eg-types/src/retrieval_verdict.rs` with distinct community and document_section kinds. Section nodes must carry document revision, parent, ordered children, granularity, summary and source span.
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.2:** **Behavior.** The planner must traverse document sections through bounded reasoning over authorized summaries and section contents. The named section_beam policy must declare breadth, depth, visits, tokens, inference calls and cost limits. It must require no vector storage.
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.3:** **Wiring.** The planner must add section traversal before fusion at `crates/eg-plan/src/exec.rs:536` through PlanCtx. run_unified_with at `src/server/handlers/query/planning.rs:424` must bind section reads and approved reasoning. No production caller today supplies this vector-free path.
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.4:** **Refusal.** The planner must reject cycles, dangling parents, stale summaries, invalid spans and unavailable reasoning. It must stop before exceeding any traversal limit. Partial paths must carry incomplete evidence.
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.5:** **Evidence.** HierarchyStageReceipt must retain ordered visits, full root-to-section paths, selected spans, pruned branches and bounded decision premises. It must bind document, summary, model and policy revisions with measured work.
  - [ ] **EG-FEDERATED-QUERY-R081.9.7.6:** **Hot-path proof.** Entry point: authenticated UnifiedQuery. `tests/retrieval/test_section_tree_served.py` must read real section records and invoke production reasoning without vector storage.
- [ ] **EG-FEDERATED-QUERY-R039.2:** Independent document-chunk encoding and aggregation [arXiv:2606.18781] (rollup).
  - [ ] **EG-FEDERATED-QUERY-R039.2.1:** **Model.** The engine must define DocumentEncodingPolicy in `crates/eg-types/src/embedding.rs` beside EmbeddingSpaceRef. It must pin tokenizer, chunk size, overlap, position mode, aggregation and model revision.
  - [ ] **EG-FEDERATED-QUERY-R039.2.2:** **Behavior.** The encoder must split token IDs into ordered windows without losing token identities. Each window must reset positions locally or preserve absolute document offsets according to policy.
  - [ ] **EG-FEDERATED-QUERY-R039.2.3:** **Behavior.** The encoder must process every chunk independently with the pinned model and no state from another chunk. It must leave query encoding unchanged.
  - [ ] **EG-FEDERATED-QUERY-R039.2.4:** **Behavior.** The named mean_chunks policy must average chunk vectors in ordinal order before one final declared normalization. Aggregation must be query-independent and produce exactly one document vector.
  - [ ] **EG-FEDERATED-QUERY-R039.2.5:** **Wiring.** The engine must validate chunk receipts at `src/server/handlers/semantic_index/worker.rs:175`, called by `src/server/handlers/semantic_index/dispatch.rs:14`. The governed ingestion worker must execute the approved encoding recipe before completing that generation. No production caller today validates this complete composition contract.
  - [ ] **EG-FEDERATED-QUERY-R039.2.6:** **Refusal.** The encoder must refuse unsupported position modes, incompatible vector dimensions, non-finite outputs and exhausted encoding budgets. Failed chunks must prevent aggregate publication. Empty input and zero-norm aggregates must return typed no_embedding outcomes.
  - [ ] **EG-FEDERATED-QUERY-R039.2.7:** **Evidence.** The encoding receipt must retain document digest, token ranges, position mode, per-chunk model revision and aggregate digest. It must bind policy, index generation and measured encoding cost.
  - [ ] **EG-FEDERATED-QUERY-R039.2.8:** **Hot-path proof.** Entry point: authenticated UnifiedQuery after governed ingestion. `tests/retrieval/test_chunk_encoding_served.py` must use actual model bindings and maintained indexes.
- [ ] **EG-FEDERATED-QUERY-R077.6:** Workload-aware column vectorization advice (rollup).
  - [ ] **EG-FEDERATED-QUERY-R077.6.1:** **Model.** The engine must add ColumnVectorAdvice beside EmbeddingBinding at `crates/eg-query/src/tables/embedding_binding.rs:398`. It must include table, column, schema revision, observed workload window, sample count, embedding eligibility and reason.
  - [ ] **EG-FEDERATED-QUERY-R077.6.2:** **Behavior.** The named workload_first policy must rank eligible text columns using observed text-search demand, average length and distinct ratio. It must use type admission before scoring and stable column identifiers for ties. Missing workload must produce insufficient_observations.
  - [ ] **EG-FEDERATED-QUERY-R077.6.3:** **Wiring.** The SQL handler must record resolved column usage at `src/server/handlers/query/sql_read.rs:379`, called by handle_sql_read at `src/server/handlers/query/sql_read.rs:100`. It must expose a bounded read-only advice relation through the existing authorized catalog. No production caller today supplies this advice contract.
  - [ ] **EG-FEDERATED-QUERY-R077.6.4:** **Refusal.** The advisor must refuse recommending any unembeddable type before scoring. It must reject stale schemas and unavailable model capability. Advice must never create a binding or queue embeddings.
  - [ ] **EG-FEDERATED-QUERY-R077.6.5:** **Evidence.** The advice receipt must record per-column workload counts, observation window, schema revision, policy revision and ranking contributions. It must omit raw query text and hidden columns.
  - [ ] **EG-FEDERATED-QUERY-R077.6.6:** **Hot-path proof.** Entry point: authenticated Sql. `tests/retrieval/test_column_advice_served.py` must issue real table queries followed by the planned advice relation.
- [ ] **EG-FEDERATED-QUERY-R040.2:** Topic and dense ranking fusion (rollup).
  - [ ] **EG-FEDERATED-QUERY-R040.2.1:** **Model.** The engine must add TopicBackend and TopicRankingPolicy beside PlanCtx in `crates/eg-plan/src/exec.rs:84`. Backend results must carry topic weights, centroid vectors, corpus revision and the retriever EmbeddingSpaceRef.
  - [ ] **EG-FEDERATED-QUERY-R040.2.2:** **Behavior.** The named weighted_topic_dense policy must combine normalized dense similarity and responsibility-weighted centroid similarity. It must use a bounded explicit weight and stable candidate identifiers for ties. Hard constraints must precede topic scoring.
  - [ ] **EG-FEDERATED-QUERY-R040.2.3:** **Wiring.** The planner must attach topic scoring before final truncation at `crates/eg-plan/src/exec.rs:536`. run_unified_with at `src/server/handlers/query/planning.rs:424` must bind the selected backend. Existing fuse_rrf at `crates/eg-plan/src/exec.rs:848` combines ranks; no production caller today supplies topic fusion.
  - [ ] **EG-FEDERATED-QUERY-R040.2.4:** **Refusal.** The planner must reject centroid dimensions, model revisions or coordinate spaces differing from the retriever. It must refuse non-finite weights, stale corpus revisions and unavailable backends. Equal dimensions alone must never establish compatibility.
  - [ ] **EG-FEDERATED-QUERY-R040.2.5:** **Evidence.** The receipt must record backend identity, centroid generation, space tuple, topic responsibilities, component scores and fusion policy. Empirical centroids must record their authorized source-vector generation.
  - [ ] **EG-FEDERATED-QUERY-R040.2.6:** **Hot-path proof.** Entry point: authenticated UnifiedQuery. `tests/retrieval/test_topic_fusion_served.py` must swap two actual topic backends over the same authorized corpus.
- [ ] **EG-FEDERATED-QUERY-R039.3:** **Behavior.** The planner must replenish only candidates satisfying R040.1 constraints. Constraint exhaustion must produce typed shortfall without relaxing any entity or relationship predicate.
- [ ] **EG-FEDERATED-QUERY-R040.3:** **Wiring.** The filtered index must consume only complete R039.2 aggregates through rank_top at `crates/eg-plan/src/exec/vector_rank.rs:65`. Its caller is execute at `crates/eg-plan/src/exec.rs:536`. It must reject stale encoding-policy generations before ranking.
- [ ] **EG-FEDERATED-QUERY-R039.4:** **Residual behavior: mixed multi-hop traversal and vector top-k.** A query must combine multi-hop graph traversal and vector similarity. It must replenish candidates until k results survive the traversal filter. Otherwise, it must return a typed shortfall naming the number of permitted matches. It must never return a silent undercount. Run the reference-corpus correctness oracle for exactly k permitted results or typed shortfall. Retain the S-01 two-hop/parallel-edge fixture and stable edge identity.
- [ ] **EG-FEDERATED-QUERY-R040.4:** **Residual behavior: filter-aware ANN and BM25 scans.** Filtered approximate-nearest-neighbor and BM25 text search must plan filter-aware index scans. These scans must consume pre-filtered candidate sets or bitmap filters. They must not scan the full unfiltered index and discard non-matching rows afterward. They must preserve the filtered-search correctness guarantee. Compare filtered ANN and BM25 latency before and after the scan change. Use a fixed corpus and run the correctness oracle.

## R084 bounded ShEx slices

Each child is one focused five-minute definition or proof slice. Split larger implementation work before delivery.

- [ ] **EG-FEDERATED-QUERY-R084:** Served ShEx validation (rollup). Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.1:** Model. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.2:** Behavior: data selection. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.3:** Behavior: supported constraints. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.4:** Behavior: semantic limits. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.5:** Wiring. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.6:** Refusal. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.7:** Scoped reads. Use the matching planned acceptance in test-spec.md.
- [ ] **EG-FEDERATED-QUERY-R084.8:** Evidence and hot-path proof. Use the matching planned acceptance in test-spec.md.

## R085 producer delivery

Preserve all previous acceptance and receipts. Deliver producer leaves before the SDK consumer. Split concrete implementation work further if a leaf exceeds five minutes.

- [ ] **EG-FEDERATED-QUERY-R085:** EG approves immutable source queries for bounded connector execution (rollup).. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.1:** Model.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.2:** Behavior: issue approval.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.3:** Behavior: verify approval.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.4:** Behavior: read-only SQL.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.5:** Wiring.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.6:** Behavior: bounded execution contract.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.7:** Refusal.. Fulfil the complete requirement and its named proving test.
- [ ] **EG-FEDERATED-QUERY-R085.8:** Evidence and hot-path proof.. Fulfil the complete requirement and its named proving test.

<!-- spec-definition-patch:owner-batch-20261012-federated-query-and-reasoning -->
## Reviewed residual definition tasks — owner batch 2026-10-12

- [ ] **EG-FEDERATED-QUERY-R070.1 (AU:71):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R070` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R070`, `GRAPHOS-OPS-R053.2`, `GRAPHOS-OPS-R053.3`, `GRAPHOS-OPS-R053.5`.
- [ ] **EG-FEDERATED-QUERY-R070.1 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend existing EG retrieval/source-access contracts with the explicit indexless operations, per-query cost/budget and typed abstention; bind the existing host/AU exploration consumer. Do not claim index scans satisfy no-index acceptance.
- [ ] **EG-FEDERATED-QUERY-R070.1 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R006.1 (EG:11):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R006` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R001`, `EG-FEDERATED-QUERY-R011`, `EG-DECISION-ENGINE-R003`, `EG-FEDERATED-QUERY-R006`.
- [ ] **EG-FEDERATED-QUERY-R006.1 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Add temporal rule semantics and origin-linked fact admission/readback under the existing owner.
- [ ] **EG-FEDERATED-QUERY-R006.1 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R040.5 (EG:20):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R040` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R007`, `EG-FEDERATED-QUERY-R040`, `EG-FEDERATED-QUERY-R040.4`, `EG-FEDERATED-QUERY-R008`.
- [ ] **EG-FEDERATED-QUERY-R040.5 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Add memory/relation semantics and scaling benchmark, retaining filtered-scan coverage.
- [ ] **EG-FEDERATED-QUERY-R040.5 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R040.6 (EG:21):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R040` using the reviewed producer/consumer route; preserve covered clauses `EG-INGEST-WORKER-R003.13`, `EG-INGEST-WORKER-R003.14`, `EG-INGEST-WORKER-R003.17`, `EG-FEDERATED-QUERY-R040`, `EG-INGEST-WORKER-R003.18`, `EG-INGEST-WORKER-R003.16`, `EG-FEDERATED-QUERY-R016`.
- [ ] **EG-FEDERATED-QUERY-R040.6 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Add taxonomy, episode formation and consumer semantics, reusing scoped transcript/search contracts.
- [ ] **EG-FEDERATED-QUERY-R040.6 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R083.7 (EG:22):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R083.1` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R083.1`, `EG-FEDERATED-QUERY-R083.2`, `EG-FEDERATED-QUERY-R083.3`, `EG-FEDERATED-QUERY-R083.4`, `EG-FEDERATED-QUERY-R083.6`.
- [ ] **EG-FEDERATED-QUERY-R083.7 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend existing R083 behavior/model/refusal acceptance with the support-margin inputs and invariant that unsupported candidates lose. Add a served counterexample with a higher-confidence unsupported candidate and an evidence-supported lower-confidence candidate. Retain current two-candidate, class, abstention and evidence obligations.
- [ ] **EG-FEDERATED-QUERY-R083.7 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R081.10 (EG:25):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R081.1` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R081.1`, `EG-FEDERATED-QUERY-R081.2`, `EG-FEDERATED-QUERY-R081.3`, `EG-FEDERATED-QUERY-R081.4`, `EG-FEDERATED-QUERY-R081.8.1`, `EG-FEDERATED-QUERY-R081.8.2`, `EG-FEDERATED-QUERY-R081.8.3`, `EG-FEDERATED-QUERY-R081.8.5`, `EG-FEDERATED-QUERY-R081.8.6`.
- [ ] **EG-FEDERATED-QUERY-R081.10 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Correct the mapping to include R081.8.1-.6 for existing follow-up-query definitions. Retain a narrower R081 residual for three-clock confusion detection and its bounded insufficient-context fallback. Add a fixture where storage/event/narrative dates disagree and verify the selected time basis and second-pass query.
- [ ] **EG-FEDERATED-QUERY-R081.10 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R028.1 (EG:29):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R028` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R028`, `EG-FEDERATED-QUERY-R045`.
- [ ] **EG-FEDERATED-QUERY-R028.1 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend existing R028 KnowledgeSet result definition with mandatory per-row provenance/policy-frame fields, plain-row compatibility and typed missing-field refusal. Bind structured and served query acceptance to the same carrier and independently test each absent field. Preserve the held-UQL restriction: this is a definition recommendation only, with no UQL implementation authorization.
- [ ] **EG-FEDERATED-QUERY-R028.1 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R005.1 (EG:31):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R005` using the reviewed producer/consumer route; preserve covered clauses `EG-DECISION-ENGINE-R034`, `EG-FEDERATED-QUERY-R001`, `EG-FEDERATED-QUERY-R005`, `EG-FEDERATED-QUERY-R011`, `EG-FEDERATED-QUERY-R012`.
- [ ] **EG-FEDERATED-QUERY-R005.1 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend the existing reasoning/validation owner with an enumerated violation matrix and typed diagnostics. Include positive controls and one independently failing fixture per requested class; preserve explicit authorization before any retraction. No held-UQL implementation is authorized.
- [ ] **EG-FEDERATED-QUERY-R005.1 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R078.7 (EG:59):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R078` using the reviewed producer/consumer route; preserve covered clauses `EG-DURABLE-KERNEL-R043`, `EG-FEDERATED-QUERY-R078`, `EG-UNIFIED-DATA-PLANE-R001`, `EG-UNIFIED-DATA-PLANE-R003`, `EG-FEDERATED-QUERY-R078.1`, `EG-FEDERATED-QUERY-R078.2`, `EG-FEDERATED-QUERY-R078.3`, `EG-FEDERATED-QUERY-R078.4`, `EG-FEDERATED-QUERY-R078.5`, `EG-FEDERATED-QUERY-R078.6`.
- [ ] **EG-FEDERATED-QUERY-R078.7 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: First link existing vocabulary/mapping IDs for each listed category; add only any remaining view/acceptance obligation. Test the complete logical view under two tenants and differing clearances without copying every attached source.
- [ ] **EG-FEDERATED-QUERY-R078.7 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R081.9.7.7 (GRAPHOS:24):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R081.9.7.1` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R081.9.7.1`, `EG-FEDERATED-QUERY-R081.9.7.2`, `EG-FEDERATED-QUERY-R081.9.7.3`, `EG-FEDERATED-QUERY-R081.9.7.4`, `EG-FEDERATED-QUERY-R081.9.7.5`, `EG-FEDERATED-QUERY-R081.9.7.6`.
- [ ] **EG-FEDERATED-QUERY-R081.9.7.7 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Map the hierarchy/granularity clauses to existing R081.9.7 children, preserve the source no-external-model constraint, and define only a compatible traversal mode/constraint and hot-path zero-external-call proof. Do not silently relax the source.
- [ ] **EG-FEDERATED-QUERY-R081.9.7.7 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R081.9.7.8 (GRAPHOS:25):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R081.9.7.1` using the reviewed producer/consumer route; preserve covered clauses `GRAPHOS-OPS-R053.1`, `GRAPHOS-OPS-R053.2`, `GRAPHOS-OPS-R053.3`, `GRAPHOS-OPS-R053.6`, `EG-FEDERATED-QUERY-R081.9.7.1`, `EG-FEDERATED-QUERY-R081.9.7.5`.
- [ ] **EG-FEDERATED-QUERY-R081.9.7.8 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Retain space/section mappings as partial coverage; define offline line-numbered TOC and bounded span-read contracts at the source/EG boundary, then link existing GraphOS/AU consumers. Do not recreate interaction spaces.
- [ ] **EG-FEDERATED-QUERY-R081.9.7.8 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-FEDERATED-QUERY-R040.2.7 (GRAPHOS:28):** Define and wire the EG-owned residual for `EG-FEDERATED-QUERY-R040.2.1` using the reviewed producer/consumer route; preserve covered clauses `EG-FEDERATED-QUERY-R040.2.1`, `EG-FEDERATED-QUERY-R040.2.2`, `EG-FEDERATED-QUERY-R040.2.3`, `EG-FEDERATED-QUERY-R040.2.6`.
- [ ] **EG-FEDERATED-QUERY-R040.2.7 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend the existing R040.2 model/acceptance text to explicitly bind each document/candidate to revision-bound topic metadata/responsibilities, and require a served fixture showing that changing that metadata changes the appropriate candidate topic contribution. Preserve all current fusion/refusal/tenant constraints. Then coordinator may map GRAPHOS:28 to those exact child clauses; do not create a second ranker or fabricate runtime credit.
- [ ] **EG-FEDERATED-QUERY-R040.2.7 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition tasks — 2026-10-12

- [ ] `EG-FEDERATED-QUERY-R081.8.100`: add the typed request/result, policy/revision binding, producer/consumer wiring contract, refusal cases and receipt/readback fields described in requirements; preserve `R081.8` and its prior children.
- [ ] `EG-FEDERATED-QUERY-R081.8.100`: add served acceptance for: Served feedback fixtures accept same-retriever reuse, require validation after retriever version changes, and refuse a failed, stale or missing transfer receipt before retrieval. Verify the strategy digest and target retriever are in the receipt, unchanged feedback budgets remain enforced, and no rejected critique changes active strategy.
- [ ] `EG-FEDERATED-QUERY-R053.100`: bind the property-key escaping rule to the actual Cypher query-construction path and add a focused build/CI regression test there; keep ForeignSourceSpec registration and Spark batch-only capability unchanged.
- [ ] `EG-FEDERATED-QUERY-R053.100` build gate: exercise generated Cypher for plain, reserved, spaced, Unicode and embedded-backtick property keys; parse safe outputs with the Cypher grammar and fail the build for an unquoted or malformed generated key. Do not add a served request, runtime receipt, or second renderer.
- [ ] `EG-FEDERATED-QUERY-R032.100`: add the typed request/result, policy/revision binding, producer/consumer wiring contract, refusal cases and receipt/readback fields described in requirements; preserve `R032` and its prior children.
- [ ] `EG-FEDERATED-QUERY-R032.100`: add served acceptance for: Served graph fixtures include ancestors and descendants across several layers, different historical failure counts, a cycle boundary and missing-history data. Verify both directions and deterministic layer/history contribution; refuse stale or malformed history, and compare existing R032 propagation outputs unchanged.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped definition tasks — 2026-10-12

- [ ] Define rollup `EG-FEDERATED-QUERY-R086` and generated child links; keep it SPECIFIED until all required children are implemented and verified.
- [ ] `EG-FEDERATED-QUERY-R086.100`: implement the typed fields, decision/refusal behavior and owner boundary stated in requirements; preserve `EG-FEDERATED-QUERY-R086`.
- [ ] `EG-FEDERATED-QUERY-R086.100` acceptance: Serve an ordered three-source fixture that returns a pinned hypothesis and verify candidate provenance and no graph revision change. Reorder source refs and assert the request digest changes. Refuse unary input, unresolved refs and stale model revision; query afterward to prove no predicted edge was asserted.
- [ ] `EG-FEDERATED-QUERY-R081.101`: implement the typed fields, decision/refusal behavior and owner boundary stated in requirements; preserve `EG-FEDERATED-QUERY-R081`.
- [ ] `EG-FEDERATED-QUERY-R081.101` acceptance: Build a state once, rerank under two query contexts and reversed candidate order, and assert identical state digest with correctly aligned per-document scores. Assert no decoder/model-generation call. Compare measured interaction cost to the same full-token baseline and refuse stale document/encoder revision, malformed segment coverage or a query-time state rewrite.
- [ ] `EG-FEDERATED-QUERY-R081.102`: implement the typed fields, decision/refusal behavior and owner boundary stated in requirements; preserve `EG-FEDERATED-QUERY-R081`.
- [ ] `EG-FEDERATED-QUERY-R081.102` acceptance: Distill and read back an artifact whose held-out quality meets threshold, then run served queries and assert zero model-service calls and scorer digest binding. Below-threshold, missing held-out evidence, stale corpus/index or teacher revision all refuse activation; verify the prior scorer remains active.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mapping-eg41-20261012 -->
## Mapped retrieval-selection task — 2026-10-12

- [ ] `EG-FEDERATED-QUERY-R081.100`: implement the joint objective, typed result fields, existing-bound enforcement and refusals specified in requirements.
- [ ] Acceptance: Through served retrieval, compare a tiny fixture against an independent exhaustive nonnegative-sparse oracle, including two highly correlated documents and an alternative diverse pair. Assert exact selection, coefficients, residual and deterministic order. Refuse embedding-space mismatch, invalid policy and non-convergence; verify existing tenant and result-count filters still apply.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tasks -->
## Cross-owner viewport query tasks — 2026-10-12

- [ ] `EG-FEDERATED-QUERY-R081.103`: define and wire the closed `ViewportQueryV1` and `ResolveViewportSelectionV1` union through existing UnifiedQuery/run_unified_with; serve billion-point scatter/line, pan/zoom, exact/bin selection and stale/denied/cap refusal cases. Verify effective encoded ceiling is the minimum of the caller request and formula cap; refuse missing, invalid and envelope-too-small requests before scanning.
- [ ] Implement separate scatter/line aggregation rules, continuity gaps, exact/aggregate selection and fixed viewport/variant bin plus serialized-byte formula.
- [ ] Serve pan/zoom/selection at billion-point cardinality; verify no full-source transfer and typed stale-index, authorization and cap refusals.
<!-- /spec-definition-patch -->
