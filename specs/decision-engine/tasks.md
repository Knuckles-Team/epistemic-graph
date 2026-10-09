# Delivery tasks and evidence record

## Task list

- [ ] D01 Audit current decision and pack public DTOs, method descriptors, generated clients and versioned digests; record exact main commit and gaps. Test A1–A2.
- [ ] D02 Finish deterministic solver/certificate verifier and explainable abstention; compare with exhaustive oracle. Test B.
- [ ] D03 Enforce visibility-before-features and monotone policy order for every candidate source. Test C.
- [ ] D04 Make `DecisionCommit` re-derive, verify and CAS the catalog in one transaction; preserve idempotent retry and synthesis evidence. Test A3–A5, D.
- [ ] D05 Verify decision-facing pack claims, reserved kinds, withdrawal and projection readiness against published component revisions. Test E.
- [ ] D06 Complete statistical feature schema, calibration, off-policy diagnostics, outcome admission and receipt-bound head promotion. Test C–D.
- [ ] D07 Add typed question adapters and per-question negative side-effect tests. Test D.
- [ ] D08 Run focused tests and quality gates on exact head, then hosted baseline CI with disposable fixtures. Test H.
- [ ] D09 Record each covered ID's `SOURCE_LANDED` main commit and `ACCEPTED` evidence independently; leave unverified IDs open.
- [ ] D10 **EG-DECISION-ENGINE-R118:** Bind posterior/reliability kernels to one independently labeled, RLS-filtered materialization; make policy, ranking and UQL read it, and prove sparse/self-label refusals.
- [ ] D11 **EG-DECISION-ENGINE-R119:** Finish shared sealed replay, fold checkpoints, bitemporal no-look-ahead, trial-count and incumbent statistics; exercise typed text and served job routes.
- [ ] D12 Close the component-schema and ingestion-side requirements no task above names: **EG-DECISION-ENGINE-R077–EG-DECISION-ENGINE-R081** (shared component schema bump, contract freeze reissue, embedding admission classifier, ingestion cost ladder, confidence-weighted community detection); see `requirements.md` for each ID's definition.

- [ ] D13 **EG-DECISION-ENGINE-R126, EG-DECISION-ENGINE-R127:** Add the coverage and guardrail read queries beside `AgentAssemble`, generate their client methods, and run test D2. The agent-utilities planner (AU-CONTROL-R027) consumes them through its capability-search and guardrail ports.

- [x] D14 **EG-DECISION-ENGINE-R017:** Add a regression test for the existing `FORBIDDEN_COMPONENT_KIND` refusal (`validate_publish` in `crates/eg-types/src/agent_component.rs`), exercised through both the bare validator and the public `AgentComponentOp::Publish.validate()` surface, so the previously untested bypass path is covered. Test: `crates/eg-types/src/agent_component.rs::tests::a_decision_record_cannot_be_published_directly`.

- [x] D15 **EG-DECISION-ENGINE-R030 (first slice):** Add the typed `IngestionLane` registry (fast/medium/slow) and `IngestionLaneRequest::check()` refusal for an unregistered lane id (`crates/eg-types/src/decision/statistical/ingestion_lane.rs`). `QuestionKind::IngestionLane` candidates still route through the ordinary generic `Decide`/declared-candidate executor (no kind-specific scoring path). Remaining for this requirement: a real ingestion call site that builds `CandidateSource::Declared` options from this registry and calls `Decide`, plus an integration test selecting the expected lane for a representative input (the requirement's acceptance test).

- [x] D16 **EG-DECISION-ENGINE-R029 (first slice):** Add the typed `RetrievalPlanKind` registry (leanrag/reciprocal-rank-fusion/direct-sparql) and `RetrievalPlanRequest::check()` refusal for an unregistered plan id (`crates/eg-types/src/decision/statistical/retrieval_plan.rs`). `QuestionKind::RetrievalPlan` candidates still route through the ordinary generic `Decide`/declared-candidate executor (no kind-specific comparison path).

- [x] D16b **EG-DECISION-ENGINE-R029 (wiring):** Wire the registry into the real `Decide` dispatch chokepoint: `candidates::declared_candidates` now refuses an option id that claims the registry's reserved `retrieval-plan:` namespace (`RetrievalPlanKind::claims_registry_namespace`) but does not name a registered plan, gated on `QuestionKind::RetrievalPlan`, before any feature schema is resolved. The check is namespace-scoped rather than exhaustive like R030's: `QuestionKind::RetrievalPlan` is reused across the existing decide-consumer test suite (`declared_abstention`'s "plan-deep"/"plan-hyde" fixtures, reused by `reputation_tests.rs`/`retrieval_tests.rs`/`evaluator_tests.rs`) for ordinary declared-option mechanics unrelated to plan selection; gating on the whole option-id space would have refused all of them. Dispatch-level regression test: `src/server/handlers/decide/stat_tests/consumer_tests.rs::an_unregistered_retrieval_plan_is_refused_through_decide`. Remaining for this requirement: a real retrieval-dispatch call site (selecting among `RetrievalPathTemplate`s, `retrieval.rs`) that builds registry options for a request and calls `Decide`, plus an integration test selecting the expected plan for a representative request shape under a published feature schema/head (the requirement's acceptance test; needs ranking, not just refusal).

## Evidence format

For each stable ID, record: `ID | sequence | delivery | acceptance | main SHA | tests/workflow | reviewed date | notes`. Until a row has an exact commit and passing required tests, use `WAITING / NOT_RUN`, `IN_PROGRESS / PARTIAL`, or `SOURCE_LANDED / PARTIAL` as observed; do not infer acceptance from a sequence label. `RETIRED` requires the superseding contract and removal test. Keep evidence records in this repository alongside the spec so external contributors can audit them.

## Initial known evidence and unresolved qualification

Existing source modules and tests provide foundations, but this spec does not assign new `ACCEPTED` states from historical branch notes. The first implementation task must inspect current published main and fill the evidence table. The decision ladder may already be source landed in part; served, hosted, replay and security proofs remain separate checks.
