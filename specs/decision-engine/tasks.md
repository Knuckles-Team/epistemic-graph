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

- [x] D17 **EG-DECISION-ENGINE-R106:** Add `every_standard_shape_has_a_conforming_template` to `src/server/graph_schema/swarm_topology_tests.rs`, proving a minimal template for each of the six standard shapes (single-agent, pipeline, fan-out/join, supervisor-to-workers, critique-loop, council) validates against the `core:swarm-topology-shapes@1` SHACL shapes landed for EG-DECISION-ENGINE-R105. No new Rust topology-contract type was needed; the gap was test coverage of the existing vocabulary and shapes, not missing fact-carrying capacity.

## Evidence format

For each stable ID, record: `ID | sequence | delivery | acceptance | main SHA | tests/workflow | reviewed date | notes`. Until a row has an exact commit and passing required tests, use `WAITING / NOT_RUN`, `IN_PROGRESS / PARTIAL`, or `SOURCE_LANDED / PARTIAL` as observed; do not infer acceptance from a sequence label. `RETIRED` requires the superseding contract and removal test. Keep evidence records in this repository alongside the spec so external contributors can audit them.

## Initial known evidence and unresolved qualification

Existing source modules and tests provide foundations, but this spec does not assign new `ACCEPTED` states from historical branch notes. The first implementation task must inspect current published main and fill the evidence table. The decision ladder may already be source landed in part; served, hosted, replay and security proofs remain separate checks.
