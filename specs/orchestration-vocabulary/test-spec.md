# EG-ORCH-VOCAB — Test spec

Positive: each term resolves and the reasoner stays consistent; each Q1-Q7 query returns the fixture rows. Negative: one planted-invalid fixture per shape (cycle in `:precedes`, cycle in `swarm:feeds`, slot with a non-kind, member without capability, rule without class, agent without model). Integration: committed assembly is queryable through Q5 and Q7. All tests live in `crates/eg-core/tests/orchestration_vocabulary.rs` against `crates/eg-core/tests/fixtures/orchestration_vocabulary/`.

Assembly-kind acceptance: a verifier-constrained slot excludes an executor-only candidate, accepts a visible verifier, and yields typed abstention without one.

Harness run-trace acceptance: a valid trace per termination reason round-trips through the client and `AddNode`. The engine refuses a negative counter, an unknown reason, and a peak above budget without `context_overflow`. The engine refuses a measured fact without evaluation-run evidence. A newer measurement supersedes an older one, and the older one stays readable. Competency question: for model M on task class C, `harness_best_config` returns the configuration with the lowest cost per solved task. It returns its trace ids and abstains below the sample floor.
Spec-folder mapping: a fixture folder through `IndexRepository` yields one node of each kind. An unknown file is refused and stored as a plain document. Every node names its deciding rule.

### EG-ORCH-VOCAB-R013 tests

- `EG-ORCH-VOCAB-R013.1`: `crates/eg-types/src/outcome_bundle.rs::tests::step_record_round_trips`.
- `EG-ORCH-VOCAB-R013.2`: `crates/eg-types/src/outcome_bundle.rs::tests::step_digest_is_canonical`.
- `EG-ORCH-VOCAB-R013.3`: `src/server/handlers/graph_ops/gateway_graph.rs::tests::add_run_trace_checks_step_digests`.
- `EG-ORCH-VOCAB-R013.4`: `crates/eg-types/src/outcome_bundle.rs::tests::bad_digest_and_gap_refused`.
- `EG-ORCH-VOCAB-R013.5`: `crates/eg-types/src/outcome_bundle.rs::tests::stored_steps_read_in_order`.
- `EG-ORCH-VOCAB-R013.6`: `tests/orchestration/test_run_trace_steps_served.py::test_first_divergence_is_step_two`.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-vocabulary -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-ORCH-VOCAB-R009.18` / AU:26:** Extend the existing EG routing/continuation contract with the joint per-run observation window and adjustment receipt; bind AU observations and trace application. Preserve host ceiling and narrow-only enforcement. Producer/consumer path: AU ObservedCost routing.py -> EG routing; AU continuation.py narrows and releases capacity; GraphOS error_budget.decide computes partitioned AIMD. These separate paths have no reviewed joint per-run adjustment producer/consumer contract. Source review `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-ORCH-VOCAB-R013.7` / AU:115:** Add the query-output and provenance contract under existing owners; retain trace definitions. Producer/consumer path: EG repository hydration supplies source relationships; AU-HARNESS-R024 emits spans; EG-ORCH-VOCAB-R013 admits trace steps through AddNode. Explanation producer, artifact schema and WebUI consumption binding are absent definitions. Source review `445704f78ae07d1c4b62f42dd23797823d91bc470f04a9dc779e3f6777fffbf0`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-ORCH-VOCAB-R003.8` / EG:12:** Add these residuals under existing assembly/program/vocabulary owners. Producer/consumer path: graph.assemble consumes capabilities; Agent Library projects them; compile_program/lookup_program reuse traces. Team similarity selection and handler-metadata consumption are absent definitions. Source review `517823a931bb93e173b301a9a31fbe6bd75c14687233c72278a59ced1940673a`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-ORCH-VOCAB-R012.7` / EG:46:** Credit R012.1-.6 rather than treating policy-node ingestion as wholly absent. Define content-to-policy-rule synthesis and link its activation path to invariant checking and reasoning consumption. A contradictory repository policy must be refused before activation while preserving prior policy state. Producer/consumer path: IndexRepository -> classify_spec_file -> SpecPolicy; missing semantic synthesis/activation link -> reasoning policy consumer. Existing check_rule_against_invariants applies at the human-override promotion path. Source review `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-ORCH-VOCAB-R004.6` / EG:51:** Extend existing capability/orchestration vocabulary and its registry query contract with gap/enhancement types and task-class relations. Prove persisted create/read and task-class filtering; distinguish a request-local abstention from a durable catalog entry. Producer/consumer path: Assembly unmet-capability AbstainReason -> proposed governed capability-gap record -> task-class registry query. The latter producer/consumer link remains undefined. Source review `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.

- **`EG-ORCH-VOCAB-R012.8` / EG:61:** Bind reviewed file revision/digest to activated EG policy. Require no autonomous source overwrite and route requested source changes via approved SDK writeback; test rejected/stale review leaves file and active policy unchanged. Producer/consumer path: Versioned repository policy source -> GraphOS approved engine policy port -> EG active policy node; source mutation separately uses governed SDK effect. Source review `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Planned mapped acceptance — 2026-10-12

No test named here has run; names describe acceptance obligations, not evidence.

- **`EG-ORCH-VOCAB-R005.100` (AU:33; source `1a32741b0a97f0c31cd1a3df4ea31d6eee0069c62c3487c0d8057b1ced4b6cf4`):** Served parallel workflow has a secret undeclared nested field and a declared sibling field; worker receives only its declared projection, while coordinator can still synthesize typed outputs. Unknown paths, stale schema or unauthorized source fields refuse before dispatch. Preserve R020 typed result and schema-refusal behavior. Producer/consumer boundary: EG declares typed step projection/shield policy; AU `_dispatch_parallel_batch`/processor applies projection before tool dispatch. DTO extra-field rejection is not data projection. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.

- **`EG-ORCH-VOCAB-R009.100` (AU:60; source `5d53cbdda932456b27c6649360c3051d4ab09f2db42018550b4ee81c785748b4`):** Served read test exports a parent with multiple spawned agents and tool calls, retrieves all correlated records across pages, and verifies stable digest, redaction and completeness. Missing child, duplicate span, unauthorized read and truncated pagination are detected/refused. Do not assert MCP transport delivery from EG read tests. Producer/consumer boundary: AU default_runtime_capabilities/run_agent exports; GraphOS MCP transport carries export; EG contract supports correlated readback. Preserve R008/R024 trace ownership. Require exact source revision, input/output digests, caller and independent readback where specified. These cases are planned only; no implementation or receipt is asserted.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — cross-owner harness analytics

No test in this section has run; these are future acceptance requirements.

- `EG-ORCH-VOCAB-R009.101`: served `SyncHarnessSessionIndexV1` and `SearchHarnessSessionsV1` across two SDK-declared harnesses, repeat after restart, verify stable index/readback, partial-source reporting and typed conflict/refusal cases.
- `EG-ORCH-VOCAB-R010.100`: served `EvolutionTraceMetricsV1` joins two pinned traces to one proposal/prompt revision; independently query stored traces; missing trace is partial/unknown, stale or unauthorized refs refuse.
- `EG-ORCH-VOCAB-R010.101`: compare all four token categories and exact costs against an independent ledger for model/project/agent groups; preserve unknown costs; measure p95 ≤500 ms with 1,000,000 indexed traces, 32 concurrent readers and recorded cache/index policy.
<!-- /spec-definition-patch -->
