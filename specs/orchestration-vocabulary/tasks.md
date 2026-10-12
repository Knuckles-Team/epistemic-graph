# EG-ORCH-VOCAB — Implementation tasks

Governing spec: [spec.md](spec.md). Design: [design.md](plan.md).

Producer first: terms, then shapes, then projection call sites, then query tests. Rows within a rollup are independent 5-minute units unless the Verification names another.

- [ ] EG-ORCH-VOCAB-R001 Task steps (rollup)
- [ ] EG-ORCH-VOCAB-R001.1 Add class `:TaskStep`
- [ ] EG-ORCH-VOCAB-R001.2 Add property `:hasStep`
- [ ] EG-ORCH-VOCAB-R001.3 Add step-order acyclicity shape
- [ ] EG-ORCH-VOCAB-R001.4 Add Q1 fixture and query test
- [ ] EG-ORCH-VOCAB-R002 Agent kinds and staffing (rollup)
- [ ] EG-ORCH-VOCAB-R002.1 Add class `:AgentKind` and seed members
- [ ] EG-ORCH-VOCAB-R002.2 Add property `:hasAgentKind`
- [ ] EG-ORCH-VOCAB-R002.3 Add property `swarm:requiresAgentKind` and the import
- [ ] EG-ORCH-VOCAB-R002.4 Add slot-kind shape
- [ ] EG-ORCH-VOCAB-R002.5 Add Q2 query test
- [ ] EG-ORCH-VOCAB-R003 Agent Library, templates and A2A members (rollup)
- [ ] EG-ORCH-VOCAB-R003.1 Add class `:AgentTemplate`
- [ ] EG-ORCH-VOCAB-R003.2 Add class `:AgentLibrary`
- [ ] EG-ORCH-VOCAB-R003.3 Add property `:memberOfLibrary`
- [ ] EG-ORCH-VOCAB-R003.4 Add property `:instanceOfTemplate`
- [ ] EG-ORCH-VOCAB-R003.5 Add library-member shape
- [ ] EG-ORCH-VOCAB-R003.6 Wire the Agent Library projection to emit membership
- [ ] EG-ORCH-VOCAB-R003.7 Add Q3 query test
- [ ] EG-ORCH-VOCAB-R004 Best practices and guardrail scope (rollup)
- [ ] EG-ORCH-VOCAB-R004.1 Add class `:BestPractice`
- [ ] EG-ORCH-VOCAB-R004.2 Add property `:appliesToTaskClass`
- [ ] EG-ORCH-VOCAB-R004.3 Add rule-scope shape
- [ ] EG-ORCH-VOCAB-R004.4 Project `GuardrailRule` components
- [ ] EG-ORCH-VOCAB-R004.5 Add Q4 query test
- [ ] EG-ORCH-VOCAB-R005 Per-subtask bindings (rollup)
- [ ] EG-ORCH-VOCAB-R005.1 Add class `:ModelProfile`
- [ ] EG-ORCH-VOCAB-R005.2 Add property `:needsSkill`
- [ ] EG-ORCH-VOCAB-R005.3 Add property `:stepAssignedAgent`
- [ ] EG-ORCH-VOCAB-R005.4 Add binding shape
- [ ] EG-ORCH-VOCAB-R005.5 Project `model_profile` components
- [ ] EG-ORCH-VOCAB-R005.6 Add Q5 query test
- [ ] EG-ORCH-VOCAB-R006 Hand-off and agent-graph DAG (rollup)
- [ ] EG-ORCH-VOCAB-R006.1 Add property `:handsOffTo`
- [ ] EG-ORCH-VOCAB-R006.2 Add property `swarm:feeds`
- [ ] EG-ORCH-VOCAB-R006.3 Add property `swarm:loopsBackTo`
- [ ] EG-ORCH-VOCAB-R006.4 Add DAG acyclicity shape
- [ ] EG-ORCH-VOCAB-R006.5 Project template topology facts to `swarm:feeds`
- [ ] EG-ORCH-VOCAB-R006.6 Add Q6 and Q7 query tests
- [ ] EG-ORCH-VOCAB-R007 Assembly consumption (rollup)
- [ ] EG-ORCH-VOCAB-R007.1 Expand a task's steps into assembly tasks
- [ ] EG-ORCH-VOCAB-R007.2 Project the assembled graph back
- [ ] EG-ORCH-VOCAB-R007.3 Add the composed read
- [ ] EG-ORCH-VOCAB-R007.4 Enforce slot agent-kind constraints before assembly selection
- [ ] EG-ORCH-VOCAB-R008 Catalog registration and ontology gates (rollup)
- [ ] EG-ORCH-VOCAB-R008.1 Register `orchestration-v1.shapes.ttl` with the shape loader
- [ ] EG-ORCH-VOCAB-R008.2 Regenerate ontology artifacts
- [ ] EG-ORCH-VOCAB-R009 Harness run-trace vocabulary (rollup)
- [ ] EG-ORCH-VOCAB-R009.1 Add property `:terminationReason`
- [ ] EG-ORCH-VOCAB-R009.2 Add property `:peakContextTokens`
- [ ] EG-ORCH-VOCAB-R009.3 Add property `:contextBudgetTokens`
- [ ] EG-ORCH-VOCAB-R009.4 Add property `:elisionCount`
- [ ] EG-ORCH-VOCAB-R009.5 Add property `:summarizationCount`
- [ ] EG-ORCH-VOCAB-R009.6 Add property `:turnCount`
- [ ] EG-ORCH-VOCAB-R009.7 Add property `:toolCallCount`
- [ ] EG-ORCH-VOCAB-R009.8 Add property `:costUsd`
- [ ] EG-ORCH-VOCAB-R009.9 Add property `:appliedHarnessConfig`
- [ ] EG-ORCH-VOCAB-R009.10 Add property `:runModel`
- [ ] EG-ORCH-VOCAB-R009.11 Add property `:runTaskClass`
- [ ] EG-ORCH-VOCAB-R009.12 Add counter shape
- [ ] EG-ORCH-VOCAB-R009.13 Add context-budget shape
- [ ] EG-ORCH-VOCAB-R009.14 Add completed-trace shape
- [ ] EG-ORCH-VOCAB-R009.15 Refuse a shape-invalid RunTrace write
- [ ] EG-ORCH-VOCAB-R009.16 Fixture trace for each termination reason
- [ ] EG-ORCH-VOCAB-R009.17 Prove the ingestion hot path
- [ ] EG-ORCH-VOCAB-R010 Run-trace metric queries (rollup)
- [ ] EG-ORCH-VOCAB-R010.1 Query success rate
- [ ] EG-ORCH-VOCAB-R010.2 Query cost per solved task
- [ ] EG-ORCH-VOCAB-R010.3 Query overflow rate
- [ ] EG-ORCH-VOCAB-R010.4 Query peak-to-budget ratio
- [ ] EG-ORCH-VOCAB-R010.5 Query mechanism counts
- [ ] EG-ORCH-VOCAB-R010.6 Query competency question
- [ ] EG-ORCH-VOCAB-R011 Measured model facts from evaluation (rollup)
- [ ] EG-ORCH-VOCAB-R011.1 Add class `:EvaluationRun`
- [ ] EG-ORCH-VOCAB-R011.2 Add class `:MeasuredModelFact`
- [ ] EG-ORCH-VOCAB-R011.3 Add properties `:factOfModel` and `:measuredProperty`
- [ ] EG-ORCH-VOCAB-R011.4 Add property `:evidencedByRun`
- [ ] EG-ORCH-VOCAB-R011.5 Add property `:measuredSupersedes`
- [ ] EG-ORCH-VOCAB-R011.6 Add measured-fact shape
- [ ] EG-ORCH-VOCAB-R011.7 Build the measured-fact envelope
- [ ] EG-ORCH-VOCAB-R011.8 Refuse a measured fact without evidence
- [ ] EG-ORCH-VOCAB-R011.9 Replace a fact by a newer measurement
- [ ] EG-ORCH-VOCAB-R011.10 Apply the envelope through the governed path
- [ ] EG-ORCH-VOCAB-R011.11 Prove the loop closes
- [ ] EG-ORCH-VOCAB-R012.1 Add the spec-folder node kinds
- [ ] EG-ORCH-VOCAB-R012.2 Classify a spec-folder file
- [ ] EG-ORCH-VOCAB-R012.3 Attach the classifier to repository indexing
- [ ] EG-ORCH-VOCAB-R012.4 Refuse an unclassifiable file
- [ ] EG-ORCH-VOCAB-R012.5 Record the rule that decided
- [ ] EG-ORCH-VOCAB-R012.6 Prove folder ingestion end to end
- [ ] **EG-ORCH-VOCAB-R013:** Run trace step record with request, argument and result digests.
- [ ] **EG-ORCH-VOCAB-R013.1:** Model. Add `StepRecord{step_index, kind, model_request_digest, tool_args_digest, result_digest}` in `crates/eg-types/src/outcome_bundle.rs` beside `ReceiptNodeKind` at line 130. Add the kind `ModelRequest` to `ReceiptNodeKind`. `kind` is closed: `model_request`, `tool_call`. Raw prompt and argument bytes stay in a referenced artifact. Proving test: `crates/eg-types/src/outcome_bundle.rs::tests::step_record_round_trips`.
- [ ] **EG-ORCH-VOCAB-R013.2:** Behavior. Add `step_digest(kind, canonical_bytes) -> Digest256` in `crates/eg-types/src/outcome_bundle.rs`. The default is SHA-256 over canonical JSON, a named replaceable recipe. The harness and the engine must use this one recipe. The harness compares digests span by span and names the first index that differs. Proving test: `crates/eg-types/src/outcome_bundle.rs::tests::step_digest_is_canonical`.
- [ ] **EG-ORCH-VOCAB-R013.3:** Wiring. `StepRecord` validation attaches where a terminal commit binds its receipt nodes, in `crates/eg-types/src/outcome_bundle.rs:668` (`RunEvent`). The production caller is the `AddNode` arm at `src/server/handlers/graph_ops/gateway_graph.rs:62`, which `EG-ORCH-VOCAB` already uses for run traces. This row creates the call site that validates the digests of each ordered step. No production replay compare reads the stored digests today. Proving test: `src/server/handlers/graph_ops/gateway_graph.rs::tests::add_run_trace_checks_step_digests`.
- [ ] **EG-ORCH-VOCAB-R013.4:** Refusal. A step with a digest that is not 64 hex characters must be refused with `STEP_DIGEST_INVALID`. A step index that skips or repeats must be refused with `STEP_ORDER_INVALID`. Proving test: `crates/eg-types/src/outcome_bundle.rs::tests::bad_digest_and_gap_refused`.
- [ ] **EG-ORCH-VOCAB-R013.5:** Evidence. Each stored step must carry `:stepIndex` and `:recordedBy`, the actor that wrote it. The run trace must expose the ordered step digests to a read. Proving test: `crates/eg-types/src/outcome_bundle.rs::tests::stored_steps_read_in_order`.
- [ ] **EG-ORCH-VOCAB-R013.6:** Hot-path proof. The entry point is the generated client run-trace write, which reaches the `AddNode` wire method. The test writes three steps, reads them back, and compares them with a replayed copy that differs at step two. The call passes through `src/server/handlers/graph_ops/gateway_graph.rs:62`. It observes that the first differing index is two. Proving test: `tests/orchestration/test_run_trace_steps_served.py::test_first_divergence_is_step_two`.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-vocabulary -->
## Reviewed residual definition tasks — owner batch 2026-10-12

- [ ] **EG-ORCH-VOCAB-R009.18 (AU:26):** Define and wire the EG-owned residual for `EG-ORCH-VOCAB-R009` using the reviewed producer/consumer route; preserve covered clauses `AU-CONTROL-R006`, `AU-CONTROL-R019`, `AU-HARNESS-R007`, `EG-ORCH-VOCAB-R009.8`, `EG-ORCH-VOCAB-R009.9`, `GRAPHOS-CAPACITY-R001.1`.
- [ ] **EG-ORCH-VOCAB-R009.18 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend the existing EG routing/continuation contract with the joint per-run observation window and adjustment receipt; bind AU observations and trace application. Preserve host ceiling and narrow-only enforcement.
- [ ] **EG-ORCH-VOCAB-R009.18 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-ORCH-VOCAB-R013.7:** Extend the existing RunTrace AddNode/UQL path with the source-pinned CodeWalkthroughV1 manifest and typed refusal checks. AU producer and WebUI consumer remain explicit owner holds until their exact requirement IDs are linked.
- [ ] **EG-ORCH-VOCAB-R013.7 acceptance:** Run `code_walkthrough_manifest_round_trips`, `add_run_trace_rejects_stale_walkthrough_source_or_artifact`, and `test_walkthrough_trace_and_artifact_read_back`; verify independent artifact and UQL digests. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R003.8:** Extend existing R135 program lookup and Agent Library projection with successful-run promotion, pinned compatible-task similarity, and typed handler metadata; keep graph.assemble as execution authority.
- [ ] **EG-ORCH-VOCAB-R003.8 acceptance:** Run `similarity_policy_and_signature_round_trip`, `team_reuse_rejects_stale_or_incompatible_handler`, and `test_successful_team_promotes_and_similar_task_reuses_constrained_plan`; include no-candidate and handler refusal. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R012.7 (EG:46):** Define and wire the EG-owned residual for `EG-ORCH-VOCAB-R012.2` using the reviewed producer/consumer route; preserve covered clauses `EG-ORCH-VOCAB-R012.1`, `EG-ORCH-VOCAB-R012.2`, `EG-ORCH-VOCAB-R012.3`, `EG-ORCH-VOCAB-R012.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`, `EG-DECISION-ENGINE-R129.3`, `EG-DECISION-ENGINE-R129.4`, `EG-DECISION-ENGINE-R129.5`, `EG-IDENTITY-R008.4`.
- [ ] **EG-ORCH-VOCAB-R012.7 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Credit R012.1-.6 rather than treating policy-node ingestion as wholly absent. Define content-to-policy-rule synthesis and link its activation path to invariant checking and reasoning consumption. A contradictory repository policy must be refused before activation while preserving prior policy state.
- [ ] **EG-ORCH-VOCAB-R012.7 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-ORCH-VOCAB-R004.6 (EG:51):** Define and wire the EG-owned residual for `EG-ORCH-VOCAB-R004` using the reviewed producer/consumer route; preserve covered clauses `EG-ORCH-VOCAB-R004`, `EG-DECISION-ENGINE-R010`.
- [ ] **EG-ORCH-VOCAB-R004.6 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Extend existing capability/orchestration vocabulary and its registry query contract with gap/enhancement types and task-class relations. Prove persisted create/read and task-class filtering; distinguish a request-local abstention from a durable catalog entry.
- [ ] **EG-ORCH-VOCAB-R004.6 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.

- [ ] **EG-ORCH-VOCAB-R012.8 (EG:61):** Define and wire the EG-owned residual for `EG-ORCH-VOCAB-R012.2` using the reviewed producer/consumer route; preserve covered clauses `EG-IDENTITY-R008.4`, `EG-ORCH-VOCAB-R012.2`, `GRAPHOS-OPS-R050.2`, `GRAPHOS-OPS-R050.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`.
- [ ] **EG-ORCH-VOCAB-R012.8 acceptance:** Deliver the positive, negative, refusal, boundary, or readback cases explicitly required by: Bind reviewed file revision/digest to activated EG policy. Require no autonomous source overwrite and route requested source changes via approved SDK writeback; test rejected/stale review leaves file and active policy unchanged.
- [ ] **EG-ORCH-VOCAB-R012.8 status:** Keep SPECIFIED until separately reviewed implementation and served evidence are available.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition tasks — 2026-10-12

- [ ] `EG-ORCH-VOCAB-R005.100`: add the typed request/result, policy/revision binding, producer/consumer wiring contract, refusal cases and receipt/readback fields described in requirements; preserve `R005` and its prior children.
- [ ] `EG-ORCH-VOCAB-R005.100`: add served acceptance for: Served parallel workflow has a secret undeclared nested field and a declared sibling field; worker receives only its declared projection, while coordinator can still synthesize typed outputs. Unknown paths, stale schema or unauthorized source fields refuse before dispatch. Preserve R020 typed result and schema-refusal behavior.
- [ ] `EG-ORCH-VOCAB-R009.100`: add the typed request/result, policy/revision binding, producer/consumer wiring contract, refusal cases and receipt/readback fields described in requirements; preserve `R009` and its prior children.
- [ ] `EG-ORCH-VOCAB-R009.100`: add served acceptance for: Served read test exports a parent with multiple spawned agents and tool calls, retrieves all correlated records across pages, and verifies stable digest, redaction and completeness. Missing child, duplicate span, unauthorized read and truncated pagination are detected/refused. Do not assert MCP transport delivery from EG read tests.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tasks -->
## Cross-owner harness analytics tasks — 2026-10-12

- [ ] `EG-ORCH-VOCAB-R009.101`: define and serve full-manifest tenant-scoped harness session index/browse/search from RunTrace; bind SDK receipt, source coverage, cursor and optional R010.101 cost profile, then test idempotent replay/refusals.
- [ ] `EG-ORCH-VOCAB-R010.100`: define and serve proposal/prompt/progress-to-trace metric joins with revisioned metric fields, partial/unknown semantics and independent readback; link AU producer and GraphOS adapter IDs before closure.
- [ ] `EG-ORCH-VOCAB-R010.101`: add typed disjoint token accounting and grouped indexed queries; aggregate exact rational cost and round once per output group/category; measure the stated 1,000,000-row p95 boundary through the served path.
- [ ] For each row, implement positive, refusal, stale-revision and cross-tenant cases exactly as its requirement specifies; preserve existing R009/R010 children and metric meanings.
<!-- /spec-definition-patch -->
