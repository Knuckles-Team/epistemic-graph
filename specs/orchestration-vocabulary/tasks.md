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

- [ ] **EG-ORCH-VOCAB-R009.18:** Implement the complete contract in `requirements.md`; Feed AU RunAdjustmentObservation from routing/continuation to Method::Decide; reuse R136, R009.17 RunTrace, and GraphOS capacity authority. Preserve `EG-ORCH-VOCAB-R009` and its existing obligations. Hold: AU-CONTROL-R006.30 is unmerged; versioned joint-policy thresholds, cost inputs, and deterministic tie breaks remain open.
- [ ] **EG-ORCH-VOCAB-R009.18 acceptance:** Run `run_observations_change_model_and_width_with_trace`: exercise the actual AU routing/continuation consumer and served EG Decide path using identical budgets but controlled joint latency/cost/error windows. Demonstrate policy-directed model and width changes, a budget-forced refusal/stop, and a trace/readback with exact before/after facts. Reject stale windows and invalid rates/denominators; verify unknown values are not zeros. Demonstrate width narrowing releases capacity and attempted mid-run widening is rejected. A host-only AIMD test or model-only allocator test is insufficient. Planned acceptance only. Hold: AU-CONTROL-R006.30 is unmerged; versioned joint-policy thresholds, cost inputs, and deterministic tie breaks remain open.
- [ ] **EG-ORCH-VOCAB-R009.18 status:** Keep SPECIFIED with empty `landed_in` and `verified_by`; no test, served readback, receipt, implementation, or closure is claimed. Source `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`.

- [ ] **EG-ORCH-VOCAB-R013.7:** Extend the existing RunTrace AddNode/UQL path with the source-pinned CodeWalkthroughV1 manifest and typed refusal validation. AU producer and WebUI consumer remain explicit owner holds until their exact requirement IDs are linked.
- [ ] **EG-ORCH-VOCAB-R013.7 acceptance:** Run `code_walkthrough_manifest_round_trips`, `add_run_trace_rejects_stale_walkthrough_source_or_artifact`, and `test_walkthrough_trace_and_artifact_read_back`; confirm independent artifact and UQL digests. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R003.8:** Extend R135 program lookup and Agent Library projection. Add successful-run promotion, pinned compatible-task similarity, and typed handler metadata. Keep `graph.assemble` as execution authority.
- [ ] **EG-ORCH-VOCAB-R003.8 acceptance:** Run `similarity_policy_and_signature_round_trip`, `team_reuse_rejects_stale_or_incompatible_handler`, and `test_successful_team_promotes_and_similar_task_reuses_constrained_plan`; include no-candidate and handler refusal. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R012.7:** Implement the complete contract in `requirements.md`; Extend SourceIngest after classify_spec_file; synthesize GuardrailRule candidates, reuse invariant checking, validate R012.8 review, and activate via R129.3/R008.4. Preserve `EG-ORCH-VOCAB-R012` and its existing obligations. Hold: Supported declarative grammar and concrete review-receipt producer remain owner prerequisites.
- [ ] **EG-ORCH-VOCAB-R012.7 acceptance:** Run `repository_policy_is_synthesized_guarded_and_used`: send a real policy file through IndexRepository, inspect typed candidates and source spans, approve its exact digest through the owner review path, activate, and execute a served decision whose outcome proves that active rule was loaded. A conflicting rule must name the stored invariant and leave the prior active revision unchanged. Also exercise unparsed prose, missing review, changed source digest, and an invariant-generation race. Classification alone or a direct pure invariant-helper test does not satisfy acceptance. Planned acceptance only. Hold: Supported declarative grammar and concrete review-receipt producer remain owner prerequisites.
- [ ] **EG-ORCH-VOCAB-R012.7 status:** Keep SPECIFIED with empty `landed_in` and `verified_by`; no test, served readback, receipt, implementation, or closure is claimed. Source `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`.

- [ ] **EG-ORCH-VOCAB-R004.6:** Implement the complete contract in `requirements.md`; Use existing ontology/shapes, governed AddNode/ChangeEnvelope, task/domain taxonomy, and Method::UnifiedQuery; keep EG:7 export separate. Preserve `EG-ORCH-VOCAB-R004` and its existing obligations. Hold: Authoritative taxonomy/task-class resolver and admitted mutation producer remain unbound; do not invent an EG:7 export ID.
- [ ] **EG-ORCH-VOCAB-R004.6 acceptance:** Run `capability_gap_and_enhancement_are_queryable`: use the real governed node-mutation route to record a gap from a served abstention and an enhancement from a distinct reviewed request. Query by two different task classes through the generated UnifiedQuery client, restart and repeat, and independently read back typed fields, edges, and provenance. Verify event deduplication, expected-revision conflict, invalid taxonomy refusal, exact versus descendant behavior, denied-tenant counts/cursors, and that capability-implementation state did not change. In-memory registry fixtures alone do not pass. Planned acceptance only. Hold: Authoritative taxonomy/task-class resolver and admitted mutation producer remain unbound; do not invent an EG:7 export ID.
- [ ] **EG-ORCH-VOCAB-R004.6 status:** Keep SPECIFIED with empty `landed_in` and `verified_by`; no test, served readback, receipt, implementation, or closure is claimed. Source `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`.

- [ ] **EG-ORCH-VOCAB-R012.8:** Implement the complete contract in `requirements.md`; Bind reviewed normative repository files to active graph policy; connect R012.2/R008.4 to R012.7 and use SDK writeback plus GraphOS review. EG does not write files. Preserve `EG-ORCH-VOCAB-R012` and its existing obligations. Hold: Concrete review-receipt producer and normative-file source-effect binding require owner confirmation.
- [ ] **EG-ORCH-VOCAB-R012.8 acceptance:** Run `reviewed_normative_source_activates_without_graph_overwrite`: pin a real repository policy and its owner review receipt, ingest and activate through served EG routes, and execute a decision that records the resulting policy revision. Independently compare source bytes and Git revision before/after graph mutation and reasoning. An attempted graph-originated normative write must yield no source effect. Missing/rejected review, a modified blob after review, changed synthesized rules, and a raced invariant/active revision must preserve prior active policy. For a permitted source change, prove actual governed writeback, reread, renewed review, and only then activation; no direct fixture activation substitutes for this path. Planned acceptance only. Hold: Concrete review-receipt producer and normative-file source-effect binding require owner confirmation.
- [ ] **EG-ORCH-VOCAB-R012.8 status:** Keep SPECIFIED with empty `landed_in` and `verified_by`; no test, served readback, receipt, implementation, or closure is claimed. Source `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition tasks — 2026-10-12

- [ ] `EG-ORCH-VOCAB-R005.100`: Add typed request/result, policy/revision binding, producer/consumer wiring, refusal, and receipt/readback fields from `requirements.md`. Preserve R005 and prior children.
- [ ] `EG-ORCH-VOCAB-R005.100`: Serve the undeclared-secret/declared-sibling projection case. Prove the worker receives declared fields only while the coordinator retains full input. Refuse unknown paths, stale schema, and unauthorized source fields before dispatch. Preserve R020 result/schema refusal.
- [ ] `EG-ORCH-VOCAB-R009.100`: Add typed request/result, policy/revision binding, producer/consumer wiring, refusal, and receipt/readback fields from `requirements.md`. Preserve R009 and prior children.
- [ ] `EG-ORCH-VOCAB-R009.100`: Serve a parent trace with multiple agents and tool calls. Retrieve all correlated records across pages. Confirm stable digest, redaction, and completeness. Detect missing child, duplicate span, unauthorized read, and truncated pagination. EG read tests do not prove MCP transport.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tasks -->
## Cross-owner harness analytics tasks — 2026-10-12

- [ ] `EG-ORCH-VOCAB-R009.101`: Define and serve full-manifest, tenant-scoped RunTrace session index/browse/search. Bind SDK receipt, source coverage, cursor, and optional R010.101 cost profile. Test idempotent replay and refusals.
- [ ] `EG-ORCH-VOCAB-R010.100`: Define and serve proposal/prompt/progress-to-trace joins with revisioned metrics and partial/unknown semantics. Independently read results. Link AU producer and GraphOS adapter IDs before closure.
- [ ] `EG-ORCH-VOCAB-R010.101`: Add disjoint token accounting and grouped indexed queries. Aggregate exact rational cost and round once per group/category. Measure the specified 1,000,000-row p95 through the served path.
- [ ] For each row, implement positive, refusal, stale-revision, and cross-tenant cases from its requirement. Preserve R009/R010 children and metric meanings.
<!-- /spec-definition-patch -->
