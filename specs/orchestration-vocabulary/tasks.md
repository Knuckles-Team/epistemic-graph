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

- [ ] **EG-ORCH-VOCAB-R009.18 (AU:26):** Implement the revisioned observation window and versioned joint policy defined in `requirements.md`. Route AU observations to `Method::Decide`. Trace committed pairs and abstentions before application. Preserve R136/R009.17 reuse and GraphOS capacity authority. Keep AU-CONTROL-R006.30 unmerged as an explicit policy dependency. Preserve all prior R009 coverage.
- [ ] **EG-ORCH-VOCAB-R009.18 acceptance:** Run `run_observations_change_model_and_width_with_trace` through AU routing/continuation and served EG Decide. Prove model/width changes under controlled latency, cost, and error windows. Verify budget-forced refusal, exact trace readback, stale-window/rate/denominator refusal, unknown preservation, narrowing release, and widening refusal. Host-only AIMD and model-only allocator tests fail acceptance. Keep owner policy thresholds, cost inputs, and tie breaks held. Do not invent defaults or claim implementation evidence.
- [ ] **EG-ORCH-VOCAB-R009.18 status:** Keep SPECIFIED with empty `landed_in` and `verified_by` until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R013.7:** Extend the existing RunTrace AddNode/UQL path with the source-pinned CodeWalkthroughV1 manifest and typed refusal validation. AU producer and WebUI consumer remain explicit owner holds until their exact requirement IDs are linked.
- [ ] **EG-ORCH-VOCAB-R013.7 acceptance:** Run `code_walkthrough_manifest_round_trips`, `add_run_trace_rejects_stale_walkthrough_source_or_artifact`, and `test_walkthrough_trace_and_artifact_read_back`; confirm independent artifact and UQL digests. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R003.8:** Extend R135 program lookup and Agent Library projection. Add successful-run promotion, pinned compatible-task similarity, and typed handler metadata. Keep `graph.assemble` as execution authority.
- [ ] **EG-ORCH-VOCAB-R003.8 acceptance:** Run `similarity_policy_and_signature_round_trip`, `team_reuse_rejects_stale_or_incompatible_handler`, and `test_successful_team_promotes_and_similar_task_reuses_constrained_plan`; include no-candidate and handler refusal. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R012.7 (EG:46):** Implement source-bound GuardrailRule candidates and activation per `requirements.md`. Use the existing source_ingest/SourceIngest route, GuardrailRule storage, and invariant validation. Bind exact review and expected policy revision. Preserve source-prose non-authority, prior R012 coverage, and R129/identity dependencies.
- [ ] **EG-ORCH-VOCAB-R012.7 acceptance:** Run `repository_policy_is_synthesized_guarded_and_used` with real IndexRepository input. Read candidates/spans; review exact digest; activate and prove served decision use. Verify conflicting rule names invariant and preserves active revision. Test unparsed prose, missing review, changed source, and generation race. Classification and helper-only tests fail acceptance. Keep declarative grammar and review-receipt producer as explicit owner holds.
- [ ] **EG-ORCH-VOCAB-R012.7 status:** Keep SPECIFIED with empty `landed_in` and `verified_by` until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R004.6 (EG:51):** Implement distinct typed gap/enhancement records and edges per `requirements.md`. Use governed AddNode/ChangeEnvelope and existing taxonomy/UnifiedQuery. Validate references, ownership, revisions, event deduplication, match mode, and authorization. Keep the unrelated EG:7 export hold separate.
- [ ] **EG-ORCH-VOCAB-R004.6 acceptance:** Run `capability_gap_and_enhancement_are_queryable` through served mutation and generated UnifiedQuery. Prove abstention alone creates no durable gap. Admit a separate gap that cites it. Read fields, edges, and provenance after restart. Test deduplication, revision conflict, invalid taxonomy, exact/descendant matching, denied-tenant pagination, and unchanged capability state. Keep resolver and mutation producer bindings open. Do not invent an EG:7 owner ID.
- [ ] **EG-ORCH-VOCAB-R004.6 status:** Keep SPECIFIED with empty `landed_in` and `verified_by` until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R012.8 (EG:61):** Implement reviewed normative-source binding and activation per `requirements.md`. Keep normative files authoritative and graph policy active. Bind SDK governed writeback and GraphOS approval. EG never writes normative files directly. Preserve R012.7 and existing identity/operations coverage.
- [ ] **EG-ORCH-VOCAB-R012.8 acceptance:** Run `reviewed_normative_source_activates_without_graph_overwrite` through served ingestion, review, activation, and decision use. Compare source bytes/revision before and after graph work. Test no-effect graph writes, missing/rejected review, modified blob, changed rules, and revision races. A permitted change requires actual governed writeback, reread, and renewed review. Fixture activation fails acceptance. Keep review-receipt and source-effect producer binding open.
- [ ] **EG-ORCH-VOCAB-R012.8 status:** Keep SPECIFIED with empty `landed_in` and `verified_by` until implementation and independent served evidence are reviewed; no closure is claimed.
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
