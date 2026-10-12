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

- [ ] **EG-ORCH-VOCAB-R009.18 (AU:26):** Specify and wire the contract at the existing owner path: EG SHALL accept one revisioned per-run observation window containing run/window IDs and times, latency samples with units, error count and eligible-attempt denominator, token usage and measured cost with currency/rate revision, current model/profile and parallel width, consumed/remaining budgets, and host capacity receipt. Missing samples/rates, a zero denominator, stale windows, or mismatched run revisions SHALL remain unknown and SHALL NOT become zero-cost or zero-error evidence. The versioned adjustment policy SHALL state latency/error thresholds, the model adequacy/profile inputs, cost prediction basis, and deterministic tie breaks. EG SHALL jointly evaluate model and width against these observations and remaining budget, return a committed feasible pair or a typed abstention/stop, and trace the before/after model, width, budget, window, policy, evidence, and reason. Mid-run continuation SHALL obey AU-CONTROL-R019: width may narrow and release capacity but SHALL NOT widen. Later-run allocation requires a distinct admission decision and capacity grant. Production wiring: Feed the proposed AU RunAdjustmentObservation from the existing routing and continuation paths to Method::Decide. Reuse R136 model adequacy/stage allocation and the existing RunTrace ingestion/readback under R009.17. Persist both adjustments and abstentions before application; GraphOS remains capacity authority. Preserve existing coverage: `AU-CONTROL-R006`, `AU-CONTROL-R019`, `AU-HARNESS-R007`, `EG-ORCH-VOCAB-R009.8`, `EG-ORCH-VOCAB-R009.9`, `GRAPHOS-CAPACITY-R001.1`.
- [ ] **EG-ORCH-VOCAB-R009.18 acceptance:** Proposed integration case run_observations_change_model_and_width_with_trace: exercise the actual AU routing/continuation consumer and served EG Decide path using identical budgets but controlled joint latency/cost/error windows. Demonstrate policy-directed model and width changes, a budget-forced refusal/stop, and a trace/readback with exact before/after facts. Reject stale windows and invalid rates/denominators; verify unknown values are not zeros. Demonstrate width narrowing releases capacity and attempted mid-run widening is rejected. A host-only AIMD test or model-only allocator test is insufficient. Open owner prerequisites: AU-CONTROL-R006.30 is unmerged. The exact joint policy thresholds, predictive cost inputs, and tie-break semantics require an owner-versioned policy; no tuning defaults or implementation claims are supplied by the source. Owner-review choices are proposed only: The joint carrier and policy-required fields make missing decisions explicit. A self-referential command to adjust appropriately is not a substitute for that published policy..
- [ ] **EG-ORCH-VOCAB-R009.18 status:** Keep SPECIFIED with empty landed_in and verified_by until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R013.7:** Extend the existing RunTrace AddNode/UQL path with the source-pinned CodeWalkthroughV1 manifest and typed refusal checks. AU producer and WebUI consumer remain explicit owner holds until their exact requirement IDs are linked.
- [ ] **EG-ORCH-VOCAB-R013.7 acceptance:** Run `code_walkthrough_manifest_round_trips`, `add_run_trace_rejects_stale_walkthrough_source_or_artifact`, and `test_walkthrough_trace_and_artifact_read_back`; verify independent artifact and UQL digests. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R003.8:** Extend existing R135 program lookup and Agent Library projection with successful-run promotion, pinned compatible-task similarity, and typed handler metadata; keep graph.assemble as execution authority.
- [ ] **EG-ORCH-VOCAB-R003.8 acceptance:** Run `similarity_policy_and_signature_round_trip`, `team_reuse_rejects_stale_or_incompatible_handler`, and `test_successful_team_promotes_and_similar_task_reuses_constrained_plan`; include no-candidate and handler refusal. These are future tests; status stays SPECIFIED.

- [ ] **EG-ORCH-VOCAB-R012.7 (EG:46):** Specify and wire the contract at the existing owner path: IndexRepository SHALL bind each classified constitution/policy source to repository, path, commit, blob digest, and source spans, and synthesize typed GuardrailRule candidates linked to those spans. The synthesis profile SHALL be versioned and declare its supported rule grammar; unparsed or ambiguous clauses SHALL remain inactive with explicit reasons. Synthesis SHALL NOT grant source prose authority to execute or activate itself. Before activating a reviewed candidate set, EG SHALL check every rule against the pinned invariant generation using R129.3, validate the review binding under R012.8, and compare the expected active-policy revision. A contradiction SHALL identify the invariant IRI, candidate rule, and source clause; a stale generation/review or unsupported clause SHALL refuse activation and preserve the previous active set. Successful activation SHALL atomically publish the candidate-set digest and policy revision. Subsequent reasoning SHALL load this active graph policy and record its revision in the decision evidence. Production wiring: Extend the existing source_ingest router and SourceIngest path after classify_spec_file. Reuse GuardrailRule storage and check_rule_against_invariants; connect activation to the policy graph consumed by the existing decision/reasoning path under EG-IDENTITY-R008.4. R129.5 human-override guarding alone does not establish repository-policy guarding. Preserve existing coverage: `EG-ORCH-VOCAB-R012.1`, `EG-ORCH-VOCAB-R012.2`, `EG-ORCH-VOCAB-R012.3`, `EG-ORCH-VOCAB-R012.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`, `EG-DECISION-ENGINE-R129.3`, `EG-DECISION-ENGINE-R129.4`, `EG-DECISION-ENGINE-R129.5`, `EG-IDENTITY-R008.4`.
- [ ] **EG-ORCH-VOCAB-R012.7 acceptance:** Proposed integration case repository_policy_is_synthesized_guarded_and_used: send a real policy file through IndexRepository, inspect typed candidates and source spans, approve its exact digest through the owner review path, activate, and execute a served decision whose outcome proves that active rule was loaded. A conflicting rule must name the stored invariant and leave the prior active revision unchanged. Also exercise unparsed prose, missing review, changed source digest, and an invariant-generation race. Classification alone or a direct pure invariant-helper test does not satisfy acceptance. Open owner prerequisites: The supported declarative grammar/synthesis profile and concrete review receipt producer must be published and bound. Unstructured text synthesis may produce candidates, but cannot silently fill those authority gaps. Owner-review choices are proposed only: Atomic publication is confined to the active graph policy set; it does not imply atomic cross-repository filesystem effects..
- [ ] **EG-ORCH-VOCAB-R012.7 status:** Keep SPECIFIED with empty landed_in and verified_by until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R004.6 (EG:51):** Specify and wire the contract at the existing owner path: EG SHALL persist distinct typed CapabilityGap and DesiredEnhancement nodes with tenant, node ID/revision, capability reference, task-class reference, domain-taxonomy reference, source evidence/digest, owner, and disposition. Edges SHALL connect each node to its capability, task class, and domain; a gap may cite a typed unmet-capability abstention, while an enhancement records a requested improvement without asserting that capability is absent. The governed mutation path SHALL validate references, ownership, and expected revisions and deduplicate a repeated source event. Unknown taxonomy or task-class references SHALL refuse instead of creating arbitrary terms. UnifiedQuery SHALL return these types filtered by task class, with explicit exact or taxonomy-descendant matching and a pinned taxonomy revision. Authorization SHALL run before rows, counts, and pagination. A gap/enhancement node SHALL NOT by itself mark a capability implemented, admitted, or successfully tested. Production wiring: Extend the existing orchestration ontology/shapes and governed AddNode/ChangeEnvelope path. Reuse the existing task-class/domain taxonomy and Method::UnifiedQuery rather than introducing a second capability registry. Keep the EG:7 owner-export manifest question separate from these persisted domain nodes. Preserve existing coverage: `EG-ORCH-VOCAB-R004`, `EG-DECISION-ENGINE-R010`.
- [ ] **EG-ORCH-VOCAB-R004.6 acceptance:** Proposed integration case capability_gap_and_enhancement_are_queryable: use the real governed node-mutation route to record a gap from a served abstention and an enhancement from a distinct reviewed request. Query by two different task classes through the generated UnifiedQuery client, restart and repeat, and independently read back typed fields, edges, and provenance. Verify event deduplication, expected-revision conflict, invalid taxonomy refusal, exact versus descendant behavior, denied-tenant counts/cursors, and that capability-implementation state did not change. In-memory registry fixtures alone do not pass. Open owner prerequisites: The authoritative domain taxonomy/task-class resolver and admitted mutation producer must be bound to existing owner contracts. Do not fabricate a new owner role-export ID to cover the unrelated EG:7 hold. Owner-review choices are proposed only: Distinct gap/enhancement types and explicit query match mode are proposed schema additions..
- [ ] **EG-ORCH-VOCAB-R004.6 status:** Keep SPECIFIED with empty landed_in and verified_by until implementation and independent served evidence are reviewed; no closure is claimed.

- [ ] **EG-ORCH-VOCAB-R012.8 (EG:61):** Specify and wire the contract at the existing owner path: EG SHALL retain epistemic state and active runtime policy in the graph while binding normative source to versioned reviewed repository files. Each activated source binding SHALL contain repository, path, commit, blob digest, candidate-set digest, review actor/receipt digest, invariant generation, and expected active-policy revision. Review SHALL cover the exact source and synthesized rule digests; stale, absent, rejected, or mismatched review SHALL refuse activation before active-policy mutation. EG graph mutations and reasoning SHALL NOT overwrite normative source files. A proposed source change SHALL use the existing governed SDK source-writeback and GraphOS approval boundary, then be reread at its committed revision and reviewed before it can become active graph policy. A graph policy edit SHALL NOT silently rewrite a file or treat an unreviewed file as runtime authority. R012.7 invariant checks and generation comparison SHALL apply at activation. Production wiring: Link the R012.2 policy classification and EG-IDENTITY-R008.4 active graph policy to R012.7 activation. Bind review/approval evidence to GRAPHOS-OPS-R050.2/.4 and the concrete SDK governed source effect. EG has no normative filesystem writer. Preserve existing coverage: `EG-IDENTITY-R008.4`, `EG-ORCH-VOCAB-R012.2`, `GRAPHOS-OPS-R050.2`, `GRAPHOS-OPS-R050.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`.
- [ ] **EG-ORCH-VOCAB-R012.8 acceptance:** Proposed integration case reviewed_normative_source_activates_without_graph_overwrite: pin a real repository policy and its owner review receipt, ingest and activate through served EG routes, and execute a decision that records the resulting policy revision. Independently compare source bytes and Git revision before/after graph mutation and reasoning. An attempted graph-originated normative write must yield no source effect. Missing/rejected review, a modified blob after review, changed synthesized rules, and a raced invariant/active revision must preserve prior active policy. For a permitted source change, prove actual governed writeback, reread, renewed review, and only then activation; no direct fixture activation substitutes for this path. Open owner prerequisites: The concrete producer of the review receipt and normative-file source-effect binding must be owner-confirmed. R050 references establish review/ops boundaries, not proof that the proposed full carrier already exists. Owner-review choices are proposed only: This preserves the accepted authority correction: reviewed files are normative source; EG owns active policy. It does not adopt file-only runtime authority..
- [ ] **EG-ORCH-VOCAB-R012.8 status:** Keep SPECIFIED with empty landed_in and verified_by until implementation and independent served evidence are reviewed; no closure is claimed.
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
