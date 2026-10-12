# EG-ORCH-VOCAB — Orchestration vocabulary

## Dependencies and unresolved owner contracts

- `EG-REPO-INGEST-R009` defines repository/source relationships; `AU-HARNESS-R024` supplies connected execution spans. Neither row currently defines the AU walkthrough producer input/output binding or the WebUI interactive artifact consumer. Those exact owner requirement IDs and served receipts remain pending coordinator assignment; `EG-ORCH-VOCAB-R013.7` is SPECIFIED and cannot be treated as end-to-end closed.
- `AU-HARNESS-R019.5-.7` are the existing study-gap producer/consumer prerequisites for `EG-CQ-ORCH-R010.7`; they must carry the same repository and revision as `EG-CQ-ORCH-R010.1-.6`. If the AU rows lack a stable proposal id/revision, that missing producer field remains a hold.
- `WEBUI-APPS-R004.7` and `.8` are named consumers for R010.100 and R010.101. AU progress-source and SDK harness-manifest/sync-receipt producer IDs remain unknown. Exact GraphOS served-adapter IDs are also unknown. These remain explicit holds.
- Existing `EG-DECISION-ENGINE-R036`, `R135.2`, `R135.4`, and `EG-TYPED-PACKS-R006` are prerequisites for `EG-ORCH-VOCAB-R003.8`; they define the assembly authority, program store, and Agent Library projection.

Owner: epistemic-graph. Cross-repo IDs: none required (consumer is graph-os intent tools via `EG-CQ-ORCH-R009`).
Public provenance: closes the MISSING-term lists in `specs/orchestration-competency` (`EG-CQ-ORCH-001`); spec-only change.

## Purpose and user stories

The competency spec found seven questions that the graph cannot answer because ontology terms are missing. This spec defines the complete vocabulary. It names existing writers for each term and explains how `graph.assemble()` (`EG-DECISION-ENGINE-R036`) reads them. It adds no planner, router, guardrail engine, or topology chooser. It adds no second catalog. Packs and Agent Library remain the content authority (`EG-TYPED-PACKS-R072`).

## Requirements and acceptance

- FR-001: Every missing term in `EG-CQ-ORCH-001` SHALL exist in the ontology file named in `design.md` (Vocabulary). That file SHALL state its domain, range, and SHACL constraints.
- FR-002: Each term SHALL have exactly one writer. Writers are typed-pack projection, Agent Library outbox, template/topology projection, or assemble commit. Each term SHALL have a reader named in `design.md` (Wiring).
- FR-003: The worked example (deploy a container) answers the seven part-queries from one fixture and one committed assembly.
- FR-004: Every caveat from the gap brief is recorded as CONFIRMED, DECIDED or BLOCKED in `design.md` (Decisions), with a Corrections list for `EG-CQ-ORCH-001`.
- SC-001: `requirements.md` rollups R001..R008 each close when their children pass their named tests.

## Scope and interfaces

Owned: the ontology terms, shapes, projection call sites and query tests in `requirements.md`. Cited, not re-specified: `specs/decision-engine` (R036, R044, R046, R106, R111, R126, R127), `specs/typed-packs-and-catalog` (R006, R013, R072), `specs/federated-query-and-reasoning`. Non-goals: solver changes, new wire methods, graph-os tool code.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| EG-ORCH-VOCAB-R001..R008 | design.md Vocabulary, Wiring, Worked example | `crates/eg-core/tests/orchestration_vocabulary.rs` | PENDING |

## Open questions

- BLOCKED (owner: epistemic-graph; `EG-DECISION-ENGINE-R106` is already VERIFIED): whether `AgentGraphEdge` can mark a loop-back edge; today it carries only `from`, `to`, `condition`. R006.5 adds the marker as a projection rule if absent.

Dependency note: the R127 guardrail storage and entailment algorithm are VERIFIED, but the served query (`EG-DECISION-ENGINE-R127.2.2`) and generated client (`R127.2.3`) remain SPECIFIED. Integration acceptance waits for those consumers.

<!-- spec-definition-patch:owner-batch-20261012-orchestration-vocabulary -->
## Reviewed owner residuals — 2026-10-12

### `EG-ORCH-VOCAB-R009.18` — AU:26

Parent obligation: `EG-ORCH-VOCAB-R009` remains in force; this child adds the definition below without replacing parent acceptance.

Original source objective: The platform monitors latency, token cost, and error frequency per run. It adjusts model selection and parallelism within declared budgets. It records each adjustment on the trace.

**Contract:** EG SHALL accept one revisioned per-run observation window containing run/window IDs and times, latency samples with units, error count and eligible-attempt denominator, token usage and measured cost with currency/rate revision, current model/profile and parallel width, consumed/remaining budgets, and host capacity receipt. Missing samples/rates, a zero denominator, stale windows, or mismatched run revisions SHALL remain unknown and SHALL NOT become zero-cost or zero-error evidence. The versioned adjustment policy SHALL state latency/error thresholds, the model adequacy/profile inputs, cost prediction basis, and deterministic tie breaks. EG SHALL jointly evaluate model and width against these observations and remaining budget, return a committed feasible pair or a typed abstention/stop, and trace the before/after model, width, budget, window, policy, evidence, and reason. Mid-run continuation SHALL obey AU-CONTROL-R019: width may narrow and release capacity but SHALL NOT widen. Later-run allocation requires a distinct admission decision and capacity grant.

**Production wiring:** Feed the proposed AU RunAdjustmentObservation from the existing routing and continuation paths to Method::Decide. Reuse R136 model adequacy/stage allocation and the existing RunTrace ingestion/readback under R009.17. Persist both adjustments and abstentions before application; GraphOS remains capacity authority.

**Planned acceptance:** `run_observations_change_model_and_width_with_trace` — Proposed integration case run_observations_change_model_and_width_with_trace: exercise the actual AU routing/continuation consumer and served EG Decide path using identical budgets but controlled joint latency/cost/error windows. Demonstrate policy-directed model and width changes, a budget-forced refusal/stop, and a trace/readback with exact before/after facts. Reject stale windows and invalid rates/denominators; verify unknown values are not zeros. Demonstrate width narrowing releases capacity and attempted mid-run widening is rejected. A host-only AIMD test or model-only allocator test is insufficient.

**Owner references:**

- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:10` — `AU-CONTROL-R006`

- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:24` — `AU-CONTROL-R019`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:298` — `EG-DECISION-ENGINE-R136.3`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:69` — `EG-ORCH-VOCAB-R009.17`

- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/adaptive-capacity/requirements.md:6` — `GRAPHOS-CAPACITY-R001.1`

**Unmerged consumer dependencies:**

- `AU-CONTROL-R006.30` at `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md` is `draft_unmerged`; Local drafting payload; not a merged producer or implementation receipt.

**Explicit holds:**

- AU-CONTROL-R006.30 is unmerged. The exact joint policy thresholds, predictive cost inputs, and tie-break semantics require an owner-versioned policy; no tuning defaults or implementation claims are supplied by the source.

**Owner-review proposal, not a source-established fact:**

- The joint carrier and policy-required fields make missing decisions explicit. A self-referential command to adjust appropriately is not a substitute for that published policy.

Source record: `AU:26`, SHA-256 `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`. Status remains SPECIFIED; `landed_in` and `verified_by` remain empty. No implementation, served receipt, or closure is claimed.

### `EG-ORCH-VOCAB-R013.7` — AU:115

Original objective: A codebase query can produce a linear walkthrough. It can produce a self-contained interactive explanation artifact. Execution notes stream to the graph as provenance.

EG-owned residual: Define the walkthrough, self-contained interactive explanation output and its graph-bound execution-note stream.

Producer/consumer boundary: EG repository hydration supplies source relationships; AU-HARNESS-R024 emits spans; EG-ORCH-VOCAB-R013 admits trace steps through AddNode. Explanation producer, artifact schema and WebUI consumption binding are absent definitions.

Preserve existing coverage: `EG-REPO-INGEST-R009`, `AU-HARNESS-R024`, `EG-ORCH-VOCAB-R013.1`, `EG-ORCH-VOCAB-R013.3`.

Owner source hash: `445704f78ae07d1c4b62f42dd23797823d91bc470f04a9dc779e3f6777fffbf0`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R003.8` — EG:12

Original objective: Successful work promotes a reusable team configuration. Similarity to a new task selects it. Capabilities are typed nodes with handler metadata.

EG-owned residual: Define team-level promotion, task-similarity reuse and typed capability handler metadata.

Producer/consumer boundary: graph.assemble consumes capabilities; Agent Library projects them; compile_program/lookup_program reuse traces. Team similarity selection and handler-metadata consumption are absent definitions.

Preserve existing coverage: `EG-DECISION-ENGINE-R135.2`, `EG-DECISION-ENGINE-R135.4`, `EG-DECISION-ENGINE-R036`, `EG-ORCH-VOCAB-R003.5`, `EG-ORCH-VOCAB-R003.6`.

Owner source hash: `517823a931bb93e173b301a9a31fbe6bd75c14687233c72278a59ced1940673a`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R012.7` — EG:46

Parent obligation: `EG-ORCH-VOCAB-R012` remains in force; this child adds the definition below without replacing parent acceptance.

Original source objective: Repository constitution and policy files are ingested and synthesized as graph policy nodes for reasoning. A policy that contradicts a stored invariant is refused.

**Contract:** IndexRepository SHALL bind each classified constitution/policy source to repository, path, commit, blob digest, and source spans, and synthesize typed GuardrailRule candidates linked to those spans. The synthesis profile SHALL be versioned and declare its supported rule grammar; unparsed or ambiguous clauses SHALL remain inactive with explicit reasons. Synthesis SHALL NOT grant source prose authority to execute or activate itself. Before activating a reviewed candidate set, EG SHALL check every rule against the pinned invariant generation using R129.3, validate the review binding under R012.8, and compare the expected active-policy revision. A contradiction SHALL identify the invariant IRI, candidate rule, and source clause; a stale generation/review or unsupported clause SHALL refuse activation and preserve the previous active set. Successful activation SHALL atomically publish the candidate-set digest and policy revision. Subsequent reasoning SHALL load this active graph policy and record its revision in the decision evidence.

**Production wiring:** Extend the existing source_ingest router and SourceIngest path after classify_spec_file. Reuse GuardrailRule storage and check_rule_against_invariants; connect activation to the policy graph consumed by the existing decision/reasoning path under EG-IDENTITY-R008.4. R129.5 human-override guarding alone does not establish repository-policy guarding.

**Planned acceptance:** `repository_policy_is_synthesized_guarded_and_used` — Proposed integration case repository_policy_is_synthesized_guarded_and_used: send a real policy file through IndexRepository, inspect typed candidates and source spans, approve its exact digest through the owner review path, activate, and execute a served decision whose outcome proves that active rule was loaded. A conflicting rule must name the stored invariant and leave the prior active revision unchanged. Also exercise unparsed prose, missing review, changed source digest, and an invariant-generation race. Classification alone or a direct pure invariant-helper test does not satisfy acceptance.

**Owner references:**

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:91` — `EG-ORCH-VOCAB-R012.2`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/engine-identity-and-scope/requirements.md:16` — `EG-IDENTITY-R008.4`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:247` — `EG-DECISION-ENGINE-R129.3`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:249` — `EG-DECISION-ENGINE-R129.5`

**Unmerged consumer dependencies:**

- None.

**Explicit holds:**

- The supported declarative grammar/synthesis profile and concrete review receipt producer must be published and bound. Unstructured text synthesis may produce candidates, but cannot silently fill those authority gaps.

**Owner-review proposal, not a source-established fact:**

- Atomic publication is confined to the active graph policy set; it does not imply atomic cross-repository filesystem effects.

Source record: `EG:46`, SHA-256 `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`. Status remains SPECIFIED; `landed_in` and `verified_by` remain empty. No implementation, served receipt, or closure is claimed.

### `EG-ORCH-VOCAB-R004.6` — EG:51

Parent obligation: `EG-ORCH-VOCAB-R004` remains in force; this child adds the definition below without replacing parent acceptance.

Original source objective: The capability registry models functional gaps and desired enhancements as typed nodes in a domain taxonomy. Queries can filter by task class.

**Contract:** EG SHALL persist distinct typed CapabilityGap and DesiredEnhancement nodes with tenant, node ID/revision, capability reference, task-class reference, domain-taxonomy reference, source evidence/digest, owner, and disposition. Edges SHALL connect each node to its capability, task class, and domain; a gap may cite a typed unmet-capability abstention, while an enhancement records a requested improvement without asserting that capability is absent. The governed mutation path SHALL validate references, ownership, and expected revisions and deduplicate a repeated source event. Unknown taxonomy or task-class references SHALL refuse instead of creating arbitrary terms. UnifiedQuery SHALL return these types filtered by task class, with explicit exact or taxonomy-descendant matching and a pinned taxonomy revision. Authorization SHALL run before rows, counts, and pagination. A gap/enhancement node SHALL NOT by itself mark a capability implemented, admitted, or successfully tested.

**Production wiring:** Extend the existing orchestration ontology/shapes and governed AddNode/ChangeEnvelope path. Reuse the existing task-class/domain taxonomy and Method::UnifiedQuery rather than introducing a second capability registry. Keep the EG:7 owner-export manifest question separate from these persisted domain nodes.

**Planned acceptance:** `capability_gap_and_enhancement_are_queryable` — Proposed integration case capability_gap_and_enhancement_are_queryable: use the real governed node-mutation route to record a gap from a served abstention and an enhancement from a distinct reviewed request. Query by two different task classes through the generated UnifiedQuery client, restart and repeat, and independently read back typed fields, edges, and provenance. Verify event deduplication, expected-revision conflict, invalid taxonomy refusal, exact versus descendant behavior, denied-tenant counts/cursors, and that capability-implementation state did not change. In-memory registry fixtures alone do not pass.

**Owner references:**

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:24` — `EG-ORCH-VOCAB-R004`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:16` — `EG-DECISION-ENGINE-R010`

**Unmerged consumer dependencies:**

- None.

**Explicit holds:**

- The authoritative domain taxonomy/task-class resolver and admitted mutation producer must be bound to existing owner contracts. Do not fabricate a new owner role-export ID to cover the unrelated EG:7 hold.

**Owner-review proposal, not a source-established fact:**

- Distinct gap/enhancement types and explicit query match mode are proposed schema additions.

Source record: `EG:51`, SHA-256 `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`. Status remains SPECIFIED; `landed_in` and `verified_by` remain empty. No implementation, served receipt, or closure is claimed.

### `EG-ORCH-VOCAB-R012.8` — EG:61

Parent obligation: `EG-ORCH-VOCAB-R012` remains in force; this child adds the definition below without replacing parent acceptance.

Original source objective: Epistemic state and active policy live in the graph. Normative rules and constitutions live in versioned files. Graph writes never overwrite them. Normative changes require review.

**Contract:** EG SHALL retain epistemic state and active runtime policy in the graph while binding normative source to versioned reviewed repository files. Each activated source binding SHALL contain repository, path, commit, blob digest, candidate-set digest, review actor/receipt digest, invariant generation, and expected active-policy revision. Review SHALL cover the exact source and synthesized rule digests; stale, absent, rejected, or mismatched review SHALL refuse activation before active-policy mutation. EG graph mutations and reasoning SHALL NOT overwrite normative source files. A proposed source change SHALL use the existing governed SDK source-writeback and GraphOS approval boundary, then be reread at its committed revision and reviewed before it can become active graph policy. A graph policy edit SHALL NOT silently rewrite a file or treat an unreviewed file as runtime authority. R012.7 invariant checks and generation comparison SHALL apply at activation.

**Production wiring:** Link the R012.2 policy classification and EG-IDENTITY-R008.4 active graph policy to R012.7 activation. Bind review/approval evidence to GRAPHOS-OPS-R050.2/.4 and the concrete SDK governed source effect. EG has no normative filesystem writer.

**Planned acceptance:** `reviewed_normative_source_activates_without_graph_overwrite` — Proposed integration case reviewed_normative_source_activates_without_graph_overwrite: pin a real repository policy and its owner review receipt, ingest and activate through served EG routes, and execute a decision that records the resulting policy revision. Independently compare source bytes and Git revision before/after graph mutation and reasoning. An attempted graph-originated normative write must yield no source effect. Missing/rejected review, a modified blob after review, changed synthesized rules, and a raced invariant/active revision must preserve prior active policy. For a permitted source change, prove actual governed writeback, reread, renewed review, and only then activation; no direct fixture activation substitutes for this path.

**Owner references:**

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:91` — `EG-ORCH-VOCAB-R012.2`

- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/engine-identity-and-scope/requirements.md:16` — `EG-IDENTITY-R008.4`

- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/hosted-api-operations/requirements.md:147` — `GRAPHOS-OPS-R050.2`

- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/hosted-api-operations/requirements.md:149` — `GRAPHOS-OPS-R050.4`

**Unmerged consumer dependencies:**

- None.

**Explicit holds:**

- The concrete producer of the review receipt and normative-file source-effect binding must be owner-confirmed. R050 references establish review/ops boundaries, not proof that the proposed full carrier already exists.

**Owner-review proposal, not a source-established fact:**

- This preserves the accepted authority correction: reviewed files are normative source; EG owns active policy. It does not adopt file-only runtime authority.

Source record: `EG:61`, SHA-256 `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`. Status remains SPECIFIED; `landed_in` and `verified_by` remain empty. No implementation, served receipt, or closure is claimed.

<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R005

Preserve the existing `R005` obligation and all prior children; add `EG-ORCH-VOCAB-R005.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-ORCH-VOCAB-R005.100` — AU:33

Extend the existing orchestration step contract with a nested input schema and explicit source-slot/path mapping for each field. Before AU dispatch, project only declared paths into the step payload. Withhold undeclared sibling/nested fields, even when the coordinator can read the full ParallelWorkspace. Define a shielded-field policy. Bind the projected-payload digest to step ID, schema revision, and source-slot digests. Reuse typed result/schema validation.

Producer/consumer: EG declares typed step projection/shield policy; AU `_dispatch_parallel_batch`/processor applies projection before tool dispatch. DTO extra-field rejection is not data projection.

Source record SHA-256: `1a32741b0a97f0c31cd1a3df4ea31d6eee0069c62c3487c0d8057b1ced4b6cf4`. Verification remains planned until independently served by each named owner.

### Extension of R009

Preserve the existing `R009` obligation and all prior children; add `EG-ORCH-VOCAB-R009.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-ORCH-VOCAB-R009.100` — AU:60

Define trace export reads by trace, parent/run, span, child-agent, and tool-call IDs. Also bind time range, source revision, and redaction profile.

Producer/consumer: AU default_runtime_capabilities/run_agent exports; GraphOS MCP transport carries export; EG contract supports correlated readback. Preserve R008/R024 trace ownership.

Source record SHA-256: `5d53cbdda932456b27c6649360c3051d4ab09f2db42018550b4ee81c785748b4`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner harness analytics producer contracts

The existing `AddNode` RunTrace write and `Uql` query substrates remain authoritative. Owner-batch additions expose three versioned reads/writes over those records. They do not replace AU discovery, GraphOS transport, or existing model/task-class metrics.

`SyncHarnessSessionIndexV1` accepts SDK sync receipts and manifest-pinned source-session identities. `SearchHarnessSessionsV1` returns tenant-scoped sessions with per-harness coverage and snapshot-bound cursors. Identical syncs are idempotent. Conflicts refuse. Missing harness sources remain visibly partial. `RunTraceUsageQueryV1` groups disjoint cached/uncached input and reasoning/nonreasoning output tokens by model, project, and agent. Costs use pinned profiles and currencies; unknown values remain unknown. Target p95 ≤500 ms for one million indexed traces and 32 concurrent readers.

`EvolutionTraceMetricsV1` joins AU proposal/prompt revisions and progress to exact RunTrace identities, metric windows, and stored outcomes. Missing traces yield partial coverage, never zero. Calibration or trace counts are not progress proxies. WebUI R004.7 consumes via GraphOS run reads. Session browse/search and usage totals map to WebUI R006.4 and R004.8. These are SPECIFIED interfaces and future acceptance only.
<!-- /spec-definition-patch -->
