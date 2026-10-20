# EG-ORCH-VOCAB — Orchestration vocabulary

## Dependencies and unresolved owner contracts

- `EG-REPO-INGEST-R009` defines repository/source relationships; `AU-HARNESS-R024` supplies connected execution spans. Neither row currently defines the AU walkthrough producer input/output binding or the WebUI interactive artifact consumer. Those exact owner requirement IDs and served receipts remain pending coordinator assignment; `EG-ORCH-VOCAB-R013.7` is SPECIFIED and cannot be treated as end-to-end closed.
- `AU-HARNESS-R019.5-.7` are the existing study-gap producer/consumer prerequisites for `EG-CQ-ORCH-R010.7`; they must carry the same repository and revision as `EG-CQ-ORCH-R010.1-.6`. If the AU rows lack a stable proposal id/revision, that missing producer field remains a hold.
- `WEBUI-APPS-R004.7` and `.8` are named consumers for R010.100 and R010.101. AU progress-source and SDK harness-manifest/sync-receipt producer IDs remain unknown. Exact GraphOS served-adapter IDs are also unknown. These remain explicit holds.
- Existing `EG-DECISION-ENGINE-R036`, `R135.2`, `R135.4`, and `EG-TYPED-PACKS-R006` are prerequisites for `EG-ORCH-VOCAB-R003.8`; they define the assembly authority, program store, and Agent Library projection.
- `EG-ORCH-VOCAB-R801` and `-R802` define the versioned harness component and the improvement library. `EG-DURABLE-KERNEL-R078` and `-R081` own the bitemporal stamps they use. `EG-DECISION-ENGINE-R803` consumes the contrast. `AU-HARNESS-CYCLE-R001` is the producer. `ARBORA-OPS-R068` serves the reads.

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

Original source objective: The platform monitors latency, token cost, and error frequency per run. It adjusts model selection and parallelism within declared budgets. It records each adjustment on the trace.
Original intake fields are recorded exactly in `requirements.md` under R009.18. The source record and its digest are authoritative there.

**Contract definition:** EG SHALL accept one revisioned per-run observation window. It binds run/window IDs and times. It carries latency samples with units, error count, and eligible-attempt denominator. It carries token usage and measured cost with currency/rate revision. It binds current model/profile, parallel width, consumed/remaining budgets, and host-capacity receipt. Missing samples or rates SHALL remain unknown. A zero denominator, stale window, or mismatched run revision SHALL also remain unknown. Unknown values SHALL NOT become zero-cost or zero-error evidence. A versioned adjustment policy SHALL state latency/error thresholds and model-adequacy/profile inputs. It SHALL also state cost-prediction basis and deterministic tie breaks. EG SHALL jointly evaluate model and width against these observations and remaining budget. It SHALL return a committed feasible pair or typed abstention/stop. It SHALL trace before/after model, width, budget, window, policy, evidence, and reason. Mid-run continuation SHALL obey AU-CONTROL-R019. Width may narrow and release capacity; it SHALL NOT widen. Later-run allocation requires distinct admission and capacity grant.

**Production wiring:** Feed proposed AU `RunAdjustmentObservation` from existing routing and continuation paths to `Method::Decide`. Reuse R136 model adequacy and stage allocation. Reuse R009.17 RunTrace ingestion/readback. Persist each adjustment and abstention before application. GraphOS remains capacity authority.

**Planned acceptance:** `run_observations_change_model_and_width_with_trace` exercises actual AU routing/continuation and served EG Decide. It uses identical budgets with controlled joint latency/cost/error windows. Show policy-directed model and width changes. Show budget-forced refusal/stop and exact before/after trace readback. Reject stale windows and invalid rates/denominators. Confirm unknown values stay nonzero-unknown, not zeros. Show narrowing releases capacity. Reject mid-run widening. A host-only AIMD or model-only allocator test is insufficient.

Preserve existing coverage: `AU-CONTROL-R006`, `AU-CONTROL-R019`, `AU-HARNESS-R007`, `EG-ORCH-VOCAB-R009.8`, `EG-ORCH-VOCAB-R009.9`, `ARBORA-CAPACITY-R001.1`.

Source record SHA-256: `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`. Owner references:
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:10` — `AU-CONTROL-R006`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:24` — `AU-CONTROL-R019`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:298` — `EG-DECISION-ENGINE-R136.3`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:69` — `EG-ORCH-VOCAB-R009.17`
- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/adaptive-capacity/requirements.md:6` — `ARBORA-CAPACITY-R001.1`

Open owner prerequisites:
- AU-CONTROL-R006.30 is unmerged. Policy thresholds, predictive cost inputs, and tie breaks require an owner-versioned policy. No tuning defaults or implementation claims come from the source.

Unmerged consumer dependencies:
- `AU-CONTROL-R006.30` (draft_unmerged; observed main `50c313ab01871ff7e10aee2a9d0254fa55d71f07`): Local drafting payload; not a merged producer or implementation receipt.

Owner-review choices (not source-established facts):
- The joint carrier and policy-required fields make missing decisions explicit. A self-referential command to adjust appropriately is not a substitute for that published policy.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

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

Original source objective: Repository constitution and policy files are ingested and synthesized as graph policy nodes for reasoning. A policy that contradicts a stored invariant is refused.

**Contract definition:** IndexRepository SHALL bind each classified constitution/policy source to repository, path, commit, blob digest, and source spans. It SHALL synthesize typed GuardrailRule candidates linked to those spans. The synthesis profile SHALL be versioned and state its supported rule grammar. Unparsed or ambiguous clauses SHALL remain inactive with explicit reasons. Source prose SHALL NOT self-activate. Before activation, EG SHALL compare every reviewed rule against the pinned invariant generation using R129.3. EG SHALL validate review binding under R012.8 and compare expected active-policy revision. A contradiction SHALL name invariant IRI, candidate rule, and source clause. Stale generation/review or unsupported clauses SHALL refuse and preserve the active set. Successful activation SHALL atomically publish candidate-set digest and policy revision. Later reasoning SHALL load the active graph policy and record its revision in decision evidence.

**Production wiring:** Extend existing source_ingest router and SourceIngest after `classify_spec_file`. Reuse GuardrailRule storage and `check_rule_against_invariants`. Link activation to the decision policy under EG-IDENTITY-R008.4. R129.5 human-override guarding alone does not guard repository policy.

**Planned acceptance:** `repository_policy_is_synthesized_guarded_and_used` sends a real policy through IndexRepository. Inspect typed candidates and source spans. Approve the exact digest through owner review. Activate it and dispatch a served decision proving rule use. A conflicting rule must name the invariant and preserve prior active revision. Also test unparsed prose, missing review, changed source digest, and invariant-generation race. Classification or a pure invariant-helper test is insufficient.

Preserve existing coverage: `EG-ORCH-VOCAB-R012.1`, `EG-ORCH-VOCAB-R012.2`, `EG-ORCH-VOCAB-R012.3`, `EG-ORCH-VOCAB-R012.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`, `EG-DECISION-ENGINE-R129.3`, `EG-DECISION-ENGINE-R129.4`, `EG-DECISION-ENGINE-R129.5`, `EG-IDENTITY-R008.4`.

Source record SHA-256: `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:91` — `EG-ORCH-VOCAB-R012.2`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/engine-identity-and-scope/requirements.md:16` — `EG-IDENTITY-R008.4`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:247` — `EG-DECISION-ENGINE-R129.3`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:249` — `EG-DECISION-ENGINE-R129.5`

Open owner prerequisites:
- The supported declarative grammar/synthesis profile and concrete review receipt producer must be published and bound. Unstructured text synthesis may produce candidates, but cannot silently fill those authority gaps.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- Atomic publication is confined to the active graph policy set; it does not imply atomic cross-repository filesystem effects.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-ORCH-VOCAB-R004.6` — EG:51

Original source objective: The capability registry models functional gaps and desired enhancements as typed nodes in a domain taxonomy. Queries can filter by task class.

**Contract definition:** EG SHALL persist distinct typed CapabilityGap and DesiredEnhancement nodes. Each node binds tenant, ID/revision, capability, task class, domain taxonomy, source evidence/digest, owner, and disposition. Edges SHALL link each node to capability, task class, and domain. A request-local unmet-capability abstention is evidence only; it is not itself a durable CapabilityGap. A separately admitted gap may cite that abstention. An enhancement records a requested improvement without asserting absence. Governed mutation SHALL validate references, ownership, and expected revisions. It SHALL deduplicate repeated source events. Unknown taxonomy/task-class refs SHALL refuse; EG SHALL NOT create arbitrary terms. UnifiedQuery SHALL return these types by task class. Matching SHALL state exact or taxonomy-descendant mode and pinned taxonomy revision. Authorization SHALL precede rows, counts, and pagination. A gap/enhancement node SHALL NOT mark capability implemented, admitted, or tested.

**Production wiring:** Extend existing orchestration ontology/shapes and governed AddNode/ChangeEnvelope. Reuse the existing task-class/domain taxonomy and `Method::UnifiedQuery`; do not add a second capability registry. Keep the EG:7 owner-export manifest question separate from these domain nodes.

**Planned acceptance:** `capability_gap_and_enhancement_are_queryable` records a gap from served abstention and enhancement from a distinct reviewed request. Use the governed mutation route. Query two task classes via generated UnifiedQuery. Restart and repeat. Independently read typed fields, edges, and provenance. Confirm deduplication, revision conflict, invalid taxonomy refusal, exact/descendant behavior, denied-tenant counts/cursors, and unchanged capability state. In-memory fixtures alone fail acceptance.

Preserve existing coverage: `EG-ORCH-VOCAB-R004`, `EG-DECISION-ENGINE-R010`.

Source record SHA-256: `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:24` — `EG-ORCH-VOCAB-R004`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/decision-engine/requirements.md:16` — `EG-DECISION-ENGINE-R010`

Open owner prerequisites:
- The authoritative domain taxonomy/task-class resolver and admitted mutation producer must be bound to existing owner contracts. Do not fabricate a new owner role-export ID to cover the unrelated EG:7 hold.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- Distinct gap/enhancement types and explicit query match mode are proposed schema additions.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-ORCH-VOCAB-R012.8` — EG:61

Original source objective: Epistemic state and active policy live in the graph. Normative rules and constitutions live in versioned files. Graph writes never overwrite them. Normative changes require review.

**Contract definition:** EG SHALL retain epistemic state and active runtime policy in the graph. It SHALL bind normative source to reviewed, versioned repository files. Each activated binding SHALL include repository, path, commit, blob digest, and candidate-set digest. It SHALL also include review actor/receipt digest, invariant generation, and expected active-policy revision. Review SHALL cover exact source and synthesized-rule digests. Stale, absent, rejected, or mismatched review SHALL refuse before active-policy mutation. EG mutations and reasoning SHALL NOT overwrite normative files. Source changes SHALL use governed SDK writeback and GraphOS approval. Then reread the committed revision and review it before graph-policy activation. A graph-policy edit SHALL NOT rewrite a file or make an unreviewed file runtime authority. R012.7 invariant checks and generation comparison apply at activation.

**Production wiring:** Link R012.2 policy classification and EG-IDENTITY-R008.4 active graph policy to R012.7 activation. Bind review evidence to ARBORA-OPS-R050.2/.4 and the concrete SDK source effect. EG has no normative filesystem writer.

**Planned acceptance:** `reviewed_normative_source_activates_without_graph_overwrite` pins a real policy and owner review receipt. Ingest and activate through served EG routes. Dispatch a decision recording policy revision. Compare source bytes and Git revision before/after graph mutation and reasoning. Graph-originated normative writes must have no source effect. Missing/rejected review, modified blob, changed synthesized rules, or raced revisions preserve prior active policy. A permitted source change requires actual governed writeback, reread, renewed review, then activation. Direct fixture activation is insufficient.

Preserve existing coverage: `EG-IDENTITY-R008.4`, `EG-ORCH-VOCAB-R012.2`, `ARBORA-OPS-R050.2`, `ARBORA-OPS-R050.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`.

Source record SHA-256: `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/orchestration-vocabulary/requirements.md:91` — `EG-ORCH-VOCAB-R012.2`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/engine-identity-and-scope/requirements.md:16` — `EG-IDENTITY-R008.4`
- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/hosted-api-operations/requirements.md:147` — `ARBORA-OPS-R050.2`
- `graph-os@069eebc785afb9fb26bbbcc907cf5a2a4bcab5c5:specs/hosted-api-operations/requirements.md:149` — `ARBORA-OPS-R050.4`

Open owner prerequisites:
- The concrete producer of the review receipt and normative-file source-effect binding must be owner-confirmed. R050 references establish review/ops boundaries, not proof that the proposed full carrier already exists.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- This preserves the accepted authority correction: reviewed files are normative source; EG owns active policy. It does not adopt file-only runtime authority.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

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
