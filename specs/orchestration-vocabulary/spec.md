# EG-ORCH-VOCAB — Orchestration vocabulary

## Dependencies and unresolved owner contracts

- `EG-REPO-INGEST-R009` supplies repository/source relationships; `AU-HARNESS-R024` supplies connected execution spans. Neither row currently defines the AU walkthrough producer input/output binding or the WebUI interactive artifact consumer. Those exact owner requirement IDs and served receipts remain pending coordinator assignment; `EG-ORCH-VOCAB-R013.7` is SPECIFIED and cannot be treated as end-to-end closed.
- `AU-HARNESS-R019.5-.7` are the existing study-gap producer/consumer prerequisites for `EG-CQ-ORCH-R010.7`; they must carry the same repository and revision as `EG-CQ-ORCH-R010.1-.6`. If the AU rows lack a stable proposal id/revision, that missing producer field remains a hold.
- `WEBUI-APPS-R004.7` and `.8` are the named display consumers for `EG-ORCH-VOCAB-R010.100` and `.101`. Exact AU progress-source and SDK harness-manifest/sync-receipt producer IDs, plus the exact GraphOS served-adapter IDs, are not established in this repository evidence and remain explicit holds.
- Existing `EG-DECISION-ENGINE-R036`, `R135.2`, `R135.4`, and `EG-TYPED-PACKS-R006` are prerequisites for `EG-ORCH-VOCAB-R003.8`; they provide the assembly authority, program store, and Agent Library projection.

Owner: epistemic-graph. Cross-repo IDs: none required (consumer is graph-os intent tools via `EG-CQ-ORCH-R009`).
Public provenance: closes the MISSING-term lists in `specs/orchestration-competency` (`EG-CQ-ORCH-001`); spec-only change.

## Purpose and user stories

The competency spec found that seven orchestration questions cannot be asked of the graph because the ontology lacks the terms. This spec designs the complete vocabulary, says which existing mechanism writes each term and how `graph.assemble()` (`EG-DECISION-ENGINE-R036`) reads it. It adds no planner, router, guardrail engine or topology chooser, and no second catalog: packs and the Agent Library stay the content authority (`EG-TYPED-PACKS-R072`).

## Requirements and acceptance

- FR-001: Every missing term listed in `EG-CQ-ORCH-001` exists in the ontology file named in `design.md` (Vocabulary), with domain, range and SHACL constraints stated there.
- FR-002: Each term has exactly one writer among existing mechanisms (typed packs projection, the Agent Library outbox, the template/topology projection, or the assemble commit) and at least one reader named in `design.md` (Wiring).
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

Original objective: The platform monitors latency, token cost and error frequency per run and adjusts model selection and parallelism within declared budgets, recording each adjustment on the trace.

EG-owned residual: Per-run error frequency over a declared window is not the host tenant/child/operation partition. No joint latency/cost/error-driven model-and-width adjustment contract records each before/after model, width, budget and rationale on that run.

Producer/consumer boundary: AU ObservedCost routing.py -> EG routing; AU continuation.py narrows and releases capacity; GraphOS error_budget.decide computes partitioned AIMD. These separate paths have no reviewed joint per-run adjustment producer/consumer contract.

Preserve existing coverage: `AU-CONTROL-R006`, `AU-CONTROL-R019`, `AU-HARNESS-R007`, `EG-ORCH-VOCAB-R009.8`, `EG-ORCH-VOCAB-R009.9`, `GRAPHOS-CAPACITY-R001.1`.

Owner source hash: `8d2f83f788c5fdd36c4dc47f85c9f7673393b6b9075dc46a5ac1c32afd2386e2`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R013.7` — AU:115

Original objective: A codebase query can produce a linear code walkthrough and a self-contained interactive explanation artifact, with execution notes streamed to the graph as provenance.

EG-owned residual: Define the walkthrough, self-contained interactive explanation output and its graph-bound execution-note stream.

Producer/consumer boundary: EG repository hydration supplies source relationships; AU-HARNESS-R024 emits spans; EG-ORCH-VOCAB-R013 admits trace steps through AddNode. Explanation producer, artifact schema and WebUI consumption binding are absent definitions.

Preserve existing coverage: `EG-REPO-INGEST-R009`, `AU-HARNESS-R024`, `EG-ORCH-VOCAB-R013.1`, `EG-ORCH-VOCAB-R013.3`.

Owner source hash: `445704f78ae07d1c4b62f42dd23797823d91bc470f04a9dc779e3f6777fffbf0`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R003.8` — EG:12

Original objective: A team that completes work successfully is promoted to a reusable configuration chosen by similarity to a new task; capabilities are typed nodes carrying handler metadata.

EG-owned residual: Define team-level promotion, task-similarity reuse and typed capability handler metadata.

Producer/consumer boundary: graph.assemble consumes capabilities; Agent Library projects them; compile_program/lookup_program reuse traces. Team similarity selection and handler-metadata consumption are absent definitions.

Preserve existing coverage: `EG-DECISION-ENGINE-R135.2`, `EG-DECISION-ENGINE-R135.4`, `EG-DECISION-ENGINE-R036`, `EG-ORCH-VOCAB-R003.5`, `EG-ORCH-VOCAB-R003.6`.

Owner source hash: `517823a931bb93e173b301a9a31fbe6bd75c14687233c72278a59ced1940673a`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R012.7` — EG:46

Original objective: Constitution and policy files from repositories are ingested and synthesized into graph policy nodes consumed by the reasoning layer; a policy that contradicts a stored invariant is refused.

EG-owned residual: Classification into SpecPolicy is not synthesis of file content into reasoning-consumed policy rules. R129 protects human-override promotion; it does not explicitly guard repository policy ingest/activation against stored invariants.

Producer/consumer boundary: IndexRepository -> classify_spec_file -> SpecPolicy; missing semantic synthesis/activation link -> reasoning policy consumer. Existing check_rule_against_invariants applies at the human-override promotion path.

Preserve existing coverage: `EG-ORCH-VOCAB-R012.1`, `EG-ORCH-VOCAB-R012.2`, `EG-ORCH-VOCAB-R012.3`, `EG-ORCH-VOCAB-R012.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`, `EG-DECISION-ENGINE-R129.3`, `EG-DECISION-ENGINE-R129.4`, `EG-DECISION-ENGINE-R129.5`, `EG-IDENTITY-R008.4`.

Owner source hash: `9de4d3274232cb97a9a85ad17ea20b368fff1bd729bdb4e7b2799991da106c58`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R004.6` — EG:51

Original objective: The capability registry models functional gaps and desired enhancements as typed nodes in a domain taxonomy, queryable by task class.

EG-owned residual: Persistent typed functional-gap and desired-enhancement nodes, their domain-taxonomy relationships and task-class query are unspecified.

Producer/consumer boundary: Assembly unmet-capability AbstainReason -> proposed governed capability-gap record -> task-class registry query. The latter producer/consumer link remains undefined.

Preserve existing coverage: `EG-ORCH-VOCAB-R004`, `EG-DECISION-ENGINE-R010`.

Owner source hash: `ed01ad35731abc6a739b3e1a39ee198549103acf0d30ec51c77316ca7cf75c49`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-ORCH-VOCAB-R012.8` — EG:61

Original objective: Epistemic state lives in the graph while normative state (rules, constitutions) lives in versioned files; the graph never overwrites normative files and normative changes require review.

EG-owned residual: A versioned reviewed normative source may coexist with EG active policy authority. Source-revision-to-active-policy binding and the graph no-overwrite invariant are not explicit in these rows. File-only runtime authority is not adopted.

Producer/consumer boundary: Versioned repository policy source -> GraphOS approved engine policy port -> EG active policy node; source mutation separately uses governed SDK effect.

Preserve existing coverage: `EG-IDENTITY-R008.4`, `EG-ORCH-VOCAB-R012.2`, `GRAPHOS-OPS-R050.2`, `GRAPHOS-OPS-R050.4`, `EG-ORCH-VOCAB-R012.5`, `EG-ORCH-VOCAB-R012.6`.

Owner source hash: `c5f1dfe7867267169ebc8c75abba87a1d9ed47568a65cbfe1bf6d183b8026e59`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R005

Preserve the existing `R005` obligation and all prior children; add `EG-ORCH-VOCAB-R005.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-ORCH-VOCAB-R005.100` — AU:33

Extend the existing orchestration step contract with a nested input schema and explicit source-slot/path mapping for each field. Before AU dispatch, project only declared paths into the step payload; undeclared sibling/nested fields are withheld even when the full ParallelWorkspace is readable by the coordinator. Define a shielded-field policy and bind the projected-payload digest to step id, schema revision and source-slot digests. Reuse typed result/schema validation.

Producer/consumer: EG declares typed step projection/shield policy; AU `_dispatch_parallel_batch`/processor applies projection before tool dispatch. DTO extra-field rejection is not data projection.

Source record SHA-256: `1a32741b0a97f0c31cd1a3df4ea31d6eee0069c62c3487c0d8057b1ced4b6cf4`. Verification remains planned until independently served by each named owner.

### Extension of R009

Preserve the existing `R009` obligation and all prior children; add `EG-ORCH-VOCAB-R009.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-ORCH-VOCAB-R009.100` — AU:60

Define trace export read data by trace id, parent/run id, span ids, child-agent ids, tool-call ids, time range, source revision and redaction profile. Child spans correlate under the parent run and a consumer can retrieve a complete export snapshot with pagination/completeness metadata and stable digest. This is an EG trace correlation/export-read contract only: AU owns span export and GraphOS owns MCP transport to the configured backend.

Producer/consumer: AU default_runtime_capabilities/run_agent exports; GraphOS MCP transport carries export; EG contract supports correlated readback. Preserve R008/R024 trace ownership.

Source record SHA-256: `5d53cbdda932456b27c6649360c3051d4ab09f2db42018550b4ee81c785748b4`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner harness analytics producer contracts

The existing `AddNode` RunTrace write and `Uql` query substrates remain authoritative. The owner-batch additions expose three versioned reads/writes over those records; they do not replace AU discovery, GraphOS transport, or the existing model/task-class metrics.

`SyncHarnessSessionIndexV1` accepts SDK sync receipts and manifest-pinned source session identities; `SearchHarnessSessionsV1` returns tenant-scoped sessions with per-harness coverage and snapshot-bound cursors. Repeated identical sync is idempotent, conflicts are refused, and missing harness sources stay visibly partial. `RunTraceUsageQueryV1` groups non-overlapping cached/uncached input and reasoning/nonreasoning output token categories by model, project and agent. Costs use pinned accounting profiles and currencies, with unknown values preserved. It targets p95 ≤500 ms at one million indexed traces and 32 concurrent readers.

`EvolutionTraceMetricsV1` joins AU proposal/prompt revisions and progress records to exact RunTrace identities, metric windows and stored outcomes. Missing traces produce partial coverage, never a zero or a proxy based on calibration or trace counts. Its served consumer is WebUI R004.7 through GraphOS runs reads; session browse/search and usage totals map to WebUI R006.4 and R004.8. These are SPECIFIED interfaces and future acceptance only.
<!-- /spec-definition-patch -->
