# EG-REPO-INGEST — Repository index and native ingestion

Status: **PROPOSED**. Delivery: **PARTIAL SOURCE PRESENT**. Acceptance: **NOT VERIFIED**. Owner: `epistemic-graph`.

**State legend:** `PROPOSED` means the build contract awaits review; `PARTIAL SOURCE PRESENT` means some implementation exists but this complete contract is not merged; `LANDED` requires an exact merged engine revision; `ACCEPTED` requires every applicable test and release receipt in [test-spec.md](test-spec.md). `NOT VERIFIED` means the latter receipts have not been collected. A prior row's `BUILT` label is historical source evidence, not whole-spec acceptance.

## Purpose

A contributor can index a repository snapshot into a durable, queryable graph without an application-specific graph writer. A caller supplies authenticated, immutable source bytes and revision membership; the engine determines parse outcomes, resolves symbols across files, records provenance, commits the projection once, and reports enough evidence to retry failures. A query can find the exact source occurrence and revision behind an answer. This contract also defines the EG side of general source ingestion; it does not require access to any private workspace or service.

## Users and acceptance stories

1. **Repository contributor (P1):** indexing two refs that share a blob parses the blob once and keeps distinct branch membership. A changed path, rename, deletion, and re-index are reflected without losing historical revision identity.
2. **Code intelligence consumer (P1):** a symbol query returns distinct declaration occurrences, deterministic in-batch `calls`, `inherits`, `realizes`, and `depends_on` links, and an exact repository/ref/revision/path/blob/line chain. Ambiguous or external references remain explicitly unresolved.
3. **Source operator (P1):** a partially unsupported or malformed source batch yields per-file disposition; committed files and their receipts are replayable. A failed batch cannot advance a checkpoint or leave a partial graph. Budget refusals have a stable code the caller can use to split and retry.
4. **Enrichment consumer (P2):** native parse and resolution commit promptly. Expensive enrichment is admitted by engine policy and placed in a durable queue, with a budget and visible terminal result. A query may request bounded on-demand enrichment without making the original index commit depend on an LLM or model service.

## Functional requirements

| ID | Requirement and acceptance rule |
|---|---|
| RI-01 | `IndexRepository` accepts a graph-scoped `IndexRepositoryScope` with repository ID, immutable Git commit IDs, ref status, file-version memberships and tombstones. It rejects malformed logical paths, invalid digests, duplicate or contradictory membership, and a ref that claims a mutable revision. The authenticated tenant and graph authorization come from the request boundary, never request fields. |
| RI-02 | One unique blob is parsed once per batch; the same bytes can belong to many paths and refs. `Blob`, `FileVersion`, and `Branch` graph entities preserve both content and occurrence identity. Declaration IDs distinguish identical text at different paths/offsets. Repeated input order or hash-map iteration does not change canonical IDs or the commit digest. |
| RI-03 | Every submitted blob returns one ordered `IndexFileOutcome` with `success`, `unsupported`, or `error`, canonical content and parser-capability digests, and bounded machine-readable diagnostics. Empty but valid source is `success`; unsupported language is never silently treated as empty success. Failed or unsupported blobs remain eligible for later reparse when parser capability changes. |
| RI-04 | Cross-file resolution uses a deterministic symbol table and emits only edges whose endpoints exist in the batch or admitted graph. Ambiguous imports/calls remain unresolved with counts and diagnostics; no guessed `depends_on` or `similar_to` edge may be committed. Model-free similarity carries algorithm/version and score, and must be repeatable for identical input. |
| RI-05 | A scoped batch lowers to one governed `ChangeEnvelope` and one durable graph transaction, including graph projection, source references, privacy policy, provenance, tombstones, and enrichment intent. Identical replay returns the original committed result without graph-version advancement. A crash before the commit can leave CAS content but cannot create partial graph rows or a success receipt. |
| RI-06 | A tombstone removes only the named ref/path/blob membership. A rename records old membership removal and new membership addition; shared blob and other refs remain valid. Deleting a ref removes its live membership without erasing immutable historical revisions. |
| RI-07 | Repository source bytes and source-relative paths are scoped to the verified tenant and repository. Graph reads and writes enforce graph ACL. Raw host identity is redacted before durable projection; code punctuation and source-relative paths are preserved. Untrusted source cannot supply policy, graph mutations, ownership, or tenant. |
| RI-08 | An over-limit batch is refused atomically with `REPOSITORY_BATCH_TOO_LARGE` before any graph mutation. The response exposes enough bounded detail for a caller to halve and resend. The same code covers operation and durable-record byte/item ceilings; limit changes are versioned in the generated contract. |
| RI-09 | Native rungs 0–2 (parse/AST, cross-file resolution, model-free statistics) finish without an external model call. Rungs 3–5 are represented as durable, budgeted work items with admission, lease, retry/park, completion and provenance. Underfunded or unauthorized work fails closed. Query-triggered enrichment is bounded and does not mutate an earlier revision. |
| RI-10 | A canonical semantic bridge maps repository entities and their source/test/spec/release relationships into the engine graph without duplicate writer authority. Each edge has source evidence and a stable schema/version. A missing target becomes an unresolved relation, not an invented entity. |
| RI-11 | `SourceIngest` accepts typed observations with manifest references, raw evidence, exact expected checkpoint, and mode (`full`, `delta`, `reconcile`). EG resolves approved mappings and commits graph changes, live-set marker, tombstones, lineage, checkpoint and receipt atomically. Empty authoritative reconciliation requires the explicit approval and capability. `SourceIngestStatus` reads the durable checkpoint after restart. |
| RI-12 | Change consumers apply source-position-idempotent graph/index/outbox updates. A Debezium-style change bridge maps an ordered source event into `ChangeEnvelope`, preserves source position and deletion semantics, and refuses unknown schema or out-of-order replay. Any accelerated read declares freshness; read-your-writes waits for the caller's committed position within a bounded timeout or reports stale. |

## Success criteria

- **SC-01:** A two-ref fixture with a shared blob, divergent blob at one path, rename, deletion and restart gives exact membership and occurrence counts; a duplicate submission leaves graph version unchanged.
- **SC-02:** Each input has exactly one parse outcome. A parser upgrade reparses an earlier unsupported/error blob without duplicating a successful revision.
- **SC-03:** A real public repository benchmark records wall time, repository size, parser distribution and p50/p95 commit latency for native rungs. The performance target is **to be set from a checked-in benchmark baseline before the performance claim is accepted**; no absolute throughput is inferred from tiny fixtures.
- **SC-04:** Concurrent, stale, oversized, unauthorized and interrupted writes fail with stable codes and leave unchanged graph version/checkpoint. Restart reads the last committed receipt.
- **SC-05:** A code query can traverse `repository → ref → revision → file version → blob → symbol occurrence → evidence`, then follow resolved dependency and spec/test/release edges without consulting another data store.

## Boundaries and interfaces

EG owns `Method::IndexRepository`, `IndexRepositoryScope`, `IndexResult`, `SourceIngest`, `ChangeEnvelope`, graph projection, CAS binding, native parsing/resolution, admission, durable work and queryable lineage. The connector SDK owns provider authentication, immutable snapshot fetch, paging, checksum validation and transport; it may retry based on EG's typed outcomes but cannot mint graph facts or checkpoint authority. Agent applications may request ingestion and consume receipts but cannot bypass EG's projection. Repository tooling may discover repositories and provide authenticated source coordinates, but is not a second graph writer.

This feature does not require a hosted Git provider, private manifest, local service inventory, model service or running cluster for its deterministic development tests. Served multi-node and provider integration remain separate release evidence.

## Traceability and evidence state

The IDs below are stable migration identifiers, not links to a private source. Their full acceptance contract is the requirements above and the local design/test/tasks files. Historical `BUILT` or `LANDED` labels do not establish acceptance for this combined feature.

| Requirement | Stable IDs | Current evidence | Required acceptance proof |
|---|---|---|---|
| RI-01–RI-06 | EG-REPO-INGEST-R009, EG-REPO-INGEST-R006 | Branch-aware types, parser, durable index tests exist | Exact-head public API, reparse, identity and restart tests |
| RI-07–RI-08 | EG-REPO-INGEST-R007, EG-REPO-INGEST-R008 | Tenant and budget guards exist in parts | Cross-principal refusal, large-batch and generated-client contract |
| RI-09 | EG-REPO-INGEST-R006, EG-REPO-INGEST-R004, EG-REPO-INGEST-R005 | Native rung evidence and an enrichment consumer exist | Durable admission/lease/restart, bounded query and throughput receipt |
| RI-10 | EG-REPO-INGEST-R009, EG-REPO-INGEST-R002, EG-REPO-INGEST-R003 | Repository graph projection exists | Semantic bridge with complete source/spec/test/release chain |
| RI-11 | EG-REPO-INGEST-R001, EG-REPO-INGEST-R002 | Native `SourceIngest` exists | Mapping, checkpoint, replay and fresh-store tests |
| RI-12 | Shared interface; owned by `unified-data-plane` | Source-position design required | Idempotent consumer, freshness and routing tests |

Foreign-source scope EG-FEDERATED-QUERY-R013 belongs to `federated-query-and-reasoning`; UDF/lifecycle isolation EG-DURABLE-KERNEL-R018/EG-DURABLE-KERNEL-R019 belongs to `durable-graph-kernel`. The source change consumers, freshness router and Debezium bridge EG-UNIFIED-DATA-PLANE-R009/EG-UNIFIED-DATA-PLANE-R011/EG-UNIFIED-DATA-PLANE-R021 belong to `unified-data-plane`. This spec uses those contracts for repository indexing without claiming their implementation authority.

The scope above does not claim coverage of independent engine work such as layout upgrades, community detection, domain-specific connectors, query grammar, or unrelated quality regressions. Those concerns require their own owner specs and acceptance evidence.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.

<!-- spec-definition-patch:owner-batch-20261012-repository-index-and-ingestion -->
## Reviewed owner residuals — 2026-10-12

### `EG-REPO-INGEST-R002.1` — AU:23

Original source objective: A skill document's procedural steps compile into an executable plan with no separate workflow file, and the graph references the skill directory instead of copying it.
Original intake fields (exact source record array): ["AU-CONTROL","A skill document's procedural steps compile into an executable plan with no separate workflow file, and the graph references the skill directory instead of copying it.","mechanism","","prompts/plans/brain_plan_329dec44...v1_20260525_002700.md","MEDIUM","WRITE"]

**Contract definition:** SourceIngest skill_workflow_ingest SHALL compile the procedural steps in a pinned SKILL.md into a typed TaskPlan without requiring a separate workflow file. Each admitted step SHALL have a unique step_id, source span, instruction digest, capability identifier, typed inputs and outputs, and dependency identifiers. The compiler SHALL resolve capabilities and input/output bindings before producing a plan, order independent steps by their source order, and refuse missing or duplicate identifiers, unresolved dependencies or capabilities, cycles, unsupported types, or absent source spans. The compilation receipt SHALL bind the skill-directory reference, document revision/digest, grammar revision, plan revision/digest, and tool/type digests. The graph SHALL store the directory reference and compilation provenance rather than a copy of the skill document. A changed document or capability/type digest SHALL invalidate the old binding before execution; unstructured prose SHALL NOT silently become an executable operation.

**Production wiring:** Attach compilation to the existing SourceIngest skill_workflow_ingest capability. The proposed AU-CONTROL-R037.30 binding enters the existing graph_orchestrate → gap/plan selection → execute_workflow path; do not add a parallel document executor.

**Planned acceptance:** Proposed integration case skill_document_alone_compiles_and_executes: provide a skill directory containing SKILL.md and its permitted assets, with no workflow file. Invoke real SourceIngest, fetch the resulting plan through AU graph_orchestrate, then run execute_workflow and verify independently observed step effects and preserved data bindings. Mutate the document between compile and run and require refusal before the affected step. Exercise duplicate step IDs, a cycle, unknown capability, wrong binding type, and a missing span. Verify the graph contains the directory reference and receipt, not copied document text.

Preserve existing coverage: `AU-HARNESS-R022.2`, `AU-CONTROL-R037.3`, `AU-CONTROL-R037.5`, `EG-REPO-INGEST-R002`.

Source record SHA-256: `3f7e133169a76ce2680ec667c28ea2c5543354ed8874d258c432e65346f2e529`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:6` — `EG-REPO-INGEST-R002`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:111` — `AU-HARNESS-R022.2`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:59` — `AU-CONTROL-R037.3`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:61` — `AU-CONTROL-R037.5`

Open owner prerequisites:
- The versioned SKILL.md procedural grammar and authoritative capability/type lookup must be published by their owners before implementation can satisfy this clause. AU-CONTROL-R037.30 is unmerged; document compilation is not proven by the SourceIngest capability name.

Unmerged consumer dependencies:
- `AU-CONTROL-R037.30` (draft_unmerged; observed main `50c313ab01871ff7e10aee2a9d0254fa55d71f07`): Local drafting payload; not a merged producer or implementation receipt.

Owner-review choices (not source-established facts):
- Step fields, deterministic source-order tie breaking, and compilation receipt are proposed contract details. No particular Markdown grammar is claimed already chosen.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-REPO-INGEST-R006.1` — AU:70

Original source objective: Ingestion throughput is profiled per stage against a target of 2000 papers per day; maintenance backlog purge and per-goal budget allocation are prioritized when throughput falls below target.

**Contract definition:** EG SHALL measure distinct canonical paper IDs that completed every required ingestion stage in the half-open rolling UTC window [window_end minus 86400 seconds, window_end). The target SHALL be 2000 completed papers per such window. Stage receipts SHALL record stage/revision, attempted and distinct completed IDs, duplicate count, elapsed time, and source/commit evidence. Retries SHALL NOT increase the completed-paper count. A partial or stale window SHALL be reported as unknown and SHALL NOT be compared as a full day. Below target, the committed decision SHALL name the count, shortfall, stage bottleneck evidence, and ordered actions: first assess safe maintenance candidates, then allocate the remaining declared budget across identified goals. A purge candidate SHALL be terminal, unleased, and non-authoritative, with retained durable source/decision receipts. Active work, authoritative records, and the last source copy SHALL be ineligible. Each action SHALL carry owner authorization, expected revision, cost ceiling, and admission verdict; missing authority or policy inputs SHALL refuse it before effects.

**Production wiring:** Read stage and work-item receipts from the native SourceIngest/deferred-work path under R006. EG owns the shortfall decision and per-goal allocation; AU observes the result; GraphOS executes admitted maintenance. Keep its concrete operation ID and producer/export binding as a dependency until that owner publishes them.

**Planned acceptance:** Proposed integration case paper_shortfall_drives_admitted_maintenance: ingest a known 24-hour stream through real stage completion and durable work queues with duplicate attempts, failures, and boundary timestamps. Independently count 1999 then 2000 unique completed papers and verify trigger/no-trigger decisions. Inspect goal allocations and their aggregate budget bound. Send admitted maintenance through the actual GraphOS owner route and read its result receipt. Active leases, changed revisions, the last source copy, missing authorization, and stale telemetry must cause no purge. An incomplete window must be unknown. Queue-only or planner-only fixtures do not establish completion.

Preserve existing coverage: `AU-RETIRE-R005`, `EG-REPO-INGEST-R006`.

Source record SHA-256: `902f964dd909186f3a31d1f8a84d146b715446e64dfeec62480c8151baa06996`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:21` — `EG-REPO-INGEST-R006`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/au-engine-duplicate-retirement/requirements.md:14` — `AU-RETIRE-R005`

Open owner prerequisites:
- The owner-selected stage set, window-freshness policy, per-goal allocation objective/tie-break policy, and GraphOS maintenance operation/authorization contract are not established in the reviewed source. Do not invent IDs, budget defaults, or a purge executor; publish these as explicit prerequisites.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- Distinct end-to-end completions and the exact window define the proposed throughput oracle. Stage telemetry remains separate from the end-to-end numerator.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-REPO-INGEST-R006.2` — AU:92

Original source objective: After a sync, embeddings are updated asynchronously with an importance penalty so high-importance features resist change; consolidation never runs in the routing loop.

**Contract definition:** After a successful sync commit, EG SHALL queue a durable re-embed job bound to the committed source revision, embedding space/model revision, old vector digest, and importance-policy revision. The worker SHALL form an importance-regularized candidate outside the routing loop. Proposed squared_l2_anchor_v1 uses L(z)=sum((z_i-p_i)^2)+lambda*sum(w_i*(z_i-o_i)^2), where p is the proposed vector, o is the old vector, finite w_i are nonnegative, and finite lambda is strictly positive; its coordinate result is (p_i+lambda*w_i*o_i)/(1+lambda*w_i). All vectors and weights SHALL have the same declared dimension and coordinate system. No silent rescaling or normalization SHALL follow this calculation. Missing/stale importance evidence, incompatible spaces, nonfinite values, or a superseded source/old-vector revision SHALL refuse before promotion. The candidate SHALL remain shadow data until the existing recall gate passes; failure SHALL retain the prior active version. Query routing SHALL perform neither this consolidation nor its embedding/model calls.

**Production wiring:** Queue through EG-FEDERATED-QUERY-R077.3. Gate promotion through EG-UNIFIED-DATA-PLANE-R040.2/.3 before EmbeddingVersionStore::tag_batch; read the embedding binding in the existing worker path. R040.3 expressly records that the production tag caller must be created. EG-FEDERATED-QUERY-R040.3 is not this gate.

**Planned acceptance:** Proposed integration case sync_importance_penalty_is_async_and_gate_bound: commit through the real sync path, drain its durable re-embed worker, and compare equal proposed coordinate displacements under low and high importance; higher importance must produce less displacement. Check exact candidate math, provenance, idempotent job handling, and the existing promotion gate before tag_batch. Stale jobs, absent importance evidence, and failed recall probes must leave the active binding unchanged. Instrument the served routing path and prove zero consolidation/embedding calls while the deferred job is pending and after completion.

Preserve existing coverage: `EG-REPO-INGEST-R006`, `EG-UNIFIED-DATA-PLANE-R040.2`, `AU-CONTEXT-R002`.

Source record SHA-256: `771acc731a6e25746641adf30cde9ec91f4eb56edd6597f2d308a42947a3abb3`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:21` — `EG-REPO-INGEST-R006`
- `agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-context-and-finance/requirements.md:6` — `AU-CONTEXT-R002`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/federated-query-and-reasoning/requirements.md:95` — `EG-FEDERATED-QUERY-R077.3`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/unified-data-plane/requirements.md:153` — `EG-UNIFIED-DATA-PLANE-R040.2`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/unified-data-plane/requirements.md:154` — `EG-UNIFIED-DATA-PLANE-R040.3`

Open owner prerequisites:
- The authoritative per-coordinate importance producer, revisioned export, and compatible embedding-space policy are not identified by the source. This is an explicit producer hold. The mathematical profile is proposed for owner acceptance; do not claim it was specified by the original source or already implemented.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- squared_l2_anchor_v1 is a precise proposed penalty, not a paper-derived claim. If the owner chooses a different profile, it must publish an equally exact objective and monotonic resistance oracle; generic importance weighting is insufficient.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-REPO-INGEST-R006.3` — EG:54

Original source objective: The graph exposes structured code maps (modules, symbols, call and dependency relations) so an agent can ask where a behavior lives; stale maps are refused after source change.

**Contract definition:** The existing unified query surface SHALL accept a typed code-map query containing tenant, repository identity, current source revision, behavior text or symbol selector, requested relation kinds, and bounded page limits. Its result SHALL bind the map generation, source-inventory revision/digest, query digest, and authorization scope, and contain module/symbol entity IDs with language, path, source span, and source revision plus typed contains, calls, and depends_on edges. Behavior matches SHALL resolve to indexed source entities and cited spans; unsupported or unresolved relations SHALL be marked incomplete rather than invented. Authorization SHALL precede results, counts, and cursors. Every page SHALL compare the map binding with the authoritative current repository source revision; a changed or unknown revision SHALL refuse as stale before serving map data, including an unchanged symbol from the older generation. A cursor SHALL bind the same map, query, and authorization revisions.

**Production wiring:** Produce maps in the native IndexRepository/SourceIngest path and consume them through Method::UnifiedQuery and its existing unified execution path. Add a typed code-map query/result mode there; do not add a separate search service or use an LLM answer as authoritative code-map evidence.

**Planned acceptance:** Proposed integration case current_code_map_locates_behavior_and_refuses_old_generation: index a repository through the served route, query behavior and a symbol through the generated unified client, and independently check modules, symbols, call/dependency edges, and source spans. Modify one source file; both changed-symbol and unchanged-symbol queries plus the old cursor must refuse until a complete new map is published. Reindex and verify changed/deleted/renamed entities and unchanged logical entity identities against current files. Exercise unauthorized tenants, changed authorization, bounded pagination, and an unresolved relation without fabricated certainty.

Preserve existing coverage: `EG-FEDERATED-QUERY-R017`, `EG-REPO-INGEST-R006`, `EG-REPO-INGEST-R009`.

Source record SHA-256: `e69e6fa3a5c26ae763809d600cb7d264d028b9ce0d80f8ca6141e1a40cf0638e`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:21` — `EG-REPO-INGEST-R006`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:24` — `EG-REPO-INGEST-R009`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/federated-query-and-reasoning/requirements.md:21` — `EG-FEDERATED-QUERY-R017`

Open owner prerequisites:
- Bind the authoritative current source-inventory producer and existing code-entity ID scheme before implementation. An optional historical query must be separately explicit; it cannot satisfy the current-map guarantee.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- The request/result carrier and cursor bindings are proposed additions to the existing query surface, not current implemented API claims.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

### `EG-REPO-INGEST-R006.4` — EG:55

Original source objective: Repository indexing is incremental, bounded in memory and releases resources on completion; it optionally resolves semantic types for the most common languages beyond syntax.

**Contract definition:** IndexRepository SHALL apply an authorized added/modified/deleted/renamed file delta against an exact base and target source revision. A mismatched base SHALL refuse before index mutation. Unchanged files SHALL reuse their parse/index artifacts; deleted and renamed entities/edges SHALL be removed or rebound by the completed target generation. The job SHALL receive a positive max_working_bytes within its host grant, account all job-owned parser/index buffers and worker allocations, and reserve each allocation before use. It SHALL backpressure or refuse before exceeding that bound, not merely report excess afterward. Existing per-batch atomic refusal remains required; only a complete target generation may become the current code map. Completion, cancellation, and failure SHALL release job buffers, handles, and workers while keeping the shared engine usable. Optional semantic typing SHALL use a versioned language/parser/resolver support table. Unsupported semantic mode SHALL explicitly refuse or return an explicitly requested syntax-only result; grammar availability SHALL NOT imply semantic-type support.

**Production wiring:** Attach delta handling to existing SourceIngest/IndexRepository and SDK-SOURCE-INGEST-R004; retain R007 batch admission and native/deferred separation under R006. Extend the existing parser/index workers and owner resource accounting. Research context: Martin Vogel et al., Codebase-Memory (2026), https://arxiv.org/abs/2603.27277. That paper motivates the approach but does not define this product contract or its supported language matrix.

**Planned acceptance:** Proposed integration case incremental_index_respects_memory_and_releases_resources: run the actual repository index route with an add/modify/delete/rename delta, count parser calls to prove unchanged files are not reparsed, and independently inspect the complete target map. Use an instrumented allocator and worker accounting to show peak job-owned bytes never exceed the grant; reject an over-limit reservation before allocation. Verify no partial target map becomes current and the previous map remains usable after failure. On success, cancellation, and injected parser failure, verify all job allocations/handles return to the declared baseline and a subsequent job succeeds. Run cross-file semantic-type fixtures for every advertised supported table row and reject unsupported modes.

Preserve existing coverage: `EG-REPO-INGEST-R006`, `EG-REPO-INGEST-R007`, `EG-REPO-INGEST-R010`, `SDK-SOURCE-INGEST-R004`.

Source record SHA-256: `340c1817f2a52dc6da4c9c13d93d215debbeb9fefd8b84b8d2400b86541d4d81`. Owner references:
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:21` — `EG-REPO-INGEST-R006`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:22` — `EG-REPO-INGEST-R007`
- `epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/repository-index-and-ingestion/requirements.md:27` — `EG-REPO-INGEST-R010`
- `agent-connector-sdk@5babc4cc48ad73928468b243842f219f51e2cca1:specs/SDK-SOURCE-INGEST/requirements.md:8` — `SDK-SOURCE-INGEST-R004`

Open owner prerequisites:
- The owner must choose and publish the initial semantic-language/resolver matrix and exact host resource-grant binding. Do not substitute grammar counts, the paper's language count, or an arbitrary common-language list. Resource evidence must include native parser/worker allocations, not only language-runtime heap counters.

Unmerged consumer dependencies:
- None.

Owner-review choices (not source-established facts):
- The bound is explicitly job-owned working memory, with shared engine memory separately accounted by the host; do not describe it as a whole-process RSS limit. The initial semantic-language roster remains a source decision hold.

This is a definition only; status remains SPECIFIED and no implementation, served result, receipt, or closure is claimed.

<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R002

Preserve the existing `R002` obligation and all prior children; add `EG-REPO-INGEST-R002.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-REPO-INGEST-R002.100` — AU:118

Extend the existing SourceIngest/result persistence contract with a write receipt binding run id, tenant, query digest, ordered result ids/content digests, graph revision and source provenance. The consumer performs a separately authorized read-only query after the write, binds its principal/grant revision and selector to that receipt, and compares every expected result id/digest/revision. A run may report success only after a matching readback receipt; missing, partial, stale, denied or mismatched readback is a typed non-success. Do not introduce a second graph writer.

Producer/consumer: EG owns durable graph write/read; AU owns execution success reporting. Reuse SourceIngest and existing query authorization, with explicit cross-owner consumer link.

Source record SHA-256: `e8ad0c2417b7fa6ce53c46899f1adf8e925977becd0bd1813db6b62d3786af11`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### `EG-REPO-INGEST-R002.101` — PIPE:10

Bind the workflow id/revision loaded from the graph, EG committed plan/decision digest, AU execution run and output digests, SourceIngest write receipt, resulting graph revision and separately authorized readback receipt. The acceptance path uses the real graph_orchestrate -> find_workflow_gaps/fill_workflow_gaps -> execute_workflow consumer and EG graph load/decision plus SourceIngest; a separate authorized graph query must read back the hydrated content and match its digest before the gate reports success. Fake engines/catalogs and generic handoff receipts do not satisfy this contract. Missing/stale workflow, execution mismatch, write failure or denied/partial readback is non-success.

**Producer/consumer boundary:** EG owns workflow graph load/decision and durable SourceIngest; AU owns graph_orchestrate/execution; SDK supplies source-effect runner. Pipelines owns the gate only and does not add a runtime orchestrator.

**Source record SHA-256:** `e0c431512565acb1a8ae00e09b63ec6fb7c8f9e81ef9c786a0fd7b26b2239455`.

**Preserved parent:** `EG-REPO-INGEST-R002` remains unchanged.
<!-- /spec-definition-patch -->
