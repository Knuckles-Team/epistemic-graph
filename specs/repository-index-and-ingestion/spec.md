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

Original objective: A skill document's procedural steps compile into an executable plan with no separate workflow file, and the graph references the skill directory instead of copying it.

EG-owned residual: No reviewed clause compiles procedural steps from the skill document itself into an executable plan or guarantees execution without a separate workflow file. The SourceIngest ownership label skill_workflow_ingest is not that behavioral definition.

Producer/consumer boundary: EG SourceIngest owns skill_workflow_ingest; AU graph_orchestrate -> execute_workflow runs an existing workflow after find_workflow_gaps/fill_workflow_gaps. The skill-document-to-executable-plan producer and its consumer link are absent.

Preserve existing coverage: `AU-HARNESS-R022.2`, `AU-CONTROL-R037.3`, `AU-CONTROL-R037.5`, `EG-REPO-INGEST-R002`.

Owner source hash: `3f7e133169a76ce2680ec667c28ea2c5543354ed8874d258c432e65346f2e529`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-REPO-INGEST-R006.1` — AU:70

Original objective: Ingestion throughput is profiled per stage against a target of 2000 papers per day; maintenance backlog purge and per-goal budget allocation are prioritized when throughput falls below target.

EG-owned residual: paper throughput numerator/denominator/window and 2000 papers/day target; below-target policy prioritizing maintenance backlog purge and per-goal budget allocation; safe purge candidates, authorization/admission, refusal and decision/result evidence
paper throughput numerator/denominator/window and 2000 papers/day target; below-target policy prioritizing maintenance backlog purge and per-goal budget allocation; safe purge candidates, authorization/admission, refusal and decision/result evidence
Producer/consumer boundary: SDK paper transport -> EG SourceIngest/native commits + enrichment WorkItems -> ingestion telemetry -> EG budget/purge decision (missing) -> GraphOS governed maintenance dispatch (missing). AU instrumentation supplies observations, not policy authority.

Preserve existing coverage: `AU-RETIRE-R005`, `EG-REPO-INGEST-R006`.

Owner source hash: `902f964dd909186f3a31d1f8a84d146b715446e64dfeec62480c8151baa06996`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-REPO-INGEST-R006.2` — AU:92

Original objective: After a sync, embeddings are updated asynchronously with an importance penalty so high-importance features resist change; consolidation never runs in the routing loop.

EG-owned residual: Importance-weighted penalty resisting loss of important features; explicit post-sync consolidation separated from routing.

Producer/consumer boundary: EG ingestion queues durable enrichment; R040.3 names the re-embed worker queued by FQR-R077.3. AU-CONTEXT-R002 requests a governed shadow ANN build/activation proposal. No importance-penalty/consolidation consumer contract exists.

Preserve existing coverage: `EG-REPO-INGEST-R006`, `EG-UNIFIED-DATA-PLANE-R040.2`, `AU-CONTEXT-R002`.

Owner source hash: `771acc731a6e25746641adf30cde9ec91f4eb56edd6597f2d308a42947a3abb3`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-REPO-INGEST-R006.3` — EG:54

Original objective: The graph exposes structured code maps (modules, symbols, call and dependency relations) so an agent can ask where a behavior lives; stale maps are refused after source change.

EG-owned residual: No complete modules/symbols/calls/dependencies code-map carrier and behavior-location query with source-revision stale-map refusal is specified. Cache invalidation is a distinct guarantee.

Producer/consumer boundary: IndexRepository/SourceIngest -> hydration -> structured query/code-map consumer; exact code-map response remains undefined.

Preserve existing coverage: `EG-FEDERATED-QUERY-R017`, `EG-REPO-INGEST-R006`, `EG-REPO-INGEST-R009`.

Owner source hash: `e69e6fa3a5c26ae763809d600cb7d264d028b9ce0d80f8ca6141e1a40cf0638e`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-REPO-INGEST-R006.4` — EG:55

Original objective: Repository indexing is incremental, bounded in memory and releases resources on completion; it optionally resolves semantic types for the most common languages beyond syntax.

EG-owned residual: These do not require incremental engine index updates, peak-memory bounds, completion resource release or optional semantic type resolution with a named language matrix. Parsing grammar coverage is not semantic typing.

Producer/consumer boundary: SDK delta SourceIngest -> EG native indexing and deferred work; semantic type producer and resource lifecycle need explicit definition.

Preserve existing coverage: `EG-REPO-INGEST-R006`, `EG-REPO-INGEST-R007`, `EG-REPO-INGEST-R010`, `SDK-SOURCE-INGEST-R004`.

Owner source hash: `340c1817f2a52dc6da4c9c13d93d215debbeb9fefd8b84b8d2400b86541d4d81`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
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
