# EG-FEDERATED-QUERY — Federated query, search, and reasoning

| Field | Value |
|---|---|
| Stable ID | `federated-query-and-reasoning` |
| Owner | epistemic-graph |
| Primary interfaces | UQL, SQL, SPARQL, OBDA, search, `REASON`, `EXPLAIN`, `PROFILE` |

## Outcome and scope

An authenticated caller can express graph, relational, RDF, time series, search, numerical and foreign-source reads through typed, bounded query surfaces. Execution preserves snapshot, authorization, proof and result semantics whether work runs in the local engine or at a permitted foreign source. The caller can inspect which fragments ran remotely, which residuals ran locally, and which budgets or freshness conditions constrained the result. Reasoning and search use EG-owned canonical schema and index generations, including after replay and restart. Semantic-index reconciliation may tombstone missing sources only after an authoritative complete-snapshot proof; an interrupted or partial scan retains previous live entries (EG-FEDERATED-QUERY-R007).

This contract covers query language and planner semantics, federation optimization, SPARQL/OBDA execution, semantic index/query behavior, reasoning correctness, and query-visible numerical operators. The [unified-data-plane](../unified-data-plane/spec.md) contract owns source registration, dialect adapters, change capture, live/accelerated routing, and virtual-graph mapping approval. This contract consumes its owner-scoped source identity, capability, watermark, and approved mapping; it must not create a second registry or bypass source policy.

Application prompts, agent orchestration, connector certification, UI presentation, and autonomous model training are outside this owner contract. Natural language may produce a *previewed* typed UQL candidate; no unreviewed natural-language text is executed as authority. A learned relevance score never grants access. Mutating foreign SQL through UQL or an implicit attached-source write path is forbidden.

## User stories and acceptance

1. **P0 — correct read:** A contributor can run equivalent native and optimized UQL/SPARQL/OBDA queries against the same permitted fixture and receive the same ordered results where order is defined, including duplicate RDF values, nulls, timestamps and typed numerics.
2. **P0 — safe foreign read:** A tenant can query only registered and granted sources. A malicious URL, DSN, redirect, private destination, credential-bearing inline spec or over-budget query fails with a stable typed refusal. No partial rows or secret-bearing trace are returned.
3. **P1 — transparent plan:** `EXPLAIN` shows a canonical and optimized plan with remote fragments, residual operators, route/freshness and maintainability reason. `PROFILE` adds observed rows, time and budget usage, with safe source labels.
4. **P1 — durable semantic read:** Search, ANN, proof, ontology and query caches serve only generations authorized for the caller and survive restart or fall back safely while a generation rebuilds.
5. **P1 — measurable compute:** Time-series, attribution, impact and motif operators share deterministic kernels across UQL and other public surfaces. Correctness fixtures and performance evidence accompany each implementation.

## Normative requirements

| ID | Requirement | Stable obligations |
|---|---|---|
| FQR-01 | Retain one read-only UQL text method. Grammar, canonical printer, dispatch table and generated language reference derive from one grammar. Every supported operation round-trips. Unsupported feature/build yields a typed error. Typed `$param` values bind only in value positions. Unbound, unused and mismatched values fail. `DecideText` consumes this lexer under its own decision contract. | EG-FEDERATED-QUERY-R020–EG-FEDERATED-QUERY-R026, EG-FEDERATED-QUERY-R058 |
| FQR-02 | Support graph traversal direction, edge predicates, named `LET`/`FROM`/`JOIN` subplans, named score channels, proof requests, `VALIDATE SHAPE`, KnowledgeSet, per-stage `EXPLAIN`/`PROFILE`, and explicit result/traversal budgets. UQL is read-only on a shared snapshot; graph-free SQL and log-search SQL are independently read-only. | EG-FEDERATED-QUERY-R005, EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R015, EG-FEDERATED-QUERY-R021, EG-FEDERATED-QUERY-R022, EG-FEDERATED-QUERY-R023, EG-FEDERATED-QUERY-R024, EG-FEDERATED-QUERY-R028 |
| FQR-03 | The optimizer may push projection, exact filters, top-N, aggregates, same-source joins and batched keys only when a source capability proves that operation sound. Keep an exact local residual for unsupported/inexact operations. Optimized execution must match the full-fetch oracle; there is no silent first-page, top-k or budget truncation. | EG-FEDERATED-QUERY-R038–EG-FEDERATED-QUERY-R042, EG-FEDERATED-QUERY-R044–EG-FEDERATED-QUERY-R048, EG-FEDERATED-QUERY-R050–EG-FEDERATED-QUERY-R053, EG-FEDERATED-QUERY-R055 |
| FQR-04 | Bind every remote fragment to an owner-scoped grant, pinned outbound target, read-only query validation and shared SSRF policy. Use per-query limits for requests, rows, bind keys and elapsed time. Refuse a `RequiresKeys` full scan; redact credentials and untrusted labels from trace, logs and errors. | EG-FEDERATED-QUERY-R013, EG-FEDERATED-QUERY-R014, EG-FEDERATED-QUERY-R043, EG-FEDERATED-QUERY-R047, EG-FEDERATED-QUERY-R054 |
| FQR-05 | Key foreign caches by source identity, owner salt, canonical request and fresh source watermark. Missing/stale watermark disables the hit. Learned cardinality estimates carry sample count and provenance; registration-time probes and rate limits cannot bypass budget or policy. | EG-FEDERATED-QUERY-R017, EG-FEDERATED-QUERY-R018, EG-FEDERATED-QUERY-R048, EG-FEDERATED-QUERY-R049 |
| FQR-06 | OBDA evaluates approved named virtual graphs with per-predicate scan groups, sound constant and semi-join reduction, typed SQL rendering and direct pushed solutions where sound. SPARQL `SERVICE` bind joins preserve multivalued RDF answers and use the same outbound guard. Iceberg pruning occurs after provider filters/projection/limit are known. | EG-FEDERATED-QUERY-R042, EG-FEDERATED-QUERY-R046, EG-FEDERATED-QUERY-R050, EG-FEDERATED-QUERY-R051, EG-FEDERATED-QUERY-R055; interface to EG-UNIFIED-DATA-PLANE-R005–EG-UNIFIED-DATA-PLANE-R007 |
| FQR-07 | Text/vector and edge-native search filter tenant, purpose and RLS *before* candidate truncation. Mixed traversal plus vector continues candidate generation until `k` permitted matches or returns a typed shortfall. Index create/backfill/activate/drop is fenced and durable; unavailable generations use a correct fallback. Graph reopen uses restored index state or a bounded rebuild, with a typed partial-materialization state until reads are safe. | EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008, EG-FEDERATED-QUERY-R009, EG-FEDERATED-QUERY-R035, EG-FEDERATED-QUERY-R036, EG-FEDERATED-QUERY-R039, EG-FEDERATED-QUERY-R040 |
| FQR-08 | Canonical EG GraphSchema and ontology generations drive proof and reasoning. EL/RL and tableau agree on consistency, disjointness and bounded derivation; a skipped projection event triggers rebuild before serving. Every proof binds premises and source/generation; no speculative inference grants permission. Outbox delivery is owned by `durable-graph-kernel`; schema source composition is owned by `typed-packs-and-catalog`. | EG-FEDERATED-QUERY-R001, EG-FEDERATED-QUERY-R004, EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R010, EG-FEDERATED-QUERY-R011, EG-FEDERATED-QUERY-R012 |
| FQR-09 | Query-visible numeric kernels are deterministic, bounded and shared. Series row identity is `series@ts` with f64 channels. Rolling constant windows are exactly zero-variance. Attribution, propagation, barrier and motif algorithms have explicit seed/CI or exactness contracts. Tail probabilities reuse `eg-numeric`, without a second local erf implementation. | EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031, EG-FEDERATED-QUERY-R032, EG-FEDERATED-QUERY-R033, EG-FEDERATED-QUERY-R034, EG-FEDERATED-QUERY-R037, EG-FEDERATED-QUERY-R056 |
| FQR-10 | Keep proof, trace, score and source metadata stable through Rust server and generated Python client contracts. Client-visible errors distinguish authorization, unsupported feature, budget, source-needs-keys and source failure. | EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R025–EG-FEDERATED-QUERY-R027, EG-FEDERATED-QUERY-R044 |
| FQR-11 | Select the sources of a cross-source question from ontology facts and approved virtual mappings, with premises per selection. Federate API, MCP, A2A and GraphQL sources through operation-bound mappings under the FQR-04 guard. | EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073; interface to EG-UNIFIED-DATA-PLANE-R037, EG-UNIFIED-DATA-PLANE-R038 |

## Acceptance crosswalk

These workstreams group the required acceptance evidence. Preserve stable IDs in PRs and evidence. Code presence alone does not establish acceptance.

| Workstream | Required acceptance |
|---|---|
| UQL language and serving, EG-FEDERATED-QUERY-R020–EG-FEDERATED-QUERY-R028 | Final merged-head grammar, served client, snapshot, proof and exhaustive round-trip gates; `DecideText` EG-DECISION-ENGINE-R104 belongs to `decision-engine` |
| Federation phase 1, EG-FEDERATED-QUERY-R041–EG-FEDERATED-QUERY-R043 | Optimized/full-fetch differential, outbound security, hosted and merged-head proof |
| Federation phase 2/3, EG-FEDERATED-QUERY-R038, EG-FEDERATED-QUERY-R044–EG-FEDERATED-QUERY-R054 | Per-source capability and served oracle for each row. Do not infer acceptance from code presence |
| Semantic index and search, EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008 and related rows (see `requirements.md`) | Fixed authority, restart, RLS, recall, held-out routing, and measured performance |
| Reasoning and proof, EG-FEDERATED-QUERY-R001 and related rows (see `requirements.md`) | Exact current-head corpus compose, consistency, replay and proof verification. Outbox EG-DURABLE-KERNEL-R002 and GraphSchema EG-TYPED-PACKS-R071 are external owner interfaces |
| Query-visible numerics, EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031 and related rows (see `requirements.md`) | Exact/golden numerical oracles, streamed parity, and resource limits |

The following IDs are classified here for traceability but require no EG feature duplicate: EG-FEDERATED-QUERY-R002 fixes the format-neutral reasoning rejection decision; EG-FEDERATED-QUERY-R003 is folded into the shared derivation budget; EG-FEDERATED-QUERY-R016 and EG-FEDERATED-QUERY-R029 are downstream TUI/client query-text fixes; EG-FEDERATED-QUERY-R019 is finance signal-state maintenance; EG-FEDERATED-QUERY-R057 is the SQL-provider file-size quality repair. Their behavior remains covered by the relevant client, finance, quality or reasoning acceptance, not by a second EG implementation.

## Success criteria

### Foreign columns and remote UQL pushdown (EG-FEDERATED-QUERY-R045, EG-FEDERATED-QUERY-R052)

**EG-FEDERATED-QUERY-R045.** A foreign source returns typed `ForeignRows` with stable column names/types, null semantics and row provenance so a planner can evaluate filters and projections on foreign columns. `apply_filter` and `apply_projection` report `Exact`, `Inexact` or `Unsupported` per operation. Only `Exact` may remove the local residual. `Inexact` may narrow transfer only when it returns a superset of matching authorized rows; the exact predicate still runs locally. `Unsupported` fetches the required columns and evaluates locally. Never discard a column needed by a residual, ORDER BY, authorization or proof channel. Schema/version mismatch and ambiguous column identity refuse instead of silently binding a different field.

**EG-FEDERATED-QUERY-R052.** For registered `RemoteEngine` sources, the planner may append authorized key filters and bounded LIMIT to a canonical read-only UQL request sent through `Method::Uql`. Pushed keys derive from caller-visible rows. Pushed LIMIT is sound only after preserving ORDER BY, residual filtering, pagination and outer-query limit semantics. The remote receives a signed owner/grant context and the same outbound URL, redirect, DNS and budget checks as other source kinds. A remote lacking the UQL feature or exact push capability falls back to the safe bounded route and retains local residuals. It cannot silently fetch an unbounded full graph. Explain/profile identify pushed and residual work with redacted source labels.

### Ontology source selection and operation-bound sources (EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073)

**EG-FEDERATED-QUERY-R072.** A question such as "How does the supply chain affect our services?" names classes, not sources. EG links the classes through object properties and entails subclass relations. EG then picks, per class, the approved virtual mapping that serves it. A class without an approved mapping is uncovered; the answer never substitutes another class. The selection is read-only and cites label, relation and mapping premises. The agent-utilities consumer (AU-CONTROL-R029) runs an interim local path search over EG relation facts. That consumer retires the local search once this method is served.

**EG-FEDERATED-QUERY-R073.** API, MCP, A2A and GraphQL sources expose entities through operations, not tables. Each kind registers as a typed `ForeignSourceSpec` with its own capability set, as EG-FEDERATED-QUERY-R053 does for Trino and Spark. A key filter is pushed only under a declared capability. The local residual filter always runs, so a widened source answer cannot change results.

All P0 scenarios pass for native and foreign query routes at the exact merged head. A recording mock verifies that every pushed request narrows data transfer without changing answers; unsupported/inexact pushdown retains the residual. Security negatives yield stable codes and zero leaked credentials. The search oracle returns all permitted matches up to `k`, or an explicit shortfall. Corpus compose and restart complete within declared budgets without stack overflow. Numerical fixtures meet the tolerances in [test-spec.md](test-spec.md). Every accepted row has a public commit, exact test command/result, and an evidence entry; source existence alone leaves acceptance open.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.

## Retrieval policy extension contract

The engine must support four-modality retrieval, hard graph constraints, document-section traversal and document-chunk encoding.
It must supply workload-based column advice and topic/dense fusion through the existing retrieval owners.
R039.1 through R039.4, R040.1 through R040.4, R077.6 and R081.9.7 define these additions.
These extensions define acceptance requirements, without claiming completed served tests.

The engine must retain community retrieval and R081/R082 termination precedence.
The SDK must execute governed ingestion recipes; the harness must apply bounded decisions.
The host must compose authenticated access.
The definitions add no textual grammar, alternate identity authority or runtime implementation.

The spec-delivery page consumes generated `status.json`.
The served journeys in test-spec.md must pass before runtime acceptance is claimed.

## ShEx validation boundary

EG-FEDERATED-QUERY-R084 defines the existing served ShEx surface and its proof obligations.
It preserves the current architecture, capability classification and feature selection.
Existing OWL, SHACL, derivation-budget and query acceptance remain unchanged.
The crate compact-syntax parser does not establish compact-syntax support in the served method.
Declared limitations prevent unsupported full-language conformance claims.
The lifecycle generator owns status; this definition supplies no runtime receipt.

## Approved source queries (R085)

EG supplies immutable, read-only source-query approvals and verifies their authority throughout bounded connector execution. SDK-SOURCE-INGEST-R004 consumes that producer contract. Query semantics, authorization and provenance remain engine-owned; native driver execution remains SDK-owned. Existing query, lease and checkpoint contracts remain intact. These definitions introduce no runtime delivery claim.

<!-- spec-definition-patch:owner-batch-20261012-federated-query-and-reasoning -->
## Reviewed owner residuals — 2026-10-12

### `EG-FEDERATED-QUERY-R070.1` — AU:71

Original objective: An agent may search a raw corpus directly with exact lexical, file-read and context-verification operations without an index; each query reports its cost, and over-budget scans abstain.

EG-owned residual: The existing indexed lexical interaction space does not guarantee raw-corpus operation without an index, exact file-read/context-verification primitives, per-scan cost reporting or refusal before exceeding the scan budget.

Producer/consumer boundary: GraphOS retrieval.space -> GovernedRetriever -> EG retrieval returns a top-K boundary; EG UQL budgets bound rows/visits. No reviewed governed indexless scan/file-read/context-verification producer contract supplies the requested operations.

Preserve existing coverage: `EG-FEDERATED-QUERY-R070`, `GRAPHOS-OPS-R053.2`, `GRAPHOS-OPS-R053.3`, `GRAPHOS-OPS-R053.5`.

Owner source hash: `0f1b775fd66c41586bdcdf0047a9ccbc52e38aa1bc21a4e8234e9c641e45c68e`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R006.1` — EG:11

Original objective: The engine derives new facts by temporal forward-chaining rules over extracted facts and records, for each derived fact, provenance back to its originating sentence or axiom.

EG-owned residual: Define temporal extracted-fact forward chaining and durable per-fact sentence/axiom provenance.

Producer/consumer boundary: EG reasoners and proof queries return answer-level evidence; the temporal fact-production API and persistent origin chain are absent.

Preserve existing coverage: `EG-FEDERATED-QUERY-R001`, `EG-FEDERATED-QUERY-R011`, `EG-DECISION-ENGINE-R003`, `EG-FEDERATED-QUERY-R006`.

Owner source hash: `2c2ffcf18d947730bc60a935fed942e1f82e9e8547f4509fe2941a34672fb8f3`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R040.5` — EG:20

Original objective: The platform absorbs new knowledge into a retrievable memory module without changing model parameters or discarding earlier knowledge; retrieval cost scales with query selectivity rather than corpus size, and cross-document relations are encoded.

EG-owned residual: Define continual parameter-free memory/retention, cross-document encoding and corpus-growth/selectivity scaling criteria.

Producer/consumer boundary: EG lifecycle and filtered query operators are adjacent infrastructure; no memory admission/encoding contract joins them with the requested guarantees.

Preserve existing coverage: `EG-FEDERATED-QUERY-R007`, `EG-FEDERATED-QUERY-R040`, `EG-FEDERATED-QUERY-R040.4`, `EG-FEDERATED-QUERY-R008`.

Owner source hash: `07df52b453f0f1b7be7aec7bf790f087c3fc5e13500915011382a9fcd8d9d41a`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R040.6` — EG:21

Original objective: Agents keep semantic, procedural and episodic memory layers, built as graph-structured episodes from multi-session dialogue, and retrieve across sessions with hybrid vector and keyword search and temporal validation.

EG-owned residual: Define memory layers, multi-session episode construction and hybrid cross-session retrieval/temporal validation.

Producer/consumer boundary: SourceIngest validates/project transcripts and UnifiedQuery supports filtered search. The episode builder and AU consumer joining these paths are absent definitions.

Preserve existing coverage: `EG-INGEST-WORKER-R003.13`, `EG-INGEST-WORKER-R003.14`, `EG-INGEST-WORKER-R003.17`, `EG-FEDERATED-QUERY-R040`, `EG-INGEST-WORKER-R003.18`, `EG-INGEST-WORKER-R003.16`, `EG-FEDERATED-QUERY-R016`.

Owner source hash: `c321db3aa79c29d891456f2bee98f545611b8d1be7c93d299b939c3d73a40279`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R083.7` — EG:22

Original objective: Source arbitration compares a direct parametric answer with an evidence-conditioned answer using margins that reward evidence-supported answers and penalize passage-prior-only answers; unsupported answers lose arbitration.

EG-owned residual: Confidence difference alone does not define an evidence-support reward or passage-prior-only penalty. A highly confident unsupported candidate can win the specified R083.2 rule; no support-aware losing invariant is specified.

Producer/consumer boundary: Planned AU-RETRIEVAL-R006 prior probe -> served cross-modal query -> eg_plan::execute -> arbitrate -> ArbitrationVerdict -> harness. R083.3/.6 identify the producer path; this audit does not claim it is implemented.

Preserve existing coverage: `EG-FEDERATED-QUERY-R083.1`, `EG-FEDERATED-QUERY-R083.2`, `EG-FEDERATED-QUERY-R083.3`, `EG-FEDERATED-QUERY-R083.4`, `EG-FEDERATED-QUERY-R083.6`.

Owner source hash: `544b7b0054e8c7cd9a76aedd0d665589d555a0bcf20d1f00679442574936af36`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R081.10` — EG:25

Original objective: Retrieval detects temporal confusion between storage date, event date and narrative time using explicit rules, and when first-pass context is insufficient runs a second pass with fallback queries.

EG-owned residual: No explicit retrieval rules distinguish and detect confusion among storage date, event date and narrative time. Ingest transcript fields distinguish event/storage time only. Existing follow-up expansion coverage should be credited, but it does not establish temporal fallback-query construction or its insufficient-first-pass trigger.

Producer/consumer boundary: EG served cross-modal query -> eg_plan::execute sufficiency/feedback -> bounded EG continuation -> harness dispatch. Ingest R003.13/.14 supplies event/storage fields but no narrative-time retrieval rule.

Preserve existing coverage: `EG-FEDERATED-QUERY-R081.1`, `EG-FEDERATED-QUERY-R081.2`, `EG-FEDERATED-QUERY-R081.3`, `EG-FEDERATED-QUERY-R081.4`, `EG-FEDERATED-QUERY-R081.8.1`, `EG-FEDERATED-QUERY-R081.8.2`, `EG-FEDERATED-QUERY-R081.8.3`, `EG-FEDERATED-QUERY-R081.8.5`, `EG-FEDERATED-QUERY-R081.8.6`.

Owner source hash: `fb8d54d8554b8158da64f7c9e8bf02ea846e5b411768a0381c2682b2e6302dac`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R028.1` — EG:29

Original objective: Typed KnowledgeSet rows carry provenance and policy frame per row; the query surface returns them as a superset of plain row sets and refuses rows lacking either.

EG-owned residual: No requirement makes provenance and policy frame mandatory on each KnowledgeSet row, states plain-row-set superset compatibility, or refuses a row missing either. ForeignRows provenance and an authorized-superset pushdown rule address a different carrier.

Producer/consumer boundary: Existing R028 names UQL and the structured query surface as KnowledgeSet consumers but does not name a complete public row producer/type. R045 ForeignRows must not be used as a substitute; resolve the shared typed result producer in the owner definition.

Preserve existing coverage: `EG-FEDERATED-QUERY-R028`, `EG-FEDERATED-QUERY-R045`.

Owner source hash: `2eb8aadb1b6ccc2d5de77a4d1964fc04a4e944bccf9bcc1793e262f146af7583`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R005.1` — EG:31

Original objective: The platform detects temporal conflicts, cardinality, self-loop and asymmetry axiom violations and ontology defects through conflict rules, and reports each with a typed reason instead of silently choosing.

EG-owned residual: The complete requested matrix of temporal conflicts, cardinality violations, self-loops and asymmetry violations, each with its own typed reason, is not specified by generic shape/consistency acceptance.

Producer/consumer boundary: Existing shape/EL-RL/tableau validation -> contradiction proposal; exact public diagnostic carrier must bind every requested violation kind.

Preserve existing coverage: `EG-DECISION-ENGINE-R034`, `EG-FEDERATED-QUERY-R001`, `EG-FEDERATED-QUERY-R005`, `EG-FEDERATED-QUERY-R011`, `EG-FEDERATED-QUERY-R012`.

Owner source hash: `973de1c3b38646a438d057299d8070070f0ef7a2056bb30acff6d1a3814df513`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R078.7` — EG:59

Original objective: Company context (organizations, roles, data assets, permissions, time-series facts) is held as one graph that agents query under the caller's tenant and clearance.

EG-owned residual: Exact composition and acceptance for organizations, roles, data assets, permissions and time-series facts as the requested company-context view remains unbound. Existing vocabulary must be located/reused, not presumed absent.

Producer/consumer boundary: Approved source mappings plus native graph/time-series -> scoped query plan -> agent consumer. One graph is a logical governed view.

Preserve existing coverage: `EG-DURABLE-KERNEL-R043`, `EG-FEDERATED-QUERY-R078`, `EG-UNIFIED-DATA-PLANE-R001`, `EG-UNIFIED-DATA-PLANE-R003`, `EG-FEDERATED-QUERY-R078.1`, `EG-FEDERATED-QUERY-R078.2`, `EG-FEDERATED-QUERY-R078.3`, `EG-FEDERATED-QUERY-R078.4`, `EG-FEDERATED-QUERY-R078.5`, `EG-FEDERATED-QUERY-R078.6`.

Owner source hash: `fd5990a7c9f30e1e1aa0eed9aa08efef4e2ebc8f5f34387598f1c9e577c458dc`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R081.9.7.7` — GRAPHOS:24

Original objective: Retrieval builds a hierarchical tree over documents and answers at multiple granularities by traversing it without external model calls.

EG-owned residual: The original no-external-model-calls constraint is not covered. R081.9.7 uses approved reasoning, inference-call budgets and model revision receipts; its plan explicitly reuses bounded model execution. Vector-free does not prove external-model-free. The requirements also explicitly say no production caller supplies this path today.

Producer/consumer boundary: Governed section-record ingestion -> PlanCtx section traversal before fusion -> run_unified_with approved reasoning -> UnifiedQuery selected spans. The specified reasoning binding does not forbid external model dispatch.

Preserve existing coverage: `EG-FEDERATED-QUERY-R081.9.7.1`, `EG-FEDERATED-QUERY-R081.9.7.2`, `EG-FEDERATED-QUERY-R081.9.7.3`, `EG-FEDERATED-QUERY-R081.9.7.4`, `EG-FEDERATED-QUERY-R081.9.7.5`, `EG-FEDERATED-QUERY-R081.9.7.6`.

Owner source hash: `b2e5de8386b3ca72b59a81ca5fa3193eaf7736af86aca472a720b9e4511961c0`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R081.9.7.8` — GRAPHOS:25

Original objective: Documents in a retrieved space are preprocessed offline into line-numbered tables of contents so agents navigate sub-document spans without reading whole files.

EG-owned residual: No cited contract requires offline preprocessing into line-numbered tables of contents or binds navigation to those line ranges without reading whole files. Source spans and directories alone do not establish line numbering, offline construction, or bounded span-read behavior.

Producer/consumer boundary: SDK source ingestion -> EG immutable document/section records -> GraphOS retrieval.space directories -> AU exploration. The offline line-numbered TOC producer and bounded line-range read contract are unbound.

Preserve existing coverage: `GRAPHOS-OPS-R053.1`, `GRAPHOS-OPS-R053.2`, `GRAPHOS-OPS-R053.3`, `GRAPHOS-OPS-R053.6`, `EG-FEDERATED-QUERY-R081.9.7.1`, `EG-FEDERATED-QUERY-R081.9.7.5`.

Owner source hash: `6797cff839c16c3b9c3877d6bbf48bf8953eae8696087abc99f2b57147daed93`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-FEDERATED-QUERY-R040.2.7` — GRAPHOS:28

Original objective: Retrieval uses document topic metadata as semantic guidance to improve ranking and relevance.

EG-owned residual: Explicit per-document topic-metadata/responsibility binding remains unclear: the model specifies backend topic weights, centroids, corpus revision and embedding space, but does not explicitly associate those topic responsibilities with each source document/candidate. Do not infer that association merely from dense-score document operands or stable document-ID tie-breaking.

Producer/consumer boundary: TopicBackend returns responsibilities, centroids, backend revision and corpus generation; `run_unified_with` binds the selected backend and attaches topic scoring before final truncation. The missing edge is each document/candidate bound to revision-matched topic responsibilities before computing its candidate-specific topic score. EG owns retrieval scoring; GraphOS only projects reads, and GRAPHOS-FLEET-R014 catalog ranking is not a substitute.

Preserve existing coverage: `EG-FEDERATED-QUERY-R040.2.1`, `EG-FEDERATED-QUERY-R040.2.2`, `EG-FEDERATED-QUERY-R040.2.3`, `EG-FEDERATED-QUERY-R040.2.6`.

Owner source hash: `f7654cd07d4d95f32f09fec742c4172b35c689749b3228aef9424830b1f7cc1b`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Reviewed mapped children — 2026-10-12

The children below extend existing rollups. Existing parent requirements, children, policy authority and refusal behavior remain in force. These are SPECIFIED definitions only; no code, producer receipt, consumer receipt or served result is claimed.

### Extension of R081.8

Preserve the existing `R081.8` obligation and all prior children; add `EG-FEDERATED-QUERY-R081.8.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-FEDERATED-QUERY-R081.8.100` — AU:77

Extend existing retrieval feedback with retriever id/version, strategy id/digest, corpus/model revisions, evaluation-set digest and transfer-validation receipt. A strategy is reusable without new validation only for the same target retriever id/version and byte-identical corpus, model and policy revisions. Any target or bound-revision change requires a target-bound validation receipt containing target retriever id/version, strategy id/digest, evaluation-set digest, metric id, measured value, acceptance threshold, pass/fail outcome, corpus revision, model revision and policy revision. Activate only when measured value meets or exceeds the declared threshold; a valid receipt below threshold refuses activation and retains the prior strategy. A different retriever never receives a copied strategy by default. Reuse R081.8 feedback and R082 budgets.

Producer/consumer: Authenticated crossmodal -> `eg-plan::exec` R081.8 feedback -> selected retriever. Preserve existing R081.8.1-.6 tiering, feedback and refusal behavior.

Source record SHA-256: `213700ec5b9e8648657ec47d0c587483a978db10f4661475b467be451d3bb2e1`. Verification remains planned until independently served by each named owner.

### Extension of R053

Preserve the existing `R053` obligation and all prior children; add `EG-FEDERATED-QUERY-R053.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-FEDERATED-QUERY-R053.100` — EG:37

Extend the existing Cypher federation renderer so every generated property key is emitted as a backtick-quoted identifier; embedded backticks use the Cypher grammar escape representation, and values remain parameters rather than interpolated identifiers. The renderer is shared by every Cypher generation path. The build gate validates generated query fixtures and fails on any property key that is unquoted or malformed; it must not add a second renderer or rely on SQL identifier quoting.

Producer/consumer: `ForeignSourceSpec::Cypher` federation renderer -> generated Cypher -> configured build gate; preserve R053 typed source registration. Cypher Manual naming rules define backtick quoting and doubled backtick representation.

Source record SHA-256: `803f4e4ffbcd939af750e23451469835555beaa37e14ce555ffd0a59c0a3ac48`. Verification remains planned until independently served by each named owner.

### Extension of R032

Preserve the existing `R032` obligation and all prior children; add `EG-FEDERATED-QUERY-R032.100`. The child definitions below state only the mapped residual and its exact owner boundary.

#### `EG-FEDERATED-QUERY-R032.100` — AU:43

Extend the existing authenticated R032 `PROPAGATE` result with a separate historical-risk dimension. Bind target node/revision, direction, bounded HOPS, ordered layer taxonomy and weights, authorized failure-dataset id/revision/window/digest, observation counts, policy revision and request digest. For each node with observations compute `failure_rate = failures / observations` and `layer_contribution = layer_weight * failure_rate`, with weights and rates in [0,1]. For a path, compute `path_risk = 1 - product(1 - layer_contribution)` over its unique nodes; aggregate duplicate paths to a node by maximum path risk. Enumerate upstream and downstream path sets separately; suppress repeated-node cycles within a path and order ties by hop count, node id and path-edge ids. Return per-node path, layer contribution, counts, coverage state and risk. Missing per-node history yields `unknown` and a partial result with `aggregate_risk = null`; an unavailable or unauthorized dataset refuses. The new score is an additional dimension and cannot replace or alter R032 Noisy-OR or Independent-Cascade results.

Producer/consumer: EG owns graph impact query/risk decision; AU requests or displays the result. Existing numeric risk helpers are not this two-direction historical impact contract.

Source record SHA-256: `327ced0e0605f34366c662017d82ac892a481c5ca5f40bba2f3693ba6a5f7649`. Verification remains planned until independently served by each named owner.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### New rollup `EG-FEDERATED-QUERY-R086` — Inductive n-ary hypothesis-only link prediction

Defines ordered n-ary inference that returns unasserted hypotheses; it does not mutate graph facts or replace observed-edge query/write contracts.

### `EG-FEDERATED-QUERY-R086.100` — EG:14

Accept an ordered list of at least two source entity refs, a relation/type constraint, tenant and pinned graph/model revisions. Return candidate target refs with confidence, evidence references, ordered source refs, model/policy revision and `hypothesis_only=true`; do not assert edges, mutate graph state or present a prediction as observed fact. Refuse arity below two, unresolved/unauthorized source refs, stale model/graph revisions and malformed relation constraints. Preserve existing binary observed-edge query and graph-write authorities.

**Producer/consumer boundary:** EG federated-query inference owns hypothesis production; AU may consume proposals. No public hyperedge-prediction operation currently exists, so this is a new contract under FQR R086.

**Source record SHA-256:** `7223250e7f899a187aad0b152e597c6174b772c49779ca2bdd12d3ba68f4f5ef`.

**Preserved parent:** `EG-FEDERATED-QUERY-R086` remains unchanged.

### `EG-FEDERATED-QUERY-R081.101` — AU:126

Build a document-only state keyed by document id/content digest, encoder revision and compression-policy revision. Store ordered segment boundaries, compressed token representations and the key/value projections required by the scorer; the state must not depend on candidate order or query context. At query time, score the query against stored projections without decoding/generation and return document digest, state revision, segment policy, score and scorer revision. Measure interaction cost against the declared full-token baseline using the same document/query set and report denominator and result digest.

**Producer/consumer boundary:** EG retrieval owns document-state construction and PlanCtx reranking; query embeddings/context remain query inputs and do not alter the reusable state. Preserve existing FQR R081 tiering and selection bounds.

**Source record SHA-256:** `deca151180a749cb3784d9946da0de17205eca89c92e5eb4f275859e1d4be165`.

**Preserved parent:** `EG-FEDERATED-QUERY-R081` remains unchanged.

### `EG-FEDERATED-QUERY-R081.102` — GRAPHOS:27

Persist an index-time distilled scorer bound to corpus/index revision, teacher model revision, teacher-judgment dataset digest, scorer artifact digest and quality metric/threshold policy. The scorer is eligible for query use only when its measured quality reaches the declared threshold on a held-out target set. Query-time retrieval applies the stored scorer without any model-service call; it never distills or repairs the scorer on demand. A below-threshold, stale or unbound artifact refuses activation and leaves the prior scorer active.

**Producer/consumer boundary:** EG owns retrieval learning/ranking and the stored scorer; AU may execute evaluation; GraphOS catalog ranking is not this document retrieval contract. Use the existing authenticated UnifiedQuery -> run_unified_with planning route; do not invent a public operation name.

**Source record SHA-256:** `baac22c9fb42470957b5b91499c6e42e1bb2410d24446d15f6f364638c88138e`.

**Preserved parent:** `EG-FEDERATED-QUERY-R081` remains unchanged.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mapping-eg41-20261012 -->
## Mapped retrieval-selection extension — 2026-10-12

### `EG-FEDERATED-QUERY-R081.100` — EG:41

For a query vector q and candidate document matrix V whose rows are revision-pinned document embeddings in the same declared embedding space, select a joint coefficient vector x by minimizing `||q - Vᵀx||₂² + λ||x||₁` subject to xᵢ >= 0 and the existing result-count/tenant bounds. λ, solver version, convergence tolerance, embedding-space revision and input digest are bound to the request. Return selected document ids, nonnegative coefficients, reconstruction residual, solver status and policy revision in deterministic coefficient-descending then document-id order. Do not substitute independent dense/lexical dual-score ranking; preserve the distinct R081.7 mechanism. Refuse mismatched embedding revisions, invalid λ, solver non-convergence or a result set that exceeds existing bounds.

**Producer/consumer boundary:** EG retrieval selection owns the joint objective and set result; the existing served UnifiedQuery/retrieval path consumes it. Preserve R081.1-.9 selection and tenant bounds; no new caller-controlled score authority.

**Source record SHA-256:** `bceeed106b5db2933cf5161d58b4203fc7afe9d714503a31d68323992906c23b`.

**Preserved parent:** `R081` remains unchanged.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner viewport-bounded query

`ViewportQueryV1` is a plan variant of the existing authenticated `UnifiedQuery` execution path; `ResolveViewportSelectionV1` resolves only the exact point or aggregate bin named by its typed token. V1 uses `min(W×H, 262,144)` scatter bins, `min(4W, 65,536)` line bins and an encoded-byte ceiling of `min(16,777,216, 65,536 + 96×max_bin_count)`, where W/H are validated viewport pixels. Those caps depend on viewport and variant, never source cardinality. Line outputs preserve first/last/min/max/count and explicit gaps; scatter returns count and axis extrema. Both bind source/index revision, viewport and policy digest and refuse stale/unauthorized/cap-exceeding requests.

The served acceptance uses an indexed billion-point source and proves pan/zoom/selection without full-source transfer. GraphOS owns its transport adapter and WebUI R001.2 owns rendered interaction. This specification does not claim runtime delivery.
<!-- /spec-definition-patch -->
