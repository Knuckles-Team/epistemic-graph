# EG-FEDERATED-QUERY — Federated query, search, and reasoning

| Field | Value |
|---|---|
| Stable ID | `federated-query-and-reasoning` |
| Owner | epistemic-graph |
| Delivery state | `PROPOSED` for this combined contract; source slices exist, but exact-head acceptance is unverified |
| Acceptance state | `NOT AUDITED` |
| Primary interfaces | UQL, SQL, SPARQL, OBDA, search, `REASON`, `EXPLAIN`, `PROFILE` |

## State legend

`PROPOSED` means a reviewed implementation contract is available. `BUILDING` means code exists on a development branch. `BUILT` means an implementation slice exists but required acceptance is incomplete. `LANDED` means the source is merged into the owner's public default branch. `ACCEPTED` requires passing exact-head behavior, safety, quality, and client tests. `DEFERRED` names work explicitly excluded from the current release. `CLOSED` means a design decision or superseded item requires no implementation. A source state and an acceptance state are separate; no row below is accepted merely because a test or branch exists.

## Outcome and scope

An authenticated caller can express graph, relational, RDF, time series, search, numerical and foreign-source reads through typed, bounded query surfaces. Execution preserves snapshot, authorization, proof and result semantics whether work runs in the local engine or at a permitted foreign source. The caller can inspect which fragments ran remotely, which residuals ran locally, and which budgets or freshness conditions constrained the result. Reasoning and search use EG-owned canonical schema and index generations, including after replay and restart. Semantic-index reconciliation may tombstone missing sources only after an authoritative complete-snapshot proof; an interrupted or partial scan retains previous live entries (EG-FEDERATED-QUERY-R007).

This contract covers query language and planner semantics, federation optimization, SPARQL/OBDA execution, semantic index/query behavior, reasoning correctness, and query-visible numerical operators. The [unified-data-plane](../unified-data-plane/spec.md) contract owns source registration, dialect adapters, change capture, live/accelerated routing, and virtual-graph mapping approval. This contract consumes its owner-scoped source identity, capability, watermark, and approved mapping; it must not create a second registry or bypass source policy.

Application prompts, agent orchestration, connector certification, UI presentation, and autonomous model training are outside this owner contract. Natural language may produce a *previewed* typed UQL candidate; no unreviewed natural-language text is executed as authority. A learned relevance score never grants access. Mutating foreign SQL through UQL or an implicit attached-source write path is forbidden.

## User stories and acceptance

1. **P0 — correct read:** A contributor can run equivalent native and optimized UQL/SPARQL/OBDA queries against the same permitted fixture and receive the same ordered results where order is defined, including duplicate RDF values, nulls, timestamps and typed numerics.
2. **P0 — safe foreign read:** A tenant can query only registered and granted sources. A malicious URL, DSN, redirect, private destination, credential-bearing inline spec or over-budget query fails with a stable typed refusal; no partial rows or secret-bearing trace are returned.
3. **P1 — transparent plan:** `EXPLAIN` shows a canonical and optimized plan with remote fragments, residual operators, route/freshness and maintainability reason. `PROFILE` adds observed rows, time and budget usage, with safe source labels.
4. **P1 — durable semantic read:** Search, ANN, proof, ontology and query caches serve only generations authorized for the caller and survive restart or fall back safely while a generation rebuilds.
5. **P1 — measurable compute:** Time-series, attribution, impact and motif operators share deterministic kernels across UQL and other public surfaces; correctness fixtures and performance evidence accompany each implementation.

## Normative requirements

| ID | Requirement | Stable obligations |
|---|---|---|
| FQR-01 | Retain one read-only UQL text method. Grammar, canonical printer, dispatch table and generated language reference derive from one grammar; every supported operation round-trips; unsupported feature/build yields a typed error. Typed `$param` values bind only in value positions; unbound, unused and mismatched values fail. `DecideText` consumes this lexer under its own decision contract. | EG-FEDERATED-QUERY-R020–EG-FEDERATED-QUERY-R026, EG-FEDERATED-QUERY-R058 |
| FQR-02 | Support graph traversal direction, edge predicates, named `LET`/`FROM`/`JOIN` subplans, named score channels, proof requests, `VALIDATE SHAPE`, KnowledgeSet, per-stage `EXPLAIN`/`PROFILE`, and explicit result/traversal budgets. UQL is read-only on a shared snapshot; graph-free SQL and log-search SQL are independently read-only. | EG-FEDERATED-QUERY-R005, EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R015, EG-FEDERATED-QUERY-R021, EG-FEDERATED-QUERY-R022, EG-FEDERATED-QUERY-R023, EG-FEDERATED-QUERY-R024, EG-FEDERATED-QUERY-R028 |
| FQR-03 | The optimizer may push projection, exact filters, top-N, aggregates, same-source joins and batched keys only when a source capability proves that operation sound. Keep an exact local residual for unsupported/inexact operations. Optimized execution must match the full-fetch oracle; there is no silent first-page, top-k or budget truncation. | EG-FEDERATED-QUERY-R038–EG-FEDERATED-QUERY-R042, EG-FEDERATED-QUERY-R044–EG-FEDERATED-QUERY-R048, EG-FEDERATED-QUERY-R050–EG-FEDERATED-QUERY-R053, EG-FEDERATED-QUERY-R055 |
| FQR-04 | Bind every remote fragment to an owner-scoped grant, pinned outbound target, read-only query validation and shared SSRF policy. Use per-query limits for requests, rows, bind keys and elapsed time. Refuse a `RequiresKeys` full scan; redact credentials and untrusted labels from trace, logs and errors. | EG-FEDERATED-QUERY-R013, EG-FEDERATED-QUERY-R014, EG-FEDERATED-QUERY-R043, EG-FEDERATED-QUERY-R047, EG-FEDERATED-QUERY-R054 |
| FQR-05 | Key foreign caches by source identity, owner salt, canonical request and fresh source watermark. Missing/stale watermark disables the hit. Learned cardinality estimates carry sample count and provenance; registration-time probes and rate limits cannot bypass budget or policy. | EG-FEDERATED-QUERY-R017, EG-FEDERATED-QUERY-R018, EG-FEDERATED-QUERY-R048, EG-FEDERATED-QUERY-R049 |
| FQR-06 | OBDA evaluates approved named virtual graphs with per-predicate scan groups, sound constant and semi-join reduction, typed SQL rendering and direct pushed solutions where sound. SPARQL `SERVICE` bind joins preserve multivalued RDF answers and use the same outbound guard. Iceberg pruning occurs after provider filters/projection/limit are known. | EG-FEDERATED-QUERY-R042, EG-FEDERATED-QUERY-R046, EG-FEDERATED-QUERY-R050, EG-FEDERATED-QUERY-R051, EG-FEDERATED-QUERY-R055; interface to EG-UNIFIED-DATA-PLANE-R005–EG-UNIFIED-DATA-PLANE-R007 |
| FQR-07 | Text/vector and edge-native search filter tenant, purpose and RLS *before* candidate truncation. Mixed traversal plus vector continues candidate generation until `k` permitted matches or returns a typed shortfall. Index create/backfill/activate/drop is fenced and durable; unavailable generations use a correct fallback. Graph reopen uses restored index state or a bounded rebuild, with a typed partial-materialization state until reads are safe. | EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008, EG-FEDERATED-QUERY-R009, EG-FEDERATED-QUERY-R035, EG-FEDERATED-QUERY-R036, EG-FEDERATED-QUERY-R039, EG-FEDERATED-QUERY-R040 |
| FQR-08 | Canonical EG GraphSchema and ontology generations drive proof and reasoning. EL/RL and tableau agree on consistency, disjointness and bounded derivation; a skipped projection event triggers rebuild before serving. Every proof binds premises and source/generation; no speculative inference grants permission. Outbox delivery is owned by `durable-graph-kernel`; schema source composition is owned by `typed-packs-and-catalog`. | EG-FEDERATED-QUERY-R001, EG-FEDERATED-QUERY-R004, EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R010, EG-FEDERATED-QUERY-R011, EG-FEDERATED-QUERY-R012 |
| FQR-09 | Query-visible numeric kernels are deterministic, bounded and shared. Series row identity is `series@ts` with f64 channels; rolling constant windows are exactly zero-variance; attribution, propagation, barrier and motif algorithms have explicit seed/CI or exactness contracts. Tail probabilities reuse `eg-numeric`, without a second local erf implementation. | EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031, EG-FEDERATED-QUERY-R032, EG-FEDERATED-QUERY-R033, EG-FEDERATED-QUERY-R034, EG-FEDERATED-QUERY-R037, EG-FEDERATED-QUERY-R056 |
| FQR-10 | Keep proof, trace, score and source metadata stable through Rust server and generated Python client contracts; client-visible errors distinguish authorization, unsupported feature, budget, source-needs-keys and source failure. | EG-FEDERATED-QUERY-R006, EG-FEDERATED-QUERY-R025–EG-FEDERATED-QUERY-R027, EG-FEDERATED-QUERY-R044 |
| FQR-11 | Select the sources of a cross-source question from ontology facts and approved virtual mappings, with premises per selection. Federate API, MCP, A2A and GraphQL sources through operation-bound mappings under the FQR-04 guard. | EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073; interface to EG-UNIFIED-DATA-PLANE-R037, EG-UNIFIED-DATA-PLANE-R038 |

## Delivery crosswalk

The following states reflect prior recorded delivery claims, not a fresh exact-head acceptance audit. Preserve the stable IDs in PRs and evidence. A contributor closes only the rows exercised by their changed behavior.

| Workstream | Recorded source state | Acceptance still required |
|---|---|---|
| UQL language and serving, EG-FEDERATED-QUERY-R020–EG-FEDERATED-QUERY-R028 | Mostly `BUILT` | Final merged-head grammar, served client, snapshot, proof and exhaustive round-trip gates; `DecideText` EG-DECISION-ENGINE-R104 belongs to `decision-engine` |
| Federation phase 1, EG-FEDERATED-QUERY-R041–EG-FEDERATED-QUERY-R043 | `BUILT` | Optimized/full-fetch differential, outbound security, hosted and merged-head proof |
| Federation phase 2/3, EG-FEDERATED-QUERY-R038, EG-FEDERATED-QUERY-R044–EG-FEDERATED-QUERY-R054 | `QUEUED` as last recorded; some later work may be present | Per-source capability and served oracle for each row; do not infer acceptance from code presence |
| Semantic index and search, EG-FEDERATED-QUERY-R059, EG-FEDERATED-QUERY-R008 and related rows (see `requirements.md`) | Source slices `BUILT` or `BUILDING` | Fixed authority, restart, RLS, recall, held-out routing, and measured performance |
| Reasoning and proof, EG-FEDERATED-QUERY-R001 and related rows (see `requirements.md`) | Earlier rows report `LANDED`; later corpus and schema slices report `BUILT` | Exact current-head corpus compose, consistency, replay and proof verification; outbox EG-DURABLE-KERNEL-R002 and GraphSchema EG-TYPED-PACKS-R071 are external owner interfaces |
| Query-visible numerics, EG-FEDERATED-QUERY-R030–EG-FEDERATED-QUERY-R031 and related rows (see `requirements.md`) | Mixed `BUILT` and `QUEUED` | Exact/golden numerical oracles, streamed parity, and resource limits |

The following IDs are classified here for traceability but require no EG feature duplicate: EG-FEDERATED-QUERY-R002 fixes the format-neutral reasoning rejection decision; EG-FEDERATED-QUERY-R003 is folded into the shared derivation budget; EG-FEDERATED-QUERY-R016 and EG-FEDERATED-QUERY-R029 are downstream TUI/client query-text fixes; EG-FEDERATED-QUERY-R019 is finance signal-state maintenance; EG-FEDERATED-QUERY-R057 is the SQL-provider file-size quality repair. Their behavior remains covered by the relevant client, finance, quality or reasoning acceptance, not by a second EG implementation.

## Success criteria

### Foreign columns and remote UQL pushdown (EG-FEDERATED-QUERY-R045, EG-FEDERATED-QUERY-R052)

**EG-FEDERATED-QUERY-R045.** A foreign source returns typed `ForeignRows` with stable column names/types, null semantics and row provenance so a planner can evaluate filters and projections on foreign columns. `apply_filter` and `apply_projection` report `Exact`, `Inexact` or `Unsupported` per operation. Only `Exact` may remove the local residual. `Inexact` may narrow transfer only when it returns a superset of matching authorized rows; the exact predicate still runs locally. `Unsupported` fetches the required columns and evaluates locally. Never discard a column needed by a residual, ORDER BY, authorization or proof channel. Schema/version mismatch and ambiguous column identity refuse instead of silently binding a different field.

**EG-FEDERATED-QUERY-R052.** For registered `RemoteEngine` sources, the planner may append authorized key filters and bounded LIMIT to a canonical read-only UQL request sent through `Method::Uql`. Pushed keys derive from caller-visible rows; pushed LIMIT is sound only after preserving ORDER BY, residual filtering, pagination and outer-query limit semantics. The remote receives a signed owner/grant context and the same outbound URL, redirect, DNS and budget checks as other source kinds. A remote lacking the UQL feature or exact push capability falls back to the safe bounded route and retains local residuals; it cannot silently fetch an unbounded full graph. Explain/profile identify pushed and residual work with redacted source labels.

### Ontology source selection and operation-bound sources (EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073)

**EG-FEDERATED-QUERY-R072.** A question such as "How does the supply chain affect our services?" names classes, not sources. EG links the classes through object properties and entails subclass relations. EG then picks, per class, the approved virtual mapping that serves it. A class without an approved mapping is uncovered; the answer never substitutes another class. The selection is read-only and cites label, relation and mapping premises. The agent-utilities consumer (AU-CONTROL-R029) runs an interim local path search over EG relation facts. That consumer retires the local search once this method is served.

**EG-FEDERATED-QUERY-R073.** API, MCP, A2A and GraphQL sources expose entities through operations, not tables. Each kind registers as a typed `ForeignSourceSpec` with its own capability set, as EG-FEDERATED-QUERY-R053 does for Trino and Spark. A key filter is pushed only under a declared capability. The local residual filter always runs, so a widened source answer cannot change results.

All P0 scenarios pass for native and foreign query routes at the exact merged head. A recording mock verifies that every pushed request narrows data transfer without changing answers; unsupported/inexact pushdown retains the residual. Security negatives yield stable codes and zero leaked credentials. The search oracle returns all permitted matches up to `k`, or an explicit shortfall. Corpus compose and restart complete within declared budgets without stack overflow. Numerical fixtures meet the tolerances in [test-spec.md](test-spec.md). Every accepted row has a public commit, exact test command/result, and an evidence entry; source existence alone leaves acceptance open.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.
