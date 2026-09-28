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

An authenticated caller can express graph, relational, RDF, time series, search, numerical and foreign-source reads through typed, bounded query surfaces. Execution preserves snapshot, authorization, proof and result semantics whether work runs in the local engine or at a permitted foreign source. The caller can inspect which fragments ran remotely, which residuals ran locally, and which budgets or freshness conditions constrained the result. Reasoning and search use EG-owned canonical schema and index generations, including after replay and restart. Semantic-index reconciliation may tombstone missing sources only after an authoritative complete-snapshot proof; an interrupted or partial scan retains previous live entries (EH-315).

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
| FQR-01 | Retain one read-only UQL text method. Grammar, canonical printer, dispatch table and generated language reference derive from one grammar; every supported operation round-trips; unsupported feature/build yields a typed error. Typed `$param` values bind only in value positions; unbound, unused and mismatched values fail. | EH-434–EH-447, EH-452, RF-007 |
| FQR-02 | Support graph traversal direction, edge predicates, named `LET`/`FROM`/`JOIN` subplans, named score channels, proof requests, `VALIDATE SHAPE`, KnowledgeSet, per-stage `EXPLAIN`/`PROFILE`, and explicit result/traversal budgets. UQL is read-only on a shared snapshot; graph-free SQL and log-search SQL are independently read-only. | EH-196, EH-197, EH-387, EH-435, EH-437, EH-438, EH-439, EH-450 |
| FQR-03 | The optimizer may push projection, exact filters, top-N, aggregates, same-source joins and batched keys only when a source capability proves that operation sound. Keep an exact local residual for unsupported/inexact operations. Optimized execution must match the full-fetch oracle; there is no silent first-page, top-k or budget truncation. | EH-563–EH-569, EH-571–EH-575, EH-577–EH-580, EH-583 |
| FQR-04 | Bind every remote fragment to an owner-scoped grant, pinned outbound target, read-only query validation and shared SSRF policy. Use per-query limits for requests, rows, bind keys and elapsed time. Refuse a `RequiresKeys` full scan; redact credentials and untrusted labels from trace, logs and errors. | EH-373, EH-378, EH-570, EH-574, EH-581 |
| FQR-05 | Key foreign caches by source identity, owner salt, canonical request and fresh source watermark. Missing/stale watermark disables the hit. Learned cardinality estimates carry sample count and provenance; registration-time probes and rate limits cannot bypass budget or policy. | EH-393, EH-400, EH-575, EH-576 |
| FQR-06 | OBDA evaluates approved named virtual graphs with per-predicate scan groups, sound constant and semi-join reduction, typed SQL rendering and direct pushed solutions where sound. SPARQL `SERVICE` bind joins preserve multivalued RDF answers and use the same outbound guard. Iceberg pruning occurs after provider filters/projection/limit are known. | EH-569, EH-573, EH-577, EH-578, EH-583; interface to EH-664–EH-666 |
| FQR-07 | Text/vector and edge-native search filter tenant, purpose and RLS *before* candidate truncation. Mixed traversal plus vector continues candidate generation until `k` permitted matches or returns a typed shortfall. Index create/backfill/activate/drop is fenced and durable; unavailable generations use a correct fallback. Graph reopen uses restored index state or a bounded rebuild, with a typed partial-materialization state until reads are safe. | RF-019, EH-351, EH-352, EH-532, EH-559, EH-564, EH-565 |
| FQR-08 | Canonical EG GraphSchema and ontology generations drive proof and reasoning. EL/RL and tableau agree on consistency, disjointness and bounded derivation; a skipped projection event triggers rebuild before serving. Every proof binds premises and source/generation; no speculative inference grants permission. | EH-119, EH-139, EH-157, EH-197, EH-337, EH-355, EH-356, EH-363 |
| FQR-09 | Query-visible numeric kernels are deterministic, bounded and shared. Series row identity is `series@ts` with f64 channels; rolling constant windows are exactly zero-variance; attribution, propagation, barrier and motif algorithms have explicit seed/CI or exactness contracts. Tail probabilities reuse `eg-numeric`, without a second local erf implementation. | EH-521–EH-523, EH-526, EH-527, EH-529, EH-562, EH-584 |
| FQR-10 | Keep proof, trace, score and source metadata stable through Rust server and generated Python client contracts; client-visible errors distinguish authorization, unsupported feature, budget, source-needs-keys and source failure. | EH-197, EH-441–EH-449, EH-571 |

## Delivery crosswalk

The following states reflect the recorded program snapshot, not a fresh exact-head acceptance audit. Preserve the stable IDs in PRs and evidence. A contributor closes only the rows exercised by their changed behavior.

| Workstream | Recorded source state | Acceptance still required |
|---|---|---|
| UQL language and serving, EH-434–EH-450, EH-452 | Mostly `BUILT` in Train 4 | Final merged-head grammar, served client, snapshot, proof and exhaustive round-trip gates |
| Federation phase 1, EH-566–EH-570 | `BUILT` in Train 6 | Optimized/full-fetch differential, outbound security, hosted and merged-head proof |
| Federation phase 2/3, EH-563, EH-571–EH-581 | `QUEUED` in the recorded train snapshot; some later work may be present | Per-source capability and served oracle for each row; do not infer acceptance from code presence |
| Semantic index and search, RF-019, EH-351/352/532/564/565 | Source slices `BUILT` or `BUILDING` | Fixed authority, restart, RLS, recall, held-out routing, and measured performance |
| Reasoning and proof, EH-119/139/197/337/355/356/363 | Earlier rows report `LANDED`; later corpus and schema slices report `BUILT` | Exact current-head corpus compose, consistency, replay and proof verification |
| Query-visible numerics, EH-521–EH-523/526/527/529/562/584 | Mixed `BUILT` and `QUEUED` | Exact/golden numerical oracles, streamed parity, and resource limits |

The following IDs are classified here for traceability but require no EG feature duplicate: EH-144 fixes the format-neutral reasoning rejection decision; EH-156 is folded into the shared derivation budget; EH-392 and EH-451 are downstream TUI/client query-text fixes; EH-415 is finance signal-state maintenance; EH-659 is the SQL-provider file-size quality repair. Their behavior remains covered by the relevant client, finance, quality or reasoning acceptance, not by a second EG implementation.

## Success criteria

All P0 scenarios pass for native and foreign query routes at the exact merged head. A recording mock verifies that every pushed request narrows data transfer without changing answers; unsupported/inexact pushdown retains the residual. Security negatives yield stable codes and zero leaked credentials. The search oracle returns all permitted matches up to `k`, or an explicit shortfall. Corpus compose and restart complete within declared budgets without stack overflow. Numerical fixtures meet the tolerances in [test-spec.md](test-spec.md). Every accepted row has a public commit, exact test command/result, and an evidence entry; source existence alone leaves acceptance open.
