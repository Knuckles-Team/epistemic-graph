# Implementation plan

## Sequence

1. **Freeze the baseline.** Record exact public default-branch SHA, method registry, current feature profiles, source kinds, index generations and existing tests. Classify each stable obligation as source present, absent, or incompatible; retain separate acceptance status. Do not translate a historical `BUILT` label into current acceptance.
2. **Close the read contract.** Make UQL grammar, lowering, printer, method registry, generated Python sender, proof and error codes agree. Add `LET`/`JOIN`, named score channels and `EXPLAIN`/`PROFILE` only where they can execute; otherwise return `UQL_UNSUPPORTED` with a stable span. Preserve the shared read snapshot and typed parameter path.
3. **Qualify federation phase 1.** Run full-fetch versus optimized oracles for SQL, HTTP, OBDA and registered foreign sources. Repair any unsound pushdown or missing residual, validate read-only rendering and single outbound guard, and attach budget/trace to the served plan.
4. **Complete phase 2.** Add column-carrying rows, direct OBDA solutions, per-source concurrency, SPARQL `SERVICE` bind joins, Iceberg manifest pruning and RemoteEngine pushdown. Keep each capability off until its conformance oracle passes. The attached-source owner supplies registry identity and approved mappings.
5. **Complete phase 3.** Add persisted source statistics with provenance, freshness/owner-bound fragment caching and additional source kinds. Cache only when the source watermark is known and current. Retire alternate path-specific guards and source catalogs once callers use the common route.
6. **Close semantic reads.** Finish generation fencing, restart/replay, filtered text/vector/edge retrieval and mixed traversal top-k. Run corpus reasoning and proof verification, including disjointness and skipped-event rebuild. Expose index fallback in explain output.
7. **Close numerical surfaces.** Implement exact/golden series, attribution, propagation, barrier and motif kernels behind one public syntax and shared compute code. Add deterministic seed and budget handling before enabling a new operator on the served wire.
8. **Publish evidence.** For each stable ID, record merged public SHA, exact test profile, positive/negative result, quality gate result and client/generator version in the native spec evidence. Mark `ACCEPTED` only after exact-head gates and affected downstream client compatibility pass.

9. **Ontology source selection.** Add a read method that takes question classes and returns the class path and per-class approved mappings with premises. Reuse the reasoner for subclass entailment and the unified-data-plane mapping approval state. Add operation-bound source kinds to the foreign source registry under the shared outbound guard. (EG-FEDERATED-QUERY-R072, EG-FEDERATED-QUERY-R073)

## Incremental delivery slices

| Slice | Depends on | Exit artifact |
|---|---|---|
| UQL/served contract | Existing `eg-plan`, method registry | Canonical grammar + generated sender + served positive/negative tests |
| Federation common core | Owner-scoped foreign catalog | Recorded remote requests, exact residual, typed budget/SSRF refusals |
| OBDA and SERVICE | Approved mappings and source identity | Direct solution and bind-join differential fixtures |
| Search and reasoning | Fixed GraphSchema and index authority | Restart/replay, prefilter/top-k, corpus consistency and proof fixtures |
| Query numerics | `eg-numeric`, `eg-tsdb`, `eg-compute` | Shared kernel goldens, deterministic CI/seed and bounded execution |

## Decisions and risk controls

- Favor narrow extensions to existing `eg-plan`, `eg-rdf`, `eg-query`, `eg-numeric` and server paths. No second query planner, registry, schema authority, index lifecycle or statistical kernel.
- Keep optimizer fallback reachable for differential testing and incident mitigation. Never bypass authorization or an outbound refusal.
- Treat SQL without explicit ordering as an unordered relation; tests compare multisets. With explicit ordering, compare ordered rows and top-N exactly.
- Treat source-feature claims as untrusted until probes or conformance establish them; a failed probe lowers capability.
- A missing watermark disables foreign-result cache. A failed semantic generation activation leaves the prior valid generation or uses a correct fallback. It does not return silently stale rows.
- Keep optional source kinds independently gated so a missing driver or live service does not block pure parser, planner, and mock tests for contributors.

The [architecture](plan.md) defines interfaces and ownership. The [test specification](test-spec.md) is the release contract, and [tasks](tasks.md) maps stable work IDs to reviewable deliveries.

## Architecture

### Existing components to reuse

| Layer | Existing owner and path | Required extension |
|---|---|---|
| UQL syntax and semantic lowering | `crates/eg-plan/src/uql/{grammar,lexer,parser,serve,print}.rs` | Add syntax at the grammar table, parser, canonical printer and execution map together. Retain one text method. |
| Query execution and optimizer | `crates/eg-plan/src/{exec,optimizer,federation}.rs`, `federation_opt/` | Build one remote-fragment planner and exact residual, not one optimizer per wire. |
| Foreign security | `crates/eg-plan/src/federation_ssrf.rs`, server owner-scoped foreign catalog | Resolve the registered grant before planning; DNS-pin and validate every outbound request. |
| SQL and Iceberg | `crates/eg-query/src/sql/{mod,iceberg_federation}.rs` | Use DataFusion provider pushdown and shared SQL renderer; do not construct an independent SQL authority. |
| RDF/OBDA | `crates/eg-rdf/src/{sparql,obda,owl}.rs`, `src/server/handlers/rdf/` | Lower approved virtual graphs and `SERVICE` through the same grants, budgets and trace. |
| Search and indexes | `crates/eg-query/src/tables/{ann_authority,index}.rs`, `crates/eg-core/src/compute/semantic_ann_*` | Keep generation authority, fences and maintenance under existing index lifecycle. |
| Numerical kernels | `crates/eg-numeric/`, `crates/eg-tsdb/`, `crates/eg-compute/` | Reuse deterministic statistical kernels from every query surface. |
| Public wire | `src/server/handlers/query/`, `epistemic_graph/generated/query.py` | Generate contracts from Rust ownership; add served conformance, no hand-edited generated client. |

### Data flow

```text
authenticated request + owner scope + snapshot
  -> parse / typed bind / capability gate
  -> canonical plan + authorization-filtered source resolution
  -> optimizer: local residual + permitted remote fragments
  -> guarded source request / native index and reasoning generations
  -> exact residual + budget meter + proof/score channels
  -> typed rows + EXPLAIN/PROFILE + stable error code
```

No optimizer step may widen the set of sources, columns, rows visible to the caller, or outbound destinations. Remote fragments receive only keys from the caller's already authorized candidate set. Proof labels, source statistics and cached rows carry tenant, owner, generation and visibility identity. A grant revocation or schema generation change invalidates affected plans and fragments.

### Query and federation contract

`SourceCapabilities` declares projection, filter precision (`Exact`, `Inexact`, `Unsupported`), order, limit, aggregates, same-source join, batched key lookup, paging, full-fetch allowance, rate and statistics. The planner may rely only on verified capabilities from the registered source, never a caller assertion. `RemoteRequest` carries canonical keys, projection, predicate, order, limit and page. `FederationSession` records request/row/key/wall budgets and a sanitized `FederationTrace` for every attempt and fallback. A remote result may be a superset when a local exact residual remains; it may never omit a required row. `FEDERATION_BUDGET_EXCEEDED:<dimension>` and `FEDERATION_SOURCE_REQUIRES_KEYS` are terminal typed refusals, not partial success.

For SQL, render identifiers/literals once through the shared renderer and revalidate the complete read-only statement. A pushed limit must respect `ORDER BY`, existing `LIMIT`, and dialect semantics. For HTTP, placeholders are allowed only in query values. Percent-encode them, validate the fixed authority at every page, and continue until short page, declared limit or budget. For SPARQL `SERVICE`, send bounded `VALUES` batches and retain exact local filters. For OBDA, derive filters per predicate-object mapping. A condition valid for one triple pattern must not filter a different pattern from the same row. For Iceberg, manifest pruning must be measured after DataFusion has supplied filters and projection.

### Semantic, reasoning and numeric contract

Search indexes bind stable node or parallel-edge IDs and a generation. Tenant/purpose/RLS predicates constrain the candidate set before BM25 or ANN top-k. Backfill activates atomically only after a durable, verified generation; on restart or blocked generation, planner chooses a correct scan or typed unavailable result. No stale index result is presented as fresh. Fixed schema generations compose EL/RL and tableau under explicit step/work budgets; inconsistency names unsatisfiable classes, and a skipped reasoning projection event requires rebuild. `WITH PROOF` exposes derivation premises and source/generation IDs for verifiable results.

Time-series operators preserve `series@timestamp` identity and f64 channel values. Implement rolling statistics with O(1) update plus an exact constant-window result. Use a re-anchor strategy and compare to a recomputed oracle. Attribution gives exact Shapley only through 16 factors, otherwise seeded sampling with confidence interval. Impact propagation is exact on a DAG and bounded on a cycle, or seeded Monte Carlo with a CI. Motif/discord algorithms consume query budgets and expose partial *progress* only when their result type explicitly says so. Probability tails call one `eg-numeric` CDF/survival implementation.

### Interface to the data-plane owner

#### EG-FEDERATED-QUERY-R045 / EG-FEDERATED-QUERY-R052 implementation seam

Extend `crates/eg-plan/src/federation.rs`, `federation_opt/` and the existing `ForeignSource` capability/result contract so `ForeignRows` carries columns through planning and execution. Keep exact residual evaluation in the shared optimizer, then project final requested columns after residual, order and proof needs are satisfied. Use the registered `RemoteEngineSource::fetch_uql` path and `Method::Uql` contract for pushed remote fragments. Resolve signed context and owner grant in the server foreign catalog before the request; do not create a second remote client, parser or source registry. Generated Python and wire shapes change only from Rust contract sources.

The local `unified-data-plane` spec supplies `AttachedSourceIdentity`, owner grants, dialect capability evidence, source watermark, and approved named virtual-graph mapping. This spec consumes those values via the existing foreign catalog and planner. Mapping proposal approval, catalog extraction, change capture, dialect certification and native/accelerated route policy stay there. Query tests must cover an attached-source implementation without depending on a private deployment.

### Compatibility and observability

Keep generated Rust/Python/wire method contracts synchronized. A grammar or error-code change requires canonical parser fixtures and generated client parity. Preserve read-only snapshot behavior; a new modality cannot bypass `Method::Uql` authorization or mutate state. Explain/profiling use registered source label, kind and short opaque fingerprint, never URL, DSN, credential or raw untrusted label. Metrics count budget refusals, source failures, fallback paths, remote bytes/rows, index-generation fallback, and reasoning rebuilds without high-cardinality secrets.

## Audit record

### 2026-10-11 — EG-FEDERATED-QUERY-R056 numerical tail acceptance

At main `6bf4ea1c7e80a375e7053e677b0ed28c162131ec`, the complete production
`crates/eg-compute/src/mining/causal_impact.rs` module and exact production
`eg-numeric` error, math, reduce, validate and kernels modules compiled in a
standalone Rust 1.96.0 test harness linked to the cached real `libm` dependency.
All five causal-impact tests passed. The acceptance test compares two-sided
probabilities at both signs of z = 3, 6, 8, 10 and 20 against checked-in
mpmath 1.3.0 50-digit references at relative tolerance below 1e-12, plus
zero and positive-infinity boundaries. The production path calls the shared
`normal_sf`; no copied approximation, substitute kernel or mock was used.

The literal test binding records this pure-module leaf only. The standalone
harness does not establish the full eg-compute crate, server, client, cross-host
or hosted release gates. No broader acceptance is claimed.

## Amendments

### 2026-10-11 — EG-FEDERATED-QUERY-R058.3.1 source-guard repair

Scope the no-cache guard to production NL handlers, excluding the adjacent
transaction handler and the test module's own forbidden-reference fixtures.
Missing source boundaries fail closed. Negative controls inject `ResultCache`,
a direct `served_response` call, and a generic `served_response` call into
the production NL section. Preserve the uncached response-path assertion.
Standalone exact-source Rust tests establish this structural guard only; they
do not establish full facade compilation or end-to-end NL query behavior.

## Recovered requirement: vectorized SQL columns

Data flow: SQL row write → `ColumnVectorBinding` check → queued embed → ANN index → nearest-neighbour query.
Attach point: `crates/eg-query/src/sql/ann.rs` and `ann_pushdown.rs`, which already own the ANN path.
Reuses the existing ANN index and embedding call. Source data stays in the SQL table.
Decision: opt-in binding per column. Rejected: embedding every text column, which spends embedder budget on unused columns.

## EG-FEDERATED-QUERY-R078 tenant predicate inside the plan

Data flow: served UQL statement → `dispatch_query_family` (`src/server/handlers/query/dispatch.rs:66`) → `handle_uql` (`src/server/handlers/query/uql_statement.rs:17`) → `inject_scope` → scan nodes → scoped rows.
Types: `PlanScope{tenant, clearance_labels, verified}` in `crates/eg-query/src/plan_scope.rs`. The predicate set is closed: tenant equality and label subset.
Attach point: after planning, before execution, so no scan runs unscoped. Production caller is `dispatch_query_family`.
Failure: an unverified caller gets `PLAN_SCOPE_UNVERIFIED` and no read runs.
Reuses the scope check pattern in `src/server/semantic_index.rs:145`. New: the plan-level predicate.
Decision: scope inside the plan. Rejected: filtering result rows after retrieval, which leaks through counts, timing and ranking.
Cross-repository: `AU-SEMANTIC` rows pass the verified tenant through the request context; owner prefix `AU-SEMANTIC`, contract `VerifiedRequestContext`.

References
- No paper is cited.
