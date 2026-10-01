# Architecture and contracts

## Existing components to reuse

| Layer | Existing owner and path | Required extension |
|---|---|---|
| UQL syntax and semantic lowering | `crates/eg-plan/src/uql/{grammar,lexer,parser,serve,print}.rs` | Add syntax at the grammar table, parser, canonical printer and execution map together; retain one text method. |
| Query execution and optimizer | `crates/eg-plan/src/{exec,optimizer,federation}.rs`, `federation_opt/` | Build one remote-fragment planner and exact residual, not one optimizer per wire. |
| Foreign security | `crates/eg-plan/src/federation_ssrf.rs`, server owner-scoped foreign catalog | Resolve the registered grant before planning; DNS-pin and validate every outbound request. |
| SQL and Iceberg | `crates/eg-query/src/sql/{mod,iceberg_federation}.rs` | Use DataFusion provider pushdown and shared SQL renderer; do not construct an independent SQL authority. |
| RDF/OBDA | `crates/eg-rdf/src/{sparql,obda,owl}.rs`, `src/server/handlers/rdf/` | Lower approved virtual graphs and `SERVICE` through the same grants, budgets and trace. |
| Search and indexes | `crates/eg-query/src/tables/{ann_authority,index}.rs`, `crates/eg-core/src/compute/semantic_ann_*` | Keep generation authority, fences and maintenance under existing index lifecycle. |
| Numerical kernels | `crates/eg-numeric/`, `crates/eg-tsdb/`, `crates/eg-compute/` | Reuse deterministic statistical kernels from every query surface. |
| Public wire | `src/server/handlers/query/`, `epistemic_graph/generated/query.py` | Generate contracts from Rust ownership; add served conformance, no hand-edited generated client. |

## Data flow

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

## Query and federation contract

`SourceCapabilities` declares projection, filter precision (`Exact`, `Inexact`, `Unsupported`), order, limit, aggregates, same-source join, batched key lookup, paging, full-fetch allowance, rate and statistics. The planner may rely only on verified capabilities from the registered source, never a caller assertion. `RemoteRequest` carries canonical keys, projection, predicate, order, limit and page. `FederationSession` records request/row/key/wall budgets and a sanitized `FederationTrace` for every attempt and fallback. A remote result may be a superset when a local exact residual remains; it may never omit a required row. `FEDERATION_BUDGET_EXCEEDED:<dimension>` and `FEDERATION_SOURCE_REQUIRES_KEYS` are terminal typed refusals, not partial success.

For SQL, render identifiers/literals once through the shared renderer and revalidate the complete read-only statement. A pushed limit must respect `ORDER BY`, existing `LIMIT`, and dialect semantics. For HTTP, placeholders are allowed only in query values; percent-encode them, validate the fixed authority at every page, and continue until short page, declared limit or budget. For SPARQL `SERVICE`, send bounded `VALUES` batches and retain exact local filters. For OBDA, derive filters per predicate-object mapping; a condition valid for one triple pattern must not filter a different pattern from the same row. For Iceberg, manifest pruning must be measured after DataFusion has supplied filters and projection.

## Semantic, reasoning and numeric contract

Search indexes bind stable node or parallel-edge IDs and a generation. Tenant/purpose/RLS predicates constrain the candidate set before BM25 or ANN top-k. Backfill activates atomically only after a durable, verified generation; on restart or blocked generation, planner chooses a correct scan or typed unavailable result. No stale index result is presented as fresh. Fixed schema generations compose EL/RL and tableau under explicit step/work budgets; inconsistency names unsatisfiable classes, and a skipped reasoning projection event requires rebuild. `WITH PROOF` exposes derivation premises and source/generation IDs for verifiable results.

Time-series operators preserve `series@timestamp` identity and f64 channel values. Implement rolling statistics with O(1) update plus an exact constant-window result; use a re-anchor strategy and compare to a recomputed oracle. Attribution gives exact Shapley only through 16 factors, otherwise seeded sampling with confidence interval. Impact propagation is exact on a DAG and bounded on a cycle, or seeded Monte Carlo with a CI. Motif/discord algorithms consume query budgets and expose partial *progress* only when their result type explicitly says so. Probability tails call one `eg-numeric` CDF/survival implementation.

## Interface to the data-plane owner

### EG-FEDERATED-QUERY-R045 / EG-FEDERATED-QUERY-R052 implementation seam

Extend `crates/eg-plan/src/federation.rs`, `federation_opt/` and the existing `ForeignSource` capability/result contract so `ForeignRows` carries columns through planning and execution. Keep exact residual evaluation in the shared optimizer, then project final requested columns after residual, order and proof needs are satisfied. Use the registered `RemoteEngineSource::fetch_uql` path and `Method::Uql` contract for pushed remote fragments. Resolve signed context and owner grant in the server foreign catalog before the request; do not create a second remote client, parser or source registry. Generated Python and wire shapes change only from Rust contract sources.

The local `unified-data-plane` spec supplies `AttachedSourceIdentity`, owner grants, dialect capability evidence, source watermark, and approved named virtual-graph mapping. This spec consumes those values via the existing foreign catalog and planner. Mapping proposal approval, catalog extraction, change capture, dialect certification and native/accelerated route policy stay there. Query tests must cover an attached-source implementation without depending on a private deployment.

## Compatibility and observability

Keep generated Rust/Python/wire method contracts synchronized. A grammar or error-code change requires canonical parser fixtures and generated client parity. Preserve read-only snapshot behavior; a new modality cannot bypass `Method::Uql` authorization or mutate state. Explain/profiling use registered source label, kind and short opaque fingerprint, never URL, DSN, credential or raw untrusted label. Metrics count budget refusals, source failures, fallback paths, remote bytes/rows, index-generation fallback, and reasoning rebuilds without high-cardinality secrets.
