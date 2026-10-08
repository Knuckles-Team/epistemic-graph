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
- Keep optimizer fallback reachable for differential testing and incident mitigation, but never use it to bypass authorization or an outbound refusal.
- Treat SQL without explicit ordering as an unordered relation; tests compare multisets. With explicit ordering, compare ordered rows and top-N exactly.
- Treat source-feature claims as untrusted until probes or conformance establish them; a failed probe lowers capability.
- A missing watermark disables foreign-result cache. A failed semantic generation activation leaves the prior valid generation or uses a correct fallback; it does not return silently stale rows.
- Keep optional source kinds independently gated so a missing driver or live service does not block pure parser, planner, and mock tests for contributors.

The [architecture](architecture.md) defines interfaces and ownership. The [test specification](test-spec.md) is the release contract, and [tasks](tasks.md) maps stable work IDs to reviewable deliveries.
