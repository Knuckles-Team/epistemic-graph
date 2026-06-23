//! Per-domain graph-op handlers. The dispatch shell (super::dispatch_graph_op)
//! routes each method here; cross-cutting write side-effects (dirty/WAL/gauge)
//! stay centralized in the shell. Each module exposes `try_handle(...) ->
//! Result<Response, Method>`: `Ok` = handled, `Err(method)` = not mine, try next.

// Feature-gated compute domains: a slim `--features server` build (no `compute`)
// excludes these entirely; their Method::Finance*/Ds* variants then fall to the
// graph_ops catch-all (a "not available in this build" error). Mirrors the `ast`
// gating of ParseFile(s).
#[cfg(feature = "datascience")]
pub(crate) mod datascience;
#[cfg(feature = "finance")]
pub(crate) mod finance;
pub(crate) mod graph_ops;
// Read-only query surface: SQL (CONCEPT:KG-2.178, DataFusion behind `query`) AND
// Cypher (CONCEPT:KG-2.179, dep-free behind `cypher`). Present when EITHER feature
// is on — a cypher-only Pi build keeps the module (and routes CypherQuery) WITHOUT
// DataFusion. With neither feature the Sql/CypherQuery variants fall to the
// graph_ops "not available" catch-all.
#[cfg(any(feature = "query", feature = "cypher"))]
pub(crate) mod query;
// Multi-op OCC ACID transactions (CONCEPT:KG-2.180). Always present with the
// `server` feature — the Txn* methods are stateful (need `state`) and not
// feature-gated to a compute domain.
pub(crate) mod txn;
// Native time-series store + primitives (CONCEPT:KG-2.210/211, feature `tsdb`). The
// Ts* methods are stateful (need the `SeriesStore` on `state`); a slim build without
// `tsdb` omits the module and the variants fall to the graph_ops not-built catch-all.
#[cfg(feature = "tsdb")]
pub(crate) mod timeseries;
