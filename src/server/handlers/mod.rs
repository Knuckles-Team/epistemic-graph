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
