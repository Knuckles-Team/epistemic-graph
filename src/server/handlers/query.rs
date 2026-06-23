//! Read-only query handler. Owns BOTH query methods (one module per domain, per
//! the dispatch conventions — `Sql` + `CypherQuery` are the one `// ── Query ──`
//! protocol section):
//!   * `Method::Sql` (CONCEPT:KG-2.178, feature `query`) — `SELECT … FROM nodes …`
//!     over ONE graph via DataFusion (eg-query::exec_sql).
//!   * `Method::CypherQuery` (CONCEPT:KG-2.179, feature `cypher`) — `MATCH … RETURN
//!     …` over ONE graph, DEP-FREE (eg-query::exec_cypher; label index / VF2 / BFS,
//!     no DataFusion). This is the lean-Pi query path.
//!
//! Both are read-only — they cannot mutate — so the centralized write side-effects
//! (dirty/WAL/gauge) in the dispatch shell never fire for them.
//!
//! Off-lock execution: take the owned `analysis_snapshot()` (a GraphView that
//! shares property bytes by Arc) under a brief read lock, then run on the blocking
//! pool via `compute_off_lock` — the VF2/algorithm idiom, so no query work runs on
//! a reactor worker or under the graph lock.
//!
//! Each arm is gated on ITS feature and returns `Err(method)` when its feature is
//! off, so a method whose feature is absent falls through to the graph_ops
//! "not available in this build" catch-all (never a panic, never a mis-route).

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use super::super::compute::compute_off_lock;
use crate::graph::GraphCore;
use crate::protocol::Method;
#[cfg(any(feature = "query", feature = "cypher"))]
use crate::protocol::{Response, ResultPayload};

/// Handle `Method::Sql` / `Method::CypherQuery`. `Err(method)` hands a non-query
/// method (or a query method whose feature is off) back to the dispatcher
/// (routing fall-through). (CONCEPT:KG-2.19 — server dispatch convention)
pub(crate) async fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        #[cfg(feature = "query")]
        Method::Sql { query, .. } => {
            // Owned, off-lock snapshot (shares property bytes by Arc).
            let snap = core.analysis_snapshot();
            let resp =
                match compute_off_lock(req_id, move || eg_query::exec_sql(&snap, &query)).await {
                    Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
                    Ok(Err(msg)) => Response::err(req_id, format!("SQL error: {msg}")),
                    Err(resp) => resp,
                };
            Ok(resp)
        }
        #[cfg(feature = "cypher")]
        Method::CypherQuery { query } => {
            // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
            // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
            let snap = core.analysis_snapshot();
            let resp = match compute_off_lock(req_id, move || eg_query::exec_cypher(&snap, &query))
                .await
            {
                Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
                Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        other => Err(other),
    }
}
