//! Read-only SQL query handler (CONCEPT:KG-2.178). Owns `Method::Sql`: a
//! `SELECT … FROM nodes …` over ONE graph via DataFusion (eg-query). Read access
//! only — SELECT cannot mutate, so the centralized write side-effects
//! (dirty/WAL/gauge) in the dispatch shell never fire for it.
//!
//! Off-lock execution: take the owned `analysis_snapshot()` (a GraphView that
//! shares property bytes by Arc) under a brief read lock, then drive DataFusion on
//! the blocking pool via `compute_off_lock` — exactly the VF2/algorithm idiom, so
//! no DataFusion work runs on a reactor worker or under the graph lock.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use super::super::compute::compute_off_lock;
use crate::graph::GraphCore;
use crate::protocol::{Method, Response, ResultPayload};

/// Handle `Method::Sql`. `Err(method)` hands a non-query method back to the
/// dispatcher (routing fall-through). (CONCEPT:KG-2.19 — server dispatch convention)
pub(crate) async fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
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
        other => Err(other),
    }
}
