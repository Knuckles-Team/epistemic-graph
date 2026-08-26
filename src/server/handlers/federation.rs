//! Query-federation handlers (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote epistemic-graph
//! engine or an HTTP/JSON API — [`eg_types::wire::ForeignSourceSpec`]) in the
//! process-global `foreign_sources` map on `ServerState`, so it can be reused by name.
//! The actual cross-engine / HTTP fetch is driven by the unified-query handler: an
//! inline-spec `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an
//! `Op::Foreign` (the UQL `FOREIGN "<name>"` marker) resolves THIS map through the
//! `eg_plan::federation::ForeignSourceRegistry` `run_unified` builds from it
//! (`handlers::query::foreign_registry_from`, CONCEPT:EG-KG.query.closure-backed-source)
//! — so a source registered here is genuinely queryable, not merely recorded. This
//! handler is the registration surface. Process-global (a foreign endpoint is not
//! graph-scoped), so it takes `state`. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};

/// Try to handle a federation method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::RegisterForeignSource { name, source } => {
            let sources = {
                let s = state.read().await;
                s.foreign_sources.clone()
            };
            sources.insert(name.clone(), source);
            Ok(Response::ok(req_id, ResultPayload::String(name)))
        }
        other => Err(other),
    }
}
