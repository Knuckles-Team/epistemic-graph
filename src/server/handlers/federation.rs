//! Query-federation handlers (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote epistemic-graph
//! engine or an HTTP/JSON API — [`eg_types::wire::ForeignSourceSpec`]) in the
//! tenant-scoped [`crate::server::foreign_catalog::ForeignSourceCatalog`] on
//! `ServerState`, under the caller's VERIFIED tenant scope (EH-373). The actual
//! cross-engine / HTTP fetch is driven by the unified-query handlers: an inline-spec
//! `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an `Op::Foreign`
//! (the UQL `FOREIGN "<name>"` marker) resolves through the registry
//! `ForeignSourceCatalog::registry_for` builds from the caller's tenant's entries only
//! (CONCEPT:EG-KG.query.closure-backed-source). One tenant can therefore neither use nor
//! overwrite another tenant's registration, even under the same name. Rows a foreign
//! source returns are not RLS-filtered. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

/// Try to handle a federation method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: Option<&CarrierAuthority>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::RegisterForeignSource { name, source } => {
            let Some(owner) = carrier else {
                crate::metrics::access_denied();
                return Ok(Response::err(
                    req_id,
                    "ACCESS_DENIED: RegisterForeignSource requires a verified tenant carrier"
                        .to_string(),
                ));
            };
            let catalog = state.read().await.foreign_sources.clone();
            catalog.register(owner, name.clone(), source);
            Ok(Response::ok(
                req_id,
                ResultPayload::scalar::<eg_types::result_contract::cluster::RegisterForeignSource>(
                    name,
                ),
            ))
        }
        other => Err(other),
    }
}
