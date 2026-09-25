//! Query-federation handlers (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! `RegisterForeignSource` records a named EXTERNAL source (a remote epistemic-graph
//! engine or an HTTP/JSON API — [`eg_types::wire::ForeignSourceSpec`]) in the
//! owner-scoped [`crate::server::foreign_catalog::ForeignSourceCatalog`] on
//! `ServerState`, under the caller's VERIFIED owner — tenant+principal (EH-373). The actual
//! cross-engine / HTTP fetch is driven by the unified-query handlers: an inline-spec
//! `Op::ForeignScan` resolves itself, and a `Named` `Op::ForeignScan` / an `Op::Foreign`
//! (the UQL `FOREIGN "<name>"` marker) resolves through the registry
//! `ForeignSourceCatalog::registry_for` builds from the caller's own entries only
//! (CONCEPT:EG-KG.query.closure-backed-source). One principal can therefore neither use
//! nor overwrite another principal's registration, even under the same name (one engine
//! is bound to one tenant; the tenant stays inside the owner key). Rows a foreign
//! source returns are not RLS-filtered. A lightweight, non-blocking insert — no
//! off-reactor work needed.

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::OwnerScopedCall;

/// Try to handle a federation method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    call: OwnerScopedCall<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = call.req_id;
    match method {
        Method::RegisterForeignSource { name, source } => {
            let owner = match call.owner("RegisterForeignSource") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            if let Err(error) = eg_plan::federation::validate_column_mapping(&source) {
                return Ok(Response::err(req_id, error));
            }
            // EH-378: provision the source's share role (assigned to nobody) under the
            // same write lock as the registration, so a registered source always has one.
            let mut s = call.state.write().await;
            if let Err(error) = crate::server::foreign_share::provision_share_role(
                &mut s.isolation,
                owner.agent_id(),
                &name,
            ) {
                return Ok(Response::err(req_id, error));
            }
            s.foreign_sources.register(&owner, name.clone(), source);
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
