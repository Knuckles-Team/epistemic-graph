//! `Method::FleetCatalog` (EH-345): the server registry's discovery
//! observations and operator overrides, and the one read projection that joins
//! them with connector-pack `AgentComponent`s.
//!
//! No second catalog: servers stay `:Server` rows ([`super::registry`]), content
//! stays `AgentComponent` revisions, and the only records this adds are the two
//! facts neither of those holds -- who observed which connector under which
//! authority, and what an operator overrode. Both live beside `:Server` in
//! `__commons__`.
//!
//! * `records` -- node identity, encoding, compare-and-set decision, visibility.
//! * `write` -- one engine compare-and-set per request.
//! * `read` -- the `__commons__` view plus the component reads.
//! * `project` -- rows, filter, order, digest, page.

use eg_types::fleet_catalog::FleetCatalogOp;
use eg_types::result_contract::MethodResult;

use super::*;

mod project;
mod read;
mod records;
mod write;

/// Encode a fleet catalog body as `M`'s declared result, or answer the refusal.
fn respond<M: MethodResult>(req_id: u64, result: Result<M::Body, String>) -> Response {
    match result.and_then(ResultPayload::of::<M>) {
        Ok(payload) => Response::ok(req_id, payload),
        Err(error) => Response::err(req_id, error),
    }
}

/// Route one fleet catalog operation. Scope and admin authority were checked
/// against the op's own `authz_action` before dispatch; tenant and principal
/// come only from `verified`.
pub(in crate::server::dispatch) async fn handle_fleet_catalog(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified: &VerifiedRequestContext,
    op: FleetCatalogOp,
) -> Response {
    if let Err(error) = op.validate() {
        return Response::err(req_id, error);
    }
    match op {
        FleetCatalogOp::RecordDiscovery { request } => {
            write::record_discovery(state, req_id, caller, verified, request).await
        }
        FleetCatalogOp::SetOverride { request } => {
            write::set_override(state, req_id, caller, verified, request).await
        }
        FleetCatalogOp::ClearOverride { request } => {
            write::clear_override(state, req_id, caller, verified, request).await
        }
        FleetCatalogOp::List { request } => read::list(state, req_id, verified, request).await,
        FleetCatalogOp::Lookup { request } => read::lookup(state, req_id, verified, request).await,
    }
}
