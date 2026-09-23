//! `Method::FleetCatalog` (EH-345): the server registry's discovery
//! observations and operator overrides, and the one read projection that joins
//! them with connector-pack `AgentComponent`s.
//!
//! No second catalog: servers stay `:Server` rows ([`super::registry`]), content
//! stays `AgentComponent` revisions, and the only records this adds are the two
//! facts neither of those holds -- who observed which connector under which
//! authority, and what an operator overrode. Both are tenant-private, so both
//! live in the tenant-scoped Agent Library owner beside the components they
//! describe (`persistence::fleet_records`), never in the shared `__commons__`.
//!
//! * `records` -- record identity, body encoding, visibility.
//! * `write` -- one owner-scoped compare-and-set per request.
//! * `read` -- the tenant's records, the registry's desired state, components.
//! * `project` -- rows, filter, order, digest, page.

use eg_types::fleet_catalog::FleetCatalogOp;
#[cfg(feature = "redb")]
use eg_types::result_contract::MethodResult;

use super::*;

#[cfg(feature = "redb")]
mod project;
#[cfg(feature = "redb")]
mod read;
#[cfg(feature = "redb")]
mod records;
#[cfg(feature = "redb")]
mod write;

/// Encode a fleet catalog body as `M`'s declared result, or answer the refusal.
#[cfg(feature = "redb")]
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
    verified: &VerifiedRequestContext,
    op: FleetCatalogOp,
) -> Response {
    if let Err(error) = op.validate() {
        return Response::err(req_id, error);
    }
    serve(state, req_id, verified, op).await
}

#[cfg(feature = "redb")]
async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: FleetCatalogOp,
) -> Response {
    let store = match state.write().await.ensure_agent_library() {
        Ok(store) => store,
        Err(error) => return Response::err(req_id, error),
    };
    match op {
        FleetCatalogOp::RecordDiscovery { request } => {
            write::record_discovery(&store, req_id, verified, request)
        }
        FleetCatalogOp::SetOverride { request } => {
            write::set_override(&store, req_id, verified, request)
        }
        FleetCatalogOp::ClearOverride { request } => {
            write::clear_override(&store, req_id, verified, request)
        }
        FleetCatalogOp::List { request } => {
            read::list(state, &store, req_id, verified, request).await
        }
        FleetCatalogOp::Lookup { request } => {
            read::lookup(state, &store, req_id, verified, request).await
        }
    }
}

#[cfg(not(feature = "redb"))]
async fn serve(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _verified: &VerifiedRequestContext,
    _op: FleetCatalogOp,
) -> Response {
    Response::err(
        req_id,
        "the fleet catalog is not available in this build (requires the `redb` feature)",
    )
}
