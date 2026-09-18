//! `Method::ConnectorPack`: atomic connector MCP pack import and its admin
//! surface (RF-ADR-009).
//!
//! The op match below is the whole routing decision and it is EXHAUSTIVE, with
//! no catch-all: a new operation must be given an owner here rather than
//! inheriting one. Each arm's target is owned by a different package after S1,
//! which is why they are separate functions rather than one body.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::contract_wave::contract_wave_stub;
use crate::server::state::ServerState;

pub(crate) mod admin;
pub(crate) mod reconcile;
pub(crate) mod reproject;

/// Route one connector-pack operation to the handler that owns it.
pub(crate) async fn handle_connector_pack(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: eg_types::connector_pack::ConnectorPackOp,
) -> Response {
    use eg_types::connector_pack::ConnectorPackOp;

    match op {
        ConnectorPackOp::Status { request } => serve_status(state, req_id, verified, request).await,
        ConnectorPackOp::Import { request } => serve_import(state, req_id, verified, request).await,
        ConnectorPackOp::Bind { request } => {
            admin::serve_bind(state, req_id, verified, request).await
        }
        ConnectorPackOp::Unbind { request } => {
            admin::serve_unbind(state, req_id, verified, request).await
        }
        ConnectorPackOp::Retire { request } => {
            admin::serve_retire(state, req_id, verified, request).await
        }
        ConnectorPackOp::Reproject { request } => {
            reproject::serve(state, req_id, verified, request).await
        }
        ConnectorPackOp::ReconcileBodies { request } => {
            reconcile::serve(state, req_id, verified, request).await
        }
    }
}

contract_wave_stub! {
    /// Read one connector's head, member counts, last receipt and warnings.
    serve_status(eg_types::connector_pack::ConnectorPackStatusRequest)
        refuses "ConnectorPack.status", tested by status_stub_tests
}

contract_wave_stub! {
    /// Validate and import one pack atomically, or refuse it with every
    /// violation named.
    serve_import(Box<eg_types::connector_pack::ConnectorPackImportRequest>)
        refuses "ConnectorPack.import", tested by import_stub_tests
}
