//! `AgentComponent.content`: serve one component revision's verified bytes.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;

/// Read one component revision through its tenant-scoped Agent Library holder,
/// then verify the engine-owned CAS bytes before returning them.
pub(crate) async fn handle_component_content(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::agent_component::AgentComponentContentRequest,
) -> Response {
    serve_component_content(state, store, req_id, verified, request).await
}

async fn serve_component_content(
    state: &Arc<RwLock<ServerState>>,
    store: &AgentLibraryStore,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::agent_component::AgentComponentContentRequest,
) -> Response {
    if request.tenant_id != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: component content tenant must match verified request tenant",
        );
    }
    #[cfg(not(feature = "blob"))]
    {
        let _ = (state, store, request);
        return Response::err(req_id, "AgentComponent.content requires the blob feature");
    }
    #[cfg(feature = "blob")]
    {
        let pointer = match store.connector_pack_content_pointer(
            &request.tenant_id,
            &request.component_id,
            request.entry_revision,
        ) {
            Ok(pointer) => pointer,
            Err(error) => return Response::err(req_id, error),
        };
        let blob = match state.read().await.blob.clone() {
            Some(blob) => blob,
            None => return Response::err(req_id, "AgentComponent.content Blob substrate disabled"),
        };
        let tenant_id = request.tenant_id;
        let read = tokio::task::spawn_blocking(move || {
            crate::server::blob::engine_bodies::read_engine_body(
                blob.store.as_ref(),
                &tenant_id,
                &pointer.engine_manifest_digest,
                pointer.body_sha256,
                pointer.length,
            )
            .map(|body| (pointer, body))
        })
        .await;
        let (pointer, body) = match read {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Response::err(req_id, error),
            Err(error) => {
                return Response::err(req_id, format!("component body read task failed: {error}"))
            }
        };
        let result = eg_types::agent_component::AgentComponentContentResult {
            schema_version: eg_types::agent_component::COMPONENT_CONTENT_SCHEMA_VERSION,
            component_id: pointer.component_id,
            entry_revision: pointer.entry_revision,
            definition_digest: pointer.definition_digest,
            content_digest: format!("sha256:{}", pointer.body_sha256),
            media_type: pointer.media_type,
            body,
        };
        Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentContent>(
                &result,
            ),
        )
    }
}
