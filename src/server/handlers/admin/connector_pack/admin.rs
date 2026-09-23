//! The connector-pack operations that change what a connector MAY publish:
//! binding an importer, removing that binding, and permanent retirement
//! (PB3, ruling D7).
//!
//! All three run under `admin:connector-pack`, which the request boundary
//! gates behind the RBAC admin capability (never bare `kg:admin`). Each takes
//! the tenant's pack lock, so none of them interleaves with an import between
//! its planning snapshot and its catalog commit.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
#[cfg(feature = "redb")]
use crate::server::persistence::agent_library::AgentLibraryStore;
#[cfg(feature = "redb")]
use crate::server::persistence::connector_pack::admin as binding;
use crate::server::state::ServerState;

/// Bind a connector to the importer allowed to publish its packs.
pub(crate) async fn serve_bind(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackBindRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        return Response::err(req_id, REQUIRES_REDB);
    }
    #[cfg(feature = "redb")]
    {
        let eg_types::connector_pack::ConnectorPackBindRequest {
            context,
            connector,
            importer,
        } = request;
        admin_write::<eg_types::result_contract::storage::ConnectorPackBind, _>(
            AdminWrite {
                state,
                req_id,
                verified,
                context,
                purpose: "connector-pack:bind",
            },
            move |store, context| {
                store.change_connector_pack_binding(
                    context,
                    &connector,
                    binding::BindingChange::Bind {
                        importer: &importer,
                    },
                )
            },
        )
        .await
    }
}

/// Remove a connector's importer binding; the configured bootstrap importer
/// applies to it again.
pub(crate) async fn serve_unbind(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackUnbindRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        return Response::err(req_id, REQUIRES_REDB);
    }
    #[cfg(feature = "redb")]
    {
        let eg_types::connector_pack::ConnectorPackUnbindRequest { context, connector } = request;
        admin_write::<eg_types::result_contract::storage::ConnectorPackUnbind, _>(
            AdminWrite {
                state,
                req_id,
                verified,
                context,
                purpose: "connector-pack:unbind",
            },
            move |store, context| {
                store.change_connector_pack_binding(
                    context,
                    &connector,
                    binding::BindingChange::Unbind,
                )
            },
        )
        .await
    }
}

/// Permanently retire named entries. Unlike withdrawal this cannot be undone
/// by a later import: a retired entry that reappears is
/// `RETIRED_ENTRY_RETURNED`.
pub(crate) async fn serve_retire(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::ConnectorPackRetireRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        return Response::err(req_id, REQUIRES_REDB);
    }
    #[cfg(feature = "redb")]
    {
        let eg_types::connector_pack::ConnectorPackRetireRequest {
            context,
            connector,
            uris,
        } = request;
        admin_write::<eg_types::result_contract::storage::ConnectorPackRetire, _>(
            AdminWrite {
                state,
                req_id,
                verified,
                context,
                purpose: "connector-pack:retire",
            },
            move |store, context| {
                store.retire_connector_pack_entries(context, &connector, uris.as_slice())
            },
        )
        .await
    }
}

#[cfg(not(feature = "redb"))]
const REQUIRES_REDB: &str = "ConnectorPack admin requires the redb feature";

/// The request-side inputs every administrative write binds.
#[cfg(feature = "redb")]
struct AdminWrite<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &'a VerifiedRequestContext,
    context: eg_types::agent_library::AgentLibraryMutationContext,
    purpose: &'static str,
}

/// Bind the verified context, take the tenant's pack lock, and run `write`
/// on the blocking pool, answering with `M`'s declared result.
#[cfg(feature = "redb")]
async fn admin_write<M, F>(request: AdminWrite<'_>, write: F) -> Response
where
    M: eg_types::result_contract::MethodResult,
    M::Encoding: eg_types::result_contract::EncodeRef<M::Body>,
    M::Body: Send + 'static,
    F: FnOnce(
            &AgentLibraryStore,
            eg_types::agent_library::AgentLibraryMutationContext,
        ) -> Result<M::Body, String>
        + Send
        + 'static,
{
    let req_id = request.req_id;
    match run_admin_write::<M, F>(request, write).await {
        Ok(body) => Response::ok(req_id, crate::protocol::ResultPayload::of_ref::<M>(&body)),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
async fn run_admin_write<M, F>(request: AdminWrite<'_>, write: F) -> Result<M::Body, String>
where
    M: eg_types::result_contract::MethodResult,
    M::Body: Send + 'static,
    F: FnOnce(
            &AgentLibraryStore,
            eg_types::agent_library::AgentLibraryMutationContext,
        ) -> Result<M::Body, String>
        + Send
        + 'static,
{
    let _tenant_pack_guard = super::tenant_pack_lock(request.verified.tenant()).await;
    let store = super::agent_library_store(request.state).await?;
    let context = crate::server::handlers::admin::agent::bind_agent_library_context(
        &store,
        request.req_id,
        request.verified,
        request.context,
        request.purpose,
        true,
    )?;
    tokio::task::spawn_blocking(move || write(&*store, context))
        .await
        .map_err(|error| format!("connector pack admin task failed: {error}"))?
}
