//! Served approval of one held connector-pack source declaration.
//!
//! Arbora's admitted read-only policy calls this existing ConnectorPack
//! mutation. The handler only binds the verified context and tenant lock; the
//! persistence owner performs the current-head/body-digest CAS.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

pub(crate) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: medulla_types::connector_pack::ConnectorPackSourceApprovalRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        Response::err(req_id, "ConnectorPack approval requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        let tenant_pack_guard = match super::tenant_pack_lock(verified.tenant()).await {
            Ok(guard) => guard,
            Err(error) => return Response::err(req_id, error),
        };
        let store = match super::agent_library_store(state).await {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        };
        let context = match crate::server::handlers::admin::agent::bind_agent_library_context(
            &store,
            req_id,
            verified,
            request.context,
            "connector-pack:source-approval",
            true,
        ) {
            Ok(context) => context,
            Err(error) => return Response::err(req_id, error),
        };
        let connector = request.connector;
        let contract_iri = request.contract_iri;
        let resource_uri = request.resource_uri;
        let expected_head = request.expected_head;
        let contract_digest = request.contract_digest;
        let result =
            crate::server::dispatch::blocking_task("connector pack source approval", move || {
                let _held = tenant_pack_guard;
                store.approve_connector_pack_source(
                    context,
                    &connector,
                    &contract_iri,
                    &resource_uri,
                    &expected_head,
                    contract_digest,
                )
            })
            .await;
        match result {
            Ok(result) => match crate::protocol::ResultPayload::of_ref::<
                medulla_types::result_contract::storage::ConnectorPackApproveSource,
            >(&result)
            {
                Ok(payload) => Response::ok(req_id, payload),
                Err(error) => Response::err(req_id, error),
            },
            Err(error) => Response::err(req_id, error),
        }
    }
}
