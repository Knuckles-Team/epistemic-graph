//! `ConnectorPack.attest_self_served_catalog`: EG issues the catalog binding
//! for a producer that serves its own MCP catalog.
//!
//! A mounted child is reconciled after its pack has published the server
//! component (`reconcile_catalog`). A self-served producer is the server, so
//! its first pack import is what publishes that component; requiring the
//! component first would make the first import impossible. Instead EG pins the
//! content digest of the exact server entry the next import carries, and that
//! import must carry both EG's binding and the pinned entry
//! (`check_self_served_import`). Every other check is the mounted path's: the
//! verified tenant, both scopes, the enabled `__commons__` registration under
//! the registry graph lock, and the catalog-generation compare-and-set.

use super::{Arc, Response, RwLock, ServerState, VerifiedRequestContext};

pub(super) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpSelfServedCatalogAttestRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        Response::err(req_id, "MCP catalog authority requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        match attest(state, req_id, verified, request).await {
            Ok(binding) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackAttestSelfServedCatalog,
                >(&binding),
            ),
            Err(error) => Response::err(req_id, error),
        }
    }
}

#[cfg(feature = "redb")]
async fn attest(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpSelfServedCatalogAttestRequest,
) -> Result<eg_types::connector_pack::McpCatalogSnapshotBinding, String> {
    use super::catalog_authority::{
        admit_catalog_write, observed_registration, require_catalog_attester, ObservedRegistration,
    };
    use crate::server::persistence::connector_pack::catalog_authority::published_server_content;
    use eg_types::connector_pack::{
        pack_component_id, self_served_candidate, self_served_component_revision,
        validate_connector, PackEntryKind, SelfServedAttester,
    };

    require_catalog_attester(verified, &request.context.tenant_id, true)?;
    validate_connector(&request.connector)?;
    let authorization_scope_digest =
        verified.tenant_local_catalog_partition_digest(&request.server_name)?;
    // The import that consumes this binding checks it under the same tenant
    // pack lock, so no import interleaves with a re-pin. Then the registry
    // lock, held until the authority rows commit, as on the mounted path.
    let _pack_guard = super::tenant_pack_lock(verified.tenant()).await?;
    let (_registry_guard, store, context) =
        admit_catalog_write(state, req_id, verified, observed_registration!(request)).await?;
    super::authorize_importer(&store, verified, &request.connector)?;
    let component_id = pack_component_id(
        request.connector.as_str(),
        PackEntryKind::McpServer,
        &request.server_name,
    );
    let published = published_server_content(
        store
            .current_component(verified.tenant(), &component_id)?
            .as_ref(),
    )?;
    let principal_id = verified.principal_persistence_id();
    let candidate = self_served_candidate(
        &request,
        SelfServedAttester {
            tenant_id: verified.tenant(),
            principal_id: &principal_id,
            authorization_scope_digest,
            component_revision: self_served_component_revision(
                published,
                request.server_entry_digest,
            ),
        },
    );
    store.reconcile_mcp_catalog_authority(context, component_id, candidate)
}
