//! Authenticated tenant-local served-child catalog reconciliation.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

fn require_catalog_attester(
    verified: &VerifiedRequestContext,
    tenant_id: &str,
    write: bool,
) -> Result<(), String> {
    if tenant_id != verified.tenant()
        || !verified.allows_action("connector:catalog-attest")
        || (write && !verified.allows_action("admin:connector-pack"))
    {
        return Err("ACCESS_DENIED: verified MCP catalog attester is unavailable".into());
    }
    Ok(())
}

fn require_catalog_owner_read(
    verified: &VerifiedRequestContext,
    tenant_id: &str,
) -> Result<(), String> {
    require_catalog_attester(verified, tenant_id, false)?;
    if !verified.allows_action("agent:pack-control") {
        return Err("ACCESS_DENIED: connector pack owner read is unavailable".into());
    }
    Ok(())
}

/// Return the actual AgentLibrary owner only to the same verified child
/// attester that has a persisted scoped catalog row and pack-control grant.
/// This is a read, not a way for the caller to assert its own owner principal.
pub(super) async fn serve_owner_principal(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogAuthorityStatusRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        Response::err(req_id, "MCP catalog authority requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        if let Err(error) = require_catalog_owner_read(verified, &request.tenant_id) {
            return Response::err(req_id, error);
        }
        let scope = match verified.tenant_local_catalog_partition_digest(&request.server_name) {
            Ok(scope) => scope,
            Err(error) => return Response::err(req_id, error),
        };
        let store = match state.write().await.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        };
        match store.mcp_catalog_authority_status(
            verified.tenant(),
            &request.server_name,
            scope,
            &verified.principal_persistence_id(),
        ) {
            Ok(Some(_)) => {
                let owner = store.owner_principal();
                let digest = owner.strip_prefix("principal:sha256:");
                if !digest.is_some_and(|value| {
                    value.len() == 64
                        && value
                            .bytes()
                            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                }) {
                    return Response::err(req_id, "AgentLibrary owner principal is invalid");
                }
                Response::ok(
                    req_id,
                    crate::protocol::ResultPayload::of_ref::<
                        eg_types::result_contract::storage::ConnectorPackCatalogOwnerPrincipal,
                    >(&owner.to_string()),
                )
            }
            Ok(None) => Response::err(req_id, "scoped MCP catalog authority is unavailable"),
            Err(error) => Response::err(req_id, error),
        }
    }
}

pub(super) async fn serve_status(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogAuthorityStatusRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        Response::err(req_id, "MCP catalog authority requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        if let Err(error) = require_catalog_attester(verified, &request.tenant_id, false) {
            return Response::err(req_id, error);
        }
        let scope = match verified.tenant_local_catalog_partition_digest(&request.server_name) {
            Ok(scope) => scope,
            Err(error) => return Response::err(req_id, error),
        };
        let store = match state.write().await.ensure_agent_library() {
            Ok(store) => store,
            Err(error) => return Response::err(req_id, error),
        };
        match store.mcp_catalog_authority_status(
            verified.tenant(),
            &request.server_name,
            scope,
            &verified.principal_persistence_id(),
        ) {
            Ok(binding) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackCatalogAuthorityStatus,
                >(&binding),
            ),
            Err(error) => Response::err(req_id, error),
        }
    }
}

pub(super) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogReconcileRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = (state, verified, request);
        Response::err(req_id, "MCP catalog authority requires the redb feature")
    }
    #[cfg(feature = "redb")]
    {
        match reconcile(state, req_id, verified, request).await {
            Ok(binding) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackReconcileCatalog,
                >(&binding),
            ),
            Err(error) => Response::err(req_id, error),
        }
    }
}

#[cfg(feature = "redb")]
async fn reconcile(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogReconcileRequest,
) -> Result<eg_types::connector_pack::McpCatalogSnapshotBinding, String> {
    use eg_types::agent_component::AgentComponentKind;
    use eg_types::agent_library::AgentLibraryLifecycle;
    use eg_types::contract::Digest256;
    use eg_types::result_contract::cluster::ServerDesiredState;

    require_catalog_attester(verified, &request.context.tenant_id, true)?;
    if request.discovery_tenant != verified.tenant()
        || request.child_id.is_empty()
        || request.local_catalog_epoch == 0
        || request.child_connection_generation == 0
    {
        return Err("invalid mounted-child catalog attestation".into());
    }
    // Registry mutation takes this same graph lock. Keep it until both EG
    // authority rows commit, so the joined registration cannot move between
    // source verification and persistence.
    let _registry_guard = crate::server::mutation_batch::lock_graph("__commons__").await;
    let (registry_revision, registry_digest, registration) =
        crate::server::dispatch::verified_served_registration(
            state,
            verified,
            &request.server_name,
        )
        .await?;
    if registry_revision != request.registry_revision
        || registry_digest != request.registry_digest
        || registration.desired != ServerDesiredState::Enabled
    {
        return Err("stale or disabled MCP server registration".into());
    }
    let registration_digest = Digest256::sha256(
        &serde_json::to_vec(&serde_json::json!({
            "url": registration.url,
            "resources": registration.resources,
        }))
        .map_err(|_| "invalid MCP registration projection")?,
    );
    if registration_digest != request.registration_config_digest {
        return Err("MCP registration configuration changed".into());
    }
    let store = state.write().await.ensure_agent_library()?;
    let context = crate::server::handlers::admin::agent::bind_agent_library_context(
        &store,
        req_id,
        verified,
        request.context,
        "mcp-catalog:reconcile",
        true,
    )?;
    let component = store
        .current_component(verified.tenant(), &request.component_id)?
        .ok_or_else(|| "MCP server component is unavailable".to_string())?;
    let expected_suffix = format!("/mcp_server/{}", request.server_name);
    let content_digest = Digest256::parse(
        component
            .content_digest
            .strip_prefix("sha256:")
            .unwrap_or(&component.content_digest),
    )?;
    if component.kind != AgentComponentKind::McpServer
        || component.lifecycle != AgentLibraryLifecycle::Published
        || component.tenant_id != verified.tenant()
        || !component.component_id.ends_with(&expected_suffix)
        || component.entry_revision != request.component_revision
        || content_digest != request.component_digest
    {
        return Err("stale or unrelated MCP server component".into());
    }
    let authorization_scope_digest =
        verified.tenant_local_catalog_partition_digest(&request.server_name)?;
    let candidate = eg_types::connector_pack::McpCatalogAuthorityCandidate {
        tenant_id: verified.tenant().to_string(),
        server_name: request.server_name,
        attester_principal_id: verified.principal_persistence_id(),
        discovery_tenant: request.discovery_tenant,
        component_revision: request.component_revision,
        component_digest: request.component_digest,
        registry_revision: request.registry_revision,
        registry_digest: request.registry_digest,
        registration_config_digest: request.registration_config_digest,
        four_family_digest: request.four_family_digest,
        child_id: request.child_id,
        local_catalog_epoch: request.local_catalog_epoch,
        child_connection_generation: request.child_connection_generation,
        authorization_scope_digest,
        expected_catalog_generation: request.expected_catalog_generation,
    };
    store.reconcile_mcp_catalog_authority(context, request.component_id, candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_and_write_require_verified_tenant_and_dedicated_attester_scope() {
        let read = VerifiedRequestContext::verified_for_test_with_scopes(
            "graph-os",
            "tenant-a",
            &["connector:catalog-attest"],
        );
        assert!(require_catalog_attester(&read, "tenant-a", false).is_ok());
        assert!(require_catalog_attester(&read, "tenant-a", true).is_err());
        assert!(require_catalog_attester(&read, "tenant-b", false).is_err());
        let write = VerifiedRequestContext::verified_for_test_with_scopes(
            "graph-os",
            "tenant-a",
            &["connector:catalog-attest", "admin:connector-pack"],
        );
        assert!(require_catalog_attester(&write, "tenant-a", true).is_ok());
        let ordinary_admin = VerifiedRequestContext::verified_for_test_with_scopes(
            "admin",
            "tenant-a",
            &["admin:connector-pack"],
        );
        assert!(require_catalog_attester(&ordinary_admin, "tenant-a", true).is_err());
        assert!(require_catalog_owner_read(&write, "tenant-a").is_err());
        let importer = VerifiedRequestContext::verified_for_test_with_scopes(
            "graph-os",
            "tenant-a",
            &["connector:catalog-attest", "agent:pack-control"],
        );
        assert!(require_catalog_owner_read(&importer, "tenant-a").is_ok());
        assert!(require_catalog_owner_read(&importer, "tenant-b").is_err());
    }
}
