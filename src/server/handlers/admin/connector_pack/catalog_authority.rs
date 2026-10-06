//! Authenticated tenant-local served-child catalog reconciliation.

use super::{Arc, Response, RwLock, ServerState, VerifiedRequestContext};

#[cfg(not(feature = "redb"))]
fn unavailable_catalog_response<T>(
    req_id: u64,
    _state: &Arc<RwLock<ServerState>>,
    _verified: &VerifiedRequestContext,
    _request: T,
) -> Response {
    Response::err(req_id, "MCP catalog authority requires the redb feature")
}

#[derive(Clone, Copy)]
enum CatalogRead {
    RequestBinding,
    RequestOwner,
    AttesterStatus,
    AttesterOwner,
}

#[cfg(feature = "redb")]
async fn read_catalog(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request: &eg_types::connector_pack::McpCatalogAuthorityStatusRequest,
    access: CatalogRead,
) -> Result<
    (
        Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
        Option<eg_types::connector_pack::McpCatalogSnapshotBinding>,
    ),
    String,
> {
    match access {
        CatalogRead::RequestBinding | CatalogRead::RequestOwner => {
            require_catalog_request_read(verified, &request.tenant_id)?
        }
        CatalogRead::AttesterStatus => {
            require_catalog_attester(verified, &request.tenant_id, false)?
        }
        CatalogRead::AttesterOwner => require_catalog_owner_read(verified, &request.tenant_id)?,
    }
    let scope = verified.tenant_local_catalog_partition_digest(&request.server_name)?;
    let store = state.write().await.ensure_agent_library()?;
    let binding = match access {
        CatalogRead::RequestBinding | CatalogRead::RequestOwner => {
            store.mcp_catalog_binding_status(verified.tenant(), &request.server_name, scope)?
        }
        CatalogRead::AttesterStatus | CatalogRead::AttesterOwner => store
            .mcp_catalog_authority_status(
                verified.tenant(),
                &request.server_name,
                scope,
                &verified.principal_persistence_id(),
            )?,
    };
    Ok((store, binding))
}

#[cfg(feature = "redb")]
fn verified_owner_principal(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
) -> Result<String, String> {
    let owner = store.owner_principal();
    let digest = owner
        .strip_prefix("principal:sha256:")
        .ok_or_else(|| "AgentLibrary owner principal is invalid".to_string())?;
    eg_types::contract::Digest256::parse(digest)
        .map_err(|_| "AgentLibrary owner principal is invalid".to_string())?;
    Ok(owner.to_string())
}

/// Both catalog-authority writes need the dedicated attester scope AND the
/// connector-pack administrative action, in the verified tenant.
pub(super) fn require_catalog_attester(
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

fn require_catalog_request_read(
    verified: &VerifiedRequestContext,
    tenant_id: &str,
) -> Result<(), String> {
    if tenant_id != verified.tenant() || !verified.allows_action("agent:pack-control") {
        return Err("ACCESS_DENIED: connector pack catalog read is unavailable".into());
    }
    Ok(())
}

/// Serve all catalog reads through one authenticated, scoped store snapshot.
async fn serve_catalog_read(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogAuthorityStatusRequest,
    access: CatalogRead,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        let _ = access;
        unavailable_catalog_response(req_id, state, verified, request)
    }
    #[cfg(feature = "redb")]
    {
        match read_catalog(state, verified, &request, access).await {
            Ok((_, binding)) if matches!(access, CatalogRead::RequestBinding) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackCatalogBindingStatus,
                >(&binding),
            ),
            Ok((_, binding)) if matches!(access, CatalogRead::AttesterStatus) => Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::ConnectorPackCatalogAuthorityStatus,
                >(&binding),
            ),
            Ok((store, Some(_))) => match verified_owner_principal(&store) {
                Ok(owner) if matches!(access, CatalogRead::AttesterOwner) => Response::ok(
                    req_id,
                    crate::protocol::ResultPayload::of_ref::<
                        eg_types::result_contract::storage::ConnectorPackCatalogOwnerPrincipal,
                    >(&owner),
                ),
                Ok(owner) => Response::ok(
                    req_id,
                    crate::protocol::ResultPayload::of_ref::<
                        eg_types::result_contract::storage::ConnectorPackCatalogRequestOwnerPrincipal,
                    >(&owner),
                ),
                Err(error) => Response::err(req_id, error),
            },
            Ok((_, None)) => Response::err(req_id, "scoped MCP catalog authority is unavailable"),
            Err(error) => Response::err(req_id, error),
        }
    }
}

// These four names are the existing ConnectorPack dispatch surface. Their
// policy and response formatting live in the single scoped read above.
macro_rules! catalog_read_handler {
    ($(#[$doc:meta])* $name:ident, $access:ident) => {
        $(#[$doc])*
        pub(super) async fn $name(
            state: &Arc<RwLock<ServerState>>,
            req_id: u64,
            verified: &VerifiedRequestContext,
            request: eg_types::connector_pack::McpCatalogAuthorityStatusRequest,
        ) -> Response {
            serve_catalog_read(state, req_id, verified, request, CatalogRead::$access).await
        }
    };
}

catalog_read_handler!(
    /// Read a binding under the verified request's tenant and server scope.
    serve_binding_status, RequestBinding
);
catalog_read_handler!(
    /// Read EG's owner principal for a request-scoped importer with a binding.
    serve_request_owner_principal, RequestOwner
);
catalog_read_handler!(
    /// Read EG's owner principal for the verified child attester.
    serve_owner_principal, AttesterOwner
);
catalog_read_handler!(
    /// Read the scoped catalog status for the verified child attester.
    serve_status, AttesterStatus
);

pub(super) async fn serve(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: eg_types::connector_pack::McpCatalogReconcileRequest,
) -> Response {
    #[cfg(not(feature = "redb"))]
    {
        unavailable_catalog_response(req_id, state, verified, request)
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
fn validate_mounted_attestation(
    verified: &VerifiedRequestContext,
    request: &eg_types::connector_pack::McpCatalogReconcileRequest,
) -> Result<(), String> {
    if request.discovery_tenant != verified.tenant()
        || request.child_id.is_empty()
        || request.local_catalog_epoch == 0
        || request.child_connection_generation == 0
    {
        return Err("invalid mounted-child catalog attestation".into());
    }
    Ok(())
}

/// The [`ObservedRegistration`] a catalog-authority request body carries.
#[cfg(feature = "redb")]
macro_rules! observed_registration {
    ($request:expr) => {
        ObservedRegistration {
            server_name: &$request.server_name,
            registry_revision: $request.registry_revision,
            registry_digest: $request.registry_digest,
            registration_config_digest: $request.registration_config_digest,
            context: $request.context.clone(),
        }
    };
}
#[cfg(feature = "redb")]
pub(super) use observed_registration;

/// The registration an attester observed, which EG re-reads before binding.
#[cfg(feature = "redb")]
pub(super) struct ObservedRegistration<'a> {
    pub(super) server_name: &'a str,
    pub(super) registry_revision: u64,
    pub(super) registry_digest: eg_types::contract::Digest256,
    pub(super) registration_config_digest: eg_types::contract::Digest256,
    pub(super) context: eg_types::agent_library::AgentLibraryMutationContext,
}

/// What an admitted catalog-authority write holds until its rows commit.
#[cfg(feature = "redb")]
pub(super) type CatalogWriteAdmission = (
    tokio::sync::OwnedMutexGuard<()>,
    Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
    eg_types::agent_library::AgentLibraryMutationContext,
);

/// Take the registry graph lock, verify the observed registration under it and
/// bind the admitted mutation context. Registry mutation takes this same lock;
/// the caller keeps the guard until both EG authority rows commit, so the
/// joined registration cannot move between verification and persistence.
#[cfg(feature = "redb")]
pub(super) async fn admit_catalog_write(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    observed: ObservedRegistration<'_>,
) -> Result<CatalogWriteAdmission, String> {
    let guard = crate::server::mutation_batch::lock_graph("__commons__").await;
    let context = observed.context.clone();
    verify_observed_registration(state, verified, observed).await?;
    let store = state.write().await.ensure_agent_library()?;
    let context = crate::server::handlers::admin::agent::bind_agent_library_context(
        &store,
        req_id,
        verified,
        context,
        "mcp-catalog:reconcile",
        true,
    )?;
    Ok((guard, store, context))
}

/// The identity of a registration's served configuration: its URL and its
/// canonical resource map, as compact JSON with sorted keys.
#[cfg(feature = "redb")]
pub(crate) fn registration_config_digest(
    registration: &eg_types::result_contract::cluster::RegisteredServerView,
) -> Result<eg_types::contract::Digest256, String> {
    Ok(eg_types::contract::Digest256::sha256(
        &serde_json::to_vec(&serde_json::json!({
            "url": registration.url,
            "resources": registration.resources,
        }))
        .map_err(|_| "invalid MCP registration projection")?,
    ))
}

/// Require the live, enabled `__commons__` registration to be exactly the one
/// the attester observed. The caller holds the registry graph lock until its
/// authority rows commit, so the registration cannot move in between.
#[cfg(feature = "redb")]
async fn verify_observed_registration(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    observed: ObservedRegistration<'_>,
) -> Result<(), String> {
    use eg_types::result_contract::cluster::ServerDesiredState;

    let (registry_revision, registry_digest, registration) =
        crate::server::dispatch::verified_served_registration(
            state,
            verified,
            observed.server_name,
        )
        .await?;
    if registry_revision != observed.registry_revision
        || registry_digest != observed.registry_digest
        || registration.desired != ServerDesiredState::Enabled
    {
        return Err("stale or disabled MCP server registration".into());
    }
    if registration_config_digest(&registration)? != observed.registration_config_digest {
        return Err("MCP registration configuration changed".into());
    }
    Ok(())
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

    require_catalog_attester(verified, &request.context.tenant_id, true)?;
    validate_mounted_attestation(verified, &request)?;
    // Registry mutation takes this same graph lock. Keep it until both EG
    // authority rows commit, so the joined registration cannot move between
    // source verification and persistence.
    let (_registry_guard, store, context) =
        admit_catalog_write(state, req_id, verified, observed_registration!(request)).await?;
    let component = store
        .current_component(verified.tenant(), &request.component_id)?
        .ok_or_else(|| "MCP server component is unavailable".to_string())?;
    let expected_suffix = format!("/mcp_server/{}", request.server_name);
    let content_digest =
        crate::server::persistence::connector_pack::catalog_authority::component_content_digest(
            &component,
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
        attestation: eg_types::connector_pack::McpCatalogAttestation::MountedChild,
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
        let request_reader = VerifiedRequestContext::verified_for_test_with_scopes(
            "connector-sync-service",
            "tenant-a",
            &["agent:pack-control"],
        );
        assert!(require_catalog_request_read(&request_reader, "tenant-a").is_ok());
        assert!(require_catalog_request_read(&request_reader, "tenant-b").is_err());
        assert!(require_catalog_owner_read(&request_reader, "tenant-a").is_err());
        assert!(require_catalog_attester(&request_reader, "tenant-a", true).is_err());
        assert!(require_catalog_request_read(&read, "tenant-a").is_err());
    }
}
