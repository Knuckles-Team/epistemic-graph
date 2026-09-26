//! Atomically persist the joined configuration and one scoped catalog row.
//!
//! The handler must authenticate the served-child attester and verify every
//! source before calling this method. An owner row alone is not authorization.

use eg_types::agent_library::AgentLibraryMutationContext;
use eg_types::connector_pack::{
    reconcile_catalog_authority, reconcile_joined_configuration, McpCatalogAuthorityCandidate,
    McpCatalogAuthorityRow, McpCatalogSnapshotBinding, McpJoinedConfigurationRow,
};
use eg_types::contract::Digest256;
use eg_types::mutation_batch::{DurabilityDomain, MutationOperation, MutationSurface};
use eg_types::protocol::Method;
use redb::ReadableTable;

use super::admitted::{PackWriteEffects, PackWriteIdentity};
use crate::server::persistence::agent_library::AgentLibraryStore;

const RESULT_SCHEMA: &str = "mcp-catalog-authority-result.v1";

fn checked_scoped_binding(
    row: McpCatalogAuthorityRow,
    tenant_id: &str,
    server_name: &str,
    scope_digest: Digest256,
    attester_principal_id: &str,
) -> Result<McpCatalogSnapshotBinding, String> {
    checked_catalog_binding(
        row,
        tenant_id,
        server_name,
        scope_digest,
        Some(attester_principal_id),
    )
}

/// Validate the persisted key and binding for either a mounted attester or a
/// separate pack-control reader. Only the attester path compares principals.
fn checked_catalog_binding(
    row: McpCatalogAuthorityRow,
    tenant_id: &str,
    server_name: &str,
    scope_digest: Digest256,
    attester_principal_id: Option<&str>,
) -> Result<McpCatalogSnapshotBinding, String> {
    let configuration_digest = Digest256::framed(
        b"eg/mcp-served-joined-config/v1",
        &[
            row.tenant_id.as_bytes(),
            row.server_name.as_bytes(),
            row.component_digest.as_bytes(),
            row.registration_config_digest.as_bytes(),
        ],
    )?;
    if row.tenant_id != tenant_id
        || row.server_name != server_name
        || row.binding.authorization_scope_digest != scope_digest
        || row.attester_principal_id.is_empty()
        || attester_principal_id.is_some_and(|principal| row.attester_principal_id != principal)
        || row.binding.configuration_revision == 0
        || row.binding.catalog_generation == 0
        || row.binding.child_connection_generation == 0
        || row.configuration_digest != configuration_digest
        || row.registry_digest == Digest256::from_bytes([0; 32])
        || row.binding.snapshot_digest == Digest256::from_bytes([0; 32])
    {
        return Err("MCP catalog authority row differs from verified scope".into());
    }
    Ok(row.binding)
}

impl AgentLibraryStore {
    /// Read the current scoped binding for a separately authenticated
    /// pack-control service. The handler must check its verified tenant/grant.
    pub(crate) fn mcp_catalog_binding_status(
        &self,
        tenant_id: &str,
        server_name: &str,
        scope_digest: Digest256,
    ) -> Result<Option<McpCatalogSnapshotBinding>, String> {
        if tenant_id.is_empty()
            || !eg_types::result_contract::cluster::is_valid_server_name(server_name)
            || scope_digest == Digest256::from_bytes([0; 32])
        {
            return Err("invalid scoped MCP catalog binding read".into());
        }
        let scope_hex = scope_digest.to_hex();
        let read = self.read()?;
        let table = read.open_owner_table(eg_storage::MCP_CATALOG_SCOPES)?;
        let row = table
            .get((tenant_id, server_name, scope_hex.as_str()))
            .map_err(|error| error.to_string())?
            .map(|value| {
                crate::server::persistence::agent_row::decode::<McpCatalogAuthorityRow>(
                    value.value(),
                    "MCP scoped catalog",
                )
            })
            .transpose()?;
        row.map(|row| checked_catalog_binding(row, tenant_id, server_name, scope_digest, None))
            .transpose()
    }

    /// Read the scoped generation from one committed owner snapshot.
    pub(crate) fn mcp_catalog_authority_status(
        &self,
        tenant_id: &str,
        server_name: &str,
        scope_digest: Digest256,
        attester_principal_id: &str,
    ) -> Result<Option<McpCatalogSnapshotBinding>, String> {
        if tenant_id.is_empty()
            || !eg_types::result_contract::cluster::is_valid_server_name(server_name)
            || attester_principal_id.is_empty()
        {
            return Err("invalid scoped MCP catalog authority read".into());
        }
        let scope_hex = scope_digest.to_hex();
        let read = self.read()?;
        let table = read.open_owner_table(eg_storage::MCP_CATALOG_SCOPES)?;
        let row = table
            .get((tenant_id, server_name, scope_hex.as_str()))
            .map_err(|error| error.to_string())?
            .map(|value| {
                crate::server::persistence::agent_row::decode::<McpCatalogAuthorityRow>(
                    value.value(),
                    "MCP scoped catalog",
                )
            })
            .transpose()?;
        row.map(|row| {
            checked_scoped_binding(
                row,
                tenant_id,
                server_name,
                scope_digest,
                attester_principal_id,
            )
        })
        .transpose()
    }

    /// One admitted owner transaction writes both authority rows or neither.
    pub(crate) fn reconcile_mcp_catalog_authority(
        &self,
        context: AgentLibraryMutationContext,
        component_id: String,
        candidate: McpCatalogAuthorityCandidate,
    ) -> Result<McpCatalogSnapshotBinding, String> {
        if context.tenant_id != candidate.tenant_id {
            return Err("catalog authority tenant differs from verified mutation context".into());
        }
        let source_digest = candidate_digest(&component_id, &candidate)?;
        let source_hex = source_digest.to_hex();
        let scope_hex = candidate.authorization_scope_digest.to_hex();
        let subject = candidate.server_name.clone();
        let identity = PackWriteIdentity {
            purpose: "mcp-catalog:reconcile",
            kind: "mcp-catalog-reconcile",
            slug: "mcp-catalog-reconcile",
            subject: &subject,
            revision: candidate.expected_catalog_generation.unwrap_or(0),
            discriminator: Some(&source_hex),
            result_schema: RESULT_SCHEMA,
            noun: "MCP catalog authority",
        };
        let effects = PackWriteEffects {
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Lifecycle,
                domain: DurabilityDomain::ControlPlane,
                method: Method::ApplyMutation {
                    event_type: "mcp_catalog_reconcile".into(),
                    query: source_hex.clone(),
                },
            }],
            outbox: Vec::new(),
        };
        self.commit_pack_write(&context, identity, effects, |write, _staged| {
            let current_revision = {
                let heads = write.open_table(eg_storage::AGENT_COMPONENT_HEADS)?;
                let revision = heads
                    .get((context.tenant_id.as_str(), component_id.as_str()))
                    .map_err(|error| error.to_string())?
                    .map(|row| row.value())
                    .ok_or_else(|| "MCP server component has no current revision".to_string())?;
                revision
            };
            let current: eg_types::agent_component::AgentComponentEntry = {
                let revisions = write.open_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
                let row = revisions
                    .get((
                        context.tenant_id.as_str(),
                        component_id.as_str(),
                        current_revision,
                    ))
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "MCP server component head is missing".to_string())?;
                crate::server::persistence::agent_row::decode(row.value(), "MCP server component")?
            };
            let current_digest = Digest256::parse(
                current
                    .content_digest
                    .strip_prefix("sha256:")
                    .unwrap_or(&current.content_digest),
            )?;
            if current.entry_revision != candidate.component_revision
                || current_digest != candidate.component_digest
                || current.lifecycle != eg_types::agent_library::AgentLibraryLifecycle::Published
                || current.kind != eg_types::agent_component::AgentComponentKind::McpServer
            {
                return Err("MCP server component changed before catalog commit".into());
            }
            let config_key = (context.tenant_id.as_str(), candidate.server_name.as_str());
            let previous_config = {
                let table = write.open_table(eg_storage::MCP_CATALOG_CONFIGS)?;
                let previous = table
                    .get(config_key)
                    .map_err(|error| error.to_string())?
                    .map(|row| {
                        crate::server::persistence::agent_row::decode::<McpJoinedConfigurationRow>(
                            row.value(),
                            "MCP joined configuration",
                        )
                    })
                    .transpose()?;
                previous
            };
            let configuration =
                reconcile_joined_configuration(previous_config.as_ref(), &candidate)?;
            let scope_key = (
                context.tenant_id.as_str(),
                candidate.server_name.as_str(),
                scope_hex.as_str(),
            );
            let previous_scope = {
                let table = write.open_table(eg_storage::MCP_CATALOG_SCOPES)?;
                let previous = table
                    .get(scope_key)
                    .map_err(|error| error.to_string())?
                    .map(|row| {
                        crate::server::persistence::agent_row::decode::<McpCatalogAuthorityRow>(
                            row.value(),
                            "MCP scoped catalog",
                        )
                    })
                    .transpose()?;
                previous
            };
            let scoped =
                reconcile_catalog_authority(previous_scope.as_ref(), &configuration, &candidate)?;
            let config_bytes =
                eg_storage::encode_bounded(&configuration, "MCP joined configuration")?;
            let scope_bytes = eg_storage::encode_bounded(&scoped, "MCP scoped catalog")?;
            write
                .open_table(eg_storage::MCP_CATALOG_CONFIGS)?
                .insert(config_key, config_bytes.as_slice())
                .map_err(|error| error.to_string())?;
            write
                .open_table(eg_storage::MCP_CATALOG_SCOPES)?
                .insert(scope_key, scope_bytes.as_slice())
                .map_err(|error| error.to_string())?;
            Ok(scoped.binding)
        })
    }
}

fn candidate_digest(
    component_id: &str,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<Digest256, String> {
    Digest256::framed(
        b"eg/mcp-catalog-authority-candidate/v1",
        &[
            candidate.tenant_id.as_bytes(),
            candidate.server_name.as_bytes(),
            candidate.attester_principal_id.as_bytes(),
            candidate.discovery_tenant.as_bytes(),
            component_id.as_bytes(),
            &candidate.component_revision.to_be_bytes(),
            candidate.component_digest.as_bytes(),
            &candidate.registry_revision.to_be_bytes(),
            candidate.registry_digest.as_bytes(),
            candidate.registration_config_digest.as_bytes(),
            candidate.four_family_digest.as_bytes(),
            candidate.child_id.as_bytes(),
            &candidate.local_catalog_epoch.to_be_bytes(),
            &candidate.child_connection_generation.to_be_bytes(),
            candidate.authorization_scope_digest.as_bytes(),
            &candidate
                .expected_catalog_generation
                .unwrap_or(0)
                .to_be_bytes(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority_row() -> McpCatalogAuthorityRow {
        let candidate = McpCatalogAuthorityCandidate {
            tenant_id: "tenant-a".into(),
            server_name: "child-a".into(),
            attester_principal_id: "principal:sha256:attester-a".into(),
            discovery_tenant: "tenant-a".into(),
            component_revision: 3,
            component_digest: Digest256::from_bytes([1; 32]),
            registry_revision: 8,
            registry_digest: Digest256::from_bytes([7; 32]),
            registration_config_digest: Digest256::from_bytes([2; 32]),
            four_family_digest: Digest256::from_bytes([3; 32]),
            child_id: "mounted-child-a".into(),
            local_catalog_epoch: 1,
            child_connection_generation: 2,
            authorization_scope_digest: Digest256::from_bytes([4; 32]),
            expected_catalog_generation: None,
        };
        let config = reconcile_joined_configuration(None, &candidate).unwrap();
        reconcile_catalog_authority(None, &config, &candidate).unwrap()
    }

    #[test]
    fn scoped_status_never_crosses_tenant_scope_or_attester() {
        let row = authority_row();
        let scope = row.binding.authorization_scope_digest;
        assert_eq!(
            checked_scoped_binding(
                row.clone(),
                "tenant-a",
                "child-a",
                scope,
                "principal:sha256:attester-a"
            )
            .unwrap(),
            row.binding
        );
        for (tenant, server, digest, principal) in [
            ("tenant-b", "child-a", scope, "principal:sha256:attester-a"),
            ("tenant-a", "child-b", scope, "principal:sha256:attester-a"),
            (
                "tenant-a",
                "child-a",
                Digest256::from_bytes([5; 32]),
                "principal:sha256:attester-a",
            ),
            ("tenant-a", "child-a", scope, "principal:sha256:attester-b"),
        ] {
            assert!(
                checked_scoped_binding(row.clone(), tenant, server, digest, principal).is_err()
            );
        }
    }

    #[test]
    fn request_binding_read_keeps_tenant_server_scope_and_digest_checks() {
        let row = authority_row();
        let scope = row.binding.authorization_scope_digest;
        assert_eq!(
            checked_catalog_binding(row.clone(), "tenant-a", "child-a", scope, None).unwrap(),
            row.binding
        );
        for (tenant, server, digest) in [
            ("tenant-b", "child-a", scope),
            ("tenant-a", "child-b", scope),
            ("tenant-a", "child-a", Digest256::from_bytes([5; 32])),
        ] {
            assert!(checked_catalog_binding(row.clone(), tenant, server, digest, None).is_err());
        }
        let mut corrupt = row.clone();
        corrupt.binding.snapshot_digest = Digest256::from_bytes([0; 32]);
        assert!(checked_catalog_binding(corrupt, "tenant-a", "child-a", scope, None).is_err());
        let mut corrupt = row;
        corrupt.configuration_digest = Digest256::from_bytes([0; 32]);
        assert!(checked_catalog_binding(corrupt, "tenant-a", "child-a", scope, None).is_err());
    }

    #[test]
    fn replay_identity_binds_scope_child_and_source() {
        let mut candidate = McpCatalogAuthorityCandidate {
            tenant_id: "tenant-a".into(),
            server_name: "child-a".into(),
            attester_principal_id: "principal:sha256:attester-a".into(),
            discovery_tenant: "tenant-a".into(),
            component_revision: 3,
            component_digest: Digest256::from_bytes([1; 32]),
            registry_revision: 8,
            registry_digest: Digest256::from_bytes([7; 32]),
            registration_config_digest: Digest256::from_bytes([2; 32]),
            four_family_digest: Digest256::from_bytes([3; 32]),
            child_id: "mounted-child-a".into(),
            local_catalog_epoch: 1,
            child_connection_generation: 2,
            authorization_scope_digest: Digest256::from_bytes([4; 32]),
            expected_catalog_generation: Some(1),
        };
        let original = candidate_digest("mcp:a/mcp_server/child-a", &candidate).unwrap();
        candidate.authorization_scope_digest = Digest256::from_bytes([5; 32]);
        assert_ne!(
            original,
            candidate_digest("mcp:a/mcp_server/child-a", &candidate).unwrap()
        );
        candidate.authorization_scope_digest = Digest256::from_bytes([4; 32]);
        candidate.child_connection_generation = 3;
        assert_ne!(
            original,
            candidate_digest("mcp:a/mcp_server/child-a", &candidate).unwrap()
        );
        candidate.child_connection_generation = 2;
        candidate.registration_config_digest = Digest256::from_bytes([6; 32]);
        assert_ne!(
            original,
            candidate_digest("mcp:a/mcp_server/child-a", &candidate).unwrap()
        );
        candidate.registration_config_digest = Digest256::from_bytes([2; 32]);
        assert_ne!(
            original,
            candidate_digest("mcp:b/mcp_server/child-a", &candidate).unwrap()
        );
    }
}
