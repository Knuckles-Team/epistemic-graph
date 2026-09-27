//! Deterministic transition for an EG-owned served MCP catalog authority row.
//!
//! The caller must obtain both configuration digests from verified EG reads,
//! the four-family digest from the mounted child, and the scope digest from a
//! trusted authorization attestation. This reducer does not authenticate those
//! sources or persist the row; a handler must do both before issuing a binding.

use serde::{Deserialize, Serialize};

use super::McpCatalogSnapshotBinding;
use crate::agent_library::AgentLibraryMutationContext;
use crate::contract::Digest256;

const CONFIG_DOMAIN: &[u8] = b"eg/mcp-served-joined-config/v1";
const SNAPSHOT_DOMAIN: &[u8] = b"eg/mcp-served-catalog-snapshot/v1";

/// Recompute the identity of the verified component and registration inputs.
pub fn joined_configuration_digest(
    tenant_id: &str,
    server_name: &str,
    component_digest: Digest256,
    registration_config_digest: Digest256,
) -> Result<Digest256, String> {
    Digest256::framed(
        CONFIG_DOMAIN,
        &[
            tenant_id.as_bytes(),
            server_name.as_bytes(),
            component_digest.as_bytes(),
            registration_config_digest.as_bytes(),
        ],
    )
}

/// Persisted tenant/server configuration identity, shared by all scope partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpJoinedConfigurationRow {
    pub tenant_id: String,
    pub server_name: String,
    pub component_revision: u64,
    pub component_digest: Digest256,
    pub registry_revision: u64,
    pub registration_config_digest: Digest256,
    pub configuration_digest: Digest256,
    pub configuration_revision: u64,
}

/// Persisted state scoped by tenant, logical MCP server and authorization scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpCatalogAuthorityRow {
    pub tenant_id: String,
    pub server_name: String,
    pub attester_principal_id: String,
    pub child_id: String,
    pub local_catalog_epoch: u64,
    pub registry_digest: Digest256,
    pub component_revision: u64,
    pub component_digest: Digest256,
    pub registry_revision: u64,
    pub registration_config_digest: Digest256,
    pub configuration_digest: Digest256,
    pub binding: McpCatalogSnapshotBinding,
}

/// Internal reconciliation input, assembled only after source verification.
/// It is deliberately not an external ConnectorPack operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpCatalogAuthorityCandidate {
    pub tenant_id: String,
    pub server_name: String,
    pub attester_principal_id: String,
    pub discovery_tenant: String,
    pub component_revision: u64,
    pub component_digest: Digest256,
    pub registry_revision: u64,
    pub registry_digest: Digest256,
    pub registration_config_digest: Digest256,
    pub four_family_digest: Digest256,
    pub child_id: String,
    pub local_catalog_epoch: u64,
    pub child_connection_generation: u64,
    pub authorization_scope_digest: Digest256,
    pub expected_catalog_generation: Option<u64>,
}

/// An authenticated served child asks EG to reconcile its observed catalog.
/// The scope digest is absent: EG derives it from the verified tenant carrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpCatalogReconcileRequest {
    pub context: AgentLibraryMutationContext,
    pub server_name: String,
    pub component_id: String,
    pub component_revision: u64,
    pub component_digest: Digest256,
    pub registry_revision: u64,
    pub registry_digest: Digest256,
    pub registration_config_digest: Digest256,
    pub four_family_digest: Digest256,
    pub child_id: String,
    pub discovery_tenant: String,
    pub local_catalog_epoch: u64,
    pub child_connection_generation: u64,
    #[serde(default)]
    pub expected_catalog_generation: Option<u64>,
}

/// Read the EG-owned generation for one verified tenant-local server scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpCatalogAuthorityStatusRequest {
    pub tenant_id: String,
    pub server_name: String,
}

/// Shared deterministic catalog candidate for reducer and persistence tests.
#[cfg(any(test, feature = "test-support"))]
pub fn sample_authority_candidate() -> McpCatalogAuthorityCandidate {
    McpCatalogAuthorityCandidate {
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
    }
}

fn configuration_source_is_stale(
    component_revision: u64,
    component_digest: Digest256,
    registry_revision: u64,
    registration_config_digest: Digest256,
    candidate: &McpCatalogAuthorityCandidate,
) -> bool {
    candidate.component_revision < component_revision
        || candidate.registry_revision < registry_revision
        || (candidate.component_revision == component_revision
            && candidate.component_digest != component_digest)
        || (candidate.registry_revision == registry_revision
            && candidate.registration_config_digest != registration_config_digest)
}

/// Advance a global joined-configuration revision before scoped catalog work.
/// The EG owner transaction must load and compare-and-set this row atomically.
pub fn reconcile_joined_configuration(
    prior: Option<&McpJoinedConfigurationRow>,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<McpJoinedConfigurationRow, String> {
    validate_joined_candidate(candidate)?;
    if let Some(row) = prior {
        validate_joined_prior(row, candidate)?;
    }
    let configuration_digest = joined_configuration_digest(
        &candidate.tenant_id,
        &candidate.server_name,
        candidate.component_digest,
        candidate.registration_config_digest,
    )?;
    let configuration_revision = match prior {
        Some(row) if row.configuration_digest == configuration_digest => row.configuration_revision,
        Some(row) => row
            .configuration_revision
            .checked_add(1)
            .ok_or("configuration revision overflow")?,
        None => 1,
    };
    Ok(McpJoinedConfigurationRow {
        tenant_id: candidate.tenant_id.clone(),
        server_name: candidate.server_name.clone(),
        component_revision: candidate.component_revision,
        component_digest: candidate.component_digest,
        registry_revision: candidate.registry_revision,
        registration_config_digest: candidate.registration_config_digest,
        configuration_digest,
        configuration_revision,
    })
}

fn validate_joined_candidate(candidate: &McpCatalogAuthorityCandidate) -> Result<(), String> {
    let zero = Digest256::from_bytes([0; 32]);
    if candidate.tenant_id.is_empty()
        || candidate.server_name.is_empty()
        || candidate.component_revision == 0
        || candidate.registry_revision == 0
        || candidate.component_digest == zero
        || candidate.registration_config_digest == zero
    {
        return Err("incomplete joined configuration source".into());
    }
    Ok(())
}

fn validate_joined_prior(
    row: &McpJoinedConfigurationRow,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<(), String> {
    let stored_digest = joined_configuration_digest(
        &row.tenant_id,
        &row.server_name,
        row.component_digest,
        row.registration_config_digest,
    )?;
    if row.tenant_id != candidate.tenant_id
        || row.server_name != candidate.server_name
        || row.configuration_revision == 0
        || row.configuration_digest != stored_digest
    {
        return Err("invalid joined configuration row".into());
    }
    if configuration_source_is_stale(
        row.component_revision,
        row.component_digest,
        row.registry_revision,
        row.registration_config_digest,
        candidate,
    ) {
        return Err("stale or conflicting configuration source".into());
    }
    Ok(())
}

/// Compute the next row after the owner transaction loads and fences `prior`.
/// The owner transaction must compare-and-set the row before publishing it.
pub fn reconcile_catalog_authority(
    prior: Option<&McpCatalogAuthorityRow>,
    configuration: &McpJoinedConfigurationRow,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<McpCatalogAuthorityRow, String> {
    let zero = Digest256::from_bytes([0; 32]);
    validate_catalog_candidate(candidate, zero)?;
    if candidate.expected_catalog_generation != prior.map(|row| row.binding.catalog_generation) {
        return Err("stale catalog authority generation".into());
    }
    validate_joined_catalog_source(configuration, candidate)?;
    if let Some(row) = prior {
        validate_prior_catalog_authority(row, configuration, candidate, zero)?;
    }
    let configuration_revision = configuration.configuration_revision;
    let snapshot_digest = Digest256::framed(
        SNAPSHOT_DOMAIN,
        &[
            candidate.tenant_id.as_bytes(),
            candidate.server_name.as_bytes(),
            &configuration_revision.to_be_bytes(),
            candidate.four_family_digest.as_bytes(),
            candidate.registry_digest.as_bytes(),
            candidate.attester_principal_id.as_bytes(),
            candidate.child_id.as_bytes(),
            &candidate.local_catalog_epoch.to_be_bytes(),
            &candidate.child_connection_generation.to_be_bytes(),
            candidate.authorization_scope_digest.as_bytes(),
        ],
    )?;
    let catalog_generation = match prior {
        Some(row) if row.binding.snapshot_digest == snapshot_digest => {
            row.binding.catalog_generation
        }
        Some(row) => row
            .binding
            .catalog_generation
            .checked_add(1)
            .ok_or("catalog generation overflow")?,
        None => 1,
    };
    Ok(McpCatalogAuthorityRow {
        tenant_id: candidate.tenant_id.clone(),
        server_name: candidate.server_name.clone(),
        attester_principal_id: candidate.attester_principal_id.clone(),
        child_id: candidate.child_id.clone(),
        local_catalog_epoch: candidate.local_catalog_epoch,
        registry_digest: candidate.registry_digest,
        component_revision: candidate.component_revision,
        component_digest: candidate.component_digest,
        registry_revision: candidate.registry_revision,
        registration_config_digest: candidate.registration_config_digest,
        configuration_digest: configuration.configuration_digest,
        binding: McpCatalogSnapshotBinding {
            configuration_revision,
            catalog_generation,
            snapshot_digest,
            child_connection_generation: candidate.child_connection_generation,
            authorization_scope_digest: candidate.authorization_scope_digest,
        },
    })
}

fn validate_catalog_candidate(
    candidate: &McpCatalogAuthorityCandidate,
    zero: Digest256,
) -> Result<(), String> {
    if invalid_catalog_identity(candidate) || invalid_catalog_digests(candidate, zero) {
        return Err("incomplete catalog authority source".into());
    }
    Ok(())
}

fn invalid_catalog_identity(candidate: &McpCatalogAuthorityCandidate) -> bool {
    candidate.tenant_id.is_empty()
        || candidate.server_name.is_empty()
        || candidate.attester_principal_id.is_empty()
        || candidate.discovery_tenant != candidate.tenant_id
        || candidate.child_id.is_empty()
        || candidate.component_revision == 0
        || candidate.registry_revision == 0
        || candidate.child_connection_generation == 0
        || candidate.local_catalog_epoch == 0
}

fn invalid_catalog_digests(candidate: &McpCatalogAuthorityCandidate, zero: Digest256) -> bool {
    candidate.component_digest == zero
        || candidate.registration_config_digest == zero
        || candidate.four_family_digest == zero
        || candidate.registry_digest == zero
        || candidate.authorization_scope_digest == zero
}

fn validate_joined_catalog_source(
    configuration: &McpJoinedConfigurationRow,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<(), String> {
    let expected_configuration_digest = joined_configuration_digest(
        &candidate.tenant_id,
        &candidate.server_name,
        candidate.component_digest,
        candidate.registration_config_digest,
    )?;
    if configuration.tenant_id != candidate.tenant_id
        || configuration.server_name != candidate.server_name
        || configuration.component_revision != candidate.component_revision
        || configuration.component_digest != candidate.component_digest
        || configuration.registry_revision != candidate.registry_revision
        || configuration.registration_config_digest != candidate.registration_config_digest
        || configuration.configuration_revision == 0
        || configuration.configuration_digest != expected_configuration_digest
    {
        return Err("joined configuration source mismatch".into());
    }
    Ok(())
}

fn validate_prior_catalog_authority(
    row: &McpCatalogAuthorityRow,
    configuration: &McpJoinedConfigurationRow,
    candidate: &McpCatalogAuthorityCandidate,
    zero: Digest256,
) -> Result<(), String> {
    validate_stored_catalog_authority(row, zero)?;
    validate_catalog_configuration_history(row, configuration, candidate)?;
    validate_mounted_catalog_child(row, candidate)
}

fn validate_stored_catalog_authority(
    row: &McpCatalogAuthorityRow,
    zero: Digest256,
) -> Result<(), String> {
    if row.binding.configuration_revision == 0
        || row.binding.catalog_generation == 0
        || row.configuration_digest == zero
        || row.binding.snapshot_digest == zero
        || row.binding.authorization_scope_digest == zero
        || row.registry_digest == zero
        || row.attester_principal_id.is_empty()
        || row.local_catalog_epoch == 0
    {
        return Err("invalid stored catalog authority".into());
    }
    Ok(())
}

fn validate_catalog_configuration_history(
    row: &McpCatalogAuthorityRow,
    configuration: &McpJoinedConfigurationRow,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<(), String> {
    if row.tenant_id != candidate.tenant_id
        || row.server_name != candidate.server_name
        || row.binding.authorization_scope_digest != candidate.authorization_scope_digest
    {
        return Err("catalog authority key mismatch".into());
    }
    if configuration.configuration_revision < row.binding.configuration_revision
        || (configuration.configuration_digest == row.configuration_digest
            && configuration.configuration_revision != row.binding.configuration_revision)
    {
        return Err("stale or conflicting joined configuration revision".into());
    }
    if configuration_source_is_stale(
        row.component_revision,
        row.component_digest,
        row.registry_revision,
        row.registration_config_digest,
        candidate,
    ) {
        return Err("stale or conflicting configuration source".into());
    }
    Ok(())
}

fn validate_mounted_catalog_child(
    row: &McpCatalogAuthorityRow,
    candidate: &McpCatalogAuthorityCandidate,
) -> Result<(), String> {
    if row.child_id == candidate.child_id
        && (candidate.child_connection_generation < row.binding.child_connection_generation
            || candidate.local_catalog_epoch < row.local_catalog_epoch)
    {
        return Err("stale mounted child generation".into());
    }
    if row.child_id == candidate.child_id
        && row.attester_principal_id != candidate.attester_principal_id
    {
        return Err("mounted child attester changed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reconcile_for_test(
        prior: Option<&McpCatalogAuthorityRow>,
        candidate: &McpCatalogAuthorityCandidate,
    ) -> Result<McpCatalogAuthorityRow, String> {
        let prior_config = prior.map(|row| McpJoinedConfigurationRow {
            tenant_id: row.tenant_id.clone(),
            server_name: row.server_name.clone(),
            component_revision: row.component_revision,
            component_digest: row.component_digest,
            registry_revision: row.registry_revision,
            registration_config_digest: row.registration_config_digest,
            configuration_digest: row.configuration_digest,
            configuration_revision: row.binding.configuration_revision,
        });
        let configuration = reconcile_joined_configuration(prior_config.as_ref(), candidate)?;
        reconcile_catalog_authority(prior, &configuration, candidate)
    }

    fn candidate() -> McpCatalogAuthorityCandidate {
        sample_authority_candidate()
    }

    #[test]
    fn stable_snapshot_reuses_generation_and_content_change_advances() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut next = candidate();
        next.expected_catalog_generation = Some(1);
        let same = reconcile_for_test(Some(&first), &next).unwrap();
        assert_eq!(same.binding, first.binding);
        next.four_family_digest = Digest256::from_bytes([5; 32]);
        let changed = reconcile_for_test(Some(&same), &next).unwrap();
        assert_eq!(changed.binding.configuration_revision, 1);
        assert_eq!(changed.binding.catalog_generation, 2);
    }

    #[test]
    fn joined_config_and_child_changes_advance_exact_identity() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut next = candidate();
        next.expected_catalog_generation = Some(1);
        next.component_revision = 4;
        next.component_digest = Digest256::from_bytes([5; 32]);
        let config = reconcile_for_test(Some(&first), &next).unwrap();
        assert_eq!(config.binding.configuration_revision, 2);
        assert_eq!(config.binding.catalog_generation, 2);
        next.expected_catalog_generation = Some(2);
        next.child_connection_generation = 3;
        let child = reconcile_for_test(Some(&config), &next).unwrap();
        assert_eq!(child.binding.catalog_generation, 3);
        next.expected_catalog_generation = Some(3);
        next.child_id = "mounted-child-b".into();
        let replacement = reconcile_for_test(Some(&child), &next).unwrap();
        assert_eq!(replacement.binding.catalog_generation, 4);
    }

    #[test]
    fn scope_partition_cannot_reuse_another_partitions_row() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut other = candidate();
        other.authorization_scope_digest = Digest256::from_bytes([6; 32]);
        other.expected_catalog_generation = Some(1);
        assert!(reconcile_for_test(Some(&first), &other).is_err());
        other.expected_catalog_generation = None;
        let global_configuration = reconcile_joined_configuration(None, &candidate()).unwrap();
        let scoped = reconcile_catalog_authority(None, &global_configuration, &other).unwrap();
        assert_ne!(
            first.binding.snapshot_digest,
            scoped.binding.snapshot_digest
        );
        assert_eq!(
            first.binding.configuration_revision,
            scoped.binding.configuration_revision
        );
    }

    #[test]
    fn registration_change_advances_config_but_unrelated_registry_revision_does_not() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut next = candidate();
        next.expected_catalog_generation = Some(1);
        next.registry_revision = 9;
        let same_config = reconcile_for_test(Some(&first), &next).unwrap();
        assert_eq!(same_config.binding.configuration_revision, 1);
        assert_eq!(same_config.binding.catalog_generation, 1);
        next.registry_revision = 10;
        next.registration_config_digest = Digest256::from_bytes([7; 32]);
        let changed = reconcile_for_test(Some(&same_config), &next).unwrap();
        assert_eq!(changed.binding.configuration_revision, 2);
        assert_eq!(changed.binding.catalog_generation, 2);
    }

    #[test]
    fn attested_registry_and_new_mount_change_snapshot_generation() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut next = candidate();
        next.expected_catalog_generation = Some(1);
        next.registry_digest = Digest256::from_bytes([8; 32]);
        let registry = reconcile_for_test(Some(&first), &next).unwrap();
        assert_eq!(registry.binding.configuration_revision, 1);
        assert_eq!(registry.binding.catalog_generation, 2);
        next.expected_catalog_generation = Some(2);
        next.child_id = "mounted-child-b".into();
        next.attester_principal_id = "principal:sha256:attester-b".into();
        next.child_connection_generation = 1;
        let replacement = reconcile_for_test(Some(&registry), &next).unwrap();
        assert_eq!(replacement.binding.catalog_generation, 3);
    }

    #[test]
    fn global_configuration_row_rejects_corruption_and_retains_equal_content() {
        let first = reconcile_joined_configuration(None, &candidate()).unwrap();
        let mut next = candidate();
        next.component_revision = 4;
        next.registry_revision = 9;
        let same = reconcile_joined_configuration(Some(&first), &next).unwrap();
        assert_eq!(same.configuration_revision, 1);
        let mut corrupt = first;
        corrupt.configuration_digest = Digest256::from_bytes([9; 32]);
        assert!(reconcile_joined_configuration(Some(&corrupt), &next).is_err());
    }

    #[test]
    fn stale_or_unattested_sources_are_rejected() {
        let first = reconcile_for_test(None, &candidate()).unwrap();
        let mut next = candidate();
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.expected_catalog_generation = Some(1);
        next.component_revision = 2;
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.component_revision = 3;
        next.component_digest = Digest256::from_bytes([9; 32]);
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.component_digest = Digest256::from_bytes([1; 32]);
        next.child_connection_generation = 1;
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.child_connection_generation = 2;
        next.authorization_scope_digest = Digest256::from_bytes([0; 32]);
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.authorization_scope_digest = Digest256::from_bytes([4; 32]);
        next.discovery_tenant = "tenant-b".into();
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.discovery_tenant = "tenant-a".into();
        next.registry_digest = Digest256::from_bytes([0; 32]);
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.registry_digest = Digest256::from_bytes([7; 32]);
        next.local_catalog_epoch = 0;
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.local_catalog_epoch = 1;
        next.attester_principal_id.clear();
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.attester_principal_id = "principal:sha256:attester-b".into();
        assert!(reconcile_for_test(Some(&first), &next).is_err());
        next.attester_principal_id = "principal:sha256:attester-a".into();
        let mut false_configuration = reconcile_joined_configuration(None, &candidate()).unwrap();
        false_configuration.configuration_digest = Digest256::from_bytes([9; 32]);
        assert!(reconcile_catalog_authority(Some(&first), &false_configuration, &next).is_err());
        let mut corrupt = first;
        corrupt.binding.configuration_revision = 0;
        assert!(reconcile_for_test(Some(&corrupt), &next).is_err());
    }
}
