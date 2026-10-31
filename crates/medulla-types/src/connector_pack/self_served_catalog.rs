//! Self-served MCP catalog attestation.
//!
//! A producer that serves its own semantic content in process (it is the MCP
//! server, not a mounted child of one) has no child connection to attest. EG
//! still owns the catalog binding: the verified producer names the exact server
//! entry its next pack carries, EG verifies the enabled registration, pins that
//! entry's content digest and issues the binding, and the import that follows
//! must carry both the binding and the pinned server entry. The binding never
//! comes from the caller.

use serde::{Deserialize, Serialize};

use super::catalog_authority::{
    McpCatalogAttestation, McpCatalogAuthorityCandidate, McpCatalogAuthorityRow,
};
use super::index::ConnectorPackIndex;
use crate::agent_library::AgentLibraryMutationContext;
use crate::contract::{Digest256, ResourceId};

/// A verified producer asks EG to issue the binding for the catalog it serves
/// itself. Tenant, attester principal and authorization scope come from the
/// verified request, never from this body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct McpSelfServedCatalogAttestRequest {
    pub context: AgentLibraryMutationContext,
    /// The connector whose pack the binding is issued for; the attester must
    /// be its bound importer.
    pub connector: ResourceId,
    /// The registered `__commons__` server name, which is also the name of
    /// the pack's `McpServer` entry.
    pub server_name: String,
    /// SHA-256 of the pack's `McpServer` entry body (`index.server.body.sha256`).
    pub server_entry_digest: Digest256,
    pub registry_revision: u64,
    pub registry_digest: Digest256,
    pub registration_config_digest: Digest256,
    /// Digest of the tools, prompts, resources and resource templates served.
    pub four_family_digest: Digest256,
    /// The generation the caller last read; `None` only for the first binding.
    #[serde(default)]
    pub expected_catalog_generation: Option<u64>,
}

/// The verified facts EG supplies around a self-served request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfServedAttester<'a> {
    pub tenant_id: &'a str,
    pub principal_id: &'a str,
    pub authorization_scope_digest: Digest256,
    /// The pinned server entry's published revision, or 0 while the next
    /// import has yet to publish it.
    pub component_revision: u64,
}

/// Build the reducer candidate for a verified self-served request.
///
/// The producer is its own observing endpoint: its child identity is the
/// attester principal, and its single in-process catalog has one epoch and one
/// connection generation. A new principal is therefore a new child, exactly
/// as a replacement mount is.
pub fn self_served_candidate(
    request: &McpSelfServedCatalogAttestRequest,
    attester: SelfServedAttester<'_>,
) -> McpCatalogAuthorityCandidate {
    McpCatalogAuthorityCandidate {
        tenant_id: attester.tenant_id.to_string(),
        server_name: request.server_name.clone(),
        attester_principal_id: attester.principal_id.to_string(),
        discovery_tenant: attester.tenant_id.to_string(),
        component_revision: attester.component_revision,
        component_digest: request.server_entry_digest,
        registry_revision: request.registry_revision,
        registry_digest: request.registry_digest,
        registration_config_digest: request.registration_config_digest,
        four_family_digest: request.four_family_digest,
        child_id: attester.principal_id.to_string(),
        local_catalog_epoch: 1,
        child_connection_generation: 1,
        authorization_scope_digest: attester.authorization_scope_digest,
        expected_catalog_generation: request.expected_catalog_generation,
        attestation: McpCatalogAttestation::SelfServed {
            connector: request.connector.as_str().to_string(),
        },
    }
}

/// The revision a self-served pin names: the published server component's
/// revision when its content is exactly the pin, otherwise 0.
pub fn self_served_component_revision(published: Option<(u64, Digest256)>, pin: Digest256) -> u64 {
    match published {
        Some((revision, digest)) if digest == pin => revision,
        _ => 0,
    }
}

/// An import of `connector`'s pack must carry EG's current self-served binding
/// for its server and exactly the server entry that binding pinned. Rows of
/// another kind or connector are not this import's authority.
pub fn check_self_served_import(
    row: &McpCatalogAuthorityRow,
    index: &ConnectorPackIndex,
) -> Result<(), String> {
    let McpCatalogAttestation::SelfServed { connector } = &row.attestation else {
        return Ok(());
    };
    if connector != index.connector.as_str() {
        return Ok(());
    }
    if index.catalog != row.binding {
        return Err(
            "MALFORMED_INDEX: catalog binding is not the current self-served binding".to_string(),
        );
    }
    if index.server.body.sha256 != row.component_digest {
        return Err(
            "MALFORMED_INDEX: server entry differs from the self-served catalog pin".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCOPE: [u8; 32] = [4; 32];

    fn request() -> McpSelfServedCatalogAttestRequest {
        let candidate = crate::connector_pack::catalog_authority::sample_authority_candidate();
        McpSelfServedCatalogAttestRequest {
            context: crate::test_support::contract_wave::decision::mutation_context(),
            connector: ResourceId::new("graph-os").unwrap(),
            server_name: "graph-os".into(),
            server_entry_digest: candidate.component_digest,
            registry_revision: candidate.registry_revision,
            registry_digest: candidate.registry_digest,
            registration_config_digest: candidate.registration_config_digest,
            four_family_digest: candidate.four_family_digest,
            expected_catalog_generation: None,
        }
    }

    fn candidate(
        request: &McpSelfServedCatalogAttestRequest,
        revision: u64,
    ) -> McpCatalogAuthorityCandidate {
        self_served_candidate(
            request,
            SelfServedAttester {
                tenant_id: "tenant-a",
                principal_id: "principal:sha256:producer-a",
                authorization_scope_digest: Digest256::from_bytes(SCOPE),
                component_revision: revision,
            },
        )
    }

    fn attest(
        prior: Option<&McpCatalogAuthorityRow>,
        candidate: &McpCatalogAuthorityCandidate,
    ) -> Result<McpCatalogAuthorityRow, String> {
        crate::connector_pack::catalog_authority::reconcile_after_row(prior, candidate)
    }

    #[test]
    fn first_pin_needs_no_published_component_and_rerun_is_idempotent() {
        let first = attest(None, &candidate(&request(), 0)).unwrap();
        assert_eq!(first.component_revision, 0);
        assert_eq!(first.binding.catalog_generation, 1);
        assert_eq!(first.binding.configuration_revision, 1);
        assert_eq!(first.binding.child_connection_generation, 1);
        let mut again = request();
        again.expected_catalog_generation = Some(1);
        // The import has since published the pinned entry at revision 1.
        let published = attest(Some(&first), &candidate(&again, 1)).unwrap();
        assert_eq!(published.binding, first.binding);
        assert_eq!(published.component_revision, 1);
        // A heartbeat moves the registry revision and digest, not the binding.
        again.registry_revision += 1;
        again.registry_digest = Digest256::from_bytes([9; 32]);
        let heartbeat = attest(Some(&published), &candidate(&again, 1)).unwrap();
        assert_eq!(heartbeat.binding, first.binding);
    }

    #[test]
    fn catalog_and_pin_changes_advance_monotonically() {
        let first = attest(None, &candidate(&request(), 0)).unwrap();
        let mut next = request();
        next.expected_catalog_generation = Some(1);
        next.four_family_digest = Digest256::from_bytes([5; 32]);
        let catalog = attest(Some(&first), &candidate(&next, 0)).unwrap();
        assert_eq!(catalog.binding.configuration_revision, 1);
        assert_eq!(catalog.binding.catalog_generation, 2);
        next.expected_catalog_generation = Some(2);
        next.server_entry_digest = Digest256::from_bytes([6; 32]);
        let pinned = attest(Some(&catalog), &candidate(&next, 0)).unwrap();
        assert_eq!(pinned.binding.configuration_revision, 2);
        assert_eq!(pinned.binding.catalog_generation, 3);
    }

    #[test]
    fn stale_generation_and_kind_changes_are_refused() {
        let first = attest(None, &candidate(&request(), 0)).unwrap();
        // Stale: the caller did not read generation 1.
        assert!(attest(Some(&first), &candidate(&request(), 0)).is_err());
        let mut next = request();
        next.expected_catalog_generation = Some(1);
        let mut other_connector = candidate(&next, 0);
        other_connector.attestation = McpCatalogAttestation::SelfServed {
            connector: "other".into(),
        };
        assert!(attest(Some(&first), &other_connector).is_err());
        let mut mounted = crate::connector_pack::catalog_authority::sample_authority_candidate();
        mounted.server_name = "graph-os".into();
        let mounted_row = attest(None, &mounted).unwrap();
        assert!(mounted_row.attestation.is_mounted_child());
        // A mounted child cannot take over the self-served row ...
        mounted.expected_catalog_generation = Some(1);
        assert!(attest(Some(&first), &mounted).is_err());
        // ... nor a self-served producer the mounted one.
        let mut over_mounted = candidate(&next, 0);
        over_mounted.authorization_scope_digest = mounted_row.binding.authorization_scope_digest;
        assert!(attest(Some(&mounted_row), &over_mounted).is_err());
    }

    #[test]
    fn mounted_rows_encode_without_the_attestation_field() {
        let mounted = crate::connector_pack::catalog_authority::sample_authority_candidate();
        let row = attest(None, &mounted).unwrap();
        let encoded = rmp_serde::to_vec_named(&row).unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();
        assert!(value.get("attestation").is_none());
        let decoded: McpCatalogAuthorityRow = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, row);
        let self_served = attest(None, &candidate(&request(), 0)).unwrap();
        let encoded = rmp_serde::to_vec_named(&self_served).unwrap();
        let decoded: McpCatalogAuthorityRow = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, self_served);
    }

    #[test]
    fn component_revision_names_only_the_published_pinned_content() {
        let pin = Digest256::from_bytes([1; 32]);
        assert_eq!(self_served_component_revision(None, pin), 0);
        assert_eq!(self_served_component_revision(Some((4, pin)), pin), 4);
        let other = Digest256::from_bytes([2; 32]);
        assert_eq!(self_served_component_revision(Some((4, other)), pin), 0);
    }

    #[test]
    fn import_must_carry_the_current_binding_and_pinned_server_entry() {
        let row = attest(None, &candidate(&request(), 0)).unwrap();
        let mut index = crate::test_support::contract_wave::pack::index();
        index.connector = ResourceId::new("graph-os").unwrap();
        index.catalog = row.binding.clone();
        index.server.body.sha256 = row.component_digest;
        assert!(check_self_served_import(&row, &index).is_ok());
        let mut stale = index.clone();
        stale.catalog.catalog_generation += 1;
        assert!(check_self_served_import(&row, &stale).is_err());
        let mut repinned = index.clone();
        repinned.server.body.sha256 = Digest256::from_bytes([8; 32]);
        assert!(check_self_served_import(&row, &repinned).is_err());
        let mut other = repinned;
        other.connector = ResourceId::new("other").unwrap();
        assert!(check_self_served_import(&row, &other).is_ok());
        let mounted = attest(
            None,
            &crate::connector_pack::catalog_authority::sample_authority_candidate(),
        )
        .unwrap();
        assert!(check_self_served_import(&mounted, &stale).is_ok());
    }
}
