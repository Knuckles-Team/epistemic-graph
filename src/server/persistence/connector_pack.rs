//! Durable ConnectorPack catalog reads.
//!
//! These rows share the Agent Library owner file so a later import can commit
//! its head, members, component revisions, holders, record, receipt, and outbox
//! under one admitted write. The commit module exposes only that complete
//! admitted batch; there is no partial catalog writer that could publish a head
//! without the component and body-holder rows it names.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{
    ConnectorPackStatus, ConnectorSchemaMapping, PackEntryKind, PackHeadView, PackImportReceipt,
    PackMemberCounts, PackProjectionState,
};
use eg_types::contract::{BoundedVec, Digest256, ResourceId};

use super::agent_library::AgentLibraryStore;

pub(crate) mod commit;
mod manifest;

pub use manifest::decode_schema_mappings;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectorPackHeadRow {
    pub head: PackHeadView,
    pub receipt: PackImportReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectorPackMemberRow {
    pub uri: String,
    pub kind: PackEntryKind,
    pub entry_digest: Digest256,
    pub component_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub lifecycle: AgentLibraryLifecycle,
    pub body_sha256: Digest256,
    pub engine_manifest_digest: String,
    pub body_length: u64,
    pub last_record_id: String,
    #[serde(default)]
    pub schema_mappings: Option<BTreeMap<String, ConnectorSchemaMapping>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConnectorPackBindingRow {
    pub importer: String,
    pub bound_by: String,
    pub bound_at_ms: u64,
}

/// Liveness link committed with one component revision. Its key carries
/// `(tenant, body_sha256, component_id, entry_revision)`; the row pins the
/// engine-owned CAS manifest the read path must verify.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorPackBodyHolderRow {
    pub engine_manifest_digest: String,
    pub length: u64,
}

/// Authoritative connector mapping bytes resolved from the current imported
/// manifest, never supplied by the ingestion caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConnectorSchemaMapping {
    pub mapping_reference: String,
    pub schema_mapping: ConnectorSchemaMapping,
    pub body_sha256: Digest256,
    pub entry_revision: u64,
    pub pack_digest: Digest256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorPackContentPointer {
    pub component_id: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    pub body_sha256: Digest256,
    pub engine_manifest_digest: String,
    pub length: u64,
    pub media_type: String,
}

impl AgentLibraryStore {
    /// Current durable membership rows for one connector, in URI order.
    ///
    /// Import planning uses this snapshot to derive withdrawals and to reject
    /// resurrection of an administratively retired member. The subsequent
    /// atomic commit still compares the pack head, so this read never becomes
    /// the write authority by itself.
    pub(crate) fn connector_pack_members(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
    ) -> Result<Vec<ConnectorPackMemberRow>, String> {
        eg_types::agent_library::validate_key(tenant_id, connector.as_str())?;
        let read = self.read()?;
        let members = read.open_owner_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
        let mut rows = Vec::new();
        for row in members
            .range((tenant_id, connector.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_connector, _) = key.value();
            if row_tenant != tenant_id || row_connector != connector.as_str() {
                break;
            }
            rows.push(super::agent_row::decode(
                value.value(),
                "connector pack member",
            )?);
        }
        Ok(rows)
    }

    /// Resolve a component revision and its engine-body holder in one Agent
    /// Library snapshot. A content reference without the exact holder is never
    /// enough to authorize a Blob read.
    pub fn connector_pack_content_pointer(
        &self,
        tenant_id: &str,
        component_id: &str,
        entry_revision: Option<u64>,
    ) -> Result<ConnectorPackContentPointer, String> {
        eg_types::agent_library::validate_key(tenant_id, component_id)?;
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revision = match entry_revision {
            Some(revision) if revision > 0 => revision,
            Some(_) => return Err("agent component revision must be positive".to_string()),
            None => heads
                .get((tenant_id, component_id))
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .ok_or_else(|| "agent component does not exist".to_string())?,
        };
        let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let entry = revisions
            .get((tenant_id, component_id, revision))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent component revision does not exist".to_string())?;
        let entry: eg_types::agent_component::AgentComponentEntry =
            super::agent_row::decode(entry.value(), "agent component revision")?;
        let body_hex = entry
            .content_ref
            .as_deref()
            .and_then(|reference| reference.strip_prefix("eg-body:sha256:"))
            .ok_or_else(|| "COMPONENT_BODY_UNAVAILABLE: revision has no engine body".to_string())?;
        let body_sha256 = Digest256::parse(body_hex)
            .map_err(|_| "COMPONENT_BODY_UNAVAILABLE: revision body reference is invalid")?;
        if entry.content_digest != format!("sha256:{body_hex}") {
            return Err("COMPONENT_BODY_UNAVAILABLE: revision body digests disagree".to_string());
        }
        let holders = read.open_owner_table(eg_storage::CONNECTOR_PACK_BODY_HOLDERS)?;
        let holder = holders
            .get((tenant_id, body_hex, component_id, revision))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "COMPONENT_BODY_UNAVAILABLE: revision has no body holder".to_string())?;
        let holder: ConnectorPackBodyHolderRow =
            super::agent_row::decode(holder.value(), "connector pack body holder")?;
        let media_type = entry
            .attributes
            .get(eg_types::agent_component::COMPONENT_MEDIA_TYPE_ATTRIBUTE)
            .cloned()
            .unwrap_or_else(|| eg_types::agent_component::DEFAULT_COMPONENT_MEDIA_TYPE.to_string());
        Ok(ConnectorPackContentPointer {
            component_id: entry.component_id,
            entry_revision: entry.entry_revision,
            definition_digest: entry.definition_digest,
            body_sha256,
            engine_manifest_digest: holder.engine_manifest_digest,
            length: holder.length,
            media_type,
        })
    }

    /// Read one connector's catalog state from one owner snapshot.
    pub fn connector_pack_status(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
    ) -> Result<ConnectorPackStatus, String> {
        eg_types::agent_library::validate_key(tenant_id, connector.as_str())?;
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::CONNECTOR_PACK_HEADS)?;
        let members = read.open_owner_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
        let bindings = read.open_owner_table(eg_storage::CONNECTOR_PACK_BINDINGS)?;

        let head = heads
            .get((tenant_id, connector.as_str()))
            .map_err(|error| error.to_string())?
            .map(|row| {
                super::agent_row::decode::<ConnectorPackHeadRow>(row.value(), "connector pack head")
            })
            .transpose()?;
        let binding = bindings
            .get((tenant_id, connector.as_str()))
            .map_err(|error| error.to_string())?
            .map(|row| {
                super::agent_row::decode::<ConnectorPackBindingRow>(
                    row.value(),
                    "connector pack binding",
                )
            })
            .transpose()?;

        let mut counts = PackMemberCounts::default();
        for row in members
            .range((tenant_id, connector.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_connector, _) = key.value();
            if row_tenant != tenant_id || row_connector != connector.as_str() {
                break;
            }
            let member: ConnectorPackMemberRow =
                super::agent_row::decode(value.value(), "connector pack member")?;
            match member.lifecycle {
                AgentLibraryLifecycle::Published => counts.published += 1,
                AgentLibraryLifecycle::Withdrawn => counts.withdrawn += 1,
                AgentLibraryLifecycle::Retired => counts.retired += 1,
            }
        }

        status_from_rows(tenant_id, connector, head, binding, counts)
    }

    pub fn connector_pack_importer(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
    ) -> Result<Option<String>, String> {
        let status = self.connector_pack_status(tenant_id, connector)?;
        Ok(status.importer)
    }

    /// Resolve an exact mapping reference against the current pack head.
    ///
    /// `manifest:<connector>#schema_mappings/<key>` selects one named mapping.
    /// The shorter `manifest:<connector>` form is retained as the single-map
    /// request; the resolver rejects it as
    /// `CONNECTOR_SCHEMA_MAPPING_AMBIGUOUS` unless the decoded manifest has
    /// exactly one mapping.
    ///
    /// The decoded mapping projection was derived from the digest-verified
    /// engine body during import and commits beside the member. Callers cannot
    /// substitute mapping bytes in an ingestion request.
    pub fn resolve_connector_schema_mapping(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
        mapping_reference: &str,
    ) -> Result<ResolvedConnectorSchemaMapping, String> {
        eg_types::agent_library::validate_key(tenant_id, connector.as_str())?;
        let selector = mapping_selector(connector, mapping_reference)?;
        let (head, manifest) = self.current_connector_manifest(tenant_id, connector)?;
        validate_current_manifest(&head, &manifest)?;
        let mappings = manifest.schema_mappings.as_ref().ok_or_else(|| {
            "CONNECTOR_SCHEMA_MAPPING_UNAVAILABLE: imported manifest has no decoded mappings"
                .to_string()
        })?;
        let selected = select_schema_mapping(mappings, selector)?;
        validate_selected_mapping(&selected)?;
        Ok(ResolvedConnectorSchemaMapping {
            mapping_reference: mapping_reference.to_string(),
            schema_mapping: selected,
            body_sha256: manifest.body_sha256,
            entry_revision: manifest.entry_revision,
            pack_digest: head.head.pack_digest,
        })
    }

    fn current_connector_manifest(
        &self,
        tenant_id: &str,
        connector: &ResourceId,
    ) -> Result<(ConnectorPackHeadRow, ConnectorPackMemberRow), String> {
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::CONNECTOR_PACK_HEADS)?;
        let members = read.open_owner_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
        let head = heads
            .get((tenant_id, connector.as_str()))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "UNKNOWN_MAPPING_REFERENCE: connector has no imported pack".to_string()
            })?;
        let head: ConnectorPackHeadRow =
            super::agent_row::decode(head.value(), "connector pack head")?;
        let mut manifest = None;
        for row in members
            .range((tenant_id, connector.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_connector, _) = key.value();
            if row_tenant != tenant_id || row_connector != connector.as_str() {
                break;
            }
            let member: ConnectorPackMemberRow =
                super::agent_row::decode(value.value(), "connector pack member")?;
            if member.kind == PackEntryKind::Manifest {
                if member.lifecycle != AgentLibraryLifecycle::Published {
                    continue;
                }
                if manifest.is_some() {
                    return Err(
                        "CONNECTOR_SCHEMA_MAPPING_AMBIGUOUS: current pack has multiple manifests"
                            .to_string(),
                    );
                }
                manifest = Some(member);
            }
        }
        manifest.map(|manifest| (head, manifest)).ok_or_else(|| {
            "UNKNOWN_MAPPING_REFERENCE: current connector pack has no manifest".to_string()
        })
    }
}

fn status_from_rows(
    tenant_id: &str,
    connector: &ResourceId,
    head: Option<ConnectorPackHeadRow>,
    binding: Option<ConnectorPackBindingRow>,
    members: PackMemberCounts,
) -> Result<ConnectorPackStatus, String> {
    let projection = head
        .as_ref()
        .map(|row| row.receipt.projection.clone())
        .unwrap_or(PackProjectionState::None);
    let warnings = BoundedVec::new(
        head.as_ref()
            .map(|row| row.receipt.warnings.as_slice().to_vec())
            .unwrap_or_default(),
    )?;
    Ok(ConnectorPackStatus {
        schema_version: eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION,
        tenant_id: tenant_id.to_string(),
        connector: connector.clone(),
        head: head.as_ref().map(|row| row.head.clone()),
        members,
        last_receipt: head.map(|row| row.receipt),
        warnings,
        importer: binding.map(|row| row.importer),
        projection,
    })
}

enum MappingSelector<'a> {
    Only,
    Key(&'a str),
}

fn mapping_selector<'a>(
    connector: &ResourceId,
    mapping_reference: &'a str,
) -> Result<MappingSelector<'a>, String> {
    let manifest_reference = format!("manifest:{}", connector.as_str());
    if mapping_reference == manifest_reference {
        return Ok(MappingSelector::Only);
    }
    let keyed_prefix = format!("{manifest_reference}#schema_mappings/");
    mapping_reference
        .strip_prefix(&keyed_prefix)
        .filter(|key| valid_mapping_key(key))
        .map(MappingSelector::Key)
        .ok_or_else(|| {
            "UNKNOWN_MAPPING_REFERENCE: connector mapping reference is not current".to_string()
        })
}

fn validate_current_manifest(
    head: &ConnectorPackHeadRow,
    manifest: &ConnectorPackMemberRow,
) -> Result<(), String> {
    if manifest.last_record_id != head.head.record_id {
        return Err("STALE_MAPPING_REFERENCE: manifest is not from the current pack".to_string());
    }
    Ok(())
}

fn select_schema_mapping(
    mappings: &BTreeMap<String, ConnectorSchemaMapping>,
    selector: MappingSelector<'_>,
) -> Result<ConnectorSchemaMapping, String> {
    match selector {
        MappingSelector::Key(key) => mappings.get(key).cloned().ok_or_else(|| {
            "UNKNOWN_MAPPING_REFERENCE: manifest has no mapping with that key".to_string()
        }),
        MappingSelector::Only if mappings.len() == 1 => {
            Ok(mappings.values().next().cloned().unwrap())
        }
        MappingSelector::Only => {
            Err("CONNECTOR_SCHEMA_MAPPING_AMBIGUOUS: mapping key is required".to_string())
        }
    }
}

fn validate_selected_mapping(mapping: &ConnectorSchemaMapping) -> Result<(), String> {
    if mapping.ontology_class.trim().is_empty() {
        return Err("CONNECTOR_SCHEMA_MAPPING_INVALID: mapping has no ontology class".to_string());
    }
    Ok(())
}

fn valid_mapping_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 256
        && !key
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_catalog_has_an_explicit_empty_status() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let connector = ResourceId::new("test-connector").unwrap();
        let status = store.connector_pack_status("tenant-a", &connector).unwrap();
        assert!(status.head.is_none());
        assert!(status.last_receipt.is_none());
        assert_eq!(status.members, PackMemberCounts::default());
        assert_eq!(status.projection, PackProjectionState::None);
    }

    #[test]
    fn mapping_reference_keys_are_bounded_and_nonblank() {
        assert!(valid_mapping_key("orders-v1"));
        assert!(!valid_mapping_key(""));
        assert!(!valid_mapping_key("orders v1"));
    }
}
