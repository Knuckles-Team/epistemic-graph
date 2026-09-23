//! One admitted Agent Library write for a complete ConnectorPack catalog cut.

use std::collections::BTreeMap;

use eg_types::agent_component::{AgentComponentEntry, AgentComponentMutationKind};
use eg_types::connector_pack::{
    PackDisposition, PackDispositionCounts, PackHeadRef, PackHeadView, PackImportReceipt,
    PackImportRecord, PackImportResult, PackProjectionState, CONNECTOR_PACK_IMPORT_TOPIC,
    CONNECTOR_PACK_RESULT_SCHEMA_ID,
};
use eg_types::mutation_batch::{
    DurabilityDomain, MutationOperation, MutationOutboxIntent, MutationSurface,
};
use eg_types::protocol::Method;
use redb::ReadableTable;

use super::admitted::{PackOwnerWrite, PackWriteEffects, PackWriteIdentity, PackWriteStaged};
use super::{ConnectorPackBodyHolderRow, ConnectorPackHeadRow, ConnectorPackMemberRow};
use crate::server::persistence::agent_component::{component_tables, ComponentLayer};
use crate::server::persistence::agent_library::{validate_context, AgentLibraryStore};
use crate::server::persistence::agent_revision::{
    apply_revision_rows, revision_key, revision_operations, revision_outbox_headers, RevisionLayer,
};

pub(crate) struct ConnectorPackComponentCommit {
    pub expected_revision: u64,
    pub kind: AgentComponentMutationKind,
    pub entry: AgentComponentEntry,
}

pub(crate) struct ConnectorPackHolderCommit {
    pub body_sha256: eg_types::contract::Digest256,
    pub component_id: String,
    pub entry_revision: u64,
    pub row: ConnectorPackBodyHolderRow,
}

pub(crate) struct ConnectorPackCommitPlan {
    pub context: eg_types::agent_library::AgentLibraryMutationContext,
    pub expected_head: Option<PackHeadRef>,
    pub record: PackImportRecord,
    pub components: Vec<ConnectorPackComponentCommit>,
    pub members: Vec<ConnectorPackMemberRow>,
    pub holders: Vec<ConnectorPackHolderCommit>,
}

impl AgentLibraryStore {
    /// Commit every catalog-visible effect of one validated import atomically.
    pub(crate) fn commit_connector_pack(
        &self,
        plan: ConnectorPackCommitPlan,
    ) -> Result<PackImportResult, String> {
        validate_plan(self, &plan)?;
        let effects = pack_effects(&plan, &plan.context)?;
        let pack_digest = plan.record.pack_digest.to_hex();
        let identity = PackWriteIdentity {
            purpose: "connector-pack:import",
            kind: "connector-pack-import",
            slug: "connector-pack",
            subject: plan.record.connector.as_str(),
            revision: plan
                .expected_head
                .as_ref()
                .map_or(0, |head| head.binding_revision),
            discriminator: Some(&pack_digest),
            result_schema: CONNECTOR_PACK_RESULT_SCHEMA_ID,
            noun: "connector pack",
        };
        self.commit_pack_write(&plan.context, identity, effects, |write, staged| {
            let receipt = import_receipt(&plan, staged)?;
            apply_pack_rows(write, &plan, &receipt)?;
            Ok(PackImportResult::Imported {
                receipt: Box::new(receipt),
            })
        })
    }
}

fn validate_plan(store: &AgentLibraryStore, plan: &ConnectorPackCommitPlan) -> Result<(), String> {
    validate_context(store, &plan.context)?;
    let expected_binding_revision = match &plan.expected_head {
        Some(head) => head
            .binding_revision
            .checked_add(1)
            .ok_or_else(|| "connector pack binding revision overflow".to_string())?,
        None => 1,
    };
    if plan.context.tenant_id != plan.record.tenant_id
        || plan.record.binding_revision != expected_binding_revision
        || plan.record.previous_pack_digest
            != plan.expected_head.as_ref().map(|head| head.pack_digest)
    {
        return Err("connector pack commit plan identity is inconsistent".to_string());
    }
    for component in &plan.components {
        component.entry.validate()?;
        if component.entry.tenant_id != plan.context.tenant_id {
            return Err("connector pack component tenant differs from its import".to_string());
        }
    }
    if plan.record.committed_at_ms != plan.context.created_at_ms
        || plan
            .members
            .iter()
            .any(|member| member.last_record_id != plan.record.record_id)
    {
        return Err("connector pack commit plan provenance is inconsistent".to_string());
    }
    let member_uris: std::collections::BTreeSet<&str> = plan
        .members
        .iter()
        .map(|member| member.uri.as_str())
        .collect();
    if member_uris.len() != plan.members.len() {
        return Err("connector pack commit plan contains duplicate member URIs".to_string());
    }
    for component in &plan.components {
        let body = component
            .entry
            .content_ref
            .as_deref()
            .and_then(|reference| reference.strip_prefix("eg-body:sha256:"))
            .ok_or_else(|| "connector pack component has no engine body reference".to_string())?;
        let matching_holders = plan
            .holders
            .iter()
            .filter(|holder| {
                holder.component_id == component.entry.component_id
                    && holder.entry_revision == component.entry.entry_revision
                    && holder.body_sha256.to_hex() == body
                    && holder.row.length
                        <= eg_types::agent_component::MAX_COMPONENT_BODY_BYTES as u64
            })
            .count();
        if matching_holders != 1 {
            return Err(
                "connector pack component must have exactly one matching body holder".to_string(),
            );
        }
    }
    Ok(())
}

fn pack_effects(
    plan: &ConnectorPackCommitPlan,
    context: &eg_types::agent_library::AgentLibraryMutationContext,
) -> Result<PackWriteEffects, String> {
    let PackWriteEffects {
        mut operations,
        mut outbox,
    } = component_effects(&plan.components, context)?;
    operations.push(MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: "connector_pack_import".to_string(),
            query: plan.record.pack_digest.to_hex(),
        },
    });
    renumber(&mut operations)?;
    let payload = eg_storage::encode_bounded(&plan.record, "connector pack import record")?;
    outbox.push(MutationOutboxIntent {
        topic: CONNECTOR_PACK_IMPORT_TOPIC.to_string(),
        key: format!(
            "{}:{}:{}",
            plan.record.tenant_id,
            plan.record.connector.as_str(),
            plan.record.binding_revision
        ),
        payload,
        headers: BTreeMap::from([
            ("tenant_id".to_string(), plan.record.tenant_id.clone()),
            (
                "connector".to_string(),
                plan.record.connector.as_str().to_string(),
            ),
            ("record_id".to_string(), plan.record.record_id.clone()),
            (
                "binding_revision".to_string(),
                plan.record.binding_revision.to_string(),
            ),
        ]),
    });
    Ok(PackWriteEffects { operations, outbox })
}

/// The component revisions a pack write commits, as operations plus one
/// `eg.agent-component.revision.v1` intent each -- exactly what a direct
/// publish of each revision would have emitted.
pub(super) fn component_effects(
    components: &[ConnectorPackComponentCommit],
    context: &eg_types::agent_library::AgentLibraryMutationContext,
) -> Result<PackWriteEffects, String> {
    let mut operations = Vec::with_capacity(components.len() + 1);
    let mut outbox = Vec::with_capacity(components.len() + 1);
    for component in components {
        operations.extend(revision_operations::<ComponentLayer>(
            component.kind,
            &component.entry,
        ));
        let event = ComponentLayer::encode_outbox_event(component.kind, &component.entry, context)?;
        outbox.push(MutationOutboxIntent {
            topic: ComponentLayer::TOPIC.to_string(),
            key: revision_key(&ComponentLayer::revision_definition(&component.entry)),
            payload: event,
            headers: revision_outbox_headers::<ComponentLayer>(&component.entry),
        });
    }
    renumber(&mut operations)?;
    Ok(PackWriteEffects { operations, outbox })
}

/// Give every operation its batch ordinal.
pub(super) fn renumber(operations: &mut [MutationOperation]) -> Result<(), String> {
    for (ordinal, operation) in operations.iter_mut().enumerate() {
        operation.ordinal = u32::try_from(ordinal)
            .map_err(|_| "connector pack operation count exceeds resource limits")?;
    }
    Ok(())
}

fn import_receipt(
    plan: &ConnectorPackCommitPlan,
    staged: &PackWriteStaged,
) -> Result<PackImportReceipt, String> {
    let mut counts = PackDispositionCounts::default();
    for entry in plan.record.entries.iter() {
        match entry.disposition {
            PackDisposition::Published => counts.published += 1,
            PackDisposition::Revised => counts.revised += 1,
            PackDisposition::Unchanged => counts.unchanged += 1,
            PackDisposition::Withdrawn => counts.withdrawn += 1,
            PackDisposition::Republished => counts.republished += 1,
        }
    }
    Ok(PackImportReceipt {
        schema_version: plan.record.schema_version,
        tenant_id: plan.record.tenant_id.clone(),
        connector: plan.record.connector.clone(),
        binding_revision: plan.record.binding_revision,
        pack_digest: plan.record.pack_digest,
        catalog: plan.record.catalog.clone(),
        previous_pack_digest: plan.record.previous_pack_digest,
        record_id: plan.record.record_id.clone(),
        batch_id: staged.batch_id.clone(),
        committed_version: staged.committed_version,
        counts,
        warnings: plan.record.warnings.clone(),
        projection: plan.record.projection.clone(),
    })
}

fn apply_pack_rows(
    write: &PackOwnerWrite<'_>,
    plan: &ConnectorPackCommitPlan,
    receipt: &PackImportReceipt,
) -> Result<(), String> {
    let visible_record_id = compare_head(write, plan)?;
    super::pins::resolve_pack_pins_in_write(write, &plan.context.tenant_id, &plan.components)?;
    apply_catalog_rows(
        write,
        &PackCatalogRows {
            context: &plan.context,
            connector: plan.record.connector.as_str(),
            components: &plan.components,
            members: &plan.members,
            holders: &plan.holders,
        },
    )?;
    apply_import_record(write, plan)?;
    apply_head(write, plan, receipt, visible_record_id)
}

/// The catalog rows one pack write names: component revisions, member rows
/// and the body holders that keep each written revision's body alive.
pub(super) struct PackCatalogRows<'a> {
    pub(super) context: &'a eg_types::agent_library::AgentLibraryMutationContext,
    pub(super) connector: &'a str,
    pub(super) components: &'a [ConnectorPackComponentCommit],
    pub(super) members: &'a [ConnectorPackMemberRow],
    pub(super) holders: &'a [ConnectorPackHolderCommit],
}

/// Write component revisions (each under its own revision CAS), member rows
/// and body holders. A holder that already exists must be byte-identical:
/// holders are retained history, never rewritten.
pub(super) fn apply_catalog_rows(
    write: &PackOwnerWrite<'_>,
    rows: &PackCatalogRows<'_>,
) -> Result<(), String> {
    for component in rows.components {
        let bytes = eg_storage::encode_bounded(&component.entry, "agent component revision")?;
        apply_revision_rows::<ComponentLayer>(
            write,
            component_tables(),
            rows.context,
            component.expected_revision,
            &component.entry,
            &bytes,
        )?;
    }
    let tenant = rows.context.tenant_id.as_str();
    let mut members = write.open_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
    for member in rows.members {
        let bytes = eg_storage::encode_bounded(member, "connector pack member")?;
        members
            .insert(
                (tenant, rows.connector, member.uri.as_str()),
                bytes.as_slice(),
            )
            .map_err(|error| error.to_string())?;
    }
    apply_holders(write, tenant, rows.holders)
}

fn apply_holders(
    write: &PackOwnerWrite<'_>,
    tenant: &str,
    holders_to_write: &[ConnectorPackHolderCommit],
) -> Result<(), String> {
    let mut holders = write.open_table(eg_storage::CONNECTOR_PACK_BODY_HOLDERS)?;
    for holder in holders_to_write {
        let body = holder.body_sha256.to_hex();
        let key = (
            tenant,
            body.as_str(),
            holder.component_id.as_str(),
            holder.entry_revision,
        );
        let bytes = eg_storage::encode_bounded(&holder.row, "connector pack body holder")?;
        let existing = holders
            .get(key)
            .map_err(|error| error.to_string())?
            .map(|existing| existing.value().to_vec());
        match existing {
            Some(existing) if existing != bytes => {
                return Err(
                    "connector pack body holder conflicts with retained history".to_string()
                );
            }
            Some(_) => {}
            None => {
                holders
                    .insert(key, bytes.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}

fn apply_import_record(
    write: &PackOwnerWrite<'_>,
    plan: &ConnectorPackCommitPlan,
) -> Result<(), String> {
    let tenant = plan.record.tenant_id.as_str();
    let connector = plan.record.connector.as_str();
    let mut imports = write.open_table(eg_storage::CONNECTOR_PACK_IMPORTS)?;
    let record_bytes = eg_storage::encode_bounded(&plan.record, "connector pack import record")?;
    if imports
        .get((tenant, connector, plan.record.binding_revision))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("connector pack import revision already exists".to_string());
    }
    imports
        .insert(
            (tenant, connector, plan.record.binding_revision),
            record_bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn apply_head(
    write: &PackOwnerWrite<'_>,
    plan: &ConnectorPackCommitPlan,
    receipt: &PackImportReceipt,
    visible_record_id: Option<String>,
) -> Result<(), String> {
    let tenant = plan.record.tenant_id.as_str();
    let connector = plan.record.connector.as_str();
    let head = ConnectorPackHeadRow {
        head: PackHeadView {
            binding_revision: plan.record.binding_revision,
            pack_digest: plan.record.pack_digest,
            catalog: plan.record.catalog.clone(),
            server_contract_version: plan.record.server.contract_version.clone(),
            server_package_version: plan.record.server.package_version.clone(),
            record_id: plan.record.record_id.clone(),
            // The prior GraphSchema source remains served until this head's
            // projection completes its exact-head visibility CAS. A pack with
            // nothing to project is visible the moment it commits.
            visible_record_id: match plan.record.projection {
                PackProjectionState::None => Some(plan.record.record_id.clone()),
                PackProjectionState::Pending
                | PackProjectionState::Applied { .. }
                | PackProjectionState::Failed { .. } => visible_record_id,
            },
            committed_at_ms: plan.record.committed_at_ms,
        },
        receipt: receipt.clone(),
    };
    let bytes = eg_storage::encode_bounded(&head, "connector pack head")?;
    write
        .open_table(eg_storage::CONNECTOR_PACK_HEADS)?
        .insert((tenant, connector), bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn compare_head(
    write: &PackOwnerWrite<'_>,
    plan: &ConnectorPackCommitPlan,
) -> Result<Option<String>, String> {
    let heads = write.open_table(eg_storage::CONNECTOR_PACK_HEADS)?;
    let actual = heads
        .get((
            plan.record.tenant_id.as_str(),
            plan.record.connector.as_str(),
        ))
        .map_err(|error| error.to_string())?
        .map(|value| {
            crate::server::persistence::agent_row::decode::<ConnectorPackHeadRow>(
                value.value(),
                "connector pack head",
            )
        })
        .transpose()?;
    let actual_head = actual.as_ref().map(|row| PackHeadRef {
        binding_revision: row.head.binding_revision,
        pack_digest: row.head.pack_digest,
    });
    if actual_head != plan.expected_head {
        return Err("PACK_HEAD_CONFLICT: connector pack head changed".to_string());
    }
    Ok(actual.and_then(|row| row.head.visible_record_id))
}
