//! One admitted Agent Library write for a complete ConnectorPack catalog cut.

use std::collections::BTreeMap;

use eg_types::agent_component::{AgentComponentEntry, AgentComponentMutationKind};
use eg_types::connector_pack::{
    PackDisposition, PackDispositionCounts, PackHeadRef, PackHeadView, PackImportReceipt,
    PackImportRecord, PackImportResult, CONNECTOR_PACK_IMPORT_TOPIC,
    CONNECTOR_PACK_RESULT_SCHEMA_ID,
};
use eg_types::mutation::MutationResult;
use eg_types::mutation_batch::{
    DurabilityDomain, MutationOperation, MutationOutboxIntent, MutationSurface,
};
use eg_types::protocol::Method;
use redb::ReadableTable;

use super::{ConnectorPackBodyHolderRow, ConnectorPackHeadRow, ConnectorPackMemberRow};
use crate::server::persistence::agent_component::{component_tables, ComponentLayer};
use crate::server::persistence::agent_library::{
    admitted_context, agent_library_operation_identity, owner_batch_receipt, resolve_nonce_first,
    validate_context, AgentLibraryStore,
};
use crate::server::persistence::agent_revision::{
    apply_owner_rows, apply_revision_rows, finish_committed_ledger, finish_replayed,
    policy_admitted_context, recorded_receipt, revision_key, revision_operations,
    revision_outbox_headers, stage_batch, within_write, RevisionLayer,
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
        let owner = self.scope_handle(&plan.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let replay_context = admitted_context(&plan.context, "connector-pack:import")?;
        let nonce = resolve_nonce_first(&self.mutations, &txn, &replay_context)?;
        let operation = agent_library_operation_identity(
            &owner,
            &replay_context,
            "connector-pack-import",
            plan.record.connector.as_str(),
            plan.expected_head
                .as_ref()
                .map_or(0, |head| head.binding_revision),
            Some(&plan.record.pack_digest.to_hex()),
        )?;
        let replay = self.mutations.resolve_replay(&txn, &operation, &nonce)?;
        if let Some(receipt) = recorded_receipt(replay, "connector pack")? {
            let result = decode_replayed_result(&receipt)?;
            return finish_replayed(&self.mutations, txn, &operation, &nonce, result, receipt);
        }
        let (txn, committed) = within_write(txn, |txn| {
            let (operations, outbox) = pack_effects(&plan, &replay_context)?;
            let batch_context = policy_admitted_context(&replay_context, &operations)?;
            let (batch_id, staged) = stage_batch(
                self,
                txn,
                &owner,
                &batch_context,
                (&operation, &nonce),
                "connector pack",
                |version, batch_id| {
                    crate::server::persistence::agent_library::native_lifecycle_batch(
                        &owner,
                        &batch_context,
                        batch_id,
                        version,
                        operations,
                        outbox,
                    )
                },
            )?;
            let receipt = import_receipt(&plan, batch_id, staged.committed_version)?;
            let result = PackImportResult::Imported {
                receipt: receipt.clone(),
            };
            let mutation_result = crate::server::persistence::agent_row::domain_result(
                &result,
                CONNECTOR_PACK_RESULT_SCHEMA_ID,
                "connector pack",
            )?;
            let result_bytes =
                eg_storage::encode_bounded(&mutation_result, "connector pack domain result")?;
            let authority_receipt = owner_batch_receipt(
                &operation,
                &nonce,
                &staged.batch,
                "connector-pack",
                mutation_result,
                staged.committed_version,
                replay_context.created_at_ms,
            )?;
            apply_owner_rows(txn, &owner, &staged.batch, |write| {
                apply_pack_rows(write, &plan, &receipt)
            })?;
            finish_committed_ledger(
                &self.mutations,
                txn,
                (&staged, &result_bytes),
                replay_context.created_at_ms,
                (&operation, &nonce, &authority_receipt),
                "connector pack",
            )?;
            Ok((staged.batch, result))
        })?;
        self.mutations.commit(txn, &committed.0)?;
        Ok(committed.1)
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
) -> Result<(Vec<MutationOperation>, Vec<MutationOutboxIntent>), String> {
    let mut operations = Vec::with_capacity(plan.components.len() + 1);
    let mut outbox = Vec::with_capacity(plan.components.len() + 1);
    for component in &plan.components {
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
    operations.push(MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: "connector_pack_import".to_string(),
            query: plan.record.pack_digest.to_hex(),
        },
    });
    for (ordinal, operation) in operations.iter_mut().enumerate() {
        operation.ordinal = u32::try_from(ordinal)
            .map_err(|_| "connector pack operation count exceeds resource limits")?;
    }
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
    Ok((operations, outbox))
}

fn import_receipt(
    plan: &ConnectorPackCommitPlan,
    batch_id: String,
    committed_version: u64,
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
        batch_id,
        committed_version,
        counts,
        warnings: plan.record.warnings.clone(),
        projection: plan.record.projection.clone(),
    })
}

fn apply_pack_rows(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
    receipt: &PackImportReceipt,
) -> Result<(), String> {
    compare_head(write, plan)?;
    apply_components(write, plan)?;
    apply_members(write, plan)?;
    apply_holders(write, plan)?;
    apply_import_record(write, plan)?;
    apply_head(write, plan, receipt)
}

fn apply_components(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
) -> Result<(), String> {
    for component in &plan.components {
        let bytes = eg_storage::encode_bounded(&component.entry, "agent component revision")?;
        apply_revision_rows::<ComponentLayer>(
            write,
            component_tables(),
            &plan.context,
            component.expected_revision,
            &component.entry,
            &bytes,
        )?;
    }
    Ok(())
}

fn apply_members(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
) -> Result<(), String> {
    let tenant = plan.record.tenant_id.as_str();
    let connector = plan.record.connector.as_str();
    let mut members = write.open_table(eg_storage::CONNECTOR_PACK_MEMBERS)?;
    for member in &plan.members {
        let bytes = eg_storage::encode_bounded(member, "connector pack member")?;
        members
            .insert((tenant, connector, member.uri.as_str()), bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn apply_holders(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
) -> Result<(), String> {
    let tenant = plan.record.tenant_id.as_str();
    let mut holders = write.open_table(eg_storage::CONNECTOR_PACK_BODY_HOLDERS)?;
    for holder in &plan.holders {
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
        if let Some(existing) = existing {
            if existing != bytes {
                return Err(
                    "connector pack body holder conflicts with retained history".to_string()
                );
            }
        } else {
            holders
                .insert(key, bytes.as_slice())
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

fn apply_import_record(
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
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
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
    receipt: &PackImportReceipt,
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
            visible_record_id: None,
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
    write: &eg_transaction::AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    plan: &ConnectorPackCommitPlan,
) -> Result<(), String> {
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
        .transpose()?
        .map(|row| PackHeadRef {
            binding_revision: row.head.binding_revision,
            pack_digest: row.head.pack_digest,
        });
    if actual != plan.expected_head {
        return Err("PACK_HEAD_CONFLICT: connector pack head changed".to_string());
    }
    Ok(())
}

fn decode_replayed_result(
    receipt: &eg_types::mutation::MutationReceipt,
) -> Result<PackImportResult, String> {
    receipt.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        return Err("CORRUPT_MUTATION_LEDGER: connector pack replay has no result".to_string());
    };
    if schema_id.as_str() != CONNECTOR_PACK_RESULT_SCHEMA_ID {
        return Err("CORRUPT_MUTATION_LEDGER: connector pack result schema differs".to_string());
    }
    crate::server::persistence::agent_row::decode(payload.as_slice(), "connector pack result")
}
