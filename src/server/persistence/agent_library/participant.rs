//! Durable AgentLibraryOwner reservation and sealed decision-record intent.
//! This participant confers no authorization and never infers an abort.

use eg_storage::AgentLibraryOwner;
use eg_transaction::{
    AdmittedMutation, Begin, CoordinatorDecisionBinding, CoordinatorDecisionCertificate,
};
use eg_types::decision::DecisionRecord;
use eg_types::{AgentLibraryMutationContext, MutationBatch};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{AgentLibraryStore, AGENT_LIBRARY_PHYSICAL_STORE};

const ACTIVE: &str = "active";
const MAX_INTENT_BYTES: usize = 16 * 1024 * 1024;
type Write<'a> = AdmittedMutation<'a, AgentLibraryOwner>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LibraryReservation {
    schema_version: u16,
    owner: String,
    binding: CoordinatorDecisionBinding,
    source_version: u64,
    context: AgentLibraryMutationContext,
    record: DecisionRecord,
    heads: Vec<(String, u64, String)>,
}

impl LibraryReservation {
    pub(crate) fn transaction_id(&self) -> &str {
        &self.binding.transaction_id
    }
    pub(crate) fn binding(&self) -> &CoordinatorDecisionBinding {
        &self.binding
    }
}

/// Implemented only by the native coordinator's durable decision adapter.
/// `None` retains the reservation, including after restart.
pub(crate) trait LibraryReservationDecisionSource {
    fn terminal_decision(
        &self,
        reservation: &LibraryReservation,
    ) -> Result<Option<CoordinatorDecisionCertificate>, String>;
}

/// Constructor is private: an ordinary writer cannot claim a reserved commit.
pub(crate) struct ResolutionPermit<'a> {
    record: &'a LibraryReservation,
}

impl ResolutionPermit<'_> {
    pub(crate) fn validate_decision(
        &self,
        context: &AgentLibraryMutationContext,
        record: &DecisionRecord,
    ) -> Result<(), String> {
        if context != &self.record.context || record != &self.record.record {
            return Err("reserved decision differs from sealed intent".into());
        }
        Ok(())
    }

    pub(crate) fn consume(
        &self,
        rows: &eg_transaction::AdmittedOwnerWrite<'_, AgentLibraryOwner>,
    ) -> Result<(), String> {
        let mut table = rows.open_table(eg_storage::AGENT_LIBRARY_PARTICIPANTS)?;
        table
            .insert(ACTIVE, b"null".as_slice())
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn decode(bytes: Option<&[u8]>) -> Result<Option<LibraryReservation>, String> {
    let Some(bytes) = bytes else { return Ok(None) };
    if bytes.len() > MAX_INTENT_BYTES {
        return Err("library reservation exceeds byte budget".into());
    }
    let result: Option<LibraryReservation> =
        serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if let Some(record) = &result {
        if record.schema_version != 1
            || record.owner != AGENT_LIBRARY_PHYSICAL_STORE
            || record.context.tenant_id != record.record.tenant_id
            || record.binding.tenant != record.context.tenant_id
        {
            return Err("invalid library reservation owner or tenant".into());
        }
        record.binding.validate()?;
    }
    Ok(result)
}

fn active(txn: &Write<'_>) -> Result<Option<LibraryReservation>, String> {
    let table = txn.open_read_table(eg_storage::AGENT_LIBRARY_PARTICIPANTS)?;
    let value = table.get(ACTIVE).map_err(|e| e.to_string())?;
    decode(value.as_ref().map(|v| v.value()))
}

pub(crate) fn check_write(
    txn: &Write<'_>,
    permit: Option<&ResolutionPermit<'_>>,
) -> Result<(), String> {
    match (active(txn)?, permit) {
        (None, None) => Ok(()),
        (Some(record), Some(permit)) if &record == permit.record => validate_inputs(txn, &record),
        _ => Err("CONFLICT: AgentLibraryOwner has an unresolved reservation".into()),
    }
}

fn component_heads(
    txn: &Write<'_>,
    record: &DecisionRecord,
) -> Result<Vec<(String, u64, String)>, String> {
    use crate::server::persistence::{
        agent_component::ComponentLayer, agent_revision::decode_revision,
    };
    let heads = txn.open_read_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let revisions = txn.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let packs = txn.open_read_table(eg_storage::CONNECTOR_PACK_HEADS)?;
    record
        .inputs
        .candidates
        .iter()
        .map(|candidate| {
            let tenant = record.tenant_id.as_str();
            let id = candidate.component_id.as_str();
            if !crate::server::persistence::connector_pack::visibility::pack_component_visible(
                id,
                |connector| {
                    Ok(packs
                        .get((tenant, connector))
                        .map_err(|e| e.to_string())?
                        .map(|v| v.value().to_vec()))
                },
            )? {
                return Err("reserved component pack is not visible".into());
            }

            let revision = heads
                .get((tenant, id))
                .map_err(|e| e.to_string())?
                .ok_or("reserved component head missing")?
                .value();
            let bytes = revisions
                .get((tenant, id, revision))
                .map_err(|e| e.to_string())?
                .ok_or("reserved component revision missing")?;
            let entry = decode_revision::<ComponentLayer>(bytes.value())?;
            if revision != candidate.entry_revision
                || entry.lifecycle != eg_types::AgentLibraryLifecycle::Published
            {
                return Err("reserved component is no longer the published head".into());
            }
            Ok((
                id.to_string(),
                revision,
                hex::encode(Sha256::digest(bytes.value())),
            ))
        })
        .collect()
}

fn validate_dependencies(txn: &Write<'_>, record: &DecisionRecord) -> Result<(), String> {
    use crate::server::persistence::{
        agent_component::ComponentLayer, agent_revision::decode_revision,
    };
    let pins = {
        let revisions = txn.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let mut pins = Vec::new();
        for candidate in &record.inputs.candidates {
            let row = revisions
                .get((
                    record.tenant_id.as_str(),
                    candidate.component_id.as_str(),
                    candidate.entry_revision,
                ))
                .map_err(|e| e.to_string())?
                .ok_or("reserved dependency source missing")?;
            let entry = decode_revision::<ComponentLayer>(row.value())?;
            pins.extend(entry.requires);
        }
        pins
    };
    crate::server::persistence::agent_component::pins::resolve_component_pins_in_write(
        txn,
        &record.tenant_id,
        "reserved candidate dependencies",
        &pins.iter().collect::<Vec<_>>(),
    )
}

fn validate_inputs(txn: &Write<'_>, reservation: &LibraryReservation) -> Result<(), String> {
    crate::server::persistence::decision_record::validate_reservation_inputs(
        txn,
        &reservation.record,
    )?;
    validate_dependencies(txn, &reservation.record)?;
    if component_heads(txn, &reservation.record)? != reservation.heads {
        return Err("reserved component preimage changed".into());
    }
    let body = txn.open_read_table(eg_storage::DECISION_RECORDS)?;
    let heads = txn.open_read_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let key = (
        reservation.record.tenant_id.as_str(),
        reservation.record.record_id.as_str(),
    );
    if body.get(key).map_err(|e| e.to_string())?.is_some()
        || heads.get(key).map_err(|e| e.to_string())?.is_some()
    {
        return Err("reserved decision target is not absent".into());
    }
    Ok(())
}

fn batch(
    store: &AgentLibraryStore,
    record: &LibraryReservation,
    phase: &str,
    version: u64,
) -> Result<MutationBatch, String> {
    let owner = store.scope_handle(&record.context.tenant_id)?;
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&(phase, record)).map_err(|e| e.to_string())?,
    ));
    // The ledger key claims the transaction permanently, independent of the
    // proposed payload or source version. Reusing it cannot mint a fresh prepare.
    let transaction = hex::encode(Sha256::digest(record.binding.transaction_id.as_bytes()));
    let id = format!("library-participant:{phase}:{transaction}");
    Ok(MutationBatch {
        schema_version: eg_types::MUTATION_BATCH_VERSION,
        batch_id: id.clone(),
        envelope: eg_types::mutation_batch::MutationEnvelope::maintenance(
            owner.principal(),
            "library_participant",
            &digest,
            &id,
        )?,
        identity: owner.identity().clone(),
        placement_epoch: 0,
        version_expectation: eg_types::VersionExpectation::Native(version),
        fencing_token: None,
        authoritative_state: None,
        operations: crate::server::persistence::agent_revision::lifecycle_operations(
            format!("library_participant_{phase}"),
            digest,
        ),
        outbox: vec![],
        created_at_ms: record.context.created_at_ms,
    })
}

fn finish(
    store: &AgentLibraryStore,
    txn: Write<'_>,
    batch: &MutationBatch,
    record: Option<&LibraryReservation>,
    source: Option<u64>,
) -> Result<(), String> {
    let owner = store.scope_handle(batch.identity.tenant().as_str())?;
    let rows = txn.owner_rows(&owner, batch)?;
    {
        let bytes = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_INTENT_BYTES {
            return Err("library reservation exceeds byte budget".into());
        }
        let mut table = rows.open_table(eg_storage::AGENT_LIBRARY_PARTICIPANTS)?;
        table
            .insert(ACTIVE, bytes.as_slice())
            .map_err(|e| e.to_string())?;
    }
    rows.finish_owner()?;
    store.mutations.finish(
        &txn,
        batch,
        Some(rmp_serde::to_vec_named(&record).map_err(|e| e.to_string())?),
        batch.created_at_ms,
        source,
    )?;
    store.mutations.commit(txn, batch)
}

impl AgentLibraryStore {
    /// Preserve the exact intent and read set under the same write lock as prepare.
    pub(crate) fn prepare_library_reservation(
        &self,
        binding: CoordinatorDecisionBinding,
        source_version: u64,
        context: AgentLibraryMutationContext,
        record: DecisionRecord,
    ) -> Result<LibraryReservation, String> {
        binding.validate()?;
        super::validate_context(self, &context)?;
        if context.tenant_id != record.tenant_id || binding.tenant != context.tenant_id {
            return Err("ACCESS_DENIED: reservation tenant mismatch".into());
        }
        let owner = self.scope_handle(&context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let candidate = LibraryReservation {
            schema_version: 1,
            owner: AGENT_LIBRARY_PHYSICAL_STORE.into(),
            binding,
            source_version,
            heads: component_heads(&txn, &record)?,
            context,
            record,
        };
        if let Some(existing) = active(&txn)? {
            if existing != candidate {
                txn.abort()?;
                return Err("CONFLICT: library reservation already exists".into());
            }
            validate_inputs(&txn, &existing)?;
            txn.abort()?;
            return Ok(existing);
        }
        validate_inputs(&txn, &candidate)?;
        let batch = batch(self, &candidate, "prepare", source_version)?;
        match txn.begin(&batch)? {
            Begin::Replay(_) => return Err("library prepare already resolved".into()),
            Begin::Apply { source_version } => {
                finish(self, txn, &batch, Some(&candidate), source_version)?
            }
        }
        Ok(candidate)
    }

    pub(crate) fn pending_library_reservation(&self) -> Result<Option<LibraryReservation>, String> {
        let read = self.read()?;
        let table = read.open_owner_table(eg_storage::AGENT_LIBRARY_PARTICIPANTS)?;
        let value = table.get(ACTIVE).map_err(|e| e.to_string())?;
        decode(value.as_ref().map(|v| v.value()))
    }

    fn require_library_prepare(
        &self,
        record: &LibraryReservation,
        commit: bool,
    ) -> Result<(), String> {
        let owner = self.scope_handle(&record.context.tenant_id)?;
        let read = self.kernel.read_scope(&owner)?;
        let prepare = batch(self, record, "prepare", record.source_version)?;
        let receipt = eg_transaction::read_ledger(&read, &prepare.batch_id)?
            .ok_or("library resolution lacks durable prepare")?;
        if receipt.status != eg_types::mutation_batch::MutationBatchStatus::Committed
            || serde_json::to_vec(&receipt.batch).map_err(|e| e.to_string())?
                != serde_json::to_vec(&prepare).map_err(|e| e.to_string())?
            || receipt.result_msgpack.as_deref()
                != Some(
                    rmp_serde::to_vec_named(&Some(record))
                        .map_err(|e| e.to_string())?
                        .as_slice(),
                )
        {
            return Err("library prepare receipt differs from sealed intent".into());
        }
        let abort = batch(
            self,
            record,
            "abort",
            record
                .source_version
                .checked_add(1)
                .ok_or("library version overflow")?,
        )?;
        if commit && eg_transaction::read_ledger(&read, &abort.batch_id)?.is_some() {
            return Err("library reservation was already aborted".into());
        }
        Ok(())
    }

    pub(crate) fn resolve_library_reservation(
        &self,
        record: &LibraryReservation,
        source: &dyn LibraryReservationDecisionSource,
    ) -> Result<bool, String> {
        let Some(certificate) = source.terminal_decision(record)? else {
            return Ok(false);
        };
        // The trusted owner retains the original decision stamp. Recovery must
        // validate that stamp, never replace it with the current wall clock.
        certificate.matches(&record.binding, certificate.commit)?;
        let commit = certificate.commit;
        self.require_library_prepare(record, commit)?;
        if commit {
            let permit = ResolutionPermit { record };
            self.commit_reserved_decision(
                record.context.clone(),
                &record.record,
                &record.record.inputs.catalog_digest,
                Some(&permit),
            )?;
        } else {
            let owner = self.scope_handle(&record.context.tenant_id)?;
            let version = record
                .source_version
                .checked_add(1)
                .ok_or("library version overflow")?;
            let batch = batch(self, record, "abort", version)?;
            let (txn, begun) = self.mutations.admit(&owner, &batch)?;
            match begun {
                Begin::Replay(receipt) => {
                    if serde_json::to_vec(&receipt.batch).map_err(|e| e.to_string())?
                        != serde_json::to_vec(&batch).map_err(|e| e.to_string())?
                    {
                        return Err("library resolution replay differs from sealed intent".into());
                    }
                    txn.abort()?;
                }
                Begin::Apply { source_version } => {
                    if active(&txn)?.as_ref() != Some(record) {
                        return Err("library abort does not match prepare".into());
                    }
                    finish(self, txn, &batch, None, source_version)?;
                }
            }
        }
        Ok(true)
    }
}

#[cfg(all(test, feature = "decide"))]
mod tests;
