//! Admission, ordering, fencing and terminal metadata for one mutation batch.

use crate::ledger::{
    idempotency_batch_id, persist_idempotency, persist_record, read_record_in_write,
    remove_private, source_version, verify_replay_identity,
};
use crate::replay::remove_replay_rows;
use crate::tables::{
    BATCHES, FENCES, IDEMPOTENCY, OUTBOX, PRIVATE_PAYLOADS, REPLAY_NONCES, REPLAY_OPERATIONS,
    SCOPE_BINDINGS, VERSIONS,
};
use crate::admitted::AdmittedMutation;
use crate::Begin;
use eg_storage::{
    decode_batch_record, decode_ledger_record, encode_bounded, ledger_scope_key, OwnerDomain,
    ScopeFence,
};
use eg_types::mutation_batch::MutationCommitPhase;
use eg_types::{
    CommittedVersion, MutationBatch, MutationBatchRecord, MutationBatchStatus,
    MutationOutboxRecord, MutationScopeIdentity, VersionExpectation, MUTATION_BATCH_VERSION,
};
use redb::{ReadableTable, WriteTransaction};

/// Validate binding, exact idempotency, OCC, and route fencing before owner rows change.
pub(crate) fn begin<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    batch.validate_write_budget()?;
    write.verify_scope(&batch.identity)?;
    if let Some(batch_id) = idempotency_batch_id(write, batch)? {
        return replay_begin(write, batch, &batch_id);
    }
    if let Some(record) = read_record_in_write(write, &batch.identity, &batch.batch_id)? {
        verify_replay_identity(batch, &record.batch)?;
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: batch '{}' exists without its idempotency row",
            batch.batch_id
        ));
    }

    let source_version = source_version(write, batch)?;
    match (batch.version_expectation, source_version) {
        (
            VersionExpectation::Graph(expected) | VersionExpectation::Native(expected),
            Some(actual),
        ) if expected != actual => {
            return Err(format!(
                "STALE_VERSION: mutation scope expected version {expected} but authoritative version is {actual}"
            ));
        }
        (VersionExpectation::Unversioned, None) => {}
        (VersionExpectation::Graph(_) | VersionExpectation::Native(_), Some(_)) => {}
        _ => {
            return Err(
                "mutation version expectation has no bound authoritative version".to_string(),
            )
        }
    }

    reject_stale_fence(write.transaction(), batch)?;
    eg_types::mutation_batch::apply_certification_fault(batch, MutationCommitPhase::BeforeRows)?;
    write.admit_apply_batch(batch)?;
    Ok(Begin::Apply { source_version })
}

fn replay_begin<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    batch_id: &str,
) -> Result<Begin, String> {
    let record = read_record_in_write(write, &batch.identity, batch_id)?.ok_or_else(|| {
        format!("CORRUPT_MUTATION_LEDGER: idempotency key points to missing batch '{batch_id}'")
    })?;
    verify_replay_identity(batch, &record.batch)?;
    if record.status != MutationBatchStatus::Committed {
        return Err(format!(
            "mutation batch '{}' is not terminally committed",
            record.batch.batch_id
        ));
    }
    Ok(Begin::Replay(Box::new(record)))
}

fn reject_stale_fence(wtx: &WriteTransaction, batch: &MutationBatch) -> Result<(), String> {
    let identity_key = ledger_scope_key(&batch.identity);
    let table = wtx.open_table(FENCES).map_err(|error| error.to_string())?;
    let current = table
        .get(identity_key.as_str())
        .map_err(|error| error.to_string())?
        .map(|value| decode_ledger_record::<ScopeFence>(value.value()))
        .transpose()?;
    let Some(current) = current else {
        return Ok(());
    };
    if current.identity != batch.identity {
        return Err("mutation fence row is not stamped with this scope identity".to_string());
    }
    let proposed_token = batch.fencing_token.unwrap_or(0);
    if batch.placement_epoch < current.placement_epoch
        || (batch.placement_epoch == current.placement_epoch
            && proposed_token < current.fencing_token)
    {
        return Err(format!(
            "STALE_FENCE: proposed route ({},{}) is older than ({},{})",
            batch.placement_epoch, proposed_token, current.placement_epoch, current.fencing_token
        ));
    }
    Ok(())
}

/// Persist terminal metadata after owner rows changed in the same transaction.
pub(crate) fn finish<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    result_msgpack: Option<Vec<u8>>,
    committed_at_ms: u64,
    source_version: Option<u64>,
) -> Result<MutationBatchRecord, String> {
    write.verify_scope(&batch.identity)?;
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        MutationCommitPhase::AfterRowsBeforeMetadata,
    )?;
    let committed_version = committed_version(batch.version_expectation, source_version)?;
    let record = MutationBatchRecord {
        batch: batch.clone(),
        identity: batch.identity.clone(),
        status: MutationBatchStatus::Committed,
        committed_version,
        result_msgpack,
        committed_at_ms,
    };
    persist_record(write, &record)?;
    persist_idempotency(write, batch)?;
    write_version(write.transaction(), batch, committed_version)?;
    write_fence(write.transaction(), batch)?;
    write_outbox(write.transaction(), batch, committed_version)?;
    write.finish_batch_admission(batch)?;
    Ok(record)
}

fn committed_version(
    expectation: VersionExpectation,
    source_version: Option<u64>,
) -> Result<CommittedVersion, String> {
    match (expectation, source_version) {
        (VersionExpectation::Graph(expected), Some(source)) if expected == source => {
            CommittedVersion::checked_graph(source)
        }
        (VersionExpectation::Native(expected), Some(source)) if expected == source => {
            CommittedVersion::checked_native(source)
        }
        (VersionExpectation::Unversioned, None) => Ok(CommittedVersion::None),
        _ => Err("mutation finish source version does not match its expectation".to_string()),
    }
}

fn write_version(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
    committed: CommittedVersion,
) -> Result<(), String> {
    let Some(target) = committed.target() else {
        return Ok(());
    };
    let binding_key = batch.identity.binding_digest().to_hex();
    wtx.open_table(VERSIONS)
        .map_err(|error| error.to_string())?
        .insert(binding_key.as_str(), target)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_fence(wtx: &WriteTransaction, batch: &MutationBatch) -> Result<(), String> {
    let fence = ScopeFence {
        identity: batch.identity.clone(),
        placement_epoch: batch.placement_epoch,
        fencing_token: batch.fencing_token.unwrap_or(0),
    };
    let bytes = encode_bounded(&fence, "mutation fence")?;
    let identity_key = ledger_scope_key(&batch.identity);
    wtx.open_table(FENCES)
        .map_err(|error| error.to_string())?
        .insert(identity_key.as_str(), bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_outbox(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
    committed_version: CommittedVersion,
) -> Result<(), String> {
    let identity_key = ledger_scope_key(&batch.identity);
    let mut table = wtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    for (ordinal, intent) in batch.outbox.iter().enumerate() {
        let outbox = MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch.batch_id.clone(),
            ordinal: ordinal as u32,
            identity: batch.identity.clone(),
            committed_version,
            intent: intent.clone(),
            created_at_ms: batch.created_at_ms,
        };
        outbox.validate_write_budget()?;
        let bytes = encode_bounded(&outbox, "mutation outbox record")?;
        table
            .insert(
                (
                    identity_key.as_str(),
                    batch.batch_id.as_str(),
                    ordinal as u32,
                ),
                bytes.as_slice(),
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(crate) fn commit<D: OwnerDomain>(
    write: AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    write.verify_scope(&batch.identity)?;
    write.validate_commit_admission(batch)?;
    eg_types::mutation_batch::apply_certification_fault(batch, MutationCommitPhase::BeforeCommit)?;
    write.commit()?;
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        MutationCommitPhase::AfterCommitBeforeAck,
    )
}

/// Atomically remove authority for one exact logical generation.
pub(crate) fn purge_scope<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    write.verify_scope(identity)?;
    let identity_key = ledger_scope_key(identity);
    let batch_ids = collect_batch_ids(write.transaction(), identity, &identity_key)?;
    for batch_id in &batch_ids {
        remove_batch_rows(write.transaction(), &identity_key, batch_id)?;
    }
    remove_scope_rows(write.transaction(), &identity_key)
}

fn collect_batch_ids(
    wtx: &WriteTransaction,
    identity: &MutationScopeIdentity,
    identity_key: &str,
) -> Result<std::collections::BTreeSet<String>, String> {
    let table = wtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let mut ids = std::collections::BTreeSet::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_identity, batch_id) = key.value();
        if row_identity != identity_key {
            continue;
        }
        let record = decode_batch_record(value.value())?;
        if record.identity != *identity || record.batch.batch_id != batch_id {
            return Err("mutation batch key does not bind its exact identity".to_string());
        }
        ids.insert(batch_id.to_string());
    }
    Ok(ids)
}

fn remove_batch_rows(
    wtx: &WriteTransaction,
    identity_key: &str,
    batch_id: &str,
) -> Result<(), String> {
    wtx.open_table(BATCHES)
        .map_err(|error| error.to_string())?
        .remove((identity_key, batch_id))
        .map_err(|error| error.to_string())?;
    let mut outbox = wtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    let ordinals = outbox
        .range((identity_key, batch_id, 0)..=(identity_key, batch_id, u32::MAX))
        .map_err(|error| error.to_string())?
        .map(|row| {
            row.map(|(key, _)| key.value().2)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for ordinal in ordinals {
        outbox
            .remove((identity_key, batch_id, ordinal))
            .map_err(|error| error.to_string())?;
    }
    remove_private(wtx, identity_key, batch_id)
}

fn remove_scope_rows(wtx: &WriteTransaction, identity_key: &str) -> Result<(), String> {
    let mut idem = wtx
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    let keys = idem
        .iter()
        .map_err(|error| error.to_string())?
        .filter_map(|row| match row {
            Ok((key, _)) if key.value().0 == identity_key => Some(Ok(key.value().1.to_string())),
            Ok(_) => None,
            Err(error) => Some(Err(error.to_string())),
        })
        .collect::<Result<Vec<_>, String>>()?;
    for key in keys {
        idem.remove((identity_key, key.as_str()))
            .map_err(|error| error.to_string())?;
    }
    wtx.open_table(FENCES)
        .map_err(|error| error.to_string())?
        .remove(identity_key)
        .map_err(|error| error.to_string())?;
    remove_replay_rows(wtx, identity_key)?;
    wtx.open_table(VERSIONS)
        .map_err(|error| error.to_string())?
        .remove(identity_key)
        .map_err(|error| error.to_string())?;
    wtx.open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?
        .remove(identity_key)
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Open every ledger table this crate owns once, inside `write`.
///
/// The storage kernel creates the whole declared census atomically at
/// `create_owner`, so this never partially creates tables; it is the ledger's
/// own fail-closed proof that each declared table is present and typed exactly
/// as this crate declares it.
pub(crate) fn open_ledger_tables<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
) -> Result<(), String> {
    let wtx = write.transaction();
    wtx.open_table(BATCHES).map_err(|e| e.to_string())?;
    wtx.open_table(IDEMPOTENCY).map_err(|e| e.to_string())?;
    wtx.open_table(VERSIONS).map_err(|e| e.to_string())?;
    wtx.open_table(FENCES).map_err(|e| e.to_string())?;
    wtx.open_table(OUTBOX).map_err(|e| e.to_string())?;
    wtx.open_table(PRIVATE_PAYLOADS).map_err(|e| e.to_string())?;
    wtx.open_table(REPLAY_NONCES).map_err(|e| e.to_string())?;
    wtx.open_table(REPLAY_OPERATIONS).map_err(|e| e.to_string())?;
    Ok(())
}
