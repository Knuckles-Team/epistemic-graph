//! Admission, ordering, fencing and terminal metadata for one mutation batch.

use crate::ledger::{
    idempotency_batch_id, persist_idempotency, persist_record, read_record_in_write,
    source_version, verify_replay_identity,
};
use crate::admitted::AdmittedMutation;
use crate::tables::{visit_ledger_tables, BATCHES, CLASSES, FENCES, OUTBOX, VERSIONS};
use crate::Begin;
use eg_storage::{
    decode_batch_record, decode_ledger_record, encode_bounded, ledger_scope_key, MutationClass,
    MutationClassRow, OwnerDomain, ScopeFence,
};
use eg_types::mutation_batch::MutationCommitPhase;
use eg_types::{
    CommittedVersion, MutationBatch, MutationBatchRecord, MutationBatchStatus,
    MutationOutboxRecord, MutationScopeIdentity, VersionExpectation, MUTATION_BATCH_VERSION,
};
use redb::ReadableTable;

/// Greatest batch id in redb's byte order, for a scope-bounded range scan.
pub(crate) const MAX_BATCH_ID_SENTINEL: &str = "\u{10FFFF}";

/// Validate binding, exact idempotency, OCC, and route fencing before owner rows change.
pub(crate) fn begin<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    class: MutationClass,
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

    reject_stale_fence(write, batch)?;
    eg_types::mutation_batch::apply_certification_fault(batch, MutationCommitPhase::BeforeRows)?;
    write.admit_apply_batch(batch, class)?;
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

fn reject_stale_fence<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    let identity_key = ledger_scope_key(&batch.identity);
    let table = write.open_table(FENCES)?;
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
    write_class(write, batch)?;
    write_version(write, batch, committed_version)?;
    write_fence(write, batch)?;
    write_outbox(write, batch, committed_version)?;
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

/// Label the batch with the class it was admitted under. Exactly one class row
/// exists per receipt, so a maintenance write is explicit in the ledger rather
/// than inferred from missing replay evidence.
pub(crate) fn write_class<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    let class = write.admitted_class()?;
    let row = MutationClassRow {
        identity: batch.identity.clone(),
        batch_id: batch.batch_id.clone(),
        class,
    };
    let bytes = encode_bounded(&row, "mutation class row")?;
    let identity_key = ledger_scope_key(&batch.identity);
    write
        .open_table(CLASSES)?
        .insert(
            (identity_key.as_str(), batch.batch_id.as_str()),
            bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Advance the scope's authoritative version by exactly one.
///
/// Every admitted batch bumps it, operation or maintenance: RF-RULING-005 makes
/// an owner-maintenance write a real mutation, and a mutation that leaves the
/// version untouched is invisible to any reader doing OCC. An unversioned batch
/// therefore still advances the counter; only a *versioned* batch additionally
/// has to agree with the value its `VersionExpectation` implies.
fn write_version<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    committed: CommittedVersion,
) -> Result<(), String> {
    let binding_key = ledger_scope_key(&batch.identity);
    let mut versions = write.open_table(VERSIONS)?;
    let current = versions
        .get(binding_key.as_str())
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .ok_or_else(|| "mutation scope binding is missing its authoritative version".to_string())?;
    let next = current
        .checked_add(1)
        .ok_or_else(|| "mutation scope version overflow".to_string())?;
    if committed.target().is_some_and(|target| target != next) {
        return Err("mutation committed version does not advance its scope by one".to_string());
    }
    versions
        .insert(binding_key.as_str(), next)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_fence<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    let fence = ScopeFence {
        identity: batch.identity.clone(),
        placement_epoch: batch.placement_epoch,
        fencing_token: batch.fencing_token.unwrap_or(0),
    };
    let bytes = encode_bounded(&fence, "mutation fence")?;
    let identity_key = ledger_scope_key(&batch.identity);
    write
        .open_table(FENCES)?
        .insert(identity_key.as_str(), bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_outbox<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    committed_version: CommittedVersion,
) -> Result<(), String> {
    let identity_key = ledger_scope_key(&batch.identity);
    let mut table = write.open_table(OUTBOX)?;
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
///
/// Every scoped ledger table of the authoritative list is swept, so a table
/// this kernel declares but does not yet write (the six outbox-delivery
/// tables) cannot leave rows behind for the next generation. The scope binding
/// and its version row are physical identity, so the storage kernel retires
/// them inside the same transaction.
pub(crate) fn purge_scope<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    write.verify_scope(identity)?;
    let identity_key = ledger_scope_key(identity);
    validate_batch_keys(write, identity, &identity_key)?;
    macro_rules! purge {
        ($table:expr) => {{
            write.purge_scoped_rows($table)?;
        }};
    }
    visit_ledger_tables!(purge);
    write.retire_scope_binding()
}

/// Every batch row under this scope key must bind its own exact identity
/// before the scope is retired, so a misfiled row is refused rather than
/// silently swept.
fn validate_batch_keys<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    identity_key: &str,
) -> Result<(), String> {
    let table = write.open_table(BATCHES)?;
    let rows = table
        .range((identity_key, "")..=(identity_key, MAX_BATCH_ID_SENTINEL))
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_identity, batch_id) = key.value();
        if row_identity != identity_key {
            return Err("mutation batch range escaped its scope prefix".to_string());
        }
        let record = decode_batch_record(value.value())?;
        if record.identity != *identity || record.batch.batch_id != batch_id {
            return Err("mutation batch key does not bind its exact identity".to_string());
        }
    }
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
    macro_rules! open {
        ($table:expr) => {{
            write.open_table($table)?;
        }};
    }
    visit_ledger_tables!(open);
    Ok(())
}
