//! Two-phase saga preparation and commit over one ledger scope.

use crate::admitted::AdmittedMutation;
use crate::commit::{begin, commit, finish, write_class};
use crate::ledger::{
    idempotency_batch_id, persist_idempotency, persist_private, persist_record,
    read_private_in_write, read_record_in_write, remove_private, source_version,
    verify_replay_identity,
};
use crate::{Begin, SagaBegin};
use eg_storage::{
    ledger_scope_key, MutationClass, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain,
};
use eg_types::{
    CommittedVersion, MutationBatch, MutationBatchRecord, MutationBatchStatus,
};

pub(crate) fn prepare_saga<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    batch: &MutationBatch,
    prepared_at_ms: u64,
) -> Result<SagaBegin, String> {
    prepare_saga_with_private_payload(authority, owner, batch, prepared_at_ms, None)
}

pub(crate) fn prepare_saga_with_private_payload<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    batch: &MutationBatch,
    prepared_at_ms: u64,
    private_payload: Option<&[u8]>,
) -> Result<SagaBegin, String> {
    batch.validate_write_budget()?;
    let write = AdmittedMutation::open(authority, owner)?;
    write.verify_scope(&batch.identity)?;
    if let Some(existing_id) = idempotency_batch_id(&write, batch)? {
        let result = resume_existing_saga(&write, batch, private_payload, &existing_id)?;
        write.abort()?;
        return Ok(result);
    }
    match begin(&write, batch, MutationClass::Operation)? {
        Begin::Replay(record) => {
            write.abort()?;
            return Ok(SagaBegin::Committed(*record));
        }
        Begin::Apply { .. } => {}
    }
    persist_prepared_saga(&write, batch, prepared_at_ms, private_payload)?;
    write.commit()?;
    Ok(SagaBegin::Execute)
}

fn resume_existing_saga<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    private_payload: Option<&[u8]>,
    existing_id: &str,
) -> Result<SagaBegin, String> {
    let record = read_record_in_write(write, &batch.identity, existing_id)?.ok_or_else(|| {
        format!(
            "CORRUPT_MUTATION_LEDGER: saga idempotency key points to missing batch '{existing_id}'"
        )
    })?;
    verify_replay_identity(batch, &record.batch)?;
    ensure_private_payload(write, private_payload, &record)?;
    match record.status {
        MutationBatchStatus::Prepared => Ok(SagaBegin::Resume(record)),
        MutationBatchStatus::Committed => Ok(SagaBegin::Committed(record)),
        MutationBatchStatus::Aborted => {
            Err(format!("mutation saga '{}' was aborted", batch.batch_id))
        }
    }
}

fn ensure_private_payload<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    private_payload: Option<&[u8]>,
    record: &MutationBatchRecord,
) -> Result<(), String> {
    if private_payload.is_some()
        && record.status == MutationBatchStatus::Prepared
        && read_private_in_write(write, record)?.is_none()
    {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: prepared saga '{}' has no authenticated private recovery payload",
            record.batch.batch_id
        ));
    }
    Ok(())
}

fn persist_prepared_saga<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    prepared_at_ms: u64,
    private_payload: Option<&[u8]>,
) -> Result<(), String> {
    let record = MutationBatchRecord {
        batch: batch.clone(),
        identity: batch.identity.clone(),
        status: MutationBatchStatus::Prepared,
        committed_version: CommittedVersion::None,
        result_msgpack: None,
        committed_at_ms: prepared_at_ms,
    };
    persist_record(write, &record)?;
    persist_idempotency(write, batch)?;
    write_class(write, batch)?;
    if let Some(payload) = private_payload {
        persist_private(write, &record, payload)?;
    }
    Ok(())
}

pub(crate) fn commit_saga<D: OwnerDomain>(
    authority: &MutationOwnerAuthority,
    owner: &OwnedStoreHandle<D>,
    batch: &MutationBatch,
    result_msgpack: Vec<u8>,
    committed_at_ms: u64,
) -> Result<(MutationBatchRecord, bool), String> {
    batch.validate_write_budget()?;
    let write = AdmittedMutation::open(authority, owner)?;
    write.verify_scope(&batch.identity)?;
    let record = read_record_in_write(&write, &batch.identity, &batch.batch_id)?
        .ok_or_else(|| format!("mutation saga '{}' was not prepared", batch.batch_id))?;
    verify_replay_identity(batch, &record.batch)?;
    if record.status == MutationBatchStatus::Committed {
        // Documented exception to "every owner write is an admitted mutation":
        // the saga already committed, and this commit only drops the sealed
        // private recovery payload that its terminal receipt made redundant.
        // It writes no owner row, produces no receipt and bumps no version --
        // deleting recovery state for an already-terminal batch is cleanup, not
        // a mutation, and re-running it is a no-op.
        let identity_key = ledger_scope_key(&batch.identity);
        remove_private(&write, &identity_key, &batch.batch_id)?;
        write.commit()?;
        return Ok((record, true));
    }
    if record.status != MutationBatchStatus::Prepared {
        return Err(format!(
            "mutation saga '{}' is not resumable",
            batch.batch_id
        ));
    }
    write.admit_prepared_batch(batch)?;
    let source_version = source_version(&write, batch)?;
    let committed = finish(
        &write,
        batch,
        Some(result_msgpack),
        committed_at_ms,
        source_version,
    )?;
    let identity_key = ledger_scope_key(&batch.identity);
    remove_private(&write, &identity_key, &batch.batch_id)?;
    commit(write, batch)?;
    Ok((committed, false))
}
