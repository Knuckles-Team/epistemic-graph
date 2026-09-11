//! Two-phase saga preparation and commit over one ledger scope.

use crate::admitted::AdmittedMutation;
use crate::commit::{begin, commit, finish, write_class};
use crate::ledger::{
    persist_private, persist_record, read_private_in_write, read_record_in_write, remove_private,
    source_version,
};
use crate::replay::{record_operation_in, resolve_replay_in, ReplayResolution};
use crate::{Begin, SagaBegin};
use eg_storage::{
    ledger_scope_key, MutationOwnerAuthority, OwnedStoreHandle, OwnerDomain, RecordedOperation,
};
use eg_types::mutation_batch::MutationEnvelope;
use eg_types::{CommittedVersion, MutationBatch, MutationBatchRecord, MutationBatchStatus};

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
    if let Some(existing_id) = recorded_saga_batch_id(&write, batch)? {
        let result = resume_existing_saga(&write, batch, private_payload, &existing_id)?;
        match &result {
            SagaBegin::Committed(_) => match begin(&write, batch)? {
                Begin::Replay(_) => commit(write, batch)?,
                Begin::Apply { .. } => {
                    return Err(
                        "committed saga replay became a fresh admission unexpectedly".to_string(),
                    )
                }
            },
            SagaBegin::Resume(_) => write.abort()?,
            SagaBegin::Execute => {
                return Err("prepared saga unexpectedly returned Execute".to_string())
            }
        }
        return Ok(result);
    }
    match begin(&write, batch)? {
        Begin::Replay(record) => {
            commit(write, batch)?;
            return Ok(SagaBegin::Committed(*record));
        }
        Begin::Apply { .. } => {}
    }
    persist_prepared_saga(&write, batch, prepared_at_ms, private_payload)?;
    write.commit()?;
    Ok(SagaBegin::Execute)
}

/// The batch id this saga's own operation identity already recorded, if any.
///
/// A saga claims its idempotency key at PREPARE, not at commit, so a resume
/// reaches this before `begin` -- with a fresh attempt nonce over the identical
/// stable operation identity, which is exactly the `ReplayedResult` case. A
/// genuinely different operation reusing the key conflicts here by name rather
/// than through a whole-batch byte comparison that a legitimate retry could
/// never satisfy.
fn recorded_saga_batch_id<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Option<String>, String> {
    let MutationEnvelope::Operation(envelope) = &batch.envelope else {
        return Err("a saga is a caller operation and cannot be a maintenance write".to_string());
    };
    let operation = envelope.operation_identity()?;
    let nonce = envelope.nonce_replay_key()?;
    match resolve_replay_in(write, &operation, &nonce)? {
        ReplayResolution::Fresh => Ok(None),
        ReplayResolution::ReplayedResult(recorded) => Ok(recorded.batch_id().map(str::to_string)),
        ReplayResolution::NonceRejected { idempotency_key } => Err(format!(
            "REPLAY_NONCE_CONSUMED: this saga attempt nonce was already consumed by idempotency \
             key '{idempotency_key}'"
        )),
        ReplayResolution::Conflict { recorded, proposed } => Err(format!(
            "IDEMPOTENCY_CONFLICT: saga key '{}' was already used by a different operation \
             (recorded {recorded}, proposed {proposed})",
            operation.idempotency_key.as_str()
        )),
    }
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
    claim_saga_key(write, batch)?;
    write_class(write, batch)?;
    if let Some(payload) = private_payload {
        persist_private(write, &record, payload)?;
    }
    Ok(())
}

/// Claim the saga's idempotency key -- and consume its attempt nonce -- in the
/// same transaction that persists the `Prepared` receipt.
///
/// The commit that follows re-records the SAME `(operation, batch)` pair, which
/// `record_operation_in` treats as a no-op rather than a second claim, so the
/// prepared and committed halves of one saga never race each other for the key.
fn claim_saga_key<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    let MutationEnvelope::Operation(envelope) = &batch.envelope else {
        return Err("a saga is a caller operation and cannot be a maintenance write".to_string());
    };
    record_operation_in(
        write,
        &envelope.operation_identity()?,
        &envelope.nonce_replay_key()?,
        RecordedOperation::Batch(batch.batch_id.clone()),
    )
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
