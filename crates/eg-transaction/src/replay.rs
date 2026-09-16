//! Two-identity mutation replay (RF-RULING-004).
//!
//! Replay has two identities and never one overloaded context digest.
//! [`OperationReplayIdentity`] is the stable operation identity over tenant,
//! actor, authority scope and purpose, method, canonical payload digest, policy
//! digest, and idempotency key; it excludes nonce, request/trace IDs and
//! timestamps. [`NonceReplayKey`] is the attempt-specific nonce identity.
//! `AuthorityContext::context_digest` includes the nonce and is therefore
//! attempt-specific, so it can never decide operation replay.
//!
//! The four outcomes are exactly:
//!
//! * the same nonce is rejected ([`ReplayResolution::NonceRejected`]);
//! * a fresh nonce plus the exact stable operation identity replays the prior
//!   result ([`ReplayResolution::ReplayedResult`]);
//! * a changed payload, scope, policy or method under the same idempotency key
//!   conflicts ([`ReplayResolution::Conflict`]);
//! * anything else is fresh ([`ReplayResolution::Fresh`]).

use crate::admitted::AdmittedMutation;
use crate::ledger::read_record_in_write;
use crate::tables::{REPLAY_NONCES, REPLAY_OPERATIONS};
use eg_storage::{
    decode_ledger_record, encode_bounded, ledger_scope_key, OperationReplayRow, OwnerDomain,
    RecordedOperation,
};
use eg_types::authority::{NonceReplayKey, OperationReplayIdentity};
use eg_types::contract::Digest256;
use eg_types::mutation::MutationReceipt;
use eg_types::{MutationBatch, MutationBatchRecord, MutationBatchStatus};

/// The one durable replay decision for one proposed attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayResolution {
    /// No nonce row and no operation row: the attempt may execute.
    Fresh,
    /// This exact attempt nonce was already consumed under `idempotency_key`.
    NonceRejected { idempotency_key: String },
    /// A fresh nonce over a byte-identical stable operation identity: the
    /// recorded result is returned instead of executing again.
    ReplayedResult(Box<RecordedOperation>),
    /// The same idempotency key was already used by a different stable
    /// operation identity -- a changed method, payload, scope, or policy.
    Conflict {
        recorded: Digest256,
        proposed: Digest256,
    },
}

pub(crate) fn resolve_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> Result<ReplayResolution, String> {
    let scope_key = ledger_scope_key(write.scope());
    let proposed = operation.digest()?;
    let nonce_digest = nonce.digest()?.to_hex();
    if let Some(idempotency_key) = read_nonce(write, &scope_key, &nonce_digest)? {
        return Ok(ReplayResolution::NonceRejected { idempotency_key });
    }
    let Some(row) = read_operation(write, &scope_key, operation.idempotency_key.as_str())? else {
        return Ok(ReplayResolution::Fresh);
    };
    if row.operation_replay_digest != proposed {
        return Ok(ReplayResolution::Conflict {
            recorded: row.operation_replay_digest,
            proposed,
        });
    }
    Ok(ReplayResolution::ReplayedResult(Box::new(row.recorded)))
}

/// Resolve only the attempt nonce in an already-open owner write.
///
/// Domain owners that must reconstruct a stable operation body from retained
/// rows need this check before opening those rows. It is deliberately read-only
/// and does not inspect the operation key: the caller must still invoke
/// [`resolve_replay_in`] with the complete operation identity before effects.
pub(crate) fn resolve_nonce_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    nonce: &NonceReplayKey,
) -> Result<Option<String>, String> {
    let scope_key = ledger_scope_key(write.scope());
    let nonce_digest = nonce.digest()?.to_hex();
    read_nonce(write, &scope_key, &nonce_digest)
}

/// Consume the attempt nonce for a successful batch replay.
///
/// [`resolve_replay_in`] deliberately only answers the replay question: a
/// caller may still abort the admitted transaction after observing that answer.
/// A successful retry therefore has one explicit finalization point. It proves
/// the returned terminal record and the operation row again inside the same
/// write transaction, then adds only the fresh nonce mapping. No batch,
/// class, version, fence or outbox row is written, so replay has no owner
/// effect and cannot advance the scope merely because its response was reused.
pub(crate) fn finalize_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    replay: &MutationBatchRecord,
) -> Result<(), String> {
    validate_replay_batch(write, batch, replay)?;

    let envelope = batch
        .envelope
        .operation()
        .ok_or_else(|| "mutation batch has no operation replay envelope".to_string())?;
    let operation = envelope.operation_identity()?;
    let nonce = envelope.nonce_replay_key()?;
    let proposed_digest = operation.digest()?;
    let scope_key = ledger_scope_key(write.scope());
    let idempotency_key = operation.idempotency_key.as_str();
    let operation_row = read_operation(write, &scope_key, idempotency_key)?.ok_or_else(|| {
        format!(
            "CORRUPT_MUTATION_LEDGER: replay finalization has no operation row for key '{idempotency_key}'"
        )
    })?;
    validate_batch_operation(
        &operation_row,
        batch,
        replay,
        idempotency_key,
        proposed_digest,
    )?;

    validate_durable_replay_batch(write, batch, replay)?;

    let nonce_digest = nonce.digest()?.to_hex();
    if read_nonce(write, &scope_key, &nonce_digest)?.is_some() {
        return Err(format!(
            "REPLAY_NONCE_CONSUMED: this attempt nonce was already recorded for idempotency key '{idempotency_key}'"
        ));
    }
    write
        .scoped_table(REPLAY_NONCES)?
        .insert((scope_key.as_str(), nonce_digest.as_str()), idempotency_key)
}

pub(crate) fn finalize_replay_receipt_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
    receipt: &MutationReceipt,
) -> Result<(), String> {
    receipt.validate()?;
    let operation_digest = operation.digest()?;
    let nonce_digest = nonce.digest()?;
    if receipt.operation_replay_digest != operation_digest
        || receipt.scope != operation.authority_scope
    {
        return Err("replay receipt does not bind the proposed operation".to_string());
    }
    let scope_key = ledger_scope_key(write.scope());
    let row = read_operation(write, &scope_key, operation.idempotency_key.as_str())?
        .ok_or_else(|| "CORRUPT_MUTATION_LEDGER: replay receipt row is missing".to_string())?;
    validate_receipt_batch(write, &row, receipt)?;
    validate_receipt_operation(write, &row, operation, operation_digest, receipt)?;
    let nonce_key = nonce_digest.to_hex();
    if read_nonce(write, &scope_key, &nonce_key)?.is_some() {
        return Err(format!(
            "REPLAY_NONCE_CONSUMED: this attempt nonce was already recorded for idempotency key '{}'",
            operation.idempotency_key.as_str()
        ));
    }
    write.scoped_table(REPLAY_NONCES)?.insert(
        (scope_key.as_str(), nonce_key.as_str()),
        operation.idempotency_key.as_str(),
    )
}

/// Consume one attempt nonce and record what this operation produced, inside the
/// same admitted write as the effect, so a crash can never leave an effect
/// without its record or a consumed nonce without its operation row.
///
/// Fails closed on **any** pre-existing row for this idempotency key that does
/// not record this exact operation, not only on a differing digest. An identical
/// digest recording a DIFFERENT result means the caller resolved `Fresh` against
/// a stale view and re-executed an operation it should have replayed; letting it
/// through would apply the effect twice and overwrite the recorded result with
/// the second one. Because resolution and recording now share one write
/// transaction, redb's write serialization makes the loser of two concurrent
/// attempts fail here rather than commit.
///
/// The one exception is re-recording the SAME `(operation, batch)` pair, which
/// is how a saga's `commit` finishes a batch its `prepare` already claimed: the
/// row is unchanged, so the write is a no-op rather than a second claim.
pub(crate) fn record_operation_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
    recorded: RecordedOperation,
) -> Result<(), String> {
    let operation_replay_digest = operation.digest()?;
    let nonce_replay_digest = nonce.digest()?;
    let scope_key = ledger_scope_key(write.scope());
    let idempotency_key = operation.idempotency_key.as_str();
    let nonce_digest = nonce_replay_digest.to_hex();
    let batch_id = write.admitted_batch_id()?;
    let existing = read_operation(write, &scope_key, idempotency_key)?;
    if existing.as_ref().is_some_and(|existing| {
        is_same_saga_batch(
            existing,
            &recorded,
            operation_replay_digest,
            nonce_replay_digest,
            &batch_id,
        )
    }) {
        return Ok(());
    }
    if read_nonce(write, &scope_key, &nonce_digest)?.is_some() {
        return Err("REPLAY_NONCE_CONSUMED: this attempt nonce was already recorded".to_string());
    }
    if let Some(existing) = existing {
        return Err(operation_record_conflict(
            &existing,
            operation_replay_digest,
        ));
    }

    let row = OperationReplayRow {
        identity: write.scope().clone(),
        idempotency_key: idempotency_key.to_string(),
        operation_replay_digest,
        nonce_replay_digest,
        batch_id,
        recorded,
    };
    let bytes = encode_bounded(&row, "mutation replay operation row")?;
    write
        .scoped_table(REPLAY_OPERATIONS)?
        .insert((scope_key.as_str(), idempotency_key), bytes.as_slice())?;
    write
        .scoped_table(REPLAY_NONCES)?
        .insert((scope_key.as_str(), nonce_digest.as_str()), idempotency_key)
}

/// Record one kernel-level [`MutationReceipt`] as this operation's result.
///
/// The receipt-bearing entry point of [`record_operation_in`]: it additionally
/// proves the receipt binds both replay digests and names this operation's own
/// scope, which a batch record proves structurally instead.
pub(crate) fn record_replay_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
    receipt: &MutationReceipt,
) -> Result<(), String> {
    if write.admitted_batch_is_maintenance()? {
        return Err(
            "MAINTENANCE_HAS_NO_REPLAY_IDENTITY: an owner-maintenance write carries no caller \
             operation identity and cannot consume an attempt nonce"
                .to_string(),
        );
    }
    receipt.validate()?;
    if receipt.operation_replay_digest != operation.digest()?
        || receipt.nonce_replay_digest != nonce.digest()?
    {
        return Err("mutation receipt does not bind both replay digests".to_string());
    }
    if receipt.scope != operation.authority_scope {
        return Err("mutation receipt names a different scope than its operation".to_string());
    }
    record_operation_in(
        write,
        operation,
        nonce,
        RecordedOperation::Receipt(Box::new(receipt.clone())),
    )
}

fn validate_replay_batch<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    replay: &MutationBatchRecord,
) -> Result<(), String> {
    if batch.is_maintenance() {
        return Err(
            "MAINTENANCE_HAS_NO_REPLAY_IDENTITY: maintenance writes cannot finalize a replay"
                .to_string(),
        );
    }
    write.verify_scope(&batch.identity)?;
    batch.validate_write_budget()?;
    replay.validate_write_budget()?;
    if replay.status != MutationBatchStatus::Committed
        || replay.identity != batch.identity
        || replay.batch.identity != batch.identity
    {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: replay receipt is not a committed record for this scope"
                .to_string(),
        );
    }

    Ok(())
}

fn validate_batch_operation(
    operation_row: &OperationReplayRow,
    batch: &MutationBatch,
    replay: &MutationBatchRecord,
    idempotency_key: &str,
    proposed_digest: Digest256,
) -> Result<(), String> {
    if operation_row.identity != batch.identity
        || operation_row.idempotency_key != idempotency_key
        || operation_row.operation_replay_digest != proposed_digest
        || operation_row.recorded.batch_id() != Some(replay.batch.batch_id.as_str())
    {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: operation row does not name this replay"
                .to_string(),
        );
    }

    Ok(())
}

fn validate_durable_replay_batch<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    replay: &MutationBatchRecord,
) -> Result<(), String> {
    let durable = read_record_in_write(write, &batch.identity, &replay.batch.batch_id)?
        .ok_or_else(|| {
            format!(
                "CORRUPT_MUTATION_LEDGER: replay operation points to missing batch '{}'",
                replay.batch.batch_id
            )
        })?;
    let durable_batch = encode_bounded(&durable.batch, "replay batch")?;
    let replay_batch = encode_bounded(&replay.batch, "replay batch")?;
    if durable_batch != replay_batch
        || durable.identity != replay.identity
        || durable.status != replay.status
        || durable.committed_version != replay.committed_version
        || durable.result_msgpack != replay.result_msgpack
        || durable.committed_at_ms != replay.committed_at_ms
    {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: supplied replay receipt differs from durable record"
                .to_string(),
        );
    }

    Ok(())
}

fn validate_receipt_batch<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    row: &OperationReplayRow,
    receipt: &MutationReceipt,
) -> Result<(), String> {
    if row.batch_id.is_empty() {
        return Err("CORRUPT_MUTATION_LEDGER: replay receipt row has no owner batch".to_string());
    }
    let durable_batch =
        read_record_in_write(write, write.scope(), &row.batch_id)?.ok_or_else(|| {
            format!(
                "CORRUPT_MUTATION_LEDGER: replay receipt row points to missing batch '{}'",
                row.batch_id
            )
        })?;
    if durable_batch.status != MutationBatchStatus::Committed
        || durable_batch.identity != *write.scope()
        || durable_batch.batch.batch_id != row.batch_id
    {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: replay receipt row is not bound to a committed batch"
                .to_string(),
        );
    }
    let result_bytes = encode_bounded(&receipt.result, "mutation receipt result")?;
    if durable_batch.result_msgpack.as_deref() != Some(result_bytes.as_slice()) {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: replay receipt result differs from durable batch"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_receipt_operation<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    row: &OperationReplayRow,
    operation: &OperationReplayIdentity,
    operation_digest: Digest256,
    receipt: &MutationReceipt,
) -> Result<(), String> {
    if row.identity != *write.scope()
        || row.idempotency_key != operation.idempotency_key.as_str()
        || row.operation_replay_digest != operation_digest
        || row.nonce_replay_digest != receipt.nonce_replay_digest
        || row.recorded != RecordedOperation::Receipt(Box::new(receipt.clone()))
    {
        return Err(
            "REPLAY_FINALIZATION_RECORD_MISMATCH: typed replay receipt differs from durable row"
                .to_string(),
        );
    }
    Ok(())
}

fn is_same_saga_batch(
    existing: &OperationReplayRow,
    recorded: &RecordedOperation,
    operation_replay_digest: Digest256,
    nonce_replay_digest: Digest256,
    batch_id: &str,
) -> bool {
    // The SAME attempt of the SAME operation, re-recording the SAME BATCH:
    // the row is already exactly what this call would write, so writing it
    // again is a no-op rather than a second claim. This is how a saga's
    // commit finishes a batch its prepare already claimed, and it is the
    // only caller that records one row twice by construction.
    //
    // Deliberately NOT extended to a receipt: `record_replay_in` is the
    // authority-context path, whose accepted contract is that it fails
    // closed on ANY pre-existing row for the key, because an identical
    // digest there means the caller resolved `Fresh` against a stale view
    // and re-executed an operation it should have replayed. The nonce must
    // match too -- a different attempt is a different attempt either way.
    matches!(recorded, RecordedOperation::Batch(_))
        && existing.operation_replay_digest == operation_replay_digest
        && existing.nonce_replay_digest == nonce_replay_digest
        && (existing.batch_id.is_empty() || existing.batch_id == batch_id)
        && &existing.recorded == recorded
}

fn operation_record_conflict(
    existing: &OperationReplayRow,
    operation_replay_digest: Digest256,
) -> String {
    if existing.operation_replay_digest == operation_replay_digest {
        "REPLAY_ALREADY_RECORDED: this operation was recorded and must be replayed, not re-executed"
            .to_string()
    } else {
        "IDEMPOTENCY_CONFLICT: key was already used by a different operation".to_string()
    }
}

fn read_nonce<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope_key: &str,
    nonce_digest: &str,
) -> Result<Option<String>, String> {
    let table = write.scoped_table(REPLAY_NONCES)?;
    let found = table
        .get((scope_key, nonce_digest))?
        .map(|value| value.value().to_string());
    Ok(found)
}

fn read_operation<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope_key: &str,
    idempotency_key: &str,
) -> Result<Option<OperationReplayRow>, String> {
    let table = write.scoped_table(REPLAY_OPERATIONS)?;
    let row = table
        .get((scope_key, idempotency_key))?
        .map(|value| decode_ledger_record::<OperationReplayRow>(value.value()))
        .transpose()?;
    Ok(row)
}
