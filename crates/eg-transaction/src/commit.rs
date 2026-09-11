//! Admission, ordering, fencing and terminal metadata for one mutation batch.

use crate::admitted::AdmittedMutation;
use crate::group::AdmittedGroup;
use crate::ledger::{
    maintenance_claim, persist_maintenance, persist_record, read_record_in_write, source_version,
};
use crate::replay::{
    finalize_replay_in, record_operation_in, record_replay_in, resolve_replay_in, ReplayResolution,
};
use crate::tables::{visit_ledger_tables, BATCHES, CLASSES, FENCES, OUTBOX, VERSIONS};
use crate::Begin;
use eg_storage::{
    decode_batch_record, decode_ledger_record, decode_outbox_record, encode_bounded,
    ledger_scope_key, owner_table_names, CollectionBudget, MutationClass, MutationClassRow,
    OwnerDomain, OwnerPayloadRetirement, RecordedOperation, ScopeFence,
};
use eg_types::mutation_batch::MutationCommitPhase;
use eg_types::mutation_batch::{MutationEnvelope, OperationEnvelope};
use eg_types::{
    CommittedVersion, MutationBatch, MutationBatchRecord, MutationBatchStatus,
    MutationOutboxRecord, MutationScopeIdentity, VersionExpectation, MUTATION_BATCH_VERSION,
};

/// The certification fault points, compiled only under `fault-injection`.
///
/// `eg_types::mutation_batch::apply_certification_fault` aborts the process at
/// an exact commit phase so the crash matrix can be proved. That is a test
/// instrument, not release behaviour, so the call is feature-gated here rather
/// than shipped on the commit path.
#[cfg(feature = "fault-injection")]
fn certification_fault(batch: &MutationBatch, phase: MutationCommitPhase) -> Result<(), String> {
    eg_types::mutation_batch::apply_certification_fault(batch, phase)
}

#[cfg(not(feature = "fault-injection"))]
fn certification_fault(_batch: &MutationBatch, _phase: MutationCommitPhase) -> Result<(), String> {
    Ok(())
}

/// Greatest batch id in redb's byte order, for a scope-bounded range scan.
pub(crate) const MAX_BATCH_ID_SENTINEL: &str = "\u{10FFFF}";

/// Validate binding, replay identity, OCC and route fencing before owner rows
/// change.
///
/// The batch's own [`MutationEnvelope`] chooses the arm: a caller operation is
/// resolved against the two replay identities RF-RULING-004 defines, and an
/// owner-maintenance write against its first-wins claim key. The envelope is
/// the structural truth; no caller-supplied class can override it.
pub(crate) fn begin<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    begin_inner(write, batch, false)
}

/// Admit one of the kernel's reserved graft batches.
///
/// The marker and destination reservation use ordinary durable maintenance
/// rows, but their deterministic namespace must not be forgeable through the
/// public operation or maintenance admission APIs.  The graft module is the
/// only caller of this crate-private path.
pub(crate) fn begin_graft<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    begin_inner(write, batch, true)
}

fn begin_inner<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    allow_reserved_graft: bool,
) -> Result<Begin, String> {
    if !allow_reserved_graft && is_reserved_graft_batch(batch) {
        return Err("reserved graft batch namespace is kernel-owned".to_string());
    }
    batch.validate_write_budget()?;
    write.verify_scope(&batch.identity)?;
    match &batch.envelope {
        MutationEnvelope::Operation(envelope) => begin_operation(write, batch, envelope),
        MutationEnvelope::Maintenance(_) => begin_maintenance(write, batch),
    }
}

/// Resolve one caller operation against the two replay identities.
///
/// The whole-batch byte comparison this replaces compared fourteen fields it had
/// no business comparing -- `created_at_ms`, the OCC expectation, the route, the
/// request and trace ids -- every one of which a second attempt of the same
/// operation legitimately re-observes. This compares the seventeen fields
/// `OperationReplayIdentity::digest` enumerates, exactly, and tolerates nothing:
/// what changed is not the strictness but what is inside the compared value.
fn begin_operation<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    envelope: &OperationEnvelope,
) -> Result<Begin, String> {
    let operation = envelope.operation_identity()?;
    let nonce = envelope.nonce_replay_key()?;
    begin_operation_with_identity(write, batch, &operation, &nonce)
}

/// Admit a domain batch with a stable replay identity reconstructed before its
/// retained rows were read. The physical batch remains fully validated; the
/// explicit identity may differ only in its canonical payload digest because
/// retry-stable domain intent can include retained metadata in final outbox bytes.
pub(crate) fn begin_with_replay_identity<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<Begin, String> {
    batch.validate_write_budget()?;
    write.verify_scope(&batch.identity)?;
    let envelope = batch
        .envelope
        .operation()
        .ok_or_else(|| "stable replay identity requires an operation batch".to_string())?;
    let batch_operation = envelope.operation_identity()?;
    let batch_nonce = envelope.nonce_replay_key()?;
    let mut comparable = batch_operation.clone();
    comparable.canonical_payload_digest = operation.canonical_payload_digest;
    if comparable != *operation || batch_nonce != *nonce {
        return Err(
            "stable replay identity does not bind the batch authority or attempt nonce".to_string(),
        );
    }
    begin_operation_with_identity(write, batch, operation, nonce)
}

fn begin_operation_with_identity<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<Begin, String> {
    match resolve_replay_in(write, operation, nonce)? {
        // A duplicated attempt nonce inside one transaction is a caller bug, not
        // a coalescing artifact, so it refuses the whole write -- and therefore
        // the whole scope group, whose members share that one transaction.
        ReplayResolution::NonceRejected { idempotency_key } => Err(format!(
            "REPLAY_NONCE_CONSUMED: this attempt nonce was already consumed by idempotency key \
             '{idempotency_key}'"
        )),
        ReplayResolution::Conflict { recorded, proposed } => Err(format!(
            "IDEMPOTENCY_CONFLICT: key '{}' was already used by a different operation \
             (recorded {recorded}, proposed {proposed})",
            operation.idempotency_key.as_str()
        )),
        ReplayResolution::ReplayedResult(recorded) => replay_recorded(write, batch, &recorded),
        ReplayResolution::Fresh => admit_fresh(write, batch),
    }
}

/// Resolve one owner-maintenance write against its first-wins claim key.
///
/// It has no caller, so it has no operation identity to compare and no attempt
/// nonce to consume (RF-RULING-005). It is still a full mutation: ledgered,
/// fenced, class-rowed and version-bumping.
fn begin_maintenance<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    match maintenance_claim(write, batch)? {
        Some(batch_id) => replay_committed_batch(write, batch, &batch_id),
        None => admit_fresh(write, batch),
    }
}

/// The recorded result of an operation this key already committed.
fn replay_recorded<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    recorded: &RecordedOperation,
) -> Result<Begin, String> {
    let batch_id = recorded.batch_id().ok_or_else(|| {
        "mutation batch replays an idempotency key recorded by a receipt-only operation".to_string()
    })?;
    replay_committed_batch(write, batch, batch_id)
}

fn replay_committed_batch<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    batch_id: &str,
) -> Result<Begin, String> {
    let record = read_record_in_write(write, &batch.identity, batch_id)?.ok_or_else(|| {
        format!("CORRUPT_MUTATION_LEDGER: idempotency key points to missing batch '{batch_id}'")
    })?;
    if record.status != MutationBatchStatus::Committed {
        return Err(format!(
            "mutation batch '{}' is not terminally committed",
            record.batch.batch_id
        ));
    }
    write.remember_replayed_batch(batch, &record)?;
    Ok(Begin::Replay(Box::new(record)))
}

pub(crate) fn read_replay_evidence_in<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch_id: &str,
) -> Result<
    Option<(
        MutationBatchRecord,
        MutationClass,
        Vec<MutationOutboxRecord>,
    )>,
    String,
> {
    let Some(record) = read_record_in_write(write, write.scope(), batch_id)? else {
        return Ok(None);
    };
    let scope_key = ledger_scope_key(write.scope());
    let class = write
        .scoped_table(CLASSES)?
        .get((scope_key.as_str(), batch_id))?
        .map(|value| decode_ledger_record::<MutationClassRow>(value.value()))
        .transpose()?
        .ok_or_else(|| "CORRUPT_MUTATION_LEDGER: replay batch has no class row".to_string())?;
    if class.identity != *write.scope() || class.batch_id != batch_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: replay batch class row is bound to a different scope"
                .to_string(),
        );
    }
    let table = write.scoped_table(OUTBOX)?;
    let mut outbox = Vec::new();
    let mut budget = CollectionBudget::default();
    for row in table.range_inclusive(
        (scope_key.as_str(), batch_id, 0),
        (scope_key.as_str(), batch_id, u32::MAX),
    )? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        budget.account(value.value().len())?;
        let (key_scope, key_batch_id, key_ordinal) = key.value();
        let record = decode_outbox_record(value.value())?;
        if key_scope != scope_key.as_str()
            || key_batch_id != batch_id
            || key_ordinal != record.ordinal
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay outbox row does not match its physical key"
                    .to_string(),
            );
        }
        outbox.push(record);
    }
    Ok(Some((record, class.class, outbox)))
}

/// OCC, route fencing and admission for a batch that has not committed before.
fn admit_fresh<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<Begin, String> {
    if let Some(record) = read_record_in_write(write, &batch.identity, &batch.batch_id)? {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: batch '{}' exists without its replay row (status {:?})",
            batch.batch_id, record.status
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
    certification_fault(batch, MutationCommitPhase::BeforeRows)?;
    write.admit_apply_batch(batch)?;
    Ok(Begin::Apply { source_version })
}

fn is_reserved_graft_batch(batch: &MutationBatch) -> bool {
    [batch.batch_id.as_str(), batch.idempotency_key()]
        .into_iter()
        .any(|key| key == "kernel.graft" || key.starts_with("kernel.graft/"))
}
fn reject_stale_fence<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    let identity_key = ledger_scope_key(&batch.identity);
    let table = write.scoped_table(FENCES)?;
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
    // A graft marker uses the maximum route fence as a durable write barrier.
    // Equality with that sentinel is not an ordinary route match: allowing a
    // caller to submit the same maximum pair would let a write land between
    // the source snapshot and phase-C retirement.
    if current.placement_epoch == crate::graft::GRAFT_FENCE
        && current.fencing_token == crate::graft::GRAFT_FENCE
    {
        return Err("STALE_FENCE: scope is under graft".to_string());
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
    finish_inner(
        write,
        batch,
        result_msgpack,
        committed_at_ms,
        source_version,
        None,
    )
}

/// Finish a batch while recording a typed authority receipt as its replay row.
/// The typed receipt and owner rows share this same write transaction.
pub(crate) fn finish_with_replay<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    result_msgpack: Option<Vec<u8>>,
    committed_at_ms: u64,
    source_version: Option<u64>,
    // The typed authority receipt filed atomically as this batch's replay
    // row: the operation identity it authenticates, the nonce it consumes,
    // and the receipt itself. These three are never meaningful apart, so
    // they travel as one group rather than as three positional parameters.
    replay_receipt: (
        &eg_types::authority::OperationReplayIdentity,
        &eg_types::authority::NonceReplayKey,
        &eg_types::mutation::MutationReceipt,
    ),
) -> Result<MutationBatchRecord, String> {
    let (operation, nonce, receipt) = replay_receipt;
    receipt.validate()?;
    let receipt_result = encode_bounded(&receipt.result, "mutation receipt result")?;
    if result_msgpack.as_deref() != Some(receipt_result.as_slice()) {
        return Err(
            "typed replay receipt result must equal the batch's durable result payload".to_string(),
        );
    }
    finish_inner(
        write,
        batch,
        result_msgpack,
        committed_at_ms,
        source_version,
        Some((operation, nonce, receipt)),
    )
}

fn finish_inner<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    result_msgpack: Option<Vec<u8>>,
    committed_at_ms: u64,
    source_version: Option<u64>,
    replay: Option<(
        &eg_types::authority::OperationReplayIdentity,
        &eg_types::authority::NonceReplayKey,
        &eg_types::mutation::MutationReceipt,
    )>,
) -> Result<MutationBatchRecord, String> {
    write.verify_scope(&batch.identity)?;
    // This guard must run before any durable row is written, including the
    // typed replay row, so a replayed member cannot overwrite its evidence.
    write.validate_finish_admission(batch)?;
    certification_fault(batch, MutationCommitPhase::AfterRowsBeforeMetadata)?;
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
    if let Some((operation, nonce, receipt)) = replay {
        record_replay_in(write, operation, nonce, receipt)?;
    } else {
        persist_key_row(write, batch)?;
    }
    write_class(write, batch)?;
    write_version(write, batch, committed_version)?;
    write_fence(write, batch)?;
    write_outbox(
        write,
        batch,
        committed_version,
        record.result_msgpack.as_deref(),
    )?;
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
    // Sourced from the batch's own envelope, not from what the admission call
    // asserted: the envelope is what makes the class structural, and a class row
    // derived from the admission argument could disagree with the batch it
    // labels.
    let class = if batch.is_maintenance() {
        MutationClass::Maintenance
    } else {
        MutationClass::Operation
    };
    let row = MutationClassRow {
        identity: batch.identity.clone(),
        batch_id: batch.batch_id.clone(),
        class,
    };
    let bytes = encode_bounded(&row, "mutation class row")?;
    let identity_key = ledger_scope_key(&batch.identity);
    write.scoped_table(CLASSES)?.insert(
        (identity_key.as_str(), batch.batch_id.as_str()),
        bytes.as_slice(),
    )
}

/// Write the ONE durable key row this batch's class owns.
///
/// An operation's key row is its `mutation_replay_operations` row, which also
/// consumes the attempt nonce, so the effect and its replay evidence commit
/// atomically. A maintenance write's is its `ledger_maintenance` first-wins
/// claim. The retired `ledger_idempotency` table wrote a third row that answered
/// the same question for both, which is exactly the second replay authority
/// RF-ADR-001 forbids.
fn persist_key_row<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    match &batch.envelope {
        MutationEnvelope::Operation(envelope) => record_operation_in(
            write,
            &envelope.operation_identity()?,
            &envelope.nonce_replay_key()?,
            RecordedOperation::Batch(batch.batch_id.clone()),
        ),
        MutationEnvelope::Maintenance(_) => persist_maintenance(write, batch),
    }
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
    let mut versions = write.scoped_table(VERSIONS)?;
    let current = versions
        .get(binding_key.as_str())?
        .map(|value| value.value())
        .ok_or_else(|| "mutation scope binding is missing its authoritative version".to_string())?;
    let next = current
        .checked_add(1)
        .ok_or_else(|| "mutation scope version overflow".to_string())?;
    if committed.target().is_some_and(|target| target != next) {
        return Err("mutation committed version does not advance its scope by one".to_string());
    }
    versions.insert(binding_key.as_str(), next)
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
        .scoped_table(FENCES)?
        .insert(identity_key.as_str(), bytes.as_slice())
}

fn write_outbox<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
    committed_version: CommittedVersion,
    result_msgpack: Option<&[u8]>,
) -> Result<(), String> {
    let terminal_batch = batch
        .outbox
        .iter()
        .any(|intent| intent.topic == eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC);
    // Carry each intent's ordinal IN THE PARENT BATCH, not its position in the
    // filtered list. Recovery validates an outbox row by looking the intent up
    // at `parent.batch.outbox[ordinal]` (`eg-storage::recovery::validate`), so
    // re-indexing over the filtered subset silently breaks that binding the
    // moment anything is filtered out -- a terminal batch carrying a
    // non-run-event intent, or a run-event whose status does not apply. The
    // store then commits fine and FAILS TO REOPEN with "mutation outbox row is
    // not exactly bound to its parent".
    //
    // Sparse ordinals are safe: the ordinal is a key component and a cursor
    // position, never a dense counter -- every consumer looks a row up by
    // `(scope, batch_id, ordinal)` or orders by the topic index, and none
    // iterates `0..n`.
    let mut effective_outbox = Vec::with_capacity(batch.outbox.len());
    for (ordinal, intent) in batch.outbox.iter().enumerate() {
        if terminal_batch && intent.topic != eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC {
            continue;
        }
        if terminal_outbox_intent_applies(intent, result_msgpack)? {
            let ordinal = u32::try_from(ordinal)
                .map_err(|_| "mutation outbox ordinal exceeds the supported range".to_string())?;
            effective_outbox.push((ordinal, intent));
        }
    }
    let identity_key = ledger_scope_key(&batch.identity);
    let commit_sequence = write
        .scoped_table(VERSIONS)?
        .get(identity_key.as_str())?
        .map(|value| value.value())
        .ok_or_else(|| "outbox row has no authoritative commit sequence".to_string())?;
    // Establish readiness before adding this commit's rows. A fresh scope
    // gets its durable complete marker here; a legacy scope with pre-existing
    // rows and no marker stays unverified until its bounded backfill proves
    // the whole primary range. Marking after insertion would mistake those
    // legacy rows for a fully indexed stream.
    if !effective_outbox.is_empty() {
        crate::outbox::mark_index_ready_in_write(write, &batch.identity)?;
    }
    {
        let mut table = write.scoped_table(OUTBOX)?;
        for (ordinal, intent) in effective_outbox {
            let outbox = MutationOutboxRecord {
                schema_version: MUTATION_BATCH_VERSION,
                batch_id: batch.batch_id.clone(),
                ordinal,
                identity: batch.identity.clone(),
                committed_version,
                commit_sequence: Some(commit_sequence),
                intent: intent.clone(),
                created_at_ms: batch.created_at_ms,
            };
            outbox.validate_write_budget()?;
            let bytes = encode_bounded(&outbox, "mutation outbox record")?;
            table.insert(
                (identity_key.as_str(), batch.batch_id.as_str(), ordinal),
                bytes.as_slice(),
            )?;
            // The commit-ordered topic index over this row, written in the same
            // transaction (`crate::outbox::index`). `ledger_outbox` sorts by batch
            // id, which is not commit order, so a consumer that resumed from a
            // position in it would skip a later batch whose id sorts earlier.
            crate::outbox::index_outbox_row(write, &outbox)?;
        }
    }
    Ok(())
}

fn terminal_outbox_intent_applies(
    intent: &eg_types::mutation_batch::MutationOutboxIntent,
    result_msgpack: Option<&[u8]>,
) -> Result<bool, String> {
    if intent.topic != eg_types::outcome_bundle::RUN_EVENT_OUTBOX_TOPIC {
        return Ok(true);
    }
    let Some(result_msgpack) = result_msgpack else {
        return Err("terminal run-event outbox requires a native WorkItem result".to_string());
    };
    let payload = rmp_serde::from_slice::<eg_types::protocol::ResultPayload>(result_msgpack)
        .map_err(|_| "terminal WorkItem result is not a decodable payload".to_string())?;
    let eg_types::protocol::ResultPayload::Json(value) = payload else {
        return Err("terminal WorkItem result does not carry a status object".to_string());
    };
    match value.get("status").and_then(|status| status.as_str()) {
        Some("missing" | "noop" | "fenced" | "retry_scheduled") => Ok(false),
        Some("succeeded" | "failed" | "cancelled" | "dead_letter") => Ok(true),
        _ => Err("terminal WorkItem result has an unknown status".to_string()),
    }
}

pub(crate) fn commit<D: OwnerDomain>(
    write: AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    seal(&write, batch)?;
    write.commit()?;
    certification_fault(batch, MutationCommitPhase::AfterCommitBeforeAck)
}

/// Everything one admitted write must prove before its transaction commits:
/// it still serves the batch's scope, its admission reached a finished state,
/// and the pre-commit certification fault point has passed.
///
/// Split out of [`commit`] rather than duplicated so a scope group's members
/// are sealed by exactly the code that seals a sole writer.
fn seal<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    batch: &MutationBatch,
) -> Result<(), String> {
    write.verify_scope(&batch.identity)?;
    write.validate_commit_admission(batch)?;
    if let Some(record) = write.replayed_record(batch)? {
        if !batch.is_maintenance() {
            finalize_replay_in(write, batch, &record)?;
        }
    }
    certification_fault(batch, MutationCommitPhase::BeforeCommit)
}

/// Seal every member of a scope group and commit the one transaction they
/// share (RF-RULING-008).
///
/// `batches` is positional: `batches[i]` is the batch member `i` was admitted
/// with, control member first. A member that fails to seal aborts the whole
/// group — its transaction is the others' transaction, so there is no partial
/// outcome to choose.
pub(crate) fn commit_group<D: OwnerDomain>(
    group: AdmittedGroup<'_, D>,
    batches: &[&MutationBatch],
) -> Result<(), String> {
    if batches.len() != group.len() {
        // End the shared transaction on a named path rather than leaving it to
        // `Drop`, exactly as the seal-failure path below does.
        let _ = group.end(false);
        return Err("scope group commit does not name every admitted member".to_string());
    }
    for (index, batch) in batches.iter().enumerate() {
        if let Err(error) = seal(group.member(index)?, batch) {
            group.end(false)?;
            return Err(error);
        }
    }
    group.end(true)?;
    for batch in batches {
        certification_fault(batch, MutationCommitPhase::AfterCommitBeforeAck)?;
    }
    Ok(())
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
    owner_payload: Option<&dyn OwnerPayloadRetirement<D>>,
) -> Result<(), String> {
    purge_scope_inner(write, identity, owner_payload, false)
}

/// Retire a source after the graft phases have proved its marker and version.
/// The source necessarily still carries the graft fence at this point, so this
/// is the one internal caller allowed to cross that fence.
pub(crate) fn purge_scope_for_graft<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    owner_payload: Option<&dyn OwnerPayloadRetirement<D>>,
) -> Result<(), String> {
    purge_scope_inner(write, identity, owner_payload, true)
}

fn purge_scope_inner<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    owner_payload: Option<&dyn OwnerPayloadRetirement<D>>,
    allow_graft_fence: bool,
) -> Result<(), String> {
    write.verify_scope(identity)?;
    let graft_fenced = is_graft_fenced(write, identity)?;
    if allow_graft_fence {
        if !graft_fenced {
            return Err("GRAFT_FENCE_MISSING: source is not fenced for retirement".to_string());
        }
    } else if graft_fenced {
        return Err("STALE_FENCE: scope is under graft".to_string());
    }
    // Owner keys carry no scope component in general, so the kernel cannot
    // sweep the domain payload itself. Retiring the authority while leaving the
    // rows behind would hand the retired generation's payload to the next
    // binding of the same logical name, so a layout that owns tables must
    // supply their retirement or the purge is refused.
    match (owner_payload, owner_table_names(D::LAYOUT).is_empty()) {
        (None, true) => {}
        (None, false) => {
            return Err(
                "scope retirement requires an owner-payload retirement for this layout".to_string(),
            )
        }
        (Some(retirement), _) => {
            retirement.retire_owner_payload(write.capability(), identity)?;
        }
    }
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

fn is_graft_fenced<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
) -> Result<bool, String> {
    let scope = ledger_scope_key(identity);
    let table = write.scoped_table(FENCES)?;
    let fence = table
        .get(scope.as_str())?
        .map(|value| decode_ledger_record::<ScopeFence>(value.value()))
        .transpose()?;
    let Some(fence) = fence else {
        return Ok(false);
    };
    if fence.identity != *identity {
        return Err("mutation fence row is not stamped with this scope identity".to_string());
    }
    Ok(fence.placement_epoch == u64::MAX && fence.fencing_token == u64::MAX)
}

/// Every batch row under this scope key must bind its own exact identity
/// before the scope is retired, so a misfiled row is refused rather than
/// silently swept.
fn validate_batch_keys<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    identity: &MutationScopeIdentity,
    identity_key: &str,
) -> Result<(), String> {
    let table = write.scoped_table(BATCHES)?;
    let rows = table.range_inclusive((identity_key, ""), (identity_key, MAX_BATCH_ID_SENTINEL))?;
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
            write.scoped_table($table)?;
        }};
    }
    visit_ledger_tables!(open);
    Ok(())
}
