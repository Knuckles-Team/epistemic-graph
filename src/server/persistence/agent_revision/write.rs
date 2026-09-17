//! The shared admitted-write lifecycle: replay admission, the commit plan, owner
//! rows, the typed receipt, and the native commit.
//!
//! Every fallible step after the write is opened runs inside [`within_write`],
//! which aborts the write and returns the step's own error. The phase order is
//! the one each layer's copy had: admission and replay, the layer's `prepare`
//! step, then plan, rows, receipt, ledger, version check, and commit.

use std::sync::Arc;

use eg_storage::OwnedStoreHandle;
use eg_transaction::{AdmittedMutation, Begin, MutationKernel};
use eg_types::authority::{NonceReplayKey, OperationReplayIdentity};

use super::super::agent_library::{
    admitted_context, agent_library_operation_identity, batch_id,
    effective_agent_library_policy_digest, owner_receipt, resolve_nonce_first, OwnerReceiptInput,
};
use super::*;

type Write<'a> = AdmittedMutation<'a, Owner>;

/// Run `steps` against the open write. On failure the write is aborted and the
/// step's error is returned; an abort that itself fails reports its own error.
pub(in crate::server::persistence) fn within_write<'a, T>(
    txn: Write<'a>,
    steps: impl FnOnce(&Write<'a>) -> Result<T, String>,
) -> Result<(Write<'a>, T), String> {
    match steps(&txn) {
        Ok(value) => Ok((txn, value)),
        Err(error) => {
            txn.abort()?;
            Err(error)
        }
    }
}

/// The expected revision a write names and the tenant scope it is admitted
/// under, resolved before the write is opened.
///
/// `noun` names the layer in the refusal.
pub(in crate::server::persistence) fn revision_scope(
    store: &AgentLibraryStore,
    context: &AgentLibraryMutationContext,
    noun: &str,
) -> Result<(u64, Arc<OwnedStoreHandle<Owner>>), String> {
    let expected_revision = context
        .expected_revision
        .ok_or_else(|| format!("{noun} writes require an explicit expected_revision"))?;
    let owner = store.scope_handle(&context.tenant_id)?;
    Ok((expected_revision, owner))
}

pub(in crate::server::persistence) fn require_context_tenant<L: RevisionLayer>(
    context: &AgentLibraryMutationContext,
    tenant_id: &str,
) -> Result<(), String> {
    if context.tenant_id != tenant_id {
        return Err(format!(
            "{} publish context tenant does not match the {}'s tenant",
            L::NOUN,
            L::RECORD
        ));
    }
    Ok(())
}

/// One publish or retire as the caller asked for it.
pub(in crate::server::persistence) struct RevisionWrite<'r, L: RevisionLayer> {
    pub(in crate::server::persistence) context: &'r AgentLibraryMutationContext,
    pub(in crate::server::persistence) kind: L::Kind,
    pub(in crate::server::persistence) record_id: &'r str,
    pub(in crate::server::persistence) expected_revision: u64,
    /// The submitted definition's digest; `None` for a retire.
    pub(in crate::server::persistence) definition_digest: Option<&'r str>,
}

struct Admission<L: RevisionLayer> {
    nonce: NonceReplayKey,
    next_revision: u64,
    replay_context: AgentLibraryMutationContext,
    operation: OperationReplayIdentity,
    replayed: Option<(L::WriteResult, MutationReceipt)>,
}

fn admit_revision<L: RevisionLayer>(
    mutations: &MutationKernel,
    txn: &Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    write: &RevisionWrite<'_, L>,
) -> Result<Admission<L>, String> {
    let nonce = resolve_nonce_first(mutations, txn, write.context)?;
    let next_revision = next_revision(write.expected_revision)?;
    let verb = L::verb(write.kind).as_str();
    let replay_context = admitted_context(write.context, &format!("{}:{verb}", L::SLUG))?;
    let operation = agent_library_operation_identity(
        owner,
        &replay_context,
        &format!("{}-{verb}", L::OPERATION),
        write.record_id,
        write.expected_revision,
        write.definition_digest,
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    let replayed = replayed_revision::<L>(replay, write.record_id)?;
    Ok(Admission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replayed,
    })
}

/// Admit one revision write and commit the entry `prepare` builds for it.
///
/// `prepare` receives the next revision number and the admitted context, and
/// runs only when the attempt is not a replay.
pub(in crate::server::persistence) fn write_revision<'a, L: RevisionLayer>(
    store: &AgentLibraryStore,
    txn: Write<'a>,
    owner: &OwnedStoreHandle<Owner>,
    tables: RevisionTables,
    write: RevisionWrite<'_, L>,
    prepare: impl FnOnce(&Write<'a>, u64, &AgentLibraryMutationContext) -> Result<L::Entry, String>,
) -> Result<L::WriteResult, String> {
    let (txn, admission) = within_write(txn, |txn| {
        admit_revision::<L>(&store.mutations, txn, owner, &write)
    })?;
    let Admission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replayed,
    } = admission;
    if let Some((result, receipt)) = replayed {
        return finish_replayed(&store.mutations, txn, &operation, &nonce, result, receipt);
    }
    let (txn, entry) = within_write(txn, |txn| prepare(txn, next_revision, &replay_context))?;
    commit_revision_in_write::<L>(
        store,
        txn,
        owner,
        tables,
        RevisionCommit {
            context: &replay_context,
            expected_revision: write.expected_revision,
            kind: write.kind,
            entry,
            operation: &operation,
            nonce: &nonce,
        },
    )
}

/// Retire the record's `expected_revision` with a durable tombstone revision.
pub(in crate::server::persistence) fn retire_revision<L: RevisionLayer>(
    store: &AgentLibraryStore,
    txn: Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    tables: RevisionTables,
    context: &AgentLibraryMutationContext,
    record_id: &str,
    expected_revision: u64,
) -> Result<L::WriteResult, String> {
    write_revision::<L>(
        store,
        txn,
        owner,
        tables,
        RevisionWrite {
            context,
            kind: L::RETIRE,
            record_id,
            expected_revision,
            definition_digest: None,
        },
        |txn, next_revision, replay_context| {
            let current = revision_at_in_write::<L>(
                txn,
                tables,
                &context.tenant_id,
                record_id,
                expected_revision,
            )?
            .ok_or_else(|| format!("{} has no revision to retire", L::NOUN))?;
            L::retired_revision(&current, next_revision, replay_context.created_at_ms)
        },
    )
}

/// Finish a replayed attempt: consume its fresh nonce against the recorded
/// receipt, then commit that alone.
pub(in crate::server::persistence) fn finish_replayed<T>(
    mutations: &MutationKernel,
    txn: Write<'_>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
    result: T,
    receipt: MutationReceipt,
) -> Result<T, String> {
    if let Err(error) = mutations.finalize_replay_receipt(&txn, operation, nonce, &receipt) {
        txn.abort()?;
        return Err(error);
    }
    mutations.commit_replay_receipt(txn)?;
    Ok(result)
}

struct RevisionCommit<'r, L: RevisionLayer> {
    context: &'r AgentLibraryMutationContext,
    expected_revision: u64,
    kind: L::Kind,
    entry: L::Entry,
    operation: &'r OperationReplayIdentity,
    nonce: &'r NonceReplayKey,
}

struct CommitPlan<L: RevisionLayer> {
    admitted_context: AgentLibraryMutationContext,
    expected_revision: u64,
    entry: L::Entry,
    entry_bytes: Vec<u8>,
    event_bytes: Vec<u8>,
    staged: StagedBatch,
    stable_result: L::Committed,
    result_bytes: Vec<u8>,
}

fn commit_revision_in_write<L: RevisionLayer>(
    store: &AgentLibraryStore,
    txn: Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    tables: RevisionTables,
    commit: RevisionCommit<'_, L>,
) -> Result<L::WriteResult, String> {
    let (operation, nonce) = (commit.operation, commit.nonce);
    let (txn, plan) = within_write(txn, |txn| {
        let plan = build_commit_plan::<L>(store, txn, owner, commit)?;
        apply_owner_rows(txn, owner, &plan.staged.batch, |owner_write| {
            apply_revision_rows::<L>(
                owner_write,
                tables,
                &plan.admitted_context,
                plan.expected_revision,
                &plan.entry,
                plan.entry_bytes.as_slice(),
            )
        })?;
        let receipt = build_receipt::<L>(&plan, operation, nonce)?;
        finish_committed_ledger(
            &store.mutations,
            txn,
            (&plan.staged, &plan.result_bytes),
            plan.admitted_context.created_at_ms,
            (operation, nonce, &receipt),
            L::NOUN,
        )?;
        Ok(plan)
    })?;
    store.mutations.commit(txn, &plan.staged.batch)?;
    Ok(L::write_result(plan.stable_result, false))
}

fn build_commit_plan<L: RevisionLayer>(
    store: &AgentLibraryStore,
    txn: &Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    commit: RevisionCommit<'_, L>,
) -> Result<CommitPlan<L>, String> {
    let RevisionCommit {
        context,
        expected_revision,
        kind,
        entry,
        operation,
        nonce,
    } = commit;
    let admitted_context =
        policy_admitted_context(context, &revision_operations::<L>(kind, &entry))?;
    let event_bytes = L::encode_outbox_event(kind, &entry, &admitted_context)?;
    let entry_bytes = eg_storage::encode_bounded(&entry, &format!("{} revision", L::NOUN))?;
    let (batch_id, staged) = stage_batch(
        store,
        txn,
        owner,
        &admitted_context,
        (operation, nonce),
        L::NOUN,
        |version, batch_id| {
            build_revision_batch::<L>(
                owner,
                &admitted_context,
                kind,
                &entry,
                version,
                batch_id,
                event_bytes.clone(),
            )
        },
    )?;
    let stable_result = L::committed(entry.clone(), batch_id, staged.committed_version);
    let result_bytes = encode_domain_result::<L>(&stable_result)?;
    Ok(CommitPlan {
        admitted_context,
        expected_revision,
        entry,
        entry_bytes,
        event_bytes,
        staged,
        stable_result,
        result_bytes,
    })
}

/// Stamp the effective policy digest of `operations` on the admitted context.
pub(in crate::server::persistence) fn policy_admitted_context(
    context: &AgentLibraryMutationContext,
    operations: &[MutationOperation],
) -> Result<AgentLibraryMutationContext, String> {
    let policy_digest = effective_agent_library_policy_digest(operations)?;
    let mut admitted_context = context.clone();
    admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
    Ok(admitted_context)
}

/// A batch begun under its replay identity, and the versions it commits over.
pub(in crate::server::persistence) struct StagedBatch {
    pub(in crate::server::persistence) batch: MutationBatch,
    pub(in crate::server::persistence) source_version: u64,
    pub(in crate::server::persistence) committed_version: u64,
}

/// Build the batch against the owner's authoritative version and begin it.
///
/// Returns the batch id beside the staged batch. `build` receives the
/// authoritative version and the batch id.
pub(in crate::server::persistence) fn stage_batch(
    store: &AgentLibraryStore,
    txn: &Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    admitted_context: &AgentLibraryMutationContext,
    replay: (&OperationReplayIdentity, &NonceReplayKey),
    noun: &str,
    build: impl FnOnce(u64, &str) -> Result<MutationBatch, String>,
) -> Result<(String, StagedBatch), String> {
    let batch_id = batch_id(&admitted_context.idempotency_key)?;
    let authoritative_version = store.mutations.current_version(txn, owner)?;
    let batch = build(authoritative_version, &batch_id)?;
    let source_version = begin_revision_commit(txn, &batch, replay, authoritative_version, noun)?;
    let committed_version = source_version
        .checked_add(1)
        .ok_or_else(|| format!("{noun} committed version overflow"))?;
    Ok((
        batch_id,
        StagedBatch {
            batch,
            source_version,
            committed_version,
        },
    ))
}

/// Begin the admitted batch under its replay identity and return the native
/// source version it applies over.
fn begin_revision_commit(
    txn: &Write<'_>,
    batch: &MutationBatch,
    replay: (&OperationReplayIdentity, &NonceReplayKey),
    authoritative_version: u64,
    noun: &str,
) -> Result<u64, String> {
    let begun = txn.begin_with_replay_identity(batch, replay.0, replay.1)?;
    let source_version = match begun {
        Begin::Apply {
            source_version: Some(source_version),
        } => source_version,
        Begin::Apply {
            source_version: None,
        } => return Err(format!("{noun} admission has no native source version")),
        Begin::Replay(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                    .to_string(),
            );
        }
    };
    if source_version != authoritative_version {
        return Err(format!(
            "{noun} source version changed while admitting write"
        ));
    }
    Ok(source_version)
}

fn build_receipt<L: RevisionLayer>(
    plan: &CommitPlan<L>,
    operation: &OperationReplayIdentity,
    nonce: &NonceReplayKey,
) -> Result<MutationReceipt, String> {
    let key = revision_key(&L::revision_definition(&plan.entry));
    let headers = revision_outbox_headers::<L>(&plan.entry);
    owner_receipt(
        operation,
        nonce,
        &plan.staged.batch,
        OwnerReceiptInput {
            slug: L::SLUG,
            topic: L::TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: domain_result::<L>(&plan.stable_result)?,
            committed_version: plan.staged.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

/// Write one layer's owner rows through the batch's owner-row capability.
pub(in crate::server::persistence) fn apply_owner_rows(
    txn: &Write<'_>,
    owner: &OwnedStoreHandle<Owner>,
    batch: &MutationBatch,
    apply: impl FnOnce(&AdmittedOwnerWrite<'_, Owner>) -> Result<(), String>,
) -> Result<(), String> {
    let owner_write = txn.owner_rows(owner, batch)?;
    apply(&owner_write)?;
    owner_write.finish_owner()
}

/// Finish the ledger half of one commit with its typed receipt, and refuse a
/// record whose committed version is not the one the result reports.
///
/// `committed` is the staged batch and the encoded domain result it records.
pub(in crate::server::persistence) fn finish_committed_ledger(
    mutations: &MutationKernel,
    txn: &Write<'_>,
    committed: (&StagedBatch, &[u8]),
    committed_at_ms: u64,
    replay: (&OperationReplayIdentity, &NonceReplayKey, &MutationReceipt),
    noun: &str,
) -> Result<(), String> {
    let (staged, result_bytes) = committed;
    let record = mutations.finish_with_replay(
        txn,
        &staged.batch,
        Some(result_bytes.to_vec()),
        committed_at_ms,
        Some(staged.source_version),
        replay,
    )?;
    let recorded_version = record
        .committed_version
        .target()
        .ok_or_else(|| format!("{noun} commit has no target version"))?;
    if recorded_version != staged.committed_version {
        return Err(format!(
            "{noun} result version differs from the committed version"
        ));
    }
    Ok(())
}
