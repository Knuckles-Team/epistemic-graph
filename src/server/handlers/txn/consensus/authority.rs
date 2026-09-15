//! Replicated transaction parent authority and final decisions.

use super::*;

#[cfg(feature = "raft")]
pub(super) fn validate_consensus_parent_authority(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    coordinator_id: &str,
    authority: &crate::raft::RaftMutationContext,
) -> Result<crate::server::handlers::admin::AdminSaga, String> {
    let parent = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        coordinator_id,
        Some(&authority.principal_fingerprint),
    )?
    .ok_or_else(|| "consensus transaction participant has no durable parent".to_string())?;
    validate_transaction_receipt_tenant(&parent.batch.batch_id, &authority.tenant_scope)?;
    transaction_plan_digest(&parent.batch)?;
    Ok(parent)
}

#[cfg(feature = "raft")]
pub(super) fn validate_consensus_participant_authority(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    coordinator_id: &str,
    txn: &GraphTxnState,
    authority: &crate::raft::RaftMutationContext,
    phase: crate::raft::TransactionParticipantPhase,
) -> Result<ParticipantAdmission, String> {
    if txn.tenant_scope != authority.tenant_scope {
        return Err(
            "consensus transaction participant does not match caller tenant scope".to_string(),
        );
    }
    let parent = validate_consensus_parent_authority(redb, coordinator_id, authority)?;
    if transaction_plan_digest(&parent.batch)? != txn.replay_intent_digest()? {
        return Err("consensus participant plan does not match parent intent".to_string());
    }
    participant_phase_admission(redb, &parent, phase)
}

/// Terminal success authorizes only a proven committed child's replay. It is
/// never permission to recreate an intent or perform another graph mutation.
#[cfg(feature = "raft")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ParticipantAdmission {
    Execute,
    ReplayOnly,
}

#[cfg(feature = "raft")]
pub(super) fn participant_phase_admission(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    parent: &crate::server::handlers::admin::AdminSaga,
    phase: crate::raft::TransactionParticipantPhase,
) -> Result<ParticipantAdmission, String> {
    use crate::raft::TransactionParticipantPhase::{Abort, Commit, Prepare};
    match (parent.prepared, parent.replayed.as_ref()) {
        (false, Some(ResultPayload::Bool(false))) if matches!(phase, Abort) => {
            return Ok(ParticipantAdmission::Execute)
        }
        (false, Some(ResultPayload::Bool(true))) if !matches!(phase, Abort) => {
            return Ok(ParticipantAdmission::ReplayOnly)
        }
        (true, None) => {}
        _ => {
            return Err(
                "consensus participant phase conflicts with terminal parent outcome".to_string(),
            )
        }
    }
    let decision = redb.xshard_decision_get(&parent.batch.batch_id)?;
    match (phase, decision) {
        (Prepare, None) | (Commit, Some(true)) | (Abort, None | Some(false)) => {
            Ok(ParticipantAdmission::Execute)
        }
        (Prepare, Some(true)) => Ok(ParticipantAdmission::ReplayOnly),
        _ => Err("consensus participant phase has no matching durable decision".to_string()),
    }
}

/// The parent lookup above binds the full intent. The derived child id binds
/// that immutable parent, graph and participant; verify the actual stored child
/// before using it as proof of a terminal replay.
#[cfg(feature = "raft")]
pub(super) async fn committed_participant_receipt(
    backend: &dyn crate::server::persistence::PersistenceBackend,
    txn: &GraphTxnState,
    plan: &ConsensusParticipantPlan,
    authority: &crate::raft::RaftMutationContext,
) -> Result<bool, String> {
    let receipt_id = consensus_participant_receipt_id(
        txn,
        &plan.coordinator_id,
        plan.participant_id,
        &plan.graph_name,
    );
    let fname = crate::persist::sanitize(&plan.graph_name);
    let Some(record) = backend.read_mutation_batch(&fname, &receipt_id).await? else {
        return Ok(false);
    };
    validate_committed_participant_record(&record, &receipt_id, &fname, authority)?;
    Ok(true)
}

#[cfg(feature = "raft")]
fn validate_committed_participant_record(
    record: &crate::mutation_batch::MutationBatchRecord,
    receipt_id: &str,
    fname: &str,
    authority: &crate::raft::RaftMutationContext,
) -> Result<(), String> {
    record.validate()?;
    if record.status != crate::mutation_batch::MutationBatchStatus::Committed
        || record.batch.batch_id != receipt_id
        || record
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str())
            != Some(fname)
        || record.committing_actor()? != authority.principal_fingerprint
        || record.committing_tenant()? != authority.tenant_scope
    {
        return Err(
            "consensus participant replay has a foreign or non-committed child receipt".to_string(),
        );
    }
    let result = decode_txn_result(
        record
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "consensus participant receipt has no result".to_string())?,
    )?;
    if !matches!(result, ResultPayload::Bool(true)) {
        return Err("consensus participant replay has no successful child outcome".to_string());
    }
    Ok(())
}

#[cfg(feature = "raft")]
pub(super) async fn finish_participant_cleanup(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    plan: &ConsensusParticipantPlan,
) -> Result<(), String> {
    redb.xshard_prepare_clear(&plan.coordinator_id, plan.participant_id)
        .await?;
    crate::server::txn::release_consensus_graph_fence(
        &plan.graph_name,
        &plan.coordinator_id,
        plan.participant_id,
    );
    Ok(())
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_decision(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    authority: &crate::raft::RaftMutationContext,
    commit: bool,
) -> Result<bool, String> {
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction decision requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction decision requires durable redb".to_string())?;
    let parent = validate_consensus_parent_authority(redb, coordinator_id, authority)?;
    if let Some(result) = parent.replayed {
        return match result {
            ResultPayload::Bool(value) if value == commit => Ok(value),
            ResultPayload::Bool(_) => {
                Err("consensus transaction decision conflicts with its parent".to_string())
            }
            _ => Err("consensus transaction parent has an invalid result".to_string()),
        };
    }
    if let Some(existing) = redb.xshard_decision_get(coordinator_id)? {
        if existing != commit {
            return Err(
                "consensus transaction decision conflicts with durable outcome".to_string(),
            );
        }
        return Ok(existing);
    }
    redb.xshard_recoverable_decision_put(coordinator_id, commit)
        .await?;
    Ok(commit)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_finalize(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    authority: &crate::raft::RaftMutationContext,
    commit: bool,
) -> Result<bool, String> {
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction finalize requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction finalize requires durable redb".to_string())?;
    let saga = validate_consensus_parent_authority(redb, coordinator_id, authority)?;
    let decision = redb
        .xshard_decision_get(coordinator_id)?
        .ok_or_else(|| "consensus transaction finalize has no durable decision".to_string())?;
    if decision != commit {
        return Err("consensus transaction finalize conflicts with durable decision".to_string());
    }
    let result = if let Some(result) = saga.replayed {
        result
    } else {
        crate::server::handlers::admin::finish_admin_saga(
            redb,
            saga.batch,
            saga.created_at_ms,
            ResultPayload::Bool(commit),
        )?
    };
    if !matches!(result, ResultPayload::Bool(value) if value == commit) {
        return Err("consensus transaction parent has a conflicting result".to_string());
    }
    redb.xshard_decision_clear(coordinator_id).await?;
    Ok(commit)
}

#[cfg(all(test, feature = "raft", feature = "security"))]
#[path = "authority_tests.rs"]
mod authority_tests;
