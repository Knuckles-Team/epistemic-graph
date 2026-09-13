use super::*;

#[cfg(feature = "raft")]
pub(crate) fn is_replicated_apply() -> bool {
    REPLICATED_APPLY.try_with(|_| ()).is_ok()
}

#[cfg(feature = "raft")]
pub(crate) fn replicated_placement_authority() -> Option<(u64, Option<u64>)> {
    REPLICATED_APPLY
        .try_with(|scope| (scope.placement_epoch, scope.fencing_token))
        .ok()
}

#[cfg(feature = "raft")]
pub(super) fn replicated_identity_bootstrap_authorized() -> bool {
    REPLICATED_APPLY
        .try_with(|scope| scope.identity_bootstrap)
        .unwrap_or(false)
}

#[cfg(not(feature = "raft"))]
pub(super) fn replicated_identity_bootstrap_authorized() -> bool {
    false
}

#[cfg(feature = "raft")]
pub(super) fn capability_authority_unavailable(method: &Method) -> bool {
    matches!(
        method,
        Method::MintWorkItemClaimCapability { .. } | Method::VerifyWorkItemClaimCapability { .. }
    )
}

#[cfg(not(feature = "raft"))]
pub(crate) fn is_replicated_apply() -> bool {
    false
}

#[cfg(not(feature = "raft"))]
pub(crate) fn replicated_placement_authority() -> Option<(u64, Option<u64>)> {
    None
}

#[cfg(not(feature = "raft"))]
pub(super) async fn propose_native_mutation(
    _state: &Arc<RwLock<ServerState>>,
    _request_graph: &str,
    request_id: u64,
    _verified_context: &VerifiedRequestContext,
    _identity_bootstrap: bool,
    _method: Method,
) -> Response {
    Response::err(
        request_id,
        "consensus routing is not available in this build",
    )
}

/// Apply a committed bounded native command through its existing domain kernel.
/// Authentication/authorization has already happened before proposal; the
/// reconstructed context contains only one-way tenant/principal scopes so no raw
/// identity enters the Raft log or snapshot.
#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_native(
    state: &Arc<RwLock<ServerState>>,
    graph: String,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    method: Method,
) -> Response {
    if capability_authority_unavailable(&method) {
        // RaftMutationContext intentionally contains only one-way routing
        // identity.  It is not an authenticated principal/session envelope,
        // so never reconstruct capability authority on a follower/replay.
        return Response::err(
            request_id,
            crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
        );
    }
    let context = match VerifiedRequestContext::replicated_mutation(authority) {
        Ok(context) => context,
        Err(error) => return Response::err(request_id, error),
    };
    let request = Request {
        id: request_id,
        graph,
        auth_token: String::new(),
        agent_id: None,
        method,
    };
    REPLICATED_APPLY
        .scope(
            replicated_apply_scope(committed_at_ms, authority),
            dispatch_with_context(state, request, Some(context)),
        )
        .await
}

#[cfg(feature = "raft")]
fn replicated_apply_scope(
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
) -> ReplicatedApplyScope {
    ReplicatedApplyScope {
        committed_at_ms,
        placement_epoch: authority.placement_epoch,
        fencing_token: authority.fencing_token,
        identity_bootstrap: authority.identity_bootstrap,
    }
}

/// The identifying fields of a replicated transaction participant, bundled so
/// [`apply_replicated_transaction_participant`] stays under the clippy
/// argument-count ceiling.
#[cfg(feature = "raft")]
pub(crate) struct ReplicatedParticipantRef<'a> {
    pub(crate) coordinator_id: &'a str,
    pub(crate) participant_id: u64,
    pub(crate) plan: Option<&'a [u8]>,
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_participant(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    phase: crate::raft::TransactionParticipantPhase,
    participant: ReplicatedParticipantRef<'_>,
) -> Result<bool, String> {
    let ReplicatedParticipantRef {
        coordinator_id,
        participant_id,
        plan,
    } = participant;
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            match phase {
                crate::raft::TransactionParticipantPhase::Prepare => {
                    handlers::txn::apply_consensus_participant_prepare(
                        state,
                        applying_group,
                        authority.placement_epoch,
                        authority.fencing_token,
                        coordinator_id,
                        participant_id,
                        plan.ok_or_else(|| "participant prepare is missing its plan".to_string())?,
                    )
                    .await
                }
                crate::raft::TransactionParticipantPhase::Commit => {
                    handlers::txn::apply_consensus_participant_commit(
                        state,
                        request_id,
                        applying_group,
                        authority,
                        handlers::txn::ConsensusParticipantCommitRef {
                            coordinator_id,
                            participant_id,
                            plan_bytes: plan.ok_or_else(|| {
                                "participant commit is missing its plan".to_string()
                            })?,
                        },
                    )
                    .await
                }
                crate::raft::TransactionParticipantPhase::Abort => {
                    handlers::txn::apply_consensus_participant_abort(
                        state,
                        coordinator_id,
                        participant_id,
                    )
                    .await
                }
            }
        })
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_prepare(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    txn_id: &str,
) -> Response {
    REPLICATED_APPLY
        .scope(
            replicated_apply_scope(committed_at_ms, authority),
            handlers::txn::prepare_consensus_commit(
                state,
                request_id,
                Some(&authority.principal_fingerprint),
                txn_id,
            ),
        )
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_decision(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    commit: bool,
) -> Result<bool, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::txn::apply_consensus_transaction_decision(
                state,
                coordinator_id,
                &authority.principal_fingerprint,
                commit,
            )
            .await
        })
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    commit: bool,
) -> Result<bool, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::txn::apply_consensus_transaction_finalize(
                state,
                coordinator_id,
                &authority.principal_fingerprint,
                commit,
            )
            .await
        })
        .await
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) async fn apply_replicated_job_publication_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    coordinator_id: &str,
    plan: &[u8],
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::jobs::apply_consensus_job_publication_commit(
                state,
                request_id,
                authority,
                applying_group,
                coordinator_id,
                plan,
            )
            .await
        })
        .await
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) async fn apply_replicated_job_publication_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    receipt: &[u8],
) -> Result<ResultPayload, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::jobs::apply_consensus_job_publication_finalize(
                state,
                committed_at_ms,
                coordinator_id,
                receipt,
            )
            .await
        })
        .await
}
