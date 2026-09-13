use super::*;

#[cfg(feature = "raft")]
struct TransactionSubmission<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    coordinator_id: &'a str,
    operation: &'a str,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_type: crate::protocol::GraphType,
    command: crate::raft::NativeMutationCommand,
}

#[cfg(feature = "raft")]
async fn submit_consensus_transaction_command(
    submission: TransactionSubmission<'_>,
) -> Result<bool, String> {
    let TransactionSubmission {
        multi,
        authority,
        request_id,
        coordinator_id,
        operation,
        group_id,
        placement_epoch,
        fencing_token,
        graph_type,
        command,
    } = submission;
    let route_key = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-consensus-transaction-route",
        coordinator_id,
        operation,
    );
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-consensus-transaction-command",
        coordinator_id,
        operation,
    );
    let committed_at_ms = authoritative_now_ms();
    let mutation = crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        None,
        authority.tenant_scope(),
        authority.actor_scope().to_string(),
        false,
        placement_epoch,
        fencing_token,
        committed_at_ms,
    )?;
    let request = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(&route_key),
        graph_name: route_key,
        graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    };
    let response = multi.client_write_group(group_id, request).await?;
    if let Some(error) = response.native_error {
        return Err(error);
    }
    match response.native_result {
        Some(ResultPayload::Bool(value)) => Ok(value),
        _ => Err("consensus transaction command returned an invalid result".to_string()),
    }
}

#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
enum TransactionConsensusPhase {
    Decision,
    Finalize,
}

#[cfg(feature = "raft")]
impl TransactionConsensusPhase {
    fn operation(self, commit: bool) -> &'static str {
        match (self, commit) {
            (Self::Decision, true) => "decision-commit",
            (Self::Decision, false) => "decision-abort",
            (Self::Finalize, true) => "finalize-commit",
            (Self::Finalize, false) => "finalize-abort",
        }
    }

    fn command(self, coordinator_id: &str, commit: bool) -> crate::raft::NativeMutationCommand {
        match self {
            Self::Decision => crate::raft::NativeMutationCommand::TransactionDecision {
                coordinator_id: coordinator_id.to_string(),
                commit,
            },
            Self::Finalize => crate::raft::NativeMutationCommand::TransactionFinalize {
                coordinator_id: coordinator_id.to_string(),
                commit,
            },
        }
    }
}

#[cfg(feature = "raft")]
struct TransactionPhaseSubmission<'a> {
    phase: TransactionConsensusPhase,
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    coordinator_id: &'a str,
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    graph_type: crate::protocol::GraphType,
    commit: bool,
}

#[cfg(feature = "raft")]
async fn submit_consensus_transaction_phase(
    submission: TransactionPhaseSubmission<'_>,
) -> Result<bool, String> {
    let TransactionPhaseSubmission {
        phase,
        multi,
        authority,
        request_id,
        coordinator_id,
        control_group,
        control_epoch,
        control_fence,
        graph_type,
        commit,
    } = submission;
    submit_consensus_transaction_command(TransactionSubmission {
        multi,
        authority,
        request_id,
        coordinator_id,
        operation: phase.operation(commit),
        group_id: control_group,
        placement_epoch: control_epoch,
        fencing_token: control_fence,
        graph_type,
        command: phase.command(coordinator_id, commit),
    })
    .await
}

#[cfg(feature = "raft")]
async fn abort_consensus_transaction(
    coordination: &TransactionCoordination<'_>,
    coordinator_id: &str,
    participants: &[crate::server::handlers::txn::ConsensusTransactionParticipant],
) -> Result<bool, String> {
    let decided = submit_consensus_transaction_phase(TransactionPhaseSubmission {
        phase: TransactionConsensusPhase::Decision,
        multi: coordination.multi,
        authority: coordination.authority,
        request_id: coordination.request_id,
        coordinator_id,
        control_group: coordination.control_group,
        control_epoch: coordination.control_epoch,
        control_fence: coordination.control_fence,
        graph_type: coordination.control_graph_type,
        commit: false,
    })
    .await?;
    if decided {
        return Err("consensus transaction abort received a commit decision".to_string());
    }
    // Abort every participant in the frozen fanout, including a participant whose
    // PREPARE reply was lost after its command committed. The abort command is
    // idempotent when no durable intent exists.
    for participant in participants {
        let command = crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Abort,
            coordinator_id.to_string(),
            participant.participant_id,
            None,
            coordination.server_secret,
        )?;
        let operation = format!("abort-{}", participant.participant_id);
        if !submit_consensus_transaction_command(TransactionSubmission {
            multi: coordination.multi,
            authority: coordination.authority,
            request_id: coordination.request_id,
            coordinator_id,
            operation: &operation,
            group_id: participant.group_id,
            placement_epoch: participant.placement_epoch,
            fencing_token: participant.fencing_token,
            graph_type: participant.graph_type,
            command,
        })
        .await?
        {
            return Err("consensus participant abort was not applied".to_string());
        }
    }
    submit_consensus_transaction_phase(TransactionPhaseSubmission {
        phase: TransactionConsensusPhase::Finalize,
        multi: coordination.multi,
        authority: coordination.authority,
        request_id: coordination.request_id,
        coordinator_id,
        control_group: coordination.control_group,
        control_epoch: coordination.control_epoch,
        control_fence: coordination.control_fence,
        graph_type: coordination.control_graph_type,
        commit: false,
    })
    .await
}

/// The control-plane consensus route a coordinator drives its decision, abort
/// and finalize records through, together with the identity every submission is
/// signed under. Bundled so each phase helper below stays inside the parameter
/// cap while still seeing the whole coordination context.
#[cfg(feature = "raft")]
struct TransactionCoordination<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    server_secret: &'a str,
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    control_graph_type: crate::protocol::GraphType,
}

#[cfg(feature = "raft")]
impl TransactionCoordination<'_> {
    async fn abort(
        &self,
        fanout: &handlers::txn::ConsensusTransactionFanout,
    ) -> Result<bool, String> {
        abort_consensus_transaction(self, &fanout.coordinator_id, &fanout.participants).await
    }
}

/// Abort after a failed prepare, and turn the abort's OWN outcome into the
/// response the caller returns. `prepare_error` is `None` for a clean refusal
/// (the transaction simply did not commit) and `Some` for a submission failure;
/// the two cases report differently, exactly as the inline arms did.
#[cfg(feature = "raft")]
async fn resolve_failed_consensus_prepare(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
    prepare_error: Option<String>,
) -> Response {
    let request_id = coordination.request_id;
    match (coordination.abort(fanout).await, prepare_error) {
        (Ok(false), None) => Response::ok(request_id, ResultPayload::Bool(false)),
        (Ok(false), Some(error)) => Response::err(
            request_id,
            format!("consensus participant prepare failed: {error}"),
        ),
        (Ok(true), _) => Response::err(request_id, "consensus abort finalized as commit"),
        (Err(cleanup_error), None) => Response::err(request_id, cleanup_error),
        (Err(cleanup_error), Some(error)) => Response::err(
            request_id,
            format!("consensus participant prepare failed: {error}; abort failed: {cleanup_error}"),
        ),
    }
}

/// Phase 1: prepare every participant. Any refusal or submission failure aborts
/// the transaction and yields the caller's response.
#[cfg(feature = "raft")]
async fn prepare_consensus_participants(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Prepare,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            coordination.server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Err(Response::err(request_id, error)),
        };
        let operation = format!("prepare-{}", participant.participant_id);
        let submitted = submit_consensus_transaction_command(TransactionSubmission {
            multi: coordination.multi,
            authority: coordination.authority,
            request_id,
            coordinator_id: &fanout.coordinator_id,
            operation: &operation,
            group_id: participant.group_id,
            placement_epoch: participant.placement_epoch,
            fencing_token: participant.fencing_token,
            graph_type: participant.graph_type,
            command,
        })
        .await;
        match submitted {
            Ok(true) => {}
            Ok(false) => {
                return Err(resolve_failed_consensus_prepare(coordination, fanout, None).await)
            }
            Err(error) => {
                return Err(
                    resolve_failed_consensus_prepare(coordination, fanout, Some(error)).await,
                )
            }
        }
    }
    Ok(())
}

/// Phase 2: record the COMMIT decision on the control group.
#[cfg(feature = "raft")]
async fn decide_consensus_commit(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    let decided = submit_consensus_transaction_phase(TransactionPhaseSubmission {
        phase: TransactionConsensusPhase::Decision,
        multi: coordination.multi,
        authority: coordination.authority,
        request_id,
        coordinator_id: &fanout.coordinator_id,
        control_group: coordination.control_group,
        control_epoch: coordination.control_epoch,
        control_fence: coordination.control_fence,
        graph_type: coordination.control_graph_type,
        commit: true,
    })
    .await;
    let decision_error = match decided {
        Ok(true) => return Ok(()),
        Ok(false) => {
            return Err(Response::err(
                request_id,
                "consensus transaction was durably aborted",
            ))
        }
        Err(error) => error,
    };
    // A prior retry may already have decided ABORT. Conversely, if the
    // COMMIT reply was merely lost, the abort decision will conflict and
    // preserve the durable COMMIT. Either way this cleanup cannot reverse a
    // recorded outcome.
    Err(match coordination.abort(fanout).await {
        Ok(false) => Response::err(
            request_id,
            format!("consensus transaction was durably aborted: {decision_error}"),
        ),
        Ok(true) => Response::err(request_id, "consensus abort finalized as commit"),
        Err(cleanup_error) => Response::err(
            request_id,
            format!(
                "consensus decision failed: {decision_error}; resolution failed: {cleanup_error}"
            ),
        ),
    })
}

/// Phase 3: drive every participant to COMMIT. The decision is already durable,
/// so a failure here is reported for retry rather than aborted.
#[cfg(feature = "raft")]
async fn commit_consensus_participants(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Commit,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            coordination.server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Err(Response::err(request_id, error)),
        };
        let operation = format!("commit-{}", participant.participant_id);
        match submit_consensus_transaction_command(TransactionSubmission {
            multi: coordination.multi,
            authority: coordination.authority,
            request_id,
            coordinator_id: &fanout.coordinator_id,
            operation: &operation,
            group_id: participant.group_id,
            placement_epoch: participant.placement_epoch,
            fencing_token: participant.fencing_token,
            graph_type: participant.graph_type,
            command,
        })
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(Response::err(
                    request_id,
                    "decided consensus participant did not commit; retry will resume",
                ))
            }
            Err(error) => {
                return Err(Response::err(
                    request_id,
                    format!("decided consensus participant commit failed: {error}"),
                ))
            }
        }
    }
    Ok(())
}

#[cfg(feature = "raft")]
pub(super) struct TransactionExecution<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) request_id: u64,
    pub(super) authority: &'a CarrierAuthority,
    pub(super) server_secret: &'a str,
    pub(super) multi: Arc<crate::raft::multi::MultiRaft>,
    pub(super) control: crate::raft::multi::RoutedRaftHandle,
    pub(super) control_graph_type: crate::protocol::GraphType,
    pub(super) prepared_bytes: &'a [u8],
}

#[cfg(feature = "raft")]
async fn execute_consensus_transaction(execution: TransactionExecution<'_>) -> Response {
    let TransactionExecution {
        state,
        request_id,
        authority,
        server_secret,
        multi,
        control,
        control_graph_type,
        prepared_bytes,
    } = execution;
    let fanout =
        match handlers::txn::build_consensus_transaction_fanout(state, prepared_bytes).await {
            Ok(fanout) => fanout,
            Err(error) => return Response::err(request_id, error),
        };
    let coordination = TransactionCoordination {
        multi: &multi,
        authority,
        request_id,
        server_secret,
        control_group: control.group_id,
        control_epoch: control.epoch,
        control_fence: control.placed.then_some(control.group_id),
        control_graph_type,
    };
    if let Err(response) = prepare_consensus_participants(&coordination, &fanout).await {
        return response;
    }
    if let Err(response) = decide_consensus_commit(&coordination, &fanout).await {
        return response;
    }
    if let Err(response) = commit_consensus_participants(&coordination, &fanout).await {
        return response;
    }
    match submit_consensus_transaction_phase(TransactionPhaseSubmission {
        phase: TransactionConsensusPhase::Finalize,
        multi: coordination.multi,
        authority: coordination.authority,
        request_id,
        coordinator_id: &fanout.coordinator_id,
        control_group: coordination.control_group,
        control_epoch: coordination.control_epoch,
        control_fence: coordination.control_fence,
        graph_type: coordination.control_graph_type,
        commit: true,
    })
    .await
    {
        Ok(true) => Response::ok(request_id, ResultPayload::Bool(true)),
        Ok(false) => Response::err(request_id, "consensus transaction finalized as abort"),
        Err(error) => Response::err(request_id, error),
    }
}
