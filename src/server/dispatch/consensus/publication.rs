use super::*;

#[cfg(all(feature = "raft", feature = "jobs"))]
struct JobPublicationSubmission<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    coordinator_id: &'a str,
    operation: &'a str,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    command: crate::raft::NativeMutationCommand,
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(super) struct JobPublicationExecution<'a> {
    pub(super) request_id: u64,
    pub(super) authority: &'a CarrierAuthority,
    pub(super) server_secret: &'a str,
    pub(super) multi: Arc<crate::raft::multi::MultiRaft>,
    pub(super) control: crate::raft::multi::RoutedRaftHandle,
    pub(super) control_graph: &'a str,
    pub(super) control_graph_type: crate::protocol::GraphType,
    pub(super) prepared_bytes: &'a [u8],
}

#[cfg(all(feature = "raft", feature = "jobs"))]
async fn submit_consensus_job_publication_response(
    submission: JobPublicationSubmission<'_>,
) -> Result<crate::raft::RaftResponse, String> {
    let JobPublicationSubmission {
        multi,
        authority,
        request_id,
        attempt_nonce,
        coordinator_id,
        operation,
        graph_name,
        graph_type,
        group_id,
        placement_epoch,
        fencing_token,
        command,
    } = submission;
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-job-publication-command",
        coordinator_id,
        operation,
    );
    let committed_at_ms = authoritative_now_ms();
    let mutation = crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        attempt_nonce,
        authority.tenant_scope(),
        authority.actor_scope().to_string(),
        false,
        placement_epoch,
        crate::raft::RaftMutationTiming {
            fencing_token,
            created_at_ms: committed_at_ms,
        },
    )?;
    let request = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(graph_name),
        graph_name: graph_name.to_string(),
        graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    };
    let response = multi.client_write_group(group_id, request).await?;
    response.validate()?;
    if let Some(error) = response.native_error {
        return Err(error);
    }
    Ok(response)
}

#[cfg(all(feature = "raft", feature = "jobs"))]
async fn submit_consensus_job_publication_command(
    submission: JobPublicationSubmission<'_>,
) -> Result<ResultPayload, String> {
    let response = submit_consensus_job_publication_response(submission).await?;
    response
        .native_result
        .ok_or_else(|| "job publication command returned no result".to_string())
}

/// Submit a job-publication target commit and return the durable typed receipt.
/// The caller must validate domain-specific publication bytes; this layer only
/// transports the state-machine's exact MutationBatchCommit without deriving a
/// substitute from `applied` or `native_result`.
#[cfg(all(feature = "raft", feature = "jobs"))]
async fn submit_consensus_job_publication_commit(
    submission: JobPublicationSubmission<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let response = submit_consensus_job_publication_response(submission).await?;
    response
        .native_commit
        .ok_or_else(|| "job publication commit returned no durable receipt".to_string())
}

/// The target group must return a valid committed MutationBatch receipt. The
/// caller retains the typed value until the jobs domain binds it to the
/// prepared batch immediately before scheduler finalization.
#[cfg(all(feature = "raft", feature = "jobs"))]
fn interpret_job_publication_commit(
    request_id: u64,
    outcome: Result<crate::mutation_batch::MutationBatchCommit, String>,
) -> Result<crate::mutation_batch::MutationBatchCommit, Response> {
    match outcome {
        Ok(commit) => commit.validate().map(|()| commit).map_err(|error| {
            Response::err(
                request_id,
                format!("job publication target returned invalid receipt: {error}"),
            )
        }),
        Err(error) => Err(Response::err(
            request_id,
            format!("job publication target commit failed: {error}"),
        )),
    }
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(super) async fn execute_consensus_job_publication(
    execution: JobPublicationExecution<'_>,
) -> Response {
    let prepared = match decode_job_publication(execution.request_id, execution.prepared_bytes) {
        Ok(prepared) => prepared,
        Err(response) => return response,
    };
    let target_route = execution.multi.route_graph(&prepared.target_graph).await;
    let (commit_plan, finalize_receipt) =
        match build_job_publication_plans(execution.request_id, prepared.clone(), target_route) {
            Ok(plans) => plans,
            Err(response) => return response,
        };
    let committed = match submit_target_job_publication_commit(
        &execution,
        &prepared,
        target_route,
        commit_plan,
    )
    .await
    {
        Ok(committed) => committed,
        Err(response) => return response,
    };
    let finalize = match build_job_publication_finalize(
        execution.request_id,
        &prepared,
        &finalize_receipt,
        execution.server_secret,
    ) {
        Ok(command) => command,
        Err(response) => return response,
    };
    finalize_consensus_job_publication(
        &JobPublicationControl {
            multi: &execution.multi,
            authority: execution.authority,
            request_id: execution.request_id,
            control: &execution.control,
            control_graph: execution.control_graph,
            control_graph_type: execution.control_graph_type,
        },
        &prepared,
        &committed,
        &prepared.coordinator_id,
        finalize,
    )
    .await
}

#[cfg(all(feature = "raft", feature = "jobs"))]
fn decode_job_publication(
    request_id: u64,
    prepared_bytes: &[u8],
) -> Result<handlers::jobs::PreparedJobPublication, Response> {
    handlers::jobs::decode_prepared_job_publication(prepared_bytes)
        .map_err(|error| Response::err(request_id, error))
}

#[cfg(all(feature = "raft", feature = "jobs"))]
fn build_job_publication_plans(
    request_id: u64,
    prepared: handlers::jobs::PreparedJobPublication,
    target_route: crate::raft::placement::PlacementRoute,
) -> Result<(Vec<u8>, Vec<u8>), Response> {
    let target_fence = target_route.placed.then_some(target_route.fencing_token());
    handlers::jobs::build_job_publication_commands(
        prepared,
        target_route.group,
        target_route.epoch,
        target_fence,
    )
    .map_err(|error| Response::err(request_id, error))
}

#[cfg(all(feature = "raft", feature = "jobs"))]
fn build_job_publication_commit(
    request_id: u64,
    prepared: &handlers::jobs::PreparedJobPublication,
    commit_plan: &[u8],
    server_secret: &str,
) -> Result<crate::raft::NativeMutationCommand, Response> {
    crate::raft::NativeMutationCommand::job_publication_commit(
        prepared.coordinator_id.clone(),
        commit_plan,
        server_secret,
    )
    .map_err(|error| Response::err(request_id, error))
}

#[cfg(all(feature = "raft", feature = "jobs"))]
async fn submit_target_job_publication_commit(
    execution: &JobPublicationExecution<'_>,
    prepared: &handlers::jobs::PreparedJobPublication,
    target_route: crate::raft::placement::PlacementRoute,
    commit_plan: Vec<u8>,
) -> Result<crate::mutation_batch::MutationBatchCommit, Response> {
    let target_fence = target_route.placed.then_some(target_route.fencing_token());
    let commit = build_job_publication_commit(
        execution.request_id,
        prepared,
        &commit_plan,
        execution.server_secret,
    )?;
    interpret_job_publication_commit(
        execution.request_id,
        submit_consensus_job_publication_commit(JobPublicationSubmission {
            multi: &execution.multi,
            authority: execution.authority,
            request_id: execution.request_id,
            attempt_nonce: execution.authority.attempt_nonce(),
            coordinator_id: &prepared.coordinator_id,
            operation: "target-commit",
            graph_name: &prepared.target_graph,
            graph_type: prepared.target_graph_type,
            group_id: target_route.group,
            placement_epoch: target_route.epoch,
            fencing_token: target_fence,
            command: commit,
        })
        .await,
    )
}

#[cfg(all(feature = "raft", feature = "jobs"))]
fn build_job_publication_finalize(
    request_id: u64,
    prepared: &handlers::jobs::PreparedJobPublication,
    finalize_receipt: &[u8],
    server_secret: &str,
) -> Result<crate::raft::NativeMutationCommand, Response> {
    crate::raft::NativeMutationCommand::job_publication_finalize(
        prepared.coordinator_id.clone(),
        finalize_receipt,
        server_secret,
    )
    .map_err(|error| Response::err(request_id, error))
}

/// The scheduler's control-group route for a job publication's finalize record.
#[cfg(all(feature = "raft", feature = "jobs"))]
struct JobPublicationControl<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    control: &'a crate::raft::multi::RoutedRaftHandle,
    control_graph: &'a str,
    control_graph_type: crate::protocol::GraphType,
}

/// Record the finalize half on the scheduler's control group, after the target
/// group has already durably committed.
#[cfg(all(feature = "raft", feature = "jobs"))]
async fn finalize_consensus_job_publication(
    control: &JobPublicationControl<'_>,
    prepared: &handlers::jobs::PreparedJobPublication,
    committed: &crate::mutation_batch::MutationBatchCommit,
    coordinator_id: &str,
    finalize: crate::raft::NativeMutationCommand,
) -> Response {
    let request_id = control.request_id;
    if let Err(error) = handlers::jobs::validate_job_publication_commit(prepared, committed) {
        return Response::err(
            request_id,
            format!("job publication finalization lost its target receipt: {error}"),
        );
    }
    let control_fence = control
        .control
        .placed
        .then_some(control.control.fencing_token());
    match submit_consensus_job_publication_command(JobPublicationSubmission {
        multi: control.multi,
        authority: control.authority,
        request_id,
        attempt_nonce: None,
        coordinator_id,
        operation: "scheduler-finalize",
        graph_name: control.control_graph,
        graph_type: control.control_graph_type,
        group_id: control.control.group_id,
        placement_epoch: control.control.epoch,
        fencing_token: control_fence,
        command: finalize,
    })
    .await
    {
        Ok(result) => Response::ok(request_id, result),
        Err(error) => Response::err(
            request_id,
            format!("job publication finalization failed: {error}"),
        ),
    }
}
