use super::*;

#[cfg(feature = "raft")]
pub(crate) async fn check_cluster_placement_before_consensus(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
) -> Result<(), Response> {
    if is_replicated_apply() {
        // The command is already committed in the owning group's log. Its
        // domain kernel must apply locally on every replica without proposing
        // the same command again.
        return Ok(());
    }
    let placement = {
        let current = timed_read(state).await;
        current.placement_authority()
    };
    if !matches!(
        placement,
        crate::server::state::PlacementAuthorityKind::Local
    ) {
        return clustered_route_admission(req, &placement);
    }
    Ok(())
}

/// Admission of one mutation under a non-local placement authority: refuse
/// local-only mutations and missing placement before any proposal.
#[cfg(feature = "raft")]
fn clustered_route_admission(
    req: &Request,
    placement: &crate::server::state::PlacementAuthorityKind,
) -> Result<(), Response> {
    use crate::server::mutation::ClusterMutationRoute;
    match crate::server::mutation::cluster_mutation_route(&req.method) {
        // `SelfRoutedAdmin` owns its OWN `MultiRaft`-presence check
        // (`handlers::raft_admin::try_handle` answers
        // `RAFT_NOT_CONFIGURED`/`CLUSTER_CONFIGURATION_INVALID` itself,
        // matching this exact pair of messages) — it must not be
        // preempted here, exactly like `ReadOnly`/`VolatileControl`.
        ClusterMutationRoute::ReadOnly
        | ClusterMutationRoute::VolatileControl
        | ClusterMutationRoute::SelfRoutedAdmin => {}
        ClusterMutationRoute::LocalOnly => {
            return Err(Response::err(
                req.id,
                crate::server::mutation::LOCAL_ONLY_CLUSTER_REFUSAL,
            ));
        }
        ClusterMutationRoute::ConsensusGraph
        | ClusterMutationRoute::ConsensusNative
        | ClusterMutationRoute::ConsensusFanout
            if placement.missing_error().is_some() =>
        {
            return Err(Response::err(
                req.id,
                placement
                    .missing_error()
                    .expect("missing placement authority has a typed error"),
            ));
        }
        ClusterMutationRoute::ConsensusGraph
        | ClusterMutationRoute::ConsensusNative
        | ClusterMutationRoute::ConsensusFanout => {}
    }
    Ok(())
}

#[cfg(feature = "raft")]
pub(crate) async fn route_consensus_before_gateway(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    identity_bootstrap: bool,
) -> Result<Request, Response> {
    if !is_replicated_apply()
        && matches!(
            timed_read(state).await.placement_authority(),
            crate::server::state::PlacementAuthorityKind::MultiRaft
        )
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusNative
        )
    {
        return Err(propose_native_mutation(
            state,
            &req.graph,
            req.id,
            verified_context,
            identity_bootstrap,
            req.method,
        )
        .await);
    }

    if !is_replicated_apply()
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusFanout
        )
    {
        return Err(match req.method {
            Method::MultiGraphBatchUpdate { batches_msgpack } => {
                multi_graph_batch_update(
                    state,
                    req.id,
                    req.agent_id.as_deref(),
                    verified_context,
                    &batches_msgpack,
                )
                .await
            }
            #[cfg(feature = "sparql-http")]
            Method::ApplyMutation { event_type, query }
                if event_type == crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT =>
            {
                coordinated_sparql_http_update(
                    state,
                    req.id,
                    req.agent_id.as_deref(),
                    verified_context,
                    &req.graph,
                    query,
                )
                .await
            }
            _ => Response::err(req.id, "consensus fanout routing error"),
        });
    }

    Ok(req)
}
