use super::*;

/// Per-tenant opaque coordinator key for a command that owns its OWN store
/// (keyed by blob/kv/series/job/statechart/catalog id) and is therefore not
/// graph-scoped. One totally-ordered consensus route per tenant is what keeps
/// replicas applying these in step.
#[cfg(feature = "raft")]
fn native_route_opaque_key(label: &str, tenant_scope: &str, kind: &str) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(label, tenant_scope, kind)
}

/// A graph-lifecycle command routes to the graph it NAMES, not to the graph the
/// request arrived on. Any other method shape falls back to the request graph.
#[cfg(feature = "raft")]
fn native_route_lifecycle_target(request_graph: &str, method: &Method) -> String {
    match method {
        Method::CreateGraph { graph_name, .. } | Method::DeleteGraph { graph_name } => {
            graph_name.clone()
        }
        _ => request_graph.to_string(),
    }
}

/// Route one of the native commands that owns an opaque, tenant-scoped store.
/// `NativeMutationCommand::domain` is generated from the same exhaustive
/// command-layout macro that validates every native variant. The string labels
/// are used only to keep this dispatch file independent of that private enum;
/// an unknown label fails closed instead of inheriting a graph route.
#[cfg(feature = "raft")]
fn native_route_opaque_target(tenant_scope: &str, domain: Option<&str>) -> Option<String> {
    if domain == Some("Blob") {
        return Some(native_route_opaque_key(
            "raft-native-blob",
            tenant_scope,
            "control",
        ));
    }
    if domain == Some("KeyValue") {
        return Some(native_route_opaque_key(
            "raft-native-kv",
            tenant_scope,
            "control",
        ));
    }
    if domain == Some("TimeSeries") {
        return Some(native_route_opaque_key(
            "raft-native-timeseries",
            tenant_scope,
            "control",
        ));
    }
    if domain == Some("AnalyticsJob") {
        return Some(native_route_opaque_key(
            "raft-native-jobs",
            tenant_scope,
            "control",
        ));
    }
    if domain == Some("Statechart") {
        return Some(native_route_opaque_key(
            "raft-native-statechart",
            tenant_scope,
            "control",
        ));
    }
    if domain == Some("SqliteCatalog") {
        return Some(native_route_opaque_key(
            "raft-native-sqlite",
            tenant_scope,
            "catalog",
        ));
    }
    None
}

/// The consensus route for one native mutation command.
///
/// `NativeMutationCommand::domain()` is itself compiler-exhaustive over the
/// command enum. This boundary keeps the route table fail-closed: the known
/// domain labels are explicit and any newly introduced label reaches the final
/// `unreachable!` rather than silently taking the request-graph route.
#[cfg(feature = "raft")]
fn native_route_target(
    request_graph: &str,
    tenant_scope: &str,
    method: &Method,
    command: &crate::raft::NativeMutationCommand,
) -> String {
    // Publication coordination commands share the AnalyticsJob domain for
    // validation, but their target is the named request graph. The sealed
    // AnalyticsJob store command below remains tenant-scoped and opaque.
    #[cfg(feature = "jobs")]
    if matches!(
        command,
        crate::raft::NativeMutationCommand::JobPublicationCommit { .. }
            | crate::raft::NativeMutationCommand::JobPublicationFinalize { .. }
    ) {
        return request_graph.to_string();
    }
    let domain = command.domain().map(|domain| format!("{domain:?}"));
    if let Some(route) = native_route_opaque_target(tenant_scope, domain.as_deref()) {
        return route;
    }
    match domain.as_deref() {
        None | Some("GraphState") | Some("Transaction") | Some("WorkItem") | Some("Multisig") => {
            request_graph.to_string()
        }
        Some("GraphLifecycle") => native_route_lifecycle_target(request_graph, method),
        Some("ClusterAdmin") | Some("SessionControl") => {
            crate::raft::placement::PLACEMENT_GRAPH.to_string()
        }
        // Identity/RBAC state is process-global authority. Every such command must
        // therefore share one Raft order; tenant-hashed groups could apply
        // concurrent add/remove transitions in different orders on each replica.
        // `__commons__` also preserves the exact bootstrap route asserted before
        // proposal and revalidated by `RaftRequest::validate`.
        Some("Identity") => "__commons__".to_string(),
        Some(other) => unreachable!("unclassified native consensus domain: {other}"),
    }
}

#[cfg(all(test, feature = "raft"))]
mod consensus_admin_route_tests {
    use super::*;

    #[test]
    fn coordinator_ledgers_are_ordered_by_the_placement_group() {
        let auth_material = "consensus-admin-route-test";
        for method in [
            Method::BeginTxn {
                graph: Some("caller-graph".to_string()),
                isolation: None,
            },
            Method::Commit {
                txn_id: "opaque-txn".to_string(),
                idempotency_key: None,
            },
            Method::CreateChannel {
                channel_id: "opaque-channel".to_string(),
                channel_type: crate::protocol::ChannelType::PeerToPeer,
                creator: "opaque-creator".to_string(),
                initial_members: Vec::new(),
            },
        ] {
            let command = crate::raft::NativeMutationCommand::from_public_method(
                method.clone(),
                auth_material,
            )
            .expect("method has a typed consensus command");
            assert_eq!(
                native_route_target("caller-graph", "tenant-scope", &method, &command),
                crate::raft::placement::PLACEMENT_GRAPH
            );
        }
    }

    #[test]
    fn identity_and_rbac_commands_share_the_commons_consensus_order() {
        let method = Method::RbacAdmin {
            op: crate::acl::RbacAdminOp::List,
        };
        let command = crate::raft::NativeMutationCommand::from_public_method(
            method.clone(),
            "consensus-identity-route-test",
        )
        .unwrap();
        assert_eq!(
            native_route_target("caller-graph", "tenant-scope", &method, &command),
            "__commons__"
        );
    }

    #[test]
    fn capability_consensus_paths_refuse_without_the_original_auth_envelope() {
        use crate::epistemic_operations_ext::{
            WorkItemClaimCapabilityMintRequest, WorkItemClaimCapabilityRequestSchemaVersion,
            WorkItemClaimCapabilityVerifyRequest,
        };

        let methods = [
            Method::MintWorkItemClaimCapability {
                request: WorkItemClaimCapabilityMintRequest {
                    schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                    work_item_id: "work-item".to_string(),
                },
            },
            Method::VerifyWorkItemClaimCapability {
                request: WorkItemClaimCapabilityVerifyRequest {
                    schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                    work_item_id: "work-item".to_string(),
                    capability: vec![0; 36],
                },
            },
        ];
        for method in methods {
            assert!(capability_authority_unavailable(&method));
            assert_eq!(
                crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
                "authority_unavailable"
            );
        }
    }

    #[test]
    fn clustered_commit_replay_terminal_result_keeps_its_keyed_shape() {
        let response = terminal_native_response(
            7,
            NativeCoordination::TransactionCommit,
            true,
            ResultPayload::Json(serde_json::json!({
                "committed": true,
                "replayed": true,
            })),
        );
        let Some(ResultPayload::Json(value)) = response.result else {
            panic!("expected keyed replay result");
        };
        assert_eq!(value["committed"], true);
        assert_eq!(value["replayed"], true);
    }

    #[test]
    fn clustered_commit_terminal_boolean_is_tagged_with_the_caller_key() {
        let response = terminal_native_response(
            8,
            NativeCoordination::TransactionCommit,
            true,
            ResultPayload::Bool(true),
        );
        let Some(ResultPayload::Json(value)) = response.result else {
            panic!("expected keyed commit result");
        };
        assert_eq!(value["committed"], true);
        assert_eq!(value["replayed"], false);
    }
}

/// The resolved identity and route of one native consensus proposal, shared by
/// the request builder and the write/coordination helpers below.
#[cfg(feature = "raft")]
pub(super) struct NativeProposal<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) request_id: u64,
    pub(super) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(super) authority: &'a CarrierAuthority,
    pub(super) server_secret: &'a str,
    pub(super) graph_name: &'a str,
    pub(super) graph_type: crate::protocol::GraphType,
    pub(super) commit_keyed: bool,
}

/// Which consensus coordination a committed native command still needs.
#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
pub(super) enum NativeCoordination {
    /// The replicated result is already terminal.
    Terminal,
    /// A worker job publication: commit on the target group, then finalize on
    /// the scheduler's control group.
    #[cfg(feature = "jobs")]
    JobPublication,
    /// A transaction commit: prepare/decide/commit/finalize the participant
    /// fanout.
    TransactionCommit,
}

/// Classified BEFORE the command is proposed, in the same order the inline
/// checks ran: job publication first, then transaction commit.
#[cfg(feature = "raft")]
fn native_coordination_for(method: &Method) -> NativeCoordination {
    #[cfg(feature = "jobs")]
    if matches!(
        method,
        Method::AnalyticsJob {
            op: eg_types::jobs::JobOp::WorkerPublish { .. }
        }
    ) {
        return NativeCoordination::JobPublication;
    }
    if matches!(method, Method::Commit { .. }) {
        return NativeCoordination::TransactionCommit;
    }
    NativeCoordination::Terminal
}

/// Resolve the consensus route for a native command: the graph whose group
/// totally orders it, that graph's type (a `CreateGraph` carries its own; every
/// other method reads the registry, defaulting to `Global`), and the placement
/// authority — all under ONE read lock.
#[cfg(feature = "raft")]
async fn resolve_native_proposal_route(
    state: &Arc<RwLock<ServerState>>,
    request_graph: &str,
    authority: &CarrierAuthority,
    method: &Method,
    command: &crate::raft::NativeMutationCommand,
) -> (
    String,
    Option<Arc<crate::raft::multi::MultiRaft>>,
    crate::protocol::GraphType,
) {
    let graph_name = native_route_target(request_graph, authority.tenant_scope(), method, command);
    let current = timed_read(state).await;
    let graph_type = match method {
        Method::CreateGraph { graph_type, .. } => *graph_type,
        _ => current
            .registry
            .get(&graph_name)
            .map(|entry| entry.graph_type)
            .unwrap_or(crate::protocol::GraphType::Global),
    };
    let multi = current.multi_raft.clone();
    drop(current);
    (graph_name, multi, graph_type)
}

#[cfg(feature = "raft")]
fn build_native_raft_request(
    proposal: &NativeProposal<'_>,
    routed: &crate::raft::multi::RoutedRaftHandle,
    command: crate::raft::NativeMutationCommand,
    identity_bootstrap: bool,
) -> Result<crate::raft::RaftRequest, Response> {
    let committed_at_ms = authoritative_now_ms();
    let graph_fname = crate::persist::sanitize(proposal.graph_name);
    let graph_scope = crate::server::mutation_batch::opaque_idempotency_key_for_context(
        "raft-graph-scope",
        proposal.authority.tenant_scope(),
        proposal.graph_name,
        None,
        &graph_fname,
    );
    let batch_id = crate::server::mutation_batch::opaque_idempotency_key_for_context(
        "raft-native",
        proposal.authority.tenant_scope(),
        &graph_scope,
        Some(proposal.authority.actor_scope()),
        proposal.authority.idempotency_key(),
    );
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        proposal.request_id,
        proposal.attempt_nonce,
        proposal.authority.tenant_scope(),
        proposal.authority.actor_scope().to_string(),
        identity_bootstrap,
        routed.epoch,
        crate::raft::RaftMutationTiming {
            fencing_token: routed.placed.then_some(routed.group_id),
            created_at_ms: committed_at_ms,
        },
    ) {
        Ok(mutation) => mutation,
        Err(error) => return Err(Response::err(proposal.request_id, error)),
    };
    Ok(crate::raft::RaftRequest {
        graph_fname,
        graph_name: proposal.graph_name.to_string(),
        graph_type: proposal.graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    })
}

/// A committed native command that still owes consensus coordination hands its
/// prepared payload to the matching coordinator. Any other result — including a
/// coordination-classified command whose result is NOT a prepared payload — is
/// already terminal and is returned verbatim.
#[cfg(feature = "raft")]
async fn coordinate_native_result(
    proposal: &NativeProposal<'_>,
    coordination: NativeCoordination,
    multi: Arc<crate::raft::multi::MultiRaft>,
    routed: crate::raft::multi::RoutedRaftHandle,
    result: ResultPayload,
) -> Response {
    match (coordination, result) {
        #[cfg(feature = "jobs")]
        (NativeCoordination::JobPublication, ResultPayload::Raw(prepared)) => {
            execute_consensus_job_publication(JobPublicationExecution {
                request_id: proposal.request_id,
                authority: proposal.authority,
                server_secret: proposal.server_secret,
                multi,
                control: routed,
                control_graph: proposal.graph_name,
                control_graph_type: proposal.graph_type,
                prepared_bytes: &prepared,
            })
            .await
        }
        (NativeCoordination::TransactionCommit, ResultPayload::Raw(prepared)) => {
            handlers::txn::tag_commit_response(
                execute_consensus_transaction(TransactionExecution {
                    state: proposal.state,
                    request_id: proposal.request_id,
                    authority: proposal.authority,
                    server_secret: proposal.server_secret,
                    multi,
                    control: routed,
                    control_graph_type: proposal.graph_type,
                    prepared_bytes: &prepared,
                })
                .await,
                handlers::txn::CommitResponseOptions {
                    replayed: false,
                    keyed: proposal.commit_keyed,
                },
            )
        }
        (coordination, terminal) => terminal_native_response(
            proposal.request_id,
            coordination,
            proposal.commit_keyed,
            terminal,
        ),
    }
}

/// A transaction-commit command whose apply already produced its terminal
/// result, with the commit response shape applied when needed.
#[cfg(feature = "raft")]
fn terminal_commit_response(
    request_id: u64,
    commit_keyed: bool,
    terminal: ResultPayload,
) -> Response {
    match terminal {
        ResultPayload::Json(value) => Response::ok(request_id, ResultPayload::Json(value)),
        terminal => handlers::txn::tag_commit_response(
            Response::ok(request_id, terminal),
            handlers::txn::CommitResponseOptions {
                replayed: false,
                keyed: commit_keyed,
            },
        ),
    }
}

/// A coordination-classified command whose apply already produced its terminal
/// result, declared as that command's result.
#[cfg(feature = "raft")]
fn terminal_native_response(
    request_id: u64,
    coordination: NativeCoordination,
    commit_keyed: bool,
    terminal: ResultPayload,
) -> Response {
    match coordination {
        NativeCoordination::TransactionCommit => {
            terminal_commit_response(request_id, commit_keyed, terminal)
        }
        _ => Response::ok(request_id, terminal),
    }
}

/// Propose through the routed group's leader and translate its reply. A write
/// failure is a stale-route redirect (the caller retries against the leader);
/// a committed entry either carries a deterministic result or is a protocol
/// violation.
#[cfg(feature = "raft")]
async fn dispatch_native_raft_write(
    proposal: &NativeProposal<'_>,
    coordination: NativeCoordination,
    multi: Arc<crate::raft::multi::MultiRaft>,
    routed: crate::raft::multi::RoutedRaftHandle,
    request: crate::raft::RaftRequest,
) -> Response {
    let request_id = proposal.request_id;
    let response = match routed.handle.client_write(request).await {
        Ok(response) => response,
        Err(error) => {
            let leader = routed.handle.current_leader().await;
            return Response::stale_route(
                request_id,
                proposal.graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                error,
            );
        }
    };
    if let Some(error) = response.native_error {
        return Response::err(request_id, error);
    }
    let Some(result) = response.native_result else {
        return Response::err(
            request_id,
            "replicated native command returned no deterministic result",
        );
    };
    coordinate_native_result(proposal, coordination, multi, routed, result).await
}

#[cfg(feature = "raft")]
pub(in crate::server::dispatch) async fn propose_native_mutation(
    state: &Arc<RwLock<ServerState>>,
    request_graph: &str,
    request_id: u64,
    verified_context: &VerifiedRequestContext,
    identity_bootstrap: bool,
    method: Method,
) -> Response {
    if capability_authority_unavailable(&method) {
        // The proposal payload carries no raw authenticated principal/session
        // envelope.  Refuse before CarrierAuthority, command construction,
        // leader routing, barriers, or any private-store mutation.
        return Response::err(
            request_id,
            crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
        );
    }
    let authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(error) => return Response::err(request_id, error),
    };
    let method = match sanitize_native_proposal(request_graph, verified_context, &authority, method)
    {
        Ok(method) => method,
        Err(error) => return Response::err(request_id, error),
    };
    let coordination = native_coordination_for(&method);
    let commit_keyed = matches!(
        &method,
        Method::Commit {
            idempotency_key: Some(_),
            ..
        }
    );
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::NativeMutationCommand::from_public_method(
        method.clone(),
        &server_secret,
    ) {
        Ok(command) => command,
        Err(_) => {
            return Response::err(
                request_id,
                "CLUSTER_MUTATION_UNAVAILABLE: no bounded native command exists",
            )
        }
    };
    let (graph_name, multi, graph_type) =
        resolve_native_proposal_route(state, request_graph, &authority, &method, &command).await;
    let Some(multi) = multi else {
        return Response::err(
            request_id,
            crate::server::state::MISSING_PLACEMENT_AUTHORITY,
        );
    };
    let Some(routed) = multi.handle_for_graph(&graph_name).await else {
        let route = multi.route_graph(&graph_name).await;
        return Response::stale_route(
            request_id,
            &graph_name,
            route.group,
            route.epoch,
            None,
            "authoritative native placement group is not running on this node",
        );
    };
    let proposal = NativeProposal {
        state,
        request_id,
        attempt_nonce: verified_context.attempt_nonce(),
        authority: &authority,
        server_secret: &server_secret,
        graph_name: &graph_name,
        graph_type,
        commit_keyed,
    };
    let request = match build_native_raft_request(&proposal, &routed, command, identity_bootstrap) {
        Ok(request) => request,
        Err(response) => return response,
    };
    dispatch_native_raft_write(&proposal, coordination, multi, routed, request).await
}
