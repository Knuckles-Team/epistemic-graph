//! Private transaction commit_multi_graph implementation.

use super::*;

pub(super) struct CommitSlice {
    graph_name: String,
    graph_fname: String,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    graph_type: crate::protocol::GraphType,
    methods: Vec<Method>,
}

/// Commit a MULTI-GRAPH staged transaction (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N wire). Builds a
/// per-graph slice from the default-graph write-set + each `extra_writes` graph
/// (validating existence + Write access on each), then:
///
///   * **Cross-shard (≥2 Raft groups) + active cluster** → route the staged write-set
///     through [`crate::raft::cross_shard_txn::CrossShardCoordinator::commit_cross_shard`]:
///     the 2PC coordinator prepares each participant group durably (commit-before-vote),
///     logs ONE durable decision (the atomic commit point), then applies every slice
///     through its group's Raft `client_write`. All-or-nothing across groups,
///     recovery-resolvable. `Bool(true)` on COMMIT, `Bool(false)` on ABORT.
///   * **Single-group collapse, OR no active cluster (incl. a non-raft build)** →
///     commit each graph slice as a deterministic child MutationBatch under a durable
///     multi-graph coordinator saga. Re-entry resumes idempotent children and records
///     one terminal parent receipt, so no staged graph is silently dropped.
pub(super) async fn commit_multi_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
    attempt_nonce: Option<Nonce>,
) -> Response {
    // Build the per-graph slices: the default graph (its write_set) + every extra
    // graph. Each graph must exist + the caller must hold Write on it.
    let mut slices: Vec<CommitSlice> = Vec::new();
    {
        let s = state.read().await;
        let mut per_graph: Vec<(String, Vec<Method>)> =
            vec![(txn.graph.clone(), txn.write_set.clone())];
        for (g, ops) in &txn.extra_writes {
            per_graph.push((g.clone(), ops.clone()));
        }
        per_graph.sort_by(|left, right| left.0.cmp(&right.0));
        for (graph_name, methods) in per_graph {
            let entry = match s.registry.get(&graph_name) {
                Some(e) => e,
                None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
            };
            if !consensus_apply_is_authorized() {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    caller,
                    &graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req_id, denied);
                }
            }
            slices.push(CommitSlice {
                graph_fname: crate::persist::sanitize(&graph_name),
                graph_type: entry.graph_type,
                graph_name,
                methods,
            });
        }
    }

    commit_recoverable_slices(state, req_id, caller, coordinator_id, slices, attempt_nonce).await
}

/// Commit an already-authorized coordinator plan through the same recoverable
/// graph-slice authority as a staged multi-graph transaction. This is the only
/// auxiliary-carrier entry point for complete graph images: clustered spans use
/// the retained-decision 2PC path; local/single-group spans use deterministic
/// child MutationBatches subordinate to the caller's durable parent receipt.
pub(crate) async fn commit_coordinated_graph_methods(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    graph_methods: Vec<(String, crate::protocol::GraphType, Vec<Method>)>,
) -> Response {
    commit_coordinated_graph_methods_with_nonce(
        state,
        req_id,
        caller,
        coordinator_id,
        graph_methods,
        None,
    )
    .await
}

pub(crate) async fn commit_coordinated_graph_methods_with_nonce(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    graph_methods: Vec<(String, crate::protocol::GraphType, Vec<Method>)>,
    attempt_nonce: Option<Nonce>,
) -> Response {
    let mut slices = Vec::with_capacity(graph_methods.len());
    {
        let current = state.read().await;
        for (graph_name, graph_type, methods) in graph_methods {
            let Some(entry) = current.registry.get(&graph_name) else {
                return Response::err(req_id, format!("Graph '{graph_name}' not found"));
            };
            if entry.graph_type != graph_type {
                return Response::err(
                    req_id,
                    format!("Graph '{graph_name}' changed type during coordination"),
                );
            }
            if !consensus_apply_is_authorized() {
                if let Err(denied) = check_graph_access(
                    &current.isolation,
                    caller,
                    &graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req_id, denied);
                }
            }
            slices.push(CommitSlice {
                graph_fname: crate::persist::sanitize(&graph_name),
                graph_type,
                graph_name,
                methods,
            });
        }
    }
    slices.sort_by(|left, right| left.graph_name.cmp(&right.graph_name));
    commit_recoverable_slices(state, req_id, caller, coordinator_id, slices, attempt_nonce).await
}

pub(super) async fn commit_recoverable_slices(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    slices: Vec<CommitSlice>,
    attempt_nonce: Option<Nonce>,
) -> Response {
    // ── CROSS-SHARD: a multi-graph span over ≥2 Raft groups routes through 2PC ──
    #[cfg(feature = "raft")]
    {
        let (multi, backend) = {
            let s = state.read().await;
            (s.multi_raft.clone(), s.persistence.clone())
        };
        let recoverable_in_flight = backend
            .as_ref()
            .and_then(|value| value.as_redb())
            .map(|redb| {
                let id = cross_shard_transaction_id(coordinator_id);
                redb.xshard_decision_retain_get(&id)
            })
            .transpose();
        let recoverable_in_flight = match recoverable_in_flight {
            Ok(value) => value.unwrap_or(false),
            Err(error) => {
                return Response::err(
                    req_id,
                    format!("cross-shard recovery lookup failed: {error}"),
                );
            }
        };
        if let Some(multi) = multi {
            if recoverable_in_flight
                || multi
                    .router()
                    .is_cross_shard(slices.iter().map(|slice| slice.graph_name.as_str()))
            {
                return commit_cross_shard(state, req_id, caller, coordinator_id, multi, slices)
                    .await;
            }
        } else if recoverable_in_flight {
            return Response::err(
                req_id,
                "cross-shard transaction recovery is waiting for the Raft groups",
            );
        }
    }

    // ── Single-group collapse OR no cluster: apply each slice locally ──
    apply_slices_locally(
        state,
        req_id,
        caller,
        coordinator_id,
        &slices,
        attempt_nonce,
    )
    .await
}

/// Route a cross-shard multi-graph txn through the 2PC coordinator (CONCEPT:EG-KG.txn.routes-cross-shard-txn).
#[cfg(feature = "raft")]
pub(super) async fn commit_cross_shard(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    coordinator_id: &str,
    multi: std::sync::Arc<crate::raft::multi::MultiRaft>,
    slices: Vec<CommitSlice>,
) -> Response {
    use crate::raft::cross_shard_txn::{
        CrossShardCoordinator, CrossShardTxn, GraphSlice, TxnOutcome,
    };

    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "cross-shard txn requires a persistence backend");
    };
    // GOC-13 fencing: capture each participant's CURRENT placement route (the same
    // `(group, epoch, fencing_token)` triple `execute_consensus_transaction`'s
    // `build_consensus_transaction_fanout` already captures for its own participant
    // fanout) alongside the slice, so `CrossShardCoordinator::prepare_participant`
    // can reject a participant whose placement moved between when this slice was
    // built and when it durably prepares (CONCEPT:EG-KG.txn.harness-crash "stale/
    // fenced participant" gap).
    let mut x_slices: Vec<GraphSlice> = Vec::with_capacity(slices.len());
    for s in slices {
        let route = multi.route_graph(&s.graph_name).await;
        x_slices.push(GraphSlice {
            graph_name: s.graph_name,
            graph_fname: s.graph_fname,
            graph_type: s.graph_type,
            methods: s.methods,
            placement_epoch: route.epoch,
            fencing_token: route.placed.then_some(route.fencing_token()),
        });
    }
    let coord = CrossShardCoordinator::new(multi, backend.clone());
    let xtxn = CrossShardTxn {
        txn_id: cross_shard_transaction_id(coordinator_id),
        slices: x_slices,
    };
    let result = match coord.commit_cross_shard_recoverable(&xtxn).await {
        Ok(TxnOutcome::Committed) => ResultPayload::Bool(true),
        Ok(TxnOutcome::Aborted) => ResultPayload::Bool(false),
        Err(e) => return Response::err(req_id, format!("cross-shard commit failed: {e}")),
    };
    Response::ok(req_id, result)
}

/// Apply each graph's slice locally through a deterministic child MutationBatch
/// (CONCEPT:EG-KG.txn.routes-cross-shard-txn — the single-group / single-node
/// multi-graph path), subordinate to the prepared/committed transaction receipt opened
/// by [`commit`]. Used when the span collapses to one Raft group or no cluster is
/// active. Returns `Bool(true)`.
pub(super) async fn apply_slices_locally(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    slices: &[CommitSlice],
    attempt_nonce: Option<Nonce>,
) -> Response {
    let principal = match txn_receipt_principal(caller) {
        Ok(principal) => principal,
        Err(error) => return Response::err(req_id, error),
    };
    apply_authorized_slices(
        state,
        req_id,
        &principal,
        coordinator_id,
        slices,
        attempt_nonce,
    )
    .await
}

async fn apply_authorized_slices(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    principal: &str,
    coordinator_id: &str,
    slices: &[CommitSlice],
    attempt_nonce: Option<Nonce>,
) -> Response {
    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "multi-graph commit requires durable persistence");
    };
    for slice in slices {
        let core = {
            let s = state.read().await;
            match s.registry.get(&slice.graph_name) {
                Some(e) => e.core.clone(),
                None => {
                    return Response::err(
                        req_id,
                        format!("Graph '{}' not found", slice.graph_name),
                    );
                }
            }
        };
        let child_id = crate::server::mutation_batch::opaque_coordinator_key(
            "multi-graph-child",
            &slice.graph_name,
            coordinator_id,
        );
        if let Err(error) = crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
            crate::server::mutation_batch::InternalGraphCommitRequest::new(
                Some(&backend),
                &core,
                crate::server::mutation_batch::CommitOrigin {
                    request_id: req_id,
                    principal: Some(principal),
                },
                &slice.graph_name,
                &child_id,
                slice.methods.clone(),
                &ResultPayload::Bool(true),
            )
            .with_attempt_nonce(attempt_nonce),
        )
        .await
        {
            return Response::err(req_id, format!("multi-graph child commit failed: {error}"));
        }
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}
