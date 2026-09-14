//! Private transaction commit_prepared implementation.

use super::*;

pub(super) type CommitPreparedAuthorized = (
    Arc<crate::graph::GraphCore>,
    Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    String,
);

pub(super) fn commit_prepared_authorize(
    s: &ServerState,
    req_id: u64,
    caller: Option<&str>,
    txn: &GraphTxnState,
) -> Result<CommitPreparedAuthorized, Response> {
    let entry = match s.registry.get(&txn.graph) {
        Some(e) => e,
        None => {
            return Err(Response::err(
                req_id,
                format!("Graph '{}' not found", txn.graph),
            ));
        }
    };
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            &txn.graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok((entry.core.clone(), s.persistence.clone(), txn.graph.clone()))
}

/// Validate the txn's OCC read-set under the topology write barrier (without
/// publishing) and short-circuit an empty write-set. `Err(_)` carries the
/// final `Response` for [`commit_prepared`] to return immediately — either an
/// OCC-conflict rollback or a no-op ack — while `Ok` hands back the write-set
/// to durably commit plus the still-live `receipt` for the caller to
/// terminalize once that commit succeeds.
pub(super) fn commit_prepared_validate(
    req_id: u64,
    core: &crate::graph::GraphCore,
    txn: &GraphTxnState,
    receipt: TxnReceipt,
) -> Result<(Vec<Method>, TxnReceipt), Response> {
    let gtxn = core.txn();
    let ok = txn.validate(core);
    drop(gtxn);
    if !ok {
        return Err(
            match finish_txn_receipt(receipt, ResultPayload::Bool(false)) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            },
        );
    }
    let applied = txn.write_set.clone();
    if applied.is_empty() {
        return Err(
            match finish_txn_receipt(receipt, ResultPayload::Bool(true)) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            },
        );
    }
    Ok((applied, receipt))
}

/// Execute a parent that is already durably Prepared with an encrypted canonical
/// plan.  Every return before `finish_txn_receipt` leaves that plan intact for the
/// next retry; terminalization atomically erases it.
pub(super) async fn commit_prepared(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn: GraphTxnState,
    receipt: TxnReceipt,
    attempt_nonce: Option<Nonce>,
) -> Response {
    let s = state.read().await;
    let coordinator_id = receipt_coordinator_id(&receipt);

    // ── Multi-graph span (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N) ────────────────────────────
    // If the txn staged ops against a graph other than its default, evaluate the
    // span against the router. A span over ≥2 Raft groups routes through the 2PC
    // coordinator (cross-shard, all-or-nothing across groups). A multi-graph span
    // that collapses to ONE group, OR no active cluster (incl. a non-raft build),
    // applies each graph's slice locally so no staged graph is silently dropped.
    if txn.is_multi_graph() {
        drop(s);
        return commit_prepared_multi_graph(
            state,
            req_id,
            caller,
            &coordinator_id,
            txn,
            receipt,
            attempt_nonce,
        )
        .await;
    }

    // ── Cross-modal span (CONCEPT:EG-KG.txn.reader-never-sees-node) ─────────────────────────────────────
    // If the txn staged vectors or blob-refs, its single-graph commit must land
    // graph + vectors + blob-refs in ONE redb WriteTransaction (all-or-nothing).
    if txn.is_cross_modal() {
        drop(s);
        return commit_prepared_cross_modal_span(
            state,
            req_id,
            caller,
            &coordinator_id,
            txn,
            receipt,
            attempt_nonce,
        )
        .await;
    }

    let (core, persistence, graph_name) = match commit_prepared_authorize(&s, req_id, caller, &txn)
    {
        Ok(value) => value,
        Err(response) => return response,
    };
    drop(s); // release the registry read lock before taking the graph write lock.

    // Serialize with ordinary graph/query/RDF gateway mutations for the entire
    // validate → durable commit → RAM publish interval.  No lock is held during
    // client think-time; it begins only after Commit consumes the staged txn.
    let mutation_guard = crate::server::mutation_batch::lock_graph(&graph_name).await;

    // Validate under the topology write barrier, but do not publish yet.  The
    // authoritative path below commits the batch first.
    let (applied, receipt) = match commit_prepared_validate(req_id, &core, &txn, receipt) {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let tenant_scope = txn.tenant_scope.clone();
    let begin_version = txn.begin_version;

    commit_prepared_durable(CommitPreparedDurableArgs {
        req_id,
        caller,
        receipt,
        core,
        persistence,
        graph_name,
        coordinator_id,
        tenant_scope,
        begin_version,
        applied,
        mutation_guard,
        attempt_nonce,
    })
    .await
}

/// The multi-graph span branch of [`commit_prepared`]: delegate to the
/// cross-shard/multi-group committer, terminalize the receipt on success, and
/// clean up the cross-shard decision row.
pub(super) async fn commit_prepared_multi_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
    receipt: TxnReceipt,
    attempt_nonce: Option<Nonce>,
) -> Response {
    let response =
        commit_multi_graph(state, req_id, caller, coordinator_id, txn, attempt_nonce).await;
    let Some(result) = response.result.clone() else {
        return response;
    };
    match finish_txn_receipt(receipt, result) {
        Ok(result) => match cleanup_cross_shard_decision(state, coordinator_id).await {
            Ok(()) => Response::ok(req_id, result),
            Err(error) => Response::err(req_id, format!("transaction cleanup failed: {error}")),
        },
        Err(error) => Response::err(req_id, error),
    }
}

/// The cross-modal span branch of [`commit_prepared`]: delegate to the
/// single-graph cross-modal committer and terminalize the receipt on success.
pub(super) async fn commit_prepared_cross_modal_span(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
    receipt: TxnReceipt,
    attempt_nonce: Option<Nonce>,
) -> Response {
    let response =
        commit_cross_modal(state, req_id, caller, coordinator_id, txn, attempt_nonce).await;
    let Some(result) = response.result.clone() else {
        return response;
    };
    match finish_txn_receipt(receipt, result) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

/// Arguments threaded into the durable-commit tail of [`commit_prepared`],
/// once OCC validate and the empty-write-set short-circuit have passed.
/// Grouped so the split-out helper keeps a readable arity
/// (clippy::too_many_arguments). `mutation_guard` is carried through by value
/// so the per-graph serialization lock stays held for exactly the
/// validate → durable commit → RAM publish interval described on
/// [`commit_prepared`], regardless of which helper is on the stack.
pub(super) struct CommitPreparedDurableArgs<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    receipt: TxnReceipt,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    graph_name: String,
    coordinator_id: String,
    tenant_scope: String,
    begin_version: u64,
    applied: Vec<Method>,
    mutation_guard: tokio::sync::OwnedMutexGuard<()>,
    attempt_nonce: Option<Nonce>,
}

/// Compile and durably commit the transaction's write-set as one
/// `MutationBatch` (commit-before-ack), then either replay the prior result
/// or publish the write-set into the in-memory model. No response leaves this
/// function before `commit_mutation_batch` has returned.
/// Arguments for [`compile_prepared_batch`], grouped so the split-out helper
/// keeps a readable arity (clippy::too_many_arguments).
pub(super) struct CompilePreparedBatchArgs<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    authority: &'a dyn crate::server::persistence::PersistenceBackend,
    graph_name: &'a str,
    batch_id: &'a str,
    tenant_scope: &'a str,
    begin_version: u64,
    applied: Vec<Method>,
    committed_at_ms: u64,
    attempt_nonce: Option<Nonce>,
}

/// Read the authoritative graph version and compile the transaction's
/// write-set into a `MutationBatch`, ready for [`commit_prepared_durable`]'s
/// durable commit call.
pub(super) async fn compile_prepared_batch(
    args: CompilePreparedBatchArgs<'_>,
) -> Result<(crate::mutation_batch::MutationBatch, Vec<u8>), Response> {
    let CompilePreparedBatchArgs {
        req_id,
        caller,
        authority,
        graph_name,
        batch_id,
        tenant_scope,
        begin_version,
        applied,
        committed_at_ms,
        attempt_nonce,
    } = args;
    let idempotency_key = batch_id.to_string();
    let authoritative_version = match authority
        .read_mutation_graph_version(&crate::persist::sanitize(graph_name))
        .await
    {
        Ok(version) => version.unwrap_or(begin_version),
        Err(error) => {
            return Err(Response::err(
                req_id,
                format!("authoritative graph version read failed: {error}"),
            ));
        }
    };
    let principal = txn_receipt_principal(caller).map_err(|error| Response::err(req_id, error))?;
    let batch = match crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: req_id,
            attempt_nonce,
            principal: Some(&principal),
            tenant: tenant_scope,
            graph: graph_name,
            placement_epoch: 0,
            idempotency_key: &idempotency_key,
            expected_graph_version: Some(authoritative_version),
            fencing_token: None,
            created_at_ms: committed_at_ms,
            default_surface: crate::mutation_batch::MutationSurface::Transaction,
            authoritative_state: None,
        },
        applied,
    ) {
        Ok(batch) => batch,
        Err(e) => {
            return Err(Response::err(
                req_id,
                format!("MutationBatch compile failed: {e}"),
            ));
        }
    };
    let result_msgpack = match rmp_serde::to_vec_named(&ResultPayload::Bool(true)) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err(Response::err(
                req_id,
                format!("MutationBatch result encode failed: {e}"),
            ));
        }
    };
    Ok((batch, result_msgpack))
}

pub(super) async fn commit_prepared_durable(args: CommitPreparedDurableArgs<'_>) -> Response {
    let CommitPreparedDurableArgs {
        req_id,
        caller,
        receipt,
        core,
        persistence,
        graph_name,
        coordinator_id,
        tenant_scope,
        begin_version,
        applied,
        mutation_guard: _mutation_guard,
        attempt_nonce,
    } = args;
    let committed_at_ms = now_ms();
    let batch_id =
        crate::server::mutation_batch::opaque_coordinator_key("txn", &graph_name, &coordinator_id);
    let Some(authority) = persistence.as_ref() else {
        return Response::err(
            req_id,
            "authoritative MutationBatch commit requires a persistence backend",
        );
    };
    let (batch, result_msgpack) = match compile_prepared_batch(CompilePreparedBatchArgs {
        req_id,
        caller,
        authority: authority.as_ref(),
        graph_name: &graph_name,
        batch_id: &batch_id,
        tenant_scope: &tenant_scope,
        begin_version,
        applied: applied.clone(),
        committed_at_ms,
        attempt_nonce,
    })
    .await
    {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    // Authoritative commit point: all graph rows + batch status/idempotency/outbox
    // are durable before the serving projection changes.
    let committed = match commit_prepared_batch_durable(
        authority.as_ref(),
        &graph_name,
        &batch,
        &result_msgpack,
        committed_at_ms,
    )
    .await
    {
        Ok(committed) => committed,
        Err(e) => {
            return Response::err(req_id, format!("MutationBatch durable commit failed: {e}"));
        }
    };
    if committed.replayed {
        return commit_prepared_replayed(
            req_id,
            receipt,
            &core,
            authority.as_ref(),
            &graph_name,
            &committed,
        )
        .await;
    }

    // Durable commit succeeded:
    // publish the complete write-set to RAM under one graph transaction.
    {
        let mut gtxn = core.txn();
        for m in &applied {
            apply_staged(&mut gtxn, m);
        }
    }
    core.mark_dirty();

    #[cfg(feature = "metrics")]
    {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            &graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    match finish_txn_receipt(receipt, ResultPayload::Bool(true)) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

async fn commit_prepared_batch_durable(
    authority: &dyn crate::server::persistence::PersistenceBackend,
    graph_name: &str,
    batch: &crate::mutation_batch::MutationBatch,
    result_msgpack: &[u8],
    committed_at_ms: u64,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let fname = crate::persist::sanitize(graph_name);
    authority
        .commit_mutation_batch(&fname, batch, Some(result_msgpack), committed_at_ms)
        .await
        .map_err(|error| error.to_string())
}

/// The `committed.replayed` branch of [`commit_prepared_durable`]: a prior
/// attempt already durably committed this batch, so re-install the
/// already-authoritative snapshot and return its stored result instead of
/// re-applying anything.
pub(super) async fn commit_prepared_replayed(
    req_id: u64,
    receipt: TxnReceipt,
    core: &Arc<crate::graph::GraphCore>,
    authority: &dyn crate::server::persistence::PersistenceBackend,
    graph_name: &str,
    committed: &crate::mutation_batch::MutationBatchCommit,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    let Some(bytes) = committed.record.result_msgpack.as_deref() else {
        return Response::err(req_id, "committed MutationBatch has no durable result");
    };
    match decode_validated_txn_result(bytes) {
        Ok(stored) => {
            let (snapshot, version) = match authority.read_authoritative_graph_snapshot(fname).await
            {
                Ok(Some(value)) => value,
                Ok(None) => {
                    return Response::err(req_id, "committed transaction graph image is missing");
                }
                Err(error) => return Response::err(req_id, error),
            };
            if let Err(error) = core.install_committed_snapshot(snapshot, version) {
                return Response::err(req_id, error);
            }
            match finish_txn_receipt(receipt, stored) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            }
        }
        Err(error) => Response::err(req_id, error),
    }
}

fn decode_validated_txn_result(bytes: &[u8]) -> Result<ResultPayload, String> {
    let result = decode_txn_result(bytes)?;
    validate_txn_commit_result(&result)?;
    Ok(result)
}
