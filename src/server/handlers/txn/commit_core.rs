//! Private transaction commit_core implementation.

use super::*;

/// The declared `Commit` result for a successful commit's boolean outcome.
/// `keyed_replay` is `None` for a commit without a caller idempotency key, else whether
/// this answer replayed an earlier commit under that key.
pub(super) fn commit_outcome(
    result: Option<ResultPayload>,
    keyed_replay: Option<bool>,
) -> Result<ResultPayload, String> {
    let Some(ResultPayload::Bool(committed)) = result else {
        return Err("transaction commit answered a non-boolean outcome".to_string());
    };
    let outcome = match keyed_replay {
        Some(replayed) => eg_types::result_contract::transactions::CommitOutcome::Keyed {
            committed,
            replayed,
        },
        None => eg_types::result_contract::transactions::CommitOutcome::Unkeyed(committed),
    };
    ResultPayload::of::<eg_types::result_contract::transactions::Commit>(outcome)
}

pub(super) async fn commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    idempotency_key: Option<&str>,
    attempt_nonce: Option<Nonce>,
    tenant_scope: Option<&str>,
) -> Response {
    if consensus_apply_is_authorized() {
        return Response::err(
            req_id,
            "clustered Commit requires the typed participant protocol",
        );
    }
    let keyed = idempotency_key.is_some();
    // Serialize every first attempt/retry for this opaque parent.  This also closes
    // the restart race where two callers simultaneously discover the same Prepared
    // plan and try to resume its remaining children.
    let parent_id = commit_receipt_id(txn_id, idempotency_key, tenant_scope);
    let _coordinator_guard = crate::server::mutation_batch::lock_graph(&parent_id).await;

    let (open, persistence, open_map) = {
        let s = state.read().await;
        (
            s.open_txns.remove(txn_id),
            s.persistence.clone(),
            s.open_txns.clone(),
        )
    };
    let (txn, receipt) = match open {
        Some((_id, txn_mutex)) => {
            match commit_open_txn(
                state,
                CommitOpenTxnArgs {
                    req_id,
                    caller,
                    txn_id,
                    idempotency_key,
                    keyed,
                    txn_mutex,
                    persistence,
                    open_map,
                    attempt_nonce,
                },
            )
            .await
            {
                Ok(pair) => pair,
                Err(response) => return response,
            }
        }
        None => match commit_resume_txn(
            state,
            req_id,
            caller,
            CommitReceiptKey {
                txn_id,
                idempotency_key,
                expected_tenant: tenant_scope,
            },
            keyed,
            persistence,
            attempt_nonce,
        )
        .await
        {
            Ok(pair) => pair,
            Err(response) => return response,
        },
    };

    let response = commit_prepared(state, req_id, caller, txn, receipt, attempt_nonce).await;
    tag_commit_response(
        response,
        CommitResponseOptions {
            replayed: false,
            keyed,
        },
    )
}

/// Arguments for [`commit_open_txn`], grouped so the split-out helper keeps a
/// readable arity (clippy::too_many_arguments).
pub(super) struct CommitOpenTxnArgs<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    txn_id: &'a str,
    idempotency_key: Option<&'a str>,
    keyed: bool,
    txn_mutex: parking_lot::Mutex<GraphTxnState>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    open_map: Arc<dashmap::DashMap<String, parking_lot::Mutex<GraphTxnState>>>,
    attempt_nonce: Option<Nonce>,
}

/// The `commit`-time path for a txn still open in RAM: authorize the staged
/// plan, atomically seal it as Prepared (or return its replay), and hand the
/// staged state + receipt back to the caller. `Err(_)` carries the final,
/// already `tag_commit_response`-tagged `Response` for an early return.
pub(super) async fn commit_open_txn(
    state: &Arc<RwLock<ServerState>>,
    args: CommitOpenTxnArgs<'_>,
) -> Result<(GraphTxnState, TxnReceipt), Response> {
    let CommitOpenTxnArgs {
        req_id,
        caller,
        txn_id,
        idempotency_key,
        keyed,
        txn_mutex,
        persistence,
        open_map,
        attempt_nonce,
    } = args;
    let txn = txn_mutex.into_inner();
    // Until parent preparation succeeds, preserve the historical retry contract:
    // an encryption/configuration/fsync error puts staging back in RAM.  Once the
    // atomic Prepared+encrypted-plan commit returns, the durable plan becomes the
    // sole mutable authority and the transaction is frozen.
    let mut restore = TxnRestoreGuard::new(open_map, txn_id, txn.clone());
    if let Err(error) = authorize_txn_plan(state, caller, &txn).await {
        return Err(Response::err(req_id, error));
    }
    // B-9: `begin_txn_receipt` resolves the receipt's identity via
    // `commit_receipt_id` -- keyed by `idempotency_key` when the caller
    // supplied one (so THIS retry, even under a freshly re-staged `txn_id`,
    // lands on the SAME durable receipt row as a prior attempt that used the
    // same key), or by `txn_id` alone exactly as before when it did not.
    let (receipt, replayed) = match begin_txn_receipt(
        persistence.clone(),
        req_id,
        caller,
        txn_id,
        &txn,
        idempotency_key,
        attempt_nonce,
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    restore.complete();
    if let Some(result) = replayed {
        let parent_id = commit_receipt_id(txn_id, idempotency_key, Some(txn.tenant_scope.as_str()));
        if let Err(error) = cleanup_cross_shard_decision(state, &parent_id).await {
            return Err(Response::err(
                req_id,
                format!("transaction cleanup failed: {error}"),
            ));
        }
        return Err(tag_commit_response(
            Response::ok(req_id, result),
            CommitResponseOptions {
                replayed: true,
                keyed,
            },
        ));
    }
    Ok((txn, receipt))
}

/// The `commit`-time path when no matching txn is open in RAM: reconcile a
/// crash-recovered commit, or resume a durably Prepared parent. `Err(_)`
/// carries the final, already `tag_commit_response`-tagged `Response` for an
/// early return.
pub(super) async fn commit_resume_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    receipt_key: CommitReceiptKey<'_>,
    keyed: bool,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    attempt_nonce: Option<Nonce>,
) -> Result<(GraphTxnState, TxnReceipt), Response> {
    let CommitReceiptKey {
        txn_id,
        idempotency_key,
        expected_tenant,
    } = receipt_key;
    match reconcile_committed_txn(
        state,
        req_id,
        caller,
        txn_id,
        idempotency_key,
        expected_tenant,
        attempt_nonce,
    )
    .await
    {
        Ok(Some(response)) => {
            let parent_id = commit_receipt_id(txn_id, idempotency_key, expected_tenant);
            if let Err(error) = cleanup_cross_shard_decision(state, &parent_id).await {
                return Err(Response::err(
                    req_id,
                    format!("transaction cleanup failed: {error}"),
                ));
            }
            return Err(tag_commit_response(
                response,
                CommitResponseOptions {
                    replayed: true,
                    keyed,
                },
            ));
        }
        Ok(None) => {}
        Err(error) => {
            return Err(Response::err(
                req_id,
                format!("transaction receipt reconciliation failed: {error}"),
            ));
        }
    }
    let resumed = match resume_txn_receipt(
        persistence,
        req_id,
        caller,
        txn_id,
        idempotency_key,
        expected_tenant,
        attempt_nonce,
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    let Some((receipt, replayed, recovered)) = resumed else {
        return Err(Response::err(
            req_id,
            format!("unknown transaction '{}'", txn_id),
        ));
    };
    if let Some(result) = replayed {
        let parent_id = commit_receipt_id(txn_id, idempotency_key, expected_tenant);
        if let Err(error) = cleanup_cross_shard_decision(state, &parent_id).await {
            return Err(Response::err(
                req_id,
                format!("transaction cleanup failed: {error}"),
            ));
        }
        return Err(tag_commit_response(
            Response::ok(req_id, result),
            CommitResponseOptions {
                replayed: true,
                keyed,
            },
        ));
    }
    let Some(txn) = recovered else {
        return Err(Response::err(
            req_id,
            "prepared transaction has no recovery plan",
        ));
    };
    Ok((txn, receipt))
}
