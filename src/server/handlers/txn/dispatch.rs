//! Private transaction dispatch implementation.

use super::*;

pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Result<Response, Method> {
    let ctx = match try_handle_prepare(state, req_id, verified_context, &method).await {
        Ok(ctx) => ctx,
        Err(response) => return Ok(response),
    };
    dispatch_txn_method(state, req_id, caller, method, ctx).await
}

/// Authority + ownership context shared by every `try_handle` dispatch arm:
/// the verified carrier authority, the ownership check on an existing open
/// txn, and the derived-read/measurement authorities some stages require.
/// Split out purely to keep [`try_handle`] itself thin — [`dispatch_txn_method`]
/// below stays a plain exhaustive `match` (not a HashMap dispatch table), so
/// the compiler keeps proving every `Method` variant is routed.
pub(super) struct TxnMethodContext {
    carrier_authority: CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    /// Only read by the sparql/query/epistemic derived-read txn stages in
    /// [`dispatch_txn_method`] (each independently feature-gated); a slim
    /// build with none of them enabled never reads this field, mirroring the
    /// original inline `let _ = &derived_read_authority;`.
    #[allow(dead_code)]
    derived_read_authority: Option<GraphReadAuthority>,
    #[cfg(feature = "tsdb")]
    measurement_authority: Option<CarrierAuthority>,
}

pub(super) async fn try_handle_prepare(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: &Method,
) -> Result<TxnMethodContext, Response> {
    let carrier_authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    let txn_owner = carrier_authority.owner_scope();
    // A transaction id is a routing handle, never a bearer credential. Bind every
    // in-memory stage/read/rollback operation to the verified tenant+actor that
    // opened it.
    if let Some(txn_id) = method_txn_id(method) {
        let state_guard = state.read().await;
        if let Some(entry) = state_guard.open_txns.get(txn_id) {
            if entry.value().lock().agent != txn_owner {
                crate::metrics::access_denied();
                return Err(Response::err(
                    req_id,
                    "ACCESS_DENIED: transaction is not owned by caller",
                ));
            }
        };
    }
    let derived_read_authority = if is_derived_read_stage(method) {
        let state_guard = state.read().await;
        match GraphReadAuthority::from_verified(verified_context, &state_guard.isolation) {
            Ok(authority) => Some(authority),
            Err(error) => return Err(Response::err(req_id, error)),
        }
    } else {
        None
    };
    #[cfg(feature = "tsdb")]
    let measurement_authority = if matches!(method, Method::TxnAddMeasurement { .. }) {
        Some(carrier_authority.clone())
    } else {
        None
    };
    Ok(TxnMethodContext {
        carrier_authority,
        attempt_nonce: verified_context.attempt_nonce(),
        derived_read_authority,
        #[cfg(feature = "tsdb")]
        measurement_authority,
    })
}

pub(super) async fn dispatch_txn_method(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    method: Method,
    ctx: TxnMethodContext,
) -> Result<Response, Method> {
    let TxnMethodContext {
        carrier_authority,
        attempt_nonce,
        derived_read_authority,
        #[cfg(feature = "tsdb")]
        measurement_authority,
    } = ctx;
    let txn_owner = carrier_authority.owner_scope();
    let lifecycle_receipt = if is_txn_lifecycle_method(&method) {
        match begin_txn_lifecycle_receipt(state, req_id, caller, &carrier_authority, &method).await
        {
            Ok(receipt) => Some(receipt),
            Err(error) => return Ok(Response::err(req_id, error)),
        }
    } else {
        None
    };
    #[cfg(feature = "redb")]
    if let Some(receipt) = lifecycle_receipt.as_ref() {
        if let Some(result) = receipt.saga.replayed.clone() {
            if let Err(error) =
                validate_txn_lifecycle_replay(state, &method, txn_owner, &result).await
            {
                return Ok(Response::err(req_id, error));
            }
            return Ok(Response::ok(req_id, result));
        }
    }
    let dispatch_args = TxnDispatchArgs {
        state,
        req_id,
        caller,
        txn_owner,
        carrier_authority: &carrier_authority,
        attempt_nonce,
        derived_read_authority: derived_read_authority.as_ref(),
        #[cfg(feature = "tsdb")]
        measurement_authority: measurement_authority.as_ref(),
    };
    let lifecycle_method = method.clone();
    let response = dispatch_txn_operation(&dispatch_args, method).await;
    let response = match (lifecycle_receipt, response) {
        (Some(receipt), Ok(response)) if response.error.is_none() => {
            let Some(result) = response.result.clone() else {
                return Ok(Response::err(
                    req_id,
                    "transaction lifecycle handler returned no result",
                ));
            };
            fault_after_txn_lifecycle_effect(req_id);
            match finish_txn_lifecycle_receipt(receipt, result, Some(&lifecycle_method)) {
                Ok(result) => Ok(Response::ok(req_id, result)),
                Err(error) => Ok(Response::err(req_id, error)),
            }
        }
        (Some(_receipt), Ok(response)) => Ok(response),
        (Some(_receipt), Err(other)) => Err(other),
        (None, response) => response,
    }?;
    Ok(response)
}

struct TxnDispatchArgs<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &'a str,
    txn_owner: &'a str,
    carrier_authority: &'a CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    derived_read_authority: Option<&'a GraphReadAuthority>,
    #[cfg(feature = "tsdb")]
    measurement_authority: Option<&'a CarrierAuthority>,
}

async fn dispatch_txn_operation(
    args: &TxnDispatchArgs<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match dispatch_txn_core(args, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    dispatch_txn_extended(args, method).await
}

async fn dispatch_txn_core(args: &TxnDispatchArgs<'_>, method: Method) -> Result<Response, Method> {
    let TxnDispatchArgs {
        state,
        req_id,
        caller,
        txn_owner,
        carrier_authority,
        attempt_nonce,
        ..
    } = args;
    match method {
        Method::BeginTxn { graph, isolation } => Ok(begin_txn(
            state,
            *req_id,
            Some(caller),
            txn_owner,
            carrier_authority.tenant_scope(),
            graph,
            isolation.as_deref(),
        )
        .await),
        Method::Commit {
            txn_id,
            idempotency_key,
        } => Ok(commit_with_owner(
            state,
            *req_id,
            Some(caller),
            &txn_id,
            idempotency_key.as_deref(),
            *attempt_nonce,
            Some(carrier_authority.tenant_scope()),
            Some(carrier_authority.owner_scope()),
        )
        .await),
        Method::Rollback { txn_id } => Ok(rollback(state, *req_id, &txn_id).await),
        other => dispatch_txn_stage(args, other).await,
    }
}

async fn dispatch_txn_stage(
    args: &TxnDispatchArgs<'_>,
    method: Method,
) -> Result<Response, Method> {
    let TxnDispatchArgs { state, req_id, .. } = args;
    match method {
        Method::TxnAddNode {
            txn_id,
            node_id,
            properties_msgpack,
            graph,
        } => Ok(stage(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            Method::AddNode {
                node_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveNode {
            txn_id,
            node_id,
            graph,
        } => Ok(stage(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            Method::RemoveNode { node_id },
        )
        .await),
        Method::TxnAddEdge {
            txn_id,
            source_id,
            target_id,
            properties_msgpack,
            graph,
        } => Ok(stage(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveEdge {
            txn_id,
            source_id,
            target_id,
            graph,
        } => Ok(stage(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            Method::RemoveEdge {
                source_id,
                target_id,
            },
        )
        .await),
        Method::TxnCas {
            txn_id,
            node_id,
            conditions_msgpack,
            updates_msgpack,
            graph,
        } => Ok(stage(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            },
        )
        .await),
        Method::TxnAddEmbedding {
            txn_id,
            node_id,
            embedding,
            graph,
        } => Ok(stage_vector(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            node_id,
            embedding,
        )
        .await),
        Method::TxnBlobRef {
            txn_id,
            node_id,
            digest,
            graph,
        } => Ok(stage_blob_ref(state, *req_id, &txn_id, graph.as_deref(), node_id, digest).await),
        other => Err(other),
    }
}

async fn dispatch_txn_extended(
    args: &TxnDispatchArgs<'_>,
    method: Method,
) -> Result<Response, Method> {
    let TxnDispatchArgs {
        state,
        req_id,
        derived_read_authority,
        #[cfg(feature = "tsdb")]
        measurement_authority,
        ..
    } = args;
    match method {
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement {
            txn_id,
            series,
            points,
            graph,
        } => Ok(stage_measurement(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            series,
            points,
            measurement_authority.expect("TxnAddMeasurement requires carrier authority"),
        )
        .await),
        #[cfg(feature = "owl")]
        Method::TxnAxiom {
            txn_id,
            turtle,
            graph,
        } => Ok(stage_axiom(state, *req_id, &txn_id, graph.as_deref(), turtle).await),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct {
            txn_id,
            sparql,
            graph,
        } => Ok(stage_construct(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            sparql,
            derived_read_authority.expect("TxnConstruct is a derived read stage"),
        )
        .await),
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback {
            txn_id,
            plan,
            anchor_id,
            relationship,
            graph,
        } => Ok(stage_plan_writeback(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            PlanWritebackArgs {
                plan,
                anchor_id,
                relationship,
            },
            derived_read_authority.expect("TxnPlanWriteback is a derived read stage"),
        )
        .await),
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief {
            txn_id,
            node_id,
            graph,
        } => Ok(stage_materialize_belief(
            state,
            *req_id,
            &txn_id,
            graph.as_deref(),
            node_id,
            derived_read_authority.expect("TxnMaterializeBelief is a derived read stage"),
        )
        .await),
        other => Err(other),
    }
}

pub(super) fn method_txn_id(method: &Method) -> Option<&str> {
    match method {
        Method::TxnAddNode { txn_id, .. }
        | Method::TxnRemoveNode { txn_id, .. }
        | Method::TxnAddEdge { txn_id, .. }
        | Method::TxnRemoveEdge { txn_id, .. }
        | Method::TxnCas { txn_id, .. }
        | Method::TxnAddEmbedding { txn_id, .. }
        | Method::TxnBlobRef { txn_id, .. }
        | Method::Rollback { txn_id } => Some(txn_id),
        Method::Commit { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "owl")]
        Method::TxnAxiom { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { txn_id, .. } => Some(txn_id),
        _ => None,
    }
}

pub(super) fn is_derived_read_stage(method: &Method) -> bool {
    // Matched only by the sparql/query/epistemic arms below (each independently
    // feature-gated); a slim build with none of them enabled still needs this
    // parameter to compile.
    let _ = method;
    #[cfg(feature = "sparql")]
    if matches!(method, Method::TxnConstruct { .. }) {
        return true;
    }
    #[cfg(feature = "query")]
    if matches!(method, Method::TxnPlanWriteback { .. }) {
        return true;
    }
    #[cfg(feature = "epistemic")]
    if matches!(method, Method::TxnMaterializeBelief { .. }) {
        return true;
    }
    false
}
