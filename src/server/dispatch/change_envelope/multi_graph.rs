use super::*;
use eg_types::result_contract::transactions as txn_results;

/// Apply a batched cross-graph write (CONCEPT:EG-KG.storage.multi-graph-batch-write).
///
/// `batches_msgpack` decodes to `Vec<(graph_name, operations_msgpack)>` where each
/// inner blob is exactly a [`Method::BatchUpdate`] payload. Every sub-batch is
/// dispatched through the ordinary per-graph write path
/// ([`dispatch_graph_op`]) CONCURRENTLY on the async runtime, so distinct graphs
/// take DISTINCT per-graph write locks and commit across the K redb shard writers
/// in parallel — the client pays ONE round-trip instead of N that each re-acquire
/// a lock. Reuses the existing `BatchUpdate` primitive, so persistence /
/// Raft / CDC / access-control all apply per sub-batch exactly as a normal batch.
///
/// The reply is `{"results": {graph: <batch_result>}, "errors": {graph: msg}}`;
/// one graph's failure never aborts the others (partial-success contract).
#[cfg(feature = "redb")]
pub(super) async fn multi_graph_batch_update(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    batches_msgpack: &[u8],
) -> Response {
    let batches = match decode_multi_graph_batches(batches_msgpack) {
        Ok(batches) => batches,
        Err(error) => return Response::err(req_id, error),
    };
    let backend = {
        let s = timed_read(state).await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "multi-graph batch requires durable persistence");
    };
    let Some(redb) = backend.as_redb() else {
        return Response::err(req_id, "multi-graph batch requires durable redb");
    };
    #[cfg(feature = "raft")]
    let clustered = timed_read(state).await.multi_raft.is_some();
    #[cfg(not(feature = "raft"))]
    let clustered = false;
    let saga = match begin_multi_graph_saga(
        redb,
        req_id,
        caller,
        verified_context.attempt_nonce(),
        batches_msgpack,
        clustered,
    ) {
        Ok(saga) => saga,
        Err(response) => return response,
    };
    let report = if batches.is_empty() {
        txn_results::MultiGraphBatchReport::default()
    } else {
        run_multi_graph_batches(state, req_id, caller, verified_context, batches).await
    };
    finish_multi_graph_report(redb, req_id, saga, report)
}

/// Encode the assembled partial-success report as the declared result and close the
/// saga (when there is one) over it.
#[cfg(feature = "redb")]
fn finish_multi_graph_report(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    saga: Option<handlers::admin::AdminSaga>,
    report: txn_results::MultiGraphBatchReport,
) -> Response {
    match ResultPayload::of::<txn_results::MultiGraphBatchUpdate>(report) {
        Ok(result) => finish_multi_graph_batch(redb, req_id, saga, result),
        Err(error) => Response::err(req_id, error),
    }
}

/// The declared JSON body of a successful in-process response; its error, or the
/// named refusal when it answered an invalid or no body.
#[cfg(feature = "redb")]
fn declared_json_response<T: serde::de::DeserializeOwned>(
    response: Response,
    invalid: &str,
    absent: &str,
) -> Result<T, String> {
    if let Some(error) = response.error {
        return Err(error);
    }
    match response.result {
        Some(ResultPayload::Json(value)) => {
            serde_json::from_value(value).map_err(|error| format!("{invalid}: {error}"))
        }
        _ => Err(absent.to_string()),
    }
}

/// Open the durable admin saga that makes a single-node multi-graph batch
/// idempotent. A clustered node has no saga (raft already orders each
/// sub-batch). `Err` is this request's final response — a begin failure, or the
/// replayed result of an attempt that already committed.
#[cfg(feature = "redb")]
fn begin_multi_graph_saga(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    batches_msgpack: &[u8],
    clustered: bool,
) -> Result<Option<handlers::admin::AdminSaga>, Response> {
    if clustered {
        return Ok(None);
    }
    let method = Method::MultiGraphBatchUpdate {
        batches_msgpack: batches_msgpack.to_vec(),
    };
    let saga = match handlers::admin::begin_admin_saga_with_nonce(
        redb,
        req_id,
        caller,
        &method,
        crate::mutation_batch::DurabilityDomain::MultiGraph,
        attempt_nonce,
    ) {
        Ok(saga) => saga,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    if let Some(result) = saga.replayed.clone() {
        return Err(Response::ok(req_id, result));
    }
    Ok(Some(saga))
}

/// Close the saga (when there is one) over the assembled partial-success reply.
#[cfg(feature = "redb")]
fn finish_multi_graph_batch(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    saga: Option<handlers::admin::AdminSaga>,
    result: ResultPayload,
) -> Response {
    let Some(saga) = saga else {
        return Response::ok(req_id, result);
    };
    match handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

/// Record one sub-batch's outcome. A graph lands in `results` with its
/// `BatchUpdate` report, or in `errors` -- its own failure, or a success that
/// answered no valid report -- so the reply names every graph exactly once.
#[cfg(feature = "redb")]
fn record_multi_graph_result(
    report: &mut txn_results::MultiGraphBatchReport,
    graph: String,
    response: Response,
) {
    match declared_json_response::<txn_results::BatchUpdateReport>(
        response,
        "sub-batch answered an invalid BatchUpdate report",
        "sub-batch answered no BatchUpdate report",
    ) {
        Ok(batch) => {
            report.results.insert(graph, batch);
        }
        Err(error) => {
            report.errors.insert(graph, error);
        }
    }
}

/// Fan each sub-batch onto its own task so distinct graphs apply concurrently.
/// The `Arc<RwLock<ServerState>>` is cheaply cloned; `dispatch_graph_op` takes
/// the registry read-lock only briefly then releases it before the per-graph
/// write lock, so the writes overlap across shard writers.
#[cfg(feature = "redb")]
async fn run_multi_graph_batches(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    batches: Vec<(String, serde_bytes::ByteBuf)>,
) -> txn_results::MultiGraphBatchReport {
    let mut report = txn_results::MultiGraphBatchReport::default();
    let caller_owned = caller.map(str::to_string);
    let mut set = tokio::task::JoinSet::new();
    for (graph, ops) in batches {
        let state = Arc::clone(state);
        let caller_owned = caller_owned.clone();
        let verified_context = VerifiedRequestContext::clone(verified_context);
        set.spawn(async move {
            let resp = dispatch_graph_op(
                &state,
                &graph,
                req_id,
                caller_owned.as_deref(),
                &verified_context,
                Method::BatchUpdate {
                    operations_msgpack: ops.into_vec(),
                },
            )
            .await;
            (graph, resp)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((graph, resp)) => record_multi_graph_result(&mut report, graph, resp),
            Err(join_err) => {
                // A panicked/cancelled sub-batch task — surface it, don't abort.
                let _ = join_err;
                let key = format!("__join_error_{}", report.errors.len());
                report
                    .errors
                    .insert(key, "sub-batch execution failed".to_string());
            }
        }
    }
    report
}

#[cfg(not(feature = "redb"))]
pub(super) async fn multi_graph_batch_update(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _batches_msgpack: &[u8],
) -> Response {
    Response::err(
        req_id,
        "multi-graph batch requires a build with durable redb support",
    )
}

const MAX_MULTI_GRAPH_BATCHES: usize = 256;
const MAX_MULTI_GRAPH_NAME_BYTES: usize = 512;
const MAX_MULTI_GRAPH_OPERATIONS_BYTES: usize = 32 * 1024 * 1024;
const MAX_MULTI_GRAPH_TOTAL_OPERATIONS_BYTES: usize = 64 * 1024 * 1024;
const MAX_MULTI_GRAPH_OPERATION_ITEMS: usize = 500_000;

pub(super) fn decode_multi_graph_batches(
    batches_msgpack: &[u8],
) -> Result<Vec<(String, serde_bytes::ByteBuf)>, String> {
    // The outer request preflight protects this decoder on the served path. Keep
    // the check local as well so direct unit/library callers cannot bypass it.
    let batches: Vec<(String, serde_bytes::ByteBuf)> = eg_types::msgpack::decode_bounded(
        batches_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_NESTED_MSGPACK_BYTES,
            MAX_NESTED_MSGPACK_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid multi-graph batch payload".to_string())?;
    if batches.len() > MAX_MULTI_GRAPH_BATCHES {
        return Err("multi-graph batch count exceeds the resource limit".to_string());
    }
    let mut names = std::collections::HashSet::with_capacity(batches.len());
    let mut total_operations_bytes = 0usize;
    for (graph, operations) in &batches {
        if graph.trim().is_empty()
            || graph.len() > MAX_MULTI_GRAPH_NAME_BYTES
            || graph.chars().any(char::is_control)
        {
            return Err("multi-graph batch contains an invalid graph identifier".to_string());
        }
        if !names.insert(graph.as_str()) {
            return Err("multi-graph batch contains a duplicate graph identifier".to_string());
        }
        total_operations_bytes = total_operations_bytes
            .checked_add(operations.len())
            .ok_or_else(|| "multi-graph batch exceeds the resource limit".to_string())?;
        if total_operations_bytes > MAX_MULTI_GRAPH_TOTAL_OPERATIONS_BYTES {
            return Err("multi-graph batch exceeds the resource limit".to_string());
        }
        crate::server::transport::validate_nested_msgpack(
            operations,
            MAX_MULTI_GRAPH_OPERATIONS_BYTES,
            MAX_MULTI_GRAPH_OPERATION_ITEMS,
        )
        .map_err(str::to_string)?;
    }
    Ok(batches)
}
