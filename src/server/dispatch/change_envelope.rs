use super::consensus::authoritative_now_ms;
use super::graph_pipeline::{dispatch_graph_op, GraphOpRouting};
use super::*;

/// Batch envelope coordinator (CONCEPT:EG-KG.ingest.batched-change-envelopes). Validates
/// each envelope's context against the verified request authority, groups envelopes
/// by their `mutation.graph`, and routes each graph's envelopes to `dispatch_graph_op`
/// (which resolves that graph's Write ACL + placement and commits the group in ONE
/// coalesced transaction). Per-graph groups are independent (partial success across
/// graphs); within a graph the commit is atomic. Per-envelope results are reassembled
/// into REQUEST order under `{"results": [...]}` so a caller can advance a watermark
/// through the contiguous success prefix.
#[cfg(not(feature = "redb"))]
pub(super) async fn dispatch_change_envelopes(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    Response::err(
        req_id,
        "batch change-envelope commit requires a build with durable redb support",
    )
}

#[cfg(feature = "redb")]
pub(super) async fn dispatch_change_envelopes(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    let total = envelopes.len();
    if total == 0 {
        return Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({ "results": [] })),
        );
    }
    if total > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
        return Response::err(
            req_id,
            format!(
                "CHANGE_BATCH_TOO_LARGE: {total} envelopes exceed the {} cap",
                crate::change_envelope::MAX_ENVELOPES_PER_BATCH
            ),
        );
    }
    if let Some(response) =
        change_envelope_batch_authority_error(req_id, verified_context, &envelopes)
    {
        return response;
    }

    let mut per_index: Vec<serde_json::Value> = vec![serde_json::Value::Null; total];
    let (groups, ungrouped) = group_change_envelopes_by_graph(envelopes);
    for index in ungrouped {
        // Unreachable in practice: `change_envelope_batch_authority_error` above
        // already rejects the whole request if any envelope's mutation scope has
        // no graph name. Kept as an explicit conflict entry, not a silent Null,
        // in case that invariant ever changes.
        per_index[index] = serde_json::json!({
            "status": "conflict",
            "error": "ApplyChangeEnvelopes requires a graph-scoped mutation",
        });
    }
    for (graph, group) in groups {
        let indices: Vec<usize> = group.iter().map(|(index, _)| *index).collect();
        let group_envelopes: Vec<crate::change_envelope::ChangeEnvelope> =
            group.into_iter().map(|(_, envelope)| envelope).collect();
        let resp = dispatch_graph_op(
            state,
            &graph,
            req_id,
            caller,
            verified_context,
            Method::ApplyChangeEnvelopes {
                envelopes: group_envelopes,
            },
        )
        .await;
        scatter_change_envelope_group_results(&mut per_index, &indices, resp);
    }

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({ "results": per_index })),
    )
}

/// Per-envelope authority binding — mirrors the single `ApplyChangeEnvelope`
/// arm, minus the two batch-varying fields: the idempotency_key (per envelope,
/// enforced by the mutation-store idempotency table) and the graph (per
/// envelope, ACL-checked per group by `dispatch_graph_op`).
#[cfg(feature = "redb")]
fn change_envelope_batch_authority_error(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    envelopes: &[crate::change_envelope::ChangeEnvelope],
) -> Option<Response> {
    let claims = verified_context.claims();
    let principal = verified_context.principal_persistence_id();
    for envelope in envelopes {
        let ctx = &envelope.mutation.context;
        // `ApplyChangeEnvelopes` groups and routes every envelope by graph name
        // (`group_change_envelopes_by_graph` below, then `dispatch_graph_op`), so
        // a native (non-graph) mutation scope — which reports no graph name at
        // all — can never be routed and must fail closed here rather than being
        // silently dropped or grouped under a sentinel.
        if envelope.mutation.identity.scope().graph_name().is_none()
            || envelope.mutation.identity.tenant().as_str() != claims.tenant
            || ctx.request_id != req_id
            || ctx.principal != principal
            || ctx.policy_fingerprint.as_deref() != Some(claims.policy_version.as_str())
        {
            return Some(Response::err(
                req_id,
                "ApplyChangeEnvelopes context does not match the verified request authority",
            ));
        }
    }
    None
}

/// Group envelopes by graph, preserving first-seen graph order and the
/// per-graph envelope order, and carrying each envelope's REQUEST index so the
/// per-graph results can be scattered back into request order.
///
/// Every envelope reaching this function already passed
/// `change_envelope_batch_authority_error`, which rejects the whole batch if
/// any envelope's mutation scope has no graph name — so in practice a native
/// (non-graph) scope never appears here. It is still handled explicitly
/// (returned as `ungrouped`, never silently coerced into a "" / sentinel
/// group) so a future change to the authority check cannot turn this into a
/// silent misroute.
#[cfg(feature = "redb")]
fn group_change_envelopes_by_graph(
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> (
    Vec<(String, Vec<(usize, crate::change_envelope::ChangeEnvelope)>)>,
    Vec<usize>,
) {
    let mut graph_order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<
        String,
        Vec<(usize, crate::change_envelope::ChangeEnvelope)>,
    > = std::collections::HashMap::new();
    let mut ungrouped: Vec<usize> = Vec::new();
    for (index, envelope) in envelopes.into_iter().enumerate() {
        let Some(graph) = envelope
            .mutation
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str().to_string())
        else {
            ungrouped.push(index);
            continue;
        };
        groups
            .entry(graph.clone())
            .or_insert_with(|| {
                graph_order.push(graph.clone());
                Vec::new()
            })
            .push((index, envelope));
    }
    let grouped = graph_order
        .into_iter()
        .map(|graph| {
            let group = groups.remove(&graph).expect("grouped graph is present");
            (graph, group)
        })
        .collect();
    (grouped, ungrouped)
}

/// Scatter one graph group's response back into request-ordered slots.
#[cfg(feature = "redb")]
fn scatter_change_envelope_group_results(
    per_index: &mut [serde_json::Value],
    indices: &[usize],
    response: Response,
) {
    if let Some(err) = response.error {
        // A transport/ACL/placement failure for the whole group (distinct from the
        // per-envelope atomic-batch abort, which returns Ok with conflict entries).
        for index in indices {
            per_index[*index] = serde_json::json!({ "status": "conflict", "error": err });
        }
        return;
    }
    let Some(ResultPayload::Json(value)) = response.result else {
        for index in indices {
            per_index[*index] = serde_json::json!({
                "status": "conflict",
                "error": "empty batch response",
            });
        }
        return;
    };
    let group_results = value
        .get("results")
        .and_then(|results| results.as_array())
        .cloned()
        .unwrap_or_default();
    for (position, index) in indices.iter().enumerate() {
        per_index[*index] = group_results.get(position).cloned().unwrap_or_else(|| {
            serde_json::json!({
                "status": "conflict",
                "error": "missing per-envelope result in batch response",
            })
        });
    }
}

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
    let saga = match begin_multi_graph_saga(redb, req_id, caller, batches_msgpack, clustered) {
        Ok(saga) => saga,
        Err(response) => return response,
    };
    let (results, errors) = if batches.is_empty() {
        (serde_json::Map::new(), serde_json::Map::new())
    } else {
        run_multi_graph_batches(state, req_id, caller, verified_context, batches).await
    };
    let result = ResultPayload::Json(serde_json::json!({"results": results, "errors": errors}));
    finish_multi_graph_batch(redb, req_id, saga, result)
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
    batches_msgpack: &[u8],
    clustered: bool,
) -> Result<Option<handlers::admin::AdminSaga>, Response> {
    if clustered {
        return Ok(None);
    }
    let method = Method::MultiGraphBatchUpdate {
        batches_msgpack: batches_msgpack.to_vec(),
    };
    let saga = match handlers::admin::begin_admin_saga(
        redb,
        req_id,
        caller,
        &method,
        crate::mutation_batch::DurabilityDomain::MultiGraph,
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

/// Record one sub-batch's outcome. A graph's failure lands in `errors`; a
/// success lands in `results`, with a non-JSON payload recorded as null so the
/// reply always names every graph exactly once.
#[cfg(feature = "redb")]
fn record_multi_graph_result(
    results: &mut serde_json::Map<String, serde_json::Value>,
    errors: &mut serde_json::Map<String, serde_json::Value>,
    graph: String,
    response: Response,
) {
    if let Some(err) = response.error {
        errors.insert(graph, serde_json::Value::String(err));
    } else if let Some(ResultPayload::Json(value)) = response.result {
        results.insert(graph, value);
    } else {
        results.insert(graph, serde_json::Value::Null);
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
) -> (
    serde_json::Map<String, serde_json::Value>,
    serde_json::Map<String, serde_json::Value>,
) {
    let mut results = serde_json::Map::new();
    let mut errors = serde_json::Map::new();
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
            Ok((graph, resp)) => record_multi_graph_result(&mut results, &mut errors, graph, resp),
            Err(join_err) => {
                // A panicked/cancelled sub-batch task — surface it, don't abort.
                let _ = join_err;
                errors.insert(
                    format!("__join_error_{}", errors.len()),
                    serde_json::Value::String("sub-batch execution failed".to_string()),
                );
            }
        }
    }
    (results, errors)
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

fn change_envelope_result(
    committed: &eg_types::ChangeEnvelopeCommit,
    projection_pending: bool,
) -> serde_json::Value {
    let mut result = serde_json::to_value(committed).unwrap_or_else(|_| {
        serde_json::json!({
            "envelope_id": committed.envelope_id,
            "batch_id": committed.batch_id,
            "replayed": committed.replayed,
        })
    });
    if let Some(object) = result.as_object_mut() {
        object.insert(
            "projection_pending".to_string(),
            serde_json::Value::Bool(projection_pending),
        );
    }
    result
}

/// Derive the native authority epoch from the registry-published graph
/// incarnation.  The caller cannot provide either value; a delete/recreate
/// therefore fences every capability from the retired incarnation.
pub(super) fn work_item_capability_authority_epoch(incarnation_id: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(incarnation_id.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("fixed digest width")).max(1)
}

async fn check_change_envelope_placement_fence(
    req_id: u64,
    #[cfg(feature = "raft")] graph_name: &str,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    #[cfg(feature = "raft")] routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
) -> Result<(), Response> {
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope commits require the current placement leader",
            ));
        }
        if envelope.mutation.placement_epoch != routed.epoch
            || envelope.mutation.fencing_token != Some(routed.group_id)
        {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope placement epoch or fencing token is stale",
            ));
        }
    } else {
        if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
            return Err(Response::err(
                req_id,
                "ChangeEnvelope carries a placement fence but no routed placement is active",
            ));
        }
    }
    #[cfg(not(feature = "raft"))]
    if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
        return Err(Response::err(
            req_id,
            "ChangeEnvelope carries a placement fence in a single-node build",
        ));
    }
    Ok(())
}

/// The already-resolved graph/placement identity a replicated ChangeEnvelope
/// commit needs; the envelope and its leader-selected timestamp stay explicit
/// because they are what is actually being replicated.
#[cfg(feature = "raft")]
struct ChangeEnvelopeReplicaCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    tenant_scope: &'a str,
    fname: &'a str,
    routed_raft: Option<&'a crate::raft::multi::RoutedRaftHandle>,
}

/// Attempt the clustered replication path for `ApplyChangeEnvelope`: one log
/// entry, one native transaction on every replica, leader-selected timestamp
/// for byte-stable replay/follower records. `Some(resp)` means the request
/// was fully handled (locally or via a stale-route redirect) by this path;
/// `None` means no routed placement is active and the caller should fall
/// through to the local `commit_change_envelope` path.
#[cfg(feature = "raft")]
async fn try_replicate_change_envelope(
    ctx: ChangeEnvelopeReplicaCtx<'_>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed_at_ms: u64,
) -> Option<Response> {
    let ChangeEnvelopeReplicaCtx {
        state,
        req_id,
        graph_name,
        graph_type,
        tenant_scope,
        fname,
        routed_raft,
    } = ctx;
    let routed = routed_raft?;
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        envelope.mutation.batch_id.clone(),
        envelope.mutation.context.request_id,
        tenant_scope,
        envelope.mutation.context.principal.clone(),
        false,
        envelope.mutation.placement_epoch,
        envelope.mutation.fencing_token,
        envelope.mutation.created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::ReplicatedMutation::change_envelope(envelope, &server_secret) {
        Ok(command) => command,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let request = crate::raft::RaftRequest {
        graph_fname: fname.to_string(),
        graph_name: graph_name.to_string(),
        graph_type,
        command,
        committed_at_ms,
        mutation,
    };
    Some(match routed.handle.client_write(request).await {
        Ok(response) if response.native_error.is_some() => {
            Response::err(req_id, response.native_error.unwrap_or_default())
        }
        Ok(response) => match response.change_envelope_commit {
            Some(committed) => {
                let mut result = change_envelope_result(&committed, response.projection_pending);
                if let Some(object) = result.as_object_mut() {
                    object.insert("replicated".to_string(), true.into());
                    object.insert("group".to_string(), routed.group_id.into());
                    object.insert("epoch".to_string(), routed.epoch.into());
                    object.insert("fencing_token".to_string(), routed.fencing_token().into());
                }
                Response::ok(req_id, ResultPayload::Json(result))
            }
            None => Response::err(
                req_id,
                "replicated ChangeEnvelope returned no commit receipt",
            ),
        },
        Err(error) => {
            let leader = routed.handle.current_leader().await;
            Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                error,
            )
        }
    })
}

/// The resolved graph context a single-envelope `ApplyChangeEnvelope` commit
/// runs against: the live core, its durability backend, the placement route and
/// the tenant authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
/// Commit a `ApplyChangeEnvelopes` batch and translate the durable result
/// (all-or-nothing per `crate::persist::sanitize`d graph) into the per-envelope
/// JSON result array the caller returns as `{"results": [...]}` -- success
/// entries and the atomic-abort's per-envelope conflict entries are the same
/// shape either way, so callers never have to branch on which happened.
async fn commit_change_envelope_batch_results(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    core: &Arc<crate::graph::GraphCore>,
    fname: &str,
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    committed_at_ms: u64,
) -> Vec<serde_json::Value> {
    match backend
        .commit_change_envelopes(fname, envelopes, committed_at_ms)
        .await
    {
        Ok(commits) => envelopes
            .iter()
            .zip(commits.iter())
            .map(|(envelope, committed)| change_envelope_applied_entry(core, envelope, committed))
            .collect(),
        Err((failing_index, error)) => {
            change_envelope_abort_entries(envelopes, failing_index, &error)
        }
    }
}

/// One committed envelope's result entry. A replayed envelope is an idempotent
/// skip and does NOT republish its projection; a freshly applied one does, and
/// a projection failure is surfaced as `projection_pending` rather than
/// pretending the durable commit did not happen.
fn change_envelope_applied_entry(
    core: &Arc<crate::graph::GraphCore>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed: &eg_types::ChangeEnvelopeCommit,
) -> serde_json::Value {
    let projection_error = if committed.replayed {
        None
    } else {
        crate::server::mutation_batch::publish_change_envelope_projection(core, envelope).err()
    };
    let mut entry = change_envelope_result(committed, projection_error.is_some());
    if let Some(object) = entry.as_object_mut() {
        let status = if committed.replayed {
            "idempotent_skip"
        } else {
            "applied"
        };
        object.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
    }
    entry
}

/// The whole graph-batch aborted atomically — nothing committed. Report the
/// batch outcome per envelope honestly: the offender carries its own error; the
/// siblings carry the abort cause.
fn change_envelope_abort_entries(
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    failing_index: usize,
    error: &str,
) -> Vec<serde_json::Value> {
    envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            let this_error = if index == failing_index {
                error.to_string()
            } else {
                format!(
                    "ABORTED_ATOMIC_GRAPH_BATCH: sibling envelope {failing_index} failed ({error})"
                )
            };
            serde_json::json!({
                "status": "conflict",
                "envelope_id": envelope.envelope_id,
                "error": this_error,
            })
        })
        .collect()
}

pub(super) async fn route_change_envelope_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let tenant_scope = ctx.tenant_scope;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;
    // ChangeEnvelope is a first-class persistence operation, not a sequence of
    // direct graph calls. It executes only after graph ACL and placement
    // resolution, and before generic replicated mutation paths can decompose it.
    match method {
        Method::ApplyChangeEnvelope { envelope } => {
            return Ok(async {
                {
                    if let Err(resp) = check_change_envelope_placement_fence(
                        req_id,
                        #[cfg(feature = "raft")]
                        graph_name,
                        &envelope,
                        #[cfg(feature = "raft")]
                        routed_raft.as_ref(),
                    )
                    .await
                    {
                        return resp;
                    }

                    let _mutation_guard =
                        crate::server::mutation_batch::lock_graph(graph_name).await;
                    // `ApplyChangeEnvelope` is a graph-scoped commit path (it targets
                    // `graph_name` and checks against `core.version()`), so its version
                    // expectation must be `Graph(_)`; any other variant (Native/
                    // Unversioned) is invalid input here and rejected rather than
                    // silently skipping the version check the old `Option::None` arm did.
                    match envelope.mutation.version_expectation {
                        crate::mutation_batch::VersionExpectation::Graph(expected) => {
                            if expected != core.version() {
                                return Response::err(
                                    req_id,
                                    format!(
                                        "STALE_GRAPH_VERSION: expected {expected}, current {}",
                                        core.version()
                                    ),
                                );
                            }
                        }
                        _ => {
                            return Response::err(
                                req_id,
                                "ApplyChangeEnvelope requires a graph version expectation",
                            );
                        }
                    }
                    let committed_at_ms =
                        authoritative_now_ms().max(envelope.mutation.created_at_ms);
                    let Some(backend) = persistence.as_ref() else {
                        return Response::err(
                            req_id,
                            "ApplyChangeEnvelope requires a configured persistence backend",
                        );
                    };
                    let fname = crate::persist::sanitize(graph_name);

                    #[cfg(feature = "raft")]
                    if let Some(resp) = try_replicate_change_envelope(
                        ChangeEnvelopeReplicaCtx {
                            state,
                            req_id,
                            graph_name,
                            graph_type,
                            tenant_scope,
                            fname: &fname,
                            routed_raft: routed_raft.as_ref(),
                        },
                        &envelope,
                        committed_at_ms,
                    )
                    .await
                    {
                        return resp;
                    }

                    let committed = match backend
                        .commit_change_envelope(&fname, &envelope, committed_at_ms)
                        .await
                    {
                        Ok(committed) => committed,
                        Err(error) => {
                            return Response::err(
                                req_id,
                                format!("ApplyChangeEnvelope atomic commit failed: {error}"),
                            )
                        }
                    };
                    let projection_error = if committed.replayed {
                        None
                    } else {
                        crate::server::mutation_batch::publish_change_envelope_projection(
                            &core, &envelope,
                        )
                        .err()
                    };
                    let result = change_envelope_result(&committed, projection_error.is_some());
                    Response::ok(req_id, ResultPayload::Json(result))
                }
            }
            .await);
        }
        // Batch envelope commit for ONE graph (the top-level `dispatch_change_envelopes`
        // groups by graph and routes each group here). Every envelope targets
        // `graph_name`; they land in ONE coalesced redb transaction — the atomic
        // graph-batch. A single failing envelope aborts the whole group and every
        // envelope in it reports the batch outcome honestly. Per-envelope results are
        // returned in group order under `{"results": [...]}`.
        Method::ApplyChangeEnvelopes { envelopes } => {
            return Ok(async {
    let req_id = req_id;
    let graph_name = graph_name;
    let core = core.clone();
    let persistence = persistence.clone();
    #[cfg(feature = "raft")]
    let routed_raft = routed_raft.clone();
    {
            // Under an active cluster placement the batch is not offered: raft keeps
            // each envelope one log entry (K=1 serializes anyway), so the client falls
            // back to per-record `ApplyChangeEnvelope`. Single-node is where the
            // one-transaction batching win lands, and prod is single-node.
            #[cfg(feature = "raft")]
            if routed_raft.is_some() {
                return Response::err(
                    req_id,
                    "CHANGE_BATCH_UNAVAILABLE_UNDER_PLACEMENT: use per-envelope ApplyChangeEnvelope",
                );
            }
            let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
            for envelope in &envelopes {
                if envelope.mutation.placement_epoch != 0
                    || envelope.mutation.fencing_token.is_some()
                {
                    return Response::err(
                        req_id,
                        "ChangeEnvelope carries a placement fence in a single-node build",
                    );
                }
            }
            let committed_at_ms = envelopes.iter().fold(authoritative_now_ms(), |acc, e| {
                acc.max(e.mutation.created_at_ms)
            });
            let Some(backend) = persistence.as_ref() else {
                return Response::err(
                    req_id,
                    "ApplyChangeEnvelopes requires a configured persistence backend",
                );
            };
            let fname = crate::persist::sanitize(graph_name);
            let results = commit_change_envelope_batch_results(
                backend,
                &core,
                &fname,
                &envelopes,
                committed_at_ms,
            )
            .await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "results": results })),
            )
        }
}
            .await);
        }
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => {
            return Ok(async {
                let req_id = req_id;
                let graph_name = graph_name;
                let persistence = persistence.clone();
                {
                    let Some(backend) = persistence.as_ref() else {
                        return Response::err(req_id, "ChangeEnvelope persistence is unavailable");
                    };
                    let fname = crate::persist::sanitize(graph_name);
                    return match backend.read_change_envelope(&fname, &envelope_id).await {
                        Ok(Some(record))
                            if record.envelope.mutation.identity.tenant().as_str() == tenant
                        // A native (non-graph) scope reports no graph name;
                        // `map(...) == Some(_)` fails closed on `None` instead of
                        // matching `graph_name` against a coerced sentinel.
                        && record
                            .envelope
                            .mutation
                            .identity
                            .scope()
                            .graph_name()
                            .map(crate::mutation_batch::LogicalName::as_str)
                            == Some(graph_name) =>
                        {
                            Response::ok(req_id, ResultPayload::raw(&record))
                        }
                        Ok(Some(_)) => {
                            Response::err(req_id, "ACCESS_DENIED: envelope tenant mismatch")
                        }
                        Ok(None) => Response::ok(
                            req_id,
                            ResultPayload::raw(
                                &Option::<crate::change_envelope::ChangeEnvelopeRecord>::None,
                            ),
                        ),
                        Err(error) => {
                            Response::err(req_id, format!("ChangeEnvelope read failed: {error}"))
                        }
                    };
                }
            }
            .await);
        }
        Method::GetContentVersion { object_id, tenant } => {
            return Ok(async {
                let req_id = req_id;
                let graph_name = graph_name;
                let persistence = persistence.clone();
                {
                    let Some(backend) = persistence.as_ref() else {
                        return Response::err(req_id, "content-version persistence is unavailable");
                    };
                    let fname = crate::persist::sanitize(graph_name);
                    return match backend
                        .read_content_version(&fname, &tenant, &object_id)
                        .await
                    {
                        Ok(version) => Response::ok(req_id, ResultPayload::raw(&version)),
                        Err(error) => {
                            Response::err(req_id, format!("content-version read failed: {error}"))
                        }
                    };
                }
            }
            .await)
        }
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => {
            return Ok(async {
                let req_id = req_id;
                let graph_name = graph_name;
                let persistence = persistence.clone();
                {
                    let Some(backend) = persistence.as_ref() else {
                        return Response::err(req_id, "change-cursor persistence is unavailable");
                    };
                    let fname = crate::persist::sanitize(graph_name);
                    return match backend
                        .read_change_cursor(&fname, &tenant, &source, &partition)
                        .await
                    {
                        Ok(cursor) => Response::ok(req_id, ResultPayload::raw(&cursor)),
                        Err(error) => {
                            Response::err(req_id, format!("change-cursor read failed: {error}"))
                        }
                    };
                }
            }
            .await)
        }
        other => Err(other),
    }
}
