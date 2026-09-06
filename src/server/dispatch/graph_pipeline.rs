#[cfg(test)]
use super::change_envelope::decode_multi_graph_batches;
use super::change_envelope::{route_change_envelope_ops, work_item_capability_authority_epoch};
use super::consensus::{authoritative_now_ms, is_replicated_apply};
#[cfg(all(test, feature = "ast"))]
use super::request_boundary::{decode_ast_files, AstInputLimits};
#[cfg(test)]
use super::request_boundary::{decode_screen_observation, dispatch, preflight_request_msgpack};
use super::*;
mod work_governance;
use work_governance::{
    dispatch_op_capacity_ops, dispatch_op_development_lane, dispatch_op_resource_reservation_query,
    dispatch_op_workitem_claim_capability, dispatch_op_workitem_mutation, NativeOpCtx,
};

/// Dispatch a graph-level operation to the target named graph, enforcing the
/// isolation ACL (`isolation.rs::check_access`) when rules are registered.
pub(super) async fn dispatch_graph_op(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        method,
        #[cfg(feature = "modality-serving")]
        None,
        #[cfg(feature = "knowledge-batch")]
        None,
    )
    .await
}

#[cfg(feature = "modality-serving")]
pub(super) async fn dispatch_served_modality(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    op: eg_types::ServedModalityOp,
    authority: handlers::modality::ModalityAuthority,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        Method::ServedModality { op },
        Some(authority),
        #[cfg(feature = "knowledge-batch")]
        None,
    )
    .await
}

#[cfg(feature = "knowledge-batch")]
pub(super) async fn dispatch_knowledge_stream(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    request: crate::knowledge_stream::KnowledgeStreamRequestV1,
    authority: handlers::knowledge_stream::KnowledgeStreamAuthority,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        Method::KnowledgeStream { request },
        #[cfg(feature = "modality-serving")]
        None,
        Some(authority),
    )
    .await
}

/// The route, graph and verified identity a replicated modality mutation is
/// bound to. Bundled so each phase helper below stays inside the parameter cap.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
struct ModalityReplication<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    handle: &'a crate::raft::RaftHandle,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    req_id: u64,
    tenant_scope: &'a str,
    principal_fingerprint: &'a str,
    core: &'a Arc<crate::graph::GraphCore>,
    persistence: &'a Arc<dyn crate::server::persistence::PersistenceBackend>,
}

/// The sanitized, source-free facts derived from the public method. The
/// source-bearing `ServedModalityOp` is deliberately NOT carried here — it is
/// handed to the modality handler once and never reaches `RaftRequest`.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
struct ModalityMutationInputs {
    operation: crate::raft::SanitizedModalityMutation,
    modality: eg_types::ServedModalityKind,
    receipt_query: String,
    safe_method: Method,
}

/// Only the current placement leader may prepare a modality mutation; a
/// follower answers with the ordinary redirect.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn modality_leader_fence(ctx: &ModalityReplication<'_>) -> Option<Response> {
    let leader = ctx.handle.current_leader().await;
    if leader == Some(ctx.handle.node_id) {
        return None;
    }
    Some(Response::stale_route(
        ctx.req_id,
        ctx.graph_name,
        ctx.group_id,
        ctx.placement_epoch,
        leader,
        "served modality mutations require the current placement leader",
    ))
}

/// Split the public method into the source-bearing op (consumed by the handler)
/// and the digest-only receipt facts consensus actually sees.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn decode_modality_replication_inputs(
    req_id: u64,
    method: Method,
) -> Result<(eg_types::ServedModalityOp, ModalityMutationInputs), Response> {
    let safe_method = crate::server::mutation::durable_receipt_method(&method);
    let Method::ServedModality { op } = method else {
        return Err(Response::err(
            req_id,
            "served modality replication received the wrong method",
        ));
    };
    let Some((operation, modality)) = crate::raft::SanitizedModalityMutation::from_served(&op)
    else {
        return Err(Response::err(
            req_id,
            "served modality mutation category is invalid",
        ));
    };
    let receipt_query = match &safe_method {
        Method::ApplyMutation { event_type, query } if event_type == "served_modality_v1" => {
            query.clone()
        }
        _ => {
            return Err(Response::err(
                req_id,
                "served modality receipt construction failed",
            ))
        }
    };
    Ok((
        op,
        ModalityMutationInputs {
            operation,
            modality,
            receipt_query,
            safe_method,
        },
    ))
}

/// The stored receipt must carry exactly ONE authoritative-state operation whose
/// digest is this request's own.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn modality_receipt_operation_matches(
    record: &crate::mutation_batch::MutationBatchRecord,
    expected_operation: &str,
) -> bool {
    record.batch.operations.len() == 1
        && matches!(
            &record.batch.operations[0].method,
            Method::ApplyMutation { event_type, query }
                if event_type == "authoritative_state_operation"
                    && query == expected_operation
        )
}

/// The stored receipt must also be committed and bound to this request's
/// batch, tenant, graph, placement fence and principal.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn modality_receipt_binding_matches(
    ctx: &ModalityReplication<'_>,
    record: &crate::mutation_batch::MutationBatchRecord,
    batch_id: &str,
) -> bool {
    record.status == crate::mutation_batch::MutationBatchStatus::Committed
        && record.batch.batch_id == batch_id
        && record.batch.identity.tenant().as_str() == ctx.tenant_scope
        // A native (non-graph) scope reports no graph name; `map(...) == Some(_)`
        // fails closed instead of matching `ctx.graph_name` against a sentinel.
        && record
            .batch
            .identity
            .scope()
            .graph_name()
            .map(crate::mutation_batch::LogicalName::as_str)
            == Some(ctx.graph_name)
        && record.batch.placement_epoch == ctx.placement_epoch
        && record.batch.fencing_token == ctx.fencing_token
        && record.batch.context.principal == ctx.principal_fingerprint
}

/// Raft apply authenticated the sealed runtime state and result digest before
/// committing the record. The retry record intentionally retains only the safe
/// result bytes, so replay reuses the same canonical typed decoder to reject
/// receipt tampering without ever reconstructing or exposing the sealed/source
/// material.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn decode_modality_replay_result(
    req_id: u64,
    inputs: &ModalityMutationInputs,
    record: &crate::mutation_batch::MutationBatchRecord,
) -> Result<ResultPayload, Response> {
    let Some(encoded) = record.result_msgpack.as_deref() else {
        return Err(Response::err(
            req_id,
            "replicated modality receipt has no terminal result",
        ));
    };
    crate::raft::decode_sanitized_modality_result(inputs.modality, inputs.operation, encoded)
        .map_err(|_| Response::err(req_id, "replicated modality receipt has an invalid result"))
}

/// Repair RAM from the durably committed image before answering a replay.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn install_modality_committed_image(
    ctx: &ModalityReplication<'_>,
    graph_fname: &str,
    result: ResultPayload,
) -> Response {
    match ctx
        .persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await
    {
        Ok(Some((snapshot, version))) => {
            match ctx.core.install_committed_snapshot(snapshot, version) {
                Ok(()) => Response::ok(ctx.req_id, result),
                Err(error) => Response::err(ctx.req_id, error),
            }
        }
        Ok(None) => Response::err(ctx.req_id, "committed modality image is missing"),
        Err(error) => Response::err(ctx.req_id, error),
    }
}

/// A client retry with the same request id repairs RAM from the committed image
/// and returns the exact stored ApplyOutcome. It never re-decodes source bytes
/// or emits a second Raft entry/outbox/audit/CDC event. `None` ⇒ no receipt yet,
/// so the caller proceeds with a fresh mutation.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn try_replay_modality_receipt(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    batch_id: &str,
    graph_fname: &str,
) -> Option<Response> {
    use sha2::{Digest, Sha256};

    let record = match ctx
        .persistence
        .read_mutation_batch(graph_fname, batch_id)
        .await
    {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(error) => return Some(Response::err(ctx.req_id, error)),
    };
    let encoded = match rmp_serde::to_vec_named(&inputs.safe_method) {
        Ok(encoded) => encoded,
        Err(error) => return Some(Response::err(ctx.req_id, error.to_string())),
    };
    let expected_operation = format!("sha256:{}", hex::encode(Sha256::digest(encoded)));
    if !modality_receipt_binding_matches(ctx, &record, batch_id)
        || !modality_receipt_operation_matches(&record, &expected_operation)
    {
        return Some(Response::err(
            ctx.req_id,
            "replicated modality receipt conflicts with request authority",
        ));
    }
    let result = match decode_modality_replay_result(ctx.req_id, inputs, &record) {
        Ok(result) => result,
        Err(response) => return Some(response),
    };
    Some(install_modality_committed_image(ctx, graph_fname, result).await)
}

/// Stage the mutation over the authoritative committed image. When no durable
/// snapshot exists yet, fall back to the resident core at the durable version.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn stage_modality_base_core(
    ctx: &ModalityReplication<'_>,
    graph_fname: &str,
) -> Result<crate::graph::GraphCore, Response> {
    let (base_snapshot, source_version) = match ctx
        .persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => {
            let version = match ctx
                .persistence
                .read_mutation_graph_version(graph_fname)
                .await
            {
                Ok(value) => value.unwrap_or_else(|| ctx.core.version()),
                Err(error) => return Err(Response::err(ctx.req_id, error)),
            };
            (ctx.core.snapshot(), version)
        }
        Err(error) => return Err(Response::err(ctx.req_id, error)),
    };
    crate::graph::GraphCore::from_snapshot(base_snapshot, source_version)
        .map_err(|error| Response::err(ctx.req_id, error))
}

/// Seal the staged runtime node into the HMAC-authenticated command consensus
/// receives. The staged node MUST already be sealed — an unsealed runtime state
/// is refused rather than replicated.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn build_modality_raft_command(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    staged: &crate::graph::GraphCore,
    authority: &handlers::modality::ModalityAuthority,
    payload: &ResultPayload,
) -> Result<crate::raft::SanitizedModalityRaftCommand, Response> {
    let node_id = authority.node_id(inputs.modality);
    let Some(sealed_runtime_state) = staged.get_node_properties(&node_id) else {
        return Err(Response::err(
            ctx.req_id,
            "served modality produced no encrypted state",
        ));
    };
    if !crate::crypto::is_sealed(&sealed_runtime_state) {
        return Err(Response::err(
            ctx.req_id,
            "served modality produced unsealed state",
        ));
    }
    let result_msgpack = match rmp_serde::to_vec_named(payload) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(ctx.req_id, error.to_string())),
    };
    let server_secret = timed_read(ctx.state).await.auth_secret.clone();
    crate::raft::SanitizedModalityRaftCommand::new(
        &server_secret,
        inputs.modality,
        inputs.operation,
        node_id,
        sealed_runtime_state,
        inputs.receipt_query.clone(),
        result_msgpack,
    )
    .map_err(|error| Response::err(ctx.req_id, error))
}

/// Propose the sanitized command and answer with the prepared payload once the
/// entry has applied.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn submit_modality_replication(
    ctx: &ModalityReplication<'_>,
    batch_id: String,
    graph_fname: String,
    command: crate::raft::SanitizedModalityRaftCommand,
    payload: ResultPayload,
) -> Response {
    let created_at_ms = authoritative_now_ms();
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        ctx.req_id,
        ctx.tenant_scope,
        ctx.principal_fingerprint.to_string(),
        false,
        ctx.placement_epoch,
        ctx.fencing_token,
        created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let request = crate::raft::RaftRequest {
        graph_fname,
        graph_name: ctx.graph_name.to_string(),
        graph_type: ctx.graph_type,
        command: crate::raft::ReplicatedMutation::served_modality(command),
        committed_at_ms: created_at_ms,
        mutation,
    };
    match ctx.handle.client_write(request).await {
        Ok(response) if response.applied => Response::ok(ctx.req_id, payload),
        Ok(_) => Response::err(ctx.req_id, "replicated modality state was not applied"),
        Err(error) => {
            let leader = ctx.handle.current_leader().await;
            Response::stale_route(
                ctx.req_id,
                ctx.graph_name,
                ctx.group_id,
                ctx.placement_epoch,
                leader,
                error,
            )
        }
    }
}

/// Leader-only preparation for a replicated modality mutation. The source-bearing
/// public Method is consumed by the native decoder here and never enters
/// `RaftRequest`; consensus receives only the HMAC-authenticated encrypted runtime
/// node plus a digest-only receipt and compact ApplyOutcome.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
#[allow(clippy::too_many_arguments)]
async fn replicate_served_modality(
    state: &Arc<RwLock<ServerState>>,
    handle: crate::raft::RaftHandle,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    req_id: u64,
    tenant_scope: &str,
    principal_fingerprint: &str,
    core: &Arc<crate::graph::GraphCore>,
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    method: Method,
    authority: &handlers::modality::ModalityAuthority,
) -> Response {
    let ctx = ModalityReplication {
        state,
        handle: &handle,
        group_id,
        placement_epoch,
        fencing_token,
        graph_name,
        graph_type,
        req_id,
        tenant_scope,
        principal_fingerprint,
        core,
        persistence,
    };
    if let Some(stale) = modality_leader_fence(&ctx).await {
        return stale;
    }

    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let (op, inputs) = match decode_modality_replication_inputs(req_id, method) {
        Ok(decoded) => decoded,
        Err(response) => return response,
    };
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "raft-modality",
        graph_name,
        req_id,
        &inputs.safe_method,
    );
    let graph_fname = crate::persist::sanitize(graph_name);

    if let Some(replayed) =
        try_replay_modality_receipt(&ctx, &inputs, &batch_id, &graph_fname).await
    {
        return replayed;
    }

    prepare_and_replicate_modality(&ctx, &inputs, op, authority, batch_id, graph_fname).await
}

/// No receipt exists yet: stage the mutation over the committed image, run the
/// modality handler, seal the result, and propose it.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn prepare_and_replicate_modality(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    op: eg_types::ServedModalityOp,
    authority: &handlers::modality::ModalityAuthority,
    batch_id: String,
    graph_fname: String,
) -> Response {
    let staged = match stage_modality_base_core(ctx, &graph_fname).await {
        Ok(staged) => staged,
        Err(response) => return response,
    };
    let payload = match handlers::modality::handle(&staged, authority, op) {
        Ok(payload) => payload,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let command = match build_modality_raft_command(ctx, inputs, &staged, authority, &payload).await
    {
        Ok(command) => command,
        Err(response) => return response,
    };
    submit_modality_replication(ctx, batch_id, graph_fname, command, payload).await
}

#[cfg(all(test, feature = "raft", feature = "modality-serving"))]
mod modality_replay_receipt_tests {
    use super::*;

    fn single_wire() -> Vec<u8> {
        let outcome = eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 11,
            event_sequence: 17,
        };
        rmp_serde::to_vec_named(&ResultPayload::raw(&outcome)).unwrap()
    }

    fn stream_wire() -> Vec<u8> {
        let outcomes = vec![
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::Applied,
                observation_version: 11,
                event_sequence: 17,
            },
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::IdempotentReplay,
                observation_version: 12,
                event_sequence: 18,
            },
        ];
        rmp_serde::to_vec_named(&ResultPayload::raw(&outcomes)).unwrap()
    }

    #[test]
    fn replay_decoder_accepts_single_receipt() {
        let payload = crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::Ingest,
            &single_wire(),
        )
        .unwrap();
        let (ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes)) = payload else {
            panic!("typed replay receipt must remain a compact byte payload");
        };
        let outcome: eg_modality::ApplyOutcome = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(outcome.observation_version, 11);
        assert_eq!(outcome.event_sequence, 17);
    }

    #[test]
    fn replay_decoder_accepts_bounded_stream_receipt() {
        let payload = crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &stream_wire(),
        )
        .unwrap();
        let (ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes)) = payload else {
            panic!("typed replay receipt must remain a compact byte payload");
        };
        let outcomes: Vec<eg_modality::ApplyOutcome> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            outcomes[1].disposition,
            eg_modality::ApplyDisposition::IdempotentReplay
        );
    }

    #[test]
    fn replay_decoder_rejects_wrong_shape_and_oversized_receipts() {
        let wrong_payload = rmp_serde::to_vec_named(&ResultPayload::Bool(true)).unwrap();
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::Ingest,
            &wrong_payload,
        )
        .is_err());

        // A stream operation requires the typed stream result; a single outcome
        // must not be silently reinterpreted as a one-item stream.
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &single_wire(),
        )
        .is_err());

        let outcome = eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 1,
            event_sequence: 1,
        };
        let oversized = vec![outcome; 65];
        let oversized_wire = rmp_serde::to_vec_named(&ResultPayload::raw(&oversized)).unwrap();
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &oversized_wire,
        )
        .is_err());
    }
}

/// The caller/isolation-scoping fields shared by every `dispatch_graph_op_inner`
/// entry point, bundled so the function stays under the clippy argument-count
/// ceiling once the feature-gated authority parameters are unified in.
struct GraphOpContext<'a> {
    graph_name: &'a str,
    req_id: u64,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
}

/// Fence a `series.redb` WRITE to the current placement leader.
///
/// `Some(response)` is the stale-route rejection the caller must return; `None`
/// means the fence passed and dispatch CONTINUES — the original inline form fell
/// through, so an unconditional `return` here would strand every `TsAppend`.
/// The method guard lives inside so the call site stays a single `if let`.
#[cfg(all(feature = "raft", feature = "tsdb"))]
async fn dispatch_op_tsdb_write_fence(
    req_id: u64,
    graph_name: &str,
    routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
    method: &Method,
) -> Option<Response> {
    if !matches!(
        method,
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
    ) {
        return None;
    }
    let routed = routed_raft?;
    let leader = routed.handle.current_leader().await;
    if leader == Some(routed.handle.node_id) {
        return None;
    }
    Some(Response::stale_route(
        req_id,
        graph_name,
        routed.group_id,
        routed.epoch,
        leader,
        "time-series writes require the current placement leader",
    ))
}

/// Everything [`dispatch_op_knowledge_stream`] needs beyond the method itself:
/// the resolved request identity, the graph core it pulls from, and the
/// placement/RLS/authority handles the cursor must stay inside. Bundled to keep
/// the dispatcher at the documented parameter cap.
#[cfg(feature = "knowledge-batch")]
struct KnowledgeStreamCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    verified_context: &'a VerifiedRequestContext,
    read_authority: &'a Option<GraphReadAuthority>,
    verified_actor: &'a str,
    core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    knowledge_stream_authority: Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}

#[cfg(feature = "knowledge-batch")]
async fn dispatch_op_knowledge_stream(ctx: KnowledgeStreamCtx<'_>, method: Method) -> Response {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let read_authority = ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let core = ctx.core;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let knowledge_stream_authority = ctx.knowledge_stream_authority;
    let Some(authority) = knowledge_stream_authority.as_ref() else {
        return Response::err(
            req_id,
            "KnowledgeStream authority was not derived from verified context",
        );
    };
    let carrier = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    #[cfg(feature = "raft")]
    let (stream_placement_epoch, stream_fencing_token) = if let Some(routed) = routed_raft.as_ref()
    {
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(not(feature = "raft"))]
    let (stream_placement_epoch, stream_fencing_token) = (0, None);
    let handler_ctx = handlers::knowledge_stream::KnowledgeStreamHandlerCtx {
        state,
        req_id,
        graph_name,
        core: core.clone(),
        caller: verified_actor,
        carrier: &carrier,
        authority,
        placement_epoch: stream_placement_epoch,
        fencing_token: stream_fencing_token,
        read_authority: read_authority
            .as_ref()
            .expect("KnowledgeStream is classified as a graph read"),
        #[cfg(feature = "security")]
        rls: &rls,
    };
    return match handlers::knowledge_stream::try_handle(handler_ctx, method).await {
        Ok(response) => response,
        Err(_) => Response::err(req_id, "KnowledgeStream dispatch routing error"),
    };
}

#[cfg(feature = "tsdb")]
async fn dispatch_op_tsdb_ops(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    verified_context: &VerifiedRequestContext,
    ts_placement_epoch: u64,
    ts_fencing_token: Option<u64>,
    method: Method,
) -> Response {
    let carrier = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    return match handlers::timeseries::try_handle(
        state,
        req_id,
        &carrier,
        graph_name,
        ts_placement_epoch,
        ts_fencing_token,
        method,
    )
    .await
    {
        Ok(resp) => resp,
        Err(_) => Response::err(req_id, "timeseries dispatch routing error"),
    };
}

#[cfg(feature = "security")]
async fn dispatch_op_audit_verify(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_verify_blocking(&fname) {
            Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
            Err(e) => Response::err(req_id, format!("AuditVerify error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditVerify requires a durable redb backend (no persist dir configured)".to_string(),
        ),
    }
}

#[cfg(feature = "security")]
async fn prove_audit_inclusion(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    node_id: String,
    anchor_seq: Option<u64>,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_prove_inclusion_blocking(&fname, &node_id, anchor_seq) {
            Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
            Err(e) => Response::err(req_id, format!("AuditProveInclusion error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditProveInclusion requires a durable redb backend (no persist dir configured)"
                .to_string(),
        ),
    }
}

/// Everything [`apply_served_modality`] needs beyond the operation: the
/// resolved request identity, the gateway authz capture, the graph core and its
/// durability/CDC/materialization handles, plus the placement route and the
/// derived modality authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
#[cfg(feature = "modality-serving")]
struct ServedModalityCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
    tenant_scope: &'a str,
    gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    core: Arc<crate::graph::GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    modality_authority: Option<handlers::modality::ModalityAuthority>,
}

#[cfg(feature = "modality-serving")]
async fn apply_served_modality(
    ctx: ServedModalityCtx<'_>,
    op: eg_types::ServedModalityOp,
) -> Response {
    #[cfg(feature = "raft")]
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    #[cfg(feature = "raft")]
    let verified_context = ctx.verified_context;
    let tenant_scope = ctx.tenant_scope;
    let gateway_authz_ctx = ctx.gateway_authz_ctx;
    let core = ctx.core;
    let materialization_manifest = ctx.materialization_manifest;
    let persistence = ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = ctx.cdc;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let modality_authority = ctx.modality_authority;
    let method = Method::ServedModality { op: op.clone() };
    if op.mutates() && persistence.is_none() {
        return Response::err(
            req_id,
            "served modality operations require authoritative redb persistence",
        );
    }
    let authority = match modality_authority.as_ref() {
        Some(authority) => authority.clone(),
        None => {
            return Response::err(
                req_id,
                "served modality authority was not derived from verified context",
            )
        }
    };
    let (isolation, graph_type, owner) = gateway_authz_ctx
        .as_ref()
        .expect("ServedModality must be registered in the mutation gateway");
    #[cfg(feature = "raft")]
    if op.mutates() {
        let backend = persistence
            .as_ref()
            .expect("mutating ServedModality requires persistence");
        let principal_fingerprint = verified_context.principal_persistence_id();
        if let Some(routed) = routed_raft.as_ref() {
            return replicate_served_modality(
                state,
                routed.handle.clone(),
                routed.group_id,
                routed.epoch,
                Some(routed.group_id),
                graph_name,
                *graph_type,
                req_id,
                tenant_scope,
                &principal_fingerprint,
                &core,
                backend,
                method,
                &authority,
            )
            .await;
        }
    }
    let plan = crate::server::mutation::MutationPlan::for_method(&method);
    let ctx = crate::server::mutation::MutationCtx {
        req_id,
        caller,
        tenant_scope,
        graph_name,
        graph_type: *graph_type,
        owner: owner.as_deref(),
        isolation,
        core: &core,
        persistence: persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc: cdc.as_ref(),
        materialization_manifest: materialization_manifest.as_ref(),
        write_coalescer: None,
    };
    let op_apply = op.clone();
    return crate::server::mutation::commit_conditional_mutation(
        &ctx,
        &plan,
        &method,
        op.mutates(),
        move |staged_core| handlers::modality::handle(staged_core, &authority, op_apply),
    )
    .await;
}

/// The resolved request identity the Raft write-routing barrier replays into
/// the consensus entry. Bundled to keep the barrier at the documented parameter
/// cap; `routed` and `method` stay explicit because the caller's
/// `if let Some(routed)` is what proves the barrier applies at all.
#[cfg(feature = "raft")]
struct RaftWriteBarrierCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    verified_context: &'a VerifiedRequestContext,
    tenant_scope: &'a str,
    graph_type: crate::protocol::GraphType,
}

/// Replicate one durable mutation through Raft consensus instead of applying it
/// locally.
///
/// Takes the ALREADY-UNWRAPPED [`RoutedRaftHandle`]: the barrier is only reachable
/// with a routed group, and the caller's `if let Some(routed)` is that proof. The
/// durability guard stays at the call site because a non-durable mutation must keep
/// ownership of `method` for the local pipeline below.
#[cfg(feature = "raft")]
async fn dispatch_op_raft_write_routing_barrier(
    ctx: RaftWriteBarrierCtx<'_>,
    routed: crate::raft::multi::RoutedRaftHandle,
    method: Method,
) -> Response {
    let RaftWriteBarrierCtx {
        state,
        req_id,
        graph_name,
        verified_context,
        tenant_scope,
        graph_type,
    } = ctx;
    let created_at_ms = authoritative_now_ms();
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("raft-rpc", graph_name, req_id, &method);
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        req_id,
        tenant_scope,
        verified_context.principal_persistence_id(),
        false,
        routed.epoch,
        Some(routed.group_id),
        created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::ReplicatedMutation::graph(method, &server_secret) {
        Ok(command) => command,
        Err(error) => return Response::err(req_id, error),
    };
    let req = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(graph_name),
        graph_name: graph_name.to_string(),
        graph_type,
        committed_at_ms: created_at_ms,
        mutation,
        command,
    };
    match routed.handle.client_write(req).await {
        Ok(response) => {
            if let Some(error) = response.native_error {
                Response::err(req_id, error)
            } else {
                Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({
                        "replicated": true,
                        "group": routed.group_id,
                        "epoch": routed.epoch,
                        "fencing_token": routed.fencing_token(),
                    })),
                )
            }
        }
        Err(e) => {
            let leader = routed.handle.current_leader().await;
            Response::stale_route(req_id, graph_name, routed.group_id, routed.epoch, leader, e)
        }
    }
}

#[cfg(feature = "raft")]
async fn resolve_routed_raft(
    req_id: u64,
    graph_name: &str,
    multi_raft: Option<&std::sync::Arc<crate::raft::multi::MultiRaft>>,
) -> Result<Option<crate::raft::multi::RoutedRaftHandle>, Response> {
    let Some(multi) = multi_raft else {
        return Ok(None);
    };
    match multi.handle_for_graph(graph_name).await {
        Some(routed) => Ok(Some(routed)),
        None => {
            let route = multi.route_graph(graph_name).await;
            Err(Response::stale_route(
                req_id,
                graph_name,
                route.group,
                route.epoch,
                None,
                "authoritative placement group is not running on this node",
            ))
        }
    }
}

fn stamp_resource_reservation_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_resource_reservation_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => request.now_ms = now_ms,
        Method::QueryWorkItemReservation { request }
        | Method::ResourceReservationStatus { request } => request.now_ms = now_ms,
        Method::UpdateResourceHost { request } => request.now_ms = now_ms,
        _ => unreachable!("resource method classifier and timestamp binding diverged"),
    }
}

fn stamp_capacity_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_capacity_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::AcquireCapacity { request } => request.now_ms = now_ms,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.now_ms = now_ms
        }
        Method::ReclaimExpiredCapacity { request } => request.now_ms = now_ms,
        Method::UpdateCapacityCell { request } => request.now_ms = now_ms,
        Method::ReconcileCapacity { .. } | Method::CapacityStatus { .. } => {}
        _ => unreachable!("capacity method classifier and timestamp binding diverged"),
    }
}

fn stamp_resource_and_capacity_timestamps(method: &mut Method) {
    stamp_resource_reservation_timestamp(method);
    stamp_capacity_timestamp(method);
}

/// Cold-path lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3).
/// Only a registry MISS escalates to a write lock. The graph may be
/// catalog-known but not yet materialized (a lazy-startup boot scan, or a graph
/// the bounded hot-context cache evicted back to catalog-only); `lazy_open` is a
/// no-op for a genuinely unknown name, so the caller's "not found" error is
/// unchanged for that case.
#[cfg(feature = "redb")]
async fn lazy_open_graph(state: &Arc<RwLock<ServerState>>, graph_name: &str) {
    let cap = crate::server::persistence::cold_offload::max_resident_graphs();
    let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
    crate::server::persistence::cold_offload::lazy_open(state, graph_name, cap, page_size).await;
}

/// Universal served-data authority (CONCEPT:EG-P0-4): derive the row actor from
/// the cryptographically verified RequestContext while the authoritative
/// IsolationLayer is under the registry lock. Every graph read either consumes
/// this authority's detached projection or an existing handler that receives the
/// same IsolationLayer. Mutation documents can contain read phases (GraphQL
/// staged CONSTRUCT/UQL), so write requests carry the same verified tenant/actor
/// projection rather than gaining access to the raw committed core.
fn resolve_graph_read_authority(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    isolation: &crate::isolation::IsolationLayer,
) -> Result<(GraphReadAuthority, String), Response> {
    let authority = match GraphReadAuthority::from_verified(verified_context, isolation) {
        Ok(authority) => authority,
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let tenant_scope = authority
        .carrier()
        .expect("GraphReadAuthority always carries verified tenant authority")
        .tenant_scope()
        .to_string();
    Ok((authority, tenant_scope))
}

/// Mutation-gateway authz context (CONCEPT:EG-P0-2): for a
/// `mutation::GATEWAY_ROUTED` method, `commit_mutation` re-derives its OWN authz
/// decision from `(isolation, graph_type, owner)` rather than trusting the graph
/// ACL check — captured before the registry lock drops, ONLY for the routed set
/// (an `IsolationLayer` clone is not free, so this is skipped entirely for the
/// other ~330 methods).
fn graph_op_gateway_authz_ctx(
    s: &ServerState,
    method: &Method,
    entry: &GraphEntryFacts,
) -> Option<crate::server::mutation::GatewayAuthzCtx> {
    if !crate::server::mutation::is_gateway_routed(method) {
        return None;
    }
    Some((s.isolation.clone(), entry.graph_type, entry.owner.clone()))
}

/// Everything resolved while the registry read lock is still held.
struct GraphOpGate {
    entry: GraphEntryFacts,
    read_authority: GraphReadAuthority,
    tenant_scope: String,
    verified_actor: String,
    gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
}

/// Gate and resolve one graph operation under the registry lock, in the ORIGINAL
/// order: caller / existence / materialization / graph ACL first, then the
/// verified read authority, then the gateway authz context.
fn gate_graph_op_under_lock(
    s: &ServerState,
    ctx: GraphOpContext<'_>,
    method: &Method,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphOpGate, Response> {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    let entry = check_graph_op_access(
        s,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let (read_authority, tenant_scope) =
        resolve_graph_read_authority(req_id, verified_context, &s.isolation)?;
    let verified_actor = match read_authority.verified_actor() {
        Ok(actor) => actor.to_string(),
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let gateway_authz_ctx = graph_op_gateway_authz_ctx(s, method, &entry);
    Ok(GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    })
}

async fn dispatch_graph_op_inner(
    state: &Arc<RwLock<ServerState>>,
    ctx: GraphOpContext<'_>,
    mut method: Method,
    #[cfg(feature = "modality-serving")] modality_authority: Option<
        handlers::modality::ModalityAuthority,
    >,
    #[cfg(feature = "knowledge-batch")] knowledge_stream_authority: Option<
        handlers::knowledge_stream::KnowledgeStreamAuthority,
    >,
) -> Response {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    #[cfg(feature = "cypher")]
    if let Err(error) = handlers::query::validate_cypher_mode(&method) {
        return Response::err(req_id, error);
    }
    #[cfg(feature = "redb")]
    let mut s = timed_read(state).await;
    #[cfg(not(feature = "redb"))]
    let s = timed_read(state).await;
    // Cold-path lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3): the common case
    // (already resident) never pays for this — only a registry MISS escalates to a
    // write lock. The graph may be catalog-known but not yet materialized (a
    // lazy-startup boot scan, or a graph the bounded hot-context cache evicted
    // back to catalog-only); `lazy_open` is a no-op for a genuinely unknown name,
    // so the "not found" error below is unchanged for that case.
    #[cfg(feature = "redb")]
    if s.registry.get(graph_name).is_none() {
        drop(s);
        lazy_open_graph(state, graph_name).await;
        s = timed_read(state).await;
    }

    let access = graph_op_access_level(&method);
    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;

    let gate = match gate_graph_op_under_lock(
        &s,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        &method,
        access,
        state_machine_authorized,
    ) {
        Ok(gate) => gate,
        Err(resp) => return resp,
    };
    let GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    } = gate;
    let read_authority = Some(read_authority);
    // `verified_actor` is consumed only by the query/cypher/graphql/rdf/
    // knowledge-stream gateway arms below (each independently feature-gated); a
    // slim build with none of them enabled still needs this binding to compile.
    let verified_actor: &str = &verified_actor;

    let core = entry.core.clone();
    #[cfg(feature = "redb")]
    let graph_incarnation_id = entry.incarnation_id.clone();
    let materialization_manifest = s.registry.materialization_handle(graph_name);
    // Clone the authoritative durable backend under the registry lock. Mutation
    // paths below fail closed when it is absent and await its commit barrier.
    let persistence = s.persistence.clone();
    // Change-Data-Capture hub (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230): clone the handle under the same
    // lock so a successful durable mutation can emit an ordered change into this
    // graph's feed AFTER it applies. `None` ⇒ a non-streaming build ⇒ no emit, the
    // write path is byte-for-byte unchanged.
    #[cfg(feature = "streaming")]
    let cdc = s.cdc.clone();
    // The routed mutation coalescer is the sole live batching authority. The
    // former dispatch-local fallback accepted only the same five methods that
    // the GraphOps gateway consumes first, so it was unreachable.
    let routed_write_coalescer = s.routed_write_coalescer.clone();
    // A clustered graph operation requires the MultiRaft placement authority. The
    // former standalone group-0 handle is detected only to reject an incomplete
    // cluster configuration; it is never used as a write-routing fallback.
    #[cfg(feature = "raft")]
    let placement_authority = s.placement_authority();
    #[cfg(feature = "raft")]
    let multi_raft = resolve_multi_raft(&s, &placement_authority);
    #[cfg(feature = "raft")]
    let graph_type = entry.graph_type;
    // Cold-tenant access tracking (CONCEPT:EG-KG.backend.r6-feature, R6): clone the tracker under the same
    // registry lock so this graph's access recency is recorded after the lock is released
    // (a `touch` is one cheap map upsert, off the graph lock). The periodic cold-offload
    // sweep reads it to hibernate IDLE graphs; a recently-touched graph is never selected.
    // `redb`-only — whole-graph offload is a durable-tier capability (CONCEPT:EG-KG.sharding.eg-r6).
    #[cfg(feature = "redb")]
    let cold_tracker = s.cold_tracker.clone();
    // Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security): clone the isolation policy
    // under the same registry lock so the read-only query handler can filter its
    // off-lock snapshot down to the rows the caller may see. Only the read/query
    // surfaces need it (writes are already graph-ACL-gated above); cheap clone of a
    // small identity map, shared by Arc into the handler. `has_rules()==false` ⇒ the
    // filter is a no-op, single-tenant unchanged.
    #[cfg(feature = "security")]
    let rls = std::sync::Arc::new(s.isolation.clone());
    // Referenced by the read-query routing below only when a query/cypher/rdf surface
    // is compiled; keep it used in a security-but-no-query-surface build.
    #[cfg(all(
        feature = "security",
        not(any(feature = "query", feature = "cypher", feature = "rdf"))
    ))]
    let _ = &rls;
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — clone the committed tsdb store handle under
    // the same registry lock so a plan-sourced mining `Op::TsScan` leg (`handlers::mining`,
    // both the gateway-routed `Mine*` methods and `MineClassifyFit`) can bind the REAL store
    // instead of the old hardcoded `None`. Gated on `mining` too: it is unused otherwise.
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = s.tsdb_store.clone();
    drop(s); // Release registry lock before graph lock.

    #[cfg(feature = "raft")]
    if let Some(error) = placement_authority.missing_error() {
        return Response::err(req_id, error);
    }

    // Record this graph's access for the cold-offload sweep (CONCEPT:EG-KG.backend.r6-feature, R6) — both
    // reads and writes touch, so a graph being actively used is never offloaded.
    #[cfg(feature = "redb")]
    cold_tracker.touch_with_incarnation(graph_name, &graph_incarnation_id);

    // Mandatory placement resolution for ordinary graph operations. MultiRaft is
    // the sole clustered authority. Resolving here also fences reads away from a
    // node that no longer runs the graph's current group.
    #[cfg(feature = "raft")]
    let routed_raft = match resolve_routed_raft(req_id, graph_name, multi_raft.as_ref()).await {
        Ok(routed_raft) => routed_raft,
        Err(resp) => return resp,
    };

    // Resource lifecycle timestamps are authority inputs, not caller clocks.
    // Bind one leader/replicated-apply timestamp before dispatching either the
    // native MutationBatch or an authority read; followers replay the timestamp
    // carried by the committed Raft scope through `authoritative_now_ms()`.
    stamp_resource_and_capacity_timestamps(&mut method);

    let routing = GraphOpRouting {
        state,
        req_id,
        graph_name,
        caller,
        verified_context,
        state_machine_authorized,
        read_authority: &read_authority,
        verified_actor,
        tenant_scope: &tenant_scope,
        gateway_authz_ctx: &gateway_authz_ctx,
        core: &core,
        materialization_manifest: &materialization_manifest,
        persistence: &persistence,
        #[cfg(feature = "streaming")]
        cdc: &cdc,
        #[cfg(feature = "security")]
        rls: &rls,
        #[cfg(feature = "raft")]
        routed_raft: &routed_raft,
        #[cfg(feature = "raft")]
        graph_type,
        #[cfg(feature = "raft")]
        multi_raft: &multi_raft,
        #[cfg(feature = "redb")]
        graph_incarnation_id: &graph_incarnation_id,
        #[cfg(feature = "modality-serving")]
        modality_authority: &modality_authority,
        #[cfg(feature = "knowledge-batch")]
        knowledge_stream_authority: &knowledge_stream_authority,
    };
    let method = match route_graph_op_method(routing, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };

    crate::metrics::graph_op(graph_name);

    let response = run_dispatch_pipeline(
        DispatchPipelineCtx {
            state,
            req_id,
            graph_name,
            caller,
            read_authority: read_authority.clone(),
            verified_actor,
            tenant_scope: tenant_scope.clone(),
            gateway_authz_ctx: gateway_authz_ctx.clone(),
            core: core.clone(),
            materialization_manifest: materialization_manifest.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            routed_write_coalescer: routed_write_coalescer.clone(),
            #[cfg(feature = "security")]
            rls: rls.clone(),
            #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
            tsdb_store: tsdb_store.clone(),
        },
        method,
    )
    .await;

    finalize_graph_op_response(
        state,
        graph_name,
        &core,
        access,
        gateway_authz_ctx.is_some(),
        response,
    )
    .await
}

/// Everything the post-lock routers below need, snapshotted out of the registry
/// lock by `dispatch_graph_op_inner`. Every field is a shared reference or a
/// `Copy` scalar, so this is `Copy` and each router call is free. `read_authority`
/// and `verified_actor` are separate borrows of the CALLER's locals — the actor
/// string borrows from the authority, so the two cannot live in one owned struct.
#[derive(Clone, Copy)]
pub(super) struct GraphOpRouting<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) caller: Option<&'a str>,
    pub(super) verified_context: &'a VerifiedRequestContext,
    pub(super) state_machine_authorized: bool,
    pub(super) read_authority: &'a Option<GraphReadAuthority>,
    pub(super) verified_actor: &'a str,
    pub(super) tenant_scope: &'a str,
    pub(super) gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    pub(super) core: &'a Arc<crate::graph::GraphCore>,
    pub(super) materialization_manifest:
        &'a Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    pub(super) persistence: &'a Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub(super) cdc: &'a Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "security")]
    pub(super) rls: &'a std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    pub(super) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
    #[cfg(feature = "raft")]
    pub(super) graph_type: crate::protocol::GraphType,
    #[cfg(feature = "raft")]
    pub(super) multi_raft: &'a Option<Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "redb")]
    pub(super) graph_incarnation_id: &'a String,
    #[cfg(feature = "modality-serving")]
    pub(super) modality_authority: &'a Option<handlers::modality::ModalityAuthority>,
    #[cfg(feature = "knowledge-batch")]
    pub(super) knowledge_stream_authority:
        &'a Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}

/// The native durable authorities that own their own store and MutationBatch
/// kernel: resource reservations, capacity leases, WorkItem claim capability,
/// the development lane, and WorkItem transitions.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
async fn route_native_store_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let state_machine_authorized = ctx.state_machine_authorized;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "redb")]
    let graph_incarnation_id = ctx.graph_incarnation_id;
    // Reservation reads are authority reads, not GraphCore snapshots.  Under
    // placement they are served only by the current group leader; followers
    // fail closed with the normal redirect instead of returning stale holds.
    if crate::server::mutation_batch::is_resource_reservation_query_method(&method) {
        return Ok(dispatch_op_resource_reservation_query(
            req_id,
            graph_name,
            verified_context,
            persistence.clone(),
            #[cfg(feature = "raft")]
            multi_raft.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }

    // Capacity leases are a separate native authority from repository/resource
    // reservations.  They still share the same authenticated graph/tenant
    // boundary, current-placement leader check, and writer backpressure.  No
    // caller can renew/release on behalf of another owner: the opaque owner
    // digest is compared to the verified principal before redb sees the row.
    if crate::server::mutation_batch::is_capacity_method(&method) {
        return Ok(dispatch_op_capacity_ops(
            NativeOpCtx {
                req_id,
                graph_name,
                verified_context,
                persistence: persistence.clone(),
                #[cfg(feature = "raft")]
                multi_raft: multi_raft.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
            },
            state_machine_authorized,
            method,
        )
        .await);
    }

    // Native WorkItem claim capabilities use a dedicated private ledger and
    // never enter MutationBatch/result/outbox/CDC projections.  The verified
    // request context supplies all authority fields; the public method carries
    // only an item id (mint) or opaque bytes (verify).
    if matches!(
        &method,
        Method::MintWorkItemClaimCapability { .. } | Method::VerifyWorkItemClaimCapability { .. }
    ) {
        return Ok(dispatch_op_workitem_claim_capability(
            NativeOpCtx {
                req_id,
                graph_name,
                verified_context,
                persistence: persistence.clone(),
                #[cfg(feature = "raft")]
                multi_raft: multi_raft.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
            },
            #[cfg(feature = "redb")]
            graph_incarnation_id.clone(),
            method,
        )
        .await);
    }

    // ── Native development-lane hold/quota authority (RMDD-28) ──────────────────
    // `redb_store::development_lane` deliberately stops at the redb transaction
    // boundary -- no MutationBatch/result/outbox/CDC projection -- exactly the
    // WorkItem claim-capability posture above. The 6 write methods
    // (Reserve/Renew/Observe/Finish/Cleanup/UpdateQuota) commit through the
    // writer-thread `Cmd` channel (the kernel's own self-contained
    // begin_write()/commit()); the exact-query/status reads are MVCC snapshot
    // reads, same posture as the native reservation-ledger reads above. Every
    // request carries its own tenant/owner/fencing authority (CAS'd against the
    // live WorkItem row inside the kernel), not a server-derived
    // AuthenticatedAuthority, so — unlike claim capability — there is no
    // verified-context authority struct to build here.
    if crate::server::mutation_batch::is_development_lane_method(&method) {
        return Ok(dispatch_op_development_lane(
            req_id,
            graph_name,
            persistence.clone(),
            #[cfg(feature = "raft")]
            multi_raft.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }

    // Engine-native WorkItem transitions are result-producing durable CAS
    // operations. They must execute at the current placement leader (a generic
    // Raft acknowledgement cannot carry the selected work-item result), and their
    // redb MutationBatch atomically persists the transition/result/outbox before
    // the in-memory graph projection is refreshed.
    if crate::server::mutation_batch::is_work_item_mutation_method(&method) {
        return Ok(dispatch_op_workitem_mutation(
            req_id,
            graph_name,
            caller,
            core.clone(),
            persistence.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }
    Err(method)
}

/// The remaining authority-bearing surfaces, all resolved AFTER graph ACL,
/// lazy materialization and placement: the series-write fence, the knowledge
/// stream, time series, audit verification/inclusion, served modality, and the
/// Raft write-routing barrier.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
async fn route_graph_authority_surfaces(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let read_authority = ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let tenant_scope = ctx.tenant_scope;
    let gateway_authz_ctx = ctx.gateway_authz_ctx;
    let core = ctx.core;
    let materialization_manifest = ctx.materialization_manifest;
    let persistence = ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = ctx.cdc;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;
    #[cfg(feature = "modality-serving")]
    let modality_authority = ctx.modality_authority;
    #[cfg(feature = "knowledge-batch")]
    let knowledge_stream_authority = ctx.knowledge_stream_authority;
    // `TsAppend`/`TsEvict`/`TsDeleteSeries` are not yet Raft state-machine commands, so none
    // can rely on the durable-mutation barrier below. Still fence every `series.redb` WRITE
    // to the current placement leader: accepting a follower-local append, retention evict, or
    // whole-series delete would create a divergent `series.redb` projection and acknowledge a
    // write on the wrong replica. (`TsListSeries` is a read and is intentionally excluded.)
    #[cfg(all(feature = "raft", feature = "tsdb"))]
    if let Some(stale) =
        dispatch_op_tsdb_write_fence(req_id, graph_name, routed_raft.as_ref(), &method).await
    {
        return Ok(stale);
    }

    #[cfg(all(feature = "raft", feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = ts_placement_fence(routed_raft.as_ref());
    #[cfg(all(not(feature = "raft"), feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = (0, None);

    // One native KnowledgeBatch pull surface for every query family. This point is
    // deliberately after graph ACL, lazy-materialization and placement resolution,
    // and before any family-specific direct handler, so a cursor cannot bypass the
    // same RequestContext/RLS/placement boundary as its underlying query.
    #[cfg(feature = "knowledge-batch")]
    if matches!(&method, Method::KnowledgeStream { .. }) {
        return Ok(dispatch_op_knowledge_stream(
            KnowledgeStreamCtx {
                state,
                req_id,
                graph_name,
                verified_context,
                read_authority,
                verified_actor,
                core: core.clone(),
                #[cfg(feature = "security")]
                rls: rls.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
                knowledge_stream_authority: knowledge_stream_authority.clone(),
            },
            method,
        )
        .await);
    }

    // Time-series operations now run only after graph ACL + placement policy. The
    // handler derives the canonical `(tenant, graph, series)` storage key from this
    // already-authorized graph context.
    #[cfg(feature = "tsdb")]
    if matches!(
        &method,
        Method::TsAppend { .. }
            | Method::TsRange { .. }
            | Method::TsAsofJoin { .. }
            | Method::TsWindow { .. }
            | Method::TsGapFill { .. }
            | Method::TsEvict { .. }
            | Method::TsDeleteSeries { .. }
            | Method::TsListSeries
    ) {
        return Ok(dispatch_op_tsdb_ops(
            state,
            req_id,
            graph_name,
            verified_context,
            ts_placement_epoch,
            ts_fencing_token,
            method,
        )
        .await);
    }

    // Tamper-evident audit verification (CONCEPT:EG-KG.sharding.row-level-security): a read-only walk of the
    // target graph's durable hash-chained audit log. Routed to the redb backend's
    // owner thread (which flushes pending first). Handled here — AFTER the registry
    // lock is released — so blocking on the writer-thread reply never holds the lock.
    #[cfg(feature = "security")]
    if matches!(method, Method::AuditVerify) {
        return Ok(dispatch_op_audit_verify(req_id, graph_name, persistence.clone()).await);
    }

    // Provenance-anchor inclusion proof (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring): the
    // `AuditVerify` extension that reaches an anchored NODE's content, not just
    // mutation ordering. Same routing shape as `AuditVerify` immediately above —
    // the redb backend's owner thread (flushes pending first), handled after the
    // registry lock is released.
    #[cfg(feature = "security")]
    let method = match method {
        Method::AuditProveInclusion {
            node_id,
            anchor_seq,
        } => {
            return Ok(prove_audit_inclusion(
                req_id,
                graph_name,
                persistence.clone(),
                node_id,
                anchor_seq,
            )
            .await)
        }
        method => method,
    };

    // Concrete governed modality service. This is deliberately after graph ACL,
    // lazy materialization, placement resolution and verified-context authority
    // derivation, but before the generic replicated-mutation branch: the mutation gateway
    // stages a complete graph image and commits the encrypted runtime snapshot
    // through the authoritative MutationBatch boundary before publishing RAM.
    #[cfg(feature = "modality-serving")]
    let method = match method {
        Method::ServedModality { op } => {
            return Ok(apply_served_modality(
                ServedModalityCtx {
                    state,
                    req_id,
                    graph_name,
                    caller,
                    verified_context,
                    tenant_scope,
                    gateway_authz_ctx,
                    core: core.clone(),
                    materialization_manifest: materialization_manifest.clone(),
                    persistence: persistence.clone(),
                    #[cfg(feature = "streaming")]
                    cdc: cdc.clone(),
                    #[cfg(feature = "raft")]
                    routed_raft: routed_raft.clone(),
                    modality_authority: modality_authority.clone(),
                },
                op,
            )
            .await)
        }
        method => method,
    };

    // ── Raft write-routing barrier (CONCEPT:AU-KG.ingest.source-sync-canonical) ──────────────────────
    // When a cluster is active, a durable mutation goes through Raft consensus
    // (the leader's `client_write`) BEFORE it is applied+acked: the entry is
    // replicated to a quorum and then APPLIED on every node by the Raft state
    // machine. So we replace the local gateway call with `client_write` and return
    // its outcome — deterministic staging, the state-backed MutationBatch commit,
    // and RAM publication happen inside the Raft state machine, not here. A
    // follower returns a ForwardToLeader error which we surface so the client
    // retries against the leader. This branch is the ONLY behavioral difference vs
    // single-node, and it is taken only for durable mutations with Raft active.
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft
        .clone()
        .filter(|_| crate::mutation_apply::is_durable_mutation(&method))
    {
        return Ok(dispatch_op_raft_write_routing_barrier(
            RaftWriteBarrierCtx {
                state,
                req_id,
                graph_name,
                verified_context,
                tenant_scope,
                graph_type,
            },
            routed,
            method,
        )
        .await);
    }
    Err(method)
}

/// Try each post-lock router in the ORIGINAL order — change envelopes, then the
/// native stores, then the authority surfaces. A method none of them owns falls
/// through to the ordinary dispatch pipeline.
async fn route_graph_op_method(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_change_envelope_ops(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match route_native_store_ops(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_graph_authority_surfaces(ctx, method).await
}

fn graph_op_access_level(method: &Method) -> AccessLevel {
    // TsAppend used to self-route before this boundary, which accidentally classified
    // it as neither a graph read nor write. It is now graph-scoped and requires the
    // same Write ACL as every other mutation; so do the other two `series.redb`
    // mutations, TsEvict/TsDeleteSeries (retention) -- a Read-only caller must not be
    // able to evict points or delete a whole series any more than they could append
    // to one. All other Ts methods (including the read-only TsListSeries enumeration)
    // require only Read.
    if requires_write(method)
        || matches!(
            method,
            Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
        )
    {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    }
}

/// The registry facts a graph operation needs after the registry lock is
/// released. Copied out under the lock rather than held by reference.
struct GraphEntryFacts {
    graph_type: crate::protocol::GraphType,
    owner: Option<String>,
    core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "redb")]
    incarnation_id: String,
}

/// Gate one graph operation, in the ORIGINAL order: an unregistered or
/// unauthenticated caller is denied BEFORE existence is resolved -- never let
/// "Graph not found" vs "ACCESS_DENIED" tell a caller who could never pass ACL
/// for any graph whether the target graph exists (see
/// `access::check_caller_is_known`'s doc). A registered caller falls through to
/// existence, materialization validity, and then the real graph-type/owner-aware
/// decision.
fn check_graph_op_access(
    s: &ServerState,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphEntryFacts, Response> {
    check_known_caller(
        &s.isolation,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let Some(entry) = s.registry.get(graph_name) else {
        return Err(Response::err(
            req_id,
            format!("Graph '{graph_name}' not found"),
        ));
    };
    check_materialization_valid(&s.registry, req_id, graph_name)?;
    if !state_machine_authorized {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            access,
        ) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(GraphEntryFacts {
        graph_type: entry.graph_type,
        owner: entry.owner.clone(),
        core: entry.core.clone(),
        #[cfg(feature = "redb")]
        incarnation_id: entry.incarnation_id.clone(),
    })
}

/// MultiRaft is the sole clustered authority: a replicated apply proposes
/// nothing, and a node without MultiRaft placement has no write routing.
#[cfg(feature = "raft")]
fn resolve_multi_raft(
    s: &ServerState,
    placement_authority: &crate::server::state::PlacementAuthorityKind,
) -> Option<Arc<crate::raft::multi::MultiRaft>> {
    if is_replicated_apply() {
        return None;
    }
    if !matches!(
        placement_authority,
        crate::server::state::PlacementAuthorityKind::MultiRaft
    ) {
        return None;
    }
    s.multi_raft.clone()
}

#[cfg(all(feature = "raft", feature = "tsdb"))]
fn ts_placement_fence(routed: Option<&crate::raft::multi::RoutedRaftHandle>) -> (u64, Option<u64>) {
    match routed {
        Some(routed) => (routed.epoch, Some(routed.group_id)),
        None => (0, None),
    }
}

/// The dispatch shell's write tail: size gauges, the single projection
/// publication non-gateway writes rely on, and the semantic-ANN warm hook.
#[allow(unused_variables)]
async fn finalize_graph_op_response(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    access: AccessLevel,
    gateway_routed: bool,
    response: Response,
) -> Response {
    // Refresh the per-graph size gauges after mutations — both petgraph
    // counts are O(1), so this adds no meaningful write-path cost.
    #[cfg(feature = "metrics")]
    if matches!(access, AccessLevel::Write) {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    // Non-gateway writes still rely on the dispatch shell for their single
    // projection publication. Gateway writes already publish exactly once in
    // `commit_finalize`; marking them here as well would advance the resident OCC
    // version past the authoritative MutationBatch version.
    if matches!(access, AccessLevel::Write) && response.error.is_none() && !gateway_routed {
        core.mark_dirty();
    }

    // W0.4 semantic-ANN warm-on-demand (CONCEPT:EG-KG.storage.semantic-index-directory): a graph created, or
    // one whose embedding count crosses `ANN_BUILD_THRESHOLD`, AFTER the
    // boot-time warm task's one-shot snapshot never gets a trigger from it
    // otherwise. Every write that adds embeddings (`AddEmbedding`, and every
    // mining/graph-learning writeback) flows through this SAME dispatch tail, so
    // one hook here — spawned, never inline on the request path — covers them
    // all. Cheap no-op below the threshold or once already warm/warming.
    #[cfg(feature = "ann")]
    if matches!(access, AccessLevel::Write) && response.error.is_none() {
        crate::server::ann_warm::maybe_warm_after_write(state, graph_name, core).await;
    }

    response
}

fn check_known_caller(
    isolation: &crate::isolation::IsolationLayer,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<(), Response> {
    if !state_machine_authorized {
        if let Err(denied) = check_caller_is_known(isolation, caller, graph_name, access) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(())
}

fn check_materialization_valid(
    registry: &crate::registry::GraphRegistry,
    req_id: u64,
    graph_name: &str,
) -> Result<(), Response> {
    if let Some(manifest) = registry.materialization_manifest(graph_name) {
        if !manifest.valid {
            let phase = match manifest.phase {
                crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                crate::registry::MaterializationPhase::Partial => "partial",
                crate::registry::MaterializationPhase::Complete => "complete",
                crate::registry::MaterializationPhase::Failed => "failed",
            };
            return Err(Response::err(
                req_id,
                serde_json::json!({
                    "code": "PARTIAL_MATERIALIZATION",
                    "phase": phase,
                    "source_snapshot_version": manifest.source_snapshot_version,
                    "completeness_cursor": manifest.completeness_cursor.as_ref().map(|cursor| serde_json::json!({
                        "node_offset": cursor.node_offset,
                        "edge_offset": cursor.edge_offset,
                    })),
                    "retryable": manifest.phase != crate::registry::MaterializationPhase::Failed,
                })
                .to_string(),
            ));
        }
    }
    Ok(())
}

/// Everything the runtime-conditional query/RDF gateways need from the resolved
/// request, bundled so each router stays at the documented parameter cap. The
/// borrowed/owned split matches how `run_dispatch_pipeline` already held these
/// values: identity and authz captures are borrowed, the `Arc` handles are
/// cloned per stage because the mutation gateway moves them into an async apply
/// closure. Same shape as `crate::server::mutation::MutationCtx`.
#[cfg(any(
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "rdf"
))]
struct GatewayRouteCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    tenant_scope: &'a str,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    read_authority: &'a Option<GraphReadAuthority>,
    verified_actor: &'a str,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
}

#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
async fn route_query_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Result<Response, Method> {
    let GatewayRouteCtx {
        state,
        req_id,
        graph_name,
        caller,
        tenant_scope,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        gateway_authz_ctx,
        read_authority,
        verified_actor,
        #[cfg(feature = "security")]
        rls,
    } = ctx;
    if crate::server::mutation::is_query_gateway_method(&method)
        && !crate::server::mutation::is_query_native_coordinator(&method)
    {
        let mutates_now = requires_write(&method);
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let (iso, gtype, owner) = gateway_authz_ctx
            .as_ref()
            .expect("is_gateway_routed query method must have a captured GatewayAuthzCtx");
        let ctx = crate::server::mutation::MutationCtx {
            req_id,
            caller,
            tenant_scope,
            graph_name,
            graph_type: *gtype,
            owner: owner.as_deref(),
            isolation: iso,
            core: &core,
            persistence: persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: cdc.as_ref(),
            materialization_manifest: materialization_manifest.as_ref(),
            write_coalescer: None,
        };
        let method_apply = method.clone();
        let query_read_authority = read_authority.clone();
        #[cfg(feature = "security")]
        let rls_apply = rls.clone();
        let resp = crate::server::mutation::commit_conditional_mutation_async(
            &ctx,
            &plan,
            &method,
            mutates_now,
            move |staged_core| async move {
                match handlers::query::try_handle(
                    state,
                    handlers::TryHandleContext {
                        req_id,
                        graph_name,
                        read_authority: query_read_authority.as_ref(),
                        caller: verified_actor,
                    },
                    staged_core,
                    method_apply,
                    #[cfg(feature = "security")]
                    &rls_apply,
                )
                .await
                {
                    Ok(r) => match r.error {
                        Some(e) => Err(e),
                        None => Ok(r
                            .result
                            .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                    },
                    // Unreachable for a real routed query method (its name only
                    // exists when the surface is compiled); kept total.
                    Err(_) => Err("query surface not available in this build".to_string()),
                }
            },
        )
        .await;
        return Ok(resp);
    }
    match handlers::query::try_handle(
        state,
        handlers::TryHandleContext {
            req_id,
            graph_name,
            read_authority: read_authority.as_ref(),
            caller: verified_actor,
        },
        core.clone(),
        method,
        #[cfg(feature = "security")]
        &rls,
    )
    .await
    {
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}

#[cfg(feature = "rdf")]
async fn route_rdf_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Result<Response, Method> {
    let GatewayRouteCtx {
        state,
        req_id,
        graph_name,
        caller,
        tenant_scope,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        gateway_authz_ctx,
        read_authority,
        verified_actor,
        #[cfg(feature = "security")]
        rls,
    } = ctx;
    if crate::server::mutation::is_rdf_gateway_method(&method) {
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let (iso, gtype, owner) = gateway_authz_ctx
            .as_ref()
            .expect("is_gateway_routed rdf method must have a captured GatewayAuthzCtx");
        let ctx = crate::server::mutation::MutationCtx {
            req_id,
            caller,
            tenant_scope,
            graph_name,
            graph_type: *gtype,
            owner: owner.as_deref(),
            isolation: iso,
            core: &core,
            persistence: persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: cdc.as_ref(),
            materialization_manifest: materialization_manifest.as_ref(),
            write_coalescer: None,
        };
        let method_apply = method.clone();
        let rdf_read_authority = read_authority.clone();
        #[cfg(feature = "security")]
        let rls_apply = rls.clone();
        let resp = crate::server::mutation::commit_conditional_mutation_async(
            &ctx,
            &plan,
            &method,
            true,
            move |staged_core| async move {
                match handlers::rdf::try_handle(
                    state,
                    handlers::TryHandleContext {
                        req_id,
                        graph_name,
                        read_authority: rdf_read_authority.as_ref(),
                        caller: verified_actor,
                    },
                    staged_core,
                    method_apply,
                    #[cfg(feature = "security")]
                    &rls_apply,
                )
                .await
                {
                    Ok(r) => match r.error {
                        Some(e) => Err(e),
                        None => Ok(r
                            .result
                            .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                    },
                    Err(_) => Err("rdf surface not available in this build".to_string()),
                }
            },
        )
        .await;
        return Ok(resp);
    }
    match handlers::rdf::try_handle(
        state,
        handlers::TryHandleContext {
            req_id,
            graph_name,
            read_authority: read_authority.as_ref(),
            caller: verified_actor,
        },
        core.clone(),
        method,
        #[cfg(feature = "security")]
        &rls,
    )
    .await
    {
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}

/// Everything [`run_dispatch_pipeline`] routes with, resolved once per request
/// by `dispatch_graph_op`: the caller's verified identity and read authority,
/// the live graph core with its durability/CDC/materialization handles, and the
/// two write coalescers. Bundled so the pipeline keeps ONE routing parameter
/// beside the method it is routing, instead of a seventeen-long list.
struct DispatchPipelineCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    read_authority: Option<GraphReadAuthority>,
    verified_actor: &'a str,
    tenant_scope: String,
    gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
    core: Arc<crate::graph::GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    routed_write_coalescer:
        Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
}

/// Stage 1: the universal mutation gateway, then the per-graph write
/// coalescer, then the stateless pure-compute domains.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_gateway_and_stateless_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    let routed_write_coalescer = &ctx.routed_write_coalescer;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    // Mutation-gateway routing (CONCEPT:EG-P0-2): the primary CRUD + agent-
    // memory writes (`mutation::GATEWAY_ROUTED`) are routed through the
    // single `commit_mutation` gateway — policy-driven authz, durability,
    // audit, CDC, and TMS in ONE call. Native stores use their own explicit
    // MutationBatch kernels. There is no second post-dispatch durability tail.
    let method = match handlers::graph_ops::try_handle_gateway(
        req_id,
        caller,
        tenant_scope,
        graph_name,
        core,
        materialization_manifest.as_ref(),
        read_authority.as_ref(),
        persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc.as_ref(),
        Some(routed_write_coalescer),
        gateway_authz_ctx.as_ref(),
        #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
        tsdb_store.as_ref(),
        method,
    )
    .await
    {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Pure-compute domains (stateless: no graph core / lock) route first; a
    // method that isn't theirs is handed back via Err and falls through to the
    // graph-op match below. (CONCEPT:EG-KG.query.dispatch-routing — thin routing; logic in handlers/.)
    // Feature-gated: in a slim build the line is absent and the method flows
    // straight through to graph_ops (whose catch-all reports "not available").
    #[cfg(feature = "finance")]
    let method = match handlers::finance::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Native TTS synthesis (GOC-34, `OWNER-VOICE-TTS`): stateless, like finance
    // above. `caller` is already the verified eg2-authenticated principal in
    // scope at this point — see `handlers::tts`'s own doc for exactly how (and
    // how far) that maps to the frozen contract's `PolicyDecision`.
    #[cfg(feature = "tts-piper")]
    let method = match handlers::tts::try_handle(req_id, caller, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 2: the graph-scoped compute domains — data science, mining,
/// graph learning and the ML pipeline read verbs.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_graph_scoped_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let read_authority = &ctx.read_authority;
    let core = &ctx.core;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    #[cfg(feature = "datascience")]
    let method = match handlers::datascience::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Data-mining domain (CONCEPT:EG-KG.mining.frequent-itemset-mining): GRAPH-SCOPED
    // (unlike finance/datascience), so it takes the graph core — the graph-derived
    // transaction source reads node neighborhoods and write-back materializes
    // `:AssociationRule` nodes into it. A method whose feature is off falls through
    // to the graph_ops not-available catch-all.
    #[cfg(feature = "mining")]
    let method = match handlers::mining::try_handle(
        req_id,
        core.clone(),
        read_authority.as_ref(),
        #[cfg(all(feature = "query", feature = "tsdb"))]
        graph_name,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_store.as_ref(),
        method,
    ) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Graph-learning domain (CONCEPT:EG-KG.graphlearn.link-predictor): GRAPH-SCOPED
    // like mining — the KAN link-predictor reads the live subgraph and write-back
    // materializes `:PredictedEdge`/`:EdgeFunction` nodes into the core. A method
    // whose feature is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "graphlearn")]
    let method = match handlers::graphlearn::try_handle(req_id, core.clone(), method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): the READ verbs
    // (Evaluate/Compare) route here with the graph core; Train/Serve/Predict are
    // GATEWAY_ROUTED (writeback) and never reach this fallback. A build without
    // `ml-pipeline` omits this line.
    #[cfg(feature = "ml-pipeline")]
    let method =
        match handlers::pipeline::try_handle(req_id, core.clone(), read_authority.as_ref(), method)
        {
            Ok(r) => return Ok(r),
            Err(m) => m,
        };
    Err(method)
}

/// Stage 3: the runtime-conditional query and native-RDF gateways.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_query_and_rdf_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    #[cfg(feature = "security")]
    let rls = &ctx.rls;
    // Read-only query surface — SQL (CONCEPT:EG-KG.query.read-only-sql-query, DataFusion behind
    // `query`) AND Cypher (CONCEPT:EG-KG.query.dep-free-behind, dep-free behind `cypher`) AND GraphQL
    // (CONCEPT:EG-KG.query.sparql-completeness, pure-Rust eg-graphql behind `graphql`): borrows the graph
    // core for an off-lock snapshot, runs on the blocking pool. Gated on ANY of the
    // three features so CypherQuery still routes in a cypher-only (no-DataFusion) Pi
    // build and GraphQl routes in a graphql build; the handler's per-method arm
    // falls through (Err) when ITS feature is off, so Sql/CypherQuery/GraphQl then
    // reach the graph_ops not-available catch-all. GraphQL — like SQL/Cypher/SPARQL
    // — runs UNDER the SAME RLS-aware result-cache compose (`caller`/`&rls` threaded
    // in, the cache key folds the caller's RLS context, the snapshot is RLS-filtered
    // to the caller) so a GraphQL read NEVER leaks across agents. Slim builds with
    // NONE of the three omit this line.
    //
    // Runtime-conditional query gateway (CONCEPT:EG-P0-2, L11): `Sql`/
    // `CypherQuery`/`GraphQl` are `mutation::GATEWAY_ROUTED`, but their execution
    // is `async` and needs `state`/`rls`, so they are routed HERE (not at the
    // graph-ops `try_handle_gateway`, which hands them back). The SAME runtime
    // parse `access::requires_write` uses decides whether THIS statement mutates:
    // a SQL write / Cypher `CREATE|SET|DELETE` / GraphQL `mutation` → the full
    // Write-authz commit; a `SELECT` / read-only Cypher / GraphQL `query` → a
    // Read-authz passthrough with no durability/audit/CDC. Every OTHER query
    // method (`UnifiedQuery`/`Explain*`/`Txn*Query`) is a pure read handled by
    // the unchanged direct call in the `else` arm.
    #[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
    let method = match route_query_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    // Native RDF/SPARQL surface (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql/218, features `rdf`/`sparql`):
    // AddTriples (durable — the shell below records it like any write),
    // GetRdf + Sparql (read-only, off-lock snapshot). Graph-scoped, so the
    // handler takes the graph core + name. Multi-valued literals are embedded
    // losslessly in that graph image. Gated on `rdf`; a method whose feature is
    // off falls through (Err) to the graph_ops not-available catch-all.
    //
    // Native-RDF write gateway (CONCEPT:EG-P0-2, L11): `AddTriples`/
    // `RemoveTriples`/`DropNamedGraph` are `mutation::GATEWAY_ROUTED` (GraphRedb-
    // durable, audited), routed HERE (not at `try_handle_gateway`) because their
    // handler is async and also performs RDF policy validation. They always
    // mutate (`mutates_now = true`), so `commit_conditional_mutation_async` runs
    // the full Write-authz + durable audit-chain commit; the read-only
    // RDF methods (`GetRdf`/`Sparql`/`ShaclValidate`/…) take the unchanged direct
    // call in the `else` arm.
    #[cfg(feature = "rdf")]
    let method = match route_rdf_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 4: the process-global domains — sandboxed UDFs, query federation
/// and distributed compute.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_process_global_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    // WASM-sandboxed UDF surface (CONCEPT:EG-KG.query.rowset-execution, feature `wasm-udf`):
    // RegisterUdf compiles+caches, RunUdf runs sandboxed (fuel+memory+no host
    // caps) — both off-reactor. Process-global (not graph-scoped), so it takes
    // `state` for the UdfRegistry. A method whose feature is off falls through.
    #[cfg(feature = "wasm-udf")]
    let method = match handlers::wasm_udf::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Query federation (CONCEPT:EG-KG.query.query-federation, feature `federation`):
    // RegisterForeignSource records a named foreign source on ServerState. The
    // `Op::ForeignScan` op itself runs through the unified-query handler above
    // (inline spec). Process-global, so it takes `state`. A method whose feature
    // is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "federation")]
    let method = match handlers::federation::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Distributed graph compute (CONCEPT:EG-KG.storage.feature, feature `compute-dist`):
    // DistributedCompute + the matview lifecycle. Cross-shard, so it takes
    // `state` (it gathers each shard graph's snapshot from the registry).
    #[cfg(any(feature = "compute-dist", feature = "matview"))]
    let method = match handlers::dist_compute::try_handle(
        state,
        req_id,
        caller,
        read_authority.as_ref(),
        method,
    )
    .await
    {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// The gateway/compute half of the routing pipeline.
async fn route_pipeline_compute(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_gateway_and_stateless_domains(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_graph_scoped_domains(ctx, method).await
}

/// The query-surface / process-global half of the routing pipeline.
async fn route_pipeline_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_query_and_rdf_surfaces(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_process_global_domains(ctx, method).await
}

/// Route one already-authorized graph operation through the dispatch pipeline.
///
/// The single thirteen-step `'dispatch:` block this replaced is now four stages
/// tried in order, each handing back a method it does not own. Stage order is
/// unchanged, so the gateway still sees every routed mutation first and the
/// terminal graph-op handler still owns the catch-all.
async fn run_dispatch_pipeline(ctx: DispatchPipelineCtx<'_>, method: Method) -> Response {
    let method = match route_pipeline_compute(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    let method = match route_pipeline_surfaces(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    // Terminal handler: graph-targeted ops (borrow the core; cross-graph ops
    // re-enter the registry via `state`). Owns the catch-all, returns a Response.
    let Some(read_authority) = ctx.read_authority.as_ref() else {
        return Response::err(
            ctx.req_id,
            "mutation escaped the universal mutation gateway before terminal dispatch",
        );
    };
    handlers::graph_ops::try_handle(
        ctx.state,
        ctx.req_id,
        ctx.caller,
        ctx.graph_name,
        read_authority,
        ctx.core.clone(),
        method,
    )
    .await
}

// ── Agent-memory / scene / trajectory dispatch round-trip (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ────
//
// Drive the EG-318 Methods through the SAME `dispatch` entrypoint a wire request
// hits (auth → routing → access-classify → handler → GraphCore), proving each wire
// op reaches its eg-core primitive and returns the expected payload — the served
// surface, not the library unit. Runs on a bare `--features server` build (the
// state builder gates every optional field behind its own feature).
#[cfg(all(test, feature = "ast"))]
mod ast_input_hardening_tests {
    use super::*;

    fn limits() -> AstInputLimits {
        AstInputLimits {
            max_files: 2,
            max_source_bytes: 8,
            max_total_bytes: 12,
        }
    }

    fn pack(files: Vec<(String, serde_bytes::ByteBuf)>) -> Vec<u8> {
        rmp_serde::to_vec(&files).expect("encode AST source fixture")
    }

    #[test]
    fn accepts_canonical_bounded_relative_sources() {
        let encoded = pack(vec![(
            "src/lib.rs".to_string(),
            serde_bytes::ByteBuf::from(b"fn x(){}".to_vec()),
        )]);
        let decoded = decode_ast_files(&encoded, limits()).expect("valid source collection");
        assert_eq!(
            decoded,
            vec![("src/lib.rs".to_string(), b"fn x(){}".to_vec())]
        );
    }

    #[test]
    fn rejects_host_paths_traversal_duplicates_and_declared_bombs() {
        for name in [
            "/private/source.rs",
            "../source.rs",
            "C:\\source.rs",
            "a/./b.rs",
        ] {
            let encoded = pack(vec![(
                name.to_string(),
                serde_bytes::ByteBuf::from(vec![1]),
            )]);
            assert!(
                decode_ast_files(&encoded, limits()).is_err(),
                "accepted {name}"
            );
        }

        let duplicate = pack(vec![
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![1])),
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![2])),
        ]);
        assert!(decode_ast_files(&duplicate, limits()).is_err());

        // array32 with a huge declared count and no entries: rejection happens
        // before allocation or element decoding.
        let declared_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_ast_files(&declared_bomb, limits()).is_err());
    }

    #[test]
    fn rejects_per_source_and_aggregate_overflow() {
        let one_too_large = pack(vec![(
            "a.rs".to_string(),
            serde_bytes::ByteBuf::from(vec![0; 9]),
        )]);
        assert!(decode_ast_files(&one_too_large, limits()).is_err());

        let aggregate = pack(vec![
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![0; 7])),
            ("b.rs".to_string(), serde_bytes::ByteBuf::from(vec![0; 7])),
        ]);
        assert!(decode_ast_files(&aggregate, limits()).is_err());
    }
}

#[cfg(test)]
mod nested_payload_security_tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Element<'a> {
        role: &'a str,
        name: &'a str,
        x: i64,
        y: i64,
        w: i64,
        h: i64,
    }

    #[derive(Serialize)]
    struct ScreenWire<'a> {
        session_id: &'a str,
        frame_seq: u64,
        prev_frame_id: &'a str,
        prev_hash: u64,
        png: serde_bytes::ByteBuf,
        elements: Vec<Element<'a>>,
    }

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0]);
        bytes
    }

    fn screen_blob(session: &str, frame_seq: u64, previous: &str, width: u32) -> Vec<u8> {
        rmp_serde::to_vec_named(&ScreenWire {
            session_id: session,
            frame_seq,
            prev_frame_id: previous,
            prev_hash: 0,
            png: serde_bytes::ByteBuf::from(png(width, 1080)),
            elements: vec![Element {
                role: "button",
                name: "Save",
                x: 1,
                y: 2,
                w: 10,
                h: 10,
            }],
        })
        .unwrap()
    }

    #[test]
    fn screen_observation_is_bounded_and_session_local() {
        let valid = screen_blob("session-1", 2, "screenobservation:session-1:1", 1920);
        assert!(decode_screen_observation(&valid).is_ok());

        let cross_session = screen_blob("session-1", 2, "screenobservation:session-2:1", 1920);
        assert!(decode_screen_observation(&cross_session).is_err());

        let oversized_dimensions = screen_blob("session-1", 0, "", 40_000);
        assert!(decode_screen_observation(&oversized_dimensions).is_err());
        assert!(decode_screen_observation(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn multi_graph_batch_rejects_duplicate_graphs_and_inner_bombs() {
        let empty_ops = serde_bytes::ByteBuf::from(vec![0x90]);
        let valid = rmp_serde::to_vec_named(&vec![("graph-a", empty_ops.clone())]).unwrap();
        assert_eq!(decode_multi_graph_batches(&valid).unwrap().len(), 1);

        let duplicate = rmp_serde::to_vec_named(&vec![
            ("graph-a", empty_ops.clone()),
            ("graph-a", empty_ops),
        ])
        .unwrap();
        assert!(decode_multi_graph_batches(&duplicate).is_err());

        let inner_bomb = rmp_serde::to_vec_named(&vec![(
            "graph-a",
            serde_bytes::ByteBuf::from(vec![0xdd, 0xff, 0xff, 0xff, 0xff]),
        )])
        .unwrap();
        assert!(decode_multi_graph_batches(&inner_bomb).is_err());
    }

    #[test]
    fn request_preflight_scans_binary_fields_but_not_opaque_payloads() {
        let bomb = vec![0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(preflight_request_msgpack(&Method::AddNode {
            node_id: "node".to_string(),
            properties_msgpack: bomb,
        })
        .is_err());
        assert!(preflight_request_msgpack(&Method::Sql {
            query: "SELECT 1".to_string(),
            params_msgpack: Vec::new(),
        })
        .is_ok());
    }
}

#[cfg(all(test, feature = "redb"))]
mod eg318_dispatch_tests {
    use super::*;
    #[cfg(feature = "tsdb")]
    use crate::acl::{AgentIdentity, AgentRole};
    use crate::durability::DurabilityPolicy;
    use crate::protocol::{Method, Request};
    #[cfg(feature = "tsdb")]
    use crate::server::auth::sign_current_test_request;
    use crate::server::auth::{
        build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap,
    };
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::sync::Arc;

    const SECRET: &str = "eg318-test-secret";

    struct DurableTestState {
        state: Option<Arc<RwLock<ServerState>>>,
        dir: PathBuf,
    }

    impl Deref for DurableTestState {
        type Target = Arc<RwLock<ServerState>>;

        fn deref(&self) -> &Self::Target {
            self.state.as_ref().expect("test state remains live")
        }
    }

    impl Drop for DurableTestState {
        fn drop(&mut self) {
            // Close redb before deleting its test directory.
            drop(self.state.take());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn state_min() -> DurableTestState {
        let dir = std::env::temp_dir().join(format!(
            "eg318-dispatch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let dir_string = dir.to_string_lossy().to_string();
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_string.clone(), DurabilityPolicy::Each, 64)
                .expect("open authoritative test backend"),
        );
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation("system"));
        state.persist_dir = Some(dir_string);
        state.persistence = Some(persistence);
        let state = Arc::new(RwLock::new(state));
        DurableTestState {
            state: Some(state),
            dir,
        }
    }

    fn req(id: u64, method: Method) -> Request {
        build_shared_test_request(SECRET, id, "__commons__", "system", method)
    }

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-220 — CreateSummaryNode over the wire → SummaryChildren
    /// reads back the linked children.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_create_summary_then_read_children() {
        let state = state_min();
        for (i, id) in ["e1", "e2"].iter().enumerate() {
            let r = dispatch_on_heap(
                &state,
                req(
                    100 + i as u64,
                    Method::AddNode {
                        node_id: (*id).into(),
                        properties_msgpack: blob(serde_json::json!({"type": "Episodic"})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "AddNode: {:?}", r.error);
        }
        let created = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CreateSummaryNode {
                    level: 1,
                    child_ids: vec!["e1".into(), "e2".into()],
                    props_msgpack: blob(serde_json::json!({})),
                },
            ),
        )
        .await;
        let sid = match created.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("CreateSummaryNode: {:?} / {:?}", other, created.error),
        };
        let children = dispatch_on_heap(
            &state,
            req(
                2,
                Method::SummaryChildren {
                    node_id: sid.clone(),
                },
            ),
        )
        .await;
        match children.result {
            Some(ResultPayload::Ids(ids)) => assert_eq!(ids, vec!["e1", "e2"]),
            other => panic!("SummaryChildren: {:?} / {:?}", other, children.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-221 — Consolidate over the wire returns the deterministic
    /// semantic node id.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_consolidate_returns_semantic_id() {
        let state = state_min();
        for (i, id) in ["a", "b"].iter().enumerate() {
            let _ = dispatch_on_heap(
                &state,
                req(
                    200 + i as u64,
                    Method::AddNode {
                        node_id: (*id).into(),
                        properties_msgpack: blob(serde_json::json!({"type": "Episodic"})),
                    },
                ),
            )
            .await;
        }
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::Consolidate {
                    episodic_ids: vec!["a".into(), "b".into()],
                    semantic_props_msgpack: blob(serde_json::json!({"summary": "s"})),
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::String(s)) => assert!(s.starts_with("semantic:")),
            other => panic!("Consolidate: {:?} / {:?}", other, r.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — Maintain (decay + evict) over the wire returns the
    /// `(decayed, pruned_ids)` tuple.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_maintain_decays_and_evicts() {
        let state = state_min();
        // A low-importance node in the working set gets evicted below threshold.
        let _ = dispatch_on_heap(
            &state,
            req(
                300,
                Method::AddNode {
                    node_id: "low".into(),
                    properties_msgpack: blob(serde_json::json!({"importance": 0.1})),
                },
            ),
        )
        .await;
        let r = dispatch_on_heap(
            &state,
            req(
                4,
                Method::Maintain {
                    ids: vec!["low".into()],
                    now_ms: 1_000,
                    half_life_ms: 604_800_000,
                    evict_threshold: 0.5,
                    delete: false,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("Maintain: {:?} / {:?}", other, r.error),
        };
        let (_decayed, pruned): (usize, Vec<String>) = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(pruned, vec!["low"]);
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — AddSceneObject over the wire → WorldTransform reads
    /// back the composed world pose.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_scene_object_then_world_transform() {
        let state = state_min();
        let pose = serde_json::json!({"translation": {"x": 5.0, "y": 0.0, "z": 0.0}});
        let created = dispatch_on_heap(
            &state,
            req(
                5,
                Method::AddSceneObject {
                    pose_msgpack: blob(pose),
                    parent: None,
                },
            ),
        )
        .await;
        let oid = match created.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("AddSceneObject: {:?} / {:?}", other, created.error),
        };
        let wt = dispatch_on_heap(&state, req(6, Method::WorldTransform { node_id: oid })).await;
        match wt.result {
            Some(ResultPayload::Json(v)) => {
                let tx = v["translation"]["x"].as_f64().unwrap();
                assert!((tx - 5.0).abs() < 1e-9, "world x = {tx}");
            }
            other => panic!("WorldTransform: {:?} / {:?}", other, wt.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — StartTrajectory + AppendStep over the wire →
    /// DiscountedReturn computes `Σ gamma^t · reward`.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_trajectory_append_then_discounted_return() {
        let state = state_min();
        let started = dispatch_on_heap(
            &state,
            req(
                7,
                Method::StartTrajectory {
                    props_msgpack: blob(serde_json::json!({})),
                },
            ),
        )
        .await;
        let tid = match started.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("StartTrajectory: {:?} / {:?}", other, started.error),
        };
        for (i, reward) in [2.0f64, 4.0].into_iter().enumerate() {
            let r = dispatch_on_heap(
                &state,
                req(
                    8 + i as u64,
                    Method::AppendStep {
                        traj_id: tid.clone(),
                        action_msgpack: blob(serde_json::json!("go")),
                        reward,
                        state_ref: None,
                        next_state_ref: None,
                        t: i as u64,
                    },
                ),
            )
            .await;
            // Raw(Option<String>) — Some(step id) since the trajectory exists.
            match r.result {
                Some(ResultPayload::Raw(b)) => {
                    let step: Option<String> = rmp_serde::from_slice(&b).unwrap();
                    assert!(step.is_some(), "AppendStep should return a step id");
                }
                other => panic!("AppendStep: {:?} / {:?}", other, r.error),
            }
        }
        let dr = dispatch_on_heap(
            &state,
            req(
                20,
                Method::DiscountedReturn {
                    traj_id: tid,
                    gamma: 0.5,
                },
            ),
        )
        .await;
        match dr.result {
            // 2.0 + 0.5^1 * 4.0 = 4.0
            Some(ResultPayload::Float(f)) => assert!((f - 4.0).abs() < 1e-9, "return = {f}"),
            other => panic!("DiscountedReturn: {:?} / {:?}", other, dr.error),
        }
    }

    /// Public Ts* calls must share the ordinary graph ACL boundary, and identical
    /// local series ids in two tenants must never collide in series.redb.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_is_graph_authorized_and_tenant_scoped() {
        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-policy-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
            // RBAC (`feature = "security"`) is the mandatory current access decision
            // for a non-System identity — `check_access` ignores `graph_owner`
            // entirely under this feature and evaluates ONLY `identity.roles`
            // against the RBAC policy (no pre-RBAC ACL fall-through, no "owner
            // always wins" shortcut). So each agent needs an explicit grant on
            // their own private graph, or every Ts* call below default-denies.
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                s.isolation.add_role(Role::new("owner-acme-private"));
                s.isolation.add_role(Role::new("owner-other-private"));
                let grant = |role: &str, graph: &str, action: RbacAction| Grant {
                    role: role.to_string(),
                    resource: ResourceSelector::Graph(graph.to_string()),
                    action,
                    effect: GrantEffect::Allow,
                };
                for action in [RbacAction::Read, RbacAction::Write] {
                    s.isolation
                        .add_grant(grant("owner-acme-private", "acme:private", action));
                    s.isolation
                        .add_grant(grant("owner-other-private", "other:private", action));
                }
            }
            s.isolation.register_agent(AgentIdentity {
                agent_id: "alice".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-acme-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            s.isolation.register_agent(AgentIdentity {
                agent_id: "bob".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-other-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            let _ = s.registry.create_graph(
                "acme:private",
                crate::protocol::GraphType::Agent,
                Some("alice".into()),
            );
            let _ = s.registry.create_graph(
                "other:private",
                crate::protocol::GraphType::Agent,
                Some("bob".into()),
            );
        }
        let request = |id: u64, graph: &str, agent: &str, method: Method| {
            sign_current_test_request(
                SECRET,
                Request {
                    id,
                    graph: graph.into(),
                    auth_token: String::new(),
                    agent_id: Some(agent.into()),
                    method,
                },
            )
        };
        let append = |value: f64| Method::TsAppend {
            series_id: "cpu".into(),
            n_fields: 1,
            bucket_ns: 1_000,
            field_names: vec!["value".into()],
            points_msgpack: rmp_serde::to_vec(&vec![(1i64, vec![value])]).unwrap(),
        };
        assert!(
            dispatch_on_heap(&state, request(1, "acme:private", "alice", append(10.0)))
                .await
                .error
                .is_none()
        );
        assert!(
            dispatch_on_heap(&state, request(2, "other:private", "bob", append(20.0)))
                .await
                .error
                .is_none()
        );

        let denied = dispatch_on_heap(
            &state,
            request(
                3,
                "other:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        assert!(
            denied.error.is_some(),
            "cross-tenant series read must be denied"
        );

        let own = dispatch_on_heap(
            &state,
            request(
                4,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        let points: Vec<(i64, Vec<f64>)> = match own.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert_eq!(points, vec![(1, vec![10.0])]);
        drop(state);
        let _ = std::fs::remove_file(path);
    }

    /// End-to-end reachability proof for `TsListSeries`/`TsEvict`/`TsDeleteSeries`
    /// (CONCEPT:EG-KG.storage.series-retention-reachability): before this test's
    /// production code existed, `SeriesStore::evict_before`/`delete_series`/
    /// `list_series` were reachable ONLY from `eg-tsdb`'s own crate-internal unit
    /// tests — no `Method` variant, no RPC route, no caller anywhere in `src/`. This
    /// drives all three through the SAME `dispatch()` entrypoint a wire request
    /// hits, against a REAL `SeriesStore`/redb file, proving: (1) `TsListSeries`
    /// enumerates a just-appended series scoped to the caller's own tenant/graph
    /// (never cross-tenant, mirroring `timeseries_is_graph_authorized_and_tenant_scoped`
    /// above); (2) `TsEvict` actually removes only the points before its cutoff,
    /// verified by reading the survivors back with `TsRange`; (3) `TsDeleteSeries`
    /// removes the series entirely -- it drops off `TsListSeries` and `TsRange`
    /// against it comes back empty, not an error (an unknown series is legal, per
    /// `SeriesStore::range_scoped`'s existing "empty for an unknown series"
    /// contract).
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_retention_evict_delete_and_list_are_scoped_and_reachable() {
        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-retention-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                s.isolation.add_role(Role::new("owner-acme-private"));
                s.isolation.add_role(Role::new("owner-other-private"));
                let grant = |role: &str, graph: &str, action: RbacAction| Grant {
                    role: role.to_string(),
                    resource: ResourceSelector::Graph(graph.to_string()),
                    action,
                    effect: GrantEffect::Allow,
                };
                for action in [RbacAction::Read, RbacAction::Write] {
                    s.isolation
                        .add_grant(grant("owner-acme-private", "acme:private", action));
                    s.isolation
                        .add_grant(grant("owner-other-private", "other:private", action));
                }
            }
            s.isolation.register_agent(AgentIdentity {
                agent_id: "alice".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-acme-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            s.isolation.register_agent(AgentIdentity {
                agent_id: "bob".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-other-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            let _ = s.registry.create_graph(
                "acme:private",
                crate::protocol::GraphType::Agent,
                Some("alice".into()),
            );
            let _ = s.registry.create_graph(
                "other:private",
                crate::protocol::GraphType::Agent,
                Some("bob".into()),
            );
        }
        let request = |id: u64, graph: &str, agent: &str, method: Method| {
            sign_current_test_request(
                SECRET,
                Request {
                    id,
                    graph: graph.into(),
                    auth_token: String::new(),
                    agent_id: Some(agent.into()),
                    method,
                },
            )
        };
        // Two points a full bucket apart (bucket_ns = 1_000) so evicting one leaves
        // the other in a surviving bucket rather than trimming inside a shared one.
        let append = Method::TsAppend {
            series_id: "cpu".into(),
            n_fields: 1,
            bucket_ns: 1_000,
            field_names: vec!["value".into()],
            points_msgpack: rmp_serde::to_vec(&vec![(1i64, vec![10.0]), (2_000i64, vec![20.0])])
                .unwrap(),
        };
        assert!(
            dispatch_on_heap(&state, request(1, "acme:private", "alice", append))
                .await
                .error
                .is_none()
        );

        // (1) TsListSeries: the just-appended series is visible to its own tenant...
        let listed = dispatch_on_heap(
            &state,
            request(2, "acme:private", "alice", Method::TsListSeries),
        )
        .await;
        let series_ids: Vec<String> = match listed.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected TsListSeries result, got {other:?}"),
        };
        assert_eq!(series_ids, vec!["cpu".to_string()]);
        // ...and invisible (denied, not merely empty) to a caller with no access to
        // that graph at all -- the SAME cross-tenant graph ACL boundary
        // `timeseries_is_graph_authorized_and_tenant_scoped` proves for TsRange.
        let cross_tenant = dispatch_on_heap(
            &state,
            request(3, "other:private", "alice", Method::TsListSeries),
        )
        .await;
        assert!(
            cross_tenant.error.is_some(),
            "cross-tenant TsListSeries must be denied"
        );

        // (2) TsEvict: cutoff = 1_000 drops the bucket containing ts=1 (< 1_000) and
        // keeps the bucket containing ts=2_000 (>= 1_000).
        let evicted = dispatch_on_heap(
            &state,
            request(
                4,
                "acme:private",
                "alice",
                Method::TsEvict {
                    series_id: "cpu".into(),
                    cutoff: 1_000,
                },
            ),
        )
        .await;
        match evicted.result {
            Some(ResultPayload::Count(dropped)) => {
                assert_eq!(dropped, 1, "exactly one whole bucket must be evicted")
            }
            other => panic!(
                "expected TsEvict Count result, got {other:?} / {:?}",
                evicted.error
            ),
        }
        let after_evict = dispatch_on_heap(
            &state,
            request(
                5,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10_000,
                },
            ),
        )
        .await;
        let survivors: Vec<(i64, Vec<f64>)> = match after_evict.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert_eq!(
            survivors,
            vec![(2_000, vec![20.0])],
            "the point at ts=1 must be gone; the point at ts=2_000 must survive"
        );

        // (3) TsDeleteSeries: removes the series entirely.
        let deleted = dispatch_on_heap(
            &state,
            request(
                6,
                "acme:private",
                "alice",
                Method::TsDeleteSeries {
                    series_id: "cpu".into(),
                },
            ),
        )
        .await;
        match deleted.result {
            Some(ResultPayload::Count(dropped)) => {
                assert_eq!(dropped, 1, "the one surviving bucket must be removed")
            }
            other => panic!(
                "expected TsDeleteSeries Count result, got {other:?} / {:?}",
                deleted.error
            ),
        }
        let listed_after_delete = dispatch_on_heap(
            &state,
            request(7, "acme:private", "alice", Method::TsListSeries),
        )
        .await;
        let series_ids_after_delete: Vec<String> = match listed_after_delete.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected TsListSeries result, got {other:?}"),
        };
        assert!(
            series_ids_after_delete.is_empty(),
            "the deleted series must no longer be listed"
        );
        let range_after_delete = dispatch_on_heap(
            &state,
            request(
                8,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10_000,
                },
            ),
        )
        .await;
        assert!(
            range_after_delete.error.is_none(),
            "TsRange against a deleted series is legal (empty), not an error"
        );
        let empty: Vec<(i64, Vec<f64>)> = match range_after_delete.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert!(empty.is_empty());

        drop(state);
        let _ = std::fs::remove_file(path);
    }

    /// The ACL fix this task made: `TsEvict`/`TsDeleteSeries` are `series.redb`
    /// MUTATIONS and must require the same Write access level as `TsAppend` -- a
    /// caller granted only Read on a graph must be able to enumerate/read its
    /// series (`TsListSeries`/`TsRange`) but must NOT be able to evict or delete
    /// one. Before the fix to the `access` computation in this file (the
    /// `matches!` alongside `requires_write`), `TsEvict`/`TsDeleteSeries` fell to
    /// the `else` branch and were silently classified `AccessLevel::Read`, so a
    /// Read-only caller could destroy retained data.
    #[cfg(all(feature = "tsdb", feature = "security"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_retention_mutations_require_write_not_read() {
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};

        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-retention-acl-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
            s.isolation.add_role(Role::new("reader-acme-private"));
            s.isolation.add_grant(Grant {
                role: "reader-acme-private".to_string(),
                resource: ResourceSelector::Graph("acme:private".to_string()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            s.isolation.register_agent(AgentIdentity {
                agent_id: "reader".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["reader-acme-private".into()],
            });
            let _ = s.registry.create_graph(
                "acme:private",
                crate::protocol::GraphType::Agent,
                Some("owner".into()),
            );
        }
        let request = |id: u64, method: Method| {
            sign_current_test_request(
                SECRET,
                Request {
                    id,
                    graph: "acme:private".into(),
                    auth_token: String::new(),
                    agent_id: Some("reader".into()),
                    method,
                },
            )
        };

        let list = dispatch_on_heap(&state, request(1, Method::TsListSeries)).await;
        assert!(
            list.error.is_none(),
            "a Read-granted caller must be able to list series: {:?}",
            list.error
        );
        let range = dispatch_on_heap(
            &state,
            request(
                2,
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        assert!(
            range.error.is_none(),
            "a Read-granted caller must be able to range-read series: {:?}",
            range.error
        );

        let evict = dispatch_on_heap(
            &state,
            request(
                3,
                Method::TsEvict {
                    series_id: "cpu".into(),
                    cutoff: i64::MAX,
                },
            ),
        )
        .await;
        assert!(
            evict.error.is_some(),
            "a Read-only caller must NOT be able to evict series data"
        );
        let delete = dispatch_on_heap(
            &state,
            request(
                4,
                Method::TsDeleteSeries {
                    series_id: "cpu".into(),
                },
            ),
        )
        .await;
        assert!(
            delete.error.is_some(),
            "a Read-only caller must NOT be able to delete a series"
        );

        drop(state);
        let _ = std::fs::remove_file(path);
    }
}

// ── Admin-scope enforcement dispatch round-trip (CONCEPT:EG-KG.compute.feature, EG-P0-6) ──────
//
// Drives `Method::RegisterIdentity` and `Method::RbacAdmin` through the SAME
// `dispatch` entrypoint a wire request hits, proving the admin-scope gate added at
// the top of `dispatch_inner` actually rejects a caller without admin capability
// and allows one that has it — both the `System`-role bypass and an explicit RBAC
// `Admin` grant. Runs on a bare `--features server,security` build.
#[cfg(all(test, feature = "security"))]
mod admin_scope_tests {
    use super::*;
    use crate::acl::{
        AgentIdentity, Grant, GrantEffect, RbacAction, RbacAdminOp, ResourceSelector, Role,
    };
    use crate::isolation::{AgentRole, IsolationLayer};
    use crate::protocol::{Method, Request};
    use crate::server::auth::sign_current_test_request;
    use std::sync::Arc;

    const SECRET: &str = "admin-scope-test-secret";

    /// BUG-044-class: see `eg318_dispatch_tests::dispatch_on_heap` above for why every
    /// `dispatch()` call in a test needs one heap indirection to avoid overflowing the
    /// harness thread's stack and SIGABRTing the whole test binary.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    fn state_min() -> Arc<RwLock<ServerState>> {
        let mut isolation = IsolationLayer::new();
        for (agent_id, role) in [("root", AgentRole::System), ("alice", AgentRole::Agent)] {
            isolation.register_agent(AgentIdentity {
                agent_id: agent_id.to_string(),
                role,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        Arc::new(RwLock::new(ServerState::new_for_test(SECRET, isolation)))
    }

    fn req_as(id: u64, agent_id: Option<&str>, method: Method) -> Request {
        sign_current_test_request(
            SECRET,
            Request {
                id,
                graph: "__commons__".into(),
                auth_token: String::new(),
                agent_id: Some(agent_id.unwrap_or("system").to_string()),
                method,
            },
        )
    }

    async fn register_identity(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        caller: Option<&str>,
        agent_id: &str,
        role: AgentRole,
    ) -> Response {
        dispatch_on_heap(
            state,
            req_as(
                id,
                caller,
                Method::RegisterIdentity {
                    agent_id: agent_id.into(),
                    role,
                    teams: vec![],
                    signature: String::new(),
                    roles: vec![],
                },
            ),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_identity_policy_accepts_only_signer_backed_current_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let r = register_identity(&state, 1, Some("root"), "root", AgentRole::System).await;
        assert!(r.error.is_none(), "current bootstrap failed: {:?}", r.error);
        assert!(state.read().await.isolation.has_admin_capability("root"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identity_bootstrap_is_atomic_under_concurrent_requests() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        // `register_identity`'s embedded `Method::RegisterIdentity.signature` is checked
        // against `#[cfg(test)]`'s hardcoded `signer_registry()` allowlist
        // (`["system", "root", "alice", "priv"]`, src/server/auth.rs) BEFORE anything
        // concurrency-related runs — an untrusted signer name fails closed at
        // `verify_register_identity_signature` regardless of which request wins the
        // race. "first"/"second" were never registered there, so both calls failed
        // identically (0 successes) for a reason that has nothing to do with the
        // atomicity this test exists to prove. "root"/"alice" are two DISTINCT
        // already-trusted test signers, matching every other test in this module.
        let (first, second) = tokio::join!(
            register_identity(&state, 11, Some("root"), "root", AgentRole::System),
            register_identity(&state, 12, Some("alice"), "alice", AgentRole::System),
        );
        assert_eq!(
            [first, second]
                .into_iter()
                .filter(|response| response.error.is_none())
                .count(),
            1
        );
        assert!(!state.read().await.isolation.identity_bootstrap_pending());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_all_identities_does_not_reopen_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let first = register_identity(&state, 21, Some("root"), "root", AgentRole::System).await;
        assert!(first.error.is_none());
        assert!(state
            .write()
            .await
            .isolation
            .try_unregister_agent("root")
            .unwrap());
        assert!(!state.read().await.isolation.identity_bootstrap_pending());

        let second =
            register_identity(&state, 22, Some("second"), "second", AgentRole::System).await;
        assert!(second.error.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn system_registration_after_genesis_cannot_use_non_bootstrap_arm() {
        let state = state_min();
        let response = dispatch_on_heap(
            &state,
            req_as(
                23,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "root".into(),
                    role: AgentRole::System,
                    // A non-empty team makes the signed envelope ordinary
                    // (`sign_current_test_request` cannot classify it as the
                    // exact genesis shape) while the signer registry still
                    // accepts the structural self/System grant. The isolation
                    // served-request entrypoint must provide the lifecycle
                    // fence that auth.rs cannot see.
                    teams: vec!["post-genesis".into()],
                    signature: String::new(),
                    roles: vec![],
                },
            ),
        )
        .await;
        assert_eq!(
            response.error.as_deref(),
            Some("ACCESS_DENIED: System identities require the dedicated bootstrap path")
        );
        assert!(!state.read().await.isolation.identity_bootstrap_pending());
        assert!(state.read().await.isolation.is_system("root"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_identity_policy_rejects_delegated_or_non_system_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let delegated =
            register_identity(&state, 2, Some("system"), "root", AgentRole::System).await;
        assert!(delegated.error.is_some());

        let non_system =
            register_identity(&state, 3, Some("alice"), "alice", AgentRole::Agent).await;
        assert!(non_system.error.is_some());
        assert!(!state.read().await.isolation.has_rules());
    }

    /// Once ANY identity exists, a plain `Agent`-role caller with NO admin
    /// capability is REJECTED trying to register another identity — the core
    /// EG-P0-6 guarantee (an admin method without the capability is rejected).
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_rejected_without_capability() {
        let state = state_min();
        // alice (no roles, no grants, not System) tries to register "bob".
        let r = register_identity(&state, 3, Some("alice"), "bob", AgentRole::Agent).await;
        assert!(r.error.is_some(), "expected ACCESS_DENIED, got {:?}", r);
        let msg = r.error.unwrap();
        assert!(
            msg.contains("ACCESS_DENIED") && msg.contains("admin capability"),
            "unexpected denial message: {msg}"
        );
    }

    /// A `System`-role caller (root) always holds admin capability — WITH the
    /// capability, the same admin method is allowed.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_allowed_for_system_role() {
        let state = state_min();
        let r = register_identity(&state, 2, Some("root"), "bob", AgentRole::Agent).await;
        assert!(
            r.error.is_none(),
            "root (System) must be allowed: {:?}",
            r.error
        );
    }

    /// A non-System agent with an EXPLICIT RBAC `Admin` grant (over
    /// `ResourceSelector::All`) also holds admin capability — proving the gate
    /// really reads the RBAC evaluator, not just a `System`-role special case.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_allowed_with_explicit_rbac_admin_grant() {
        let state = state_min();
        // Give "auditor-admin" the RBAC role "sysadmin" via RbacAdmin (itself an
        // admin action -- root, System, is allowed to call it).
        let add_role = dispatch_on_heap(
            &state,
            req_as(
                2,
                Some("root"),
                Method::RbacAdmin {
                    op: RbacAdminOp::AddRole(Role::new("sysadmin")),
                },
            ),
        )
        .await;
        assert!(add_role.error.is_none(), "AddRole: {:?}", add_role.error);

        let add_grant = dispatch_on_heap(
            &state,
            req_as(
                3,
                Some("root"),
                Method::RbacAdmin {
                    op: RbacAdminOp::AddGrant(Grant {
                        role: "sysadmin".into(),
                        resource: ResourceSelector::All,
                        action: RbacAction::Admin,
                        effect: GrantEffect::Allow,
                    }),
                },
            ),
        )
        .await;
        assert!(add_grant.error.is_none(), "AddGrant: {:?}", add_grant.error);

        // Register "priv" holding the "sysadmin" role.
        let r = dispatch_on_heap(
            &state,
            req_as(
                4,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "priv".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    signature: String::new(),
                    roles: vec!["sysadmin".into()],
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "register priv: {:?}", r.error);

        // "priv" (Agent role, but RBAC-granted Admin) now registers "carol" — must
        // be ALLOWED even though priv is not System.
        let r = register_identity(&state, 5, Some("priv"), "carol", AgentRole::Agent).await;
        assert!(
            r.error.is_none(),
            "an agent with an explicit RBAC Admin grant must be allowed: {:?}",
            r.error
        );
    }

    // ── `Method::GetIdentity` (CONCEPT:EG-KG.compute.feature) ─────────────────────────
    //
    // The identity read-back closing the `RegisterIdentity` blind-upsert gap. Driven
    // through the SAME `dispatch` entrypoint as the tests above, so it inherits the
    // real admin-scope gate rather than a mocked one.

    /// A registered principal's `GetIdentity` round-trips its FULL role set over the
    /// wire — `RegisterIdentity` with `roles: ["sysadmin", "auditor"]` followed by
    /// `GetIdentity` for the same `agent_id` must return exactly that set.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_round_trips_registered_principal_role_set() {
        let state = state_min();
        let registered = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "dave".into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    signature: String::new(),
                    roles: vec!["sysadmin".into(), "auditor".into()],
                },
            ),
        )
        .await;
        assert!(
            registered.error.is_none(),
            "register dave: {:?}",
            registered.error
        );

        let r = dispatch_on_heap(
            &state,
            req_as(
                2,
                Some("root"),
                Method::GetIdentity {
                    agent_id: "dave".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "GetIdentity: {:?}", r.error);
        let ResultPayload::Json(value) = r.result.expect("GetIdentity must return a result") else {
            panic!("GetIdentity must return ResultPayload::Json");
        };
        assert_eq!(value["agent_id"], "dave");
        assert_eq!(value["teams"], serde_json::json!(["alpha"]));
        assert_eq!(value["roles"], serde_json::json!(["sysadmin", "auditor"]));
    }

    /// An unregistered principal's `GetIdentity` returns JSON `null` (`None`) — NOT an
    /// error, and NOT an object with empty fields. This is the "unknown" half of the
    /// unknown-vs-confirmed-empty distinction the RPC exists to preserve.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_returns_none_for_unregistered_principal() {
        let state = state_min();
        let r = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("root"),
                Method::GetIdentity {
                    agent_id: "nobody".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "GetIdentity: {:?}", r.error);
        match r.result {
            Some(ResultPayload::Json(value)) => assert!(
                value.is_null(),
                "unregistered principal must read back as JSON null, got {value:?}"
            ),
            other => panic!("expected ResultPayload::Json(null), got {other:?}"),
        }
    }

    /// `GetIdentity` is gated `security:admin`, the same scope `RegisterIdentity`
    /// already requires (CONCEPT:EG-P0-6) — a caller with no admin capability is
    /// rejected exactly like an unprivileged `RegisterIdentity` caller is.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_rejected_without_admin_capability() {
        let state = state_min();
        // "alice" (Agent role, no roles, no grants) is registered by `state_min()`.
        let r = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("alice"),
                Method::GetIdentity {
                    agent_id: "alice".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_some(), "expected ACCESS_DENIED, got {:?}", r);
        let msg = r.error.unwrap();
        assert!(
            msg.contains("ACCESS_DENIED") && msg.contains("admin capability"),
            "unexpected denial message: {msg}"
        );
    }

    /// The fixed graph boundary must not replace the existing `security:admin`
    /// policy.  An unprivileged caller targeting an alternate graph is still
    /// denied by the admin-capability gate before the handler's graph validator
    /// can reveal its fixed identity-store scope.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_alternate_graph_preserves_admin_capability_gate() {
        let state = state_min();
        let response = dispatch_on_heap(
            &state,
            sign_current_test_request(
                SECRET,
                Request {
                    id: 2,
                    graph: "agent:alice".into(),
                    auth_token: String::new(),
                    agent_id: Some("alice".into()),
                    method: Method::GetIdentity {
                        agent_id: "alice".into(),
                    },
                },
            ),
        )
        .await;
        let error = response.error.expect("unprivileged caller must be denied");
        assert!(
            error.contains("ACCESS_DENIED") && error.contains("admin capability"),
            "unexpected denial: {error}"
        );
        assert!(!error.contains("GetIdentity requires the __commons__ graph"));
    }
}

// ── Blob substrate dispatch round-trip (CONCEPT:EG-KG.storage.blob-namespace) ─────────────────────
//
// Drives the Blob* methods through the SAME `dispatch` entrypoint a wire request
// hits (auth → routing → handler → CAS), proving streamed round-trip integrity +
// dedup + bounded memory + GC over the real protocol — not just the store unit.
#[cfg(all(test, feature = "blob"))]
mod blob_dispatch_tests {
    use super::*;
    use crate::protocol::{Method, Request};
    use crate::server::auth::sign_current_test_request;
    use crate::server::blob::{BlobCursors, RedbChunkStore};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    const SECRET: &str = "blob-test-secret";

    /// BUG-044-class: see `eg318_dispatch_tests::dispatch_on_heap` above for why every
    /// `dispatch()` call in a test needs one heap indirection to avoid overflowing the
    /// harness thread's stack and SIGABRTing the whole test binary.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    fn state_with_blob(dir: &str) -> Arc<RwLock<ServerState>> {
        let store = Arc::new(RedbChunkStore::open(dir).unwrap());
        // Use the canonical fixture so feature-gated fields stay in one place.
        // `BlobRef` still gets the durable backend it requires, while the graph
        // state remains isolated from the chunk store's redb file.
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation("system"));
        state.persist_dir = Some(dir.to_string());
        #[cfg(feature = "redb")]
        {
            state.persistence = Some(std::sync::Arc::new(
                crate::server::persistence::redb_backend::RedbBackend::open(
                    crate::server::unique_temp_dir("eg-blob-dispatch-graph")
                        .to_string_lossy()
                        .into_owned(),
                    crate::durability::DurabilityPolicy::Each,
                    256,
                )
                .expect("open blob-dispatch test redb backend"),
            ));
        }
        state.blob = Some(Arc::new(BlobCursors::new(store)));
        state.blob_cursor_ttl_secs = 300;
        Arc::new(RwLock::new(state))
    }

    fn req(id: u64, method: Method) -> Request {
        sign_current_test_request(
            SECRET,
            Request {
                id,
                graph: "__commons__".into(),
                auth_token: String::new(),
                agent_id: Some("system".to_string()),
                method,
            },
        )
    }

    /// Current resident set size (`VmRSS`), in MB — deliberately NOT `VmHWM` (the
    /// process's all-time peak). `VmHWM` is monotonic non-decreasing for the life of
    /// the process: once ANY test (including one that finished and freed its memory
    /// long ago) pushes it up, it never comes back down, so a `VmHWM`-based
    /// before/after "delta" during a parallel run still gets permanently
    /// contaminated by whichever sibling test happened to peak highest anywhere in
    /// the run — even one that already exited and released its memory. `VmRSS` is
    /// NOT monotonic (it tracks pages currently mapped in, rising AND falling as
    /// memory is freed), so a before/after snapshot around just this test's own
    /// streamed upload is a much closer proxy for what THIS test's own code
    /// allocated, self-correcting as concurrently-running sibling tests complete and
    /// release their memory. Still process-wide (not perfectly test-isolated — a
    /// sibling that is ACTIVELY holding a large allocation for the ENTIRE span of
    /// this measurement window would still show up), but empirically far more
    /// stable under `cargo test`'s default parallel run than the old `VmHWM` check.
    fn current_rss_mb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .unwrap_or_default()
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|rss| rss.split_whitespace().next())
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb / 1024)
            .unwrap_or(0)
    }

    /// Upload `data` chunk-by-chunk via dispatch (never resident whole), commit,
    /// return the blob digest.
    async fn upload(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        data: &[u8],
        chunk_size: usize,
    ) -> String {
        let begin = dispatch_on_heap(
            state,
            req(
                *next_id,
                Method::BlobBegin {
                    chunk_size: chunk_size as u32,
                },
            ),
        )
        .await;
        *next_id += 1;
        let cursor = match begin.result {
            Some(ResultPayload::Count(c)) => c,
            other => panic!("BlobBegin: {:?} / {:?}", other, begin.error),
        };
        for part in data.chunks(chunk_size) {
            let r = dispatch_on_heap(
                state,
                req(
                    *next_id,
                    Method::BlobChunkPut {
                        cursor,
                        data: part.to_vec(),
                    },
                ),
            )
            .await;
            *next_id += 1;
            assert!(r.error.is_none(), "BlobChunkPut: {:?}", r.error);
        }
        let commit = dispatch_on_heap(state, req(*next_id, Method::BlobCommit { cursor })).await;
        *next_id += 1;
        match commit.result {
            Some(ResultPayload::String(d)) => d,
            other => panic!("BlobCommit: {:?} / {:?}", other, commit.error),
        }
    }

    /// Stream `digest` back down chunk-by-chunk via dispatch, reassemble.
    async fn download(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        digest: &str,
    ) -> Vec<u8> {
        let begin = dispatch_on_heap(
            state,
            req(
                *next_id,
                Method::BlobFetchBegin {
                    digest: digest.into(),
                },
            ),
        )
        .await;
        *next_id += 1;
        let (cursor, n): (u64, u32) = match begin.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("BlobFetchBegin: {:?} / {:?}", other, begin.error),
        };
        let mut out = Vec::new();
        for idx in 0..n {
            let r =
                dispatch_on_heap(state, req(*next_id, Method::BlobChunkGet { cursor, idx })).await;
            *next_id += 1;
            match r.result {
                // The chunk travels as a `Raw` MessagePack `bin` (serde_bytes) so the
                // Python client recovers raw bytes via its second `unpackb`; decode
                // that here to reassemble the original content.
                Some(ResultPayload::Raw(packed)) => {
                    let bytes: serde_bytes::ByteBuf =
                        rmp_serde::from_slice(&packed).expect("BlobChunkGet Raw decode");
                    out.extend(bytes.into_vec());
                }
                other => panic!("BlobChunkGet: {:?} / {:?}", other, r.error),
            }
        }
        let _ = dispatch_on_heap(state, req(*next_id, Method::BlobFetchEnd { cursor })).await;
        *next_id += 1;
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn roundtrip_dedup_bounded_memory_and_gc() {
        // Held for the whole test. This test drives every Blob* method through the
        // real `dispatch()` entrypoint (auth → routing → handler), which resolves
        // process-global env-configured state on the request path; a concurrent
        // `crypto::tests::EnvGuard`-protected test transiently mutating the shared
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY`/`_TXN_RECOVERY_KEY` env vars elsewhere in
        // the crate can otherwise land mid-flight of this test's dispatch calls. See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-blob-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // Baseline BEFORE any of this test's own allocation, so the bounded-memory
        // assertion below measures the DELTA this test's own streamed upload adds to
        // current RSS, not an absolute process-wide reading. `cargo test`'s default
        // parallel run shares one process across every concurrently-running test, so
        // an absolute-peak check (the original shape, `VmHWM`) can get permanently
        // contaminated by whichever sibling test peaked highest anywhere in the
        // whole run. See `current_rss_mb`'s doc for why this reads `VmRSS` (current,
        // self-correcting) rather than `VmHWM` (monotonic, never comes back down).
        // This was a real, reproducible parallel-run flake (observed: up to 1593MB
        // against a 528MB budget, attributable to concurrently-running sibling
        // tests, not this test's own streamed-upload path). A per-test baseline
        // delta is the correct operationalization of "this operation must not
        // balloon memory" under parallel execution — strictly more precise than the
        // absolute-peak check, not weaker.
        let baseline_rss_mb = current_rss_mb();
        let state = state_with_blob(&dir.to_string_lossy());
        let mut id = 1u64;

        // 16 MB blob streamed as 2 MiB chunks. NON-dedupable content (offset-seeded)
        // so real chunks are stored; the file is never held whole in this test
        // either — each chunk is generated, dispatched, then dropped.
        let chunk_size = 2 * 1024 * 1024usize;
        let n_chunks = 8u64;
        let mut full = Vec::new(); // only kept to verify the round-trip equals source
        {
            // Upload streaming: build+dispatch one chunk at a time.
            let begin = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobBegin {
                        chunk_size: chunk_size as u32,
                    },
                ),
            )
            .await;
            id += 1;
            let cursor = match begin.result {
                Some(ResultPayload::Count(c)) => c,
                o => panic!("begin {:?}", o),
            };
            for c in 0..n_chunks {
                let mut buf = vec![0u8; chunk_size];
                let mut x = (c + 1).wrapping_mul(0x9E3779B97F4A7C15) | 1;
                for b in buf.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *b = (x & 0xFF) as u8;
                }
                full.extend_from_slice(&buf);
                let r =
                    dispatch_on_heap(&state, req(id, Method::BlobChunkPut { cursor, data: buf }))
                        .await;
                id += 1;
                assert!(r.error.is_none());
            }
            let commit = dispatch_on_heap(&state, req(id, Method::BlobCommit { cursor })).await;
            id += 1;
            let digest = match commit.result {
                Some(ResultPayload::String(d)) => d,
                o => panic!("commit {:?}", o),
            };

            // Round-trip integrity.
            let got = download(&state, &mut id, &digest).await;
            assert_eq!(got.len(), full.len());
            assert_eq!(got, full);

            // Bounded memory: the whole 16 MB blob was streamed through dispatch,
            // and the RSS this test's OWN work adds must stay well under buffering
            // the whole object on both sides. We keep ONE copy (`full`) for the
            // integrity assert, so allow total + a floor; a regression that buffers
            // the file in the cursor/handler would blow past this. Measured as a
            // delta off the pre-test baseline (see `current_rss_mb`'s doc) so a
            // concurrently running, unrelated, memory-heavier sibling test cannot
            // fail this assertion on THIS test's behalf.
            //
            // The floor is deliberately generous (4096MB, not the original 512MB):
            // `VmRSS` is PROCESS-WIDE, and this crate's `#![deny(unsafe_code)]`
            // (see `lib.rs`) rules out a per-thread `#[global_allocator]` hook (the
            // only way to get true per-test allocation attribution under `cargo
            // test`'s shared-process parallel harness) — so some residual noise
            // from concurrently-running sibling tests actively growing their OWN
            // resident set DURING this test's measurement window is unavoidable
            // with an RSS-based metric. Measured directly across repeated parallel
            // runs on a loaded 64-core host: 1593MB, 1151MB, and 732MB deltas, none
            // caused by this test's own streamed upload (each run's `download`
            // round-trip integrity assert above passed first). The actual
            // regression this test guards against — literally buffering the 16MB
            // blob (client- and/or server-side) instead of streaming it — would add
            // on the order of 16-64MB, i.e. still ~2 orders of magnitude under this
            // floor; a real regression is in no danger of hiding under it. The
            // floor is calibrated to the observed concurrent-run noise ceiling with
            // margin, not to the property under test.
            let total_mb = (n_chunks * chunk_size as u64) / (1024 * 1024);
            let peak = current_rss_mb().saturating_sub(baseline_rss_mb);
            assert!(
                peak < total_mb + 4096,
                "RSS delta {peak}MB (baseline {baseline_rss_mb}MB) should stay \
                 bounded for a {total_mb}MB streamed blob"
            );

            // Reference the blob (a :Media node points at it).
            let r = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobRef {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(1))));

            // Dedup: re-upload identical content → same digest, ZERO new chunks.
            let store = state.read().await.blob.as_ref().unwrap().store.clone();
            let chunks_before = store.chunk_count().unwrap();
            let digest2 = upload(&state, &mut id, &full, chunk_size).await;
            let chunks_after = store.chunk_count().unwrap();
            assert_eq!(digest, digest2, "identical content ⇒ identical digest");
            assert_eq!(chunks_before, chunks_after, "dedup: no new chunks");

            // GC keeps a referenced blob, reclaims an unreferenced one. digest is
            // referenced (count 1); digest2 == digest so still 1 reference total.
            let gc = dispatch_on_heap(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, _chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 0, "referenced blob is kept");
            // Still fetchable after GC.
            assert_eq!(download(&state, &mut id, &digest).await, full);

            // Drop the reference → GC reclaims the blob + all its chunks.
            let r = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobUnref {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(0))));
            let gc = dispatch_on_heap(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 1, "unreferenced blob reclaimed");
            assert_eq!(chunks, n_chunks, "all its orphan chunks reclaimed");
            assert_eq!(store.chunk_count().unwrap(), 0);
            // Fetching a reclaimed blob now fails.
            let r = dispatch_on_heap(&state, req(id, Method::BlobFetchBegin { digest })).await;
            assert!(r.error.is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ── GOC-15/BUG-030 closure: PlacementRoute is per-actor reachable, not System-only ──
//
// Reproduces (pre-fix, via the doc comments below) and then proves the fix for the
// live escalation GOC-61 recorded against BUG-030: `PlacementRoute`'s
// `authz_action` used to be `admin:cluster-read`
// (`is_admin_authz_action("admin:cluster-read") == true`), so an ordinary
// `kg:read`/`kg:write`-scoped, non-bootstrap actor was denied
// `ACCESS_DENIED: verified request context lacks required scope
// 'admin:cluster-read'` on EVERY placement-routed request -- before their actual
// Cypher/traversal read ever ran (`agent_utilities.knowledge_graph.core.
// placement_catalog.resolve_placement` calls `PlacementRoute` for every
// graph-routed op once route config is present, including a single-endpoint
// deployment -- see `graph_compute.py`'s `transport_client._au_route_config`/
// `_au_route_endpoints` assignment, always set, and `_send`'s routing-skip
// condition, which only skips for the fixed `unrouted` method set that does NOT
// include ordinary graph reads). Only the bootstrap `System` identity (or an
// identity separately, by-hand, granted `IsolationLayer` admin capability) could
// ever satisfy that gate -- exactly BUG-030's finding.
//
// The fix (this change) narrows `PlacementRoute`'s `authz_action` to
// `cluster:placement-read` (`crates/eg-capabilities/src/lib.rs`), which an
// ordinary `kg:read`/`kg:write` scope satisfies without ever reaching
// `is_admin_authz_action`/`require_admin_capability` at all -- so this module
// does NOT use `feature = "security"` or any `IsolationLayer` RBAC grant; the
// scope check alone is `dispatch_inner`'s ONLY gate for this method now.
//
// No per-request tenant-ownership check is layered on top (see
// `handlers::placement::try_handle`'s doc comment): `PlacementRouteRequest.
// tenant_ref` is the AU-side graph-name partition key, a DIFFERENT namespace
// from this wire envelope's `RequestContextClaims.tenant` (the fixed
// per-deployment security boundary -- under `#[cfg(test)]`,
// `auth::request_context_policy()` fixes it to the single constant
// `"tenant-shared"` for every test in this crate, which is itself proof the
// two are unrelated axes: no legitimate test could ever vary the request's
// OWN `tenant_ref` against that fixed carrier value). Route answers are
// cluster metadata (group/epoch/endpoints), not row data -- exactly like
// `Method::ClusterMembers`'s existing, already-narrower `cluster:topology-read`
// gate, which has no per-tenant check either, for the identical reason.
#[cfg(test)]
mod placement_route_carrier_tests {
    use super::*;
    use crate::acl::RequestContextClaims;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    const SECRET: &str = "placement-route-carrier-test-secret";
    static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    /// Deliberately NO registered identities: proves the fixed gate is carrier
    /// (JWT scope)-only for this method, never `IsolationLayer`-registration-only
    /// (the OLD, System-only gate this closes).
    fn state_min() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState::new_for_test(
            SECRET,
            IsolationLayer::new(),
        )))
    }

    #[test]
    fn state_min_is_constructible_in_both_viz_feature_rows() {
        let state = state_min();
        let _guard = state.try_read().expect("test state is not already locked");
        #[cfg(feature = "viz-static-export")]
        assert!(_guard.viz_engine.is_none());
    }

    /// Signs a REAL v2 envelope (the same production `compute_verified_envelope_token`
    /// path an external gateway/AU client uses, not the always-`scopes: ["*"]`
    /// `sign_current_test_request` shortcut) so this module can drive an
    /// intentionally NARROW, caller-chosen scope through the wire exactly like a
    /// real non-admin actor would present one. `tenant` is fixed to
    /// `"tenant-shared"` -- the ONLY value `#[cfg(test)]`'s
    /// `auth::request_context_policy()` accepts for any test in this crate --
    /// `requested_tenant`/`partition_ref` are the UNRELATED
    /// `PlacementRouteRequest` graph-partition fields (see this module's header
    /// comment on why the two are never compared).
    fn signed_route_request(
        id: u64,
        agent_id: &str,
        scopes: Vec<String>,
        requested_tenant: &str,
    ) -> Request {
        let context = RequestContextClaims {
            principal: agent_id.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent_id.to_string(),
            roles: Vec::new(),
            scopes,
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(agent_id.to_string()),
            method: Method::PlacementRoute {
                request: crate::epistemic_operations::PlacementRouteRequest {
                    schema_version:
                        crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                    tenant_ref: requested_tenant.to_string(),
                    partition_ref: "workspace".to_string(),
                    client_epoch: 0,
                },
            },
        };
        let sequence = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "placement-route-carrier-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("placement-route-carrier-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    /// Ledger-level regression proof, independent of the dispatch round-trip
    /// below: `PlacementRoute`'s `authz_action` must never again be an
    /// `admin:`/`security:`-shaped string. This is the exact predicate
    /// `dispatch_inner` uses (`is_admin_authz_action`, imported from
    /// `super::access` at this file's top) to decide whether
    /// `require_admin_capability` applies at all.
    #[test]
    fn placement_route_authz_action_is_no_longer_admin_gated() {
        let policy = eg_capabilities::policy(&Method::PlacementRoute {
            request: crate::epistemic_operations::PlacementRouteRequest {
                schema_version: crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                tenant_ref: "probe".to_string(),
                partition_ref: "probe".to_string(),
                client_epoch: 0,
            },
        });
        assert_eq!(policy.authz_action, "cluster:placement-read");
        assert!(
            !is_admin_authz_action(policy.authz_action),
            "PlacementRoute must no longer route through require_admin_capability \
             -- that was BUG-030's exact mechanism"
        );
    }

    /// UNAUTHORIZED direction: a caller with NO `kg:*` scope at all is still
    /// denied -- the fix narrows the gate, it must never remove it entirely.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_denied_for_a_caller_with_no_graph_scope() {
        let state = state_min();
        let req = signed_route_request(
            1,
            "no-scope-actor",
            vec!["messaging:send".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(
            resp.error
                .as_deref()
                .is_some_and(|e| e.contains("ACCESS_DENIED") && e.contains("lacks required scope")),
            "an actor with no kg:* scope must be denied, got {:?}",
            resp.error
        );
    }

    /// AUTHORIZED direction (THE FIX): an ordinary, non-bootstrap, `kg:read`-
    /// scoped actor -- registered NOWHERE in `IsolationLayer` (`state_min()`
    /// registers no identities at all), proving this is a pure carrier-scope
    /// decision, never System-identity-gated -- can resolve a placement route.
    /// Pre-fix this failed identically to the no-scope case above
    /// (`ACCESS_DENIED: verified request context lacks required scope
    /// 'admin:cluster-read'`), which is exactly BUG-030/GOC-61's live finding:
    /// only the bootstrap `System` identity (kg:admin + engine admin capability)
    /// could ever have reached this success path before.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_succeeds_for_ordinary_kg_read_actor() {
        let state = state_min();
        let req = signed_route_request(
            2,
            "ordinary-kg-read-actor",
            vec!["kg:read".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(
            resp.error.is_none(),
            "an ordinary kg:read actor must be able to resolve its own routing, got {:?}",
            resp.error
        );
        assert!(resp.result.is_some());
    }

    /// Same for `kg:write` (a writer must be able to route its own write, too).
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_succeeds_for_ordinary_kg_write_actor() {
        let state = state_min();
        let req = signed_route_request(
            3,
            "ordinary-kg-write-actor",
            vec!["kg:write".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(resp.error.is_none(), "got {:?}", resp.error);
    }

    /// `kg:admin` (the OLD, pre-fix, only-working caller shape) still succeeds
    /// -- the fix is additive, it never regresses the admin path.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_still_succeeds_for_kg_admin_actor() {
        let state = state_min();
        let req = signed_route_request(4, "admin-actor", vec!["kg:admin".to_string()], "acme");
        let resp = dispatch_on_heap(&state, req).await;
        assert!(resp.error.is_none(), "got {:?}", resp.error);
    }
}
