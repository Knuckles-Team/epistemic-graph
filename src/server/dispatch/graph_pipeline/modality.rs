use super::*;
/// The route, graph and verified identity a replicated modality mutation is
/// bound to. Bundled so each phase helper below stays inside the parameter cap.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) struct ModalityReplication<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    handle: &'a crate::raft::RaftHandle,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    req_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    tenant_scope: &'a str,
    principal_fingerprint: &'a str,
    core: &'a Arc<crate::graph::GraphCore>,
    persistence: &'a Arc<dyn crate::server::persistence::PersistenceBackend>,
}

/// The sanitized, source-free facts derived from the public method. The
/// source-bearing `ServedModalityOp` is deliberately NOT carried here — it is
/// handed to the modality handler once and never reaches `RaftRequest`.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) struct ModalityMutationInputs {
    operation: crate::raft::SanitizedModalityMutation,
    modality: eg_types::ServedModalityKind,
    receipt_query: String,
    safe_method: Method,
}

/// Owned and borrowed inputs for the leader-side modality replication path.
/// Grouping the route, identity, and sealed-state handles keeps the public
/// helper at the dispatch parameter limit without changing the wire contract.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) struct ModalityReplicationRequest<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) handle: crate::raft::RaftHandle,
    pub(super) group_id: crate::raft::GroupId,
    pub(super) placement_epoch: u64,
    pub(super) fencing_token: Option<u64>,
    pub(super) graph_name: &'a str,
    pub(super) graph_type: crate::protocol::GraphType,
    pub(super) req_id: u64,
    pub(super) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(super) idempotency_key: &'a str,
    pub(super) tenant_scope: &'a str,
    pub(super) principal_fingerprint: &'a str,
    pub(super) core: &'a Arc<crate::graph::GraphCore>,
    pub(super) persistence: &'a Arc<dyn crate::server::persistence::PersistenceBackend>,
    pub(super) method: Method,
    pub(super) authority: &'a handlers::modality::ModalityAuthority,
}

/// Only the current placement leader may prepare a modality mutation; a
/// follower answers with the ordinary redirect.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) async fn modality_leader_fence(ctx: &ModalityReplication<'_>) -> Option<Response> {
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
pub(super) fn decode_modality_replication_inputs(
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
pub(super) fn modality_receipt_operation_matches(
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
pub(super) fn modality_receipt_binding_matches(
    ctx: &ModalityReplication<'_>,
    record: &crate::mutation_batch::MutationBatchRecord,
    batch_id: &str,
    graph_fname: &str,
) -> bool {
    record.status == crate::mutation_batch::MutationBatchStatus::Committed
        && record.batch.batch_id == batch_id
        && matches!(
            record.committing_tenant(),
            Ok(tenant) if tenant == ctx.tenant_scope
        )
        // A native (non-graph) scope reports no graph name; `map(...) == Some(_)`
        // fails closed. The durable bind replaces the logical graph with this
        // sanitized physical key while preserving the logical name in the
        // protocol scope and opaque request-key inputs.
        && record
            .batch
            .identity
            .scope()
            .graph_name()
            .map(crate::mutation_batch::LogicalName::as_str)
            == Some(graph_fname)
        && record.batch.placement_epoch == ctx.placement_epoch
        && record.batch.fencing_token == ctx.fencing_token
        && matches!(
            record.committing_actor(),
            Ok(actor) if actor == ctx.principal_fingerprint
        )
}

/// Raft apply authenticated the sealed runtime state and result digest before
/// committing the record. The retry record intentionally retains only the safe
/// result bytes, so replay reuses the same canonical typed decoder to reject
/// receipt tampering without ever reconstructing or exposing the sealed/source
/// material.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) fn decode_modality_replay_result(
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
pub(super) async fn install_modality_committed_image(
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

#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn modality_operation_digest(method: &Method) -> Result<String, String> {
    use sha2::{Digest, Sha256};

    let encoded = rmp_serde::to_vec_named(method).map_err(|error| error.to_string())?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(encoded))))
}

/// A client retry with the same authenticated idempotency key repairs RAM from
/// the committed image and returns the exact stored ApplyOutcome, even when the
/// transport request id and nonce are fresh. It never re-decodes source bytes or
/// emits a second Raft entry/outbox/audit/CDC event. `None` ⇒ no receipt yet, so
/// the caller proceeds with a fresh mutation.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) async fn try_replay_modality_receipt(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    batch_id: &str,
    graph_fname: &str,
) -> Option<Response> {
    let record = match ctx
        .persistence
        .read_mutation_batch(graph_fname, batch_id)
        .await
    {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(error) => return Some(Response::err(ctx.req_id, error)),
    };
    let expected_operation = match modality_operation_digest(&inputs.safe_method) {
        Ok(digest) => digest,
        Err(error) => return Some(Response::err(ctx.req_id, error)),
    };
    if !modality_receipt_binding_matches(ctx, &record, batch_id, graph_fname)
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

#[cfg(all(test, feature = "raft", feature = "modality-serving"))]
mod replicated_identity_tests {
    use super::*;

    fn operation(query: &str) -> Method {
        Method::ApplyMutation {
            event_type: "served_modality_v1".to_string(),
            query: query.to_string(),
        }
    }

    #[test]
    fn retry_reuses_identity_and_changed_operation_keeps_the_conflict_row() {
        let graph_name = "tenant-a:docs/a#b";
        let graph_fname = crate::persist::sanitize(graph_name);
        let first = super::super::dispatch_helpers::replicated_graph_batch_id(
            "raft-modality",
            "tenant-scope-a",
            graph_name,
            &graph_fname,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "stable-envelope-key",
        );
        let retry = super::super::dispatch_helpers::replicated_graph_batch_id(
            "raft-modality",
            "tenant-scope-a",
            graph_name,
            &graph_fname,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "stable-envelope-key",
        );
        let changed_operation = super::super::dispatch_helpers::replicated_graph_batch_id(
            "raft-modality",
            "tenant-scope-a",
            graph_name,
            &graph_fname,
            "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "stable-envelope-key",
        );

        assert_eq!(first, retry);
        assert_eq!(first, changed_operation);
        assert_ne!(
            modality_operation_digest(&operation("operation-a")).unwrap(),
            modality_operation_digest(&operation("operation-b")).unwrap()
        );
        // Because operation bytes are excluded from `first`, the second digest
        // probes the same durable row and the receipt/HMAC check rejects it.
    }

    #[test]
    fn modality_identity_binds_tenant_logical_and_physical_graph() {
        let identity = |tenant: &str, logical: &str, physical: &str| {
            super::super::dispatch_helpers::replicated_graph_batch_id(
                "raft-modality",
                tenant,
                logical,
                physical,
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "stable-envelope-key",
            )
        };
        let baseline = identity("tenant-a", "docs/a#b", "docs~2fa~23b");

        assert_ne!(baseline, identity("tenant-b", "docs/a#b", "docs~2fa~23b"));
        assert_ne!(baseline, identity("tenant-a", "docs/a#c", "docs~2fa~23b"));
        assert_ne!(baseline, identity("tenant-a", "docs/a#b", "docs~2fa~23c"));
    }
}

/// Stage the mutation over the authoritative committed image. When no durable
/// snapshot exists yet, fall back to the resident core at the durable version.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) async fn stage_modality_base_core(
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
pub(super) async fn build_modality_raft_command(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    staged: &crate::graph::GraphCore,
    authority: &handlers::modality::ModalityAuthority,
    graph_fname: &str,
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
        (ctx.tenant_scope, ctx.graph_name, graph_fname),
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
pub(super) async fn submit_modality_replication(
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
        ctx.attempt_nonce,
        ctx.tenant_scope,
        ctx.principal_fingerprint.to_string(),
        false,
        ctx.placement_epoch,
        crate::raft::RaftMutationTiming {
            fencing_token: ctx.fencing_token,
            created_at_ms,
        },
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
pub(super) async fn replicate_served_modality(request: ModalityReplicationRequest<'_>) -> Response {
    let method = request.method;
    let ctx = ModalityReplication {
        state: request.state,
        handle: &request.handle,
        group_id: request.group_id,
        placement_epoch: request.placement_epoch,
        fencing_token: request.fencing_token,
        graph_name: request.graph_name,
        graph_type: request.graph_type,
        req_id: request.req_id,
        attempt_nonce: request.attempt_nonce,
        tenant_scope: request.tenant_scope,
        principal_fingerprint: request.principal_fingerprint,
        core: request.core,
        persistence: request.persistence,
    };
    if let Some(stale) = modality_leader_fence(&ctx).await {
        return stale;
    }

    let _mutation_guard = crate::server::mutation_batch::lock_graph(request.graph_name).await;
    let (op, inputs) = match decode_modality_replication_inputs(request.req_id, method) {
        Ok(decoded) => decoded,
        Err(response) => return response,
    };
    let graph_fname = crate::persist::sanitize(request.graph_name);
    let batch_id = super::dispatch_helpers::replicated_graph_batch_id(
        "raft-modality",
        request.tenant_scope,
        request.graph_name,
        &graph_fname,
        request.principal_fingerprint,
        request.idempotency_key,
    );

    if let Some(replayed) =
        try_replay_modality_receipt(&ctx, &inputs, &batch_id, &graph_fname).await
    {
        return replayed;
    }

    prepare_and_replicate_modality(&ctx, &inputs, op, request.authority, batch_id, graph_fname)
        .await
}

/// No receipt exists yet: stage the mutation over the committed image, run the
/// modality handler, seal the result, and propose it.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
pub(super) async fn prepare_and_replicate_modality(
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
    let command =
        match build_modality_raft_command(ctx, inputs, &staged, authority, &graph_fname, &payload)
            .await
        {
            Ok(command) => command,
            Err(response) => return response,
        };
    submit_modality_replication(ctx, batch_id, graph_fname, command, payload).await
}
