use super::*;
async fn route_native_resource_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "redb")]
    let graph_incarnation_id = ctx.graph_incarnation_id;
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
            ctx.state_machine_authorized,
            method,
        )
        .await);
    }
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
    Err(method)
}

async fn route_native_lifecycle_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "redb")]
    let agent_library = ctx.state.read().await.agent_library.clone();
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;

    // Native development-lane hold/quota authority. The domain handler itself
    // enumerates all six writes and both reads, so dispatch owns no parallel
    // classifier and a future Method cannot fall into a wildcard commit.
    let method = match handlers::development_lane::try_handle(
        handlers::development_lane::HandleContext {
            req_id,
            graph_name,
            persistence,
            #[cfg(feature = "raft")]
            multi_raft: ctx.multi_raft,
            #[cfg(feature = "raft")]
            routed_raft,
        },
        method,
    )
    .await
    {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };

    // RF-020 is an authenticated adapter over the same native WorkItem
    // admission transaction. The retained Agent Library owner is resolved
    // before lowering to the existing consensus/local WorkItem authority.
    #[cfg(feature = "redb")]
    let method = match handlers::delegation::try_handle(
        handlers::delegation::HandleContext {
            state: ctx.state,
            req_id,
            graph_name,
            caller,
            verified_context,
            core,
            persistence,
            agent_library: &agent_library,
            #[cfg(feature = "raft")]
            routed_raft,
        },
        method,
    )
    .await
    {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    #[cfg(not(feature = "redb"))]
    if matches!(&method, Method::KgDelegate { .. }) {
        return Ok(Response::err(
            req_id,
            "kg-delegate requires the redb Agent Library owner",
        ));
    }

    // The WorkItem handler owns its six lifecycle Methods explicitly and keeps
    // their authoritative MutationBatch effect. Submission and reservation
    // operations deliberately fall through to their existing native route.
    let method = match handlers::work_item::try_handle(
        handlers::work_item::HandleContext {
            req_id,
            graph_name,
            caller,
            verified_context,
            core,
            persistence,
            #[cfg(feature = "raft")]
            routed_raft,
        },
        method,
    )
    .await
    {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    Err(method)
}

pub(super) async fn route_native_store_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let method = match route_source_ingestion_or_resources(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match route_native_lifecycle_ops(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };

    if matches!(
        &method,
        Method::SubmitWorkItem { .. } | Method::SubmitWorkItems { .. }
    ) || crate::server::mutation_batch::is_resource_reservation_method(&method)
    {
        return Ok(dispatch_op_workitem_submission_or_resources(
            handlers::work_item::HandleContext {
                req_id,
                graph_name,
                caller,
                verified_context,
                core,
                persistence,
                #[cfg(feature = "raft")]
                routed_raft,
            },
            method,
        )
        .await);
    }
    Err(method)
}

/// The typed native read/lease surfaces (EH-219, graph-os EG-2/EG-3), then the
/// native resource/capacity routes. Sits in front of `route_native_resource_ops`
/// so the existing router chain gains no branch.
async fn route_native_typed_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[cfg(feature = "redb")]
    let method = match route_work_item_reads(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    #[cfg(feature = "redb")]
    let method = match refuse_foreign_control_lease_writes(ctx, method) {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_native_resource_ops(ctx, method).await
}

/// EH-219 typed WorkItem reads. A native durable read, so under raft it runs
/// on the current placement leader behind a read barrier -- the same gate the
/// native reservation reads use -- before the handler applies the verified
/// carrier-tenant rule and reads the redb snapshot.
#[cfg(feature = "redb")]
async fn route_work_item_reads(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let read = match method {
        Method::GetWorkItem {
            tenant,
            work_item_id,
        } => handlers::work_item_read::WorkItemRead::Get {
            tenant,
            work_item_id,
        },
        Method::ListWorkItems {
            tenant,
            cursor,
            limit,
            kind,
        } => handlers::work_item_read::WorkItemRead::List(
            eg_types::work_item_read::WorkItemListRequest {
                tenant,
                cursor,
                limit,
                kind,
            },
        ),
        Method::GetWorkItemOutcome {
            tenant,
            work_item_id,
        } => handlers::work_item_read::WorkItemRead::Outcome {
            tenant,
            work_item_id,
        },
        Method::GetControlLease { tenant, lease_id } => {
            handlers::work_item_read::WorkItemRead::ControlLease { tenant, lease_id }
        }
        other => return Err(other),
    };
    #[cfg(feature = "raft")]
    if let Err(response) = enforce_native_read_leadership(
        ctx.req_id,
        ctx.graph_name,
        ctx.multi_raft.as_ref(),
        ctx.routed_raft.as_ref(),
        "WorkItem reads require the current placement leader",
        "WorkItem read linearizability barrier failed",
    )
    .await
    {
        return Ok(response);
    }
    Ok(handlers::work_item_read::answer(
        ctx.req_id,
        ctx.graph_name,
        ctx.verified_context.tenant(),
        ctx.persistence,
        read,
    )
    .await)
}

/// graph-os EG-2: a control-lease write names its tenant in the body, and that
/// tenant must be the VERIFIED carrier tenant -- no aggregate or `kg:admin`
/// exception. A matching write falls through (`Err`) to the WorkItem handler,
/// which commits it through the shared durable WorkItem kernel.
#[cfg(feature = "redb")]
fn refuse_foreign_control_lease_writes(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let named = match &method {
        Method::IssueControlLease { request } => Some(request.tenant.as_str()),
        Method::TransitionControlLease { request } => Some(request.tenant.as_str()),
        _ => None,
    };
    let denied = named.and_then(|tenant| {
        handlers::work_item_read::require_carrier_tenant(tenant, ctx.verified_context.tenant())
            .err()
    });
    match denied {
        Some(denied) => {
            crate::metrics::access_denied();
            Ok(Response::err(ctx.req_id, denied))
        }
        None => Err(method),
    }
}

/// RF-ADR-009 source ingestion is graph-scoped and therefore reaches this
/// route only after graph ACL, tenant binding, lazy-open and placement lookup.
/// It prepares raw CAS + one ChangeEnvelope, then delegates the actual commit
/// to the existing ChangeEnvelope authority. Non-source methods continue into
/// the resource router without duplicating the dispatch-chain control flow.
async fn route_source_ingestion_or_resources(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let source_method =
        match method {
            Method::SourceIngest { request } => SourceIngestionRoute::Ingest(request),
            Method::SourceIngestStatus { connector, stream } => SourceIngestionRoute::Status(
                eg_types::source_ingestion::SourceIngestStatusRequest { connector, stream },
            ),
            method => return route_native_typed_ops(ctx, method).await,
        };
    route_source_ingestion_dispatch(ctx, source_method).await
}

async fn route_source_ingestion_dispatch(
    ctx: GraphOpRouting<'_>,
    source_method: SourceIngestionRoute,
) -> Result<Response, Method> {
    let (placement_epoch, fencing_token) = match source_ingestion_route_fence(&ctx).await {
        Ok(fence) => fence,
        Err(response) => return Ok(response),
    };

    let response = match source_method {
        SourceIngestionRoute::Status(request) => route_source_ingestion_status(ctx, request).await,
        SourceIngestionRoute::Ingest(request) => {
            route_source_ingestion_request(ctx, *request, placement_epoch, fencing_token).await
        }
    };
    Ok(response)
}

async fn route_source_ingestion_status(
    ctx: GraphOpRouting<'_>,
    request: eg_types::source_ingestion::SourceIngestStatusRequest,
) -> Response {
    let Some(persistence) = ctx.persistence.as_ref() else {
        return Response::err(
            ctx.req_id,
            "source ingestion status requires durable persistence",
        );
    };
    handlers::source_ingestion::status(
        ctx.req_id,
        ctx.graph_name,
        ctx.tenant_scope,
        ctx.verified_context,
        persistence,
        request,
    )
    .await
}

async fn route_source_ingestion_request(
    ctx: GraphOpRouting<'_>,
    request: eg_types::source_ingestion::SourceIngestionRequest,
    placement_epoch: u64,
    fencing_token: Option<u64>,
) -> Response {
    let prepared = match handlers::source_ingestion::prepare(
        handlers::source_ingestion::PrepareContext {
            state: ctx.state,
            request_id: ctx.req_id,
            graph_name: ctx.graph_name,
            tenant_scope: ctx.tenant_scope,
            verified: ctx.verified_context,
            graph_version: ctx.core.version(),
            placement_epoch,
            fencing_token,
        },
        request,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let envelope = prepared.envelope.clone();
    let response =
        crate::server::dispatch::change_envelope::apply_one_change_envelope(ctx, envelope).await;
    handlers::source_ingestion::finish(prepared, response)
}

enum SourceIngestionRoute {
    Ingest(Box<eg_types::source_ingestion::SourceIngestionRequest>),
    Status(eg_types::source_ingestion::SourceIngestStatusRequest),
}

#[cfg(feature = "raft")]
async fn source_ingestion_route_fence(
    ctx: &GraphOpRouting<'_>,
) -> Result<(u64, Option<u64>), Response> {
    let Some(routed) = ctx.routed_raft.as_ref() else {
        return Ok((0, None));
    };
    let leader = routed.handle.current_leader().await;
    if leader != Some(routed.handle.node_id) {
        return Err(Response::stale_route(
            ctx.req_id,
            ctx.graph_name,
            routed.group_id,
            routed.epoch,
            leader,
            "Source ingestion requires the current placement leader",
        ));
    }
    Ok((routed.epoch, Some(routed.group_id)))
}

#[cfg(not(feature = "raft"))]
async fn source_ingestion_route_fence(
    _ctx: &GraphOpRouting<'_>,
) -> Result<(u64, Option<u64>), Response> {
    Ok((0, None))
}

/// The remaining authority-bearing surfaces, all resolved AFTER graph ACL,
/// lazy materialization and placement: the series-write fence, the knowledge
/// stream, time series, audit verification/inclusion, served modality, and the
/// Raft write-routing barrier.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
pub(super) async fn route_graph_authority_surfaces(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_graph_placement_surfaces(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match route_graph_audit_and_modality(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    #[cfg(feature = "raft")]
    if let Some(routed) = ctx
        .routed_raft
        .clone()
        .filter(|_| crate::mutation_apply::is_durable_mutation(&method))
    {
        return Ok(dispatch_op_raft_write_routing_barrier(
            RaftWriteBarrierCtx {
                state: ctx.state,
                req_id: ctx.req_id,
                graph_name: ctx.graph_name,
                verified_context: ctx.verified_context,
                tenant_scope: ctx.tenant_scope,
                graph_type: ctx.graph_type,
            },
            routed,
            method,
        )
        .await);
    }
    Err(method)
}

#[allow(unused_variables)]
async fn route_graph_placement_surfaces(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[cfg(all(feature = "raft", feature = "tsdb"))]
    if let Some(stale) = dispatch_op_tsdb_write_fence(
        ctx.req_id,
        ctx.graph_name,
        ctx.routed_raft.as_ref(),
        &method,
    )
    .await
    {
        return Ok(stale);
    }

    #[cfg(all(feature = "raft", feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = ts_placement_fence(ctx.routed_raft.as_ref());
    #[cfg(all(not(feature = "raft"), feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = (0, None);

    #[cfg(feature = "knowledge-batch")]
    if matches!(&method, Method::KnowledgeStream { .. }) {
        return Ok(dispatch_op_knowledge_stream(
            KnowledgeStreamCtx {
                state: ctx.state,
                req_id: ctx.req_id,
                graph_name: ctx.graph_name,
                verified_context: ctx.verified_context,
                read_authority: ctx.read_authority,
                verified_actor: ctx.verified_actor,
                core: ctx.core.clone(),
                #[cfg(feature = "security")]
                rls: ctx.rls.clone(),
                #[cfg(feature = "raft")]
                routed_raft: ctx.routed_raft.clone(),
                knowledge_stream_authority: ctx.knowledge_stream_authority.clone(),
            },
            method,
        )
        .await);
    }

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
            ctx.state,
            ctx.req_id,
            ctx.graph_name,
            ctx.verified_context,
            ts_placement_epoch,
            ts_fencing_token,
            method,
        )
        .await);
    }
    Err(method)
}

#[allow(unused_variables)]
async fn route_graph_audit_and_modality(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[cfg(feature = "security")]
    if matches!(method, Method::AuditVerify) {
        return Ok(
            dispatch_op_audit_verify(ctx.req_id, ctx.graph_name, ctx.persistence.clone()).await,
        );
    }

    #[cfg(feature = "security")]
    let method = match method {
        Method::AuditProveInclusion {
            node_id,
            anchor_seq,
        } => {
            return Ok(prove_audit_inclusion(
                ctx.req_id,
                ctx.graph_name,
                ctx.persistence.clone(),
                node_id,
                anchor_seq,
            )
            .await)
        }
        method => method,
    };

    #[cfg(feature = "modality-serving")]
    let method = match method {
        Method::ServedModality { op } => {
            return Ok(apply_served_modality(
                ServedModalityCtx {
                    state: ctx.state,
                    req_id: ctx.req_id,
                    graph_name: ctx.graph_name,
                    caller: ctx.caller,
                    verified_context: ctx.verified_context,
                    tenant_scope: ctx.tenant_scope,
                    gateway_authz_ctx: ctx.gateway_authz_ctx,
                    core: ctx.core.clone(),
                    materialization_manifest: ctx.materialization_manifest.clone(),
                    persistence: ctx.persistence.clone(),
                    #[cfg(feature = "streaming")]
                    cdc: ctx.cdc.clone(),
                    #[cfg(feature = "raft")]
                    routed_raft: ctx.routed_raft.clone(),
                    modality_authority: ctx.modality_authority.clone(),
                },
                op,
            )
            .await)
        }
        method => method,
    };
    Err(method)
}

/// Try each post-lock router in the ORIGINAL order — change envelopes, then the
/// native stores, then the authority surfaces. A method none of them owns falls
/// through to the ordinary dispatch pipeline.
pub(super) async fn route_graph_op_method(
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
