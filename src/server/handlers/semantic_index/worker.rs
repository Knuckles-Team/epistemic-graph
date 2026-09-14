use std::sync::Arc;

use eg_transaction::OutboxClaimBudget;
use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{SemanticIndexOp, SemanticStageLeaseEntry, SemanticStageLeasePage};

use crate::protocol::Response;
use crate::server::semantic_index::{
    AuthorizedSqlSourceClaim, SemanticIndexServerAdapter, SqlSourceStageCompletion,
};

use super::{
    blocking, consumer_ack, contracts, current_binding, decode_cursor, own_lease, read_port, reply,
    typed_payload, SemanticIndexContext,
};

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    match op {
        SemanticIndexOp::SubscribeStageConsumer { .. } => subscribe(ctx).await,
        SemanticIndexOp::ClaimStageLeases {
            queue_class,
            limit,
            lease_ms,
            ..
        } => claim(ctx, queue_class, limit, lease_ms).await,
        SemanticIndexOp::ValidateStageLease { lease, .. } => validate(ctx, *lease).await,
        SemanticIndexOp::StageStatus { .. } => status(ctx).await,
        SemanticIndexOp::CompleteStage {
            lease,
            transition,
            artifact,
            successor,
            ..
        } => complete_stage(ctx, *lease, *transition, *artifact, successor.map(|v| *v)).await,
        SemanticIndexOp::CompleteGenerationStage {
            lease,
            transition,
            artifact,
            successor,
            ..
        } => {
            complete_generation_stage(ctx, *lease, *transition, *artifact, successor.map(|v| *v))
                .await
        }
        SemanticIndexOp::CompleteSqlSourceStage {
            lease,
            transition,
            successor,
            page_cursor,
            ..
        } => {
            complete_sql_source_stage(ctx, *lease, *transition, successor.map(|v| *v), page_cursor)
                .await
        }
        SemanticIndexOp::ReplayCompletedSqlSourceStage {
            lease,
            expected_intent,
            ..
        } => replay(ctx, *lease, *expected_intent).await,
        SemanticIndexOp::ReleaseStageLease { lease, .. } => release(ctx, *lease).await,
        _ => unreachable!("worker handler received a non-worker operation"),
    }
}

async fn subscribe(ctx: &SemanticIndexContext<'_>) -> Response {
    let consumer = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    reply::<ingestion_results::SemanticIndexSubscribeStageConsumer, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service
                .subscribe_stage_consumer(&consumer)
                .map(|()| consumer_ack())
        })
        .await,
    )
}

async fn claim(
    ctx: &SemanticIndexContext<'_>,
    queue_class: eg_types::semantic_index::SemanticQueueClass,
    limit: u32,
    lease_ms: u64,
) -> Response {
    let consumer = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    let claimed = blocking(ctx.req_id, move || {
        let mut budget = OutboxClaimBudget::new(limit, lease_ms, now_ms)
            .map_err(eg_core::compute::semantic_ann_codes::SemanticCodeError::Refused)?;
        let outcome = service.claim_stage_leases(&consumer, &mut budget)?;
        let mut entries = Vec::with_capacity(outcome.claims.len());
        let mut released_other_class = 0u32;
        for lease in outcome.claims {
            let intent = service.validate_stage_lease(&lease, &consumer, now_ms)?;
            if intent.stage.queue_class() == queue_class {
                entries.push(SemanticStageLeaseEntry {
                    lease,
                    queue_class,
                    intent,
                });
            } else {
                service.release_stage_lease(&lease)?;
                released_other_class = released_other_class.saturating_add(1);
            }
        }
        Ok(SemanticStageLeasePage {
            queue_class,
            entries,
            more_available: outcome.more_available,
            released_other_class,
        })
    })
    .await;
    reply::<ingestion_results::SemanticIndexClaimStageLeases, _>(ctx.req_id, claimed)
}

async fn validate(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
) -> Response {
    let consumer = ctx.authority.agent_id().to_string();
    if lease.consumer != consumer {
        return Response::err(
            ctx.req_id,
            "ACCESS_DENIED: semantic lease owner does not match verified carrier",
        );
    }
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexValidateStageLease, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.validate_stage_lease(&lease, &consumer, now_ms)
        })
        .await,
    )
}

async fn status(ctx: &SemanticIndexContext<'_>) -> Response {
    let consumer = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    let status = blocking(ctx.req_id, move || service.stage_status(&consumer, now_ms))
        .await
        .map(contracts::outbox_status);
    reply::<ingestion_results::SemanticIndexStageStatus, _>(ctx.req_id, status)
}

async fn complete_stage(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
    transition: eg_types::semantic_index::SemanticStageTransition,
    artifact: eg_types::semantic_index::SemanticStageArtifact,
    successor: Option<eg_types::semantic_index::SemanticStageIntent>,
) -> Response {
    if let Err(error) = own_lease(&lease, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexCompleteStage, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.complete_stage(&lease, &transition, &artifact, successor.as_ref(), now_ms)
        })
        .await
        .map(contracts::receipt),
    )
}

async fn complete_generation_stage(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
    transition: eg_types::semantic_index::SemanticStageTransition,
    artifact: eg_types::semantic_index::SemanticGenerationArtifact,
    successor: Option<eg_types::semantic_index::SemanticStageIntent>,
) -> Response {
    if let Err(error) = own_lease(&lease, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexCompleteGenerationStage, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.complete_generation_stage(
                &lease,
                &transition,
                &artifact,
                successor.as_ref(),
                now_ms,
            )
        })
        .await
        .map(contracts::receipt),
    )
}

async fn complete_sql_source_stage(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
    transition: eg_types::semantic_index::SemanticStageTransition,
    successor: Option<eg_types::semantic_index::SemanticStageIntent>,
    page_cursor: Option<String>,
) -> Response {
    if let Err(error) = own_lease(&lease, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let page_cursor = match decode_cursor(page_cursor) {
        Ok(cursor) => cursor,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let (binding, claim) = match claim_sql_source(ctx, transition.intent.clone(), page_cursor).await
    {
        Ok(claim) => claim,
        Err(response) => return response,
    };
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    match adapter
        .complete_sql_source_stage(
            ctx.req_id,
            read_port(ctx.persist_dir, ctx.authority),
            binding,
            SqlSourceStageCompletion {
                lease,
                transition,
                claim,
                successor,
            },
            ctx.now_ms,
        )
        .await
    {
        Ok(receipt) => typed_payload::<ingestion_results::SemanticIndexCompleteSqlSourceStage, _>(
            ctx.req_id,
            &contracts::receipt(receipt),
        ),
        Err(response) => response,
    }
}

async fn claim_sql_source(
    ctx: &SemanticIndexContext<'_>,
    intent: eg_types::semantic_index::SemanticStageIntent,
    page_cursor: Option<Vec<u8>>,
) -> Result<
    (
        eg_types::semantic_index::SemanticBinding,
        AuthorizedSqlSourceClaim,
    ),
    Response,
> {
    let binding = current_binding(ctx.req_id, &ctx.service).await?;
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    adapter
        .authorize_binding_worker(&binding, ctx.authority)
        .map_err(|error| Response::err(ctx.req_id, error))?;
    let claim = adapter
        .claim_sql_source(
            ctx.req_id,
            read_port(ctx.persist_dir, ctx.authority),
            binding.clone(),
            intent,
            page_cursor,
        )
        .await?;
    Ok((binding, claim))
}

async fn replay(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
    expected_intent: eg_types::semantic_index::SemanticStageIntent,
) -> Response {
    if let Err(error) = own_lease(&lease, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    match adapter
        .replay_sql_source_stage(
            ctx.req_id,
            ctx.authority.clone(),
            lease,
            expected_intent,
            ctx.now_ms,
        )
        .await
    {
        Ok(receipt) => typed_payload::<
            ingestion_results::SemanticIndexReplayCompletedSqlSourceStage,
            _,
        >(ctx.req_id, &contracts::replay(receipt)),
        Err(response) => response,
    }
}

async fn release(
    ctx: &SemanticIndexContext<'_>,
    lease: eg_types::mutation_batch::MutationOutboxLease,
) -> Response {
    if let Err(error) = own_lease(&lease, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let service = Arc::clone(&ctx.service);
    reply::<ingestion_results::SemanticIndexReleaseStageLease, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || service.release_stage_lease(&lease))
            .await
            .map(|()| true),
    )
}
