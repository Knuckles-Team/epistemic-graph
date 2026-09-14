use std::sync::Arc;

use eg_core::compute::semantic_ann_codes::OperationAttribution;
use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{SemanticBinding, SemanticIndexOp, SemanticSqlSourceManifest};

use crate::protocol::Response;
use crate::server::semantic_index::SemanticIndexServerAdapter;

use super::{blocking, contracts, reply, stamp_draft_identity, SemanticIndexContext};

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    match op {
        SemanticIndexOp::AdmitBinding {
            draft,
            idempotency_key,
            ..
        } => admit_binding(ctx, draft, idempotency_key).await,
        SemanticIndexOp::RefreshBinding {
            expected_generation,
            draft,
            source_manifest,
            idempotency_key,
            ..
        } => {
            refresh_binding(
                ctx,
                expected_generation,
                draft,
                *source_manifest,
                idempotency_key,
            )
            .await
        }
        SemanticIndexOp::TransitionBinding {
            expected_generation,
            next_state,
            idempotency_key,
            ..
        } => transition_binding(ctx, expected_generation, next_state, idempotency_key).await,
        SemanticIndexOp::DropBinding {
            expected_generation,
            idempotency_key,
            ..
        } => drop_binding(ctx, expected_generation, idempotency_key).await,
        _ => unreachable!("binding handler received a non-binding operation"),
    }
}

async fn admit_binding(
    ctx: &SemanticIndexContext<'_>,
    mut draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    idempotency_key: String,
) -> Response {
    stamp_draft_identity(&mut draft, ctx.authority);
    let binding = match SemanticBinding::create(*draft) {
        Ok(binding) => binding,
        Err(error) => {
            return Response::err(ctx.req_id, format!("semantic binding rejected: {error:?}"))
        }
    };
    let nonce = match ctx.authority.attempt_nonce() {
        Some(nonce) => nonce,
        None => {
            return Response::err(
                ctx.req_id,
                "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
            )
        }
    };
    let actor = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexAdmitBinding, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.admit_binding_operation(&binding, now_ms, &actor, &idempotency_key, nonce)
        })
        .await
        .map(contracts::receipt),
    )
}

async fn refresh_binding(
    ctx: &SemanticIndexContext<'_>,
    expected_generation: u64,
    mut draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    source_manifest: eg_types::semantic_index::SemanticSqlSourceManifestDraft,
    idempotency_key: String,
) -> Response {
    stamp_draft_identity(&mut draft, ctx.authority);
    let replacement = match SemanticBinding::create(*draft) {
        Ok(binding) => binding,
        Err(error) => {
            return Response::err(ctx.req_id, format!("semantic binding rejected: {error:?}"))
        }
    };
    let manifest = match SemanticSqlSourceManifest::create(source_manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("semantic source manifest rejected: {error:?}"),
            )
        }
    };
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    if let Err(error) = adapter.authorize_binding_worker(&replacement, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let nonce = match ctx.authority.attempt_nonce() {
        Some(nonce) => nonce,
        None => {
            return Response::err(
                ctx.req_id,
                "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
            )
        }
    };
    let actor = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexRefreshBinding, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.refresh_binding_operation(
                expected_generation,
                &replacement,
                &manifest,
                now_ms,
                OperationAttribution {
                    actor: &actor,
                    idempotency_key: &idempotency_key,
                    nonce,
                },
            )
        })
        .await
        .map(contracts::receipt),
    )
}

async fn transition_binding(
    ctx: &SemanticIndexContext<'_>,
    expected_generation: u64,
    next_state: eg_types::semantic_index::SemanticBindingState,
    idempotency_key: String,
) -> Response {
    let nonce = match ctx.authority.attempt_nonce() {
        Some(nonce) => nonce,
        None => {
            return Response::err(
                ctx.req_id,
                "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
            )
        }
    };
    let actor = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexTransitionBinding, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.transition_binding_operation(
                expected_generation,
                next_state,
                now_ms,
                &actor,
                &idempotency_key,
                nonce,
            )
        })
        .await
        .map(contracts::receipt),
    )
}

async fn drop_binding(
    ctx: &SemanticIndexContext<'_>,
    expected_generation: u64,
    idempotency_key: String,
) -> Response {
    let nonce = match ctx.authority.attempt_nonce() {
        Some(nonce) => nonce,
        None => {
            return Response::err(
                ctx.req_id,
                "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
            )
        }
    };
    let actor = ctx.authority.agent_id().to_string();
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexDropBinding, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.drop_binding_operation(
                expected_generation,
                now_ms,
                &actor,
                &idempotency_key,
                nonce,
            )
        })
        .await
        .map(contracts::receipt),
    )
}
