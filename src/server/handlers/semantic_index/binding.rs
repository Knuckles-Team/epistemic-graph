use std::sync::Arc;

use eg_core::compute::semantic_ann_codes::OperationAttribution;
use eg_core::compute::semantic_index_service::SemanticIndexService;
use eg_types::contract::Nonce;
use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{SemanticBinding, SemanticIndexOp, SemanticSqlSourceManifest};

use crate::protocol::Response;
use crate::server::semantic_index::SemanticIndexServerAdapter;

use super::{blocking, contracts, reply, stamp_draft_identity, SemanticIndexContext};

pub(super) fn invalid_semantic_input(req_id: u64, subject: &'static str) -> Response {
    Response::err(req_id, format!("INVALID_ARGUMENT: {subject} rejected"))
}

/// Every operation-bound binding mutation consumes the same verified
/// attempt nonce before crossing the service's durable mutation boundary.
fn verified_attempt_nonce(ctx: &SemanticIndexContext<'_>) -> Result<Nonce, Response> {
    ctx.authority.attempt_nonce().ok_or_else(|| {
        Response::err(
            ctx.req_id,
            "ACCESS_DENIED: semantic mutation requires a verified attempt nonce",
        )
    })
}

/// Capture one verified operation identity and service handle before an
/// operation crosses into the blocking native mutation path.
fn binding_operation_inputs(
    ctx: &SemanticIndexContext<'_>,
) -> Result<(Arc<SemanticIndexService>, u64, String, Nonce), Response> {
    let nonce = verified_attempt_nonce(ctx)?;
    Ok((
        Arc::clone(&ctx.service),
        ctx.now_ms,
        ctx.authority.agent_id().to_string(),
        nonce,
    ))
}

/// Validate a replacement against its stamped caller identity and worker
/// authorization before either mutation path can touch durable state.
pub(super) fn checked_replacement(
    ctx: &SemanticIndexContext<'_>,
    mut draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    source_manifest: eg_types::semantic_index::SemanticSqlSourceManifestDraft,
) -> Result<(SemanticBinding, SemanticSqlSourceManifest), Response> {
    stamp_draft_identity(&mut draft, ctx.authority);
    let replacement = SemanticBinding::create(*draft)
        .map_err(|_| invalid_semantic_input(ctx.req_id, "semantic binding"))?;
    let manifest = SemanticSqlSourceManifest::create(source_manifest)
        .map_err(|_| invalid_semantic_input(ctx.req_id, "semantic source manifest"))?;
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    adapter
        .authorize_binding_worker(&replacement, ctx.authority)
        .map_err(|error| Response::err(ctx.req_id, error))?;
    Ok((replacement, manifest))
}

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
        } => {
            binding_lifecycle(
                ctx,
                expected_generation,
                BindingLifecycle::Transition(next_state),
                idempotency_key,
            )
            .await
        }
        SemanticIndexOp::DropBinding {
            expected_generation,
            idempotency_key,
            ..
        } => {
            binding_lifecycle(
                ctx,
                expected_generation,
                BindingLifecycle::Drop,
                idempotency_key,
            )
            .await
        }
        _ => unreachable!("binding handler received a non-binding operation"),
    }
}

fn checked_admission_inputs(
    ctx: &SemanticIndexContext<'_>,
    mut draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
) -> Result<
    (
        SemanticBinding,
        Arc<SemanticIndexService>,
        u64,
        String,
        Nonce,
    ),
    Response,
> {
    stamp_draft_identity(&mut draft, ctx.authority);
    let binding = SemanticBinding::create(*draft)
        .map_err(|_| invalid_semantic_input(ctx.req_id, "semantic binding"))?;
    let (service, now_ms, actor, nonce) = binding_operation_inputs(ctx)?;
    Ok((binding, service, now_ms, actor, nonce))
}

async fn admit_binding(
    ctx: &SemanticIndexContext<'_>,
    draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    idempotency_key: String,
) -> Response {
    let (binding, service, now_ms, actor, nonce) = match checked_admission_inputs(ctx, draft) {
        Ok(inputs) => inputs,
        Err(response) => return response,
    };
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
    draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    source_manifest: eg_types::semantic_index::SemanticSqlSourceManifestDraft,
    idempotency_key: String,
) -> Response {
    let (replacement, manifest) = match checked_replacement(ctx, draft, source_manifest) {
        Ok(validated) => validated,
        Err(response) => return response,
    };
    let (service, now_ms, actor, nonce) = match binding_operation_inputs(ctx) {
        Ok(inputs) => inputs,
        Err(response) => return response,
    };
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

enum BindingLifecycle {
    Transition(eg_types::semantic_index::SemanticBindingState),
    Drop,
}

async fn binding_lifecycle(
    ctx: &SemanticIndexContext<'_>,
    expected_generation: u64,
    command: BindingLifecycle,
    idempotency_key: String,
) -> Response {
    let (service, now_ms, actor, nonce) = match binding_operation_inputs(ctx) {
        Ok(inputs) => inputs,
        Err(response) => return response,
    };
    match command {
        BindingLifecycle::Transition(next_state) => {
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
        BindingLifecycle::Drop => reply::<ingestion_results::SemanticIndexDropBinding, _>(
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
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::invalid_semantic_input;

    #[test]
    fn malformed_semantic_input_has_a_declared_stable_code_and_safe_detail() {
        assert!(eg_capabilities::error_routing::method_allows_error(
            "SemanticIndex",
            "INVALID_ARGUMENT"
        ));
        for (subject, detail) in [
            ("semantic binding", "semantic binding rejected"),
            (
                "semantic source manifest",
                "semantic source manifest rejected",
            ),
        ] {
            let response = invalid_semantic_input(19, subject);
            assert_eq!(response.id, 19);
            assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
            assert_eq!(response.error_detail.as_deref(), Some(detail));
        }
    }
}
