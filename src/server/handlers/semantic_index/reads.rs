use std::sync::Arc;

use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{
    SemanticBindingPage, SemanticIndexCommand, SemanticIndexOp, SemanticIndexOperation,
    SemanticIndexOutcome, SemanticIndexRequest, SemanticIndexResponse, SemanticIndexResult,
};

use crate::protocol::Response;

use super::{blocking, reply, SemanticIndexContext};
use crate::server::semantic_index::SemanticIndexServerAdapter;

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    match op {
        SemanticIndexOp::GetBindingRequest { request } => get_binding_request(ctx, *request).await,
        SemanticIndexOp::Binding { .. } => binding(ctx).await,
        SemanticIndexOp::SqlSourceManifest {
            generation,
            source_entity_id,
            ..
        } => sql_source_manifest(ctx, generation, source_entity_id).await,
        SemanticIndexOp::ListBindings { filter, cursor, .. } => {
            list_bindings(ctx, filter, cursor).await
        }
        SemanticIndexOp::LiveGeneration { .. } => live_generation(ctx).await,
        _ => unreachable!("read handler received a non-read operation"),
    }
}

async fn get_binding_request(
    ctx: &SemanticIndexContext<'_>,
    request: SemanticIndexRequest,
) -> Response {
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    if let Err(error) = adapter.authorize(&request, ctx.authority, ctx.now_ms) {
        return Response::err(ctx.req_id, error);
    }
    if !matches!(request.command, SemanticIndexCommand::GetBinding { .. }) {
        return Response::err(ctx.req_id, "unsupported semantic-index request command");
    }
    let service = Arc::clone(&ctx.service);
    let binding = match blocking(ctx.req_id, move || service.binding()).await {
        Ok(binding) => binding,
        Err(response) => return response,
    };
    let outcome = match binding {
        Some(binding) => {
            if request
                .validate_against_binding(&binding, ctx.now_ms)
                .is_err()
            {
                // An existing binding under another purpose or policy must be
                // indistinguishable from an absent owner to this caller.
                SemanticIndexOutcome::NotFound
            } else {
                SemanticIndexOutcome::Accepted {
                    result: SemanticIndexResult::Binding {
                        binding: Box::new(binding),
                    },
                    receipt_digest: None,
                }
            }
        }
        None => SemanticIndexOutcome::NotFound,
    };
    let response = SemanticIndexResponse {
        request_id: request.request_id,
        operation: SemanticIndexOperation::GetBinding,
        outcome,
    };
    if let Err(error) = response.validate() {
        return Response::err(ctx.req_id, format!("semantic response invalid: {error:?}"));
    }
    reply::<ingestion_results::SemanticIndexGetBindingRequest, _>(ctx.req_id, Ok(response))
}

async fn binding(ctx: &SemanticIndexContext<'_>) -> Response {
    let service = Arc::clone(&ctx.service);
    reply::<ingestion_results::SemanticIndexBinding, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || service.binding()).await,
    )
}

async fn sql_source_manifest(
    ctx: &SemanticIndexContext<'_>,
    generation: u64,
    source_entity_id: String,
) -> Response {
    let service = Arc::clone(&ctx.service);
    reply::<ingestion_results::SemanticIndexSqlSourceManifest, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.sql_source_manifest(generation, &source_entity_id)
        })
        .await,
    )
}

async fn list_bindings(
    ctx: &SemanticIndexContext<'_>,
    filter: eg_types::semantic_index::SemanticIndexFilter,
    cursor: Option<String>,
) -> Response {
    let service = Arc::clone(&ctx.service);
    let page = blocking(ctx.req_id, move || {
        service
            .list_bindings(&filter, cursor.as_deref())
            .map(|(entries, next_cursor)| SemanticBindingPage {
                entries,
                next_cursor,
            })
    })
    .await;
    reply::<ingestion_results::SemanticIndexListBindings, _>(ctx.req_id, page)
}

async fn live_generation(ctx: &SemanticIndexContext<'_>) -> Response {
    let service = Arc::clone(&ctx.service);
    reply::<ingestion_results::SemanticIndexLiveGeneration, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || service.live_generation()).await,
    )
}
