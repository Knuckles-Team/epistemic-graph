use std::sync::Arc;

use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{SemanticBindingPage, SemanticIndexOp};

use crate::protocol::Response;

use super::{blocking, contracts, reply, SemanticIndexContext};

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    match op {
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
