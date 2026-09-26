use std::sync::Arc;

use eg_types::result_contract::ingestion as ingestion_results;
use eg_types::semantic_index::{SemanticBinding, SemanticIndexOp, SemanticSqlSourceManifest};

use crate::protocol::Response;
use crate::server::semantic_index::SemanticIndexServerAdapter;

use super::binding::invalid_semantic_input;
use super::{
    blocking, contracts, decode_cursor, read_port, reply, stamp_draft_identity,
    SemanticIndexContext,
};

pub(super) async fn handle(ctx: &SemanticIndexContext<'_>, op: SemanticIndexOp) -> Response {
    match op {
        SemanticIndexOp::AdmitSourceRecord { record, .. } => admit_record(ctx, *record).await,
        SemanticIndexOp::AdmitSourcePage { record, cursor, .. } => {
            admit_page(ctx, *record, cursor).await
        }
        SemanticIndexOp::AdmitSourceReconcile { record, .. } => admit_reconcile(ctx, *record).await,
        SemanticIndexOp::AdmitSourceReplacement {
            draft,
            source_manifest,
            record,
            ..
        } => admit_replacement(ctx, draft, *source_manifest, *record).await,
        _ => unreachable!("source handler received a non-source operation"),
    }
}

async fn admit_record(
    ctx: &SemanticIndexContext<'_>,
    record: eg_types::mutation_batch::MutationOutboxRecord,
) -> Response {
    let port = read_port(ctx.persist_dir, ctx.authority);
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexAdmitSourceRecord, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.admit_sql_source_dirty_record(&record, &port, now_ms)
        })
        .await
        .map(contracts::receipt),
    )
}

async fn admit_page(
    ctx: &SemanticIndexContext<'_>,
    record: eg_types::mutation_batch::MutationOutboxRecord,
    cursor: Option<String>,
) -> Response {
    let cursor = match decode_cursor(cursor) {
        Ok(cursor) => cursor,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let port = read_port(ctx.persist_dir, ctx.authority);
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexAdmitSourcePage, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.admit_sql_source_dirty_page(&record, &port, cursor.as_deref(), now_ms)
        })
        .await
        .map(contracts::page),
    )
}

async fn admit_reconcile(
    ctx: &SemanticIndexContext<'_>,
    record: eg_types::mutation_batch::MutationOutboxRecord,
) -> Response {
    let port = read_port(ctx.persist_dir, ctx.authority);
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexAdmitSourceReconcile, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.admit_sql_source_dirty_reconcile(&record, &port, now_ms)
        })
        .await
        .map(contracts::reconciliation),
    )
}

async fn admit_replacement(
    ctx: &SemanticIndexContext<'_>,
    mut draft: Box<eg_types::semantic_index::SemanticBindingDraft>,
    source_manifest: eg_types::semantic_index::SemanticSqlSourceManifestDraft,
    record: eg_types::mutation_batch::MutationOutboxRecord,
) -> Response {
    stamp_draft_identity(&mut draft, ctx.authority);
    let replacement = match SemanticBinding::create(*draft) {
        Ok(binding) => binding,
        Err(_) => return invalid_semantic_input(ctx.req_id, "semantic binding"),
    };
    let manifest = match SemanticSqlSourceManifest::create(source_manifest) {
        Ok(manifest) => manifest,
        Err(_) => return invalid_semantic_input(ctx.req_id, "semantic source manifest"),
    };
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&ctx.service));
    if let Err(error) = adapter.authorize_binding_worker(&replacement, ctx.authority) {
        return Response::err(ctx.req_id, error);
    }
    let service = Arc::clone(&ctx.service);
    let now_ms = ctx.now_ms;
    reply::<ingestion_results::SemanticIndexAdmitSourceReplacement, _>(
        ctx.req_id,
        blocking(ctx.req_id, move || {
            service.admit_sql_source_dirty_replacement(&replacement, &manifest, &record, now_ms)
        })
        .await
        .map(contracts::receipt),
    )
}

#[cfg(test)]
mod tests {
    use super::invalid_semantic_input;

    #[test]
    fn replacement_input_refusals_use_the_declared_semantic_code() {
        for subject in ["semantic binding", "semantic source manifest"] {
            let response = invalid_semantic_input(31, subject);
            assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
            assert_eq!(
                response.error_detail.as_deref(),
                Some(format!("{subject} rejected").as_str())
            );
        }
    }
}
