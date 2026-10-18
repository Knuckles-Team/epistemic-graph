//! The graph-scoped routes of `EnrichDocument` (EG-UNIFIED-DATA-PLANE-R651.7)
//! and `SelectWebPages` (EG-DECISION-ENGINE-R802).
//!
//! Both arrive after graph ACL, tenant binding, lazy-open and placement
//! lookup. Enrichment reads one RLS-visible Document, embeds off the reactor
//! and commits its write-set through the existing ChangeEnvelope authority,
//! exactly like a scoped `IndexRepository`. Selection reads the visible
//! `Document` rows and answers; it writes nothing.

#[cfg(all(feature = "query", feature = "modality-serving"))]
use super::repository_index::commit_context;
use super::*;

/// Route the two web-ingest methods; hand any other method back.
pub(super) async fn route_web_ingest(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::EnrichDocument { request } => Ok(enrich(ctx, *request).await),
        Method::SelectWebPages { request } => Ok(select(ctx, *request).await),
        other => Err(other),
    }
}

#[cfg(all(feature = "query", feature = "modality-serving"))]
fn visible(ctx: &GraphOpRouting<'_>, node_id: &str) -> bool {
    ctx.read_authority
        .as_ref()
        .is_none_or(|authority| authority.node_visible(ctx.core, node_id))
}

#[cfg(all(feature = "query", feature = "modality-serving"))]
async fn enrich(
    ctx: GraphOpRouting<'_>,
    request: eg_types::source_ingestion::web_document::EnrichDocumentRequest,
) -> Response {
    use handlers::web_ingest as web;
    let req_id = ctx.req_id;
    let doc_id = request.document_id.clone();
    let properties = match ctx.core.get_node_properties(&doc_id) {
        Some(properties) if visible(&ctx, &doc_id) => properties,
        _ => {
            return Response::err(
                req_id,
                format!("DOCUMENT_NOT_FOUND: no Document '{doc_id}'"),
            )
        }
    };
    let (document, config) = match web::held_document(&request, &properties)
        .and_then(|document| web::chunking(&request).map(|config| (document, config)))
    {
        Ok(pair) => pair,
        Err(error) => return Response::err(req_id, error),
    };
    let records = ctx.core.get_nodes_by_label(web::ENRICHMENT_TYPE, 0);
    let (current, prior) = web::prior_enrichments(&doc_id, &document.digest_hex, records);
    if let Some(record) = current {
        let receipt = web::replay_receipt(&doc_id, &document.digest_hex, &record);
        return respond(req_id, receipt);
    }
    let embedder = web::embedder::EmbedderConfig::from_env();
    let embedded = crate::server::compute::compute_off_lock(req_id, move || {
        let spans = web::chunks(&document, &config)?;
        let texts: Vec<&str> = spans.iter().map(|span| span.text(&document.text)).collect();
        let vectors = embedder.embed(&texts)?;
        Ok::<_, String>((document, spans, vectors, embedder))
    })
    .await;
    let (document, spans, vectors, embedder) = match embedded {
        Ok(Ok(parts)) => parts,
        Ok(Err(error)) => return Response::err(req_id, error),
        Err(response) => return response,
    };
    let plan = web::EnrichmentPlan {
        document_id: &doc_id,
        document: &document,
        config,
        spans: &spans,
        vectors,
        embedder: embedder.name(),
        embedding_model: embedder.model.clone(),
        dimensions: embedder.dims,
        prior,
    };
    match web::lower(plan) {
        Ok((write_set, receipt)) => commit(ctx, write_set, receipt).await,
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(all(feature = "query", feature = "modality-serving"))]
async fn commit(
    ctx: GraphOpRouting<'_>,
    write_set: handlers::repository_index::IndexWriteSet,
    receipt: eg_types::source_ingestion::web_document::EnrichDocumentReceipt,
) -> Response {
    let (placement_epoch, fencing_token) = match source_ingestion_route_fence(&ctx).await {
        Ok(fence) => fence,
        Err(response) => return response,
    };
    let commit_ctx = commit_context(&ctx, placement_epoch, fencing_token);
    let envelope = match handlers::repository_index::build_labelled_envelope(
        &commit_ctx,
        write_set,
        &handlers::web_ingest::LABELS,
    ) {
        Ok(envelope) => envelope,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let committed =
        crate::server::dispatch::change_envelope::apply_one_change_envelope(ctx, envelope).await;
    match committed.refusal_text() {
        Some(refusal) => Response::err(committed.id, refusal),
        None => respond(committed.id, receipt),
    }
}

#[cfg(all(feature = "query", feature = "modality-serving"))]
fn respond(
    req_id: u64,
    receipt: eg_types::source_ingestion::web_document::EnrichDocumentReceipt,
) -> Response {
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::ingestion::EnrichDocument>(receipt),
    )
}

#[cfg(not(all(feature = "query", feature = "modality-serving")))]
async fn enrich(
    ctx: GraphOpRouting<'_>,
    _request: eg_types::source_ingestion::web_document::EnrichDocumentRequest,
) -> Response {
    Response::err(
        ctx.req_id,
        "ENRICHMENT_UNAVAILABLE: document enrichment requires the query and modality-serving features",
    )
}

#[cfg(all(feature = "query", feature = "modality-serving", feature = "decide"))]
async fn select(
    ctx: GraphOpRouting<'_>,
    request: eg_types::decision::web_fetch_selection::SelectWebPagesRequest,
) -> Response {
    use handlers::web_ingest as web;
    let documents: Vec<_> = ctx
        .core
        .get_nodes_by_label("Document", 0)
        .into_iter()
        .filter(|(id, _)| visible(&ctx, id))
        .collect();
    let (held, digests) = web::held_pages(documents);
    let now_ms = request
        .now_ms
        .unwrap_or_else(crate::server::dispatch::authoritative_now_ms);
    let digest = web::query_digest(&request.query);
    match eg_numeric::decision::web_fetch_selection::select_web_pages(
        &request, &held, &digests, now_ms, digest,
    ) {
        Ok(answer) => Response::ok(
            ctx.req_id,
            ResultPayload::of::<eg_types::result_contract::query::SelectWebPages>(answer),
        ),
        Err(error) => Response::err(ctx.req_id, error),
    }
}

#[cfg(not(all(feature = "query", feature = "modality-serving", feature = "decide")))]
async fn select(
    ctx: GraphOpRouting<'_>,
    _request: eg_types::decision::web_fetch_selection::SelectWebPagesRequest,
) -> Response {
    Response::err(
        ctx.req_id,
        "SELECTION_UNAVAILABLE: web page selection requires the decide feature",
    )
}
