//! `Method::EdgeIndex` and `Method::EdgeSearch` (EH-351 / EH-352): the
//! user-managed edge-native indexes of the request graph.
//!
//! The verified tenant's SQL catalog is the durable home of every edge index
//! (its registration and its activated generation). An index is installed into
//! the request graph's `IndexManager` on the tenant's first edge operation after
//! a restart — restored and reconciled against the graph as it is, never
//! rebuilt — and is maintained from then on by every committed batch. A search
//! walks the caller's row-level-security view: an edge whose endpoint the caller
//! may not see is absent from that view, and the edge's own visibility metadata
//! is checked inside the walk with the same rule the graph applies.

use eg_query::edge_index::{
    create_durable_edge_index, drop_durable_edge_index, edge_index, install_edge_indexes,
    refresh_durable_edge_index, EdgeIndexKind, EdgeIndexSpec, EdgeQuery, EdgeScope,
    EdgeSearchRequest,
};
use eg_query::VectorMetric;
use eg_types::managed_index::{
    self as wire, EdgeIndexOp, EdgeIndexStatusView, EdgeSearchHit, EdgeSearchView,
};
use eg_types::{CmpOp, RowPredicate};

use super::*;

/// Route the two edge-index methods; every other method falls through.
pub(super) async fn dispatch_edge_method(
    ctx: &QueryHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::EdgeIndex { op } => Ok(handle_edge_index(ctx, *op).await),
        Method::EdgeSearch { request } => Ok(handle_edge_search(ctx, *request).await),
        other => Err(other),
    }
}

/// The verified tenant, and its SQL catalog with every registered edge index of
/// the request graph installed.
async fn tenant_catalog(
    ctx: &QueryHandlerCtx<'_>,
) -> Result<(String, eg_query::TableStore), String> {
    let carrier = ctx
        .read_authority
        .and_then(GraphReadAuthority::carrier)
        .ok_or_else(|| "ACCESS_DENIED: current signed tenant authority is required".to_string())?;
    let persist_dir = sql_persist_dir(ctx.state).await?;
    let store =
        crate::server::sql_tables::tenant_table_store(carrier.tenant_scope(), &persist_dir)?;
    install_edge_indexes(&store, ctx.graph_name, ctx.core)?;
    Ok((carrier.tenant_scope().to_string(), store))
}

async fn handle_edge_index(ctx: &QueryHandlerCtx<'_>, op: EdgeIndexOp) -> Response {
    let req_id = ctx.req_id;
    let (tenant, store) = match tenant_catalog(ctx).await {
        Ok(found) => found,
        Err(error) => return Response::err(req_id, error),
    };
    let core = Arc::clone(ctx.core);
    let graph = ctx.graph_name.to_string();
    let outcome = compute_off_lock(req_id, move || {
        apply_edge_index_op(&store, &graph, &core, &tenant, op)?;
        status_view(&core)
    })
    .await;
    match outcome {
        Ok(Ok(view)) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::query::EdgeIndex>(view),
        ),
        Ok(Err(error)) => Response::err(req_id, error),
        Err(response) => response,
    }
}

fn apply_edge_index_op(
    store: &eg_query::TableStore,
    graph: &str,
    core: &GraphCore,
    tenant: &str,
    op: EdgeIndexOp,
) -> Result<(), String> {
    let block_error = |block: eg_types::managed_index::IndexBlock| {
        format!("{}: {}", block.reason.as_str(), block.detail)
    };
    match op {
        EdgeIndexOp::Create { definition } => {
            let name = definition.name.clone();
            create_durable_edge_index(store, graph, core, edge_spec(tenant, definition))
                .map_err(block_error)?;
            refresh_durable_edge_index(store, graph, core, &name).map(|_| ())
        }
        EdgeIndexOp::Refresh { name } => {
            refresh_durable_edge_index(store, graph, core, &name).map(|_| ())
        }
        EdgeIndexOp::Drop { name } => {
            drop_durable_edge_index(store, graph, core, &name).map(|_| ())
        }
        EdgeIndexOp::Status => Ok(()),
    }
}

fn edge_spec(tenant: &str, definition: wire::EdgeIndexDefinition) -> EdgeIndexSpec {
    EdgeIndexSpec {
        name: definition.name,
        property: definition.property,
        kind: match definition.kind {
            wire::EdgeIndexKind::Vector { metric } => EdgeIndexKind::Vector {
                metric: match metric {
                    wire::EdgeVectorMetric::L2 => VectorMetric::L2,
                    wire::EdgeVectorMetric::Cosine => VectorMetric::Cosine,
                    wire::EdgeVectorMetric::InnerProduct => VectorMetric::InnerProduct,
                },
            },
            wire::EdgeIndexKind::Text => EdgeIndexKind::Text,
        },
        scope: EdgeScope {
            tenant: tenant.to_string(),
            purpose: definition.purpose,
        },
    }
}

fn status_view(core: &GraphCore) -> Result<EdgeIndexStatusView, String> {
    Ok(EdgeIndexStatusView {
        indexes: eg_types::contract::BoundedVec::new(core.indexes().managed_statuses())?,
    })
}

async fn handle_edge_search(
    ctx: &QueryHandlerCtx<'_>,
    request: wire::EdgeSearchRequest,
) -> Response {
    let req_id = ctx.req_id;
    let (tenant, _store) = match tenant_catalog(ctx).await {
        Ok(found) => found,
        Err(error) => return Response::err(req_id, error),
    };
    let Some(authority) = ctx.read_authority else {
        return Response::err(
            req_id,
            "ACCESS_DENIED: a verified read authority is required".to_string(),
        );
    };
    match search(ctx, authority, &tenant, request) {
        Ok(view) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::query::EdgeSearch>(view),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// The caller's search: the RLS view of the request graph at the version read
/// before it, the edge's own visibility and the typed filters inside the walk.
fn search(
    ctx: &QueryHandlerCtx<'_>,
    authority: &GraphReadAuthority,
    tenant: &str,
    request: wire::EdgeSearchRequest,
) -> Result<EdgeSearchView, String> {
    let index = edge_index(ctx.core, &request.index)
        .ok_or_else(|| format!("edge index `{}` does not exist", request.index))?;
    let version = ctx.core.version();
    let mut view = ctx.core.analysis_snapshot();
    authority.filter_view(&mut view);
    let scope = EdgeScope {
        tenant: tenant.to_string(),
        purpose: request.purpose.clone(),
    };
    let prefilter = edge_prefilter(&request);
    let visible = |blob: &[u8]| authority.can_see_blob(blob);
    let vector: Vec<f32>;
    let query = match &request.query {
        wire::EdgeSearchQuery::Vector { vector: values } => {
            vector = values.as_slice().to_vec();
            EdgeQuery::Vector(&vector)
        }
        wire::EdgeSearchQuery::Text { text } => EdgeQuery::Text(text),
    };
    let answer = index.search(
        &view,
        version,
        &EdgeSearchRequest {
            scope: &scope,
            query,
            k: (request.k as usize).min(wire::MAX_EDGE_SEARCH_HITS),
            prefilter: prefilter.as_ref(),
            visible: Some(&visible),
        },
    )?;
    let hits = answer
        .hits
        .into_iter()
        .map(|hit| EdgeSearchHit {
            source: hit.edge.source,
            target: hit.edge.target,
            ordinal: hit.edge.ordinal,
            score: f64::from(hit.score),
        })
        .collect();
    Ok(EdgeSearchView {
        hits: eg_types::contract::BoundedVec::new(hits)?,
        generation: answer.path.ok(),
        fallback: answer.path.err().map(|reason| format!("{reason:?}")),
    })
}

/// The typed filters as one conjunction over the edge's properties.
fn edge_prefilter(request: &wire::EdgeSearchRequest) -> Option<RowPredicate> {
    let equals = |col: &str, value: serde_json::Value| RowPredicate::Cmp {
        col: col.to_string(),
        op: CmpOp::Eq,
        value,
    };
    let parts: Vec<RowPredicate> = request
        .edge_type
        .iter()
        .map(|kind| equals("type", serde_json::Value::String(kind.clone())))
        .chain(
            request
                .property_equals
                .iter()
                .map(|filter| equals(&filter.property, filter.value.clone())),
        )
        .collect();
    (!parts.is_empty()).then_some(RowPredicate::And(parts))
}
