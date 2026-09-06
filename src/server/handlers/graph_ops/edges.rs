use super::*;

use super::terminal::GraphOpsContext;

/// Intelligent overload backstop for the `GetEdges` full-graph dump — the
/// edge-count sibling of [`oversize_dump_error`]. Given the graph's edge `count`
/// and the configured `cap`, returns `Some(error_message)` when the dump would
/// exceed the cap, or `None` when it is within bounds and safe to materialize.
/// The cap is always positive in served state and cannot be disabled. Pure +
/// side-effect-free so the threshold logic is unit-tested directly, independent
/// of process-global env.
fn oversize_edge_dump_error(count: usize, cap: usize) -> Option<String> {
    if count > cap {
        Some(format!(
            "RESULT_TOO_LARGE: GetEdges would return {count} edges (> cap {cap}); \
             the full-graph dump is refused to protect the connection. Use a \
             bounded query instead (GetEdgesPage(after, limit) / paginate), or \
             raise EPISTEMIC_GRAPH_MAX_RESPONSE_EDGES."
        ))
    } else {
        None
    }
}

/// `GetEdgePropertiesBatch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_edge_properties_batch(
    req_id: u64,
    core: &Arc<GraphCore>,
    edges: Vec<(String, String)>,
) -> Response {
    if edges.len() > MAX_BATCH_IDS {
        return Response::err(
            req_id,
            format!(
                "batch too large: {} edges (max {})",
                edges.len(),
                MAX_BATCH_IDS
            ),
        );
    }
    let g = &**core;
    // One round-trip for N edges. Each entry is the list of property blobs
    // for that (src, tgt) pair (a pair may have multiple edges), in input
    // order; an empty inner list ⇒ no such edge.
    let out: Vec<Vec<serde_bytes::ByteBuf>> = edges
        .into_iter()
        .map(|(src, tgt)| {
            g.get_edge_properties(&src, &tgt)
                .into_iter()
                .map(serde_bytes::ByteBuf::from)
                .collect()
        })
        .collect();
    Response::ok(req_id, ResultPayload::raw(&out))
}

/// Route gateway-owned edge mutations.
pub(super) async fn try_handle_edge_writes(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let _ = ctx;
    match method {
        Method::AddEdge { .. } => unreachable!(
            "AddEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::RemoveEdge { .. } => unreachable!(
            "RemoveEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // InvalidateEdge/SupersedeEdge (CONCEPT:EG-P0-2 bypass guard, L11):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above.
        Method::InvalidateEdge { .. } => unreachable!(
            "InvalidateEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
                 route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::SupersedeEdge { .. } => unreachable!(
            "SupersedeEdge is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
                 route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        other => ControlFlow::Continue(other),
    }
}

/// Handle point and paginated edge reads.
pub(super) async fn try_handle_edge_reads(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::HasEdge {
            source_id,
            target_id,
        } => {
            let g = &*core;
            Response::ok(
                req_id,
                ResultPayload::Bool(g.has_edge(&source_id, &target_id)),
            )
        }
        Method::GetEdges => {
            let g = &*core;
            // Intelligent overload backstop (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation), the edge-count
            // sibling of the `GetNodes` guard just above `try_handle`'s match:
            // check the cheap O(1) edge count BEFORE building the Vec, and
            // return a typed, catchable error instead of the pathological
            // gigabyte-scale frame. `GetEdgesPage` (bounded pagination) is
            // intentionally unaffected.
            if let Some(msg) = oversize_edge_dump_error(g.edge_count(), max_response_edges()) {
                return ControlFlow::Break(Response::err(req_id, msg));
            }
            Response::ok(req_id, ResultPayload::EdgeList(g.get_edges()))
        }
        Method::GetEdgesPage { after, limit } => {
            let g = &*core;
            let after_ref = after
                .as_ref()
                .map(|(s, t, ord)| (s.as_str(), t.as_str(), *ord));
            let edges = g.get_edges_page(after_ref, limit);
            Response::ok(req_id, ResultPayload::raw(&edges))
        }
        Method::GetEdgeProperties {
            source_id,
            target_id,
        } => {
            let g = &*core;
            let props = g.get_edge_properties(&source_id, &target_id);
            let val: Vec<serde_json::Value> = props
                .into_iter()
                .map(|p| {
                    eg_types::msgpack::decode_property_value(&p).unwrap_or(serde_json::json!({}))
                })
                .collect();
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(val)))
        }
        Method::GetEdgePropertiesBatch { edges } => {
            handle_get_edge_properties_batch(req_id, core, edges)
        }
        // ClearGraph (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above.
        other => return ControlFlow::Continue(other),
    })
}

/// Handle graph clearing and count reads.
pub(super) async fn try_handle_graph_counts(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::ClearGraph => unreachable!(
            "ClearGraph is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::EdgeCount => {
            let g = &*core;
            Response::ok(req_id, ResultPayload::Count(g.edge_count() as u64))
        }
        // TopologicalSort / FindCycle / GetShortestPath / components / blast
        // radius / degree centrality are single-pass O(V+E); they run on a cheap
        // topology snapshot (Phase C-B: the read algorithms take an unlocked
        // GraphView, so the structural copy replaces the held read lock).
        other => return ControlFlow::Continue(other),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edges_under_cap_returns_no_error_so_data_is_served() {
        assert_eq!(oversize_edge_dump_error(0, 50_000), None);
        assert_eq!(oversize_edge_dump_error(1, 50_000), None);
        assert_eq!(oversize_edge_dump_error(49_999, 50_000), None);
        // Exactly at the cap is allowed (the guard fires only when EXCEEDED).
        assert_eq!(oversize_edge_dump_error(50_000, 50_000), None);
    }

    #[test]
    fn edges_over_cap_returns_typed_error_not_a_giant_payload() {
        let err = oversize_edge_dump_error(166_000, 50_000)
            .expect("over-cap dump must produce an error, not the data");
        assert!(err.starts_with("RESULT_TOO_LARGE"), "got: {err}");
        // The message must steer the caller to the bounded alternative.
        assert!(err.contains("GetEdgesPage"), "got: {err}");
        assert!(
            err.contains("166000") && err.contains("50000"),
            "got: {err}"
        );
    }

    #[test]
    fn edges_zero_cap_fails_safe() {
        assert!(oversize_edge_dump_error(1, 0).is_some());
    }
}
