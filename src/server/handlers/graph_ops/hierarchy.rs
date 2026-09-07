use super::*;

use super::terminal::GraphOpsContext;

/// `ClusterHierarchyRefresh`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_cluster_hierarchy_refresh(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    label: Option<String>,
    resolution: f64,
    seed: u64,
) -> Response {
    let snap = { core.analysis_snapshot() };
    let computed = match compute_off_lock(req_id, move || {
        crate::algorithms::cluster_hierarchy(&snap, label.as_deref(), resolution, seed)
    })
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let persistence = { state.read().await.persistence.clone() };
    let cached = if let Some(p) = persistence.as_ref() {
        let blob = match rmp_serde::to_vec_named(&computed) {
            Ok(b) => b,
            Err(e) => {
                return Response::err(req_id, format!("failed to encode cluster hierarchy: {e}"))
            }
        };
        match p.save_cluster_hierarchy(graph_name, blob).await {
            Ok(()) => true,
            Err(e) => {
                return Response::err(req_id, format!("failed to persist cluster hierarchy: {e}"))
            }
        }
    } else {
        false
    };
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "graph": graph_name,
            "levels": computed.levels.len(),
            "base_node_count": computed.base_node_count,
            "base_edge_count": computed.base_edge_count,
            "top_level_clusters": computed.levels.last().map(|l| l.clusters.len()).unwrap_or(0),
            "cached": cached,
        })),
    )
}

/// `ClusterHierarchyClusters`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_cluster_hierarchy_clusters(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    level: usize,
    parent_cluster_id: Option<String>,
) -> Response {
    let hierarchy = match load_cached_cluster_hierarchy(state, req_id, graph_name).await {
        Ok(h) => h,
        Err(resp) => return resp,
    };
    if level == 0 || level > hierarchy.levels.len() {
        return Response::err(
            req_id,
            format!(
                "level {level} out of range (cached hierarchy has {} level(s))",
                hierarchy.levels.len()
            ),
        );
    }
    let level_data = &hierarchy.levels[level - 1];
    let (clusters_json, remap) =
        project_level_clusters(&level_data.clusters, parent_cluster_id.as_deref());
    let inter_cluster_edges: Vec<serde_json::Value> = level_data
        .inter_cluster_edges
        .iter()
        .filter_map(|&(s, d, w)| {
            let ls = remap.get(&(s as usize))?;
            let ld = remap.get(&(d as usize))?;
            Some(serde_json::json!({ "src_idx": ls, "dst_idx": ld, "weight": w }))
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "level": level,
            "clusters": clusters_json,
            "inter_cluster_edges": inter_cluster_edges,
        })),
    )
}

/// Load + decode this graph's cached cluster hierarchy, or return the exact
/// error `Response` the two `ClusterHierarchy*` read handlers previously built
/// inline (both carried a byte-identical copy of this block before the
/// extraction). `Err(Response)` is the caller's early return.
async fn load_cached_cluster_hierarchy(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
) -> Result<crate::algorithms::ClusterHierarchyResult, Response> {
    let persistence = { state.read().await.persistence.clone() };
    let Some(p) = persistence.as_ref() else {
        return Err(Response::err(
            req_id,
            "cluster hierarchy cache unavailable on this backend".to_string(),
        ));
    };
    let blob = match p.load_cluster_hierarchy(graph_name).await {
        Ok(Some(b)) => b,
        Ok(None) => {
            return Err(Response::err(
                req_id,
                "no cluster hierarchy cached for this graph -- call \
                 ClusterHierarchyRefresh first"
                    .to_string(),
            ))
        }
        Err(e) => {
            return Err(Response::err(
                req_id,
                format!("failed to load cluster hierarchy: {e}"),
            ))
        }
    };
    rmp_serde::from_slice(&blob)
        .map_err(|e| Response::err(req_id, format!("cached cluster hierarchy is corrupt: {e}")))
}

/// Project one level's clusters to wire JSON, optionally filtered to one
/// parent's children, returning the JSON alongside the local-index remap.
///
/// Local (array-local, per the VIZ-1 contract) indices: unfiltered ⇒ identity
/// map; filtered by `parent_cluster_id` ⇒ remapped to the returned subset's own
/// 0..k positions, so the caller can filter `inter_cluster_edges` down to edges
/// between two clusters BOTH still present. Split out of
/// `handle_cluster_hierarchy_clusters` so the filter loop's nesting does not
/// count against the handler; byte-identical behaviour.
fn project_level_clusters(
    clusters: &[crate::algorithms::ClusterMeta],
    parent_cluster_id: Option<&str>,
) -> (
    Vec<serde_json::Value>,
    std::collections::HashMap<usize, u32>,
) {
    let mut remap: std::collections::HashMap<usize, u32> = std::collections::HashMap::new();
    let mut clusters_json: Vec<serde_json::Value> = Vec::new();
    for (i, c) in clusters.iter().enumerate() {
        if parent_cluster_id.is_some_and(|pid| c.parent_id.as_deref() != Some(pid)) {
            continue;
        }
        remap.insert(i, clusters_json.len() as u32);
        clusters_json.push(serde_json::json!({
            "id": c.id,
            "label": c.label,
            "node_count": c.node_count,
            "edge_count": c.edge_count,
            "top_node_types": c.top_node_types,
        }));
    }
    (clusters_json, remap)
}

/// `ClusterHierarchyExpand`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_cluster_hierarchy_expand(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    cluster_id: String,
) -> Response {
    let Some((level, local_idx)) = crate::algorithms::parse_cluster_id(&cluster_id) else {
        return Response::err(req_id, format!("malformed cluster_id: {cluster_id}"));
    };
    let hierarchy = match load_cached_cluster_hierarchy(state, req_id, graph_name).await {
        Ok(h) => h,
        Err(resp) => return resp,
    };
    if level == 1 {
        expand_leaf_cluster_to_nodes(req_id, core, &hierarchy, local_idx, &cluster_id)
    } else {
        expand_coarse_cluster_to_children(req_id, &hierarchy, level, &cluster_id)
    }
}

/// `ClusterHierarchyExpand` at the finest computed level: drill all the way to
/// real graph nodes, read LIVE off the (already RLS-projected) core rather than
/// any stale snapshot inside the cache -- membership is what's frozen until the
/// next refresh, never the member nodes' own content. Split out of
/// `handle_cluster_hierarchy_expand`'s `level == 1` branch; byte-identical
/// behaviour.
fn expand_leaf_cluster_to_nodes(
    req_id: u64,
    core: &Arc<GraphCore>,
    hierarchy: &crate::algorithms::ClusterHierarchyResult,
    local_idx: usize,
    cluster_id: &str,
) -> Response {
    let member_ids: Vec<String> = hierarchy
        .leaf_membership
        .iter()
        .filter(|(_, idx)| *idx as usize == local_idx)
        .map(|(id, _)| id.clone())
        .collect();
    if member_ids.is_empty() {
        return Response::err(req_id, format!("unknown cluster_id: {cluster_id}"));
    }
    let sub = core.get_subgraph(&member_ids);
    let nodes: Vec<serde_json::Value> = sub
        .node_properties
        .iter()
        .map(|(id, blob)| {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            serde_json::json!({ "id": id, "properties": props })
        })
        .collect();
    let edges = expand_subgraph_edges_to_wire(&sub);
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "nodes": nodes,
            "edges": edges,
            "child_clusters": Vec::<serde_json::Value>::new(),
        })),
    )
}

/// Flatten an induced subgraph's per-pair edge property blobs to the VIZ-1 wire
/// edge shape. Split out so the nested pair/blob loop does not count against
/// `expand_leaf_cluster_to_nodes`; byte-identical behaviour, including the `"_"`
/// fallback for a blob with no decodable `relationship`.
fn expand_subgraph_edges_to_wire(sub: &crate::graph::GraphView) -> Vec<serde_json::Value> {
    let mut edges: Vec<serde_json::Value> = Vec::new();
    for ((src, tgt), blobs) in &sub.edge_properties {
        for blob in blobs {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            let relationship = props
                .get("relationship")
                .and_then(|v| v.as_str())
                .unwrap_or("_");
            edges.push(serde_json::json!({
                "src_id": src, "dst_id": tgt, "type": relationship,
            }));
        }
    }
    edges
}

/// `ClusterHierarchyExpand` at a coarser level: drill down ONE level at a time
/// -- hand back its children (from level - 1) rather than raw nodes, matching
/// "expand-on-demand" (the caller `expand`s again on one child to go further,
/// instead of every level materializing every node). Split out of
/// `handle_cluster_hierarchy_expand`'s `else` branch; byte-identical behaviour.
fn expand_coarse_cluster_to_children(
    req_id: u64,
    hierarchy: &crate::algorithms::ClusterHierarchyResult,
    level: usize,
    cluster_id: &str,
) -> Response {
    let Some(child_level) = hierarchy.levels.get(level - 2) else {
        return Response::err(req_id, format!("malformed cluster_id: {cluster_id}"));
    };
    let child_clusters: Vec<serde_json::Value> = child_level
        .clusters
        .iter()
        .filter(|c| c.parent_id.as_deref() == Some(cluster_id))
        .map(|c| {
            serde_json::json!({
                "id": c.id,
                "label": c.label,
                "node_count": c.node_count,
                "edge_count": c.edge_count,
                "top_node_types": c.top_node_types,
            })
        })
        .collect();
    if child_clusters.is_empty() {
        return Response::err(req_id, format!("unknown cluster_id: {cluster_id}"));
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "nodes": Vec::<serde_json::Value>::new(),
            "edges": Vec::<serde_json::Value>::new(),
            "child_clusters": child_clusters,
        })),
    )
}

/// Handle persisted cluster hierarchy visualization operations.
pub(super) async fn try_handle_hierarchy_visualization(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        state,
        req_id,
        graph_name,
        core,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::ClusterHierarchyRefresh {
            label,
            resolution,
            seed,
        } => {
            handle_cluster_hierarchy_refresh(
                state, req_id, graph_name, core, label, resolution, seed,
            )
            .await
        }
        Method::ClusterHierarchyClusters {
            level,
            parent_cluster_id,
        } => {
            handle_cluster_hierarchy_clusters(state, req_id, graph_name, level, parent_cluster_id)
                .await
        }
        Method::ClusterHierarchyExpand { cluster_id } => {
            handle_cluster_hierarchy_expand(state, req_id, graph_name, core, cluster_id).await
        }
        // PruneByLifecycle (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED —
        // see the AddNode/RemoveNode comment above.
        other => return ControlFlow::Continue(other),
    })
}
