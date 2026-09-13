use super::*;

use super::terminal::GraphOpsContext;
use eg_types::compute_result::algorithms::{
    ClusterExpansion, ClusterHierarchySummary, ClusterLevelView, ClusterMemberEdge,
    ClusterMemberNode, ClusterSummary, InterClusterEdge,
};
use eg_types::result_contract::compute as results;

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
        ResultPayload::of::<results::ClusterHierarchyRefresh>(ClusterHierarchySummary {
            graph: graph_name.to_string(),
            levels: computed.levels.len(),
            base_node_count: computed.base_node_count,
            base_edge_count: computed.base_edge_count,
            top_level_clusters: computed
                .levels
                .last()
                .map(|l| l.clusters.len())
                .unwrap_or(0),
            cached,
        }),
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
    let (clusters, remap) =
        project_level_clusters(&level_data.clusters, parent_cluster_id.as_deref());
    let inter_cluster_edges: Vec<InterClusterEdge> = level_data
        .inter_cluster_edges
        .iter()
        .filter_map(|&(s, d, weight)| {
            Some(InterClusterEdge {
                src_idx: *remap.get(&(s as usize))?,
                dst_idx: *remap.get(&(d as usize))?,
                weight,
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<results::ClusterHierarchyClusters>(ClusterLevelView {
            level,
            clusters,
            inter_cluster_edges,
        }),
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

/// Project one level's clusters to their wire summaries, optionally filtered to
/// one parent's children, returning the summaries alongside the local-index remap.
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
) -> (Vec<ClusterSummary>, std::collections::HashMap<usize, u32>) {
    let mut remap: std::collections::HashMap<usize, u32> = std::collections::HashMap::new();
    let mut summaries: Vec<ClusterSummary> = Vec::new();
    for (i, c) in clusters.iter().enumerate() {
        if parent_cluster_id.is_some_and(|pid| c.parent_id.as_deref() != Some(pid)) {
            continue;
        }
        remap.insert(i, summaries.len() as u32);
        summaries.push(cluster_summary(c));
    }
    (summaries, remap)
}

/// One cached cluster's wire summary.
fn cluster_summary(c: &crate::algorithms::ClusterMeta) -> ClusterSummary {
    ClusterSummary {
        id: c.id.clone(),
        label: c.label.clone(),
        node_count: c.node_count,
        edge_count: c.edge_count,
        top_node_types: c.top_node_types.clone(),
    }
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
    let nodes: Vec<ClusterMemberNode> = sub
        .node_properties
        .iter()
        .map(|(id, blob)| ClusterMemberNode {
            id: id.clone(),
            properties: eg_types::msgpack::decode_property_value(blob)
                .unwrap_or(serde_json::Value::Null),
        })
        .collect();
    let edges = expand_subgraph_edges_to_wire(&sub);
    Response::ok(
        req_id,
        ResultPayload::of::<results::ClusterHierarchyExpand>(ClusterExpansion {
            nodes,
            edges,
            child_clusters: Vec::new(),
        }),
    )
}

/// Flatten an induced subgraph's per-pair edge property blobs to the VIZ-1 wire
/// edge shape. Split out so the nested pair/blob loop does not count against
/// `expand_leaf_cluster_to_nodes`; byte-identical behaviour, including the `"_"`
/// fallback for a blob with no decodable `relationship`.
fn expand_subgraph_edges_to_wire(sub: &crate::graph::GraphView) -> Vec<ClusterMemberEdge> {
    let mut edges: Vec<ClusterMemberEdge> = Vec::new();
    for ((src, tgt), blobs) in &sub.edge_properties {
        for blob in blobs {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            let relationship = props
                .get("relationship")
                .and_then(|v| v.as_str())
                .unwrap_or("_");
            edges.push(ClusterMemberEdge {
                src_id: src.clone(),
                dst_id: tgt.clone(),
                relationship: relationship.to_string(),
            });
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
    let child_clusters: Vec<ClusterSummary> = child_level
        .clusters
        .iter()
        .filter(|c| c.parent_id.as_deref() == Some(cluster_id))
        .map(cluster_summary)
        .collect();
    if child_clusters.is_empty() {
        return Response::err(req_id, format!("unknown cluster_id: {cluster_id}"));
    }
    Response::ok(
        req_id,
        ResultPayload::of::<results::ClusterHierarchyExpand>(ClusterExpansion {
            nodes: Vec::new(),
            edges: Vec::new(),
            child_clusters,
        }),
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
