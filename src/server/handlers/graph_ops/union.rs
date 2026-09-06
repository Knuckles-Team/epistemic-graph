use super::*;

use super::terminal::GraphOpsContext;

/// Resolve a cross-graph union read's graph set to their cores (CONCEPT:EG-KG.query.cross-graph-union).
///
/// Access-checks each graph as Read, clones the `Arc<GraphCore>`s, and holds only
/// the registry read lock for the resolution — the per-graph topology locks are
/// NOT taken here; the caller reads each core sequentially after this returns, so
/// two graph locks are never held at once (the cross-graph deadlock discipline).
/// Missing graphs are skipped (a lane graph may not exist yet); one denied graph
/// fails the whole union.
async fn resolve_union_cores(
    state: &Arc<RwLock<ServerState>>,
    read_authority: &GraphReadAuthority,
    graphs: &[String],
) -> Result<Vec<Arc<GraphCore>>, String> {
    let s = state.read().await;
    let mut cores = Vec::with_capacity(graphs.len());
    for name in graphs {
        let entry = match s.registry.get(name) {
            Some(e) => e,
            None => continue,
        };
        check_graph_access(
            &s.isolation,
            read_authority.actor(),
            name,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        )?;
        cores.push(entry.core.clone());
    }
    drop(s);
    Ok(cores
        .iter()
        .map(|core| read_authority.project_core(core))
        .collect())
}

/// `UnionGetNodeProperties`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_union_get_node_properties(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    graphs: Vec<String>,
    node_id: String,
) -> Response {
    // First-found across the graph set (in order); point reads, no
    // snapshot. Registry lock released before any per-core read.
    let cores = match resolve_union_cores(state, read_authority, &graphs).await {
        Ok(c) => c,
        Err(denied) => return Response::err(req_id, denied),
    };
    for c in &cores {
        if let Some(props) = c.get_node_properties(&node_id) {
            return Response::ok(req_id, ResultPayload::PropertiesMsgpack(props));
        }
    }
    Response::ok(req_id, ResultPayload::Json(serde_json::Value::Null))
}

/// `DiffAgainst`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_diff_against(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    core: &Arc<GraphCore>,
    other_graph: String,
) -> Response {
    let s_lock = state.read().await;
    let other_entry = match s_lock.registry.get(&other_graph) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Other graph '{}' not found", other_graph)),
    };
    // Diffing reads the other graph's content — gate it as a read.
    if let Err(denied) = check_graph_access(
        &s_lock.isolation,
        read_authority.actor(),
        &other_graph,
        other_entry.graph_type,
        other_entry.owner.as_deref(),
        AccessLevel::Read,
    ) {
        return Response::err(req_id, denied);
    }
    let other_core = other_entry.core.clone();
    drop(s_lock);

    // Snapshot the other graph first, then diff under only THIS
    // graph's lock — never hold two graph locks at once (two
    // concurrent opposite-direction diffs plus a queued writer can
    // deadlock a write-preferring RwLock). The diff itself is a
    // single O(V+E) comparison, so it stays under-lock (KG-2.51).
    let other_core = read_authority.project_core(&other_core);
    let other_snap = { other_core.analysis_snapshot() };
    let g1 = &**core;
    let diff_str = g1.diff_against(&other_snap);
    match serde_json::from_slice::<serde_json::Value>(diff_str.as_bytes()) {
        Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
        Err(e) => Response::err(req_id, e.to_string()),
    }
}

/// `UnionGetNodesByLabel`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_union_get_nodes_by_label(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    graphs: Vec<String>,
    label: String,
    limit: usize,
) -> Response {
    let cores = match resolve_union_cores(state, read_authority, &graphs).await {
        Ok(c) => c,
        Err(denied) => return Response::err(req_id, denied),
    };
    let nodes = union_dedup_nodes_by_label(&cores, &label, limit);
    Response::ok(req_id, ResultPayload::NodeList(nodes))
}

/// Union + dedup the label-matching nodes across `cores`, in `cores` order,
/// stopping at `limit` (0 = unbounded). Split out of
/// `handle_union_get_nodes_by_label` so the accumulation loop's nesting does not
/// count against the handler's own complexity. Byte-identical behaviour: the
/// `'outer` label became a plain early `return` from this function, which is
/// what breaking out of the loop did.
fn union_dedup_nodes_by_label(
    cores: &[Arc<GraphCore>],
    label: &str,
    limit: usize,
) -> Vec<(String, serde_json::Value)> {
    let mut seen = std::collections::HashSet::new();
    let mut nodes: Vec<(String, serde_json::Value)> = Vec::new();
    for c in cores {
        for (k, p) in c.get_nodes_by_label(label, limit) {
            if !seen.insert(k.clone()) {
                continue;
            }
            let val = eg_types::msgpack::decode_property_value(&p).unwrap_or(serde_json::json!({}));
            nodes.push((k, val));
            if limit != 0 && nodes.len() >= limit {
                return nodes;
            }
        }
    }
    nodes
}

/// `UnionGetNeighbors`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_union_get_neighbors(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    graphs: Vec<String>,
    node_id: String,
) -> Response {
    let cores = match resolve_union_cores(state, read_authority, &graphs).await {
        Ok(c) => c,
        Err(denied) => return Response::err(req_id, denied),
    };
    let out = union_dedup_neighbors(&cores, &node_id);
    Response::ok(req_id, ResultPayload::Ids(out))
}

/// Union + dedup the neighbour ids of `node_id` across `cores`, in `cores`
/// order. Split out of `handle_union_get_neighbors` so the accumulation loop's
/// nesting does not count against the handler's own complexity. Byte-identical
/// behaviour, including silently skipping a core that errors on the lookup.
fn union_dedup_neighbors(cores: &[Arc<GraphCore>], node_id: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for c in cores {
        let Ok(ns) = c.get_neighbors(node_id) else {
            continue;
        };
        for n in ns {
            if seen.insert(n.clone()) {
                out.push(n);
            }
        }
    }
    out
}

/// Handle cross-graph union reads.
pub(super) async fn try_handle_cross_graph_union(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        state,
        req_id,
        read_authority,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::UnionGetNodeProperties { graphs, node_id } => {
            handle_union_get_node_properties(state, req_id, read_authority, graphs, node_id).await
        }
        Method::UnionGetNodesByLabel {
            graphs,
            label,
            limit,
        } => {
            handle_union_get_nodes_by_label(state, req_id, read_authority, graphs, label, limit)
                .await
        }
        Method::UnionGetNeighbors { graphs, node_id } => {
            handle_union_get_neighbors(state, req_id, read_authority, graphs, node_id).await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Handle subgraph comparison and compaction operations.
pub(super) async fn try_handle_subgraph_comparison(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext {
        state,
        req_id,
        read_authority,
        core,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::DiffAgainst { other_graph } => {
            handle_diff_against(state, req_id, read_authority, core, other_graph).await
        }
        // CompactNodesByType (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED
        // — see the AddNode/RemoveNode comment above.
        Method::CompactNodesByType { .. } => unreachable!(
            "CompactNodesByType is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
                 route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        other => return ControlFlow::Continue(other),
    })
}
