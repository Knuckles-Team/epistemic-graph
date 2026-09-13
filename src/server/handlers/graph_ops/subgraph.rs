use super::*;

use super::terminal::GraphOpsContext;

/// `GetSubgraph`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
fn handle_get_subgraph(req_id: u64, core: &Arc<GraphCore>, node_ids: &[String]) -> Response {
    // Batched subgraph read: return the induced nodes (with DECODED
    // properties) and the edges among them in ONE round-trip, so callers
    // never loop per-node `GetNodeProperties` or pull the whole edge set.
    // (Previously serialized to msgpack then mis-parsed as JSON → error.)
    let g = &**core;
    let sub = g.get_subgraph(node_ids);
    let mut nodes = Vec::with_capacity(sub.node_properties.len());
    for (id, blob) in &sub.node_properties {
        let props =
            eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
        nodes.push(serde_json::json!({ "id": id, "properties": props }));
    }
    let mut edges = Vec::new();
    for ((src, tgt), blobs) in &sub.edge_properties {
        for blob in blobs {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            edges.push(serde_json::json!({
                "source": src, "target": tgt, "properties": props
            }));
        }
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({ "nodes": nodes, "edges": edges })),
    )
}

/// `Fork`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
fn handle_fork(req_id: u64, core: &Arc<GraphCore>) -> Response {
    // Cannot return the forked GraphCore directly because it needs to be registered.
    // A true fork method in the registry might be better.
    // For now, we return the JSON representation of the fork.
    let g = &**core;
    let sub = g.fork();
    match sub.to_msgpack() {
        Ok(json) => match serde_json::from_slice::<serde_json::Value>(&json) {
            Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
            Err(e) => Response::err(req_id, e.to_string()),
        },
        Err(e) => Response::err(req_id, e),
    }
}

/// `Vf2SubgraphMatch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
pub(super) async fn handle_vf2_subgraph_match(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    core: &Arc<GraphCore>,
    pattern_graph_name: String,
    max_results: usize,
    max_steps: usize,
) -> Response {
    let s = state.read().await;
    // The pattern graph is read too — gate it like any other read.
    if let Some(entry) = s.registry.get(&pattern_graph_name) {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            read_authority.actor(),
            &pattern_graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        ) {
            return Response::err(req_id, denied);
        }
    }
    let pattern_core = s
        .registry
        .get(&pattern_graph_name)
        .map(|entry| entry.core.clone());
    drop(s);
    if let Some(p_core) = pattern_core {
        // Exponential-worst-case matching never runs under either
        // graph's lock: snapshot pattern then host SEQUENTIALLY (no
        // nested cross-graph locks), compute off-lock (KG-2.51).
        let p_core = read_authority.project_core(&p_core);
        let p_snap = p_core.analysis_snapshot();
        // vf2_subgraph_match snapshots the host internally, so the
        // NP-hard backtracking (bounded by max_results/max_steps) runs
        // entirely off-lock.
        let host = core.clone();
        match compute_off_lock(req_id, move || {
            host.vf2_subgraph_match(&p_snap, max_results, max_steps)
        })
        .await
        {
            Ok((matches, truncated)) => Response::ok(
                req_id,
                ResultPayload::of::<eg_types::result_contract::compute::Vf2SubgraphMatch>(
                    Vf2MatchResult { matches, truncated },
                ),
            ),
            Err(resp) => resp,
        }
    } else {
        Response::err(
            req_id,
            format!("Pattern graph '{}' not found", pattern_graph_name),
        )
    }
}

/// Handle subgraph extraction and forking.
pub(super) async fn try_handle_subgraph_reads(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::GetSubgraph { node_ids } => handle_get_subgraph(req_id, core, &node_ids),
        Method::Fork => handle_fork(req_id, core),
        other => return ControlFlow::Continue(other),
    })
}
