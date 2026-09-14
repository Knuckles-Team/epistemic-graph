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
        nodes.push(eg_types::types::SubgraphNode {
            id: id.clone(),
            properties: props,
        });
    }
    let mut edges = Vec::new();
    for ((src, tgt), blobs) in &sub.edge_properties {
        for blob in blobs {
            let props =
                eg_types::msgpack::decode_property_value(blob).unwrap_or(serde_json::Value::Null);
            edges.push(eg_types::types::SubgraphEdge {
                source: src.clone(),
                target: tgt.clone(),
                properties: props,
            });
        }
    }
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::graph::GetSubgraph>(
            eg_types::types::SubgraphResult { nodes, edges },
        ),
    )
}

fn prepare_fork_value(snapshot: &crate::graph::GraphSnapshot) -> Result<serde_json::Value, String> {
    let mut value = snapshot.to_msgpack().and_then(|bytes| {
        rmp_serde::from_slice::<serde_json::Value>(&bytes).map_err(|e| e.to_string())
    })?;
    let nodes = decode_fork_nodes(&snapshot.nodes)?;
    let edges = decode_fork_edges(&snapshot.edges)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "fork snapshot did not encode as a JSON object".to_string())?;
    object.insert("nodes".to_string(), serde_json::Value::Array(nodes));
    object.insert("edges".to_string(), serde_json::Value::Array(edges));
    Ok(value)
}

/// `Fork`: pure extract-method from `try_handle`'s match arm, byte-identical
/// behaviour, no signature change.
fn handle_fork(req_id: u64, core: &Arc<GraphCore>) -> Response {
    // Cannot return the forked GraphCore directly because it needs to be registered.
    // A true fork method in the registry might be better.
    // Return the snapshot's established JSON shape, replacing its internal property blobs
    // with the caller-authored property values promised by the Fork result contract.
    let snapshot = (**core).fork().snapshot();
    let value = match prepare_fork_value(&snapshot) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };

    Response::ok(
        req_id,
        ResultPayload::of_dynamic::<eg_types::result_contract::graph::Fork, _>(&value),
    )
}

fn decode_fork_property(blob: &[u8], subject: &str) -> Result<serde_json::Value, String> {
    eg_types::msgpack::decode_property_value(blob)
        .map_err(|_| format!("fork {subject} has an invalid property blob"))
}

fn decode_fork_nodes(nodes: &[(String, Arc<Vec<u8>>)]) -> Result<Vec<serde_json::Value>, String> {
    nodes
        .iter()
        .map(|(id, blob)| {
            decode_fork_property(blob, &format!("node '{id}'")).map(|properties| {
                serde_json::Value::Array(vec![serde_json::Value::String(id.clone()), properties])
            })
        })
        .collect()
}

fn decode_fork_edges(
    edges: &[(String, String, Arc<Vec<u8>>)],
) -> Result<Vec<serde_json::Value>, String> {
    edges
        .iter()
        .map(|(source, target, blob)| {
            decode_fork_property(blob, &format!("edge '{source}' -> '{target}'")).map(
                |properties| {
                    serde_json::Value::Array(vec![
                        serde_json::Value::String(source.clone()),
                        serde_json::Value::String(target.clone()),
                        properties,
                    ])
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn fork_snapshot_projects_decoded_nested_properties() {
        use crate::graph::GraphCore;
        use crate::protocol::ResultPayload;
        use std::sync::Arc;

        let core = Arc::new(GraphCore::new());
        core.add_node(
            "n1".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "Doc",
                "metadata": {"priority": 7, "tags": ["a", "b"]}
            }))
            .expect("encode test properties"),
        );
        core.add_node(
            "n2".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"type": "Folder"}))
                .expect("encode test properties"),
        );
        core.add_edge(
            "n1".to_string(),
            "n2".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "relationship": "CONTAINS",
                "metadata": {"weight": 0.75}
            }))
            .expect("encode test edge properties"),
        )
        .expect("add test edge");

        let response = super::handle_fork(1, &core);
        assert!(
            response.error.is_none(),
            "fork failed: {:?}",
            response.error
        );
        let ResultPayload::Json(snapshot) = response.result.expect("fork result") else {
            panic!("fork result was not JSON");
        };
        let object = snapshot.as_object().expect("fork snapshot object");
        let node = object["nodes"]
            .as_array()
            .expect("fork nodes")
            .iter()
            .find(|row| row[0] == "n1")
            .expect("n1 row");
        assert_eq!(
            node[1],
            serde_json::json!({
                "type": "Doc",
                "metadata": {"priority": 7, "tags": ["a", "b"]}
            })
        );
        let edge = object["edges"]
            .as_array()
            .expect("fork edges")
            .iter()
            .find(|row| row[0] == "n1" && row[1] == "n2")
            .expect("n1 -> n2 row");
        assert_eq!(
            edge[2],
            serde_json::json!({
                "relationship": "CONTAINS",
                "metadata": {"weight": 0.75}
            })
        );
    }

    #[test]
    fn fork_snapshot_rejects_malformed_property_blob() {
        use crate::graph::GraphCore;
        use std::sync::Arc;

        let core = Arc::new(GraphCore::new());
        core.add_node("bad".to_string(), vec![0xc1]);

        let response = super::handle_fork(2, &core);

        assert!(response.result.is_none());
        assert_eq!(
            response.error.as_deref(),
            Some("fork node 'bad' has an invalid property blob")
        );
    }

    #[test]
    fn fork_snapshot_rejects_malformed_edge_property_blob() {
        use crate::graph::GraphCore;
        use std::sync::Arc;

        let core = Arc::new(GraphCore::new());
        let properties = rmp_serde::to_vec_named(&serde_json::json!({"type": "Node"}))
            .expect("encode test properties");
        core.add_node("n1".to_string(), properties.clone());
        core.add_node("n2".to_string(), properties);
        core.add_edge("n1".to_string(), "n2".to_string(), vec![0xc1])
            .expect("add test edge");

        let response = super::handle_fork(3, &core);

        assert!(response.result.is_none());
        assert_eq!(
            response.error.as_deref(),
            Some("fork edge 'n1' -> 'n2' has an invalid property blob")
        );
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
                    eg_types::compute_result::algorithms::Vf2MatchResult { matches, truncated },
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
