//! Graph-learning ops (CONCEPT:EG-KG.graphlearn.link-predictor): a KAN link-predictor
//! learned over a graph-derived subgraph, with KG write-back of BOTH the predicted
//! links (`:PredictedEdge`) and — the differentiator — the learned per-feature edge
//! functions (`:EdgeFunction`), so the learned scoring curve is itself queryable.
//!
//! Like mining, this is GRAPH-SCOPED: `GraphLearnFit` reads the live subgraph off the
//! core (positives = observed edges, negatives = sampled non-edges) and write-back
//! materializes typed nodes into the same core. So it routes in `dispatch_graph_op`
//! with the graph core in hand, and its writeback WAL-replays by re-deriving from the
//! current graph state (deterministic given the seed).

// The Result router moves the large `Method` enum by value on the fall-through path;
// boxing the Err would allocate per non-graphlearn request (see mining.rs).
#![allow(clippy::result_large_err)]

use std::collections::HashSet;
use std::sync::Arc;

use eg_compute::graphlearn::edge_fn::Basis;
use eg_compute::graphlearn::link_predict::{
    self, FeatureCtx, KanLinkConfig, KanLinkModel,
};
use eg_compute::graph_algos::AdjacencyGraph;

use crate::graph::GraphCore;
use crate::protocol::{GraphLearnParams, GraphSource, Method, Response, ResultPayload};

/// Handle a `GraphLearn*` method. `Err(method)` hands a non-graphlearn method back to
/// the dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention.)
pub(crate) fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::GraphLearnFit {
            source,
            params,
            writeback,
        } => Ok(handle_fit(req_id, &core, source, params, writeback)),
        Method::GraphLearnPredict {
            model,
            source,
            candidate_pairs,
            top_k,
            writeback,
        } => Ok(handle_predict(
            req_id,
            &core,
            model,
            source,
            candidate_pairs,
            top_k,
            writeback,
        )),
        other => Err(other),
    }
}

/// Re-run a `GraphLearn*` writeback purely for its side effect on WAL replay
/// (CONCEPT:EG-KG.graphlearn.link-predictor). Deterministic given the seed; the
/// response is discarded.
#[allow(dead_code)]
pub(crate) fn replay(core: &GraphCore, method: &Method) {
    match method {
        Method::GraphLearnFit {
            source,
            params,
            writeback: true,
        } => {
            let _ = handle_fit(0, core, source.clone(), params.clone(), true);
        }
        Method::GraphLearnPredict {
            model,
            source,
            candidate_pairs,
            top_k,
            writeback: true,
        } => {
            let _ = handle_predict(
                0,
                core,
                model.clone(),
                source.clone(),
                candidate_pairs.clone(),
                *top_k,
                true,
            );
        }
        _ => {}
    }
}

// ─────────────────────────── Fit ───────────────────────────

fn handle_fit(
    req_id: u64,
    core: &GraphCore,
    source: GraphSource,
    params: GraphLearnParams,
    writeback: bool,
) -> Response {
    let (graph, positives) = build_graph(core, &source);
    if graph.node_count() < 2 {
        return Response::err(
            req_id,
            "graphlearn: subgraph has < 2 nodes (nothing to learn)".to_string(),
        );
    }
    if positives.is_empty() {
        return Response::err(
            req_id,
            "graphlearn: subgraph has no observed edges (no positives to fit)".to_string(),
        );
    }
    let config = to_config(&params);
    let ctx = FeatureCtx::build(&graph, config.alpha);
    let model = link_predict::fit_link_predictor(&ctx, &positives, &config);

    let model_json = serde_json::to_value(&model).unwrap_or(serde_json::Value::Null);

    let written = if writeback {
        materialize_edge_functions(core, &source.node_label, &model)
    } else {
        0
    };

    // Summarise the learned per-feature edge functions for the caller.
    let edge_fns: Vec<serde_json::Value> = edge_function_rows(&model);

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "model": model_json,
            "n_nodes": graph.node_count(),
            "n_edges": positives.len(),
            "train_auc": model.train_auc,
            "basis": basis_name(model.basis),
            "degree": model.degree,
            "edge_functions": edge_fns,
            "edge_functions_written": written,
        })),
    )
}

/// Materialize each learned edge function as a typed `:EdgeFunction` node
/// (CONCEPT:EG-KG.graphlearn.edge-function-writeback). Id is a deterministic digest of
/// `node_label` + feature + hidden-output index, so re-fitting the same subgraph
/// idempotently replaces the curve. The coefficients ARE the interpretable artifact.
fn materialize_edge_functions(core: &GraphCore, node_label: &str, model: &KanLinkModel) -> usize {
    let basis = basis_name(model.basis);
    // The first layer carries the per-INPUT-FEATURE edge functions (in the default
    // hidden=0 model there is exactly one output, so one fn per feature).
    let Some(layer0) = model.layers.first() else {
        return 0;
    };
    let mut written = 0usize;
    for (j, row) in layer0.fns.iter().enumerate() {
        for (i, f) in row.iter().enumerate() {
            let feature = model
                .feature_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("f{i}"));
            let node_id = edge_fn_node_id(node_label, &feature, j);
            let props = serde_json::json!({
                "type": "EdgeFunction",
                "node_label": node_label,
                "feature": feature,
                "hidden_output": j,
                "basis": basis,
                "degree": f.degree,
                "coefficients": f.coeffs,
                "train_auc": model.train_auc,
            });
            let Ok(blob) = rmp_serde::to_vec_named(&props) else {
                continue;
            };
            core.add_node(node_id, blob);
            written += 1;
        }
    }
    written
}

fn edge_function_rows(model: &KanLinkModel) -> Vec<serde_json::Value> {
    let basis = basis_name(model.basis);
    let mut rows = Vec::new();
    if let Some(layer0) = model.layers.first() {
        for (j, row) in layer0.fns.iter().enumerate() {
            for (i, f) in row.iter().enumerate() {
                let feature = model
                    .feature_names
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("f{i}"));
                rows.push(serde_json::json!({
                    "feature": feature,
                    "hidden_output": j,
                    "basis": basis,
                    "coefficients": f.coeffs,
                }));
            }
        }
    }
    rows
}

// ─────────────────────────── Predict ───────────────────────────

#[allow(clippy::too_many_arguments)]
fn handle_predict(
    req_id: u64,
    core: &GraphCore,
    model_json: serde_json::Value,
    source: GraphSource,
    candidate_pairs: Vec<(String, String)>,
    top_k: usize,
    writeback: bool,
) -> Response {
    let model: KanLinkModel = match serde_json::from_value(model_json) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("graphlearn: invalid model blob: {e}")),
    };
    let (graph, existing) = build_graph_with_set(core, &source);
    if graph.node_count() < 2 {
        return Response::err(req_id, "graphlearn: subgraph has < 2 nodes".to_string());
    }
    let ctx = FeatureCtx::build(&graph, model.alpha);

    // Resolve candidate pairs → compact indices, or enumerate top-k missing links.
    let scored: Vec<(usize, usize, f64)> = if candidate_pairs.is_empty() {
        link_predict::predict_missing_links(&model, &ctx, &existing, top_k)
    } else {
        let pairs: Vec<(usize, usize)> = candidate_pairs
            .iter()
            .filter_map(|(a, b)| Some((graph.index_of(a)?, graph.index_of(b)?)))
            .collect();
        let mut s = link_predict::predict_pairs(&model, &ctx, &pairs);
        if top_k > 0 && s.len() > top_k {
            s.truncate(top_k);
        }
        s
    };

    // Map indices back to node ids.
    let predicted: Vec<(String, String, f64)> = scored
        .iter()
        .map(|&(a, b, score)| {
            (
                graph.node_at(a).clone(),
                graph.node_at(b).clone(),
                score,
            )
        })
        .collect();

    let model_id = model_digest(&model);
    let written = if writeback {
        materialize_predicted_edges(core, &predicted, &model_id)
    } else {
        0
    };

    let rows: Vec<serde_json::Value> = predicted
        .iter()
        .map(|(s, d, score)| serde_json::json!({ "src": s, "dst": d, "score": score }))
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "predicted": rows,
            "n_predicted": predicted.len(),
            "model": model_id,
            "written_back": written,
        })),
    )
}

/// Materialize each scored pair as a typed `:PredictedEdge` node
/// (CONCEPT:EG-KG.graphlearn.predicted-edge-writeback), linked to its endpoints via
/// `PREDICTED_LINK` edges when they are resident. Id is a deterministic digest of the
/// endpoints + model id, so replay is idempotent.
fn materialize_predicted_edges(
    core: &GraphCore,
    predicted: &[(String, String, f64)],
    model_id: &str,
) -> usize {
    let mut written = 0usize;
    for (src, dst, score) in predicted {
        let node_id = predicted_edge_node_id(src, dst, model_id);
        let props = serde_json::json!({
            "type": "PredictedEdge",
            "src": src,
            "dst": dst,
            "score": score,
            "model": model_id,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for endpoint in [src, dst] {
            if core.has_node(endpoint) {
                let edge = serde_json::json!({ "relation": "PREDICTED_LINK" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), endpoint.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

// ─────────────────────────── Graph construction ───────────────────────────

/// Build the learning subgraph (an `AdjacencyGraph<String>` over the label's nodes,
/// isolated nodes included) + the observed positive edges as compact-index pairs.
fn build_graph(core: &GraphCore, source: &GraphSource) -> (AdjacencyGraph<String>, Vec<(usize, usize)>) {
    let (graph, edge_set) = build_graph_with_set(core, source);
    let positives: Vec<(usize, usize)> = edge_set.into_iter().collect();
    (graph, positives)
}

/// Build the subgraph AND the canonical `(min,max)`-index edge set (used by predict to
/// exclude already-existing links from the missing-link enumeration).
fn build_graph_with_set(
    core: &GraphCore,
    source: &GraphSource,
) -> (AdjacencyGraph<String>, HashSet<(usize, usize)>) {
    let owners = core.get_nodes_by_label(&source.node_label, source.limit);
    let id_set: HashSet<String> = owners.iter().map(|(id, _)| id.clone()).collect();

    let mut adjacency: Vec<(String, Vec<(String, f64)>)> = Vec::with_capacity(owners.len());
    let mut edge_ids: HashSet<(String, String)> = HashSet::new();
    for (owner, _blob) in &owners {
        let neighbors = neighbors_in_direction(core, owner, &source.direction);
        let mut nbrs: Vec<(String, f64)> = Vec::new();
        for nbr in neighbors {
            if !id_set.contains(&nbr) || &nbr == owner {
                continue; // only intra-label, non-self links
            }
            if let Some(rel) = &source.relation {
                if !edge_matches_relation(core, owner, &nbr, &source.direction, rel) {
                    continue;
                }
            }
            nbrs.push((nbr.clone(), 1.0));
            let key = if owner < &nbr {
                (owner.clone(), nbr.clone())
            } else {
                (nbr.clone(), owner.clone())
            };
            edge_ids.insert(key);
        }
        adjacency.push((owner.clone(), nbrs));
    }
    let graph = AdjacencyGraph::from_adjacency(adjacency);
    let edge_set: HashSet<(usize, usize)> = edge_ids
        .iter()
        .filter_map(|(a, b)| {
            let (ia, ib) = (graph.index_of(a)?, graph.index_of(b)?);
            Some(if ia < ib { (ia, ib) } else { (ib, ia) })
        })
        .collect();
    (graph, edge_set)
}

fn neighbors_in_direction(core: &GraphCore, node_id: &str, direction: &str) -> Vec<String> {
    match direction {
        "out" => core.get_successors(node_id).unwrap_or_default(),
        "in" => core.get_predecessors(node_id).unwrap_or_default(),
        // "any" (default) and anything else.
        _ => {
            let mut v = core.get_successors(node_id).unwrap_or_default();
            v.extend(core.get_predecessors(node_id).unwrap_or_default());
            v.sort_unstable();
            v.dedup();
            v
        }
    }
}

fn edge_matches_relation(
    core: &GraphCore,
    owner: &str,
    neighbor: &str,
    direction: &str,
    relation: &str,
) -> bool {
    let pairs: &[(&str, &str)] = match direction {
        "in" => &[(neighbor, owner)],
        "out" => &[(owner, neighbor)],
        _ => &[(owner, neighbor), (neighbor, owner)],
    };
    for &(s, t) in pairs {
        for blob in core.get_edge_properties(s, t) {
            if let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(&blob) {
                for key in ["relation", "type", "rel"] {
                    if val.get(key).and_then(|v| v.as_str()) == Some(relation) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

// ─────────────────────────── Helpers ───────────────────────────

fn to_config(params: &GraphLearnParams) -> KanLinkConfig {
    KanLinkConfig {
        basis: parse_basis(&params.basis),
        degree: params.degree.max(1),
        hidden: params.hidden,
        epochs: params.epochs.max(1),
        lr: if params.lr > 0.0 { params.lr } else { 0.05 },
        neg_ratio: if params.neg_ratio > 0.0 {
            params.neg_ratio
        } else {
            1.0
        },
        seed: params.seed,
        alpha: params.alpha.clamp(0.0, 1.0),
    }
}

fn parse_basis(s: &str) -> Basis {
    match s.to_ascii_lowercase().as_str() {
        "jacobi" => Basis::Jacobi,
        _ => Basis::Chebyshev,
    }
}

fn basis_name(b: Basis) -> &'static str {
    match b {
        Basis::Chebyshev => "chebyshev",
        Basis::Jacobi => "jacobi",
    }
}

fn edge_fn_node_id(node_label: &str, feature: &str, hidden: usize) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(node_label.as_bytes());
    hasher.update([0u8]);
    hasher.update(feature.as_bytes());
    hasher.update([0u8]);
    hasher.update(hidden.to_le_bytes());
    format!("edgefn:{}", hex::encode(&hasher.finalize()[..12]))
}

fn predicted_edge_node_id(src: &str, dst: &str, model_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(model_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(src.as_bytes());
    hasher.update([0u8]);
    hasher.update(dst.as_bytes());
    format!("prededge:{}", hex::encode(&hasher.finalize()[..12]))
}

/// A short content digest of a fitted model (for `:PredictedEdge.model` provenance).
fn model_digest(model: &KanLinkModel) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(model).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    format!("kanlink:{}", hex::encode(&hasher.finalize()[..8]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    /// A small planted graph: two dense triangles (community A: a0..a2, community B:
    /// b0..b2) with heavy intra-community linking, so intra pairs share neighbours.
    fn seed_community_graph(core: &Arc<GraphCore>) {
        for id in ["a0", "a1", "a2", "a3", "b0", "b1", "b2", "b3"] {
            core.add_node(id.into(), node(serde_json::json!({ "type": "Person" })));
        }
        let edges = [
            ("a0", "a1"), ("a0", "a2"), ("a1", "a2"), ("a2", "a3"), ("a1", "a3"),
            ("b0", "b1"), ("b0", "b2"), ("b1", "b2"), ("b2", "b3"), ("b1", "b3"),
            ("a0", "b0"), // one weak cross link
        ];
        for (s, d) in edges {
            let _ = core.add_edge(s.into(), d.into(), node(serde_json::json!({ "relation": "KNOWS" })));
        }
        core.mark_dirty();
    }

    #[test]
    fn non_graphlearn_method_falls_through() {
        let core = Arc::new(GraphCore::new());
        let m = Method::NodeCount;
        assert!(matches!(try_handle(1, core, m), Err(Method::NodeCount)));
    }

    #[test]
    fn fit_learns_and_writes_edge_functions() {
        let core = Arc::new(GraphCore::new());
        seed_community_graph(&core);
        let m = Method::GraphLearnFit {
            source: GraphSource {
                node_label: "Person".into(),
                direction: "any".into(),
                relation: None,
                limit: 0,
            },
            params: GraphLearnParams::default(),
            writeback: true,
        };
        let resp = try_handle(1, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_nodes"], 8);
        assert!(v["n_edges"].as_u64().unwrap() >= 10);
        assert!(v["train_auc"].as_f64().unwrap() > 0.7, "train_auc={}", v["train_auc"]);
        let written = v["edge_functions_written"].as_u64().unwrap();
        assert_eq!(written, 7); // one per feature (default hidden=0)
        // The learned edge functions are now queryable :EdgeFunction nodes.
        core.mark_dirty();
        let ef_nodes = core.get_nodes_by_label("EdgeFunction", 0);
        assert_eq!(ef_nodes.len() as u64, written);
    }

    #[test]
    fn fit_then_predict_scores_and_writes_edges() {
        let core = Arc::new(GraphCore::new());
        seed_community_graph(&core);
        let source = GraphSource {
            node_label: "Person".into(),
            direction: "any".into(),
            relation: None,
            limit: 0,
        };
        let fit = Method::GraphLearnFit {
            source: source.clone(),
            params: GraphLearnParams::default(),
            writeback: false,
        };
        let resp = try_handle(1, Arc::clone(&core), fit).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        let model = v["model"].clone();

        let predict = Method::GraphLearnPredict {
            model,
            source,
            candidate_pairs: vec![],
            top_k: 5,
            writeback: true,
        };
        let resp = try_handle(2, Arc::clone(&core), predict).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        let n = v["n_predicted"].as_u64().unwrap();
        assert!(n >= 1 && n <= 5);
        assert_eq!(v["written_back"].as_u64().unwrap(), n);
        // Each prediction has a score in [0,1].
        for row in v["predicted"].as_array().unwrap() {
            let s = row["score"].as_f64().unwrap();
            assert!((0.0..=1.0).contains(&s));
        }
        core.mark_dirty();
        let pe = core.get_nodes_by_label("PredictedEdge", 0);
        assert_eq!(pe.len() as u64, n);
    }
}
