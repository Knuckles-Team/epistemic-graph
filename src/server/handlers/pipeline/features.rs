//! Feature construction shared by the pipeline train, evaluate, and predict paths.

use crate::graph::GraphCore;
use crate::protocol::GraphSource;
use eg_compute::graph_algos::AdjacencyGraph;
use eg_compute::graphlearn::embeddings::{
    fastrp, l2_normalize_rows, node2vec, to_f64_rows, FastRpConfig, Node2VecConfig,
};
use eg_types::wire::FeatureStep;

/// Build the `(node_id, row)` feature matrix by running the ordered feature steps.
/// Explicit `x` short-circuits the producing steps (only transforms apply).
pub(super) fn build_features(
    core: &GraphCore,
    source: &Option<GraphSource>,
    x: &[Vec<f64>],
    steps: &[FeatureStep],
) -> Result<(Vec<String>, Vec<Vec<f64>>), String> {
    if !x.is_empty() {
        let mut rows = x.to_vec();
        for step in steps {
            if matches!(step, FeatureStep::Normalize {}) {
                l2_normalize_rows(&mut rows);
            }
        }
        let ids: Vec<String> = (0..rows.len()).map(|i| i.to_string()).collect();
        return Ok((ids, rows));
    }
    let source = source
        .as_ref()
        .ok_or_else(|| "pipeline: no explicit `x` and no `source` to build features".to_string())?;
    build_source_features(core, source, steps)
}

fn build_source_features(
    core: &GraphCore,
    source: &GraphSource,
    steps: &[FeatureStep],
) -> Result<(Vec<String>, Vec<Vec<f64>>), String> {
    let mut ids: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<f64>> = Vec::new();
    let mut produced = false;
    for step in steps {
        match step {
            FeatureStep::Embedding { .. } => {
                let (graph, _) = super::super::graphlearn::build_graph_with_set(core, source);
                if graph.node_count() == 0 {
                    return Err("pipeline: source subgraph is empty".to_string());
                }
                rows = compute_embedding_rows(&graph, step);
                ids = graph.nodes().iter().map(|n| n.to_string()).collect();
                produced = true;
            }
            FeatureStep::NodeVector {} => {
                let (i, r) = gather_node_vectors(core, &source.node_label, source.limit);
                ids = i;
                rows = r;
                produced = true;
            }
            FeatureStep::Normalize {} => {
                if !produced {
                    return Err(
                        "pipeline: `normalize` before any producing feature step".to_string()
                    );
                }
                l2_normalize_rows(&mut rows);
            }
        }
    }
    if !produced {
        return Err(
            "pipeline: no producing feature step (embedding | node_vector) and no explicit `x`"
                .to_string(),
        );
    }
    Ok((ids, rows))
}

/// Compute structural-embedding rows (`f64`, index-ordered) for an `Embedding` step.
pub(super) fn compute_embedding_rows(
    graph: &AdjacencyGraph<String>,
    step: &FeatureStep,
) -> Vec<Vec<f64>> {
    let FeatureStep::Embedding {
        method,
        dim,
        iterations,
        walk_length,
        walks_per_node,
        window,
        epochs,
        seed,
    } = step
    else {
        return Vec::new();
    };
    match method.to_ascii_lowercase().as_str() {
        "node2vec" => {
            let cfg = Node2VecConfig {
                dim: *dim,
                walk_length: *walk_length,
                walks_per_node: *walks_per_node,
                window: *window,
                epochs: *epochs,
                seed: *seed,
                l2_normalize: false,
                ..Default::default()
            };
            to_f64_rows(&node2vec(graph, &cfg))
        }
        _ => {
            let cfg = FastRpConfig {
                dim: *dim,
                iterations: *iterations,
                seed: *seed,
                l2_normalize: false,
                ..Default::default()
            };
            to_f64_rows(&fastrp(graph, &cfg))
        }
    }
}

/// The first `Embedding` step (feeds the KAN embedding channel for graphlearn).
pub(super) fn first_embedding(steps: &[FeatureStep]) -> Option<&FeatureStep> {
    steps
        .iter()
        .find(|s| matches!(s, FeatureStep::Embedding { .. }))
}

/// Read each label node's pre-stored embedding from the SemanticStore (`NodeVector`).
fn gather_node_vectors(
    core: &GraphCore,
    node_label: &str,
    limit: usize,
) -> (Vec<String>, Vec<Vec<f64>>) {
    let owners = core.get_nodes_by_label(node_label, limit);
    let store = core.semantic_store.read();
    let mut ids = Vec::with_capacity(owners.len());
    let mut rows = Vec::with_capacity(owners.len());
    for (id, _) in owners {
        if let Some(vec) = store.get_embedding(&id) {
            rows.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(id);
        }
    }
    (ids, rows)
}
