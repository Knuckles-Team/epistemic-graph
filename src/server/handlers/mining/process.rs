use super::writeback::*;
use super::*;
use eg_compute::mining::{causal_impact, process_mining, root_cause};
use eg_types::compute_result::mining::{
    CausalImpactMiningResult, CausalRelationRow, DirectlyFollowsRow, ParallelRelationRow,
    ProcessMiningResult,
};
use eg_types::result_contract::compute as results;

// ─────────────────────────── Causal impact (ITS / DiD) ───────────────────────────

pub(in crate::server::handlers) fn handle_causal_impact(
    req_id: u64,
    core: &GraphCore,
    series: Vec<f64>,
    control: Vec<f64>,
    intervention_index: usize,
    series_id: String,
    writeback: WritebackOptions,
) -> Response {
    if series.is_empty() {
        return Response::err(
            req_id,
            "mining: causal_impact requires a non-empty `series`",
        );
    }
    let effect = if control.is_empty() {
        causal_impact::interrupted_time_series(&series, intervention_index)
    } else {
        causal_impact::diff_in_diff(&series, &control, intervention_index)
    };
    let written = if writeback.enabled {
        materialize_causal_effect(core, &effect, &series_id, &series, &control)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_causal_effect_claim(core, &effect, &series_id, &series, &control);
    }
    Response::ok(
        req_id,
        ResultPayload::of::<results::MineCausalImpact>(CausalImpactMiningResult {
            pre_mean: effect.pre_mean,
            post_mean: effect.post_mean,
            effect_size: effect.effect_size,
            relative_effect: effect.relative_effect,
            std_error: effect.std_error,
            confidence: effect.confidence,
            method: causal_method_name(&control).to_string(),
            written_back: written,
        }),
    )
}

/// `its` (interrupted time series) without a control series, else `did`
/// (difference in differences).
fn causal_method_name(control: &[f64]) -> &'static str {
    if control.is_empty() {
        "its"
    } else {
        "did"
    }
}

/// Materialize the estimate as a typed `:CausalEffect` node (CONCEPT:EG-KG.mining.causal-impact),
/// id = a deterministic digest of `method` + `series_id` (or the input series
/// when empty, so identical explicit input reproduces the same id on WAL replay).
/// Linked to a resident node named `series_id` via a `CAUSAL_EFFECT_OF` edge
/// when one exists.
pub(super) fn materialize_causal_effect(
    core: &GraphCore,
    effect: &causal_impact::CausalEffect,
    series_id: &str,
    series: &[f64],
    control: &[f64],
) -> usize {
    let method = if control.is_empty() { "its" } else { "did" };
    let node_id = causal_effect_node_id(method, series_id, series, control);
    let props = serde_json::json!({
        "type": "CausalEffect",
        "method": method,
        "pre_mean": effect.pre_mean,
        "post_mean": effect.post_mean,
        "effect_size": effect.effect_size,
        "relative_effect": effect.relative_effect,
        "std_error": effect.std_error,
        "confidence": effect.confidence,
        "series_id": series_id,
    });
    if !writeback_node(core, &node_id, &props) {
        return 0;
    }
    if !series_id.is_empty() && core.has_node(series_id) {
        writeback_relationship(core, &node_id, series_id, "CAUSAL_EFFECT_OF");
    }
    1
}

pub(super) fn causal_effect_node_id(
    method: &str,
    series_id: &str,
    series: &[f64],
    control: &[f64],
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(method.as_bytes());
    hasher.update([0u8]);
    hasher.update(series_id.as_bytes());
    hasher.update([0u8]);
    for v in series {
        hasher.update(v.to_le_bytes());
    }
    hasher.update([0u8]);
    for v in control {
        hasher.update(v.to_le_bytes());
    }
    format!("causal_effect:{}", hex::encode(&hasher.finalize()[..12]))
}

#[cfg(feature = "epistemic")]
pub(super) fn materialize_causal_effect_claim(
    core: &GraphCore,
    effect: &causal_impact::CausalEffect,
    series_id: &str,
    series: &[f64],
    control: &[f64],
) {
    let method = if control.is_empty() { "its" } else { "did" };
    let node_id = causal_effect_node_id(method, series_id, series, control);
    let provenance = if series_id.is_empty() {
        "series:values".to_string()
    } else {
        format!("series:{series_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "causal_impact",
        effect.confidence.clamp(0.0, 1.0),
        &provenance,
    );
}

// ─────────────────────────── Process mining ───────────────────────────

pub(in crate::server::handlers) fn handle_process(
    req_id: u64,
    core: &GraphCore,
    traces: Vec<Vec<String>>,
    process_id: String,
    writeback: WritebackOptions,
) -> Response {
    if traces.is_empty() {
        return Response::err(req_id, "mining: process mining requires non-empty `traces`");
    }
    let (labels, model) = process_mining::alpha_lite_labeled(&traces);
    let written = if writeback.enabled {
        materialize_process_model(core, &model, &labels, &process_id)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_process_model_claim(core, &model, &labels, &process_id);
    }
    let label_of = |i: process_mining::ActivityId| labels[i as usize].clone();
    let dfg: Vec<DirectlyFollowsRow> = model
        .dfg_edges
        .iter()
        .map(|&(a, b, count)| DirectlyFollowsRow {
            from: label_of(a),
            to: label_of(b),
            count,
        })
        .collect();
    let causal: Vec<CausalRelationRow> = model
        .causal
        .iter()
        .map(|&(a, b)| CausalRelationRow {
            from: label_of(a),
            to: label_of(b),
        })
        .collect();
    let parallel: Vec<ParallelRelationRow> = model
        .parallel
        .iter()
        .map(|&(a, b)| ParallelRelationRow {
            a: label_of(a),
            b: label_of(b),
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<results::MineProcess>(ProcessMiningResult {
            dfg,
            causal,
            parallel,
            start_activities: model
                .start_activities
                .iter()
                .map(|&i| label_of(i))
                .collect(),
            end_activities: model.end_activities.iter().map(|&i| label_of(i)).collect(),
            n_traces: traces.len(),
            n_activities: model.n_activities,
            written_back: written,
        }),
    )
}

/// Materialize the footprint as a typed `:ProcessModel` node (CONCEPT:EG-KG.mining.process-mining),
/// linked to every activity that IS a resident node via `PROCESS_MEMBER` edges.
pub(super) fn materialize_process_model(
    core: &GraphCore,
    model: &process_mining::ProcessModel,
    labels: &[String],
    process_id: &str,
) -> usize {
    let node_id = process_model_node_id(process_id, labels, model);
    let causal: Vec<serde_json::Value> = model
        .causal
        .iter()
        .map(|&(a, b)| serde_json::json!({ "from": labels[a as usize], "to": labels[b as usize] }))
        .collect();
    let parallel: Vec<serde_json::Value> = model
        .parallel
        .iter()
        .map(|&(a, b)| serde_json::json!({ "a": labels[a as usize], "b": labels[b as usize] }))
        .collect();
    let props = serde_json::json!({
        "type": "ProcessModel",
        "activities": labels,
        "causal": causal,
        "parallel": parallel,
        "start_activities": model.start_activities.iter().map(|&i| labels[i as usize].clone()).collect::<Vec<_>>(),
        "end_activities": model.end_activities.iter().map(|&i| labels[i as usize].clone()).collect::<Vec<_>>(),
        "process_id": process_id,
    });
    if !writeback_node(core, &node_id, &props) {
        return 0;
    }
    for label in labels {
        if core.has_node(label) {
            writeback_relationship(core, &node_id, label, "PROCESS_MEMBER");
        }
    }
    1
}

pub(super) fn process_model_node_id(
    process_id: &str,
    labels: &[String],
    model: &process_mining::ProcessModel,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(process_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(labels.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    for &(a, b, c) in &model.dfg_edges {
        hasher.update(a.to_le_bytes());
        hasher.update(b.to_le_bytes());
        hasher.update((c as u64).to_le_bytes());
    }
    format!("process_model:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = FOOTPRINT COVERAGE: the fraction of all possible activity pairs
/// (`n choose 2`) that the log actually observed directly-following (causal OR
/// parallel) at least once — already `[0,1]`, `0.0` when fewer than 2 activities.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_process_model_claim(
    core: &GraphCore,
    model: &process_mining::ProcessModel,
    labels: &[String],
    process_id: &str,
) {
    let node_id = process_model_node_id(process_id, labels, model);
    let n = model.n_activities;
    let total_possible = if n >= 2 {
        (n * (n - 1) / 2) as f64
    } else {
        0.0
    };
    let observed = (model.causal.len() + model.parallel.len()) as f64;
    let confidence = if total_possible > 0.0 {
        (observed / total_possible).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let provenance = if process_id.is_empty() {
        "process:traces".to_string()
    } else {
        format!("process:{process_id}")
    };
    materialize_claim(core, &node_id, "process_mining", confidence, &provenance);
}

// ─────────────────────────── Root-cause propagation ───────────────────────────

/// Index every distinct `edges` endpoint against `nodes` (positional ids),
/// dropping any edge referencing an id NOT present in `nodes`.
pub(super) fn edge_indices(
    nodes: &[String],
    edges: &[(String, String, f64)],
) -> Vec<(usize, usize, f64)> {
    let index: std::collections::HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    edges
        .iter()
        .filter_map(|(a, b, w)| Some((*index.get(a.as_str())?, *index.get(b.as_str())?, *w)))
        .collect()
}

pub(super) fn run_root_cause(
    nodes: &[String],
    scores: &[f64],
    edges: &[(String, String, f64)],
    symptom: &str,
    max_hops: usize,
    decay: f64,
) -> Option<root_cause::RootCauseResult> {
    let symptom_idx = nodes.iter().position(|n| n == symptom)?;
    let idx_edges = edge_indices(nodes, edges);
    Some(root_cause::find_root_cause(
        nodes.len(),
        &idx_edges,
        scores,
        symptom_idx,
        max_hops,
        decay,
    ))
}
