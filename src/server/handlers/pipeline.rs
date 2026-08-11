//! ML pipeline handler (CONCEPT:EG-KG.mining.ml-pipeline): a composable
//! train→eval→serve→predict lifecycle over a versioned `:Model` artifact that
//! GENERALIZES the KAN one-off. A `PipelineSpec` is `feature steps → split → a
//! pluggable model family`, where the family is one of the primitives that ALREADY
//! live in `eg-compute` — this handler only COMPOSES them:
//!
//!   * `classify`   → `eg_compute::mining::classify` (node classification — Gaussian/
//!     Multinomial NB, k-NN, one-vs-rest logistic / linear-SVC).
//!   * `estimator`  → `eg_compute::datascience::estimators` (ridge/lasso/elasticnet/
//!     tree/forest/boosting/adaboost/svr regression).
//!   * `graphlearn` → `eg_compute::graphlearn::link_predict` (the KAN link-predictor —
//!     the one-off, now just one registered family behind the same lifecycle).
//!
//! Feature steps compose the structural embedders (`fastrp`/`node2vec`) and stored
//! node vectors; metrics come from `eg_compute::datascience::metrics` (+ the KAN's own
//! `auc`). The fitted model is persisted as a versioned `:Model` node (`model:<name>:
//! v<n>`), so two versions are queryable and comparable; `serve` writes a
//! `:ServedModel` pointer so predict-by-name resolves the deployed version.
//!
//! GRAPH-SCOPED like mining/graphlearn — the feature steps read the live subgraph and
//! the `:Model`/`:ServedModel`/`:Prediction` write-backs materialize into the same
//! core. `Train`/`Serve`/`Predict` are RUNTIME-CONDITIONAL writes routed through
//! `graph_ops::try_handle_gateway` → `commit_conditional_mutation`; `Evaluate`/`Compare`
//! are read-only and route through this module's `try_handle`.

// The Result router moves the large `Method` enum by value on the fall-through path;
// boxing the Err would allocate per non-pipeline request (see mining.rs / graphlearn.rs).
#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use eg_compute::datascience::primitives::train_test_split;
use eg_compute::datascience::{estimators, metrics};
use eg_compute::graph_algos::AdjacencyGraph;
use eg_compute::graphlearn::edge_fn::Basis;
use eg_compute::graphlearn::embeddings::{
    fastrp, node2vec, to_f64_rows, FastRpConfig, Node2VecConfig,
};
use eg_compute::graphlearn::link_predict::{self, FeatureCtx, KanLinkConfig, KanLinkModel};
use eg_compute::mining::classify;
use serde_json::{json, Value};

use crate::graph::GraphCore;
use crate::protocol::{GraphSource, Method, Response, ResultPayload};
use eg_types::wire::{
    EstimatorParams, FeatureStep, FittedClassifier, FittedModel, ModelSpec, PipelineSpec,
};

/// Handle a `MiningPipeline*` READ method (`Evaluate`/`Compare`). `Err(method)` hands a
/// non-pipeline method back to the dispatcher. `Train`/`Serve`/`Predict` are
/// `GATEWAY_ROUTED`; `dispatch_graph_op` routes them through `try_handle_gateway`
/// BEFORE this fallback (the `unreachable!()` arms are the structural proof, exactly
/// like graphlearn's).
///
/// BUG-034: both arms read the graph through label-index scans (`Evaluate`'s
/// `source`-driven feature build, `Compare`'s loaded `:Model` metrics) that must
/// see only the caller's RLS-visible rows — same requirement `mining::try_handle`
/// already enforces for its one non-gateway-routed read, `MineClassifyFit`
/// (`authority.project_core(&core)` before the first primitive touches the
/// graph). Before this fix `core` here was the raw, unfiltered live core: an
/// `Evaluate`/`Compare` caller with graph-level Read access got `n`/metrics
/// computed over EVERY node carrying the source label, including rows a
/// non-grantee cannot see — an existence/count side channel identical in kind
/// to the one `GraphReadAuthority::project_core`'s own doc names.
pub(crate) fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    read_authority: Option<&crate::server::access::GraphReadAuthority>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::MiningPipelineEvaluate {
            name,
            version,
            source,
            x,
            y,
        } => {
            let authority = read_authority
                .expect("MiningPipelineEvaluate must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_evaluate(req_id, &core, name, version, source, x, y))
        }
        Method::MiningPipelineCompare {
            name,
            version_a,
            version_b,
        } => {
            let authority = read_authority
                .expect("MiningPipelineCompare must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_compare(req_id, &core, name, version_a, version_b))
        }
        Method::MiningPipelineTrain { .. } => unreachable!(
            "MiningPipelineTrain is GATEWAY_ROUTED; dispatch_graph_op routes it through \
             try_handle_gateway before this fallback"
        ),
        Method::MiningPipelineServe { .. } => unreachable!(
            "MiningPipelineServe is GATEWAY_ROUTED; dispatch_graph_op routes it through \
             try_handle_gateway before this fallback"
        ),
        Method::MiningPipelinePredict { .. } => unreachable!(
            "MiningPipelinePredict is GATEWAY_ROUTED; dispatch_graph_op routes it through \
             try_handle_gateway before this fallback"
        ),
        other => Err(other),
    }
}

// ─────────────────────────── Train ───────────────────────────

/// Fit the pipeline's model over composed features, evaluate on a held-out split,
/// and (when `writeback`) persist a versioned `:Model` artifact. `pub(crate)` so the
/// graph-ops gateway drives it under `commit_conditional_mutation` (mirrors
/// `graphlearn::handle_fit`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_train(
    req_id: u64,
    core: &GraphCore,
    name: String,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    y: Vec<i64>,
    spec: PipelineSpec,
    writeback: bool,
) -> Response {
    let family = normalize_family(&spec.model.family);
    match family.as_str() {
        "graphlearn" => handle_train_graphlearn(req_id, core, &name, source, &spec, writeback),
        "classify" | "estimator" => {
            handle_train_tabular(req_id, core, &name, source, x, y, &spec, &family, writeback)
        }
        other => Response::err(
            req_id,
            format!("pipeline: unknown model family {other:?} (classify | estimator | graphlearn)"),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_train_tabular(
    req_id: u64,
    core: &GraphCore,
    name: &str,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    y: Vec<i64>,
    spec: &PipelineSpec,
    family: &str,
    writeback: bool,
) -> Response {
    let (ids, rows) = match build_features(core, &source, &x, &spec.features) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if rows.is_empty() {
        return Response::err(req_id, "pipeline: no feature rows produced".to_string());
    }
    let labels = match resolve_labels_f64(core, &ids, &y, &spec.label_property) {
        Ok(l) => l,
        Err(e) => return Response::err(req_id, e),
    };
    if labels.len() != rows.len() {
        return Response::err(
            req_id,
            format!(
                "pipeline: {} feature rows but {} labels",
                rows.len(),
                labels.len()
            ),
        );
    }
    let n_features = rows[0].len();
    // Compose the deterministic seeded split.
    let (x_train, x_test, y_train_f, y_test_f) = train_test_split(
        &rows,
        &labels,
        spec.split.test_ratio,
        spec.split.shuffle,
        spec.split.seed,
    );
    if x_train.is_empty() {
        return Response::err(
            req_id,
            "pipeline: empty training split (raise sample count or lower test_ratio)".to_string(),
        );
    }

    let (blob, classes, train_metrics, test_metrics) = match family {
        "classify" => {
            let y_train = to_i64_round(&y_train_f);
            let y_test = to_i64_round(&y_test_f);
            let algo = match classify_algo_from_spec(&spec.model) {
                Ok(a) => a,
                Err(e) => return Response::err(req_id, e),
            };
            let model = match classify::fit(&x_train, &y_train, algo) {
                Ok(m) => m,
                Err(e) => return Response::err(req_id, format!("pipeline: classify fit: {e}")),
            };
            let train_pred = classify::predict(&model, &x_train).labels;
            let test_pred = classify::predict(&model, &x_test).labels;
            let train_m = json!({
                "accuracy": metrics::accuracy(&y_train, &train_pred),
                "macro_f1": metrics::macro_f1(&y_train, &train_pred),
            });
            let test_m = json!({
                "accuracy": metrics::accuracy(&y_test, &test_pred),
                "macro_f1": metrics::macro_f1(&y_test, &test_pred),
            });
            let classes = classify_classes(&model);
            let blob = serde_json::to_value(&model).unwrap_or(Value::Null);
            (blob, Value::from(classes), train_m, test_m)
        }
        _ => {
            // estimator (regression)
            let (est_name, params) = estimator_from_spec(&spec.model);
            let model = match estimators::fit_estimator(&est_name, &x_train, &y_train_f, &params) {
                Ok(m) => m,
                Err(e) => return Response::err(req_id, format!("pipeline: estimator fit: {e}")),
            };
            let train_pred = estimators::predict(&model, &x_train);
            let test_pred = estimators::predict(&model, &x_test);
            let train_m = json!({
                "r2": metrics::r2(&y_train_f, &train_pred),
                "rmse": metrics::rmse(&y_train_f, &train_pred),
            });
            let test_m = json!({
                "r2": metrics::r2(&y_test_f, &test_pred),
                "rmse": metrics::rmse(&y_test_f, &test_pred),
            });
            let blob = serde_json::to_value(&model).unwrap_or(Value::Null);
            (blob, Value::Null, train_m, test_m)
        }
    };

    let metrics_obj = json!({ "train": train_metrics, "test": test_metrics });
    finish_train(
        req_id,
        core,
        name,
        family,
        &spec.model.algorithm,
        spec,
        blob,
        classes,
        metrics_obj,
        n_features,
        x_train.len(),
        x_test.len(),
        writeback,
    )
}

fn handle_train_graphlearn(
    req_id: u64,
    core: &GraphCore,
    name: &str,
    source: Option<GraphSource>,
    spec: &PipelineSpec,
    writeback: bool,
) -> Response {
    let Some(source) = source else {
        return Response::err(
            req_id,
            "pipeline: graphlearn family requires a `source` (the learning subgraph)".to_string(),
        );
    };
    // Reuse graphlearn's own subgraph builder (label vertices + observed intra-label
    // edges as positives) — one builder, no drift.
    let (graph, edge_set) = super::graphlearn::build_graph_with_set(core, &source);
    if graph.node_count() < 2 {
        return Response::err(
            req_id,
            "pipeline: graphlearn subgraph has < 2 nodes".to_string(),
        );
    }
    let positives: Vec<(usize, usize)> = edge_set.into_iter().collect();
    if positives.is_empty() {
        return Response::err(
            req_id,
            "pipeline: graphlearn subgraph has no observed edges (no positives)".to_string(),
        );
    }
    let config = kan_config_from_spec(&spec.model);
    // A structural-embedding feature step feeds the KAN's embedding channel; otherwise
    // the 7 pure-structural features.
    let ctx = match first_embedding(&spec.features) {
        Some(step) => {
            let rows = compute_embedding_rows(&graph, step);
            FeatureCtx::build_with_embeddings(&graph, config.alpha, rows)
        }
        None => FeatureCtx::build(&graph, config.alpha),
    };
    let model = link_predict::fit_link_predictor(&ctx, &positives, &config);
    let n_features = model.feature_names.len();
    let metrics_obj = json!({ "train": { "auc": model.train_auc } });
    let blob = serde_json::to_value(&model).unwrap_or(Value::Null);
    finish_train(
        req_id,
        core,
        name,
        "graphlearn",
        &spec.model.algorithm,
        spec,
        blob,
        Value::Null,
        metrics_obj,
        n_features,
        positives.len(),
        0,
        writeback,
    )
}

/// Version + (optionally) persist the fitted model as a `:Model` node, then respond.
#[allow(clippy::too_many_arguments)]
fn finish_train(
    req_id: u64,
    core: &GraphCore,
    name: &str,
    family: &str,
    algorithm: &str,
    spec: &PipelineSpec,
    blob: Value,
    classes: Value,
    metrics_obj: Value,
    n_features: usize,
    n_train: usize,
    n_test: usize,
    writeback: bool,
) -> Response {
    let version = if writeback {
        next_version(core, name)
    } else {
        0
    };
    let model_id = model_node_id(name, version);
    let feature_spec = serde_json::to_value(&spec.features).unwrap_or(Value::Null);
    if writeback {
        let props = json!({
            "type": "Model",
            "name": name,
            "version": version,
            "family": family,
            "algorithm": algorithm,
            "metrics": metrics_obj,
            "classes": classes,
            "n_features": n_features,
            "n_train": n_train,
            "n_test": n_test,
            "feature_spec": feature_spec,
            "label_property": spec.label_property,
            "blob": blob,
            "created_ts": now_secs(),
        });
        match rmp_serde::to_vec_named(&props) {
            Ok(b) => {
                core.add_node(model_id.clone(), b);
                core.mark_dirty();
            }
            Err(e) => return Response::err(req_id, format!("pipeline: serialize model node: {e}")),
        }
    }
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "name": name,
            "version": version,
            "model_id": model_id,
            "family": family,
            "algorithm": algorithm,
            "metrics": metrics_obj,
            "n_features": n_features,
            "n_train": n_train,
            "n_test": n_test,
            "classes": classes,
            "written_back": writeback,
        })),
    )
}

// ─────────────────────────── Serve ───────────────────────────

/// Deploy a versioned `:Model` as the served version for `name`: writes a
/// `:ServedModel` pointer so `predict`/`evaluate` with `version=0` resolve to it.
pub(crate) fn handle_serve(req_id: u64, core: &GraphCore, name: String, version: u64) -> Response {
    let model_id = model_node_id(&name, version);
    if core.get_node_properties(&model_id).is_none() {
        return Response::err(
            req_id,
            format!("pipeline: model {model_id:?} not found (train version {version} first)"),
        );
    }
    let props = json!({
        "type": "ServedModel",
        "name": name,
        "version": version,
        "model_id": model_id,
        "served_ts": now_secs(),
    });
    match rmp_serde::to_vec_named(&props) {
        Ok(b) => {
            core.add_node(served_node_id(&name), b);
            core.mark_dirty();
        }
        Err(e) => return Response::err(req_id, format!("pipeline: serialize served pointer: {e}")),
    }
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "name": name,
            "version": version,
            "model_id": model_id,
            "served": true,
        })),
    )
}

// ─────────────────────────── Predict ───────────────────────────

pub(crate) fn handle_predict(
    req_id: u64,
    core: &GraphCore,
    name: String,
    version: u64,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    writeback: bool,
) -> Response {
    let version = match resolve_version(core, &name, version) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    let model = match load_model(core, &name, version) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, e),
    };
    match model.family.as_str() {
        "graphlearn" => predict_graphlearn(req_id, core, &model, source, writeback),
        "classify" => predict_classify(req_id, core, &model, source, x, writeback),
        "estimator" => predict_estimator(req_id, core, &model, source, x, writeback),
        other => Response::err(req_id, format!("pipeline: unknown stored family {other:?}")),
    }
}

fn predict_classify(
    req_id: u64,
    core: &GraphCore,
    model: &LoadedModel,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    writeback: bool,
) -> Response {
    let (ids, rows) = match build_features(core, &source, &x, &model.feature_spec) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if rows.is_empty() {
        return Response::err(req_id, "pipeline: no feature rows to predict".to_string());
    }
    let clf: FittedClassifier = match serde_json::from_value(model.blob.clone()) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("pipeline: invalid classify blob: {e}")),
    };
    let out = classify::predict(&clf, &rows);
    let written = if writeback {
        materialize_predictions_classify(core, &model.model_id, &ids, &out)
    } else {
        0
    };
    let rows_json: Vec<Value> = (0..rows.len())
        .map(|i| {
            json!({
                "id": ids.get(i).cloned().unwrap_or_else(|| i.to_string()),
                "label": out.labels[i],
                "proba": out.proba[i],
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "model_id": model.model_id,
            "family": "classify",
            "rows": rows_json,
            "classes": out.classes,
            "n_rows": rows.len(),
            "written_back": written,
        })),
    )
}

fn predict_estimator(
    req_id: u64,
    core: &GraphCore,
    model: &LoadedModel,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    writeback: bool,
) -> Response {
    let (ids, rows) = match build_features(core, &source, &x, &model.feature_spec) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if rows.is_empty() {
        return Response::err(req_id, "pipeline: no feature rows to predict".to_string());
    }
    let fm: FittedModel = match serde_json::from_value(model.blob.clone()) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("pipeline: invalid estimator blob: {e}")),
    };
    let yhat = estimators::predict(&fm, &rows);
    let written = if writeback {
        materialize_predictions_reg(core, &model.model_id, &ids, &yhat)
    } else {
        0
    };
    let rows_json: Vec<Value> = (0..rows.len())
        .map(|i| {
            json!({
                "id": ids.get(i).cloned().unwrap_or_else(|| i.to_string()),
                "value": yhat.get(i).copied().unwrap_or(0.0),
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "model_id": model.model_id,
            "family": "estimator",
            "rows": rows_json,
            "n_rows": rows.len(),
            "written_back": written,
        })),
    )
}

fn predict_graphlearn(
    req_id: u64,
    core: &GraphCore,
    model: &LoadedModel,
    source: Option<GraphSource>,
    _writeback: bool,
) -> Response {
    let Some(source) = source else {
        return Response::err(
            req_id,
            "pipeline: graphlearn predict requires a `source` subgraph".to_string(),
        );
    };
    let kan: KanLinkModel = match serde_json::from_value(model.blob.clone()) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("pipeline: invalid graphlearn blob: {e}")),
    };
    let (graph, existing) = super::graphlearn::build_graph_with_set(core, &source);
    if graph.node_count() < 2 {
        return Response::err(
            req_id,
            "pipeline: graphlearn subgraph < 2 nodes".to_string(),
        );
    }
    let ctx = match first_embedding(&model.feature_spec) {
        Some(step) => {
            let rows = compute_embedding_rows(&graph, step);
            FeatureCtx::build_with_embeddings(&graph, kan.alpha, rows)
        }
        None => FeatureCtx::build(&graph, kan.alpha),
    };
    // Top-k highest-probability missing links (a sensible default surface; the fuller
    // candidate-pairs API stays on `graph_learn predict`).
    let scored = link_predict::predict_missing_links(&kan, &ctx, &existing, 50);
    let rows_json: Vec<Value> = scored
        .iter()
        .map(|&(a, b, score)| {
            json!({ "src": graph.node_at(a), "dst": graph.node_at(b), "score": score })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "model_id": model.model_id,
            "family": "graphlearn",
            "predicted": rows_json,
            "n_predicted": scored.len(),
        })),
    )
}

// ─────────────────────────── Evaluate ───────────────────────────

fn handle_evaluate(
    req_id: u64,
    core: &GraphCore,
    name: String,
    version: u64,
    source: Option<GraphSource>,
    x: Vec<Vec<f64>>,
    y: Vec<i64>,
) -> Response {
    let version = match resolve_version(core, &name, version) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    let model = match load_model(core, &name, version) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, e),
    };
    if model.family == "graphlearn" {
        return Response::err(
            req_id,
            "pipeline: evaluate is for classify/estimator families; the graphlearn \
             family's held-out AUC is recorded at train time (see compare)"
                .to_string(),
        );
    }
    let (ids, rows) = match build_features(core, &source, &x, &model.feature_spec) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if rows.is_empty() {
        return Response::err(req_id, "pipeline: no feature rows to evaluate".to_string());
    }
    let labels = match resolve_labels_f64(core, &ids, &y, &model.label_property) {
        Ok(l) => l,
        Err(e) => return Response::err(req_id, e),
    };
    if labels.len() != rows.len() {
        return Response::err(
            req_id,
            format!("pipeline: {} rows but {} labels", rows.len(), labels.len()),
        );
    }
    let metrics_obj = match model.family.as_str() {
        "classify" => {
            let clf: FittedClassifier = match serde_json::from_value(model.blob.clone()) {
                Ok(m) => m,
                Err(e) => return Response::err(req_id, format!("pipeline: invalid blob: {e}")),
            };
            let y_true = to_i64_round(&labels);
            let pred = classify::predict(&clf, &rows).labels;
            json!({
                "accuracy": metrics::accuracy(&y_true, &pred),
                "macro_f1": metrics::macro_f1(&y_true, &pred),
            })
        }
        _ => {
            let fm: FittedModel = match serde_json::from_value(model.blob.clone()) {
                Ok(m) => m,
                Err(e) => return Response::err(req_id, format!("pipeline: invalid blob: {e}")),
            };
            let yhat = estimators::predict(&fm, &rows);
            json!({
                "r2": metrics::r2(&labels, &yhat),
                "rmse": metrics::rmse(&labels, &yhat),
            })
        }
    };
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "name": name,
            "version": version,
            "family": model.family,
            "metrics": metrics_obj,
            "n": rows.len(),
        })),
    )
}

// ─────────────────────────── Compare ───────────────────────────

fn handle_compare(
    req_id: u64,
    core: &GraphCore,
    name: String,
    version_a: u64,
    version_b: u64,
) -> Response {
    let a = match load_model(core, &name, version_a) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, e),
    };
    let b = match load_model(core, &name, version_b) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, e),
    };
    let ma = primary_metrics(&a.metrics);
    let mb = primary_metrics(&b.metrics);
    let diff = diff_metrics(ma, mb);
    Response::ok(
        req_id,
        ResultPayload::Json(json!({
            "name": name,
            "version_a": version_a,
            "version_b": version_b,
            "algorithm_a": a.algorithm,
            "algorithm_b": b.algorithm,
            "metrics_a": a.metrics,
            "metrics_b": b.metrics,
            "diff": diff,
        })),
    )
}

/// The primary held-out metric object: `test` when present (tabular), else `train`
/// (the graphlearn family records only train-time AUC).
fn primary_metrics(m: &Value) -> &Value {
    m.get("test")
        .filter(|v| v.is_object())
        .or_else(|| m.get("train"))
        .unwrap_or(m)
}

/// Per-metric `b − a` for every numeric key present in both objects.
fn diff_metrics(a: &Value, b: &Value) -> Value {
    let (Some(ao), Some(bo)) = (a.as_object(), b.as_object()) else {
        return Value::Null;
    };
    let mut out = serde_json::Map::new();
    for (k, av) in ao {
        if let (Some(af), Some(bf)) = (av.as_f64(), bo.get(k).and_then(|v| v.as_f64())) {
            out.insert(k.clone(), json!(bf - af));
        }
    }
    Value::Object(out)
}

// ─────────────────────────── Feature construction ───────────────────────────

/// Build the `(node_id, row)` feature matrix by running the ordered feature steps.
/// Explicit `x` short-circuits the producing steps (only transforms apply).
fn build_features(
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
    let mut ids: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<f64>> = Vec::new();
    let mut produced = false;
    for step in steps {
        match step {
            FeatureStep::Embedding { .. } => {
                let (graph, _) = super::graphlearn::build_graph_with_set(core, source);
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
fn compute_embedding_rows(graph: &AdjacencyGraph<String>, step: &FeatureStep) -> Vec<Vec<f64>> {
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

/// The first `Embedding` step (feeds the KAN embedding channel for the graphlearn family).
fn first_embedding(steps: &[FeatureStep]) -> Option<&FeatureStep> {
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

/// L2-normalize each feature row in place (a zero row is left untouched). Inlined —
/// eg-compute's own `l2_normalize_rows` is crate-private.
fn l2_normalize_rows(rows: &mut [Vec<f64>]) {
    for row in rows.iter_mut() {
        let norm: f64 = row.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 0.0 {
            for x in row.iter_mut() {
                *x /= norm;
            }
        }
    }
}

// ─────────────────────────── Labels ───────────────────────────

/// Resolve labels as `f64` (the internal split currency): explicit `y` (cast) wins;
/// otherwise read each node's `label_property`. Classification rounds to `i64` at fit.
fn resolve_labels_f64(
    core: &GraphCore,
    ids: &[String],
    explicit_y: &[i64],
    label_property: &str,
) -> Result<Vec<f64>, String> {
    if !explicit_y.is_empty() {
        return Ok(explicit_y.iter().map(|&l| l as f64).collect());
    }
    if label_property.is_empty() {
        return Err(
            "pipeline: no labels — pass explicit `y` or set the spec's `label_property`"
                .to_string(),
        );
    }
    let mut labels = Vec::with_capacity(ids.len());
    for id in ids {
        let blob = core
            .get_node_properties(id)
            .ok_or_else(|| format!("pipeline: node {id:?} not found for label read"))?;
        let val = eg_types::msgpack::decode_property_value(&blob)
            .map_err(|e| format!("pipeline: decode {id:?}: {e:?}"))?;
        let label = val
            .get(label_property)
            .and_then(|v| v.as_f64())
            .ok_or_else(|| {
                format!("pipeline: node {id:?} missing numeric label property {label_property:?}")
            })?;
        labels.push(label);
    }
    Ok(labels)
}

// ─────────────────────────── Model artifact (versioned :Model node) ──────────

/// A loaded `:Model` artifact (the fitted blob + the recipe needed to rebuild features).
struct LoadedModel {
    family: String,
    algorithm: String,
    blob: Value,
    feature_spec: Vec<FeatureStep>,
    label_property: String,
    metrics: Value,
    model_id: String,
}

fn model_node_id(name: &str, version: u64) -> String {
    format!("model:{name}:v{version}")
}

fn served_node_id(name: &str) -> String {
    format!("servedmodel:{name}")
}

/// Next version for `name` = 1 + the max existing `:Model` version carrying that name.
fn next_version(core: &GraphCore, name: &str) -> u64 {
    let mut max = 0u64;
    for (_, blob) in core.get_nodes_by_label("Model", 0) {
        if let Ok(v) = eg_types::msgpack::decode_property_value(&blob) {
            if v.get("name").and_then(|x| x.as_str()) == Some(name) {
                if let Some(ver) = v.get("version").and_then(|x| x.as_u64()) {
                    max = max.max(ver);
                }
            }
        }
    }
    max + 1
}

/// Resolve `version` (0 ⇒ the served pointer's version).
fn resolve_version(core: &GraphCore, name: &str, version: u64) -> Result<u64, String> {
    if version > 0 {
        return Ok(version);
    }
    let blob = core
        .get_node_properties(&served_node_id(name))
        .ok_or_else(|| {
            format!(
                "pipeline: no served version for {name:?} — call serve or pass an explicit version"
            )
        })?;
    let v = eg_types::msgpack::decode_property_value(&blob)
        .map_err(|e| format!("pipeline: decode served pointer: {e:?}"))?;
    v.get("version")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| "pipeline: served pointer missing version".to_string())
}

fn load_model(core: &GraphCore, name: &str, version: u64) -> Result<LoadedModel, String> {
    let model_id = model_node_id(name, version);
    let blob = core
        .get_node_properties(&model_id)
        .ok_or_else(|| format!("pipeline: model {model_id:?} not found"))?;
    let v = eg_types::msgpack::decode_property_value(&blob)
        .map_err(|e| format!("pipeline: decode model {model_id:?}: {e:?}"))?;
    let feature_spec: Vec<FeatureStep> = v
        .get("feature_spec")
        .cloned()
        .and_then(|fv| serde_json::from_value(fv).ok())
        .unwrap_or_default();
    Ok(LoadedModel {
        family: v
            .get("family")
            .and_then(|x| x.as_str())
            .unwrap_or("classify")
            .to_string(),
        algorithm: v
            .get("algorithm")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        blob: v.get("blob").cloned().unwrap_or(Value::Null),
        feature_spec,
        label_property: v
            .get("label_property")
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string(),
        metrics: v.get("metrics").cloned().unwrap_or(Value::Null),
        model_id,
    })
}

// ─────────────────────────── Prediction write-back ───────────────────────────

fn prediction_node_id(model_id: &str, src: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(model_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(src.as_bytes());
    format!("prediction:{}", hex::encode(&hasher.finalize()[..12]))
}

fn materialize_predictions_classify(
    core: &GraphCore,
    model_id: &str,
    ids: &[String],
    out: &classify::Classification,
) -> usize {
    let mut written = 0usize;
    for i in 0..out.labels.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        written += write_prediction(
            core,
            model_id,
            &src,
            json!({
                "type": "Prediction",
                "source": src,
                "label": out.labels[i],
                "proba": out.proba.get(i),
                "model": model_id,
            }),
        );
    }
    written
}

fn materialize_predictions_reg(
    core: &GraphCore,
    model_id: &str,
    ids: &[String],
    yhat: &[f64],
) -> usize {
    let mut written = 0usize;
    for (i, &value) in yhat.iter().enumerate() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        written += write_prediction(
            core,
            model_id,
            &src,
            json!({
                "type": "Prediction",
                "source": src,
                "value": value,
                "model": model_id,
            }),
        );
    }
    written
}

fn write_prediction(core: &GraphCore, model_id: &str, src: &str, props: Value) -> usize {
    let node_id = prediction_node_id(model_id, src);
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if core.has_node(src) {
        if let Ok(eb) = rmp_serde::to_vec_named(&json!({ "relationship": "PREDICTED_FOR" })) {
            let _ = core.add_edge(node_id, src.to_string(), eb);
        }
    }
    1
}

// ─────────────────────────── Spec parsing ───────────────────────────

fn normalize_family(family: &str) -> String {
    match family.to_ascii_lowercase().as_str() {
        "regress" | "regression" | "estimator" => "estimator".to_string(),
        "kan" | "graphlearn" | "link_prediction" | "linkpredict" => "graphlearn".to_string(),
        "" | "classify" | "classification" => "classify".to_string(),
        other => other.to_string(),
    }
}

fn classify_algo_from_spec(m: &ModelSpec) -> Result<classify::Algorithm, String> {
    let p = &m.params;
    let f = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    let u = |k: &str, d: usize| {
        p.get(k)
            .and_then(|v| v.as_u64())
            .map(|x| x as usize)
            .unwrap_or(d)
    };
    match m.algorithm.to_ascii_lowercase().as_str() {
        "" | "gaussiannb" => Ok(classify::Algorithm::GaussianNb),
        "multinomialnb" => Ok(classify::Algorithm::MultinomialNb {
            alpha: f("alpha", 1.0),
        }),
        "knn" => Ok(classify::Algorithm::Knn { k: u("k", 5) }),
        "logistic" => Ok(classify::Algorithm::Logistic {
            lr: f("lr", 0.1),
            epochs: u("epochs", 300),
            l2: f("l2", 0.0),
        }),
        "svc" => Ok(classify::Algorithm::LinearSvc {
            c: f("C", 1.0),
            epochs: u("epochs", 300),
            lr: f("lr", 0.1),
        }),
        other => Err(format!(
            "pipeline: unknown classify algorithm {other:?} \
             (gaussiannb | multinomialnb | knn | logistic | svc)"
        )),
    }
}

fn estimator_from_spec(m: &ModelSpec) -> (String, EstimatorParams) {
    let params: EstimatorParams = serde_json::from_value(m.params.clone()).unwrap_or_default();
    let name = if m.algorithm.is_empty() {
        "ridge".to_string()
    } else {
        m.algorithm.clone()
    };
    (name, params)
}

fn kan_config_from_spec(m: &ModelSpec) -> KanLinkConfig {
    let p = &m.params;
    let f = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    let u = |k: &str, d: usize| {
        p.get(k)
            .and_then(|v| v.as_u64())
            .map(|x| x as usize)
            .unwrap_or(d)
    };
    let basis = match p
        .get("basis")
        .and_then(|v| v.as_str())
        .unwrap_or("chebyshev")
        .to_ascii_lowercase()
        .as_str()
    {
        "jacobi" => Basis::Jacobi,
        _ => Basis::Chebyshev,
    };
    let lr = f("lr", 0.05);
    let neg_ratio = f("neg_ratio", 1.0);
    KanLinkConfig {
        basis,
        degree: u("degree", 4).max(1),
        hidden: u("hidden", 0),
        epochs: u("epochs", 200).max(1),
        lr: if lr > 0.0 { lr } else { 0.05 },
        neg_ratio: if neg_ratio > 0.0 { neg_ratio } else { 1.0 },
        seed: p.get("seed").and_then(|v| v.as_u64()).unwrap_or(42),
        alpha: f("alpha", 0.5).clamp(0.0, 1.0),
    }
}

// ─────────────────────────── Small helpers ───────────────────────────

fn to_i64_round(v: &[f64]) -> Vec<i64> {
    v.iter().map(|&x| x.round() as i64).collect()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sorted class set of a fitted classifier (for the `:Model` node + response).
fn classify_classes(model: &FittedClassifier) -> Vec<i64> {
    match model {
        FittedClassifier::GaussianNb { classes, .. }
        | FittedClassifier::MultinomialNb { classes, .. }
        | FittedClassifier::Knn { classes, .. }
        | FittedClassifier::LinearOvr { classes, .. } => classes.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolation::IsolationLayer;
    use crate::protocol::GraphSource;
    use crate::server::access::GraphReadAuthority;
    use crate::server::auth::VerifiedRequestContext;
    use eg_types::wire::{FeatureStep, ModelSpec, SplitSpec};

    fn node(props: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    /// A [`GraphReadAuthority`] for `agent_id` over a fresh, empty (default-deny)
    /// RLS policy — the same construction `access.rs`'s own RLS proof tests use.
    fn authority_for(agent_id: &str) -> GraphReadAuthority {
        let isolation = IsolationLayer::new();
        let ctx = VerifiedRequestContext::verified_for_test(agent_id);
        GraphReadAuthority::from_verified(&ctx, &isolation).unwrap()
    }

    /// Two dense communities of `Person` nodes (a planted partition), each node tagged
    /// with its community as the integer `label` property — the node-classification
    /// fixture. Community 0 = c0_*, community 1 = c1_*; dense intra, one weak cross edge.
    fn seed_two_community_graph(core: &GraphCore) {
        let c0 = ["c0a", "c0b", "c0c", "c0d", "c0e", "c0f"];
        let c1 = ["c1a", "c1b", "c1c", "c1d", "c1e", "c1f"];
        for id in c0 {
            core.add_node(id.into(), node(json!({ "type": "Person", "label": 0 })));
        }
        for id in c1 {
            core.add_node(id.into(), node(json!({ "type": "Person", "label": 1 })));
        }
        // Dense within each community (near-clique), plus a single cross link.
        for group in [&c0, &c1] {
            for i in 0..group.len() {
                for j in (i + 1)..group.len() {
                    let _ = core.add_edge(
                        group[i].into(),
                        group[j].into(),
                        node(json!({ "relationship": "KNOWS" })),
                    );
                }
            }
        }
        let _ = core.add_edge(
            "c0a".into(),
            "c1a".into(),
            node(json!({ "relationship": "KNOWS" })),
        );
        core.mark_dirty();
    }

    fn person_source() -> GraphSource {
        GraphSource {
            node_label: "Person".into(),
            direction: "any".into(),
            relation: None,
            limit: 0,
        }
    }

    fn classify_spec(algorithm: &str) -> PipelineSpec {
        PipelineSpec {
            features: vec![FeatureStep::Embedding {
                method: "fastrp".into(),
                dim: 32,
                iterations: 4,
                walk_length: 40,
                walks_per_node: 10,
                window: 5,
                epochs: 5,
                seed: 7,
            }],
            split: SplitSpec {
                test_ratio: 0.34,
                shuffle: true,
                seed: 13,
            },
            label_property: "label".into(),
            model: ModelSpec {
                family: "classify".into(),
                algorithm: algorithm.into(),
                params: json!({ "epochs": 400, "lr": 0.2 }),
            },
        }
    }

    fn json_of(resp: Response) -> Value {
        match resp.result {
            Some(ResultPayload::Json(v)) => v,
            _ => panic!("expected json result, got error: {:?}", resp.error),
        }
    }

    /// E2E acceptance: train + eval + serve + predict a node-classification pipeline
    /// end-to-end over a fixture graph; then a SECOND version + a metrics-diff compare.
    #[test]
    fn node_classification_pipeline_end_to_end() {
        let core = Arc::new(GraphCore::new());
        seed_two_community_graph(&core);

        // ── train v1 (logistic) ──
        let v1 = json_of(handle_train(
            1,
            &core,
            "community".into(),
            Some(person_source()),
            vec![],
            vec![],
            classify_spec("logistic"),
            true,
        ));
        assert_eq!(v1["version"], 1);
        assert_eq!(v1["family"], "classify");
        assert_eq!(v1["written_back"], true);
        assert_eq!(v1["model_id"], "model:community:v1");
        let test_acc = v1["metrics"]["test"]["accuracy"].as_f64().unwrap();
        assert!(
            test_acc >= 0.7,
            "held-out accuracy should recover the planted communities: {test_acc}"
        );
        // The versioned :Model artifact is a queryable KG node.
        let models = core.get_nodes_by_label("Model", 0);
        assert_eq!(models.len(), 1);

        // ── eval v1 on the same labeled nodes (a distinct lifecycle verb) ──
        let ev = json_of(handle_evaluate(
            2,
            &core,
            "community".into(),
            1,
            Some(person_source()),
            vec![],
            vec![],
        ));
        assert!(ev["metrics"]["accuracy"].as_f64().unwrap() >= 0.7);
        assert_eq!(ev["n"], 12);

        // ── serve v1 ──
        let sv = json_of(handle_serve(3, &core, "community".into(), 1));
        assert_eq!(sv["served"], true);
        assert_eq!(sv["version"], 1);

        // ── predict via the served version (version=0 resolves the pointer) ──
        let pr = json_of(handle_predict(
            4,
            &core,
            "community".into(),
            0,
            Some(person_source()),
            vec![],
            true,
        ));
        assert_eq!(pr["n_rows"], 12);
        assert_eq!(pr["written_back"], 12);
        // Every predicted label is one of the trained classes.
        for row in pr["rows"].as_array().unwrap() {
            let lbl = row["label"].as_i64().unwrap();
            assert!(lbl == 0 || lbl == 1, "label {lbl}");
        }
        // :Prediction nodes were materialized.
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("Prediction", 0).len(), 12);

        // ── train v2 (knn) → a comparable second version ──
        let v2 = json_of(handle_train(
            5,
            &core,
            "community".into(),
            Some(person_source()),
            vec![],
            vec![],
            classify_spec("knn"),
            true,
        ));
        assert_eq!(v2["version"], 2);
        assert_eq!(core.get_nodes_by_label("Model", 0).len(), 2);

        // ── compare v1 vs v2 (metrics diff) ──
        let cmp = json_of(handle_compare(6, &core, "community".into(), 1, 2));
        assert_eq!(cmp["version_a"], 1);
        assert_eq!(cmp["version_b"], 2);
        assert!(cmp["metrics_a"]["test"]["accuracy"].is_number());
        assert!(cmp["metrics_b"]["test"]["accuracy"].is_number());
        // The diff carries the per-metric delta between the two versions.
        assert!(cmp["diff"]["accuracy"].is_number());
        let a = cmp["metrics_a"]["test"]["accuracy"].as_f64().unwrap();
        let b = cmp["metrics_b"]["test"]["accuracy"].as_f64().unwrap();
        let d = cmp["diff"]["accuracy"].as_f64().unwrap();
        assert!((d - (b - a)).abs() < 1e-9, "diff must be b - a");
    }

    // ── BUG-034: `MiningPipelineEvaluate`/`Compare` must not leak existence via
    // the label-index read their feature source (`Evaluate`) or loaded `:Model`
    // (`Compare`) touches ──────────────────────────────────────────────────

    /// Community 0 ("pub*") is explicitly public; community 1 ("priv*") is owned
    /// by `bob` and marked private. Same dense-intra/one-cross-edge topology as
    /// [`seed_two_community_graph`], just RLS-tagged so a non-grantee's
    /// `project_core` hides exactly community 1.
    fn seed_two_community_graph_rls(core: &GraphCore) {
        let pub_ids = ["pub_a", "pub_b", "pub_c", "pub_d", "pub_e", "pub_f"];
        let priv_ids = ["priv_a", "priv_b", "priv_c", "priv_d", "priv_e", "priv_f"];
        for id in pub_ids {
            core.add_node(
                id.into(),
                node(json!({ "type": "Person", "label": 0, "_visibility": "public" })),
            );
        }
        for id in priv_ids {
            core.add_node(
                id.into(),
                node(json!({
                    "type": "Person",
                    "label": 1,
                    "_owner": "bob",
                    "_visibility": "private",
                })),
            );
        }
        for group in [&pub_ids, &priv_ids] {
            for i in 0..group.len() {
                for j in (i + 1)..group.len() {
                    let _ = core.add_edge(
                        group[i].into(),
                        group[j].into(),
                        node(json!({ "relationship": "KNOWS" })),
                    );
                }
            }
        }
        let _ = core.add_edge(
            "pub_a".into(),
            "priv_a".into(),
            node(json!({ "relationship": "KNOWS" })),
        );
        core.mark_dirty();
    }

    /// Explicitly RLS-tag an already-trained `:Model` node (CONCEPT:EG-KG.sharding.row-level-security).
    /// A freshly-trained model carries NO `_owner`/`_visibility`/`_grants` fields —
    /// `RowVisibility::tagged` is therefore `false`, and an untagged row is
    /// default-DENIED to every non-System caller (there is no implicit-public
    /// fallback for an untagged row, only for an explicit `_visibility` other than
    /// `"private"` — see `row_visibility`'s doc). Tests that read a model through a
    /// non-System `GraphReadAuthority` must tag it explicitly, same as any other
    /// row whose visibility they care about.
    fn tag_model_visibility(core: &GraphCore, model_id: &str, visibility: &str, owner: Option<&str>) {
        let mut props =
            eg_types::msgpack::decode_property_value(&core.get_node_properties(model_id).unwrap())
                .unwrap();
        if let Some(owner) = owner {
            props["_owner"] = json!(owner);
        }
        props["_visibility"] = json!(visibility);
        core.add_node(
            model_id.to_string(),
            rmp_serde::to_vec_named(&props).unwrap(),
        );
        core.mark_dirty();
    }

    /// RED before the BUG-034 fix: `try_handle`'s `Evaluate`/`Compare` arms called
    /// `handle_evaluate`/`handle_compare` against the raw, unfiltered core, so a
    /// non-grantee's `n` (a label-index-derived row count — the exact `count(n)`-
    /// shaped side channel BUG-034 names) and model visibility included bob's
    /// private community. GREEN after: `try_handle` now projects `core` through
    /// `GraphReadAuthority::project_core` before either read, exactly like
    /// `mining::try_handle`'s `MineClassifyFit` already does.
    #[test]
    fn mining_pipeline_evaluate_hides_invisible_rows_from_non_grantee() {
        let core = Arc::new(GraphCore::new());
        seed_two_community_graph_rls(&core);

        // Train once, in full, over the WHOLE graph (both communities) — training
        // is not the surface under test here; only the later read (`Evaluate`) is.
        let train = json_of(handle_train(
            1,
            &core,
            "community".into(),
            Some(person_source()),
            vec![],
            vec![],
            classify_spec("logistic"),
            true,
        ));
        assert_eq!(train["version"], 1);
        // A freshly-trained `:Model` node carries NO RLS tags of its own — under
        // the mandatory default-deny RLS posture (CONCEPT:EG-KG.sharding.row-level-security,
        // `RowVisibility::tagged`), an UNTAGGED row is invisible to every non-System
        // caller, not implicitly public. This test is about the row-visibility of
        // the PERSON data Evaluate reads, not the model's own visibility, so tag the
        // model explicitly public — exactly the fixture pattern
        // `mining_pipeline_compare_respects_model_visibility` uses for its own
        // (explicitly private) v1 tag, just with the opposite value.
        tag_model_visibility(&core, "model:community:v1", "public", None);

        // alice is neither bob nor bob's manager: `priv_*` must stay invisible.
        let alice = authority_for("alice");
        let resp = handlers_try_handle_evaluate(&core, &alice, "community", 1);
        let ev = json_of(resp);
        assert_eq!(
            ev["n"], 6,
            "a non-grantee's Evaluate must see only the 6 public rows, not all 12 \
             (existence/count leak of bob's private community)"
        );

        // bob (the owner) gets the correct, COMPLETE count — the fix must not
        // turn this into an unconditional deny.
        let bob = authority_for("bob");
        let resp = handlers_try_handle_evaluate(&core, &bob, "community", 1);
        let ev = json_of(resp);
        assert_eq!(
            ev["n"], 12,
            "the owner's own Evaluate must still see the complete, correct count"
        );
    }

    /// Same existence-leak shape as above, but for `Compare`'s loaded `:Model`
    /// nodes: a `:Model` trained/serving inside a graph a non-owner otherwise has
    /// Read access to is itself just another RLS-tagged node. Confirms `Compare`
    /// goes through the SAME `project_core` gate — a model visible to its trainer
    /// must not silently resolve for anyone else once tagged private.
    #[test]
    fn mining_pipeline_compare_respects_model_visibility() {
        let core = Arc::new(GraphCore::new());
        seed_two_community_graph_rls(&core);
        let train = json_of(handle_train(
            1,
            &core,
            "community".into(),
            Some(person_source()),
            vec![],
            vec![],
            classify_spec("logistic"),
            true,
        ));
        assert_eq!(train["version"], 1);
        // Tag the trained `:Model` node itself private to bob — a real deployment
        // shape (a model trained over a private cohort, e.g. `:Model` inheriting
        // its source's ownership).
        tag_model_visibility(&core, "model:community:v1", "private", Some("bob"));

        let train2 = json_of(handle_train(
            2,
            &core,
            "community".into(),
            Some(person_source()),
            vec![],
            vec![],
            classify_spec("knn"),
            true,
        ));
        assert_eq!(train2["version"], 2);
        // v2 carries no RLS tags of its own (untagged ⇒ default-deny under the
        // mandatory RLS posture, same as the evaluate test's model tag) — this
        // test is about v1's PRIVATE visibility specifically, so v2 must be
        // explicitly public or its own invisibility would mask the assertion
        // this test exists to prove (bob CAN resolve it once v1's block clears).
        tag_model_visibility(&core, "model:community:v2", "public", None);

        let alice = authority_for("alice");
        let resp = pipeline_try_handle(
            &core,
            &alice,
            Method::MiningPipelineCompare {
                name: "community".into(),
                version_a: 1,
                version_b: 2,
            },
        );
        let err = match resp.result {
            Some(_) => panic!("alice must not resolve bob's private v1 model"),
            None => resp.error.unwrap(),
        };
        assert!(
            err.contains("model") && err.contains("not found"),
            "expected a not-found error for the invisible model, got: {err}"
        );

        let bob = authority_for("bob");
        let resp = pipeline_try_handle(
            &core,
            &bob,
            Method::MiningPipelineCompare {
                name: "community".into(),
                version_a: 1,
                version_b: 2,
            },
        );
        let cmp = json_of(resp);
        assert_eq!(cmp["version_a"], 1);
        assert_eq!(cmp["version_b"], 2);
    }

    /// Drive the real `try_handle` entry point (not the bare `handle_evaluate`
    /// helper) so the proof exercises the actual dispatch-reachable code path,
    /// including the `project_core` gate this fix adds.
    fn handlers_try_handle_evaluate(
        core: &Arc<GraphCore>,
        authority: &GraphReadAuthority,
        name: &str,
        version: u64,
    ) -> Response {
        pipeline_try_handle(
            core,
            authority,
            Method::MiningPipelineEvaluate {
                name: name.to_string(),
                version,
                source: Some(person_source()),
                x: vec![],
                y: vec![],
            },
        )
    }

    fn pipeline_try_handle(
        core: &Arc<GraphCore>,
        authority: &GraphReadAuthority,
        method: Method,
    ) -> Response {
        super::try_handle(1, Arc::clone(core), Some(authority), method)
            .unwrap_or_else(|m| panic!("method should be handled by pipeline::try_handle: {m:?}"))
    }

    /// The estimator (regression) family shares the same feature→split→version→predict
    /// skeleton over an explicit matrix — proves the pipeline is not classify-only.
    #[test]
    fn estimator_family_trains_versions_and_predicts() {
        let core = Arc::new(GraphCore::new());
        // y ≈ 2·x0 + 1 — a trivially learnable line.
        let x = vec![
            vec![0.0],
            vec![1.0],
            vec![2.0],
            vec![3.0],
            vec![4.0],
            vec![5.0],
            vec![6.0],
            vec![7.0],
        ];
        let y = vec![1, 3, 5, 7, 9, 11, 13, 15];
        let spec = PipelineSpec {
            features: vec![],
            split: SplitSpec {
                test_ratio: 0.25,
                shuffle: true,
                seed: 5,
            },
            label_property: String::new(),
            model: ModelSpec {
                family: "estimator".into(),
                algorithm: "ridge".into(),
                params: json!({ "alpha": 0.01 }),
            },
        };
        let tr = json_of(handle_train(
            1,
            &core,
            "line".into(),
            None,
            x.clone(),
            y,
            spec,
            true,
        ));
        assert_eq!(tr["family"], "estimator");
        assert_eq!(tr["version"], 1);
        assert!(
            tr["metrics"]["test"]["r2"].as_f64().unwrap() > 0.9,
            "ridge should fit the line: {}",
            tr["metrics"]["test"]["r2"]
        );
        // Predict with explicit x through the stored model.
        let pr = json_of(handle_predict(
            2,
            &core,
            "line".into(),
            1,
            None,
            vec![vec![10.0]],
            false,
        ));
        let yhat = pr["rows"][0]["value"].as_f64().unwrap();
        assert!(
            (yhat - 21.0).abs() < 2.0,
            "expected ≈21 for x=10, got {yhat}"
        );
    }

    /// The graphlearn (KAN) family is now one pipeline family: trainable as a versioned
    /// :Model artifact and predict-able — the literal generalization of the one-off.
    #[test]
    fn graphlearn_family_versioned_artifact() {
        let core = Arc::new(GraphCore::new());
        seed_two_community_graph(&core);
        let spec = PipelineSpec {
            features: vec![],
            split: SplitSpec::default(),
            label_property: String::new(),
            model: ModelSpec {
                family: "graphlearn".into(),
                algorithm: String::new(),
                params: json!({ "epochs": 120, "degree": 3 }),
            },
        };
        let tr = json_of(handle_train(
            1,
            &core,
            "links".into(),
            Some(person_source()),
            vec![],
            vec![],
            spec,
            true,
        ));
        assert_eq!(tr["family"], "graphlearn");
        assert_eq!(tr["version"], 1);
        assert!(tr["metrics"]["train"]["auc"].as_f64().unwrap() > 0.5);
        // Predict top-k missing links through the stored KAN model.
        let pr = json_of(handle_predict(
            2,
            &core,
            "links".into(),
            1,
            Some(person_source()),
            vec![],
            false,
        ));
        assert_eq!(pr["family"], "graphlearn");
        assert!(pr["n_predicted"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn non_pipeline_method_falls_through() {
        let core = Arc::new(GraphCore::new());
        assert!(matches!(
            try_handle(1, core, None, Method::NodeCount),
            Err(Method::NodeCount)
        ));
    }
}
