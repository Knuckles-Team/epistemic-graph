use super::*;
use super::{input::*, writeback::*};
use eg_compute::mining::{
    anomaly,
    classify::{self, FittedClassifier},
    cluster, reduce,
};
use eg_types::compute_result::mining::{
    AnomalyMiningResult, AnomalyRow, ClassificationMiningResult, ClassifiedRow,
    ClassifierFitResult, ClusterMiningResult, ClusterRow, ReducedRow, ReductionMiningResult,
    RowRef,
};
use eg_types::result_contract::compute as results;

// ─────────────────────────── Clustering ───────────────────────────

/// Handle `MineCluster` (CONCEPT:EG-KG.mining.dbscan-density): build the feature
/// rows (explicit or node embeddings), run the chosen clustering engine, return
/// `{clusters, labels, ...}`, and optionally write `:Cluster` nodes back.
pub(crate) struct ClusterRequest {
    pub(crate) features: Vec<Vec<f64>>,
    pub(crate) source: Option<VectorSource>,
    #[cfg(feature = "query")]
    pub(crate) plan: Option<crate::wire::Plan>,
    pub(crate) algorithm: ClusterAlgorithm,
    pub(crate) eps: f64,
    pub(crate) min_pts: usize,
    pub(crate) k: usize,
    pub(crate) linkage: Linkage,
    pub(crate) max_iter: usize,
    pub(crate) seed: u64,
    pub(crate) writeback: WritebackOptions,
}

pub(crate) fn handle_cluster(
    req_id: u64,
    core: &Arc<GraphCore>,
    request: ClusterRequest,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let ClusterRequest {
        features,
        source,
        #[cfg(feature = "query")]
        plan,
        algorithm,
        eps,
        min_pts,
        k,
        linkage,
        max_iter,
        seed,
        writeback,
    } = request;
    let (rows, ids) = match build_vectors(
        core,
        &features,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = cluster_algo(algorithm, eps, min_pts, k, linkage, max_iter, seed);
    let out = cluster::cluster(&rows, algo);

    let written = if writeback.enabled {
        materialize_clusters(core, &out, &ids, algorithm)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_cluster_claims(core, &out, &ids, algorithm, cluster_provenance(&source));
    }

    let clusters: Vec<ClusterRow> = out
        .clusters
        .iter()
        .map(|c| ClusterRow {
            cluster_id: c.cluster_id,
            // Report member node ids when the rows came from a node source, else the
            // raw row indices.
            members: c.members.iter().map(|&i| RowRef::at(&ids, i)).collect(),
            centroid: c.centroid.clone(),
            score: c.score,
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::of::<results::MineCluster>(ClusterMiningResult {
            clusters,
            n_rows: rows.len(),
            n_clusters: out.clusters.iter().filter(|c| c.cluster_id >= 0).count(),
            written_back: written,
            labels: out.labels,
            responsibilities: out.responsibilities,
        }),
    )
}

pub(super) fn cluster_algo(
    a: ClusterAlgorithm,
    eps: f64,
    min_pts: usize,
    k: usize,
    linkage: Linkage,
    max_iter: usize,
    seed: u64,
) -> cluster::Algorithm {
    match a {
        ClusterAlgorithm::Dbscan => cluster::Algorithm::Dbscan { eps, min_pts },
        ClusterAlgorithm::Hierarchical => cluster::Algorithm::Hierarchical {
            k,
            linkage: to_linkage(linkage),
        },
        ClusterAlgorithm::Gmm => cluster::Algorithm::Gmm { k, max_iter, seed },
        ClusterAlgorithm::Kmedoids => cluster::Algorithm::KMedoids { k, max_iter },
    }
}

pub(super) fn to_linkage(l: Linkage) -> cluster::Linkage {
    match l {
        Linkage::Single => cluster::Linkage::Single,
        Linkage::Complete => cluster::Linkage::Complete,
        Linkage::Average => cluster::Linkage::Average,
    }
}

/// Materialize each non-noise cluster as a typed `:Cluster` node (CONCEPT:EG-KG.mining.cluster-writeback),
/// id = a deterministic digest of `algo` + its sorted member node-ids (idempotent
/// replay). Members that are resident nodes are linked via `CLUSTER_MEMBER` edges.
pub(super) fn materialize_clusters(
    core: &GraphCore,
    out: &cluster::Clustering,
    ids: &[String],
    algorithm: ClusterAlgorithm,
) -> usize {
    let algo = cluster_algo_name(algorithm);
    let mut written = 0usize;
    for c in &out.clusters {
        if c.cluster_id < 0 {
            continue; // never materialize the DBSCAN noise bucket
        }
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| match ids.get(i) {
                Some(id) => id.clone(),
                None => i.to_string(),
            })
            .collect();
        let node_id = cluster_node_id(algo, &member_ids);
        let props = serde_json::json!({
            "type": "Cluster",
            "algo": algo,
            "cluster_id": c.cluster_id,
            "size": member_ids.len(),
            "members": member_ids,
            "centroid": c.centroid,
            "score": c.score,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for mid in &member_ids {
            if core.has_node(mid) {
                writeback_relationship(core, &node_id, mid, "CLUSTER_MEMBER");
            }
        }
        written += 1;
    }
    written
}

pub(super) fn cluster_algo_name(a: ClusterAlgorithm) -> &'static str {
    match a {
        ClusterAlgorithm::Dbscan => "dbscan",
        ClusterAlgorithm::Hierarchical => "hierarchical",
        ClusterAlgorithm::Gmm => "gmm",
        ClusterAlgorithm::Kmedoids => "kmedoids",
    }
}

pub(super) fn cluster_node_id(algo: &str, member_ids: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted = member_ids.to_vec();
    sorted.sort();
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(sorted.join("\u{1}").as_bytes());
    format!("cluster:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Anomaly detection ───────────────────────────

/// Handle `MineAnomaly` (CONCEPT:EG-KG.mining.isolation-forest): build rows
/// (explicit features, a 1-D values series, or node embeddings), run the detector,
/// return per-row `{id, anomaly_score, is_anomaly}`, and optionally write `:Anomaly`
/// nodes back for the flagged rows.
pub(crate) struct AnomalyRequest {
    pub(crate) features: Vec<Vec<f64>>,
    pub(crate) values: Vec<f64>,
    pub(crate) source: Option<VectorSource>,
    #[cfg(feature = "query")]
    pub(crate) plan: Option<crate::wire::Plan>,
    pub(crate) algorithm: AnomalyAlgorithm,
    pub(crate) k: usize,
    pub(crate) n_trees: usize,
    pub(crate) sample_size: usize,
    pub(crate) seed: u64,
    pub(crate) nu: f64,
    pub(crate) gamma: f64,
    pub(crate) kernel: SvmKernel,
    pub(crate) threshold: Option<f64>,
    pub(crate) writeback: WritebackOptions,
}

pub(crate) fn handle_anomaly(
    req_id: u64,
    core: &Arc<GraphCore>,
    request: AnomalyRequest,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_anomaly_rows(
        core,
        &request.features,
        &request.values,
        &request.source,
        #[cfg(feature = "query")]
        &request.plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = anomaly_algo(
        request.algorithm,
        request.k,
        request.n_trees,
        request.sample_size,
        request.seed,
        request.nu,
        request.gamma,
        request.kernel,
    );
    let out = anomaly::detect(&rows, algo, request.threshold);

    let written = if request.writeback.enabled {
        materialize_anomalies(core, &out, &ids, request.algorithm)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if request.writeback.enabled && request.writeback.as_claim {
        materialize_anomaly_claims(
            core,
            &out,
            &ids,
            request.algorithm,
            anomaly_provenance(&request.source),
        );
    }

    let scored: Vec<AnomalyRow> = (0..rows.len())
        .map(|i| AnomalyRow {
            id: RowRef::at(&ids, i),
            anomaly_score: out.scores[i],
            is_anomaly: out.is_anomaly[i],
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::of::<results::MineAnomaly>(AnomalyMiningResult {
            rows: scored,
            n_rows: rows.len(),
            n_anomalies: out.is_anomaly.iter().filter(|&&a| a).count(),
            threshold: out.threshold,
            written_back: written,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn anomaly_algo(
    a: AnomalyAlgorithm,
    k: usize,
    n_trees: usize,
    sample_size: usize,
    seed: u64,
    nu: f64,
    gamma: f64,
    kernel: SvmKernel,
) -> anomaly::Algorithm {
    match a {
        AnomalyAlgorithm::Zscore => anomaly::Algorithm::ZScoreMad,
        AnomalyAlgorithm::Isoforest => anomaly::Algorithm::IsolationForest {
            n_trees,
            sample_size,
            seed,
        },
        AnomalyAlgorithm::Lof => anomaly::Algorithm::Lof { k },
        AnomalyAlgorithm::Ocsvm => anomaly::Algorithm::OneClassSvm {
            kernel: match kernel {
                SvmKernel::Linear => anomaly::Kernel::Linear,
                SvmKernel::Rbf => anomaly::Kernel::Rbf { gamma },
            },
            nu,
        },
    }
}

/// Materialize each FLAGGED row as a typed `:Anomaly` node (CONCEPT:EG-KG.mining.anomaly-writeback),
/// id = a deterministic digest of `algo` + the source node-id / row index. Linked
/// to its source node via an `ANOMALY_OF` edge when that node is resident.
pub(super) fn materialize_anomalies(
    core: &GraphCore,
    out: &anomaly::Anomalies,
    ids: &[String],
    algorithm: AnomalyAlgorithm,
) -> usize {
    let algo = anomaly_algo_name(algorithm);
    let mut written = 0usize;
    for i in 0..out.scores.len() {
        if !out.is_anomaly[i] {
            continue;
        }
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = anomaly_node_id(algo, &src);
        let props = serde_json::json!({
            "type": "Anomaly",
            "algo": algo,
            "score": out.scores[i],
            "is_anomaly": true,
            "source": src,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        link_writeback_source(core, &node_id, &src, "ANOMALY_OF");
        written += 1;
    }
    written
}

/// Link a materialized writeback node to its resident source using the canonical
/// relationship property. Missing sources and serialization failures retain the
/// writeback's existing best-effort behavior.
pub(super) fn link_writeback_source(
    core: &GraphCore,
    node_id: &str,
    source_id: &str,
    relationship: &str,
) {
    if core.has_node(source_id) {
        writeback_relationship(core, node_id, source_id, relationship);
    }
}

pub(super) fn anomaly_algo_name(a: AnomalyAlgorithm) -> &'static str {
    match a {
        AnomalyAlgorithm::Zscore => "zscore",
        AnomalyAlgorithm::Isoforest => "isoforest",
        AnomalyAlgorithm::Lof => "lof",
        AnomalyAlgorithm::Ocsvm => "ocsvm",
    }
}

pub(super) fn anomaly_node_id(algo: &str, source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("anomaly:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Classification (fit / predict) ───────────────────────────

/// Handle `MineClassifyFit` (CONCEPT:EG-KG.mining.naive-bayes): build the feature rows
/// (explicit or node embeddings — the cross-modal "classify these nodes using their
/// embeddings + ontology features" hook), fit the chosen classifier, and return the
/// serializable model blob. PREDICTIVE + read-only (no graph mutation).
pub(super) struct ClassifyFitRequest<'a> {
    pub(super) x: Vec<Vec<f64>>,
    pub(super) source: Option<VectorSource>,
    #[cfg(feature = "query")]
    pub(super) plan: Option<crate::wire::Plan>,
    pub(super) y: Vec<i64>,
    pub(super) algorithm: ClassifyAlgorithm,
    pub(super) k: usize,
    pub(super) alpha: f64,
    pub(super) lr: f64,
    pub(super) epochs: usize,
    pub(super) l2: f64,
    pub(super) c: f64,
    #[cfg(all(feature = "query", feature = "tsdb"))]
    pub(super) tsdb: MiningTsdbBind<'a>,
    #[cfg(not(all(feature = "query", feature = "tsdb")))]
    pub(super) marker: std::marker::PhantomData<&'a ()>,
}

pub(super) fn handle_classify_fit(
    req_id: u64,
    core: &Arc<GraphCore>,
    request: ClassifyFitRequest<'_>,
) -> Response {
    let ClassifyFitRequest {
        x,
        source,
        #[cfg(feature = "query")]
        plan,
        y,
        algorithm,
        k,
        alpha,
        lr,
        epochs,
        l2,
        c,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
        #[cfg(not(all(feature = "query", feature = "tsdb")))]
            marker: _,
    } = request;
    let (rows, _ids) = match build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = classify_algo(algorithm, k, alpha, lr, epochs, l2, c);
    match classify::fit(&rows, &y, algo) {
        Ok(model) => Response::ok(
            req_id,
            ResultPayload::of::<results::MineClassifyFit>(ClassifierFitResult {
                classes: classify_classes(&model),
                model,
                algorithm: classify_algo_name(algorithm).to_string(),
                n_samples: rows.len(),
            }),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// Handle `MineClassifyPredict` (CONCEPT:EG-KG.mining.naive-bayes): build rows, run the
/// fitted model, return per-row `{id, label, proba}`, and optionally write
/// `:Classification` nodes back for each prediction.
pub(crate) struct ClassifyPredictRequest {
    pub(crate) model: FittedClassifier,
    pub(crate) x: Vec<Vec<f64>>,
    pub(crate) source: Option<VectorSource>,
    #[cfg(feature = "query")]
    pub(crate) plan: Option<crate::wire::Plan>,
    pub(crate) writeback: WritebackOptions,
}

pub(crate) fn handle_classify_predict(
    req_id: u64,
    core: &Arc<GraphCore>,
    request: ClassifyPredictRequest,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let ClassifyPredictRequest {
        model,
        x,
        source,
        #[cfg(feature = "query")]
        plan,
        writeback,
    } = request;
    let (rows, ids) = match build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let out = classify::predict(&model, &rows);

    let written = if writeback.enabled {
        materialize_classifications(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_classification_claims(core, &out, &ids, classify_provenance(&source));
    }

    let classified: Vec<ClassifiedRow> = (0..rows.len())
        .map(|i| ClassifiedRow {
            id: RowRef::at(&ids, i),
            label: out.labels[i],
            proba: out.proba[i].clone(),
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::of::<results::MineClassifyPredict>(ClassificationMiningResult {
            rows: classified,
            classes: out.classes,
            n_rows: rows.len(),
            written_back: written,
        }),
    )
}

pub(super) fn classify_algo(
    a: ClassifyAlgorithm,
    k: usize,
    alpha: f64,
    lr: f64,
    epochs: usize,
    l2: f64,
    c: f64,
) -> classify::Algorithm {
    match a {
        ClassifyAlgorithm::Gaussiannb => classify::Algorithm::GaussianNb,
        ClassifyAlgorithm::Multinomialnb => classify::Algorithm::MultinomialNb { alpha },
        ClassifyAlgorithm::Knn => classify::Algorithm::Knn { k },
        ClassifyAlgorithm::Logistic => classify::Algorithm::Logistic { lr, epochs, l2 },
        ClassifyAlgorithm::Svc => classify::Algorithm::LinearSvc { c, epochs, lr },
    }
}

pub(super) fn classify_algo_name(a: ClassifyAlgorithm) -> &'static str {
    match a {
        ClassifyAlgorithm::Gaussiannb => "gaussiannb",
        ClassifyAlgorithm::Multinomialnb => "multinomialnb",
        ClassifyAlgorithm::Knn => "knn",
        ClassifyAlgorithm::Logistic => "logistic",
        ClassifyAlgorithm::Svc => "svc",
    }
}

/// The sorted class set embedded in a fitted model (for the fit response).
pub(super) fn classify_classes(model: &FittedClassifier) -> Vec<i64> {
    match model {
        FittedClassifier::GaussianNb { classes, .. }
        | FittedClassifier::MultinomialNb { classes, .. }
        | FittedClassifier::Knn { classes, .. }
        | FittedClassifier::LinearOvr { classes, .. } => classes.clone(),
    }
}

/// Materialize each prediction as a typed `:Classification` node (CONCEPT:EG-KG.mining.classify-writeback),
/// id = a deterministic digest of the source node-id / row index. Linked to its source
/// node via a `CLASSIFIED_AS` edge when that node is resident.
pub(super) fn materialize_classifications(
    core: &GraphCore,
    out: &classify::Classification,
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for i in 0..out.labels.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = classification_node_id(&src);
        let props = serde_json::json!({
            "type": "Classification",
            "label": out.labels[i],
            "proba": out.proba[i],
            "classes": out.classes,
            "source": src,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        link_writeback_source(core, &node_id, &src, "CLASSIFIED_AS");
        written += 1;
    }
    written
}

pub(super) fn classification_node_id(source: &str) -> String {
    WritebackNodeId::new("classification", &[source]).into_string()
}

// ─────────────────────────── Dimensionality reduction ───────────────────────────

/// Handle `MineReduce` (CONCEPT:EG-KG.mining.truncated-svd): build rows (explicit or
/// node embeddings — reduce node vectors for the graphviz), run the chosen reduction,
/// return per-row `{id, coords}`, and optionally write `:Embedding2D` nodes back.
pub(crate) struct ReduceRequest {
    pub(crate) x: Vec<Vec<f64>>,
    pub(crate) source: Option<VectorSource>,
    #[cfg(feature = "query")]
    pub(crate) plan: Option<crate::wire::Plan>,
    pub(crate) labels: Vec<i64>,
    pub(crate) algorithm: ReduceAlgorithm,
    pub(crate) n_components: usize,
    pub(crate) n_neighbors: usize,
    pub(crate) min_dist: f64,
    pub(crate) perplexity: f64,
    pub(crate) epochs: usize,
    pub(crate) lr: f64,
    pub(crate) seed: u64,
    pub(crate) writeback: WritebackOptions,
}

pub(crate) fn handle_reduce(
    req_id: u64,
    core: &Arc<GraphCore>,
    request: ReduceRequest,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Response {
    let (rows, ids) = match build_vectors(
        core,
        &request.x,
        &request.source,
        #[cfg(feature = "query")]
        &request.plan,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb,
    ) {
        Ok(v) => v,
        Err(e) => return Response::err(req_id, e),
    };
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    if matches!(request.algorithm, ReduceAlgorithm::Lda) && request.labels.len() != rows.len() {
        return Response::err(
            req_id,
            "mining: LDA requires one label per row (supervised)",
        );
    }
    let algo = reduce_algo(
        request.algorithm,
        request.n_neighbors,
        request.min_dist,
        request.perplexity,
        request.epochs,
        request.lr,
        request.seed,
    );
    let lbls = (!request.labels.is_empty()).then_some(request.labels.as_slice());
    let out = reduce::reduce(&rows, lbls, algo, request.n_components);

    let written = if request.writeback.enabled {
        materialize_embeddings(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if request.writeback.enabled && request.writeback.as_claim {
        materialize_reduce_claims(core, &rows, &out, &ids, request.algorithm, &request.source);
    }

    let n_components = out.coords.first().map(|c| c.len()).unwrap_or(0);
    // Singular values are published only by an algorithm that produces them.
    let singular_values = (!out.singular_values.is_empty()).then_some(out.singular_values);
    let reduced: Vec<ReducedRow> = out
        .coords
        .into_iter()
        .enumerate()
        .map(|(i, coords)| ReducedRow {
            id: RowRef::at(&ids, i),
            coords,
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::of::<results::MineReduce>(ReductionMiningResult {
            rows: reduced,
            algorithm: reduce_algo_name(request.algorithm).to_string(),
            n_rows: rows.len(),
            n_components,
            written_back: written,
            singular_values,
        }),
    )
}

pub(super) fn reduce_algo(
    a: ReduceAlgorithm,
    n_neighbors: usize,
    min_dist: f64,
    perplexity: f64,
    epochs: usize,
    lr: f64,
    seed: u64,
) -> reduce::Algorithm {
    match a {
        ReduceAlgorithm::Svd => reduce::Algorithm::TruncatedSvd,
        ReduceAlgorithm::Lda => reduce::Algorithm::Lda,
        ReduceAlgorithm::Umap => reduce::Algorithm::Umap {
            n_neighbors,
            min_dist,
            epochs,
            seed,
        },
        ReduceAlgorithm::Tsne => reduce::Algorithm::Tsne {
            perplexity,
            epochs,
            learning_rate: lr,
            seed,
        },
    }
}

pub(super) fn reduce_algo_name(a: ReduceAlgorithm) -> &'static str {
    match a {
        ReduceAlgorithm::Svd => "svd",
        ReduceAlgorithm::Lda => "lda",
        ReduceAlgorithm::Umap => "umap",
        ReduceAlgorithm::Tsne => "tsne",
    }
}

/// Materialize each row's reduced vector as a typed `:Embedding2D` node
/// (CONCEPT:EG-KG.mining.reduce-writeback), id = a deterministic digest of the source
/// node-id / row index. Linked to its source node via a `REDUCED_FROM` edge when that
/// node is resident — feeding the web-UI graphviz + downstream clustering.
pub(super) fn materialize_embeddings(
    core: &GraphCore,
    out: &reduce::Reduction,
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for (i, coords) in out.coords.iter().enumerate() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = embedding2d_node_id(&src);
        let props = serde_json::json!({
            "type": "Embedding2D",
            "coords": coords,
            "dims": coords.len(),
            "source": src,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        link_writeback_source(core, &node_id, &src, "REDUCED_FROM");
        written += 1;
    }
    written
}

pub(super) fn embedding2d_node_id(source: &str) -> String {
    WritebackNodeId::new("embedding2d", &[source]).into_string()
}
