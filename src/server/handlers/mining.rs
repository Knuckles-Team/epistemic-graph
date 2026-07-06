//! Data-mining ops (CONCEPT:EG-KG.mining.frequent-itemset-mining): association-rule
//! mining over EITHER explicit transactions OR a graph-derived transaction source
//! (compute-near-data), with optional KG write-back of the mined rules.
//!
//! Unlike the stateless finance/datascience handlers, mining is GRAPH-SCOPED: the
//! graph-derived source reads node neighborhoods off the live core, and write-back
//! materializes `:AssociationRule` nodes into the same core. So it routes in the
//! `dispatch_graph_op` chain with the graph core in hand (like the query/rdf
//! handlers) rather than the pre-graph pure-compute path.

// The Result router moves the large `Method` enum by value on the fall-through
// path; boxing the Err would allocate per non-mining request (see datascience.rs).
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use eg_compute::mining::anomaly;
use eg_compute::mining::association::{self, Algorithm, LabeledRule};
use eg_compute::mining::cluster;

use crate::graph::GraphCore;
use crate::protocol::{
    AnomalyAlgorithm, ClusterAlgorithm, Linkage, Method, MineAlgorithm, Response, ResultPayload,
    SvmKernel, TransactionSource, VectorSource,
};

/// Handle a `Mine*` method. `Err(method)` hands a non-mining method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention.)
pub(crate) fn try_handle(
    req_id: u64,
    core: Arc<GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::MineAssociate {
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
        } => Ok(handle_associate(
            req_id,
            &core,
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback,
        )),
        Method::MineCluster {
            features,
            source,
            algorithm,
            eps,
            min_pts,
            k,
            linkage,
            max_iter,
            seed,
            writeback,
        } => Ok(handle_cluster(
            req_id, &core, features, source, algorithm, eps, min_pts, k, linkage, max_iter, seed,
            writeback,
        )),
        Method::MineAnomaly {
            features,
            values,
            source,
            algorithm,
            k,
            n_trees,
            sample_size,
            seed,
            nu,
            gamma,
            kernel,
            threshold,
            writeback,
        } => Ok(handle_anomaly(
            req_id,
            &core,
            features,
            values,
            source,
            algorithm,
            k,
            n_trees,
            sample_size,
            seed,
            nu,
            gamma,
            kernel,
            threshold,
            writeback,
        )),
        other => Err(other),
    }
}

/// Re-run a `MineAssociate` purely for its write-back side effect on WAL replay
/// (CONCEPT:EG-KG.mining.frequent-itemset-mining). Deterministic for explicit
/// transactions; a graph-derived source re-derives from the current graph state
/// (like the broker/memory replay ops). The response is discarded.
#[allow(dead_code)]
pub(crate) fn replay(core: &GraphCore, method: &Method) {
    match method {
        Method::MineAssociate {
            transactions,
            source,
            min_support,
            min_confidence,
            algorithm,
            writeback: true,
        } => {
            let txns = match build_transactions(core, transactions, source) {
                Ok(t) => t,
                Err(_) => return,
            };
            let rules = association::mine_labeled(
                &txns,
                *min_support,
                *min_confidence,
                to_algo(*algorithm),
            );
            materialize_rules(core, &rules);
        }
        Method::MineCluster {
            features,
            source,
            algorithm,
            eps,
            min_pts,
            k,
            linkage,
            max_iter,
            seed,
            writeback: true,
        } => {
            let (rows, ids) = build_vectors(core, features, source);
            if rows.is_empty() {
                return;
            }
            let algo = cluster_algo(*algorithm, *eps, *min_pts, *k, *linkage, *max_iter, *seed);
            let out = cluster::cluster(&rows, algo);
            materialize_clusters(core, &out, &ids, *algorithm);
        }
        Method::MineAnomaly {
            features,
            values,
            source,
            algorithm,
            k,
            n_trees,
            sample_size,
            seed,
            nu,
            gamma,
            kernel,
            threshold,
            writeback: true,
        } => {
            let (rows, ids) = build_anomaly_rows(core, features, values, source);
            if rows.is_empty() {
                return;
            }
            let algo = anomaly_algo(
                *algorithm,
                *k,
                *n_trees,
                *sample_size,
                *seed,
                *nu,
                *gamma,
                *kernel,
            );
            let out = anomaly::detect(&rows, algo, *threshold);
            materialize_anomalies(core, &out, &ids, *algorithm);
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_associate(
    req_id: u64,
    core: &GraphCore,
    transactions: Vec<Vec<String>>,
    source: Option<TransactionSource>,
    min_support: f64,
    min_confidence: f64,
    algorithm: MineAlgorithm,
    writeback: bool,
) -> Response {
    let txns = match build_transactions(core, &transactions, &source) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };
    let rules = association::mine_labeled(&txns, min_support, min_confidence, to_algo(algorithm));

    let written = if writeback {
        materialize_rules(core, &rules)
    } else {
        0
    };

    let rows: Vec<serde_json::Value> = rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "antecedent": r.antecedent,
                "consequent": r.consequent,
                "support": r.support,
                "confidence": r.confidence,
                "lift": r.lift,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rules": rows,
            "n_transactions": txns.len(),
            "n_rules": rules.len(),
            "written_back": written,
        })),
    )
}

/// Resolve the transaction set: explicit `transactions` win; otherwise derive them
/// from the graph via `source`. An empty request (neither provided) yields no
/// transactions (⇒ no rules), which is a valid empty result, not an error.
fn build_transactions(
    core: &GraphCore,
    transactions: &[Vec<String>],
    source: &Option<TransactionSource>,
) -> Result<Vec<Vec<String>>, String> {
    if !transactions.is_empty() {
        return Ok(transactions.to_vec());
    }
    match source {
        Some(spec) => Ok(derive_from_graph(core, spec)),
        None => Ok(Vec::new()),
    }
}

/// Build one transaction per `node_label` instance from its neighbor set
/// (CONCEPT:EG-KG.mining.graph-derived-transactions). Each transaction is the deduped
/// set of `item_field` values over the owner's neighbors in `direction`, optionally
/// filtered to a `relation`.
fn derive_from_graph(core: &GraphCore, spec: &TransactionSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let neighbors = neighbors_in_direction(core, &owner_id, &spec.direction);
        let mut basket: Vec<String> = Vec::new();
        for nbr in neighbors {
            if let Some(rel) = &spec.relation {
                if !edge_matches_relation(core, &owner_id, &nbr, &spec.direction, rel) {
                    continue;
                }
            }
            if let Some(item) = extract_item(core, &nbr, &spec.item_field) {
                basket.push(item);
            }
        }
        basket.sort_unstable();
        basket.dedup();
        if !basket.is_empty() {
            out.push(basket);
        }
    }
    out
}

fn neighbors_in_direction(core: &GraphCore, node_id: &str, direction: &str) -> Vec<String> {
    match direction {
        "in" => core.get_predecessors(node_id).unwrap_or_default(),
        "any" => {
            let mut v = core.get_successors(node_id).unwrap_or_default();
            v.extend(core.get_predecessors(node_id).unwrap_or_default());
            v.sort_unstable();
            v.dedup();
            v
        }
        // "out" (default) and anything else.
        _ => core.get_successors(node_id).unwrap_or_default(),
    }
}

/// Whether an edge between owner and neighbor carries `relation` on its
/// `relation`/`type` property. Checks both directions when `direction == "any"`.
fn edge_matches_relation(
    core: &GraphCore,
    owner: &str,
    neighbor: &str,
    direction: &str,
    relation: &str,
) -> bool {
    let pairs: &[(&str, &str)] = match direction {
        "in" => &[(neighbor, owner)],
        "any" => &[(owner, neighbor), (neighbor, owner)],
        _ => &[(owner, neighbor)],
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

/// Extract the item value for `neighbor` per `item_field`:
///   * `None`         ⇒ the neighbor's node id.
///   * `"label"`      ⇒ the neighbor's type/label.
///   * `"prop:<key>"` ⇒ the neighbor's property `<key>`.
fn extract_item(core: &GraphCore, neighbor: &str, item_field: &Option<String>) -> Option<String> {
    let field = match item_field {
        None => return Some(neighbor.to_string()),
        Some(f) => f.as_str(),
    };
    let props = core.get_node_properties(neighbor)?;
    let val: serde_json::Value = rmp_serde::from_slice(&props).ok()?;
    if field == "label" {
        for key in ["type", "node_type", "label"] {
            if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
                return Some(s.to_string());
            }
        }
        return None;
    }
    if let Some(key) = field.strip_prefix("prop:") {
        return val.get(key).and_then(json_scalar_string);
    }
    // Bare field name ⇒ treat as a property key.
    val.get(field).and_then(json_scalar_string)
}

fn json_scalar_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Materialize each rule as a typed `:AssociationRule` node (the discovery
/// flywheel, CONCEPT:EG-KG.mining.rule-writeback). The node id is a deterministic
/// digest of `antecedent ⇒ consequent` so replay is idempotent. Each rule is linked
/// (best-effort) to any item that is itself a resident node id, via a `RULE_ITEM`
/// edge — so OWL reasoning + the next mining pass can traverse from the rule to its
/// sources. Returns the number of rule nodes written.
fn materialize_rules(core: &GraphCore, rules: &[LabeledRule]) -> usize {
    let mut written = 0usize;
    for r in rules {
        let node_id = rule_node_id(&r.antecedent, &r.consequent);
        let props = serde_json::json!({
            "type": "AssociationRule",
            "antecedent": r.antecedent,
            "consequent": r.consequent,
            "support": r.support,
            "confidence": r.confidence,
            "lift": r.lift,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        // Link the rule to any item that is a resident node (source objects).
        for item in r.antecedent.iter().chain(r.consequent.iter()) {
            if core.has_node(item) {
                let edge = serde_json::json!({ "relation": "RULE_ITEM" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), item.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a rule (order-stable — the items
/// are already sorted within each side by the rule generator).
fn rule_node_id(antecedent: &[String], consequent: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(antecedent.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    hasher.update(consequent.join("\u{1}").as_bytes());
    let digest = hasher.finalize();
    format!("assocrule:{}", hex::encode(&digest[..12]))
}

fn to_algo(a: MineAlgorithm) -> Algorithm {
    match a {
        MineAlgorithm::Apriori => Algorithm::Apriori,
        MineAlgorithm::Fpgrowth => Algorithm::FpGrowth,
        MineAlgorithm::Eclat => Algorithm::Eclat,
    }
}

// ─────────────────────────── Clustering ───────────────────────────

/// Handle `MineCluster` (CONCEPT:EG-KG.mining.dbscan-density): build the feature
/// rows (explicit or node embeddings), run the chosen clustering engine, return
/// `{clusters, labels, ...}`, and optionally write `:Cluster` nodes back.
#[allow(clippy::too_many_arguments)]
fn handle_cluster(
    req_id: u64,
    core: &GraphCore,
    features: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    algorithm: ClusterAlgorithm,
    eps: f64,
    min_pts: usize,
    k: usize,
    linkage: Linkage,
    max_iter: usize,
    seed: u64,
    writeback: bool,
) -> Response {
    let (rows, ids) = build_vectors(core, &features, &source);
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = cluster_algo(algorithm, eps, min_pts, k, linkage, max_iter, seed);
    let out = cluster::cluster(&rows, algo);

    let written = if writeback {
        materialize_clusters(core, &out, &ids, algorithm)
    } else {
        0
    };

    let cluster_rows: Vec<serde_json::Value> = out
        .clusters
        .iter()
        .map(|c| {
            // Report member node ids when the rows came from a node source, else the
            // raw row indices.
            let members: Vec<serde_json::Value> = c
                .members
                .iter()
                .map(|&i| match ids.get(i) {
                    Some(id) => serde_json::Value::String(id.clone()),
                    None => serde_json::json!(i),
                })
                .collect();
            serde_json::json!({
                "cluster_id": c.cluster_id,
                "members": members,
                "centroid": c.centroid,
                "score": c.score,
            })
        })
        .collect();

    let mut payload = serde_json::json!({
        "clusters": cluster_rows,
        "labels": out.labels,
        "n_rows": rows.len(),
        "n_clusters": out.clusters.iter().filter(|c| c.cluster_id >= 0).count(),
        "written_back": written,
    });
    if let Some(resp) = &out.responsibilities {
        payload["responsibilities"] = serde_json::json!(resp);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

fn cluster_algo(
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

fn to_linkage(l: Linkage) -> cluster::Linkage {
    match l {
        Linkage::Single => cluster::Linkage::Single,
        Linkage::Complete => cluster::Linkage::Complete,
        Linkage::Average => cluster::Linkage::Average,
    }
}

/// Materialize each non-noise cluster as a typed `:Cluster` node (CONCEPT:EG-KG.mining.cluster-writeback),
/// id = a deterministic digest of `algo` + its sorted member node-ids (idempotent
/// replay). Members that are resident nodes are linked via `CLUSTER_MEMBER` edges.
fn materialize_clusters(
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
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for mid in &member_ids {
            if core.has_node(mid) {
                let edge = serde_json::json!({ "relation": "CLUSTER_MEMBER" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), mid.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn cluster_algo_name(a: ClusterAlgorithm) -> &'static str {
    match a {
        ClusterAlgorithm::Dbscan => "dbscan",
        ClusterAlgorithm::Hierarchical => "hierarchical",
        ClusterAlgorithm::Gmm => "gmm",
        ClusterAlgorithm::Kmedoids => "kmedoids",
    }
}

fn cluster_node_id(algo: &str, member_ids: &[String]) -> String {
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
#[allow(clippy::too_many_arguments)]
fn handle_anomaly(
    req_id: u64,
    core: &GraphCore,
    features: Vec<Vec<f64>>,
    values: Vec<f64>,
    source: Option<VectorSource>,
    algorithm: AnomalyAlgorithm,
    k: usize,
    n_trees: usize,
    sample_size: usize,
    seed: u64,
    nu: f64,
    gamma: f64,
    kernel: SvmKernel,
    threshold: Option<f64>,
    writeback: bool,
) -> Response {
    let (rows, ids) = build_anomaly_rows(core, &features, &values, &source);
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = anomaly_algo(algorithm, k, n_trees, sample_size, seed, nu, gamma, kernel);
    let out = anomaly::detect(&rows, algo, threshold);

    let written = if writeback {
        materialize_anomalies(core, &out, &ids, algorithm)
    } else {
        0
    };

    let rows_json: Vec<serde_json::Value> = (0..rows.len())
        .map(|i| {
            let id = match ids.get(i) {
                Some(id) => serde_json::Value::String(id.clone()),
                None => serde_json::json!(i),
            };
            serde_json::json!({
                "id": id,
                "anomaly_score": out.scores[i],
                "is_anomaly": out.is_anomaly[i],
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rows": rows_json,
            "n_rows": rows.len(),
            "n_anomalies": out.is_anomaly.iter().filter(|&&a| a).count(),
            "threshold": out.threshold,
            "written_back": written,
        })),
    )
}

#[allow(clippy::too_many_arguments)]
fn anomaly_algo(
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
fn materialize_anomalies(
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
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relation": "ANOMALY_OF" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn anomaly_algo_name(a: AnomalyAlgorithm) -> &'static str {
    match a {
        AnomalyAlgorithm::Zscore => "zscore",
        AnomalyAlgorithm::Isoforest => "isoforest",
        AnomalyAlgorithm::Lof => "lof",
        AnomalyAlgorithm::Ocsvm => "ocsvm",
    }
}

fn anomaly_node_id(algo: &str, source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("anomaly:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Row builders (shared) ───────────────────────────

/// Resolve the cluster feature rows: explicit `features` win; otherwise gather the
/// embeddings of the `source` node label (the cross-modal hook). Returns the rows
/// AND a parallel `ids` vec (node ids for the embedding path, empty for explicit).
fn build_vectors(
    core: &GraphCore,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// Resolve the anomaly rows: explicit `features` win, then a 1-D `values` series
/// (each scalar → a one-element row — the tsdb RCA path), then node embeddings.
fn build_anomaly_rows(
    core: &GraphCore,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    if !values.is_empty() {
        return (values.iter().map(|&v| vec![v]).collect(), Vec::new());
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// Gather the stored embedding of every node carrying `spec.node_label` (skipping
/// nodes without one). Compute-near-data: the vectors are read straight off the
/// resident semantic store.
fn gather_embeddings(core: &GraphCore, spec: &VectorSource) -> (Vec<Vec<f64>>, Vec<String>) {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let store = core.semantic_store.read();
    let mut rows: Vec<Vec<f64>> = Vec::with_capacity(owners.len());
    let mut ids: Vec<String> = Vec::with_capacity(owners.len());
    for (node_id, _blob) in owners {
        if let Some(vec) = store.get_embedding(&node_id) {
            rows.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (rows, ids)
}

/// Reject a ragged feature matrix (rows of differing width) with a clean error
/// rather than letting a distance computation panic; an empty matrix is allowed
/// (⇒ an empty result).
fn validate_matrix(rows: &[Vec<f64>]) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let width = rows[0].len();
    if width == 0 {
        return Err("mining: feature rows must be non-empty".into());
    }
    if rows.iter().any(|r| r.len() != width) {
        return Err("mining: all feature rows must have the same dimensionality".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;
    use crate::protocol::TransactionSource;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    #[test]
    fn non_mining_method_falls_through() {
        let core = Arc::new(GraphCore::new());
        let m = Method::NodeCount;
        assert!(matches!(try_handle(1, core, m), Err(Method::NodeCount)));
    }

    #[test]
    fn explicit_transactions_produce_rules() {
        let core = Arc::new(GraphCore::new());
        let txns = vec![
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "c".to_string()],
            vec!["b".to_string(), "c".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
        ];
        let m = Method::MineAssociate {
            transactions: txns,
            source: None,
            min_support: 0.4,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Apriori,
            writeback: false,
        };
        let resp = try_handle(7, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 5);
        assert!(v["n_rules"].as_u64().unwrap() > 0);
        assert_eq!(v["written_back"], 0);
    }

    #[test]
    fn graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Two "Cart" owners, each linked to its purchased "Item" nodes.
        core.add_node("cart1".into(), node(serde_json::json!({"type": "Cart"})));
        core.add_node("cart2".into(), node(serde_json::json!({"type": "Cart"})));
        for item in ["milk", "bread"] {
            core.add_node(item.into(), node(serde_json::json!({"type": "Item"})));
        }
        let _ = core.add_edge("cart1".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart1".into(), "bread".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "milk".into(), node(serde_json::json!({})));
        let _ = core.add_edge("cart2".into(), "bread".into(), node(serde_json::json!({})));

        let m = Method::MineAssociate {
            transactions: Vec::new(),
            source: Some(TransactionSource {
                node_label: "Cart".into(),
                direction: "out".into(),
                item_field: None, // neighbor node id ⇒ "milk"/"bread"
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: MineAlgorithm::Fpgrowth,
            writeback: true,
        };
        let resp = try_handle(9, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_transactions"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        // The dispatch shell calls `mark_dirty()` after a write (invalidating the
        // lazy label index); mirror that here so the label query sees the new nodes.
        core.mark_dirty();
        // Write-back created queryable :AssociationRule nodes.
        let rule_nodes = core.get_nodes_by_label("AssociationRule", 0);
        assert_eq!(rule_nodes.len() as u64, written);
    }

    #[test]
    fn cluster_explicit_features_dbscan() {
        let core = Arc::new(GraphCore::new());
        let features = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.1],
            vec![0.2, 0.0],
            vec![10.0, 10.0],
            vec![10.1, 9.9],
            vec![10.0, 10.2],
        ];
        let m = Method::MineCluster {
            features,
            source: None,
            algorithm: ClusterAlgorithm::Dbscan,
            eps: 1.0,
            min_pts: 2,
            k: 3,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: false,
        };
        let resp = try_handle(1, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        assert_eq!(v["n_clusters"], 2);
        assert_eq!(v["written_back"], 0);
    }

    #[test]
    fn cluster_over_node_embeddings_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Six :Doc nodes with 2-D embeddings forming two groups.
        let embs = [
            ("d0", [0.0f32, 0.0]),
            ("d1", [0.2, 0.1]),
            ("d2", [0.1, 0.2]),
            ("d3", [9.0, 9.0]),
            ("d4", [9.2, 8.9]),
            ("d5", [8.9, 9.1]),
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Doc"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec());
        }
        let m = Method::MineCluster {
            features: Vec::new(),
            source: Some(VectorSource {
                node_label: "Doc".into(),
                limit: 0,
            }),
            algorithm: ClusterAlgorithm::Kmedoids,
            eps: 0.5,
            min_pts: 5,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: true,
        };
        let resp = try_handle(2, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        assert_eq!(v["n_clusters"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        // Members are reported as node ids (not indices).
        let first = &v["clusters"][0]["members"][0];
        assert!(first.is_string());
        core.mark_dirty();
        let cluster_nodes = core.get_nodes_by_label("Cluster", 0);
        assert_eq!(cluster_nodes.len() as u64, written);
    }

    #[test]
    fn anomaly_values_series_zscore_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // A flat series with one spike — the tsdb RCA path via `values`.
        let mut values: Vec<f64> = (0..10).map(|i| 1.0 + 0.01 * i as f64).collect();
        values.push(100.0); // the anomaly
        let m = Method::MineAnomaly {
            features: Vec::new(),
            values,
            source: None,
            algorithm: AnomalyAlgorithm::Zscore,
            k: 20,
            n_trees: 100,
            sample_size: 256,
            seed: 0,
            nu: 0.1,
            gamma: 0.0,
            kernel: SvmKernel::Rbf,
            threshold: None,
            writeback: false,
        };
        let resp = try_handle(3, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 11);
        assert_eq!(v["n_anomalies"], 1);
        // The spike (last row) is the flagged one.
        let rows = v["rows"].as_array().unwrap();
        assert!(rows[10]["is_anomaly"].as_bool().unwrap());
    }

    #[test]
    fn anomaly_over_node_embeddings_writeback_links_source() {
        let core = Arc::new(GraphCore::new());
        let embs = [
            ("m0", [0.0f32, 0.0]),
            ("m1", [0.1, 0.0]),
            ("m2", [0.0, 0.1]),
            ("m3", [0.1, 0.1]),
            ("m4", [0.05, 0.05]),
            ("m5", [50.0, 50.0]), // outlier
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Metric"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec());
        }
        let m = Method::MineAnomaly {
            features: Vec::new(),
            values: Vec::new(),
            source: Some(VectorSource {
                node_label: "Metric".into(),
                limit: 0,
            }),
            algorithm: AnomalyAlgorithm::Zscore,
            k: 20,
            n_trees: 100,
            sample_size: 256,
            seed: 0,
            nu: 0.1,
            gamma: 0.0,
            kernel: SvmKernel::Rbf,
            threshold: None,
            writeback: true,
        };
        let resp = try_handle(4, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 6);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written >= 1);
        core.mark_dirty();
        let anomaly_nodes = core.get_nodes_by_label("Anomaly", 0);
        assert_eq!(anomaly_nodes.len() as u64, written);
        // The anomaly node links to its source (m5) via ANOMALY_OF.
        let succ = core.get_successors(&anomaly_nodes[0].0).unwrap_or_default();
        assert!(succ.iter().any(|s| s == "m5"));
    }
}
