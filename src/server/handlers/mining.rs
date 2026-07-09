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
use eg_compute::mining::classify::{self, FittedClassifier};
use eg_compute::mining::cluster;
use eg_compute::mining::forecast;
use eg_compute::mining::reduce;
use eg_compute::mining::sequence::{self, LabeledPattern};
use eg_compute::mining::subgraph::{self, HostGraph};
use eg_compute::mining::text;

use crate::graph::GraphCore;
use crate::protocol::{
    AnomalyAlgorithm, ClassifyAlgorithm, ClusterAlgorithm, ForecastAlgorithm, Linkage, Method,
    MineAlgorithm, MineSeqAlgorithm, ReduceAlgorithm, Response, ResultPayload, SequenceSource,
    SubgraphAlgorithm, SvmKernel, TextAlgorithm, TextSource, TransactionSource, VectorSource,
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
        } => Ok(handle_cluster(
            req_id,
            &core,
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
        )),
        Method::MineAnomaly {
            features,
            values,
            source,
            #[cfg(feature = "query")]
            plan,
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
            #[cfg(feature = "query")]
            plan,
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
        Method::MineClassifyFit {
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
        } => Ok(handle_classify_fit(
            req_id,
            &core,
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
        )),
        Method::MineClassifyPredict {
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback,
        } => Ok(handle_classify_predict(
            req_id,
            &core,
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback,
        )),
        Method::MineReduce {
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            labels,
            algorithm,
            n_components,
            n_neighbors,
            min_dist,
            perplexity,
            epochs,
            lr,
            seed,
            writeback,
        } => Ok(handle_reduce(
            req_id,
            &core,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            labels,
            algorithm,
            n_components,
            n_neighbors,
            min_dist,
            perplexity,
            epochs,
            lr,
            seed,
            writeback,
        )),
        Method::MineSequence {
            sequences,
            source,
            min_support,
            algorithm,
            writeback,
        } => Ok(handle_sequence(
            req_id,
            &core,
            sequences,
            source,
            min_support,
            algorithm,
            writeback,
        )),
        Method::MineForecast {
            values,
            algorithm,
            horizon,
            p,
            d,
            q,
            period,
            alpha,
            beta,
            gamma,
            confidence,
            series_id,
            writeback,
        } => Ok(handle_forecast(
            req_id, &core, values, algorithm, horizon, p, d, q, period, alpha, beta, gamma,
            confidence, series_id, writeback,
        )),
        Method::MineText {
            docs,
            source,
            algorithm,
            k,
            alpha,
            beta,
            iterations,
            seed,
            top_n,
            writeback,
        } => Ok(handle_text(
            req_id, &core, docs, source, algorithm, k, alpha, beta, iterations, seed, top_n,
            writeback,
        )),
        Method::MineSubgraph {
            label,
            min_support,
            max_edges,
            algorithm,
            writeback,
        } => Ok(handle_subgraph(
            req_id,
            &core,
            label,
            min_support,
            max_edges,
            algorithm,
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
            #[cfg(feature = "query")]
            plan,
            algorithm,
            eps,
            min_pts,
            k,
            linkage,
            max_iter,
            seed,
            writeback: true,
        } => {
            let (rows, ids) = build_vectors(
                core,
                features,
                source,
                #[cfg(feature = "query")]
                plan,
            );
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
            #[cfg(feature = "query")]
            plan,
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
            let (rows, ids) = build_anomaly_rows(
                core,
                features,
                values,
                source,
                #[cfg(feature = "query")]
                plan,
            );
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
        Method::MineClassifyPredict {
            model,
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            writeback: true,
        } => {
            let (rows, ids) = build_vectors(
                core,
                x,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let out = classify::predict(model, &rows);
            materialize_classifications(core, &out, &ids);
        }
        Method::MineReduce {
            x,
            source,
            #[cfg(feature = "query")]
            plan,
            labels,
            algorithm,
            n_components,
            n_neighbors,
            min_dist,
            perplexity,
            epochs,
            lr,
            seed,
            writeback: true,
        } => {
            let (rows, ids) = build_vectors(
                core,
                x,
                source,
                #[cfg(feature = "query")]
                plan,
            );
            if rows.is_empty() {
                return;
            }
            let algo = reduce_algo(
                *algorithm,
                *n_neighbors,
                *min_dist,
                *perplexity,
                *epochs,
                *lr,
                *seed,
            );
            let lbls = (!labels.is_empty()).then_some(labels.as_slice());
            let out = reduce::reduce(&rows, lbls, algo, *n_components);
            materialize_embeddings(core, &out, &ids);
        }
        Method::MineSequence {
            sequences,
            source,
            min_support,
            algorithm,
            writeback: true,
        } => {
            let seqs = match build_sequences(core, sequences, source) {
                Ok(s) => s,
                Err(_) => return,
            };
            let patterns = sequence::mine_labeled(&seqs, *min_support, to_seq_algo(*algorithm));
            materialize_patterns(core, &patterns);
        }
        Method::MineForecast {
            values,
            algorithm,
            horizon,
            p,
            d,
            q,
            period,
            alpha,
            beta,
            gamma,
            confidence,
            series_id,
            writeback: true,
        } => {
            if values.is_empty() {
                return;
            }
            let algo = forecast_algo(*algorithm, *p, *d, *q, *period, *alpha, *beta, *gamma);
            let out = forecast::forecast(values, algo, *horizon, *confidence);
            materialize_forecast(
                core,
                &out,
                *horizon,
                series_id,
                values,
                forecast_algo_name(*algorithm),
            );
        }
        Method::MineText {
            docs,
            source,
            algorithm,
            k,
            alpha,
            beta,
            iterations,
            seed,
            top_n,
            writeback: true,
        } => {
            if matches!(algorithm, TextAlgorithm::Tfidf) {
                return; // tfidf has no topics to write back
            }
            let (tokenized, ids) = build_text_docs(core, docs, source);
            if tokenized.is_empty() {
                return;
            }
            let algo = to_text_algo(*algorithm, *k, *alpha, *beta, *iterations, *seed);
            let out = text::mine_labeled(&tokenized, algo, *top_n);
            materialize_topics(core, &out, &ids, text_algo_name(*algorithm));
        }
        Method::MineSubgraph {
            label,
            min_support,
            max_edges,
            algorithm,
            writeback: true,
        } => {
            if matches!(algorithm, SubgraphAlgorithm::Motif) {
                return; // motif has no patterns to write back
            }
            let (host, ids) = build_host_graph(core, label);
            if host.node_count() == 0 {
                return;
            }
            let results = subgraph::mine_gspan(&host, *min_support, *max_edges);
            materialize_subgraphs(core, &results, &ids);
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
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    algorithm: ClusterAlgorithm,
    eps: f64,
    min_pts: usize,
    k: usize,
    linkage: Linkage,
    max_iter: usize,
    seed: u64,
    writeback: bool,
) -> Response {
    let (rows, ids) = build_vectors(
        core,
        &features,
        &source,
        #[cfg(feature = "query")]
        &plan,
    );
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
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
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
    let (rows, ids) = build_anomaly_rows(
        core,
        &features,
        &values,
        &source,
        #[cfg(feature = "query")]
        &plan,
    );
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

// ─────────────────────────── Classification (fit / predict) ───────────────────────────

/// Handle `MineClassifyFit` (CONCEPT:EG-KG.mining.naive-bayes): build the feature rows
/// (explicit or node embeddings — the cross-modal "classify these nodes using their
/// embeddings + ontology features" hook), fit the chosen classifier, and return the
/// serializable model blob. PREDICTIVE + read-only (no graph mutation).
#[allow(clippy::too_many_arguments)]
fn handle_classify_fit(
    req_id: u64,
    core: &GraphCore,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    y: Vec<i64>,
    algorithm: ClassifyAlgorithm,
    k: usize,
    alpha: f64,
    lr: f64,
    epochs: usize,
    l2: f64,
    c: f64,
) -> Response {
    let (rows, _ids) = build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
    );
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let algo = classify_algo(algorithm, k, alpha, lr, epochs, l2, c);
    match classify::fit(&rows, &y, algo) {
        Ok(model) => Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({
                "model": model,
                "algorithm": classify_algo_name(algorithm),
                "n_samples": rows.len(),
                "classes": classify_classes(&model),
            })),
        ),
        Err(e) => Response::err(req_id, e),
    }
}

/// Handle `MineClassifyPredict` (CONCEPT:EG-KG.mining.naive-bayes): build rows, run the
/// fitted model, return per-row `{id, label, proba}`, and optionally write
/// `:Classification` nodes back for each prediction.
fn handle_classify_predict(
    req_id: u64,
    core: &GraphCore,
    model: FittedClassifier,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    writeback: bool,
) -> Response {
    let (rows, ids) = build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
    );
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    let out = classify::predict(&model, &rows);

    let written = if writeback {
        materialize_classifications(core, &out, &ids)
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
                "label": out.labels[i],
                "proba": out.proba[i],
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "rows": rows_json,
            "classes": out.classes,
            "n_rows": rows.len(),
            "written_back": written,
        })),
    )
}

fn classify_algo(
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

fn classify_algo_name(a: ClassifyAlgorithm) -> &'static str {
    match a {
        ClassifyAlgorithm::Gaussiannb => "gaussiannb",
        ClassifyAlgorithm::Multinomialnb => "multinomialnb",
        ClassifyAlgorithm::Knn => "knn",
        ClassifyAlgorithm::Logistic => "logistic",
        ClassifyAlgorithm::Svc => "svc",
    }
}

/// The sorted class set embedded in a fitted model (for the fit response).
fn classify_classes(model: &FittedClassifier) -> Vec<i64> {
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
fn materialize_classifications(
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
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relation": "CLASSIFIED_AS" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn classification_node_id(source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"classification");
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("classification:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Dimensionality reduction ───────────────────────────

/// Handle `MineReduce` (CONCEPT:EG-KG.mining.truncated-svd): build rows (explicit or
/// node embeddings — reduce node vectors for the graphviz), run the chosen reduction,
/// return per-row `{id, coords}`, and optionally write `:Embedding2D` nodes back.
#[allow(clippy::too_many_arguments)]
fn handle_reduce(
    req_id: u64,
    core: &GraphCore,
    x: Vec<Vec<f64>>,
    source: Option<VectorSource>,
    #[cfg(feature = "query")] plan: Option<crate::wire::Plan>,
    labels: Vec<i64>,
    algorithm: ReduceAlgorithm,
    n_components: usize,
    n_neighbors: usize,
    min_dist: f64,
    perplexity: f64,
    epochs: usize,
    lr: f64,
    seed: u64,
    writeback: bool,
) -> Response {
    let (rows, ids) = build_vectors(
        core,
        &x,
        &source,
        #[cfg(feature = "query")]
        &plan,
    );
    if let Err(e) = validate_matrix(&rows) {
        return Response::err(req_id, e);
    }
    if matches!(algorithm, ReduceAlgorithm::Lda) && labels.len() != rows.len() {
        return Response::err(
            req_id,
            "mining: LDA requires one label per row (supervised)",
        );
    }
    let algo = reduce_algo(
        algorithm,
        n_neighbors,
        min_dist,
        perplexity,
        epochs,
        lr,
        seed,
    );
    let lbls = (!labels.is_empty()).then_some(labels.as_slice());
    let out = reduce::reduce(&rows, lbls, algo, n_components);

    let written = if writeback {
        materialize_embeddings(core, &out, &ids)
    } else {
        0
    };

    let rows_json: Vec<serde_json::Value> = (0..out.coords.len())
        .map(|i| {
            let id = match ids.get(i) {
                Some(id) => serde_json::Value::String(id.clone()),
                None => serde_json::json!(i),
            };
            serde_json::json!({ "id": id, "coords": out.coords[i] })
        })
        .collect();

    let mut payload = serde_json::json!({
        "rows": rows_json,
        "algorithm": reduce_algo_name(algorithm),
        "n_rows": rows.len(),
        "n_components": out.coords.first().map(|c| c.len()).unwrap_or(0),
        "written_back": written,
    });
    if !out.singular_values.is_empty() {
        payload["singular_values"] = serde_json::json!(out.singular_values);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

fn reduce_algo(
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

fn reduce_algo_name(a: ReduceAlgorithm) -> &'static str {
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
fn materialize_embeddings(core: &GraphCore, out: &reduce::Reduction, ids: &[String]) -> usize {
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
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        if core.has_node(&src) {
            let edge = serde_json::json!({ "relation": "REDUCED_FROM" });
            if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                let _ = core.add_edge(node_id.clone(), src.clone(), eb);
            }
        }
        written += 1;
    }
    written
}

fn embedding2d_node_id(source: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"embedding2d");
    hasher.update([0u8]);
    hasher.update(source.as_bytes());
    format!("embedding2d:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Sequential-pattern mining ───────────────────────────

/// Handle `MineSequence` (CONCEPT:EG-KG.mining.prefixspan — Phase 4): build the
/// ordered sequences (explicit or graph-derived), run the chosen engine
/// (PrefixSpan/GSP — both agree), return `{patterns, ...}`, and optionally write
/// `:SequentialPattern` nodes back.
fn handle_sequence(
    req_id: u64,
    core: &GraphCore,
    sequences: Vec<Vec<String>>,
    source: Option<SequenceSource>,
    min_support: f64,
    algorithm: MineSeqAlgorithm,
    writeback: bool,
) -> Response {
    let seqs = match build_sequences(core, &sequences, &source) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, e),
    };
    let patterns = sequence::mine_labeled(&seqs, min_support, to_seq_algo(algorithm));

    let written = if writeback {
        materialize_patterns(core, &patterns)
    } else {
        0
    };

    let rows: Vec<serde_json::Value> = patterns
        .iter()
        .map(|p| {
            serde_json::json!({
                "items": p.items,
                "support": p.support,
                "count": p.count,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "patterns": rows,
            "n_sequences": seqs.len(),
            "n_patterns": patterns.len(),
            "written_back": written,
        })),
    )
}

/// Resolve the sequence set: explicit `sequences` win; otherwise derive them
/// from the graph via `source`. An empty request yields no sequences (⇒ no
/// patterns), a valid empty result.
fn build_sequences(
    core: &GraphCore,
    sequences: &[Vec<String>],
    source: &Option<SequenceSource>,
) -> Result<Vec<Vec<String>>, String> {
    if !sequences.is_empty() {
        return Ok(sequences.to_vec());
    }
    match source {
        Some(spec) => Ok(derive_sequences_from_graph(core, spec)),
        None => Ok(Vec::new()),
    }
}

/// Build one ORDERED sequence per `node_label` instance from its neighbor list
/// (CONCEPT:EG-KG.mining.prefixspan): each sequence is the `item_field` values of
/// the owner's neighbors in `direction`, restored to chronological (edge
/// insertion) order — unlike `derive_from_graph`'s unordered dedup, order is the
/// whole point of a sequence — optionally filtered to a `relation`.
///
/// `core.get_successors`/`get_predecessors` walk the underlying petgraph
/// adjacency list, which is LIFO (the most-recently-added edge comes back
/// FIRST); `neighbors_in_direction` passes that through unchanged for
/// `out`/`in`, so it is reversed here to recover true chronological order.
fn derive_sequences_from_graph(core: &GraphCore, spec: &SequenceSource) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let mut neighbors = neighbors_in_direction(core, &owner_id, &spec.direction);
        if spec.direction != "any" {
            neighbors.reverse();
        }
        let mut seq: Vec<String> = Vec::new();
        for nbr in neighbors {
            if let Some(rel) = &spec.relation {
                if !edge_matches_relation(core, &owner_id, &nbr, &spec.direction, rel) {
                    continue;
                }
            }
            if let Some(item) = extract_item(core, &nbr, &spec.item_field) {
                seq.push(item);
            }
        }
        if !seq.is_empty() {
            out.push(seq);
        }
    }
    out
}

fn to_seq_algo(a: MineSeqAlgorithm) -> sequence::Algorithm {
    match a {
        MineSeqAlgorithm::Prefixspan => sequence::Algorithm::PrefixSpan,
        MineSeqAlgorithm::Gsp => sequence::Algorithm::Gsp,
    }
}

/// Materialize each mined pattern as a typed `:SequentialPattern` node
/// (CONCEPT:EG-KG.mining.sequence-writeback), id = a deterministic digest of its
/// (order-preserving) item list. Linked to any item that is a resident node via
/// a `PATTERN_ITEM` edge, mirroring `materialize_rules`.
fn materialize_patterns(core: &GraphCore, patterns: &[LabeledPattern]) -> usize {
    let mut written = 0usize;
    for p in patterns {
        let node_id = pattern_node_id(&p.items);
        let props = serde_json::json!({
            "type": "SequentialPattern",
            "items": p.items,
            "support": p.support,
            "count": p.count,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for item in &p.items {
            if core.has_node(item) {
                let edge = serde_json::json!({ "relation": "PATTERN_ITEM" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(node_id.clone(), item.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a pattern (order matters, so
/// the digest is over the items in sequence — unlike a rule's antecedent/
/// consequent, which are pre-sorted sets).
fn pattern_node_id(items: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(items.join("\u{1}").as_bytes());
    format!("seqpattern:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Forecasting ───────────────────────────

/// Handle `MineForecast` (CONCEPT:EG-KG.mining.arima — Phase 4): forecast
/// `horizon` future points off a 1-D `values` series (a tsdb window handed in
/// by the caller — the same client-supplied cut `MineAnomaly` took in Phase 2)
/// via ARIMA/Holt-Winters/STL, return `{forecast, lower, upper, ...}`, and
/// optionally write a `:Forecast` node back.
#[allow(clippy::too_many_arguments)]
fn handle_forecast(
    req_id: u64,
    core: &GraphCore,
    values: Vec<f64>,
    algorithm: ForecastAlgorithm,
    horizon: usize,
    p: usize,
    d: usize,
    q: usize,
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
    confidence: f64,
    series_id: String,
    writeback: bool,
) -> Response {
    if values.is_empty() {
        return Response::err(
            req_id,
            "mining: forecast requires a non-empty `values` series",
        );
    }
    let algo = forecast_algo(algorithm, p, d, q, period, alpha, beta, gamma);
    let out = forecast::forecast(&values, algo, horizon, confidence);

    let written = if writeback {
        materialize_forecast(
            core,
            &out,
            horizon,
            &series_id,
            &values,
            forecast_algo_name(algorithm),
        )
    } else {
        0
    };

    let mut payload = serde_json::json!({
        "forecast": out.values,
        "lower": out.lower,
        "upper": out.upper,
        "algorithm": forecast_algo_name(algorithm),
        "horizon": horizon,
        "n_obs": values.len(),
        "written_back": written,
    });
    if matches!(algorithm, ForecastAlgorithm::Stl) {
        payload["trend"] = serde_json::json!(out.trend);
        payload["seasonal"] = serde_json::json!(out.seasonal);
        payload["residual"] = serde_json::json!(out.residual);
    }
    Response::ok(req_id, ResultPayload::Json(payload))
}

#[allow(clippy::too_many_arguments)]
fn forecast_algo(
    a: ForecastAlgorithm,
    p: usize,
    d: usize,
    q: usize,
    period: usize,
    alpha: f64,
    beta: f64,
    gamma: f64,
) -> forecast::Algorithm {
    match a {
        ForecastAlgorithm::Arima => forecast::Algorithm::Arima { p, d, q },
        ForecastAlgorithm::Holtwinters => forecast::Algorithm::HoltWinters {
            period,
            alpha,
            beta,
            gamma,
        },
        ForecastAlgorithm::Stl => forecast::Algorithm::Stl { period },
    }
}

fn forecast_algo_name(a: ForecastAlgorithm) -> &'static str {
    match a {
        ForecastAlgorithm::Arima => "arima",
        ForecastAlgorithm::Holtwinters => "holtwinters",
        ForecastAlgorithm::Stl => "stl",
    }
}

/// Materialize the forecast as a typed `:Forecast` node
/// (CONCEPT:EG-KG.mining.forecast-writeback), id = a deterministic digest of
/// `algo` + (`series_id` when given, else the input `values` — so identical
/// explicit input reproduces the same id on WAL replay). Linked to a resident
/// node named `series_id` via a `FORECAST_OF` edge when one exists.
fn materialize_forecast(
    core: &GraphCore,
    out: &forecast::Forecast,
    horizon: usize,
    series_id: &str,
    values: &[f64],
    algo: &str,
) -> usize {
    let node_id = forecast_node_id(algo, series_id, values);
    let props = serde_json::json!({
        "type": "Forecast",
        "algo": algo,
        "horizon": horizon,
        "values": out.values,
        "lower": out.lower,
        "upper": out.upper,
        "series_id": series_id,
    });
    let Ok(blob) = rmp_serde::to_vec_named(&props) else {
        return 0;
    };
    core.add_node(node_id.clone(), blob);
    if !series_id.is_empty() && core.has_node(series_id) {
        let edge = serde_json::json!({ "relation": "FORECAST_OF" });
        if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
            let _ = core.add_edge(node_id, series_id.to_string(), eb);
        }
    }
    1
}

fn forecast_node_id(algo: &str, series_id: &str, values: &[f64]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    if !series_id.is_empty() {
        hasher.update(series_id.as_bytes());
    } else {
        for v in values {
            hasher.update(v.to_bits().to_le_bytes());
        }
    }
    format!("forecast:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Text mining ───────────────────────────

/// Handle `MineText` (CONCEPT:EG-KG.mining.tfidf — Phase 4): tokenize the
/// corpus (explicit or graph-derived), run the chosen engine, return
/// `{doc_terms}` (tfidf) or `{topics, doc_topics}` (lda/nmf), and optionally
/// write `:Topic` nodes back (lda/nmf only).
#[allow(clippy::too_many_arguments)]
fn handle_text(
    req_id: u64,
    core: &GraphCore,
    docs: Vec<Vec<String>>,
    source: Option<TextSource>,
    algorithm: TextAlgorithm,
    k: usize,
    alpha: f64,
    beta: f64,
    iterations: usize,
    seed: u64,
    top_n: usize,
    writeback: bool,
) -> Response {
    let (tokenized, ids) = build_text_docs(core, &docs, &source);
    if tokenized.is_empty() {
        return Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({
                "doc_terms": [],
                "topics": [],
                "doc_topics": [],
                "n_docs": 0,
                "written_back": 0,
            })),
        );
    }
    let algo = to_text_algo(algorithm, k, alpha, beta, iterations, seed);
    let out = text::mine_labeled(&tokenized, algo, top_n);

    let written = if writeback && !matches!(algorithm, TextAlgorithm::Tfidf) {
        materialize_topics(core, &out, &ids, text_algo_name(algorithm))
    } else {
        0
    };

    let doc_terms_json: Vec<serde_json::Value> = out
        .doc_terms
        .iter()
        .enumerate()
        .map(|(i, terms)| {
            let id = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
            let term_rows: Vec<serde_json::Value> = terms
                .iter()
                .map(|(t, w)| serde_json::json!({ "term": t, "weight": w }))
                .collect();
            serde_json::json!({ "doc_id": id, "terms": term_rows })
        })
        .collect();

    let topics_json: Vec<serde_json::Value> = out
        .topics
        .iter()
        .enumerate()
        .map(|(i, terms)| {
            let term_rows: Vec<serde_json::Value> = terms
                .iter()
                .map(|(t, w)| serde_json::json!({ "term": t, "weight": w }))
                .collect();
            serde_json::json!({ "topic_id": i, "terms": term_rows })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "doc_terms": doc_terms_json,
            "topics": topics_json,
            "doc_topics": out.doc_topics,
            "algorithm": text_algo_name(algorithm),
            "n_docs": tokenized.len(),
            "written_back": written,
        })),
    )
}

fn to_text_algo(
    a: TextAlgorithm,
    k: usize,
    alpha: f64,
    beta: f64,
    iterations: usize,
    seed: u64,
) -> text::Algorithm {
    match a {
        TextAlgorithm::Tfidf => text::Algorithm::Tfidf,
        TextAlgorithm::Lda => text::Algorithm::Lda {
            k,
            alpha,
            beta,
            iterations,
            seed,
        },
        TextAlgorithm::Nmf => text::Algorithm::Nmf {
            k,
            iterations,
            seed,
        },
    }
}

fn text_algo_name(a: TextAlgorithm) -> &'static str {
    match a {
        TextAlgorithm::Tfidf => "tfidf",
        TextAlgorithm::Lda => "lda",
        TextAlgorithm::Nmf => "nmf",
    }
}

/// Resolve the tokenized corpus: explicit `docs` win (already tokenized, ids
/// empty); otherwise tokenize the `field` string property of every
/// `source.node_label` instance (compute-near-data — no Tantivy/eg-text
/// dependency), skipping nodes with no non-empty text. Returns the corpus AND
/// a parallel `ids` vec (node ids for the graph-derived path).
fn build_text_docs(
    core: &GraphCore,
    docs: &[Vec<String>],
    source: &Option<TextSource>,
) -> (Vec<Vec<String>>, Vec<String>) {
    if !docs.is_empty() {
        return (docs.to_vec(), Vec::new());
    }
    let Some(spec) = source else {
        return (Vec::new(), Vec::new());
    };
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut tokenized = Vec::with_capacity(owners.len());
    let mut ids = Vec::with_capacity(owners.len());
    for (node_id, blob) in owners {
        let Ok(props) = rmp_serde::from_slice::<serde_json::Value>(&blob) else {
            continue;
        };
        let Some(text_val) = props.get(&spec.field).and_then(|v| v.as_str()) else {
            continue;
        };
        let toks = text::tokenize(text_val);
        if toks.is_empty() {
            continue;
        }
        tokenized.push(toks);
        ids.push(node_id);
    }
    (tokenized, ids)
}

/// Materialize each topic as a typed `:Topic` node
/// (CONCEPT:EG-KG.mining.topic-writeback), id = a deterministic digest of `algo` +
/// its top terms (order-sensitive — the terms are already sorted by
/// descending weight). Linked, via a `HAS_TOPIC` edge, to every resident
/// source document whose DOMINANT topic (argmax of its `doc_topics`
/// distribution) is this one — only available when the corpus came from a
/// graph-derived `source` (`ids` non-empty).
fn materialize_topics(
    core: &GraphCore,
    out: &text::LabeledTextResult,
    ids: &[String],
    algo: &str,
) -> usize {
    let dominant: Vec<usize> = out
        .doc_topics
        .iter()
        .map(|dist| {
            dist.iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(t, _)| t)
                .unwrap_or(0)
        })
        .collect();

    let mut written = 0usize;
    for (t, terms) in out.topics.iter().enumerate() {
        let term_labels: Vec<&str> = terms.iter().map(|(term, _)| term.as_str()).collect();
        let node_id = topic_node_id(algo, &term_labels);
        let term_rows: Vec<serde_json::Value> = terms
            .iter()
            .map(|(term, w)| serde_json::json!({ "term": term, "weight": w }))
            .collect();
        let props = serde_json::json!({
            "type": "Topic",
            "algo": algo,
            "terms": term_rows,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for (i, doc_id) in ids.iter().enumerate() {
            if dominant.get(i) == Some(&t) && core.has_node(doc_id) {
                let edge = serde_json::json!({ "relation": "HAS_TOPIC" });
                if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                    let _ = core.add_edge(doc_id.clone(), node_id.clone(), eb);
                }
            }
        }
        written += 1;
    }
    written
}

fn topic_node_id(algo: &str, terms: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(algo.as_bytes());
    hasher.update([0u8]);
    hasher.update(terms.join("\u{1}").as_bytes());
    format!("topic:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Frequent subgraph mining + motifs ───────────────────────────

/// Handle `MineSubgraph` (CONCEPT:EG-KG.mining.gspan-frequent-subgraph — Phase
/// 4, the graph-native family member): build a labeled host graph from the
/// RESIDENT graph itself (no rows/vectors handed in), run gSpan-style
/// frequent-subgraph mining or a motif census, and optionally write
/// `:FrequentSubgraph` nodes back (`gspan` only).
fn handle_subgraph(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    min_support: f64,
    max_edges: usize,
    algorithm: SubgraphAlgorithm,
    writeback: bool,
) -> Response {
    let (host, ids) = build_host_graph(core, &label);
    let n_host_nodes = host.node_count();
    let n_host_edges = host.edge_count();

    match algorithm {
        SubgraphAlgorithm::Gspan => {
            let results = subgraph::mine_gspan(&host, min_support, max_edges);
            let written = if writeback {
                materialize_subgraphs(core, &results, &ids)
            } else {
                0
            };
            let patterns: Vec<serde_json::Value> = results
                .iter()
                .map(|r| {
                    let edges: Vec<serde_json::Value> = r
                        .pattern
                        .edges
                        .iter()
                        .map(|(a, b, lbl)| serde_json::json!({ "from": a, "to": b, "label": lbl }))
                        .collect();
                    serde_json::json!({
                        "nodes": r.pattern.node_labels,
                        "edges": edges,
                        "support": r.support,
                        "count": r.count,
                    })
                })
                .collect();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "patterns": patterns,
                    "algorithm": subgraph_algo_name(SubgraphAlgorithm::Gspan),
                    "n_host_nodes": n_host_nodes,
                    "n_host_edges": n_host_edges,
                    "written_back": written,
                })),
            )
        }
        SubgraphAlgorithm::Motif => {
            let motifs = subgraph::count_motifs(&host);
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "motifs": {
                        "wedge": motifs.wedge,
                        "triangle": motifs.triangle,
                        "directed_cycle3": motifs.directed_cycle3,
                    },
                    "algorithm": subgraph_algo_name(SubgraphAlgorithm::Motif),
                    "n_host_nodes": n_host_nodes,
                    "n_host_edges": n_host_edges,
                    "written_back": 0,
                })),
            )
        }
    }
}

/// Build a [`HostGraph`] from the resident graph (CONCEPT:EG-KG.mining.gspan-frequent-subgraph):
/// every node's type/label property (checked in the same `type`/`node_type`/
/// `label` precedence as `extract_item`'s `"label"` field), every edge's
/// relation label (`relation`/`type`/`rel`, defaulting to `"_"`). When
/// `label_filter` is given, only nodes of that ONE type are included (both
/// edge endpoints must be included for the edge to count). Returns the host
/// graph AND a parallel `ids` vec (dense index → resident node id).
fn build_host_graph(core: &GraphCore, label_filter: &Option<String>) -> (HostGraph, Vec<String>) {
    let all_nodes = core.get_nodes();
    let mut ids: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (node_id, blob) in &all_nodes {
        let node_label = node_type_label(blob).unwrap_or_else(|| "_".to_string());
        if let Some(want) = label_filter {
            if &node_label != want {
                continue;
            }
        }
        index.insert(node_id.clone(), ids.len());
        ids.push(node_id.clone());
        labels.push(node_label);
    }

    let all_edges = core.get_edges();
    let mut edges: Vec<(usize, usize, String)> = Vec::new();
    for (src, dst, blob) in &all_edges {
        let (Some(&si), Some(&di)) = (index.get(src), index.get(dst)) else {
            continue;
        };
        let rel = edge_relation_label(blob);
        edges.push((si, di, rel));
    }
    (HostGraph::build(labels, &edges), ids)
}

/// Extract a node's type/label from its property blob, per the
/// `type`/`node_type`/`label` precedence used elsewhere in this handler.
fn node_type_label(blob: &[u8]) -> Option<String> {
    let val: serde_json::Value = rmp_serde::from_slice(blob).ok()?;
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

/// Extract an edge's relation label from its property blob (`relation`/
/// `type`/`rel`, defaulting to `"_"` when none is set — an unlabeled edge is
/// still a valid, matchable edge, just under one shared label).
fn edge_relation_label(blob: &[u8]) -> String {
    let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(blob) else {
        return "_".to_string();
    };
    for key in ["relation", "type", "rel"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    "_".to_string()
}

fn subgraph_algo_name(a: SubgraphAlgorithm) -> &'static str {
    match a {
        SubgraphAlgorithm::Gspan => "gspan",
        SubgraphAlgorithm::Motif => "motif",
    }
}

/// Materialize each frequent pattern as a typed `:FrequentSubgraph` node
/// (CONCEPT:EG-KG.mining.gspan-frequent-subgraph), id = a deterministic digest
/// of its canonical shape (node labels + edges). Linked, via a
/// `SUBGRAPH_MEMBER` edge, to every resident host node appearing in ANY of its
/// embeddings.
fn materialize_subgraphs(
    core: &GraphCore,
    results: &[subgraph::FrequentSubgraph],
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for r in results {
        let node_id = subgraph_node_id(&r.pattern);
        let edges_json: Vec<serde_json::Value> = r
            .pattern
            .edges
            .iter()
            .map(|(a, b, lbl)| serde_json::json!({ "from": a, "to": b, "label": lbl }))
            .collect();
        let props = serde_json::json!({
            "type": "FrequentSubgraph",
            "nodes": r.pattern.node_labels,
            "edges": edges_json,
            "support": r.support,
            "count": r.count,
        });
        let Ok(blob) = rmp_serde::to_vec_named(&props) else {
            continue;
        };
        core.add_node(node_id.clone(), blob);
        for &member_idx in &r.member_nodes {
            if let Some(member_id) = ids.get(member_idx) {
                if core.has_node(member_id) {
                    let edge = serde_json::json!({ "relation": "SUBGRAPH_MEMBER" });
                    if let Ok(eb) = rmp_serde::to_vec_named(&edge) {
                        let _ = core.add_edge(node_id.clone(), member_id.clone(), eb);
                    }
                }
            }
        }
        written += 1;
    }
    written
}

fn subgraph_node_id(pattern: &subgraph::Pattern) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(pattern.node_labels.join("\u{1}").as_bytes());
    hasher.update([0u8]);
    for (a, b, lbl) in &pattern.edges {
        hasher.update(a.to_le_bytes());
        hasher.update(b.to_le_bytes());
        hasher.update(lbl.as_bytes());
        hasher.update([0u8]);
    }
    format!("subgraph:{}", hex::encode(&hasher.finalize()[..12]))
}

// ─────────────────────────── Row builders (shared) ───────────────────────────

/// Resolve the cluster feature rows: explicit `features` win; then a fused
/// upstream `plan` (CONCEPT:EG-KG.mining.fused-plan-source); then the `source`
/// node-label embedding scan (the cross-modal hook). Returns the rows AND a
/// parallel `ids` vec (node ids for the embedding/plan path, empty for explicit).
fn build_vectors(
    core: &GraphCore,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(core, p);
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// Resolve the anomaly rows: explicit `features` win, then a 1-D `values` series
/// (each scalar → a one-element row — the tsdb RCA path), then a fused upstream
/// `plan`, then node embeddings.
fn build_anomaly_rows(
    core: &GraphCore,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    if !values.is_empty() {
        return (values.iter().map(|&v| vec![v]).collect(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(core, p);
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

/// Run an upstream cross-modal RETRIEVAL `plan` (`Op::Scan|Filter|Traverse|Rank|…`)
/// over a fresh graph+semantic snapshot and resolve each resulting row's id to its
/// stored embedding — the SAME lookup [`gather_embeddings`] uses for a bare
/// `VectorSource` label scan, generalized to an ARBITRARY upstream plan
/// (CONCEPT:EG-KG.mining.fused-plan-source). This is the fused `retrieve → mine →
/// writeback` mechanism: the retrieval legs (vector rank / graph traverse / SQL
/// filter / OWL reason / …) run FIRST, compute-near-data, over the SAME snapshot
/// the mining op then reads embeddings from — ONE round-trip, no client
/// marshalling between "retrieve the candidate set" and "mine it". A plan
/// execution error degrades to an empty row set (never panics/propagates) —
/// consistent with every other mining source's "no match ⇒ empty" contract.
///
/// NOTE (scope cut): the committed native tsdb store is NOT threaded through this
/// synchronous, graph-scoped path (mining dispatches off `Arc<GraphCore>` alone,
/// unlike the async `UnifiedQuery` handler which also carries the server's live
/// tsdb handle) — a plan containing `Op::TsScan` degrades to no rows for that leg
/// exactly like an unbound embedder degrades `RankEmbed`, rather than erroring.
/// Wiring the live tsdb store into the mining dispatch path is a follow-up.
#[cfg(feature = "query")]
fn gather_plan_rows(core: &GraphCore, plan: &crate::wire::Plan) -> (Vec<Vec<f64>>, Vec<String>) {
    let snap = core.analysis_snapshot();
    let semantic = core.semantic_store.read().clone();
    let rows = match crate::server::handlers::query::run_unified(
        plan.clone(),
        None,
        &snap,
        &semantic,
        #[cfg(feature = "tsdb")]
        None,
        #[cfg(feature = "tsdb")]
        None,
    ) {
        Ok(rows) => rows,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let store = core.semantic_store.read();
    let mut feats: Vec<Vec<f64>> = Vec::with_capacity(rows.len());
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    for (node_id, _score) in rows {
        if let Some(vec) = store.get_embedding(&node_id) {
            feats.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (feats, ids)
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
            #[cfg(feature = "query")]
            plan: None,
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
            #[cfg(feature = "query")]
            plan: None,
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

    /// The headline fused example (CONCEPT:EG-KG.mining.fused-plan-source, Phase 5):
    /// vector-retrieve a neighborhood via an upstream `Op::Rank` PLAN — no
    /// `VectorSource` label spec at all — cluster the retrieved rows, and write
    /// `:Cluster` nodes back, all in ONE `MineCluster` call. Proves
    /// `retrieve → mine → writeback` composes as ONE plan (compute-near-data, no
    /// client round-trip between the retrieval leg and the mining leg).
    #[test]
    #[cfg(feature = "query")]
    fn fused_plan_rank_then_cluster_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Six :Doc nodes with 2-D embeddings forming two well-separated groups.
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
        // Upstream retrieval plan: scan all :Doc nodes, then rank by cosine
        // similarity to a query near the FIRST group — a cross-modal
        // Scan→Rank→Limit leg that runs BEFORE the mining op ever sees a row.
        let plan = crate::wire::Plan::new(vec![
            crate::wire::Op::Scan {
                label: "Doc".into(),
            },
            crate::wire::Op::Rank {
                query: vec![0.1, 0.1],
            },
            crate::wire::Op::Limit { k: 6 },
        ]);
        let m = Method::MineCluster {
            features: Vec::new(),
            source: None,
            plan: Some(plan),
            algorithm: ClusterAlgorithm::Kmedoids,
            eps: 0.5,
            min_pts: 5,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: true,
        };
        let resp = try_handle(42, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        // The plan's Rank/Limit legs ran FIRST — the vector kNN leg is an
        // approximate search (documented "approximate, small-N" contract shared
        // with UMAP/t-SNE elsewhere in this surface), so it may recall slightly
        // fewer than all 6 candidates; the mining op then clustered exactly the
        // rows the plan handed it — one round trip, no client marshalling
        // between "retrieve" and "mine".
        let n_rows = v["n_rows"].as_u64().unwrap();
        assert!(
            (4..=6).contains(&n_rows),
            "expected the Rank leg to recall most of the 6 candidates, got {n_rows}"
        );
        assert_eq!(v["n_clusters"], 2); // k-medoids always forms exactly k=2 groups
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let cluster_nodes = core.get_nodes_by_label("Cluster", 0);
        assert_eq!(cluster_nodes.len() as u64, written);
    }

    /// A plan that finds NO matching rows (a label the graph doesn't carry)
    /// degrades to an empty feature set rather than erroring — the same
    /// "no match ⇒ empty" contract every other mining source honors.
    #[test]
    #[cfg(feature = "query")]
    fn fused_plan_no_match_degrades_to_empty() {
        let core = Arc::new(GraphCore::new());
        let plan = crate::wire::Plan::new(vec![crate::wire::Op::Scan {
            label: "NoSuchLabel".into(),
        }]);
        let m = Method::MineCluster {
            features: Vec::new(),
            source: None,
            plan: Some(plan),
            algorithm: ClusterAlgorithm::Dbscan,
            eps: 1.0,
            min_pts: 2,
            k: 2,
            linkage: Linkage::Average,
            max_iter: 100,
            seed: 0,
            writeback: false,
        };
        let resp = try_handle(43, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 0);
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
            #[cfg(feature = "query")]
            plan: None,
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
            #[cfg(feature = "query")]
            plan: None,
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

    #[test]
    fn classify_fit_then_predict_roundtrip() {
        let core = Arc::new(GraphCore::new());
        // Separable 2-class training set.
        let x = vec![
            vec![0.0, 0.0],
            vec![0.5, 0.3],
            vec![0.2, 0.8],
            vec![10.0, 10.0],
            vec![10.5, 9.7],
            vec![9.8, 10.4],
        ];
        let y = vec![0, 0, 0, 1, 1, 1];
        let fit = Method::MineClassifyFit {
            x: x.clone(),
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            y,
            algorithm: ClassifyAlgorithm::Logistic,
            k: 5,
            alpha: 1.0,
            lr: 0.5,
            epochs: 500,
            l2: 0.0,
            c: 1.0,
        };
        let resp = try_handle(1, Arc::clone(&core), fit).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_samples"], 6);
        let model: FittedClassifier = serde_json::from_value(v["model"].clone()).unwrap();

        let predict = Method::MineClassifyPredict {
            model,
            x: vec![vec![0.3, 0.3], vec![10.0, 10.0]],
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            writeback: false,
        };
        let resp = try_handle(2, core, predict).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        let rows = v["rows"].as_array().unwrap();
        assert_eq!(rows[0]["label"], 0);
        assert_eq!(rows[1]["label"], 1);
    }

    #[test]
    fn classify_predict_over_embeddings_writeback() {
        let core = Arc::new(GraphCore::new());
        // Fit a GaussianNB in-memory, then predict over node embeddings + writeback.
        let x = vec![
            vec![0.0, 0.0],
            vec![0.2, 0.1],
            vec![9.0, 9.0],
            vec![9.1, 8.8],
        ];
        let y = vec![0, 0, 1, 1];
        let model = eg_compute::mining::classify::fit(
            &x,
            &y,
            eg_compute::mining::classify::Algorithm::GaussianNb,
        )
        .unwrap();
        for (id, e) in [("p0", [0.1f32, 0.0]), ("p1", [9.0, 9.1])] {
            core.add_node(id.into(), node(serde_json::json!({"type": "Sample"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec());
        }
        let m = Method::MineClassifyPredict {
            model,
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Sample".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            writeback: true,
        };
        let resp = try_handle(3, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let cls_nodes = core.get_nodes_by_label("Classification", 0);
        assert_eq!(cls_nodes.len() as u64, written);
        // The classification node links to its source via CLASSIFIED_AS.
        let succ = core.get_successors(&cls_nodes[0].0).unwrap_or_default();
        assert!(succ.iter().any(|s| s == "p0" || s == "p1"));
    }

    #[test]
    fn reduce_svd_and_writeback_embedding2d() {
        let core = Arc::new(GraphCore::new());
        let embs = [
            ("d0", [1.0f32, 0.0, 0.0]),
            ("d1", [0.0, 1.0, 0.0]),
            ("d2", [1.0, 1.0, 0.0]),
            ("d3", [2.0, 1.0, 0.0]),
        ];
        for (id, e) in embs {
            core.add_node(id.into(), node(serde_json::json!({"type": "Vec"})));
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), e.to_vec());
        }
        let m = Method::MineReduce {
            x: Vec::new(),
            source: Some(VectorSource {
                node_label: "Vec".into(),
                limit: 0,
            }),
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(),
            algorithm: ReduceAlgorithm::Svd,
            n_components: 2,
            n_neighbors: 15,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 300,
            lr: 100.0,
            seed: 0,
            writeback: true,
        };
        let resp = try_handle(4, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json");
        };
        assert_eq!(v["n_rows"], 4);
        assert_eq!(v["n_components"], 2);
        assert!(v["singular_values"].is_array());
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 4);
        core.mark_dirty();
        let e2d = core.get_nodes_by_label("Embedding2D", 0);
        assert_eq!(e2d.len() as u64, written);
    }

    #[test]
    fn reduce_lda_requires_labels() {
        let core = Arc::new(GraphCore::new());
        let m = Method::MineReduce {
            x: vec![vec![0.0, 0.0], vec![1.0, 1.0]],
            source: None,
            #[cfg(feature = "query")]
            plan: None,
            labels: Vec::new(), // missing → error for LDA
            algorithm: ReduceAlgorithm::Lda,
            n_components: 1,
            n_neighbors: 15,
            min_dist: 0.1,
            perplexity: 30.0,
            epochs: 300,
            lr: 100.0,
            seed: 0,
            writeback: false,
        };
        let resp = try_handle(5, core, m).expect("handled");
        assert!(resp.result.is_none()); // an error response carries no result payload
    }

    #[test]
    fn explicit_sequences_produce_patterns() {
        let core = Arc::new(GraphCore::new());
        let seqs = vec![
            vec![
                "login".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
            vec![
                "login".to_string(),
                "search".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
            vec![
                "login".to_string(),
                "browse".to_string(),
                "purchase".to_string(),
            ],
        ];
        let m = Method::MineSequence {
            sequences: seqs,
            source: None,
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Prefixspan,
            writeback: false,
        };
        let resp = try_handle(11, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_sequences"], 3);
        assert!(v["n_patterns"].as_u64().unwrap() > 0);
        assert_eq!(v["written_back"], 0);
        let patterns = v["patterns"].as_array().unwrap();
        assert!(patterns
            .iter()
            .any(|p| { p["items"] == serde_json::json!(["login", "browse", "purchase"]) }));
    }

    #[test]
    fn sequence_graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Two "Session" owners whose ordered "out" edges (insertion order) are the
        // event sequence: session1/session2 both go view -> add_cart -> checkout.
        core.add_node("s1".into(), node(serde_json::json!({"type": "Session"})));
        core.add_node("s2".into(), node(serde_json::json!({"type": "Session"})));
        for ev in ["view", "add_cart", "checkout"] {
            core.add_node(ev.into(), node(serde_json::json!({"type": "Event"})));
        }
        for owner in ["s1", "s2"] {
            for ev in ["view", "add_cart", "checkout"] {
                let _ = core.add_edge(owner.into(), ev.into(), node(serde_json::json!({})));
            }
        }
        let m = Method::MineSequence {
            sequences: Vec::new(),
            source: Some(SequenceSource {
                node_label: "Session".into(),
                direction: "out".into(),
                item_field: None, // neighbor node id ⇒ "view"/"add_cart"/"checkout"
                relation: None,
                limit: 0,
            }),
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Gsp,
            writeback: true,
        };
        let resp = try_handle(13, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_sequences"], 2);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        core.mark_dirty();
        let pattern_nodes = core.get_nodes_by_label("SequentialPattern", 0);
        assert_eq!(pattern_nodes.len() as u64, written);
        // The full 3-item pattern must have been recovered (both sessions match).
        let patterns = v["patterns"].as_array().unwrap();
        assert!(patterns
            .iter()
            .any(|p| { p["items"] == serde_json::json!(["view", "add_cart", "checkout"]) }));
    }

    #[test]
    fn forecast_arima_and_writeback() {
        let core = Arc::new(GraphCore::new());
        let values: Vec<f64> = (0..30).map(|t| 5.0 + 3.0 * t as f64).collect();
        let m = Method::MineForecast {
            values: values.clone(),
            algorithm: ForecastAlgorithm::Arima,
            horizon: 5,
            p: 1,
            d: 1,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: "metric1".into(),
            writeback: true,
        };
        let resp = try_handle(17, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let forecast_vals = v["forecast"].as_array().unwrap();
        assert_eq!(forecast_vals.len(), 5);
        // A pure linear trend (5 + 3t) should extrapolate close to truth at h=1..5.
        for (h, fv) in forecast_vals.iter().enumerate() {
            let t = 30 + h;
            let truth = 5.0 + 3.0 * t as f64;
            let got = fv.as_f64().unwrap();
            assert!(
                (got - truth).abs() < 3.0,
                "forecast[{h}]={got} truth={truth}"
            );
        }
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 1);
        core.mark_dirty();
        let forecast_nodes = core.get_nodes_by_label("Forecast", 0);
        assert_eq!(forecast_nodes.len(), 1);
    }

    #[test]
    fn forecast_missing_values_is_error() {
        let core = Arc::new(GraphCore::new());
        let m = Method::MineForecast {
            values: Vec::new(),
            algorithm: ForecastAlgorithm::Arima,
            horizon: 5,
            p: 1,
            d: 1,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: String::new(),
            writeback: false,
        };
        let resp = try_handle(19, core, m).expect("handled");
        assert!(resp.result.is_none());
    }

    #[test]
    fn forecast_holtwinters_seasonal() {
        let core = Arc::new(GraphCore::new());
        let values: Vec<f64> = (0..48)
            .map(|t| {
                10.0 + 2.0 * t as f64 + 5.0 * (2.0 * std::f64::consts::PI * t as f64 / 12.0).sin()
            })
            .collect();
        let m = Method::MineForecast {
            values,
            algorithm: ForecastAlgorithm::Holtwinters,
            horizon: 12,
            p: 1,
            d: 1,
            q: 0,
            period: 12,
            alpha: 0.5,
            beta: 0.3,
            gamma: 0.3,
            confidence: 0.95,
            series_id: String::new(),
            writeback: false,
        };
        let resp = try_handle(21, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let lower = v["lower"].as_array().unwrap();
        let upper = v["upper"].as_array().unwrap();
        assert_eq!(lower.len(), 12);
        for i in 0..12 {
            assert!(lower[i].as_f64().unwrap() <= upper[i].as_f64().unwrap());
        }
    }

    fn words(s: &str) -> Vec<String> {
        s.split_whitespace().map(|w| w.to_string()).collect()
    }

    #[test]
    fn text_tfidf_explicit_docs() {
        let core = Arc::new(GraphCore::new());
        let docs = vec![
            words("the cat sat on the mat"),
            words("the dog ran in the park"),
            words("the rocket launched into orbit"),
        ];
        let m = Method::MineText {
            docs,
            source: None,
            algorithm: TextAlgorithm::Tfidf,
            k: 3,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 1,
            top_n: 10,
            writeback: true, // ignored for tfidf
        };
        let resp = try_handle(23, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_docs"], 3);
        assert_eq!(v["written_back"], 0); // tfidf never writes back
        let doc_terms = v["doc_terms"].as_array().unwrap();
        assert_eq!(doc_terms.len(), 3);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("Topic", 0).len(), 0);
    }

    #[test]
    fn text_lda_graph_derived_source_and_writeback() {
        let core = Arc::new(GraphCore::new());
        let pet_words = ["cat", "dog", "pet", "leash", "vet"];
        let fin_words = ["stock", "market", "bond", "yield", "trader"];
        for i in 0..15 {
            let n = 6 + (i % 4);
            let pet_text: String = (0..n)
                .map(|j| pet_words[(i + j) % pet_words.len()])
                .collect::<Vec<_>>()
                .join(" ");
            let fin_text: String = (0..n)
                .map(|j| fin_words[(i + j) % fin_words.len()])
                .collect::<Vec<_>>()
                .join(" ");
            core.add_node(
                format!("doc_pet_{i}"),
                node(serde_json::json!({"type": "Doc", "body": pet_text})),
            );
            core.add_node(
                format!("doc_fin_{i}"),
                node(serde_json::json!({"type": "Doc", "body": fin_text})),
            );
        }
        let m = Method::MineText {
            docs: Vec::new(),
            source: Some(TextSource {
                node_label: "Doc".into(),
                field: "body".into(),
                limit: 0,
            }),
            algorithm: TextAlgorithm::Lda,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 42,
            top_n: 5,
            writeback: true,
        };
        let resp = try_handle(25, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_docs"], 30);
        let written = v["written_back"].as_u64().unwrap();
        assert_eq!(written, 2);
        core.mark_dirty();
        let topic_nodes = core.get_nodes_by_label("Topic", 0);
        assert_eq!(topic_nodes.len(), 2);
        // Every doc must have exactly one HAS_TOPIC edge (its dominant topic).
        for i in 0..15 {
            for prefix in ["doc_pet_", "doc_fin_"] {
                let id = format!("{prefix}{i}");
                let succ = core.get_successors(&id).unwrap();
                let topic_edges: Vec<&String> =
                    succ.iter().filter(|s| s.starts_with("topic:")).collect();
                assert_eq!(
                    topic_edges.len(),
                    1,
                    "doc {id} should link to exactly one topic"
                );
            }
        }
    }

    #[test]
    fn text_nmf_explicit_docs_topics_and_doc_topics() {
        let core = Arc::new(GraphCore::new());
        let docs = vec![
            words("cat dog pet leash vet cat dog"),
            words("stock market bond yield trader stock market"),
            words("cat dog pet vet leash"),
            words("bond yield trader stock market"),
        ];
        let m = Method::MineText {
            docs,
            source: None,
            algorithm: TextAlgorithm::Nmf,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 200,
            seed: 7,
            top_n: 5,
            writeback: false,
        };
        let resp = try_handle(27, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        let topics = v["topics"].as_array().unwrap();
        assert_eq!(topics.len(), 2);
        let doc_topics = v["doc_topics"].as_array().unwrap();
        assert_eq!(doc_topics.len(), 4);
        assert_eq!(v["written_back"], 0); // writeback=false
    }

    #[test]
    fn subgraph_gspan_recovers_planted_pattern_and_writeback() {
        let core = Arc::new(GraphCore::new());
        // Plant 4 instances of :Concept --touches--> :Capability, plus a
        // handful of unrelated noise nodes/edges under different types.
        for i in 0..4 {
            core.add_node(
                format!("concept_{i}"),
                node(serde_json::json!({"type": "Concept"})),
            );
            core.add_node(
                format!("capability_{i}"),
                node(serde_json::json!({"type": "Capability"})),
            );
            let _ = core.add_edge(
                format!("concept_{i}"),
                format!("capability_{i}"),
                node(serde_json::json!({"relation": "touches"})),
            );
        }
        core.add_node("noise_a".into(), node(serde_json::json!({"type": "Noise"})));
        core.add_node("noise_b".into(), node(serde_json::json!({"type": "Noise"})));
        let _ = core.add_edge(
            "noise_a".into(),
            "noise_b".into(),
            node(serde_json::json!({"relation": "unrelated"})),
        );

        let m = Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 1,
            algorithm: SubgraphAlgorithm::Gspan,
            writeback: true,
        };
        let resp = try_handle(29, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["n_host_nodes"], 10);
        assert_eq!(v["n_host_edges"], 5);
        let patterns = v["patterns"].as_array().unwrap();
        let hit = patterns.iter().find(|p| {
            let nodes = p["nodes"].as_array().unwrap();
            let has_concept = nodes.iter().any(|n| n == "Concept");
            let has_capability = nodes.iter().any(|n| n == "Capability");
            has_concept && has_capability && p["edges"].as_array().unwrap().len() == 1
        });
        assert!(
            hit.is_some(),
            "planted pattern not in response: {patterns:?}"
        );
        assert_eq!(hit.unwrap()["count"], 4);
        let written = v["written_back"].as_u64().unwrap();
        assert!(written > 0);
        core.mark_dirty();
        let subgraph_nodes = core.get_nodes_by_label("FrequentSubgraph", 0);
        assert_eq!(subgraph_nodes.len() as u64, written);
        // The planted pattern's :FrequentSubgraph must link to all 8 involved nodes.
        let sg_id = subgraph_nodes
            .iter()
            .find(|(_, blob)| {
                let props: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
                let nodes = props["nodes"].as_array().unwrap();
                nodes.iter().any(|n| n == "Concept") && nodes.iter().any(|n| n == "Capability")
            })
            .map(|(id, _)| id.clone())
            .expect("planted subgraph node present");
        let members = core.get_successors(&sg_id).unwrap();
        assert_eq!(members.len(), 8); // 4 concept + 4 capability nodes
    }

    #[test]
    fn subgraph_motif_census_is_readonly() {
        let core = Arc::new(GraphCore::new());
        for i in 0..3 {
            core.add_node(format!("n{i}"), node(serde_json::json!({"type": "N"})));
        }
        let _ = core.add_edge(
            "n0".into(),
            "n1".into(),
            node(serde_json::json!({"relation": "e"})),
        );
        let _ = core.add_edge(
            "n1".into(),
            "n2".into(),
            node(serde_json::json!({"relation": "e"})),
        );
        let _ = core.add_edge(
            "n2".into(),
            "n0".into(),
            node(serde_json::json!({"relation": "e"})),
        );

        let m = Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 3,
            algorithm: SubgraphAlgorithm::Motif,
            writeback: true, // ignored for motif
        };
        let resp = try_handle(31, Arc::clone(&core), m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        assert_eq!(v["motifs"]["triangle"], 1);
        assert_eq!(v["motifs"]["directed_cycle3"], 1);
        assert_eq!(v["written_back"], 0);
        core.mark_dirty();
        assert_eq!(core.get_nodes_by_label("FrequentSubgraph", 0).len(), 0);
    }

    #[test]
    fn subgraph_label_filter_restricts_host_graph() {
        let core = Arc::new(GraphCore::new());
        core.add_node("a".into(), node(serde_json::json!({"type": "A"})));
        core.add_node("b".into(), node(serde_json::json!({"type": "B"})));
        core.add_node("a2".into(), node(serde_json::json!({"type": "A"})));
        let _ = core.add_edge(
            "a".into(),
            "b".into(),
            node(serde_json::json!({"relation": "e"})),
        );
        let _ = core.add_edge(
            "a".into(),
            "a2".into(),
            node(serde_json::json!({"relation": "e"})),
        );

        let m = Method::MineSubgraph {
            label: Some("A".into()),
            min_support: 0.1,
            max_edges: 1,
            algorithm: SubgraphAlgorithm::Gspan,
            writeback: false,
        };
        let resp = try_handle(33, core, m).expect("handled");
        let Some(ResultPayload::Json(v)) = resp.result else {
            panic!("expected json payload");
        };
        // Only the two A nodes + the a->a2 edge should be in the filtered host
        // graph (a->b is excluded since b is not type A).
        assert_eq!(v["n_host_nodes"], 2);
        assert_eq!(v["n_host_edges"], 1);
    }
}
