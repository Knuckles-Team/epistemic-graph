use super::*;
use super::{
    classic::{edge_relation_label, node_type_label},
    process::*,
    vector::link_writeback_source,
    writeback::*,
};
use eg_compute::graph_algos::AdjacencyGraph;
use eg_compute::mining::{
    community, ontology_gap, retrieval_quality, risk_propagation, root_cause,
};

pub(in crate::server::handlers) struct RootCauseRequest {
    pub(in crate::server::handlers) nodes: Vec<String>,
    pub(in crate::server::handlers) scores: Vec<f64>,
    pub(in crate::server::handlers) edges: Vec<(String, String, f64)>,
    pub(in crate::server::handlers) symptom: String,
    pub(in crate::server::handlers) max_hops: usize,
    pub(in crate::server::handlers) decay: f64,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_root_cause(
    req_id: u64,
    core: &GraphCore,
    request: RootCauseRequest,
) -> Response {
    let RootCauseRequest {
        nodes,
        scores,
        edges,
        symptom,
        max_hops,
        decay,
        writeback,
    } = request;
    let Some(out) = run_root_cause(&nodes, &scores, &edges, &symptom, max_hops, decay) else {
        return Response::err(
            req_id,
            "mining: root_cause requires `symptom` to be present in `nodes`",
        );
    };
    let written = if writeback.enabled {
        materialize_root_cause(core, &out, &nodes, &symptom)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_root_cause_claim(core, &out, &nodes, &symptom);
    }
    let candidates: Vec<serde_json::Value> = out
        .candidates
        .iter()
        .map(|c| {
            serde_json::json!({
                "node": nodes.get(c.node).cloned().unwrap_or_else(|| c.node.to_string()),
                "score": c.score,
                "hops": c.hops,
            })
        })
        .collect();
    let best = out.best().map(|c| {
        nodes
            .get(c.node)
            .cloned()
            .unwrap_or_else(|| c.node.to_string())
    });
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "symptom": symptom,
            "candidates": candidates,
            "best": best,
            "written_back": written,
        })),
    )
}

/// Materialize the TOP candidate as a typed `:RootCause` node (CONCEPT:EG-KG.mining.root-cause),
/// linked to the symptom (`ROOT_CAUSE_OF`) and the candidate itself
/// (`ROOT_CAUSE_CANDIDATE`) when resident.
pub(super) fn materialize_root_cause(
    core: &GraphCore,
    out: &root_cause::RootCauseResult,
    nodes: &[String],
    symptom: &str,
) -> usize {
    let Some(best) = out.best() else {
        return 0;
    };
    let cause_id = nodes
        .get(best.node)
        .cloned()
        .unwrap_or_else(|| best.node.to_string());
    let node_id = root_cause_node_id(symptom, &cause_id);
    let props = serde_json::json!({
        "type": "RootCause",
        "symptom": symptom,
        "cause": cause_id,
        "score": best.score,
        "hops": best.hops,
    });
    if !writeback_node(core, &node_id, &props) {
        return 0;
    }
    if core.has_node(symptom) {
        writeback_relationship(core, &node_id, symptom, "ROOT_CAUSE_OF");
    }
    if core.has_node(&cause_id) {
        writeback_relationship(core, &node_id, &cause_id, "ROOT_CAUSE_CANDIDATE");
    }
    1
}

pub(super) fn root_cause_node_id(symptom: &str, cause: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(symptom.as_bytes());
    hasher.update([0u8]);
    hasher.update(cause.as_bytes());
    format!("root_cause:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality mirrors `anomaly`'s `score / (1 + score)` confidence mapping — the
/// TOP candidate's OWN raw responsibility score, not normalized against the
/// candidate list (which would be trivially `1.0` for the top candidate).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_root_cause_claim(
    core: &GraphCore,
    out: &root_cause::RootCauseResult,
    nodes: &[String],
    symptom: &str,
) {
    let Some(best) = out.best() else {
        return;
    };
    let cause_id = nodes
        .get(best.node)
        .cloned()
        .unwrap_or_else(|| best.node.to_string());
    let node_id = root_cause_node_id(symptom, &cause_id);
    let confidence = (best.score / (1.0 + best.score)).clamp(0.0, 1.0);
    materialize_claim(
        core,
        &node_id,
        "root_cause",
        confidence,
        &format!("symptom:{symptom}"),
    );
}

// ─────────────────────────── Seeded risk propagation ───────────────────────────

pub(super) fn run_risk_propagation(
    nodes: &[String],
    seed: &[f64],
    edges: &[(String, String, f64)],
    damping: f64,
    tolerance: f64,
    max_iterations: usize,
) -> risk_propagation::RiskScores {
    let idx_edges = edge_indices(nodes, edges);
    let config = risk_propagation::RiskConfig {
        damping: if damping > 0.0 { damping } else { 0.85 },
        tolerance: if tolerance > 0.0 { tolerance } else { 1e-7 },
        max_iterations: max_iterations.max(1),
    };
    risk_propagation::propagate(nodes.len(), &idx_edges, seed, &config)
}

pub(in crate::server::handlers) struct RiskPropagationRequest {
    pub(in crate::server::handlers) nodes: Vec<String>,
    pub(in crate::server::handlers) seed: Vec<f64>,
    pub(in crate::server::handlers) edges: Vec<(String, String, f64)>,
    pub(in crate::server::handlers) damping: f64,
    pub(in crate::server::handlers) tolerance: f64,
    pub(in crate::server::handlers) max_iterations: usize,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_risk_propagation(
    req_id: u64,
    core: &GraphCore,
    request: RiskPropagationRequest,
) -> Response {
    let RiskPropagationRequest {
        nodes,
        seed,
        edges,
        damping,
        tolerance,
        max_iterations,
        writeback,
    } = request;
    if nodes.is_empty() {
        return Response::err(
            req_id,
            "mining: risk_propagation requires non-empty `nodes`",
        );
    }
    let out = run_risk_propagation(&nodes, &seed, &edges, damping, tolerance, max_iterations);
    let written = if writeback.enabled {
        materialize_risk_scores(core, &out, &nodes)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_risk_score_claims(core, &out, &nodes);
    }
    let rows: Vec<serde_json::Value> = nodes
        .iter()
        .zip(&out.scores)
        .map(|(id, &s)| serde_json::json!({ "node": id, "score": s }))
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "scores": rows,
            "iterations": out.iterations,
            "converged": out.converged,
            "written_back": written,
        })),
    )
}

/// Materialize every node with a NON-ZERO propagated score as a typed
/// `:RiskScore` node (CONCEPT:EG-KG.mining.risk-propagation), linked via
/// `RISK_SCORE_OF` when resident.
pub(super) fn materialize_risk_scores(
    core: &GraphCore,
    out: &risk_propagation::RiskScores,
    nodes: &[String],
) -> usize {
    let mut written = 0usize;
    for (i, id) in nodes.iter().enumerate() {
        let score = out.scores.get(i).copied().unwrap_or(0.0);
        if score <= 0.0 {
            continue;
        }
        let node_id = risk_score_node_id(id);
        let props = serde_json::json!({ "type": "RiskScore", "of": id, "score": score });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        link_writeback_source(core, &node_id, id, "RISK_SCORE_OF");
        written += 1;
    }
    written
}

pub(super) fn risk_score_node_id(of: &str) -> String {
    WritebackNodeId::new("risk_score", &[of]).into_string()
}

/// Quality = the node's OWN propagated share, already `[0,1]` (mass-conserving).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_risk_score_claims(
    core: &GraphCore,
    out: &risk_propagation::RiskScores,
    nodes: &[String],
) {
    for (i, id) in nodes.iter().enumerate() {
        let score = out.scores.get(i).copied().unwrap_or(0.0);
        if score <= 0.0 {
            continue;
        }
        let node_id = risk_score_node_id(id);
        materialize_claim(
            core,
            &node_id,
            "risk_propagation",
            score.clamp(0.0, 1.0),
            "risk:propagated",
        );
    }
}

// ─────────────────────────── Ontology-gap detection ───────────────────────────

/// Project the resident graph's class nodes into [`ontology_gap::ClassNode`]s
/// (CONCEPT:EG-KG.mining.ontology-gap, GRAPH-NATIVE). A node is a "class" when
/// `label` names its exact type, or (when `label` is `None`) its
/// `type`/`node_type` is `Class` or `OwlClass`. `HAS_PROPERTY` edges count
/// declared properties; a `SUBCLASS_OF` edge names a declared parent (resolved
/// when the target is ALSO in this class set); `edge_count` is the class
/// node's total incident edge count anywhere in the graph.
pub(super) fn build_ontology_classes(
    core: &GraphCore,
    label: &Option<String>,
) -> (Vec<ontology_gap::ClassNode>, Vec<String>) {
    let ids: Vec<String> = core
        .get_nodes()
        .into_iter()
        .filter_map(|(node_id, blob)| {
            let node_label = node_type_label(&blob).unwrap_or_default();
            let is_class = match label {
                Some(want) => &node_label == want,
                None => node_label == "Class" || node_label == "OwlClass",
            };
            is_class.then_some(node_id)
        })
        .collect();
    if ids.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let class_set: std::collections::HashSet<String> = ids.iter().cloned().collect();
    let mut projection = OntologyClassProjection::default();
    for (src, dst, blob) in core.get_edges() {
        projection.observe_edge(&class_set, src, dst, edge_relation_label(&blob));
    }
    let classes: Vec<ontology_gap::ClassNode> = ids
        .iter()
        .map(|id| projection.class_node(&class_set, id))
        .collect();
    (classes, ids)
}

/// Edge-derived facts for the ontology classes selected by
/// [`build_ontology_classes`]. Keeping the three coupled indexes together makes
/// the projection's one-pass semantics explicit, including first-parent wins
/// and counting a self-loop at both incident ends.
#[derive(Default)]
pub(super) struct OntologyClassProjection {
    property_count: std::collections::HashMap<String, usize>,
    subclass_of: std::collections::HashMap<String, String>,
    edge_count: std::collections::HashMap<String, usize>,
}

impl OntologyClassProjection {
    fn observe_edge(
        &mut self,
        class_set: &std::collections::HashSet<String>,
        source: String,
        target: String,
        relationship: String,
    ) {
        if class_set.contains(&source) {
            *self.edge_count.entry(source.clone()).or_insert(0) += 1;
            if relationship.eq_ignore_ascii_case("has_property")
                || relationship.eq_ignore_ascii_case("hasproperty")
            {
                *self.property_count.entry(source.clone()).or_insert(0) += 1;
            }
            if relationship.eq_ignore_ascii_case("subclass_of")
                || relationship.eq_ignore_ascii_case("subclassof")
            {
                self.subclass_of
                    .entry(source.clone())
                    .or_insert(target.clone());
            }
        }
        if class_set.contains(&target) {
            *self.edge_count.entry(target).or_insert(0) += 1;
        }
    }

    fn class_node(
        &self,
        class_set: &std::collections::HashSet<String>,
        id: &str,
    ) -> ontology_gap::ClassNode {
        ontology_gap::ClassNode {
            property_count: self.property_count.get(id).copied().unwrap_or(0),
            declares_parent: self.subclass_of.contains_key(id),
            parent_resolves: self
                .subclass_of
                .get(id)
                .is_some_and(|parent| class_set.contains(parent)),
            edge_count: self.edge_count.get(id).copied().unwrap_or(0),
        }
    }
}

pub(crate) fn handle_ontology_gap(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    writeback: WritebackOptions,
) -> Response {
    let (classes, class_ids) = build_ontology_classes(core, &label);
    let gaps = ontology_gap::find_gaps(&classes);
    let written = if writeback.enabled {
        materialize_ontology_gaps(core, &gaps, &class_ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_ontology_gap_claims(core, &gaps, &class_ids, &label);
    }
    let rows: Vec<serde_json::Value> = gaps
        .iter()
        .map(|g| {
            serde_json::json!({
                "class": class_ids.get(g.class_index).cloned().unwrap_or_default(),
                "kind": g.kind.name(),
                "severity": g.kind.severity(),
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "gaps": rows,
            "n_classes": classes.len(),
            "n_gaps": gaps.len(),
            "written_back": written,
        })),
    )
}

/// Materialize each gap as a typed `:OntologyGap` node (CONCEPT:EG-KG.mining.ontology-gap),
/// linked to its class via a `GAP_OF` edge.
pub(super) fn materialize_ontology_gaps(
    core: &GraphCore,
    gaps: &[ontology_gap::OntologyGap],
    class_ids: &[String],
) -> usize {
    let mut written = 0usize;
    for g in gaps {
        let Some(class_id) = class_ids.get(g.class_index) else {
            continue;
        };
        let node_id = ontology_gap_node_id(class_id, g.kind.name());
        let props = serde_json::json!({
            "type": "OntologyGap",
            "class": class_id,
            "kind": g.kind.name(),
            "severity": g.kind.severity(),
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        if core.has_node(class_id) {
            writeback_relationship(core, &node_id, class_id, "GAP_OF");
        }
        written += 1;
    }
    written
}

pub(super) fn ontology_gap_node_id(class_id: &str, kind: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(class_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(kind.as_bytes());
    format!("ontology_gap:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = the gap kind's fixed documented severity (see `ontology_gap` module docs).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_ontology_gap_claims(
    core: &GraphCore,
    gaps: &[ontology_gap::OntologyGap],
    class_ids: &[String],
    label: &Option<String>,
) {
    let provenance = match label {
        Some(l) => format!("ontology:{l}"),
        None => "ontology:*".to_string(),
    };
    for g in gaps {
        let Some(class_id) = class_ids.get(g.class_index) else {
            continue;
        };
        let node_id = ontology_gap_node_id(class_id, g.kind.name());
        materialize_claim(
            core,
            &node_id,
            "ontology_gap",
            g.kind.severity(),
            &provenance,
        );
    }
}

// ─────────────────────────── Retrieval quality ───────────────────────────

pub(super) fn to_retrieval_trace(spec: &RetrievalTraceSpec) -> retrieval_quality::RetrievalTrace {
    retrieval_quality::RetrievalTrace {
        retrieved: spec.retrieved.clone(),
        relevant: spec.relevant.clone(),
    }
}

pub(in crate::server::handlers) fn handle_retrieval_quality(
    req_id: u64,
    core: &GraphCore,
    traces: Vec<RetrievalTraceSpec>,
    k: usize,
    query_id: String,
    writeback: WritebackOptions,
) -> Response {
    if traces.is_empty() {
        return Response::err(
            req_id,
            "mining: retrieval_quality requires non-empty `traces`",
        );
    }
    let specs: Vec<retrieval_quality::RetrievalTrace> =
        traces.iter().map(to_retrieval_trace).collect();
    let report = retrieval_quality::evaluate(&specs, k);
    let written = if writeback.enabled {
        materialize_retrieval_quality(core, &report, &query_id)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_retrieval_quality_claim(core, &report, &query_id);
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "precision_at_k": report.precision_at_k,
            "recall_at_k": report.recall_at_k,
            "mrr": report.mrr,
            "f1": report.f1,
            "ndcg_at_k": report.ndcg_at_k,
            "n_queries": report.n_queries,
            "k": report.k,
            "written_back": written,
        })),
    )
}

/// Materialize the aggregate report as a typed `:RetrievalQuality` node
/// (CONCEPT:EG-KG.mining.retrieval-quality), linked to a resident node named
/// `query_id` via `RETRIEVAL_QUALITY_OF` when one exists.
pub(super) fn materialize_retrieval_quality(
    core: &GraphCore,
    report: &retrieval_quality::RetrievalQuality,
    query_id: &str,
) -> usize {
    let node_id = retrieval_quality_node_id(query_id, report);
    let props = serde_json::json!({
        "type": "RetrievalQuality",
        "precision_at_k": report.precision_at_k,
        "recall_at_k": report.recall_at_k,
        "mrr": report.mrr,
        "f1": report.f1,
        "ndcg_at_k": report.ndcg_at_k,
        "n_queries": report.n_queries,
        "k": report.k,
        "query_id": query_id,
    });
    if !writeback_node(core, &node_id, &props) {
        return 0;
    }
    if !query_id.is_empty() && core.has_node(query_id) {
        writeback_relationship(core, &node_id, query_id, "RETRIEVAL_QUALITY_OF");
    }
    1
}

pub(super) fn retrieval_quality_node_id(
    query_id: &str,
    report: &retrieval_quality::RetrievalQuality,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(query_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(report.n_queries.to_le_bytes());
    hasher.update(report.k.to_le_bytes());
    hasher.update(report.precision_at_k.to_le_bytes());
    hasher.update(report.recall_at_k.to_le_bytes());
    format!(
        "retrieval_quality:{}",
        hex::encode(&hasher.finalize()[..12])
    )
}

/// Quality = the report's own F1 (harmonic mean of precision@k/recall@k), already `[0,1]`.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_retrieval_quality_claim(
    core: &GraphCore,
    report: &retrieval_quality::RetrievalQuality,
    query_id: &str,
) {
    let node_id = retrieval_quality_node_id(query_id, report);
    let provenance = if query_id.is_empty() {
        "retrieval:traces".to_string()
    } else {
        format!("retrieval:{query_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "retrieval_quality",
        report.f1.clamp(0.0, 1.0),
        &provenance,
    );
}

// ─────────────────────────── Community detection (wraps existing GDS) ───────────────────────────

/// Project the resident graph (optionally restricted to one `label`) into a
/// dense `AdjacencyGraph<usize>` for [`community::detect`], mirroring
/// `build_host_graph`'s node/edge projection but over dense usize indices
/// (what `eg_compute::graph_algos` operates on) instead of [`HostGraph`].
pub(super) fn build_id_graph(
    core: &GraphCore,
    label: &Option<String>,
) -> (AdjacencyGraph<usize>, Vec<String>) {
    let all_nodes = core.get_nodes();
    let mut ids: Vec<String> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (node_id, blob) in &all_nodes {
        let node_label = node_type_label(blob).unwrap_or_else(|| "_".to_string());
        if let Some(want) = label {
            if &node_label != want {
                continue;
            }
        }
        index.insert(node_id.clone(), ids.len());
        ids.push(node_id.clone());
    }
    let all_edges = core.get_edges();
    let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
        (0..ids.len()).map(|i| (i, Vec::new())).collect();
    for (src, dst, _blob) in &all_edges {
        let (Some(&si), Some(&di)) = (index.get(src), index.get(dst)) else {
            continue;
        };
        adjacency[si].1.push((di, 1.0));
    }
    (AdjacencyGraph::from_adjacency(adjacency), ids)
}

pub(super) fn to_community_algo(a: CommunityAlgorithm) -> community::Algorithm {
    match a {
        CommunityAlgorithm::Louvain => community::Algorithm::Louvain,
        CommunityAlgorithm::LabelPropagation => community::Algorithm::LabelPropagation,
    }
}

pub(in crate::server::handlers) struct CommunityRequest {
    pub(in crate::server::handlers) label: Option<String>,
    pub(in crate::server::handlers) algorithm: CommunityAlgorithm,
    pub(in crate::server::handlers) resolution: f64,
    pub(in crate::server::handlers) max_iterations: usize,
    pub(in crate::server::handlers) seed: u64,
    pub(in crate::server::handlers) weighted: bool,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_community(
    req_id: u64,
    core: &GraphCore,
    request: CommunityRequest,
) -> Response {
    let CommunityRequest {
        label,
        algorithm,
        resolution,
        max_iterations,
        seed,
        weighted,
        writeback,
    } = request;
    let (graph, ids) = build_id_graph(core, &label);
    let algo = to_community_algo(algorithm);
    let out = community::detect(&graph, algo, resolution, max_iterations, seed, weighted);
    let written = if writeback.enabled {
        materialize_communities(core, &out, &ids)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_community_claims(core, &out, &ids, community_provenance(&label));
    }
    let communities: Vec<serde_json::Value> = out
        .communities
        .iter()
        .map(|c| {
            serde_json::json!({
                "members": c.members.iter().map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string())).collect::<Vec<_>>(),
                "density": c.density,
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "communities": communities,
            "modularity": out.modularity,
            "n_nodes": graph.node_count(),
            "written_back": written,
            // Truncation must not be silent: a budget-expired Louvain run
            // returns the best partition so far, which is otherwise
            // indistinguishable from a converged one — and `writeback` will
            // already have PERSISTED it. See `community::CommunityResult`.
            "deadline_hit": out.deadline_hit,
        })),
    )
}

/// Materialize each MULTI-MEMBER community as a typed `:Community` node
/// (CONCEPT:EG-KG.mining.community-writeback) — a singleton community carries
/// no relational signal worth writing back — linked to its members via
/// `COMMUNITY_MEMBER` edges when resident.
pub(super) fn materialize_communities(
    core: &GraphCore,
    out: &community::CommunityResult,
    ids: &[String],
) -> usize {
    let mut written = 0usize;
    for c in &out.communities {
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
            .collect();
        if member_ids.len() < 2 {
            continue;
        }
        let node_id = community_node_id(&member_ids);
        let props = serde_json::json!({
            "type": "Community",
            "members": member_ids,
            "density": c.density,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for m in &member_ids {
            if core.has_node(m) {
                writeback_relationship(core, &node_id, m, "COMMUNITY_MEMBER");
            }
        }
        written += 1;
    }
    written
}

pub(super) fn community_node_id(member_ids: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted = member_ids.to_vec();
    sorted.sort();
    let mut hasher = Sha256::new();
    hasher.update(sorted.join("\u{1}").as_bytes());
    format!("community:{}", hex::encode(&hasher.finalize()[..12]))
}

/// Quality = the community's own internal-edge density, already `[0,1]`.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_community_claims(
    core: &GraphCore,
    out: &community::CommunityResult,
    ids: &[String],
    provenance: String,
) {
    for c in &out.communities {
        let member_ids: Vec<String> = c
            .members
            .iter()
            .map(|&i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
            .collect();
        if member_ids.len() < 2 {
            continue;
        }
        let node_id = community_node_id(&member_ids);
        materialize_claim(
            core,
            &node_id,
            "community",
            c.density.clamp(0.0, 1.0),
            &provenance,
        );
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn community_provenance(label: &Option<String>) -> String {
    match label {
        Some(l) => format!("graph:{l}"),
        None => "graph:*".to_string(),
    }
}
