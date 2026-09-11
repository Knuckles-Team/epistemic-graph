use super::*;
use super::{association::*, classic::*, vector::*};
use eg_compute::mining::{
    anomaly, association::LabeledRule, classify, cluster, reduce, sequence::LabeledPattern,
    subgraph, text,
};

#[derive(Clone, Copy)]
pub(crate) struct WritebackOptions {
    pub(crate) enabled: bool,
    #[cfg(feature = "epistemic")]
    pub(crate) as_claim: bool,
}

pub(super) struct WritebackNodeId(String);

/// Serialize and materialize one mining-owned node. A serialization failure is
/// reported to the family loop so it preserves its existing skip/count behavior.
pub(super) fn writeback_node(
    core: &GraphCore,
    node_id: &str,
    properties: &serde_json::Value,
) -> bool {
    let Ok(blob) = rmp_serde::to_vec_named(properties) else {
        return false;
    };
    core.add_node(node_id.to_string(), blob);
    true
}

/// Materialize one best-effort relationship between resident mining objects.
pub(super) fn writeback_relationship(
    core: &GraphCore,
    source: &str,
    target: &str,
    relationship: &str,
) {
    let properties = serde_json::json!({ "relationship": relationship });
    if let Ok(blob) = rmp_serde::to_vec_named(&properties) {
        let _ = core.add_edge(source.to_string(), target.to_string(), blob);
    }
}

impl WritebackNodeId {
    pub(super) fn new(prefix: &str, parts: &[&str]) -> Self {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(prefix.as_bytes());
        hasher.update([0u8]);
        for (index, part) in parts.iter().enumerate() {
            if index > 0 {
                hasher.update([0u8]);
            }
            hasher.update(part.as_bytes());
        }
        Self(format!(
            "{prefix}:{}",
            hex::encode(&hasher.finalize()[..12])
        ))
    }

    pub(super) fn into_string(self) -> String {
        self.0
    }
}

/// State seeded on each fresh mining claim and its evidence until validation.
#[cfg(feature = "epistemic")]
pub(super) const CLAIM_VALIDATION_STATE: &str = "unvalidated";

/// This crate's own build version — the `algo_code_version` leg of the universal
/// writeback-lineage tuple (CONCEPT:EG-P3-1), mirroring `eg_jobs::AlgoVersion::code_version`'s
/// own doc ("the engine build that ran it, e.g. `CARGO_PKG_VERSION`").
#[cfg(feature = "epistemic")]
pub(super) const ALGO_CODE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The runtime/feature-set fingerprint the algorithm ran under (CONCEPT:EG-P3-1) — the
/// `algo_env_version` leg, mirroring `eg_jobs::AlgoVersion::env_version`'s own doc. Kept
/// simple/deterministic-per-build: target OS + architecture.
#[cfg(feature = "epistemic")]
pub(super) fn algo_env_version() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

/// Materialize the epistemic quartet (`:Claim` + `:Evidence` + `:Activity` + their
/// `SUPPORTS`/`GENERATED_BY` edges) for ONE mined finding whose typed node
/// (`mined_node_id`) was just written back. Beyond the claim/evidence pair (E6), this now
/// ALSO stamps the universal writeback-lineage tuple (CONCEPT:EG-P3-1): an input-snapshot
/// handle (the core's OCC `version()` at commit time), algo family/code/env version,
/// a `calibration` slot (honestly `null` — no calibration signal is computed by the
/// generic mining path today; a future family-specific caller can populate it),
/// and `invalidation_deps` — the ids whose change/removal invalidates this claim (the
/// mined finding + its evidence node; this is also exactly the `SUPPORTS` topology
/// `eg_epistemic::propagate_confidence` already walks, so a change to either
/// automatically reflows through the claim's belief — this property just makes that
/// dependency set an explicit, directly-queryable list).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_claim(
    core: &GraphCore,
    mined_node_id: &str,
    family: &str,
    confidence: f64,
    provenance: &str,
) {
    let confidence = confidence.clamp(0.0, 1.0);
    let claim_id = claim_node_id(family, mined_node_id);
    let evidence_id = evidence_node_id(family, mined_node_id, provenance);
    let activity_id = activity_node_id(family, provenance);
    let claim_props = serde_json::json!({
        "type": "Claim",
        "family": family,
        "about": mined_node_id,
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        // CONCEPT:EG-P3-1 — universal writeback-lineage tuple.
        "input_snapshot_version": core.version(),
        "algo_family": family,
        "algo_provenance": provenance,
        "algo_code_version": ALGO_CODE_VERSION,
        "algo_env_version": algo_env_version(),
        "calibration": serde_json::Value::Null,
        "invalidation_deps": [mined_node_id, evidence_id.as_str()],
    });
    let _ = writeback_node(core, &claim_id, &claim_props);
    // The mined finding itself is evidence FOR the claim.
    supports_edge(core, mined_node_id, &claim_id);
    // A provenance-anchored Evidence node (distinct provenance ⇒ corroboration).
    let ev_props = serde_json::json!({
        "type": "Evidence",
        "family": family,
        "about": mined_node_id,
        "provenance": provenance,
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
    });
    let _ = writeback_node(core, &evidence_id, &ev_props);
    supports_edge(core, &evidence_id, &claim_id);

    // CONCEPT:EG-P3-1 — the generating Activity (the mining run itself). One per
    // `(family, provenance)` (idempotent, mirrors `evidence_node_id`'s dedup-by-
    // provenance): re-running the same family+provenance converges on the SAME
    // Activity rather than accumulating a fresh one per call.
    let activity_props = serde_json::json!({
        "type": "Activity",
        "family": family,
        "provenance": provenance,
        "algo_code_version": ALGO_CODE_VERSION,
        "algo_env_version": algo_env_version(),
        "input_snapshot_version": core.version(),
    });
    let _ = writeback_node(core, &activity_id, &activity_props);
    generated_by_edge(core, &claim_id, &activity_id);
}

/// Write one epistemic `source --SUPPORTS--> target` edge using the canonical `relationship`
/// property key `eg_epistemic` reads (NOT the `relation` key the structural mining edges
/// use). Both endpoints are freshly resident, so `add_edge` always binds.
#[cfg(feature = "epistemic")]
pub(super) fn supports_edge(core: &GraphCore, source: &str, target: &str) {
    writeback_relationship(core, source, target, "SUPPORTS");
}

/// Write one `claim --GENERATED_BY--> activity` edge (CONCEPT:EG-P3-1) — deliberately
/// NOT one of `classify_relationship`'s whitelisted values, so `BeliefGraph` ignores it
/// (epistemically neutral, exactly like the mining handlers' `relation`-keyed structural
/// edges). `eg-plan`'s `KnowledgeSet::from_rowset` resolves it into
/// `KnowledgeRow::transformation_ids`.
#[cfg(feature = "epistemic")]
pub(super) fn generated_by_edge(core: &GraphCore, source: &str, target: &str) {
    writeback_relationship(core, source, target, "GENERATED_BY");
}

/// Deterministic `:Claim` node id — folds in `family` + the mined node id, so re-mining
/// the same finding re-points at the same claim (idempotent replay + corroboration).
#[cfg(feature = "epistemic")]
pub(super) fn claim_node_id(family: &str, mined_node_id: &str) -> String {
    WritebackNodeId::new("claim", &[family, mined_node_id]).into_string()
}

/// Deterministic `:Evidence` node id — ALSO folds in `provenance`, so two runs over
/// DIFFERENT provenance produce distinct evidence nodes that both support the same claim.
#[cfg(feature = "epistemic")]
pub(super) fn evidence_node_id(family: &str, mined_node_id: &str, provenance: &str) -> String {
    WritebackNodeId::new("evidence", &[family, mined_node_id, provenance]).into_string()
}

/// Deterministic `:Activity` node id (CONCEPT:EG-P3-1) — folds in `(family, provenance)`,
/// so repeated runs over the SAME provenance converge on the SAME generating Activity
/// (idempotent, mirrors `evidence_node_id`'s dedup-by-provenance).
#[cfg(feature = "epistemic")]
pub(super) fn activity_node_id(family: &str, provenance: &str) -> String {
    WritebackNodeId::new("activity", &[family, provenance]).into_string()
}

// ── Per-family claim passes (mirror each `materialize_*` node/id + quality score) ──

/// Association rules → claims. Quality = `support × confidence` (both already `[0,1]`,
/// jointly monotonic). Provenance = the transaction `source` label (or `explicit`).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_rule_claims(
    core: &GraphCore,
    rules: &[LabeledRule],
    source: &Option<TransactionSource>,
) {
    let provenance = assoc_provenance(source);
    for r in rules {
        let node_id = rule_node_id(&r.antecedent, &r.consequent);
        let confidence = (r.support * r.confidence).clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "association", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn assoc_provenance(source: &Option<TransactionSource>) -> String {
    match source {
        Some(s) => format!("txn:{}/{}", s.node_label, s.direction),
        None => "txn:explicit".to_string(),
    }
}

/// Clusters → claims (skipping the DBSCAN noise bucket, mirroring `materialize_clusters`).
/// Quality = `1/(1 + compactness score)` — a tighter cluster (lower mean member→centroid
/// distance) yields a higher-confidence claim.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_cluster_claims(
    core: &GraphCore,
    out: &cluster::Clustering,
    ids: &[String],
    algorithm: ClusterAlgorithm,
    provenance: String,
) {
    let algo = cluster_algo_name(algorithm);
    for c in &out.clusters {
        if c.cluster_id < 0 {
            continue; // never claim the DBSCAN noise bucket
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
        let confidence = 1.0 / (1.0 + c.score.max(0.0));
        materialize_claim(core, &node_id, "cluster", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn cluster_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Flagged anomalies → claims (only the flagged rows, mirroring `materialize_anomalies`).
/// Quality = `score / (1 + score)` — a higher anomaly score yields a higher-confidence
/// "this row is anomalous" claim.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_anomaly_claims(
    core: &GraphCore,
    out: &anomaly::Anomalies,
    ids: &[String],
    algorithm: AnomalyAlgorithm,
    provenance: String,
) {
    let algo = anomaly_algo_name(algorithm);
    for i in 0..out.scores.len() {
        if !out.is_anomaly[i] {
            continue;
        }
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = anomaly_node_id(algo, &src);
        let s = out.scores[i].max(0.0);
        let confidence = s / (1.0 + s);
        materialize_claim(core, &node_id, "anomaly", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn anomaly_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Sequential patterns → claims. Quality = the pattern's fractional `support`.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_sequence_claims(
    core: &GraphCore,
    patterns: &[LabeledPattern],
    provenance: String,
) {
    for p in patterns {
        let node_id = pattern_node_id(&p.items);
        let confidence = p.support.clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "sequence", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn sequence_provenance(source: &Option<SequenceSource>) -> String {
    match source {
        Some(s) => format!("seq:{}/{}", s.node_label, s.direction),
        None => "seq:explicit".to_string(),
    }
}

/// The single forecast → one claim. Quality = the forecast's `confidence` band level.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_forecast_claim(
    core: &GraphCore,
    series_id: &str,
    values: &[f64],
    algo: &str,
    confidence: f64,
) {
    let node_id = forecast_node_id(algo, series_id, values);
    let provenance = if series_id.is_empty() {
        "series:values".to_string()
    } else {
        format!("series:{series_id}")
    };
    materialize_claim(
        core,
        &node_id,
        "forecast",
        confidence.clamp(0.0, 1.0),
        &provenance,
    );
}

/// Frequent subgraph patterns → claims. Quality = the pattern's fractional `support`.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_subgraph_claims(
    core: &GraphCore,
    results: &[subgraph::FrequentSubgraph],
    provenance: String,
) {
    for r in results {
        let node_id = subgraph_node_id(&r.pattern);
        let confidence = r.support.clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "subgraph", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn subgraph_provenance(label: &Option<String>) -> String {
    match label {
        Some(l) => format!("graph:{l}"),
        None => "graph:*".to_string(),
    }
}

// ── D3 — the 3 remaining mining families (E6 left these ungated) ──
//
// MineClassifyPredict → claims (principled: the prediction's OWN max class
// probability, already [0,1] by construction — a probability-simplex row).
// MineReduce → claims, `svd` ONLY (principled: the retained explained-variance
// ratio; `lda`/`umap`/`tsne` have no such score — documented no-op, NOT fabricated).
// MineText → claims, `lda`/`nmf` ONLY (principled: per-topic mean doc-membership
// strength among its dominantly-assigned documents — both engines' `doc_topics`
// are already [0,1] distributions summing to 1, see `eg_compute::mining::text`
// module docs; `tfidf` has no topics — documented no-op, mirroring `writeback`).

/// Each prediction → a claim. Quality = the row's OWN max class probability
/// (`out.proba[i]`'s argmax) — already `[0,1]`, no normalization needed.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_classification_claims(
    core: &GraphCore,
    out: &classify::Classification,
    ids: &[String],
    provenance: String,
) {
    for i in 0..out.labels.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = classification_node_id(&src);
        let confidence = out.proba[i]
            .iter()
            .cloned()
            .fold(0.0_f64, f64::max)
            .clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "classification", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn classify_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Reduced rows → claims, `svd` ONLY (mirroring `writeback`'s per-algorithm gate —
/// this is a DOCUMENTED SKIP for `lda`/`umap`/`tsne`, not a fabricated score: LDA's
/// discriminant eigenvalues aren't returned by `reduce::reduce`; UMAP/t-SNE are
/// approximate neighborhood LAYOUTS with no reconstruction-error analogue). Quality =
/// the retained EXPLAINED-VARIANCE RATIO `Σ singular_values² / Σ ‖row‖²` — the SAME
/// Frobenius-energy ratio `eg_compute::mining::reduce`'s own
/// `truncated_svd_reconstructs_low_rank` test validates against, so it is principled
/// and requires no extra normalization beyond the final `[0,1]` clamp (guards the
/// pathological case where retained energy rounds slightly above total due to
/// floating-point error). The ratio is a property of the WHOLE projection, not any
/// one row, so every materialized `:Embedding2D` row node gets a claim sharing this
/// ONE reduction-level score (mirrors the "one score per materialized artifact"
/// shape every other mining family's claim pass uses).
#[cfg(feature = "epistemic")]
pub(super) fn materialize_reduce_claims(
    core: &GraphCore,
    rows: &[Vec<f64>],
    out: &reduce::Reduction,
    ids: &[String],
    algorithm: ReduceAlgorithm,
    source: &Option<VectorSource>,
) {
    if !matches!(algorithm, ReduceAlgorithm::Svd) || out.singular_values.is_empty() {
        return; // no principled [0,1] score for lda/umap/tsne — documented skip
    }
    let total_energy: f64 = rows.iter().flatten().map(|&v| v * v).sum();
    if total_energy <= 0.0 {
        return; // degenerate (all-zero) input — no meaningful ratio to claim
    }
    let retained: f64 = out.singular_values.iter().map(|&s| s * s).sum();
    let confidence = (retained / total_energy).clamp(0.0, 1.0);
    let provenance = reduce_provenance(source);
    for i in 0..out.coords.len() {
        let src = ids.get(i).cloned().unwrap_or_else(|| i.to_string());
        let node_id = embedding2d_node_id(&src);
        materialize_claim(core, &node_id, "reduce", confidence, &provenance);
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn reduce_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}

/// Each topic → a claim, `lda`/`nmf` ONLY (mirroring `writeback`'s tfidf no-op — a
/// bag-of-weights table has no topics to claim about). Quality = the topic's mean
/// doc-membership strength among the documents DOMINANTLY assigned to it
/// (`mean(doc_topics[d][t])` over docs `d` whose argmax topic is `t`) — a topic-
/// coherence proxy: both LDA's Dirichlet posterior and NMF's row-normalized `W` are
/// already `[0,1]` distributions summing to 1 across topics (see
/// `eg_compute::mining::text` module docs), so this is principled and needs no extra
/// normalization. A topic nobody is dominantly assigned to (can happen for `nmf`,
/// whose factors are not a hard partition) has no coherence signal — skipped rather
/// than fabricated.
#[cfg(feature = "epistemic")]
pub(super) fn materialize_topic_claims(
    core: &GraphCore,
    out: &text::LabeledTextResult,
    algo: &str,
) {
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

    for (t, terms) in out.topics.iter().enumerate() {
        let term_labels: Vec<&str> = terms.iter().map(|(term, _)| term.as_str()).collect();
        let mut sum = 0.0_f64;
        let mut n = 0usize;
        for (i, dist) in out.doc_topics.iter().enumerate() {
            if dominant.get(i) == Some(&t) {
                if let Some(&w) = dist.get(t) {
                    sum += w;
                    n += 1;
                }
            }
        }
        if n == 0 {
            continue; // no document dominantly assigned to this topic — no signal
        }
        let node_id = topic_node_id(algo, &term_labels);
        let confidence = (sum / n as f64).clamp(0.0, 1.0);
        materialize_claim(core, &node_id, "topic", confidence, &format!("text:{algo}"));
    }
}
