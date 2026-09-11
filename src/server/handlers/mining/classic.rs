use super::*;
use super::{association::*, writeback::*};
use eg_compute::mining::{
    forecast,
    sequence::{self, LabeledPattern},
    subgraph::{self, HostGraph},
    text,
};

// ─────────────────────────── Sequential-pattern mining ───────────────────────────

/// Handle `MineSequence` (CONCEPT:EG-KG.mining.prefixspan — Phase 4): build the
/// ordered sequences (explicit or graph-derived), run the chosen engine
/// (PrefixSpan/GSP — both agree), return `{patterns, ...}`, and optionally write
/// `:SequentialPattern` nodes back.
pub(in crate::server::handlers) fn handle_sequence(
    req_id: u64,
    core: &GraphCore,
    sequences: Vec<Vec<String>>,
    source: Option<SequenceSource>,
    min_support: f64,
    algorithm: MineSeqAlgorithm,
    writeback: WritebackOptions,
) -> Response {
    let seqs = build_sequences(core, &sequences, &source);
    let patterns = sequence::mine_labeled(&seqs, min_support, to_seq_algo(algorithm));

    let written = if writeback.enabled {
        materialize_patterns(core, &patterns)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_sequence_claims(core, &patterns, sequence_provenance(&source));
    }

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
pub(super) fn build_sequences(
    core: &GraphCore,
    sequences: &[Vec<String>],
    source: &Option<SequenceSource>,
) -> Vec<Vec<String>> {
    if !sequences.is_empty() {
        return sequences.to_vec();
    }
    match source {
        Some(spec) => derive_sequences_from_graph(core, spec),
        None => Vec::new(),
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
pub(super) fn derive_sequences_from_graph(
    core: &GraphCore,
    spec: &SequenceSource,
) -> Vec<Vec<String>> {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let mut out: Vec<Vec<String>> = Vec::with_capacity(owners.len());
    for (owner_id, _blob) in owners {
        let seq = project_neighbor_items(
            core,
            &owner_id,
            &spec.direction,
            &spec.relation,
            &spec.item_field,
            true,
        );
        if !seq.is_empty() {
            out.push(seq);
        }
    }
    out
}

pub(super) fn to_seq_algo(a: MineSeqAlgorithm) -> sequence::Algorithm {
    match a {
        MineSeqAlgorithm::Prefixspan => sequence::Algorithm::PrefixSpan,
        MineSeqAlgorithm::Gsp => sequence::Algorithm::Gsp,
    }
}

/// Materialize each mined pattern as a typed `:SequentialPattern` node
/// (CONCEPT:EG-KG.mining.sequence-writeback), id = a deterministic digest of its
/// (order-preserving) item list. Linked to any item that is a resident node via
/// a `PATTERN_ITEM` edge, mirroring `materialize_rules`.
pub(super) fn materialize_patterns(core: &GraphCore, patterns: &[LabeledPattern]) -> usize {
    let mut written = 0usize;
    for p in patterns {
        let node_id = pattern_node_id(&p.items);
        let props = serde_json::json!({
            "type": "SequentialPattern",
            "items": p.items,
            "support": p.support,
            "count": p.count,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for item in &p.items {
            if core.has_node(item) {
                writeback_relationship(core, &node_id, item, "PATTERN_ITEM");
            }
        }
        written += 1;
    }
    written
}

/// Deterministic, collision-resistant node id for a pattern (order matters, so
/// the digest is over the items in sequence — unlike a rule's antecedent/
/// consequent, which are pre-sorted sets).
pub(super) fn pattern_node_id(items: &[String]) -> String {
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
pub(in crate::server::handlers) struct ForecastRequest {
    pub(in crate::server::handlers) values: Vec<f64>,
    pub(in crate::server::handlers) algorithm: ForecastAlgorithm,
    pub(in crate::server::handlers) horizon: usize,
    pub(in crate::server::handlers) p: usize,
    pub(in crate::server::handlers) d: usize,
    pub(in crate::server::handlers) q: usize,
    pub(in crate::server::handlers) period: usize,
    pub(in crate::server::handlers) alpha: f64,
    pub(in crate::server::handlers) beta: f64,
    pub(in crate::server::handlers) gamma: f64,
    pub(in crate::server::handlers) confidence: f64,
    pub(in crate::server::handlers) series_id: String,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_forecast(
    req_id: u64,
    core: &GraphCore,
    request: ForecastRequest,
) -> Response {
    let ForecastRequest {
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
    } = request;
    if values.is_empty() {
        return Response::err(
            req_id,
            "mining: forecast requires a non-empty `values` series",
        );
    }
    let algo = forecast_algo(algorithm, p, d, q, period, alpha, beta, gamma);
    let out = forecast::forecast(&values, algo, horizon, confidence);

    let written = if writeback.enabled {
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
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_forecast_claim(
            core,
            &series_id,
            &values,
            forecast_algo_name(algorithm),
            confidence,
        );
    }

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
pub(super) fn forecast_algo(
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

pub(super) fn forecast_algo_name(a: ForecastAlgorithm) -> &'static str {
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
pub(super) fn materialize_forecast(
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
    if !writeback_node(core, &node_id, &props) {
        return 0;
    }
    if !series_id.is_empty() && core.has_node(series_id) {
        writeback_relationship(core, &node_id, series_id, "FORECAST_OF");
    }
    1
}

pub(super) fn forecast_node_id(algo: &str, series_id: &str, values: &[f64]) -> String {
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
pub(in crate::server::handlers) struct TextRequest {
    pub(in crate::server::handlers) docs: Vec<Vec<String>>,
    pub(in crate::server::handlers) source: Option<TextSource>,
    pub(in crate::server::handlers) algorithm: TextAlgorithm,
    pub(in crate::server::handlers) k: usize,
    pub(in crate::server::handlers) alpha: f64,
    pub(in crate::server::handlers) beta: f64,
    pub(in crate::server::handlers) iterations: usize,
    pub(in crate::server::handlers) seed: u64,
    pub(in crate::server::handlers) top_n: usize,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_text(
    req_id: u64,
    core: &GraphCore,
    request: TextRequest,
) -> Response {
    let TextRequest {
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
    } = request;
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

    let written = if writeback.enabled && !matches!(algorithm, TextAlgorithm::Tfidf) {
        materialize_topics(core, &out, &ids, text_algo_name(algorithm))
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim && !matches!(algorithm, TextAlgorithm::Tfidf) {
        materialize_topic_claims(core, &out, text_algo_name(algorithm));
    }

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

pub(super) fn to_text_algo(
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

pub(super) fn text_algo_name(a: TextAlgorithm) -> &'static str {
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
pub(super) fn build_text_docs(
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
        let Ok(props) = eg_types::msgpack::decode_property_value(&blob) else {
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
pub(super) fn materialize_topics(
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
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for (i, doc_id) in ids.iter().enumerate() {
            if dominant.get(i) == Some(&t) && core.has_node(doc_id) {
                writeback_relationship(core, doc_id, &node_id, "HAS_TOPIC");
            }
        }
        written += 1;
    }
    written
}

pub(super) fn topic_node_id(algo: &str, terms: &[&str]) -> String {
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
pub(in crate::server::handlers) fn handle_subgraph(
    req_id: u64,
    core: &GraphCore,
    label: Option<String>,
    min_support: f64,
    max_edges: usize,
    algorithm: SubgraphAlgorithm,
    writeback: WritebackOptions,
) -> Response {
    let (host, ids) = build_host_graph(core, &label);
    let n_host_nodes = host.node_count();
    let n_host_edges = host.edge_count();

    match algorithm {
        SubgraphAlgorithm::Gspan => {
            let results = subgraph::mine_gspan(&host, min_support, max_edges);
            let written = if writeback.enabled {
                materialize_subgraphs(core, &results, &ids)
            } else {
                0
            };
            #[cfg(feature = "epistemic")]
            if writeback.enabled && writeback.as_claim {
                materialize_subgraph_claims(core, &results, subgraph_provenance(&label));
            }
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
/// canonical `relationship` label (defaulting to `"_"` when absent). When
/// `label_filter` is given, only nodes of that ONE type are included (both
/// edge endpoints must be included for the edge to count). Returns the host
/// graph AND a parallel `ids` vec (dense index → resident node id).
pub(super) fn build_host_graph(
    core: &GraphCore,
    label_filter: &Option<String>,
) -> (HostGraph, Vec<String>) {
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
pub(super) fn node_type_label(blob: &[u8]) -> Option<String> {
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}

/// Extract an edge's canonical `relationship` from its property blob, defaulting
/// to `"_"` when none is set — an unlabeled edge is
/// still a valid, matchable edge, just under one shared label.
pub(super) fn edge_relation_label(blob: &[u8]) -> String {
    let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
        return "_".to_string();
    };
    if let Some(s) = val.get("relationship").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    "_".to_string()
}

pub(super) fn subgraph_algo_name(a: SubgraphAlgorithm) -> &'static str {
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
pub(super) fn materialize_subgraphs(
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
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for &member_idx in &r.member_nodes {
            if let Some(member_id) = ids.get(member_idx) {
                if core.has_node(member_id) {
                    writeback_relationship(core, &node_id, member_id, "SUBGRAPH_MEMBER");
                }
            }
        }
        written += 1;
    }
    written
}

pub(super) fn subgraph_node_id(pattern: &subgraph::Pattern) -> String {
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
