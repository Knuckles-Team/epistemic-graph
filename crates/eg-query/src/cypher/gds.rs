//! Graph-Data-Science `CALL gds.*` procedures (CONCEPT:EG-KG.query.gds-call-procedures).
//!
//! This is the Cypher `CALL gds.<algo>(config) YIELD …` surface over the
//! deterministic Neo4j-GDS-parity kernels shipped in
//! [`eg_compute::graph_algos`] (CONCEPT:EG-KG.compute.graph-data-science-algorithms). Each procedure:
//!
//!   1. **Projects** the current graph view (nodes + edges, with an optional
//!      `relationshipWeightProperty`) into an
//!      [`eg_compute::graph_algos::AdjacencyGraph`] — the same generic,
//!      unit-tested value the kernels operate on;
//!   2. **Parses** the GDS-style config map (`dampingFactor`, `maxIterations`,
//!      `relationshipWeightProperty`, `topK`, `similarityCutoff`,
//!      `orientation`, …) out of the first `{…}` argument;
//!   3. **Runs** the kernel and **streams** the result back as Cypher `YIELD`
//!      rows (`nodeId`, `score` / `communityId` / `componentId` / `cost` /
//!      `similarity`).
//!
//! The whole surface is **deterministic**: the kernels have no RNG (Louvain's
//! optional seeded shuffle is left at its order-deterministic default) and every
//! tie-break falls back to sorted node-id order, so a given graph + config always
//! yields the same rows.
//!
//! Backward-compatibility (CONCEPT:EG-KG.query.gds-call-procedures): these procedures REPLACE the earlier
//! EG-143 GDS stubs (which ran over the live `GraphView` with no config), and they
//! keep yielding the legacy `node` / `score` / `communityId` / `componentId`
//! columns *in addition to* the GDS-canonical `nodeId` column, so existing
//! `YIELD node, score` queries keep working unchanged.

use std::collections::HashMap;

use eg_core::graph::GraphView;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde_json::Value;

use eg_compute::graph_algos::{
    all_pairs_similarity, betweenness_centrality, degree_centrality, dijkstra, knn_similarity,
    label_propagation, louvain, pagerank, strongly_connected_components,
    weakly_connected_components, AdjacencyGraph, DegreeKind, Direction, LabelPropagationConfig,
    LouvainConfig, Metric, PageRankConfig,
};

use super::proc::{CypherProcedure, ProcRow, YieldValue};

/// Every GDS procedure, ready to fold into the procedure registry
/// (CONCEPT:EG-KG.query.gds-call-procedures / CONCEPT:EG-KG.query.gds-procedure-routing). Consumed
/// by `proc::build_registry`. The base 10 (EG-298 + `labelPropagation`/`knn`)
/// always route to always-on `graph_algos` kernels; `gds.dbscan` and
/// `gds.linkPrediction` are gated behind the `cypher-mining`/`cypher-graphlearn`
/// features (they route to heavier eg-compute domains — see `Cargo.toml`).
pub fn gds_procedures() -> Vec<Box<dyn CypherProcedure>> {
    #[allow(unused_mut)]
    let mut v: Vec<Box<dyn CypherProcedure>> = vec![
        Box::new(PageRank),
        Box::new(Betweenness),
        Box::new(Degree),
        Box::new(Louvain),
        Box::new(Wcc),
        Box::new(Scc),
        Box::new(Dijkstra),
        Box::new(NodeSimilarity),
        Box::new(LabelPropagation),
        Box::new(Knn),
    ];
    #[cfg(feature = "cypher-mining")]
    v.push(Box::new(Dbscan));
    #[cfg(feature = "cypher-graphlearn")]
    v.push(Box::new(LinkPrediction));
    v
}

// ── projection + config helpers (CONCEPT:EG-KG.query.gds-call-procedures) ────────────────────────────────

/// Project the current graph view into a generic [`AdjacencyGraph`] over string
/// node ids (CONCEPT:EG-KG.query.gds-call-procedures). Every node in the view is registered (so isolated
/// nodes still appear in per-node results); each directed edge contributes weight
/// `1.0`, or — when `weight_prop` is set — the numeric value of that property on
/// the edge (falling back to `1.0` when absent). Parallel edges between the same
/// ordered pair are summed by the `AdjacencyGraph` builder.
fn project(view: &GraphView, weight_prop: Option<&str>) -> AdjacencyGraph<String> {
    let mut by_src: HashMap<String, Vec<(String, f64)>> = view
        .node_map
        .keys()
        .map(|k| (k.clone(), Vec::new()))
        .collect();
    for eref in view.graph.edge_references() {
        let s = view.graph[eref.source()].clone();
        let t = view.graph[eref.target()].clone();
        let w = weight_prop
            .and_then(|p| edge_weight(view, &s, &t, p))
            .unwrap_or(1.0);
        by_src.entry(s).or_default().push((t, w));
    }
    AdjacencyGraph::from_adjacency(by_src)
}

/// The numeric `prop` on the first `(s, t)` edge property record carrying it, if
/// any (CONCEPT:EG-KG.query.gds-call-procedures).
fn edge_weight(view: &GraphView, s: &str, t: &str, prop: &str) -> Option<f64> {
    let blobs = view.edge_properties.get(&(s.to_string(), t.to_string()))?;
    for blob in blobs {
        if let Ok(Value::Object(m)) = rmp_serde::from_slice::<Value>(blob) {
            if let Some(w) = m.get(prop).and_then(|v| v.as_f64()) {
                return Some(w);
            }
        }
    }
    None
}

/// A parsed GDS config map (CONCEPT:EG-KG.query.gds-call-procedures): the first `{…}` object argument to a
/// `CALL gds.*(config)`, with typed accessors that supply GDS-default values.
struct Config<'a> {
    map: Option<&'a serde_json::Map<String, Value>>,
}

impl<'a> Config<'a> {
    fn of(args: &'a [Value]) -> Self {
        let map = args.iter().find_map(|a| match a {
            Value::Object(m) => Some(m),
            _ => None,
        });
        Self { map }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.map.and_then(|m| m.get(key))
    }

    fn f64(&self, key: &str, default: f64) -> f64 {
        self.get(key).and_then(Value::as_f64).unwrap_or(default)
    }

    fn usize(&self, key: &str, default: usize) -> usize {
        self.get(key)
            .and_then(Value::as_u64)
            .map(|v| v as usize)
            .unwrap_or(default)
    }

    fn string(&self, key: &str) -> Option<String> {
        self.get(key).and_then(Value::as_str).map(str::to_string)
    }
}

/// A finite `f64` as a JSON number (an integer when integral, mirroring the
/// executor's numeric coercion), else null (CONCEPT:EG-KG.query.gds-call-procedures).
fn num(x: f64) -> Value {
    if x.fract() == 0.0 && x.abs() < 9.007e15 {
        Value::Number((x as i64).into())
    } else {
        serde_json::Number::from_f64(x)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

/// One `(node, nodeId, <score-col>)` row: the id is bound twice — as the legacy
/// anchorable `node` column and as the GDS-canonical `nodeId` column (CONCEPT:EG-KG.query.gds-call-procedures).
fn scored_row(id: String, col: &str, score: f64) -> ProcRow {
    vec![
        ("node".to_string(), YieldValue::Node(id.clone())),
        ("nodeId".to_string(), YieldValue::Node(id)),
        (col.to_string(), YieldValue::Scalar(num(score))),
    ]
}

/// `(node, nodeId, <score-col>)` rows from a `Vec<(id, f64)>` kernel result.
fn scored_rows(scored: Vec<(String, f64)>, col: &str) -> Vec<ProcRow> {
    scored
        .into_iter()
        .map(|(id, s)| scored_row(id, col, s))
        .collect()
}

/// `(node, nodeId, <group-col>)` rows from a partition (`Vec<Vec<id>>`): every node
/// tagged with its 0-based group index (CONCEPT:EG-KG.query.gds-call-procedures).
fn partition_rows(groups: Vec<Vec<String>>, col: &str) -> Vec<ProcRow> {
    let mut out = Vec::new();
    for (gid, members) in groups.into_iter().enumerate() {
        for id in members {
            out.push(vec![
                ("node".to_string(), YieldValue::Node(id.clone())),
                ("nodeId".to_string(), YieldValue::Node(id)),
                (
                    col.to_string(),
                    YieldValue::Scalar(Value::Number((gid as u64).into())),
                ),
            ]);
        }
    }
    out
}

// ── centrality / ranking (CONCEPT:EG-KG.query.gds-call-procedures) ───────────────────────────────────────

/// `gds.pageRank(config)` — power-iteration PageRank (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `dampingFactor` (0.85), `maxIterations` (20), `tolerance` (1e-7),
/// `relationshipWeightProperty`. Yields `nodeId` / `node`, `score`.
struct PageRank;
impl CypherProcedure for PageRank {
    fn name(&self) -> &'static str {
        "gds.pageRank"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let pr = PageRankConfig {
            damping: cfg.f64("dampingFactor", 0.85),
            tolerance: cfg.f64("tolerance", 1e-7),
            max_iterations: cfg.usize("maxIterations", 20),
        };
        Ok(scored_rows(pagerank(&g, &pr).scores, "score"))
    }
}

/// `gds.betweenness(config)` — Brandes betweenness centrality (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `orientation` (`NATURAL` directed [default] / `UNDIRECTED`),
/// `relationshipWeightProperty` (topology only; Brandes here is hop-based).
/// Yields `nodeId` / `node`, `score`.
struct Betweenness;
impl CypherProcedure for Betweenness {
    fn name(&self) -> &'static str {
        "gds.betweenness"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let directed = !cfg
            .string("orientation")
            .map(|o| o.eq_ignore_ascii_case("UNDIRECTED"))
            .unwrap_or(false);
        Ok(scored_rows(betweenness_centrality(&g, directed), "score"))
    }
}

/// `gds.degree(config)` — degree centrality (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `orientation` (`NATURAL`→out-degree [default] / `REVERSE`→in-degree /
/// `UNDIRECTED`→total). Yields `nodeId` / `node`, `score`.
struct Degree;
impl CypherProcedure for Degree {
    fn name(&self) -> &'static str {
        "gds.degree"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, None);
        let kind = match cfg.string("orientation").as_deref() {
            Some(o) if o.eq_ignore_ascii_case("REVERSE") => DegreeKind::In,
            Some(o) if o.eq_ignore_ascii_case("UNDIRECTED") => DegreeKind::Total,
            _ => DegreeKind::Out,
        };
        Ok(scored_rows(degree_centrality(&g, kind), "score"))
    }
}

// ── community / components (CONCEPT:EG-KG.query.gds-call-procedures) ──────────────────────────────────────

/// `gds.louvain(config)` — Louvain community detection (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `resolution` (1.0), `maxLevels` (50), `maxIterations`/`maxSweeps` (100),
/// `relationshipWeightProperty`. Yields `nodeId` / `node`, `communityId`.
struct Louvain;
impl CypherProcedure for Louvain {
    fn name(&self) -> &'static str {
        "gds.louvain"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "communityId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let lc = LouvainConfig {
            resolution: cfg.f64("resolution", 1.0),
            seed: None,
            max_sweeps: cfg.usize("maxIterations", cfg.usize("maxSweeps", 100)),
            max_levels: cfg.usize("maxLevels", 50),
        };
        Ok(partition_rows(louvain(&g, &lc).communities, "communityId"))
    }
}

/// `gds.labelPropagation(config)` — synchronous Label Propagation (LPA) community
/// detection (CONCEPT:EG-KG.query.gds-procedure-routing). Config: `maxIterations` (10),
/// `relationshipWeightProperty` (edge-weighted neighbour voting; unweighted votes
/// when absent). Yields `nodeId` / `node`, `communityId`. Routes to
/// `eg_compute::graph_algos::label_propagation` (CONCEPT:EG-KG.compute.label-propagation) —
/// no algorithm reimplementation here.
struct LabelPropagation;
impl CypherProcedure for LabelPropagation {
    fn name(&self) -> &'static str {
        "gds.labelPropagation"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "communityId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let weight_prop = cfg.string("relationshipWeightProperty");
        let g = project(view, weight_prop.as_deref());
        let lpc = LabelPropagationConfig {
            max_iterations: cfg.usize("maxIterations", 10),
            weighted: weight_prop.is_some(),
        };
        Ok(partition_rows(
            label_propagation(&g, &lpc).communities,
            "communityId",
        ))
    }
}

/// `gds.wcc(config)` — weakly-connected components (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `relationshipWeightProperty` (topology only). Yields `nodeId` / `node`,
/// `componentId`.
struct Wcc;
impl CypherProcedure for Wcc {
    fn name(&self) -> &'static str {
        "gds.wcc"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "componentId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        Ok(partition_rows(
            weakly_connected_components(&g),
            "componentId",
        ))
    }
}

/// `gds.scc(config)` — strongly-connected components (Tarjan) (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `relationshipWeightProperty` (topology only). Yields `nodeId` / `node`,
/// `componentId`.
struct Scc;
impl CypherProcedure for Scc {
    fn name(&self) -> &'static str {
        "gds.scc"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "componentId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        Ok(partition_rows(
            strongly_connected_components(&g),
            "componentId",
        ))
    }
}

// ── shortest path (CONCEPT:EG-KG.query.gds-call-procedures) ──────────────────────────────────────────────

/// `gds.dijkstra(source [, target] [, config])` — single-source (or
/// source→target) weighted shortest path (CONCEPT:EG-KG.query.gds-call-procedures). String args are the
/// source and optional target node ids; an optional `{…}` config supplies
/// `relationshipWeightProperty`. Yields `nodeId` / `node`, `cost` — one row per
/// reachable node (or a single row for `target` when given, empty when
/// unreachable).
struct Dijkstra;
impl CypherProcedure for Dijkstra {
    fn name(&self) -> &'static str {
        "gds.dijkstra"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "cost"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let ids: Vec<String> = args
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let source = ids
            .first()
            .ok_or_else(|| "gds.dijkstra requires a source node id argument".to_string())?;
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let si = g
            .index_of(source)
            .ok_or_else(|| format!("source node `{source}` not in graph"))?;
        let res = dijkstra(&g, si);

        if let Some(target) = ids.get(1) {
            let ti = g
                .index_of(target)
                .ok_or_else(|| format!("target node `{target}` not in graph"))?;
            Ok(match res.distance_to(ti) {
                Some(c) => vec![scored_row(target.clone(), "cost", c)],
                None => Vec::new(),
            })
        } else {
            Ok(scored_rows(res.distances(), "cost"))
        }
    }
}

// ── node similarity (CONCEPT:EG-KG.query.gds-call-procedures) ────────────────────────────────────────────

/// `gds.nodeSimilarity(config)` — all-pairs Jaccard/cosine node similarity
/// (CONCEPT:EG-KG.query.gds-call-procedures). Config: `similarityMetric` (`JACCARD` [default] / `COSINE`),
/// `similarityCutoff` (0.0), `topK` (0 = unlimited), `relationshipWeightProperty`
/// (weights the cosine vectors). Yields `node1`, `node2`, `similarity` — unordered
/// pairs (`node1 < node2`) sorted by descending score then ascending ids, capped
/// at `topK` when set.
struct NodeSimilarity;
impl CypherProcedure for NodeSimilarity {
    fn name(&self) -> &'static str {
        "gds.nodeSimilarity"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node1", "node2", "similarity"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let metric = match cfg.string("similarityMetric").as_deref() {
            Some(m) if m.eq_ignore_ascii_case("COSINE") => Metric::Cosine,
            _ => Metric::Jaccard,
        };
        let cutoff = cfg.f64("similarityCutoff", 0.0);
        let top_k = cfg.usize("topK", 0);

        let mut pairs = all_pairs_similarity(&g, metric, Direction::Out, cutoff);
        if top_k > 0 && pairs.len() > top_k {
            pairs.truncate(top_k);
        }
        Ok(pairs
            .into_iter()
            .map(|p| {
                vec![
                    ("node1".to_string(), YieldValue::Node(p.a)),
                    ("node2".to_string(), YieldValue::Node(p.b)),
                    ("similarity".to_string(), YieldValue::Scalar(num(p.score))),
                ]
            })
            .collect())
    }
}

/// `gds.knn(config)` — per-node top-`k` nearest-neighbour similarity
/// (CONCEPT:EG-KG.query.gds-procedure-routing). Distinct from `gds.nodeSimilarity`'s global
/// all-pairs cutoff sweep: each node independently keeps its `topK` best-scoring
/// matches. Config: `similarityMetric` (`JACCARD` [default] / `COSINE`),
/// `similarityCutoff` (0.0), `topK` (10), `relationshipWeightProperty`. Yields
/// `node1`, `node2`, `similarity`. Routes to
/// `eg_compute::graph_algos::knn_similarity` (CONCEPT:EG-KG.compute.node-similarity) — exact
/// top-`k` via a full sweep, not Neo4j's approximate KNN-descent sampling.
struct Knn;
impl CypherProcedure for Knn {
    fn name(&self) -> &'static str {
        "gds.knn"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node1", "node2", "similarity"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let metric = match cfg.string("similarityMetric").as_deref() {
            Some(m) if m.eq_ignore_ascii_case("COSINE") => Metric::Cosine,
            _ => Metric::Jaccard,
        };
        let cutoff = cfg.f64("similarityCutoff", 0.0);
        let top_k = cfg.usize("topK", 10);
        let pairs = knn_similarity(&g, metric, Direction::Out, top_k, cutoff);
        Ok(pairs
            .into_iter()
            .map(|p| {
                vec![
                    ("node1".to_string(), YieldValue::Node(p.a)),
                    ("node2".to_string(), YieldValue::Node(p.b)),
                    ("similarity".to_string(), YieldValue::Scalar(num(p.score))),
                ]
            })
            .collect())
    }
}

// ── density clustering (CONCEPT:EG-KG.query.gds-procedure-routing) ───────────────────────────────

/// `gds.dbscan(config)` — density-based clustering over a node feature vector
/// (CONCEPT:EG-KG.query.gds-procedure-routing). Config: `nodeProperty` (REQUIRED — the node
/// property holding the feature vector: a JSON array of numbers, or a single
/// number treated as a 1-dim vector), `eps` (0.5), `minPts` (2). Nodes whose
/// `nodeProperty` is absent or non-numeric are skipped (not assigned a
/// `clusterId` row) — GDS's own `gds.dbscan` likewise only scores nodes that
/// carry the configured property. Yields `nodeId` / `node`, `clusterId` (`-1` =
/// DBSCAN noise). Routes to `eg_compute::mining::cluster::dbscan`
/// (CONCEPT:EG-KG.mining.dbscan-density) — no algorithm reimplementation; gated behind the
/// `cypher-mining` feature (routes to the `mining` eg-compute domain).
#[cfg(feature = "cypher-mining")]
struct Dbscan;
#[cfg(feature = "cypher-mining")]
impl CypherProcedure for Dbscan {
    fn name(&self) -> &'static str {
        "gds.dbscan"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "node", "clusterId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let prop = cfg
            .string("nodeProperty")
            .ok_or_else(|| "gds.dbscan requires a `nodeProperty` config key".to_string())?;
        let eps = cfg.f64("eps", 0.5);
        let min_pts = cfg.usize("minPts", 2);

        let mut ids: Vec<String> = view.node_map.keys().cloned().collect();
        ids.sort();
        let mut points: Vec<eg_compute::mining::cluster::Point> = Vec::new();
        let mut kept_ids: Vec<String> = Vec::new();
        for id in ids {
            if let Some(v) = node_feature_vector(view, &id, &prop) {
                points.push(v);
                kept_ids.push(id);
            }
        }
        if points.is_empty() {
            return Ok(Vec::new());
        }
        let labels = eg_compute::mining::cluster::dbscan(&points, eps, min_pts);
        Ok(kept_ids
            .into_iter()
            .zip(labels)
            .map(|(id, lbl)| {
                vec![
                    ("node".to_string(), YieldValue::Node(id.clone())),
                    ("nodeId".to_string(), YieldValue::Node(id)),
                    (
                        "clusterId".to_string(),
                        YieldValue::Scalar(Value::Number((lbl).into())),
                    ),
                ]
            })
            .collect())
    }
}

/// Read a node's `prop` as a feature vector: a JSON array of numbers (used
/// as-is) or a bare number (a 1-dim vector). Any other shape (absent, string,
/// bool, non-numeric array element) ⇒ `None`, meaning the node is skipped by
/// `gds.dbscan` (CONCEPT:EG-KG.query.gds-procedure-routing).
#[cfg(feature = "cypher-mining")]
fn node_feature_vector(view: &GraphView, id: &str, prop: &str) -> Option<Vec<f64>> {
    let blob = view.node_properties.get(id)?;
    let v: Value = rmp_serde::from_slice(blob).ok()?;
    let field = v.as_object()?.get(prop)?;
    match field {
        Value::Array(arr) => arr.iter().map(|x| x.as_f64()).collect(),
        Value::Number(_) => field.as_f64().map(|f| vec![f]),
        _ => None,
    }
}

// ── link prediction (CONCEPT:EG-KG.query.gds-procedure-routing) ──────────────────────────────────

/// `gds.linkPrediction(config)` — fit a KAN link-predictor over the graph's
/// existing edges (as positives, vs sampled non-edges) then score the top-`k`
/// missing links (CONCEPT:EG-KG.query.gds-procedure-routing). Unlike every other GDS
/// procedure here (project → run → YIELD), this is a fit-THEN-predict workflow,
/// both folded into one `CALL`. Config: `topK` (50), `epochs` (200, KAN training
/// rounds), `alpha` (0.5, 1-hop neighbour-aggregation self-retention). Yields
/// `node1`, `node2`, `probability`. Routes to
/// `eg_compute::graphlearn::link_predict` (CONCEPT:EG-KG.graphlearn.link-predictor) — no
/// model reimplementation; gated behind the `cypher-graphlearn` feature (implies
/// `datascience` in eg-compute for the shared Adam kernels).
#[cfg(feature = "cypher-graphlearn")]
struct LinkPrediction;
#[cfg(feature = "cypher-graphlearn")]
impl CypherProcedure for LinkPrediction {
    fn name(&self) -> &'static str {
        "gds.linkPrediction"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node1", "node2", "probability"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        use eg_compute::graphlearn::link_predict::{
            fit_link_predictor, predict_missing_links, FeatureCtx, KanLinkConfig,
        };
        use std::collections::HashSet;

        let cfg = Config::of(args);
        let g = project(view, None);
        if g.node_count() < 2 {
            return Ok(Vec::new());
        }
        let top_k = cfg.usize("topK", 50);
        let mut kcfg = KanLinkConfig::default();
        kcfg.epochs = cfg.usize("epochs", kcfg.epochs);
        kcfg.alpha = cfg.f64("alpha", kcfg.alpha);

        // Undirected positive edges, canonicalised + deduped by compact index.
        let mut existing: HashSet<(usize, usize)> = HashSet::new();
        for a in 0..g.node_count() {
            for &(b, _) in g.out_edges(a) {
                if a != b {
                    existing.insert(if a < b { (a, b) } else { (b, a) });
                }
            }
        }
        if existing.is_empty() {
            return Ok(Vec::new());
        }
        let positives: Vec<(usize, usize)> = existing.iter().copied().collect();
        let ctx = FeatureCtx::build(&g, kcfg.alpha);
        let model = fit_link_predictor(&ctx, &positives, &kcfg);
        let preds = predict_missing_links(&model, &ctx, &existing, top_k);
        Ok(preds
            .into_iter()
            .map(|(a, b, prob)| {
                vec![
                    ("node1".to_string(), YieldValue::Node(g.node_at(a).clone())),
                    ("node2".to_string(), YieldValue::Node(g.node_at(b).clone())),
                    ("probability".to_string(), YieldValue::Scalar(num(prob))),
                ]
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::exec_cypher;
    use eg_core::graph::GraphCore;
    use eg_types::protocol::QueryResult;

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// alice→bob→carol chain (unweighted KNOWS) + isolated d1; a→b weighted 5,
    /// b→c weighted 1 under the `weight` property for the weighted-path test.
    fn fixture() -> GraphView {
        let core = GraphCore::new();
        for id in ["alice", "bob", "carol", "d1"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"type": "Person"})));
        }
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(serde_json::json!({"relationship": "KNOWS", "weight": 5.0})),
        )
        .unwrap();
        core.add_edge(
            "bob".into(),
            "carol".into(),
            pbytes(serde_json::json!({"relationship": "KNOWS", "weight": 1.0})),
        )
        .unwrap();
        core.analysis_snapshot()
    }

    fn rows(qr: &QueryResult) -> Vec<Vec<Value>> {
        qr.rows
            .iter()
            .map(|b| rmp_serde::from_slice(b).unwrap())
            .collect()
    }

    // ── CONCEPT:EG-KG.query.gds-call-procedures — projection + config helpers ────────────────────────────

    #[test]
    fn eg298_project_includes_isolated_nodes_and_edges() {
        let v = fixture();
        let g = project(&v, None);
        // 4 nodes incl. isolated d1; 2 directed edges.
        assert_eq!(g.node_count(), 4);
        assert_eq!(g.edge_count(), 2);
        assert!(g.index_of(&"d1".to_string()).is_some());
    }

    #[test]
    fn eg298_config_parses_typed_values() {
        let args = vec![serde_json::json!({
            "dampingFactor": 0.5,
            "maxIterations": 7,
            "relationshipWeightProperty": "weight"
        })];
        let cfg = Config::of(&args);
        assert!((cfg.f64("dampingFactor", 0.85) - 0.5).abs() < 1e-12);
        assert_eq!(cfg.usize("maxIterations", 20), 7);
        assert_eq!(
            cfg.string("relationshipWeightProperty").as_deref(),
            Some("weight")
        );
        // Absent keys fall back to the supplied GDS default.
        assert!((cfg.f64("tolerance", 1e-7) - 1e-7).abs() < 1e-20);
    }

    // ── CONCEPT:EG-KG.query.gds-call-procedures — CALL gds.* over a known small graph ────────────────────

    #[test]
    fn eg298_call_gds_pagerank_ranks_and_projects_nodeid() {
        let v = fixture();
        // YIELD the GDS-canonical `nodeId` column (EG-298) + score.
        let qr = exec_cypher(
            &v,
            "CALL gds.pageRank({dampingFactor: 0.85, maxIterations: 30}) \
             YIELD nodeId, score RETURN nodeId, score",
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["nodeId", "score"]);
        let mut by_node: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            by_node.insert(r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap());
        }
        assert_eq!(by_node.len(), 4);
        // carol is the chain sink ⇒ outranks alice (chain source).
        assert!(by_node["carol"] > by_node["alice"], "{by_node:?}");
    }

    #[test]
    fn eg298_call_gds_pagerank_legacy_node_column_still_yields() {
        // Backward-compat: the pre-EG-298 `YIELD node, score` shape keeps working.
        let v = fixture();
        let qr = exec_cypher(&v, "CALL gds.pageRank() YIELD node, score RETURN node").unwrap();
        assert_eq!(rows(&qr).len(), 4);
    }

    #[test]
    fn eg298_call_gds_louvain_yields_community_ids() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.louvain({resolution: 1.0}) YIELD nodeId, communityId \
             RETURN nodeId, communityId",
        )
        .unwrap();
        let mut comm: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            comm.insert(r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap());
        }
        // The connected chain alice-bob-carol shares one community.
        assert_eq!(comm["alice"], comm["bob"]);
        assert_eq!(comm["bob"], comm["carol"]);
    }

    #[test]
    fn eg298_call_gds_wcc_and_scc_partition_differently() {
        let v = fixture();
        // WCC: alice/bob/carol weakly connected (one component), d1 its own.
        let wcc = exec_cypher(
            &v,
            "CALL gds.wcc() YIELD nodeId, componentId RETURN nodeId, componentId",
        )
        .unwrap();
        let mut w: HashMap<String, i64> = HashMap::new();
        for r in rows(&wcc) {
            w.insert(r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap());
        }
        assert_eq!(w["alice"], w["carol"]);
        assert_ne!(w["alice"], w["d1"]);

        // SCC: the chain has no back-edges ⇒ every node is its own SCC (4 distinct).
        let scc = exec_cypher(
            &v,
            "CALL gds.scc() YIELD nodeId, componentId RETURN nodeId, componentId",
        )
        .unwrap();
        let mut s: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for r in rows(&scc) {
            s.insert(r[1].as_i64().unwrap());
        }
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn eg298_call_gds_betweenness_middle_node_highest() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.betweenness({orientation: 'UNDIRECTED'}) YIELD nodeId, score \
             RETURN nodeId, score",
        )
        .unwrap();
        let mut bc: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            bc.insert(r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap());
        }
        // On the undirected path alice-bob-carol, bob sits on the only shortest
        // path between the endpoints ⇒ strictly highest betweenness.
        assert!(bc["bob"] > bc["alice"], "{bc:?}");
        assert!(bc["bob"] > bc["carol"], "{bc:?}");
    }

    #[test]
    fn eg298_call_gds_dijkstra_all_targets_costs() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.dijkstra('alice') YIELD nodeId, cost RETURN nodeId, cost",
        )
        .unwrap();
        let mut cost: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            cost.insert(r[0].as_str().unwrap().to_string(), r[1].as_f64().unwrap());
        }
        // Unweighted chain: alice=0, bob=1, carol=2; d1 unreachable (absent).
        assert_eq!(cost["alice"], 0.0);
        assert_eq!(cost["bob"], 1.0);
        assert_eq!(cost["carol"], 2.0);
        assert!(!cost.contains_key("d1"));
    }

    #[test]
    fn eg298_call_gds_dijkstra_weighted_source_target() {
        let v = fixture();
        // With relationshipWeightProperty, alice→bob costs 5, bob→carol costs 1.
        let qr = exec_cypher(
            &v,
            "CALL gds.dijkstra('alice', 'carol', {relationshipWeightProperty: 'weight'}) \
             YIELD nodeId, cost RETURN nodeId, cost",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0].as_str().unwrap(), "carol");
        assert!((r[0][1].as_f64().unwrap() - 6.0).abs() < 1e-9);
    }

    #[test]
    fn eg298_call_gds_node_similarity_jaccard() {
        // Two nodes sharing all out-neighbours score similarity 1.0.
        let core = GraphCore::new();
        for id in ["a", "b", "x", "y"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"type": "N"})));
        }
        for (s, t) in [("a", "x"), ("a", "y"), ("b", "x"), ("b", "y")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.nodeSimilarity({similarityMetric: 'JACCARD', similarityCutoff: 0.1}) \
             YIELD node1, node2, similarity RETURN node1, node2, similarity",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r[0][0].as_str().unwrap(), "a");
        assert_eq!(r[0][1].as_str().unwrap(), "b");
        assert!((r[0][2].as_f64().unwrap() - 1.0).abs() < 1e-9);
    }

    // ── CONCEPT:EG-KG.query.gds-procedure-routing — GDS breadth (Track I) ──────────────────────────────

    #[test]
    fn call_gds_label_propagation_splits_two_weighted_clusters() {
        // Two 4-cliques bridged by one NEAR-ZERO-weight edge (the realistic way a
        // caller signals "this link is weak" to LPA's raw-majority-vote
        // criterion — an equal-weight bridge is a documented general LPA
        // ambiguity, not specific to this routing; see
        // `eg_compute::graph_algos::label_propagation`'s module doc).
        let core = GraphCore::new();
        let clique1 = ["a", "b", "c", "d"];
        let clique2 = ["w", "x", "y", "z"];
        for id in clique1.iter().chain(clique2.iter()) {
            core.add_node((*id).into(), pbytes(serde_json::json!({"type": "N"})));
        }
        for c in [&clique1, &clique2] {
            for i in 0..c.len() {
                for j in (i + 1)..c.len() {
                    core.add_edge(
                        c[i].into(),
                        c[j].into(),
                        pbytes(serde_json::json!({"weight": 1.0})),
                    )
                    .unwrap();
                }
            }
        }
        core.add_edge(
            "d".into(),
            "w".into(),
            pbytes(serde_json::json!({"weight": 0.001})),
        )
        .unwrap();
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.labelPropagation({relationshipWeightProperty: 'weight'}) \
             YIELD nodeId, communityId RETURN nodeId, communityId",
        )
        .unwrap();
        let mut comm: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            comm.insert(r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap());
        }
        assert_eq!(comm.len(), 8);
        for id in clique1 {
            assert_eq!(comm[id], comm["a"], "clique1 member {id} split off");
        }
        for id in clique2 {
            assert_eq!(comm[id], comm["w"], "clique2 member {id} split off");
        }
        assert_ne!(comm["a"], comm["w"]);
    }

    #[test]
    fn call_gds_knn_keeps_top_k_per_node() {
        // Two nodes sharing all out-neighbours (a,b) plus a third (c) sharing
        // fewer — same fixture shape as the nodeSimilarity test.
        let core = GraphCore::new();
        for id in ["a", "b", "c", "x", "y"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"type": "N"})));
        }
        for (s, t) in [("a", "x"), ("a", "y"), ("b", "x"), ("b", "y"), ("c", "x")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.knn({topK: 1}) YIELD node1, node2, similarity \
             RETURN node1, node2, similarity",
        )
        .unwrap();
        let r = rows(&qr);
        assert!(r.iter().any(|row| row[0].as_str().unwrap() == "a"
            && row[1].as_str().unwrap() == "b"
            && (row[2].as_f64().unwrap() - 1.0).abs() < 1e-9));
        // b's own top-1 is a (score 1.0, beats c's 0.5) ⇒ (b, c) never appears.
        assert!(!r.iter().any(|row| {
            let ids = [row[0].as_str().unwrap(), row[1].as_str().unwrap()];
            ids.contains(&"b") && ids.contains(&"c")
        }));
    }

    #[cfg(feature = "cypher-mining")]
    #[test]
    fn call_gds_dbscan_clusters_by_node_property() {
        let core = GraphCore::new();
        // Two dense 2D clusters far apart; dbscan(eps=1.5, minPts=2) should
        // separate them (and no isolated singleton).
        let pts: [(&str, f64, f64); 6] = [
            ("a1", 0.0, 0.0),
            ("a2", 0.5, 0.5),
            ("a3", 0.2, 0.8),
            ("b1", 20.0, 20.0),
            ("b2", 20.5, 20.5),
            ("b3", 20.2, 20.8),
        ];
        for (id, x, y) in pts {
            core.add_node(
                id.into(),
                pbytes(serde_json::json!({"type": "Point", "loc": [x, y]})),
            );
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.dbscan({nodeProperty: 'loc', eps: 2.0, minPts: 2}) \
             YIELD nodeId, clusterId RETURN nodeId, clusterId",
        )
        .unwrap();
        let mut cluster: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            cluster.insert(r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap());
        }
        assert_eq!(cluster.len(), 6);
        assert_eq!(cluster["a1"], cluster["a2"]);
        assert_eq!(cluster["a2"], cluster["a3"]);
        assert_eq!(cluster["b1"], cluster["b2"]);
        assert_ne!(cluster["a1"], cluster["b1"]);
        assert!(cluster["a1"] >= 0, "core points must not be noise");
    }

    #[cfg(feature = "cypher-mining")]
    #[test]
    fn call_gds_dbscan_requires_node_property_config() {
        let v = fixture();
        let err = exec_cypher(
            &v,
            "CALL gds.dbscan() YIELD nodeId, clusterId RETURN nodeId",
        )
        .unwrap_err();
        assert!(err.contains("nodeProperty"), "{err}");
    }

    #[cfg(feature = "cypher-graphlearn")]
    #[test]
    fn call_gds_link_prediction_scores_missing_links_over_a_planted_structure() {
        // Two triangles {a,b,c} and {x,y,z} bridged by one edge (c-x): the
        // predictor should learn that "shares neighbours" ⇒ likely-linked, and
        // score the missing intra-triangle-adjacent pairs above unrelated ones.
        let core = GraphCore::new();
        for id in ["a", "b", "c", "x", "y", "z"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"type": "N"})));
        }
        for (s, t) in [("a", "b"), ("b", "c"), ("x", "y"), ("y", "z"), ("c", "x")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
            core.add_edge(t.into(), s.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.linkPrediction({topK: 5, epochs: 50}) \
             YIELD node1, node2, probability RETURN node1, node2, probability",
        )
        .unwrap();
        let r = rows(&qr);
        // There are 15 possible pairs over 6 nodes minus 5 existing edges = 10
        // missing candidates; topK=5 caps the result, never exceeds it.
        assert!(!r.is_empty());
        assert!(r.len() <= 5);
        for row in &r {
            let p = row[2].as_f64().unwrap();
            assert!((0.0..=1.0).contains(&p), "probability out of range: {p}");
        }
    }
}
