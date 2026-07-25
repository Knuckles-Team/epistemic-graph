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
//! The procedure schema is current-only: node-valued results expose `nodeId` and
//! the algorithm-specific value column. Alternate node-column aliases are rejected
//! by normal `YIELD` validation.

use std::collections::HashMap;

use eg_core::graph::GraphView;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde_json::Value;

use eg_compute::graph_algos::{
    a_star, all_pairs_similarity, article_rank, betweenness_centrality, closeness_centrality,
    degree_centrality, dijkstra, eigenvector_centrality, harmonic_centrality, haversine_km,
    k1_coloring, k_core, knn_similarity, knn_similarity_approx, label_propagation, leiden,
    local_clustering_coefficient, louvain, pagerank, random_walk, steiner_tree,
    strongly_connected_components, triangle_count, weakly_connected_components,
    yen_k_shortest_paths, AdjacencyGraph, ArticleRankConfig, ClosenessConfig, DegreeKind,
    Direction, EigenvectorConfig, LabelPropagationConfig, LeidenConfig, LouvainConfig, Metric,
    PageRankConfig, RandomWalkConfig,
};

use super::proc::{CypherProcedure, ProcRow, YieldValue};

/// Every GDS procedure, ready to fold into the procedure registry
/// (CONCEPT:EG-KG.query.gds-call-procedures / CONCEPT:EG-KG.query.gds-procedure-routing). Consumed
/// by `proc::build_registry`. The always-on base (EG-298 + `labelPropagation`/
/// `knn` + the W4.1 GDS-parity expansion — Leiden/triangle-count/
/// local-clustering-coefficient/k-core/k1-coloring community family,
/// eigenvector/ArticleRank/closeness/harmonic centrality, and
/// A*/Yen's-k/Steiner-tree/random-walk paths) all route to always-on
/// `graph_algos` kernels; `gds.dbscan` and `gds.linkPrediction` are gated
/// behind the `cypher-mining`/`cypher-graphlearn` features (they route to
/// heavier eg-compute domains — see `Cargo.toml`).
pub fn gds_procedures() -> Vec<Box<dyn CypherProcedure>> {
    #[allow(unused_mut)]
    let mut v: Vec<Box<dyn CypherProcedure>> = vec![
        Box::new(PageRank),
        Box::new(Betweenness),
        Box::new(Degree),
        Box::new(Eigenvector),
        Box::new(ArticleRank),
        Box::new(Closeness),
        Box::new(Harmonic),
        Box::new(Louvain),
        Box::new(Leiden),
        Box::new(Wcc),
        Box::new(Scc),
        Box::new(TriangleCount),
        Box::new(LocalClusteringCoefficient),
        Box::new(KCore),
        Box::new(K1Coloring),
        Box::new(Dijkstra),
        Box::new(AStar),
        Box::new(Yens),
        Box::new(SteinerTree),
        Box::new(RandomWalk),
        Box::new(NodeSimilarity),
        Box::new(LabelPropagation),
        Box::new(Knn),
    ];
    #[cfg(feature = "cypher-mining")]
    v.push(Box::new(Dbscan));
    #[cfg(feature = "cypher-graphlearn")]
    v.push(Box::new(LinkPrediction));
    // ── W4.2 structural embeddings (CONCEPT:EG-KG.graphlearn.structural-embeddings) ──
    // Appended at the END of the registration list to keep the merge-conflict surface
    // minimal with concurrent gds.rs edits. Both route to the `eg_compute::graphlearn::
    // embeddings` FastRP / Node2Vec kernels, so they share `cypher-graphlearn`.
    #[cfg(feature = "cypher-graphlearn")]
    v.push(Box::new(FastRp));
    #[cfg(feature = "cypher-graphlearn")]
    v.push(Box::new(Node2Vec));
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

/// A node's numeric property `prop`, if present and numeric
/// (CONCEPT:EG-KG.query.gds-call-procedures) — used by `gds.shortestPath.astar`'s
/// lat/lon heuristic. The ungated counterpart of the `cypher-mining`-gated
/// `node_feature_vector` helper below (same shape, one scalar instead of a
/// vector).
fn node_number_prop(view: &GraphView, id: &str, prop: &str) -> Option<f64> {
    let blob = view.node_properties.get(id)?;
    let v = eg_types::msgpack::decode_property_value(blob).ok()?;
    v.as_object()?.get(prop)?.as_f64()
}

/// The numeric `prop` on the first `(s, t)` edge property record carrying it, if
/// any (CONCEPT:EG-KG.query.gds-call-procedures).
fn edge_weight(view: &GraphView, s: &str, t: &str, prop: &str) -> Option<f64> {
    let blobs = view.edge_properties.get(&(s.to_string(), t.to_string()))?;
    for blob in blobs {
        if let Ok(Value::Object(m)) = eg_types::msgpack::decode_property_value(blob) {
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

/// One `(nodeId, <score-col>)` result row (CONCEPT:EG-KG.query.gds-call-procedures).
fn scored_row(id: String, col: &str, score: f64) -> ProcRow {
    vec![
        ("nodeId".to_string(), YieldValue::Node(id)),
        (col.to_string(), YieldValue::Scalar(num(score))),
    ]
}

/// `(nodeId, <score-col>)` rows from a `Vec<(id, f64)>` kernel result.
fn scored_rows(scored: Vec<(String, f64)>, col: &str) -> Vec<ProcRow> {
    scored
        .into_iter()
        .map(|(id, s)| scored_row(id, col, s))
        .collect()
}

/// One `(nodeId, <col>)` result row for an INTEGER-valued per-node metric
/// (triangle counts, core numbers, coloring ids — CONCEPT:EG-KG.query.gds-call-procedures) —
/// the `u64` sibling of [`scored_row`].
fn int_row(id: String, col: &str, v: u64) -> ProcRow {
    vec![
        ("nodeId".to_string(), YieldValue::Node(id)),
        (col.to_string(), YieldValue::Scalar(Value::Number(v.into()))),
    ]
}

/// `(nodeId, <col>)` rows from a `Vec<(id, u64)>` kernel result.
fn int_rows(scored: Vec<(String, u64)>, col: &str) -> Vec<ProcRow> {
    scored
        .into_iter()
        .map(|(id, v)| int_row(id, col, v))
        .collect()
}

/// `(nodeId, <group-col>)` rows from a partition (`Vec<Vec<id>>`): every node
/// tagged with its 0-based group index (CONCEPT:EG-KG.query.gds-call-procedures).
fn partition_rows(groups: Vec<Vec<String>>, col: &str) -> Vec<ProcRow> {
    let mut out = Vec::new();
    for (gid, members) in groups.into_iter().enumerate() {
        for id in members {
            out.push(vec![
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
        &["nodeId", "score"]
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
        &["nodeId", "score"]
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
        &["nodeId", "score"]
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

/// `gds.eigenvector(config)` — eigenvector centrality via power iteration
/// (CONCEPT:EG-KG.query.gds-call-procedures). Config: `maxIterations` (20), `tolerance` (1e-7),
/// `relationshipWeightProperty`. Yields `nodeId` / `node`, `score`. Routes to
/// `eg_compute::graph_algos::eigenvector_centrality` (CONCEPT:EG-KG.compute.eigenvector-centrality) —
/// see that kernel's doc for the two honest degenerate cases (pure DAG ⇒ zero;
/// periodic structure ⇒ `converged` may be false).
struct Eigenvector;
impl CypherProcedure for Eigenvector {
    fn name(&self) -> &'static str {
        "gds.eigenvector"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let ec = EigenvectorConfig {
            tolerance: cfg.f64("tolerance", 1e-7),
            max_iterations: cfg.usize("maxIterations", 20),
        };
        Ok(scored_rows(eigenvector_centrality(&g, &ec).scores, "score"))
    }
}

/// `gds.articleRank(config)` — ArticleRank, a PageRank variant that discounts
/// low-out-degree sources (CONCEPT:EG-KG.query.gds-call-procedures). Config: `dampingFactor`
/// (0.85), `maxIterations` (20), `tolerance` (1e-7), `relationshipWeightProperty`.
/// Yields `nodeId` / `node`, `score`. Routes to
/// `eg_compute::graph_algos::article_rank` (CONCEPT:EG-KG.compute.article-rank) — see that
/// kernel's doc for why ArticleRank scores do not sum to 1 (documented, expected).
struct ArticleRank;
impl CypherProcedure for ArticleRank {
    fn name(&self) -> &'static str {
        "gds.articleRank"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let ac = ArticleRankConfig {
            damping: cfg.f64("dampingFactor", 0.85),
            tolerance: cfg.f64("tolerance", 1e-7),
            max_iterations: cfg.usize("maxIterations", 20),
        };
        Ok(scored_rows(article_rank(&g, &ac).scores, "score"))
    }
}

/// `gds.closeness(config)` — closeness centrality (Freeman), optionally the
/// Wasserman–Faust "improved" correction (CONCEPT:EG-KG.query.gds-call-procedures). Config:
/// `useWassermanFaust` (false, mirrors legacy GDS's own flag name),
/// `relationshipWeightProperty`. Yields `nodeId` / `node`, `score`. Routes to
/// `eg_compute::graph_algos::closeness_centrality` (CONCEPT:EG-KG.compute.closeness-centrality).
struct Closeness;
impl CypherProcedure for Closeness {
    fn name(&self) -> &'static str {
        "gds.closeness"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let cc = ClosenessConfig {
            improved: cfg
                .get("useWassermanFaust")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        Ok(scored_rows(closeness_centrality(&g, &cc), "score"))
    }
}

/// `gds.harmonic(config)` — harmonic centrality (Marchiori & Latora)
/// (CONCEPT:EG-KG.query.gds-call-procedures). Config: `relationshipWeightProperty`. Yields
/// `nodeId` / `node`, `score`. Routes to
/// `eg_compute::graph_algos::harmonic_centrality` (CONCEPT:EG-KG.compute.harmonic-centrality).
struct Harmonic;
impl CypherProcedure for Harmonic {
    fn name(&self) -> &'static str {
        "gds.harmonic"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        Ok(scored_rows(harmonic_centrality(&g), "score"))
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
        &["nodeId", "communityId"]
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

/// `gds.leiden(config)` — Leiden community detection with a
/// connectivity-guaranteeing refinement phase (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `resolution` (alias `gamma`, 1.0), `maxLevels` (50),
/// `maxIterations`/`maxSweeps` (100), `relationshipWeightProperty`. Yields
/// `nodeId` / `node`, `communityId`. Routes to `eg_compute::graph_algos::leiden`
/// (CONCEPT:EG-KG.compute.leiden-community-detection) — see that module's doc for the exact
/// guarantee (every returned community's induced subgraph is connected, the
/// defect Traag, Waltman & van Eck 2019 prove plain Louvain does not avoid).
struct Leiden;
impl CypherProcedure for Leiden {
    fn name(&self) -> &'static str {
        "gds.leiden"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "communityId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let lc = LeidenConfig {
            resolution: cfg.f64("resolution", cfg.f64("gamma", 1.0)),
            seed: None,
            max_sweeps: cfg.usize("maxIterations", cfg.usize("maxSweeps", 100)),
            max_levels: cfg.usize("maxLevels", 50),
        };
        Ok(partition_rows(leiden(&g, &lc).communities, "communityId"))
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
        &["nodeId", "communityId"]
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
        &["nodeId", "componentId"]
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
        &["nodeId", "componentId"]
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

// ── structural / community metrics (W4.1, CONCEPT:EG-KG.query.gds-call-procedures) ────────────────

/// `gds.triangleCount(config)` — per-node triangle count over the undirected
/// symmetrisation (CONCEPT:EG-KG.query.gds-call-procedures). Config:
/// `relationshipWeightProperty` (topology only — triangle count ignores
/// weight/direction). Yields `nodeId` / `node`, `triangleCount`. Routes to
/// `eg_compute::graph_algos::triangle_count` (CONCEPT:EG-KG.compute.triangle-counting).
struct TriangleCount;
impl CypherProcedure for TriangleCount {
    fn name(&self) -> &'static str {
        "gds.triangleCount"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "triangleCount"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let g = project(view, None);
        Ok(int_rows(triangle_count(&g), "triangleCount"))
    }
}

/// `gds.localClusteringCoefficient(config)` — per-node local clustering
/// coefficient (CONCEPT:EG-KG.query.gds-call-procedures). Yields `nodeId` / `node`,
/// `localClusteringCoefficient`. Routes to
/// `eg_compute::graph_algos::local_clustering_coefficient` (CONCEPT:EG-KG.compute.triangle-counting).
struct LocalClusteringCoefficient;
impl CypherProcedure for LocalClusteringCoefficient {
    fn name(&self) -> &'static str {
        "gds.localClusteringCoefficient"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "localClusteringCoefficient"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let g = project(view, None);
        Ok(scored_rows(
            local_clustering_coefficient(&g),
            "localClusteringCoefficient",
        ))
    }
}

/// `gds.kcore(config)` — k-core decomposition (degeneracy / coreness)
/// (CONCEPT:EG-KG.query.gds-call-procedures). Yields `nodeId` / `node`, `coreValue`. Routes to
/// `eg_compute::graph_algos::k_core` (CONCEPT:EG-KG.compute.k-core-decomposition).
struct KCore;
impl CypherProcedure for KCore {
    fn name(&self) -> &'static str {
        "gds.kcore"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "coreValue"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let g = project(view, None);
        Ok(int_rows(k_core(&g), "coreValue"))
    }
}

/// `gds.k1coloring(config)` — greedy proper graph coloring (Welsh–Powell
/// largest-degree-first) (CONCEPT:EG-KG.query.gds-call-procedures). Yields `nodeId` / `node`,
/// `color`. Routes to `eg_compute::graph_algos::k1_coloring` (CONCEPT:EG-KG.compute.k1-coloring).
struct K1Coloring;
impl CypherProcedure for K1Coloring {
    fn name(&self) -> &'static str {
        "gds.k1coloring"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "color"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let g = project(view, None);
        Ok(int_rows(k1_coloring(&g), "color"))
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
        &["nodeId", "cost"]
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

/// `gds.shortestPath.astar(source, target [, config])` — A* shortest path
/// guided by a haversine heuristic over a node lat/lon property pair
/// (CONCEPT:EG-KG.query.gds-call-procedures). String args are source and target node ids.
/// Config: `latitudeProperty` (`"latitude"`), `longitudeProperty`
/// (`"longitude"`), `relationshipWeightProperty`. A node missing either
/// coordinate property falls back to heuristic `0.0` (never an error —
/// degrades toward plain Dijkstra for that node, never wrong, just less
/// informed). Yields a SINGLE row (the same shape `gds.dijkstra` uses when a
/// target is given): `nodeId` / `node` = target, `cost` = total path cost;
/// empty when unreachable. Routes to `eg_compute::graph_algos::a_star`
/// (CONCEPT:EG-KG.compute.astar-search).
struct AStar;
impl CypherProcedure for AStar {
    fn name(&self) -> &'static str {
        "gds.shortestPath.astar"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "cost"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let ids: Vec<String> = args
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let source = ids.first().ok_or_else(|| {
            "gds.shortestPath.astar requires a source node id argument".to_string()
        })?;
        let target = ids.get(1).ok_or_else(|| {
            "gds.shortestPath.astar requires a target node id argument".to_string()
        })?;
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let si = g
            .index_of(source)
            .ok_or_else(|| format!("source node `{source}` not in graph"))?;
        let ti = g
            .index_of(target)
            .ok_or_else(|| format!("target node `{target}` not in graph"))?;

        let lat_prop = cfg
            .string("latitudeProperty")
            .unwrap_or_else(|| "latitude".to_string());
        let lon_prop = cfg
            .string("longitudeProperty")
            .unwrap_or_else(|| "longitude".to_string());
        let target_coord = node_number_prop(view, target, &lat_prop)
            .zip(node_number_prop(view, target, &lon_prop));
        let heuristic = |idx: usize| -> f64 {
            let Some((t_lat, t_lon)) = target_coord else {
                return 0.0;
            };
            let id = g.node_at(idx);
            match node_number_prop(view, id, &lat_prop).zip(node_number_prop(view, id, &lon_prop)) {
                Some((lat, lon)) => haversine_km(lat, lon, t_lat, t_lon),
                None => 0.0,
            }
        };

        Ok(match a_star(&g, si, ti, heuristic) {
            Some((_path, total_cost)) => vec![scored_row(target.clone(), "cost", total_cost)],
            None => Vec::new(),
        })
    }
}

/// `gds.shortestPath.yens(source, target [, config])` — the `k` shortest
/// LOOPLESS paths (CONCEPT:EG-KG.query.gds-call-procedures). Config: `k` (3),
/// `relationshipWeightProperty`. Yields `index` (0-based rank, ascending
/// cost), `nodeIds` (the path as a JSON array of node id strings — a scalar
/// LIST column, like GDS's own `nodeIds`; not individually bindable node
/// references), `cost`. Routes to
/// `eg_compute::graph_algos::yen_k_shortest_paths` (CONCEPT:EG-KG.compute.yens-k-shortest-paths).
struct Yens;
impl CypherProcedure for Yens {
    fn name(&self) -> &'static str {
        "gds.shortestPath.yens"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["index", "nodeIds", "cost"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let ids: Vec<String> = args
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let source = ids.first().ok_or_else(|| {
            "gds.shortestPath.yens requires a source node id argument".to_string()
        })?;
        let target = ids.get(1).ok_or_else(|| {
            "gds.shortestPath.yens requires a target node id argument".to_string()
        })?;
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let si = g
            .index_of(source)
            .ok_or_else(|| format!("source node `{source}` not in graph"))?;
        let ti = g
            .index_of(target)
            .ok_or_else(|| format!("target node `{target}` not in graph"))?;
        let k = cfg.usize("k", 3);

        Ok(yen_k_shortest_paths(&g, si, ti, k)
            .into_iter()
            .enumerate()
            .map(|(idx, p)| {
                vec![
                    (
                        "index".to_string(),
                        YieldValue::Scalar(Value::Number((idx as u64).into())),
                    ),
                    (
                        "nodeIds".to_string(),
                        YieldValue::Scalar(Value::Array(
                            p.nodes.into_iter().map(Value::String).collect(),
                        )),
                    ),
                    ("cost".to_string(), YieldValue::Scalar(num(p.cost))),
                ]
            })
            .collect())
    }
}

/// `gds.steinerTree(root, terminal1, terminal2, … [, config])` — Steiner tree
/// connecting `root` and every reachable terminal (CONCEPT:EG-KG.query.gds-call-procedures).
/// Config: `relationshipWeightProperty`. Yields `nodeId` / `node`, `parentId`
/// (`null` for `root`, a scalar id reference like GDS's own — not an
/// anchorable node), `weight` (the parent-edge weight, `null` for `root`). An
/// unreachable terminal is silently omitted from the tree, never an error.
/// Routes to `eg_compute::graph_algos::steiner_tree` (CONCEPT:EG-KG.compute.steiner-tree) —
/// see that kernel's doc for the 2-approximation ratio and its one documented gap.
struct SteinerTree;
impl CypherProcedure for SteinerTree {
    fn name(&self) -> &'static str {
        "gds.steinerTree"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "parentId", "weight"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let ids: Vec<String> = args
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let root = ids
            .first()
            .ok_or_else(|| "gds.steinerTree requires a root node id argument".to_string())?;
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let ri = g
            .index_of(root)
            .ok_or_else(|| format!("root node `{root}` not in graph"))?;
        let terminals: Vec<usize> = ids[1..].iter().filter_map(|t| g.index_of(t)).collect();

        Ok(steiner_tree(&g, ri, &terminals)
            .nodes
            .into_iter()
            .map(|(id, parent, weight)| {
                vec![
                    ("nodeId".to_string(), YieldValue::Node(id)),
                    (
                        "parentId".to_string(),
                        YieldValue::Scalar(parent.map(Value::String).unwrap_or(Value::Null)),
                    ),
                    (
                        "weight".to_string(),
                        YieldValue::Scalar(weight.map(num).unwrap_or(Value::Null)),
                    ),
                ]
            })
            .collect())
    }
}

/// `gds.randomWalk(start [, config])` — weighted random walk with restart
/// (CONCEPT:EG-KG.query.gds-call-procedures). Config: `steps` (10), `restartProbability`
/// (0.0), `seed` (0), `relationshipWeightProperty`. Yields `index` (0-based
/// step position), `nodeId` / `node` (the node visited at that step). Routes
/// to `eg_compute::graph_algos::random_walk` (CONCEPT:EG-KG.compute.random-walk).
struct RandomWalk;
impl CypherProcedure for RandomWalk {
    fn name(&self) -> &'static str {
        "gds.randomWalk"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["index", "nodeId"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let cfg = Config::of(args);
        let ids: Vec<String> = args
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let start = ids
            .first()
            .ok_or_else(|| "gds.randomWalk requires a start node id argument".to_string())?;
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let si = g
            .index_of(start)
            .ok_or_else(|| format!("start node `{start}` not in graph"))?;
        let rc = RandomWalkConfig {
            steps: cfg.usize("steps", 10),
            restart_probability: cfg.f64("restartProbability", 0.0),
            seed: cfg.get("seed").and_then(Value::as_u64).unwrap_or(0),
        };

        Ok(random_walk(&g, si, &rc)
            .into_iter()
            .enumerate()
            .map(|(idx, id)| {
                vec![
                    (
                        "index".to_string(),
                        YieldValue::Scalar(Value::Number((idx as u64).into())),
                    ),
                    ("nodeId".to_string(), YieldValue::Node(id)),
                ]
            })
            .collect())
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
/// `similarityCutoff` (0.0), `topK` (10), `relationshipWeightProperty`, and the
/// mode selector `mode` (`exact` [default] / `approximate`). `exact` routes to
/// `eg_compute::graph_algos::knn_similarity` (a full `O(V²·d̄)` sweep); `approximate`
/// routes to `eg_compute::graph_algos::knn_similarity_approx` — Neo4j-style seeded
/// NN-descent sampling that trades exactness for a sub-quadratic cost at large V,
/// with the extra knobs `sampleRate` (0.5), `maxIterations` (100), `deltaThreshold`
/// (0.001), and `randomSeed` (42). Both modes yield identical
/// `node1`/`node2`/`similarity` shape + ordering. CONCEPT:EG-KG.compute.node-similarity
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
        let approximate = cfg
            .string("mode")
            .map(|m| {
                let m = m.trim();
                m.eq_ignore_ascii_case("approximate")
                    || m.eq_ignore_ascii_case("approx")
                    || m.eq_ignore_ascii_case("sampled")
            })
            .unwrap_or(false);
        let pairs = if approximate {
            let sample_rate = cfg.f64("sampleRate", 0.5);
            let max_iters = cfg.usize("maxIterations", 100);
            let delta = cfg.f64("deltaThreshold", 0.001);
            let seed = cfg.usize("randomSeed", 42) as u64;
            knn_similarity_approx(
                &g,
                metric,
                Direction::Out,
                top_k,
                cutoff,
                sample_rate,
                max_iters,
                delta,
                seed,
            )
        } else {
            knn_similarity(&g, metric, Direction::Out, top_k, cutoff)
        };
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
        &["nodeId", "clusterId"]
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
    let v = eg_types::msgpack::decode_property_value(blob).ok()?;
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

// ── structural embeddings (CONCEPT:EG-KG.graphlearn.structural-embeddings, W4.2) ──────────────────
//
// Two stream procedures over `eg_compute::graphlearn::embeddings` that turn graph
// topology into per-node vectors (Neo4j `gds.fastRP.stream` / `gds.node2Vec.stream`
// shape). Both project the current view, run the deterministic kernel, and YIELD
// `(nodeId, embedding)` where `embedding` is a numeric list. Gated behind
// `cypher-graphlearn` (routes to the heavier eg-compute graphlearn domain). Appended at
// the END of this file to minimize merge-conflict surface with concurrent edits.

/// `(nodeId, embedding)` rows from index-ordered embedding vectors + the projected graph.
#[cfg(feature = "cypher-graphlearn")]
fn embedding_rows(g: &AdjacencyGraph<String>, rows: Vec<Vec<f32>>) -> Vec<ProcRow> {
    g.nodes()
        .iter()
        .cloned()
        .zip(rows)
        .map(|(id, row)| {
            let arr = Value::Array(row.into_iter().map(|x| num(x as f64)).collect());
            vec![
                ("nodeId".to_string(), YieldValue::Node(id)),
                ("embedding".to_string(), YieldValue::Scalar(arr)),
            ]
        })
        .collect()
}

/// `gds.fastRP(config)` — FastRP structural embeddings
/// (CONCEPT:EG-KG.graphlearn.fastrp). Config: `embeddingDimension` (128), `iterations`
/// (3), `normalizationStrength` (0.5), `randomSeed` (42), `relationshipWeightProperty`.
/// Yields `nodeId`, `embedding` (a numeric list). Routes to
/// `eg_compute::graphlearn::embeddings::fastrp` — deterministic given the seed.
#[cfg(feature = "cypher-graphlearn")]
struct FastRp;
#[cfg(feature = "cypher-graphlearn")]
impl CypherProcedure for FastRp {
    fn name(&self) -> &'static str {
        "gds.fastRP"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "embedding"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        use eg_compute::graphlearn::embeddings::{fastrp, FastRpConfig};
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let def = FastRpConfig::default();
        let fcfg = FastRpConfig {
            dim: cfg.usize("embeddingDimension", def.dim),
            iterations: cfg.usize("iterations", def.iterations),
            normalization_strength: cfg.f64("normalizationStrength", def.normalization_strength),
            seed: cfg.usize("randomSeed", def.seed as usize) as u64,
            ..def
        };
        Ok(embedding_rows(&g, fastrp(&g, &fcfg)))
    }
}

/// `gds.node2vec(config)` — Node2Vec biased-walk + SGNS structural embeddings
/// (CONCEPT:EG-KG.graphlearn.node2vec). Config: `embeddingDimension` (128), `walkLength`
/// (40), `walksPerNode` (10), `windowSize` (5), `returnFactor` p (1.0), `inOutFactor` q
/// (1.0), `negativeSamplingRate` (5), `iterations` (training epochs, 10), `randomSeed`
/// (42), `relationshipWeightProperty`. Yields `nodeId`, `embedding`. Routes to
/// `eg_compute::graphlearn::embeddings::node2vec` — deterministic given the seed.
#[cfg(feature = "cypher-graphlearn")]
struct Node2Vec;
#[cfg(feature = "cypher-graphlearn")]
impl CypherProcedure for Node2Vec {
    fn name(&self) -> &'static str {
        "gds.node2vec"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeId", "embedding"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        use eg_compute::graphlearn::embeddings::{node2vec, Node2VecConfig};
        let cfg = Config::of(args);
        let g = project(view, cfg.string("relationshipWeightProperty").as_deref());
        let def = Node2VecConfig::default();
        let ncfg = Node2VecConfig {
            dim: cfg.usize("embeddingDimension", def.dim),
            walk_length: cfg.usize("walkLength", def.walk_length),
            walks_per_node: cfg.usize("walksPerNode", def.walks_per_node),
            window: cfg.usize("windowSize", def.window),
            p: cfg.f64("returnFactor", def.p),
            q: cfg.f64("inOutFactor", def.q),
            negatives: cfg.usize("negativeSamplingRate", def.negatives),
            epochs: cfg.usize("iterations", def.epochs),
            seed: cfg.usize("randomSeed", def.seed as usize) as u64,
            ..def
        };
        Ok(embedding_rows(&g, node2vec(&g, &ncfg)))
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
            core.add_node(
                id.into(),
                pbytes(serde_json::json!({"node_type": "Person"})),
            );
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

    fn node_id(value: &Value) -> &str {
        value
            .as_str()
            .or_else(|| value.get("id").and_then(Value::as_str))
            .expect("node-valued GDS projection")
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
            by_node.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        assert_eq!(by_node.len(), 4);
        // carol is the chain sink ⇒ outranks alice (chain source).
        assert!(by_node["carol"] > by_node["alice"], "{by_node:?}");
    }

    #[test]
    fn call_gds_eigenvector_symmetric_triangle_is_uniform() {
        // Bidirectional triangle (every pair connected both ways, equal
        // weight): eigenvector centrality's fixed point is uniform.
        let core = GraphCore::new();
        for id in ["a", "b", "c"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "a"),
            ("b", "c"),
            ("c", "b"),
            ("a", "c"),
            ("c", "a"),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.eigenvector() YIELD nodeId, score RETURN nodeId, score",
        )
        .unwrap();
        let mut scores: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            scores.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        let vals: Vec<f64> = scores.values().copied().collect();
        for w in [(vals[0], vals[1]), (vals[1], vals[2])] {
            assert!((w.0 - w.1).abs() < 1e-6, "{scores:?}");
        }
        assert!(vals[0] > 0.0);
    }

    #[test]
    fn call_gds_article_rank_ranks_the_chains_sink_highest() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.articleRank({dampingFactor: 0.85}) YIELD nodeId, score \
             RETURN nodeId, score",
        )
        .unwrap();
        let mut by_node: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            by_node.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        assert_eq!(by_node.len(), 4);
        // carol is the chain sink (bob->carol, plus alice->bob->carol upstream)
        // ⇒ still outranks alice, same qualitative shape as gds.pageRank.
        assert!(by_node["carol"] > by_node["alice"], "{by_node:?}");
    }

    #[test]
    fn call_gds_closeness_and_harmonic_cross_checked_on_a_path() {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "a"),
            ("b", "c"),
            ("c", "b"),
            ("c", "d"),
            ("d", "c"),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();

        let closeness = exec_cypher(
            &v,
            "CALL gds.closeness() YIELD nodeId, score RETURN nodeId, score",
        )
        .unwrap();
        let mut c: HashMap<String, f64> = HashMap::new();
        for r in rows(&closeness) {
            c.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        // From b: reaches a(1),c(1),d(2) ⇒ classic closeness = 3/4.
        assert!((c["b"] - 0.75).abs() < 1e-9, "{c:?}");

        let improved = exec_cypher(
            &v,
            "CALL gds.closeness({useWassermanFaust: true}) YIELD nodeId, score \
             RETURN nodeId, score",
        )
        .unwrap();
        let mut ci: HashMap<String, f64> = HashMap::new();
        for r in rows(&improved) {
            ci.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        // Wasserman-Faust scales by reachable/(N-1) = 3/3 = 1 for b (b reaches
        // everyone) ⇒ unchanged; cross-checks that the flag actually threads
        // through the Cypher config parser to the kernel.
        assert!((ci["b"] - 0.75).abs() < 1e-9, "{ci:?}");

        let harmonic = exec_cypher(
            &v,
            "CALL gds.harmonic() YIELD nodeId, score RETURN nodeId, score",
        )
        .unwrap();
        let mut h: HashMap<String, f64> = HashMap::new();
        for r in rows(&harmonic) {
            h.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        // From b: (1/1 + 1/1 + 1/2) / 3 = 2.5/3.
        assert!((h["b"] - 2.5 / 3.0).abs() < 1e-9, "{h:?}");
    }

    #[test]
    fn gds_rejects_noncanonical_node_column() {
        let v = fixture();
        let error =
            exec_cypher(&v, "CALL gds.pageRank() YIELD node, score RETURN node").unwrap_err();
        assert!(error.contains("node"), "{error}");
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
            comm.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
        }
        // The connected chain alice-bob-carol shares one community.
        assert_eq!(comm["alice"], comm["bob"]);
        assert_eq!(comm["bob"], comm["carol"]);
    }

    #[test]
    fn call_gds_leiden_yields_community_ids() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.leiden({resolution: 1.0}) YIELD nodeId, communityId \
             RETURN nodeId, communityId",
        )
        .unwrap();
        let mut comm: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            comm.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
        }
        // The connected chain alice-bob-carol shares one community; d1 is isolated.
        assert_eq!(comm.len(), 4);
        assert_eq!(comm["alice"], comm["bob"]);
        assert_eq!(comm["bob"], comm["carol"]);
        assert_ne!(comm["alice"], comm["d1"]);
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
            w.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
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

    /// Fixture for the structural family: a real triangle {a,b,c} plus an
    /// isolated d, so triangle-count/LCC/k-core/coloring all have a known,
    /// hand-computable expected shape.
    fn triangle_plus_isolated_fixture() -> GraphView {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [("a", "b"), ("b", "c"), ("a", "c")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        core.analysis_snapshot()
    }

    #[test]
    fn call_gds_triangle_count_counts_the_known_triangle() {
        let v = triangle_plus_isolated_fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.triangleCount() YIELD nodeId, triangleCount \
             RETURN nodeId, triangleCount",
        )
        .unwrap();
        let mut counts: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            counts.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
        }
        assert_eq!(counts["a"], 1);
        assert_eq!(counts["b"], 1);
        assert_eq!(counts["c"], 1);
        assert_eq!(counts["d"], 0);
    }

    #[test]
    fn call_gds_local_clustering_coefficient_full_triangle_is_one() {
        let v = triangle_plus_isolated_fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.localClusteringCoefficient() \
             YIELD nodeId, localClusteringCoefficient \
             RETURN nodeId, localClusteringCoefficient",
        )
        .unwrap();
        let mut lcc: HashMap<String, f64> = HashMap::new();
        for r in rows(&qr) {
            lcc.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
        }
        assert!((lcc["a"] - 1.0).abs() < 1e-9);
        assert!(
            (lcc["d"] - 0.0).abs() < 1e-9,
            "isolated node has no neighbours"
        );
    }

    #[test]
    fn call_gds_kcore_triangle_is_a_2_core_isolated_is_a_0_core() {
        let v = triangle_plus_isolated_fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.kcore() YIELD nodeId, coreValue RETURN nodeId, coreValue",
        )
        .unwrap();
        let mut core: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            core.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
        }
        assert_eq!(core["a"], 2);
        assert_eq!(core["b"], 2);
        assert_eq!(core["c"], 2);
        assert_eq!(core["d"], 0);
    }

    #[test]
    fn call_gds_k1coloring_is_a_proper_coloring() {
        let v = triangle_plus_isolated_fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.k1coloring() YIELD nodeId, color RETURN nodeId, color",
        )
        .unwrap();
        let mut color: HashMap<String, i64> = HashMap::new();
        for r in rows(&qr) {
            color.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
        }
        // The genuine correctness property: no two adjacent nodes (the triangle)
        // share a color; the triangle needs exactly 3 distinct colors.
        assert_ne!(color["a"], color["b"]);
        assert_ne!(color["b"], color["c"]);
        assert_ne!(color["a"], color["c"]);
        let distinct: std::collections::HashSet<i64> =
            [color["a"], color["b"], color["c"]].into_iter().collect();
        assert_eq!(distinct.len(), 3);
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
            bc.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
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
            cost.insert(node_id(&r[0]).to_string(), r[1].as_f64().unwrap());
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
        assert_eq!(node_id(&r[0][0]), "carol");
        assert!((r[0][1].as_f64().unwrap() - 6.0).abs() < 1e-9);
    }

    #[test]
    fn call_gds_astar_matches_dijkstra_with_haversine_heuristic() {
        // Same admissible-heuristic fixture as the eg-compute a_star tests:
        // an equator chain (150 each hop) plus a 400-cost direct shortcut.
        let core = GraphCore::new();
        let coords: [(&str, f64, f64); 4] = [
            ("a", 0.0, 0.0),
            ("b", 0.0, 1.0),
            ("c", 0.0, 2.0),
            ("d", 0.0, 3.0),
        ];
        for (id, lat, lon) in coords {
            core.add_node(
                id.into(),
                pbytes(serde_json::json!({"node_type": "N", "latitude": lat, "longitude": lon})),
            );
        }
        for (s, t, w) in [
            ("a", "b", 150.0),
            ("b", "c", 150.0),
            ("c", "d", 150.0),
            ("a", "d", 400.0),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({"weight": w})))
                .unwrap();
        }
        let v = core.analysis_snapshot();

        let astar = exec_cypher(
            &v,
            "CALL gds.shortestPath.astar('a', 'd', {relationshipWeightProperty: 'weight'}) \
             YIELD nodeId, cost RETURN nodeId, cost",
        )
        .unwrap();
        let ra = rows(&astar);
        assert_eq!(ra.len(), 1);
        assert_eq!(node_id(&ra[0][0]), "d");
        assert!((ra[0][1].as_f64().unwrap() - 400.0).abs() < 1e-9);

        let dijkstra = exec_cypher(
            &v,
            "CALL gds.dijkstra('a', 'd', {relationshipWeightProperty: 'weight'}) \
             YIELD nodeId, cost RETURN nodeId, cost",
        )
        .unwrap();
        let rd = rows(&dijkstra);
        assert!((rd[0][1].as_f64().unwrap() - ra[0][1].as_f64().unwrap()).abs() < 1e-9);
    }

    #[test]
    fn call_gds_yens_ranks_three_known_paths() {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t, w) in [
            ("a", "b", 1.0),
            ("b", "d", 1.0),
            ("a", "c", 2.0),
            ("c", "d", 1.0),
            ("a", "d", 5.0),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({"weight": w})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.shortestPath.yens('a', 'd', {k: 3, relationshipWeightProperty: 'weight'}) \
             YIELD index, nodeIds, cost RETURN index, nodeIds, cost",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 3);
        assert!((r[0][2].as_f64().unwrap() - 2.0).abs() < 1e-9);
        assert!((r[1][2].as_f64().unwrap() - 3.0).abs() < 1e-9);
        assert!((r[2][2].as_f64().unwrap() - 5.0).abs() < 1e-9);
        let path0: Vec<String> = r[0][1]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(path0, vec!["a", "b", "d"]);
    }

    #[test]
    fn call_gds_steiner_tree_connects_the_three_spokes() {
        let core = GraphCore::new();
        for id in ["h", "t1", "t2", "t3"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t, w) in [("h", "t1", 3.0), ("h", "t2", 4.0), ("h", "t3", 5.0)] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({"weight": w})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.steinerTree('h', 't1', 't2', 't3', {relationshipWeightProperty: 'weight'}) \
             YIELD nodeId, parentId, weight RETURN nodeId, parentId, weight",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 4);
        let mut by_id: HashMap<String, (Option<String>, Option<f64>)> = HashMap::new();
        for row in &r {
            let id = node_id(&row[0]).to_string();
            by_id.insert(id, (row[1].as_str().map(str::to_string), row[2].as_f64()));
        }
        assert_eq!(by_id["h"], (None, None));
        assert_eq!(by_id["t1"], (Some("h".to_string()), Some(3.0)));
        assert_eq!(by_id["t2"], (Some("h".to_string()), Some(4.0)));
        assert_eq!(by_id["t3"], (Some("h".to_string()), Some(5.0)));
    }

    #[test]
    fn call_gds_random_walk_restart_probability_one_always_returns_to_start() {
        let core = GraphCore::new();
        for id in ["a", "b", "c"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [("a", "b"), ("b", "c"), ("c", "a")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.randomWalk('a', {steps: 4, restartProbability: 1.0, seed: 7}) \
             YIELD index, nodeId RETURN index, nodeId",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 5);
        for row in &r {
            // RETURN index, nodeId ⇒ row[0]=index, row[1]=nodeId.
            assert_eq!(node_id(&row[1]), "a");
        }
    }

    #[test]
    fn eg298_call_gds_node_similarity_jaccard() {
        // Two nodes sharing all out-neighbours score similarity 1.0.
        let core = GraphCore::new();
        for id in ["a", "b", "x", "y"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
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
        assert_eq!(node_id(&r[0][0]), "a");
        assert_eq!(node_id(&r[0][1]), "b");
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
            core.add_node((*id).into(), pbytes(serde_json::json!({"node_type": "N"})));
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
            comm.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
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
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
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
        assert!(r.iter().any(|row| node_id(&row[0]) == "a"
            && node_id(&row[1]) == "b"
            && (row[2].as_f64().unwrap() - 1.0).abs() < 1e-9));
        // b's own top-1 is a (score 1.0, beats c's 0.5) ⇒ (b, c) never appears.
        assert!(!r.iter().any(|row| {
            let ids = [node_id(&row[0]), node_id(&row[1])];
            ids.contains(&"b") && ids.contains(&"c")
        }));
    }

    #[test]
    fn call_gds_knn_approximate_mode_finds_clusters() {
        // 4 blocks × 6 members, each block's members share 3 block-local feature
        // targets ⇒ within-block similarity 1.0, cross-block 0.0. n=24 sources > k+1
        // so `mode: "approximate"` runs real NN-descent (not the tiny-graph fallback).
        let core = GraphCore::new();
        let mut expected_block: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for blk in 0..4 {
            for member in 0..6 {
                let src = format!("n{blk}_{member}");
                core.add_node(src.clone(), pbytes(serde_json::json!({"node_type": "N"})));
                expected_block.insert(src.clone(), blk);
                for f in 0..3 {
                    let tgt = format!("f{blk}_{f}");
                    core.add_node(tgt.clone(), pbytes(serde_json::json!({"node_type": "F"})));
                    core.add_edge(src.clone(), tgt, pbytes(serde_json::json!({})))
                        .unwrap();
                }
            }
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.knn({topK: 3, mode: 'approximate', sampleRate: 0.8, randomSeed: 7}) \
             YIELD node1, node2, similarity RETURN node1, node2, similarity",
        )
        .unwrap();
        let r = rows(&qr);
        assert!(!r.is_empty(), "approximate knn must yield pairs");
        // Every returned pair is a valid within-block (score 1.0) similarity edge:
        // cross-block pairs score 0.0 and are cut by the default cutoff.
        for row in &r {
            let (a, b) = (node_id(&row[0]), node_id(&row[1]));
            let s = row[2].as_f64().unwrap();
            assert!((0.0..=1.0).contains(&s) && s > 0.0);
            if let (Some(ba), Some(bb)) = (expected_block.get(a), expected_block.get(b)) {
                assert_eq!(ba, bb, "an approx pair ({a},{b}) must be within one block");
            }
        }
        // NN-descent must recover at least one exact 1.0 neighbour pair per block it
        // touches — verify it found the strong structure, not just noise.
        assert!(
            r.iter()
                .any(|row| (row[2].as_f64().unwrap() - 1.0).abs() < 1e-9),
            "approx knn must recover within-block 1.0 pairs"
        );
    }

    #[test]
    fn call_gds_knn_approximate_matches_exact_on_tiny_graph() {
        // n=3 sources ≤ topK+1 ⇒ `mode: "approximate"` falls back to the exact sweep,
        // so it must reproduce the exact test's (a,b)@1.0 result — confirms routing.
        let core = GraphCore::new();
        for id in ["a", "b", "c", "x", "y"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [("a", "x"), ("a", "y"), ("b", "x"), ("b", "y"), ("c", "x")] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.knn({topK: 1, mode: 'approximate'}) YIELD node1, node2, similarity \
             RETURN node1, node2, similarity",
        )
        .unwrap();
        let r = rows(&qr);
        assert!(r.iter().any(|row| node_id(&row[0]) == "a"
            && node_id(&row[1]) == "b"
            && (row[2].as_f64().unwrap() - 1.0).abs() < 1e-9));
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
                pbytes(serde_json::json!({"node_type": "Point", "loc": [x, y]})),
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
            cluster.insert(node_id(&r[0]).to_string(), r[1].as_i64().unwrap());
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
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
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

    // ── W4.2 structural embeddings (CONCEPT:EG-KG.graphlearn.structural-embeddings) ──

    /// Two disjoint triangles: `CALL gds.fastRP` streams one `dim`-length embedding per
    /// node, and same-triangle nodes are more cosine-similar than across triangles.
    #[cfg(feature = "cypher-graphlearn")]
    #[test]
    fn call_gds_fastrp_streams_structural_embeddings() {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "x", "y", "z"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.fastRP({embeddingDimension: 32, iterations: 3, randomSeed: 7}) \
             YIELD nodeId, embedding RETURN nodeId, embedding",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 6, "one embedding row per node");
        let mut emb: HashMap<String, Vec<f64>> = HashMap::new();
        for row in &r {
            let vec: Vec<f64> = row[1]
                .as_array()
                .expect("embedding is a list")
                .iter()
                .map(|x| x.as_f64().unwrap())
                .collect();
            assert_eq!(vec.len(), 32, "embedding has the configured dimension");
            emb.insert(node_id(&row[0]).to_string(), vec);
        }
        let cos = |a: &[f64], b: &[f64]| {
            let dot: f64 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
            let nb: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
            if na * nb > 0.0 {
                dot / (na * nb)
            } else {
                0.0
            }
        };
        let intra = cos(&emb["a"], &emb["b"]);
        let inter = cos(&emb["a"], &emb["x"]);
        assert!(
            intra > inter,
            "same-triangle {intra} should exceed cross {inter}"
        );
    }

    /// `CALL gds.fastRP` is deterministic given the seed (same rows twice).
    #[cfg(feature = "cypher-graphlearn")]
    #[test]
    fn call_gds_fastrp_is_deterministic() {
        let v = fixture();
        let q = "CALL gds.fastRP({embeddingDimension: 16, randomSeed: 3}) \
                 YIELD nodeId, embedding RETURN nodeId, embedding";
        let a = rows(&exec_cypher(&v, q).unwrap());
        let b = rows(&exec_cypher(&v, q).unwrap());
        assert_eq!(a, b);
    }

    /// `CALL gds.node2vec` streams one `dim`-length embedding per node.
    #[cfg(feature = "cypher-graphlearn")]
    #[test]
    fn call_gds_node2vec_streams_structural_embeddings() {
        let core = GraphCore::new();
        for id in ["a", "b", "c", "x", "y", "z"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type": "N"})));
        }
        for (s, t) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
        ] {
            core.add_edge(s.into(), t.into(), pbytes(serde_json::json!({})))
                .unwrap();
        }
        let v = core.analysis_snapshot();
        let qr = exec_cypher(
            &v,
            "CALL gds.node2vec({embeddingDimension: 16, walkLength: 20, walksPerNode: 10, \
             iterations: 5, randomSeed: 7}) YIELD nodeId, embedding RETURN nodeId, embedding",
        )
        .unwrap();
        let r = rows(&qr);
        assert_eq!(r.len(), 6);
        for row in &r {
            let len = row[1].as_array().expect("embedding is a list").len();
            assert_eq!(len, 16);
        }
    }
}
