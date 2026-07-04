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
    all_pairs_similarity, betweenness_centrality, degree_centrality, dijkstra, louvain, pagerank,
    strongly_connected_components, weakly_connected_components, AdjacencyGraph, DegreeKind,
    Direction, LouvainConfig, Metric, PageRankConfig,
};

use super::proc::{CypherProcedure, ProcRow, YieldValue};

/// Every EG-298 GDS procedure, ready to fold into the procedure registry
/// (CONCEPT:EG-KG.query.gds-call-procedures). Consumed by `proc::build_registry`.
pub fn gds_procedures() -> Vec<Box<dyn CypherProcedure>> {
    vec![
        Box::new(PageRank),
        Box::new(Betweenness),
        Box::new(Degree),
        Box::new(Louvain),
        Box::new(Wcc),
        Box::new(Scc),
        Box::new(Dijkstra),
        Box::new(NodeSimilarity),
    ]
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
}
