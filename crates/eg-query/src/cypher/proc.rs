//! The Cypher procedure registry (CONCEPT:EG-142) and its built-in APOC/GDS-style
//! procedure library (CONCEPT:EG-143). A `CALL proc.name(args) YIELD …` stage
//! consults [`registry`] for a [`CypherProcedure`] by (case-insensitive) name and
//! materializes its result rows into the Cypher pipeline.
//!
//! The built-ins REUSE the eg-compute graph algorithms (`pagerank`,
//! `betweenness_centrality`, `degree_centrality_all`, `community_detection`,
//! `connected_components`, `personalized_pagerank`) so the graph-data-science surface
//! is exactly the engine's own kernels adapted into `YIELD` rows over the current
//! graph view — NO second implementation. A handful of APOC/`db.` metadata + list
//! utilities round out a tractable representative set.
//!
//! WASM/user-defined procedures (via eg-wasm) are a documented follow-up: the trait +
//! registry framework below is what a dynamic provider would register into.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use eg_core::graph::GraphView;
use serde_json::Value;

use eg_compute::algorithms;

/// A single yielded column value: either a graph node id (bindable as an anchorable
/// node variable downstream) or an opaque scalar (CONCEPT:EG-142).
#[derive(Debug, Clone)]
pub enum YieldValue {
    Node(String),
    Scalar(Value),
}

/// One procedure result row: an ordered list of `(column, value)` pairs.
pub type ProcRow = Vec<(String, YieldValue)>;

/// A callable Cypher procedure (CONCEPT:EG-142). Stateless — the registry holds one
/// shared instance per name and the executor calls it with resolved args + the live
/// (read-only) graph view.
pub trait CypherProcedure: Send + Sync {
    /// The canonical dotted name (`gds.pageRank`).
    fn name(&self) -> &'static str;
    /// The columns this procedure yields, in order (for docs / validation).
    fn columns(&self) -> &'static [&'static str];
    /// Run the procedure, producing YIELD rows over `view`.
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String>;
}

/// The process-wide procedure registry, keyed by lower-cased name (CONCEPT:EG-142).
pub fn registry() -> &'static HashMap<String, Box<dyn CypherProcedure>> {
    static REG: OnceLock<HashMap<String, Box<dyn CypherProcedure>>> = OnceLock::new();
    REG.get_or_init(build_registry)
}

fn build_registry() -> HashMap<String, Box<dyn CypherProcedure>> {
    let mut m: HashMap<String, Box<dyn CypherProcedure>> = HashMap::new();
    let mut add = |p: Box<dyn CypherProcedure>| {
        m.insert(p.name().to_ascii_lowercase(), p);
    };
    // ── GDS graph-data-science kernels (CONCEPT:EG-143) ───────────────────────
    add(Box::new(PageRank));
    add(Box::new(Betweenness));
    add(Box::new(Degree));
    add(Box::new(Louvain));
    add(Box::new(Wcc));
    add(Box::new(PersonalizedPageRank));
    // ── APOC / db. metadata + list utilities (CONCEPT:EG-143) ─────────────────
    add(Box::new(DbLabels));
    add(Box::new(DbRelationshipTypes));
    add(Box::new(ApocMetaStats));
    add(Box::new(ApocCollSum));
    add(Box::new(ApocCollMax));
    add(Box::new(ApocCollMin));
    add(Box::new(ApocCollAvg));
    m
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// A finite `f64` as a JSON number (an integer when integral, mirroring the
/// executor's numeric coercion), else null.
fn num(x: f64) -> Value {
    if x.fract() == 0.0 && x.abs() < 9.007e15 {
        Value::Number((x as i64).into())
    } else {
        serde_json::Number::from_f64(x)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

/// `(node, score)` rows from a `Vec<(id, f64)>` algorithm result.
fn score_rows(scored: Vec<(String, f64)>) -> Vec<ProcRow> {
    scored
        .into_iter()
        .map(|(id, s)| {
            vec![
                ("node".to_string(), YieldValue::Node(id)),
                ("score".to_string(), YieldValue::Scalar(num(s))),
            ]
        })
        .collect()
}

/// `(node, <group-col>)` rows from a partition (`Vec<Vec<id>>`): each node tagged with
/// its 0-based group index.
fn group_rows(groups: Vec<Vec<String>>, group_col: &str) -> Vec<ProcRow> {
    let mut out = Vec::new();
    for (gid, members) in groups.into_iter().enumerate() {
        for id in members {
            out.push(vec![
                ("node".to_string(), YieldValue::Node(id)),
                (
                    group_col.to_string(),
                    YieldValue::Scalar(Value::Number((gid as u64).into())),
                ),
            ]);
        }
    }
    out
}

/// Decode a node/edge property blob into a JSON object.
fn decode_obj(blob: &[u8]) -> Option<serde_json::Map<String, Value>> {
    match rmp_serde::from_slice::<Value>(blob) {
        Ok(Value::Object(m)) => Some(m),
        _ => None,
    }
}

/// Collect the distinct labels carried by all nodes (the same fields the label index
/// keys on: `type`/`node_type`/`label` scalars + the `labels` array).
fn distinct_labels(view: &GraphView) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for blob in view.node_properties.values() {
        let Some(obj) = decode_obj(blob) else {
            continue;
        };
        for key in ["type", "node_type", "label"] {
            if let Some(s) = obj.get(key).and_then(|v| v.as_str()) {
                set.insert(s.to_string());
            }
        }
        if let Some(arr) = obj.get("labels").and_then(|v| v.as_array()) {
            for x in arr {
                if let Some(s) = x.as_str() {
                    set.insert(s.to_string());
                }
            }
        }
    }
    set
}

/// Collect the distinct relationship types across all edges (`relationship`/`type`).
fn distinct_rel_types(view: &GraphView) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for blobs in view.edge_properties.values() {
        for blob in blobs {
            let Some(obj) = decode_obj(blob) else {
                continue;
            };
            if let Some(s) = obj
                .get("relationship")
                .or_else(|| obj.get("type"))
                .and_then(|v| v.as_str())
            {
                set.insert(s.to_string());
            }
        }
    }
    set
}

/// The seed `(id, weight)` list for personalized PageRank from the call args: a single
/// list arg (`['a','b']`) or several scalar id args; each seed gets weight 1.0.
fn seed_ids(args: &[Value]) -> Vec<(String, f64)> {
    let mut seeds = Vec::new();
    let mut push = |v: &Value| {
        if let Some(s) = v.as_str() {
            seeds.push((s.to_string(), 1.0));
        }
    };
    for a in args {
        match a {
            Value::Array(items) => items.iter().for_each(&mut push),
            other => push(other),
        }
    }
    seeds
}

/// The list argument to an `apoc.coll.*` utility: the first arg if it is an array.
fn coll_arg(args: &[Value]) -> Result<&Vec<Value>, String> {
    match args.first() {
        Some(Value::Array(a)) => Ok(a),
        _ => Err("apoc.coll.* expects a list argument".to_string()),
    }
}

fn coll_nums(list: &[Value]) -> Vec<f64> {
    list.iter().filter_map(|v| v.as_f64()).collect()
}

fn single_value_row(v: Value) -> Vec<ProcRow> {
    vec![vec![("value".to_string(), YieldValue::Scalar(v))]]
}

// ── GDS procedures (CONCEPT:EG-143) ─────────────────────────────────────────────

struct PageRank;
impl CypherProcedure for PageRank {
    fn name(&self) -> &'static str {
        "gds.pageRank"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "score"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(score_rows(algorithms::pagerank(view, 0.85, 20)))
    }
}

struct Betweenness;
impl CypherProcedure for Betweenness {
    fn name(&self) -> &'static str {
        "gds.betweenness"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "score"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(score_rows(algorithms::betweenness_centrality(view)))
    }
}

struct Degree;
impl CypherProcedure for Degree {
    fn name(&self) -> &'static str {
        "gds.degree"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "score"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(score_rows(algorithms::degree_centrality_all(view)))
    }
}

struct Louvain;
impl CypherProcedure for Louvain {
    fn name(&self) -> &'static str {
        "gds.louvain"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "communityId"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(group_rows(
            algorithms::community_detection(view, 1.0),
            "communityId",
        ))
    }
}

struct Wcc;
impl CypherProcedure for Wcc {
    fn name(&self) -> &'static str {
        "gds.wcc"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "componentId"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(group_rows(
            algorithms::connected_components(view),
            "componentId",
        ))
    }
}

struct PersonalizedPageRank;
impl CypherProcedure for PersonalizedPageRank {
    fn name(&self) -> &'static str {
        "gds.pastPersonalizedPageRank"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["node", "score"]
    }
    fn call(&self, args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let seeds = seed_ids(args);
        Ok(score_rows(algorithms::personalized_pagerank(
            view, &seeds, 0.85, 20,
        )))
    }
}

// ── APOC / db. metadata + list utilities (CONCEPT:EG-143) ───────────────────────

struct DbLabels;
impl CypherProcedure for DbLabels {
    fn name(&self) -> &'static str {
        "db.labels"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["label"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(distinct_labels(view)
            .into_iter()
            .map(|l| vec![("label".to_string(), YieldValue::Scalar(Value::String(l)))])
            .collect())
    }
}

struct DbRelationshipTypes;
impl CypherProcedure for DbRelationshipTypes {
    fn name(&self) -> &'static str {
        "db.relationshipTypes"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["relationshipType"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        Ok(distinct_rel_types(view)
            .into_iter()
            .map(|t| {
                vec![(
                    "relationshipType".to_string(),
                    YieldValue::Scalar(Value::String(t)),
                )]
            })
            .collect())
    }
}

struct ApocMetaStats;
impl CypherProcedure for ApocMetaStats {
    fn name(&self) -> &'static str {
        "apoc.meta.stats"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["nodeCount", "relCount", "labelCount", "relTypeCount"]
    }
    fn call(&self, _args: &[Value], view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let node_count = view.node_map.len() as u64;
        let rel_count = view.graph.edge_count() as u64;
        let label_count = distinct_labels(view).len() as u64;
        let rel_type_count = distinct_rel_types(view).len() as u64;
        Ok(vec![vec![
            (
                "nodeCount".to_string(),
                YieldValue::Scalar(Value::Number(node_count.into())),
            ),
            (
                "relCount".to_string(),
                YieldValue::Scalar(Value::Number(rel_count.into())),
            ),
            (
                "labelCount".to_string(),
                YieldValue::Scalar(Value::Number(label_count.into())),
            ),
            (
                "relTypeCount".to_string(),
                YieldValue::Scalar(Value::Number(rel_type_count.into())),
            ),
        ]])
    }
}

struct ApocCollSum;
impl CypherProcedure for ApocCollSum {
    fn name(&self) -> &'static str {
        "apoc.coll.sum"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["value"]
    }
    fn call(&self, args: &[Value], _view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let sum: f64 = coll_nums(coll_arg(args)?).iter().sum();
        Ok(single_value_row(num(sum)))
    }
}

struct ApocCollMax;
impl CypherProcedure for ApocCollMax {
    fn name(&self) -> &'static str {
        "apoc.coll.max"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["value"]
    }
    fn call(&self, args: &[Value], _view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let v = coll_nums(coll_arg(args)?)
            .into_iter()
            .fold(f64::NEG_INFINITY, f64::max);
        Ok(single_value_row(if v.is_finite() {
            num(v)
        } else {
            Value::Null
        }))
    }
}

struct ApocCollMin;
impl CypherProcedure for ApocCollMin {
    fn name(&self) -> &'static str {
        "apoc.coll.min"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["value"]
    }
    fn call(&self, args: &[Value], _view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let v = coll_nums(coll_arg(args)?)
            .into_iter()
            .fold(f64::INFINITY, f64::min);
        Ok(single_value_row(if v.is_finite() {
            num(v)
        } else {
            Value::Null
        }))
    }
}

struct ApocCollAvg;
impl CypherProcedure for ApocCollAvg {
    fn name(&self) -> &'static str {
        "apoc.coll.avg"
    }
    fn columns(&self) -> &'static [&'static str] {
        &["value"]
    }
    fn call(&self, args: &[Value], _view: &GraphView) -> Result<Vec<ProcRow>, String> {
        let nums = coll_nums(coll_arg(args)?);
        let v = if nums.is_empty() {
            Value::Null
        } else {
            num(nums.iter().sum::<f64>() / nums.len() as f64)
        };
        Ok(single_value_row(v))
    }
}
