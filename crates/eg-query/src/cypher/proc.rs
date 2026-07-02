//! The Cypher procedure registry (CONCEPT:EG-142). A `CALL proc.name(args) YIELD …`
//! stage consults [`registry`] for a [`CypherProcedure`] by (case-insensitive) name
//! and materializes its result rows into the Cypher pipeline.
//!
//! This module lands the native procedure-invocation FRAMEWORK — the trait, the
//! process-wide registry, and one trivial built-in (`db.labels`) that proves the
//! `CALL … YIELD` path end to end. The APOC/GDS procedure LIBRARY (graph-data-science
//! kernels + APOC utilities) is registered on top of this framework under
//! CONCEPT:EG-143. WASM/user-defined procedures (via eg-wasm) are a documented
//! follow-up: the trait + registry below is what a dynamic provider registers into.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use eg_core::graph::GraphView;
use serde_json::Value;

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
    /// The canonical dotted name (`db.labels`).
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
    // A trivial metadata built-in that proves the CALL … YIELD framework end to end.
    // The full APOC/GDS library is registered here under CONCEPT:EG-143.
    add(Box::new(DbLabels));
    m
}

// ── helpers ───────────────────────────────────────────────────────────────────

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
        let Some(obj) = decode_obj(blob) else { continue };
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

// ── db.labels (CONCEPT:EG-142 framework proof) ──────────────────────────────────

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
