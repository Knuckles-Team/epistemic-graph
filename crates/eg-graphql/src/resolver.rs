//! The GraphQL resolver (CONCEPT:KG-2.235): compile a parsed [`Query`] to scans +
//! BFS over the SAME `GraphView` the Cypher / unified executor reads, and materialize
//! the result as JSON. NO second graph copy, NO async-graphql — the same "more scans
//! over the one substrate" approach as eg-rdf's SPARQL compile.
//!
//! Resolution per field shape (the "which maps to what" contract — kept byte-identical
//! to eg-query/cypher's primitives so a GraphQL query returns the SAME nodes/fields as
//! the equivalent Cypher query):
//!   * a ROOT field `Type(args) { … }` → the eg-core label index (the
//!     `type`/`node_type`/`label`/`labels` keys), filtered by the property-equality
//!     `args`, capped by `first`/`limit`, then each node's selection resolved;
//!   * a SCALAR field `prop` → that property from the node's blob (`null` if absent);
//!   * an OBJECT field `rel { … }` → follow outgoing edges typed `rel` (the same edge
//!     `type`/`relationship` check `rel_matches` uses) to the target nodes, each
//!     recursively resolved → a LIST.

use std::collections::HashSet;

use eg_core::graph::GraphView;
use petgraph::visit::EdgeRef;
use serde_json::{Map, Value};

use crate::parser::{parse, Field, GqlValue, Query};
use crate::schema::{decode, node_labels, Schema};

/// Default cap on root-field rows when no `first`/`limit` arg is given (mirrors the
/// Cypher surface's implicit bound — one Response per Request).
const MAX_ROOT_ROWS: usize = 50_000;

/// Parse + execute a GraphQL query string over `view`, returning the GraphQL-shaped
/// `{"data": { … }}` JSON. A parse error or an unknown root type is an `Err`.
pub fn execute(view: &GraphView, query: &str) -> Result<Value, String> {
    let q = parse(query).map_err(|e| e.to_string())?;
    execute_query(view, &q)
}

/// Execute an already-parsed [`Query`], validating each root field against the schema
/// derived from `view`.
pub fn execute_query(view: &GraphView, q: &Query) -> Result<Value, String> {
    let schema = Schema::from_view(view);
    let mut data = Map::new();
    for root in &q.roots {
        if !schema.has_type(&root.name) {
            return Err(format!(
                "GraphQL: no node type `{}` in the graph (root fields must be node labels)",
                root.name
            ));
        }
        let nodes = resolve_root(view, root)?;
        data.insert(root.alias.clone(), Value::Array(nodes));
    }
    Ok(Value::Object(
        [("data".to_string(), Value::Object(data))]
            .into_iter()
            .collect(),
    ))
}

/// Resolve a root field: label scan + arg filter + `first`/`limit`, then per-node
/// selection. Returns the list of result objects.
fn resolve_root(view: &GraphView, field: &Field) -> Result<Vec<Value>, String> {
    let (limit, filters) = split_args(&field.args)?;
    let cap = limit.unwrap_or(MAX_ROOT_ROWS).min(MAX_ROOT_ROWS);

    // Candidate node ids carrying this label, in a stable (sorted) order so the result
    // is deterministic across runs (matching a DB's stable scan order).
    let mut candidates: Vec<String> = view
        .node_properties
        .iter()
        .filter_map(|(id, blob)| {
            let val = decode(blob)?;
            if node_labels(&val).iter().any(|l| l == &field.name) {
                Some(id.clone())
            } else {
                None
            }
        })
        .collect();
    candidates.sort();

    let mut out = Vec::new();
    for id in candidates {
        if out.len() >= cap {
            break;
        }
        let Some(val) = view.node_properties.get(&id).and_then(|b| decode(b)) else {
            continue;
        };
        // property-equality filters from the args.
        if !filters.iter().all(|(k, v)| prop_eq(&val, k, v)) {
            continue;
        }
        out.push(resolve_selection(view, &id, &val, &field.selection)?);
    }
    Ok(out)
}

/// Resolve a node's selection set into a JSON object.
fn resolve_selection(
    view: &GraphView,
    node_id: &str,
    val: &Value,
    selection: &[Field],
) -> Result<Value, String> {
    let mut obj = Map::new();
    // A node with no selection set still resolves to its id (so `{ Person }` is legal).
    if selection.is_empty() {
        obj.insert("id".to_string(), Value::String(node_id.to_string()));
        return Ok(Value::Object(obj));
    }
    for f in selection {
        if f.selection.is_empty() && f.args.is_empty() {
            // scalar field: the node property (or `id`), `null` if absent.
            let cell = if f.name == "id" {
                Value::String(node_id.to_string())
            } else {
                prop_value(val, &f.name).unwrap_or(Value::Null)
            };
            obj.insert(f.alias.clone(), cell);
        } else {
            // object field: an edge relationship — traverse to targets, recurse.
            let targets = resolve_edge(view, node_id, f)?;
            obj.insert(f.alias.clone(), Value::Array(targets));
        }
    }
    Ok(Value::Object(obj))
}

/// Resolve an edge field `rel { … }`: outgoing edges typed `rel` from `node_id` →
/// the target nodes, each resolved against `field.selection`. Honors an optional
/// `first`/`limit` arg on the edge field.
fn resolve_edge(view: &GraphView, node_id: &str, field: &Field) -> Result<Vec<Value>, String> {
    let (limit, filters) = split_args(&field.args)?;
    let cap = limit.unwrap_or(MAX_ROOT_ROWS).min(MAX_ROOT_ROWS);

    let mut targets = outgoing_targets(view, node_id, &field.name);
    targets.sort();
    targets.dedup();

    let mut out = Vec::new();
    for tid in targets {
        if out.len() >= cap {
            break;
        }
        let Some(val) = view.node_properties.get(&tid).and_then(|b| decode(b)) else {
            continue;
        };
        if !filters.iter().all(|(k, v)| prop_eq(&val, k, v)) {
            continue;
        }
        out.push(resolve_selection(view, &tid, &val, &field.selection)?);
    }
    Ok(out)
}

/// Target node ids of outgoing edges typed `rel` from `src` (the same edge `type`/
/// `relationship` check eg-query/cypher's `rel_matches` uses).
fn outgoing_targets(view: &GraphView, src: &str, rel: &str) -> Vec<String> {
    let Some(&src_idx) = view.node_map.get(src) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for edge in view
        .graph
        .edges_directed(src_idx, petgraph::Direction::Outgoing)
    {
        let tgt = edge.target();
        let Some(tid) = view.graph.node_weight(tgt) else {
            continue;
        };
        if rel_matches(view, src, tid, rel) && seen.insert(tid.clone()) {
            out.push(tid.clone());
        }
    }
    out
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge blobs'
/// `relationship`/`type` field — identical to eg-query/cypher's `rel_matches`.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: &str) -> bool {
    let Some(blobs) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return false;
    };
    for blob in blobs {
        if let Some(Value::Object(m)) = decode(blob) {
            let stored = m
                .get("relationship")
                .or_else(|| m.get("type"))
                .and_then(|v| v.as_str());
            if stored == Some(rel) {
                return true;
            }
        }
    }
    false
}

/// Split a field's args into (the `first`/`limit` cap, the property-equality filters).
fn split_args(
    args: &[(String, GqlValue)],
) -> Result<(Option<usize>, Vec<(String, Value)>), String> {
    let mut limit = None;
    let mut filters = Vec::new();
    for (k, v) in args {
        if k == "first" || k == "limit" {
            match v {
                GqlValue::Int(n) if *n >= 0 => limit = Some(*n as usize),
                _ => return Err(format!("`{k}` must be a non-negative integer")),
            }
        } else {
            filters.push((k.clone(), gql_to_json(v)));
        }
    }
    Ok((limit, filters))
}

/// Read one property from a node's blob, unwrapping the property-graph `{value: …}`
/// cell shape if present (so a typed literal returns its value, matching SPARQL).
fn prop_value(val: &Value, key: &str) -> Option<Value> {
    let cell = val.get(key)?;
    // A property-graph cell may be `{"value": …, "datatype": …}` or a bare scalar.
    if let Some(obj) = cell.as_object() {
        if let Some(inner) = obj.get("value") {
            return Some(inner.clone());
        }
    }
    Some(cell.clone())
}

/// Property-equality filter: does `key`'s (unwrapped) value equal `expected`?
fn prop_eq(val: &Value, key: &str, expected: &Value) -> bool {
    match prop_value(val, key) {
        Some(actual) => values_eq(&actual, expected),
        None => false,
    }
}

/// Equality across JSON types, tolerant of string-vs-number (a GraphQL `name: "30"`
/// matches a stored numeric `30` and vice-versa, since property cells are untyped).
fn values_eq(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a.as_str(), b.as_str()) {
        (Some(sa), Some(sb)) => sa == sb,
        _ => {
            let sa = scalar_string(a);
            let sb = scalar_string(b);
            sa.is_some() && sa == sb
        }
    }
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn gql_to_json(v: &GqlValue) -> Value {
    match v {
        GqlValue::Int(n) => Value::Number((*n).into()),
        GqlValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        GqlValue::Str(s) => Value::String(s.clone()),
        GqlValue::Bool(b) => Value::Bool(*b),
    }
}
