//! Cypher execution (CONCEPT:KG-2.179). Runs a parsed [`CypherQuery`] over an
//! off-lock `GraphView` and materializes a `QueryResult` — the SAME carrier the
//! SQL surface uses. NO DataFusion: every pattern shape compiles to one of the
//! engine's own primitives.
//!
//! Strategy:
//!   * a node's `:Label` predicate            → the eg-core label index (the same
//!     `type`/`node_type`/`label`/`labels` fields `get_nodes_by_label` keys on),
//!     resolved here directly off the `GraphView`;
//!   * a linear MATCH path                     → an incremental neighbour-walk
//!     (`resolve_match`): start from the label-index candidates, then extend hop by
//!     hop. A FIXED hop extends to relationship-typed neighbours; a VARIABLE-length
//!     hop (`*min..max`) extends via petgraph BFS. Fixed and variable-length hops
//!     freely combine in one pattern (CONCEPT:EG-063), and an already-bound node var
//!     anchors its position — which is also how `OPTIONAL MATCH` / `WITH`
//!     pipelining join onto prior bindings (CONCEPT:EG-062).
//!
//! Read clauses (CONCEPT:EG-062): a read query is a pipeline of reading stages
//! (`MATCH`/`OPTIONAL MATCH`/`WITH`) terminated by a `RETURN` that supports
//! `ORDER BY`/`SKIP`/`LIMIT`/`DISTINCT`/`*` and aggregation.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use eg_core::graph::{GraphCore, GraphView};
use petgraph::visit::EdgeRef;
use serde_json::Value;

// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::parser;
use super::plan::{
    AggArg, AggFunc, CompareOp, Condition, CypherQuery, Direction, EdgePat, Expr, NodePat, Pattern,
    ReadStage, RemoveItem, ReturnItem, ReturnSpec, SetItem, Statement, Test, WhereExpr, WithItem,
    WriteOp, WriteQuery,
};

/// Implicit max rows (mirrors the SQL surface): one Response per Request, so an
/// unbounded RETURN would buffer the whole result in one message.
const MAX_ROWS: usize = 50_000;

/// A var→node-id binding row. A path variable (CONCEPT:EG-063) is stored under the
/// `@path@<var>` key as a JSON-array string of the node ids along the path; an edge
/// variable (write path) under `@edge@<var>` as `src\0tgt`.
type Binding = HashMap<String, String>;

/// Parse + run `cypher` over `view` (read-only, single graph). Synchronous and
/// dep-free — safe to call inside `spawn_blocking` like `exec_sql`.
pub fn exec_cypher(view: &GraphView, cypher: &str) -> Result<QueryResult, String> {
    let query = parser::parse(cypher)?;
    let bindings = run_stages(view, &query.stages)?;
    finalize(view, &query, bindings)
}

// ── read-stage pipeline (CONCEPT:EG-062) ─────────────────────────────────────

/// Run the reading-stage pipeline, threading bindings from one stage to the next.
fn run_stages(view: &GraphView, stages: &[ReadStage]) -> Result<Vec<Binding>, String> {
    // Seed with one empty binding so the first MATCH resolves from scratch.
    let mut bindings: Vec<Binding> = vec![HashMap::new()];
    for stage in stages {
        match stage {
            ReadStage::Match {
                pattern,
                optional,
                where_clause,
                path_var,
            } => {
                let mut out: Vec<Binding> = Vec::new();
                for incoming in &bindings {
                    let mut matched = resolve_match(view, pattern, where_clause, incoming)?;
                    if let Some(pv) = path_var {
                        for b in matched.iter_mut() {
                            record_path(pattern, b, pv);
                        }
                    }
                    if matched.is_empty() && *optional {
                        // OPTIONAL MATCH that didn't extend → keep the prior binding;
                        // the stage's new vars project as null.
                        out.push(incoming.clone());
                    } else {
                        out.extend(matched);
                    }
                }
                bindings = out;
            }
            ReadStage::With {
                items,
                where_clause,
            } => {
                let mut out: Vec<Binding> = Vec::new();
                for b in &bindings {
                    let nb = project_with(b, items);
                    if where_holds(view, &nb, where_clause) {
                        out.push(nb);
                    }
                }
                bindings = out;
            }
        }
    }
    Ok(bindings)
}

/// Resolve a linear MATCH `pattern` into var→node-id bindings, applying `where`.
/// `anchor` pre-binds variables (empty for a fresh MATCH; the incoming binding for
/// an `OPTIONAL MATCH` / post-`WITH` MATCH) — any pattern position whose variable is
/// already in `anchor` is constrained to that id, which is the join mechanism
/// (CONCEPT:EG-062). Fixed and variable-length hops combine freely (CONCEPT:EG-063).
fn resolve_match(
    view: &GraphView,
    pattern: &Pattern,
    where_clause: &Option<WhereExpr>,
    anchor: &Binding,
) -> Result<Vec<Binding>, String> {
    // Start candidates: the anchored id if the start var is bound, else the label set.
    let start_ids: Vec<String> = match pattern.start.var.as_ref().and_then(|v| anchor.get(v)) {
        Some(id) => vec![id.clone()],
        None => label_candidates(view, &pattern.start),
    };

    // (binding, current-node-id) partials, extended hop by hop.
    let mut partials: Vec<(Binding, String)> = Vec::new();
    for sid in start_ids {
        let mut b = anchor.clone();
        if let Some(v) = &pattern.start.var {
            b.insert(v.clone(), sid.clone());
        }
        partials.push((b, sid));
    }

    for (edge, node) in &pattern.hops {
        let mut next: Vec<(Binding, String)> = Vec::new();
        for (b, cur) in &partials {
            let targets: Vec<String> = match edge.var_len {
                Some((min, max)) => bfs_reachable(view, cur, edge, min, max),
                None => neighbors(view, cur, edge),
            };
            for t in targets {
                // node label filter
                if let Some(lbl) = &node.label {
                    if !node_has_label_id(view, &t, lbl) {
                        continue;
                    }
                }
                // anchor / already-bound consistency
                if let Some(v) = &node.var {
                    if let Some(bound) = b.get(v) {
                        if bound != &t {
                            continue;
                        }
                    }
                }
                let mut nb = b.clone();
                if let Some(v) = &node.var {
                    nb.insert(v.clone(), t.clone());
                }
                next.push((nb, t));
            }
        }
        partials = next;
    }

    let mut out: Vec<Binding> = Vec::new();
    for (b, _) in partials {
        if where_holds(view, &b, where_clause) {
            out.push(b);
        }
    }
    Ok(out)
}

/// Relationship-typed neighbours of `cur` in `edge.direction` (a single fixed hop).
fn neighbors(view: &GraphView, cur: &str, edge: &EdgePat) -> Vec<String> {
    let Some(&idx) = view.node_map.get(cur) else {
        return Vec::new();
    };
    let dir = match edge.direction {
        Direction::Right => petgraph::Direction::Outgoing,
        Direction::Left => petgraph::Direction::Incoming,
    };
    let mut out = Vec::new();
    for e in view.graph.edges_directed(idx, dir) {
        let from_id = &view.graph[e.source()];
        let to_id = &view.graph[e.target()];
        if !rel_matches(view, from_id, to_id, edge.rel_type.as_deref()) {
            continue;
        }
        let nbr = match edge.direction {
            Direction::Right => e.target(),
            Direction::Left => e.source(),
        };
        out.push(view.graph[nbr].clone());
    }
    out
}

/// BFS from `src` over REL-typed edges in `edge.direction`, returning every node
/// id reached at a hop-depth within `[min,max]` (depth ≥ 1). Each target appears
/// once (the shallowest depth that reaches it).
fn bfs_reachable(
    view: &GraphView,
    src: &str,
    edge: &EdgePat,
    min: usize,
    max: usize,
) -> Vec<String> {
    let Some(&src_idx) = view.node_map.get(src) else {
        return Vec::new();
    };
    let mut reached: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    let mut frontier: Vec<petgraph::stable_graph::NodeIndex> = vec![src_idx];
    let mut visited: HashSet<petgraph::stable_graph::NodeIndex> = HashSet::new();
    visited.insert(src_idx);

    for depth in 1..=max {
        let mut next: Vec<petgraph::stable_graph::NodeIndex> = Vec::new();
        for &node in &frontier {
            let dir = match edge.direction {
                Direction::Right => petgraph::Direction::Outgoing,
                Direction::Left => petgraph::Direction::Incoming,
            };
            for e in view.graph.edges_directed(node, dir) {
                let nbr = match edge.direction {
                    Direction::Right => e.target(),
                    Direction::Left => e.source(),
                };
                let from_id = &view.graph[e.source()];
                let to_id = &view.graph[e.target()];
                if !rel_matches(view, from_id, to_id, edge.rel_type.as_deref()) {
                    continue;
                }
                if visited.insert(nbr) {
                    next.push(nbr);
                }
                if depth >= min {
                    let nbr_id = view.graph[nbr].clone();
                    if reached.insert(nbr_id.clone()) {
                        out.push(nbr_id);
                    }
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    out
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge's
/// property blobs (`relationship` or `type` field). `None` ⇒ any relationship.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: Option<&str>) -> bool {
    let Some(rel) = rel else { return true };
    let Some(props_list) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return false;
    };
    for blob in props_list {
        if let Ok(Value::Object(m)) = rmp_serde::from_slice::<Value>(blob) {
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

// ── path / WITH plumbing (CONCEPT:EG-062 / EG-063) ───────────────────────────

/// The binding key a path variable's node-id sequence is stored under.
fn path_key(var: &str) -> String {
    format!("@path@{var}")
}

/// Record the node-id sequence of `pattern` for the path variable `pv`, at each
/// NAMED node position (intermediate hops of a variable-length segment are not
/// enumerated — the documented simplification). Stored as a JSON array string.
fn record_path(pattern: &Pattern, binding: &mut Binding, pv: &str) {
    let mut seq: Vec<String> = Vec::new();
    if let Some(v) = &pattern.start.var {
        if let Some(id) = binding.get(v) {
            seq.push(id.clone());
        }
    }
    for (_, node) in &pattern.hops {
        if let Some(v) = &node.var {
            if let Some(id) = binding.get(v) {
                seq.push(id.clone());
            }
        }
    }
    binding.insert(path_key(pv), serde_json::to_string(&seq).unwrap_or_default());
}

/// Project a binding through a `WITH` item list: keep only the listed variables,
/// applying aliases (and carrying their path-var sidecar) (CONCEPT:EG-062).
fn project_with(b: &Binding, items: &[WithItem]) -> Binding {
    let mut nb = Binding::new();
    for it in items {
        let target = it.alias.clone().unwrap_or_else(|| it.var.clone());
        if let Some(id) = b.get(&it.var) {
            nb.insert(target.clone(), id.clone());
        }
        if let Some(p) = b.get(&path_key(&it.var)) {
            nb.insert(path_key(&target), p.clone());
        }
    }
    nb
}

/// The in-scope variable names (in declaration order) for `RETURN *` — node vars and
/// path vars accumulate across MATCH stages; a `WITH` narrows scope to its outputs.
fn scope_vars(stages: &[ReadStage]) -> Vec<String> {
    let mut scope: Vec<String> = Vec::new();
    let push = |v: &str, scope: &mut Vec<String>| {
        if !scope.iter().any(|s| s == v) {
            scope.push(v.to_string());
        }
    };
    for stage in stages {
        match stage {
            ReadStage::Match {
                pattern, path_var, ..
            } => {
                if let Some(v) = &pattern.start.var {
                    push(v, &mut scope);
                }
                for (_, node) in &pattern.hops {
                    if let Some(v) = &node.var {
                        push(v, &mut scope);
                    }
                }
                if let Some(pv) = path_var {
                    push(pv, &mut scope);
                }
            }
            ReadStage::With { items, .. } => {
                scope = items
                    .iter()
                    .map(|it| it.alias.clone().unwrap_or_else(|| it.var.clone()))
                    .collect();
            }
        }
    }
    scope
}

// ── WHERE evaluation (CONCEPT:EG-062) ────────────────────────────────────────

fn where_holds(view: &GraphView, binding: &Binding, where_clause: &Option<WhereExpr>) -> bool {
    match where_clause {
        None => true,
        Some(e) => where_expr_holds(view, binding, e),
    }
}

fn where_expr_holds(view: &GraphView, binding: &Binding, e: &WhereExpr) -> bool {
    match e {
        WhereExpr::Or(alts) => alts.iter().any(|a| where_expr_holds(view, binding, a)),
        WhereExpr::And(parts) => parts.iter().all(|p| where_expr_holds(view, binding, p)),
        WhereExpr::Cond(c) => cond_holds(view, binding, c),
    }
}

fn cond_holds(view: &GraphView, binding: &Binding, c: &Condition) -> bool {
    let actual = binding
        .get(&c.var)
        .and_then(|id| node_prop(view, id, &c.prop));
    test_holds(actual.as_ref(), &c.test)
}

fn test_holds(actual: Option<&Value>, test: &Test) -> bool {
    match test {
        Test::Cmp(op, expected) => compare(actual, op, expected),
        Test::In(list) => actual.is_some_and(|a| list.iter().any(|l| l == a)),
        Test::StartsWith(s) => actual
            .and_then(|v| v.as_str())
            .is_some_and(|a| a.starts_with(s.as_str())),
        Test::EndsWith(s) => actual
            .and_then(|v| v.as_str())
            .is_some_and(|a| a.ends_with(s.as_str())),
        Test::Contains(s) => actual
            .and_then(|v| v.as_str())
            .is_some_and(|a| a.contains(s.as_str())),
        // A missing value reads as null, so IS NULL holds.
        Test::IsNull => actual.is_none_or(|v| v.is_null()),
        Test::IsNotNull => actual.is_some_and(|v| !v.is_null()),
    }
}

/// Compare an actual property value against a literal. Equality works across JSON
/// types; ordering works on numbers and strings. A missing value only satisfies `!=`.
fn compare(actual: Option<&Value>, op: &CompareOp, expected: &Value) -> bool {
    let Some(actual) = actual else {
        return matches!(op, CompareOp::Ne);
    };
    match op {
        CompareOp::Eq => actual == expected,
        CompareOp::Ne => actual != expected,
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
            match (as_f64(actual), as_f64(expected)) {
                (Some(a), Some(b)) => apply_ord(op, a.partial_cmp(&b)),
                _ => match (actual.as_str(), expected.as_str()) {
                    (Some(a), Some(b)) => apply_ord(op, Some(a.cmp(b))),
                    _ => false,
                },
            }
        }
    }
}

fn apply_ord(op: &CompareOp, ord: Option<Ordering>) -> bool {
    let Some(ord) = ord else { return false };
    match op {
        CompareOp::Lt => ord == Ordering::Less,
        CompareOp::Le => ord != Ordering::Greater,
        CompareOp::Gt => ord == Ordering::Greater,
        CompareOp::Ge => ord != Ordering::Less,
        _ => false,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

// ── RETURN finalization (CONCEPT:EG-062) ─────────────────────────────────────

/// Project bindings through the RETURN spec: expand `*`, evaluate items (with
/// aggregation + grouping), then `DISTINCT` → `ORDER BY` → `SKIP` → `LIMIT` → encode.
fn finalize(
    view: &GraphView,
    query: &CypherQuery,
    bindings: Vec<Binding>,
) -> Result<QueryResult, String> {
    let ret: &ReturnSpec = &query.ret;
    let items: Vec<ReturnItem> = if ret.star {
        scope_vars(&query.stages)
            .into_iter()
            .map(|v| ReturnItem {
                expr: Expr::Var(v),
                alias: None,
            })
            .collect()
    } else {
        ret.items.clone()
    };
    let columns: Vec<String> = items.iter().map(|i| i.column()).collect();

    // (cells, source-binding). The binding is kept so ORDER BY can reach an
    // un-projected `var.prop`; aggregated rows carry an empty binding.
    let mut rows: Vec<(Vec<Value>, Binding)> = if items.iter().any(|i| is_agg(&i.expr)) {
        aggregate(view, &items, &bindings)
    } else {
        bindings
            .iter()
            .map(|b| {
                let cells = items.iter().map(|i| eval_scalar(view, b, &i.expr)).collect();
                (cells, b.clone())
            })
            .collect()
    };

    if ret.distinct {
        let mut seen: HashSet<String> = HashSet::new();
        rows.retain(|(cells, _)| seen.insert(serde_json::to_string(cells).unwrap_or_default()));
    }

    if !ret.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for key in &ret.order_by {
                let va = order_value(view, &columns, a, &key.expr);
                let vb = order_value(view, &columns, b, &key.expr);
                let mut ord = cmp_values(&va, &vb);
                if key.desc {
                    ord = ord.reverse();
                }
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    let skip = ret.skip.unwrap_or(0);
    let cap = ret.limit.unwrap_or(MAX_ROWS).min(MAX_ROWS);
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (cells, _) in rows.into_iter().skip(skip).take(cap) {
        out.push(rmp_serde::to_vec(&cells).map_err(|e| format!("encode row: {e}"))?);
    }
    Ok(QueryResult { columns, rows: out })
}

fn is_agg(e: &Expr) -> bool {
    matches!(e, Expr::CountStar | Expr::Aggregate(..))
}

/// Evaluate a non-aggregate projection expression for one binding.
fn eval_scalar(view: &GraphView, binding: &Binding, expr: &Expr) -> Value {
    match expr {
        Expr::Var(v) => {
            if let Some(p) = binding.get(&path_key(v)) {
                serde_json::from_str(p).unwrap_or(Value::Null)
            } else if let Some(id) = binding.get(v) {
                Value::String(id.clone())
            } else {
                Value::Null
            }
        }
        Expr::Prop(v, p) => binding
            .get(v)
            .and_then(|id| node_prop(view, id, p))
            .unwrap_or(Value::Null),
        // Aggregates never reach here (the agg path owns them).
        Expr::CountStar | Expr::Aggregate(..) => Value::Null,
    }
}

/// Compute the grouped aggregate rows (CONCEPT:EG-062). The non-aggregate items form
/// the GROUP BY key; with no such items the whole input is one group (so `count(*)`
/// over an empty input still returns a single `0` row).
fn aggregate(
    view: &GraphView,
    items: &[ReturnItem],
    bindings: &[Binding],
) -> Vec<(Vec<Value>, Binding)> {
    let has_keys = items.iter().any(|i| !is_agg(&i.expr));
    if !has_keys {
        let group: Vec<&Binding> = bindings.iter().collect();
        let cells = items
            .iter()
            .map(|i| compute_agg(view, &i.expr, &group))
            .collect();
        return vec![(cells, HashMap::new())];
    }

    // Group by the tuple of non-aggregate cell values, preserving first-seen order.
    let mut groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    for (bi, b) in bindings.iter().enumerate() {
        let key_vals: Vec<Value> = items
            .iter()
            .filter(|i| !is_agg(&i.expr))
            .map(|i| eval_scalar(view, b, &i.expr))
            .collect();
        let key_str = serde_json::to_string(&key_vals).unwrap_or_default();
        match index.get(&key_str) {
            Some(&gi) => groups[gi].1.push(bi),
            None => {
                index.insert(key_str, groups.len());
                groups.push((key_vals, vec![bi]));
            }
        }
    }

    let mut out: Vec<(Vec<Value>, Binding)> = Vec::new();
    for (key_vals, idxs) in groups {
        let group: Vec<&Binding> = idxs.iter().map(|&i| &bindings[i]).collect();
        let mut cells = Vec::with_capacity(items.len());
        let mut ki = 0;
        for item in items {
            if is_agg(&item.expr) {
                cells.push(compute_agg(view, &item.expr, &group));
            } else {
                cells.push(key_vals[ki].clone());
                ki += 1;
            }
        }
        out.push((cells, HashMap::new()));
    }
    out
}

/// Compute one aggregate over a group of bindings (CONCEPT:EG-062).
fn compute_agg(view: &GraphView, expr: &Expr, group: &[&Binding]) -> Value {
    match expr {
        Expr::CountStar => Value::Number(group.len().into()),
        Expr::Aggregate(func, arg) => {
            // Collected non-null argument values.
            let vals: Vec<Value> = group
                .iter()
                .filter_map(|b| arg_value(view, b, arg))
                .filter(|v| !v.is_null())
                .collect();
            match func {
                AggFunc::Count => Value::Number(vals.len().into()),
                AggFunc::Collect => Value::Array(vals),
                AggFunc::Sum => {
                    let sum: f64 = vals.iter().filter_map(as_f64).sum();
                    number_value(sum)
                }
                AggFunc::Avg => {
                    let nums: Vec<f64> = vals.iter().filter_map(as_f64).collect();
                    if nums.is_empty() {
                        Value::Null
                    } else {
                        serde_json::Number::from_f64(nums.iter().sum::<f64>() / nums.len() as f64)
                            .map(Value::Number)
                            .unwrap_or(Value::Null)
                    }
                }
                AggFunc::Min => vals
                    .into_iter()
                    .reduce(|a, b| if cmp_values(&a, &b) == Ordering::Greater { b } else { a })
                    .unwrap_or(Value::Null),
                AggFunc::Max => vals
                    .into_iter()
                    .reduce(|a, b| if cmp_values(&a, &b) == Ordering::Less { b } else { a })
                    .unwrap_or(Value::Null),
            }
        }
        _ => Value::Null,
    }
}

/// The value of an aggregate argument for one binding (`Some` even when null; `None`
/// only when the variable is unbound).
fn arg_value(view: &GraphView, b: &Binding, arg: &AggArg) -> Option<Value> {
    match arg {
        AggArg::Var(v) => {
            if let Some(p) = b.get(&path_key(v)) {
                Some(serde_json::from_str(p).unwrap_or(Value::Null))
            } else {
                b.get(v).map(|id| Value::String(id.clone()))
            }
        }
        AggArg::Prop(v, p) => b.get(v).map(|id| node_prop(view, id, p).unwrap_or(Value::Null)),
    }
}

/// A numeric `Value`: an integer when the float is integral, else a float.
fn number_value(x: f64) -> Value {
    if x.fract() == 0.0 && x.abs() < 9.007e15 {
        Value::Number((x as i64).into())
    } else {
        serde_json::Number::from_f64(x)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

/// The ORDER BY sort value for a row: a projected column when the key names one,
/// else evaluated from the (non-aggregated) source binding.
fn order_value(
    view: &GraphView,
    columns: &[String],
    row: &(Vec<Value>, Binding),
    expr: &Expr,
) -> Value {
    let name = expr.column();
    if let Some(idx) = columns.iter().position(|c| *c == name) {
        return row.0.get(idx).cloned().unwrap_or(Value::Null);
    }
    eval_scalar(view, &row.1, expr)
}

/// A total-ish ordering over JSON values: numbers numerically, strings
/// lexicographically, nulls/mismatches last.
fn cmp_values(a: &Value, b: &Value) -> Ordering {
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x.cmp(y);
    }
    // Push nulls (and type mismatches) to the end.
    let rank = |v: &Value| if v.is_null() { 1 } else { 0 };
    rank(a).cmp(&rank(b))
}

// ── label / property helpers ─────────────────────────────────────────────────

/// Node ids matching a `(var:Label)` — via the same fields the eg-core label index
/// keys on. No label ⇒ every node.
fn label_candidates(view: &GraphView, node: &NodePat) -> Vec<String> {
    match &node.label {
        None => view.node_map.keys().cloned().collect(),
        Some(label) => view
            .node_properties
            .iter()
            .filter(|(_, blob)| node_has_label(blob, label))
            .map(|(id, _)| id.clone())
            .collect(),
    }
}

/// Does the node `id` carry `label`?
fn node_has_label_id(view: &GraphView, id: &str, label: &str) -> bool {
    view.node_properties
        .get(id)
        .is_some_and(|blob| node_has_label(blob, label))
}

/// Does a node's property blob carry `label` on any of `type`/`node_type`/`label`
/// or in the `labels` array — mirroring `GraphCore::build_label_index`.
fn node_has_label(blob: &[u8], label: &str) -> bool {
    let Ok(val) = rmp_serde::from_slice::<Value>(blob) else {
        return false;
    };
    for key in ["type", "node_type", "label"] {
        if val.get(key).and_then(|v| v.as_str()) == Some(label) {
            return true;
        }
    }
    if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
        if arr.iter().any(|x| x.as_str() == Some(label)) {
            return true;
        }
    }
    false
}

/// Read one property from a node's blob.
fn node_prop(view: &GraphView, node_id: &str, prop: &str) -> Option<Value> {
    let blob = view.node_properties.get(node_id)?;
    let val: Value = rmp_serde::from_slice(blob).ok()?;
    val.get(prop).cloned()
}

// ── write path (CONCEPT:EG-020 / EG-061) ─────────────────────────────────────

/// Parse + run a Cypher statement that MAY mutate `core` — `CREATE`/`MERGE`/`SET`/
/// `[DETACH] DELETE`/`REMOVE`, with an optional leading `MATCH … WHERE` and trailing
/// `RETURN` (CONCEPT:EG-020/EG-061). A pure-read query is delegated to the unchanged
/// snapshot read path, so this is the one entry-point a caller needs whether the
/// statement reads or writes. Writes map to eg-core's OWN native ops — NO DataFusion
/// — and `mark_dirty()` is called once after a mutation so caches refresh.
pub fn exec_cypher_write(core: &GraphCore, cypher: &str) -> Result<QueryResult, String> {
    match parser::parse_statement(cypher)? {
        Statement::Read(_) => {
            let view = core.analysis_snapshot();
            exec_cypher(&view, cypher)
        }
        Statement::Write(w) => exec_write(core, &w),
    }
}

/// Execute a parsed write statement against `core` (CONCEPT:EG-020 / EG-061).
fn exec_write(core: &GraphCore, w: &WriteQuery) -> Result<QueryResult, String> {
    // Resolve the leading MATCH (if any) over a snapshot into bindings. No MATCH ⇒
    // one empty binding (the write clauses run exactly once).
    let snap = core.analysis_snapshot();
    let mut bindings: Vec<Binding> = match &w.match_pattern {
        Some(pattern) => resolve_match(&snap, pattern, &w.where_clause, &HashMap::new())?,
        None => vec![HashMap::new()],
    };

    // Enrich bindings with edge variables (`-[r:REL]->`) so `DELETE r` can resolve the
    // edge endpoints. For each named-edge hop, map `@edge@r -> "src\0tgt"`.
    if let Some(pattern) = &w.match_pattern {
        for binding in bindings.iter_mut() {
            let mut prev_var = node_var(&pattern.start, 0);
            for (i, (edge, node)) in pattern.hops.iter().enumerate() {
                let next_var = node_var(node, i + 1);
                if let Some(evar) = &edge.var {
                    if let (Some(a), Some(b)) = (
                        binding.get(&prev_var).cloned(),
                        binding.get(&next_var).cloned(),
                    ) {
                        let (src, tgt) = match edge.direction {
                            Direction::Right => (a, b),
                            Direction::Left => (b, a),
                        };
                        binding.insert(edge_key(evar), format!("{src}\u{0}{tgt}"));
                    }
                }
                prev_var = next_var;
            }
        }
    }

    let mut mutated = false;
    for binding in bindings.iter_mut() {
        for op in &w.ops {
            apply_write_op(core, &snap, binding, op, &mut mutated)?;
        }
    }
    if mutated {
        core.mark_dirty();
    }

    if w.returns.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    }
    let post = core.analysis_snapshot();
    project_write(&post, &bindings, &w.returns)
}

/// Apply ONE write clause for a single binding, extending the binding with any
/// newly created/merged variables (CONCEPT:EG-020 / EG-061).
fn apply_write_op(
    core: &GraphCore,
    snap: &GraphView,
    binding: &mut Binding,
    op: &WriteOp,
    mutated: &mut bool,
) -> Result<(), String> {
    match op {
        WriteOp::Create(pattern) => apply_create(core, binding, pattern, mutated)?,
        WriteOp::Merge(node) => apply_merge(core, binding, node, mutated)?,
        WriteOp::Set(items) => apply_set(core, binding, items, mutated)?,
        WriteOp::Delete { vars, detach } => {
            apply_delete(core, snap, binding, vars, *detach, mutated)?
        }
        WriteOp::Remove(items) => apply_remove(core, binding, items, mutated)?,
    }
    Ok(())
}

/// `CREATE <pattern>`: realize each node (reuse a bound var, else create) and each
/// hop's edge (CONCEPT:EG-020).
fn apply_create(
    core: &GraphCore,
    binding: &mut Binding,
    pattern: &Pattern,
    mutated: &mut bool,
) -> Result<(), String> {
    let start_id = realize_node(core, binding, &pattern.start, mutated)?;
    let mut prev_id = start_id;
    for (edge, node) in &pattern.hops {
        let next_id = realize_node(core, binding, node, mutated)?;
        let (src, tgt) = match edge.direction {
            Direction::Right => (prev_id.clone(), next_id.clone()),
            Direction::Left => (next_id.clone(), prev_id.clone()),
        };
        let mut props = props_to_map(edge.props.as_deref());
        if let Some(rel) = &edge.rel_type {
            props.insert("relationship".into(), Value::String(rel.clone()));
        }
        let blob = rmp_serde::to_vec_named(&Value::Object(props))
            .map_err(|e| format!("encode edge props: {e}"))?;
        core.add_edge(src, tgt, blob)
            .map_err(|e| format!("CREATE edge: {e}"))?;
        *mutated = true;
        prev_id = next_id;
    }
    Ok(())
}

/// Resolve a CREATE node position to an id: reuse a bound variable, else create a new
/// node carrying its label (`type`) + inline props (CONCEPT:EG-020).
fn realize_node(
    core: &GraphCore,
    binding: &mut Binding,
    node: &NodePat,
    mutated: &mut bool,
) -> Result<String, String> {
    if let Some(var) = &node.var {
        if let Some(existing) = binding.get(var) {
            return Ok(existing.clone());
        }
    }
    let mut props = props_to_map(node.props.as_deref());
    if let Some(label) = &node.label {
        props
            .entry("type".to_string())
            .or_insert_with(|| Value::String(label.clone()));
    }
    let id = match props.get("id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => gen_node_id(),
    };
    let blob =
        rmp_serde::to_vec_named(&Value::Object(props)).map_err(|e| format!("encode node: {e}"))?;
    core.add_node(id.clone(), blob);
    *mutated = true;
    if let Some(var) = &node.var {
        binding.insert(var.clone(), id.clone());
    }
    Ok(id)
}

/// `MERGE (n:Label {props})`: match a node by label + ALL inline props; create iff
/// absent. Idempotent. Binds `n` (CONCEPT:EG-020).
fn apply_merge(
    core: &GraphCore,
    binding: &mut Binding,
    node: &NodePat,
    mutated: &mut bool,
) -> Result<(), String> {
    let want = props_to_map(node.props.as_deref());
    let candidates: Vec<(String, Vec<u8>)> = match &node.label {
        Some(label) => core.get_nodes_by_label(label, 0),
        None => core.get_nodes(),
    };
    for (id, blob) in &candidates {
        let Ok(Value::Object(obj)) = rmp_serde::from_slice::<Value>(blob) else {
            continue;
        };
        if want.iter().all(|(k, v)| obj.get(k) == Some(v)) {
            if let Some(var) = &node.var {
                binding.insert(var.clone(), id.clone());
            }
            return Ok(());
        }
    }
    realize_node(core, binding, node, mutated)?;
    Ok(())
}

/// `SET v.prop = literal [, …]`: merge each assignment onto the bound node via the
/// engine's atomic `compare_and_set_fields` (CONCEPT:EG-020).
fn apply_set(
    core: &GraphCore,
    binding: &Binding,
    items: &[SetItem],
    mutated: &mut bool,
) -> Result<(), String> {
    let mut by_var: HashMap<&str, serde_json::Map<String, Value>> = HashMap::new();
    for it in items {
        by_var
            .entry(it.var.as_str())
            .or_default()
            .insert(it.prop.clone(), it.value.clone());
    }
    for (var, updates) in by_var {
        let id = binding
            .get(var)
            .ok_or_else(|| format!("SET refers to unbound variable `{var}`"))?;
        let conditions = serde_json::Map::new();
        if core.compare_and_set_fields(id, &conditions, &updates) {
            *mutated = true;
        }
    }
    Ok(())
}

/// `REMOVE v.prop | v:Label [, …]`: delete a property or remove a label from the
/// bound node (CONCEPT:EG-061). A read-modify-write over the engine's field map: read
/// the node blob, drop the field / label, write the blob back via `add_node` (which
/// replaces an existing node's properties in place).
fn apply_remove(
    core: &GraphCore,
    binding: &Binding,
    items: &[RemoveItem],
    mutated: &mut bool,
) -> Result<(), String> {
    // Group removals per target variable so each node is rewritten once.
    let mut props_by_var: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut labels_by_var: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut order: Vec<&str> = Vec::new();
    for it in items {
        let var = match it {
            RemoveItem::Property { var, .. } | RemoveItem::Label { var, .. } => var.as_str(),
        };
        if !order.contains(&var) {
            order.push(var);
        }
        match it {
            RemoveItem::Property { prop, .. } => {
                props_by_var.entry(var).or_default().push(prop.as_str())
            }
            RemoveItem::Label { label, .. } => {
                labels_by_var.entry(var).or_default().push(label.as_str())
            }
        }
    }

    for var in order {
        let id = binding
            .get(var)
            .ok_or_else(|| format!("REMOVE refers to unbound variable `{var}`"))?;
        let blob = core
            .get_node_properties(id)
            .ok_or_else(|| format!("REMOVE on absent node `{var}` ({id})"))?;
        let mut val: Value =
            rmp_serde::from_slice(&blob).map_err(|e| format!("decode node `{id}`: {e}"))?;
        let Some(obj) = val.as_object_mut() else {
            continue;
        };
        let mut changed = false;
        for prop in props_by_var.get(var).into_iter().flatten() {
            if obj.remove(*prop).is_some() {
                changed = true;
            }
        }
        for label in labels_by_var.get(var).into_iter().flatten() {
            if remove_label(obj, label) {
                changed = true;
            }
        }
        if changed {
            let reenc =
                rmp_serde::to_vec_named(&val).map_err(|e| format!("encode node `{id}`: {e}"))?;
            core.add_node(id.clone(), reenc);
            *mutated = true;
        }
    }
    Ok(())
}

/// Remove `label` from a node's property object, mirroring the label-index fields
/// (`type`/`node_type`/`label` scalar, or membership in the `labels` array). Returns
/// whether anything changed (CONCEPT:EG-061).
fn remove_label(obj: &mut serde_json::Map<String, Value>, label: &str) -> bool {
    let mut changed = false;
    for key in ["type", "node_type", "label"] {
        if obj.get(key).and_then(|v| v.as_str()) == Some(label) {
            obj.remove(key);
            changed = true;
        }
    }
    if let Some(Value::Array(arr)) = obj.get_mut("labels") {
        let before = arr.len();
        arr.retain(|x| x.as_str() != Some(label));
        if arr.len() != before {
            changed = true;
        }
    }
    changed
}

/// `[DETACH] DELETE v [, …]`: remove bound node variables (DETACH also drops their
/// incident edges), or a bound edge variable's edge (CONCEPT:EG-020).
fn apply_delete(
    core: &GraphCore,
    snap: &GraphView,
    binding: &Binding,
    vars: &[String],
    detach: bool,
    mutated: &mut bool,
) -> Result<(), String> {
    for var in vars {
        if let Some(edge) = binding.get(&edge_key(var)) {
            if let Some((src, tgt)) = edge.split_once('\u{0}') {
                core.remove_edge(src.to_string(), tgt.to_string());
                *mutated = true;
                continue;
            }
        }
        let id = binding
            .get(var)
            .ok_or_else(|| format!("DELETE refers to unbound variable `{var}`"))?;
        if !detach && node_has_incident_edge(snap, id) {
            return Err(format!(
                "DELETE on node `{var}` ({id}) which still has relationships — use DETACH DELETE"
            ));
        }
        core.remove_node(id.clone());
        *mutated = true;
    }
    Ok(())
}

/// Project bindings into a [`QueryResult`] for a WRITE `RETURN` — simple per-row
/// scalar projection (no aggregation/ORDER BY).
fn project_write(
    view: &GraphView,
    bindings: &[Binding],
    returns: &[ReturnItem],
) -> Result<QueryResult, String> {
    let columns: Vec<String> = returns.iter().map(|r| r.column()).collect();
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for binding in bindings.iter().take(MAX_ROWS) {
        let cells: Vec<Value> = returns
            .iter()
            .map(|item| eval_scalar(view, binding, &item.expr))
            .collect();
        rows.push(rmp_serde::to_vec(&cells).map_err(|e| format!("encode row: {e}"))?);
    }
    Ok(QueryResult { columns, rows })
}

/// The binding key an edge variable is stored under.
fn edge_key(var: &str) -> String {
    format!("@edge@{var}")
}

/// Does `id` have any incident edge in the snapshot (for the non-DETACH DELETE guard)?
fn node_has_incident_edge(view: &GraphView, id: &str) -> bool {
    let Some(&idx) = view.node_map.get(id) else {
        return false;
    };
    view.graph
        .edges_directed(idx, petgraph::Direction::Outgoing)
        .next()
        .is_some()
        || view
            .graph
            .edges_directed(idx, petgraph::Direction::Incoming)
            .next()
            .is_some()
}

/// A fresh, process-unique node id for an un-pinned CREATE/MERGE node.
fn gen_node_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, AtomicOrdering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("cy_{nanos:x}_{n:x}")
}

/// A Cypher inline-property list → a JSON object map.
fn props_to_map(props: Option<&[(String, Value)]>) -> serde_json::Map<String, Value> {
    let mut m = serde_json::Map::new();
    if let Some(list) = props {
        for (k, v) in list {
            m.insert(k.clone(), v.clone());
        }
    }
    m
}

/// The variable name for a node position, auto-naming anonymous nodes so the write
/// path's edge enrichment can address each position.
fn node_var(node: &NodePat, pos: usize) -> String {
    node.var.clone().unwrap_or_else(|| format!("__anon{pos}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::{vf2_match_views, GraphCore};

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Build a small fixture: 3 People (alice, bob, carol), alice-KNOWS->bob,
    /// bob-KNOWS->carol, plus a Doc node. Returns an off-lock snapshot view.
    fn fixture() -> GraphView {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(serde_json::json!({"type":"Person","name":"Alice"})),
        );
        core.add_node(
            "bob".into(),
            pbytes(serde_json::json!({"type":"Person","name":"Bob"})),
        );
        core.add_node(
            "carol".into(),
            pbytes(serde_json::json!({"type":"Person","name":"Carol"})),
        );
        core.add_node(
            "d1".into(),
            pbytes(serde_json::json!({"type":"Doc","size":42})),
        );
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(serde_json::json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.add_edge(
            "bob".into(),
            "carol".into(),
            pbytes(serde_json::json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.analysis_snapshot()
    }

    fn ids(qr: &QueryResult, col: usize) -> Vec<String> {
        let mut out: Vec<String> = qr
            .rows
            .iter()
            .map(|b| {
                let cells: Vec<Value> = rmp_serde::from_slice(b).unwrap();
                cells[col].as_str().unwrap().to_string()
            })
            .collect();
        out.sort();
        out
    }

    fn cells_of(qr: &QueryResult, row: usize) -> Vec<Value> {
        rmp_serde::from_slice(&qr.rows[row]).unwrap()
    }

    #[test]
    fn match_label_returns_label_index_set() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN a").unwrap();
        assert_eq!(qr.columns, vec!["a"]);
        assert_eq!(ids(&qr, 0), vec!["alice", "bob", "carol"]);
    }

    #[test]
    fn match_with_where_filters() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a").unwrap();
        assert_eq!(ids(&qr, 0), vec!["alice"]);
    }

    #[test]
    fn two_hop_pattern_matches_vf2_expectation() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let pairs: HashSet<(String, String)> = qr
            .rows
            .iter()
            .map(|b| {
                let c: Vec<Value> = rmp_serde::from_slice(b).unwrap();
                (
                    c[0].as_str().unwrap().to_string(),
                    c[1].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let expected: HashSet<(String, String)> = [
            ("alice".to_string(), "bob".to_string()),
            ("bob".to_string(), "carol".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(pairs, expected);

        // Cross-check the same edges via the engine's own VF2.
        let mut pat = GraphView::default();
        for n in ["pa", "pb"] {
            let i = pat.graph.add_node(n.to_string());
            pat.node_map.insert(n.to_string(), i);
            pat.node_properties.insert(
                n.to_string(),
                std::sync::Arc::new(pbytes(serde_json::json!({}))),
            );
        }
        pat.graph
            .add_edge(pat.node_map["pa"], pat.node_map["pb"], "pa:pb".into());
        pat.edge_properties.insert(
            ("pa".into(), "pb".into()),
            vec![std::sync::Arc::new(pbytes(
                serde_json::json!({"relationship":"KNOWS"}),
            ))],
        );
        let vf2 = vf2_match_views(&v, &pat);
        assert_eq!(vf2.len(), 2);
    }

    #[test]
    fn variable_length_path_reaches_transitively() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["bob", "carol"]);

        let qr1 = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr1, 0), vec!["bob"]);
    }

    #[test]
    fn return_property_projection() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Doc) RETURN a.size").unwrap();
        assert_eq!(qr.columns, vec!["a.size"]);
        assert_eq!(cells_of(&qr, 0)[0], Value::Number(42.into()));
    }

    #[test]
    fn where_numeric_comparison() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Doc) WHERE a.size > 10 RETURN a").unwrap();
        assert_eq!(ids(&qr, 0), vec!["d1"]);
        let none = exec_cypher(&v, "MATCH (a:Doc) WHERE a.size > 100 RETURN a").unwrap();
        assert!(none.rows.is_empty());
    }

    #[test]
    fn limit_caps_rows() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN a LIMIT 2").unwrap();
        assert_eq!(qr.rows.len(), 2);
    }

    // ── read clauses (CONCEPT:EG-062) ──────────────────────────────────────────

    #[test]
    fn order_by_skip_limit() {
        let v = fixture();
        // Names ascending: Alice, Bob, Carol → SKIP 1 LIMIT 1 ⇒ Bob.
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person) RETURN a.name ORDER BY a.name SKIP 1 LIMIT 1",
        )
        .unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(cells_of(&qr, 0)[0], Value::String("Bob".into()));

        // DESC: Carol, Bob, Alice → first row is Carol.
        let qr2 = exec_cypher(&v, "MATCH (a:Person) RETURN a.name ORDER BY a.name DESC").unwrap();
        assert_eq!(cells_of(&qr2, 0)[0], Value::String("Carol".into()));
    }

    #[test]
    fn with_pipelining_filters_then_returns() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b WHERE b.name = 'Carol' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["carol"]);
    }

    #[test]
    fn optional_match_yields_null_rows() {
        let v = fixture();
        // carol has no outgoing KNOWS → b is null on that row.
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a, b",
        )
        .unwrap();
        let mut by_a: HashMap<String, Value> = HashMap::new();
        for r in 0..qr.rows.len() {
            let c = cells_of(&qr, r);
            by_a.insert(c[0].as_str().unwrap().to_string(), c[1].clone());
        }
        assert_eq!(by_a["alice"], Value::String("bob".into()));
        assert_eq!(by_a["bob"], Value::String("carol".into()));
        assert_eq!(by_a["carol"], Value::Null);
    }

    #[test]
    fn where_or_in_starts_with() {
        let v = fixture();
        // STARTS WITH 'A' → Alice; OR name IN ['Carol'] → Carol. Not Bob.
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person) WHERE a.name STARTS WITH 'A' OR a.name IN ['Carol'] RETURN a",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["alice", "carol"]);

        let qr2 = exec_cypher(
            &v,
            "MATCH (a:Person) WHERE a.name CONTAINS 'o' RETURN a",
        )
        .unwrap();
        // 'Bob' and 'Carol' contain 'o'.
        assert_eq!(ids(&qr2, 0), vec!["bob", "carol"]);
    }

    #[test]
    fn aggregation_count_and_collect() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN count(*)").unwrap();
        assert_eq!(qr.columns, vec!["count(*)"]);
        assert_eq!(cells_of(&qr, 0)[0], Value::Number(3.into()));

        let qr2 = exec_cypher(&v, "MATCH (a:Person) RETURN count(a), collect(a.name)").unwrap();
        let c = cells_of(&qr2, 0);
        assert_eq!(c[0], Value::Number(3.into()));
        let names = c[1].as_array().unwrap();
        assert_eq!(names.len(), 3);

        // Grouped aggregation + sum.
        let qr3 = exec_cypher(&v, "MATCH (a:Doc) RETURN a.type, sum(a.size)").unwrap();
        let c3 = cells_of(&qr3, 0);
        assert_eq!(c3[0], Value::String("Doc".into()));
        assert_eq!(c3[1], Value::Number(42.into()));
    }

    #[test]
    fn distinct_dedups_rows() {
        let v = fixture();
        // Every Person has type 'Person' → DISTINCT collapses to one row.
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN DISTINCT a.type").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(cells_of(&qr, 0)[0], Value::String("Person".into()));
    }

    #[test]
    fn return_star_projects_scope() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) WHERE a.name = 'Alice' RETURN *").unwrap();
        assert_eq!(qr.columns, vec!["a"]);
        assert_eq!(cells_of(&qr, 0)[0], Value::String("alice".into()));
    }

    // ── var-length generalization (CONCEPT:EG-063) ─────────────────────────────

    #[test]
    fn mixed_fixed_and_var_length_pattern() {
        // chain: a -KNOWS-> b -KNOWS*1..2-> x, anchored at alice.
        // alice-KNOWS->bob (fixed), then bob-KNOWS*1..2->{carol}. So x = carol.
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS*1..2]->(x:Person) \
             WHERE a.name = 'Alice' RETURN x",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["carol"]);
    }

    #[test]
    fn path_variable_binds_node_sequence() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH p = (a:Person)-[:KNOWS*1..1]->(b:Person) WHERE a.name = 'Alice' RETURN p",
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["p"]);
        let path = cells_of(&qr, 0)[0].as_array().unwrap().clone();
        let seq: Vec<&str> = path.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(seq, vec!["alice", "bob"]);
    }

    // ── write path (CONCEPT:EG-020 / EG-061) ───────────────────────────────────

    fn col0(qr: &QueryResult) -> Vec<String> {
        let mut out: Vec<String> = qr
            .rows
            .iter()
            .map(|b| {
                let cells: Vec<Value> = rmp_serde::from_slice(b).unwrap();
                cells[0].as_str().unwrap().to_string()
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn create_then_match_sees_it() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'alice', name: 'Alice'})").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (a:Person) RETURN a").unwrap();
        assert_eq!(col0(&qr), vec!["alice"]);
        let qr2 = exec_cypher_write(&core, "MATCH (a:Person) RETURN a.name").unwrap();
        assert_eq!(cells_of(&qr2, 0)[0], Value::String("Alice".into()));
    }

    #[test]
    fn create_edge_between_new_nodes() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "CREATE (a:Person {id: 'a'})-[:KNOWS]->(b:Person {id: 'b'})",
        )
        .unwrap();
        let qr =
            exec_cypher_write(&core, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        let cells = cells_of(&qr, 0);
        assert_eq!(cells[0], Value::String("a".into()));
        assert_eq!(cells[1], Value::String("b".into()));
    }

    #[test]
    fn merge_is_idempotent() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "MERGE (n:City {id: 'paris', name: 'Paris'})").unwrap();
        exec_cypher_write(&core, "MERGE (n:City {id: 'paris', name: 'Paris'})").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (c:City) RETURN c").unwrap();
        assert_eq!(col0(&qr), vec!["paris"]);
    }

    #[test]
    fn set_updates_property() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'bob', rank: 1})").unwrap();
        exec_cypher_write(&core, "MATCH (n:Person) WHERE n.id = 'bob' SET n.rank = 9").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n.rank").unwrap();
        assert_eq!(cells_of(&qr, 0)[0], Value::Number(9.into()));
    }

    #[test]
    fn delete_removes_node() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'carol'})").unwrap();
        exec_cypher_write(&core, "MATCH (n:Person) WHERE n.id = 'carol' DELETE n").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert!(qr.rows.is_empty());
    }

    #[test]
    fn detach_delete_drops_edges_then_node() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "CREATE (a:Person {id: 'x'})-[:KNOWS]->(b:Person {id: 'y'})",
        )
        .unwrap();
        let err =
            exec_cypher_write(&core, "MATCH (a:Person) WHERE a.id = 'x' DELETE a").unwrap_err();
        assert!(err.contains("DETACH"), "{err}");
        exec_cypher_write(&core, "MATCH (a:Person) WHERE a.id = 'x' DETACH DELETE a").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(col0(&qr), vec!["y"]);
    }

    #[test]
    fn delete_edge_by_variable() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "CREATE (a:Person {id: 'p'})-[:KNOWS]->(b:Person {id: 'q'})",
        )
        .unwrap();
        exec_cypher_write(
            &core,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = 'p' DELETE r",
        )
        .unwrap();
        let qr =
            exec_cypher_write(&core, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a").unwrap();
        assert!(qr.rows.is_empty());
        let nodes = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(col0(&nodes), vec!["p", "q"]);
    }

    #[test]
    fn create_with_return_projects_new_node() {
        let core = GraphCore::new();
        let qr = exec_cypher_write(
            &core,
            "CREATE (n:Task {id: 't1', state: 'open'}) RETURN n.state",
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["n.state"]);
        assert_eq!(cells_of(&qr, 0)[0], Value::String("open".into()));
    }

    // ── REMOVE (CONCEPT:EG-061) ─────────────────────────────────────────────────

    #[test]
    fn remove_property_deletes_field() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'bob', age: 40, name: 'Bob'})").unwrap();
        exec_cypher_write(&core, "MATCH (n:Person) WHERE n.id = 'bob' REMOVE n.age").unwrap();
        // age is gone (null); name survives.
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n.age, n.name").unwrap();
        let c = cells_of(&qr, 0);
        assert_eq!(c[0], Value::Null);
        assert_eq!(c[1], Value::String("Bob".into()));
    }

    #[test]
    fn remove_label_drops_from_label_index() {
        let core = GraphCore::new();
        // A node that is both Person (type) and carries Admin in a labels array.
        exec_cypher_write(
            &core,
            "CREATE (n:Person {id: 'al', name: 'Al'})",
        )
        .unwrap();
        // Give it a secondary label via SET (labels array), then REMOVE it.
        // (SET only takes literals; build the labels array through a fresh create.)
        let core2 = GraphCore::new();
        core2.add_node(
            "al".into(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "Person", "labels": ["Admin"], "id": "al"
            }))
            .unwrap(),
        );
        // Remove the type label → no longer a Person.
        exec_cypher_write(&core2, "MATCH (n:Person) WHERE n.id = 'al' REMOVE n:Person").unwrap();
        let persons = exec_cypher_write(&core2, "MATCH (n:Person) RETURN n").unwrap();
        assert!(persons.rows.is_empty(), "type label removed");
        // Remove the array label → no longer an Admin.
        exec_cypher_write(&core2, "MATCH (n:Admin) WHERE n.id = 'al' REMOVE n:Admin").unwrap();
        let admins = exec_cypher_write(&core2, "MATCH (n:Admin) RETURN n").unwrap();
        assert!(admins.rows.is_empty(), "array label removed");
        let _ = core; // silence unused in this combined test
    }
}
