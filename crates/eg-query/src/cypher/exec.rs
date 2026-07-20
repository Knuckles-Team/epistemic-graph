//! Cypher execution (CONCEPT:EG-KG.query.dep-free-behind). Runs a parsed [`CypherQuery`] over an
//! off-lock `GraphView` and materializes a `QueryResult` — the SAME carrier the
//! SQL surface uses. NO DataFusion: every pattern shape compiles to one of the
//! engine's own primitives.
//!
//! Strategy:
//!   * a node's `:Label` predicate            → canonical `node_type` plus the
//!     explicit multi-label `labels` array,
//!     resolved here directly off the `GraphView`;
//!   * a linear MATCH path                     → an incremental neighbour-walk
//!     (`resolve_match`): start from the label-index candidates, then extend hop by
//!     hop. A FIXED hop extends to relationship-typed neighbours; a VARIABLE-length
//!     hop (`*min..max`) extends via petgraph BFS. Fixed and variable-length hops
//!     freely combine in one pattern (CONCEPT:EG-KG.query.concept-2), and an already-bound node var
//!     anchors its position — which is also how `OPTIONAL MATCH` / `WITH`
//!     pipelining join onto prior bindings (CONCEPT:EG-KG.query.eg-extend-read-side).
//!
//! Read clauses (CONCEPT:EG-KG.query.eg-extend-read-side): a read query is a pipeline of reading stages
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
    AggArg, AggFunc, CompareOp, Condition, CypherQuery, Direction, EdgePat, Expr, ListExpr,
    NodePat, Pattern, PropVal, QuantifiedGroup, ReadStage, RemoveItem, ReturnItem, ReturnSpec,
    SetItem, Statement, Test, WhereExpr, WithItem, WriteOp, WriteQuery, YieldItem,
};
use super::proc::{registry, YieldValue};

/// Implicit max rows (mirrors the SQL surface): one Response per Request, so an
/// unbounded RETURN would buffer the whole result in one message.
const MAX_ROWS: usize = 50_000;

/// Query parameters (`$name` → JSON value), supplied by the caller (CONCEPT:EG-KG.query.param-list-drives-unwind).
pub type Params = serde_json::Map<String, Value>;

/// A var→node-id binding row. A path variable (CONCEPT:EG-KG.query.concept-2) is stored under the
/// `@path@<var>` key as a JSON-array string of the node ids along the path; an edge
/// variable (write path) under `@edge@<var>` as `src\0tgt`; a SCALAR value bound by
/// `UNWIND`/`CALL`/`YIELD` (CONCEPT:EG-KG.query.param-list-drives-unwind/142) under `@val@<var>` as a JSON string.
/// Cypher 25 group variables use `@qpp-node@<var>` / `@qpp-edge@<var>` JSON arrays
/// so their ordered per-repetition values cannot be confused with singleton
/// variables in the surrounding scope.
type Binding = HashMap<String, String>;

/// Parse + run `cypher` over `view` (read-only, single graph). Synchronous and
/// dep-free — safe to call inside `spawn_blocking` like `exec_sql`.
pub fn exec_cypher(view: &GraphView, cypher: &str) -> Result<QueryResult, String> {
    exec_cypher_params(view, cypher, &Params::new())
}

/// Parse + run `cypher` over `view` with `$name` query parameters (CONCEPT:EG-KG.query.param-list-drives-unwind) —
/// e.g. `UNWIND $ids AS x MATCH (n {id: x}) RETURN n`. `exec_cypher` is the
/// zero-parameter form.
pub fn exec_cypher_params(
    view: &GraphView,
    cypher: &str,
    params: &Params,
) -> Result<QueryResult, String> {
    let query = parser::parse(cypher)?;
    let bindings = run_stages(view, &query.stages, params)?;
    finalize(view, &query, bindings)
}

// ── read-stage pipeline (CONCEPT:EG-KG.query.eg-extend-read-side) ─────────────────────────────────────

/// Run the reading-stage pipeline, threading bindings from one stage to the next.
fn run_stages(
    view: &GraphView,
    stages: &[ReadStage],
    params: &Params,
) -> Result<Vec<Binding>, String> {
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
                    let mut matched = resolve_match(view, pattern, where_clause, incoming, params)?;
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
            ReadStage::Unwind { list, var } => {
                let mut out: Vec<Binding> = Vec::new();
                for b in &bindings {
                    for elem in eval_list(b, params, list)? {
                        let mut nb = b.clone();
                        nb.insert(
                            val_key(var),
                            serde_json::to_string(&elem).unwrap_or_default(),
                        );
                        out.push(nb);
                    }
                }
                bindings = out;
            }
            ReadStage::Call { subquery } => {
                let additions = subquery_additions(view, subquery, params)?;
                let mut out: Vec<Binding> = Vec::new();
                for b in &bindings {
                    for add in &additions {
                        let mut nb = b.clone();
                        nb.extend(add.clone());
                        out.push(nb);
                    }
                }
                bindings = out;
            }
            ReadStage::CallProc { name, args, yields } => {
                bindings = run_call_proc(view, &bindings, name, args, yields, params)?;
            }
        }
    }
    Ok(bindings)
}

/// The binding key a scalar value (from UNWIND/CALL/YIELD) is stored under.
fn val_key(var: &str) -> String {
    format!("@val@{var}")
}

/// Binding sidecar for a quantified-path node group variable.
fn qpp_node_key(var: &str) -> String {
    format!("@qpp-node@{var}")
}

/// Binding sidecar for a quantified-path relationship group variable.
fn qpp_edge_key(var: &str) -> String {
    format!("@qpp-edge@{var}")
}

/// Decode one internal JSON string-list sidecar. Internal bindings are produced
/// by this module, so a malformed value is treated as an empty accumulator rather
/// than exposed to callers as parser state.
fn binding_list(binding: &Binding, key: &str) -> Vec<String> {
    binding
        .get(key)
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default()
}

fn append_binding_list(binding: &mut Binding, key: String, value: String) {
    let mut values = binding_list(binding, &key);
    values.push(value);
    binding.insert(key, serde_json::to_string(&values).unwrap_or_default());
}

/// Evaluate an UNWIND list operand into its element values (CONCEPT:EG-KG.query.param-list-drives-unwind).
fn eval_list(b: &Binding, params: &Params, list: &ListExpr) -> Result<Vec<Value>, String> {
    match list {
        ListExpr::List(items) => items
            .iter()
            .map(|pv| resolve_prop_val(b, params, pv))
            .collect(),
        ListExpr::Param(name) => match params.get(name) {
            Some(Value::Array(a)) => Ok(a.clone()),
            Some(other) => Err(format!("UNWIND $ {name} expects a list, found {other}")),
            None => Err(format!("UNWIND references undefined parameter ${name}")),
        },
        ListExpr::Ref(var) => match bound_value(b, var) {
            Some(Value::Array(a)) => Ok(a),
            Some(_) | None => Ok(Vec::new()),
        },
    }
}

/// Resolve a [`PropVal`] to a JSON value against the live params + binding
/// (CONCEPT:EG-KG.query.param-list-drives-unwind): a literal as-is, a `$param` from `params`, a `Ref` from the
/// binding (its scalar `@val@` value, else its bound node id as a string).
fn resolve_prop_val(b: &Binding, params: &Params, pv: &PropVal) -> Result<Value, String> {
    match pv {
        PropVal::Lit(v) => Ok(v.clone()),
        PropVal::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| format!("undefined parameter ${name}")),
        PropVal::Ref(var) => Ok(bound_value(b, var).unwrap_or(Value::Null)),
    }
}

/// The value a bound variable carries while resolving expressions: its `@val@`
/// scalar (decoded), else its node id as a string, else `None`.
///
/// This is deliberately an internal binding value, not the public projection of a
/// node. A bare node expression is materialized by [`eval_scalar`] as the canonical
/// node map returned on the Cypher wire.
fn bound_value(b: &Binding, var: &str) -> Option<Value> {
    if let Some(ids) = b.get(&qpp_node_key(var)) {
        serde_json::from_str(ids).ok()
    } else if let Some(edges) = b.get(&qpp_edge_key(var)) {
        serde_json::from_str(edges).ok()
    } else if let Some(s) = b.get(&val_key(var)) {
        Some(serde_json::from_str(s).unwrap_or(Value::Null))
    } else {
        b.get(var).map(|id| Value::String(id.clone()))
    }
}

/// Run a `CALL { subquery }` (CONCEPT:EG-KG.query.cypher-planning) and produce the per-result-row binding
/// additions to merge (cartesian) onto each outer row. Node-valued RETURN vars stay
/// anchorable node bindings; scalar/property/aggregate columns become `@val@`
/// sidecars keyed by the projected column name.
fn subquery_additions(
    view: &GraphView,
    subquery: &CypherQuery,
    params: &Params,
) -> Result<Vec<Binding>, String> {
    let sub_bindings = run_stages(view, &subquery.stages, params)?;
    let ret = &subquery.ret;
    let items: Vec<ReturnItem> = if ret.star {
        scope_vars(&subquery.stages)
            .into_iter()
            .map(|v| ReturnItem {
                expr: Expr::Var(v),
                alias: None,
            })
            .collect()
    } else {
        ret.items.clone()
    };

    // An aggregating subquery collapses rows; run the full RETURN and expose each
    // resulting column as a scalar sidecar.
    if items.iter().any(|i| is_agg(&i.expr)) {
        let qr = finalize(view, subquery, sub_bindings)?;
        let mut out = Vec::new();
        for row in &qr.rows {
            let cells: Vec<Value> = eg_types::msgpack::decode_bounded(
                row,
                eg_types::msgpack::MsgpackLimits::new(
                    eg_types::msgpack::MAX_PROPERTY_BYTES,
                    eg_types::msgpack::MAX_PROPERTY_ITEMS,
                    eg_types::msgpack::DEFAULT_MAX_DEPTH,
                ),
            )
            .map_err(|_| "decode subquery row failed".to_string())?;
            let mut add = Binding::new();
            for (i, col) in qr.columns.iter().enumerate() {
                let v = cells.get(i).cloned().unwrap_or(Value::Null);
                add.insert(val_key(col), serde_json::to_string(&v).unwrap_or_default());
            }
            out.push(add);
        }
        return Ok(out);
    }

    // Non-aggregating: one addition per sub-binding, preserving each var's kind.
    let mut out = Vec::new();
    for b in &sub_bindings {
        let mut add = Binding::new();
        for it in &items {
            let name = it.column();
            match &it.expr {
                Expr::Var(v) => {
                    if let Some(p) = b.get(&path_key(v)) {
                        add.insert(path_key(&name), p.clone());
                    } else if let Some(s) = b.get(&val_key(v)) {
                        add.insert(val_key(&name), s.clone());
                    } else if let Some(id) = b.get(v) {
                        add.insert(name.clone(), id.clone());
                    }
                }
                Expr::Prop(v, p) => {
                    let val = b
                        .get(v)
                        .and_then(|id| node_prop(view, id, p))
                        .unwrap_or(Value::Null);
                    add.insert(
                        val_key(&name),
                        serde_json::to_string(&val).unwrap_or_default(),
                    );
                }
                _ => {}
            }
        }
        out.push(add);
    }
    Ok(out)
}

/// Run a `CALL proc.name(args) YIELD …` stage (CONCEPT:EG-KG.query.cypher-planning): resolve the args,
/// consult the procedure registry, and bind each yielded column onto every incoming
/// row × procedure result row. A `Node` yield binds an anchorable node id; a `Scalar`
/// yield binds a `@val@` sidecar under the (aliased) column name.
fn run_call_proc(
    view: &GraphView,
    bindings: &[Binding],
    name: &str,
    args: &[PropVal],
    yields: &[YieldItem],
    params: &Params,
) -> Result<Vec<Binding>, String> {
    let reg = registry();
    let proc = reg
        .get(&name.to_ascii_lowercase())
        .ok_or_else(|| format!("unknown procedure `{name}`"))?;
    let mut out = Vec::new();
    for b in bindings {
        let argv: Vec<Value> = args
            .iter()
            .map(|pv| resolve_prop_val(b, params, pv))
            .collect::<Result<_, _>>()?;
        let rows = proc.call(&argv, view)?;
        for row in rows {
            let mut nb = b.clone();
            for y in yields {
                let out_name = y.alias.clone().unwrap_or_else(|| y.col.clone());
                match row.iter().find(|(c, _)| *c == y.col) {
                    Some((_, YieldValue::Node(id))) => {
                        nb.insert(out_name, id.clone());
                    }
                    Some((_, YieldValue::Scalar(v))) => {
                        nb.insert(
                            val_key(&out_name),
                            serde_json::to_string(v).unwrap_or_default(),
                        );
                    }
                    None => {
                        return Err(format!(
                            "procedure `{name}` does not yield column `{}`",
                            y.col
                        ))
                    }
                }
            }
            out.push(nb);
        }
    }
    Ok(out)
}

/// Resolve a linear MATCH `pattern` into var→node-id bindings, applying `where`.
/// `anchor` pre-binds variables (empty for a fresh MATCH; the incoming binding for
/// an `OPTIONAL MATCH` / post-`WITH` MATCH) — any pattern position whose variable is
/// already in `anchor` is constrained to that id, which is the join mechanism
/// (CONCEPT:EG-KG.query.eg-extend-read-side). Fixed and variable-length hops combine freely (CONCEPT:EG-KG.query.concept-2).
fn resolve_match(
    view: &GraphView,
    pattern: &Pattern,
    where_clause: &Option<WhereExpr>,
    anchor: &Binding,
    params: &Params,
) -> Result<Vec<Binding>, String> {
    // Start candidates: the anchored id if the start var is bound, else the label set,
    // then narrowed by any inline property constraints (CONCEPT:EG-KG.query.param-list-drives-unwind).
    let start_ids: Vec<String> = match pattern.start.var.as_ref().and_then(|v| anchor.get(v)) {
        Some(id) => vec![id.clone()],
        None => label_candidates(view, &pattern.start),
    }
    .into_iter()
    // An ANCHORED start node still has its `:Label`/inline props enforced here — the
    // label-index candidate set only pre-filters the un-anchored case (CONCEPT:EG-KG.query.cypher-planning
    // lets a CALL/YIELD node id flow into a labelled MATCH).
    .filter(|id| {
        pattern
            .start
            .label
            .as_ref()
            .is_none_or(|l| node_has_label_id(view, id, l))
            && node_props_match(view, id, &pattern.start, anchor, params)
    })
    .collect();

    // (binding, current-node-id) partials, extended hop by hop.
    let mut partials: Vec<(Binding, String)> = Vec::new();
    for sid in start_ids {
        let mut b = anchor.clone();
        if let Some(v) = &pattern.start.var {
            b.insert(v.clone(), sid.clone());
        }
        partials.push((b, sid));
    }

    partials = walk_hops(view, &pattern.hops, partials, params)?;

    let mut out: Vec<Binding> = Vec::new();
    for (b, _) in partials {
        if where_holds(view, &b, where_clause) {
            out.push(b);
        }
    }
    Ok(out)
}

/// Walk a fixed/variable-length/quantified-group hop chain from `partials`
/// (binding, current-node-id pairs), applying label/prop/anchor constraints hop
/// by hop. Factored out of [`resolve_match`] so quantified-path-pattern group
/// expansion (CONCEPT:EG-KG.query.quantified-path-pattern) can recursively reuse the exact
/// same hop-walking semantics for its inner sub-pattern — a group hop's `edge.group`
/// dispatches to [`quantified_group_matches`] instead of [`neighbors`]/[`bfs_reachable`];
/// everything downstream (label/prop/anchor checks, variable binding) is identical.
fn walk_hops(
    view: &GraphView,
    hops: &[(EdgePat, NodePat)],
    mut partials: Vec<(Binding, String)>,
    params: &Params,
) -> Result<Vec<(Binding, String)>, String> {
    for (edge, node) in hops {
        let mut next: Vec<(Binding, String)> = Vec::new();
        for (b, cur) in &partials {
            if let Some(group) = &edge.group {
                for (group_binding, target) in
                    quantified_group_matches(view, cur, group, b, params)?
                {
                    if let Some(nb) = bind_target_node(view, node, &group_binding, &target, params)
                    {
                        next.push((nb, target));
                    }
                    if next.len() > MAX_ROWS {
                        return Err(format!(
                            "quantified path pattern exceeded the {MAX_ROWS}-row expansion limit"
                        ));
                    }
                }
                continue;
            }

            let targets = match edge.var_len {
                Some((min, max)) => bfs_reachable(view, cur, edge, min, max),
                None => neighbors(view, cur, edge),
            };
            for t in targets {
                let Some(mut nb) = bind_target_node(view, node, b, &t, params) else {
                    continue;
                };
                // Bind a named edge variable (`-[r]->`) on the READ path too — not just
                // write's DELETE-only enrichment — so `RETURN type(r)` (CONCEPT:EG-KG.query.rel-type-projection)
                // can resolve it. Only meaningful for a single fixed hop; QPP
                // relationship variables are captured per iteration separately.
                if let (Some(evar), None) = (&edge.var, edge.var_len) {
                    let (src, tgt) = match edge.direction {
                        Direction::Right => (cur.clone(), t.clone()),
                        Direction::Left => (t.clone(), cur.clone()),
                        Direction::Both => resolve_undirected_endpoints(view, cur, &t),
                    };
                    nb.insert(edge_key(evar), format!("{src}\u{0}{tgt}"));
                }
                next.push((nb, t));
            }
        }
        partials = next;
    }
    Ok(partials)
}

/// Apply the outer node constraints/binding after an ordinary or quantified hop.
fn bind_target_node(
    view: &GraphView,
    node: &NodePat,
    binding: &Binding,
    target: &str,
    params: &Params,
) -> Option<Binding> {
    if node
        .label
        .as_ref()
        .is_some_and(|label| !node_has_label_id(view, target, label))
        || !node_props_match(view, target, node, binding, params)
    {
        return None;
    }
    if let Some(var) = &node.var {
        if binding.get(var).is_some_and(|bound| bound != target) {
            return None;
        }
    }
    let mut next = binding.clone();
    if let Some(var) = &node.var {
        next.insert(var.clone(), target.to_string());
    }
    Some(next)
}

/// Every distinct path produced by repeating `group`'s whole inner sub-pattern
/// between `min` and `max` times. Unlike a reachability BFS, this preserves the
/// ordered per-repetition values for every variable declared inside the group.
/// Two paths ending at the same node therefore remain distinct when their group
/// bindings differ, as required by Cypher 25 group-variable semantics.
fn quantified_group_matches(
    view: &GraphView,
    src: &str,
    group: &QuantifiedGroup,
    binding: &Binding,
    params: &Params,
) -> Result<Vec<(Binding, String)>, String> {
    let (min, max) = group.quantifier;
    let mut seed = binding.clone();
    initialize_group_variables(&mut seed, group);
    let mut out = Vec::new();
    if min == 0 {
        out.push((seed.clone(), src.to_string()));
    }
    if max == 0 {
        return Ok(out);
    }
    let mut frontier = vec![(seed, src.to_string())];

    for depth in 1..=max {
        let mut next_frontier = Vec::new();
        for (state, current) in &frontier {
            for matched in expand_group_once(view, group, current, state, params)? {
                next_frontier.push(matched);
                if next_frontier.len() > MAX_ROWS {
                    return Err(format!(
                        "quantified path pattern exceeded the {MAX_ROWS}-row expansion limit"
                    ));
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        if depth >= min {
            out.extend(next_frontier.iter().cloned());
            if out.len() > MAX_ROWS {
                return Err(format!(
                    "quantified path pattern exceeded the {MAX_ROWS}-row result limit"
                ));
            }
        }
        frontier = next_frontier;
    }
    Ok(out)
}

/// One application of `group`'s inner sub-pattern anchored at `cur` (the group's
/// `start` position must resolve to `cur`, subject to its own label/prop
/// constraints), returning every resulting binding/end node. Group-local plain
/// variables are captured into ordered list sidecars before the iteration result
/// escapes to the outer scope (CONCEPT:EG-KG.query.quantified-path-pattern).
fn expand_group_once(
    view: &GraphView,
    group: &QuantifiedGroup,
    cur: &str,
    binding: &Binding,
    params: &Params,
) -> Result<Vec<(Binding, String)>, String> {
    if let Some(lbl) = &group.start.label {
        if !node_has_label_id(view, cur, lbl) {
            return Ok(Vec::new());
        }
    }
    if !node_props_match(view, cur, &group.start, binding, params) {
        return Ok(Vec::new());
    }
    let mut local = binding.clone();
    clear_group_singletons(&mut local, group);
    if let Some(v) = &group.start.var {
        local.insert(v.clone(), cur.to_string());
    }
    Ok(
        walk_hops(view, &group.hops, vec![(local, cur.to_string())], params)?
            .into_iter()
            .map(|(local, end)| {
                let mut captured = local;
                capture_group_iteration(&mut captured, group);
                (captured, end)
            })
            .collect(),
    )
}

fn initialize_group_variables(binding: &mut Binding, group: &QuantifiedGroup) {
    for var in group_node_variables(group) {
        binding
            .entry(qpp_node_key(var))
            .or_insert_with(|| "[]".to_string());
    }
    for var in group_edge_variables(group) {
        binding
            .entry(qpp_edge_key(var))
            .or_insert_with(|| "[]".to_string());
    }
}

fn clear_group_singletons(binding: &mut Binding, group: &QuantifiedGroup) {
    for var in group_node_variables(group) {
        binding.remove(var);
    }
    for var in group_edge_variables(group) {
        binding.remove(&edge_key(var));
    }
}

fn capture_group_iteration(binding: &mut Binding, group: &QuantifiedGroup) {
    for var in group_node_variables(group) {
        if let Some(id) = binding.remove(var) {
            append_binding_list(binding, qpp_node_key(var), id);
        }
    }
    for var in group_edge_variables(group) {
        if let Some(edge) = binding.remove(&edge_key(var)) {
            append_binding_list(binding, qpp_edge_key(var), edge);
        }
    }
}

fn group_node_variables(group: &QuantifiedGroup) -> Vec<&str> {
    let mut vars = Vec::new();
    if let Some(var) = group.start.var.as_deref() {
        vars.push(var);
    }
    for (_, node) in &group.hops {
        if let Some(var) = node.var.as_deref() {
            if !vars.contains(&var) {
                vars.push(var);
            }
        }
    }
    vars
}

fn group_edge_variables(group: &QuantifiedGroup) -> Vec<&str> {
    let mut vars = Vec::new();
    for (edge, _) in &group.hops {
        if let Some(var) = edge.var.as_deref() {
            if !vars.contains(&var) {
                vars.push(var);
            }
        }
    }
    vars
}

fn group_scope_variables(group: &QuantifiedGroup) -> Vec<&str> {
    let mut vars = group_node_variables(group);
    for var in group_edge_variables(group) {
        if !vars.contains(&var) {
            vars.push(var);
        }
    }
    for (edge, _) in &group.hops {
        if let Some(nested) = edge.group.as_deref() {
            for var in group_scope_variables(nested) {
                if !vars.contains(&var) {
                    vars.push(var);
                }
            }
        }
    }
    vars
}

/// The petgraph traversal direction(s) `direction` resolves to: one for
/// `Right`/`Left`, BOTH for `Both` (undirected — CONCEPT:EG-KG.query.undirected-relationship-pattern
/// — an undirected hop matches an edge stored in either direction between the two
/// endpoints, so the scan unions the outgoing and incoming edge sets).
fn petgraph_directions(direction: Direction) -> &'static [petgraph::Direction] {
    use petgraph::Direction as PgDir;
    match direction {
        Direction::Right => &[PgDir::Outgoing],
        Direction::Left => &[PgDir::Incoming],
        Direction::Both => &[PgDir::Outgoing, PgDir::Incoming],
    }
}

/// Relationship-typed neighbours of `cur` in `edge.direction` (a single fixed hop).
fn neighbors(view: &GraphView, cur: &str, edge: &EdgePat) -> Vec<String> {
    let Some(&idx) = view.node_map.get(cur) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen: HashSet<petgraph::stable_graph::NodeIndex> = HashSet::new();
    for &dir in petgraph_directions(edge.direction) {
        for e in view.graph.edges_directed(idx, dir) {
            let from_id = &view.graph[e.source()];
            let to_id = &view.graph[e.target()];
            if !rel_matches(view, from_id, to_id, edge.rel_type.as_deref()) {
                continue;
            }
            let nbr = match dir {
                petgraph::Direction::Outgoing => e.target(),
                petgraph::Direction::Incoming => e.source(),
            };
            // `Both` scans outgoing then incoming — dedup so a pair connected by
            // edges in both directions (or a self-loop) doesn't yield the same
            // neighbour twice for one undirected hop.
            if seen.insert(nbr) {
                out.push(view.graph[nbr].clone());
            }
        }
    }
    out
}

/// BFS from `src` over REL-typed edges in `edge.direction`, returning every node
/// id reached at a hop-depth within `[min,max]` (depth ≥ 1). Each target appears
/// once (the shallowest depth that reaches it). `edge.direction == Both` walks
/// edges in either direction at every hop (CONCEPT:EG-KG.query.undirected-relationship-pattern).
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
    let dirs = petgraph_directions(edge.direction);

    for depth in 1..=max {
        let mut next: Vec<petgraph::stable_graph::NodeIndex> = Vec::new();
        for &node in &frontier {
            for &dir in dirs {
                for e in view.graph.edges_directed(node, dir) {
                    let nbr = match dir {
                        petgraph::Direction::Outgoing => e.target(),
                        petgraph::Direction::Incoming => e.source(),
                    };
                    let from_id = &view.graph[e.source()];
                    let to_id = &view.graph[e.target()];
                    if !rel_matches(view, from_id, to_id, edge.rel_type.as_deref()) {
                        continue;
                    }
                    if visited.insert(nbr) {
                        next.push(nbr);
                    }
                    // Exclude the START node itself from `reached`: a directed
                    // acyclic hop essentially never revisits `src`, but an
                    // undirected hop (CONCEPT:EG-KG.query.undirected-relationship-pattern) trivially
                    // does at depth 2 (src -> nbr -> src, walking the SAME edge
                    // back) — without this guard the source would incorrectly
                    // appear as one of its own var-length results.
                    if depth >= min && nbr != src_idx {
                        let nbr_id = view.graph[nbr].clone();
                        if reached.insert(nbr_id.clone()) {
                            out.push(nbr_id);
                        }
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

/// Resolve which of `(a→b)` / `(b→a)` is the REAL stored edge direction for a
/// named-variable edge bound through an undirected `-[r]-` hop
/// (CONCEPT:EG-KG.query.undirected-relationship-pattern) — the MATCH that produced the binding only
/// proved SOME edge connects `a` and `b`, not which way. Prefers `(a, b)` when that
/// direction actually exists in the topology; falls back to `(b, a)` (the only other
/// possibility, since the MATCH could not have bound them otherwise). Used so a
/// downstream `DELETE r` targets the edge that is really there.
fn resolve_undirected_endpoints(view: &GraphView, a: &str, b: &str) -> (String, String) {
    if let (Some(&a_idx), Some(&b_idx)) = (view.node_map.get(a), view.node_map.get(b)) {
        if view.graph.find_edge(a_idx, b_idx).is_some() {
            return (a.to_string(), b.to_string());
        }
    }
    (b.to_string(), a.to_string())
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge's
/// canonical `relationship` property stamped by every current writer. An ordinary
/// `type` property is payload, not edge identity. `None` means any relationship.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: Option<&str>) -> bool {
    let Some(rel) = rel else { return true };
    let Some(props_list) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return false;
    };
    for blob in props_list {
        if let Ok(Value::Object(m)) = eg_types::msgpack::decode_property_value(blob) {
            let stored = m.get("relationship").and_then(|v| v.as_str());
            if stored == Some(rel) {
                return true;
            }
        }
    }
    false
}

/// The stored edge `(from→to)`'s relationship type — the value `type(r)`
/// (CONCEPT:EG-KG.query.rel-type-projection) projects, and the SAME canonical
/// `relationship` field [`rel_matches`] reads, so `type(r)` is never null for an
/// edge a typed pattern could have matched. `None` if that field is absent.
fn edge_rel_type(view: &GraphView, from: &str, to: &str) -> Option<String> {
    let props_list = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))?;
    for blob in props_list {
        if let Ok(Value::Object(m)) = eg_types::msgpack::decode_property_value(blob) {
            let stored = m.get("relationship").and_then(|v| v.as_str());
            if let Some(s) = stored {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Materialize a relationship binding as a property map with authoritative
/// endpoint fields. QPP relationship group variables project an ordered list of
/// these maps; singleton relationship variables use the same representation.
fn edge_value(view: &GraphView, edge: &str) -> Value {
    let Some((from, to)) = edge.split_once('\u{0}') else {
        return Value::Null;
    };
    let mut obj = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
        .and_then(|props| props.first())
        .and_then(|blob| eg_types::msgpack::decode_property_value(blob).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    obj.insert("source".to_string(), Value::String(from.to_string()));
    obj.insert("target".to_string(), Value::String(to.to_string()));
    Value::Object(obj)
}

fn edge_prop_value(view: &GraphView, edge: &str, prop: &str) -> Option<Value> {
    edge_value(view, edge).get(prop).cloned()
}

// ── path / WITH plumbing (CONCEPT:EG-KG.query.eg-extend-read-side / EG-063) ───────────────────────────

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
    binding.insert(
        path_key(pv),
        serde_json::to_string(&seq).unwrap_or_default(),
    );
}

/// Project a binding through a `WITH` item list: keep only the listed variables,
/// applying aliases (and carrying their path-var sidecar) (CONCEPT:EG-KG.query.eg-extend-read-side).
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
        if let Some(v) = b.get(&val_key(&it.var)) {
            nb.insert(val_key(&target), v.clone());
        }
        if let Some(v) = b.get(&qpp_node_key(&it.var)) {
            nb.insert(qpp_node_key(&target), v.clone());
        }
        if let Some(v) = b.get(&qpp_edge_key(&it.var)) {
            nb.insert(qpp_edge_key(&target), v.clone());
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
                for (edge, node) in &pattern.hops {
                    if let Some(group) = edge.group.as_deref() {
                        for var in group_scope_variables(group) {
                            push(var, &mut scope);
                        }
                    } else if let Some(var) = edge.var.as_deref() {
                        push(var, &mut scope);
                    }
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
            ReadStage::Unwind { var, .. } => push(var, &mut scope),
            ReadStage::CallProc { yields, .. } => {
                for y in yields {
                    push(
                        &y.alias.clone().unwrap_or_else(|| y.col.clone()),
                        &mut scope,
                    );
                }
            }
            ReadStage::Call { subquery } => {
                if subquery.ret.star {
                    for v in scope_vars(&subquery.stages) {
                        push(&v, &mut scope);
                    }
                } else {
                    for it in &subquery.ret.items {
                        push(&it.column(), &mut scope);
                    }
                }
            }
        }
    }
    scope
}

// ── WHERE evaluation (CONCEPT:EG-KG.query.eg-extend-read-side) ────────────────────────────────────────

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

// ── RETURN finalization (CONCEPT:EG-KG.query.eg-extend-read-side) ─────────────────────────────────────

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
                let cells = items
                    .iter()
                    .map(|i| eval_scalar(view, b, &i.expr))
                    .collect();
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
            } else if binding.contains_key(&qpp_node_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_node_key(v))
                        .into_iter()
                        .map(|id| materialize_node(view, &id))
                        .collect(),
                )
            } else if binding.contains_key(&qpp_edge_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_edge_key(v))
                        .into_iter()
                        .map(|edge| edge_value(view, &edge))
                        .collect(),
                )
            } else if let Some(s) = binding.get(&val_key(v)) {
                // A scalar bound by UNWIND/CALL/YIELD (CONCEPT:EG-KG.query.param-list-drives-unwind/142).
                serde_json::from_str(s).unwrap_or(Value::Null)
            } else if let Some(id) = binding.get(v) {
                materialize_node(view, id)
            } else if let Some(edge) = binding.get(&edge_key(v)) {
                edge_value(view, edge)
            } else {
                Value::Null
            }
        }
        Expr::Prop(v, p) => {
            if binding.contains_key(&qpp_node_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_node_key(v))
                        .into_iter()
                        .map(|id| node_prop(view, &id, p).unwrap_or(Value::Null))
                        .collect(),
                )
            } else if binding.contains_key(&qpp_edge_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_edge_key(v))
                        .into_iter()
                        .map(|edge| edge_prop_value(view, &edge, p).unwrap_or(Value::Null))
                        .collect(),
                )
            } else {
                binding
                    .get(v)
                    .and_then(|id| node_prop(view, id, p))
                    .or_else(|| {
                        binding
                            .get(&edge_key(v))
                            .and_then(|edge| edge_prop_value(view, edge, p))
                    })
                    .unwrap_or(Value::Null)
            }
        }
        Expr::RelType(v) => {
            if binding.contains_key(&qpp_edge_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_edge_key(v))
                        .into_iter()
                        .map(|edge| {
                            edge.split_once('\u{0}')
                                .and_then(|(from, to)| edge_rel_type(view, from, to))
                                .map(Value::String)
                                .unwrap_or(Value::Null)
                        })
                        .collect(),
                )
            } else {
                binding
                    .get(&edge_key(v))
                    .and_then(|edge| edge.split_once('\u{0}'))
                    .and_then(|(from, to)| edge_rel_type(view, from, to))
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }
        }
        // Aggregates never reach here (the agg path owns them).
        Expr::CountStar | Expr::Aggregate(..) => Value::Null,
    }
}

/// Compute the grouped aggregate rows (CONCEPT:EG-KG.query.eg-extend-read-side). The non-aggregate items form
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

/// Compute one aggregate over a group of bindings (CONCEPT:EG-KG.query.eg-extend-read-side).
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
                    .reduce(|a, b| {
                        if cmp_values(&a, &b) == Ordering::Greater {
                            b
                        } else {
                            a
                        }
                    })
                    .unwrap_or(Value::Null),
                AggFunc::Max => vals
                    .into_iter()
                    .reduce(|a, b| {
                        if cmp_values(&a, &b) == Ordering::Less {
                            b
                        } else {
                            a
                        }
                    })
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
            } else if b.contains_key(&qpp_node_key(v)) {
                Some(Value::Array(
                    binding_list(b, &qpp_node_key(v))
                        .into_iter()
                        .map(|id| materialize_node(view, &id))
                        .collect(),
                ))
            } else if b.contains_key(&qpp_edge_key(v)) {
                Some(Value::Array(
                    binding_list(b, &qpp_edge_key(v))
                        .into_iter()
                        .map(|edge| edge_value(view, &edge))
                        .collect(),
                ))
            } else if let Some(s) = b.get(&val_key(v)) {
                Some(serde_json::from_str(s).unwrap_or(Value::Null))
            } else {
                b.get(v)
                    .map(|id| materialize_node(view, id))
                    .or_else(|| b.get(&edge_key(v)).map(|edge| edge_value(view, edge)))
            }
        }
        AggArg::Prop(v, p) => {
            if b.contains_key(&qpp_node_key(v)) {
                Some(Value::Array(
                    binding_list(b, &qpp_node_key(v))
                        .into_iter()
                        .map(|id| node_prop(view, &id, p).unwrap_or(Value::Null))
                        .collect(),
                ))
            } else if b.contains_key(&qpp_edge_key(v)) {
                Some(Value::Array(
                    binding_list(b, &qpp_edge_key(v))
                        .into_iter()
                        .map(|edge| edge_prop_value(view, &edge, p).unwrap_or(Value::Null))
                        .collect(),
                ))
            } else {
                b.get(v)
                    .map(|id| node_prop(view, id, p).unwrap_or(Value::Null))
                    .or_else(|| {
                        b.get(&edge_key(v))
                            .map(|edge| edge_prop_value(view, edge, p).unwrap_or(Value::Null))
                    })
            }
        }
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

/// Does a node's property blob carry `label` as canonical `node_type` or in the
/// explicit multi-label `labels` array?
fn node_has_label(blob: &[u8], label: &str) -> bool {
    let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
        return false;
    };
    if val.get("node_type").and_then(|v| v.as_str()) == Some(label) {
        return true;
    }
    if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
        if arr.iter().any(|x| x.as_str() == Some(label)) {
            return true;
        }
    }
    false
}

/// Do node `id`'s stored properties satisfy `node`'s inline property constraints
/// (`(n {k: v})`, CONCEPT:EG-KG.query.param-list-drives-unwind)? Each constraint value resolves against the live
/// params + binding, then must equal the node's stored property. No inline map ⇒ true.
fn node_props_match(
    view: &GraphView,
    id: &str,
    node: &NodePat,
    binding: &Binding,
    params: &Params,
) -> bool {
    let Some(props) = &node.props else {
        return true;
    };
    for (key, pv) in props {
        let Ok(expected) = resolve_prop_val(binding, params, pv) else {
            return false;
        };
        if node_prop(view, id, key).as_ref() != Some(&expected) {
            return false;
        }
    }
    true
}

/// Materialize the public Cypher value for a node variable.
///
/// The graph key is the authoritative node identity, so it always wins over a
/// payload field named `id`. `node_type` is the sole canonical primary label and
/// is always present in the projected map (null for an explicitly untyped node).
/// This keeps result decoding uniform across native, Bolt, and delegated clients.
fn materialize_node(view: &GraphView, node_id: &str) -> Value {
    let mut obj = view
        .node_properties
        .get(node_id)
        .and_then(|blob| eg_types::msgpack::decode_property_value(blob).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    obj.insert("id".to_string(), Value::String(node_id.to_string()));
    if !obj.get("node_type").is_some_and(Value::is_string) {
        obj.insert("node_type".to_string(), Value::Null);
    }
    Value::Object(obj)
}

/// Read one property from a node. `id` is a virtual canonical property backed by
/// the graph key; every other property, including `node_type`, comes from the
/// stored node map.
fn node_prop(view: &GraphView, node_id: &str, prop: &str) -> Option<Value> {
    if prop == "id" {
        return view
            .node_map
            .contains_key(node_id)
            .then(|| Value::String(node_id.to_string()));
    }
    let blob = view.node_properties.get(node_id)?;
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    val.get(prop).cloned()
}

// ── write path (CONCEPT:EG-KG.query.register-each-user-table / EG-061) ─────────────────────────────────────

/// Parse + run a Cypher statement that MAY mutate `core` — `CREATE`/`MERGE`/`SET`/
/// `[DETACH] DELETE`/`REMOVE`, with an optional leading `MATCH … WHERE` and trailing
/// `RETURN` (CONCEPT:EG-KG.query.register-each-user-table/EG-061). A pure-read query is delegated to the unchanged
/// snapshot read path, so this is the one entry-point a caller needs whether the
/// statement reads or writes. Writes map to eg-core's OWN native ops — NO DataFusion
/// — and `mark_dirty()` is called once after a mutation so caches refresh.
pub fn exec_cypher_write(core: &GraphCore, cypher: &str) -> Result<QueryResult, String> {
    exec_cypher_write_params(core, cypher, &Params::new())
}

/// The parameterized form of [`exec_cypher_write`] (CONCEPT:EG-KG.query.param-list-drives-unwind) — the single
/// entry-point for a read-or-write statement with `$name` query parameters.
pub fn exec_cypher_write_params(
    core: &GraphCore,
    cypher: &str,
    params: &Params,
) -> Result<QueryResult, String> {
    match parser::parse_statement(cypher)? {
        Statement::Read(_) => {
            let view = core.analysis_snapshot();
            exec_cypher_params(&view, cypher, params)
        }
        Statement::Write(w) => exec_write(core, &w, params),
    }
}

/// Execute a parsed write statement against `core` (CONCEPT:EG-KG.query.register-each-user-table / EG-061).
fn exec_write(core: &GraphCore, w: &WriteQuery, params: &Params) -> Result<QueryResult, String> {
    // Resolve the leading MATCH (if any) over a snapshot into bindings. No MATCH ⇒
    // one empty binding (the write clauses run exactly once).
    let snap = core.analysis_snapshot();
    let mut bindings: Vec<Binding> = match &w.match_pattern {
        Some(pattern) => resolve_match(&snap, pattern, &w.where_clause, &HashMap::new(), params)?,
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
                            // Undirected: the MATCH that produced this binding only
                            // proved AN edge exists between `a` and `b` in either
                            // direction (CONCEPT:EG-KG.query.undirected-relationship-pattern) —
                            // resolve which one actually exists so a downstream
                            // `DELETE r`/edge-var reference points at the real
                            // stored edge, not an endpoint order that may not exist.
                            Direction::Both => resolve_undirected_endpoints(&snap, &a, &b),
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
            apply_write_op(core, &snap, binding, op, params, &mut mutated)?;
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
/// newly created/merged variables (CONCEPT:EG-KG.query.register-each-user-table / EG-061).
fn apply_write_op(
    core: &GraphCore,
    snap: &GraphView,
    binding: &mut Binding,
    op: &WriteOp,
    params: &Params,
    mutated: &mut bool,
) -> Result<(), String> {
    match op {
        WriteOp::Create(pattern) => apply_create(core, binding, pattern, params, mutated)?,
        WriteOp::Merge(node) => apply_merge(core, binding, node, params, mutated)?,
        WriteOp::Set(items) => apply_set(core, binding, items, mutated)?,
        WriteOp::Delete { vars, detach } => {
            apply_delete(core, snap, binding, vars, *detach, mutated)?
        }
        WriteOp::Remove(items) => apply_remove(core, binding, items, mutated)?,
    }
    Ok(())
}

/// `CREATE <pattern>`: realize each node (reuse a bound var, else create) and each
/// hop's edge (CONCEPT:EG-KG.query.register-each-user-table).
fn apply_create(
    core: &GraphCore,
    binding: &mut Binding,
    pattern: &Pattern,
    params: &Params,
    mutated: &mut bool,
) -> Result<(), String> {
    let start_id = realize_node(core, binding, &pattern.start, params, mutated)?;
    create_hops(core, binding, &pattern.hops, start_id, params, mutated)?;
    Ok(())
}

/// Materialize a hop sequence from an already-realized start node. Quantified
/// groups recurse through this same routine, so multi-hop and nested groups use
/// exactly the ordinary CREATE node/edge semantics.
fn create_hops(
    core: &GraphCore,
    binding: &mut Binding,
    hops: &[(EdgePat, NodePat)],
    mut prev_id: String,
    params: &Params,
    mutated: &mut bool,
) -> Result<String, String> {
    for (edge, node) in hops {
        if let Some(group) = edge.group.as_deref() {
            prev_id = create_quantified_group(core, binding, group, prev_id, params, mutated)?;
            apply_existing_node_spec(core, binding, node, &prev_id, params, mutated)?;
            continue;
        }
        // Undirected `-[...]-` (CONCEPT:EG-KG.query.undirected-relationship-pattern) is a MATCH-only
        // shape: it names no concrete direction to store the edge in, so — like the
        // quantified-group's own inner edges — CREATE rejects it rather than guessing.
        if edge.direction == Direction::Both {
            return Err(
                "CREATE requires a directed relationship (`->` or `<-`); `-[...]-` (undirected) has no direction to create".into(),
            );
        }
        let next_id = realize_node(core, binding, node, params, mutated)?;
        let (src, tgt) = match edge.direction {
            Direction::Right => (prev_id.clone(), next_id.clone()),
            Direction::Left => (next_id.clone(), prev_id.clone()),
            Direction::Both => unreachable!("rejected above"),
        };
        let mut props = props_to_map(edge.props.as_deref(), binding, params)?;
        if let Some(rel) = &edge.rel_type {
            props.insert("relationship".into(), Value::String(rel.clone()));
        }
        let blob = rmp_serde::to_vec_named(&Value::Object(props))
            .map_err(|e| format!("encode edge props: {e}"))?;
        if let Some(var) = &edge.var {
            binding.insert(edge_key(var), format!("{src}\u{0}{tgt}"));
        }
        core.add_edge(src, tgt, blob)
            .map_err(|e| format!("CREATE edge: {e}"))?;
        *mutated = true;
        prev_id = next_id;
    }
    Ok(prev_id)
}

/// CREATE extension for a quantified group. A range deterministically
/// materializes its inclusive upper bound (`{m,n}` creates `n` repetitions), so
/// write cardinality never depends on planner choice. Per-iteration variables are
/// captured into the same ordered group-variable lists as MATCH.
fn create_quantified_group(
    core: &GraphCore,
    binding: &mut Binding,
    group: &QuantifiedGroup,
    mut current: String,
    params: &Params,
    mutated: &mut bool,
) -> Result<String, String> {
    initialize_group_variables(binding, group);
    for _ in 0..group.quantifier.1 {
        let mut local = binding.clone();
        clear_group_singletons(&mut local, group);
        apply_existing_node_spec(core, &mut local, &group.start, &current, params, mutated)?;
        let end = create_hops(core, &mut local, &group.hops, current, params, mutated)?;
        capture_group_iteration(&mut local, group);
        *binding = local;
        current = end;
    }
    Ok(current)
}

/// Apply a node pattern to an existing QPP boundary node. Missing label/property
/// constraints are added; conflicting constraints fail instead of silently
/// overwriting an already-created node.
fn apply_existing_node_spec(
    core: &GraphCore,
    binding: &mut Binding,
    node: &NodePat,
    id: &str,
    params: &Params,
    mutated: &mut bool,
) -> Result<(), String> {
    if let Some(var) = &node.var {
        if binding.get(var).is_some_and(|bound| bound != id) {
            return Err(format!(
                "CREATE node variable `{var}` is already bound to a different node"
            ));
        }
        binding.insert(var.clone(), id.to_string());
    }

    let blob = core
        .get_node_properties(id)
        .ok_or_else(|| format!("CREATE QPP boundary node `{id}` is absent"))?;
    let mut value = eg_types::msgpack::decode_property_value(&blob)
        .map_err(|_| format!("decode node `{id}` failed"))?;
    let obj = value
        .as_object_mut()
        .ok_or_else(|| format!("CREATE QPP boundary node `{id}` has non-object properties"))?;
    let mut changed = false;

    if let Some(label) = &node.label {
        match obj.get("node_type").and_then(Value::as_str) {
            Some(existing) if existing != label => {
                return Err(format!(
                    "CREATE QPP boundary label `{label}` conflicts with `{existing}`"
                ));
            }
            Some(_) => {}
            None if obj.contains_key("node_type") => {
                return Err(
                    "CREATE QPP boundary node_type must be a string when a label is present"
                        .to_string(),
                );
            }
            None => {
                obj.insert("node_type".to_string(), Value::String(label.clone()));
                changed = true;
            }
        }
    }
    for (key, wanted) in props_to_map(node.props.as_deref(), binding, params)? {
        if key == "id" {
            if wanted.as_str() != Some(id) {
                return Err(format!(
                    "CREATE QPP boundary id constraint does not match `{id}`"
                ));
            }
            continue;
        }
        match obj.get(&key) {
            Some(existing) if existing != &wanted => {
                return Err(format!(
                    "CREATE QPP boundary property `{key}` conflicts with its existing value"
                ));
            }
            Some(_) => {}
            None => {
                obj.insert(key, wanted);
                changed = true;
            }
        }
    }
    if changed {
        let encoded =
            rmp_serde::to_vec_named(&value).map_err(|e| format!("encode node `{id}`: {e}"))?;
        core.add_node(id.to_string(), encoded);
        *mutated = true;
    }
    Ok(())
}

/// Resolve a CREATE node position to an id: reuse a bound variable, else create a new
/// node carrying its canonical `node_type` + inline props (CONCEPT:EG-KG.query.register-each-user-table).
fn realize_node(
    core: &GraphCore,
    binding: &mut Binding,
    node: &NodePat,
    params: &Params,
    mutated: &mut bool,
) -> Result<String, String> {
    if let Some(var) = &node.var {
        if let Some(existing) = binding.get(var) {
            return Ok(existing.clone());
        }
    }
    let mut props = props_to_map(node.props.as_deref(), binding, params)?;
    if let Some(label) = &node.label {
        match props.get("node_type").and_then(Value::as_str) {
            Some(explicit) if explicit != label => {
                return Err(format!(
                    "node label `{label}` conflicts with explicit node_type `{explicit}`"
                ));
            }
            Some(_) => {}
            None if props.contains_key("node_type") => {
                return Err("node_type must be a string when a node label is present".to_string());
            }
            None => {
                props.insert("node_type".to_string(), Value::String(label.clone()));
            }
        }
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
/// absent. Idempotent. Binds `n` (CONCEPT:EG-KG.query.register-each-user-table).
fn apply_merge(
    core: &GraphCore,
    binding: &mut Binding,
    node: &NodePat,
    params: &Params,
    mutated: &mut bool,
) -> Result<(), String> {
    let want = props_to_map(node.props.as_deref(), binding, params)?;
    let candidates: Vec<(String, Vec<u8>)> = match &node.label {
        Some(label) => core.get_nodes_by_label(label, 0),
        None => core.get_nodes(),
    };
    for (id, blob) in &candidates {
        if let Some(label) = &node.label {
            // GraphCore's general label index intentionally serves other engine
            // surfaces too. Cypher's current-only contract is narrower: a legacy
            // payload `type`/`label` must never satisfy a Cypher node label.
            if !node_has_label(blob, label) {
                continue;
            }
        }
        let Ok(Value::Object(obj)) = eg_types::msgpack::decode_property_value(blob) else {
            continue;
        };
        if want.iter().all(|(k, v)| obj.get(k) == Some(v)) {
            if let Some(var) = &node.var {
                binding.insert(var.clone(), id.clone());
            }
            return Ok(());
        }
    }
    realize_node(core, binding, node, params, mutated)?;
    Ok(())
}

/// `SET v.prop = literal [, …]`: merge each assignment onto the bound node via the
/// engine's atomic `compare_and_set_fields` (CONCEPT:EG-KG.query.register-each-user-table).
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
/// bound node (CONCEPT:EG-KG.query.cypher-execution). A read-modify-write over the engine's field map: read
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
        let mut val = eg_types::msgpack::decode_property_value(&blob)
            .map_err(|_| format!("decode node `{id}` failed"))?;
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

/// Remove `label` from canonical `node_type` or the explicit `labels` array.
/// Returns whether anything changed (CONCEPT:EG-KG.query.cypher-execution).
fn remove_label(obj: &mut serde_json::Map<String, Value>, label: &str) -> bool {
    let mut changed = false;
    if obj.get("node_type").and_then(|v| v.as_str()) == Some(label) {
        obj.remove("node_type");
        changed = true;
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
/// incident edges), or a bound edge variable's edge (CONCEPT:EG-KG.query.register-each-user-table).
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

/// A Cypher inline-property list → a JSON object map, resolving each value against the
/// live params + binding (literal / `$param` / bound-var reference; CONCEPT:EG-KG.query.param-list-drives-unwind).
fn props_to_map(
    props: Option<&[(String, PropVal)]>,
    binding: &Binding,
    params: &Params,
) -> Result<serde_json::Map<String, Value>, String> {
    let mut m = serde_json::Map::new();
    if let Some(list) = props {
        for (k, pv) in list {
            m.insert(k.clone(), resolve_prop_val(binding, params, pv)?);
        }
    }
    Ok(m)
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
            pbytes(serde_json::json!({"node_type":"Person","name":"Alice"})),
        );
        core.add_node(
            "bob".into(),
            pbytes(serde_json::json!({"node_type":"Person","name":"Bob"})),
        );
        core.add_node(
            "carol".into(),
            pbytes(serde_json::json!({"node_type":"Person","name":"Carol"})),
        );
        core.add_node(
            "d1".into(),
            pbytes(serde_json::json!({"node_type":"Doc","size":42})),
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
                projected_id(&cells[col]).unwrap().to_string()
            })
            .collect();
        out.sort();
        out
    }

    fn projected_id(value: &Value) -> Option<&str> {
        value
            .as_str()
            .or_else(|| value.get("id").and_then(Value::as_str))
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
    fn bare_node_projection_is_a_canonical_map() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a, a.id, a.node_type",
        )
        .unwrap();
        let cells = cells_of(&qr, 0);
        let node = cells[0].as_object().expect("bare node must be a map");
        assert_eq!(node.get("id"), Some(&Value::String("alice".into())));
        assert_eq!(node.get("node_type"), Some(&Value::String("Person".into())));
        assert_eq!(node.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(cells[1], Value::String("alice".into()));
        assert_eq!(cells[2], Value::String("Person".into()));
    }

    #[test]
    fn graph_key_overrides_spoofed_payload_id_in_node_projection() {
        let core = GraphCore::new();
        core.add_node(
            "canonical-id".into(),
            pbytes(serde_json::json!({
                "id": "spoofed-id", "node_type": "Person", "name": "Ada"
            })),
        );
        let qr = exec_cypher(&core.analysis_snapshot(), "MATCH (n:Person) RETURN n, n.id").unwrap();
        let cells = cells_of(&qr, 0);
        assert_eq!(projected_id(&cells[0]), Some("canonical-id"));
        assert_eq!(cells[1], Value::String("canonical-id".into()));
    }

    #[test]
    fn cypher_labels_reject_legacy_type_and_label_aliases() {
        let core = GraphCore::new();
        core.add_node(
            "legacy-type".into(),
            pbytes(serde_json::json!({"type": "Person"})),
        );
        core.add_node(
            "legacy-label".into(),
            pbytes(serde_json::json!({"label": "Person"})),
        );
        core.add_node(
            "canonical".into(),
            pbytes(serde_json::json!({"node_type": "Person"})),
        );
        let qr = exec_cypher(&core.analysis_snapshot(), "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(ids(&qr, 0), vec!["canonical"]);
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
                    projected_id(&c[0]).unwrap().to_string(),
                    projected_id(&c[1]).unwrap().to_string(),
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

    /// Typed patterns and `type(r)` consume the same canonical relationship field.
    /// Mirrors the delegation router's grouped discovery shape.
    fn relationship_fixture() -> GraphView {
        let core = GraphCore::new();
        core.add_node(
            "srv:a".into(),
            pbytes(serde_json::json!({"node_type":"Server","name":"a-mcp"})),
        );
        core.add_node(
            "srv:b".into(),
            pbytes(serde_json::json!({"node_type":"Server","name":"b-mcp"})),
        );
        for r in ["res:a1", "res:a2", "res:b1"] {
            core.add_node(
                r.into(),
                pbytes(serde_json::json!({"node_type":"CallableResource","name":r})),
            );
        }
        for (s, t) in [
            ("srv:a", "res:a1"),
            ("srv:a", "res:a2"),
            ("srv:b", "res:b1"),
        ] {
            core.add_edge(
                s.into(),
                t.into(),
                pbytes(serde_json::json!({"relationship":"PROVIDES"})),
            )
            .unwrap();
        }
        core.analysis_snapshot()
    }

    #[test]
    fn typed_traversal_matches_canonical_relationship() {
        let v = relationship_fixture();
        // Typed directed traversal must find all three PROVIDES edges.
        let qr = exec_cypher(
            &v,
            "MATCH (s:Server)-[:PROVIDES]->(r:CallableResource) RETURN s, r",
        )
        .unwrap();
        assert_eq!(
            qr.rows.len(),
            3,
            "expected 3 PROVIDES edges, got {}",
            qr.rows.len()
        );
    }

    #[test]
    fn grouped_count_over_canonical_relationship() {
        let v = relationship_fixture();
        // The delegation router's discovery shape: per-server tool counts.
        let qr = exec_cypher(
            &v,
            "MATCH (s:Server)-[:PROVIDES]->(r:CallableResource) \
             RETURN s.name AS server, count(r) AS tools ORDER BY tools DESC",
        )
        .unwrap();
        assert_eq!(qr.rows.len(), 2, "expected one row per server");
        // Highest-count server first (a-mcp: 2, b-mcp: 1).
        let top = cells_of(&qr, 0);
        assert_eq!(top[0].as_str(), Some("a-mcp"));
        assert_eq!(top[1].as_i64(), Some(2));
        let second = cells_of(&qr, 1);
        assert_eq!(second[0].as_str(), Some("b-mcp"));
        assert_eq!(second[1].as_i64(), Some(1));
    }

    /// `RETURN type(r)` over a bound edge variable projects the canonical
    /// relationship and proves the read path binds the edge variable.
    #[test]
    fn type_function_projects_canonical_relationship() {
        let v = relationship_fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (s:Server {name:'a-mcp'})-[r]->(res:CallableResource) \
             RETURN type(r), res.name ORDER BY res.name",
        )
        .unwrap();
        assert_eq!(qr.rows.len(), 2, "expected both of a-mcp's outbound edges");
        for i in 0..2 {
            let row = cells_of(&qr, i);
            assert_eq!(
                row[0].as_str(),
                Some("PROVIDES"),
                "type(r) must project the canonical relationship name, not null"
            );
        }
        assert_eq!(cells_of(&qr, 0)[1].as_str(), Some("res:a1"));
        assert_eq!(cells_of(&qr, 1)[1].as_str(), Some("res:a2"));
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

    /// CONCEPT:EG-KG.query.undirected-relationship-pattern regression: the exact failing shape from
    /// the bug report, `(n {id:$id})-[*1..2]-(m)` — untyped, undirected, bounded
    /// var-length. Previously a raw "expected ArrowRight, found Some(Dash)" parse
    /// error; now runs a bounded BFS that walks edges in EITHER direction. Anchored
    /// at `bob` (mid-chain alice->bob->carol): within 1..2 undirected hops bob
    /// reaches alice (1 hop, incoming) and carol (1 hop, outgoing) — both directions
    /// are actually exercised, not just the outgoing one `Direction::Right` already
    /// covered.
    #[test]
    fn undirected_variable_length_path_walks_both_directions() {
        // The graph key is the virtual `id` property, so the anchor works without
        // duplicating identity in every stored property map.
        let core = GraphCore::new();
        for id in ["alice", "bob", "carol"] {
            core.add_node(id.into(), pbytes(serde_json::json!({"node_type":"Person"})));
        }
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
        let v = core.analysis_snapshot();

        let mut params = Params::new();
        params.insert("id".to_string(), Value::String("bob".to_string()));
        let qr = exec_cypher_params(&v, "MATCH (n {id:$id})-[*1..2]-(m) RETURN m", &params)
            .expect("undirected var-length MATCH must execute, not error");
        assert_eq!(ids(&qr, 0), vec!["alice", "carol"]);
    }

    /// A plain (non-var-length) undirected relationship also walks either direction
    /// — `bob` reaches `alice` only via the INCOMING alice->bob edge.
    #[test]
    fn undirected_fixed_hop_matches_incoming_edge() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.name = 'Bob' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["alice", "carol"]);
    }

    /// `CREATE` has no defined direction for `-[...]-` (undirected) and must reject
    /// it with a clear typed error rather than guessing a direction
    /// (CONCEPT:EG-KG.query.undirected-relationship-pattern). This remains true
    /// inside a natively materialized quantified group.
    #[test]
    fn create_rejects_undirected_relationship() {
        let core = GraphCore::new();
        let err = exec_cypher_write(
            &core,
            "CREATE (a:Person {name:'X'})-[:KNOWS]-(b:Person {name:'Y'})",
        )
        .unwrap_err();
        assert!(
            err.contains("undirected") || err.contains("directed"),
            "{err}"
        );
    }

    // ── CONCEPT:EG-KG.query.quantified-path-pattern — Cypher 25 QPP execution ───────────────────────

    #[test]
    fn quantified_group_single_hop_matches_equivalent_var_length() {
        let v = fixture();
        // ((x)-[:KNOWS]->(y)){1,3} over one relationship type is semantically the
        // same reachability as -[:KNOWS*1..3]->.
        let qpp = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[:KNOWS]->(y)){1,3}(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        let var_len = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qpp, 0), ids(&var_len, 0));
        assert_eq!(ids(&qpp, 0), vec!["bob", "carol"]);
    }

    #[test]
    fn quantified_group_exact_one_repetition_is_a_single_hop() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[:KNOWS]->(y)){1,1}(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["bob"]);
    }

    #[test]
    fn quantified_group_projects_ordered_per_iteration_variables() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[r:KNOWS]->(y)){1,3}(b:Person) \
             WHERE a.name = 'Alice' RETURN x.name, y.name, type(r), b",
        )
        .unwrap();
        assert_eq!(qr.rows.len(), 2);

        let by_end: HashMap<String, Vec<Value>> = (0..qr.rows.len())
            .map(|row| {
                let cells = cells_of(&qr, row);
                (projected_id(&cells[3]).unwrap().to_string(), cells)
            })
            .collect();
        assert_eq!(by_end["bob"][0], serde_json::json!(["Alice"]));
        assert_eq!(by_end["bob"][1], serde_json::json!(["Bob"]));
        assert_eq!(by_end["bob"][2], serde_json::json!(["KNOWS"]));
        assert_eq!(by_end["carol"][0], serde_json::json!(["Alice", "Bob"]));
        assert_eq!(by_end["carol"][1], serde_json::json!(["Bob", "Carol"]));
        assert_eq!(by_end["carol"][2], serde_json::json!(["KNOWS", "KNOWS"]));
    }

    #[test]
    fn quantified_group_zero_repetitions_binds_empty_group_lists() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[r:KNOWS]->(y)){0}(b) \
             WHERE a.name = 'Alice' RETURN x, y, type(r), b",
        )
        .unwrap();
        let cells = cells_of(&qr, 0);
        assert_eq!(cells[0], serde_json::json!([]));
        assert_eq!(cells[1], serde_json::json!([]));
        assert_eq!(cells[2], serde_json::json!([]));
        assert_eq!(projected_id(&cells[3]), Some("alice"));
    }

    #[test]
    fn quantified_group_lists_survive_with_aliasing() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[:KNOWS]->(y)){2}(b) \
             WHERE a.name = 'Alice' WITH y AS steps RETURN steps",
        )
        .unwrap();
        let cells = cells_of(&qr, 0);
        let steps = cells[0]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| projected_id(value).unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(steps, vec!["bob".to_string(), "carol".to_string()]);
    }

    #[test]
    fn quantified_group_multi_hop_inner_pattern_repeats_the_whole_subpath() {
        let v = fixture();
        // Repeating the 2-hop inner pattern {alice->bob->carol} once from alice
        // reaches carol; twice would need a 4th node, which the fixture lacks.
        // The group's own inner vars are available as ordered lists; this query
        // only projects the outer trailing node.
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person {name: 'Alice'})((x)-[:KNOWS]->()-[:KNOWS]->(y)){1,2}(end) \
             RETURN end",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["carol"]);
    }

    #[test]
    fn quantified_group_trailing_node_label_filters_end_position() {
        let v = fixture();
        // d1 is a :Doc, unreachable via :KNOWS anyway, but this proves the
        // trailing node's label constraint is applied to the group's output.
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)((x)-[:KNOWS]->(y)){1,3}(b:Doc) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert!(qr.rows.is_empty());
    }

    #[test]
    fn create_quantified_group_materializes_upper_bound_and_returns_group_lists() {
        let core = GraphCore::new();
        let qr = exec_cypher_write(
            &core,
            "CREATE (a:Person {id:'qpp-a', name:'A'}) \
             ((x)-[r:KNOWS]->(y:Person {name:'Step'})){1,3}(end) \
             RETURN x.name, y.name, type(r), end",
        )
        .unwrap();
        let cells = cells_of(&qr, 0);
        assert_eq!(cells[0], serde_json::json!(["A", "Step", "Step"]));
        assert_eq!(cells[1], serde_json::json!(["Step", "Step", "Step"]));
        assert_eq!(cells[2], serde_json::json!(["KNOWS", "KNOWS", "KNOWS"]));
        assert!(projected_id(&cells[3]).is_some());

        let reach = exec_cypher_write(
            &core,
            "MATCH (a:Person {id:'qpp-a'})-[:KNOWS*1..3]->(b) RETURN b",
        )
        .unwrap();
        assert_eq!(reach.rows.len(), 3);
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

    // ── read clauses (CONCEPT:EG-KG.query.eg-extend-read-side) ──────────────────────────────────────────

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
            by_a.insert(projected_id(&c[0]).unwrap().to_string(), c[1].clone());
        }
        assert_eq!(projected_id(&by_a["alice"]), Some("bob"));
        assert_eq!(projected_id(&by_a["bob"]), Some("carol"));
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

        let qr2 = exec_cypher(&v, "MATCH (a:Person) WHERE a.name CONTAINS 'o' RETURN a").unwrap();
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
        let qr3 = exec_cypher(&v, "MATCH (a:Doc) RETURN a.node_type, sum(a.size)").unwrap();
        let c3 = cells_of(&qr3, 0);
        assert_eq!(c3[0], Value::String("Doc".into()));
        assert_eq!(c3[1], Value::Number(42.into()));
    }

    #[test]
    fn distinct_dedups_rows() {
        let v = fixture();
        // Every Person has node_type 'Person' → DISTINCT collapses to one row.
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN DISTINCT a.node_type").unwrap();
        assert_eq!(qr.rows.len(), 1);
        assert_eq!(cells_of(&qr, 0)[0], Value::String("Person".into()));
    }

    #[test]
    fn return_star_projects_scope() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) WHERE a.name = 'Alice' RETURN *").unwrap();
        assert_eq!(qr.columns, vec!["a"]);
        assert_eq!(projected_id(&cells_of(&qr, 0)[0]), Some("alice"));
    }

    // ── var-length generalization (CONCEPT:EG-KG.query.concept-2) ─────────────────────────────

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

    // ── UNWIND (CONCEPT:EG-KG.query.param-list-drives-unwind) ─────────────────────────────────────────────────

    #[test]
    fn unwind_list_literal_yields_one_row_per_element() {
        let v = fixture();
        let qr = exec_cypher(&v, "UNWIND [1, 2, 3] AS x RETURN x").unwrap();
        assert_eq!(qr.columns, vec!["x"]);
        assert_eq!(qr.rows.len(), 3);
        let vals: Vec<i64> = (0..3)
            .map(|r| cells_of(&qr, r)[0].as_i64().unwrap())
            .collect();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn unwind_param_then_match_inline_prop() {
        // CONCEPT:EG-KG.query.param-list-drives-unwind — a $param list drives UNWIND, and each unwound value is
        // referenced by a read-side inline property `(n:Person {name: nm})`.
        let v = fixture();
        let mut params = Params::new();
        params.insert("names".into(), serde_json::json!(["Alice", "Carol"]));
        let qr = exec_cypher_params(
            &v,
            "UNWIND $names AS nm MATCH (n:Person {name: nm}) RETURN n",
            &params,
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["alice", "carol"]);
    }

    #[test]
    fn unwind_pipelines_into_aggregation() {
        let v = fixture();
        let qr = exec_cypher(&v, "UNWIND [10, 20, 30] AS x RETURN sum(x), count(*)").unwrap();
        let c = cells_of(&qr, 0);
        assert_eq!(c[0], Value::Number(60.into()));
        assert_eq!(c[1], Value::Number(3.into()));
    }

    // ── CALL subquery + procedures (CONCEPT:EG-KG.query.cypher-planning / EG-143) ────────────────────

    #[test]
    fn call_subquery_joins_rows() {
        let v = fixture();
        // Subquery returns the 3 Person ids; the outer just forwards them.
        let qr = exec_cypher(&v, "CALL { MATCH (a:Person) RETURN a } RETURN a").unwrap();
        assert_eq!(ids(&qr, 0), vec!["alice", "bob", "carol"]);
    }

    #[test]
    fn call_subquery_with_aggregate_scalar() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL { MATCH (a:Person) RETURN count(a) AS c } RETURN c",
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["c"]);
        assert_eq!(cells_of(&qr, 0)[0], Value::Number(3.into()));
    }

    #[test]
    fn call_db_labels_builtin() {
        let v = fixture();
        let qr = exec_cypher(&v, "CALL db.labels() YIELD label RETURN label").unwrap();
        assert_eq!(qr.columns, vec!["label"]);
        assert_eq!(ids(&qr, 0), vec!["Doc", "Person"]);
    }

    #[test]
    fn unknown_procedure_errors() {
        let v = fixture();
        let err = exec_cypher(&v, "CALL no.such.proc() YIELD x RETURN x").unwrap_err();
        assert!(err.contains("unknown procedure"), "{err}");
    }

    // ── APOC/GDS procedure library (CONCEPT:EG-KG.query.eg-2) ─────────────────────────────

    #[test]
    fn call_db_relationship_types_builtin() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["KNOWS"]);
    }

    #[test]
    fn call_gds_pagerank_scores_nodes() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.pageRank() YIELD nodeId, score RETURN nodeId, score",
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["nodeId", "score"]);
        // Every node scored; carol (a KNOWS sink) should out-rank alice.
        assert_eq!(qr.rows.len(), 4);
        let mut by_node: HashMap<String, f64> = HashMap::new();
        for r in 0..qr.rows.len() {
            let c = cells_of(&qr, r);
            by_node.insert(
                projected_id(&c[0]).unwrap().to_string(),
                c[1].as_f64().unwrap(),
            );
        }
        assert!(by_node["carol"] > by_node["alice"]);
    }

    #[test]
    fn call_gds_wcc_groups_components() {
        let v = fixture();
        // alice/bob/carol are one weakly-connected component; d1 is its own.
        let qr = exec_cypher(
            &v,
            "CALL gds.wcc() YIELD nodeId, componentId RETURN nodeId, componentId",
        )
        .unwrap();
        let mut comp: HashMap<String, i64> = HashMap::new();
        for r in 0..qr.rows.len() {
            let c = cells_of(&qr, r);
            comp.insert(
                projected_id(&c[0]).unwrap().to_string(),
                c[1].as_i64().unwrap(),
            );
        }
        assert_eq!(comp["alice"], comp["bob"]);
        assert_eq!(comp["bob"], comp["carol"]);
        assert_ne!(comp["alice"], comp["d1"]);
    }

    #[test]
    fn call_apoc_coll_sum_and_meta_stats() {
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL apoc.coll.sum([1, 2, 3, 4]) YIELD value RETURN value",
        )
        .unwrap();
        assert_eq!(cells_of(&qr, 0)[0], Value::Number(10.into()));

        let stats = exec_cypher(
            &v,
            "CALL apoc.meta.stats() YIELD nodeCount, relCount RETURN nodeCount, relCount",
        )
        .unwrap();
        let c = cells_of(&stats, 0);
        assert_eq!(c[0], Value::Number(4.into())); // 4 nodes
        assert_eq!(c[1], Value::Number(2.into())); // 2 KNOWS edges
    }

    #[test]
    fn call_proc_result_feeds_downstream_match() {
        // CONCEPT:EG-KG.query.eg-2 — a YIELD `nodeId` binds an anchorable node id, so a downstream
        // labelled MATCH re-anchors on it and filters by label (keeps only the Doc).
        let v = fixture();
        let qr = exec_cypher(
            &v,
            "CALL gds.degree() YIELD nodeId MATCH (nodeId:Doc) RETURN nodeId",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["d1"]);
    }

    // ── write path (CONCEPT:EG-KG.query.register-each-user-table / EG-061) ───────────────────────────────────

    fn col0(qr: &QueryResult) -> Vec<String> {
        let mut out: Vec<String> = qr
            .rows
            .iter()
            .map(|b| {
                let cells: Vec<Value> = rmp_serde::from_slice(b).unwrap();
                projected_id(&cells[0]).unwrap().to_string()
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
    fn create_persists_only_canonical_node_type_for_label() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'alice'})").unwrap();
        let stored = eg_types::msgpack::decode_property_value(
            &core.get_node_properties("alice").expect("created node"),
        )
        .unwrap();
        assert_eq!(
            stored.get("node_type"),
            Some(&Value::String("Person".into()))
        );
        assert!(stored.get("type").is_none());
        assert!(stored.get("label").is_none());
    }

    #[test]
    fn create_rejects_conflicting_explicit_node_type() {
        let core = GraphCore::new();
        let err = exec_cypher_write(
            &core,
            "CREATE (n:Person {id: 'alice', node_type: 'Service'})",
        )
        .unwrap_err();
        assert!(err.contains("conflicts"), "{err}");
        assert!(core.get_node_properties("alice").is_none());
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
        assert_eq!(projected_id(&cells[0]), Some("a"));
        assert_eq!(projected_id(&cells[1]), Some("b"));
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

    // ── REMOVE (CONCEPT:EG-KG.query.cypher-execution) ─────────────────────────────────────────────────

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
        // A node that is both Person (node_type) and carries Admin in a labels array.
        exec_cypher_write(&core, "CREATE (n:Person {id: 'al', name: 'Al'})").unwrap();
        // Give it a secondary label via SET (labels array), then REMOVE it.
        // (SET only takes literals; build the labels array through a fresh create.)
        let core2 = GraphCore::new();
        core2.add_node(
            "al".into(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "node_type": "Person", "labels": ["Admin"], "id": "al"
            }))
            .unwrap(),
        );
        // Remove the canonical label → no longer a Person.
        exec_cypher_write(&core2, "MATCH (n:Person) WHERE n.id = 'al' REMOVE n:Person").unwrap();
        let persons = exec_cypher_write(&core2, "MATCH (n:Person) RETURN n").unwrap();
        assert!(persons.rows.is_empty(), "canonical label removed");
        // Remove the array label → no longer an Admin.
        exec_cypher_write(&core2, "MATCH (n:Admin) WHERE n.id = 'al' REMOVE n:Admin").unwrap();
        let admins = exec_cypher_write(&core2, "MATCH (n:Admin) RETURN n").unwrap();
        assert!(admins.rows.is_empty(), "array label removed");
        let _ = core; // silence unused in this combined test
    }
}
