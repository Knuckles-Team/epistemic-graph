//! Cypher execution (CONCEPT:KG-2.179). Runs a parsed [`CypherQuery`] over an
//! off-lock `GraphView` and materializes a `QueryResult` — the SAME carrier the
//! SQL surface uses. NO DataFusion: every pattern shape compiles to one of the
//! engine's own primitives.
//!
//! Strategy per pattern shape (this is the "which maps to what" contract):
//!   * a node's `:Label` predicate            → the eg-core label index (the same
//!     `type`/`node_type`/`label`/`labels` fields `get_nodes_by_label` keys on),
//!     resolved here directly off the `GraphView`;
//!   * a fixed-shape multi-hop (all single hops) → a synthesized pattern
//!     `GraphView` whose edges carry `{"relationship": REL}` fed to
//!     `eg_core::graph::vf2_match_views` (VF2 enforces topology + edge type);
//!     labels + WHERE are enforced by post-filtering bindings against the label
//!     index and the predicate set;
//!   * any variable-length hop `*min..max`     → petgraph BFS over the `GraphView`
//!     from each candidate source, walking only edges whose relationship matches
//!     REL, collecting targets reached at a depth in `[min,max]`.

use std::collections::{HashMap, HashSet};

use eg_core::graph::{vf2_match_views, GraphCore, GraphView};
use petgraph::visit::EdgeRef;
use serde_json::Value;

// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::parser;
use super::plan::{
    CompareOp, CypherQuery, Direction, EdgePat, NodePat, Pattern, Predicate, SetItem, Statement,
    WriteOp, WriteQuery,
};

/// Implicit max rows (mirrors the SQL surface): one Response per Request, so an
/// unbounded RETURN would buffer the whole result in one message.
const MAX_ROWS: usize = 50_000;

/// Parse + run `cypher` over `view` (read-only, single graph). Synchronous and
/// dep-free — safe to call inside `spawn_blocking` like `exec_sql`.
pub fn exec_cypher(view: &GraphView, cypher: &str) -> Result<QueryResult, String> {
    let query = parser::parse(cypher)?;
    let bindings = resolve(view, &query)?;

    // RETURN projection → columns + msgpack-per-row.
    let columns: Vec<String> = query.returns.iter().map(|r| r.column()).collect();
    let cap = query.limit.unwrap_or(MAX_ROWS).min(MAX_ROWS);

    let mut rows: Vec<Vec<u8>> = Vec::new();
    for binding in bindings.iter() {
        if rows.len() >= cap {
            break;
        }
        let mut cells: Vec<Value> = Vec::with_capacity(query.returns.len());
        for item in &query.returns {
            let node_id = binding
                .get(&item.var)
                .ok_or_else(|| format!("RETURN refers to unbound variable '{}'", item.var))?;
            let cell = match &item.prop {
                // bare var → the node id
                None => Value::String(node_id.clone()),
                // var.prop → that property from the node's blob
                Some(p) => node_prop(view, node_id, p).unwrap_or(Value::Null),
            };
            cells.push(cell);
        }
        let blob = rmp_serde::to_vec(&cells).map_err(|e| format!("encode row: {e}"))?;
        rows.push(blob);
    }

    Ok(QueryResult { columns, rows })
}

// ── write path (CONCEPT:EG-020) ──────────────────────────────────────────────

/// Parse + run a Cypher statement that MAY mutate `core` — `CREATE`/`MERGE`/`SET`/
/// `[DETACH] DELETE`, with an optional leading `MATCH … WHERE` and trailing `RETURN`
/// (CONCEPT:EG-020). A pure-read `MATCH … RETURN` is delegated to the unchanged
/// snapshot read path, so this is the one entry-point a caller needs whether the
/// statement reads or writes. Writes map to eg-core's OWN native ops (`add_node`,
/// `add_edge`, `compare_and_set_fields`, `remove_node`, `remove_edge`) — NO
/// DataFusion — and `mark_dirty()` is called once after a mutation so the label
/// index / caches refresh.
pub fn exec_cypher_write(core: &GraphCore, cypher: &str) -> Result<QueryResult, String> {
    match parser::parse_statement(cypher)? {
        // A read: take an off-lock snapshot and run the untouched read path.
        Statement::Read(_) => {
            let view = core.analysis_snapshot();
            exec_cypher(&view, cypher)
        }
        Statement::Write(w) => exec_write(core, &w),
    }
}

/// Execute a parsed write statement against `core` (CONCEPT:EG-020).
fn exec_write(core: &GraphCore, w: &WriteQuery) -> Result<QueryResult, String> {
    // Resolve the leading MATCH (if any) over a snapshot into bindings. No MATCH ⇒
    // one empty binding (the write clauses run exactly once).
    let snap = core.analysis_snapshot();
    let mut bindings: Vec<HashMap<String, String>> = match &w.match_pattern {
        Some(pattern) => {
            let q = CypherQuery {
                pattern: pattern.clone(),
                where_clause: w.where_clause.clone(),
                returns: Vec::new(),
                limit: None,
            };
            resolve(&snap, &q)?
        }
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
                    if let (Some(a), Some(b)) =
                        (binding.get(&prev_var).cloned(), binding.get(&next_var).cloned())
                    {
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

    // RETURN (if present) projects over a FRESH post-write snapshot so `var.prop`
    // reflects the new values.
    if w.returns.is_empty() {
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
        });
    }
    let post = core.analysis_snapshot();
    project_bindings(&post, &bindings, &w.returns, None)
}

/// Apply ONE write clause for a single binding, extending the binding with any
/// newly created/merged variables (CONCEPT:EG-020).
fn apply_write_op(
    core: &GraphCore,
    snap: &GraphView,
    binding: &mut HashMap<String, String>,
    op: &WriteOp,
    mutated: &mut bool,
) -> Result<(), String> {
    match op {
        WriteOp::Create(pattern) => {
            apply_create(core, binding, pattern, mutated)?;
        }
        WriteOp::Merge(node) => {
            apply_merge(core, binding, node, mutated)?;
        }
        WriteOp::Set(items) => {
            apply_set(core, binding, items, mutated)?;
        }
        WriteOp::Delete { vars, detach } => {
            apply_delete(core, snap, binding, vars, *detach, mutated)?;
        }
    }
    Ok(())
}

/// `CREATE <pattern>`: realize each node (reuse a bound var, else create) and each
/// hop's edge (CONCEPT:EG-020).
fn apply_create(
    core: &GraphCore,
    binding: &mut HashMap<String, String>,
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
        core.add_edge(src, tgt, blob).map_err(|e| format!("CREATE edge: {e}"))?;
        *mutated = true;
        prev_id = next_id;
    }
    Ok(())
}

/// Resolve a CREATE node position to an id: reuse a bound variable, else create a new
/// node carrying its label (`type`) + inline props (CONCEPT:EG-020).
fn realize_node(
    core: &GraphCore,
    binding: &mut HashMap<String, String>,
    node: &NodePat,
    mutated: &mut bool,
) -> Result<String, String> {
    // A bound variable references an existing node — do NOT recreate it.
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
    // An explicit `id` property pins the node id; otherwise generate a fresh one.
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
/// absent. Idempotent across calls (the next call's snapshot includes the created
/// node). Binds `n` (CONCEPT:EG-020).
fn apply_merge(
    core: &GraphCore,
    binding: &mut HashMap<String, String>,
    node: &NodePat,
    mutated: &mut bool,
) -> Result<(), String> {
    let want = props_to_map(node.props.as_deref());
    // Look at the LIVE graph (label index) so a node created earlier this session is
    // seen. `limit 0` = all candidates for the label (or all nodes when label-less).
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
            return Ok(()); // matched an existing node — MERGE is a no-op.
        }
    }
    // No match → create it (reuse the CREATE node realizer).
    realize_node(core, binding, node, mutated)?;
    Ok(())
}

/// `SET v.prop = literal [, …]`: merge each assignment onto the bound node via the
/// engine's atomic `compare_and_set_fields` (empty conditions ⇒ unconditional merge)
/// (CONCEPT:EG-020).
fn apply_set(
    core: &GraphCore,
    binding: &HashMap<String, String>,
    items: &[SetItem],
    mutated: &mut bool,
) -> Result<(), String> {
    // Group assignments by target variable so each node is updated once.
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
        let applied = core.compare_and_set_fields(id, &conditions, &updates);
        if applied {
            *mutated = true;
        }
    }
    Ok(())
}

/// `[DETACH] DELETE v [, …]`: remove bound node variables (DETACH also drops their
/// incident edges), or a bound edge variable's edge (CONCEPT:EG-020).
fn apply_delete(
    core: &GraphCore,
    snap: &GraphView,
    binding: &HashMap<String, String>,
    vars: &[String],
    detach: bool,
    mutated: &mut bool,
) -> Result<(), String> {
    for var in vars {
        // An edge variable (bound to `src\u{0}tgt`) deletes that edge.
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

/// The binding key an edge variable is stored under (namespaced so it never collides
/// with a node variable of the same name).
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
    use std::sync::atomic::{AtomicU64, Ordering};
    static CTR: AtomicU64 = AtomicU64::new(0);
    let n = CTR.fetch_add(1, Ordering::Relaxed);
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

/// Project a set of bindings into a [`QueryResult`] (shared by the read path and the
/// write RETURN). `cap` bounds the row count.
fn project_bindings(
    view: &GraphView,
    bindings: &[HashMap<String, String>],
    returns: &[super::plan::ReturnItem],
    cap: Option<usize>,
) -> Result<QueryResult, String> {
    let columns: Vec<String> = returns.iter().map(|r| r.column()).collect();
    let cap = cap.unwrap_or(MAX_ROWS).min(MAX_ROWS);
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for binding in bindings {
        if rows.len() >= cap {
            break;
        }
        let mut cells: Vec<Value> = Vec::with_capacity(returns.len());
        for item in returns {
            let node_id = binding
                .get(&item.var)
                .ok_or_else(|| format!("RETURN refers to unbound variable '{}'", item.var))?;
            let cell = match &item.prop {
                None => Value::String(node_id.clone()),
                Some(p) => node_prop(view, node_id, p).unwrap_or(Value::Null),
            };
            cells.push(cell);
        }
        rows.push(rmp_serde::to_vec(&cells).map_err(|e| format!("encode row: {e}"))?);
    }
    Ok(QueryResult { columns, rows })
}

/// Resolve the MATCH pattern into a list of variable→node-id bindings, applying
/// WHERE. Dispatches to the right primitive based on the pattern shape.
fn resolve(view: &GraphView, query: &CypherQuery) -> Result<Vec<HashMap<String, String>>, String> {
    let pattern = &query.pattern;

    // Single node: pure label-index lookup.
    if pattern.hops.is_empty() {
        let var = pattern
            .start
            .var
            .clone()
            .ok_or("start node needs a variable to RETURN")?;
        let candidates = label_candidates(view, &pattern.start);
        let mut out: Vec<HashMap<String, String>> = Vec::new();
        for id in candidates {
            let mut b = HashMap::new();
            b.insert(var.clone(), id);
            if where_holds(view, &b, &query.where_clause) {
                out.push(b);
            }
        }
        return Ok(out);
    }

    // Any variable-length hop ⇒ the BFS path engine (handles a single such hop —
    // the documented subset).
    if pattern.hops.iter().any(|(e, _)| e.var_len.is_some()) {
        return resolve_var_length(view, query);
    }

    // Otherwise a fixed-shape multi-hop ⇒ synthesize a pattern GraphView and reuse
    // VF2; then post-filter on labels + WHERE.
    resolve_fixed(view, query)
}

/// Fixed-shape multi-hop via `vf2_match_views`. The pattern GraphView carries the
/// edge relationship type so VF2's edge-prop subset check enforces it; node labels
/// and WHERE are enforced after VF2 returns bindings.
fn resolve_fixed(
    view: &GraphView,
    query: &CypherQuery,
) -> Result<Vec<HashMap<String, String>>, String> {
    let pattern = &query.pattern;

    // Assign a stable variable to every node position (auto-name the anonymous ones
    // so VF2 has distinct pattern nodes).
    let mut var_names: Vec<String> = Vec::new();
    var_names.push(node_var(&pattern.start, 0));
    for (i, (_, n)) in pattern.hops.iter().enumerate() {
        var_names.push(node_var(n, i + 1));
    }

    // Build the pattern GraphView: one node per position, one edge per hop with
    // `{"relationship": REL}` so VF2 matches the edge type. Direction picks the
    // (src,tgt) order.
    let mut pat = GraphView::default();
    for v in &var_names {
        let idx = pat.graph.add_node(v.clone());
        pat.node_map.insert(v.clone(), idx);
        // Empty props ⇒ VF2 matches topology only; labels are post-filtered.
        pat.node_properties.insert(
            v.clone(),
            std::sync::Arc::new(rmp_serde::to_vec(&Value::Object(Default::default())).unwrap()),
        );
    }
    for (i, (edge, _)) in pattern.hops.iter().enumerate() {
        let (a, b) = (&var_names[i], &var_names[i + 1]);
        let (src, tgt) = match edge.direction {
            Direction::Right => (a, b),
            Direction::Left => (b, a),
        };
        let (si, ti) = (pat.node_map[src], pat.node_map[tgt]);
        pat.graph.add_edge(si, ti, format!("{src}:{tgt}"));
        if let Some(rel) = &edge.rel_type {
            let mut m = serde_json::Map::new();
            m.insert("relationship".into(), Value::String(rel.clone()));
            pat.edge_properties.insert(
                (src.clone(), tgt.clone()),
                vec![std::sync::Arc::new(
                    rmp_serde::to_vec(&Value::Object(m)).unwrap(),
                )],
            );
        }
    }

    let raw_matches = vf2_match_views(view, &pat);

    // Per-position label candidate sets (None ⇒ no constraint).
    let mut label_sets: Vec<Option<HashSet<String>>> = Vec::new();
    label_sets.push(label_set(view, &pattern.start));
    for (_, n) in &pattern.hops {
        label_sets.push(label_set(view, n));
    }

    let mut out: Vec<HashMap<String, String>> = Vec::new();
    for m in raw_matches {
        // m maps pattern-var → host-id. Apply label filters.
        let mut ok = true;
        for (pos, var) in var_names.iter().enumerate() {
            if let Some(set) = &label_sets[pos] {
                let host = match m.get(var) {
                    Some(h) => h,
                    None => {
                        ok = false;
                        break;
                    }
                };
                if !set.contains(host) {
                    ok = false;
                    break;
                }
            }
        }
        if ok && where_holds(view, &m, &query.where_clause) {
            out.push(m);
        }
    }
    Ok(out)
}

/// Variable-length path via petgraph BFS. The documented subset: exactly one
/// variable-length hop in the pattern (`(a)-[:REL*min..max]->(b)`); the optional
/// surrounding fixed hops are not combined with it. Resolves all `(a,b)` pairs
/// where `b` is reachable from `a` over `min..=max` REL-typed edges.
fn resolve_var_length(
    view: &GraphView,
    query: &CypherQuery,
) -> Result<Vec<HashMap<String, String>>, String> {
    let pattern = &query.pattern;
    if pattern.hops.len() != 1 {
        return Err("variable-length paths are supported only as a single hop \
                    `(a)-[:REL*m..n]->(b)`"
            .into());
    }
    let (edge, end_node) = &pattern.hops[0];
    let (min, max) = edge.var_len.expect("checked by caller");
    let start_var = pattern
        .start
        .var
        .clone()
        .ok_or("start node needs a variable")?;
    let end_var = end_node.var.clone().ok_or("end node needs a variable")?;

    let starts = label_candidates(view, &pattern.start);
    let end_filter = label_set(view, end_node);

    let mut out: Vec<HashMap<String, String>> = Vec::new();
    for s in &starts {
        for target in bfs_reachable(view, s, edge, min, max) {
            if let Some(set) = &end_filter {
                if !set.contains(&target) {
                    continue;
                }
            }
            let mut b = HashMap::new();
            b.insert(start_var.clone(), s.clone());
            b.insert(end_var.clone(), target);
            if where_holds(view, &b, &query.where_clause) {
                out.push(b);
            }
        }
    }
    Ok(out)
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
                // The two endpoint ids for the relationship-type lookup, oriented
                // src→tgt as the edge is stored.
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

/// Same as [`label_candidates`] but as a set for O(1) membership post-filtering;
/// `None` ⇒ no label constraint.
fn label_set(view: &GraphView, node: &NodePat) -> Option<HashSet<String>> {
    node.label
        .as_ref()
        .map(|_| label_candidates(view, node).into_iter().collect())
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

/// All WHERE predicates hold for this binding (conjunctive).
fn where_holds(view: &GraphView, binding: &HashMap<String, String>, preds: &[Predicate]) -> bool {
    preds.iter().all(|p| pred_holds(view, binding, p))
}

fn pred_holds(view: &GraphView, binding: &HashMap<String, String>, p: &Predicate) -> bool {
    let Some(node_id) = binding.get(&p.var) else {
        return false;
    };
    let actual = node_prop(view, node_id, &p.prop);
    compare(actual.as_ref(), &p.op, &p.value)
}

/// Compare an actual property value against the predicate literal. Equality works
/// across JSON types; ordering works on numbers and strings. A missing value only
/// satisfies `!=`.
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

fn apply_ord(op: &CompareOp, ord: Option<std::cmp::Ordering>) -> bool {
    use std::cmp::Ordering::*;
    let Some(ord) = ord else { return false };
    match op {
        CompareOp::Lt => ord == Less,
        CompareOp::Le => ord != Greater,
        CompareOp::Gt => ord == Greater,
        CompareOp::Ge => ord != Less,
        _ => false,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// The variable name for a node position, auto-naming anonymous nodes so VF2 has
/// distinct pattern node ids.
fn node_var(node: &NodePat, pos: usize) -> String {
    node.var.clone().unwrap_or_else(|| format!("__anon{pos}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;

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
        // alice-KNOWS->bob is the only 2-node KNOWS pattern starting at a named
        // Person and ending at a Person (and bob->carol).
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

        // Cross-check against the engine's own VF2 over an equivalent hand-built
        // pattern (edge carries the relationship, nodes are label-free).
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
        // VF2 finds the same two directed KNOWS edges.
        assert_eq!(vf2.len(), 2);
    }

    #[test]
    fn variable_length_path_reaches_transitively() {
        let v = fixture();
        // alice -KNOWS*1..2-> reaches bob (1 hop) and carol (2 hops).
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["bob", "carol"]);

        // Depth 1 only ⇒ just bob.
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
        let cells: Vec<Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        assert_eq!(cells[0], Value::Number(42.into()));
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

    // ── write path (CONCEPT:EG-020) ───────────────────────────────────────────

    /// IDs of a single-column QueryResult (the `ids` helper takes a GraphView).
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
        // The property round-trips.
        let qr2 = exec_cypher_write(&core, "MATCH (a:Person) RETURN a.name").unwrap();
        let cells: Vec<Value> = rmp_serde::from_slice(&qr2.rows[0]).unwrap();
        assert_eq!(cells[0], Value::String("Alice".into()));
    }

    #[test]
    fn create_edge_between_new_nodes() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "CREATE (a:Person {id: 'a'})-[:KNOWS]->(b:Person {id: 'b'})",
        )
        .unwrap();
        let qr = exec_cypher_write(
            &core,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
        )
        .unwrap();
        let cells: Vec<Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        assert_eq!(cells[0], Value::String("a".into()));
        assert_eq!(cells[1], Value::String("b".into()));
    }

    #[test]
    fn merge_is_idempotent() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "MERGE (n:City {id: 'paris', name: 'Paris'})").unwrap();
        exec_cypher_write(&core, "MERGE (n:City {id: 'paris', name: 'Paris'})").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (c:City) RETURN c").unwrap();
        assert_eq!(col0(&qr), vec!["paris"], "MERGE created exactly one node");
    }

    #[test]
    fn set_updates_property() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'bob', rank: 1})").unwrap();
        exec_cypher_write(&core, "MATCH (n:Person) WHERE n.id = 'bob' SET n.rank = 9").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n.rank").unwrap();
        let cells: Vec<Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        assert_eq!(cells[0], Value::Number(9.into()));
    }

    #[test]
    fn delete_removes_node() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'carol'})").unwrap();
        exec_cypher_write(&core, "MATCH (n:Person) WHERE n.id = 'carol' DELETE n").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert!(qr.rows.is_empty(), "node was deleted");
    }

    #[test]
    fn detach_delete_drops_edges_then_node() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "CREATE (a:Person {id: 'x'})-[:KNOWS]->(b:Person {id: 'y'})",
        )
        .unwrap();
        // A plain DELETE on a node with an edge is refused.
        let err = exec_cypher_write(&core, "MATCH (a:Person) WHERE a.id = 'x' DELETE a")
            .unwrap_err();
        assert!(err.contains("DETACH"), "{err}");
        // DETACH DELETE succeeds.
        exec_cypher_write(&core, "MATCH (a:Person) WHERE a.id = 'x' DETACH DELETE a").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(col0(&qr), vec!["y"], "only the detached node remains");
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
        // The edge is gone; both nodes remain.
        let qr = exec_cypher_write(
            &core,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a",
        )
        .unwrap();
        assert!(qr.rows.is_empty(), "edge deleted");
        let nodes = exec_cypher_write(&core, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(col0(&nodes), vec!["p", "q"]);
    }

    #[test]
    fn create_with_return_projects_new_node() {
        let core = GraphCore::new();
        let qr = exec_cypher_write(&core, "CREATE (n:Task {id: 't1', state: 'open'}) RETURN n.state")
            .unwrap();
        assert_eq!(qr.columns, vec!["n.state"]);
        let cells: Vec<Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        assert_eq!(cells[0], Value::String("open".into()));
    }
}
