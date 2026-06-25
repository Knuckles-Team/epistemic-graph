//! W2 — Native SPARQL 1.1 evaluation over a `GraphView` (CONCEPT:KG-2.218).
//!
//! DECISION (embed-vs-compile, from the spike): we COMPILE `spargebra`'s parsed
//! algebra down to scans over OUR property-graph `GraphView`, rather than EMBED
//! oxigraph's evaluator over an adapter. Why this side:
//!   * spargebra is parser+algebra ONLY (no store, no async, tiny dep) — it gives
//!     the SPARQL 1.1 grammar + the typed `GraphPattern` algebra for free.
//!   * The evaluator walks that algebra resolving each triple pattern against the
//!     SAME `node_properties` / `edge_properties` / topology the eg-plan executor
//!     reads. So a SPARQL BGP is literally "more scans over the one substrate" — no
//!     second copy of the graph, no oxigraph store to keep in sync.
//!
//! Increment-1 algebra coverage: BGP (triple-pattern match + join on shared vars),
//! FILTER (Bound + comparison + And/Or/Not), OPTIONAL (left-join), UNION, JOIN,
//! PROJECT, DISTINCT, SLICE, and a BASIC fixed-length property path (`p1/p2` seq
//! and a single predicate).
//!
//! Completeness increment (CONCEPT:KG-2.235): aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/
//! `MAX` with `GROUP BY` — the `Group`+`Extend` algebra), the fuller property paths
//! (`p+` / `p*` / `p?`, alternative `a|b`, inverse `^p`, and their nesting), and the
//! `GRAPH ?g { … }` named-graph form (a single dataset here ⇒ `?g` binds the request
//! graph). Sub-SELECT / SERVICE stay deferred (see the findings "SPARQL completeness").
//!
//! Performance note (carried from the spike): this evaluator does a full scan per
//! triple pattern + a materialized join. That is the documented naive-evaluator gap
//! — an SPO/POS index + selectivity join-ordering is W2 follow-on, not a substrate
//! limitation.

use std::collections::HashMap;

use eg_core::graph::GraphView;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

/// One solution: variable name → bound term (in our node-id / literal lexical form).
pub type Solution = HashMap<String, Binding>;

/// A bound value: a graph-node id (`<iri>` / `_:b`) or a literal lexical value.
#[derive(Clone, Debug, PartialEq)]
pub enum Binding {
    Node(String),
    Literal(String),
}

impl Binding {
    pub fn as_str(&self) -> &str {
        match self {
            Binding::Node(s) | Binding::Literal(s) => s,
        }
    }
    pub fn is_node(&self) -> bool {
        matches!(self, Binding::Node(_))
    }
}

/// A materialized SELECT result: the projected variable order + the solution rows.
#[derive(Debug, Clone)]
pub struct SparqlResult {
    pub vars: Vec<String>,
    pub solutions: Vec<Solution>,
}

impl SparqlResult {
    /// Project to a wire-friendly row table: `vars` columns, each row a
    /// `Vec<Option<String>>` aligned to `vars` (None = unbound). Lets the protocol
    /// return a flat `{columns, rows}` shape matching `Sql`/`Cypher`.
    pub fn to_rows(&self) -> (Vec<String>, Vec<Vec<Option<String>>>) {
        let rows = self
            .solutions
            .iter()
            .map(|s| {
                self.vars
                    .iter()
                    .map(|v| s.get(v).map(|b| b.as_str().to_string()))
                    .collect()
            })
            .collect();
        (self.vars.clone(), rows)
    }
}

/// Parse a SPARQL 1.1 query string into the spargebra algebra.
pub fn parse_query(q: &str) -> Result<Query, String> {
    Query::parse(q, None).map_err(|e| format!("sparql parse: {e}"))
}

/// Parse + evaluate a SPARQL SELECT over the GraphView in one call.
pub fn run(view: &GraphView, query_str: &str) -> Result<SparqlResult, String> {
    let q = parse_query(query_str)?;
    evaluate(view, &q)
}

/// Evaluate a parsed SELECT query over the GraphView.
pub fn evaluate(view: &GraphView, query: &Query) -> Result<SparqlResult, String> {
    match query {
        Query::Select { pattern, .. } => {
            let solutions = eval_pattern(view, pattern)?;
            let vars = collect_vars(pattern);
            Ok(SparqlResult { vars, solutions })
        }
        _ => Err("eg-rdf SPARQL supports SELECT only".into()),
    }
}

fn collect_vars(p: &GraphPattern) -> Vec<String> {
    let mut vs = Vec::new();
    p.on_in_scope_variable(|v| {
        let n = v.as_str().to_string();
        if !vs.contains(&n) {
            vs.push(n);
        }
    });
    vs
}

/// The algebra walker.
fn eval_pattern(view: &GraphView, p: &GraphPattern) -> Result<Vec<Solution>, String> {
    match p {
        GraphPattern::Bgp { patterns } => Ok(eval_bgp(view, patterns)),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => eval_path(view, subject, path, object),
        GraphPattern::Filter { expr, inner } => {
            let inner_sols = eval_pattern(view, inner)?;
            Ok(inner_sols
                .into_iter()
                .filter(|s| eval_filter(expr, s))
                .collect())
        }
        GraphPattern::Join { left, right } => {
            let l = eval_pattern(view, left)?;
            let r = eval_pattern(view, right)?;
            Ok(hash_join(&l, &r))
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            // OPTIONAL: keep every left solution; extend with a compatible right
            // (passing the optional FILTER) where one exists.
            let l = eval_pattern(view, left)?;
            let r = eval_pattern(view, right)?;
            Ok(left_join(&l, &r, expression.as_ref()))
        }
        GraphPattern::Union { left, right } => {
            let mut l = eval_pattern(view, left)?;
            let mut r = eval_pattern(view, right)?;
            l.append(&mut r);
            Ok(l)
        }
        GraphPattern::Project { inner, .. } => eval_pattern(view, inner),
        // GROUP BY + aggregates (CONCEPT:KG-2.235). `Group` produces one solution per
        // group binding the GROUP BY vars + the aggregate-result vars; the wrapping
        // `Extend` (below) re-binds those to the projected names. With no GROUP BY var
        // the whole result is one group (`SELECT (COUNT(*) AS ?n) …`).
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let rows = eval_pattern(view, inner)?;
            Ok(eval_group(rows, variables, aggregates))
        }
        // BIND / the aggregate-projection rename. `Extend` binds `variable` to the
        // value of `expression` in each solution. We evaluate the (already-aggregated
        // or scalar) expression and bind it; an unevaluable expression leaves it
        // unbound (SPARQL: an error in Extend yields no binding for that var).
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let rows = eval_pattern(view, inner)?;
            Ok(rows
                .into_iter()
                .map(|mut s| {
                    if let Some(val) = expr_str(expression, &s) {
                        s.insert(variable.as_str().to_string(), Binding::Literal(val));
                    }
                    s
                })
                .collect())
        }
        // GRAPH ?g { … } (CONCEPT:KG-2.235). The engine evaluates over ONE graph (the
        // request graph), so the named-graph form binds `?g` to that single graph's
        // name and evaluates the inner pattern against it. A constant graph IRI passes
        // through (it selects the same single dataset).
        GraphPattern::Graph { name, inner } => {
            let inner_sols = eval_pattern(view, inner)?;
            match name {
                NamedNodePattern::Variable(v) => {
                    // ONE dataset here: `?g` binds the (single) request graph's IRI.
                    let g = Binding::Node(DEFAULT_GRAPH_IRI.to_string());
                    Ok(inner_sols
                        .into_iter()
                        .map(|mut s| {
                            s.entry(v.as_str().to_string()).or_insert_with(|| g.clone());
                            s
                        })
                        .collect())
                }
                NamedNodePattern::NamedNode(_) => Ok(inner_sols),
            }
        }
        GraphPattern::Distinct { inner } => {
            let mut seen = std::collections::HashSet::new();
            Ok(eval_pattern(view, inner)?
                .into_iter()
                .filter(|s| seen.insert(canonical_solution(s)))
                .collect())
        }
        GraphPattern::Reduced { inner } => eval_pattern(view, inner),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let all = eval_pattern(view, inner)?;
            let end = length.map(|l| start + l).unwrap_or(all.len());
            Ok(all
                .into_iter()
                .skip(*start)
                .take(end.saturating_sub(*start))
                .collect())
        }
        other => Err(format!("eg-rdf SPARQL: unsupported algebra node {other:?}")),
    }
}

fn canonical_solution(s: &Solution) -> String {
    let mut kv: Vec<_> = s
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().to_string()))
        .collect();
    kv.sort();
    format!("{kv:?}")
}

/// The IRI `?g` binds to in a `GRAPH ?g {}` over the single request dataset.
const DEFAULT_GRAPH_IRI: &str = "<urn:eg:graph:default>";

// ── GROUP BY + aggregates (CONCEPT:KG-2.235) ────────────────────────────────────

/// Evaluate `GROUP BY group_vars` + the `aggregates` over `rows`. Returns one
/// solution per distinct group-key, binding each group-by var to its value AND each
/// aggregate-result var (the internal name spargebra assigns) to the computed scalar.
/// The wrapping `Extend` re-binds those internal vars to the user's projected names.
fn eval_group(
    rows: Vec<Solution>,
    group_vars: &[spargebra::term::Variable],
    aggregates: &[(spargebra::term::Variable, AggregateExpression)],
) -> Vec<Solution> {
    use std::collections::BTreeMap;

    // Bucket rows by the tuple of group-by values (a stable string key keeps the
    // result deterministic). With no GROUP BY var, ALL rows fall in one "" group.
    let mut groups: BTreeMap<String, Vec<Solution>> = BTreeMap::new();
    for row in rows {
        let key = group_vars
            .iter()
            .map(|v| {
                row.get(v.as_str())
                    .map(|b| b.as_str().to_string())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\u{1f}");
        groups.entry(key).or_default().push(row);
    }

    let mut out = Vec::new();
    for (_key, members) in groups {
        let mut sol = Solution::new();
        // Carry the group-by var values (taken from the first member of the group).
        if let Some(first) = members.first() {
            for gv in group_vars {
                if let Some(b) = first.get(gv.as_str()) {
                    sol.insert(gv.as_str().to_string(), b.clone());
                }
            }
        }
        // Compute each aggregate over the group's members.
        for (out_var, agg) in aggregates {
            let value = compute_aggregate(agg, &members);
            sol.insert(out_var.as_str().to_string(), Binding::Literal(value));
        }
        out.push(sol);
    }
    out
}

/// Compute ONE aggregate over a group's member solutions, returning its lexical value.
fn compute_aggregate(agg: &AggregateExpression, members: &[Solution]) -> String {
    match agg {
        // COUNT(*) — count solutions (DISTINCT counts distinct whole solutions).
        AggregateExpression::CountSolutions { distinct } => {
            let n = if *distinct {
                let mut seen = std::collections::HashSet::new();
                members
                    .iter()
                    .filter(|s| seen.insert(canonical_solution(s)))
                    .count()
            } else {
                members.len()
            };
            n.to_string()
        }
        AggregateExpression::FunctionCall {
            name,
            expr,
            distinct,
        } => {
            // The per-row values of the aggregated expression (skipping unbound rows).
            let mut vals: Vec<String> = members.iter().filter_map(|s| expr_str(expr, s)).collect();
            if *distinct {
                let mut seen = std::collections::HashSet::new();
                vals.retain(|v| seen.insert(v.clone()));
            }
            agg_over(name, &vals)
        }
    }
}

/// Apply an aggregate function to the already-collected per-row lexical values.
fn agg_over(func: &AggregateFunction, vals: &[String]) -> String {
    let nums: Vec<f64> = vals.iter().filter_map(|v| v.parse::<f64>().ok()).collect();
    match func {
        AggregateFunction::Count => vals.len().to_string(),
        AggregateFunction::Sum => fmt_num(nums.iter().sum::<f64>()),
        AggregateFunction::Avg => {
            if nums.is_empty() {
                "0".to_string()
            } else {
                fmt_num(nums.iter().sum::<f64>() / nums.len() as f64)
            }
        }
        AggregateFunction::Min => {
            if nums.is_empty() {
                vals.iter().min().cloned().unwrap_or_default()
            } else {
                fmt_num(nums.iter().cloned().fold(f64::INFINITY, f64::min))
            }
        }
        AggregateFunction::Max => {
            if nums.is_empty() {
                vals.iter().max().cloned().unwrap_or_default()
            } else {
                fmt_num(nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
            }
        }
        AggregateFunction::GroupConcat { separator } => {
            vals.join(separator.as_deref().unwrap_or(" "))
        }
        AggregateFunction::Sample => vals.first().cloned().unwrap_or_default(),
        AggregateFunction::Custom(_) => String::new(),
    }
}

/// Format an f64 aggregate result without a trailing `.0` for integral values, so a
/// `SUM`/`COUNT` of integers reads as an integer (matching the stored lexical form).
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

// ── BGP — match each triple pattern, join on shared variables ───────────────────

fn eval_bgp(view: &GraphView, patterns: &[TriplePattern]) -> Vec<Solution> {
    let mut acc: Vec<Solution> = vec![Solution::new()];
    for tp in patterns {
        let matches = match_triple_pattern(view, tp);
        let mut next = Vec::new();
        for base in &acc {
            for m in &matches {
                if let Some(merged) = merge(base, m) {
                    next.push(merged);
                }
            }
        }
        acc = next;
        if acc.is_empty() {
            break;
        }
    }
    acc
}

/// Property-path evaluation (CONCEPT:KG-2.235). spargebra DESUGARS a sequence path
/// (`p1/p2`) into a BGP with anonymous-bnode intermediates, so a single-predicate
/// path reaching here is handled by the one-triple-pattern matcher. The variable-
/// length / combinator forms (`p+`, `p*`, `p?`, alternative `a|b`, inverse `^p`, and
/// their nesting) are evaluated by [`path_pairs`]: it computes the `(start, end)`
/// resource pairs the path connects, then binds subject/object against them.
fn eval_path(
    view: &GraphView,
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Result<Vec<Solution>, String> {
    // A single named predicate stays the literal/edge triple-pattern matcher (it also
    // matches literal-valued predicates, which the resource-only path engine doesn't).
    if let PropertyPathExpression::NamedNode(n) = path {
        let pred = oxrdf::NamedNode::new(n.as_str()).map_err(|e| e.to_string())?;
        let tp = TriplePattern {
            subject: subject.clone(),
            predicate: NamedNodePattern::NamedNode(pred),
            object: object.clone(),
        };
        return Ok(match_triple_pattern(view, &tp));
    }

    // The combinator forms resolve over RESOURCE edges (a property path connects nodes,
    // not literals). Enumerate the connected pairs, then bind the subject/object terms.
    let pairs = path_pairs(view, path)?;
    let mut out = Vec::new();
    for (s, o) in pairs {
        let mut sol = Solution::new();
        if !bind_subject(subject, &s, &mut sol) {
            continue;
        }
        if !bind_object_node(object, &o, &mut sol) {
            continue;
        }
        out.push(sol);
    }
    Ok(out)
}

/// All `(start, end)` resource-node id pairs the property `path` connects, over the
/// GraphView's typed edges. Recurses on the path combinators:
///   * `NamedNode(p)`  → every edge typed `p`.
///   * `Reverse(p)`    → the pairs of `p` flipped (`^p`).
///   * `Sequence(a,b)` → join: `a` then `b` (shared midpoint).
///   * `Alternative(a,b)` → the union of both.
///   * `OneOrMore(p)`  → transitive closure (`p+`, ≥1 hop).
///   * `ZeroOrMore(p)` → reflexive-transitive closure (`p*`, incl. identity on EVERY
///     node, per SPARQL `x p* x`).
///   * `ZeroOrOne(p)`  → `p` ∪ identity (`p?`).
fn path_pairs(
    view: &GraphView,
    path: &PropertyPathExpression,
) -> Result<Vec<(String, String)>, String> {
    Ok(match path {
        PropertyPathExpression::NamedNode(n) => edge_pairs(view, n.as_str()),
        PropertyPathExpression::Reverse(inner) => path_pairs(view, inner)?
            .into_iter()
            .map(|(s, o)| (o, s))
            .collect(),
        PropertyPathExpression::Sequence(a, b) => {
            let left = path_pairs(view, a)?;
            let right = path_pairs(view, b)?;
            let mut out = Vec::new();
            for (s, mid) in &left {
                for (rs, o) in &right {
                    if rs == mid {
                        out.push((s.clone(), o.clone()));
                    }
                }
            }
            dedup_pairs(out)
        }
        PropertyPathExpression::Alternative(a, b) => {
            let mut out = path_pairs(view, a)?;
            out.extend(path_pairs(view, b)?);
            dedup_pairs(out)
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let base = path_pairs(view, inner)?;
            transitive_closure(&base, false, view)
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let base = path_pairs(view, inner)?;
            transitive_closure(&base, true, view)
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let mut out = path_pairs(view, inner)?;
            // identity on every node (`x p? x`).
            for id in view.node_properties.keys() {
                out.push((id.clone(), id.clone()));
            }
            dedup_pairs(out)
        }
        PropertyPathExpression::NegatedPropertySet(_) => {
            return Err("eg-rdf SPARQL: negated property set `!p` not supported".into());
        }
    })
}

/// Every `(subject, object)` resource pair carrying a typed edge `pred`.
fn edge_pairs(view: &GraphView, pred: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                if v.get("type").and_then(|x| x.as_str()) == Some(pred) {
                    out.push((s.clone(), o.clone()));
                    break;
                }
            }
        }
    }
    out
}

/// Transitive closure of `base` edge pairs. `reflexive` adds the identity pair on
/// EVERY graph node (the `p*` semantics: `x p* x` for any `x`, even isolated nodes).
fn transitive_closure(
    base: &[(String, String)],
    reflexive: bool,
    view: &GraphView,
) -> Vec<(String, String)> {
    use std::collections::{HashMap, HashSet};
    // adjacency.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (s, o) in base {
        adj.entry(s.as_str()).or_default().push(o.as_str());
    }
    let starts: HashSet<&str> = base.iter().map(|(s, _)| s.as_str()).collect();
    let mut out: HashSet<(String, String)> = HashSet::new();
    // BFS reachability (≥1 hop) from each start.
    for &start in &starts {
        let mut stack = vec![start];
        let mut visited: HashSet<&str> = HashSet::new();
        while let Some(cur) = stack.pop() {
            if let Some(next) = adj.get(cur) {
                for &n in next {
                    if visited.insert(n) {
                        out.insert((start.to_string(), n.to_string()));
                        stack.push(n);
                    }
                }
            }
        }
    }
    if reflexive {
        for id in view.node_properties.keys() {
            out.insert((id.clone(), id.clone()));
        }
    }
    out.into_iter().collect()
}

fn dedup_pairs(v: Vec<(String, String)>) -> Vec<(String, String)> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    v.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// Resolve ONE triple pattern against the GraphView. Predicate may be an IRI or a
/// variable; object an IRI/literal/variable; subject an IRI/bnode/variable.
fn match_triple_pattern(view: &GraphView, tp: &TriplePattern) -> Vec<Solution> {
    let mut out = Vec::new();

    // EDGE patterns: object is a resource. Scan edges.
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            let v: serde_json::Value = match rmp_serde::from_slice(blob.as_slice()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(pred) = v.get("type").and_then(|x| x.as_str()) else {
                continue;
            };
            let mut sol = Solution::new();
            if !bind_subject(&tp.subject, s, &mut sol) {
                continue;
            }
            if !bind_predicate_iri(&tp.predicate, pred, &mut sol) {
                continue;
            }
            if !bind_object_node(&tp.object, o, &mut sol) {
                continue;
            }
            out.push(sol);
        }
    }

    // LITERAL patterns: object is a literal. Scan node property cells.
    for (id, blob) in &view.node_properties {
        let v: serde_json::Value = match rmp_serde::from_slice(blob.as_slice()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(obj) = v.as_object() else { continue };
        for (k, cell) in obj {
            // `type` is exported as an rdf:type edge separately; skip here.
            if k == "type" {
                continue;
            }
            let Some(lit_val) = cell.get("value").and_then(|x| x.as_str()) else {
                continue;
            };
            let mut sol = Solution::new();
            if !bind_subject(&tp.subject, id, &mut sol) {
                continue;
            }
            if !bind_predicate_iri(&tp.predicate, k, &mut sol) {
                continue;
            }
            if !bind_object_literal(&tp.object, lit_val, &mut sol) {
                continue;
            }
            out.push(sol);
        }
    }

    out
}

/// The variable name a query blank node binds under. A blank node in a QUERY
/// pattern is a non-distinguished variable (SPARQL semantics) — spargebra desugars
/// a sequence property path `p1/p2` into a BGP whose intermediate is exactly such a
/// bnode. We key it in the solution by a reserved name so it joins like any other
/// variable but is dropped from the projected output (it isn't in `collect_vars`).
fn bnode_var(b: &spargebra::term::BlankNode) -> String {
    format!("__bnode__{}", b.as_str())
}

fn bind_subject(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        _ => false,
    }
}

fn bind_predicate_iri(pat: &NamedNodePattern, pred_iri: &str, sol: &mut Solution) -> bool {
    match pat {
        NamedNodePattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(pred_iri.to_string()));
            true
        }
        NamedNodePattern::NamedNode(n) => n.as_str() == pred_iri,
    }
}

fn bind_object_node(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::Literal(_) => false, // a literal pattern can't match a resource
    }
}

fn bind_object_literal(pat: &TermPattern, lit_val: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(
                v.as_str().to_string(),
                Binding::Literal(lit_val.to_string()),
            );
            true
        }
        TermPattern::Literal(l) => l.value() == lit_val,
        _ => false, // a resource pattern can't match a literal
    }
}

/// Merge two solutions if they agree on every shared variable.
fn merge(a: &Solution, b: &Solution) -> Option<Solution> {
    let mut out = a.clone();
    for (k, v) in b {
        match out.get(k) {
            Some(existing) if existing != v => return None,
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Some(out)
}

fn hash_join(l: &[Solution], r: &[Solution]) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        for b in r {
            if let Some(m) = merge(a, b) {
                out.push(m);
            }
        }
    }
    out
}

fn left_join(l: &[Solution], r: &[Solution], filter: Option<&Expression>) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        let mut matched = false;
        for b in r {
            if let Some(m) = merge(a, b) {
                if filter.map(|e| eval_filter(e, &m)).unwrap_or(true) {
                    out.push(m);
                    matched = true;
                }
            }
        }
        if !matched {
            out.push(a.clone()); // OPTIONAL: keep the un-extended left solution
        }
    }
    out
}

// ── FILTER — a small expression evaluator (the increment's subset) ──────────────

fn eval_filter(expr: &Expression, sol: &Solution) -> bool {
    eval_expr_bool(expr, sol).unwrap_or(false)
}

fn eval_expr_bool(expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::Bound(v) => Some(sol.contains_key(v.as_str())),
        Expression::Equal(a, b) => Some(expr_str(a, sol)? == expr_str(b, sol)?),
        Expression::Greater(a, b) => Some(num(a, sol)? > num(b, sol)?),
        Expression::GreaterOrEqual(a, b) => Some(num(a, sol)? >= num(b, sol)?),
        Expression::Less(a, b) => Some(num(a, sol)? < num(b, sol)?),
        Expression::LessOrEqual(a, b) => Some(num(a, sol)? <= num(b, sol)?),
        Expression::And(a, b) => Some(eval_expr_bool(a, sol)? && eval_expr_bool(b, sol)?),
        Expression::Or(a, b) => Some(eval_expr_bool(a, sol)? || eval_expr_bool(b, sol)?),
        Expression::Not(a) => Some(!eval_expr_bool(a, sol)?),
        _ => None,
    }
}

fn expr_str(e: &Expression, sol: &Solution) -> Option<String> {
    match e {
        Expression::Variable(v) => sol.get(v.as_str()).map(|b| b.as_str().to_string()),
        Expression::Literal(l) => Some(l.value().to_string()),
        Expression::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        _ => None,
    }
}

fn num(e: &Expression, sol: &Solution) -> Option<f64> {
    expr_str(e, sol)?.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{load_triples, parse_turtle, IriStore};

    fn loaded_view() -> GraphView {
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"   ; ex:age "25"^^xsd:integer .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age "40"^^xsd:integer ; ex:knows ex:alice .
"#;
        let core = eg_core::graph::GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(
            &core,
            &mut iris,
            "g",
            parse_turtle(ttl).unwrap(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        core.analysis_snapshot()
    }

    /// W2: a ≥2-pattern BGP + a FILTER returns the right solutions.
    #[test]
    fn bgp_two_patterns_plus_filter() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              ?p ex:age ?age .
              ?p ex:knows ?other .
              FILTER (?age > 28)
            }"#,
        )
        .unwrap();
        assert!(res.vars.contains(&"name".to_string()));
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        // alice (30, knows bob) and carol (40, knows alice) qualify; bob (25) is
        // filtered out AND has no ex:knows anyway.
        assert_eq!(names, vec!["Alice", "Carol"], "got {names:?}");
    }

    /// W2: OPTIONAL left-join returns an unbound variable for the no-match case.
    #[test]
    fn optional_left_join() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let mut rows: Vec<(String, Option<String>)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("name").unwrap().as_str().to_string(),
                    s.get("other").map(|b| b.as_str().to_string()),
                )
            })
            .collect();
        rows.sort();
        assert_eq!(rows.len(), 3, "got {rows:?}");
        let bob = rows.iter().find(|(n, _)| n == "Bob").unwrap();
        assert_eq!(bob.1, None, "bob has no OPTIONAL knows-target");
        let alice = rows.iter().find(|(n, _)| n == "Alice").unwrap();
        assert!(alice.1.is_some(), "alice DOES know someone");
    }

    /// W2: UNION merges two graph patterns' solutions.
    #[test]
    fn union_merges_branches() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              { ?p ex:name "Alice" . ?p ex:name ?name . }
              UNION
              { ?p ex:name "Bob" . ?p ex:name ?name . }
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(names, vec!["Alice", "Bob"], "got {names:?}");
    }

    /// W2: a fixed-length sequence property path `ex:knows/ex:name`.
    #[test]
    fn sequence_property_path() {
        let view = loaded_view();
        // carol knows alice; alice's name is "Alice".
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?n WHERE { ex:carol ex:knows/ex:name ?n . }"#,
        )
        .unwrap();
        let names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("n").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(names, vec!["Alice"], "got {names:?}");
    }

    /// `to_rows` projects unbound OPTIONAL variables as None (wire shape).
    #[test]
    fn to_rows_projects_unbound_as_none() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let (cols, rows) = res.to_rows();
        assert!(cols.contains(&"name".to_string()) && cols.contains(&"other".to_string()));
        let other_idx = cols.iter().position(|c| c == "other").unwrap();
        assert!(
            rows.iter().any(|r| r[other_idx].is_none()),
            "bob's ?other must be None"
        );
    }

    // ── CONCEPT:KG-2.235 — SPARQL completeness ──────────────────────────────

    /// Pull the single aggregate cell from a 1-row, 1-projected-var result.
    fn agg_cell(res: &SparqlResult, var: &str) -> String {
        assert_eq!(res.solutions.len(), 1, "expected ONE group, got {res:?}");
        res.solutions[0].get(var).unwrap().as_str().to_string()
    }

    /// COUNT(*) over a BGP — the whole result is one group.
    #[test]
    fn aggregate_count_all() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person . }"#,
        )
        .unwrap();
        // alice, bob, carol.
        assert_eq!(agg_cell(&res, "n"), "3");
    }

    /// SUM(?age) over the three people = 30+25+40 = 95.
    #[test]
    fn aggregate_sum() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (SUM(?age) AS ?total) WHERE { ?p ex:age ?age . }"#,
        )
        .unwrap();
        assert_eq!(agg_cell(&res, "total"), "95");
    }

    /// GROUP BY a constant property + COUNT — every Person shares the same rdf:type,
    /// so a GROUP BY ?type yields one group of 3.
    #[test]
    fn aggregate_group_by_count() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT ?type (COUNT(?p) AS ?n) WHERE { ?p rdf:type ?type . }
            GROUP BY ?type"#,
        )
        .unwrap();
        // one group (ex:Person), count 3.
        assert_eq!(res.solutions.len(), 1, "one group");
        assert_eq!(
            res.solutions[0].get("n").unwrap().as_str(),
            "3",
            "got {res:?}"
        );
    }

    /// `p+` transitive closure: carol knows alice, alice knows bob ⇒
    /// `ex:carol ex:knows+ ?who` reaches BOTH alice and bob.
    #[test]
    fn property_path_one_or_more() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol ex:knows+ ?who . }"#,
        )
        .unwrap();
        let mut who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        who.sort();
        assert_eq!(
            who,
            vec![
                "<http://example.org/alice>".to_string(),
                "<http://example.org/bob>".to_string()
            ],
            "got {who:?}"
        );
    }

    /// `^p` inverse path: `ex:alice ^ex:knows ?who` ⇒ whoever knows alice = carol.
    #[test]
    fn property_path_inverse() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:alice ^ex:knows ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(who, vec!["<http://example.org/carol>".to_string()], "got {who:?}");
    }

    /// `a|b` alternative path — match either of two predicates (here `knows`
    /// alternated with itself, so the result equals the plain `knows` pairs).
    #[test]
    fn property_path_alternative() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol (ex:knows|ex:knows) ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(who, vec!["<http://example.org/alice>".to_string()], "got {who:?}");
    }

    /// `GRAPH ?g { … }` — over the single dataset, the inner BGP resolves and `?g`
    /// binds the request graph IRI.
    #[test]
    fn graph_named_form_binds_g() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?g ?name WHERE { GRAPH ?g { ex:alice ex:name ?name . } }"#,
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 1, "got {res:?}");
        let s = &res.solutions[0];
        assert_eq!(s.get("name").unwrap().as_str(), "Alice");
        assert_eq!(s.get("g").unwrap().as_str(), DEFAULT_GRAPH_IRI);
    }
}
