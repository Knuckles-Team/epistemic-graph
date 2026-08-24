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
use super::plan_cache;
use super::proc::{registry, YieldValue};

/// Implicit max rows (mirrors the SQL surface): one Response per Request, so an
/// unbounded RETURN would buffer the whole result in one message.
const MAX_ROWS: usize = 50_000;

/// Cypher walk instrumentation (CONCEPT:EG-KG.query.cypher-limit-shortcircuit) — the
/// per-execution work counters that PROVE the LIMIT short-circuit is O(limit·deg):
/// a naive materialize-then-LIMIT expands every one of N matches, so `hop_expansions`
/// scales with N; the short-circuit stops at the budget, so it scales with the LIMIT.
/// The increments are compiled to empty inlined no-ops OUTSIDE tests (zero hot-path
/// cost, respecting the Pi-lean `cypher` feature's no-`tracing` boundary); under
/// `cfg(test)` they accumulate into thread-local `Cell`s that `snapshot`/`reset` back
/// the differential test with.
pub(crate) mod walk_metrics {
    #[cfg(test)]
    use std::cell::Cell;
    #[cfg(test)]
    thread_local! {
        static STARTS: Cell<u64> = const { Cell::new(0) };
        static HOPS: Cell<u64> = const { Cell::new(0) };
        static WARM_LABEL_HITS: Cell<u64> = const { Cell::new(0) };
    }

    /// A start-node candidate whose hop walk was initiated (the label-index / anchor
    /// candidate the walk actually expanded from — NOT merely enumerated).
    #[cfg(test)]
    pub(crate) fn note_start() {
        STARTS.with(|c| c.set(c.get().saturating_add(1)));
    }
    #[cfg(not(test))]
    #[inline(always)]
    pub(crate) fn note_start() {}

    /// One neighbour/target considered while extending a partial across a hop.
    #[cfg(test)]
    pub(crate) fn note_hop_expansion() {
        HOPS.with(|c| c.set(c.get().saturating_add(1)));
    }
    #[cfg(not(test))]
    #[inline(always)]
    pub(crate) fn note_hop_expansion() {}

    /// The warm `GraphCore.label_index` prefilter ([`super::indexed_label_candidates`],
    /// this lane's fix) was actually consulted AND answered — i.e. both version
    /// brackets held — for a label-only START candidate resolution, as opposed to
    /// falling back to the cold whole-snapshot `label_candidates` scan. Lets a test
    /// assert the warm path was taken without depending on wall-clock timing
    /// (GOC-70) — a counter, not a stopwatch.
    #[cfg(test)]
    pub(crate) fn note_warm_label_hit() {
        WARM_LABEL_HITS.with(|c| c.set(c.get().saturating_add(1)));
    }
    #[cfg(not(test))]
    #[inline(always)]
    pub(crate) fn note_warm_label_hit() {}

    #[cfg(test)]
    pub(crate) fn reset() {
        STARTS.with(|c| c.set(0));
        HOPS.with(|c| c.set(0));
        WARM_LABEL_HITS.with(|c| c.set(0));
    }

    /// `(starts_expanded, hop_expansions)` since the last [`reset`].
    #[cfg(test)]
    pub(crate) fn snapshot() -> (u64, u64) {
        (STARTS.with(Cell::get), HOPS.with(Cell::get))
    }

    /// Count of [`note_warm_label_hit`] calls since the last [`reset`].
    #[cfg(test)]
    pub(crate) fn warm_label_hits() -> u64 {
        WARM_LABEL_HITS.with(Cell::get)
    }
}

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

/// A live handle back to the graph's bounded, demand-driven secondary-property index
/// (`GraphCore::indexes()`, CONCEPT:EG-KG.storage.index-manager-seam), paired with the OCC
/// [`GraphCore::version`] the accompanying `GraphView` snapshot was captured at
/// (CONCEPT:EG-KG.txn.occ-graph-core).
///
/// Optional performance hint for [`resolve_match`]'s START-position candidate
/// resolution: an unlabeled, unbound start position otherwise enumerates the WHOLE
/// graph (`label_candidates`). When the pattern/WHERE offers an indexable equality or
/// `IN` predicate on that start variable, [`indexed_start_candidates`] narrows the
/// candidate set through this handle instead — but ONLY when `core.version()` reads
/// back the SAME `version` both immediately before and immediately after the lookup
/// (bracketing it, the standard OCC read pattern this engine already uses elsewhere):
/// that equality is the proof no concurrent commit could have changed the live index's
/// answer out from under the point-in-time `GraphView` this executor otherwise reads
/// exclusively. Any mismatch — or no `IndexSource` at all — falls back to today's
/// `label_candidates` full scan; the RESULT is byte-for-byte identical either way, only
/// the WORK to reach it differs. See [`exec_cypher_params_indexed`]'s doc for why most
/// callers do not (and need not) supply one.
///
/// Only constructible under `result-cache` (its one field-carrying form): every
/// producer of a real one (`exec_cypher_params_indexed`, this crate's re-export) is
/// gated the same way, since the version-bracket guarantee this type exists to carry
/// rests on `GraphCore::analysis_snapshot_versioned`'s atomic pairing, itself
/// `result-cache`-only. Without that feature `Option<IndexSource<'_>>` is always
/// `None` in practice, so the type collapses to a fieldless placeholder — it still
/// needs to EXIST (it appears in `resolve_match`'s always-compiled signature), just
/// with nothing to read, so it carries no dead-code warning in a lean build.
#[cfg(feature = "result-cache")]
#[derive(Clone, Copy)]
pub struct IndexSource<'a> {
    core: &'a GraphCore,
    version: u64,
}

#[cfg(feature = "result-cache")]
impl<'a> IndexSource<'a> {
    pub fn new(core: &'a GraphCore, version: u64) -> Self {
        Self { core, version }
    }
}

#[cfg(not(feature = "result-cache"))]
#[derive(Clone, Copy)]
pub struct IndexSource<'a>(std::marker::PhantomData<&'a GraphCore>);

/// Parse + run `cypher` over `view` (read-only, single graph). Synchronous and
/// dep-free — safe to call inside `spawn_blocking` like `exec_sql`.
pub fn exec_cypher(view: &GraphView, cypher: &str) -> Result<QueryResult, String> {
    exec_cypher_params(view, cypher, &Params::new())
}

/// Parse + run `cypher` over `view` with `$name` query parameters (CONCEPT:EG-KG.query.param-list-drives-unwind) —
/// e.g. `UNWIND $ids AS x MATCH (n {id: x}) RETURN n`. `exec_cypher` is the
/// zero-parameter form.
///
/// Parsing is served from the process-wide, schema-independent [`plan_cache`] —
/// a repeat call with IDENTICAL query text (any `$params`, any graph) reuses the
/// already-parsed AST instead of re-parsing (see that module's doc for why a plan
/// never needs to invalidate). Sized by `EPISTEMIC_GRAPH_CYPHER_PLAN_CACHE`.
///
/// No property-index consultation ([`IndexSource`]) — every unlabeled start scans
/// `view` in full, exactly as before. Use [`exec_cypher_params_indexed`] when a paired
/// `(GraphCore, version)` is available.
pub fn exec_cypher_params(
    view: &GraphView,
    cypher: &str,
    params: &Params,
) -> Result<QueryResult, String> {
    exec_cypher_params_inner(view, cypher, params, None)
}

/// [`exec_cypher_params`]'s indexed form (CONCEPT:EG-KG.storage.index-manager-seam): identical read
/// semantics and identical results — only `resolve_match`'s unlabeled-start candidate
/// resolution may consult `index` instead of full-scanning `view`. Callers that hold a
/// `GraphView` paired with the exact `GraphCore`/version it was snapshotted at (e.g. via
/// `GraphCore::analysis_snapshot_versioned`) should prefer this over
/// [`exec_cypher_params`] — the `Method::CypherQuery` pure-read handler is the one
/// production caller that measurably benefits (unlabeled `MATCH (n) WHERE n.id = …`
/// during fleet/tool registration). Every other caller either lacks a paired core+version
/// (a `GraphView` alone, e.g. GraphQL/bolt-wire/gds) or is a write statement's embedded
/// MATCH (a different, unmeasured workload shape) — both keep calling
/// [`exec_cypher_params`] unchanged, `#[cfg(feature = "result-cache")]` guarding this
/// entry point's OCC version bracket like [`GraphView::projection_scope`]'s identical gate.
#[cfg(feature = "result-cache")]
pub fn exec_cypher_params_indexed(
    view: &GraphView,
    cypher: &str,
    params: &Params,
    index: IndexSource<'_>,
) -> Result<QueryResult, String> {
    exec_cypher_params_inner(view, cypher, params, Some(index))
}

fn exec_cypher_params_inner(
    view: &GraphView,
    cypher: &str,
    params: &Params,
    index: Option<IndexSource<'_>>,
) -> Result<QueryResult, String> {
    let query = plan_cache::global().get_or_parse(cypher)?;
    // The `EPISTEMIC_GRAPH_CYPHER_ENGINE` rollout (legacy | plan | shadow) is a
    // `full`-only surface; a lean `cypher`-only build always runs legacy.
    #[cfg(feature = "cypher-plan")]
    {
        engine::dispatch(view, cypher, &query, params, index)
    }
    #[cfg(not(feature = "cypher-plan"))]
    {
        run_legacy(view, &query, params, index)
    }
}

/// Run an already-parsed query through the legacy binding-table walk — the Phase-A
/// pushdowns (per-hop WHERE, LIMIT short-circuit, label-index-first start) are part of
/// this path. The default engine, and the result SHADOW mode serves.
fn run_legacy(
    view: &GraphView,
    query: &CypherQuery,
    params: &Params,
    index: Option<IndexSource<'_>>,
) -> Result<QueryResult, String> {
    let bindings = run_stages(view, &query.stages, params, row_budget(query), index)?;
    finalize(view, query, bindings)
}

/// The LIMIT short-circuit budget (CONCEPT:EG-KG.query.cypher-limit-shortcircuit): the
/// maximum number of MATCH rows the pipeline can possibly need when the final result is
/// a pure PREFIX of the binding stream. `Some(skip+limit)` only when NO blocking
/// operator reorders/collapses/re-counts rows — no `ORDER BY`, no `DISTINCT`, no
/// aggregation — AND the pipeline is a SINGLE `MATCH` stage (a downstream
/// `WITH`/`UNWIND`/`CALL` could filter or multiply rows, so capping upstream would be
/// unsound). `None` ⇒ full materialization, byte-for-byte the prior behavior. The cap
/// is honored by [`resolve_match`]'s depth-first walk, which stops expanding once the
/// budget is met instead of materializing every match and truncating in [`finalize`].
fn row_budget(query: &CypherQuery) -> Option<usize> {
    let ret = &query.ret;
    let limit = ret.limit?;
    if ret.distinct || !ret.order_by.is_empty() {
        return None;
    }
    if ret.items.iter().any(|i| is_agg(&i.expr)) {
        return None;
    }
    // A later stage may filter (WITH … WHERE) or multiply (UNWIND/CALL) rows, so the
    // short-circuit is sound only for a lone MATCH — exactly the acceptance shape
    // (`MATCH … LIMIT k`).
    if !matches!(query.stages.as_slice(), [ReadStage::Match { .. }]) {
        return None;
    }
    Some(limit.saturating_add(ret.skip.unwrap_or(0)).min(MAX_ROWS))
}

// ── read-stage pipeline (CONCEPT:EG-KG.query.eg-extend-read-side) ─────────────────────────────────────

/// Run the reading-stage pipeline, threading bindings from one stage to the next.
fn run_stages(
    view: &GraphView,
    stages: &[ReadStage],
    params: &Params,
    budget: Option<usize>,
    index: Option<IndexSource<'_>>,
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
                    // `budget` is `Some` only for a single-MATCH pipeline (see
                    // `row_budget`), so the short-circuit cap is applied to the one and
                    // only stage here; a multi-stage query always carries `None`.
                    let mut matched = resolve_match(
                        view,
                        pattern,
                        where_clause,
                        incoming,
                        params,
                        budget,
                        index,
                    )?;
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
                    if where_holds(view, &nb, params, where_clause)? {
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
                let additions = subquery_additions(view, subquery, params, index)?;
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
    index: Option<IndexSource<'_>>,
) -> Result<Vec<Binding>, String> {
    // A CALL subquery has its own LIMIT scope (applied by its own `finalize` below);
    // the outer query's short-circuit budget never applies here.
    let sub_bindings = run_stages(view, &subquery.stages, params, None, index)?;
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

/// The variables a WHERE sub-expression reads (each leaf [`Condition`] names one
/// `var.prop`). Drives per-hop pushdown: a conjunct is evaluable once every variable
/// it reads is bound.
fn where_referenced_vars(e: &WhereExpr, out: &mut HashSet<String>) {
    match e {
        WhereExpr::Or(parts) | WhereExpr::And(parts) => {
            parts.iter().for_each(|p| where_referenced_vars(p, out));
        }
        WhereExpr::Cond(c) => {
            out.insert(c.var.clone());
        }
    }
}

/// Map each pattern variable to the walk position at which it becomes BOUND: the start
/// node → 0; a FIXED single hop `j`'s target-node var and (single) edge var → `j+1`.
/// Only plain id-valued bindings are mapped — a quantified-group hop binds LIST-valued
/// group variables (not comparable by `var.prop`) and a variable-length hop binds no
/// single edge, so their variables are omitted and any WHERE conjunct over them falls
/// to the post-walk filter, preserving today's semantics exactly. A repeated variable
/// keeps its EARLIEST position (where its value is first determined).
fn pattern_pushdown_positions(pattern: &Pattern) -> HashMap<String, usize> {
    let mut pos: HashMap<String, usize> = HashMap::new();
    if let Some(v) = &pattern.start.var {
        pos.entry(v.clone()).or_insert(0);
    }
    for (j, (edge, node)) in pattern.hops.iter().enumerate() {
        if edge.group.is_some() {
            continue;
        }
        if let Some(v) = &node.var {
            pos.entry(v.clone()).or_insert(j + 1);
        }
        if edge.var_len.is_none() {
            if let Some(v) = &edge.var {
                pos.entry(v.clone()).or_insert(j + 1);
            }
        }
    }
    pos
}

/// A MATCH's WHERE, split for pushdown (CONCEPT:EG-KG.query.cypher-where-pushdown):
///   * `start_preds` — conjuncts evaluable once the START node is bound (position 0);
///   * `hop_preds[j]` — conjuncts evaluable once hop `j`'s target is bound (position
///     j+1), applied by [`walk_hops`] the instant that hop binds;
///   * `final_preds` — conjuncts reading a variable NOT bound at a known position
///     (group / variable-length edge vars, or a var carried from a prior stage's anchor
///     that is not itself constrained here), applied post-walk exactly as before.
struct WherePartition {
    hop_preds: Vec<Vec<WhereExpr>>,
    start_preds: Vec<WhereExpr>,
    final_preds: Vec<WhereExpr>,
}

/// Split a MATCH's WHERE into per-position conjuncts. A top-level `AND` is flattened to
/// its conjuncts; a bare `Cond`/`Or` is one conjunct pushed to the max position over the
/// vars it reads. This is a pure optimization: every conjunct is still evaluated exactly
/// once, just as early as its inputs allow — filtered-out partials are dropped before
/// the walk expands them further.
fn partition_where(
    pattern: &Pattern,
    anchor: &Binding,
    where_clause: &Option<WhereExpr>,
) -> WherePartition {
    let mut part = WherePartition {
        hop_preds: vec![Vec::new(); pattern.hops.len()],
        start_preds: Vec::new(),
        final_preds: Vec::new(),
    };
    let Some(where_clause) = where_clause else {
        return part;
    };
    let positions = pattern_pushdown_positions(pattern);
    let conjuncts: Vec<WhereExpr> = match where_clause {
        WhereExpr::And(parts) => parts.clone(),
        other => vec![other.clone()],
    };
    for c in conjuncts {
        let mut vars = HashSet::new();
        where_referenced_vars(&c, &mut vars);
        // The earliest position at which EVERY referenced var is bound: the max over its
        // vars' bind positions. A var not in the pattern but present in `anchor` is bound
        // before the walk (position 0); an entirely unknown var forces the final bucket.
        let earliest = vars.iter().try_fold(0usize, |acc, v| {
            if let Some(p) = positions.get(v) {
                Some(acc.max(*p))
            } else if anchor.contains_key(v) {
                Some(acc)
            } else {
                None
            }
        });
        match earliest {
            Some(0) => part.start_preds.push(c),
            Some(p) => part.hop_preds[p - 1].push(c), // position p ⇒ applied after hop p-1
            None => part.final_preds.push(c),
        }
    }
    part
}

/// Label-index-first start selection (CONCEPT:EG-KG.query.cypher-label-first-start). When
/// the START node is UNLABELED and unbound — so its candidate set is the WHOLE graph —
/// but the pattern's FAR-END node carries a `:Label` (candidate set = the label index,
/// almost always far smaller), rewrite the linear pattern to walk it in REVERSE,
/// beginning at the labeled end. The reversed pattern binds the identical variable set
/// over the identical path set — each hop's direction is flipped, which combined with
/// swapping current/target reaches the SAME stored edges (`rel_matches` reads the real
/// orientation either way) — so the result is unchanged as a set; only the far cheaper
/// start enumeration differs. Restricted to a chain of FIXED, DIRECTED single hops (no
/// variable-length / quantified-group hop, whose reversal is not a plain direction flip;
/// no undirected hop, whose edge-variable endpoint resolution is order-sensitive); those
/// keep start-first. `None` when no rewrite applies.
fn reorder_labeled_start(view: &GraphView, pattern: &Pattern, anchor: &Binding) -> Option<Pattern> {
    if pattern.start.label.is_some() || pattern.hops.is_empty() {
        return None;
    }
    if pattern
        .start
        .var
        .as_ref()
        .is_some_and(|v| anchor.contains_key(v))
    {
        return None; // an anchored start is already a single candidate
    }
    if pattern.hops.iter().any(|(e, _)| {
        e.var_len.is_some() || e.group.is_some() || matches!(e.direction, Direction::Both)
    }) {
        return None;
    }
    let end = &pattern.hops.last()?.1;
    end.label.as_ref()?;
    // Only worth it when the labeled end enumerates strictly fewer candidates than the
    // whole-graph start scan the unlabeled start would otherwise do.
    if label_candidates(view, end).len() >= view.node_map.len() {
        return None;
    }
    Some(reverse_pattern(pattern))
}

/// Reverse a linear chain of FIXED, directed single hops so it starts at the current far
/// end: `n0 -e1- n1 … -ek- nk` becomes `nk -flip(ek)- n{k-1} … -flip(e1)- n0`, each
/// edge keeping its type/var/props with only its `direction` flipped (Right↔Left).
/// Caller guarantees no variable-length / quantified-group / undirected hop.
fn reverse_pattern(pattern: &Pattern) -> Pattern {
    let mut nodes: Vec<NodePat> = Vec::with_capacity(pattern.hops.len() + 1);
    nodes.push(pattern.start.clone());
    for (_, n) in &pattern.hops {
        nodes.push(n.clone());
    }
    let edges: Vec<EdgePat> = pattern.hops.iter().map(|(e, _)| e.clone()).collect();
    let new_start = nodes[nodes.len() - 1].clone();
    let mut new_hops: Vec<(EdgePat, NodePat)> = Vec::with_capacity(edges.len());
    for j in (0..edges.len()).rev() {
        let mut e = edges[j].clone();
        e.direction = match e.direction {
            Direction::Right => Direction::Left,
            Direction::Left => Direction::Right,
            Direction::Both => Direction::Both,
        };
        new_hops.push((e, nodes[j].clone()));
    }
    Pattern {
        start: new_start,
        hops: new_hops,
    }
}

/// Resolve a linear MATCH `pattern` into var→node-id bindings, applying `where`.
/// `anchor` pre-binds variables (empty for a fresh MATCH; the incoming binding for
/// an `OPTIONAL MATCH` / post-`WITH` MATCH) — any pattern position whose variable is
/// already in `anchor` is constrained to that id, which is the join mechanism
/// (CONCEPT:EG-KG.query.eg-extend-read-side). Fixed and variable-length hops combine freely (CONCEPT:EG-KG.query.concept-2).
///
/// Phase-A pushdowns: WHERE predicates are applied at the EARLIEST var-bound position
/// during the walk ([`partition_where`], not post-materialization); an unlabeled start
/// with a labeled far end is walked from the labeled end ([`reorder_labeled_start`]);
/// and when `budget` is `Some(k)` (a single `MATCH … LIMIT k` with no blocking op —
/// see [`row_budget`]) the walk is DEPTH-FIRST and stops once `k` rows are produced
/// ([`walk_hops_dfs`]), so a `LIMIT` touches O(k · degree) work instead of every match.
fn resolve_match(
    view: &GraphView,
    pattern: &Pattern,
    where_clause: &Option<WhereExpr>,
    anchor: &Binding,
    params: &Params,
    budget: Option<usize>,
    index: Option<IndexSource<'_>>,
) -> Result<Vec<Binding>, String> {
    // Label-index-first: rebind the walk to start at a labeled end when the start is a
    // full-graph scan. The reversed pattern binds the same variables over the same paths.
    let reordered = reorder_labeled_start(view, pattern, anchor);
    let pattern = reordered.as_ref().unwrap_or(pattern);

    let WherePartition {
        hop_preds,
        start_preds,
        final_preds,
    } = partition_where(pattern, anchor, where_clause);

    // Start candidates: the anchored id if the start var is bound; else, when unbound
    // (labeled OR unlabeled — an otherwise whole-graph or whole-label scan), try
    // narrowing through the bounded property index (CONCEPT:EG-KG.storage.index-manager-seam) via an
    // indexable inline-prop or start-position WHERE equality/IN predicate; else the
    // label set (or the whole graph, unlabeled). `indexed_start_candidates` returning
    // `None` (no `IndexSource`, no usable predicate, or the index/version-race guard
    // declined) is NOT the same as an empty candidate set — it means "fall back to the
    // full scan", so it is never conflated with `Some(vec![])` (a real, indexed,
    // zero-match answer) here. The `.filter(...)` below re-enforces `node`'s label (and
    // any other inline props) on whichever candidate source answered, so an indexed
    // labeled start can't bypass label enforcement.
    // Did the candidate set come from a whole-label enumeration (`label_candidates`,
    // which ALREADY paid to build/consult the memoized whole-graph `label_index_memo`
    // to produce it), or from something small and known ahead of any index — a bound
    // `anchor` id, or `indexed_start_candidates`' id fast path? The distinction drives
    // which of [`node_has_label_id`]/[`node_has_label_point`] the filter below uses:
    // reusing the already-built index is free in the first case, but PAYING to build
    // it (an O(V) decode of every node's property blob) just to answer a handful of
    // point checks in the second case would silently reintroduce the exact
    // whole-graph-scan cost the id fast path exists to eliminate — see
    // `node_has_label_point`'s doc.
    let anchored_id = pattern.start.var.as_ref().and_then(|v| anchor.get(v));
    let (start_candidates, from_label_scan): (Vec<String>, bool) = match anchored_id {
        Some(id) => (vec![id.clone()], false),
        None => match indexed_start_candidates(index, &pattern.start, &start_preds, anchor, params)
        {
            Some(ids) => (ids, false),
            // No inline-prop/WHERE equality to index — but a LABELED start can still
            // avoid `label_candidates`' cold, per-VIEW `label_index_memo` build (an
            // O(V) msgpack-decode-every-node pass that starts over on every fresh
            // snapshot, see that memo's doc) by first trying `GraphCore`'s PERSISTENT,
            // write-path-maintained `label_index` through `warm_label_candidates` —
            // built once and incrementally kept warm across every later query, not
            // just this one. `false` here (not `from_label_scan`) is deliberate and
            // matches `indexed_start_candidates`' own candidates just above: the warm
            // index is a hint that only NARROWS the set, so the filter below must
            // still re-verify Cypher's narrower label predicate per candidate via
            // `node_has_label_point` rather than trusting it outright — see
            // `warm_label_candidates`'s doc for why the two indexes' semantics differ
            // and why that re-verification is what keeps this result-identical.
            None => match pattern
                .start
                .label
                .as_deref()
                .and_then(|label| warm_label_candidates(index, label))
            {
                Some(ids) => (ids, false),
                None => (label_candidates(view, &pattern.start), true),
            },
        },
    };
    let start_ids: Vec<String> = start_candidates
        .into_iter()
        // An ANCHORED start node still has its `:Label`/inline props enforced here — the
        // label-index candidate set only pre-filters the un-anchored case (CONCEPT:EG-KG.query.cypher-planning
        // lets a CALL/YIELD node id flow into a labelled MATCH). The `view.node_map`
        // membership check is a no-op for `label_candidates` (which is already built from
        // `view` and can't return anything else) but is load-bearing for an INDEXED
        // candidate: the index answers off LIVE core state (CONCEPT:EG-KG.storage.index-manager-seam), so
        // an id it returns that this exact point-in-time `view` doesn't have (RLS-filtered,
        // or added/removed by a write that raced the snapshot) is dropped here rather than
        // trusted — every downstream WHERE/prop read only ever consults `view`, never `index`.
        .filter(|id| {
            view.node_map.contains_key(id)
                && pattern.start.label.as_ref().is_none_or(|l| {
                    if from_label_scan {
                        node_has_label_id(view, id, l)
                    } else {
                        node_has_label_point(view, id, l)
                    }
                })
                && node_props_match(view, id, &pattern.start, anchor, params)
        })
        .collect();

    // The DEPTH-FIRST budgeted walk is the LIMIT short-circuit; it does not expand
    // quantified groups, so a pattern with a group hop keeps the breadth-first walk.
    let dfs_budget = budget.filter(|_| pattern.hops.iter().all(|(e, _)| e.group.is_none()));

    if let Some(k) = dfs_budget {
        let mut out: Vec<Binding> = Vec::new();
        for sid in start_ids {
            if out.len() >= k {
                break;
            }
            let mut b = anchor.clone();
            if let Some(v) = &pattern.start.var {
                b.insert(v.clone(), sid.clone());
            }
            if !all_where_hold(view, &b, params, &start_preds)? {
                continue;
            }
            walk_metrics::note_start();
            DfsWalk {
                view,
                hops: &pattern.hops,
                hop_preds: &hop_preds,
                final_preds: &final_preds,
                params,
                budget: k,
            }
            .run(&b, &sid, 0, &mut out)?;
        }
        return Ok(out);
    }

    // (binding, current-node-id) partials, extended hop by hop.
    let mut partials: Vec<(Binding, String)> = Vec::new();
    for sid in start_ids {
        let mut b = anchor.clone();
        if let Some(v) = &pattern.start.var {
            b.insert(v.clone(), sid.clone());
        }
        // Start-node WHERE conjuncts drop a candidate before ANY hop expands from it.
        if !all_where_hold(view, &b, params, &start_preds)? {
            continue;
        }
        walk_metrics::note_start();
        partials.push((b, sid));
    }

    partials = walk_hops(view, &pattern.hops, &hop_preds, partials, params)?;

    let mut out: Vec<Binding> = Vec::new();
    for (b, _) in partials {
        if all_where_hold(view, &b, params, &final_preds)? {
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
    hop_preds: &[Vec<WhereExpr>],
    mut partials: Vec<(Binding, String)>,
    params: &Params,
) -> Result<Vec<(Binding, String)>, String> {
    for (j, (edge, node)) in hops.iter().enumerate() {
        // WHERE conjuncts evaluable once this hop's target is bound (position j+1) —
        // applied inline so a failing partial is dropped BEFORE the next hop expands it
        // (CONCEPT:EG-KG.query.cypher-where-pushdown). A group/var-len hop carries none
        // (its vars fall to the post-walk filter), so this is a no-op there.
        let preds = hop_preds.get(j).map(Vec::as_slice).unwrap_or(&[]);
        let mut next: Vec<(Binding, String)> = Vec::new();
        for (b, cur) in &partials {
            if let Some(group) = &edge.group {
                for (group_binding, target) in
                    quantified_group_matches(view, cur, group, b, params)?
                {
                    walk_metrics::note_hop_expansion();
                    if let Some(nb) = bind_target_node(view, node, &group_binding, &target, params)
                    {
                        if all_where_hold(view, &nb, params, preds)? {
                            next.push((nb, target));
                        }
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
                walk_metrics::note_hop_expansion();
                let Some(mut nb) = bind_target_node(view, node, b, &t, params) else {
                    continue;
                };
                bind_edge_var(view, &mut nb, edge, cur, &t);
                if all_where_hold(view, &nb, params, preds)? {
                    next.push((nb, t));
                }
            }
        }
        partials = next;
    }
    Ok(partials)
}

/// Bind a named edge variable (`-[r]->`) on the READ path too — not just write's
/// DELETE-only enrichment — so `RETURN type(r)` / `r.prop`
/// (CONCEPT:EG-KG.query.rel-type-projection) can resolve it. Only meaningful for a single
/// FIXED hop; a variable-length hop binds no single edge, and QPP relationship variables
/// are captured per iteration separately, so both are skipped.
fn bind_edge_var(view: &GraphView, nb: &mut Binding, edge: &EdgePat, cur: &str, t: &str) {
    if let (Some(evar), None) = (&edge.var, edge.var_len) {
        let (src, tgt) = match edge.direction {
            Direction::Right => (cur.to_string(), t.to_string()),
            Direction::Left => (t.to_string(), cur.to_string()),
            Direction::Both => resolve_undirected_endpoints(view, cur, t),
        };
        nb.insert(edge_key(evar), format!("{src}\u{0}{tgt}"));
    }
}

/// Depth-first, budget-bounded expansion of a FIXED/variable-length hop chain
/// (CONCEPT:EG-KG.query.cypher-limit-shortcircuit) — the LIMIT short-circuit. The args
/// invariant across the recursion (view, hops, pushed predicates, params, budget) are
/// held once here; [`DfsWalk::run`] threads only what varies (binding, current node, hop
/// index, output). Unlike the breadth-first [`walk_hops`] — which materializes every
/// partial at each hop before [`finalize`] truncates — it never expands more than the
/// first `budget` complete paths, so a `MATCH … LIMIT k` touches O(k · degree) work. The
/// caller guarantees a group-free pattern (quantified groups keep the breadth-first walk).
struct DfsWalk<'a> {
    view: &'a GraphView,
    hops: &'a [(EdgePat, NodePat)],
    hop_preds: &'a [Vec<WhereExpr>],
    final_preds: &'a [WhereExpr],
    params: &'a Params,
    budget: usize,
}

impl DfsWalk<'_> {
    /// Extend `binding` (currently at node `cur`, having bound hops `0..hop_idx`) depth
    /// first, pushing each COMPLETE binding into `out` and stopping the instant
    /// `out.len()` reaches `budget`. WHERE conjuncts are applied at their earliest bound
    /// position (`hop_preds[j]` after hop `j`; `final_preds` at the leaf), identically to
    /// the breadth-first path.
    fn run(
        &self,
        binding: &Binding,
        cur: &str,
        hop_idx: usize,
        out: &mut Vec<Binding>,
    ) -> Result<(), String> {
        if out.len() >= self.budget {
            return Ok(());
        }
        if hop_idx == self.hops.len() {
            if all_where_hold(self.view, binding, self.params, self.final_preds)? {
                out.push(binding.clone());
            }
            return Ok(());
        }
        let (edge, node) = &self.hops[hop_idx];
        let preds = self
            .hop_preds
            .get(hop_idx)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let targets = match edge.var_len {
            Some((min, max)) => bfs_reachable(self.view, cur, edge, min, max),
            None => neighbors(self.view, cur, edge),
        };
        for t in targets {
            if out.len() >= self.budget {
                break;
            }
            walk_metrics::note_hop_expansion();
            let Some(mut nb) = bind_target_node(self.view, node, binding, &t, self.params) else {
                continue;
            };
            bind_edge_var(self.view, &mut nb, edge, cur, &t);
            if !all_where_hold(self.view, &nb, self.params, preds)? {
                continue;
            }
            self.run(&nb, &t, hop_idx + 1, out)?;
        }
        Ok(())
    }
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
        // The group's inner sub-pattern carries no OUTER WHERE pushdown (outer conjuncts
        // over group variables fall to the post-walk filter — see `partition_where`).
        walk_hops(
            view,
            &group.hops,
            &[],
            vec![(local, cur.to_string())],
            params,
        )?
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

/// BUG-035 hardening: WHERE evaluation is fallible now — a Condition whose bound
/// variable's node carries an undecodable stored property blob (`node_prop_checked`)
/// can no longer be silently read as "property absent" (which a predicate cannot tell
/// apart from a genuine NULL). A row this can't be verified for aborts the query with
/// an explicit error instead of silently being excluded — see `node_prop_checked`'s doc.
fn where_holds(
    view: &GraphView,
    binding: &Binding,
    params: &Params,
    where_clause: &Option<WhereExpr>,
) -> Result<bool, String> {
    match where_clause {
        None => Ok(true),
        Some(e) => where_expr_holds(view, binding, params, e),
    }
}

fn where_expr_holds(
    view: &GraphView,
    binding: &Binding,
    params: &Params,
    e: &WhereExpr,
) -> Result<bool, String> {
    match e {
        WhereExpr::Or(alts) => {
            for a in alts {
                if where_expr_holds(view, binding, params, a)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        WhereExpr::And(parts) => {
            for p in parts {
                if !where_expr_holds(view, binding, params, p)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        WhereExpr::Cond(c) => cond_holds(view, binding, params, c),
    }
}

/// `Result`-propagating analogue of `[WhereExpr]::iter().all(...)` for the per-hop/
/// per-start/final WHERE-pushdown call sites (CONCEPT:EG-KG.query.cypher-where-pushdown)
/// — the same short-circuit-on-first-false shape `.all()` had, plus short-circuit on the
/// first unevaluable predicate (BUG-035).
fn all_where_hold(
    view: &GraphView,
    binding: &Binding,
    params: &Params,
    preds: &[WhereExpr],
) -> Result<bool, String> {
    for w in preds {
        if !where_expr_holds(view, binding, params, w)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn cond_holds(
    view: &GraphView,
    binding: &Binding,
    params: &Params,
    c: &Condition,
) -> Result<bool, String> {
    let actual = match binding.get(&c.var) {
        Some(id) => node_prop_checked(view, id, &c.prop)?,
        None => None,
    };
    test_holds(actual.as_ref(), &c.test, binding, params)
}

/// `<op> <literal>`/`IN`/`IS [NOT] NULL` are pure — the operand is already a
/// resolved literal in the AST. `STARTS WITH`/`ENDS WITH`/`CONTAINS` are NOT: their
/// operand is a `PropVal` (literal OR `$param`, CONCEPT:EG-KG.query.param-list-drives-unwind), so
/// evaluating them needs the live binding+params to resolve a param reference —
/// hence this now takes `binding`/`params` and returns `Result` (an undefined `$param`
/// is a real error, surfaced via `resolve_prop_val`, not silently swallowed).
fn test_holds(
    actual: Option<&Value>,
    test: &Test,
    binding: &Binding,
    params: &Params,
) -> Result<bool, String> {
    match test {
        Test::Cmp(op, expected) => Ok(compare(actual, op, expected)),
        Test::In(list) => Ok(actual.is_some_and(|a| list.iter().any(|l| l == a))),
        Test::StartsWith(operand) => str_test_holds(actual, binding, params, operand, |a, s| {
            a.starts_with(s)
        }),
        Test::EndsWith(operand) => {
            str_test_holds(actual, binding, params, operand, |a, s| a.ends_with(s))
        }
        Test::Contains(operand) => {
            str_test_holds(actual, binding, params, operand, |a, s| a.contains(s))
        }
        // A missing value reads as null, so IS NULL holds.
        Test::IsNull => Ok(actual.is_none_or(|v| v.is_null())),
        Test::IsNotNull => Ok(actual.is_some_and(|v| !v.is_null())),
    }
}

/// Evaluate a `STARTS WITH`/`ENDS WITH`/`CONTAINS` string predicate.
///
/// Operand resolution: `operand` resolves through `resolve_prop_val` — the same
/// live params+binding path an inline property map or an UNWIND list element
/// uses — so `$param` works here exactly like everywhere else `PropVal` appears
/// in the grammar. An undefined `$param`, or a param/expression that resolves to
/// a non-string, is a genuine error (surfaced, not silently coerced or dropped —
/// BUG-035's "fail loud, not silently wrong" precedent).
///
/// Left-hand (property) semantics — per Cypher, and distinct from the operand
/// error above: a MISSING property or a NON-STRING property value is `null`, so
/// the predicate is simply false for that row — never an error, never treated
/// as an accidental match. `test_holds`'s `actual` is already the checked
/// (BUG-035) property read, so "couldn't decode the node's own blob" already
/// errored upstream in `node_prop_checked`; what lands here as `None` is a
/// genuinely absent/non-string property.
fn str_test_holds(
    actual: Option<&Value>,
    binding: &Binding,
    params: &Params,
    operand: &PropVal,
    op: impl Fn(&str, &str) -> bool,
) -> Result<bool, String> {
    let resolved = resolve_prop_val(binding, params, operand)?;
    let Some(needle) = resolved.as_str() else {
        return Err(format!(
            "STARTS WITH/ENDS WITH/CONTAINS operand must resolve to a string, found {resolved}"
        ));
    };
    Ok(actual
        .and_then(|v| v.as_str())
        .is_some_and(|haystack| op(haystack, needle)))
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
    // un-projected `var.prop`; aggregated rows carry an empty binding. Only
    // ORDER BY ever reads `.1` (via `order_value` below) — ratified two lines
    // down, where it's the sole consumer before the final skip/take discards
    // it — so a query without ORDER BY carries an empty `Binding` instead of
    // cloning the (potentially multi-variable) source binding for every row.
    let needs_binding = !ret.order_by.is_empty();
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
                let carried = if needs_binding {
                    b.clone()
                } else {
                    Binding::new()
                };
                (cells, carried)
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
        Expr::Labels(v) => {
            if binding.contains_key(&qpp_node_key(v)) {
                Value::Array(
                    binding_list(binding, &qpp_node_key(v))
                        .into_iter()
                        .map(|id| {
                            Value::Array(
                                node_labels(view, &id).into_iter().map(Value::String).collect(),
                            )
                        })
                        .collect(),
                )
            } else if let Some(id) = binding.get(v) {
                Value::Array(node_labels(view, id).into_iter().map(Value::String).collect())
            } else {
                // `v` isn't bound to a node at all (e.g. a scalar/edge variable) —
                // `labels()` only ever applies to nodes, so this is null rather than
                // an empty list (distinct from "bound node, no label").
                Value::Null
            }
        }
        // Aggregates never reach here (the agg path owns them).
        Expr::CountStar | Expr::Aggregate(..) => Value::Null,
    }
}

/// The node's labels for `labels(n)`: the canonical `node_type` (if any) followed
/// by the explicit multi-label `labels` property array (if any) — EXACTLY the two
/// fields [`node_has_label`]/[`build_cypher_label_index`] key on, so `labels(n)`
/// always agrees with what `(n:Label)` pattern-matched against. Deduplicated,
/// primary (`node_type`) label first, insertion order otherwise preserved (not
/// sorted — Cypher doesn't guarantee an order, but a stable "primary label first"
/// is the more useful contract for callers). A node with neither field, or whose
/// property blob is missing/undecodable, is unlabelled: `[]` — never null, never
/// an error; a projection expression's "no value" is `Value::Null` at the `Expr`
/// level (see the `else` arm above), not here at the per-node level.
fn node_labels(view: &GraphView, node_id: &str) -> Vec<String> {
    let Some(blob) = view.node_properties.get(node_id) else {
        return Vec::new();
    };
    let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
        return Vec::new();
    };
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    if let Some(lbl) = val.get("node_type").and_then(|v| v.as_str()) {
        if seen.insert(lbl.to_string()) {
            out.push(lbl.to_string());
        }
    }
    if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
        for x in arr {
            if let Some(lbl) = x.as_str() {
                if seen.insert(lbl.to_string()) {
                    out.push(lbl.to_string());
                }
            }
        }
    }
    out
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

// ── indexed start-candidate resolution (CONCEPT:EG-KG.storage.index-manager-seam) ───────────────────

/// Try to resolve pattern `node`'s START candidates WITHOUT a whole-candidate-set
/// scan — for an UNLABELED start that would otherwise be `label_candidates`' full
/// `view.node_map` scan, AND for a LABELED start whose candidate set is
/// `label_candidates`' (cheaper but still O(label-cardinality)) enumeration PLUS a
/// property-blob decode per candidate during `node_props_match`/WHERE evaluation —
/// exactly the shape a single-node `MERGE (m:Label {id: $x})`/`MATCH (m:Label {id:
/// $x})` pays for when `Label`'s cardinality is large (CONCEPT:EG-KG.storage.index-manager-seam,
/// the production `IngestManifest`/`RunTrace`/`Concept` slow-query shapes). A labeled
/// start does NOT skip label enforcement by going through here: the caller
/// (`resolve_match`)'s existing post-filter re-checks `node_has_label_id` on every
/// candidate this function returns, exactly as it always has for `label_candidates`'
/// own output — an indexed candidate is a hint to narrow the SET, never a substitute
/// for the label check.
///
/// Two indexable shapes, checked in order:
///   1. An inline property map (`(n {id: 'x'})` / `(n {id: $p})`) — every key resolves
///      against `anchor`/`params` (CONCEPT:EG-KG.query.param-list-drives-unwind) to a scalar literal.
///   2. A `start_preds` conjunct (CONCEPT:EG-KG.query.cypher-where-pushdown) that is a bare `var.prop = <literal>`
///      or `var.prop IN [<literals>]` test on `node`'s own variable — deliberately NOT
///      an `Or`: a disjunction (like the `tenant_id = … OR tenant_id IS NULL OR …`
///      production shape) is never narrowed here, it stays a post-narrowing FILTER
///      applied by the caller's unchanged `all_where_hold(start_preds)` walk, exactly
///      as before this function existed.
///
/// `id` gets its OWN fast path, ahead of and independent from the general property
/// index: it is a VIRTUAL property backed by the node's GRAPH KEY (`node_prop`'s own
/// `if prop == "id" { … view.node_map … }` special case), never a literal field a
/// node's stored property blob necessarily carries — `GraphCore::nodes_by_property`
/// indexes literal blob fields, so routing `id` through it would silently answer
/// "indexed, zero matches" for every node whose blob happens not to duplicate its own
/// id as a field (the common case; see the doc on [`indexed_where_cond`]). Resolving
/// `id` needs no `IndexSource`/`core` at all — the literal(s) are handed back AS THE
/// CANDIDATE SET and re-validated by the caller's EXISTING `view.node_map.contains_key`
/// check together with `all_where_hold(start_preds)`, exactly like every other
/// candidate source. Every OTHER property key routes through [`indexed_where_cond`] /
/// [`lookup_property_conjunction`] — the real `IndexManager`/`PropertyEqIndex` seam,
/// which DOES need a version-bracketed `IndexSource` (CONCEPT:EG-KG.storage.index-manager-seam).
///
/// Returns:
///   * `Some(ids)` — resolved without a full scan, INCLUDING the legitimate empty case
///     (indexed, zero matches, or an `id` literal not shaped like a string). The
///     caller must serve this as-is, never treat it as "try the fallback instead".
///   * `None` — nothing indexable was found, the covering index refused (bounded cap
///     full), or the version bracket ([`IndexSource`]'s doc) caught a concurrent write
///     racing the snapshot ⇒ the caller MUST fall back to `label_candidates`. `None`
///     here is never conflated with `Some(vec![])`: one means "no answer, full-scan",
///     the other means "answered: nothing matches".
///
/// A `:Label` on `node` is NOT a reason to decline here — see this function's doc.
/// The candidates returned (from either leg) are UNFILTERED by label; `resolve_match`
/// intersects them with the label constraint (and any other inline props) itself, the
/// same post-filter it already applies to `label_candidates`' output.
fn indexed_start_candidates(
    index: Option<IndexSource<'_>>,
    node: &NodePat,
    start_preds: &[WhereExpr],
    anchor: &Binding,
    params: &Params,
) -> Option<Vec<String>> {
    if let Some(props) = &node.props {
        if !props.is_empty() {
            if let Some(ids) = indexed_inline_props(index, props, anchor, params) {
                return Some(ids);
            }
        }
    }
    let var = node.var.as_deref()?;
    for w in start_preds {
        let WhereExpr::Cond(c) = w else { continue };
        if c.var != var {
            continue;
        }
        if let Some(ids) = indexed_where_cond(index, c) {
            return Some(ids);
        }
    }
    None
}

/// The inline-property-map leg of [`indexed_start_candidates`]: resolve every key
/// against `anchor`/`params`, then answer `id` directly (see that function's doc) or
/// the rest as the intersection of each key's [`lookup_property_eq`].
fn indexed_inline_props(
    index: Option<IndexSource<'_>>,
    props: &[(String, PropVal)],
    anchor: &Binding,
    params: &Params,
) -> Option<Vec<String>> {
    let mut resolved: Vec<(String, Value)> = Vec::with_capacity(props.len());
    for (key, pv) in props {
        resolved.push((key.clone(), resolve_prop_val(anchor, params, pv).ok()?));
    }
    if let Some((_, id_val)) = resolved.iter().find(|(k, _)| k == "id") {
        // A direct id pin already narrows to AT MOST ONE candidate — any OTHER
        // inline props in the same map are re-verified by `resolve_match`'s
        // existing `node_props_match` filter regardless, so nothing is lost by not
        // also routing them through the index here.
        return Some(vec![id_val.as_str()?.to_string()]);
    }
    let _ = &index; // referenced unconditionally so a `result-cache`-off build doesn't warn
    #[cfg(feature = "result-cache")]
    {
        let index = index?;
        let mut pairs: Vec<(String, String)> = Vec::with_capacity(resolved.len());
        for (key, val) in &resolved {
            pairs.push((key.clone(), GraphCore::property_value_key(val)?));
        }
        lookup_property_conjunction(index, &pairs)
    }
    #[cfg(not(feature = "result-cache"))]
    {
        None
    }
}

/// The WHERE-conjunct leg of [`indexed_start_candidates`]: `id` resolves directly off
/// the literal(s) (no index, no `core` — see that function's doc); any other property
/// key routes through the version-bracketed [`lookup_property_eq`]/[`lookup_property_in`]
/// seam. `None` when `c` isn't a bare `Cmp(Eq, _)`/non-empty `In` test, when a literal
/// isn't the right shape (a non-string `id` literal can never match a real node id; a
/// non-scalar property literal isn't equality-indexable), or when the general index
/// declines — the caller then tries the next conjunct, if any, before giving up.
fn indexed_where_cond(index: Option<IndexSource<'_>>, c: &Condition) -> Option<Vec<String>> {
    let _ = &index; // referenced unconditionally so a `result-cache`-off build doesn't warn
    match &c.test {
        Test::Cmp(CompareOp::Eq, value) if c.prop == "id" => {
            Some(vec![value.as_str()?.to_string()])
        }
        Test::In(values) if !values.is_empty() && c.prop == "id" => {
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                out.push(v.as_str()?.to_string());
            }
            out.sort_unstable();
            out.dedup();
            Some(out)
        }
        #[cfg(feature = "result-cache")]
        Test::Cmp(CompareOp::Eq, value) => {
            let index = index?;
            let canon = GraphCore::property_value_key(value)?;
            lookup_property_eq(index, &c.prop, &canon)
        }
        #[cfg(feature = "result-cache")]
        Test::In(values) if !values.is_empty() => {
            let index = index?;
            lookup_property_in(index, &c.prop, values)
        }
        _ => None,
    }
}

/// One `PropertyEq` lookup through the `IndexManager`/`PropertyEqIndex` seam
/// (CONCEPT:EG-KG.storage.index-manager-seam), version-bracketed per [`IndexSource`]'s contract: `core.version()`
/// must read back the SAME `index.version` both immediately before and immediately
/// after the lookup, or the answer is discarded (`None` ⇒ caller full-scans) since a
/// concurrent commit could otherwise have changed the live index's answer out from
/// under the point-in-time snapshot the rest of this query reads exclusively.
#[cfg(feature = "result-cache")]
fn lookup_property_eq(index: IndexSource<'_>, key: &str, value: &str) -> Option<Vec<String>> {
    if index.core.version() != index.version {
        return None;
    }
    let result = index.core.indexes().lookup(
        index.core,
        &eg_core::index::Predicate::PropertyEq {
            key: key.to_string(),
            value: value.to_string(),
        },
    );
    if index.core.version() != index.version {
        return None;
    }
    result
}

/// `var.prop IN [v1, v2, …]` (CONCEPT:EG-KG.query.param-list-drives-unwind) via the SAME per-key lookup
/// [`lookup_property_eq`] uses, unioned (sorted + deduped) across every list value.
/// `None` if ANY value isn't a scalar-indexable literal, the key isn't indexable, or a
/// version race is caught on any leg — never a partial union.
#[cfg(feature = "result-cache")]
fn lookup_property_in(index: IndexSource<'_>, key: &str, values: &[Value]) -> Option<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for v in values {
        let canon = GraphCore::property_value_key(v)?;
        out.extend(lookup_property_eq(index, key, &canon)?);
    }
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// Every inline-prop key ANDed together (CONCEPT:EG-KG.query.param-list-drives-unwind) — the intersection of each
/// key's [`lookup_property_eq`] result, smallest set first. `None` if any key isn't
/// indexable (mirrors `GraphCore::nodes_by_properties`' identical all-or-nothing
/// contract — a partial pushdown here would risk silently dropping a valid match).
#[cfg(feature = "result-cache")]
fn lookup_property_conjunction(
    index: IndexSource<'_>,
    pairs: &[(String, String)],
) -> Option<Vec<String>> {
    let mut sets: Vec<Vec<String>> = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        sets.push(lookup_property_eq(index, key, value)?);
    }
    sets.sort_by_key(|s| s.len());
    let mut acc = sets.remove(0);
    for s in &sets {
        acc.retain(|id| s.binary_search(id).is_ok());
    }
    Some(acc)
}

// ── label / property helpers ─────────────────────────────────────────────────

/// Node ids matching a `(var:Label)` — via the same fields the eg-core label index
/// keys on. No label ⇒ every node.
///
/// Consults `view`'s memoized [`GraphView::label_index`] instead of scanning +
/// decoding every node's property blob on every call: the O(N) decode pass now
/// runs at most ONCE per snapshot (the first label lookup builds and caches it),
/// and every later `(var:Label)` in the same query — a common shape across
/// multi-hop patterns — is an O(1) map hit over already-decoded ids.
fn label_candidates(view: &GraphView, node: &NodePat) -> Vec<String> {
    match &node.label {
        None => view.node_map.keys().cloned().collect(),
        Some(label) => view
            .label_index(build_cypher_label_index)
            .get(label)
            .cloned()
            .unwrap_or_default(),
    }
}

/// Does the node `id` carry `label`? Consults the same memoized index as
/// [`label_candidates`] (built at most once per snapshot) instead of re-decoding
/// `id`'s property blob on every call.
fn node_has_label_id(view: &GraphView, id: &str, label: &str) -> bool {
    view.label_index(build_cypher_label_index)
        .get(label)
        .is_some_and(|ids| ids.binary_search_by(|probe| probe.as_str().cmp(id)).is_ok())
}

/// Point form of [`node_has_label_id`]: decode just `id`'s OWN stored blob
/// (`view.node_properties.get(id)`, an O(1) map hit already loaded in this
/// snapshot) and test it directly with [`node_has_label`], instead of consulting
/// — and, on a cold `GraphView`, PAYING TO BUILD — the memoized whole-graph
/// [`GraphView::label_index`] (`build_cypher_label_index` decodes EVERY node's
/// property blob in the graph on its first call per snapshot).
///
/// `resolve_match`'s start-candidate filter uses this whenever the candidate set
/// is already small and known ahead of any index — a bound/anchored start, or
/// [`indexed_start_candidates`]' `id` fast path — exactly the `MATCH (n:Label)
/// WHERE n.id IN [...] RETURN ...` shape the durable ACL hydration path issues on
/// every governed read (CONCEPT:EG-KG.storage.index-manager-seam). A fresh,
/// per-request `GraphView` (`analysis_snapshot_versioned` clones one per call)
/// starts with a cold `label_index_memo`, so without this split, verifying the
/// label on that handful of already-resolved ids still decoded every node's
/// blob in the WHOLE graph the first time any labelled MATCH ran — silently
/// reintroducing, one filter step later, the exact O(graph) cost the id fast
/// path exists to eliminate. `node_has_label_id` stays the right choice for
/// `label_candidates`' own (unindexed, whole-label-scan) callers, which already
/// paid to build the same index to enumerate their candidate set in the first
/// place, so consulting it again there is free, not redundant.
///
/// Same `node_type`/`labels` semantics as `build_cypher_label_index`/
/// `node_has_label_id` (both ultimately test the identical two fields
/// [`node_has_label`] tests) — this returns the byte-for-byte same answer for
/// any given `(id, label)`, only the WORK to reach it differs, same contract as
/// [`indexed_start_candidates`] itself.
fn node_has_label_point(view: &GraphView, id: &str, label: &str) -> bool {
    view.node_properties
        .get(id)
        .is_some_and(|blob| node_has_label(blob, label))
}

/// `resolve_match`'s always-compiled entry point for the warm-index leg of
/// label-only START-candidate resolution (this lane's fix, CONCEPT:EG-KG.compute.consult-lazy sibling
/// of `apply_merge`'s identical `core.get_nodes_by_label` + narrow-verify shape on
/// the write path). `None` immediately in a `result-cache`-off build (no
/// `IndexSource` ever exists then, mirroring [`indexed_inline_props`]/
/// [`indexed_where_cond`]'s identical `let _ = …` pattern) or when `index` is
/// `None`; otherwise defers to [`indexed_label_candidates`].
fn warm_label_candidates(index: Option<IndexSource<'_>>, label: &str) -> Option<Vec<String>> {
    let _ = &index; // referenced unconditionally so a `result-cache`-off build doesn't warn
    #[cfg(feature = "result-cache")]
    {
        indexed_label_candidates(index?, label)
    }
    #[cfg(not(feature = "result-cache"))]
    {
        let _ = label;
        None
    }
}

/// Warm-index leg of label-only START-candidate resolution: when a
/// version-bracketed [`IndexSource`] is available, narrow `label`'s candidate
/// set through `GraphCore`'s PERSISTENT, write-path-maintained `label_index`
/// (`GraphCore::get_nodes_by_label`) instead of paying to build THIS
/// snapshot's own `label_index_memo` from scratch — [`build_cypher_label_index`]'s
/// O(V) msgpack-decode-EVERY-node pass, which starts cold on every fresh
/// `GraphView` (`label_index_memo`'s doc), unlike `GraphCore.label_index`,
/// which is built once and incrementally maintained across every later query
/// until the next write invalidates it (`label_index_add`/`_remove`/`_refile`).
///
/// `GraphCore.label_index` keys on a BROADER write-path field set (`type`/
/// `node_type`/`label`/`labels`, see that field's doc on `GraphCore`) than
/// Cypher's `(var:Label)` semantics (`node_type`/`labels` only, see
/// [`node_has_label`]) — every field the narrow test reads is also read by the
/// broad one, so the broad index is a structural SUPERSET of the narrow one for
/// any label, which is why this is safe to use as a PREFILTER ONLY: every id
/// this returns still passes through `resolve_match`'s existing post-filter
/// (`node_has_label_point`, since — like [`indexed_start_candidates`]' other
/// legs — this candidate source is index-derived, not an already-built whole-
/// snapshot scan) before being trusted. That re-verification is what makes the
/// served result byte-for-byte identical to the cold `label_candidates` full
/// scan either way — only the work to reach it differs, and a broader-but-not-
/// narrower candidate (e.g. a node carrying `label`/`type` but no
/// `node_type`/`labels`) is dropped there rather than wrongly matched.
///
/// Version-bracketed exactly like [`lookup_property_eq`]: `core.version()` must
/// read back the SAME `index.version` both before and after the lookup, or the
/// answer is discarded (`None` ⇒ caller falls back to `label_candidates`) since
/// a concurrent commit could otherwise have changed the live index's answer out
/// from under the point-in-time `view` the rest of this query reads exclusively.
///
/// Only ever reached when `index` is `Some` — i.e. through
/// [`exec_cypher_params_indexed`]'s one production caller, which snapshots via
/// `analysis_snapshot_versioned` and applies only RLS filtering, never the
/// read-your-own-writes overlay (`GraphView::overlay_add_node` et al. are used
/// exclusively by a separate, non-Cypher transaction-replay path). A future
/// caller that starts pairing an `IndexSource` with an overlaid view would need
/// to additionally reconcile the overlay's buffered adds/removes against this
/// prefilter — e.g. an overlay-added node the live `core` index cannot yet know
/// about — before trusting it; today's one caller never does, so that case is
/// out of scope here and this function must not be reused for one without
/// re-checking that assumption.
#[cfg(feature = "result-cache")]
fn indexed_label_candidates(index: IndexSource<'_>, label: &str) -> Option<Vec<String>> {
    if index.core.version() != index.version {
        return None;
    }
    let candidates = index.core.get_nodes_by_label(label, 0);
    if index.core.version() != index.version {
        return None;
    }
    walk_metrics::note_warm_label_hit();
    Some(candidates.into_iter().map(|(id, _blob)| id).collect())
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

/// Build the `label → node ids` index for [`GraphView::label_index`], keyed on
/// EXACTLY the fields [`node_has_label`] reads (`node_type` + the `labels`
/// array) — deliberately narrower than `GraphCore.label_index`'s `type`/
/// `node_type`/`label`, which also serves the write path's broader contract
/// (see `apply_merge`'s own comment on this same distinction). Consulting the
/// memoized index therefore returns the identical candidate set the old
/// per-call `node_has_label` scan did. Ids are sorted + deduped per label (a
/// node matching via both `node_type` and its own `labels` array must still
/// contribute exactly one row, matching the old scan's 1-node-1-row behavior
/// since `node_properties` is keyed by id).
fn build_cypher_label_index(view: &GraphView) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for (id, blob) in &view.node_properties {
        let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
            continue;
        };
        if let Some(lbl) = val.get("node_type").and_then(|v| v.as_str()) {
            index.entry(lbl.to_string()).or_default().push(id.clone());
        }
        if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
            for x in arr {
                if let Some(lbl) = x.as_str() {
                    index.entry(lbl.to_string()).or_default().push(id.clone());
                }
            }
        }
    }
    for ids in index.values_mut() {
        ids.sort_unstable();
        ids.dedup();
    }
    index
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

/// [`node_prop`]'s WHERE-clause counterpart (BUG-035 hardening): distinguishes a
/// genuinely ABSENT property (`Ok(None)` — no stored blob, or the blob decodes but
/// has no such key) from an UNDECODABLE stored blob (`Err`). `node_prop`'s `.ok()?`
/// collapses both into `None`, which a predicate cannot tell apart from a real NULL —
/// `IS NULL` would silently read a corrupted/unreadable node as matching, and `=`/`IN`
/// would silently read it as not matching, with no error either way. That is exactly
/// the failure class BUG-035 exists to close: a result that reads as a real answer
/// when the underlying data could not actually be evaluated. Every WHERE-predicate
/// evaluation path (`cond_holds`) goes through this, never through plain `node_prop`;
/// every other property read (RETURN projection, ORDER BY, inline MATCH `{prop: val}`
/// constraints) is unaffected and keeps today's `node_prop` behavior.
fn node_prop_checked(view: &GraphView, node_id: &str, prop: &str) -> Result<Option<Value>, String> {
    if prop == "id" {
        return Ok(view
            .node_map
            .contains_key(node_id)
            .then(|| Value::String(node_id.to_string())));
    }
    let Some(blob) = view.node_properties.get(node_id) else {
        return Ok(None);
    };
    match eg_types::msgpack::decode_property_value(blob) {
        Ok(val) => Ok(val.get(prop).cloned()),
        Err(e) => Err(format!(
            "cannot evaluate WHERE predicate on `{prop}`: node `{node_id}`'s stored \
             property blob is undecodable ({e:?}) — refusing to silently treat it as NULL"
        )),
    }
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
    //
    // `analysis_snapshot_versioned` (the `result-cache` build) pairs the snapshot with
    // the OCC version read under the SAME topology lock, exactly like the
    // `Method::CypherQuery` read handler — safe to hand `resolve_match` as an
    // `IndexSource` HERE because this leading MATCH runs before ANY of this
    // statement's own write ops touch the graph (the mutation loop below only starts
    // once `bindings` is fully resolved); the only staleness this needs to guard
    // against is a *concurrent, other* writer racing the snapshot, which the
    // version bracket already catches (falls back to a full scan rather than a torn
    // answer). It must NOT be threaded into the mutation loop that follows:
    // `core.add_node`/`compare_and_set_fields`/etc. write straight into `core`'s live
    // state without bumping `core.version()` — that only happens once, in
    // `mark_dirty()`, at the very end of this function — so a node created/changed by
    // an EARLIER binding or op in this SAME statement would be invisible to a cached
    // property-index answer while still passing the version bracket (the bracket only
    // detects an version bump, and there isn't one yet). `apply_merge` below
    // deliberately does not consult this or any `IndexSource` for exactly that
    // reason; see its own doc.
    #[cfg(feature = "result-cache")]
    let (snap, leading_match_index) = {
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, Some(IndexSource::new(core, version)))
    };
    #[cfg(not(feature = "result-cache"))]
    let (snap, leading_match_index): (GraphView, Option<IndexSource<'_>>) =
        (core.analysis_snapshot(), None);
    let mut bindings: Vec<Binding> = match &w.match_pattern {
        Some(pattern) => resolve_match(
            &snap,
            pattern,
            &w.where_clause,
            &HashMap::new(),
            params,
            None,
            leading_match_index,
        )?,
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

    // Fast path (CONCEPT:EG-KG.storage.index-manager-seam): the dominant production shape is
    // `MERGE (m:Label {id: $x}) SET …` — a single-node point lookup, not a scan.
    // `GraphCore::get_node_properties` is a DIRECT DashMap read keyed by the node's
    // GRAPH KEY (no property index, no cache, no staleness of any kind — it reads
    // whatever `core` holds live at this instant, so it is exactly as fresh as the
    // full scan below and safe to consult even mid-statement, unlike the general
    // property index; see `exec_write`'s doc on why THAT index is not used here).
    // A hit is checked against the SAME `label` + `want` conditions the full-scan
    // loop below checks for one candidate — so a match here is unconditionally the
    // same match the loop would find, and it's returned immediately without ever
    // touching `get_nodes_by_label`'s O(label-cardinality) scan+decode. A miss (no
    // node at that graph key, wrong label, or its blob doesn't carry `want`
    // verbatim — e.g. a non-Cypher-authored node whose blob's own `id` field
    // diverges from its graph key) falls through to the unchanged full scan rather
    // than being treated as "absent": this fast path can only ever ADD a match,
    // never suppress one the slow path would have found.
    if let Some(id_val) = want.get("id").and_then(Value::as_str) {
        if let Some(blob) = core.get_node_properties(id_val) {
            let label_ok = node
                .label
                .as_deref()
                .is_none_or(|label| node_has_label(&blob, label));
            if label_ok {
                if let Ok(Value::Object(obj)) = eg_types::msgpack::decode_property_value(&blob) {
                    if want.iter().all(|(k, v)| obj.get(k) == Some(v)) {
                        if let Some(var) = &node.var {
                            binding.insert(var.clone(), id_val.to_string());
                        }
                        return Ok(());
                    }
                }
            }
        }
    }

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

// ── Phase B: EPISTEMIC_GRAPH_CYPHER_ENGINE rollout — legacy | plan | shadow ────────
//
// Design note / ADR deviation (CONCEPT:EG-KG.query.cypher-engine-shadow). The ADR
// (reports/wave1/ADR-cypher-lowering.md) specified lowering the Cypher AST onto eg-plan's
// RowSet ops so `GlobalChainCost` reorders them. Implementation found that infeasible for
// THREE verified reasons, so this module realizes the ADR's INTENT (route Cypher through
// cost-based planning; shadow-differential rollout; zero-divergence gate) without the
// infeasible lowering:
//   1. Crate DAG direction: `eg-plan` depends on `eg-query` (optional, `query` feature),
//      so eg-query — home of the Cypher AST + executor — CANNOT depend on eg-plan's cost
//      model / Op algebra (a cycle the workspace DAG rejects at compile time).
//   2. RowSet currency: eg-plan's `RowSet` is a single-column, id-DEDUPLICATED set (its
//      own docs call multi-column projection "an EXPLICIT later increment"); it cannot
//      represent a Cypher binding table (bag semantics, multi-var a/r/b rows, edge
//      identity for `type(r)`). Rebuilding it ripples through ~35 ops + the cost model +
//      optimizer + DAG executor + differential oracle + cross-shard exchange — a
//      multi-wave rewrite, and beyond this task's "surgical" + "no optimizer rule changes
//      beyond what lowering requires" bounds.
//   3. Reordering headroom: the Cypher grammar here is linear-path-only (no comma-joined
//      disjoint patterns — a grammar change is an explicit NON-GOAL), so a chain's only
//      degrees of freedom are start-end + hop-direction; the N-ary join reordering
//      `GlobalChainCost` exists for has almost nothing to reorder.
//
// So `plan` is a cost-based start/traversal-order chooser over the SAME semantics-exact
// binding-table walk (DAG-legal: statistics computed locally over the GraphView, the same
// signals PlanStats/ColumnStats derive), and `shadow` dual-executes both and logs any
// divergence — the differential infrastructure the ADR's rollout section mandates, which
// also de-risks a future full lowering. Full details in the W1.3 report.
#[cfg(feature = "cypher-plan")]
mod engine {
    use super::*;
    use std::sync::OnceLock;

    /// Which Cypher execution engine to use (CONCEPT:EG-KG.query.cypher-engine-shadow),
    /// selected once by `EPISTEMIC_GRAPH_CYPHER_ENGINE`. Default `Legacy`; the flip to
    /// `Plan` is a later release, gated on the shadow harness reaching zero divergences.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum CypherEngine {
        Legacy,
        Plan,
        Shadow,
    }

    /// Read + cache the engine selection. Unset / `legacy` / any unrecognized value ⇒ the
    /// safe `Legacy` default (the flag never fails closed onto an unproven engine).
    pub(crate) fn selected_engine() -> CypherEngine {
        static ENGINE: OnceLock<CypherEngine> = OnceLock::new();
        *ENGINE.get_or_init(|| {
            let e = match std::env::var("EPISTEMIC_GRAPH_CYPHER_ENGINE")
                .ok()
                .as_deref()
            {
                Some("plan") => CypherEngine::Plan,
                Some("shadow") => CypherEngine::Shadow,
                _ => CypherEngine::Legacy,
            };
            if e != CypherEngine::Legacy {
                tracing::info!(
                    target: "epistemic_graph::cypher_shadow",
                    engine = ?e,
                    "non-default Cypher engine selected via EPISTEMIC_GRAPH_CYPHER_ENGINE"
                );
            }
            e
        })
    }

    /// Execute `query` under the selected engine. `legacy`/`plan` run one engine; `shadow`
    /// runs BOTH, SERVES legacy, and logs any divergence with the query text.
    pub(crate) fn dispatch(
        view: &GraphView,
        text: &str,
        query: &CypherQuery,
        params: &Params,
        index: Option<IndexSource<'_>>,
    ) -> Result<QueryResult, String> {
        match selected_engine() {
            CypherEngine::Legacy => run_legacy(view, query, params, index),
            CypherEngine::Plan => run_plan(view, query, params, index),
            CypherEngine::Shadow => run_shadow(view, text, query, params, index),
        }
    }

    /// The `plan` engine: cost-based start/traversal-order selection over the SAME
    /// binding-table walk. A pure AST pre-pass ([`cost_plan`]) rewrites each MATCH pattern
    /// to begin at the cost-cheapest end (reusing the semantics-preserving
    /// [`reverse_pattern`]); the rewritten query then runs through the identical
    /// `run_stages`/`finalize`, so the result is unchanged as a set. `index` flows through
    /// unchanged — the property-index start-candidate narrowing lives in the SHARED
    /// `resolve_match`, so both engines benefit identically (CONCEPT:EG-KG.storage.index-manager-seam).
    fn run_plan(
        view: &GraphView,
        query: &CypherQuery,
        params: &Params,
        index: Option<IndexSource<'_>>,
    ) -> Result<QueryResult, String> {
        let planned = cost_plan(view, query);
        let bindings = run_stages(view, &planned.stages, params, row_budget(&planned), index)?;
        finalize(view, &planned, bindings)
    }

    /// SHADOW mode (CONCEPT:EG-KG.query.cypher-engine-shadow): run BOTH engines, SERVE
    /// legacy, and `tracing::warn!` each divergence — row-set equality MODULO ORDERING
    /// when there is no ORDER BY, exact ordered equality otherwise — with the query text.
    /// Zero divergences over the corpus + generator is the flip gate.
    fn run_shadow(
        view: &GraphView,
        text: &str,
        query: &CypherQuery,
        params: &Params,
        index: Option<IndexSource<'_>>,
    ) -> Result<QueryResult, String> {
        let legacy = run_legacy(view, query, params, index);
        let plan = run_plan(view, query, params, index);
        match (&legacy, &plan) {
            (Ok(l), Ok(p)) => {
                if let Some(reason) = result_divergence(l, p, query) {
                    tracing::warn!(
                        target: "epistemic_graph::cypher_shadow",
                        query = %text,
                        reason = %reason,
                        "Cypher shadow-mode divergence (serving legacy)"
                    );
                }
            }
            (Ok(_), Err(e)) => tracing::warn!(
                target: "epistemic_graph::cypher_shadow",
                query = %text,
                plan_error = %e,
                "Cypher shadow: plan engine errored while legacy succeeded"
            ),
            (Err(e), Ok(_)) => tracing::warn!(
                target: "epistemic_graph::cypher_shadow",
                query = %text,
                legacy_error = %e,
                "Cypher shadow: plan engine succeeded while legacy errored"
            ),
            // Both errored — same failure class, nothing to reconcile.
            (Err(_), Err(_)) => {}
        }
        legacy // ALWAYS serve legacy under shadow
    }

    /// `None` when the two results are equal (modulo row ordering unless the query has an
    /// ORDER BY), else a short reason for the shadow log. Rows are msgpack cell-vectors,
    /// so a byte-wise sort is a stable multiset compare for the un-ordered case.
    fn result_divergence(a: &QueryResult, b: &QueryResult, query: &CypherQuery) -> Option<String> {
        if a.columns != b.columns {
            return Some(format!("columns {:?} vs {:?}", a.columns, b.columns));
        }
        if a.rows.len() != b.rows.len() {
            return Some(format!("row count {} vs {}", a.rows.len(), b.rows.len()));
        }
        if query.ret.order_by.is_empty() {
            let mut la: Vec<&Vec<u8>> = a.rows.iter().collect();
            let mut lb: Vec<&Vec<u8>> = b.rows.iter().collect();
            la.sort_unstable();
            lb.sort_unstable();
            if la != lb {
                return Some("row multiset differs (no ORDER BY)".to_string());
            }
        } else if a.rows != b.rows {
            return Some("ordered rows differ (ORDER BY)".to_string());
        }
        None
    }

    /// Rewrite each MATCH stage's pattern to start at the cost-cheapest end (a
    /// semantics-preserving reversal). Non-MATCH stages and patterns with no cheaper
    /// reversal are left untouched.
    fn cost_plan(view: &GraphView, query: &CypherQuery) -> CypherQuery {
        let mut q = query.clone();
        for stage in &mut q.stages {
            if let ReadStage::Match { pattern, .. } = stage {
                if let Some(rewritten) = cost_reorder(view, pattern) {
                    *pattern = rewritten;
                }
            }
        }
        q
    }

    /// Cost-choose the start end of a linear pattern: reverse iff the far end enumerates
    /// strictly fewer start candidates than the current start. Unlike the legacy
    /// label-first heuristic (which only fires for an unlabeled start), this also reorders
    /// a both-ends-labeled pattern toward the smaller label. Restricted to the same
    /// fixed/directed single-hop chains [`reverse_pattern`] handles.
    fn cost_reorder(view: &GraphView, pattern: &Pattern) -> Option<Pattern> {
        if pattern.hops.is_empty() {
            return None;
        }
        if pattern.hops.iter().any(|(e, _)| {
            e.var_len.is_some() || e.group.is_some() || matches!(e.direction, Direction::Both)
        }) {
            return None;
        }
        let start_cost = start_candidate_cost(view, &pattern.start);
        let end = &pattern.hops.last()?.1;
        let end_cost = start_candidate_cost(view, end);
        (end_cost < start_cost).then(|| reverse_pattern(pattern))
    }

    /// Estimated start-candidate count a node position seeds — the local analogue of
    /// eg-plan's `PlanStats` label-cardinality selectivity: an inline `{id: …}` pin is a
    /// single-node point lookup; a labeled position is its label-index size; an unlabeled
    /// position is the whole-graph node count.
    fn start_candidate_cost(view: &GraphView, node: &NodePat) -> usize {
        if node
            .props
            .as_ref()
            .is_some_and(|ps| ps.iter().any(|(k, _)| k == "id"))
        {
            return 1;
        }
        match &node.label {
            Some(_) => label_candidates(view, node).len(),
            None => view.node_map.len(),
        }
    }

    /// Differential entry for the shadow harness (CONCEPT:EG-KG.query.cypher-engine-shadow):
    /// run BOTH engines over `text` and return the divergence reason (`None` = agree). A
    /// query legacy itself rejects is skipped (`Ok(None)`) — both engines share the parser,
    /// so a parse/semantic error is not an engine divergence.
    #[cfg(test)]
    pub(crate) fn diff_for_test(
        view: &GraphView,
        text: &str,
        params: &Params,
    ) -> Result<Option<String>, String> {
        let query = plan_cache::global().get_or_parse(text)?;
        let Ok(l) = run_legacy(view, &query, params, None) else {
            return Ok(None);
        };
        match run_plan(view, &query, params, None) {
            Ok(p) => Ok(result_divergence(&l, &p, &query)),
            Err(e) => Ok(Some(format!("plan engine errored: {e}"))),
        }
    }

    /// Run ONLY the `plan` engine (bypassing the cached env selection) — the harness uses
    /// this to prove the plan engine genuinely cost-reorders, not merely that it agrees.
    #[cfg(test)]
    pub(crate) fn run_plan_for_test(
        view: &GraphView,
        text: &str,
        params: &Params,
    ) -> Result<QueryResult, String> {
        let query = plan_cache::global().get_or_parse(text)?;
        run_plan(view, &query, params, None)
    }

    /// Run ONLY the legacy engine (bypassing the cached env selection).
    #[cfg(test)]
    pub(crate) fn run_legacy_for_test(
        view: &GraphView,
        text: &str,
        params: &Params,
    ) -> Result<QueryResult, String> {
        let query = plan_cache::global().get_or_parse(text)?;
        run_legacy(view, &query, params, None)
    }
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

    /// Like [`ids`], but preserves row order — for asserting an `ORDER BY`
    /// result rather than merely a result *set*.
    fn ids_in_order(qr: &QueryResult, col: usize) -> Vec<String> {
        qr.rows
            .iter()
            .map(|b| {
                let cells: Vec<Value> = rmp_serde::from_slice(b).unwrap();
                projected_id(&cells[col]).unwrap().to_string()
            })
            .collect()
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
        assert_eq!(qr.columns, vec!["n", "id"]);
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
        let (vf2, truncated) = vf2_match_views(&v, &pat, 0, 0);
        assert_eq!(vf2.len(), 2);
        assert!(!truncated, "tiny fixture must not hit the default budget");
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

    // ── CONCEPT:EG-KG.query.anon-propmap-parity — anonymous-node inline-property-map parity (W0.8) ──
    //
    // `agent-utilities` (`orchestration/manager.py:132`, `agent_digital_twin.py`)
    // both carried a defensive comment claiming this engine's Cypher executor
    // silently under-matches (zero rows, no error) an ANONYMOUS node pattern
    // carrying an inline property map — e.g. `MATCH (:RunTrace {id: $tid})` —
    // relative to the identical pattern written with a bound-but-otherwise-unused
    // variable (`MATCH (t:RunTrace {id: $tid})`), and both worked around it by
    // always naming the variable. Diagnosis (this change) found
    // `resolve_match`/`walk_hops`/`bind_target_node`/`node_props_match` already
    // apply label + inline-property constraints uniformly regardless of
    // `NodePat.var` — true back to the read-side inline prop-map's original
    // introduction (CONCEPT:EG-KG.query.param-list-drives-unwind) — so the differential does not reproduce
    // against this engine version. These tests lock that invariant in as a
    // permanent regression gate across every pattern position (start, hop
    // target, multi-hop interior), value source (`$param` vs inline literal),
    // property count, and combined with labels/WHERE/OPTIONAL MATCH: the
    // anonymous and bound-variable forms must produce byte-identical rows.

    /// Start-node position, inline literal.
    #[test]
    fn anon_start_node_propmap_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b:Person) RETURN b",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (:Person {name:'Alice'})-[:KNOWS]->(b:Person) RETURN b",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["bob"]);
    }

    /// Start-node position, value from a `$param` rather than an inline literal.
    #[test]
    fn anon_start_node_propmap_matches_bound_form_with_param() {
        let v = fixture();
        let mut params = Params::new();
        params.insert("tid".into(), Value::String("Alice".into()));
        let bound = exec_cypher_params(
            &v,
            "MATCH (t:Person {name: $tid})-[:KNOWS]->(tc:Person) RETURN tc",
            &params,
        )
        .unwrap();
        let anon = exec_cypher_params(
            &v,
            "MATCH (:Person {name: $tid})-[:KNOWS]->(tc:Person) RETURN tc",
            &params,
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["bob"]);
    }

    /// Hop-TARGET position (not the start node).
    #[test]
    fn anon_hop_target_propmap_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(:Person {name:'Bob'}) RETURN a",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["alice"]);
    }

    /// Interior node of a multi-hop (2-hop) chain — neither the start nor the
    /// final hop target.
    #[test]
    fn anon_multi_hop_interior_propmap_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(b:Person {name:'Bob'})-[:KNOWS]->(c:Person) RETURN c",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (a:Person)-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(c:Person) RETURN c",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["carol"]);
    }

    /// Fully anonymous: no var AND no label, just a propmap.
    #[test]
    fn anon_no_label_propmap_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(&v, "MATCH (a {name:'Alice'})-[:KNOWS]->(b) RETURN b").unwrap();
        let anon = exec_cypher(&v, "MATCH ({name:'Alice'})-[:KNOWS]->(b) RETURN b").unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["bob"]);
    }

    /// The EXACT real-world shape from `manager.py`/`agent_digital_twin.py`: a
    /// RunTrace keyed by the VIRTUAL `id` property (the graph key, not a stored
    /// field — `node_prop`'s special-cased branch), matched by a `$tid` param,
    /// anonymously, then hopping over a typed relationship to a
    /// differently-named ToolCall. A second graph fixture (distinct from
    /// `fixture()`/`relationship_fixture()` above) purpose-built for this shape.
    #[test]
    fn anon_runtrace_shape_matches_bound_form() {
        let core = GraphCore::new();
        core.add_node(
            "trace:pref_run_abc123".into(),
            pbytes(serde_json::json!({"node_type":"RunTrace","status":"completed"})),
        );
        core.add_node(
            "trace:pref_run_other".into(),
            pbytes(serde_json::json!({"node_type":"RunTrace","status":"completed"})),
        );
        core.add_node(
            "tc:1".into(),
            pbytes(serde_json::json!({"node_type":"ToolCall","tool_name":"graph_query"})),
        );
        core.add_node(
            "tc:2".into(),
            pbytes(serde_json::json!({"node_type":"ToolCall","tool_name":"other_run_tool"})),
        );
        core.add_edge(
            "trace:pref_run_abc123".into(),
            "tc:1".into(),
            pbytes(serde_json::json!({"relationship":"USED_TOOL"})),
        )
        .unwrap();
        core.add_edge(
            "trace:pref_run_other".into(),
            "tc:2".into(),
            pbytes(serde_json::json!({"relationship":"USED_TOOL"})),
        )
        .unwrap();
        let v = core.analysis_snapshot();

        let mut params = Params::new();
        params.insert(
            "tid".to_string(),
            Value::String("trace:pref_run_abc123".to_string()),
        );
        let bound = exec_cypher_params(
            &v,
            "MATCH (t:RunTrace {id: $tid})-[:USED_TOOL]->(tc:ToolCall) RETURN tc.tool_name AS tool_name",
            &params,
        )
        .unwrap();
        let anon = exec_cypher_params(
            &v,
            "MATCH (:RunTrace {id: $tid})-[:USED_TOOL]->(tc:ToolCall) RETURN tc.tool_name AS tool_name",
            &params,
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(
            bound.rows.len(),
            1,
            "must find exactly the one matching ToolCall"
        );
        assert_eq!(cells_of(&anon, 0)[0], Value::String("graph_query".into()));
    }

    /// Combined with WHERE + multiple inline properties on the anonymous node.
    #[test]
    fn anon_multi_prop_and_where_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (a:Person {name:'Alice', node_type:'Person'})-[:KNOWS]->(b:Person) \
             WHERE b.name = 'Bob' RETURN b",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (:Person {name:'Alice', node_type:'Person'})-[:KNOWS]->(b:Person) \
             WHERE b.name = 'Bob' RETURN b",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["bob"]);
    }

    /// Inside `OPTIONAL MATCH` — the anonymous propmap node matches nothing, so
    /// both forms must degrade identically to the carried-forward binding with
    /// the stage's new variables left unbound.
    #[test]
    fn anon_optional_match_propmap_matches_bound_form() {
        let v = fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (p:Person) OPTIONAL MATCH (x:Person {name:'Nobody'})-[:KNOWS]->(p) RETURN p",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (p:Person) OPTIONAL MATCH (:Person {name:'Nobody'})-[:KNOWS]->(p) RETURN p",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["alice", "bob", "carol"]);
    }

    /// Same start/hop-target/interior parity matrix over the SECOND fixture
    /// (`relationship_fixture`, Server/PROVIDES/CallableResource) — "several
    /// graph fixtures" per the acceptance bar, not just one shape repeated.
    #[test]
    fn anon_propmap_matches_bound_form_over_relationship_fixture() {
        let v = relationship_fixture();
        let bound = exec_cypher(
            &v,
            "MATCH (s:Server {name:'a-mcp'})-[:PROVIDES]->(r:CallableResource) RETURN r",
        )
        .unwrap();
        let anon = exec_cypher(
            &v,
            "MATCH (:Server {name:'a-mcp'})-[:PROVIDES]->(r:CallableResource) RETURN r",
        )
        .unwrap();
        assert_eq!(anon.rows, bound.rows, "byte-identical rows required");
        assert_eq!(ids(&anon, 0), vec!["res:a1", "res:a2"]);
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
        assert_eq!(qr.columns, vec!["size"]);
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

    /// `ORDER BY` on a property that is NOT itself a projected column (the
    /// projection is the bare node `a`, column name `"a"`; the sort key is
    /// `a.name`, deterministic scalar column name `"name"` — they don't match)
    /// must still resolve
    /// via the row's carried source `Binding`, per `order_value`'s fallback to
    /// `eval_scalar(view, &row.1, expr)`. This is the scenario `finalize()`'s
    /// per-row binding carry-through exists for: a query WITHOUT `ORDER BY`
    /// skips the clone entirely (an empty `Binding` is never read), but a query
    /// WITH `ORDER BY` must still carry the real one. Ordering DESC makes a
    /// dropped/empty binding detectable: `eval_scalar` would return `Null` for
    /// every row, `cmp_values` would treat them all as tied, and a stable sort
    /// would leave the label-index-order (ascending by id: alice, bob, carol)
    /// untouched instead of reversing it.
    #[test]
    fn order_by_unprojected_property_resolves_via_carried_binding() {
        let v = fixture();
        let qr = exec_cypher(&v, "MATCH (a:Person) RETURN a ORDER BY a.name DESC").unwrap();
        assert_eq!(
            ids_in_order(&qr, 0),
            vec!["carol", "bob", "alice"],
            "ORDER BY on an unprojected property must sort via the real binding, not tie"
        );
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

    /// Fixture matching the confirmed-production defect's shape: `:Preference`
    /// nodes keyed by a dotted `id` (the graph key, read via the virtual `id`
    /// property), plus a `value` property that is deliberately a mix of string,
    /// number, and absent — for exercising `STARTS WITH`/`ENDS WITH`/`CONTAINS`
    /// null/type semantics on the left-hand side.
    fn preference_fixture() -> GraphView {
        let core = GraphCore::new();
        core.add_node(
            "pref:llm.provider".into(),
            pbytes(serde_json::json!({"node_type":"Preference","value":"anthropic"})),
        );
        core.add_node(
            "pref:llm.model".into(),
            pbytes(serde_json::json!({"node_type":"Preference","value":"sonnet"})),
        );
        core.add_node(
            "pref:ui.theme".into(),
            pbytes(serde_json::json!({"node_type":"Preference","value":"dark"})),
        );
        // Non-string `value` — must not error and must not match a STARTS WITH/
        // ENDS WITH/CONTAINS predicate on `value`.
        core.add_node(
            "pref:count".into(),
            pbytes(serde_json::json!({"node_type":"Preference","value":7})),
        );
        // `value` entirely absent — same requirement (missing reads as null).
        core.add_node(
            "pref:novalue".into(),
            pbytes(serde_json::json!({"node_type":"Preference"})),
        );
        core.add_node(
            "other:not-a-preference".into(),
            pbytes(serde_json::json!({"node_type":"Other"})),
        );
        core.analysis_snapshot()
    }

    /// The exact production repro from the defect report:
    /// `MATCH (p:Preference) WHERE p.id STARTS WITH $prefix RETURN p.id AS id, p.value AS value`.
    /// This is the parse-error regression test's execution-level counterpart —
    /// it proves the query not only parses but MATCHES the right rows, end to end.
    #[test]
    fn starts_with_param_matches_production_repro_query() {
        let v = preference_fixture();
        let mut params = Params::new();
        params.insert("prefix".to_string(), Value::String("pref:llm.".to_string()));
        let qr = exec_cypher_params(
            &v,
            "MATCH (p:Preference) WHERE p.id STARTS WITH $prefix RETURN p.id AS id, p.value AS value",
            &params,
        )
        .unwrap();
        assert_eq!(qr.columns, vec!["id", "value"]);
        assert_eq!(ids(&qr, 0), vec!["pref:llm.model", "pref:llm.provider"]);
    }

    #[test]
    fn starts_with_param_no_match_returns_empty_not_error() {
        let v = preference_fixture();
        let mut params = Params::new();
        params.insert("prefix".to_string(), Value::String("nope:".to_string()));
        let qr = exec_cypher_params(
            &v,
            "MATCH (p:Preference) WHERE p.id STARTS WITH $prefix RETURN p.id AS id",
            &params,
        )
        .unwrap();
        assert!(qr.rows.is_empty(), "expected empty result, got {:?}", qr.rows);
    }

    #[test]
    fn starts_with_undefined_param_is_a_loud_error_not_a_silent_scan() {
        let v = preference_fixture();
        let err = exec_cypher(
            &v,
            "MATCH (p:Preference) WHERE p.id STARTS WITH $missing RETURN p.id",
        )
        .unwrap_err();
        assert!(
            err.contains("undefined parameter"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn starts_with_on_non_string_or_missing_property_is_false_not_error() {
        let v = preference_fixture();
        // "sonnet" (pref:llm.model) is the only string `value` starting with 's';
        // the numeric `value` (pref:count) and the entirely absent `value`
        // (pref:novalue) must silently NOT match — no error, no accidental match.
        let qr = exec_cypher(
            &v,
            "MATCH (p:Preference) WHERE p.value STARTS WITH 's' RETURN p.id AS id",
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["pref:llm.model"]);
    }

    #[test]
    fn ends_with_and_contains_accept_param_operand_end_to_end() {
        let v = preference_fixture();
        let mut suffix_params = Params::new();
        suffix_params.insert("suffix".to_string(), Value::String(".model".to_string()));
        let qr = exec_cypher_params(
            &v,
            "MATCH (p:Preference) WHERE p.id ENDS WITH $suffix RETURN p.id AS id",
            &suffix_params,
        )
        .unwrap();
        assert_eq!(ids(&qr, 0), vec!["pref:llm.model"]);

        let mut needle_params = Params::new();
        needle_params.insert("needle".to_string(), Value::String("llm".to_string()));
        let qr2 = exec_cypher_params(
            &v,
            "MATCH (p:Preference) WHERE p.id CONTAINS $needle RETURN p.id AS id",
            &needle_params,
        )
        .unwrap();
        assert_eq!(ids(&qr2, 0), vec!["pref:llm.model", "pref:llm.provider"]);
    }

    /// `labels(n)`: fixture nodes covering a single canonical `node_type`, a
    /// `node_type` PLUS an explicit multi-label `labels` array, and a fully
    /// unlabelled node (no `node_type`, no `labels`).
    fn labels_fixture() -> GraphView {
        let core = GraphCore::new();
        core.add_node("n1".into(), pbytes(serde_json::json!({"node_type":"Person"})));
        core.add_node(
            "n2".into(),
            pbytes(serde_json::json!({"node_type":"Person","labels":["Employee","Manager"]})),
        );
        core.add_node("n3".into(), pbytes(serde_json::json!({})));
        core.analysis_snapshot()
    }

    #[test]
    fn labels_returns_node_type_and_multi_label_array() {
        let v = labels_fixture();
        let qr = exec_cypher(&v, "MATCH (n) WHERE n.id = 'n1' RETURN labels(n)").unwrap();
        assert_eq!(
            cells_of(&qr, 0)[0],
            Value::Array(vec![Value::String("Person".into())])
        );

        let qr2 = exec_cypher(&v, "MATCH (n) WHERE n.id = 'n2' RETURN labels(n)").unwrap();
        assert_eq!(
            cells_of(&qr2, 0)[0],
            Value::Array(vec![
                Value::String("Person".into()),
                Value::String("Employee".into()),
                Value::String("Manager".into()),
            ])
        );
    }

    #[test]
    fn labels_on_unlabelled_node_is_empty_list_not_null_or_error() {
        let v = labels_fixture();
        let qr = exec_cypher(&v, "MATCH (n) WHERE n.id = 'n3' RETURN labels(n)").unwrap();
        assert_eq!(cells_of(&qr, 0)[0], Value::Array(vec![]));
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

    /// BUG-035 repro (CONCEPT:EG-KG.query.cypher-where-pushdown): `IS NULL` /
    /// `IS NOT NULL` over an unlabeled `MATCH (n)` must partition the full node
    /// count, and equality must agree with a singleton `IN` list, INCLUDING for a
    /// leading-underscore property name (the reported failing case was `_owner_id`).
    #[test]
    fn bug_035_null_and_equality_predicates_on_unlabeled_match() {
        let core = GraphCore::new();
        // Two nodes carry `_owner_id`; two do not (mirrors "unowned" nodes).
        core.add_node(
            "n1".into(),
            pbytes(serde_json::json!({"node_type":"Widget","_owner_id":"u1"})),
        );
        core.add_node(
            "n2".into(),
            pbytes(serde_json::json!({"node_type":"Widget","_owner_id":"u2"})),
        );
        core.add_node(
            "n3".into(),
            pbytes(serde_json::json!({"node_type":"Gadget"})),
        );
        core.add_node(
            "n4".into(),
            pbytes(serde_json::json!({"node_type":"Gadget"})),
        );
        let v = core.analysis_snapshot();

        let total = exec_cypher(&v, "MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(cells_of(&total, 0)[0], Value::Number(4.into()));

        let is_null =
            exec_cypher(&v, "MATCH (n) WHERE n._owner_id IS NULL RETURN count(n)").unwrap();
        let is_not_null = exec_cypher(
            &v,
            "MATCH (n) WHERE n._owner_id IS NOT NULL RETURN count(n)",
        )
        .unwrap();
        let null_n = cells_of(&is_null, 0)[0].as_i64().unwrap();
        let not_null_n = cells_of(&is_not_null, 0)[0].as_i64().unwrap();
        // The two predicates must partition the total — this is the exact
        // contradiction measured live: 47455 / 0 / 0.
        assert_eq!(
            null_n + not_null_n,
            4,
            "IS NULL + IS NOT NULL must sum to total"
        );
        assert_eq!(null_n, 2, "n3, n4 lack _owner_id");
        assert_eq!(not_null_n, 2, "n1, n2 carry _owner_id");

        // Equality vs IN must return the SAME rows for a normal property...
        let eq_nt =
            exec_cypher(&v, "MATCH (n) WHERE n.node_type = 'Widget' RETURN count(n)").unwrap();
        let in_nt = exec_cypher(
            &v,
            "MATCH (n) WHERE n.node_type IN ['Widget'] RETURN count(n)",
        )
        .unwrap();
        assert_eq!(cells_of(&eq_nt, 0)[0], Value::Number(2.into()));
        assert_eq!(cells_of(&eq_nt, 0)[0], cells_of(&in_nt, 0)[0]);

        // ...and for a leading-underscore property.
        let eq_owner =
            exec_cypher(&v, "MATCH (n) WHERE n._owner_id = 'u1' RETURN count(n)").unwrap();
        let in_owner =
            exec_cypher(&v, "MATCH (n) WHERE n._owner_id IN ['u1'] RETURN count(n)").unwrap();
        assert_eq!(cells_of(&eq_owner, 0)[0], Value::Number(1.into()));
        assert_eq!(cells_of(&eq_owner, 0)[0], cells_of(&in_owner, 0)[0]);
    }

    /// Coordinator disproof check (2026-08-09): the earlier `bug_035_null_and_...`
    /// test mixed present + absent `_owner_id` on the SAME query. This isolates the
    /// claim precisely: a property NO node in the graph carries at all. If the
    /// coordinator is right, `IS NULL` on a universally-absent property returns 0
    /// instead of the total, and `IS NULL` + `IS NOT NULL` do not sum to the total.
    #[test]
    fn coordinator_disproof_universally_absent_property_is_null_check() {
        let core = GraphCore::new();
        core.add_node(
            "n1".into(),
            pbytes(serde_json::json!({"node_type":"Widget"})),
        );
        core.add_node(
            "n2".into(),
            pbytes(serde_json::json!({"node_type":"Widget"})),
        );
        core.add_node(
            "n3".into(),
            pbytes(serde_json::json!({"node_type":"Gadget"})),
        );
        core.add_node(
            "n4".into(),
            pbytes(serde_json::json!({"node_type":"Gadget"})),
        );
        let v = core.analysis_snapshot();

        let total = exec_cypher(&v, "MATCH (n) RETURN count(n)").unwrap();
        assert_eq!(cells_of(&total, 0)[0], Value::Number(4.into()));

        let is_null = exec_cypher(
            &v,
            "MATCH (n) WHERE n.zzz_definitely_absent_property_xyz IS NULL RETURN count(n)",
        )
        .unwrap();
        let is_not_null = exec_cypher(
            &v,
            "MATCH (n) WHERE n.zzz_definitely_absent_property_xyz IS NOT NULL RETURN count(n)",
        )
        .unwrap();
        eprintln!("IS NULL result: {is_null:?}");
        eprintln!("IS NOT NULL result: {is_not_null:?}");
        assert_eq!(
            cells_of(&is_null, 0)[0],
            Value::Number(4.into()),
            "openCypher: a universally-absent property must satisfy IS NULL for every row"
        );
        assert_eq!(
            cells_of(&is_not_null, 0)[0],
            Value::Number(0.into()),
            "a universally-absent property must satisfy IS NOT NULL for no row"
        );
    }

    /// Coordinator lead #3 (2026-08-09), CORRECTED: the first attempt at this test
    /// sent `$_visibility_owner_id` straight to `exec_cypher_params` and got a PARSE
    /// ERROR — `Test::Cmp`'s operand is `plan::Value` (a resolved literal, see
    /// `plan.rs:222-227`), not a parameterizable expression, so `parse_literal`
    /// (`parser.rs:656-672`) has no `Tok::Param` arm and the WHERE-clause `=`/`IN`
    /// grammar genuinely cannot reference `$name` AT ALL — confirmed independently by
    /// `EpistemicGraphBackend._inline_cypher_params`'s own docstring
    /// (`agent-utilities/.../backends/epistemic_graph_backend.py:172-180`): "the
    /// engine's hand-written parser only implements `literal := string | number |
    /// true | false`". That is why the Python backend renders every `$param`
    /// reference into a literal via `_cypher_literal` BEFORE the query ever reaches
    /// this parser — `execute_read`/`execute_write` never call `exec_cypher_params`
    /// with bound parameters; they always send fully-inlined literal text. My first
    /// attempt tested a shape the real system never sends. This version sends the
    /// exact literal-inlined text `_inline_cypher_params` actually produces:
    /// ```text
    /// python3 -c "
    /// from agent_utilities.knowledge_graph.core.cypher_scoping import inject_and_predicate
    /// from agent_utilities.knowledge_graph.backends.epistemic_graph_backend import EpistemicGraphBackend
    /// cond = \"(n._owner_id = \$_visibility_owner_id OR n._shared_scope IN ['org', 'commons'] OR n._owner_id IS NULL)\"
    /// scoped = inject_and_predicate('MATCH (n) WHERE n.node_type IS NOT NULL RETURN count(n)', cond)
    /// print(EpistemicGraphBackend._inline_cypher_params(scoped, {'_visibility_owner_id': 'me'}))
    /// "
    /// ```
    #[test]
    fn coordinator_disproof_composed_visibility_and_predicate() {
        let core = GraphCore::new();
        // A mix: some nodes visible via owner match, some via org/commons scope, some
        // via the owner-null branch (unowned/public), one node_type absent everywhere
        // relevant, all node_type values present so IN/`=` have real rows to find.
        core.add_node(
            "owned_by_me".into(),
            pbytes(serde_json::json!({"node_type":"InboundMessage","_owner_id":"me"})),
        );
        core.add_node(
            "owned_by_other".into(),
            pbytes(serde_json::json!({"node_type":"InboundMessage","_owner_id":"someone_else"})),
        );
        core.add_node(
            "org_scoped".into(),
            pbytes(
                serde_json::json!({"node_type":"InboundMessage","_owner_id":"someone_else","_shared_scope":"org"}),
            ),
        );
        core.add_node(
            "unowned".into(),
            pbytes(serde_json::json!({"node_type":"Thread"})),
        );
        let v = core.analysis_snapshot();

        // Sanity: the bare, LITERAL-INLINED visibility predicate alone (mirrors the
        // aggregate base query, which the live evidence shows returns the true total).
        let base = exec_cypher(
            &v,
            "MATCH (n) WHERE (n._owner_id = 'me' OR n._shared_scope IN ['org', 'commons'] OR n._owner_id IS NULL) RETURN count(n)",
        )
        .unwrap();
        // owned_by_me (owner match), org_scoped (org scope), unowned (owner IS NULL) are
        // visible; owned_by_other is not (owned by someone else, not org/commons-scoped).
        assert_eq!(
            cells_of(&base, 0)[0],
            Value::Number(3.into()),
            "bare visibility predicate sanity check"
        );

        let is_not_null = exec_cypher(
            &v,
            "MATCH (n) WHERE (n._owner_id = 'me' OR n._shared_scope IN ['org', 'commons'] OR n._owner_id IS NULL) AND (n.node_type IS NOT NULL) RETURN count(n)",
        )
        .unwrap();
        let eq = exec_cypher(
            &v,
            "MATCH (n) WHERE (n._owner_id = 'me' OR n._shared_scope IN ['org', 'commons'] OR n._owner_id IS NULL) AND (n.node_type = 'InboundMessage') RETURN count(n)",
        )
        .unwrap();
        let inn = exec_cypher(
            &v,
            "MATCH (n) WHERE (n._owner_id = 'me' OR n._shared_scope IN ['org', 'commons'] OR n._owner_id IS NULL) AND (n.node_type IN ['InboundMessage']) RETURN count(n)",
        )
        .unwrap();
        eprintln!("composed IS NOT NULL: {is_not_null:?}");
        eprintln!("composed =: {eq:?}");
        eprintln!("composed IN: {inn:?}");

        // Every node in this fixture HAS node_type, so IS NOT NULL should equal the
        // base visibility count (3), same as the bare-form test already proved.
        assert_eq!(cells_of(&is_not_null, 0)[0], Value::Number(3.into()));
        // Visible InboundMessage nodes: owned_by_me + org_scoped = 2 (owned_by_other is
        // filtered by visibility regardless of node_type).
        assert_eq!(cells_of(&eq, 0)[0], Value::Number(2.into()));
        assert_eq!(
            cells_of(&eq, 0)[0],
            cells_of(&inn, 0)[0],
            "composed = and composed IN must agree"
        );
    }

    /// Coordinator disproof check, point 4: `=` vs `IN` against a UNIVERSALLY-absent
    /// property. Both must agree (both false for every row, since the property is
    /// absent everywhere) — this isolates whether the earlier `node_type = 'X'` vs
    /// `IN ['X']` divergence reproduces for an absent (as opposed to present) property.
    #[test]
    fn coordinator_disproof_absent_property_eq_vs_in_agree() {
        let core = GraphCore::new();
        core.add_node(
            "n1".into(),
            pbytes(serde_json::json!({"node_type":"Widget"})),
        );
        core.add_node(
            "n2".into(),
            pbytes(serde_json::json!({"node_type":"Gadget"})),
        );
        let v = core.analysis_snapshot();

        let eq = exec_cypher(
            &v,
            "MATCH (n) WHERE n.zzz_definitely_absent_property_xyz = 'nope' RETURN count(n)",
        )
        .unwrap();
        let inn = exec_cypher(
            &v,
            "MATCH (n) WHERE n.zzz_definitely_absent_property_xyz IN ['nope'] RETURN count(n)",
        )
        .unwrap();
        assert_eq!(cells_of(&eq, 0)[0], Value::Number(0.into()));
        assert_eq!(
            cells_of(&eq, 0)[0],
            cells_of(&inn, 0)[0],
            "= and IN must agree on an absent property too"
        );
    }

    /// BUG-035 hardening: a WHERE predicate over a node whose STORED property blob is
    /// undecodable (corrupted bytes / a cross-version encoding mismatch) must abort the
    /// query with an explicit error — never silently read the property as absent, which
    /// `IS NULL` cannot tell apart from a genuine NULL. BEFORE this fix, `node_prop`'s
    /// `.ok()?` collapsed a decode failure into `None`, so `n._owner_id IS NULL` would
    /// silently (and wrongly) count the corrupted node as a real NULL match — exactly
    /// the "0 reads as a real answer" failure class BUG-035 exists to close, just with
    /// a genuinely unrecoverable input instead of a merely-absent one. AFTER this fix,
    /// `exec_cypher` returns `Err` instead of a falsely-precise count.
    #[test]
    fn bug_035_corrupted_property_blob_errors_loudly_instead_of_reading_as_null() {
        let core = GraphCore::new();
        core.add_node(
            "good1".into(),
            pbytes(serde_json::json!({"node_type": "Widget"})),
        );
        // 0xC1 is a reserved MessagePack byte that never appears in a valid encoding —
        // this blob cannot decode under any interpretation, simulating on-disk
        // corruption or a cross-version encoding mismatch.
        core.add_node("corrupt1".into(), vec![0xC1]);
        let v = core.analysis_snapshot();

        let result = exec_cypher(&v, "MATCH (n) WHERE n._owner_id IS NULL RETURN count(n)");
        assert!(
            result.is_err(),
            "a corrupted property blob must abort the query with an explicit error, \
             not silently read as NULL (got {result:?})"
        );
    }

    /// ADR-5 / W2.2 item 5: the WorkItem lifecycle-state DISTRIBUTION is queryable via
    /// Cypher over the co-located statechart projection (the `machine_state` node
    /// property the phase-1 mirror writes). A grouped `count(*)` over `machine_state`
    /// answers "how many work items are in each lifecycle state" directly against the
    /// authoritative graph — no bespoke aggregation endpoint needed.
    #[test]
    fn work_item_machine_state_distribution_is_queryable() {
        let core = GraphCore::new();
        // A small fleet: 2 leased, 1 running, 1 succeeded, plus a non-WorkItem node the
        // label filter must exclude.
        for (id, state) in [
            ("wi-1", "leased"),
            ("wi-2", "leased"),
            ("wi-3", "running"),
            ("wi-4", "succeeded"),
        ] {
            core.add_node(
                id.into(),
                pbytes(serde_json::json!({
                    "node_type": "WorkItem",
                    "status": state,
                    "machine_state": state,
                    "machine_version": 1,
                })),
            );
        }
        core.add_node(
            "other".into(),
            pbytes(serde_json::json!({"node_type": "Agent"})),
        );
        let v = core.analysis_snapshot();

        let qr = exec_cypher(
            &v,
            "MATCH (w:WorkItem) RETURN w.machine_state AS state, count(*) AS n",
        )
        .unwrap();
        // One row per distinct lifecycle state (implicit GROUP BY the non-agg key).
        let mut dist: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
        for row in 0..qr.rows.len() {
            let cells = cells_of(&qr, row);
            let state = cells[0].as_str().unwrap().to_string();
            let n = cells[1].as_i64().unwrap();
            dist.insert(state, n);
        }
        assert_eq!(dist.get("leased"), Some(&2));
        assert_eq!(dist.get("running"), Some(&1));
        assert_eq!(dist.get("succeeded"), Some(&1));
        assert_eq!(
            dist.len(),
            3,
            "only the three occupied WorkItem states appear"
        );

        // A single lifecycle state is likewise a plain label + property filter — the
        // "which work items are stuck in X" operator question.
        let leased = exec_cypher(
            &v,
            "MATCH (w:WorkItem) WHERE w.machine_state = 'leased' RETURN w.id AS id",
        )
        .unwrap();
        assert_eq!(leased.rows.len(), 2, "exactly the two leased work items");
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
        assert_eq!(qr.columns, vec!["state"]);
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

    // ── plan cache (CONCEPT:EG-KG.query.dep-free-behind) — cached vs uncached parity ──────

    /// A representative sample of the corpus's shapes above (label scan, WHERE,
    /// fixed + variable-length hop, OPTIONAL MATCH, WITH + filter, ORDER BY/SKIP/
    /// LIMIT, aggregation, DISTINCT, `RETURN *`, UNWIND, CALL subquery, path
    /// variable, undirected edge, quantified group) — every string here is copied
    /// from an already-passing test above, so this only exercises the plan-cache
    /// wiring, not new query semantics.
    const PLAN_CACHE_COVERAGE_QUERIES: &[&str] = &[
        "MATCH (a:Person) RETURN a",
        "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a, a.id, a.node_type",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) WHERE a.name = 'Alice' RETURN b",
        "MATCH (p:Person) OPTIONAL MATCH (x:Person {name:'Nobody'})-[:KNOWS]->(p) RETURN p",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WITH b WHERE b.name = 'Carol' RETURN b",
        "MATCH (a:Person) RETURN a.name ORDER BY a.name SKIP 1 LIMIT 1",
        "MATCH (a:Person) RETURN count(a), collect(a.name)",
        "MATCH (a:Person) RETURN DISTINCT a.node_type",
        "MATCH (a:Person) WHERE a.name = 'Alice' RETURN *",
        "UNWIND [1, 2, 3] AS x RETURN x",
        "CALL { MATCH (a:Person) RETURN a } RETURN a",
        "MATCH p = (a:Person)-[:KNOWS*1..1]->(b:Person) WHERE a.name = 'Alice' RETURN p",
        "MATCH (a:Person)-[:KNOWS]-(b:Person) WHERE a.name = 'Bob' RETURN b",
        "MATCH (a:Person)((x)-[:KNOWS]->(y)){1,3}(b:Person) WHERE a.name = 'Alice' RETURN b",
    ];

    #[test]
    fn plan_cache_hit_path_is_byte_identical_to_the_uncached_first_call() {
        let v = fixture();
        for text in PLAN_CACHE_COVERAGE_QUERIES {
            // First call: a miss (or a hit left over from another test/parallel run
            // using the same text — either way harmless, see plan_cache's module
            // doc) always parses+executes correctly. Second call is guaranteed a
            // HIT (the first call just populated the cache for this exact text).
            let first = exec_cypher(&v, text).unwrap_or_else(|e| panic!("{text}: {e}"));
            let second = exec_cypher(&v, text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(first.columns, second.columns, "columns diverged for {text}");
            assert_eq!(first.rows, second.rows, "rows diverged for {text}");
        }
    }

    #[test]
    fn plan_cache_hit_path_is_byte_identical_with_query_parameters() {
        // Same parity proof, but through the `$params` form — the AST caches on
        // TEXT alone, so a parameter binding must still resolve identically on a
        // cache hit as it did on the miss that populated the entry.
        let v = fixture();
        let text = "MATCH (t:Person {name: $tid})-[:KNOWS]->(tc:Person) RETURN tc";
        let mut params = Params::new();
        params.insert("tid".into(), Value::String("Alice".into()));

        let first = exec_cypher_params(&v, text, &params).unwrap();
        let second = exec_cypher_params(&v, text, &params).unwrap();
        assert_eq!(first.columns, second.columns);
        assert_eq!(first.rows, second.rows);
    }

    // ── Phase A: WHERE pushdown · LIMIT short-circuit · label-index-first start ────

    /// A hub with `mids` MID-children, each carrying `leaves` LEAF-children, so the
    /// two-hop `(hub)-[:MID]->(m)-[:LEAF]->(x)` pattern has `mids * leaves` matches.
    fn wide_fixture(mids: usize, leaves: usize) -> GraphView {
        let core = GraphCore::new();
        core.add_node("hub".into(), pbytes(serde_json::json!({"node_type":"Hub"})));
        for i in 0..mids {
            let m = format!("m{i}");
            core.add_node(m.clone(), pbytes(serde_json::json!({"node_type":"Mid"})));
            core.add_edge(
                "hub".into(),
                m.clone(),
                pbytes(serde_json::json!({"relationship":"MID"})),
            )
            .unwrap();
            for j in 0..leaves {
                let x = format!("x{i}_{j}");
                core.add_node(x.clone(), pbytes(serde_json::json!({"node_type":"Leaf"})));
                core.add_edge(
                    m.clone(),
                    x.clone(),
                    pbytes(serde_json::json!({"relationship":"LEAF"})),
                )
                .unwrap();
            }
        }
        core.analysis_snapshot()
    }

    /// ACCEPTANCE (CONCEPT:EG-KG.query.cypher-limit-shortcircuit): `MATCH … LIMIT 1`
    /// over a 40k-match set expands O(limit · degree) partials, NOT all 40k. The
    /// instrumented hop-expansion counter is the proof: the un-limited walk builds every
    /// one of the 40k bindings; the LIMIT walk reaches the first complete row after a
    /// tiny constant number of expansions (one per hop level), and is orders of magnitude
    /// cheaper. This is the DEPTH-FIRST short-circuit — a breadth-first last-hop cap could
    /// not avoid the intermediate MID-level blow-up this two-hop pattern exercises.
    #[test]
    fn limit_short_circuit_is_o_limit_deg_on_40k_matches() {
        let v = wide_fixture(200, 200); // 40_000 two-hop matches
        let q = "MATCH (h:Hub)-[:MID]->(m)-[:LEAF]->(x) RETURN x";

        // Baseline: no LIMIT materializes every match.
        walk_metrics::reset();
        let full = exec_cypher(&v, q).unwrap();
        let (_, full_hops) = walk_metrics::snapshot();
        assert_eq!(full.rows.len(), 40_000);
        assert!(
            full_hops >= 40_000,
            "baseline must expand every match, got {full_hops}"
        );

        // Short-circuit: LIMIT 1 reaches the first row after O(1) work per hop.
        walk_metrics::reset();
        let one = exec_cypher(&v, &format!("{q} LIMIT 1")).unwrap();
        let (_, ltd_hops) = walk_metrics::snapshot();
        assert_eq!(one.rows.len(), 1);
        // Reaching the first leaf touches the first mid (1 MID expansion) then its first
        // leaf (1 LEAF expansion): a tiny constant, FAR below the 40k the naive path pays.
        assert!(
            ltd_hops <= 8,
            "LIMIT 1 must be O(limit·deg), expanded {ltd_hops}"
        );
        assert!(
            ltd_hops.saturating_mul(100) < full_hops,
            "short-circuit must be orders of magnitude cheaper: {ltd_hops} vs {full_hops}"
        );
    }

    /// The DFS short-circuit honours `SKIP`+`LIMIT` (budget = skip+limit) and returns the
    /// right row count, and is DISABLED by a blocking op (ORDER BY) so that path stays
    /// fully materialized and correctly ordered.
    #[test]
    fn limit_short_circuit_respects_skip_and_disables_on_order_by() {
        let v = wide_fixture(10, 10); // 100 matches
        let skipped = exec_cypher(
            &v,
            "MATCH (h:Hub)-[:MID]->(m)-[:LEAF]->(x) RETURN x SKIP 3 LIMIT 2",
        )
        .unwrap();
        assert_eq!(skipped.rows.len(), 2, "SKIP 3 LIMIT 2 ⇒ 2 rows");

        // ORDER BY forces full materialization (no short-circuit) and a correct sort.
        let ordered = exec_cypher(
            &v,
            "MATCH (h:Hub)-[:MID]->(m)-[:LEAF]->(x) RETURN x.node_type ORDER BY x.node_type LIMIT 5",
        )
        .unwrap();
        assert_eq!(ordered.rows.len(), 5);
    }

    /// Per-hop WHERE pushdown (CONCEPT:EG-KG.query.cypher-where-pushdown): a predicate on
    /// the START variable is applied BEFORE any hop expands, so only the surviving start's
    /// neighbours are walked — the counter proves the pruned start's subtree is untouched.
    #[test]
    fn where_on_start_var_is_pushed_before_hops() {
        let core = GraphCore::new();
        for hub in ["keep", "drop"] {
            core.add_node(
                hub.into(),
                pbytes(serde_json::json!({"node_type":"Hub","tag":hub})),
            );
            for j in 0..100 {
                let x = format!("{hub}_{j}");
                core.add_node(x.clone(), pbytes(serde_json::json!({"node_type":"Leaf"})));
                core.add_edge(
                    hub.into(),
                    x.clone(),
                    pbytes(serde_json::json!({"relationship":"HAS"})),
                )
                .unwrap();
            }
        }
        let v = core.analysis_snapshot();

        walk_metrics::reset();
        let qr = exec_cypher(
            &v,
            "MATCH (h:Hub)-[:HAS]->(x) WHERE h.tag = 'keep' RETURN x",
        )
        .unwrap();
        let (_, hops) = walk_metrics::snapshot();
        assert_eq!(qr.rows.len(), 100, "only the kept hub's 100 leaves");
        assert_eq!(
            hops, 100,
            "WHERE on the start var must prune 'drop' before its 100 hops expand"
        );
    }

    /// A WHERE on a HOP-TARGET variable is applied the instant that hop binds, dropping
    /// the partial before the NEXT hop expands from it (CONCEPT:EG-KG.query.cypher-where-pushdown).
    #[test]
    fn where_on_hop_target_prunes_before_next_hop() {
        let core = GraphCore::new();
        core.add_node("hub".into(), pbytes(serde_json::json!({"node_type":"Hub"})));
        for mid in ["keep", "drop"] {
            core.add_node(
                mid.into(),
                pbytes(serde_json::json!({"node_type":"Mid","tag":mid})),
            );
            core.add_edge(
                "hub".into(),
                mid.into(),
                pbytes(serde_json::json!({"relationship":"MID"})),
            )
            .unwrap();
            for j in 0..50 {
                let x = format!("{mid}_{j}");
                core.add_node(x.clone(), pbytes(serde_json::json!({"node_type":"Leaf"})));
                core.add_edge(
                    mid.into(),
                    x.clone(),
                    pbytes(serde_json::json!({"relationship":"LEAF"})),
                )
                .unwrap();
            }
        }
        let v = core.analysis_snapshot();
        walk_metrics::reset();
        let qr = exec_cypher(
            &v,
            "MATCH (h:Hub)-[:MID]->(m)-[:LEAF]->(x) WHERE m.tag='keep' RETURN x",
        )
        .unwrap();
        let (_, hops) = walk_metrics::snapshot();
        assert_eq!(qr.rows.len(), 50);
        // hop0: 2 mids expand (2). hop1: only 'keep' (dropped after the MID hop) expands
        // 50. = 52. Without pushdown both mids' 100 leaves expand (102) then filter to 50.
        assert_eq!(
            hops, 52,
            "m.tag WHERE must prune 'drop' after the MID hop, before LEAF expands"
        );
    }

    /// Multi-position WHERE (start var + hop-target var) partitions correctly across
    /// positions and yields the same rows a post-materialization filter would; the edge
    /// binding (`type(r)`) survives the pushdown.
    #[test]
    fn multi_position_where_pushdown_is_correct() {
        let v = fixture(); // alice-KNOWS->bob-KNOWS->carol
        let qr = exec_cypher(
            &v,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.name='Alice' AND b.name='Bob' \
             RETURN a.name, type(r), b.name",
        )
        .unwrap();
        assert_eq!(qr.rows.len(), 1);
        let row = cells_of(&qr, 0);
        assert_eq!(row[0].as_str(), Some("Alice"));
        assert_eq!(row[1].as_str(), Some("KNOWS"));
        assert_eq!(row[2].as_str(), Some("Bob"));
    }

    /// Label-index-first start selection (CONCEPT:EG-KG.query.cypher-label-first-start): an
    /// UNLABELED start with a LABELED far end walks from the labeled end, so only the few
    /// labeled nodes seed the walk instead of the whole graph. The start counter proves it,
    /// and the result set is unchanged (all 500 sources still reach the rare sink).
    #[test]
    fn unlabeled_start_with_labeled_end_walks_from_the_label() {
        let core = GraphCore::new();
        core.add_node(
            "rare".into(),
            pbytes(serde_json::json!({"node_type":"Rare"})),
        );
        for i in 0..500 {
            let s = format!("s{i}");
            core.add_node(s.clone(), pbytes(serde_json::json!({"node_type":"Src"})));
            core.add_edge(
                s.clone(),
                "rare".into(),
                pbytes(serde_json::json!({"relationship":"TO"})),
            )
            .unwrap();
        }
        let v = core.analysis_snapshot();

        walk_metrics::reset();
        let qr = exec_cypher(&v, "MATCH (a)-[:TO]->(b:Rare) RETURN a").unwrap();
        let (starts, _) = walk_metrics::snapshot();
        assert_eq!(
            ids(&qr, 0).len(),
            500,
            "all 500 sources reach the rare sink"
        );
        assert_eq!(
            starts, 1,
            "must seed from the single labeled end, not the 501-node full-graph scan"
        );
    }

    /// The label-first REVERSAL is semantics-preserving: reversing to start at the labeled
    /// end binds the identical rows (incl. the intermediate + edge variables) that a
    /// forced forward walk would. Cross-checks the reversed result against the same query
    /// with the start explicitly labeled (which does NOT reverse).
    #[test]
    fn label_first_reversal_matches_forward_walk() {
        let v = fixture(); // alice-KNOWS->bob (Person), etc.
                           // Unlabeled start, labeled end ⇒ reversed internally.
        let rev = exec_cypher(&v, "MATCH (a)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        // Labeled start ⇒ forward walk, same result set.
        let fwd = exec_cypher(&v, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        assert_eq!(ids(&rev, 0), ids(&fwd, 0), "start-column set must match");
        assert_eq!(ids(&rev, 1), ids(&fwd, 1), "end-column set must match");
    }

    // ── Phase B: EPISTEMIC_GRAPH_CYPHER_ENGINE plan/shadow differential harness ────

    /// The `plan` engine genuinely cost-reorders a BOTH-labeled pattern toward the smaller
    /// label (which the legacy label-first heuristic — unlabeled-start-only — does not),
    /// AND still agrees with legacy on the result set. 100 `:A` each → one `:B`.
    #[cfg(feature = "cypher-plan")]
    #[test]
    fn plan_engine_cost_reorders_both_labeled_and_agrees() {
        let core = GraphCore::new();
        core.add_node("b".into(), pbytes(serde_json::json!({"node_type":"B"})));
        for i in 0..100 {
            let a = format!("a{i}");
            core.add_node(a.clone(), pbytes(serde_json::json!({"node_type":"A"})));
            core.add_edge(
                a.clone(),
                "b".into(),
                pbytes(serde_json::json!({"relationship":"R"})),
            )
            .unwrap();
        }
        let v = core.analysis_snapshot();
        let q = "MATCH (a:A)-[:R]->(b:B) RETURN a";

        // Agreement — the shadow flip gate.
        assert_eq!(engine::diff_for_test(&v, q, &Params::new()).unwrap(), None);

        // Legacy keeps the labeled start (seeds from 100 :A); plan cost-reverses (seeds
        // from the single :B). Same 100-row result, very different work.
        walk_metrics::reset();
        let legacy = engine::run_legacy_for_test(&v, q, &Params::new()).unwrap();
        let (legacy_starts, _) = walk_metrics::snapshot();

        walk_metrics::reset();
        let planned = engine::run_plan_for_test(&v, q, &Params::new()).unwrap();
        let (plan_starts, _) = walk_metrics::snapshot();

        assert_eq!(legacy.rows.len(), 100);
        assert_eq!(planned.rows.len(), 100);
        assert_eq!(legacy_starts, 100, "legacy keeps the 100 :A labeled start");
        assert_eq!(
            plan_starts, 1,
            "plan engine must cost-reverse to seed from the single :B"
        );
    }

    /// Shadow-mode ZERO-DIVERGENCE over a representative corpus: labeled/unlabeled starts,
    /// multi-hop, WHERE, LIMIT/SKIP, ORDER BY, DISTINCT, aggregation, `type(r)`, and a
    /// variable-length hop — the plan engine must return the SAME rows legacy does (modulo
    /// order when there is no ORDER BY) for every one.
    #[cfg(feature = "cypher-plan")]
    #[test]
    fn shadow_corpus_zero_divergence() {
        let f = fixture();
        let corpus: &[&str] = &[
            "MATCH (a:Person) RETURN a",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            "MATCH (a)-[:KNOWS]->(b:Person) RETURN a, b",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b",
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r), b.name ORDER BY b.name",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' RETURN b",
            "MATCH (a:Person) RETURN a LIMIT 1",
            "MATCH (a:Person) RETURN a.name ORDER BY a.name DESC SKIP 1 LIMIT 1",
            "MATCH (a:Person) RETURN count(a)",
            "MATCH (a:Person) RETURN DISTINCT a.node_type",
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN b",
            "MATCH (a) RETURN a",
            // OPTIONAL MATCH, WITH pipelining, and an undirected hop — the plan engine
            // leaves the latter two shapes' order untouched, but shadow must still confirm
            // no accidental divergence on the full read grammar.
            "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) RETURN a, b",
            "MATCH (a:Person) WITH a WHERE a.name = 'Alice' RETURN a",
            "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a, b",
            "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
        ];
        for q in corpus {
            assert_eq!(
                engine::diff_for_test(&f, q, &Params::new()).unwrap(),
                None,
                "shadow-mode divergence on: {q}"
            );
        }
    }

    /// Property-based shadow differential: a seeded generator (xorshift, no `rand` dep)
    /// builds bounded random graphs and random linear patterns, asserting ZERO divergence
    /// between the legacy and plan engines across every generated shape. This is the
    /// differential the ADR's flip gate requires; a real reversal/cost bug would surface
    /// as a divergence here rather than passing silently.
    #[cfg(feature = "cypher-plan")]
    #[test]
    fn shadow_property_based_zero_divergence() {
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let labels = ["A", "B", "C"];
        let rels = ["R", "S"];
        let node_pat = |var: &str, r: &mut dyn FnMut() -> u64| -> String {
            // ~2/3 of positions carry a label (so cost_reorder has asymmetric ends to
            // reorder); the rest are unlabeled.
            if r().is_multiple_of(3) {
                format!("({var})")
            } else {
                format!("({var}:{})", labels[(r() % labels.len() as u64) as usize])
            }
        };

        for iter in 0..300 {
            let core = GraphCore::new();
            let n = 3 + (rng() % 8) as usize; // 3..10 nodes
            let ids: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
            for id in &ids {
                let lbl = labels[(rng() % labels.len() as u64) as usize];
                core.add_node(
                    id.clone(),
                    pbytes(serde_json::json!({"node_type": lbl, "v": (rng() % 5) as i64})),
                );
            }
            let edges = (rng() % (2 * n as u64)) as usize;
            for _ in 0..edges {
                let a = &ids[(rng() % n as u64) as usize];
                let b = &ids[(rng() % n as u64) as usize];
                if a != b {
                    let rel = rels[(rng() % rels.len() as u64) as usize];
                    let _ = core.add_edge(
                        a.clone(),
                        b.clone(),
                        pbytes(serde_json::json!({"relationship": rel})),
                    );
                }
            }
            let v = core.analysis_snapshot();

            let hops = 1 + (rng() % 3) as usize; // 1..3 hops
            let mut q = String::from("MATCH ");
            q.push_str(&node_pat("a0", &mut rng));
            for h in 0..hops {
                let rel = rels[(rng() % rels.len() as u64) as usize];
                q.push_str(&format!("-[:{rel}]->"));
                q.push_str(&node_pat(&format!("a{}", h + 1), &mut rng));
            }
            q.push_str(" RETURN a0");
            if rng() % 2 == 0 {
                q.push_str(" LIMIT 3");
            }

            assert_eq!(
                engine::diff_for_test(&v, &q, &Params::new()).unwrap(),
                None,
                "shadow divergence on generated query (iter {iter}): {q}"
            );
        }
    }

    // ── indexed start-candidate resolution (CONCEPT:EG-KG.storage.index-manager-seam) ───────────────

    /// Like [`fixture`] but also returns the owning `GraphCore` (kept alive alongside
    /// the snapshot) and the OCC version it was captured at — what
    /// [`IndexSource`]/`exec_cypher_params_indexed` need. Mirrors the production
    /// `Method::CypherQuery` handler's own `analysis_snapshot_versioned()` pairing.
    #[cfg(feature = "result-cache")]
    fn fixture_versioned() -> (GraphCore, GraphView, u64) {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(serde_json::json!({"node_type":"Person","name":"Alice","tenant_id":"homelab"})),
        );
        core.add_node(
            "bob".into(),
            pbytes(serde_json::json!({"node_type":"Person","name":"Bob","tenant_id":"homelab"})),
        );
        core.add_node(
            "carol".into(),
            pbytes(serde_json::json!({"node_type":"Person","name":"Carol","tenant_id":"other"})),
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
        let (view, version) = core.analysis_snapshot_versioned();
        (core, view, version)
    }

    /// Fixture for the warm-label-index prefilter (this lane's fix,
    /// CONCEPT:EG-KG.compute.consult-lazy): one node per field `GraphCore.label_index`'s
    /// BROADER write-path contract keys on (`type`/`node_type`/`label`/`labels`), so
    /// tests can pin exactly which shapes the narrower Cypher `(var:Label)` predicate
    /// (`node_type` + `labels` only, see `node_has_label`) must and must NOT match:
    ///   * `na1` — labeled ONLY via `node_type` (narrow-matchable).
    ///   * `la1` — labeled ONLY via the multi-valued `labels` array (narrow-matchable).
    ///   * `tb1` — labeled ONLY via `type` — the TRAP: broad-index-matchable but
    ///     narrow-UNMATCHABLE, so it must never appear in a Cypher `:Gamma` result.
    ///   * `lb1` — labeled ONLY via `label` — the same trap, the other broad-only field.
    fn label_trap_fixture_versioned() -> (GraphCore, GraphView, u64) {
        let core = GraphCore::new();
        core.add_node("na1".into(), pbytes(serde_json::json!({"node_type": "Alpha"})));
        core.add_node(
            "la1".into(),
            pbytes(serde_json::json!({"labels": ["Beta", "Other"]})),
        );
        core.add_node("tb1".into(), pbytes(serde_json::json!({"type": "Gamma"})));
        core.add_node("lb1".into(), pbytes(serde_json::json!({"label": "Gamma"})));
        let (view, version) = core.analysis_snapshot_versioned();
        (core, view, version)
    }

    /// (a) Result-identical vs the cold `label_candidates` scan for every fixture
    /// shape — INCLUDING the trap case, which the naive "just swap the index" fix
    /// this lane's task explicitly warns against would get wrong: `tb1`/`lb1` carry
    /// only the BROADER `type`/`label` fields `GraphCore.label_index` keys on, so a
    /// naive swap would start matching `:Gamma` for nodes that must never match it.
    /// The prefilter-then-verify shape (`indexed_label_candidates` narrows via the
    /// warm index, `resolve_match`'s existing `node_has_label_point` re-verifies the
    /// NARROW predicate on each candidate) must produce the exact same answer the
    /// cold, unindexed path does in every case.
    #[test]
    #[cfg(feature = "result-cache")]
    fn warm_label_prefilter_matches_cold_scan_including_trap_labels() {
        let (core, view, version) = label_trap_fixture_versioned();
        let index = IndexSource::new(&core, version);
        for (label, want) in [
            ("Alpha", vec!["na1"]),
            ("Beta", vec!["la1"]),
            // The trap: `type`/`label`-only nodes carry the broad index's label but
            // must NOT satisfy Cypher's narrower `(var:Label)` test.
            ("Gamma", Vec::<&str>::new()),
        ] {
            let q = format!("MATCH (n:{label}) RETURN n.id AS id");
            let unindexed = exec_cypher(&view, &q).unwrap();
            let indexed =
                exec_cypher_params_indexed(&view, &q, &Params::new(), index).unwrap();
            assert_eq!(
                indexed.rows, unindexed.rows,
                "label {label}: indexed and cold-scan results diverged"
            );
            assert_eq!(
                ids(&indexed, 0),
                want,
                "label {label}: unexpected match set (trap case must stay empty)"
            );
        }
    }

    /// (c) The warm path is actually taken for the matching cases — a counter, not a
    /// stopwatch (GOC-70): `walk_metrics::warm_label_hits()` only increments inside
    /// `indexed_label_candidates` once both OCC version brackets hold, so seeing it
    /// increase proves `resolve_match` really consulted `GraphCore.label_index`
    /// rather than silently falling back to the cold `label_candidates` scan.
    #[test]
    #[cfg(feature = "result-cache")]
    fn warm_label_prefilter_is_actually_consulted() {
        let (core, view, version) = label_trap_fixture_versioned();
        let index = IndexSource::new(&core, version);
        walk_metrics::reset();
        assert_eq!(walk_metrics::warm_label_hits(), 0);
        let result = exec_cypher_params_indexed(
            &view,
            "MATCH (n:Alpha) RETURN n.id AS id",
            &Params::new(),
            index,
        )
        .unwrap();
        assert_eq!(ids(&result, 0), vec!["na1"]);
        assert!(
            walk_metrics::warm_label_hits() >= 1,
            "expected the warm GraphCore.label_index prefilter to be consulted at least \
             once, got {} hits — resolve_match must have fallen back to the cold scan",
            walk_metrics::warm_label_hits()
        );

        // The unindexed path must NEVER touch the warm prefilter — it has no
        // `IndexSource` to consult at all.
        walk_metrics::reset();
        let _ = exec_cypher(&view, "MATCH (n:Alpha) RETURN n.id AS id").unwrap();
        assert_eq!(
            walk_metrics::warm_label_hits(),
            0,
            "the unindexed path must never record a warm-label hit"
        );
    }

    /// (b) A label the warm index cannot vouch for falls back correctly, in the two
    /// distinct ways that can happen:
    ///   * a GENUINELY absent label — the persistent index legitimately answers
    ///     "zero candidates" (`Some(vec![])`, still a warm HIT, not a fallback) —
    ///     this is the exact `count(:__NoSuchLabel__)` shape this lane's fix targets.
    ///   * an OCC version race — the `IndexSource` is stamped with a version that no
    ///     longer matches the live `GraphCore.version()`, so the prefilter must
    ///     decline (`None`) rather than trust a possibly-stale answer, and
    ///     `resolve_match` must fall back to the cold `label_candidates` scan and
    ///     still return the correct (non-empty) result.
    #[test]
    #[cfg(feature = "result-cache")]
    fn warm_label_prefilter_absent_label_and_version_race_fall_back_correctly() {
        let (core, view, version) = label_trap_fixture_versioned();

        // Genuinely absent label: warm index answers Some(vec![]) directly.
        walk_metrics::reset();
        assert_eq!(
            indexed_label_candidates(IndexSource::new(&core, version), "NoSuchLabel"),
            Some(Vec::new())
        );
        assert_eq!(
            walk_metrics::warm_label_hits(),
            1,
            "an absent label is still a warm HIT (the index legitimately says zero), \
             not a decline"
        );
        let empty = exec_cypher_params_indexed(
            &view,
            "MATCH (n:NoSuchLabel) RETURN n.id AS id",
            &Params::new(),
            IndexSource::new(&core, version),
        )
        .unwrap();
        assert!(empty.rows.is_empty());

        // Version race: a stale `IndexSource` must decline outright.
        walk_metrics::reset();
        assert_eq!(
            indexed_label_candidates(IndexSource::new(&core, version.wrapping_add(1)), "Alpha"),
            None,
            "a version mismatch must decline, never answer stale"
        );
        assert_eq!(
            walk_metrics::warm_label_hits(),
            0,
            "a declined (version-race) lookup must not count as a warm hit"
        );
        // The end-to-end query must still be correct via the cold fallback.
        let stale = exec_cypher_params_indexed(
            &view,
            "MATCH (n:Alpha) RETURN n.id AS id",
            &Params::new(),
            IndexSource::new(&core, version.wrapping_add(1)),
        )
        .unwrap();
        assert_eq!(ids(&stale, 0), vec!["na1"]);
    }

    /// The exact production shape from the slow-query log: an unlabeled `MATCH (n)`
    /// with a WHERE that ANDs a tenant disjunction (never indexable — stays a
    /// post-narrowing filter) with a plain `n.id = <literal>` equality (indexable).
    /// The indexed path (`exec_cypher_params_indexed`) must return byte-for-byte the
    /// SAME rows as the unindexed legacy path (`exec_cypher`).
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_id_equality_matches_full_scan_production_shape() {
        let (core, view, version) = fixture_versioned();
        let q =
            "MATCH (n) WHERE (n.tenant_id = 'homelab' OR n.tenant_id IS NULL OR n.tenant_id = '') \
                  AND (n.id = 'bob') RETURN n.id AS id, n.name AS name";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.columns, unindexed.columns);
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["bob"]);
    }

    /// The second production shape: `n.id IN [literal]`.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_id_in_list_matches_full_scan() {
        let (core, view, version) = fixture_versioned();
        let q =
            "MATCH (n) WHERE n.id IN ['bob', 'carol'] RETURN n.id AS id, n.tenant_id AS tenant_id";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["bob", "carol"]);
    }

    /// `n.id = <literal not present>`: the index legitimately resolves to ZERO
    /// matches (`Some(vec![])`), not "not indexable" (`None`). The indexed path must
    /// still return the identical (empty) rows the full scan does — this is the
    /// end-to-end companion to `indexed_lookup_returns_some_empty_vec_not_none`
    /// below, which pins the exact `Option` shape a conflation bug would flip.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_id_equality_no_match_is_empty_not_whole_graph() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n) WHERE n.id = 'nobody' RETURN n.id AS id";
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert!(
            indexed.rows.is_empty(),
            "expected zero rows, got {} — a None/Some(vec![]) conflation would fall back to \
             the full scan and return every node instead",
            indexed.rows.len()
        );
    }

    /// Inline node-property equality (`(n {id: 'bob'})`) is ALSO narrowed through the
    /// index, not just a WHERE conjunct.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_inline_prop_equality_matches_full_scan() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n {id: 'bob'}) RETURN n.id AS id";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["bob"]);
    }

    /// A WHERE that is PURELY a disjunction over the start var (no plain equality/IN
    /// conjunct at all — the tenant-only shape) offers nothing indexable:
    /// `indexed_start_candidates` must decline (`None`), not silently narrow to
    /// nothing. The end-to-end answer still has to match the full scan.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_disjunction_only_where_falls_back_and_still_matches() {
        let (core, view, version) = fixture_versioned();
        let q =
            "MATCH (n) WHERE n.tenant_id = 'homelab' OR n.tenant_id = 'other' RETURN n.id AS id";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["alice", "bob", "carol"]);

        // Directly pin the `None` this shape must produce (no plain Cond, only an Or).
        let anchor = Binding::new();
        let start_preds = vec![WhereExpr::Or(vec![
            WhereExpr::Cond(Condition {
                var: "n".to_string(),
                prop: "tenant_id".to_string(),
                test: Test::Cmp(CompareOp::Eq, Value::String("homelab".to_string())),
            }),
            WhereExpr::Cond(Condition {
                var: "n".to_string(),
                prop: "tenant_id".to_string(),
                test: Test::Cmp(CompareOp::Eq, Value::String("other".to_string())),
            }),
        ])];
        let node = NodePat {
            var: Some("n".to_string()),
            label: None,
            props: None,
        };
        assert_eq!(
            indexed_start_candidates(
                Some(IndexSource::new(&core, version)),
                &node,
                &start_preds,
                &anchor,
                &Params::new(),
            ),
            None,
            "a pure disjunction offers no plain equality/IN conjunct to index"
        );
    }

    /// THE conflation-catching test (per task spec): directly pins that an indexed,
    /// zero-match predicate returns `Some(vec![])`, never `None`. If a future edit
    /// swapped in something like `if ids.is_empty() { None } else { Some(ids) }`, this
    /// assertion fails immediately (`None != Some(vec![])`), whereas an end-to-end
    /// query-result test alone could not distinguish "no predicate was indexable" from
    /// "the index says zero rows" — both legitimately produce zero output rows via
    /// (fallback-then-filter) vs (index-then-filter). A DIFFERENT unindexable shape
    /// (`None`, from `indexed_disjunction_only_where_falls_back_and_still_matches`
    /// above) is asserted right alongside it so the two `Option` shapes are pinned
    /// side by side, not just individually plausible.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_lookup_returns_some_empty_vec_not_none() {
        // Uses `tenant_id` (a REAL stored blob field, routed through
        // `GraphCore::nodes_by_property`/`IndexManager`), not `id` (which has its own
        // always-`Some` fast path — see `indexed_where_cond`'s doc — and so can't
        // exercise a genuine "indexed, zero matches" answer at this layer).
        let (core, _view, version) = fixture_versioned();
        let anchor = Binding::new();
        let node = NodePat {
            var: Some("n".to_string()),
            label: None,
            props: None,
        };

        // Indexed, real predicate, zero matches ⇒ Some(vec![]).
        let no_match_preds = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "tenant_id".to_string(),
            test: Test::Cmp(
                CompareOp::Eq,
                Value::String("nonexistent-tenant".to_string()),
            ),
        })];
        let empty = indexed_start_candidates(
            Some(IndexSource::new(&core, version)),
            &node,
            &no_match_preds,
            &anchor,
            &Params::new(),
        );
        assert_eq!(empty, Some(Vec::<String>::new()));
        assert_ne!(
            empty, None,
            "an indexed zero-match answer is Some(vec![]), never None"
        );

        // No IndexSource at all ⇒ genuinely None (nothing to conflate it with).
        let no_index =
            indexed_start_candidates(None, &node, &no_match_preds, &anchor, &Params::new());
        assert_eq!(no_index, None);

        // A real match ⇒ Some(vec![the matching ids]).
        let match_preds = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "tenant_id".to_string(),
            test: Test::Cmp(CompareOp::Eq, Value::String("homelab".to_string())),
        })];
        let mut hit = indexed_start_candidates(
            Some(IndexSource::new(&core, version)),
            &node,
            &match_preds,
            &anchor,
            &Params::new(),
        )
        .unwrap();
        hit.sort();
        assert_eq!(hit, vec!["alice".to_string(), "bob".to_string()]);
    }

    /// `id`'s own fast path (distinct from the general property index tested above):
    /// it ALWAYS answers `Some` for a bare `Cmp(Eq, <string>)`/non-empty `In` test —
    /// including a literal that names no real node — because it hands back the
    /// literal(s) themselves for `resolve_match`'s EXISTING `view.node_map`
    /// membership + `all_where_hold` re-check to validate, exactly like every other
    /// candidate source. No `IndexSource` is consulted at all.
    #[test]
    fn indexed_id_fast_path_echoes_literals_unconditionally() {
        let node = NodePat {
            var: Some("n".to_string()),
            label: None,
            props: None,
        };
        let no_such_node = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "id".to_string(),
            test: Test::Cmp(CompareOp::Eq, Value::String("nobody".to_string())),
        })];
        assert_eq!(
            indexed_start_candidates(None, &node, &no_such_node, &Binding::new(), &Params::new()),
            Some(vec!["nobody".to_string()]),
            "the id fast path needs no IndexSource and doesn't pre-filter existence"
        );

        // A non-string `id` literal can never match a real (always-string) node id —
        // the fast path declines rather than fabricating a non-string candidate.
        let numeric_id = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "id".to_string(),
            test: Test::Cmp(CompareOp::Eq, Value::Number(7.into())),
        })];
        assert_eq!(
            indexed_start_candidates(None, &node, &numeric_id, &Binding::new(), &Params::new()),
            None
        );
    }

    /// OCC version-race guard (CONCEPT:EG-KG.txn.occ-graph-core): an `IndexSource` stamped with a
    /// version that does NOT match the live `GraphCore.version()` must be refused
    /// (`None`) even though the predicate is perfectly indexable and would match —
    /// proving the bracket actually gates on version equality rather than always
    /// trusting the live index.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_lookup_declines_on_version_mismatch() {
        // `tenant_id` again — `id`'s own fast path never touches `core.version()`,
        // so it cannot exercise the bracket this test targets.
        let (core, _view, version) = fixture_versioned();
        let anchor = Binding::new();
        let node = NodePat {
            var: Some("n".to_string()),
            label: None,
            props: None,
        };
        let preds = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "tenant_id".to_string(),
            test: Test::Cmp(CompareOp::Eq, Value::String("homelab".to_string())),
        })];
        let stale = indexed_start_candidates(
            Some(IndexSource::new(&core, version.wrapping_add(1))),
            &node,
            &preds,
            &anchor,
            &Params::new(),
        );
        assert_eq!(
            stale, None,
            "a version mismatch must decline, never answer stale"
        );

        // Sanity: the SAME query with the CORRECT version does resolve.
        let fresh = indexed_start_candidates(
            Some(IndexSource::new(&core, version)),
            &node,
            &preds,
            &anchor,
            &Params::new(),
        )
        .unwrap();
        let mut fresh = fresh;
        fresh.sort();
        assert_eq!(fresh, vec!["alice".to_string(), "bob".to_string()]);
    }

    /// A labeled start never consults the property index at all (the label index
    /// already narrows it cheaply) — `indexed_start_candidates` must decline
    /// immediately regardless of an otherwise-indexable WHERE predicate.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_lookup_declines_for_labeled_start() {
        let (core, _view, version) = fixture_versioned();
        let anchor = Binding::new();
        let node = NodePat {
            var: Some("n".to_string()),
            label: Some("Person".to_string()),
            props: None,
        };
        let preds = vec![WhereExpr::Cond(Condition {
            var: "n".to_string(),
            prop: "id".to_string(),
            test: Test::Cmp(CompareOp::Eq, Value::String("bob".to_string())),
        })];
        assert_eq!(
            indexed_start_candidates(
                Some(IndexSource::new(&core, version)),
                &node,
                &preds,
                &anchor,
                &Params::new(),
            ),
            None
        );
    }

    /// Full shadow-mode equivalence over the indexed path: `plan` engine + index vs
    /// `legacy` engine + index must still agree (the index lives in the SHARED
    /// `resolve_match`, so this is mostly a non-regression check that indexing didn't
    /// silently break the plan-engine cost reorder).
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_lookup_agrees_across_legacy_and_plan_engines() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n) WHERE n.id IN ['bob', 'carol'] RETURN n.id AS id";
        let index = Some(IndexSource::new(&core, version));
        let query = plan_cache::global().get_or_parse(q).unwrap();
        let legacy_bindings = run_stages(
            &view,
            &query.stages,
            &Params::new(),
            row_budget(&query),
            index,
        )
        .unwrap();
        let legacy = finalize(&view, &query, legacy_bindings).unwrap();
        assert_eq!(ids(&legacy, 0), vec!["bob", "carol"]);
    }

    // ── labelled-start indexed resolution (LANE D-engine) ────────────────────────

    /// A LABELLED start with an indexable inline `id` property now narrows through
    /// the same fast path an unlabeled start already used — `indexed_start_candidates`
    /// no longer bails out on `node.label.is_some()`. Directly proves the gate is
    /// gone: no `IndexSource` is even required for the `id` leg (mirrors
    /// `indexed_id_fast_path_echoes_literals_unconditionally`, but with a label).
    #[test]
    fn indexed_labeled_inline_id_no_longer_declines() {
        let node = NodePat {
            var: Some("n".to_string()),
            label: Some("Person".to_string()),
            props: Some(vec![("id".to_string(), PropVal::Lit(Value::String("bob".to_string())))]),
        };
        assert_eq!(
            indexed_start_candidates(None, &node, &[], &Binding::new(), &Params::new()),
            Some(vec!["bob".to_string()]),
            "a labelled start's inline `id` prop must resolve through the same \
             no-IndexSource-needed fast path an unlabeled start already gets"
        );
    }

    /// End-to-end companion: `MATCH (n:Person {id: 'bob'}) RETURN …` — the indexed
    /// path must return byte-for-byte the same row the full scan does.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_labeled_inline_id_matches_full_scan() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n:Person {id: 'bob'}) RETURN n.id AS id, n.name AS name";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["bob"]);
    }

    /// A labelled start with a WHERE equality on a REAL stored property (not `id`)
    /// also narrows through the general `PropertyEqIndex` seam now that the label
    /// gate is gone — `n.tenant_id = 'homelab'` picks alice+bob (both `:Person`),
    /// never carol (`tenant_id = 'other'`) even though carol also matches the
    /// predicate as an UNLABELED candidate — proving the label constraint is still
    /// enforced on an indexed labelled start, not bypassed by the narrowing.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_labeled_where_equality_matches_full_scan_and_respects_label() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n:Person) WHERE n.tenant_id = 'homelab' RETURN n.id AS id";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["alice", "bob"]);
    }

    /// The `id` fast path answers off the LITERAL alone, unconditionally of label —
    /// so a labelled MATCH whose id literal names a node of a DIFFERENT label (`bob`
    /// is `:Person`, not `:Doc`) must still resolve to an EMPTY result, exactly like
    /// the full scan: `resolve_match`'s post-filter (`node_has_label_id`) is what
    /// enforces the label, not `indexed_start_candidates` itself. This is the
    /// "narrowing by label must not become a way to bypass enforcement" invariant.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_labeled_inline_id_wrong_label_returns_empty_not_wrong_node() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n:Doc {id: 'bob'}) RETURN n.id AS id";
        let unindexed = exec_cypher(&view, q).unwrap();
        assert!(unindexed.rows.is_empty(), "sanity: bob is not a :Doc");
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert!(indexed.rows.is_empty());

        // Directly pin that `indexed_start_candidates` itself is unfiltered by label
        // — the (unlabeled-looking) candidate set includes "bob" regardless of the
        // `:Doc` label on `node`; only the caller's post-filter removes it.
        let node = NodePat {
            var: Some("n".to_string()),
            label: Some("Doc".to_string()),
            props: Some(vec![("id".to_string(), PropVal::Lit(Value::String("bob".to_string())))]),
        };
        assert_eq!(
            indexed_start_candidates(
                Some(IndexSource::new(&core, version)),
                &node,
                &[],
                &Binding::new(),
                &Params::new()
            ),
            Some(vec!["bob".to_string()]),
            "indexed_start_candidates itself does not enforce the label — the caller must"
        );
    }

    /// A labelled start whose WHERE is a pure disjunction (no plain equality/IN
    /// conjunct) still offers nothing indexable and must fall back to a full scan —
    /// same as the unlabeled case, just now reachable with a label present. The
    /// end-to-end answer must still match the full scan exactly.
    #[test]
    #[cfg(feature = "result-cache")]
    fn indexed_labeled_non_indexable_where_falls_back_and_matches() {
        let (core, view, version) = fixture_versioned();
        let q = "MATCH (n:Person) WHERE n.tenant_id = 'homelab' OR n.tenant_id = 'other' \
                  RETURN n.id AS id";
        let unindexed = exec_cypher(&view, q).unwrap();
        let indexed =
            exec_cypher_params_indexed(&view, q, &Params::new(), IndexSource::new(&core, version))
                .unwrap();
        assert_eq!(indexed.rows, unindexed.rows);
        assert_eq!(ids(&indexed, 0), vec!["alice", "bob", "carol"]);

        // Pin the `None` directly: a labelled node with only a disjunction is exactly
        // as un-indexable as the unlabeled case was.
        let node = NodePat {
            var: Some("n".to_string()),
            label: Some("Person".to_string()),
            props: None,
        };
        let preds = vec![WhereExpr::Or(vec![
            WhereExpr::Cond(Condition {
                var: "n".to_string(),
                prop: "tenant_id".to_string(),
                test: Test::Cmp(CompareOp::Eq, Value::String("homelab".to_string())),
            }),
            WhereExpr::Cond(Condition {
                var: "n".to_string(),
                prop: "tenant_id".to_string(),
                test: Test::Cmp(CompareOp::Eq, Value::String("other".to_string())),
            }),
        ])];
        assert_eq!(
            indexed_start_candidates(
                Some(IndexSource::new(&core, version)),
                &node,
                &preds,
                &Binding::new(),
                &Params::new()
            ),
            None
        );
    }

    // ── MERGE point-lookup fast path (LANE D-engine, apply_merge) ────────────────

    /// The exact production shape (`MERGE (m:Label {id: $x}) SET …`): the SECOND
    /// MERGE against an id that already exists must hit `apply_merge`'s new
    /// `get_node_properties` point-lookup fast path, bind the SAME node (not create
    /// a duplicate), and a following `SET` must land on it. Only one node may exist
    /// afterward.
    #[test]
    fn merge_fast_path_finds_existing_node_and_set_lands_on_it() {
        let core = GraphCore::new();
        exec_cypher_write(
            &core,
            "MERGE (m:IngestManifest {id: 'manifest-1'}) SET m.graph_name = 'first'",
        )
        .unwrap();
        exec_cypher_write(
            &core,
            "MERGE (m:IngestManifest {id: 'manifest-1'}) SET m.graph_name = 'second'",
        )
        .unwrap();
        let qr = exec_cypher_write(&core, "MATCH (m:IngestManifest) RETURN m.graph_name").unwrap();
        // Exactly one node — the second MERGE found (not duplicated) the first.
        assert_eq!(cells_of(&qr, 0), vec![Value::String("second".to_string())]);
        assert_eq!(qr.rows.len(), 1);
    }

    /// `merge_is_idempotent`'s companion at the unit level: the fast path must be
    /// reached (not just "some path" that happens to also be idempotent) — pin node
    /// COUNT stays 1 across 3 MERGEs of the same id, and that a DIFFERENT id creates
    /// a genuinely second node.
    #[test]
    fn merge_fast_path_does_not_duplicate_across_repeated_merges() {
        let core = GraphCore::new();
        for _ in 0..3 {
            exec_cypher_write(&core, "MERGE (n:City {id: 'paris', name: 'Paris'})").unwrap();
        }
        exec_cypher_write(&core, "MERGE (n:City {id: 'lyon', name: 'Lyon'})").unwrap();
        let qr = exec_cypher_write(&core, "MATCH (c:City) RETURN c.id").unwrap();
        assert_eq!(col0(&qr), vec!["lyon", "paris"]);
    }

    /// A label mismatch at the SAME graph-key id (an edge case only reachable via
    /// two different labels sharing one literal `id`) must behave IDENTICALLY to the
    /// pre-existing full-scan-only `apply_merge`: the fast path declines (its
    /// `label_ok` check fails) and falls through to the unchanged full scan, which
    /// also finds no label-matching candidate and creates via `realize_node` —
    /// `core.add_node` on an existing graph key REPLACES that node's blob wholesale.
    /// This test pins that documented (if surprising) contract is unchanged by the
    /// fast path, not a new divergence it introduces.
    #[test]
    fn merge_fast_path_label_mismatch_falls_back_like_full_scan() {
        let core = GraphCore::new();
        exec_cypher_write(&core, "CREATE (n:Person {id: 'x', name: 'Original'})").unwrap();
        exec_cypher_write(&core, "MERGE (m:City {id: 'x', name: 'NewCity'})").unwrap();

        // No :Person node with id 'x' remains — the merge fell through to
        // `realize_node`, which overwrote the graph-key `x` node's blob entirely.
        let persons = exec_cypher_write(&core, "MATCH (p:Person) RETURN p.id").unwrap();
        assert!(persons.rows.is_empty());
        let cities = exec_cypher_write(&core, "MATCH (c:City) RETURN c.id, c.name").unwrap();
        assert_eq!(cities.rows.len(), 1);
        assert_eq!(ids(&cities, 0), vec!["x"]);
    }

    // ── realistic-scale benchmark: labelled id-IN start (LANE perf/engine-node-id-index) ──
    //
    // The durable ACL hydration path (`agent_utilities/knowledge_graph/core/
    // secured_reads.py`) issues `MATCH (n:Label) WHERE n.id IN [...] RETURN ...` on
    // EVERY governed read, against a brand-new `GraphView` per call
    // (`GraphCore::analysis_snapshot_versioned` clones one per request, so
    // `label_index_memo` starts cold every single time). Before this lane, the
    // START-candidate `id` fast path (`indexed_start_candidates`, already landed —
    // see `49bf22b`/`8770345c`) resolved the candidate ids in O(k), but
    // `resolve_match`'s post-filter then re-verified the `:Label` constraint via
    // `node_has_label_id`, which consults (and, cold, PAYS TO BUILD) the memoized
    // WHOLE-GRAPH label index — an O(V) decode of every node's property blob,
    // silently reintroducing the exact whole-graph-scan cost the id fast path was
    // supposed to have eliminated. `node_has_label_point` (added in this lane) closes
    // that gap with a per-candidate point decode instead.

    /// The label used to round-robin `["Tool", "Concept", "Memory", "Document",
    /// "Skill", "MCPServer"]` across a synthetic large graph — kept as one array so
    /// the graph builder and the id-picking logic below can never drift apart on
    /// which nodes are `:Tool`.
    const BENCH_LABELS: [&str; 6] =
        ["Tool", "Concept", "Memory", "Document", "Skill", "MCPServer"];

    /// `n` nodes round-robined across [`BENCH_LABELS`] — large enough (≥25k) to make
    /// an O(V) whole-graph blob-decode pass measurably expensive, so the benchmarks
    /// below have something real to show. Built via `add_node_no_ledger`: this
    /// synthetic graph's ledger is never read (same sanctioned pattern
    /// `GraphReadAuthority::build_projection` uses for throwaway construction — see
    /// that function's own doc on why plain `add_node` would dominate the build time
    /// with wasted per-byte ledger hex-encoding at this scale).
    fn build_large_labelled_graph(n: usize) -> GraphCore {
        let core = GraphCore::new();
        for i in 0..n {
            let lbl = BENCH_LABELS[i % BENCH_LABELS.len()];
            core.add_node_no_ledger(
                format!("n{i}"),
                pbytes(serde_json::json!({"node_type": lbl, "seq": i as i64})),
            );
        }
        core
    }

    /// 20 ids guaranteed to carry `label` (every `BENCH_LABELS.len()`-th id starting
    /// at `label`'s own offset into [`BENCH_LABELS`]), spread across nearly the whole
    /// `0..n` id space rather than a contiguous prefix — the shape a batched ACL
    /// hydration `IN [...]` sends.
    fn scattered_ids_of_label(n: usize, label: &str) -> Vec<String> {
        let offset = BENCH_LABELS.iter().position(|&l| l == label).unwrap();
        let stride = BENCH_LABELS.len() * (n / (20 * BENCH_LABELS.len()));
        (0..20)
            .map(|i| format!("n{}", offset + i * stride))
            .collect()
    }

    /// Pins the two label-check strategies' cost on a realistic 25k-node graph:
    /// [`node_has_label_point`] (this lane's fix — a per-candidate point decode) vs
    /// [`node_has_label_id`] (the pre-existing memoized-index path, which on a COLD
    /// view pays a full O(V) blob-decode of the WHOLE graph to answer even one
    /// lookup). Each strategy runs against a FRESH, cold snapshot per iteration,
    /// mirroring one `GraphView` per production request.
    ///
    /// `#[ignore]`d by default (a wall-clock benchmark, not a correctness
    /// assertion) — run explicitly with `cargo test --release -- --ignored
    /// bench_labelled_id_in`.
    #[test]
    #[ignore = "wall-clock benchmark, not a correctness assertion — run explicitly"]
    fn bench_labelled_id_in_point_vs_index_build() {
        const N: usize = 25_000;
        const RUNS: usize = 20;
        let core = build_large_labelled_graph(N);
        let target_ids = scattered_ids_of_label(N, "Tool");

        // "After" this lane: `node_has_label_point` on a cold, freshly cloned view
        // per run — O(k) point decodes, k == target_ids.len().
        let point_elapsed = {
            let start = std::time::Instant::now();
            for _ in 0..RUNS {
                let view = core.analysis_snapshot();
                for id in &target_ids {
                    assert!(node_has_label_point(&view, id, "Tool"));
                }
            }
            start.elapsed()
        };

        // "Before" this lane: what `resolve_match`'s start-candidate filter did for
        // ANY candidate set, no matter how small — `node_has_label_id` on a cold,
        // freshly cloned view per run. Its first call per run pays a full O(V) decode
        // of every node's property blob in the graph to build `label_index_memo`.
        let index_build_elapsed = {
            let start = std::time::Instant::now();
            for _ in 0..RUNS {
                let view = core.analysis_snapshot();
                for id in &target_ids {
                    assert!(node_has_label_id(&view, id, "Tool"));
                }
            }
            start.elapsed()
        };

        eprintln!(
            "bench_labelled_id_in_point_vs_index_build: N={N} candidates={} runs={RUNS} \
             point={point_elapsed:?} index_build={index_build_elapsed:?} speedup={:.1}x",
            target_ids.len(),
            index_build_elapsed.as_secs_f64() / point_elapsed.as_secs_f64().max(1e-9)
        );

        // The algorithmic gap is 3-4 orders of magnitude (20 point decodes vs 25,000
        // whole-graph decodes per run), so a generous 5x margin is robust against
        // ordinary timing noise while still proving the fix eliminates the
        // whole-graph scan on this shape.
        assert!(
            index_build_elapsed > point_elapsed * 5,
            "expected the whole-graph label-index build to be at least 5x slower than \
             the point-check fast path on a {N}-node graph; point={point_elapsed:?} \
             index_build={index_build_elapsed:?}"
        );
    }

    /// End-to-end companion: the exact production query shape (`MATCH (n:Label)
    /// WHERE n.id IN [...] RETURN ...`) through the full `exec_cypher_params_indexed`
    /// pipeline, on a fresh per-call snapshot each iteration (mirroring one
    /// `GraphView` per request). Asserts correctness AND reports the realistic-scale
    /// wall-clock cost end to end (start-candidate resolution + label re-check +
    /// projection), not just the isolated label-check primitives above.
    ///
    /// `#[ignore]`d by default — run explicitly with `cargo test --release --features
    /// result-cache -- --ignored bench_labelled_id_in_end_to_end`.
    #[test]
    #[cfg(feature = "result-cache")]
    #[ignore = "wall-clock benchmark, not a correctness assertion — run explicitly"]
    fn bench_labelled_id_in_end_to_end() {
        const N: usize = 25_000;
        const RUNS: usize = 20;
        let core = build_large_labelled_graph(N);
        let target_ids = scattered_ids_of_label(N, "Tool");
        let ids_literal = target_ids
            .iter()
            .map(|id| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!("MATCH (n:Tool) WHERE n.id IN [{ids_literal}] RETURN n.id AS id");

        let start = std::time::Instant::now();
        let mut last_len = 0;
        for _ in 0..RUNS {
            let (view, version) = core.analysis_snapshot_versioned();
            let result = exec_cypher_params_indexed(
                &view,
                &q,
                &Params::new(),
                IndexSource::new(&core, version),
            )
            .unwrap();
            last_len = result.rows.len();
        }
        let elapsed = start.elapsed();
        assert_eq!(
            last_len,
            target_ids.len(),
            "every scattered id is a real :Tool node and should resolve"
        );
        eprintln!(
            "bench_labelled_id_in_end_to_end: N={N} candidates={} runs={RUNS} total={elapsed:?} \
             avg_per_query={:?}",
            target_ids.len(),
            elapsed / RUNS as u32
        );
    }
}
