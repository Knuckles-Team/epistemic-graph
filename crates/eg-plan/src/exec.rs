//! The fused executor (CONCEPT:KG-2.208) — sequences the EXISTING legs into ONE
//! pipeline over a single off-lock snapshot. Each operator takes the previous
//! [`RowSet`] and returns the next:
//!
//! * `Scan` — label scan over the `GraphView` property blobs (dep-free).
//! * `Filter` — **real DataFusion** via `eg_query::exec_sql` over the schema-on-read
//!   `nodes` provider. DataFusion stays the relational sub-engine; this op SEQUENCES
//!   it, it does not reimplement SQL.
//! * `Traverse` — petgraph BFS over the `GraphView` topology (variable-length hops,
//!   matching eg-query/cypher's `rel_matches`), dep-free.
//! * `Rank` — the vector kNN of the `SemanticStore` (`semantic_search`).
//! * `Limit` — order-respecting top-k.
//!
//! The intermediate is always a `RowSet` (ids + optional scores) — the cross-modal
//! currency — so the legs compose with no impedance mismatch beyond "Arrow id column
//! → ids" at the relational boundary. The whole module is gated behind `query`
//! because the FILTER leg needs DataFusion; without it, the algebra/cost/IR still
//! compile dep-free (the Pi contract).

use std::collections::HashSet;

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphView;

use crate::algebra::{Op, Plan, Pred};
use crate::rowset::RowSet;

/// Everything an operator might touch, gathered from ONE consistent snapshot. In a
/// handler this is exactly what is already available off-lock: the `GraphView`
/// (topology + property blobs) and a `SemanticStore` clone, both taken at one
/// `GraphCore::version()` — so the cross-modal read is snapshot-isolated for free
/// (CONCEPT:KG-2.180).
pub struct PlanCtx<'a> {
    pub view: &'a GraphView,
    pub semantic: &'a SemanticStore,
    /// The lexical BM25 index for the `RankText` / `FuseRrf` ops (CONCEPT:KG-2.215).
    /// `None` when no text index is configured — a `RankText` then yields no hits
    /// (the plan degrades, never errs), exactly as an absent embedding does for
    /// `Rank`. Gated behind `text`, so a non-text build's `PlanCtx` is unchanged.
    #[cfg(feature = "text")]
    pub text: Option<&'a eg_text::TextIndex>,
}

impl<'a> PlanCtx<'a> {
    /// Construct a ctx with NO text index (the common case + every non-text plan).
    /// Keeps call-sites (and existing tests) feature-agnostic: the `text` field, when
    /// the feature is on, defaults to `None`. Use [`Self::with_text`] to attach one.
    pub fn new(view: &'a GraphView, semantic: &'a SemanticStore) -> Self {
        Self {
            view,
            semantic,
            #[cfg(feature = "text")]
            text: None,
        }
    }

    /// Attach a lexical BM25 index so `RankText` / `FuseRrf` ops can run.
    #[cfg(feature = "text")]
    pub fn with_text(mut self, text: &'a eg_text::TextIndex) -> Self {
        self.text = Some(text);
        self
    }
}

/// Execute a [`Plan`] over `ctx`, returning the final `RowSet`. A free function (not
/// an inherent method) because `Plan` is the wire DTO defined in eg-types — the
/// orphan rule forbids an `impl Plan` here; behavior attaches to the foreign type
/// from this crate this way. See [`crate::PlanExt`] for the `plan.execute(&ctx)`
/// ergonomic form.
pub fn execute(plan: &Plan, ctx: &PlanCtx) -> Result<RowSet, String> {
    let mut cur = RowSet::new();
    for op in &plan.ops {
        cur = apply(op, cur, ctx)?;
    }
    Ok(cur)
}

/// Ergonomic `plan.execute(&ctx)` over the foreign [`Plan`] wire DTO (the orphan
/// rule forbids an inherent method; an extension trait is the idiomatic way to hang
/// behavior on a type from another crate).
pub trait PlanExt {
    fn execute(&self, ctx: &PlanCtx) -> Result<RowSet, String>;
}

impl PlanExt for Plan {
    fn execute(&self, ctx: &PlanCtx) -> Result<RowSet, String> {
        execute(self, ctx)
    }
}

fn apply(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::Scan { label } => Ok(scan_label(ctx.view, label)),

        Op::Filter { preds } => {
            // Predicate pushdown across the modality boundary: when the input is
            // already a candidate set (from a prior TRAVERSE/RANK), restrict the
            // relational scan to those ids (`id IN (...)`) instead of the whole graph.
            let restrict: Option<Vec<String>> = if input.is_empty() {
                None
            } else {
                Some(input.ids())
            };
            let passed = sql_filter_ids(ctx.view, preds, restrict.as_deref())?;
            // Preserve the input's order (so a vector-first plan stays ranked); if
            // there was no input (Filter is the source), the SQL order is the order.
            if input.is_empty() {
                Ok(RowSet::from_ids(passed))
            } else {
                let passed_set: HashSet<&str> = passed.iter().map(String::as_str).collect();
                Ok(input.intersect_keep_order(&passed_set))
            }
        }

        Op::Traverse { rel, min, max } => {
            let reached = bfs_reached(ctx.view, &input.ids(), rel, *min, *max);
            Ok(RowSet::from_ids(reached))
        }

        Op::Rank { query } => {
            // kNN over the FULL store, then keep only the current candidate set, in
            // similarity order. (Equivalent to "rank these candidates": the store's
            // kNN is over all embeddings, so we over-fetch then intersect.)
            let candidates = input.id_set();
            let k = candidates.len().max(1);
            let want = (k * 4).max(k + 32);
            let ranked = ctx.semantic.semantic_search(query, want);
            let scored: Vec<(String, f32)> = ranked
                .into_iter()
                .filter(|(id, _)| candidates.contains(id.as_str()))
                .collect();
            Ok(RowSet::from_scored(scored))
        }

        #[cfg(feature = "text")]
        Op::RankText { query } => Ok(rank_text(ctx, &input, query)),

        #[cfg(feature = "text")]
        Op::FuseRrf { left, right, k } => fuse_rrf(ctx, &input, left, right, *k),

        #[cfg(feature = "owl")]
        Op::Reason {
            target_class,
            ontology,
        } => Ok(reason_source(ctx.view, target_class, ontology)),

        #[cfg(feature = "owl")]
        Op::SparqlBgp { query, var } => sparql_source(ctx.view, query, var),

        Op::Limit { k } => Ok(input.limit(*k)),
    }
}

/// RANK (lexical, BM25): re-order the candidate set by BM25 relevance to `query`.
/// Symmetric to the vector `Rank` — BM25 top-k over the FULL index (over-fetch),
/// then keep only the current candidates, in BM25-score order. With no text index
/// configured the result is empty (degrade, never err), mirroring `Rank` over an
/// empty embedding store.
#[cfg(feature = "text")]
fn rank_text(ctx: &PlanCtx, input: &RowSet, query: &str) -> RowSet {
    let Some(index) = ctx.text else {
        return RowSet::new();
    };
    let candidates = input.id_set();
    let k = candidates.len().max(1);
    let want = (k * 4).max(k + 32);
    let hits = index.search(query, want);
    let scored: Vec<(String, f32)> = hits
        .into_iter()
        .filter(|h| candidates.contains(h.id.as_str()))
        .map(|h| (h.id, h.score))
        .collect();
    RowSet::from_scored(scored)
}

/// FUSE (hybrid): run the `left` and `right` SUB-PLANS over the SAME `input` seed
/// (snapshot-isolated, same `ctx`), then reciprocal-rank-fuse their ranked id lists
/// into one RowSet (CONCEPT:KG-2.215). Each branch is a normal `Vec<Op>` plan — so
/// the canonical hybrid is `left = [Rank{vec}]`, `right = [RankText{q}]`, but any two
/// ranking sub-plans compose. RRF fuses the RANKS (not the incomparable cosine/BM25
/// score scales), so a doc strong in BOTH branches out-ranks one strong in only one
/// — the property that makes the fused query beat either branch alone.
#[cfg(feature = "text")]
fn fuse_rrf(
    ctx: &PlanCtx,
    input: &RowSet,
    left: &[Op],
    right: &[Op],
    k: f32,
) -> Result<RowSet, String> {
    let run_branch = |ops: &[Op]| -> Result<Vec<String>, String> {
        let mut cur = input.clone();
        for op in ops {
            cur = apply(op, cur, ctx)?;
        }
        Ok(cur.ids())
    };
    let l = run_branch(left)?;
    let r = run_branch(right)?;
    let k = if k > 0.0 { k } else { eg_text::RRF_K };
    let fused = eg_text::rrf_fuse(&[&l, &r], k);
    Ok(RowSet::from_scored(fused))
}

/// SOURCE (OWL): the individuals the native OWL 2 reasoner INFERS to be members of
/// `target_class` (CONCEPT:KG-2.220). Parses the `ontology` Turtle (or, when empty,
/// the axioms already present in the graph view's blobs — they round-trip as RDF),
/// runs EL⁺ classification, reads the graph's asserted instance types, and projects
/// every (possibly only-inferred) member of `target_class` into a `RowSet`. These are
/// ids the property-graph stored NO explicit `target_class` type edge for, yet they
/// then flow — like any RowSet — into a downstream `Traverse`/`Rank`/`Filter`/`Limit`.
#[cfg(feature = "owl")]
fn reason_source(view: &GraphView, target_class: &str, ontology: &str) -> RowSet {
    use eg_rdf::owl::{asserted_types_from_view, instances_of, Reasoner};

    // Axioms: an explicit ontology document, else the triples already in the graph.
    let triples = if ontology.trim().is_empty() {
        eg_rdf::owl::tbox_triples_from_view(view)
    } else {
        eg_rdf::mapping::parse_turtle(ontology).unwrap_or_default()
    };
    let mut reasoner = Reasoner::from_triples(&triples);
    let cls = reasoner.classify();

    // Asserted instance→class assignments from the live graph's folded `type`.
    let asserted = asserted_types_from_view(view);
    let target = normalize_class(target_class);
    let members = instances_of(&cls, &asserted, &target);
    RowSet::from_ids(members)
}

/// SOURCE (SPARQL): the node bindings of `var` in the SPARQL `query` over the view
/// (CONCEPT:KG-2.220) — a SPARQL-selected candidate set as a RowSet. Only resource
/// (node) bindings become ids; literal bindings are skipped (an id set is node ids).
#[cfg(feature = "owl")]
fn sparql_source(view: &GraphView, query: &str, var: &str) -> Result<RowSet, String> {
    let res = eg_rdf::sparql::run(view, query)?;
    let ids = res.solutions.iter().filter_map(|sol| {
        sol.get(var).and_then(|b| match b {
            eg_rdf::sparql::Binding::Node(n) => Some(n.clone()),
            eg_rdf::sparql::Binding::Literal(_) => None,
        })
    });
    Ok(RowSet::from_ids(ids))
}

/// Canonicalize a class id to the ontology's `<iri>` form (accept a bare IRI too).
#[cfg(feature = "owl")]
fn normalize_class(c: &str) -> String {
    if c.starts_with('<') {
        c.to_string()
    } else if c.starts_with("http") {
        format!("<{c}>")
    } else {
        c.to_string()
    }
}

/// SOURCE: all node ids whose `type` property equals `label`.
fn scan_label(view: &GraphView, label: &str) -> RowSet {
    let ids = view.node_properties.iter().filter_map(|(id, blob)| {
        let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
        (v.get("type").and_then(|t| t.as_str()) == Some(label)).then(|| id.clone())
    });
    RowSet::from_ids(ids)
}

// ── the relational FILTER leg — real DataFusion via eg-query ────────────────────

/// Compile `preds` to a SQL `WHERE` fragment. Numeric literals are emitted bare;
/// string equality is single-quote-escaped. (The planner could equally hand
/// DataFusion a pre-built `LogicalPlan`; a string keeps the leg legible and reuses
/// `eg_query::exec_sql` verbatim — DataFusion as the relational sub-engine.)
fn where_clause(preds: &[Pred]) -> String {
    if preds.is_empty() {
        return "1=1".into();
    }
    preds
        .iter()
        .map(|p| match p {
            Pred::Eq { prop, value } => {
                format!("{prop} = '{}'", value.replace('\'', "''"))
            }
            Pred::GtNum { prop, n } => format!("{prop} > {n}"),
            Pred::LtNum { prop, n } => format!("{prop} < {n}"),
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Run the FILTER leg through real DataFusion (`eg_query::exec_sql`) over the
/// schema-on-read `nodes` provider, returning the matching node ids in scan order.
///
/// `restrict_to`: when `Some`, an `id IN (...)` is appended — the
/// predicate-pushdown-across-the-modality-boundary primitive used when a prior
/// TRAVERSE/RANK already narrowed the candidate set (filter only those, not the
/// whole graph).
fn sql_filter_ids(
    view: &GraphView,
    preds: &[Pred],
    restrict_to: Option<&[String]>,
) -> Result<Vec<String>, String> {
    let mut sql = format!("SELECT id FROM nodes WHERE {}", where_clause(preds));
    if let Some(ids) = restrict_to {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let in_list = ids
            .iter()
            .map(|i| format!("'{}'", i.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" AND id IN ({in_list})"));
    }

    // `exec_sql` builds its own current-thread runtime to drive DataFusion's async
    // collect (safe to call inside spawn_blocking, exactly as the Sql handler does).
    let result = eg_query::exec_sql(view, &sql)?;
    // `result.rows[i]` is a MessagePack-encoded `Vec<serde_json::Value>` aligned to
    // `result.columns` (here a single `id` column). Decode the id cell of each row.
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let cells: Vec<serde_json::Value> =
            rmp_serde::from_slice(row).map_err(|e| format!("decode filter row: {e}"))?;
        match cells.first().and_then(|v| v.as_str()) {
            Some(id) => out.push(id.to_string()),
            None => return Err("filter result row had no string id cell".into()),
        }
    }
    Ok(out)
}

// ── the graph TRAVERSE leg — petgraph BFS over the GraphView topology ───────────

/// BFS over the petgraph topology following OUTGOING edges of relationship `rel`,
/// for `min..=max` hops. Returns the reached node ids — matching eg-query/cypher's
/// variable-length-hop traversal.
///
/// IMPORTANT (a real data-model detail): the petgraph edge *weight* is the synthetic
/// `"{src}:{tgt}"` string (`GraphCore::add_edge`), NOT the relationship type — the
/// relationship lives in the edge's property blob (`relationship`/`type` field),
/// exactly as eg-query/cypher's `rel_matches` reads it. So the BFS matches on the
/// blob, not the weight.
pub(crate) fn bfs_reached(
    view: &GraphView,
    seeds: &[String],
    rel: &str,
    min: usize,
    max: usize,
) -> Vec<String> {
    use petgraph::visit::EdgeRef;
    let mut reached: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    let mut frontier: Vec<petgraph::stable_graph::NodeIndex> = seeds
        .iter()
        .filter_map(|s| view.node_map.get(s).copied())
        .collect();
    let mut visited: HashSet<petgraph::stable_graph::NodeIndex> =
        frontier.iter().copied().collect();

    let mut depth = 0;
    while depth < max && !frontier.is_empty() {
        depth += 1;
        let mut next = Vec::new();
        for &node in &frontier {
            for e in view
                .graph
                .edges_directed(node, petgraph::Direction::Outgoing)
            {
                let from_id = &view.graph[e.source()];
                let to_id = &view.graph[e.target()];
                if !rel_matches(view, from_id, to_id, rel) {
                    continue;
                }
                let nbr = e.target();
                if visited.insert(nbr) {
                    next.push(nbr);
                }
                if depth >= min {
                    let id = view.graph[nbr].clone();
                    if reached.insert(id.clone()) {
                        out.push(id);
                    }
                }
            }
        }
        frontier = next;
    }
    out
}

/// Does the stored edge `(from→to)` carry relationship `rel`? Reads the edge's
/// property blobs (`relationship` or `type` field) — mirrors eg-query/cypher.
fn rel_matches(view: &GraphView, from: &str, to: &str, rel: &str) -> bool {
    let Some(blobs) = view
        .edge_properties
        .get(&(from.to_string(), to.to_string()))
    else {
        return false;
    };
    blobs.iter().any(|b| {
        rmp_serde::from_slice::<serde_json::Value>(b.as_slice())
            .ok()
            .map(|v| {
                let r = v.get("relationship").or_else(|| v.get("type"));
                r.and_then(|x| x.as_str()) == Some(rel)
            })
            .unwrap_or(false)
    })
}
