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
use eg_types::wire::TimeAxis;

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
    /// The WASM UDF registry for the `Udf { id }` op (CONCEPT:KG-2.228). `None` when no
    /// registry is attached — a `Udf` op then errs (a UDF must be registered to run).
    /// Gated behind `wasm-udf`, so a non-wasm build's `PlanCtx` is unchanged.
    #[cfg(feature = "wasm-udf")]
    pub udf: Option<&'a eg_wasm::UdfRegistry>,
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
            #[cfg(feature = "wasm-udf")]
            udf: None,
        }
    }

    /// Attach a lexical BM25 index so `RankText` / `FuseRrf` ops can run.
    #[cfg(feature = "text")]
    pub fn with_text(mut self, text: &'a eg_text::TextIndex) -> Self {
        self.text = Some(text);
        self
    }

    /// Attach a WASM UDF registry so a `Udf { id }` op can run a sandboxed function.
    #[cfg(feature = "wasm-udf")]
    pub fn with_udf(mut self, udf: &'a eg_wasm::UdfRegistry) -> Self {
        self.udf = Some(udf);
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

pub(crate) fn apply(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
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

        Op::RankNodeDistance { center } => Ok(rank_node_distance(ctx.view, input, center)),

        Op::RankMentions {} => Ok(rank_mentions(ctx.view, input)),

        Op::RankMmr { lambda, k } => Ok(rank_mmr(ctx.semantic, input, *lambda, *k)),

        #[cfg(feature = "text")]
        Op::RankText { query } => Ok(rank_text(ctx, &input, query)),

        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => fuse_rrf(ctx, &input, branches, *k),

        #[cfg(feature = "owl")]
        Op::Reason {
            target_class,
            ontology,
        } => Ok(reason_source(ctx.view, target_class, ontology)),

        #[cfg(feature = "owl")]
        Op::SparqlBgp { query, var } => sparql_source(ctx.view, query, var),

        #[cfg(feature = "wasm-udf")]
        Op::Udf { id } => udf_transform(ctx, &input, id),

        #[cfg(feature = "federation")]
        Op::ForeignScan { source, join } => foreign_scan(input, source, *join),

        // TIME — `AS OF [TX] @<ts>` is a real RowSet-narrowing temporal filter
        // (CONCEPT:KG-2.250): drop rows whose fact is not live at `ts` on the chosen
        // timeline. Dep-free blob scan (no DataFusion), so it runs in the Pi tier.
        Op::AsOf { ts, axis } => Ok(as_of_filter(ctx.view, input, *ts, *axis)),

        // FEDERATION / WINDOW context ops (CONCEPT:KG-2.235). RowSet-preserving markers
        // the UQL `WINDOW <dur>` / `FOREIGN "<name>"` clauses lower to. The windowed
        // aggregate (eg-tsdb) and the cross-source pull (federation) are downstream
        // seams; today they pass the rows through so a composed plan that carries them
        // runs unchanged. (`FOREIGN "<name>"` is the name MARKER; the resolved
        // federation EXECUTOR is `ForeignScan` above.)
        Op::Window { .. } | Op::Foreign { .. } => Ok(input),

        Op::Limit { k } => Ok(input.limit(*k)),
    }
}

/// SOURCE (federation): read rows from an EXTERNAL source — a remote epistemic-graph
/// engine or a generic HTTP/JSON API (CONCEPT:KG-2.232) — into the RowSet. When `join`
/// is false the foreign rows REPLACE the input (a pure source, like `Scan`). When
/// `join` is true the foreign rows are intersected with the current candidate set keyed
/// on id (a foreign∩local JOIN), preserving the input's order — so a plan can seed
/// locally, then narrow to ids a foreign source also returns. The `fetch()` runs
/// synchronously on the blocking pool, exactly like the SQL/vector legs.
#[cfg(feature = "federation")]
fn foreign_scan(
    input: RowSet,
    source: &eg_types::wire::ForeignSourceSpec,
    join: bool,
) -> Result<RowSet, String> {
    let foreign = crate::federation::source_for(source).fetch()?;
    Ok(fuse_foreign(input, foreign, join))
}

/// Fuse a foreign RowSet with the local candidate set the SAME way for EVERY foreign
/// kind (remote-engine / HTTP-JSON / external-SQL): `join=false` ⇒ the foreign rows
/// REPLACE the input (a pure source); `join=true` ⇒ intersect keyed on id, preserving
/// the input's order (a foreign∩local JOIN). Factored out so a compose-join proof can
/// exercise the EXACT fuse the executor runs against a mock-fetched foreign RowSet,
/// without standing up a live external DB.
#[cfg(feature = "federation")]
pub(crate) fn fuse_foreign(input: RowSet, foreign: RowSet, join: bool) -> RowSet {
    if join && !input.is_empty() {
        let keep = foreign.id_set();
        input.intersect_keep_order(&keep)
    } else {
        foreign
    }
}

/// UDF (WASM): transform the current `RowSet` through a registered, SANDBOXED wasm
/// function (CONCEPT:KG-2.228). Serializes the input rows `[(id, score?), …]` to
/// MessagePack, runs the UDF `id` under fuel + memory limits with NO host caps, and
/// deserializes the SAME-shape output rows back into the pipeline. A registry-less ctx
/// or an unknown UDF id errs (a UDF must be registered to run). The bytes contract is
/// the engine's — a UDF reads `Vec<(String, Option<f32>)>` and returns the same.
#[cfg(feature = "wasm-udf")]
fn udf_transform(ctx: &PlanCtx, input: &RowSet, id: &str) -> Result<RowSet, String> {
    let Some(registry) = ctx.udf else {
        return Err("Udf op requires a UDF registry on the PlanCtx (none attached)".into());
    };
    let rows: Vec<(String, Option<f32>)> = input
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect();
    let payload = rmp_serde::to_vec_named(&rows).map_err(|e| format!("udf input encode: {e}"))?;
    let out = registry.run(id, &payload).map_err(|e| e.to_string())?;
    let out_rows: Vec<(String, Option<f32>)> =
        rmp_serde::from_slice(&out).map_err(|e| format!("udf output decode: {e}"))?;
    Ok(RowSet::from_rows(out_rows))
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

/// FUSE (hybrid): run each of the N SUB-PLAN `branches` over the SAME `input` seed
/// (snapshot-isolated, same `ctx`), then reciprocal-rank-fuse their ranked id lists
/// into one RowSet (CONCEPT:KG-2.215 / KG-2.253). Each branch is a normal `Vec<Op>`
/// plan, so the canonical tri-modal hybrid is `[[Rank{vec}], [RankText{q}],
/// [RankNodeDistance{c}]]`, but any number of ranking sub-plans compose. RRF fuses the
/// RANKS (not the incomparable cosine/BM25/distance scales), so a doc strong across
/// MORE branches out-ranks one strong in only one — the property that makes the fused
/// query beat any single branch alone.
#[cfg(feature = "text")]
fn fuse_rrf(ctx: &PlanCtx, input: &RowSet, branches: &[Vec<Op>], k: f32) -> Result<RowSet, String> {
    let mut ranked: Vec<Vec<String>> = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut cur = input.clone();
        for op in branch {
            cur = apply(op, cur, ctx)?;
        }
        ranked.push(cur.ids());
    }
    let k = if k > 0.0 { k } else { eg_text::RRF_K };
    let refs: Vec<&[String]> = ranked.iter().map(|v| v.as_slice()).collect();
    let fused = eg_text::rrf_fuse(&refs, k);
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
    use eg_rdf::owl::{asserted_types_with_confidence_from_view, instances_of_weighted, Reasoner};

    // Axioms: an explicit ontology document, else the triples already in the graph.
    let triples = if ontology.trim().is_empty() {
        eg_rdf::owl::tbox_triples_from_view(view)
    } else {
        eg_rdf::mapping::parse_turtle(ontology).unwrap_or_default()
    };
    let mut reasoner = Reasoner::from_triples(&triples);
    // Confidence-weighted (CONCEPT:KG-2.236): each inferred member carries its
    // membership confidence as the RowSet SCORE, so a bare `Reason` plan is already
    // ranked by confidence and composes with a downstream vector `Rank`/`Limit`. The
    // closure is identical to the unweighted one for a HARD ontology (every score 1.0).
    let cls = reasoner.classify_weighted();

    // Asserted instance→class assignments + their per-fact confidence. `now = 0` keeps
    // the time-decay NEUTRAL inside the structural plan op (the time-aware decay is the
    // server `OwlReason` surface, which threads the real wall-clock `now`); the AXIOM
    // confidence still flows through into the score.
    let asserted = asserted_types_with_confidence_from_view(view, 0, 0.0);
    let target = normalize_class(target_class);
    let scored: Vec<(String, f32)> = instances_of_weighted(&cls, &asserted, &target, 0.0)
        .into_iter()
        .map(|(id, conf)| (id, conf as f32))
        .collect();
    RowSet::from_scored(scored)
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

// ── the temporal AS OF leg — bi-temporal point-in-time filter (KG-2.250) ────────

/// True when the fact in `blob` is live at instant `ts` on the chosen timeline.
/// Half-open window `[from, until)` in epoch seconds: a missing `from` means
/// "has always been" (0); a missing `until` means "still current" (open). Decodes
/// the SAME blob `scan_label` reads — dep-free, so the time path runs in the Pi tier.
fn live_at(blob: &[u8], ts: u64, from_key: &str, until_key: &str) -> bool {
    let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) else {
        return false;
    };
    let from = v.get(from_key).and_then(|x| x.as_u64()).unwrap_or(0);
    let until = v.get(until_key).and_then(|x| x.as_u64());
    from <= ts && until.is_none_or(|u| ts < u)
}

/// Narrow `input` to rows live at `ts` on the `axis` timeline (CONCEPT:KG-2.250).
/// Order-preserving (a vector-`Rank`-then-`AS OF` plan stays ranked, via
/// [`RowSet::intersect_keep_order`]). When `input` is empty the op acts as a source,
/// yielding every node live at `ts`.
fn as_of_filter(view: &GraphView, input: RowSet, ts: f64, axis: TimeAxis) -> RowSet {
    let ts = ts.max(0.0) as u64;
    let (from_key, until_key) = match axis {
        TimeAxis::Valid => ("valid_from", "valid_until"),
        TimeAxis::Transaction => ("tx_from", "tx_to"),
    };
    if input.is_empty() {
        let ids = view
            .node_properties
            .iter()
            .filter(|(_, blob)| live_at(blob.as_slice(), ts, from_key, until_key))
            .map(|(id, _)| id.clone());
        return RowSet::from_ids(ids);
    }
    let kept: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            view.node_properties
                .get(r.id.as_str())
                .is_some_and(|b| live_at(b.as_slice(), ts, from_key, until_key))
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&kept)
}

// ── graph-native rerankers (CONCEPT:KG-2.254) ───────────────────────────────────

/// RANK by inverse shortest-path hop distance from `center` over the topology
/// (Graphiti's `node_distance`). Score `1/(1+hops)`; an unreachable or absent-center
/// candidate scores 0 but is kept (degrade, never err — mirrors `Rank` over an empty
/// store). Unweighted BFS from `center` following OUTGOING edges (the same topology
/// the `Traverse` leg walks). Order follows score desc, ties by id for determinism.
fn rank_node_distance(view: &GraphView, input: RowSet, center: &str) -> RowSet {
    use petgraph::visit::EdgeRef;
    let candidates = input.id_set();
    if candidates.is_empty() {
        return input;
    }
    // BFS hop distances from center over the whole topology.
    let mut dist: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    if let Some(&start) = view.node_map.get(center) {
        let mut visited: HashSet<petgraph::stable_graph::NodeIndex> = HashSet::new();
        visited.insert(start);
        dist.insert(center.to_string(), 0);
        let mut frontier = vec![start];
        let mut depth = 0usize;
        while !frontier.is_empty() {
            depth += 1;
            let mut next = Vec::new();
            for &node in &frontier {
                for e in view
                    .graph
                    .edges_directed(node, petgraph::Direction::Outgoing)
                {
                    let nbr = e.target();
                    if visited.insert(nbr) {
                        dist.entry(view.graph[nbr].clone()).or_insert(depth);
                        next.push(nbr);
                    }
                }
            }
            frontier = next;
        }
    }
    let scored: Vec<(String, f32)> = input
        .rows()
        .iter()
        .map(|r| {
            let s = match dist.get(&r.id) {
                Some(&h) => 1.0 / (1.0 + h as f32),
                None => 0.0,
            };
            (r.id.clone(), s)
        })
        .collect();
    RowSet::from_scored(sort_by_score_desc(scored))
}

/// RANK by provenance salience: how many edges point AT each candidate (incoming-edge
/// count), normalized to the max in the set (Graphiti's `episode_mentions`). Pure
/// topology, dep-free. Ties broken by id for determinism.
fn rank_mentions(view: &GraphView, input: RowSet) -> RowSet {
    let counts: Vec<(String, f32)> = input
        .rows()
        .iter()
        .map(|r| {
            let c = view
                .node_map
                .get(&r.id)
                .map(|&idx| {
                    view.graph
                        .edges_directed(idx, petgraph::Direction::Incoming)
                        .count()
                })
                .unwrap_or(0);
            (r.id.clone(), c as f32)
        })
        .collect();
    let max = counts.iter().map(|(_, c)| *c).fold(0.0f32, f32::max);
    let scored: Vec<(String, f32)> = counts
        .into_iter()
        .map(|(id, c)| (id, if max > 0.0 { c / max } else { 0.0 }))
        .collect();
    RowSet::from_scored(sort_by_score_desc(scored))
}

/// RANK by Maximal Marginal Relevance (CONCEPT:KG-2.255): greedily re-order the
/// candidates trading off relevance against diversity. Relevance is each row's
/// incoming score (from a prior `Rank`; defaults to a uniform rank-decay when scores
/// are absent). Similarity is cosine over the candidates' stored embeddings. Picks the
/// item maximizing `lambda*rel - (1-lambda)*max_sim_to_selected` each step. `k` caps
/// the output (0 ⇒ all). Candidates with no embedding still participate (sim treated as
/// 0, so they are neither boosted nor penalized for diversity). Degrades to the input
/// order when there are no embeddings at all (never errs).
fn rank_mmr(semantic: &SemanticStore, input: RowSet, lambda: f32, k: usize) -> RowSet {
    let rows = input.rows();
    let n = rows.len();
    if n <= 1 {
        return input;
    }
    let lambda = lambda.clamp(0.0, 1.0);
    // Relevance per row: use the carried score; if a row lacks one, fall back to a
    // position-based decay so earlier (already-ranked) rows are more relevant.
    let rel: Vec<f32> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| r.score.unwrap_or(1.0 / (1.0 + i as f32)))
        .collect();
    let embs: Vec<Option<Vec<f32>>> = rows.iter().map(|r| semantic.get_embedding(&r.id)).collect();
    let limit = if k == 0 { n } else { k.min(n) };

    let mut selected: Vec<usize> = Vec::with_capacity(limit);
    let mut remaining: Vec<usize> = (0..n).collect();
    while selected.len() < limit && !remaining.is_empty() {
        // pick the remaining index with the best MMR score
        let mut best_pos = 0usize;
        let mut best_val = f32::NEG_INFINITY;
        for (pos, &idx) in remaining.iter().enumerate() {
            let max_sim = selected
                .iter()
                .filter_map(|&s| match (&embs[idx], &embs[s]) {
                    (Some(a), Some(b)) => Some(cosine(a, b)),
                    _ => None,
                })
                .fold(0.0f32, f32::max);
            let mmr = lambda * rel[idx] - (1.0 - lambda) * max_sim;
            // tie-break deterministically by id
            if mmr > best_val || (mmr == best_val && rows[idx].id < rows[remaining[best_pos]].id) {
                best_val = mmr;
                best_pos = pos;
            }
        }
        selected.push(remaining.remove(best_pos));
    }
    let out: Vec<(String, f32)> = selected
        .iter()
        .enumerate()
        .map(|(rank, &idx)| (rows[idx].id.clone(), 1.0 / (1.0 + rank as f32)))
        .collect();
    RowSet::from_scored(out)
}

/// Cosine similarity over two equal-length vectors (0 on degenerate input).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|y| y * y).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// Stable score-desc sort (ties by id) so a reranker's output is deterministic.
fn sort_by_score_desc(mut scored: Vec<(String, f32)>) -> Vec<(String, f32)> {
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored
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
