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
    /// The federation foreign-source registry backing `Op::Foreign` (the UQL
    /// `FOREIGN "<name>"` marker) and a `Named` `Op::ForeignScan` (CONCEPT:EG-073).
    /// `None` when no foreign sources are registered — the name-resolving ops then keep
    /// their prior behavior (`Op::Foreign` passes its input through unchanged), so a
    /// default ctx is byte-for-byte the old one. Gated behind `federation`, so a
    /// non-federation build's `PlanCtx` is unchanged.
    #[cfg(feature = "federation")]
    pub foreign: Option<&'a crate::federation::ForeignSourceRegistry>,
    /// CONCEPT:EG-304 — the content-addressed [`eg_tensor::TensorStore`] into which the
    /// tensor executor WRITES BACK every derived tensor an `Op::TensorOp` produces, so a
    /// derived tensor becomes a durable, dedup-shared CAS blob addressable by its
    /// deterministic content hash — the EG-085 CAS-write-back follow-up that v1 left
    /// documented-but-unbuilt. `None` (the default) preserves today's validate-only
    /// behavior byte-for-byte: no write-back, fully backward-compatible. Held behind a
    /// [`std::sync::Mutex`] so the write-back is interior-mutable through the shared
    /// `&PlanCtx` the executor threads, while `PlanCtx` itself stays `Send + Sync`. An
    /// identical derived tensor content-addresses to the SAME blob, so re-running a plan
    /// (or many rows yielding the same result) DEDUPS to one blob. Gated behind `tensor`,
    /// so a non-tensor build's `PlanCtx` is unchanged.
    #[cfg(feature = "tensor")]
    pub tensor_store: Option<&'a std::sync::Mutex<eg_tensor::TensorStore>>,
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
            #[cfg(feature = "federation")]
            foreign: None,
            #[cfg(feature = "tensor")]
            tensor_store: None,
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

    /// Attach a foreign-source registry so `Op::Foreign` / a `Named` `Op::ForeignScan`
    /// resolve their name → rows through it (CONCEPT:EG-073). A server/facade builds the
    /// registry once at setup and threads it in here — the library seam that keeps the
    /// name→source binding out of the wire DTO.
    #[cfg(feature = "federation")]
    pub fn with_foreign(mut self, registry: &'a crate::federation::ForeignSourceRegistry) -> Self {
        self.foreign = Some(registry);
        self
    }

    /// Attach a content-addressed [`eg_tensor::TensorStore`] so the tensor executor
    /// WRITES BACK each derived tensor an `Op::TensorOp` produces into the CAS
    /// (CONCEPT:EG-304). A server/facade owns the store (behind a [`std::sync::Mutex`])
    /// and can later `persist` it to disk for durability; without this call the executor
    /// keeps its prior validate-only behavior (no write-back), so a default ctx is
    /// byte-for-byte the old one.
    #[cfg(feature = "tensor")]
    pub fn with_tensor_store(
        mut self,
        store: &'a std::sync::Mutex<eg_tensor::TensorStore>,
    ) -> Self {
        self.tensor_store = Some(store);
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

        Op::Filter { preds } => filter_op(ctx, preds, input),

        Op::Traverse { rel, min, max } => {
            let reached = bfs_reached(ctx.view, &input.ids(), rel, *min, *max);
            Ok(RowSet::from_ids(reached))
        }

        Op::Rank { query } => {
            // CONCEPT:EG-070 — hybrid metadata pre-filter. Push the current candidate
            // set INTO the ANN scan as an allowlist so the returned top-k already
            // satisfies the predicate — filtering happens DURING the probe instead of
            // over-fetching `k*4` and post-filtering. Semantics are unchanged: with an
            // empty candidate set the allowlist rejects everything (a source `Rank`
            // yields no rows, exactly as the prior over-fetch-then-intersect did).
            let candidates = input.id_set();
            let k = candidates.len().max(1);
            let scored = ctx
                .semantic
                .semantic_search_filtered(query, k, |id| candidates.contains(id));
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
        Op::ForeignScan { source, join } => foreign_scan(input, source, *join, ctx),

        // TIME — `AS OF [TX] @<ts>` is a real RowSet-narrowing temporal filter
        // (CONCEPT:KG-2.250): drop rows whose fact is not live at `ts` on the chosen
        // timeline. Dep-free blob scan (no DataFusion), so it runs in the Pi tier.
        Op::AsOf { ts, axis } => Ok(as_of_filter(ctx.view, input, *ts, *axis)),

        // TIME (`WINDOW <dur>`, CONCEPT:EG-067) — a REAL windowed aggregate over the
        // input RowSet's time/value columns via eg-tsdb's Pi-path `time_bucket`,
        // replacing the pass-through seam. Gated behind `timeseries` (the eg-plan→
        // eg-tsdb edge), kept OUT of the Pi tier; without the feature the op stays the
        // RowSet-preserving marker the UQL `WINDOW <dur>` clause lowers to, so a plan
        // that carries it still runs unchanged.
        #[cfg(feature = "timeseries")]
        Op::Window { secs } => Ok(window_aggregate(ctx.view, input, *secs)),
        #[cfg(not(feature = "timeseries"))]
        Op::Window { .. } => Ok(input),

        // FEDERATION (`FOREIGN "<name>"`, CONCEPT:KG-2.235 / EG-073) — the name MARKER
        // the UQL clause lowers to. With a `ForeignSourceRegistry` attached to the ctx
        // (`federation` build) it now RESOLVES the name → rows through the registry (a
        // source op: the foreign rows replace the input); an unbound name is a clean
        // error. With NO registry attached — or a non-federation build — it passes the
        // rows through unchanged, exactly as before (existing behavior preserved).
        Op::Foreign { name } => foreign_named(name, input, ctx),

        // SOURCE (spatial, CONCEPT:EG-083) — an eg-geo packed-Hilbert-R-tree bbox scan
        // over the layer's geometries. Gated behind `geo`; the variant only exists when
        // eg-types/geo is on (pulled by eg-plan/geo), so a non-geo build has neither the
        // variant nor this arm (the ForeignScan gating precedent).
        #[cfg(feature = "geo")]
        Op::SpatialScan { layer, bbox } => Ok(spatial_scan(ctx.view, layer, *bbox)),

        // TRANSFORM (spatial CRS, CONCEPT:EG-255) — reproject each row's geometry into
        // `to_epsg`; TRANSFORM (constructive, CONCEPT:EG-259) — derive a geometry per row
        // via the eg-geo `algebra` op. Both gated behind `geo`; the variants only exist
        // when eg-types/geo is on (the SpatialScan/ForeignScan gating precedent).
        #[cfg(feature = "geo")]
        Op::Reproject { to_epsg, from_epsg } => {
            Ok(spatial_reproject(ctx.view, input, *to_epsg, *from_epsg))
        }
        #[cfg(feature = "geo")]
        Op::SpatialOp { kind } => Ok(spatial_op(ctx.view, input, kind)),

        // SOURCE (tensor, CONCEPT:EG-085) — seed the RowSet with the `layer`'s
        // tensor-bearing nodes; TRANSFORM (tensor) — apply the eg-tensor op to each
        // row's stored tensor. Gated behind `tensor`; the variants only exist when
        // eg-types/tensor is on (pulled by eg-plan/tensor), so a non-tensor build has
        // neither the variants nor these arms (the geo/ForeignScan gating precedent).
        #[cfg(feature = "tensor")]
        Op::TensorScan { layer } => Ok(tensor_scan(ctx.view, layer)),
        #[cfg(feature = "tensor")]
        Op::TensorOp { kind } => Ok(tensor_op(ctx, input, kind)),

        // TRANSFORM (probabilistic, CONCEPT:EG-086) — score each row by a closed-form
        // probabilistic query (expectation / marginal / conditional posterior / seeded
        // sample) over its stored `Distribution` value and re-order by that score
        // descending, dropping rows without a valid distribution or where the query does
        // not apply. Gated behind `probabilistic`; the variant only exists when
        // eg-types/probabilistic is on (pulled by eg-plan/probabilistic), so a build
        // without it has neither the variant nor this arm (the tensor/stream precedent).
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { query } => Ok(probabilistic_op(ctx.view, input, query)),

        // TRANSFORM (stream, CONCEPT:EG-088) — interpret the input RowSet as a
        // time-ordered event stream and run the eg-stream bounded NFA CEP engine over it,
        // keeping the rows that participate in a match. Gated behind `stream`; the variant
        // only exists when eg-types/stream is on (pulled by eg-plan/stream), so a
        // non-stream build has neither the variant nor this arm (the tensor/geo gating
        // precedent).
        #[cfg(feature = "stream")]
        Op::Cep { pattern } => Ok(cep_op(ctx.view, input, pattern)),

        // FUSE (multimodal sensor fusion, CONCEPT:EG-098) — time-align the named sensor
        // streams to ONE common clock via eg-tsdb's ASOF-backed `sensor_fuse` and emit a
        // fused row per instant (id = the aligned ts, score = the present-channel count).
        // A SOURCE op: it resolves its streams off the snapshot and REPLACES the input,
        // exactly like `TensorScan`/`SpatialScan`. Gated behind `timeseries`; the variant
        // only exists when eg-types/timeseries is on (pulled by eg-plan/timeseries), so a
        // non-timeseries build has neither the variant nor this arm (the tensor/stream
        // precedent). Reuses the eg-plan→eg-tsdb edge `Op::Window` already opened.
        #[cfg(feature = "timeseries")]
        Op::SensorFuse {
            streams,
            tolerance_ns,
        } => Ok(sensor_fuse_op(ctx.view, streams, *tolerance_ns)),

        // SOURCE (time-series, CONCEPT:EG-363) — seed the RowSet from native TSDB series.
        // LANE 0 PLACEHOLDER: the real eg-tsdb `SeriesStore` scan (`tsdb_scan_op`, plus
        // the `PlanCtx.tsdb` field) is owned by Lane C; this minimal not-yet-wired arm
        // only keeps the `timeseries` build exhaustive (no business logic). REPLACE in
        // Lane C.
        #[cfg(feature = "timeseries")]
        Op::TsScan { .. } => Err("Op::TsScan not yet implemented (Lane C)".to_string()),

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
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    // CONCEPT:EG-073 — a `Named` source resolves by name through the registry on the
    // ctx; every self-describing spec kind (remote-engine / HTTP-JSON / SQL) still
    // fetches via `source_for` exactly as before.
    let foreign = match source {
        eg_types::wire::ForeignSourceSpec::Named { name } => {
            let registry = ctx.foreign.ok_or_else(|| {
                format!(
                    "federation: Op::ForeignScan names foreign source '{name}' but no \
                     ForeignSourceRegistry is attached to the PlanCtx (CONCEPT:EG-073)"
                )
            })?;
            registry.resolve(name)?
        }
        other => crate::federation::source_for(other).fetch()?,
    };
    Ok(fuse_foreign(input, foreign, join))
}

/// FEDERATION (`FOREIGN "<name>"`, CONCEPT:EG-073) — resolve the named foreign source
/// through the registry on the ctx. With a registry attached the named source's rows
/// REPLACE the input (a source op) and an unbound name is a clean typed error; with NO
/// registry attached the op passes its input through unchanged (the prior behavior — so
/// existing plans are untouched).
#[cfg(feature = "federation")]
fn foreign_named(name: &str, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match ctx.foreign {
        Some(registry) => registry.resolve(name),
        None => Ok(input),
    }
}

/// Without `federation` there is no registry and no foreign machinery, so the
/// `FOREIGN "<name>"` marker keeps its pass-through behavior (CONCEPT:EG-073).
#[cfg(not(feature = "federation"))]
fn foreign_named(_name: &str, input: RowSet, _ctx: &PlanCtx) -> Result<RowSet, String> {
    Ok(input)
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

// ── the TIME WINDOW leg — native eg-tsdb windowed aggregate (CONCEPT:EG-067) ─────

/// TIME (`WINDOW <dur>`): a REAL windowed aggregate over the input RowSet's time and
/// value columns, replacing the pass-through seam (CONCEPT:EG-067). It reads the SAME
/// per-node columns the neighbouring ops do — the event time from each row's
/// `valid_from` property blob (exactly as `Op::AsOf`'s Valid axis; see [`live_at`]),
/// and the value from the row's carried `score` (what `Op::Rank` produces), falling
/// back to a numeric `value` property so a STANDALONE `Window` (no preceding `Rank`)
/// still aggregates. Those `(ts, value)` pairs become `eg_tsdb::Point`s, are ts-sorted
/// (RowSet order is rank/discovery order, but `time_bucket` needs a sorted series),
/// and are fed to the engine's Pi-path `time_bucket` primitive (`eg_tsdb::query` — no
/// DataFusion) with `width = secs` (same unit as `valid_from`) and a MEAN aggregate
/// (the canonical windowed agg; the wire `Op::Window { secs }` carries no agg
/// selector). Each non-empty bucket becomes one output row — `id` = the bucket's
/// aligned start, `score` = the aggregate — so a downstream `Limit`/`Rank` composes
/// over the windowed series. `secs <= 0`, or no timed-and-valued rows, yields an empty
/// RowSet (degrade, never err — mirroring `Rank` over an empty store).
#[cfg(feature = "timeseries")]
fn window_aggregate(view: &GraphView, input: RowSet, secs: f64) -> RowSet {
    use eg_tsdb::point::Point;
    use eg_tsdb::query::{time_bucket, Agg};

    let width = secs.max(0.0) as i64;
    let mut pts: Vec<Point> = input
        .rows()
        .iter()
        .filter_map(|r| {
            let blob = view.node_properties.get(r.id.as_str())?;
            let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
            let ts = v.get("valid_from").and_then(|x| x.as_i64())?;
            let value = r
                .score
                .map(|s| s as f64)
                .or_else(|| v.get("value").and_then(|x| x.as_f64()))?;
            Some(Point::single(ts, value))
        })
        .collect();
    pts.sort_by_key(|p| p.ts);
    let scored = time_bucket(&pts, width, Agg::Mean)
        .into_iter()
        .map(|b| (b.bucket_start.to_string(), b.value as f32));
    RowSet::from_scored(scored)
}

// ── the sensor-fusion leg — native eg-tsdb ASOF alignment (CONCEPT:EG-098) ───────

/// FUSE (multimodal sensor fusion): time-align N heterogeneous sensor `streams` to ONE
/// common clock and emit fused multi-channel rows (CONCEPT:EG-098). Each named stream is
/// a node LAYER (matched on the `type` property, exactly as [`scan_label`]) whose nodes
/// carry a `valid_from` event time (ns, the same column `Op::AsOf`/`Op::Window` read) and
/// EITHER a numeric `value` (a scalar reading) OR an opaque tensor-frame reference in a
/// `tensor`/`frame` string property (an EG-085 camera/LiDAR blob id, carried through
/// untouched). The resolved, ts-sorted per-stream samples feed eg-tsdb's Pi-path
/// `sensor_fuse` (`eg_tsdb::query` — no DataFusion), which reuses the ASOF backward-join to
/// carry each stream's latest sample at-or-before every reference instant within
/// `tolerance_ns` (`0` ⇒ exact-instant matches only). Each fused instant becomes one output
/// row — `id` = the aligned ts, `score` = the count of PRESENT (non-gap) channels — so a
/// downstream `Limit`/`Rank` composes over the fused series. No timed streams ⇒ an empty
/// RowSet (degrade, never err — mirroring `window_aggregate`).
#[cfg(feature = "timeseries")]
fn sensor_fuse_op(view: &GraphView, streams: &[String], tolerance_ns: u64) -> RowSet {
    use eg_tsdb::query::{sensor_fuse, Cell, Sample, SeriesRef};

    // Resolve each named stream (a node layer / `type`) into a ts-sorted sample series.
    let series: Vec<SeriesRef> = streams
        .iter()
        .map(|name| {
            let mut samples: Vec<Sample> = Vec::new();
            for blob in view.node_properties.values() {
                let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
                    continue;
                };
                if v.get("type").and_then(|t| t.as_str()) != Some(name.as_str()) {
                    continue;
                }
                let Some(ts) = v.get("valid_from").and_then(|x| x.as_i64()) else {
                    continue;
                };
                // Scalar reading, else an opaque tensor/frame blob reference.
                let cell = if let Some(val) = v.get("value").and_then(|x| x.as_f64()) {
                    Cell::Scalar(val)
                } else if let Some(r) = v
                    .get("tensor")
                    .or_else(|| v.get("frame"))
                    .and_then(|x| x.as_str())
                {
                    Cell::Blob(r.to_string())
                } else {
                    continue;
                };
                samples.push(Sample { ts, cell });
            }
            samples.sort_by_key(|s| s.ts);
            SeriesRef {
                name: name.clone(),
                samples,
            }
        })
        .collect();

    let scored = sensor_fuse(&series, Some(tolerance_ns as i64))
        .into_iter()
        .map(|row| {
            let present = row.channels.iter().filter(|c| c.is_some()).count();
            (row.ts.to_string(), present as f32)
        });
    RowSet::from_scored(scored)
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

// ── the FILTER op — relational (DataFusion) + spatial (eg-geo) preds ─────────────

/// Execute a `Filter` op. Relational preds (`Eq`/`GtNum`/`LtNum`) run through real
/// DataFusion (`sql_filter_ids`), preserving the input order + candidate-set pushdown.
/// Spatial preds (`SpatialWithin`/`SpatialDWithin`, CONCEPT:EG-083) are split OUT and
/// applied per-row by eg-geo against each row's stored geometry — DataFusion has no
/// spatial. A non-geo build has no spatial Pred variants, so every pred is relational
/// and this is byte-for-byte the original SQL path.
fn filter_op(ctx: &PlanCtx, preds: &[Pred], input: RowSet) -> Result<RowSet, String> {
    // Partition per-row preds (spatial via eg-geo, JSON via eg-core::jsonpath —
    // CONCEPT:EG-084) from relational preds lowered to SQL. DataFusion has neither
    // spatial nor JSONPath, so each is split out and applied per-row against the stored
    // blob. A non-geo build has no spatial variants; JSONPath is always present under
    // `query`, so `relational` is every remaining pred.
    let mut relational: Vec<Pred> = Vec::with_capacity(preds.len());
    #[cfg(feature = "geo")]
    let mut spatial: Vec<Pred> = Vec::new();
    let mut jsonpath: Vec<Pred> = Vec::new();
    for p in preds {
        #[cfg(feature = "geo")]
        if is_spatial_pred(p) {
            spatial.push(p.clone());
            continue;
        }
        if is_jsonpath_pred(p) {
            jsonpath.push(p.clone());
            continue;
        }
        relational.push(p.clone());
    }

    // Predicate pushdown across the modality boundary: when the input is already a
    // candidate set (from a prior TRAVERSE/RANK), restrict the relational scan to those
    // ids (`id IN (...)`) instead of the whole graph.
    let restrict: Option<Vec<String>> = if input.is_empty() {
        None
    } else {
        Some(input.ids())
    };
    let passed = sql_filter_ids(ctx.view, &relational, restrict.as_deref())?;
    // Preserve the input's order (so a vector-first plan stays ranked); if there was no
    // input (Filter is the source), the SQL order is the order.
    let mut out = if input.is_empty() {
        RowSet::from_ids(passed)
    } else {
        let passed_set: HashSet<&str> = passed.iter().map(String::as_str).collect();
        input.intersect_keep_order(&passed_set)
    };

    // Apply each spatial predicate against the stored per-row geometry (order-preserving).
    #[cfg(feature = "geo")]
    for p in &spatial {
        out = spatial_filter(ctx.view, out, p)?;
    }
    // Apply each JSONPath predicate against the stored per-row JSON (CONCEPT:EG-084),
    // order- and score-preserving — exactly the spatial leg's shape.
    for p in &jsonpath {
        out = jsonpath_filter(ctx.view, out, p)?;
    }
    Ok(out)
}

// ── the document/JSON leg — eg-core::jsonpath over the GraphView blobs (EG-084) ──

/// Is `pred` a JSONPath predicate (evaluated per-row by `eg_core::jsonpath`, NOT lowered
/// to SQL)? (CONCEPT:EG-084.)
fn is_jsonpath_pred(pred: &Pred) -> bool {
    matches!(pred, Pred::JsonPath { .. })
}

/// FILTER (document/JSON, CONCEPT:EG-084): keep rows whose stored JSON satisfies the
/// JSONPath predicate `pred`. Each row's blob is decoded to a `serde_json::Value` and the
/// path/op is evaluated by `eg_core::jsonpath` (existence / `->>`-equality / `@>`
/// containment). Rows with no/undecodable blob are dropped. Order- and score-preserving
/// via [`RowSet::intersect_keep_order`] — the same shape as `spatial_filter`.
fn jsonpath_filter(view: &GraphView, input: RowSet, pred: &Pred) -> Result<RowSet, String> {
    use eg_types::wire::JsonPathOp;
    let Pred::JsonPath { path, op } = pred else {
        // Non-JSON preds never reach here (`filter_op` splits them out).
        return Ok(input);
    };
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_json(view, &r.id).is_some_and(|v| match op {
                JsonPathOp::Exists => eg_core::jsonpath::path_exists(&v, path),
                JsonPathOp::Eq { value } => eg_core::jsonpath::path_eq(&v, path, value),
                JsonPathOp::Contains { value } => eg_core::jsonpath::path_contains(&v, path, value),
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    Ok(input.intersect_keep_order(&keep))
}

/// Decode node `id`'s stored property blob to a `serde_json::Value` (CONCEPT:EG-084) —
/// the JSON leg's counterpart to `row_geometry`.
fn row_json(view: &GraphView, id: &str) -> Option<serde_json::Value> {
    let blob = view.node_properties.get(id)?;
    rmp_serde::from_slice(blob.as_slice()).ok()
}

// ── the spatial leg — eg-geo geometry / R-tree over the GraphView blobs (EG-083) ─

/// SOURCE (spatial): every node in `layer` (matched on the `type` property, exactly as
/// [`scan_label`]) whose stored geometry's bounding box intersects `bbox`, found via
/// eg-geo's packed Hilbert R-tree (CONCEPT:EG-083). Each node's geometry is read as a
/// WKT string from a conventional `geometry` (or `geom`) property — a dep-free view
/// scan, mirroring how `scan_label`/`as_of_filter` decode the blob. The R-tree is built
/// over the layer's geometries and queried once; the matching ids seed the RowSet.
/// (A durable R-tree persisted beside the shard — per the concept row — is a follow-up;
/// v1 builds the index over the live snapshot.)
#[cfg(feature = "geo")]
fn spatial_scan(view: &GraphView, layer: &str, bbox: [f64; 4]) -> RowSet {
    let mut ids: Vec<String> = Vec::new();
    let mut boxes: Vec<(usize, eg_geo::Bbox)> = Vec::new();
    for (id, blob) in view.node_properties.iter() {
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
            continue;
        };
        if v.get("type").and_then(|t| t.as_str()) != Some(layer) {
            continue;
        }
        let Some(bb) = geometry_from_value(&v).and_then(|g| g.bbox()) else {
            continue;
        };
        let idx = ids.len();
        ids.push(id.clone());
        boxes.push((idx, bb));
    }
    let rtree = eg_geo::RTree::build(&boxes);
    let hits = rtree.query_bbox(&eg_geo::Bbox::from_array(bbox));
    RowSet::from_ids(hits.into_iter().map(|i| ids[i].clone()))
}

/// Is `pred` a spatial predicate (evaluated per-row by eg-geo, NOT lowered to SQL)?
/// Covers EG-083's within/dwithin plus the EG-258 DE-9IM relation set.
#[cfg(feature = "geo")]
fn is_spatial_pred(pred: &Pred) -> bool {
    matches!(
        pred,
        Pred::SpatialWithin { .. }
            | Pred::SpatialDWithin { .. }
            | Pred::SpatialContains { .. }
            | Pred::SpatialCovers { .. }
            | Pred::SpatialTouches { .. }
            | Pred::SpatialCrosses { .. }
            | Pred::SpatialOverlaps { .. }
            | Pred::SpatialEquals { .. }
            | Pred::SpatialDisjoint { .. }
    )
}

/// FILTER (spatial): keep rows whose stored geometry satisfies `pred` (CONCEPT:EG-083 for
/// within/dwithin; CONCEPT:EG-258 for the DE-9IM relations), reading each row's geometry
/// as WKT from the node property named by the pred's `column`. The relation is evaluated
/// as `relation(row_geometry, query_geometry)` — e.g. `SpatialContains` keeps rows whose
/// geometry CONTAINS the query. Rows with no/invalid geometry are dropped. Order- and
/// score-preserving via [`RowSet::intersect_keep_order`].
#[cfg(feature = "geo")]
fn spatial_filter(view: &GraphView, input: RowSet, pred: &Pred) -> Result<RowSet, String> {
    /// The relation to test between the row geometry and the query geometry.
    enum Rel {
        Within,
        DWithin(f64),
        Contains,
        Covers,
        Touches,
        Crosses,
        Overlaps,
        Equals,
        Disjoint,
    }
    let (column, wkt, rel): (&str, &str, Rel) = match pred {
        Pred::SpatialWithin { column, wkt } => (column, wkt, Rel::Within),
        Pred::SpatialDWithin {
            column,
            wkt,
            distance,
        } => (column, wkt, Rel::DWithin(*distance)),
        Pred::SpatialContains { column, wkt } => (column, wkt, Rel::Contains),
        Pred::SpatialCovers { column, wkt } => (column, wkt, Rel::Covers),
        Pred::SpatialTouches { column, wkt } => (column, wkt, Rel::Touches),
        Pred::SpatialCrosses { column, wkt } => (column, wkt, Rel::Crosses),
        Pred::SpatialOverlaps { column, wkt } => (column, wkt, Rel::Overlaps),
        Pred::SpatialEquals { column, wkt } => (column, wkt, Rel::Equals),
        Pred::SpatialDisjoint { column, wkt } => (column, wkt, Rel::Disjoint),
        // Relational preds never reach here (`filter_op` splits them out).
        _ => return Ok(input),
    };
    let query_geom = eg_geo::parse_wkt(wkt)?;
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry(view, &r.id, column).is_some_and(|g| match rel {
                Rel::Within => eg_geo::within(&g, &query_geom),
                Rel::DWithin(d) => eg_geo::distance(&g, &query_geom) <= d,
                Rel::Contains => eg_geo::contains(&g, &query_geom),
                Rel::Covers => eg_geo::covers(&g, &query_geom),
                Rel::Touches => eg_geo::touches(&g, &query_geom),
                Rel::Crosses => eg_geo::crosses(&g, &query_geom),
                Rel::Overlaps => eg_geo::overlaps(&g, &query_geom),
                Rel::Equals => eg_geo::equals(&g, &query_geom),
                Rel::Disjoint => eg_geo::disjoint(&g, &query_geom),
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    Ok(input.intersect_keep_order(&keep))
}

/// TRANSFORM (spatial CRS, CONCEPT:EG-255): reproject each row's stored geometry into
/// `to_epsg`. The source CRS is the row geometry's EWKT `SRID=…;` tag, or `from_epsg` when
/// given (an explicit override always wins). Rows with no geometry, no resolvable source
/// CRS, or an unsupported/failed reprojection are DROPPED — order- and score-preserving,
/// exactly as the tensor/spatial legs. A v1 executor does NOT persist the reprojected
/// geometry back to the blob (mirroring `tensor_op`); it validates the transform per row.
#[cfg(feature = "geo")]
fn spatial_reproject(
    view: &GraphView,
    input: RowSet,
    to_epsg: u32,
    from_epsg: Option<u32>,
) -> RowSet {
    let to = eg_geo::Crs::from_epsg(to_epsg);
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry_srid(view, &r.id).is_some_and(|(srid, g)| {
                match from_epsg.or(srid).map(eg_geo::Crs::from_epsg) {
                    Some(from) => eg_geo::reproject(&g, from, to).is_ok(),
                    None => false, // unknown source CRS ⇒ cannot reproject
                }
            })
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// TRANSFORM (constructive, CONCEPT:EG-259): apply the eg-geo `algebra` op `kind` to each
/// row's stored geometry (conventional `geometry`/`geom` property), producing a derived
/// geometry. Rows whose geometry is missing/invalid or where the op yields nothing (e.g.
/// an empty intersection, a degenerate centroid) are DROPPED — order- and score-preserving,
/// exactly as [`tensor_op`]. The derived geometry validates the op per row; a v1 executor
/// does NOT persist it back to the blob (the documented follow-up).
#[cfg(feature = "geo")]
fn spatial_op(view: &GraphView, input: RowSet, kind: &eg_types::wire::SpatialOpKind) -> RowSet {
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            row_geometry_conv(view, &r.id).is_some_and(|g| apply_spatial_op(&g, kind).is_some())
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// Map the pure-serde wire `SpatialOpKind` (eg-types) to the eg-geo `algebra` op and run
/// it, returning the derived geometry (`None` when the op has no result). Unary ops always
/// succeed; the binary WKT-operand ops parse the second geometry then apply the op.
#[cfg(feature = "geo")]
fn apply_spatial_op(
    g: &eg_geo::Geometry,
    kind: &eg_types::wire::SpatialOpKind,
) -> Option<eg_geo::Geometry> {
    use eg_types::wire::SpatialOpKind;
    match kind {
        SpatialOpKind::Buffer { distance } => Some(eg_geo::buffer(g, *distance)),
        SpatialOpKind::ConvexHull => Some(eg_geo::convex_hull(g)),
        SpatialOpKind::Simplify { tolerance } => Some(eg_geo::simplify(g, *tolerance)),
        SpatialOpKind::Centroid => eg_geo::centroid(g).map(eg_geo::Geometry::Point),
        SpatialOpKind::Union { wkt } => eg_geo::parse_wkt(wkt).ok().map(|o| eg_geo::union(g, &o)),
        SpatialOpKind::Intersection { wkt } => eg_geo::parse_wkt(wkt)
            .ok()
            .and_then(|o| eg_geo::intersection(g, &o)),
        SpatialOpKind::Difference { wkt } => eg_geo::parse_wkt(wkt)
            .ok()
            .and_then(|o| eg_geo::difference(g, &o)),
    }
}

/// Read node `id`'s geometry from the WKT string in property `column` of its blob.
#[cfg(feature = "geo")]
fn row_geometry(view: &GraphView, id: &str, column: &str) -> Option<eg_geo::Geometry> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let wkt = v.get(column)?.as_str()?;
    eg_geo::parse_wkt(wkt).ok()
}

/// Read a geometry from a node's decoded blob via the conventional `geometry`/`geom`
/// WKT property (used by the layer-wide `SpatialScan`).
#[cfg(feature = "geo")]
fn geometry_from_value(v: &serde_json::Value) -> Option<eg_geo::Geometry> {
    let wkt = v.get("geometry").or_else(|| v.get("geom"))?.as_str()?;
    eg_geo::parse_wkt(wkt).ok()
}

/// Read node `id`'s geometry from the conventional `geometry`/`geom` WKT property (the
/// column-free read used by `Op::SpatialOp`, CONCEPT:EG-259).
#[cfg(feature = "geo")]
fn row_geometry_conv(view: &GraphView, id: &str) -> Option<eg_geo::Geometry> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    geometry_from_value(&v)
}

/// Read node `id`'s geometry AND its EWKT `SRID=…;` CRS tag from the conventional
/// `geometry`/`geom` WKT property (used by `Op::Reproject`, CONCEPT:EG-255). The `srid`
/// is `None` when the stored WKT carries no EWKT prefix.
#[cfg(feature = "geo")]
fn row_geometry_srid(view: &GraphView, id: &str) -> Option<(Option<u32>, eg_geo::Geometry)> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let wkt = v.get("geometry").or_else(|| v.get("geom"))?.as_str()?;
    eg_geo::parse_with_srid(wkt).ok()
}

// ── the tensor leg — eg-tensor N-D arrays over the GraphView blobs (EG-085) ──────

/// SOURCE (tensor): every node in `layer` (matched on the `type` property, exactly as
/// [`scan_label`]) that carries a valid dense N-D array in the conventional `tensor`
/// property (CONCEPT:EG-085). A dep-free view scan, mirroring `spatial_scan` — the
/// matching ids seed the RowSet; the tensor itself is read per-row by [`tensor_op`].
/// (Content-addressing the tensor bytes in the blob CAS — `ChunkStore` + EG-071 — is a
/// documented follow-up; v1 reads the tensor as a typed node property off the live
/// snapshot.)
#[cfg(feature = "tensor")]
fn tensor_scan(view: &GraphView, layer: &str) -> RowSet {
    let mut ids: Vec<String> = Vec::new();
    for (id, blob) in view.node_properties.iter() {
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
            continue;
        };
        if v.get("type").and_then(|t| t.as_str()) != Some(layer) {
            continue;
        }
        if tensor_from_value(&v).is_some() {
            ids.push(id.clone());
        }
    }
    RowSet::from_ids(ids)
}

/// TRANSFORM (tensor): apply the eg-tensor op `kind` (slice/reduce/elementwise) to each
/// row's stored tensor (CONCEPT:EG-085). Rows whose tensor is missing/invalid or where
/// the op fails (e.g. a slice out of bounds) are DROPPED — order- and score-preserving
/// via [`RowSet::intersect_keep_order`], exactly as the spatial `Filter` leg drops rows
/// with no geometry.
///
/// CONCEPT:EG-304 — CAS write-back: when a [`PlanCtx::tensor_store`] is attached, each
/// derived tensor that the op successfully produces is WRITTEN BACK into that
/// content-addressed [`eg_tensor::TensorStore`] (via [`eg_tensor::TensorStore::put`]),
/// so the result becomes a durable, dedup-shared blob addressable by its deterministic
/// content hash — closing the EG-085 follow-up. The write-back is additive and does NOT
/// change which rows survive: a row is kept iff the op succeeds, exactly as before, so a
/// `None` store yields byte-for-byte the prior (validate-only) behavior. Deterministic:
/// identical derived tensors content-address to one blob (idempotent dedup) regardless of
/// row order or how many times the plan runs.
#[cfg(feature = "tensor")]
fn tensor_op(ctx: &PlanCtx, input: RowSet, kind: &eg_types::wire::TensorOpKind) -> RowSet {
    let keep: HashSet<&str> = input
        .rows()
        .iter()
        .filter(|r| {
            // Compute the derived tensor to VALIDATE the op per row; on success, write it
            // back to the CAS (CONCEPT:EG-304) when a store is attached.
            match row_tensor(ctx.view, &r.id) {
                Some(t) => match apply_tensor_op(&t, kind) {
                    Ok(derived) => {
                        if let Some(store) = ctx.tensor_store {
                            // Recover from a poisoned lock: `put` is infallible, so the
                            // guard is never left in a bad state; keep the write-back
                            // durable rather than propagating a poison panic.
                            store
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .put(&derived);
                        }
                        true
                    }
                    Err(_) => false,
                },
                None => false,
            }
        })
        .map(|r| r.id.as_str())
        .collect();
    input.intersect_keep_order(&keep)
}

/// Read node `id`'s tensor from the conventional `tensor` property of its blob
/// (CONCEPT:EG-085), decoding the typed serde form.
#[cfg(feature = "tensor")]
fn row_tensor(view: &GraphView, id: &str) -> Option<eg_tensor::Tensor> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    tensor_from_value(&v)
}

/// Read a tensor from a node's decoded blob via the conventional `tensor` property.
#[cfg(feature = "tensor")]
fn tensor_from_value(v: &serde_json::Value) -> Option<eg_tensor::Tensor> {
    serde_json::from_value::<eg_tensor::Tensor>(v.get("tensor")?.clone()).ok()
}

/// Map the pure-serde wire `TensorOpKind` (eg-types) to the eg-tensor op and run it.
/// The wire enums are defined in eg-types (Pi-safe, no eg-tensor dep); this seam turns
/// them into the real N-D array math behind the `tensor` gate.
#[cfg(feature = "tensor")]
fn apply_tensor_op(
    t: &eg_tensor::Tensor,
    kind: &eg_types::wire::TensorOpKind,
) -> Result<eg_tensor::Tensor, String> {
    use eg_types::wire::{TensorElementwiseOp, TensorOpKind, TensorReduceKind};
    match kind {
        TensorOpKind::Slice { ranges } => t.slice(ranges),
        TensorOpKind::Reduce { axis, kind } => {
            let k = match kind {
                TensorReduceKind::Sum => eg_tensor::ReduceKind::Sum,
                TensorReduceKind::Mean => eg_tensor::ReduceKind::Mean,
                TensorReduceKind::Max => eg_tensor::ReduceKind::Max,
                TensorReduceKind::Min => eg_tensor::ReduceKind::Min,
            };
            t.reduce(*axis, k)
        }
        TensorOpKind::Elementwise { op, scalar } => {
            let o = match op {
                TensorElementwiseOp::Add => eg_tensor::ElementwiseOp::Add,
                TensorElementwiseOp::Sub => eg_tensor::ElementwiseOp::Sub,
                TensorElementwiseOp::Mul => eg_tensor::ElementwiseOp::Mul,
                TensorElementwiseOp::Div => eg_tensor::ElementwiseOp::Div,
            };
            Ok(t.elementwise(o, *scalar))
        }
    }
}

// ── the probabilistic leg — eg-types Distribution + eg-compute Bayes (EG-086) ────

/// TRANSFORM (probabilistic, CONCEPT:EG-086): evaluate `query` against each row's stored
/// `Distribution` value and SCORE the row with the closed-form result, returning the rows
/// re-ordered by that score DESCENDING — a ranked RowSet a downstream `Limit` respects,
/// exactly like `Rank`. Rows whose `distribution` property is missing/invalid, or where
/// the query does not apply (an unsupported `Conditional` conjugate pair), are DROPPED.
/// Deterministic: a `Sample { seed }` draws the SAME value every run (no RNG-from-clock).
#[cfg(feature = "probabilistic")]
fn probabilistic_op(view: &GraphView, input: RowSet, query: &eg_types::wire::ProbQuery) -> RowSet {
    let mut scored: Vec<(String, f32)> = input
        .rows()
        .iter()
        .filter_map(|r| {
            let dist = row_distribution(view, &r.id)?;
            let val = eval_prob_query(&dist, query)?;
            Some((r.id.clone(), val as f32))
        })
        .collect();
    // Descending by score → a ranked order (a NaN score sorts last, never above a real one).
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    RowSet::from_scored(scored)
}

/// Read node `id`'s distribution from the conventional `distribution` property of its
/// blob (CONCEPT:EG-086), decoding the tagged serde form of `eg_types::Distribution`.
#[cfg(feature = "probabilistic")]
fn row_distribution(view: &GraphView, id: &str) -> Option<eg_types::Distribution> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    serde_json::from_value::<eg_types::Distribution>(v.get("distribution")?.clone()).ok()
}

/// Lower the pure-serde wire `ProbQuery` (eg-types) onto the real distribution math: the
/// `Distribution` moment/pdf/pmf/sample methods (eg-types) + the conjugate
/// `bayesian_update` (eg-compute) behind the `probabilistic` gate (CONCEPT:EG-086).
/// Returns `None` when the query does not apply (unsupported conjugate pair) so the
/// caller drops the row.
#[cfg(feature = "probabilistic")]
fn eval_prob_query(
    dist: &eg_types::Distribution,
    query: &eg_types::wire::ProbQuery,
) -> Option<f64> {
    use eg_types::wire::{ProbEvidenceSpec, ProbQuery};
    match query {
        ProbQuery::Expectation => Some(dist.mean()),
        ProbQuery::Marginal { at, label } => Some(match label {
            Some(l) => dist.pmf(l),
            None => dist.pdf(*at),
        }),
        ProbQuery::Conditional { evidence } => {
            let ev = match evidence {
                ProbEvidenceSpec::Bernoulli {
                    successes,
                    failures,
                } => eg_compute::probabilistic::Evidence::Bernoulli {
                    successes: *successes,
                    failures: *failures,
                },
                ProbEvidenceSpec::Gaussian {
                    observations,
                    known_variance,
                } => eg_compute::probabilistic::Evidence::Gaussian {
                    observations: observations.clone(),
                    known_variance: *known_variance,
                },
            };
            eg_compute::probabilistic::bayesian_update(dist, &ev)
                .ok()
                .map(|post| post.mean())
        }
        ProbQuery::Sample { seed } => Some(dist.sample(*seed)),
    }
}

// ── the stream / CEP leg — eg-stream bounded NFA over the GraphView blobs (EG-088) ─

/// The reserved attribute key under which [`cep_op`] stashes each event's originating
/// node id, so a detected [`eg_stream::Match`]'s events can be mapped back to the input
/// rows that participated. Namespaced with a leading `_` to avoid colliding with a
/// user's own event attributes.
#[cfg(feature = "stream")]
const CEP_ID_ATTR: &str = "_eg_row_id";

/// TRANSFORM (stream, CONCEPT:EG-088): interpret the input RowSet as a time-ordered
/// event stream — each row's node blob supplies `ts` (u64), `key` (string) and `attrs`
/// (object) — run the eg-stream bounded NFA for `spec.pattern` over `spec.window`, and
/// keep the input rows that PARTICIPATE in a detected match (order- and score-preserving
/// via [`RowSet::intersect_keep_order`], exactly as the spatial/tensor legs narrow their
/// input). Rows whose blob lacks a numeric `ts` are not events and never match.
/// (EG-067 `Op::Window` is the plan-side windowing primitive; a live standing CEP query
/// fed by the EG-064 CDC `ChangeNotifier` bus — matching incrementally as the bus
/// advances the window — is the documented follow-up; this batch executor is what lands.)
#[cfg(feature = "stream")]
fn cep_op(view: &GraphView, input: RowSet, spec: &eg_types::wire::CepPatternSpec) -> RowSet {
    // Build the event stream from the input rows, tagging each event with its source row
    // id so matches map back. `run` sorts by `ts` internally.
    let mut events: Vec<eg_stream::Event> = Vec::new();
    for r in input.rows() {
        if let Some(mut ev) = row_event(view, &r.id) {
            ev.attrs.insert(
                CEP_ID_ATTR.to_string(),
                serde_json::Value::String(r.id.clone()),
            );
            events.push(ev);
        }
    }
    let pattern = cep_pattern_from_spec(&spec.pattern);
    let window = cep_window_from_spec(spec.window);
    let matches = eg_stream::run(&pattern, &events, window);

    let keep_ids: HashSet<String> = matches
        .iter()
        .flat_map(|m| m.events.iter())
        .filter_map(|e| e.attrs.get(CEP_ID_ATTR).and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .collect();
    let keep: HashSet<&str> = keep_ids.iter().map(|s| s.as_str()).collect();
    input.intersect_keep_order(&keep)
}

/// Read node `id`'s event from its blob (CONCEPT:EG-088): the conventional `ts` (u64,
/// required — a node with no numeric `ts` is not an event), `key` (string, defaults to
/// empty) and `attrs` (object, defaults to empty).
#[cfg(feature = "stream")]
fn row_event(view: &GraphView, id: &str) -> Option<eg_stream::Event> {
    let blob = view.node_properties.get(id)?;
    let v: serde_json::Value = rmp_serde::from_slice(blob.as_slice()).ok()?;
    let ts = v.get("ts").and_then(|t| t.as_u64())?;
    let key = v
        .get("key")
        .and_then(|k| k.as_str())
        .unwrap_or_default()
        .to_string();
    let attrs = v
        .get("attrs")
        .and_then(|a| a.as_object())
        .cloned()
        .unwrap_or_default();
    Some(eg_stream::Event { ts, key, attrs })
}

/// Map the pure-serde wire `CepWindowSpec` (eg-types) to eg-stream's `Window`.
#[cfg(feature = "stream")]
fn cep_window_from_spec(w: eg_types::wire::CepWindowSpec) -> eg_stream::Window {
    use eg_types::wire::CepWindowSpec;
    match w {
        CepWindowSpec::Sliding { size } => eg_stream::Window::Sliding { size },
        CepWindowSpec::Tumbling { size } => eg_stream::Window::Tumbling { size },
    }
}

/// Map a pure-serde wire `CepMatcherSpec` (eg-types) to eg-stream's `EventMatcher`.
#[cfg(feature = "stream")]
fn cep_matcher_from_spec(m: &eg_types::wire::CepMatcherSpec) -> eg_stream::EventMatcher {
    use eg_types::wire::CepAttrPredSpec;
    let preds = m
        .preds
        .iter()
        .map(|p| match p {
            CepAttrPredSpec::Eq { field, value } => eg_stream::AttrPredicate::Eq {
                field: field.clone(),
                value: value.clone(),
            },
            CepAttrPredSpec::Gt { field, value } => eg_stream::AttrPredicate::Gt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Lt { field, value } => eg_stream::AttrPredicate::Lt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Exists { field } => eg_stream::AttrPredicate::Exists {
                field: field.clone(),
            },
        })
        .collect();
    eg_stream::EventMatcher {
        key: m.key.clone(),
        preds,
    }
}

/// Map the pure-serde wire `CepNodeSpec` tree (eg-types) to eg-stream's `CepPattern`.
/// The wire enums are Pi-safe (no eg-stream dep); this seam turns them into the real NFA
/// pattern behind the `stream` gate.
#[cfg(feature = "stream")]
fn cep_pattern_from_spec(p: &eg_types::wire::CepNodeSpec) -> eg_stream::CepPattern {
    use eg_types::wire::CepNodeSpec;
    match p {
        CepNodeSpec::Sequence(matchers) => {
            eg_stream::CepPattern::Sequence(matchers.iter().map(cep_matcher_from_spec).collect())
        }
        CepNodeSpec::Within { within, pattern } => eg_stream::CepPattern::Within {
            within: *within,
            pattern: Box::new(cep_pattern_from_spec(pattern)),
        },
        CepNodeSpec::Absence { a, b, within } => eg_stream::CepPattern::Absence {
            a: cep_matcher_from_spec(a),
            b: cep_matcher_from_spec(b),
            within: *within,
        },
    }
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
            // JSONPath preds (CONCEPT:EG-084) are NOT lowered to SQL — `filter_op` splits
            // them out and applies them per-row via `eg_core::jsonpath`, so they never
            // reach here. This defensive arm keeps the match exhaustive: a no-op `1=1`.
            Pred::JsonPath { .. } => "1=1".into(),
            // Spatial preds (CONCEPT:EG-083 / EG-258) are NOT lowered to SQL — `filter_op`
            // splits them out and applies them per-row via eg-geo, so they never reach here.
            // This defensive arm keeps the match exhaustive under `geo`: a no-op `1=1`.
            #[cfg(feature = "geo")]
            Pred::SpatialWithin { .. }
            | Pred::SpatialDWithin { .. }
            | Pred::SpatialContains { .. }
            | Pred::SpatialCovers { .. }
            | Pred::SpatialTouches { .. }
            | Pred::SpatialCrosses { .. }
            | Pred::SpatialOverlaps { .. }
            | Pred::SpatialEquals { .. }
            | Pred::SpatialDisjoint { .. } => "1=1".into(),
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
