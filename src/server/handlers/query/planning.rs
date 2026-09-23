use super::*;

/// The process-wide server-side text→vector embedder for the UQL `RANK BY ~ "text"`
/// (`Op::RankEmbed`) NL→vector seam (CONCEPT:EG-KG.query.bind-server-side-text / EG-411) — the FACADE injection point.
/// Returns the bound embedder, or `None` when none is configured (an `Op::RankEmbed` then
/// errors cleanly). The engine stores embeddings but produces them client-side today, so no
/// in-process model ships and the default is `None`; `EG_UQL_TEXT_EMBEDDER=hash` binds the
/// deterministic `HashEmbedder` fallback (offline/testing — arbitrary ranking). A real
/// embedding model (an ONNX/remote-service impl of `eg_plan::TextEmbedder`) is wired in HERE.
#[cfg(feature = "query")]
pub(crate) fn uql_text_embedder() -> Option<&'static dyn eg_plan::TextEmbedder> {
    use std::sync::OnceLock;
    static EMBEDDER: OnceLock<Option<eg_plan::HashEmbedder>> = OnceLock::new();
    EMBEDDER
        .get_or_init(
            || match std::env::var("EG_UQL_TEXT_EMBEDDER").ok().as_deref() {
                Some("hash") => Some(eg_plan::HashEmbedder::default()),
                _ => None,
            },
        )
        .as_ref()
        .map(|e| e as &dyn eg_plan::TextEmbedder)
}

/// Bind the SQL `eg_embed(text)` function's process-wide embedder to the SAME
/// `eg_plan::TextEmbedder` [`uql_text_embedder`] already gives the UQL `RANK BY ~ "text"`
/// leg (design `plans/semantic-indexing/DESIGN-embedding-bindings.md` §9 phase 2). ONE
/// trait, TWO callers: `eg-query` cannot name `eg_plan::TextEmbedder` (`eg-plan` depends
/// on `eg-query`, so the reverse edge is a Cargo package cycle — see
/// `eg_query::sql::embed_udf`'s module doc), so this facade — which depends on BOTH
/// crates — is where the concrete embedder crosses that seam, as a closure.
///
/// Idempotent and cheap; called at the top of the SQL entry point rather than from a
/// server-startup hook because `uql_text_embedder` is itself lazily resolved and
/// `bind_text_embedder` is one-shot, so "first SQL statement" is the earliest moment the
/// binding is both possible and needed. With no embedder configured
/// (`EG_UQL_TEXT_EMBEDDER` unset — today's default, since the engine stores embeddings
/// but produces them client-side) nothing is bound and `eg_embed(...)` returns its typed
/// "no server-side text embedder is bound" error rather than a zero vector.
///
/// `pub(crate)` so the pgwire/wire SQL entry points can bind through this same function
/// rather than growing a second injection site (they do not call [`handle_sql`]).
#[cfg(feature = "query")]
pub(crate) fn bind_sql_text_embedder() {
    static BOUND: std::sync::Once = std::sync::Once::new();
    BOUND.call_once(|| {
        let Some(embedder) = uql_text_embedder() else {
            return;
        };
        let _ = eg_query::sql::bind_text_embedder(std::sync::Arc::new(move |text: &str| {
            embedder.embed(text)
        }));
    });
}

/// Compute a SOUND dependency set for a UQL/`UnifiedQuery` plan
/// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7), or `None` when the plan's
/// shape cannot be reduced to one — the caller then uses the coarse version-keyed result-cache
/// path (unchanged). ONLY a plan built from pure node-relational ops is dependency-scoped:
///   * `Scan { label }` — the sole source: a labeled scan depends on that label, an unlabeled scan
///     on the whole node set (both tracked by the [`eg_core::dep_scope::DepClock`]);
///   * `Filter` / `Limit` — RowSet-narrowing transforms over the ALREADY-sourced rows, adding no
///     graph dependency beyond the source's (they read only the scanned rows' own properties,
///     which the source's label/all-nodes dimension already covers).
///
/// ANY other op — a `Traverse` (edges + arbitrary reached nodes), a vector/lexical `Rank`, a
/// temporal `AsOf`, a reasoner/SPARQL/federation/tensor/spatial/tsdb/epistemic leg — reads state
/// the clock does not model, so the WHOLE plan falls back to coarse invalidation. That
/// conservative boundary is what makes a stale hit impossible: a dependency set is only ever
/// returned when it PROVABLY captures everything the query reads.
#[cfg(feature = "result-cache")]
pub(crate) fn plan_dependency_set(plan: &eg_plan::Plan) -> Option<eg_core::dep_scope::DepSet> {
    use eg_core::dep_scope::{DepSet, Dim};
    let mut dims: Vec<Dim> = Vec::new();
    let mut has_source = false;
    for op in &plan.ops {
        match op {
            eg_plan::Op::Scan { label } => {
                has_source = true;
                if label.is_empty() {
                    dims.push(Dim::AllNodes);
                } else {
                    dims.push(Dim::Label(label.clone()));
                }
            }
            eg_plan::Op::Filter { .. } | eg_plan::Op::Limit { .. } => {}
            // Any op reading state outside the dependency clock's model ⇒ coarse fallback.
            _ => return None,
        }
    }
    // A plan with no graph SOURCE op (e.g. a pure federation/tsdb seed) is not a bounded node
    // read — fall back rather than claim an empty dependency set.
    if has_source {
        Some(DepSet::new(dims))
    } else {
        None
    }
}

/// Does `ops` reference a lexical text op — an `Op::RankText` at the top level or nested
/// inside an `Op::FuseRrf` branch (CONCEPT:EG-KG.query.served-text-index-binding)? Drives whether
/// `run_unified` builds+binds a served text index at all, so a non-text plan pays nothing.
#[cfg(all(feature = "query", feature = "text"))]
pub(crate) fn plan_needs_text(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::RankText { .. } => true,
        eg_plan::Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_needs_text(b)),
        _ => false,
    })
}

/// Does `ops` reference `Op::SpatialScan` — at the top level or nested inside an
/// `Op::FuseRrf` branch (CONCEPT:EG-KG.storage.incremental-spatial, L37, mirroring `plan_needs_text`)?
/// Drives whether `run_unified` binds the served spatial index at all, so a non-spatial
/// plan pays nothing.
#[cfg(all(feature = "query", feature = "geo"))]
pub(crate) fn plan_needs_spatial(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::SpatialScan { .. } => true,
        eg_plan::Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_needs_spatial(b)),
        _ => false,
    })
}

/// Does `ops` NAME a registered foreign source — an `Op::Foreign` (the UQL
/// `FOREIGN "<name>"` marker) or a `Named` `Op::ForeignScan`, at the top level or nested
/// inside an `Op::FuseRrf` branch (CONCEPT:EG-KG.query.closure-backed-source, mirroring
/// `plan_needs_text`)? Drives whether a served path builds the caller's owner-scoped
/// foreign-source registry at all
/// (`crate::server::foreign_catalog::ForeignSourceCatalog::resolve_for_plan`), so a
/// non-federated plan pays nothing. A self-describing (inline-spec)
/// `Op::ForeignScan` resolves without a registry, so it does not need the binding.
#[cfg(all(feature = "query", feature = "federation"))]
pub(crate) fn plan_needs_foreign(ops: &[eg_plan::Op]) -> bool {
    ops.iter().any(|op| match op {
        eg_plan::Op::Foreign { .. } => true,
        eg_plan::Op::ForeignScan { source, .. } => {
            matches!(**source, eg_types::wire::ForeignSourceSpec::Named { .. })
        }
        eg_plan::Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_needs_foreign(b)),
        _ => false,
    })
}

/// Build a BM25 [`eg_text::TextIndex`] from a graph snapshot's node blobs
/// (CONCEPT:EG-KG.query.served-text-index-binding) — the served lexical index for `Op::RankText` /
/// `Op::FuseRrf`. Each node's indexable text is the concatenation of every STRING leaf in its
/// JSON property blob (so `name` / `description` / `text` / `content` / `title` / … are all
/// searchable, matching the human-readable fields `Discover` hydrates), keyed by node id. An
/// in-memory index (no persist dir needed — it is rebuilt per served text query off the exact
/// queried snapshot, so it is always current with the read). Returns `None` on a build/commit
/// error (the plan then degrades to no lexical hits — never errs), mirroring an absent index.
#[cfg(all(feature = "query", feature = "text"))]
pub(crate) fn build_text_index_from_view(
    view: &crate::graph::GraphView,
) -> Option<eg_text::TextIndex> {
    /// Append every string leaf in `v` (recursing objects/arrays) to `out`, space-separated.
    fn collect_strings(v: &serde_json::Value, out: &mut String) {
        match v {
            serde_json::Value::String(s) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(s);
            }
            serde_json::Value::Array(a) => a.iter().for_each(|e| collect_strings(e, out)),
            serde_json::Value::Object(o) => o.values().for_each(|e| collect_strings(e, out)),
            _ => {}
        }
    }
    let mut index = eg_text::TextIndex::in_memory().ok()?;
    for (id, blob) in &view.node_properties {
        let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
            continue;
        };
        let mut text = String::new();
        collect_strings(&v, &mut text);
        if !text.is_empty() {
            index.upsert(id, &text);
        }
    }
    index.commit().ok()?;
    Some(index)
}

/// Bundles the served-adapter modality indexes `run_unified` pushes a plan's legs down
/// into (CONCEPT:EG-KG.query.served-text-index-binding / CONCEPT:EG-KG.storage.incremental-spatial) — ONE parameter
/// rather than one per modality, so `run_unified`'s argument count stays sane as more
/// served-index modalities are added over time (L37 added spatial beside text; a future
/// modality adds a field here, not a new top-level parameter). Each field is `Some` and
/// `.available()` ⇒ the matching op searches the MAINTAINED persistent index directly, NO
/// per-query rebuild; otherwise `run_unified` falls back to the prior behavior (a
/// snapshot-derived index for text, an unbound `PlanCtx::spatial` — ephemeral R-tree — for
/// spatial), exactly as if this bundle were never passed.
#[cfg(feature = "query")]
#[derive(Default)]
pub(crate) struct ServedIndexes<'a> {
    #[cfg(feature = "text")]
    pub text: Option<&'a crate::server::secondary_indexes::ServedTextIndex>,
    #[cfg(feature = "geo")]
    pub spatial: Option<&'a crate::server::secondary_indexes::ServedSpatialIndex>,
    /// CONCEPT:EG-KG.query.closure-backed-source — the CALLER'S owner-scoped foreign
    /// registry (EH-373), built by
    /// [`crate::server::foreign_catalog::ForeignSourceCatalog::registry_for`] from only the
    /// caller's own (tenant+principal) `RegisterForeignSource` entries. An `Op::Foreign`
    /// (the UQL `FOREIGN "<name>"` marker) / a `Named` `Op::ForeignScan` resolves through
    /// it, so another principal's source name resolves as not-registered. `None` ⇒ no registry is
    /// bound and a name-resolving op stays a clean typed error — never a silent empty
    /// set, never silently-local rows.
    #[cfg(feature = "federation")]
    pub foreign: Option<&'a eg_plan::federation::ForeignSourceRegistry>,
    /// EH-196 — the graph's composed GraphSchema SHACL shapes
    /// (`handlers::rdf::ServedShapes`), which a `VALIDATE SHAPE` stage without its own
    /// `USING` document validates against. `None` ⇒ such a stage is a typed error.
    #[cfg(all(feature = "shacl", feature = "owl-plan"))]
    pub shapes: Option<&'a dyn eg_plan::exec::ShapeSource>,
    // Keeps `'a` used even when neither `text` nor `geo` is built, so `ServedIndexes<'_>`
    // stays a valid (zero-field-active) type in every feature combination.
    #[cfg(not(any(feature = "text", feature = "geo")))]
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// A graph's maintained secondary indexes, opened where a plan runs (inside
/// the off-lock closure) and lent to `run_unified` as [`ServedIndexes`]: the
/// one construction every served plan path shares (UnifiedQuery, NL, in-txn
/// overlay, wire UQL, mining-sourced plans).
pub(crate) struct CoreIndexes<'c> {
    #[cfg(feature = "text")]
    text: crate::server::secondary_indexes::ServedTextIndex,
    #[cfg(feature = "geo")]
    spatial: crate::server::secondary_indexes::ServedSpatialIndex,
    #[cfg(all(feature = "shacl", feature = "owl-plan"))]
    shapes: crate::server::handlers::rdf::ServedShapes<'c>,
    core: std::marker::PhantomData<&'c ()>,
}

impl<'c> CoreIndexes<'c> {
    pub(crate) fn open(core: &'c std::sync::Arc<crate::graph::GraphCore>) -> Self {
        #[cfg(not(any(
            feature = "text",
            feature = "geo",
            all(feature = "shacl", feature = "owl-plan")
        )))]
        let _ = core;
        Self {
            #[cfg(feature = "text")]
            text: crate::server::secondary_indexes::ServedTextIndex::new(core.clone()),
            #[cfg(feature = "geo")]
            spatial: crate::server::secondary_indexes::ServedSpatialIndex::new(core.clone()),
            #[cfg(all(feature = "shacl", feature = "owl-plan"))]
            shapes: crate::server::handlers::rdf::ServedShapes::new(core),
            core: std::marker::PhantomData,
        }
    }

    /// These indexes as `run_unified` takes them, with the caller's owner-scoped
    /// foreign registry (EH-373) when the plan names a foreign source.
    pub(crate) fn served<'a>(
        &'a self,
        #[cfg(feature = "federation")] foreign: Option<
            &'a eg_plan::federation::ForeignSourceRegistry,
        >,
    ) -> ServedIndexes<'a> {
        ServedIndexes {
            #[cfg(feature = "text")]
            text: Some(&self.text),
            #[cfg(feature = "geo")]
            spatial: Some(&self.spatial),
            #[cfg(feature = "federation")]
            foreign,
            #[cfg(all(feature = "shacl", feature = "owl-plan"))]
            shapes: Some(&self.shapes),
            #[cfg(not(any(feature = "text", feature = "geo")))]
            _marker: std::marker::PhantomData,
        }
    }
}

/// Bundles `run_unified`'s tsdb `Op::TsScan` leg-binding parameters — ONE
/// parameter rather than four, so the function's argument count stays under the
/// clippy ceiling (mirrors the `ServedIndexes` rationale above). Only exists
/// under the `tsdb` feature; a non-`tsdb` build omits the parameter entirely.
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) struct TsdbLegBind<'a> {
    /// The committed native tsdb `SeriesStore` backing `Op::TsScan`, threaded in
    /// so a UQL plan fuses its time-series leg with the graph/vector/relational
    /// legs. `None` ⇒ a `TsScan` yields no rows (degrade, never err).
    pub tsdb: Option<&'a eg_tsdb::store::SeriesStore>,
    /// Verified tenant + actor-owned graph policy context for committed TsScan
    /// reads. Served callers bind both or omit the store entirely.
    pub tsdb_tenant: Option<&'a str>,
    pub tsdb_graph: Option<&'a str>,
    /// In-txn tsdb read-your-own-writes (CONCEPT:EG-KG.query.txn-tsdb-read-your): the resolved txn's OWN staged,
    /// uncommitted series points, overlaid onto `Op::TsScan` BEFORE the committed store so
    /// an in-txn UQL reads its own measurements. `None` off-txn ⇒ committed series only.
    pub staged_series: Option<&'a eg_plan::StagedSeries>,
}

/// Execute a unified cross-modal plan (CONCEPT:AU-KG.compute.vector/209) over one off-lock
/// snapshot and return the result rows as `[id, score|nil]`. The plan is routed through the
/// full cost optimizer by `eg_plan::execute` (CONCEPT:EG-KG.query.served-plan-optimize-routing); a
/// lexical `Op::RankText`/`Op::FuseRrf` leg is served over the MAINTAINED persistent BM25
/// index when one is registered, falling back to a snapshot-derived index otherwise
/// (CONCEPT:EG-KG.query.served-text-index-binding). Synchronous — runs on the blocking pool via
/// `compute_off_lock`, like the SQL/Cypher legs.
#[cfg(feature = "query")]
pub(crate) fn run_unified(
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    semantic: &eg_core::compute::semantic::SemanticStore,
    served: ServedIndexes<'_>,
    #[cfg(feature = "tsdb")] tsdb_ctx: TsdbLegBind<'_>,
) -> Result<Vec<(String, Option<f32>)>, String> {
    #[cfg(feature = "tsdb")]
    let TsdbLegBind {
        tsdb,
        tsdb_tenant,
        tsdb_graph,
        staged_series,
    } = tsdb_ctx;
    #[cfg(feature = "text")]
    let served_text = served.text;
    #[cfg(feature = "geo")]
    let served_spatial = served.spatial;
    #[cfg(all(feature = "shacl", feature = "owl-plan"))]
    let served_shapes = served.shapes;
    #[cfg(not(any(feature = "text", feature = "geo", feature = "federation")))]
    let ServedIndexes { .. } = served;
    use eg_plan::PlanCtx;

    // CONCEPT:EG-KG.query.served-plan-optimize-routing — the served path hands the plan
    // directly to `eg_plan::execute`, which applies the complete cost optimizer using
    // snapshot-derived cardinality and cost statistics. Optimizer rules are
    // answer-preserving within the EG-405 non-empty guard.
    let ops = plan.ops;

    // CONCEPT:EG-KG.query.served-text-index-binding — bind a live BM25 lexical search surface into the
    // served `PlanCtx` so a served `UnifiedQuery`/`UnifiedQueryText` whose plan carries
    // `Op::RankText` or an `Op::FuseRrf` text branch gets REAL lexical scores (it
    // previously always rebuilt a throwaway index from the queried snapshot on EVERY
    // request — the EG-P1-4 gap). Preference order:
    //   1. the MAINTAINED persistent per-graph `GraphTextIndex`, via `served_text`, when
    //      one is registered — no per-query rebuild, and it reflects every committed
    //      write incrementally (CONCEPT:EG-KG.storage.incremental-text);
    //   2. a snapshot-derived `eg_text::TextIndex` built from `view` (the PRIOR
    //      behavior), for a graph with no `ServerIndexFactory` installed (a bare test
    //      harness, or a graph that predates the factory).
    // Built/bound ONLY when the plan actually references a text op, so a non-text
    // served query pays nothing either way.
    let ctx = PlanCtx::new(view, semantic);
    #[cfg(feature = "text")]
    let need_text = plan_needs_text(&ops);
    #[cfg(feature = "text")]
    let persistent_text = served_text.filter(|st| st.available());
    #[cfg(feature = "text")]
    let snapshot_text_index: Option<eg_text::TextIndex> = if need_text && persistent_text.is_none()
    {
        build_text_index_from_view(view)
    } else {
        None
    };
    #[cfg(feature = "text")]
    let ctx = if !need_text {
        ctx
    } else if let Some(served) = persistent_text {
        ctx.with_text(served)
    } else if let Some(index) = snapshot_text_index.as_ref() {
        ctx.with_text(index)
    } else {
        ctx
    };
    // CONCEPT:EG-KG.storage.incremental-spatial, L37 — bind a persistent spatial index into the served `PlanCtx` so a
    // served `Op::SpatialScan` gets pushed into the MAINTAINED per-graph
    // `GraphSpatialIndex` instead of rebuilding a throwaway packed Hilbert R-tree on
    // EVERY request. `None` (no factory installed, or the plan has no spatial op) keeps
    // `spatial_scan`'s prior ephemeral-build fallback — byte-for-byte the old behavior.
    #[cfg(feature = "geo")]
    let ctx = run_unified_bind_spatial(ctx, &ops, served_spatial);
    // CONCEPT:EG-KG.query.closure-backed-source — bind the caller's owner-scoped
    // foreign registry so a served `Op::Foreign` (`FOREIGN "<name>"`) / a `Named`
    // `Op::ForeignScan` resolves the caller's OWN registered sources (EH-373).
    #[cfg(feature = "federation")]
    let ctx = run_unified_bind_foreign(ctx, served.foreign);
    // CONCEPT:EG-KG.query.bind-server-side-text — bind the server-side text→vector embedder so a UQL `RANK BY ~ "text"`
    // (`Op::RankEmbed`) resolves its query vector at exec time (the NL→vector seam,
    // EG-411). This is the facade INJECTION POINT: the engine stores embeddings but
    // produces them CLIENT-side today (no in-process model), so a real embedding model — an
    // ONNX/remote embedding-service impl of `eg_plan::TextEmbedder` producing vectors in the
    // graph's embedding space — is bound HERE. Absent a bound model an `Op::RankEmbed` is a
    // clean typed error (never a panic), exactly the documented unbound behavior. The
    // deterministic `HashEmbedder` fallback is opt-in via `EG_UQL_TEXT_EMBEDDER=hash` so the
    // seam is exercisable end-to-end offline (its ranking is deterministic but semantically
    // arbitrary — never the production default).
    let ctx = run_unified_bind_embedder(ctx);
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): attach the committed
    // store and its ownership scope atomically, plus the txn's staged-series
    // overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your). A partial/missing scope never
    // leaves a raw store reachable through `TsScan`.
    #[cfg(feature = "tsdb")]
    let ctx = run_unified_bind_tsdb(ctx, tsdb, tsdb_tenant, tsdb_graph, staged_series);
    // CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — bind the tensor CAS
    // write-back sink so a served `Op::TensorOp` actually runs instead of its
    // documented-but-unreachable "TensorOp requires a bound tensor store" error.
    #[cfg(feature = "tensor")]
    let ctx = run_unified_bind_tensor(ctx);
    #[cfg(all(feature = "shacl", feature = "owl-plan"))]
    let ctx = match served_shapes {
        Some(shapes) => ctx.with_shape_source(shapes),
        None => ctx,
    };
    let result = eg_plan::execute(&eg_plan::Plan::new(ops), &ctx)?;
    Ok(result
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect())
}

/// The `Op::SpatialScan` leg-binding of [`run_unified`] (CONCEPT:EG-KG.storage.incremental-spatial, L37): bind a
/// persistent spatial index into the served `PlanCtx` only when the plan needs one
/// and a live, available index was supplied — otherwise keep `spatial_scan`'s
/// prior ephemeral-build fallback, byte-for-byte the old behavior.
#[cfg(feature = "geo")]
pub(crate) fn run_unified_bind_spatial<'a>(
    ctx: eg_plan::PlanCtx<'a>,
    ops: &[eg_plan::Op],
    served_spatial: Option<&'a crate::server::secondary_indexes::ServedSpatialIndex>,
) -> eg_plan::PlanCtx<'a> {
    if !plan_needs_spatial(ops) {
        return ctx;
    }
    match served_spatial.filter(|s| s.available()) {
        Some(served) => ctx.with_spatial(served),
        None => ctx,
    }
}

/// The `Op::Foreign`/`Op::ForeignScan` leg-binding of [`run_unified`]
/// (CONCEPT:EG-KG.query.closure-backed-source): attach the caller's owner-scoped registry, if any.
#[cfg(feature = "federation")]
pub(crate) fn run_unified_bind_foreign<'a>(
    ctx: eg_plan::PlanCtx<'a>,
    foreign_registry: Option<&'a eg_plan::federation::ForeignSourceRegistry>,
) -> eg_plan::PlanCtx<'a> {
    match foreign_registry {
        Some(registry) => ctx.with_foreign(registry),
        None => ctx,
    }
}

/// The `Op::RankEmbed` leg-binding of [`run_unified`] (CONCEPT:EG-KG.query.bind-server-side-text): attach the
/// server-side text→vector embedder, if one is bound (`EG_UQL_TEXT_EMBEDDER=hash`
/// for the deterministic offline fallback; otherwise absent, and `Op::RankEmbed` is
/// a clean typed error).
#[cfg(feature = "query")]
pub(crate) fn run_unified_bind_embedder(ctx: eg_plan::PlanCtx<'_>) -> eg_plan::PlanCtx<'_> {
    match uql_text_embedder() {
        Some(embedder) => ctx.with_embedder(embedder),
        None => ctx,
    }
}

/// The `Op::TsScan` leg-binding of [`run_unified`] (CONCEPT:EG-KG.query.native-time-series): attach the committed
/// store and its ownership scope atomically (a partial/missing scope never leaves
/// a raw store reachable through `TsScan`), then the txn's staged-series overlay
/// (CONCEPT:EG-KG.query.txn-tsdb-read-your) so an in-txn `TsScan` reads its own uncommitted points.
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) fn run_unified_bind_tsdb<'a>(
    ctx: eg_plan::PlanCtx<'a>,
    tsdb: Option<&'a eg_tsdb::store::SeriesStore>,
    tsdb_tenant: Option<&'a str>,
    tsdb_graph: Option<&'a str>,
    staged_series: Option<&'a eg_plan::StagedSeries>,
) -> eg_plan::PlanCtx<'a> {
    let ctx = match (tsdb, tsdb_tenant, tsdb_graph) {
        (Some(store), Some(tenant), Some(graph)) => {
            ctx.with_tsdb(store).with_tsdb_scope(tenant, graph)
        }
        _ => ctx,
    };
    match staged_series {
        Some(staged) => ctx.with_staged_series(staged),
        None => ctx,
    }
}

/// The `Op::TensorOp` leg-binding of [`run_unified`] (CONCEPT:EG-KG.storage.derived-tensor-writeback-sink): bind the
/// tensor CAS write-back sink so a served `Op::TensorOp` actually runs instead of
/// its documented-but-unreachable "TensorOp requires a bound tensor store" error.
/// `Op::TensorScan`/`Op::TensorOp` read their INPUT tensor directly off the
/// queried `GraphView`'s node properties (`eg_plan::exec::row_tensor`) — this
/// store is purely the write-back destination for a TensorOp's DERIVED output, so
/// a process-wide singleton (not threaded through `ServerState`/callers) is
/// sufficient and still gives real content-address dedup across requests, unlike a
/// fresh store per call. In-memory only for now — `TensorStore::persist`/`load`
/// (disk durability across restarts) is a follow-up, tracked the same way
/// `tsdb_store` earned its own dedicated `ServerState` wiring.
#[cfg(feature = "tensor")]
pub(crate) fn run_unified_bind_tensor(ctx: eg_plan::PlanCtx<'_>) -> eg_plan::PlanCtx<'_> {
    static TENSOR_STORE: std::sync::OnceLock<std::sync::Mutex<eg_tensor::TensorStore>> =
        std::sync::OnceLock::new();
    let store = TENSOR_STORE.get_or_init(|| std::sync::Mutex::new(eg_tensor::TensorStore::new()));
    ctx.with_tensor_store(store)
}

/// What a unified plan run hands back: the outer `Err` is a ready response
/// (off-lock execution failed), the inner result is the plan's scored rows or
/// its error message.
#[cfg(feature = "query")]
pub(crate) type UnifiedRunOutcome = Result<Result<Vec<(String, Option<f32>)>, String>, Response>;

/// The verified-carrier-scoped legs of one served unified plan, resolved once per
/// request before the result-cache probe: the tsdb `(tenant, namespace)` scope and the
/// caller's owner-scoped (tenant+principal) foreign-source registry (EH-373). Both are
/// derived from the verified read authority only, and both make a result
/// caller-specific, so
/// [`Self::salt_cache_key`] folds them into the result-cache key.
#[cfg(feature = "query")]
#[derive(Default)]
pub(crate) struct ServedPlanLegs {
    #[cfg(feature = "tsdb")]
    pub(crate) tsdb_scope: Option<(String, String)>,
    #[cfg(feature = "federation")]
    pub(crate) foreign: Option<crate::server::foreign_catalog::OwnedForeignRegistry>,
}

#[cfg(feature = "query")]
impl ServedPlanLegs {
    /// Resolve both legs for `plan`; `Err` is the caller-facing refusal text.
    pub(crate) async fn resolve(
        state: &Arc<RwLock<ServerState>>,
        graph_name: &str,
        read_authority: Option<&GraphReadAuthority>,
        plan: &eg_plan::Plan,
    ) -> Result<Self, String> {
        #[cfg(not(feature = "tsdb"))]
        let _ = graph_name;
        #[cfg(not(feature = "federation"))]
        let _ = state;
        #[cfg(not(any(feature = "tsdb", feature = "federation")))]
        let _ = (read_authority, plan);
        Ok(Self {
            #[cfg(feature = "tsdb")]
            tsdb_scope: served_tsdb_scope(plan, graph_name, read_authority)?,
            #[cfg(feature = "federation")]
            foreign: served_foreign_leg(state, plan, read_authority).await?,
        })
    }

    /// Append the tenant-specific parts of these legs to a result-cache key payload
    /// (the tsdb tenant + namespace first, byte-identical to the prior tsdb salt, then the
    /// foreign registry's digest of its owner and every resolved source, so a grant,
    /// revocation or re-registration never serves stale foreign rows).
    #[cfg(feature = "result-cache")]
    pub(crate) fn salt_cache_key(&self, payload: &mut Vec<u8>) {
        #[cfg(feature = "tsdb")]
        if let Some((tenant, graph)) = self.tsdb_scope.as_ref() {
            payload.extend_from_slice(tenant.as_bytes());
            payload.extend_from_slice(graph.as_bytes());
        }
        #[cfg(feature = "federation")]
        if let Some(foreign) = self.foreign.as_ref() {
            payload.extend_from_slice(foreign.cache_salt().as_bytes());
        }
        #[cfg(not(any(feature = "tsdb", feature = "federation")))]
        let _ = payload;
    }
}

/// Resolve the tsdb/text/geo/federation legs and run `plan` off-lock via
/// `run_unified`, exactly as `UnifiedQuery`/`UnifiedQueryText`/`NlQuery` already
/// did inline — pure extract-method out of those three arms' bodies (identical
/// duplicated code in each), no behaviour change. Returns `compute_off_lock`'s
/// raw nested result unchanged; callers keep doing their own
/// `Ok(Ok(rows))`/`Ok(Err(msg))`/`Err(resp)` handling (result-cache storage and
/// the error message text differ per caller, so that stays out of here).
#[cfg(feature = "query")]
pub(crate) async fn run_unified_off_lock(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    core: &Arc<GraphCore>,
    snap: Arc<crate::graph::GraphView>,
    plan: eg_plan::Plan,
    legs: ServedPlanLegs,
) -> UnifiedRunOutcome {
    let core_for_ctx = core.clone();
    #[cfg(feature = "tsdb")]
    let tsdb_scope = legs.tsdb_scope;
    #[cfg(feature = "federation")]
    let foreign = legs.foreign;
    #[cfg(not(any(feature = "tsdb", feature = "federation")))]
    let ServedPlanLegs {} = legs;
    #[cfg(feature = "tsdb")]
    let tsdb = if tsdb_scope.is_some() {
        state.read().await.tsdb_store.clone()
    } else {
        None
    };
    #[cfg(feature = "tsdb")]
    let (tsdb_tenant, tsdb_graph) = match tsdb_scope {
        Some((tenant, graph)) => (Some(tenant), Some(graph)),
        None => (None, None),
    };
    #[cfg(not(feature = "tsdb"))]
    let _ = state;
    compute_off_lock(req_id, move || {
        run_unified_with_staged(
            plan,
            &snap,
            &core_for_ctx,
            &[],
            #[cfg(feature = "federation")]
            bound_registry(&foreign),
            #[cfg(feature = "tsdb")]
            TsdbLegBind {
                tsdb: tsdb.as_deref(),
                tsdb_tenant: tsdb_tenant.as_deref(),
                tsdb_graph: tsdb_graph.as_deref(),
                // Off-txn: no staged-series overlay (CONCEPT:EG-KG.query.txn-tsdb-read-your).
                staged_series: None,
            },
        )
    })
    .await
}

/// CONCEPT:EG-KG.storage.derived-tensor-writeback-sink — served-path proof that
/// `run_unified` (not just `eg-plan`'s own internal executor, already proven by
/// `crates/eg-plan/src/tensor_tests.rs`) now binds a tensor store: an
/// `Op::TensorScan` + `Op::TensorOp` plan run through the SAME entry point every
/// `UnifiedQuery`/`UnifiedQueryText` request uses now executes and returns rows
/// instead of the "TensorOp requires a bound tensor store" error `run_unified`
/// deterministically returned before the `.with_tensor_store(...)` binding was
/// added.
#[cfg(all(test, feature = "tensor"))]
mod tensor_served_round_trip_tests {
    use super::*;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_tensor::{Buffer, Tensor};
    use eg_types::wire::{TensorOpKind, TensorReduceKind};

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// A `Frame` layer of three nodes each holding the same dense 2×3 tensor in
    /// their conventional `tensor` property, mirroring
    /// `eg_plan::tensor_tests::frames()`.
    fn frames_view() -> crate::graph::GraphView {
        let core = GraphCore::new();
        let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
        let tv = serde_json::to_value(&t).unwrap();
        for id in ["F1", "F2", "F3"] {
            core.add_node(
                id.into(),
                blob(serde_json::json!({ "type": "Frame", "tensor": tv })),
            );
        }
        core.analysis_snapshot()
    }

    fn served_indexes() -> ServedIndexes<'static> {
        ServedIndexes {
            #[cfg(feature = "text")]
            text: None,
            #[cfg(feature = "geo")]
            spatial: None,
            #[cfg(feature = "federation")]
            foreign: None,
            #[cfg(all(feature = "shacl", feature = "owl-plan"))]
            shapes: None,
            #[cfg(not(any(feature = "text", feature = "geo")))]
            _marker: std::marker::PhantomData,
        }
    }

    fn call_run_unified(plan: eg_plan::Plan) -> Result<Vec<(String, Option<f32>)>, String> {
        let view = frames_view();
        let semantic = SemanticStore::new();
        run_unified(
            plan,
            &view,
            &semantic,
            served_indexes(),
            #[cfg(feature = "tsdb")]
            TsdbLegBind {
                tsdb: None,
                tsdb_tenant: None,
                tsdb_graph: None,
                staged_series: None,
            },
        )
    }

    #[test]
    fn served_tensor_scan_and_op_executes_instead_of_erroring() {
        let plan = eg_plan::Plan::new(vec![
            eg_plan::Op::TensorScan {
                layer: "Frame".into(),
            },
            eg_plan::Op::TensorOp {
                kind: TensorOpKind::Reduce {
                    axis: 1,
                    kind: TensorReduceKind::Mean,
                },
            },
        ]);
        let rows = call_run_unified(plan).expect(
            "served TensorOp must execute now that run_unified binds a tensor store, \
             not error with 'TensorOp requires a bound tensor store'",
        );
        let mut ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["F1", "F2", "F3"]);
    }

    /// Before the fix, `run_unified` had no `tensor_store` binding at all, so this
    /// exact plan deterministically failed with "TensorOp requires a bound tensor
    /// store" regardless of input — the gap this test closes.
    #[test]
    fn served_tensor_op_without_the_fix_would_have_errored() {
        let plan = eg_plan::Plan::new(vec![
            eg_plan::Op::TensorScan {
                layer: "Frame".into(),
            },
            eg_plan::Op::TensorOp {
                kind: TensorOpKind::Elementwise {
                    op: eg_types::wire::TensorElementwiseOp::Mul,
                    scalar: 2.0,
                },
            },
        ]);
        assert!(
            call_run_unified(plan).is_ok(),
            "TensorOp over the served path must not deterministically error"
        );
    }
}
