use super::*;

// ─────────────────────────── Row builders (shared) ───────────────────────────

/// Resolve the cluster feature rows: explicit `features` win; then a fused
/// upstream `plan` (CONCEPT:EG-KG.mining.fused-plan-source); then the `source`
/// node-label embedding scan (the cross-modal hook). Returns the rows AND a
/// parallel `ids` vec (node ids for the embedding/plan path, empty for explicit).
/// `Err` only ever originates from the plan leg (CONCEPT:EG-KG.mining.tsdb-typed-absent).
pub(super) fn build_vectors(
    core: &Arc<GraphCore>,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    if !features.is_empty() {
        return Ok((features.to_vec(), Vec::new()));
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(
            core,
            p,
            #[cfg(feature = "tsdb")]
            tsdb,
        );
    }
    match source {
        Some(spec) => Ok(gather_embeddings(core, spec)),
        None => Ok((Vec::new(), Vec::new())),
    }
}

/// Resolve the anomaly rows: explicit `features` win, then a 1-D `values` series
/// (each scalar → a one-element row — the tsdb RCA path), then a fused upstream
/// `plan`, then node embeddings. `Err` only ever originates from the plan leg
/// (CONCEPT:EG-KG.mining.tsdb-typed-absent).
pub(super) fn build_anomaly_rows(
    core: &Arc<GraphCore>,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
    #[cfg(all(feature = "query", feature = "tsdb"))] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    if !features.is_empty() {
        return Ok((features.to_vec(), Vec::new()));
    }
    if !values.is_empty() {
        return Ok((values.iter().map(|&v| vec![v]).collect(), Vec::new()));
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows(
            core,
            p,
            #[cfg(feature = "tsdb")]
            tsdb,
        );
    }
    match source {
        Some(spec) => Ok(gather_embeddings(core, spec)),
        None => Ok((Vec::new(), Vec::new())),
    }
}

/// WAL-replay counterpart of [`build_vectors`] (CONCEPT:EG-KG.mining.frequent-itemset-mining, L34). `replay`
/// (crash recovery) runs off a bare `&GraphCore` — no live `Arc` in hand, unlike the served
/// `Mine*` handlers `try_handle` dispatches with one — so it cannot construct a
/// `ServedTextIndex` for a plan-sourced `RankText`/`FuseRrf` leg; that pushdown is a served-
/// path optimization (L34 mirrors EG-P1-4's `run_unified` served call sites), not a crash-
/// recovery requirement, and re-deriving `wal.rs`'s `apply`/`replay` call chain to carry an
/// `Arc<GraphCore>` instead would be an unrelated, much larger refactor. This keeps the
/// pre-L34 snapshot-derived text-index behavior for replay's plan leg (a documented, narrow
/// scope cut); its explicit-features and embedding-label-scan legs are identical either way.
pub(super) fn build_vectors_replay(
    core: &GraphCore,
    features: &[Vec<f64>],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows_snapshot(core, p);
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// WAL-replay counterpart of [`build_anomaly_rows`] — see [`build_vectors_replay`]'s docs
/// for why replay keeps the snapshot-derived (non-`Arc`) plan leg.
pub(super) fn build_anomaly_rows_replay(
    core: &GraphCore,
    features: &[Vec<f64>],
    values: &[f64],
    source: &Option<VectorSource>,
    #[cfg(feature = "query")] plan: &Option<crate::wire::Plan>,
) -> (Vec<Vec<f64>>, Vec<String>) {
    if !features.is_empty() {
        return (features.to_vec(), Vec::new());
    }
    if !values.is_empty() {
        return (values.iter().map(|&v| vec![v]).collect(), Vec::new());
    }
    #[cfg(feature = "query")]
    if let Some(p) = plan {
        return gather_plan_rows_snapshot(core, p);
    }
    match source {
        Some(spec) => gather_embeddings(core, spec),
        None => (Vec::new(), Vec::new()),
    }
}

/// Gather the stored embedding of every node carrying `spec.node_label` (skipping
/// nodes without one). Compute-near-data: the vectors are read straight off the
/// resident semantic store.
pub(super) fn gather_embeddings(
    core: &GraphCore,
    spec: &VectorSource,
) -> (Vec<Vec<f64>>, Vec<String>) {
    let owners = core.get_nodes_by_label(&spec.node_label, spec.limit);
    let store = core.semantic_store.read();
    let mut rows: Vec<Vec<f64>> = Vec::with_capacity(owners.len());
    let mut ids: Vec<String> = Vec::with_capacity(owners.len());
    for (node_id, _blob) in owners {
        if let Some(vec) = store.get_embedding(&node_id) {
            rows.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (rows, ids)
}

/// Bundles what a plan-sourced mining leg (CONCEPT:EG-KG.mining.fused-plan-source) needs to
/// resolve and bind the server's live tsdb store for an `Op::TsScan` leg, mirroring
/// `query::run_unified`'s own `TsdbLegBind` — the difference is WHERE the scope gets
/// resolved: a served `UnifiedQuery` resolves it once per request at the top of its own
/// handler, while mining resolves it once per plan inside [`gather_plan_rows`], since
/// FIVE different `Mine*` handlers share that one call site (resolving there, rather
/// than duplicating the "does this plan need tsdb" check in every handler, keeps a
/// single source of truth). `read_authority`/`tsdb_store` are `None` when no live server
/// wiring is available (the `#[cfg(test)]` `dispatch_for_test` harness); a `TsScan`-bearing
/// plan is then a typed error rather than the old silent-empty degrade
/// (CONCEPT:EG-KG.mining.tsdb-typed-absent) — a plan that never touches tsdb is unaffected.
#[cfg(all(feature = "query", feature = "tsdb"))]
#[derive(Clone, Copy)]
pub(crate) struct MiningTsdbBind<'a> {
    pub graph_name: &'a str,
    pub read_authority: Option<&'a crate::server::access::GraphReadAuthority>,
    pub tsdb_store: Option<&'a Arc<eg_tsdb::store::SeriesStore>>,
}

/// Run an upstream cross-modal RETRIEVAL `plan` (`Op::Scan|Filter|Traverse|Rank|…`)
/// over a fresh graph+semantic snapshot and resolve each resulting row's id to its
/// stored embedding — the SAME lookup [`gather_embeddings`] uses for a bare
/// `VectorSource` label scan, generalized to an ARBITRARY upstream plan
/// (CONCEPT:EG-KG.mining.fused-plan-source). This is the fused `retrieve → mine →
/// writeback` mechanism: the retrieval legs (vector rank / graph traverse / SQL
/// filter / OWL reason / …) run FIRST, compute-near-data, over the SAME snapshot
/// the mining op then reads embeddings from — ONE round-trip, no client
/// marshalling between "retrieve the candidate set" and "mine it". A plan
/// execution error degrades to an empty row set (never panics/propagates) —
/// consistent with every other mining source's "no match ⇒ empty" contract —
/// EXCEPT for a plan-sourced `Op::TsScan` leg (CONCEPT:EG-KG.mining.tsdb-typed-absent):
/// that ONE case now returns a typed `Err` when the server genuinely has no live
/// tsdb store (or no verified carrier authority) bound, rather than silently
/// degrading to empty — a `TsScan`-bearing mining plan is otherwise
/// indistinguishable from "your query legitimately matched nothing".
#[cfg(feature = "query")]
pub(super) fn gather_plan_rows(
    core: &Arc<GraphCore>,
    plan: &crate::wire::Plan,
    #[cfg(feature = "tsdb")] tsdb: MiningTsdbBind<'_>,
) -> Result<(Vec<Vec<f64>>, Vec<String>), String> {
    let snap = core.analysis_snapshot();
    // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
    // vector leg into the LIVE persistent `SemanticStore` via a guard, reused for the embedding
    // lookups below too, instead of a `.clone()` that (on the default HNSW backend) would have
    // forced a full rebuild on its first search. L34: `gather_plan_rows` now takes an
    // `Arc<GraphCore>` (mirroring EG-P1-4's served `run_unified` call sites), so a `RankText`/
    // `FuseRrf` leg in a mining-sourced plan ALSO pushes down into the graph's MAINTAINED
    // persistent `GraphTextIndex` via `ServedTextIndex`, instead of falling back to a
    // snapshot-derived index rebuilt from `snap` on every mining request.
    #[cfg(feature = "text")]
    let served_text = crate::server::secondary_indexes::ServedTextIndex::new(core.clone());
    // L37: an `Arc<GraphCore>` in hand ⇒ a `SpatialScan` leg in a mining-sourced plan ALSO
    // pushes down into the graph's MAINTAINED persistent spatial index, same as the text leg.
    #[cfg(feature = "geo")]
    let served_spatial = crate::server::secondary_indexes::ServedSpatialIndex::new(core.clone());
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — resolve the SAME verified tenant/namespace scope
    // the served `UnifiedQuery` path resolves (`query::served_tsdb_scope`, single source of
    // truth), THEN require the live store to actually be bound before falling through to the
    // old silent-empty degrade. `None` (the plan has no `TsScan` leg) is unaffected.
    #[cfg(feature = "tsdb")]
    let tsdb_scope = crate::server::handlers::query::served_tsdb_scope(
        plan,
        tsdb.graph_name,
        tsdb.read_authority,
    )?;
    #[cfg(feature = "tsdb")]
    if tsdb_scope.is_some() && tsdb.tsdb_store.is_none() {
        return Err(
            "graph_mine: plan requires Op::TsScan but this server has no time-series store \
             configured"
                .to_string(),
        );
    }
    let store = core.semantic_store.read();
    let rows = match crate::server::handlers::query::run_unified(
        plan.clone(),
        &snap,
        &store,
        crate::server::handlers::query::ServedIndexes {
            #[cfg(feature = "text")]
            text: Some(&served_text),
            #[cfg(feature = "geo")]
            spatial: Some(&served_spatial),
            // CONCEPT:EG-KG.query.closure-backed-source — `gather_plan_rows` runs off a bare
            // `Arc<GraphCore>` (the WAL-replay-compatible signature), with NO `ServerState`
            // in hand, so the registered foreign sources cannot be threaded here without
            // widening `build_vectors`/`build_anomaly_rows` too. A mining-sourced plan with
            // a NAMED foreign leg therefore still returns the clean "no registry attached"
            // typed error rather than silently-local rows — the served `UnifiedQuery`
            // /`UnifiedQueryText`/NL/in-txn/wire paths all bind it. Threading it into the
            // mining source is a follow-up.
            #[cfg(feature = "federation")]
            foreign: None,
            #[cfg(not(any(feature = "text", feature = "geo")))]
            _marker: std::marker::PhantomData,
        },
        #[cfg(feature = "tsdb")]
        match &tsdb_scope {
            Some((tenant, graph)) => crate::server::handlers::query::TsdbLegBind {
                tsdb: tsdb.tsdb_store.map(|store| store.as_ref()),
                tsdb_tenant: Some(tenant.as_str()),
                tsdb_graph: Some(graph.as_str()),
                staged_series: None,
            },
            None => crate::server::handlers::query::TsdbLegBind {
                tsdb: None,
                tsdb_tenant: None,
                tsdb_graph: None,
                staged_series: None,
            },
        },
    ) {
        Ok(rows) => rows,
        Err(_) => return Ok((Vec::new(), Vec::new())),
    };
    let mut feats: Vec<Vec<f64>> = Vec::with_capacity(rows.len());
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    for (row_id, score) in rows {
        if let Some(vec) = store.get_embedding(&row_id) {
            feats.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(row_id);
            continue;
        }
        // CONCEPT:EG-KG.mining.tsdb-typed-absent — a time-series SOURCE row (an `Op::TsScan`
        // point): the id IS the point timestamp — not a graph node, so it never resolves to
        // an embedding — and `score` IS the value. Mirrors EXACTLY how `window_aggregate`
        // (CONCEPT:EG-KG.compute.tsscan-series-window-60s, `crates/eg-plan/src/exec.rs`)
        // already disambiguates a TsScan-produced row from a graph-node row. Without this
        // fallback a TsScan-sourced mining plan would still yield zero feature rows even
        // after binding the live tsdb store above: the row would round-trip through
        // `run_unified` correctly, then be silently dropped HERE by the embedding lookup —
        // the exact same "a TsScan row's id is not a node, so it's dropped" failure mode
        // `window_aggregate`'s own fix note describes. A row id that merely LOOKS numeric
        // but isn't a real timestamp is indistinguishable from a genuine one at this layer,
        // exactly as `window_aggregate` accepts the same ambiguity.
        if let (Ok(_ts), Some(value)) = (row_id.parse::<i64>(), score) {
            feats.push(vec![value as f64]);
            ids.push(row_id);
        }
    }
    Ok((feats, ids))
}

/// WAL-replay counterpart of [`gather_plan_rows`] — the pre-L34 behavior, kept for
/// [`build_vectors_replay`]/[`build_anomaly_rows_replay`] (see their docs): no `Arc` in
/// hand, so no `ServedTextIndex` — a `RankText`/`FuseRrf` leg in a replay-sourced plan falls
/// back to the snapshot-derived text index built fresh from `snap`, exactly as every
/// mining plan leg behaved before the served-path pushdown.
#[cfg(feature = "query")]
pub(super) fn gather_plan_rows_snapshot(
    core: &GraphCore,
    plan: &crate::wire::Plan,
) -> (Vec<Vec<f64>>, Vec<String>) {
    let snap = core.analysis_snapshot();
    let store = core.semantic_store.read();
    let rows = match crate::server::handlers::query::run_unified(
        plan.clone(),
        &snap,
        &store,
        crate::server::handlers::query::ServedIndexes::default(),
        #[cfg(feature = "tsdb")]
        crate::server::handlers::query::TsdbLegBind {
            tsdb: None,
            tsdb_tenant: None,
            tsdb_graph: None,
            staged_series: None,
        },
    ) {
        Ok(rows) => rows,
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let mut feats: Vec<Vec<f64>> = Vec::with_capacity(rows.len());
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    for (node_id, _score) in rows {
        if let Some(vec) = store.get_embedding(&node_id) {
            feats.push(vec.into_iter().map(|f| f as f64).collect());
            ids.push(node_id);
        }
    }
    (feats, ids)
}

/// Reject a ragged feature matrix (rows of differing width) with a clean error
/// rather than letting a distance computation panic; an empty matrix is allowed
/// (⇒ an empty result).
pub(super) fn validate_matrix(rows: &[Vec<f64>]) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }
    let width = rows[0].len();
    if width == 0 {
        return Err("mining: feature rows must be non-empty".into());
    }
    if rows.iter().any(|r| r.len() != width) {
        return Err("mining: all feature rows must have the same dimensionality".into());
    }
    Ok(())
}
