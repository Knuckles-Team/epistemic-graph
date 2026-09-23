use super::*;

/// Resolve an OPEN txn, build a snapshot OVERLAID with its staged write-set +
/// embeddings, and run a unified cross-modal plan over it with read-your-own-writes
/// (CONCEPT:EG-KG.query.txn-cross-modal-ryow). The overlay is built under the (brief) state read + per-txn
/// lock, then the CPU-heavy plan runs OFF-lock on the blocking pool — the same
/// off-lock idiom as `run_unified`. Not result-cached (staged writes don't bump
/// `version()`). RLS filters the committed base snapshot to the caller's visible
/// rows BEFORE the txn's own staged writes are overlaid, so the txn always reads
/// its own writes while committed data stays isolation-scoped.
#[cfg(feature = "query")]
/// The txn-resolution half of [`run_unified_overlaid`]: verify the txn exists and is
/// owned by `caller`, then snapshot its target graph's committed base (RLS-filtered)
/// plus its staged write-set/embeddings, holding only the cheap state read + per-txn
/// lock for the duration — everything returned is OWNED, so no lock is held across
/// the off-lock compute the caller runs next.
#[cfg(feature = "query")]
#[allow(clippy::type_complexity)]
pub(crate) async fn run_unified_overlaid_resolve_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    read_authority: Option<&GraphReadAuthority>,
    caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<
    (
        crate::graph::GraphView,
        Vec<crate::protocol::Method>,
        Vec<(String, Vec<f32>)>,
        Arc<GraphCore>,
        String,
    ),
    Response,
> {
    #[cfg(not(feature = "security"))]
    let _ = caller;
    let s = state.read().await;
    let Some(entry) = s.open_txns.get(txn_id) else {
        return Err(Response::err(
            req_id,
            format!("unknown transaction '{}'", txn_id),
        ));
    };
    let guard = entry.value().lock();
    let Some(expected_owner) = read_authority
        .and_then(GraphReadAuthority::carrier)
        .map(crate::server::access::CarrierAuthority::owner_scope)
    else {
        crate::metrics::access_denied();
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: transaction read requires verified owner authority",
        ));
    };
    if guard.agent != expected_owner {
        crate::metrics::access_denied();
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: transaction is not owned by caller",
        ));
    }
    let Some(g) = s.registry.get(&guard.graph) else {
        return Err(Response::err(
            req_id,
            format!("Graph '{}' not found", guard.graph),
        ));
    };
    let core = g.core.clone();
    // Committed base snapshot (O(V+E) structural copy), taken at ONE point in time
    // so the cross-modal read is snapshot-isolated.
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut view = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut view);
    Ok((
        view,
        guard.write_set.clone(),
        guard.vectors.clone(),
        core,
        guard.graph.clone(),
    ))
    // `guard` + `s` drop here — no lock held across the compute the caller runs next.
}

/// CONCEPT:EG-KG.query.txn-tsdb-read-your — the in-txn tsdb read-your-own-writes overlay of
/// [`run_unified_overlaid`]: seed a `StagedSeries` from the txn's OWN staged,
/// uncommitted `GraphTxnState.measurements` so an in-txn `Op::TsScan` sees its own
/// points (merged BEFORE the committed store), while an off-txn read (no overlay)
/// still sees committed only. `SeriesStore` is redb-file-backed with no in-memory
/// overlay, so this dep-free map is the RYOW source. Empty when the txn is gone by
/// the time this runs (best-effort, never an error).
#[cfg(all(feature = "query", feature = "tsdb"))]
pub(crate) async fn run_unified_overlaid_staged_series(
    state: &Arc<RwLock<ServerState>>,
    txn_id: &str,
) -> eg_plan::StagedSeries {
    let s = state.read().await;
    let mut staged = eg_plan::StagedSeries::new();
    let Some(entry) = s.open_txns.get(txn_id) else {
        return staged;
    };
    let guard = entry.value().lock();
    for m in &guard.measurements {
        let series = eg_tsdb::store::SeriesKey::decode(&m.series)
            .map(|key| key.series)
            .unwrap_or_else(|| m.series.clone());
        staged.push_points(&series, m.points.iter().cloned());
    }
    staged
}

#[cfg(feature = "query")]
pub(crate) async fn run_unified_overlaid<M>(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    plan: eg_plan::Plan,
    read_authority: Option<&GraphReadAuthority>,
    caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response
where
    M: MethodResult<Body = Vec<(String, Option<f32>)>, Encoding = encoding::Raw>,
    M::Encoding: EncodeRef<M::Body>,
{
    // Resolve the txn's target core + snapshot its staged write-set/embeddings while
    // holding only the cheap state read + per-txn lock; everything moved into the
    // off-lock closure is OWNED, so no lock is held across the compute.
    // `core` itself (an `Arc`) is threaded OUT of this block too — CONCEPT:EG-KG.query.served-vector-index-binding
    // / served-text-index-binding: the committed `SemanticStore`/text index are pushed
    // down via a guard taken INSIDE the off-lock closure below (not cloned here), so
    // `committed_semantic` is no longer materialized eagerly — see the closure.
    let (mut view, write_set, vectors, core, _tsdb_graph) = match run_unified_overlaid_resolve_txn(
        state,
        req_id,
        txn_id,
        read_authority,
        caller,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(resp) => return resp,
    };
    #[cfg(feature = "tsdb")]
    let tsdb_scope = match served_tsdb_scope(&plan, &_tsdb_graph, read_authority) {
        Ok(scope) => scope,
        Err(denied) => return Response::err(req_id, denied),
    };
    // RECONCILE (CONCEPT:EG-KG.query.native-time-series): the committed tsdb `SeriesStore` for `Op::TsScan`
    // fusion inside the txn, so an in-txn UQL reads COMMITTED series.
    #[cfg(feature = "tsdb")]
    let tsdb = if tsdb_scope.is_some() {
        state.read().await.tsdb_store.clone()
    } else {
        None
    };
    #[cfg(feature = "tsdb")]
    let (tsdb_tenant, tsdb_graph_scope) = match tsdb_scope {
        Some((tenant, graph)) => (Some(tenant), Some(graph)),
        None => (None, None),
    };
    // CONCEPT:EG-KG.query.closure-backed-source — the CALLER'S owner-scoped foreign
    // registry (EH-373): an in-txn `FOREIGN "<name>"` / `Named` `ForeignScan` leg
    // resolves only sources the caller (tenant+principal) registered.
    #[cfg(feature = "federation")]
    let foreign = match served_foreign_leg(state, &plan, read_authority).await {
        Ok(foreign) => foreign,
        Err(denied) => return Response::err(req_id, denied),
    };
    // CONCEPT:EG-KG.query.txn-tsdb-read-your — the in-txn tsdb read-your-own-writes overlay: seed a `StagedSeries`
    // from the txn's OWN staged, uncommitted `GraphTxnState.measurements` so an in-txn
    // `Op::TsScan` sees its own points (merged BEFORE the committed store), while an
    // off-txn read (no overlay) still sees committed only. `SeriesStore` is redb-file-
    // backed with no in-memory overlay, so this dep-free map is the RYOW source.
    #[cfg(feature = "tsdb")]
    let staged_series = run_unified_overlaid_staged_series(state, txn_id).await;
    // Overlay the txn's staged graph writes onto the RLS-filtered committed snapshot.
    overlay_write_set(&mut view, &write_set);
    // CONCEPT:EG-KG.query.overlay-leg-rls-filter — RLS on the STAGED-OVERLAY leg too. The committed base was
    // `filter_view`d above, but the staged write-set is overlaid AFTER that filter, so a
    // staged node the caller may not see (an owned+private `_owner`/`_visibility` blob)
    // would otherwise leak through an in-txn fused read. Re-filter the overlaid view so
    // BOTH the committed and the staged legs of a fused `Reason → Rank` honor per-agent
    // row visibility. A no-op when no RLS rules are registered (`has_rules()==false`
    // short-circuits `filter_view`), so the single-tenant RYOW path is byte-for-byte
    // unchanged.
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut view);
    match compute_off_lock(req_id, move || {
        run_unified_with_staged(
            plan,
            &view,
            &core,
            &vectors,
            #[cfg(feature = "federation")]
            bound_registry(&foreign),
            #[cfg(feature = "tsdb")]
            TsdbLegBind {
                tsdb: tsdb.as_deref(),
                tsdb_tenant: tsdb_tenant.as_deref(),
                tsdb_graph: tsdb_graph_scope.as_deref(),
                staged_series: Some(&staged_series),
            },
        )
    })
    .await
    {
        Ok(Ok(rows)) => result_response::<M>(req_id, &rows),
        Ok(Err(msg)) => Response::err(req_id, format!("UnifiedQuery error: {msg}")),
        Err(resp) => resp,
    }
}

/// Replay a txn's staged durable-mutation `write_set` onto a cloned `GraphView` as an
/// overlay (CONCEPT:EG-KG.query.txn-cross-modal-ryow — in-txn cross-modal RYOW). Mirrors `handlers::txn::
/// apply_staged`, but against a view's overlay ops (no ledger/durability): so the
/// Filter (node props) and Traverse (BFS over staged edges) legs of an in-txn unified
/// query observe the txn's own uncommitted graph writes. Only the durable-mutation set
/// is ever staged (the protocol restricts `Txn*` to it); any other variant is a no-op.
#[cfg(feature = "query")]
pub(crate) fn overlay_write_set(view: &mut crate::graph::GraphView, write_set: &[Method]) {
    for m in write_set {
        match m {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => view.overlay_add_node(node_id.clone(), properties_msgpack.clone()),
            Method::RemoveNode { node_id } => view.overlay_remove_node(node_id),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => {
                view.overlay_add_edge(
                    source_id.clone(),
                    target_id.clone(),
                    properties_msgpack.clone(),
                );
            }
            Method::RemoveEdge {
                source_id,
                target_id,
            } => view.overlay_remove_edge(source_id, target_id),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => {
                // A decode failure is a no-op overlay (mirrors `apply_staged`).
                if let (Ok(conditions), Ok(updates)) = (
                    eg_types::msgpack::decode_property_object(conditions_msgpack),
                    eg_types::msgpack::decode_property_object(updates_msgpack),
                ) {
                    view.overlay_compare_and_set_fields(node_id, &conditions, &updates);
                }
            }
            _ => {}
        }
    }
}

/// Run `plan` over `view` with `core`'s maintained indexes bound (and the
/// caller's tenant-scoped foreign registry, EH-373, when bound), against `core`'s
/// semantic store with `staged` embeddings overlaid (read-your-own-writes). No
/// staged embedding ⇒ the COMMITTED store is searched through a guard -- no
/// clone, no forced HNSW rebuild (CONCEPT:EG-KG.query.served-vector-index-
/// binding); only a txn that actually staged embeddings pays for the
/// `semantic_overlay` copy. Off-txn callers pass no staged embeddings.
#[cfg(feature = "query")]
pub(crate) fn run_unified_with_staged(
    plan: eg_plan::Plan,
    view: &crate::graph::GraphView,
    core: &Arc<GraphCore>,
    staged: &[(String, Vec<f32>)],
    #[cfg(feature = "federation")] foreign: Option<&eg_plan::federation::ForeignSourceRegistry>,
    #[cfg(feature = "tsdb")] tsdb_ctx: TsdbLegBind<'_>,
) -> Result<Vec<(String, Option<f32>)>, String> {
    let indexes = CoreIndexes::open(core);
    let served = indexes.served(
        #[cfg(feature = "federation")]
        foreign,
    );
    if staged.is_empty() {
        let committed = core.semantic_store.read();
        return run_unified(
            plan,
            view,
            &committed,
            served,
            #[cfg(feature = "tsdb")]
            tsdb_ctx,
        );
    }
    let committed = core.semantic_store.read().clone();
    let semantic = eg_core::compute::semantic::semantic_overlay(committed, staged);
    run_unified(
        plan,
        view,
        &semantic,
        served,
        #[cfg(feature = "tsdb")]
        tsdb_ctx,
    )
}
