use super::*;

#[cfg(feature = "cypher")]
/// Cypher WRITE surface (CONCEPT:EG-KG.query.register-each-user-table/EG-023) — the
/// `CypherMode::Write` arm of [`handle_cypher_query`]: a `CREATE`/`MERGE`/`SET`/
/// `DELETE`/`REMOVE` statement applied to the LIVE `GraphCore` via
/// `exec_cypher_write` (native eg-core write ops — NO DataFusion; it calls
/// `mark_dirty` once after the mutation). NOT cached, NOT RLS pre-filtered
/// (writes are graph-ACL-gated upstream — this method classified Write).
pub(crate) async fn handle_cypher_write(
    req_id: u64,
    core: Arc<GraphCore>,
    query: String,
) -> Response {
    let core_w = core.clone();
    match compute_off_lock(req_id, move || eg_query::exec_cypher_write(&core_w, &query)).await {
        Ok(Ok(result)) => dynamic_response::<query_results::CypherQuery, _>(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
        Err(resp) => resp,
    }
}

#[cfg(feature = "cypher")]
pub(crate) async fn handle_cypher_query(
    ctx: &QueryHandlerCtx<'_>,
    query: String,
    mode: crate::protocol::CypherMode,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let validation_method = Method::CypherQuery {
        query: query.clone(),
        mode,
    };
    if let Err(error) = validate_cypher_mode(&validation_method) {
        return Ok(Response::err(req_id, error));
    }
    if matches!(mode, crate::protocol::CypherMode::Write) {
        return Ok(handle_cypher_write(req_id, ctx.core.clone(), query).await);
    }
    Ok(handle_cypher_read(ctx, query).await)
}

#[cfg(feature = "cypher")]
async fn handle_cypher_read(ctx: &QueryHandlerCtx<'_>, query: String) -> Response {
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    // Same off-lock snapshot + blocking-pool idiom as SQL — but DEP-FREE
    // (label index / VF2 / BFS), so it runs in a no-DataFusion Pi build.
    // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231) wraps
    // it identically; this is the lean-Pi cached query path. The cache KEY folds
    // in the caller's RLS context so agent A's filtered rows are never served to
    // agent B, and the snapshot is RLS-filtered before execution.
    #[cfg(feature = "result-cache")]
    let (snap, version, hash) = {
        let hash = rls_cache_hash(
            "cypher",
            query.as_bytes(),
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        );
        // BUG-267: `analysis_snapshot()` was materialized unconditionally
        // BEFORE this cache lookup — an O(V+E) clone of the entire
        // node_map/node_properties/edge_properties paid on every call
        // regardless of cache hit or miss, defeating the cache that
        // follows it (measured p99 2.49s on plain cached reads). Probe
        // with the cheap `core.version()` atomic load first; only a MISS
        // pays for `analysis_snapshot_versioned()`. The fresh `version`
        // it returns (not the one used for this probe) is what `put`
        // stores under below, so an entry can never claim a version
        // newer than the data it reflects.
        //
        // BUG-267 follow-up (regression caught by
        // `result_cache_dispatch_tests::hit_on_unchanged_then_write_invalidates`
        // and `rls_aware_cache_no_cross_agent_leak::
        // agent_a_cached_result_is_not_served_to_agent_b`, both of which assert
        // an EXACT miss delta of 1 per served request): an earlier version of
        // this fix re-checked the cache a SECOND time after the snapshot too, to
        // guard a concurrent-populate race. `ResultCache::get` bumps the
        // hit/miss counters on EVERY call — including a probe that finds
        // nothing — so that second check silently double-counted every miss.
        // There is only ONE counted lookup on this path now; a rare concurrent
        // populate between the probe and the snapshot just costs a redundant
        // recompute (the `put` below still lands under this call's own fresh
        // `version`, so nothing stale or cross-actor is ever served).
        if let Some(bytes) = core.result_cache().get(hash, core.version()) {
            return Ok(Response::ok(
                req_id,
                ResultPayload::of_encoded::<query_results::CypherQuery>(bytes),
            ));
        }
        // perf/cold-query-floor-analysis (UNCOMPILED PROPOSAL — see
        // `crate::rls_view_cache` in eg-core, not yet exercised by any test or
        // build): a whole-RESULT-cache MISS used to unconditionally pay for
        // `analysis_snapshot_versioned()` (an O(V+E) clone of every node/edge
        // property blob's Arc handle) PLUS `rls.filter_view` — which, per node
        // in the ENTIRE snapshot (not just the rows this query's WHERE clause
        // ultimately matches), fully msgpack-decodes that node's property blob
        // (`IsolationLayer::can_see_node` -> `row_visibility`) just to read 2-3
        // small RLS metadata keys. That is architecturally the SAME bug
        // `build_cypher_label_index` had before `perf/warm-label-index`
        // (`2662713b`) fixed it for the label index, except unmemoized: this
        // filtered view is a pure function of (graph content, actor's ACL
        // grants) at one `version()`, so a same-(actor, version) repeat pays the
        // full O(V) decode again on every distinct query text (a whole-result
        // cache miss), which is exactly the reported ~900ms fixed floor —
        // measured live via the structurally identical `project_core` cache's
        // own `epistemic_graph_projection_cache_miss_build_seconds` (298
        // misses, mean 1.10s/miss). Probe the per-actor filtered-view cache
        // FIRST; only a genuine cold (actor, version) pair pays for the
        // snapshot + filter, exactly mirroring the `result_cache` probe just
        // above (BUG-267) and `GraphReadAuthority::project_core`'s existing
        // cache-then-build shape (`src/server/access.rs`). RLS safety: keyed by
        // (actor, version) exactly like `project_core`'s cache — never a
        // cross-actor share — and invalidated on every whole-image transition
        // `project_core`'s cache is (`GraphCore::invalidate_filtered_view_cache`,
        // called alongside `invalidate_projection_cache` at both its sites).
        // `version` MUST be the exact version the returned `snap` reflects (the
        // same BUG-267 invariant the `result_cache.put` below relies on: an
        // entry can never claim a version newer than the data it reflects) —
        // NOT a fresh `core.version()` re-read after the cache probe, which
        // could have raced a concurrent commit. The hit branch reuses
        // `probe_version` (what `cached_filtered_view` matched against); the
        // miss branch reuses `built_version` (what `analysis_snapshot_versioned`
        // itself captured), exactly as the pre-existing code did.
        #[cfg(feature = "security")]
        let (snap, version): (Arc<crate::graph::GraphView>, u64) =
            versioned_rls_snapshot(&core, caller, rls);
        #[cfg(not(feature = "security"))]
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, version, hash)
    };
    #[cfg(not(feature = "result-cache"))]
    let snap = rls_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    // `version`-paired snapshot (`result-cache` build only, see above): hand
    // `exec_cypher_params_indexed` an `IndexSource` so an unlabeled `MATCH (n)
    // WHERE n.id = …`/`IN […]` — the fleet/tool-registration slow-query shape —
    // can narrow through the bounded property index instead of a whole-graph
    // scan (CONCEPT:EG-KG.storage.index-manager-seam). The index answers off `core`'s LIVE
    // state, so `exec_cypher_params_indexed` brackets it against `core.version()`
    // and discards the answer (full-scan fallback) if a concurrent commit raced
    // this snapshot; the SERVED result is identical to plain `exec_cypher` either
    // way — only the work to reach it differs.
    #[cfg(feature = "result-cache")]
    let core_for_index = core.clone();
    #[cfg(feature = "result-cache")]
    let resp = match compute_off_lock(req_id, move || {
        eg_query::exec_cypher_params_indexed(
            &snap,
            &query,
            &eg_query::Params::new(),
            eg_query::IndexSource::new(&core_for_index, version),
        )
    })
    .await
    {
        Ok(Ok(result)) => match ResultPayload::of_dynamic::<query_results::CypherQuery, _>(&result)
        {
            Ok(payload) => {
                eg_core::result_cache::cache_result(core.result_cache(), hash, version, &payload);
                Response::ok(req_id, payload)
            }
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
        Err(resp) => resp,
    };
    #[cfg(not(feature = "result-cache"))]
    let resp = match compute_off_lock(req_id, move || eg_query::exec_cypher(&snap, &query)).await {
        Ok(Ok(result)) => dynamic_response::<query_results::CypherQuery, _>(req_id, &result),
        Ok(Err(msg)) => Response::err(req_id, format!("Cypher error: {msg}")),
        Err(resp) => resp,
    };
    resp
}
