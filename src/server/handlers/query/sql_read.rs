use super::*;

#[cfg(feature = "query")]
#[derive(Clone, Copy)]
struct SqlReadScope<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    core: &'a Arc<GraphCore>,
    caller: &'a str,
    authority: &'a crate::server::access::CarrierAuthority,
    persist_dir: &'a std::path::Path,
    #[cfg(feature = "security")]
    rls: &'a Arc<crate::isolation::IsolationLayer>,
}

#[cfg(feature = "query")]
fn required_sql_authority(
    req_id: u64,
    read_authority: Option<&GraphReadAuthority>,
) -> Result<
    (
        &GraphReadAuthority,
        &crate::server::access::CarrierAuthority,
    ),
    Response,
> {
    let Some(read_authority) = read_authority else {
        crate::metrics::access_denied();
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: current signed tenant authority is required".to_string(),
        ));
    };
    let Some(authority) = read_authority.carrier() else {
        crate::metrics::access_denied();
        return Err(Response::err(
            req_id,
            "ACCESS_DENIED: current signed tenant authority is required".to_string(),
        ));
    };
    Ok((read_authority, authority))
}

#[cfg(feature = "query")]
async fn sql_persist_dir(state: &Arc<RwLock<ServerState>>) -> Result<std::path::PathBuf, String> {
    state
        .read()
        .await
        .persist_dir
        .clone()
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            "SQL error: tenant SQL catalog requires the configured persistence directory"
                .to_string()
        })
}

#[cfg(feature = "query")]
fn sql_tenant_store(
    authority: &crate::server::access::CarrierAuthority,
    persist_dir: &std::path::Path,
    req_id: u64,
) -> Result<eg_query::TableStore, Response> {
    crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)
        .map_err(|error| Response::err(req_id, format!("SQL error: {error}")))
}

#[cfg(feature = "query")]
fn sql_read_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> (Arc<crate::graph::GraphView>, u64) {
    #[cfg(feature = "result-cache")]
    {
        #[cfg(feature = "security")]
        let (snap, version) = versioned_rls_snapshot(core, caller, rls);
        #[cfg(not(feature = "security"))]
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, version)
    }
    #[cfg(not(feature = "result-cache"))]
    {
        let snap = rls_snapshot(
            core,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        );
        let version = core.version();
        (snap, version)
    }
}

#[cfg(feature = "query")]
async fn handle_sql_read(scope: SqlReadScope<'_>, query: String) -> Response {
    let SqlReadScope {
        state: _,
        req_id,
        core,
        caller,
        authority,
        persist_dir,
        #[cfg(feature = "security")]
        rls,
    } = scope;
    let (snap, _graph_version) = sql_read_snapshot(
        core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    let cancel = eg_query::CancellationToken::new();
    let _cancel_guard = crate::server::request_cancel::register(req_id, cancel.clone());
    let timeout_task = crate::server::request_cancel::spawn_timeout(cancel.clone());
    let cancel_for_task = cancel.clone();
    let authority = authority.clone();
    let persist_dir = persist_dir.to_path_buf();
    let resp = match compute_off_lock(req_id, move || {
        let authorized =
            crate::server::sql_catalog_acl::authorized_read_store(&authority, &persist_dir)?;
        eg_query::exec_sql_typed_with_tables_cancellable(
            &snap,
            authorized.store(),
            &query,
            &cancel_for_task,
        )
    })
    .await
    {
        Ok(Ok(typed)) => typed_sql_response(req_id, typed),
        Ok(Err(message)) => Response::err(req_id, format!("SQL error: {message}")),
        Err(response) => response,
    };
    if let Some(task) = timeout_task {
        task.abort();
    }
    resp
}

#[cfg(feature = "query")]
pub(crate) async fn handle_sql(
    ctx: &QueryHandlerCtx<'_>,
    query: String,
    params_msgpack: Vec<u8>,
) -> Result<Response, Method> {
    bind_sql_text_embedder();
    let req_id = ctx.req_id;
    let (read_authority, authority) = match required_sql_authority(req_id, ctx.read_authority) {
        Ok(authority) => authority,
        Err(response) => return Ok(response),
    };
    let persist_dir = match sql_persist_dir(ctx.state).await {
        Ok(persist_dir) => persist_dir,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let persist_path = persist_dir.as_path();
    let store = match sql_tenant_store(authority, persist_path, req_id) {
        Ok(store) => store,
        Err(response) => return Ok(response),
    };
    let sql_method = Method::Sql {
        query: query.clone(),
        params_msgpack,
    };
    match eg_query::classify(&query) {
        Ok(kind) if !matches!(kind, eg_query::StatementKind::Read) => Ok(exec_sql_write(
            req_id,
            SqlWriteScope {
                graph_name: ctx.graph_name,
                tenant_scope: authority.tenant_scope(),
                caller: Some(authority.actor_scope()),
                authority,
                persist_dir: persist_path,
            },
            read_authority,
            sql_method,
            ctx.core,
            &store,
            kind,
        )
        .await),
        _ => Ok(handle_sql_read(
            SqlReadScope {
                state: ctx.state,
                req_id,
                core: ctx.core,
                caller: ctx.caller,
                authority,
                persist_dir: persist_path,
                #[cfg(feature = "security")]
                rls: ctx.rls,
            },
            query,
        )
        .await),
    }
}

#[cfg(feature = "query")]
fn unified_response<M>(
    req_id: u64,
    result: UnifiedRunOutcome,
    #[cfg(feature = "result-cache")] core: &Arc<GraphCore>,
    #[cfg(feature = "result-cache")] dep: &Option<eg_core::dep_scope::DepSet>,
    #[cfg(feature = "result-cache")] version: u64,
    #[cfg(feature = "result-cache")] hash: u128,
) -> Response
where
    M: MethodResult<Body = Vec<(String, Option<f32>)>, Encoding = encoding::Raw>,
    M::Encoding: EncodeRef<M::Body>,
{
    match result {
        Ok(Ok(rows)) => match ResultPayload::of_ref::<M>(&rows) {
            Ok(payload) => {
                #[cfg(feature = "result-cache")]
                match dep {
                    Some(deps) => eg_core::result_cache::cache_dep_result(
                        core.result_cache(),
                        hash,
                        0,
                        version,
                        deps.clone(),
                        &payload,
                    ),
                    None => eg_core::result_cache::cache_result(
                        core.result_cache(),
                        hash,
                        version,
                        &payload,
                    ),
                }
                Response::ok(req_id, payload)
            }
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(message)) => Response::err(req_id, format!("UnifiedQuery error: {message}")),
        Err(response) => response,
    }
}

#[cfg(feature = "query")]
pub(crate) async fn handle_unified_query(
    ctx: &QueryHandlerCtx<'_>,
    plan: eg_plan::Plan,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "tsdb")]
    let tsdb_scope = match served_tsdb_scope(&plan, ctx.graph_name, ctx.read_authority) {
        Ok(scope) => scope,
        Err(denied) => return Ok(Response::err(req_id, denied)),
    };
    // ONE cross-modal plan (CONCEPT:AU-KG.compute.vector/209): filter (DataFusion) →
    // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot. Take
    // BOTH the GraphView (topology + property blobs) and a SemanticStore clone
    // under a brief read each — same point-in-time, so the cross-modal read is
    // snapshot-isolated — then run the whole pipeline on the blocking pool.
    // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
    // on the plan bytes + the caller's RLS context. The plan + semantic store
    // both reflect `version`, so a write retires the entry; the
    // RLS-context salt keeps agent A's fused result out of agent B's lookups.
    // Dependency-scoped invalidation (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
    // W1.6/P7): a plan reducible to a bounded node read (Scan/Filter/Limit) is cached in
    // the dependency-scoped namespace, so it survives every write DISJOINT from its
    // labels; any other plan shape keeps the coarse version-keyed path unchanged.
    #[cfg(feature = "result-cache")]
    let dep = plan_dependency_set(&plan);
    #[cfg(feature = "result-cache")]
    let (snap, version, hash) = {
        let mut payload = match msgpack_bytes(&plan) {
            Ok(payload) => payload,
            Err(error) => return Ok(Response::err(req_id, error)),
        };
        #[cfg(feature = "tsdb")]
        if let Some((tenant, graph)) = tsdb_scope.as_ref() {
            payload.extend_from_slice(tenant.as_bytes());
            payload.extend_from_slice(graph.as_bytes());
        }
        let hash = rls_cache_hash(
            "unified",
            &payload,
            #[cfg(feature = "security")]
            ctx.caller,
            #[cfg(feature = "security")]
            rls,
        );
        // BUG-267: probe the cache BEFORE paying for the O(V+E)
        // `analysis_snapshot_versioned()` clone. `version()` is a bare
        // atomic load and `dep_clock()` a cheap ref, so a HIT never
        // materializes the graph at all. This is the ONLY counted
        // cache lookup on this request's path (BUG-267 follow-up: an
        // earlier version of this fix re-checked the cache a SECOND
        // time after the snapshot, on the theory that a concurrent
        // request might have populated it in between -- but
        // `ResultCache::get`/`get_dep` bump the hit/miss counters on
        // EVERY call, so that second check double-counted every miss
        // and broke `result_cache_dispatch_tests::
        // hit_on_unchanged_then_write_invalidates` +
        // `rls_aware_cache_no_cross_agent_leak::
        // agent_a_cached_result_is_not_served_to_agent_b`, both of
        // which assert an EXACT miss delta of 1 per served request.
        // A miss here just proceeds to compute+cache below; the rare
        // concurrent-populate race is a redundant recompute, not a
        // correctness issue -- `put`/`put_dep` after the compute
        // still stores under the FRESH version/deps this call's own
        // snapshot reflects, so a stale or cross-actor entry can
        // never result).
        let probe = match &dep {
            Some(_) => core.result_cache().get_dep(hash, 0, core.dep_clock()),
            None => core.result_cache().get(hash, core.version()),
        };
        if let Some(bytes) = probe {
            return Ok(Response::ok(
                req_id,
                ResultPayload::of_cache_hit::<query_results::UnifiedQuery>(bytes),
            ));
        }
        // perf/row-visibility-index (B-sweep): a result-cache MISS still
        // used to unconditionally pay for `filter_view`'s full per-node RLS
        // decode — mirror `Method::CypherQuery`'s per-(actor,version)
        // `FilteredViewCache` probe-then-build here too.
        #[cfg(feature = "security")]
        let (snap, version) = versioned_rls_snapshot(&core, ctx.caller, rls);
        #[cfg(not(feature = "security"))]
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, version, hash)
    };
    #[cfg(not(feature = "result-cache"))]
    let snap = rls_snapshot(
        &core,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        rls,
    );
    // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
    // vector kNN AND lexical legs down into the LIVE persistent indexes instead
    // of cloning/rebuilding them per request, via the shared off-lock runner.
    let result = run_unified_off_lock(
        state,
        req_id,
        &core,
        snap,
        plan,
        #[cfg(feature = "tsdb")]
        tsdb_scope,
    )
    .await;
    let resp = unified_response::<query_results::UnifiedQuery>(
        req_id,
        result,
        #[cfg(feature = "result-cache")]
        &core,
        #[cfg(feature = "result-cache")]
        &dep,
        #[cfg(feature = "result-cache")]
        version,
        #[cfg(feature = "result-cache")]
        hash,
    );
    Ok(resp)
}

#[cfg(feature = "query")]
pub(crate) async fn handle_unified_query_text(
    ctx: &QueryHandlerCtx<'_>,
    text: String,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let core = ctx.core.clone();
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    let plan = match eg_plan::uql::parse(&text) {
        Ok(plan) => plan,
        Err(e) => return Ok(Response::err(req_id, e.render(&text))),
    };
    #[cfg(feature = "tsdb")]
    let tsdb_scope = match served_tsdb_scope(&plan, ctx.graph_name, ctx.read_authority) {
        Ok(scope) => scope,
        Err(denied) => return Ok(Response::err(req_id, denied)),
    };
    // UQL (CONCEPT:AU-KG.query.top-nodes-by-degree): parse the TEXT query into the SAME `wire::Plan`
    // `UnifiedQuery` carries, then run the IDENTICAL `run_unified` executor —
    // a pure front-end, no new execution path. A parse error is a clear,
    // caret-annotated error Response (never a panic).
    // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): key
    // on the text + the caller's RLS context (the parse is deterministic, so
    // caching pre-parse is sound and skips the parse on a hit too). The
    // RLS-context salt keeps agent A's result out of agent B's lookups.
    // Dependency-scoped invalidation (W1.6/P7): identical to the `UnifiedQuery` arm — a
    // Scan/Filter/Limit plan is cached in the dependency-scoped namespace (survives
    // disjoint writes); any other shape keeps the version-keyed path.
    #[cfg(feature = "result-cache")]
    let dep = plan_dependency_set(&plan);
    #[cfg(feature = "result-cache")]
    let (snap, version, hash) = {
        let mut payload = text.clone().into_bytes();
        #[cfg(feature = "tsdb")]
        if let Some((tenant, graph)) = tsdb_scope.as_ref() {
            payload.extend_from_slice(tenant.as_bytes());
            payload.extend_from_slice(graph.as_bytes());
        }
        let hash = rls_cache_hash(
            "unified-text",
            &payload,
            #[cfg(feature = "security")]
            ctx.caller,
            #[cfg(feature = "security")]
            rls,
        );
        // BUG-267: same cheap-probe-before-snapshot ordering as the
        // `UnifiedQuery` arm above — see its comment for the invariant
        // AND for why there is only ONE counted cache lookup here (a
        // second post-snapshot check double-counts misses against
        // `ResultCache`'s hit/miss stats).
        let probe = match &dep {
            Some(_) => core.result_cache().get_dep(hash, 0, core.dep_clock()),
            None => core.result_cache().get(hash, core.version()),
        };
        if let Some(bytes) = probe {
            return Ok(Response::ok(
                req_id,
                ResultPayload::of_cache_hit::<query_results::UnifiedQueryText>(bytes),
            ));
        }
        // perf/row-visibility-index (B-sweep): a result-cache MISS still
        // used to unconditionally pay for `filter_view`'s full per-node RLS
        // decode — mirror `Method::CypherQuery`'s per-(actor,version)
        // `FilteredViewCache` probe-then-build here too.
        #[cfg(feature = "security")]
        let (snap, version) = versioned_rls_snapshot(&core, ctx.caller, rls);
        #[cfg(not(feature = "security"))]
        let (snap, version) = core.analysis_snapshot_versioned();
        (snap, version, hash)
    };
    #[cfg(not(feature = "result-cache"))]
    let snap = rls_snapshot(
        &core,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        rls,
    );
    // See the `UnifiedQuery` arm above: push the vector + lexical legs down
    // into the live persistent indexes via the shared off-lock runner, instead
    // of pre-cloning the whole `SemanticStore` here.
    let result = run_unified_off_lock(
        state,
        req_id,
        &core,
        snap,
        plan,
        #[cfg(feature = "tsdb")]
        tsdb_scope,
    )
    .await;
    let resp = unified_response::<query_results::UnifiedQueryText>(
        req_id,
        result,
        #[cfg(feature = "result-cache")]
        &core,
        #[cfg(feature = "result-cache")]
        &dep,
        #[cfg(feature = "result-cache")]
        version,
        #[cfg(feature = "result-cache")]
        hash,
    );
    Ok(resp)
}
