use super::*;

#[cfg(feature = "query")]
#[derive(Clone, Copy)]
struct SqlReadScope<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    core: &'a Arc<GraphCore>,
    graph_name: &'a str,
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
pub(crate) async fn sql_persist_dir(
    state: &Arc<RwLock<ServerState>>,
) -> Result<std::path::PathBuf, String> {
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
pub(crate) fn sql_read_snapshot(
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
        state,
        req_id,
        core,
        graph_name,
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
    let graph = crate::server::sql_catalog_acl::RequestGraph {
        name: graph_name.to_string(),
        core: Arc::clone(core),
    };
    catalog_sql_response(
        state,
        req_id,
        snap,
        authority.clone(),
        persist_dir.to_path_buf(),
        query,
        graph,
    )
    .await
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
                graph_name: ctx.graph_name,
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

/// Where one served read's answer is cached: version-keyed and RLS-aware, in the
/// dependency-scoped namespace when the read has a bounded dependency set (it then
/// survives writes disjoint from it), stamped with the version of the snapshot it was
/// computed over.
#[cfg(all(feature = "query", feature = "result-cache"))]
pub(crate) struct CacheSlot {
    pub(crate) hash: u128,
    pub(crate) dep: Option<eg_core::dep_scope::DepSet>,
    pub(crate) version: u64,
}

#[cfg(all(feature = "query", feature = "result-cache"))]
impl CacheSlot {
    /// Store `payload` under this slot.
    pub(crate) fn store(&self, core: &GraphCore, payload: &ResultPayload) {
        match &self.dep {
            Some(deps) => eg_core::result_cache::cache_dep_result(
                core.result_cache(),
                self.hash,
                0,
                self.version,
                deps.clone(),
                payload,
            ),
            None => eg_core::result_cache::cache_result(
                core.result_cache(),
                self.hash,
                self.version,
                payload,
            ),
        }
    }
}

/// The cache key of one served read: `payload` salted with the verified legs, hashed
/// under the caller's RLS context so agent A's answer never reaches agent B.
#[cfg(all(feature = "query", feature = "result-cache"))]
pub(crate) fn served_cache_hash(
    ctx: &QueryHandlerCtx<'_>,
    kind: &str,
    mut payload: Vec<u8>,
    legs: &ServedPlanLegs,
) -> u128 {
    #[cfg(not(feature = "security"))]
    let _ = ctx;
    legs.salt_cache_key(&mut payload);
    legs.salt_watermarks(&mut payload);
    rls_cache_hash(
        kind,
        &payload,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        ctx.rls,
    )
}

/// The cached answer for `hash`, probed BEFORE the O(V+E) snapshot so a hit never
/// materializes the graph (BUG-267), and exactly once per request (each probe bumps
/// the hit/miss counters).
#[cfg(all(feature = "query", feature = "result-cache"))]
pub(crate) fn cached_payload(
    core: &GraphCore,
    hash: u128,
    dep: &Option<eg_core::dep_scope::DepSet>,
) -> Option<Vec<u8>> {
    match dep {
        Some(_) => core.result_cache().get_dep(hash, 0, core.dep_probe()),
        None => core.result_cache().get(hash, core.version()),
    }
}

/// Encode one served read's answer as `M`'s result, caching it under `slot` when there
/// is one; an execution error is `"{label} error: …"`.
#[cfg(feature = "query")]
pub(crate) fn served_response<M>(
    req_id: u64,
    result: Result<Result<M::Body, String>, Response>,
    label: &str,
    #[cfg(feature = "result-cache")] core: &Arc<GraphCore>,
    #[cfg(feature = "result-cache")] slot: Option<CacheSlot>,
) -> Response
where
    M: MethodResult,
    M::Encoding: EncodeRef<M::Body>,
{
    match result {
        Ok(Ok(body)) => match ResultPayload::of_ref::<M>(&body) {
            Ok(payload) => {
                #[cfg(feature = "result-cache")]
                if let Some(slot) = slot {
                    slot.store(core, &payload);
                }
                Response::ok(req_id, payload)
            }
            Err(error) => Response::err(req_id, error),
        },
        Ok(Err(message)) => Response::err(req_id, format!("{label} error: {message}")),
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
    // Verified-carrier legs (tsdb scope + the caller's owner-scoped foreign registry, EH-373).
    let legs = match ctx.served_legs(&plan).await {
        Ok(legs) => legs,
        Err(refusal) => return Ok(refusal),
    };
    // ONE cross-modal plan (CONCEPT:AU-KG.compute.vector/209): filter (DataFusion) →
    // traverse (BFS) → rank (kNN) over ONE consistent off-lock snapshot, run on the
    // blocking pool. Version-keyed, RLS-aware result cache
    // (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231) keyed on the
    // plan bytes + the verified legs; a plan whose reads reduce to a dependency set (labels,
    // edge types, row visibility, the embedding generation — EH-393) is cached in the
    // dependency-scoped namespace
    // (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7) so it
    // survives writes disjoint from it. A plan whose legs read state outside the graph
    // version (the decision log, a foreign source without a fresh watermark — EH-400) is
    // never cached; a fresh foreign source's watermark joins the key.
    #[cfg(feature = "result-cache")]
    let key = match legs
        .cache_admissible()
        .then(|| msgpack_bytes(&plan))
        .transpose()
    {
        Ok(bytes) => bytes.map(|bytes| {
            let hash = served_cache_hash(ctx, "unified", bytes, &legs);
            let stamp = core.dep_probe().embedding_generation();
            (hash, plan_dependency_set(&plan, stamp))
        }),
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    #[cfg(feature = "result-cache")]
    if let Some(response) =
        cached_served_response::<query_results::UnifiedQuery>(req_id, &core, &key)
    {
        return Ok(response);
    }
    let (snap, version) = sql_read_snapshot(
        &core,
        #[cfg(feature = "security")]
        ctx.caller,
        #[cfg(feature = "security")]
        rls,
    );
    #[cfg(not(feature = "result-cache"))]
    let _ = version;
    // CONCEPT:EG-KG.query.served-vector-index-binding / served-text-index-binding — push the
    // vector kNN AND lexical legs down into the LIVE persistent indexes instead
    // of cloning/rebuilding them per request, via the shared off-lock runner.
    let result = run_unified_off_lock(state, req_id, &core, snap, plan, legs).await;
    Ok(served_response::<query_results::UnifiedQuery>(
        req_id,
        result,
        "UnifiedQuery",
        #[cfg(feature = "result-cache")]
        &core,
        #[cfg(feature = "result-cache")]
        key.map(|(hash, dep)| CacheSlot { hash, dep, version }),
    ))
}

/// Run one read statement against the tenant's authorized SQL catalog off the
/// async runtime, cancellable by the request's cancel token and timeout: the
/// execution both served SQL read paths (`handle_sql`, KnowledgeStream) share. The
/// caller's read-only relations (the decision record views, EH-066) join the catalog
/// when the statement can see them.
#[cfg(feature = "query")]
pub(super) async fn catalog_sql_response(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    snap: Arc<crate::graph::GraphView>,
    authority: crate::server::access::CarrierAuthority,
    persist_dir: std::path::PathBuf,
    query: String,
    graph: crate::server::sql_catalog_acl::RequestGraph,
) -> Response {
    let read_only =
        crate::server::handlers::decide::read_only_relations(state, &authority, &query).await;
    let cancel = eg_query::CancellationToken::new();
    let _cancel_guard = crate::server::request_cancel::register(req_id, cancel.clone());
    let timeout_task = crate::server::request_cancel::spawn_timeout(cancel.clone());
    let cancel_for_task = cancel.clone();
    let resp = match compute_off_lock(req_id, move || {
        let authorized = crate::server::sql_catalog_acl::authorized_read_store_for_query(
            &authority,
            &persist_dir,
            &query,
            read_only.as_deref(),
            &graph,
        )?;
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
