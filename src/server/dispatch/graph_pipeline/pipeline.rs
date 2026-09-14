use super::*;
/// Everything [`run_dispatch_pipeline`] routes with, resolved once per request
/// by `dispatch_graph_op`: the caller's verified identity and read authority,
/// the live graph core with its durability/CDC/materialization handles, and the
/// two write coalescers. Bundled so the pipeline keeps ONE routing parameter
/// beside the method it is routing, instead of a seventeen-long list.
pub(super) struct DispatchPipelineCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) caller: Option<&'a str>,
    pub(super) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(super) idempotency_key: &'a str,
    pub(super) read_authority: Option<GraphReadAuthority>,
    pub(super) verified_actor: &'a str,
    pub(super) tenant_scope: String,
    pub(super) gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
    pub(super) core: Arc<crate::graph::GraphCore>,
    pub(super) materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    pub(super) persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub(super) cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    pub(super) routed_write_coalescer:
        Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
    #[cfg(feature = "security")]
    pub(super) rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    pub(super) tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
}

/// Stage 1: the universal mutation gateway, then the per-graph write
/// coalescer, then the stateless pure-compute domains.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
pub(super) async fn route_gateway_and_stateless_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let attempt_nonce = ctx.attempt_nonce;
    let idempotency_key = ctx.idempotency_key;
    let read_authority = &ctx.read_authority;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    let routed_write_coalescer = &ctx.routed_write_coalescer;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    // Mutation-gateway routing (CONCEPT:EG-P0-2): the primary CRUD + agent-
    // memory writes (`mutation::GATEWAY_ROUTED`) are routed through the
    // single `commit_mutation` gateway — policy-driven authz, durability,
    // audit, CDC, and TMS in ONE call. Native stores use their own explicit
    // MutationBatch kernels. There is no second post-dispatch durability tail.
    let method = match handlers::graph_ops::try_handle_gateway(
        req_id,
        caller,
        attempt_nonce,
        idempotency_key,
        tenant_scope,
        graph_name,
        core,
        materialization_manifest.as_ref(),
        read_authority.as_ref(),
        persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc.as_ref(),
        Some(routed_write_coalescer),
        gateway_authz_ctx.as_ref(),
        #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
        tsdb_store.as_ref(),
        method,
    )
    .await
    {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Pure-compute domains (stateless: no graph core / lock) route first; a
    // method that isn't theirs is handed back via Err and falls through to the
    // graph-op match below. (CONCEPT:EG-KG.query.dispatch-routing — thin routing; logic in handlers/.)
    // Feature-gated: in a slim build the line is absent and the method flows
    // straight through to graph_ops (whose catch-all reports "not available").
    #[cfg(feature = "finance")]
    let method = match handlers::finance::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 2: the graph-scoped compute domains — data science, mining,
/// graph learning and the ML pipeline read verbs.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
pub(super) async fn route_graph_scoped_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let read_authority = &ctx.read_authority;
    let core = &ctx.core;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    #[cfg(feature = "datascience")]
    let method = match handlers::datascience::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Data-mining domain (CONCEPT:EG-KG.mining.frequent-itemset-mining): GRAPH-SCOPED
    // (unlike finance/datascience), so it takes the graph core — the graph-derived
    // transaction source reads node neighborhoods and write-back materializes
    // `:AssociationRule` nodes into it. A method whose feature is off falls through
    // to the graph_ops not-available catch-all.
    #[cfg(feature = "mining")]
    let method = match handlers::mining::try_handle(
        req_id,
        core.clone(),
        read_authority.as_ref(),
        #[cfg(all(feature = "query", feature = "tsdb"))]
        graph_name,
        #[cfg(all(feature = "query", feature = "tsdb"))]
        tsdb_store.as_ref(),
        method,
    ) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Graph-learning domain (CONCEPT:EG-KG.graphlearn.link-predictor): GRAPH-SCOPED
    // like mining — the KAN link-predictor reads the live subgraph and write-back
    // materializes `:PredictedEdge`/`:EdgeFunction` nodes into the core. A method
    // whose feature is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "graphlearn")]
    let method = match handlers::graphlearn::try_handle(req_id, core.clone(), method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): the READ verbs
    // (Evaluate/Compare) route here with the graph core; Train/Serve/Predict are
    // GATEWAY_ROUTED (writeback) and never reach this fallback. A build without
    // `ml-pipeline` omits this line.
    #[cfg(feature = "ml-pipeline")]
    let method =
        match handlers::pipeline::try_handle(req_id, core.clone(), read_authority.as_ref(), method)
        {
            Ok(r) => return Ok(r),
            Err(m) => m,
        };
    Err(method)
}

/// Stage 3: the runtime-conditional query and native-RDF gateways.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
pub(super) async fn route_query_and_rdf_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    #[cfg(feature = "security")]
    let rls = &ctx.rls;
    // Read-only query surface — SQL (CONCEPT:EG-KG.query.read-only-sql-query, DataFusion behind
    // `query`) AND Cypher (CONCEPT:EG-KG.query.dep-free-behind, dep-free behind `cypher`) AND GraphQL
    // (CONCEPT:EG-KG.query.sparql-completeness, pure-Rust eg-graphql behind `graphql`): borrows the graph
    // core for an off-lock snapshot, runs on the blocking pool. Gated on ANY of the
    // three features so CypherQuery still routes in a cypher-only (no-DataFusion) Pi
    // build and GraphQl routes in a graphql build; the handler's per-method arm
    // falls through (Err) when ITS feature is off, so Sql/CypherQuery/GraphQl then
    // reach the graph_ops not-available catch-all. GraphQL — like SQL/Cypher/SPARQL
    // — runs UNDER the SAME RLS-aware result-cache compose (`caller`/`&rls` threaded
    // in, the cache key folds the caller's RLS context, the snapshot is RLS-filtered
    // to the caller) so a GraphQL read NEVER leaks across agents. Slim builds with
    // NONE of the three omit this line.
    //
    // Runtime-conditional query gateway (CONCEPT:EG-P0-2, L11): `Sql`/
    // `CypherQuery`/`GraphQl` are `mutation::GATEWAY_ROUTED`, but their execution
    // is `async` and needs `state`/`rls`, so they are routed HERE (not at the
    // graph-ops `try_handle_gateway`, which hands them back). The SAME runtime
    // parse `access::requires_write` uses decides whether THIS statement mutates:
    // a SQL write / Cypher `CREATE|SET|DELETE` / GraphQL `mutation` → the full
    // Write-authz commit; a `SELECT` / read-only Cypher / GraphQL `query` → a
    // Read-authz passthrough with no durability/audit/CDC. Every OTHER query
    // method (`UnifiedQuery`/`Explain*`/`Txn*Query`) is a pure read handled by
    // the unchanged direct call in the `else` arm.
    #[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
    let method = match route_query_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            attempt_nonce: ctx.attempt_nonce,
            idempotency_key: ctx.idempotency_key,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    // Native RDF/SPARQL surface (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql/218, features `rdf`/`sparql`):
    // AddTriples (durable — the shell below records it like any write),
    // GetRdf + Sparql (read-only, off-lock snapshot). Graph-scoped, so the
    // handler takes the graph core + name. Multi-valued literals are embedded
    // losslessly in that graph image. Gated on `rdf`; a method whose feature is
    // off falls through (Err) to the graph_ops not-available catch-all.
    //
    // Native-RDF write gateway (CONCEPT:EG-P0-2, L11): `AddTriples`/
    // `RemoveTriples`/`DropNamedGraph` are `mutation::GATEWAY_ROUTED` (GraphRedb-
    // durable, audited), routed HERE (not at `try_handle_gateway`) because their
    // handler is async and also performs RDF policy validation. They always
    // mutate (`mutates_now = true`), so `commit_conditional_mutation_async` runs
    // the full Write-authz + durable audit-chain commit; the read-only
    // RDF methods (`GetRdf`/`Sparql`/`ShaclValidate`/…) take the unchanged direct
    // call in the `else` arm.
    #[cfg(feature = "rdf")]
    let method = match route_rdf_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            attempt_nonce: ctx.attempt_nonce,
            idempotency_key: ctx.idempotency_key,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 4: the process-global domains — sandboxed UDFs, query federation
/// and distributed compute.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
pub(super) async fn route_process_global_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    // WASM-sandboxed UDF surface (CONCEPT:EG-KG.query.rowset-execution, feature `wasm-udf`):
    // RegisterUdf compiles+caches, RunUdf runs sandboxed (fuel+memory+no host
    // caps) — both off-reactor. Process-global (not graph-scoped), so it takes
    // `state` for the UdfRegistry. A method whose feature is off falls through.
    #[cfg(feature = "wasm-udf")]
    let method = match handlers::wasm_udf::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Query federation (CONCEPT:EG-KG.query.query-federation, feature `federation`):
    // RegisterForeignSource records a named foreign source on ServerState. The
    // `Op::ForeignScan` op itself runs through the unified-query handler above
    // (inline spec). Process-global, so it takes `state`. A method whose feature
    // is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "federation")]
    let method = match handlers::federation::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Distributed graph compute (CONCEPT:EG-KG.storage.feature, feature `compute-dist`):
    // DistributedCompute + the matview lifecycle. Cross-shard, so it takes
    // `state` (it gathers each shard graph's snapshot from the registry).
    #[cfg(any(feature = "compute-dist", feature = "matview"))]
    let method = match handlers::dist_compute::try_handle(
        state,
        req_id,
        caller,
        read_authority.as_ref(),
        method,
    )
    .await
    {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// The gateway/compute half of the routing pipeline.
pub(super) async fn route_pipeline_compute(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_gateway_and_stateless_domains(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_graph_scoped_domains(ctx, method).await
}

/// The query-surface / process-global half of the routing pipeline.
pub(super) async fn route_pipeline_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_query_and_rdf_surfaces(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_process_global_domains(ctx, method).await
}

/// Route one already-authorized graph operation through the dispatch pipeline.
///
/// The single thirteen-step `'dispatch:` block this replaced is now four stages
/// tried in order, each handing back a method it does not own. Stage order is
/// unchanged, so the gateway still sees every routed mutation first and the
/// terminal graph-op handler still owns the catch-all.
pub(super) async fn run_dispatch_pipeline(
    ctx: DispatchPipelineCtx<'_>,
    method: Method,
) -> Response {
    let method = match route_pipeline_compute(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    let method = match route_pipeline_surfaces(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    // Terminal handler: graph-targeted ops (borrow the core; cross-graph ops
    // re-enter the registry via `state`). Owns the catch-all, returns a Response.
    let Some(read_authority) = ctx.read_authority.as_ref() else {
        return Response::err(
            ctx.req_id,
            "mutation escaped the universal mutation gateway before terminal dispatch",
        );
    };
    handlers::graph_ops::try_handle(
        ctx.state,
        ctx.req_id,
        ctx.caller,
        ctx.graph_name,
        read_authority,
        ctx.core.clone(),
        method,
    )
    .await
}
