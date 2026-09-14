use super::*;
/// Everything the runtime-conditional query/RDF gateways need from the resolved
/// request, bundled so each router stays at the documented parameter cap. The
/// borrowed/owned split matches how `run_dispatch_pipeline` already held these
/// values: identity and authz captures are borrowed, the `Arc` handles are
/// cloned per stage because the mutation gateway moves them into an async apply
/// closure. Same shape as `crate::server::mutation::MutationCtx`.
#[cfg(any(
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "rdf"
))]
pub(super) struct GatewayRouteCtx<'a> {
    pub(super) state: &'a Arc<RwLock<ServerState>>,
    pub(super) req_id: u64,
    pub(super) graph_name: &'a str,
    pub(super) caller: Option<&'a str>,
    pub(super) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(super) idempotency_key: &'a str,
    pub(super) tenant_scope: &'a str,
    pub(super) core: Arc<crate::graph::GraphCore>,
    pub(super) persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    pub(super) cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    pub(super) materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    pub(super) gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    pub(super) read_authority: &'a Option<GraphReadAuthority>,
    pub(super) verified_actor: &'a str,
    #[cfg(feature = "security")]
    pub(super) rls: std::sync::Arc<crate::isolation::IsolationLayer>,
}

#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
async fn commit_query_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Response {
    if crate::server::mutation::is_query_gateway_method(&method)
        && !crate::server::mutation::is_query_native_coordinator(&method)
    {
        let mutates_now = requires_write(&method);
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let (iso, gtype, owner) = ctx
            .gateway_authz_ctx
            .as_ref()
            .expect("is_gateway_routed query method must have a captured GatewayAuthzCtx");
        let mutation_ctx = crate::server::mutation::MutationCtx {
            req_id: ctx.req_id,
            caller: ctx.caller,
            attempt_nonce: ctx.attempt_nonce,
            idempotency_key: ctx.idempotency_key,
            tenant_scope: ctx.tenant_scope,
            graph_name: ctx.graph_name,
            graph_type: *gtype,
            owner: owner.as_deref(),
            isolation: iso,
            core: &ctx.core,
            persistence: ctx.persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: ctx.cdc.as_ref(),
            materialization_manifest: ctx.materialization_manifest.as_ref(),
            write_coalescer: None,
        };
        let method_apply = method.clone();
        let query_read_authority = ctx.read_authority.clone();
        #[cfg(feature = "security")]
        let rls_apply = ctx.rls.clone();
        let state = ctx.state;
        let req_id = ctx.req_id;
        let graph_name = ctx.graph_name;
        let verified_actor = ctx.verified_actor;
        let resp = crate::server::mutation::commit_conditional_mutation_async(
            &mutation_ctx,
            &plan,
            &method,
            mutates_now,
            move |staged_core| async move {
                match handlers::query::try_handle(
                    state,
                    handlers::TryHandleContext {
                        req_id,
                        graph_name,
                        read_authority: query_read_authority.as_ref(),
                        caller: verified_actor,
                    },
                    staged_core,
                    method_apply,
                    #[cfg(feature = "security")]
                    &rls_apply,
                )
                .await
                {
                    Ok(r) => match r.error {
                        Some(e) => Err(e),
                        None => Ok(r
                            .result
                            .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                    },
                    Err(_) => Err("query surface not available in this build".to_string()),
                }
            },
        )
        .await;
        return resp;
    }
    unreachable!("gateway commit helper called for an unclassified method");
}

#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
pub(super) async fn route_query_gateway(
    ctx: GatewayRouteCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    if crate::server::mutation::is_query_gateway_method(&method)
        && !crate::server::mutation::is_query_native_coordinator(&method)
    {
        return Ok(commit_query_gateway(ctx, method).await);
    }
    match handlers::query::try_handle(
        ctx.state,
        handlers::TryHandleContext {
            req_id: ctx.req_id,
            graph_name: ctx.graph_name,
            read_authority: ctx.read_authority.as_ref(),
            caller: ctx.verified_actor,
        },
        ctx.core.clone(),
        method,
        #[cfg(feature = "security")]
        &ctx.rls,
    )
    .await
    {
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}
#[cfg(feature = "rdf")]
async fn commit_rdf_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Response {
    if crate::server::mutation::is_rdf_gateway_method(&method) {
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let (iso, gtype, owner) = ctx
            .gateway_authz_ctx
            .as_ref()
            .expect("is_gateway_routed rdf method must have a captured GatewayAuthzCtx");
        let mutation_ctx = crate::server::mutation::MutationCtx {
            req_id: ctx.req_id,
            caller: ctx.caller,
            attempt_nonce: ctx.attempt_nonce,
            idempotency_key: ctx.idempotency_key,
            tenant_scope: ctx.tenant_scope,
            graph_name: ctx.graph_name,
            graph_type: *gtype,
            owner: owner.as_deref(),
            isolation: iso,
            core: &ctx.core,
            persistence: ctx.persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: ctx.cdc.as_ref(),
            materialization_manifest: ctx.materialization_manifest.as_ref(),
            write_coalescer: None,
        };
        let method_apply = method.clone();
        let rdf_read_authority = ctx.read_authority.clone();
        #[cfg(feature = "security")]
        let rls_apply = ctx.rls.clone();
        let state = ctx.state;
        let req_id = ctx.req_id;
        let graph_name = ctx.graph_name;
        let verified_actor = ctx.verified_actor;
        let resp = crate::server::mutation::commit_conditional_mutation_async(
            &mutation_ctx,
            &plan,
            &method,
            true,
            move |staged_core| async move {
                match handlers::rdf::try_handle(
                    state,
                    handlers::TryHandleContext {
                        req_id,
                        graph_name,
                        read_authority: rdf_read_authority.as_ref(),
                        caller: verified_actor,
                    },
                    staged_core,
                    method_apply,
                    #[cfg(feature = "security")]
                    &rls_apply,
                )
                .await
                {
                    Ok(r) => match r.error {
                        Some(e) => Err(e),
                        None => Ok(r
                            .result
                            .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                    },
                    Err(_) => Err("rdf surface not available in this build".to_string()),
                }
            },
        )
        .await;
        return resp;
    }
    unreachable!("gateway commit helper called for an unclassified method");
}

#[cfg(feature = "rdf")]
pub(super) async fn route_rdf_gateway(
    ctx: GatewayRouteCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    if crate::server::mutation::is_rdf_gateway_method(&method) {
        return Ok(commit_rdf_gateway(ctx, method).await);
    }
    match handlers::rdf::try_handle(
        ctx.state,
        handlers::TryHandleContext {
            req_id: ctx.req_id,
            graph_name: ctx.graph_name,
            read_authority: ctx.read_authority.as_ref(),
            caller: ctx.verified_actor,
        },
        ctx.core.clone(),
        method,
        #[cfg(feature = "security")]
        &ctx.rls,
    )
    .await
    {
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}
