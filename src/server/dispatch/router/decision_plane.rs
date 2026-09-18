use super::*;

/// The decision plane: the Decide layer, the solver, the connector pack and
/// the mutation-outbox admin surface.
///
/// One link per group, chained with `?` on `ControlFlow` exactly like the
/// control plane above it: `Break(response)` short-circuits, `Continue(method)`
/// hands the method to the next group.
pub(super) async fn dispatch_decision_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let method = dispatch_decision_methods(ctx, method).await?;
    dispatch_catalog_admin_methods(ctx, method).await
}

/// The four Decide methods and the solver. None carries a `#[cfg]`: the wire
/// contract is unconditional, and each handler owns whatever feature gate its
/// own implementation needs.
async fn dispatch_decision_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::AgentAssemble { request } => {
            dispatch_boxed(async {
                handlers::decide::handle_agent_assemble(state, req.id, verified_context, *request)
                    .await
            })
            .await
        }
        Method::DecisionCommit { request } => {
            dispatch_boxed(async {
                handlers::decide::handle_decision_commit(state, req.id, verified_context, *request)
                    .await
            })
            .await
        }
        Method::Decide { request } => {
            dispatch_boxed(async {
                handlers::decide::handle_decide(state, req.id, verified_context, *request).await
            })
            .await
        }
        Method::DecisionFit { op } => {
            dispatch_boxed(async {
                handlers::decide::handle_decision_fit(state, req.id, verified_context, *op).await
            })
            .await
        }
        Method::DecisionEval { op } => {
            dispatch_boxed(async {
                handlers::decide::handle_decision_eval(state, req.id, verified_context, *op).await
            })
            .await
        }
        Method::Solve { request } => {
            dispatch_boxed(async { handlers::solve::handle_solve(req.id, *request).await }).await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The two catalog-administration surfaces: connector packs and the mutation
/// outbox.
async fn dispatch_catalog_admin_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::ConnectorPack { op } => {
            dispatch_boxed(async {
                handlers::admin::handle_connector_pack(state, req.id, verified_context, *op).await
            })
            .await
        }
        Method::MutationOutbox { op } => {
            dispatch_boxed(async {
                handlers::mutation_outbox::handle_mutation_outbox(
                    state,
                    req.id,
                    verified_context,
                    *op,
                )
                .await
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}
