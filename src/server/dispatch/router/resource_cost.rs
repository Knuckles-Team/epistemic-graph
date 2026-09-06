use super::*;

/// Cost / efficiency telemetry (CONCEPT:EG-KG.compute.lane-v, Lane V): the unpaged
/// snapshot and its paged form. These two are ONE surface; the pre-domain cut
/// had them in two different groups.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_resource_cost_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(feature = "cost"))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(feature = "cost")]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Cost / efficiency (CONCEPT:EG-KG.compute.lane-v, Lane V) ──────────────
            #[cfg(feature = "cost")]
            Method::ResourceStatsPage {
                cursor,
                limit,
                summary,
            } => {
                dispatch_boxed(async {
                    let state = state;
                    let req_id = req.id;
                    let verified_context = verified_context;
                    {
                        dispatch_resource_stats(
                            state,
                            req_id,
                            verified_context,
                            crate::cost::ResourceStatsRequest {
                                cursor,
                                limit,
                                summary,
                            },
                        )
                        .await
                    }
                })
                .await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}
