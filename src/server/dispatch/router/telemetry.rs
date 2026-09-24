use super::*;

/// Telemetry facts (EH-408 / EH-409): `TelemetryDerive` self-routes here,
/// reads the in-process observability store and the request graph, and
/// self-translates its facts into one gateway-routed `BatchUpdate` against the
/// request graph. Without the `obs` feature there is no store to read.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_telemetry_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let Method::TelemetryDerive {
        from_ms,
        to_ms,
        streams,
        policy_msgpack,
    } = method
    else {
        return ControlFlow::Continue(method);
    };
    #[cfg(feature = "obs")]
    {
        use super::super::telemetry::{handle_telemetry_derive, DeriveRequest, DeriveTarget};

        let target = DeriveTarget {
            graph: &ctx.req.graph,
            req_id: ctx.req.id,
            caller: ctx.req.agent_id.as_deref(),
            verified: ctx.verified_context,
        };
        let request = DeriveRequest {
            from_ms,
            to_ms,
            streams,
            policy_msgpack,
        };
        ControlFlow::Break(
            dispatch_boxed(handle_telemetry_derive(ctx.state, target, request)).await,
        )
    }
    #[cfg(not(feature = "obs"))]
    {
        let _ = (from_ms, to_ms, streams, policy_msgpack);
        ControlFlow::Break(Response::err(
            ctx.req.id,
            "TelemetryDerive requires the `obs` feature",
        ))
    }
}
