use super::*;

macro_rules! route_broker_methods {
    ($ctx:ident, $method:ident, { $($arms:tt)* }) => {{
        #[cfg(not(feature = "broker"))]
        {
            let _ = $ctx;
            ControlFlow::Continue($method)
        }
        #[cfg(feature = "broker")]
        {
            let _ = $ctx;
            match $method {
                $($arms)*
            }
        }
    }};
}

use super::terminal::GraphOpsContext;

/// Route broker exchange, queue, and publish operations.
pub(super) async fn try_handle_broker_exchange(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    route_broker_methods!(ctx, method, {
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. } => unreachable!(
            "DeclareExchange is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::DeleteExchange { .. } => unreachable!(
            "DeleteExchange is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BindQueue { .. } => unreachable!(
            "BindQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::UnbindQueue { .. } => unreachable!(
            "UnbindQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::Publish { .. } => unreachable!(
            "Publish is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::DeclareQueue { .. } => unreachable!(
            "DeclareQueue is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::PublishEx { .. } => unreachable!(
            "PublishEx is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        other => ControlFlow::Continue(other),
    })
}

/// Route broker consumption and acknowledgement operations.
pub(super) async fn try_handle_broker_consumption(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    route_broker_methods!(ctx, method, {
        #[cfg(feature = "broker")]
        Method::BrokerConsume { .. } => unreachable!(
            "BrokerConsume is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerAck { .. } => unreachable!(
            "BrokerAck is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerReject { .. } => unreachable!(
            "BrokerReject is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::SweepExpired { .. } => unreachable!(
            "SweepExpired is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        other => ControlFlow::Continue(other),
    })
}

/// Handle replayable stream operations.
pub(super) async fn try_handle_streams(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[cfg(feature = "broker")]
    let GraphOpsContext { req_id, core, .. } = ctx;
    route_broker_methods!(ctx, method, {
        #[cfg(feature = "broker")]
        Method::StreamDeclare { .. } => unreachable!(
            "StreamDeclare is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamPublish { .. } => unreachable!(
            "StreamPublish is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamRead {
            stream,
            from_offset,
            max,
        } => {
            let from = crate::broker::ReadFrom::from_wire(from_offset);
            let msgs = crate::broker::stream_read(core, &stream, from, max as usize);
            ControlFlow::Break(Response::ok(req_id, ResultPayload::of_ref::<eg_types::result_contract::messaging::StreamRead>(&msgs)))
        }
        #[cfg(feature = "broker")]
        Method::StreamTrim { .. } => unreachable!(
            "StreamTrim is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset { .. } => unreachable!(
            "StreamCommitOffset is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::StreamCommittedOffset { stream, group } => {
            let committed = crate::broker::committed_offset(core, &stream, &group);
            ControlFlow::Break(Response::ok(req_id, ResultPayload::of_ref::<eg_types::result_contract::messaging::StreamCommittedOffset>(&committed)))
        }
        other => ControlFlow::Continue(other),
    })
}

/// Route broker publisher-confirm and tag operations.
pub(super) async fn try_handle_publisher_confirms(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    route_broker_methods!(ctx, method, {
        #[cfg(feature = "broker")]
        Method::PublishConfirmed { .. } => unreachable!(
            "PublishConfirmed is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::PublishIdempotent { .. } => unreachable!(
            "PublishIdempotent is mutation::GATEWAY_ROUTED; dispatch_graph_op must \
             route it through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerAckTag { .. } => unreachable!(
            "BrokerAckTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerNackTag { .. } => unreachable!(
            "BrokerNackTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag { .. } => unreachable!(
            "BrokerRenewTag is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
             through try_handle_gateway before it ever reaches this terminal handler"
        ),
        // ── Agent-memory / scene-graph / trajectory wire ops (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ────
        // Route each Method to its eg-core `GraphCore` primitive. The mutating arms
        // share the SAME durable/deterministic contract as the broker precedent: the
        // dispatch shell records them (via `is_durable_mutation`) and `mutation_apply::apply`
        // re-runs the SAME primitive over the same pre-image, and every generated id
        // derives deterministically from sorted inputs / node-count / step ordinals,
        // so a replayed WAL record reproduces byte-identical state. Reads are pure.
        // CreateSummaryNode/Consolidate/Reinforce (CONCEPT:EG-P0-2 bypass guard):
        // GATEWAY_ROUTED — see the AddNode/RemoveNode comment above for why these
        // are structurally unreachable here, not merely undocumented.
        other => ControlFlow::Continue(other),
    })
}
