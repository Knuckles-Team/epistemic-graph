use super::*;

use super::gateway::commit_gateway;
#[cfg(feature = "broker")]
use eg_types::result_contract::messaging as results;

/// `DeclareExchange`: pure extract-method from `try_handle_gateway`'s closure,
/// byte-identical behaviour, no signature change.
#[cfg(feature = "broker")]
fn apply_declare_exchange(
    core: &GraphCore,
    exchange: &str,
    kind: &str,
) -> Result<ResultPayload, String> {
    let Some(k) = crate::broker::ExchangeKind::parse(kind) else {
        return Err(format!(
            "unknown exchange kind '{kind}' (want direct/topic/fanout)"
        ));
    };
    crate::broker::declare_exchange(core, exchange, k)
        .map(|()| ResultPayload::scalar::<results::DeclareExchange>("ok".to_string()))
}

#[derive(Clone, Copy)]
enum BrokerRoute {
    Exchange,
    Queue,
    Delivery,
    Stream,
    PublishConfirmations,
    TagConfirmations,
    Other,
}

fn route_for(method: &Method) -> BrokerRoute {
    match method {
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. }
        | Method::Publish { .. } => BrokerRoute::Exchange,
        Method::DeclareQueue { .. } | Method::PublishEx { .. } => BrokerRoute::Queue,
        Method::BrokerConsume { .. } | Method::BrokerAck { .. } | Method::BrokerReject { .. } => {
            BrokerRoute::Delivery
        }
        Method::SweepExpired { .. }
        | Method::StreamDeclare { .. }
        | Method::StreamPublish { .. }
        | Method::StreamTrim { .. }
        | Method::StreamCommitOffset { .. } => BrokerRoute::Stream,
        Method::PublishConfirmed { .. } | Method::PublishIdempotent { .. } => {
            BrokerRoute::PublishConfirmations
        }
        Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => BrokerRoute::TagConfirmations,
        _ => BrokerRoute::Other,
    }
}

pub(super) async fn try_handle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    match route_for(method) {
        BrokerRoute::Exchange => try_handle_exchange(ctx, plan, method).await,
        BrokerRoute::Queue => try_handle_queue(ctx, plan, method).await,
        BrokerRoute::Delivery => try_handle_delivery(ctx, plan, method).await,
        BrokerRoute::Stream => try_handle_stream(ctx, plan, method).await,
        BrokerRoute::PublishConfirmations => {
            try_handle_publish_confirmations(ctx, plan, method).await
        }
        BrokerRoute::TagConfirmations => try_handle_tag_confirmations(ctx, plan, method).await,
        BrokerRoute::Other => None,
    }
}

async fn try_handle_exchange(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::DeclareExchange { exchange, kind } => {
            let (exchange, kind) = (exchange.clone(), kind.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_declare_exchange(core, &exchange, &kind)
            })
            .await
        }
        Method::DeleteExchange { exchange } => {
            let exchange = exchange.clone();
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::delete_exchange(core, &exchange);
                Ok(ResultPayload::scalar::<results::DeleteExchange>(existed))
            })
            .await
        }
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(ctx, plan, method, move |core| {
                crate::broker::bind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::scalar::<results::BindQueue>(
                    "ok".to_string(),
                ))
            })
            .await
        }
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::unbind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::scalar::<results::UnbindQueue>(existed))
            })
            .await
        }
        Method::Publish {
            exchange,
            routing_key,
            payload,
        } => {
            let (exchange, routing_key, payload) =
                (exchange.clone(), routing_key.clone(), payload.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let delivered = crate::broker::publish(core, &exchange, &routing_key, &payload);
                Ok(ResultPayload::scalar::<results::Publish>(delivered as u64))
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_queue(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::DeclareQueue {
            queue,
            dl_exchange,
            dl_routing_key,
            max_delivery_count,
            message_ttl_ms,
            queue_expiry_ms,
            max_priority,
        } => {
            let (
                queue,
                dl_exchange,
                dl_routing_key,
                max_delivery_count,
                message_ttl_ms,
                queue_expiry_ms,
                max_priority,
            ) = (
                queue.clone(),
                dl_exchange.clone(),
                dl_routing_key.clone(),
                *max_delivery_count,
                *message_ttl_ms,
                *queue_expiry_ms,
                *max_priority,
            );
            commit_gateway(ctx, plan, method, move |core| {
                let policy = crate::broker::QueuePolicy {
                    dl_exchange,
                    dl_routing_key,
                    max_delivery_count,
                    message_ttl_ms,
                    queue_expiry_ms,
                    max_priority,
                };
                crate::broker::declare_queue(core, &queue, &policy);
                Ok(ResultPayload::scalar::<results::DeclareQueue>(
                    "ok".to_string(),
                ))
            })
            .await
        }
        Method::PublishEx {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let (exchange, routing_key, payload, priority, delay_ms, ttl_ms, now_ms) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(ctx, plan, method, move |core| {
                let delivered = crate::broker::publish_ex(
                    core,
                    &exchange,
                    &routing_key,
                    &payload,
                    priority,
                    delay_ms,
                    ttl_ms,
                    now_ms,
                );
                Ok(ResultPayload::scalar::<results::PublishEx>(
                    delivered as u64,
                ))
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_stream(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::SweepExpired { now_ms } => {
            let now_ms = *now_ms;
            commit_gateway(ctx, plan, method, move |core| {
                let acted = crate::broker::sweep_expired(core, now_ms);
                Ok(ResultPayload::scalar::<results::SweepExpired>(acted as u64))
            })
            .await
        }
        Method::StreamDeclare {
            stream,
            max_messages,
            max_age_ms,
        } => {
            let (stream, max_messages, max_age_ms) = (stream.clone(), *max_messages, *max_age_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let retention = crate::broker::StreamRetention {
                    max_messages,
                    max_age_ms,
                };
                crate::broker::declare_stream(core, &stream, &retention);
                Ok(ResultPayload::scalar::<results::StreamDeclare>(
                    "ok".to_string(),
                ))
            })
            .await
        }
        Method::StreamPublish {
            stream,
            payload,
            now_ms,
        } => {
            let (stream, payload, now_ms) = (stream.clone(), payload.clone(), *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let offset = crate::broker::stream_publish(core, &stream, &payload, now_ms);
                Ok(ResultPayload::scalar::<results::StreamPublish>(
                    offset as u64,
                ))
            })
            .await
        }
        Method::StreamTrim { stream, now_ms } => {
            let (stream, now_ms) = (stream.clone(), *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let dropped = crate::broker::stream_trim(core, &stream, now_ms);
                Ok(ResultPayload::scalar::<results::StreamTrim>(dropped as u64))
            })
            .await
        }
        Method::StreamCommitOffset {
            stream,
            group,
            offset,
        } => {
            let (stream, group, offset) = (stream.clone(), group.clone(), *offset);
            commit_gateway(ctx, plan, method, move |core| {
                crate::broker::commit_offset(core, &stream, &group, offset);
                Ok(ResultPayload::scalar::<results::StreamCommitOffset>(
                    "ok".to_string(),
                ))
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_delivery(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::BrokerConsume {
            queue,
            group,
            consumer,
            now_ms,
            lease_ms,
            prefetch,
        } => {
            let (queue, group, consumer, now_ms, lease_ms, prefetch) = (
                queue.clone(),
                group.clone(),
                consumer.clone(),
                *now_ms,
                *lease_ms,
                *prefetch,
            );
            commit_gateway(ctx, plan, method, move |core| {
                let claimed = crate::broker::broker_consume(
                    core, &queue, &group, &consumer, now_ms, lease_ms, prefetch,
                );
                ResultPayload::of_ref::<results::BrokerConsume>(&claimed)
            })
            .await
        }
        Method::BrokerAck { queue, node_id } => {
            let (queue, node_id) = (queue.clone(), node_id.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::broker_ack(core, &queue, &node_id);
                Ok(ResultPayload::scalar::<results::BrokerAck>(existed))
            })
            .await
        }
        Method::BrokerReject {
            queue,
            node_id,
            requeue,
            now_ms,
        } => {
            let (queue, node_id, requeue, now_ms) =
                (queue.clone(), node_id.clone(), *requeue, *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let outcome = crate::broker::broker_reject(core, &queue, &node_id, requeue, now_ms);
                Ok(ResultPayload::scalar::<results::BrokerReject>(outcome))
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_publish_confirmations(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::PublishConfirmed {
            exchange,
            routing_key,
            payload,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let (exchange, routing_key, payload, priority, delay_ms, ttl_ms, now_ms) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(ctx, plan, method, move |core| {
                let token = crate::broker::publish_confirmed(
                    core,
                    &exchange,
                    &routing_key,
                    &payload,
                    priority,
                    delay_ms,
                    ttl_ms,
                    now_ms,
                );
                ResultPayload::of_ref::<results::PublishConfirmed>(&token)
            })
            .await
        }
        Method::PublishIdempotent {
            exchange,
            routing_key,
            payload,
            producer_id,
            seq,
            priority,
            delay_ms,
            ttl_ms,
            now_ms,
        } => {
            let (
                exchange,
                routing_key,
                payload,
                producer_id,
                seq,
                priority,
                delay_ms,
                ttl_ms,
                now_ms,
            ) = (
                exchange.clone(),
                routing_key.clone(),
                payload.clone(),
                producer_id.clone(),
                *seq,
                *priority,
                *delay_ms,
                *ttl_ms,
                *now_ms,
            );
            commit_gateway(ctx, plan, method, move |core| {
                let result =
                    crate::broker::publish_idempotent(crate::broker::IdempotentPublishRequest {
                        core,
                        exchange: &exchange,
                        routing_key: &routing_key,
                        payload: &payload,
                        producer_id: producer_id.as_deref(),
                        seq,
                        priority,
                        delay_ms,
                        ttl_ms,
                        now_ms,
                    });
                ResultPayload::of_ref::<results::PublishIdempotent>(&result)
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}

async fn try_handle_tag_confirmations(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let response = match method {
        Method::BrokerAckTag {
            delivery_tag,
            consumer,
        } => {
            let (delivery_tag, consumer) = (*delivery_tag, consumer.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::broker_ack_tag(core, delivery_tag, &consumer);
                Ok(ResultPayload::scalar::<results::BrokerAckTag>(existed))
            })
            .await
        }
        Method::BrokerNackTag {
            delivery_tag,
            consumer,
            requeue,
            now_ms,
        } => {
            let (delivery_tag, consumer, requeue, now_ms) =
                (*delivery_tag, consumer.clone(), *requeue, *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let outcome =
                    crate::broker::broker_nack_tag(core, delivery_tag, &consumer, requeue, now_ms);
                Ok(ResultPayload::scalar::<results::BrokerNackTag>(outcome))
            })
            .await
        }
        Method::BrokerRenewTag {
            delivery_tag,
            consumer,
            now_ms,
            lease_ms,
        } => {
            let (delivery_tag, consumer, now_ms, lease_ms) =
                (*delivery_tag, consumer.clone(), *now_ms, *lease_ms);
            commit_gateway(ctx, plan, method, move |core| {
                Ok(ResultPayload::scalar::<results::BrokerRenewTag>(
                    crate::broker::broker_renew_tag(
                        core,
                        delivery_tag,
                        &consumer,
                        now_ms,
                        lease_ms,
                    ),
                ))
            })
            .await
        }
        _ => return None,
    };
    Some(response)
}
