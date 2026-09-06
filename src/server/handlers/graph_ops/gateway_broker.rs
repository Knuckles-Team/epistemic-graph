use super::*;

use super::gateway::commit_gateway;

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
        .map(|()| ResultPayload::String("ok".to_string()))
}

pub(super) async fn try_handle(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Option<Response> {
    let resp = match method {
        // ── L11 rollout batch 2: message-broker / stream family (Outbox
        // durability domain), behind `feature = "broker"` — see the module docs. ──
        #[cfg(feature = "broker")]
        Method::DeclareExchange { exchange, kind } => {
            let (exchange, kind) = (exchange.clone(), kind.clone());
            commit_gateway(ctx, plan, method, move |core| {
                apply_declare_exchange(core, &exchange, &kind)
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::DeleteExchange { exchange } => {
            let exchange = exchange.clone();
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::delete_exchange(core, &exchange);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(ctx, plan, method, move |core| {
                crate::broker::bind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            let (exchange, queue, routing_key) =
                (exchange.clone(), queue.clone(), routing_key.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::unbind_queue(core, &exchange, &queue, &routing_key);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::Publish {
            exchange,
            routing_key,
            payload,
        } => {
            let (exchange, routing_key, payload) =
                (exchange.clone(), routing_key.clone(), payload.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let delivered = crate::broker::publish(core, &exchange, &routing_key, &payload);
                Ok(ResultPayload::Count(delivered as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::Count(delivered as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::raw(&claimed))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerAck { queue, node_id } => {
            let (queue, node_id) = (queue.clone(), node_id.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::broker_ack(core, &queue, &node_id);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::String(outcome))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::SweepExpired { now_ms } => {
            let now_ms = *now_ms;
            commit_gateway(ctx, plan, method, move |core| {
                let acted = crate::broker::sweep_expired(core, now_ms);
                Ok(ResultPayload::Count(acted as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamPublish {
            stream,
            payload,
            now_ms,
        } => {
            let (stream, payload, now_ms) = (stream.clone(), payload.clone(), *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let offset = crate::broker::stream_publish(core, &stream, &payload, now_ms);
                Ok(ResultPayload::Count(offset as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamTrim { stream, now_ms } => {
            let (stream, now_ms) = (stream.clone(), *now_ms);
            commit_gateway(ctx, plan, method, move |core| {
                let dropped = crate::broker::stream_trim(core, &stream, now_ms);
                Ok(ResultPayload::Count(dropped as u64))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::StreamCommitOffset {
            stream,
            group,
            offset,
        } => {
            let (stream, group, offset) = (stream.clone(), group.clone(), *offset);
            commit_gateway(ctx, plan, method, move |core| {
                crate::broker::commit_offset(core, &stream, &group, offset);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::raw(&token))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                let result = crate::broker::publish_idempotent(
                    core,
                    &exchange,
                    &routing_key,
                    &payload,
                    producer_id.as_deref(),
                    seq,
                    priority,
                    delay_ms,
                    ttl_ms,
                    now_ms,
                );
                Ok(ResultPayload::raw(&result))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerAckTag {
            delivery_tag,
            consumer,
        } => {
            let (delivery_tag, consumer) = (*delivery_tag, consumer.clone());
            commit_gateway(ctx, plan, method, move |core| {
                let existed = crate::broker::broker_ack_tag(core, delivery_tag, &consumer);
                Ok(ResultPayload::Bool(existed))
            })
            .await
        }
        #[cfg(feature = "broker")]
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
                Ok(ResultPayload::String(outcome))
            })
            .await
        }
        #[cfg(feature = "broker")]
        Method::BrokerRenewTag {
            delivery_tag,
            consumer,
            now_ms,
            lease_ms,
        } => {
            let (delivery_tag, consumer, now_ms, lease_ms) =
                (*delivery_tag, consumer.clone(), *now_ms, *lease_ms);
            commit_gateway(ctx, plan, method, move |core| {
                Ok(ResultPayload::Bool(crate::broker::broker_renew_tag(
                    core,
                    delivery_tag,
                    &consumer,
                    now_ms,
                    lease_ms,
                )))
            })
            .await
        }
        _ => return None,
    };
    Some(resp)
}
