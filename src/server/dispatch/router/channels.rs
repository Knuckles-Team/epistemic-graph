use super::*;

use eg_types::messaging_wire::{
    ChannelCreated, ChannelDeparture, ChannelDepartureStatus, ChannelSummary,
};
use eg_types::result_contract::messaging as results;

/// Channel and messaging operations: channel lifecycle, membership and message
/// send/read.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_channel_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    if creator != carrier.agent_id() {
                        return Response::err(
                            req_id,
                            "ACCESS_DENIED: channel creator must be caller",
                        );
                    }
                    let mut s = timed_write(state).await;
                    match s.channels.create_channel_scoped(
                        &channel_id,
                        carrier.tenant_scope(),
                        channel_type,
                        carrier.agent_id(),
                        initial_members,
                    ) {
                        Ok(()) => Response::ok(
                            req_id,
                            ResultPayload::of::<results::CreateChannel>(ChannelCreated {
                                channel: channel_id,
                            }),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    if agent_id != carrier.agent_id() {
                        return Response::err(
                            req_id,
                            "ACCESS_DENIED: channel join actor must be caller",
                        );
                    }
                    let mut s = timed_write(state).await;
                    if let Err(error) = s
                        .channels
                        .authorize_tenant(&channel_id, carrier.tenant_scope())
                    {
                        return Response::err(req_id, error);
                    }
                    match s.channels.join_channel(&channel_id, carrier.agent_id()) {
                        Ok(()) => Response::ok(
                            req_id,
                            ResultPayload::scalar::<results::JoinChannel>("joined".to_string()),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    if agent_id != carrier.agent_id() {
                        return Response::err(
                            req_id,
                            "ACCESS_DENIED: channel leave actor must be caller",
                        );
                    }
                    let mut s = timed_write(state).await;
                    if let Err(error) = s.channels.authorize_member(
                        &channel_id,
                        carrier.tenant_scope(),
                        carrier.agent_id(),
                    ) {
                        return Response::err(req_id, error);
                    }
                    match s.channels.leave_channel(&channel_id, carrier.agent_id()) {
                        Ok(imprint) => Response::ok(
                            req_id,
                            ResultPayload::of::<results::LeaveChannel>(ChannelDeparture::new(
                                imprint,
                                ChannelDepartureStatus::Left,
                            )),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::CloseChannel {
            channel_id,
            summary_embedding,
            topic_metadata,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    let mut s = timed_write(state).await;
                    if let Err(error) = s.channels.authorize_creator(
                        &channel_id,
                        carrier.tenant_scope(),
                        carrier.agent_id(),
                    ) {
                        return Response::err(req_id, error);
                    }
                    match s
                        .channels
                        .close_channel(&channel_id, summary_embedding, topic_metadata)
                    {
                        Ok(imprint) => Response::ok(
                            req_id,
                            ResultPayload::of::<results::CloseChannel>(ChannelDeparture::new(
                                imprint,
                                ChannelDepartureStatus::Closed,
                            )),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    if sender != carrier.agent_id() {
                        return Response::err(
                            req_id,
                            "ACCESS_DENIED: channel sender must be caller",
                        );
                    }
                    let mut s = timed_write(state).await;
                    if let Err(error) = s.channels.authorize_member(
                        &channel_id,
                        carrier.tenant_scope(),
                        carrier.agent_id(),
                    ) {
                        return Response::err(req_id, error);
                    }
                    match s
                        .channels
                        .send_message(&channel_id, carrier.agent_id(), &payload)
                    {
                        Ok(()) => Response::ok(
                            req_id,
                            ResultPayload::scalar::<results::SendMessage>("sent".to_string()),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::GetChannelMessages { channel_id, limit } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    let s = timed_read(state).await;
                    if let Err(error) = s.channels.authorize_member(
                        &channel_id,
                        carrier.tenant_scope(),
                        carrier.agent_id(),
                    ) {
                        return Response::err(req_id, error);
                    }
                    match s.channels.get_messages(&channel_id, limit) {
                        Ok(msgs) => Response::ok(
                            req_id,
                            ResultPayload::of::<results::GetChannelMessages>(
                                msgs.into_iter().cloned().collect(),
                            ),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }

        Method::ListChannels => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    let s = timed_read(state).await;
                    let channels: Vec<ChannelSummary> = s
                        .channels
                        .list_channels_for(carrier.tenant_scope(), carrier.agent_id())
                        .into_iter()
                        .map(|(id, channel_type, members)| ChannelSummary {
                            id,
                            channel_type,
                            members,
                        })
                        .collect();
                    Response::ok(req_id, ResultPayload::of::<results::ListChannels>(channels))
                }
            })
            .await
        }

        Method::GetChannelMembers { channel_id } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    let s = timed_read(state).await;
                    if let Err(error) = s.channels.authorize_member(
                        &channel_id,
                        carrier.tenant_scope(),
                        carrier.agent_id(),
                    ) {
                        return Response::err(req_id, error);
                    }
                    match s.channels.get_members(&channel_id) {
                        Ok(members) => Response::ok(
                            req_id,
                            ResultPayload::scalar::<results::GetChannelMembers>(members),
                        ),
                        Err(e) => Response::err(req_id, e),
                    }
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}
