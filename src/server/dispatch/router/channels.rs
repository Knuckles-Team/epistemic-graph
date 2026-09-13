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
    ControlFlow::Break(match method {
        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            dispatch_boxed(create_channel(
                ctx,
                channel_id,
                channel_type,
                creator,
                initial_members,
            ))
            .await
        }

        Method::JoinChannel {
            channel_id,
            agent_id,
        } => dispatch_boxed(join_channel(ctx, channel_id, agent_id)).await,

        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => dispatch_boxed(leave_channel(ctx, channel_id, agent_id)).await,

        Method::CloseChannel {
            channel_id,
            summary_embedding,
            topic_metadata,
        } => {
            dispatch_boxed(close_channel(
                ctx,
                channel_id,
                summary_embedding,
                topic_metadata,
            ))
            .await
        }

        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => dispatch_boxed(send_message(ctx, channel_id, sender, payload)).await,

        Method::GetChannelMessages { channel_id, limit } => {
            dispatch_boxed(get_channel_messages(ctx, channel_id, limit)).await
        }

        Method::ListChannels => dispatch_boxed(list_channels(ctx)).await,

        Method::GetChannelMembers { channel_id } => {
            dispatch_boxed(get_channel_members(ctx, channel_id)).await
        }
        other => return ControlFlow::Continue(other),
    })
}

async fn create_channel(
    ctx: DispatchCtx<'_>,
    channel_id: String,
    channel_type: crate::protocol::ChannelType,
    creator: String,
    initial_members: Vec<String>,
) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    if creator != carrier.agent_id() {
        return Response::err(req_id, "ACCESS_DENIED: channel creator must be caller");
    }
    let mut state = timed_write(ctx.state).await;
    match state.channels.create_channel_scoped(
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
        Err(error) => Response::err(req_id, error),
    }
}

async fn join_channel(ctx: DispatchCtx<'_>, channel_id: String, agent_id: String) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    if agent_id != carrier.agent_id() {
        return Response::err(req_id, "ACCESS_DENIED: channel join actor must be caller");
    }
    let mut state = timed_write(ctx.state).await;
    if let Err(error) = state
        .channels
        .authorize_tenant(&channel_id, carrier.tenant_scope())
    {
        return Response::err(req_id, error);
    }
    match state.channels.join_channel(&channel_id, carrier.agent_id()) {
        Ok(()) => Response::ok(
            req_id,
            ResultPayload::scalar::<results::JoinChannel>("joined".to_string()),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn leave_channel(ctx: DispatchCtx<'_>, channel_id: String, agent_id: String) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    if agent_id != carrier.agent_id() {
        return Response::err(req_id, "ACCESS_DENIED: channel leave actor must be caller");
    }
    let mut state = timed_write(ctx.state).await;
    if let Err(error) =
        state
            .channels
            .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
    {
        return Response::err(req_id, error);
    }
    match state
        .channels
        .leave_channel(&channel_id, carrier.agent_id())
    {
        Ok(imprint) => Response::ok(
            req_id,
            ResultPayload::of::<results::LeaveChannel>(ChannelDeparture::new(
                imprint,
                ChannelDepartureStatus::Left,
            )),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn close_channel(
    ctx: DispatchCtx<'_>,
    channel_id: String,
    summary_embedding: Option<Vec<f32>>,
    topic_metadata: Option<String>,
) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    let mut state = timed_write(ctx.state).await;
    if let Err(error) =
        state
            .channels
            .authorize_creator(&channel_id, carrier.tenant_scope(), carrier.agent_id())
    {
        return Response::err(req_id, error);
    }
    match state
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
        Err(error) => Response::err(req_id, error),
    }
}

async fn send_message(
    ctx: DispatchCtx<'_>,
    channel_id: String,
    sender: String,
    payload: String,
) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    if sender != carrier.agent_id() {
        return Response::err(req_id, "ACCESS_DENIED: channel sender must be caller");
    }
    let mut state = timed_write(ctx.state).await;
    if let Err(error) =
        state
            .channels
            .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
    {
        return Response::err(req_id, error);
    }
    match state
        .channels
        .send_message(&channel_id, carrier.agent_id(), &payload)
    {
        Ok(()) => Response::ok(
            req_id,
            ResultPayload::scalar::<results::SendMessage>("sent".to_string()),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn get_channel_messages(
    ctx: DispatchCtx<'_>,
    channel_id: String,
    limit: Option<usize>,
) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    let state = timed_read(ctx.state).await;
    if let Err(error) =
        state
            .channels
            .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
    {
        return Response::err(req_id, error);
    }
    match state.channels.get_messages(&channel_id, limit) {
        Ok(messages) => Response::ok(
            req_id,
            ResultPayload::of::<results::GetChannelMessages>(
                messages.into_iter().cloned().collect(),
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn list_channels(ctx: DispatchCtx<'_>) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    let state = timed_read(ctx.state).await;
    let channels: Vec<ChannelSummary> = state
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

async fn get_channel_members(ctx: DispatchCtx<'_>, channel_id: String) -> Response {
    let req_id = ctx.req.id;
    let carrier = match CarrierAuthority::from_verified(ctx.verified_context) {
        Ok(authority) => authority,
        Err(denied) => return Response::err(req_id, denied),
    };
    let state = timed_read(ctx.state).await;
    if let Err(error) =
        state
            .channels
            .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
    {
        return Response::err(req_id, error);
    }
    match state.channels.get_members(&channel_id) {
        Ok(members) => Response::ok(
            req_id,
            ResultPayload::scalar::<results::GetChannelMembers>(members),
        ),
        Err(error) => Response::err(req_id, error),
    }
}
