//! AMQP 0.9.1 wire-protocol listener (CONCEPT:EG-KG.query.amqp-codec-arg-cursor) — a HAND-ROLLED AMQP 0.9.1
//! server that lets a standard AMQP client (pika, amqplib, the `rabbitmqadmin`-style
//! tools) speak to the native message broker built on the KG-2.303 work-queue.
//!
//! ## What this is (and is NOT)
//! Like the pgwire / mysql-wire shims, this is an ADAPTER, not a second broker. Every
//! exchange/binding/queue/message lives as graph nodes on a control graph; this module
//! only frames AMQP 0.9.1 on the wire and maps each method onto the broker primitives
//! (`crate::broker`) THROUGH the engine dispatch (`crate::server::dispatch::dispatch`)
//! — so an AMQP publish takes the SAME routing + atomic enqueue + WAL/CDC path a
//! `Method::Publish` RPC does, and consumption uses the native
//! `Method::BrokerConsume`/`BrokerAck` lifecycle. No parallel mechanism.
//!
//! It links NO AMQP crate — every byte layout is hand-rolled against the published
//! AMQP 0.9.1 spec (the Pi-contract idiom pgwire / mysql-wire / sparql-http use), so a
//! default/pi build carries zero AMQP dependency (asserted by `cargo tree`).
//!
//! ## Protocol subset (CONCEPT:EG-KG.query.amqp-codec-arg-cursor)
//! LANDED: the connection handshake (`connection.start`/`start-ok`/`tune`/`tune-ok`/
//! `open`/`open-ok`/`close`), `channel.open`/`close`, `exchange.declare`/`delete`,
//! `queue.declare`/`bind`/`unbind`, `basic.publish` (+ content header/body frames),
//! `basic.consume`/`deliver` (a poll-driven push pump), `basic.get`/`get-ok`/
//! `get-empty`, and `basic.ack`. SASL PLAIN authentication is mandatory: the password
//! is a domain-separated HMAC derived from `GRAPH_SERVICE_AUTH_SECRET`, and the
//! authenticated principal becomes a secret-keyed pseudonymous actor reference before
//! every engine request. The direct listener is loopback-only; remote access must use
//! a TLS/mTLS identity-binding gateway. Unsupported methods fail closed.
//!
//! ## Publisher confirms + idempotent publish (CONCEPT:EG-KG.ingest.broker-reject-publish / EG-284)
//! `confirm.select` puts a channel into publisher-confirm mode: every subsequent
//! `basic.publish` is answered with a `basic.ack` (delivery-tag = the per-channel
//! 1-based publish sequence) once the broker durably accepts it, or a `basic.nack`
//! when the target exchange is unknown — mapping the EG-284 confirm surface onto the
//! AMQP wire. A publish that carries the idempotency application-headers
//! `x-producer-id` (string) + `x-producer-seq` (int) is routed through
//! `Method::PublishIdempotent` (CONCEPT:EG-KG.ingest.broker-reject-publish): the broker dedups a re-published
//! `(producer_id, seq)` against that producer's durable high-water mark, so a client
//! that retries after an ambiguous confirm gets effectively-once delivery (the
//! duplicate is dropped but STILL `basic.ack`-ed). A publish with no producer header
//! behaves exactly as before (at-least-once). The AMQP `priority` property is threaded
//! through to EG-278 priority queues on this path.
//!
//! ## Stream reads (CONCEPT:EG-KG.compute.replayable-append-log) mapping
//! AMQP 0.9.1 has no request/response frame for reading a RETAINED log by offset, so
//! EG-283 stream reads are NOT exposed over this wire — they are reached through the
//! RPC surface (`Method::StreamRead`) or a future STOMP/native frame. A `basic.consume`
//! here maps to the DESTRUCTIVE queue-claim path (EG-275/280), not a stream replay.

use std::sync::atomic::AtomicU64;

use crate::protocol::{Method, ResultPayload};
use crate::server::broker_wire::{self, invalid_data, prelude::*, BrokerProtocol};
use crate::server::broker_wire::{
    derive_password as derive_amqp_password_impl, verify_password as verify_amqp_password_impl,
};
use crate::server::ServerState;

/// Env var: when set (and the binary is built `--features amqp-wire`), the AMQP wire
/// listener binds this address (documented loopback default `127.0.0.1:5672`). Unset ⇒
/// no listener.
pub const AMQP_ADDR_ENV: &str = "EPISTEMIC_GRAPH_AMQP_ADDR";
/// Env var: the control graph broker state lives on (exchanges/bindings/queues/
/// messages). Defaults to `__commons__` (mirrors mysql-wire's default-graph idiom).
pub const AMQP_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_AMQP_GRAPH";

const WIRE: BrokerProtocol = BrokerProtocol::Amqp;

mod codec;
use codec::*;

static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Derive the SASL PLAIN password for an AMQP principal.
pub fn derive_amqp_password(secret: &str, principal: &str) -> String {
    derive_amqp_password_impl(WIRE, secret, principal)
}

fn verify_amqp_password(secret: &str, principal: &str, password: &str) -> bool {
    verify_amqp_password_impl(WIRE, secret, principal, password.as_bytes(), 4 * 1024)
}

/// Fail closed before binding the plaintext AMQP listener.
pub fn validate_startup_policy(addr: &str, secret: &str) -> std::io::Result<()> {
    crate::server::validate_direct_wire_security(addr, "amqp-wire", !secret.is_empty())
}

/// Serve the AMQP 0.9.1 wire protocol on `addr` until the listener errors.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph = std::env::var(AMQP_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    validate_startup_policy(addr, &auth_secret)?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "amqp-wire: serving authenticated AMQP 0.9.1 on loopback (broker graph '{}'; \
         remote access requires a TLS identity-binding gateway)",
        default_graph
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let g = default_graph.clone();
        let secret = auth_secret.clone();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, st, g, secret).await {
                tracing::debug!("amqp-wire connection from {peer} ended: {e}");
            }
        });
    }
}

/// Claim one deliverable message through the native broker lifecycle. Returns
/// `(node_id, routing_key, exchange, body)` or `None`.
async fn claim_one(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    queue: &str,
    consumer: &str,
    prefetch: u32,
) -> Option<(String, String, String, Vec<u8>)> {
    let payload = broker_wire::engine_call(
        state,
        graph,
        actor,
        next_req_id,
        Method::BrokerConsume {
            queue: queue.to_string(),
            group: "amqp".to_string(),
            consumer: consumer.to_string(),
            now_ms: broker_wire::current_time_ms(),
            lease_ms: BROKER_LEASE_MS,
            prefetch,
        },
    )
    .await;
    let claim = broker_wire::decode_claim(
        payload,
        MAX_AMQP_CONTENT_BYTES,
        MAX_BROKER_RESULT_ITEMS,
        None,
        None,
    )?;
    Some((claim.node_id, claim.routing_key, claim.exchange, claim.body))
}

// ── Per-connection state ────────────────────────────────────────────────

/// An active `basic.consume` subscription.
struct Consumer {
    channel: u16,
    tag: String,
    queue: String,
    consumer_id: String,
}

/// Mutable protocol state and engine context for one AMQP connection.
struct ConnectionState {
    state: Arc<RwLock<ServerState>>,
    graph: String,
    auth_secret: String,
    consumers: Vec<Consumer>,
    authenticated_actor: Option<String>,
    delivery_tag: u64,
    // delivery-tag → (queue, graph node id) for native broker acknowledgement.
    unacked: std::collections::HashMap<u64, (String, String)>,
    // CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms: channels switched into confirm mode + their
    // per-channel 1-based publish sequence (the delivery-tag returned in basic.ack/nack).
    confirm_channels: std::collections::HashSet<u16>,
    publish_seq: std::collections::HashMap<u16, u64>,
}

/// Whether the connection should continue or close after a frame.
#[derive(PartialEq, Eq)]
enum FrameAction {
    Continue,
    Close,
}

type FrameResult = std::io::Result<FrameAction>;

/// Shared wire context for handlers that only borrow connection state.
struct MethodContext<'a> {
    socket: &'a mut TcpStream,
    ctx: &'a ConnectionState,
    channel: u16,
    actor: &'a str,
    args: &'a [u8],
}

impl MethodContext<'_> {
    async fn write_method(&mut self, payload: &[u8]) -> std::io::Result<()> {
        write_frame(self.socket, FRAME_METHOD, self.channel, payload).await
    }
}

const MAX_AMQP_CONSUMERS: usize = 1_024;
const MAX_AMQP_CHANNELS: usize = 4_096;
const MAX_AMQP_UNACKED: usize = 65_536;
const MAX_BROKER_RESULT_ITEMS: usize = 1_000_000;
const BROKER_LEASE_MS: u64 = 5 * 60 * 1_000;
const BROKER_PREFETCH: u32 = 32;

fn decode_broker_result<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    broker_wire::decode_broker_result(bytes, MAX_AMQP_CONTENT_BYTES, MAX_BROKER_RESULT_ITEMS)
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    auth_secret: String,
) -> std::io::Result<()> {
    if !read_protocol_header(socket).await? {
        return Ok(());
    }
    write_frame(socket, FRAME_METHOD, 0, &build_connection_start()).await?;

    let mut ctx = ConnectionState {
        state,
        graph,
        auth_secret,
        consumers: Vec::new(),
        authenticated_actor: None,
        delivery_tag: 0,
        unacked: std::collections::HashMap::new(),
        confirm_channels: std::collections::HashSet::new(),
        publish_seq: std::collections::HashMap::new(),
    };

    loop {
        let Some(frame) = read_next_frame(socket, &mut ctx).await? else {
            break;
        };
        if handle_frame(socket, &mut ctx, frame).await? == FrameAction::Close {
            break;
        }
    }
    Ok(())
}

async fn read_next_frame(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
) -> std::io::Result<Option<Frame>> {
    loop {
        if ctx.consumers.is_empty() {
            return read_frame(socket).await;
        }
        match tokio::time::timeout(std::time::Duration::from_millis(200), read_frame(socket)).await
        {
            Ok(result) => return result,
            Err(_) => {
                let actor = authenticated_actor(ctx)?;
                // Poll timeout: pump deliveries to every active consumer.
                pump_consumers(
                    socket,
                    &ctx.state,
                    &ctx.graph,
                    &actor,
                    &ctx.consumers,
                    &mut ctx.delivery_tag,
                    &mut ctx.unacked,
                )
                .await?;
            }
        }
    }
}

async fn handle_frame(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    frame: Frame,
) -> std::io::Result<FrameAction> {
    validate_frame_kind(frame.kind)?;
    let Some(call) = parse_method(&frame.payload) else {
        return Ok(FrameAction::Continue);
    };
    if (call.class, call.method) == (C_CONNECTION, 11) {
        authenticate_connection(socket, ctx, call.args).await?;
        return Ok(FrameAction::Continue);
    }
    let actor = authenticated_actor(ctx)?;
    dispatch_method(socket, ctx, frame.channel, &actor, &call).await
}

fn validate_frame_kind(kind: u8) -> std::io::Result<()> {
    match kind {
        FRAME_HEARTBEAT => Err(invalid_data("AMQP heartbeats are not negotiated")),
        FRAME_METHOD => Ok(()),
        _ => Err(invalid_data("unsupported AMQP frame type")),
    }
}

fn authenticated_actor(protocol: &ConnectionState) -> std::io::Result<String> {
    protocol
        .authenticated_actor
        .clone()
        .ok_or_else(|| invalid_data("AMQP authentication required"))
}

async fn authenticate_connection(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    args: &[u8],
) -> std::io::Result<()> {
    if ctx.authenticated_actor.is_some() {
        return Err(invalid_data("duplicate AMQP authentication"));
    }
    let actor = authenticate_start_ok(args, &ctx.auth_secret)
        .ok_or_else(|| invalid_data("AMQP authentication failed"))?;
    ctx.authenticated_actor = Some(actor);
    write_frame(socket, FRAME_METHOD, 0, &build_connection_tune()).await
}

async fn dispatch_method(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    actor: &str,
    call: &MethodCall<'_>,
) -> std::io::Result<FrameAction> {
    match call.class {
        C_CONNECTION => handle_connection_method(socket, call.method).await,
        C_CHANNEL => handle_channel_method(socket, channel, call.method).await,
        C_EXCHANGE => {
            handle_exchange_method(
                MethodContext {
                    socket,
                    ctx,
                    channel,
                    actor,
                    args: call.args,
                },
                call.method,
            )
            .await
        }
        C_QUEUE => {
            handle_queue_method(
                MethodContext {
                    socket,
                    ctx,
                    channel,
                    actor,
                    args: call.args,
                },
                call.method,
            )
            .await
        }
        C_CONFIRM => handle_confirm_method(socket, ctx, channel, call).await,
        C_BASIC => handle_basic_method(socket, ctx, channel, actor, call).await,
        _ => Err(invalid_data("unsupported AMQP method")),
    }
}

async fn handle_connection_method(
    socket: &mut TcpStream,
    method: u16,
) -> std::io::Result<FrameAction> {
    match method {
        31 => Ok(FrameAction::Continue), // connection.tune-ok → (await open)
        40 => {
            write_frame(socket, FRAME_METHOD, 0, &build_connection_open_ok()).await?;
            Ok(FrameAction::Continue)
        }
        50 => {
            write_frame(socket, FRAME_METHOD, 0, &method_header(C_CONNECTION, 51)).await?;
            Ok(FrameAction::Close)
        }
        51 => Ok(FrameAction::Close), // connection.close-ok
        _ => Err(invalid_data("unsupported AMQP method")),
    }
}

async fn handle_channel_method(
    socket: &mut TcpStream,
    channel: u16,
    method: u16,
) -> std::io::Result<FrameAction> {
    match method {
        10 => {
            let mut p = method_header(C_CHANNEL, 11);
            put_longstr(&mut p, b""); // reserved-1
            write_frame(socket, FRAME_METHOD, channel, &p).await?;
        }
        40 => {
            write_frame(socket, FRAME_METHOD, channel, &method_header(C_CHANNEL, 41)).await?;
        }
        41 => {} // channel.close-ok
        _ => return Err(invalid_data("unsupported AMQP method")),
    }
    Ok(FrameAction::Continue)
}

async fn handle_exchange_method(method: MethodContext<'_>, method_id: u16) -> FrameResult {
    match method_id {
        10 => handle_exchange_declare(method).await,
        20 => handle_exchange_delete(method).await,
        _ => Err(invalid_data("unsupported AMQP method")),
    }
}

async fn handle_exchange_declare(mut method: MethodContext<'_>) -> FrameResult {
    let [exchange, kind] =
        parse_shortstr_args::<2>(method.args, "invalid AMQP exchange.declare arguments")?;
    let kind = if kind.is_empty() {
        "direct".into()
    } else {
        kind
    };
    let _ = broker_wire::engine_call(
        &method.ctx.state,
        &method.ctx.graph,
        method.actor,
        next_req_id,
        Method::DeclareExchange { exchange, kind },
    )
    .await;
    method.write_method(&method_header(C_EXCHANGE, 11)).await?;
    Ok(FrameAction::Continue)
}

async fn handle_exchange_delete(mut method: MethodContext<'_>) -> FrameResult {
    let [exchange] =
        parse_shortstr_args::<1>(method.args, "invalid AMQP exchange.delete arguments")?;
    let _ = broker_wire::engine_call(
        &method.ctx.state,
        &method.ctx.graph,
        method.actor,
        next_req_id,
        Method::DeleteExchange { exchange },
    )
    .await;
    method.write_method(&method_header(C_EXCHANGE, 21)).await?;
    Ok(FrameAction::Continue)
}

async fn handle_queue_method(method: MethodContext<'_>, method_id: u16) -> FrameResult {
    match method_id {
        10 => handle_queue_declare(method).await,
        20 | 50 => handle_queue_binding(method, method_id).await,
        _ => Err(invalid_data("unsupported AMQP method")),
    }
}

async fn handle_queue_declare(mut method: MethodContext<'_>) -> FrameResult {
    let [mut queue] =
        parse_shortstr_args::<1>(method.args, "invalid AMQP queue.declare arguments")?;
    if queue.is_empty() {
        queue = format!("amq.gen-{}", next_req_id());
    }
    // Ensure the queue's durable seq counter exists so it is publishable.
    let _ = broker_wire::engine_call(
        &method.ctx.state,
        &method.ctx.graph,
        method.actor,
        next_req_id,
        Method::BindQueue {
            exchange: String::new(),
            queue: queue.clone(),
            routing_key: queue.clone(),
        },
    )
    .await;
    let mut p = method_header(C_QUEUE, 11);
    put_shortstr(&mut p, queue.as_bytes());
    put_u32(&mut p, 0); // message-count
    put_u32(&mut p, 0); // consumer-count
    method.write_method(&p).await?;
    Ok(FrameAction::Continue)
}

async fn handle_queue_binding(mut method: MethodContext<'_>, method_id: u16) -> FrameResult {
    let [queue, exchange, routing_key] = parse_shortstr_args::<3>(
        method.args,
        if method_id == 20 {
            "invalid AMQP queue.bind arguments"
        } else {
            "invalid AMQP queue.unbind arguments"
        },
    )?;
    let broker_method = if method_id == 20 {
        Method::BindQueue {
            exchange,
            queue,
            routing_key,
        }
    } else {
        Method::UnbindQueue {
            exchange,
            queue,
            routing_key,
        }
    };
    let _ = broker_wire::engine_call(
        &method.ctx.state,
        &method.ctx.graph,
        method.actor,
        next_req_id,
        broker_method,
    )
    .await;
    let response_method = if method_id == 20 { 21 } else { 51 };
    method
        .write_method(&method_header(C_QUEUE, response_method))
        .await?;
    Ok(FrameAction::Continue)
}

async fn handle_confirm_method(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    call: &MethodCall<'_>,
) -> std::io::Result<FrameAction> {
    if call.method != 10 {
        return Err(invalid_data("unsupported AMQP method"));
    }
    let nowait = call
        .args
        .first()
        .map(|b| b & 0x01 != 0)
        .ok_or_else(|| invalid_data("invalid AMQP confirm.select arguments"))?;
    if !ctx.confirm_channels.contains(&channel) && ctx.confirm_channels.len() >= MAX_AMQP_CHANNELS {
        return Err(invalid_data("AMQP channel limit exceeded"));
    }
    ctx.confirm_channels.insert(channel);
    ctx.publish_seq.entry(channel).or_insert(0);
    if !nowait {
        write_frame(socket, FRAME_METHOD, channel, &method_header(C_CONFIRM, 11)).await?;
    }
    Ok(FrameAction::Continue)
}

async fn handle_basic_method(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    actor: &str,
    call: &MethodCall<'_>,
) -> std::io::Result<FrameAction> {
    match call.method {
        20 => handle_basic_consume(socket, ctx, channel, actor, call.args).await,
        40 => handle_basic_publish(socket, ctx, channel, actor, call.args).await,
        70 => handle_basic_get(socket, ctx, channel, actor, call.args).await,
        80 => handle_basic_ack(ctx, actor, call.args).await,
        _ => Err(invalid_data("unsupported AMQP method")),
    }
}

async fn handle_basic_publish(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    actor: &str,
    args: &[u8],
) -> std::io::Result<FrameAction> {
    let mut c = Cursor::new(args);
    c.u16(); // reserved-1
    let exchange = c.shortstr();
    let routing_key = c.shortstr();
    if !c.valid {
        return Err(invalid_data("invalid AMQP basic.publish arguments"));
    }
    let (props, body) = read_content(socket, channel).await?;
    // Route EVERY publish through the idempotent path — with no producer-id
    // it is byte-identical to a plain publish; with one it dedups (EG-314).
    let result = broker_wire::engine_call(
        &ctx.state,
        &ctx.graph,
        actor,
        next_req_id,
        Method::PublishIdempotent {
            exchange,
            routing_key,
            payload: body,
            producer_id: props.producer_id,
            seq: props.producer_seq.unwrap_or(0),
            priority: props.priority,
            delay_ms: None,
            ttl_ms: None,
            now_ms: None,
        },
    )
    .await;
    if ctx.confirm_channels.contains(&channel) {
        write_publish_confirmation(socket, ctx, channel, &result).await?;
    }
    Ok(FrameAction::Continue)
}

async fn write_publish_confirmation(
    socket: &mut TcpStream,
    protocol: &mut ConnectionState,
    channel: u16,
    result: &ResultPayload,
) -> std::io::Result<()> {
    let confirmed = decode_confirmed(result);
    let tag = next_publish_sequence(protocol, channel)?;
    let frame = if confirmed {
        build_basic_ack(tag, false)
    } else {
        build_basic_nack(tag, false, false)
    };
    write_frame(socket, FRAME_METHOD, channel, &frame).await
}

fn next_publish_sequence(protocol: &mut ConnectionState, channel: u16) -> std::io::Result<u64> {
    let entry = protocol.publish_seq.entry(channel).or_insert(0);
    *entry = (*entry)
        .checked_add(1)
        .ok_or_else(|| invalid_data("AMQP publish sequence exhausted"))?;
    Ok(*entry)
}

async fn handle_basic_consume(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    actor: &str,
    args: &[u8],
) -> std::io::Result<FrameAction> {
    if ctx.consumers.len() >= MAX_AMQP_CONSUMERS {
        return Err(invalid_data("AMQP consumer limit exceeded"));
    }
    let mut c = Cursor::new(args);
    c.u16();
    let queue = c.shortstr();
    let mut tag = c.shortstr();
    if !c.valid {
        return Err(invalid_data("invalid AMQP basic.consume arguments"));
    }
    if tag.is_empty() {
        tag = format!("ctag-{}", next_req_id());
    }
    let mut p = method_header(C_BASIC, 21);
    put_shortstr(&mut p, tag.as_bytes());
    write_frame(socket, FRAME_METHOD, channel, &p).await?;
    ctx.consumers.push(Consumer {
        channel,
        tag,
        queue,
        consumer_id: format!("{actor}:{}", next_req_id()),
    });
    Ok(FrameAction::Continue)
}

async fn handle_basic_get(
    socket: &mut TcpStream,
    ctx: &mut ConnectionState,
    channel: u16,
    actor: &str,
    args: &[u8],
) -> std::io::Result<FrameAction> {
    if ctx.unacked.len() >= MAX_AMQP_UNACKED {
        return Err(invalid_data("AMQP unacknowledged delivery limit exceeded"));
    }
    let mut c = Cursor::new(args);
    c.u16();
    let queue = c.shortstr();
    if !c.valid {
        return Err(invalid_data("invalid AMQP basic.get arguments"));
    }
    match claim_one(&ctx.state, &ctx.graph, actor, &queue, actor, 1).await {
        Some((node_id, rk, ex, body)) => {
            let tag = next_delivery_tag(&mut ctx.delivery_tag)?;
            ctx.unacked.insert(tag, (queue, node_id));
            let mut p = method_header(C_BASIC, 71); // get-ok
            put_u64(&mut p, tag);
            p.push(0); // redelivered = false
            put_shortstr(&mut p, ex.as_bytes());
            put_shortstr(&mut p, rk.as_bytes());
            put_u32(&mut p, 0); // message-count
            write_frame(socket, FRAME_METHOD, channel, &p).await?;
            write_content(socket, channel, &body).await?;
        }
        None => {
            let mut p = method_header(C_BASIC, 72); // get-empty
            put_shortstr(&mut p, b""); // reserved
            write_frame(socket, FRAME_METHOD, channel, &p).await?;
        }
    }
    Ok(FrameAction::Continue)
}

fn next_delivery_tag(delivery_tag: &mut u64) -> std::io::Result<u64> {
    *delivery_tag = (*delivery_tag)
        .checked_add(1)
        .ok_or_else(|| invalid_data("AMQP delivery sequence exhausted"))?;
    Ok(*delivery_tag)
}

async fn handle_basic_ack(
    ctx: &mut ConnectionState,
    actor: &str,
    args: &[u8],
) -> std::io::Result<FrameAction> {
    let mut c = Cursor::new(args);
    let tag = c.u64();
    if !c.valid {
        return Err(invalid_data("invalid AMQP basic.ack arguments"));
    }
    if let Some((queue, node_id)) = ctx.unacked.remove(&tag) {
        broker_wire::ack_message(&ctx.state, &ctx.graph, actor, next_req_id, &queue, &node_id)
            .await;
    }
    Ok(FrameAction::Continue)
}

/// Deliver up to a bounded batch of pending messages to each active consumer.
async fn pump_consumers(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    consumers: &[Consumer],
    delivery_tag: &mut u64,
    unacked: &mut std::collections::HashMap<u64, (String, String)>,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for cons in consumers {
        for _ in 0..MAX_PER_POLL {
            if unacked.len() >= MAX_AMQP_UNACKED {
                return Ok(());
            }
            let Some((node_id, rk, ex, body)) = claim_one(
                state,
                graph,
                actor,
                &cons.queue,
                &cons.consumer_id,
                BROKER_PREFETCH,
            )
            .await
            else {
                break;
            };
            let tag = next_delivery_tag(delivery_tag)?;
            unacked.insert(tag, (cons.queue.clone(), node_id));
            let mut p = method_header(C_BASIC, 60); // basic.deliver
            put_shortstr(&mut p, cons.tag.as_bytes());
            put_u64(&mut p, tag);
            p.push(0); // redelivered = false
            put_shortstr(&mut p, ex.as_bytes());
            put_shortstr(&mut p, rk.as_bytes());
            write_frame(socket, FRAME_METHOD, cons.channel, &p).await?;
            write_content(socket, cons.channel, &body).await?;
        }
    }
    Ok(())
}

/// True when an engine publish result confirms the message was durably accepted
/// (CONCEPT:EG-KG.ingest.broker-reject-publish / EG-284). Decodes the `IdempotentPublish.confirmed` flag; a
/// non-`Raw` / undecodable result fails closed and is negatively acknowledged.
fn decode_confirmed(result: &ResultPayload) -> bool {
    if let ResultPayload::Raw(bytes) = result {
        if let Some(ip) = decode_broker_result::<crate::broker::IdempotentPublish>(bytes) {
            return ip.confirmed;
        }
    }
    false
}

// ── Handshake method builders ─────────────────────────────────────────────

/// Parse and verify a `connection.start-ok` SASL PLAIN response. An optional authzid
/// must be empty or equal the authenticated principal, preventing identity confusion.
fn authenticate_start_ok(args: &[u8], secret: &str) -> Option<String> {
    let mut c = Cursor::new(args);
    let _client_properties = c.longstr_slice();
    let mechanism = c.shortstr();
    let response = c.longstr_slice();
    let _locale = c.shortstr();
    if !c.valid || c.remaining() != 0 || mechanism != "PLAIN" {
        return None;
    }
    let mut parts = response.split(|byte| *byte == 0);
    let authzid = parts.next()?;
    let principal_bytes = parts.next()?;
    let password_bytes = parts.next()?;
    if parts.next().is_some() || principal_bytes.is_empty() {
        return None;
    }
    let principal = std::str::from_utf8(principal_bytes).ok()?;
    let password = std::str::from_utf8(password_bytes).ok()?;
    if (!authzid.is_empty() && authzid != principal_bytes)
        || !verify_amqp_password(secret, principal, password)
    {
        return None;
    }
    crate::server::pseudonymous_broker_actor(secret, principal).ok()
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.query.amqp-codec-arg-cursor — codec + arg-cursor unit tests (the byte layouts the hand-rolled
    //! AMQP framing depends on). The full socket handshake is exercised by the served
    //! integration path; these pin the primitives.
    use super::*;

    #[test]
    fn eg275_shortstr_roundtrips_through_cursor() {
        let mut buf = Vec::new();
        put_shortstr(&mut buf, b"my-queue");
        put_shortstr(&mut buf, b"log.error");
        let mut c = Cursor::new(&buf);
        assert_eq!(c.shortstr(), "my-queue");
        assert_eq!(c.shortstr(), "log.error");
        assert_eq!(c.shortstr(), ""); // exhausted
    }

    #[test]
    fn eg275_cursor_reads_short_and_longlong() {
        let mut buf = Vec::new();
        put_u16(&mut buf, 0);
        put_u64(&mut buf, 0xDEAD_BEEF_1234);
        let mut c = Cursor::new(&buf);
        assert_eq!(c.u16(), 0);
        assert_eq!(c.u64(), 0xDEAD_BEEF_1234);
    }

    #[test]
    fn eg275_method_header_encodes_class_and_method() {
        let h = method_header(C_BASIC, 60);
        assert_eq!(h, vec![0x00, 60, 0x00, 60]); // class=60, method=60 (deliver)
    }

    #[test]
    fn eg275_connection_start_is_a_well_formed_method_frame() {
        let p = build_connection_start();
        // class 10, method 10, version 0.9
        assert_eq!(&p[0..4], &[0x00, 10, 0x00, 10]);
        assert_eq!(p[4], 0); // major
        assert_eq!(p[5], 9); // minor
    }

    #[test]
    fn sasl_plain_authentication_is_verified_and_identity_bound() {
        let principal = "agent:publisher";
        let password = derive_amqp_password("test", principal);
        let mut args = Vec::new();
        put_u32(&mut args, 0); // empty client-properties table
        put_shortstr(&mut args, b"PLAIN");
        let response = [
            b"".as_slice(),
            b"\0",
            principal.as_bytes(),
            b"\0",
            password.as_bytes(),
        ]
        .concat();
        put_longstr(&mut args, &response);
        put_shortstr(&mut args, b"en_US");
        let actor = authenticate_start_ok(&args, "test").unwrap();
        assert_eq!(
            actor,
            crate::server::pseudonymous_broker_actor("test", principal).unwrap()
        );
        assert!(!actor.contains(principal));
        assert!(authenticate_start_ok(&args, "other").is_none());
        assert!(authenticate_start_ok(&args, "").is_none());
    }

    #[test]
    fn startup_policy_rejects_anonymous_or_remote_amqp() {
        assert!(validate_startup_policy("127.0.0.1:5672", "").is_err());
        assert!(validate_startup_policy("0.0.0.0:5672", "test").is_err());
        assert!(validate_startup_policy("127.0.0.1:5672", "test").is_ok());
    }

    // ── CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms + idempotent-publish headers ────

    /// Assemble a content-header payload carrying an application-`headers` table
    /// (bit 0x2000) + a `priority` octet (bit 0x0800), the layout `parse_content_props`
    /// walks.
    fn content_header_with(producer_id: &str, seq: i32, priority: u8) -> Vec<u8> {
        // The field-table body: two entries.
        let mut table = Vec::new();
        put_shortstr(&mut table, b"x-producer-id");
        table.push(b'S');
        put_longstr(&mut table, producer_id.as_bytes());
        put_shortstr(&mut table, b"x-producer-seq");
        table.push(b'I');
        put_u32(&mut table, seq as u32);

        let mut p = Vec::new();
        put_u16(&mut p, C_BASIC); // class-id
        put_u16(&mut p, 0); // weight
        put_u64(&mut p, 4); // body-size
        put_u16(&mut p, 0x2000 | 0x0800); // flags: headers + priority
        put_longstr(&mut p, &table); // headers table (u32 len + body)
        p.push(priority); // priority octet
        p
    }

    #[test]
    fn eg314_parse_content_props_extracts_producer_id_seq_and_priority() {
        let payload = content_header_with("prod-7", 42, 5);
        let props = parse_content_props(&payload).unwrap();
        assert_eq!(props.producer_id.as_deref(), Some("prod-7"));
        assert_eq!(props.producer_seq, Some(42));
        assert_eq!(props.priority, 5);
    }

    #[test]
    fn eg314_parse_content_props_absent_headers_is_default() {
        // Minimal header: class/weight/body-size + zero property flags → no producer
        // stamp, priority 0 (the at-least-once, unchanged path).
        let mut p = Vec::new();
        put_u16(&mut p, C_BASIC);
        put_u16(&mut p, 0);
        put_u64(&mut p, 0);
        put_u16(&mut p, 0x0000);
        let props = parse_content_props(&p).unwrap();
        assert!(props.producer_id.is_none());
        assert!(props.producer_seq.is_none());
        assert_eq!(props.priority, 0);
    }

    #[test]
    fn eg314_field_value_decodes_string_and_int_tags() {
        // 'S' longstr string then 'l' 64-bit int.
        let mut buf = Vec::new();
        buf.push(b'S');
        put_longstr(&mut buf, b"hello");
        buf.push(b'l');
        put_u64(&mut buf, 9);
        let mut c = Cursor::new(&buf);
        match c.field_value() {
            Some(FieldVal::Str(s)) => assert_eq!(s, "hello"),
            _ => panic!("expected string"),
        }
        match c.field_value() {
            Some(FieldVal::Int(n)) => assert_eq!(n, 9),
            _ => panic!("expected int"),
        }
        // An unknown tag ends the scan.
        let bad = vec![b'?'];
        assert!(Cursor::new(&bad).field_value().is_none());
    }

    #[test]
    fn eg314_basic_ack_frame_carries_delivery_tag() {
        let f = build_basic_ack(3, false);
        // class 60, method 80.
        assert_eq!(&f[0..4], &[0x00, 60, 0x00, 80]);
        assert_eq!(u64::from_be_bytes(f[4..12].try_into().unwrap()), 3);
        assert_eq!(f[12], 0x00); // multiple = false
    }

    #[test]
    fn eg314_basic_nack_frame_sets_requeue_bit() {
        let f = build_basic_nack(9, false, true);
        // class 60, method 120.
        assert_eq!(&f[0..4], &[0x00, 60, 0x00, 120]);
        assert_eq!(u64::from_be_bytes(f[4..12].try_into().unwrap()), 9);
        assert_eq!(f[12], 0x02); // multiple=0, requeue=1
    }

    #[test]
    fn eg314_decode_confirmed_reads_idempotent_publish_flag() {
        let confirmed = ResultPayload::of_ref::<
            eg_types::result_contract::messaging::PublishIdempotent,
        >(&crate::broker::IdempotentPublish {
            confirmed: true,
            duplicate: false,
            delivered: 1,
        })
        .unwrap();
        assert!(decode_confirmed(&confirmed));
        let nacked =
            ResultPayload::of_ref::<eg_types::result_contract::messaging::PublishIdempotent>(
                &crate::broker::IdempotentPublish {
                    confirmed: false,
                    duplicate: false,
                    delivered: 0,
                },
            )
            .unwrap();
        assert!(!decode_confirmed(&nacked));
    }
}
