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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::dispatch::dispatch_authenticated_broker_actor;
use crate::server::ServerState;

/// Env var: when set (and the binary is built `--features amqp-wire`), the AMQP wire
/// listener binds this address (documented loopback default `127.0.0.1:5672`). Unset ⇒
/// no listener.
pub const AMQP_ADDR_ENV: &str = "EPISTEMIC_GRAPH_AMQP_ADDR";
/// Env var: the control graph broker state lives on (exchanges/bindings/queues/
/// messages). Defaults to `__commons__` (mirrors mysql-wire's default-graph idiom).
pub const AMQP_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_AMQP_GRAPH";

// ── Frame types + class/method ids (AMQP 0.9.1) ──────────────────────────
const FRAME_METHOD: u8 = 1;
const FRAME_HEADER: u8 = 2;
const FRAME_BODY: u8 = 3;
const FRAME_HEARTBEAT: u8 = 8;
const FRAME_END: u8 = 0xCE;

const C_CONNECTION: u16 = 10;
const C_CHANNEL: u16 = 20;
const C_EXCHANGE: u16 = 40;
const C_QUEUE: u16 = 50;
const C_BASIC: u16 = 60;
/// AMQP `confirm` class (CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms).
const C_CONFIRM: u16 = 85;

static REQ_ID: AtomicU64 = AtomicU64::new(1);
type HmacSha256 = Hmac<Sha256>;

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Derive the SASL PLAIN password for an AMQP principal.
pub fn derive_amqp_password(secret: &str, principal: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"amqp:");
    mac.update(principal.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_amqp_password(secret: &str, principal: &str, password: &str) -> bool {
    if secret.is_empty()
        || principal.is_empty()
        || principal.len() > 4 * 1024
        || password.len() != 64
    {
        return false;
    }
    let Ok(candidate) = hex::decode(password) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"amqp:");
    mac.update(principal.as_bytes());
    mac.verify_slice(&candidate).is_ok()
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

// ── Engine bridge ─────────────────────────────────────────────────────────

/// Run one broker `Method` through the engine dispatch against the broker graph,
/// authenticating exactly as an RPC client would (compute the per-request HMAC token).
async fn engine_call(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    method: Method,
) -> ResultPayload {
    let id = next_req_id();
    let req = Request {
        id,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: None,
        method,
    };
    let resp = dispatch_authenticated_broker_actor(state, req, actor).await;
    resp.result.unwrap_or(ResultPayload::Bool(false))
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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
    let payload = engine_call(
        state,
        graph,
        actor,
        Method::BrokerConsume {
            queue: queue.to_string(),
            group: "amqp".to_string(),
            consumer: consumer.to_string(),
            now_ms: current_time_ms(),
            lease_ms: BROKER_LEASE_MS,
            prefetch,
        },
    )
    .await;
    let ResultPayload::Raw(bytes) = payload else {
        return None;
    };
    let claimed: Option<(String, serde_json::Value)> = decode_broker_result(&bytes)?;
    let (id, props) = claimed?;
    let rk = props
        .get("routing_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let ex = props
        .get("exchange")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let body = props
        .get("payload")
        .and_then(|v| v.as_str())
        .and_then(crate::broker::hex_decode)
        .unwrap_or_default();
    Some((id, rk, ex, body))
}

/// Finalize a delivered message through the native broker acknowledgement path.
async fn ack_message(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    queue: &str,
    node_id: &str,
) {
    let _ = engine_call(
        state,
        graph,
        actor,
        Method::BrokerAck {
            queue: queue.to_string(),
            node_id: node_id.to_string(),
        },
    )
    .await;
}

// ── Per-connection state ────────────────────────────────────────────────

/// One raw AMQP frame off the wire.
struct Frame {
    kind: u8,
    channel: u16,
    payload: Vec<u8>,
}

/// A raised AMQP method: class/method ids + the argument bytes.
struct MethodCall<'a> {
    class: u16,
    method: u16,
    args: &'a [u8],
}

/// An active `basic.consume` subscription.
struct Consumer {
    channel: u16,
    tag: String,
    queue: String,
    consumer_id: String,
}

/// Hard per-frame allocation ceiling for untrusted AMQP size prefixes.
const MAX_AMQP_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// A content body is assembled from multiple frames, so it needs an independent
/// aggregate cap rather than relying on the per-frame ceiling.
const MAX_AMQP_CONTENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_AMQP_CONSUMERS: usize = 1_024;
const MAX_AMQP_CHANNELS: usize = 4_096;
const MAX_AMQP_UNACKED: usize = 65_536;
const MAX_AMQP_HEADER_FIELDS: usize = 4_096;
const MAX_BROKER_RESULT_ITEMS: usize = 1_000_000;
const BROKER_LEASE_MS: u64 = 5 * 60 * 1_000;
const BROKER_PREFETCH: u32 = 32;

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn decode_broker_result<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AMQP_CONTENT_BYTES,
            MAX_BROKER_RESULT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .ok()
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    auth_secret: String,
) -> std::io::Result<()> {
    // ── Protocol header ──
    let mut hdr = [0u8; 8];
    socket.read_exact(&mut hdr).await?;
    if &hdr != b"AMQP\x00\x00\x09\x01" {
        // Tell the client the version we speak, then close.
        socket.write_all(b"AMQP\x00\x00\x09\x01").await?;
        return Ok(());
    }
    // ── connection.start ──
    write_frame(socket, FRAME_METHOD, 0, &build_connection_start()).await?;

    let mut consumers: Vec<Consumer> = Vec::new();
    let mut authenticated_actor: Option<String> = None;
    let mut delivery_tag: u64 = 0;
    // delivery-tag → (queue, graph node id) for native broker acknowledgement.
    let mut unacked: std::collections::HashMap<u64, (String, String)> =
        std::collections::HashMap::new();
    // CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms: channels switched into confirm mode + their
    // per-channel 1-based publish sequence (the delivery-tag returned in basic.ack/nack).
    let mut confirm_channels: std::collections::HashSet<u16> = std::collections::HashSet::new();
    let mut publish_seq: std::collections::HashMap<u16, u64> = std::collections::HashMap::new();

    loop {
        // When consumers are active, bound the read so the poll pump can run; else
        // block until the next client frame.
        let frame = if consumers.is_empty() {
            match read_frame(socket).await? {
                Some(f) => f,
                None => break, // clean EOF
            }
        } else {
            match tokio::time::timeout(std::time::Duration::from_millis(200), read_frame(socket))
                .await
            {
                Ok(Ok(Some(f))) => f,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    let actor = authenticated_actor
                        .as_deref()
                        .ok_or_else(|| invalid_data("AMQP authentication required"))?;
                    // Poll timeout: pump deliveries to every active consumer.
                    pump_consumers(
                        socket,
                        &state,
                        &graph,
                        actor,
                        &consumers,
                        &mut delivery_tag,
                        &mut unacked,
                    )
                    .await?;
                    continue;
                }
            }
        };

        match frame.kind {
            FRAME_HEARTBEAT => return Err(invalid_data("AMQP heartbeats are not negotiated")),
            FRAME_METHOD => {}
            _ => return Err(invalid_data("unsupported AMQP frame type")),
        }
        let Some(mc) = parse_method(&frame.payload) else {
            continue;
        };
        let ch = frame.channel;

        if (mc.class, mc.method) == (C_CONNECTION, 11) {
            if authenticated_actor.is_some() {
                return Err(invalid_data("duplicate AMQP authentication"));
            }
            let actor = authenticate_start_ok(mc.args, &auth_secret)
                .ok_or_else(|| invalid_data("AMQP authentication failed"))?;
            authenticated_actor = Some(actor);
            write_frame(socket, FRAME_METHOD, 0, &build_connection_tune()).await?;
            continue;
        }
        let actor = authenticated_actor
            .as_deref()
            .ok_or_else(|| invalid_data("AMQP authentication required"))?;

        match (mc.class, mc.method) {
            // connection.tune-ok → (await open)
            (C_CONNECTION, 31) => {}
            // connection.open → open-ok
            (C_CONNECTION, 40) => {
                write_frame(socket, FRAME_METHOD, 0, &build_connection_open_ok()).await?;
            }
            // connection.close → close-ok, then done
            (C_CONNECTION, 50) => {
                write_frame(socket, FRAME_METHOD, 0, &method_header(C_CONNECTION, 51)).await?;
                break;
            }
            (C_CONNECTION, 51) => break, // close-ok
            // channel.open → open-ok
            (C_CHANNEL, 10) => {
                let mut p = method_header(C_CHANNEL, 11);
                put_longstr(&mut p, b""); // reserved-1
                write_frame(socket, FRAME_METHOD, ch, &p).await?;
            }
            // channel.close → close-ok
            (C_CHANNEL, 40) => {
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_CHANNEL, 41)).await?;
            }
            (C_CHANNEL, 41) => {} // channel.close-ok
            // exchange.declare
            (C_EXCHANGE, 10) => {
                let mut c = Cursor::new(mc.args);
                c.u16(); // reserved-1
                let exchange = c.shortstr();
                let kind = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP exchange.declare arguments"));
                }
                let kind = if kind.is_empty() {
                    "direct".into()
                } else {
                    kind
                };
                let _ = engine_call(
                    &state,
                    &graph,
                    actor,
                    Method::DeclareExchange { exchange, kind },
                )
                .await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_EXCHANGE, 11)).await?;
            }
            // exchange.delete
            (C_EXCHANGE, 20) => {
                let mut c = Cursor::new(mc.args);
                c.u16();
                let exchange = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP exchange.delete arguments"));
                }
                let _ =
                    engine_call(&state, &graph, actor, Method::DeleteExchange { exchange }).await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_EXCHANGE, 21)).await?;
            }
            // queue.declare
            (C_QUEUE, 10) => {
                let mut c = Cursor::new(mc.args);
                c.u16();
                let mut queue = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP queue.declare arguments"));
                }
                if queue.is_empty() {
                    queue = format!("amq.gen-{}", next_req_id());
                }
                // Ensure the queue's durable seq counter exists so it is publishable.
                let _ = engine_call(
                    &state,
                    &graph,
                    actor,
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
                write_frame(socket, FRAME_METHOD, ch, &p).await?;
            }
            // queue.bind
            (C_QUEUE, 20) => {
                let mut c = Cursor::new(mc.args);
                c.u16();
                let queue = c.shortstr();
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP queue.bind arguments"));
                }
                let _ = engine_call(
                    &state,
                    &graph,
                    actor,
                    Method::BindQueue {
                        exchange,
                        queue,
                        routing_key,
                    },
                )
                .await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_QUEUE, 21)).await?;
            }
            // queue.unbind
            (C_QUEUE, 50) => {
                let mut c = Cursor::new(mc.args);
                c.u16();
                let queue = c.shortstr();
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP queue.unbind arguments"));
                }
                let _ = engine_call(
                    &state,
                    &graph,
                    actor,
                    Method::UnbindQueue {
                        exchange,
                        queue,
                        routing_key,
                    },
                )
                .await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_QUEUE, 51)).await?;
            }
            // confirm.select → confirm.select-ok (CONCEPT:EG-KG.ingest.broker-reject-publish): enter confirm mode.
            (C_CONFIRM, 10) => {
                let nowait = mc
                    .args
                    .first()
                    .map(|b| b & 0x01 != 0)
                    .ok_or_else(|| invalid_data("invalid AMQP confirm.select arguments"))?;
                if !confirm_channels.contains(&ch) && confirm_channels.len() >= MAX_AMQP_CHANNELS {
                    return Err(invalid_data("AMQP channel limit exceeded"));
                }
                confirm_channels.insert(ch);
                publish_seq.entry(ch).or_insert(0);
                if !nowait {
                    write_frame(socket, FRAME_METHOD, ch, &method_header(C_CONFIRM, 11)).await?;
                }
            }
            // basic.publish → read content header (+ idempotency headers) + body, then
            // publish; in confirm mode answer basic.ack / basic.nack (CONCEPT:EG-KG.ingest.broker-reject-publish).
            (C_BASIC, 40) => {
                let mut c = Cursor::new(mc.args);
                c.u16(); // reserved-1
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP basic.publish arguments"));
                }
                let (props, body) = read_content(socket, ch).await?;
                // Route EVERY publish through the idempotent path — with no producer-id
                // it is byte-identical to a plain publish; with one it dedups (EG-314).
                let result = engine_call(
                    &state,
                    &graph,
                    actor,
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
                if confirm_channels.contains(&ch) {
                    let confirmed = decode_confirmed(&result);
                    let tag = {
                        let e = publish_seq.entry(ch).or_insert(0);
                        *e = (*e)
                            .checked_add(1)
                            .ok_or_else(|| invalid_data("AMQP publish sequence exhausted"))?;
                        *e
                    };
                    let frame = if confirmed {
                        build_basic_ack(tag, false)
                    } else {
                        build_basic_nack(tag, false, false)
                    };
                    write_frame(socket, FRAME_METHOD, ch, &frame).await?;
                }
            }
            // basic.consume → consume-ok, register subscription
            (C_BASIC, 20) => {
                if consumers.len() >= MAX_AMQP_CONSUMERS {
                    return Err(invalid_data("AMQP consumer limit exceeded"));
                }
                let mut c = Cursor::new(mc.args);
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
                write_frame(socket, FRAME_METHOD, ch, &p).await?;
                consumers.push(Consumer {
                    channel: ch,
                    tag,
                    queue,
                    consumer_id: format!("{actor}:{}", next_req_id()),
                });
            }
            // basic.get → get-ok + content, or get-empty
            (C_BASIC, 70) => {
                if unacked.len() >= MAX_AMQP_UNACKED {
                    return Err(invalid_data("AMQP unacknowledged delivery limit exceeded"));
                }
                let mut c = Cursor::new(mc.args);
                c.u16();
                let queue = c.shortstr();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP basic.get arguments"));
                }
                match claim_one(&state, &graph, actor, &queue, actor, 1).await {
                    Some((node_id, rk, ex, body)) => {
                        delivery_tag = delivery_tag
                            .checked_add(1)
                            .ok_or_else(|| invalid_data("AMQP delivery sequence exhausted"))?;
                        unacked.insert(delivery_tag, (queue, node_id));
                        let mut p = method_header(C_BASIC, 71); // get-ok
                        put_u64(&mut p, delivery_tag);
                        p.push(0); // redelivered = false
                        put_shortstr(&mut p, ex.as_bytes());
                        put_shortstr(&mut p, rk.as_bytes());
                        put_u32(&mut p, 0); // message-count
                        write_frame(socket, FRAME_METHOD, ch, &p).await?;
                        write_content(socket, ch, &body).await?;
                    }
                    None => {
                        let mut p = method_header(C_BASIC, 72); // get-empty
                        put_shortstr(&mut p, b""); // reserved
                        write_frame(socket, FRAME_METHOD, ch, &p).await?;
                    }
                }
            }
            // basic.ack
            (C_BASIC, 80) => {
                let mut c = Cursor::new(mc.args);
                let tag = c.u64();
                if !c.valid {
                    return Err(invalid_data("invalid AMQP basic.ack arguments"));
                }
                if let Some((queue, node_id)) = unacked.remove(&tag) {
                    ack_message(&state, &graph, actor, &queue, &node_id).await;
                }
            }
            _ => return Err(invalid_data("unsupported AMQP method")),
        }
    }
    Ok(())
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
            *delivery_tag = (*delivery_tag)
                .checked_add(1)
                .ok_or_else(|| invalid_data("AMQP delivery sequence exhausted"))?;
            unacked.insert(*delivery_tag, (cons.queue.clone(), node_id));
            let mut p = method_header(C_BASIC, 60); // basic.deliver
            put_shortstr(&mut p, cons.tag.as_bytes());
            put_u64(&mut p, *delivery_tag);
            p.push(0); // redelivered = false
            put_shortstr(&mut p, ex.as_bytes());
            put_shortstr(&mut p, rk.as_bytes());
            write_frame(socket, FRAME_METHOD, cons.channel, &p).await?;
            write_content(socket, cons.channel, &body).await?;
        }
    }
    Ok(())
}

/// Idempotency / priority fields lifted from a `basic.publish` content header
/// (CONCEPT:EG-KG.ingest.broker-reject-publish). All optional — an absent header leaves the default (no producer
/// stamp, priority 0), so a publish with no properties behaves exactly as before.
#[derive(Default)]
struct ContentProps {
    /// `x-producer-id` application header (the idempotent-publish producer identity).
    producer_id: Option<String>,
    /// `x-producer-seq` application header (the per-producer monotonic sequence).
    producer_seq: Option<i64>,
    /// AMQP basic `priority` property → EG-278 priority band.
    priority: i64,
}

/// Read the content-header frame (body size + basic-properties) then accumulate body
/// frames. Parses the idempotency application-headers + priority (CONCEPT:EG-KG.ingest.broker-reject-publish).
async fn read_content(
    socket: &mut TcpStream,
    expected_channel: u16,
) -> std::io::Result<(ContentProps, Vec<u8>)> {
    // Content header frame.
    let header = match read_frame(socket).await? {
        Some(f) if f.kind == FRAME_HEADER && f.channel == expected_channel => f,
        _ => return Err(invalid_data("invalid AMQP content header")),
    };
    // header payload: class(2) weight(2) body-size(8) property-flags(2) properties…
    if header.payload.len() < 12 {
        return Err(invalid_data("invalid AMQP content header"));
    }
    if u16::from_be_bytes([header.payload[0], header.payload[1]]) != C_BASIC
        || header.payload[2] != 0
        || header.payload[3] != 0
    {
        return Err(invalid_data("invalid AMQP content header"));
    }
    let declared_body_size = u64::from_be_bytes([
        header.payload[4],
        header.payload[5],
        header.payload[6],
        header.payload[7],
        header.payload[8],
        header.payload[9],
        header.payload[10],
        header.payload[11],
    ]);
    let body_size = usize::try_from(declared_body_size)
        .ok()
        .filter(|size| *size <= MAX_AMQP_CONTENT_BYTES)
        .ok_or_else(|| invalid_data("AMQP content body exceeds the resource limit"))?;
    let props = parse_content_props(&header.payload)
        .ok_or_else(|| invalid_data("invalid AMQP content properties"))?;
    // Do not reserve the entire attacker-declared size before any body bytes
    // arrive. Capacity grows only with frames that have actually been read.
    let mut body = Vec::new();
    while body.len() < body_size {
        match read_frame(socket).await? {
            Some(f) if f.kind == FRAME_BODY && f.channel == expected_channel => {
                if f.payload.is_empty() {
                    return Err(invalid_data("empty AMQP content body frame"));
                }
                if f.payload.len() > body_size - body.len() {
                    return Err(invalid_data("AMQP content body exceeds its declared size"));
                }
                if body.is_empty() {
                    body = f.payload;
                } else {
                    body.extend_from_slice(&f.payload);
                }
            }
            _ => return Err(invalid_data("incomplete AMQP content body")),
        }
    }
    Ok((props, body))
}

/// Parse a `basic.publish` content-header payload for the idempotency headers +
/// priority (CONCEPT:EG-KG.ingest.broker-reject-publish). Walks the AMQP basic-properties in flag order to reach
/// the application-`headers` table (bit `0x2000`) and the `priority` octet (`0x0800`).
/// A multi-word property-flags preamble (continuation bit `0x0001`, vanishingly rare
/// for a publish) is not decoded — extraction is skipped and the publish still lands.
fn parse_content_props(payload: &[u8]) -> Option<ContentProps> {
    let mut props = ContentProps::default();
    if payload.len() < 14 {
        return None;
    }
    let flags = u16::from_be_bytes([payload[12], payload[13]]);
    if flags & 0x0001 != 0 {
        return None; // unsupported multi-word flags must not be partially accepted
    }
    let mut c = Cursor::new(&payload[14..]);
    if flags & 0x8000 != 0 {
        let _content_type = c.shortstr();
    }
    if flags & 0x4000 != 0 {
        let _content_encoding = c.shortstr();
    }
    if flags & 0x2000 != 0 {
        let table = c.longstr_slice();
        if !parse_headers_table(table, &mut props) {
            return None;
        }
    }
    if flags & 0x1000 != 0 {
        let _delivery_mode = c.u8();
    }
    if flags & 0x0800 != 0 {
        props.priority = c.u8() as i64;
    }
    if flags & 0x0400 != 0 {
        let _correlation_id = c.shortstr();
    }
    if flags & 0x0200 != 0 {
        let _reply_to = c.shortstr();
    }
    if flags & 0x0100 != 0 {
        let _expiration = c.shortstr();
    }
    if flags & 0x0080 != 0 {
        let _message_id = c.shortstr();
    }
    if flags & 0x0040 != 0 {
        let _timestamp = c.u64();
    }
    if flags & 0x0020 != 0 {
        let _message_type = c.shortstr();
    }
    if flags & 0x0010 != 0 {
        let _user_id = c.shortstr();
    }
    if flags & 0x0008 != 0 {
        let _app_id = c.shortstr();
    }
    if flags & 0x0004 != 0 {
        let _cluster_id = c.shortstr();
    }
    (c.valid && c.remaining() == 0).then_some(props)
}

/// Scan an AMQP field-table for the idempotency headers (CONCEPT:EG-KG.ingest.broker-reject-publish): `x-producer-id`
/// (a string value) and `x-producer-seq` (an int, or a numeric string). Unknown value
/// types whose width can't be determined end the scan (the already-found keys stand).
fn parse_headers_table(bytes: &[u8], props: &mut ContentProps) -> bool {
    let mut c = Cursor::new(bytes);
    let mut fields = 0usize;
    while c.remaining() > 0 {
        fields += 1;
        if fields > MAX_AMQP_HEADER_FIELDS {
            return false;
        }
        let name = c.shortstr();
        let Some(val) = c.field_value() else {
            return false;
        };
        match name.as_str() {
            "x-producer-id" => {
                if let FieldVal::Str(s) = val {
                    props.producer_id = Some(s);
                }
            }
            "x-producer-seq" => match val {
                FieldVal::Int(n) => props.producer_seq = Some(n),
                FieldVal::Str(s) => props.producer_seq = s.parse().ok(),
                FieldVal::Skip => {}
            },
            _ => {}
        }
    }
    c.valid
}

/// A decoded AMQP field-table value we care about (CONCEPT:EG-KG.ingest.broker-reject-publish) — a string, an
/// integer, or a correctly-sized value we skip over.
enum FieldVal {
    Str(String),
    Int(i64),
    Skip,
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

/// Build a server `basic.ack` (class 60 / method 80): delivery-tag + `multiple` bit
/// (CONCEPT:EG-KG.ingest.broker-reject-publish publisher confirms).
fn build_basic_ack(delivery_tag: u64, multiple: bool) -> Vec<u8> {
    let mut p = method_header(C_BASIC, 80);
    put_u64(&mut p, delivery_tag);
    p.push(u8::from(multiple));
    p
}

/// Build a server `basic.nack` (class 60 / method 120): delivery-tag + `multiple`/
/// `requeue` bits (CONCEPT:EG-KG.ingest.broker-reject-publish — a publish the broker could not accept).
fn build_basic_nack(delivery_tag: u64, multiple: bool, requeue: bool) -> Vec<u8> {
    let mut p = method_header(C_BASIC, 120);
    put_u64(&mut p, delivery_tag);
    let mut bits = 0u8;
    if multiple {
        bits |= 0x01;
    }
    if requeue {
        bits |= 0x02;
    }
    p.push(bits);
    p
}

/// Emit a content header + single body frame for `body` on `channel`.
async fn write_content(socket: &mut TcpStream, channel: u16, body: &[u8]) -> std::io::Result<()> {
    let mut hp = Vec::new();
    put_u16(&mut hp, C_BASIC); // class-id
    put_u16(&mut hp, 0); // weight
    put_u64(&mut hp, body.len() as u64); // body-size
    put_u16(&mut hp, 0); // property-flags (no properties)
    write_frame(socket, FRAME_HEADER, channel, &hp).await?;
    if !body.is_empty() {
        write_frame(socket, FRAME_BODY, channel, body).await?;
    }
    Ok(())
}

// ── Frame codec ───────────────────────────────────────────────────────────

async fn read_frame(socket: &mut TcpStream) -> std::io::Result<Option<Frame>> {
    let mut head = [0u8; 7];
    // A clean EOF at a frame boundary ⇒ None (connection closed).
    if let Err(e) = socket.read_exact(&mut head).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    let kind = head[0];
    let channel = u16::from_be_bytes([head[1], head[2]]);
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    if !matches!(
        kind,
        FRAME_METHOD | FRAME_HEADER | FRAME_BODY | FRAME_HEARTBEAT
    ) {
        return Err(invalid_data("invalid AMQP frame type"));
    }
    if kind == FRAME_HEARTBEAT && (channel != 0 || size != 0) {
        return Err(invalid_data("invalid AMQP heartbeat frame"));
    }
    if size > MAX_AMQP_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "AMQP frame exceeds the resource limit",
        ));
    }
    let mut payload = vec![0u8; size];
    socket.read_exact(&mut payload).await?;
    let mut end = [0u8; 1];
    socket.read_exact(&mut end).await?;
    if end[0] != FRAME_END {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad AMQP frame-end octet",
        ));
    }
    Ok(Some(Frame {
        kind,
        channel,
        payload,
    }))
}

async fn write_frame(
    socket: &mut TcpStream,
    kind: u8,
    channel: u16,
    payload: &[u8],
) -> std::io::Result<()> {
    if payload.len() > MAX_AMQP_FRAME_BYTES || u32::try_from(payload.len()).is_err() {
        return Err(invalid_data("AMQP output frame exceeds the resource limit"));
    }
    let capacity = payload
        .len()
        .checked_add(8)
        .ok_or_else(|| invalid_data("AMQP output frame length overflow"))?;
    let mut buf = Vec::with_capacity(capacity);
    buf.push(kind);
    put_u16(&mut buf, channel);
    put_u32(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    buf.push(FRAME_END);
    socket.write_all(&buf).await
}

fn parse_method(payload: &[u8]) -> Option<MethodCall<'_>> {
    if payload.len() < 4 {
        return None;
    }
    let class = u16::from_be_bytes([payload[0], payload[1]]);
    let method = u16::from_be_bytes([payload[2], payload[3]]);
    Some(MethodCall {
        class,
        method,
        args: &payload[4..],
    })
}

/// Method-frame payload header: class-id + method-id.
fn method_header(class: u16, method: u16) -> Vec<u8> {
    let mut v = Vec::with_capacity(4);
    put_u16(&mut v, class);
    put_u16(&mut v, method);
    v
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

fn build_connection_start() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 10);
    p.push(0); // version-major
    p.push(9); // version-minor
    put_u32(&mut p, 0); // server-properties: empty field-table
    put_longstr(&mut p, b"PLAIN"); // mechanisms
    put_longstr(&mut p, b"en_US"); // locales
    p
}

fn build_connection_tune() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 30);
    put_u16(&mut p, 0); // channel-max (0 = no limit)
    put_u32(&mut p, 131_072); // frame-max
    put_u16(&mut p, 0); // heartbeat (0 = off)
    p
}

fn build_connection_open_ok() -> Vec<u8> {
    let mut p = method_header(C_CONNECTION, 41);
    put_shortstr(&mut p, b""); // reserved-1
    p
}

// ── Primitive encoders ──────────────────────────────────────────────────

fn put_u16(v: &mut Vec<u8>, x: u16) {
    v.extend_from_slice(&x.to_be_bytes());
}
fn put_u32(v: &mut Vec<u8>, x: u32) {
    v.extend_from_slice(&x.to_be_bytes());
}
fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_be_bytes());
}
fn put_shortstr(v: &mut Vec<u8>, s: &[u8]) {
    v.push(s.len().min(255) as u8);
    v.extend_from_slice(&s[..s.len().min(255)]);
}
fn put_longstr(v: &mut Vec<u8>, s: &[u8]) {
    put_u32(v, s.len() as u32);
    v.extend_from_slice(s);
}

/// A minimal read cursor over AMQP method argument bytes.
struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
    valid: bool,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self {
            b,
            i: 0,
            valid: true,
        }
    }
    fn u8(&mut self) -> u8 {
        if self.i >= self.b.len() {
            self.valid = false;
            return 0;
        }
        let x = self.b[self.i];
        self.i += 1;
        x
    }
    fn u16(&mut self) -> u16 {
        let Some(end) = self.i.checked_add(2).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let x = u16::from_be_bytes([self.b[self.i], self.b[self.i + 1]]);
        self.i = end;
        x
    }
    fn u32(&mut self) -> u32 {
        let Some(end) = self.i.checked_add(4).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let mut a = [0u8; 4];
        a.copy_from_slice(&self.b[self.i..end]);
        self.i = end;
        u32::from_be_bytes(a)
    }
    fn u64(&mut self) -> u64 {
        let Some(end) = self.i.checked_add(8).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return 0;
        };
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.b[self.i..end]);
        self.i = end;
        u64::from_be_bytes(a)
    }
    fn shortstr(&mut self) -> String {
        if self.i >= self.b.len() {
            self.valid = false;
            return String::new();
        }
        let len = self.b[self.i] as usize;
        self.i += 1;
        let Some(end) = self.i.checked_add(len).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return String::new();
        };
        let s = match std::str::from_utf8(&self.b[self.i..end]) {
            Ok(value) => value.to_owned(),
            Err(_) => {
                self.valid = false;
                String::new()
            }
        };
        self.i = end;
        s
    }
    /// Bytes remaining in the buffer.
    fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.i)
    }
    /// Read exactly `n` bytes, failing the cursor on truncation.
    fn take(&mut self, n: usize) -> &'a [u8] {
        let Some(end) = self.i.checked_add(n).filter(|end| *end <= self.b.len()) else {
            self.valid = false;
            self.i = self.b.len();
            return &[];
        };
        let out = &self.b[self.i..end];
        self.i = end;
        out
    }
    /// A `u32`-length-prefixed byte block (AMQP `longstr` / field-table framing).
    fn longstr_slice(&mut self) -> &'a [u8] {
        let len = self.u32() as usize;
        self.take(len)
    }
    /// Decode one AMQP field-table value by its 1-byte type tag (CONCEPT:EG-KG.ingest.broker-reject-publish). Returns
    /// `None` when the tag is unknown (its width is undeterminable → the caller stops).
    fn field_value(&mut self) -> Option<FieldVal> {
        if self.remaining() == 0 {
            return None;
        }
        let tag = self.u8();
        let v = match tag {
            b't' => {
                self.u8();
                FieldVal::Skip // boolean
            }
            b'b' => FieldVal::Int(self.u8() as i8 as i64),
            b'B' => FieldVal::Int(self.u8() as i64),
            b'U' | b's' => {
                let n = self.u16() as i16;
                FieldVal::Int(n as i64)
            }
            b'u' => FieldVal::Int(self.u16() as i64),
            b'I' => FieldVal::Int(self.u32() as i32 as i64),
            b'i' => FieldVal::Int(self.u32() as i64),
            b'L' | b'l' => FieldVal::Int(self.u64() as i64),
            b'T' => FieldVal::Int(self.u64() as i64), // timestamp
            b'f' => {
                self.u32();
                FieldVal::Skip // float
            }
            b'd' => {
                self.u64();
                FieldVal::Skip // double
            }
            b'D' => {
                self.u8(); // decimal scale
                self.u32(); // decimal value
                FieldVal::Skip
            }
            b'S' => {
                let bytes = self.longstr_slice();
                let value = match std::str::from_utf8(bytes) {
                    Ok(value) if self.valid => value,
                    _ => {
                        self.valid = false;
                        return None;
                    }
                };
                FieldVal::Str(value.to_owned())
            }
            b'x' | b'F' | b'A' => {
                let _ = self.longstr_slice(); // byte-array / nested table / array
                FieldVal::Skip
            }
            b'V' => FieldVal::Skip, // void
            _ => return None,       // unknown type — width unknown, stop the scan
        };
        self.valid.then_some(v)
    }
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
        let confirmed = ResultPayload::raw(&crate::broker::IdempotentPublish {
            confirmed: true,
            duplicate: false,
            delivered: 1,
        });
        assert!(decode_confirmed(&confirmed));
        let nacked = ResultPayload::raw(&crate::broker::IdempotentPublish {
            confirmed: false,
            duplicate: false,
            delivered: 0,
        });
        assert!(!decode_confirmed(&nacked));
    }
}
