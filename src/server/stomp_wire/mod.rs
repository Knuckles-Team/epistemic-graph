//! STOMP 1.2 wire-protocol listener (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit) — a HAND-ROLLED STOMP text-frame
//! server that lets a standard STOMP client (stomp.py, stompjs, an ActiveMQ/RabbitMQ
//! STOMP consumer) speak to the native message broker built on the KG-2.303 work-queue.
//!
//! ## What this is (and is NOT)
//! Like the amqp-wire / mqtt-wire / pgwire shims, this is an ADAPTER, not a second
//! broker. Every exchange/binding/queue/message lives as graph nodes on a control graph
//! (`crate::broker`); this module only frames STOMP 1.2 text frames on the wire and maps
//! each command onto the SAME broker primitives THROUGH the engine dispatch
//! (`crate::server::dispatch::dispatch`) that the AMQP wire (CONCEPT:EG-KG.compute.message-broker-exchanges/276..280)
//! uses — no parallel mechanism, no new broker method.
//!
//! A STOMP `destination` (`/queue/foo`, `/topic/bar`) maps onto a broker DIRECT exchange
//! keyed by the destination string: a `SEND` publishes via `Method::Publish`, and each
//! `SUBSCRIBE` binds a per-subscription queue to that exchange (exact-destination match),
//! whose messages are streamed back as `MESSAGE` frames through the native
//! `Method::BrokerConsume`/`BrokerAck`/`BrokerReject` lifecycle.
//! Delivery is broadcast: every subscriber of a destination gets its own copy.
//!
//! It links NO STOMP crate — every byte layout is hand-rolled against the published
//! STOMP 1.2 spec (the Pi-contract idiom pgwire / amqp-wire / mqtt-wire use), so a
//! default/pi build carries zero STOMP dependency.
//!
//! ## Protocol subset (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit)
//! LANDED: CONNECT/STOMP → CONNECTED, SEND (→ broker publish), SUBSCRIBE/MESSAGE
//! (destination → per-subscription queue, streamed), ACK/NACK (ack modes
//! `auto`/`client`/`client-individual`), UNSUBSCRIBE, DISCONNECT, and `receipt`-header
//! RECEIPT handling. CONNECT `login`/`passcode` authentication is mandatory: the
//! passcode is a domain-separated HMAC derived from `GRAPH_SERVICE_AUTH_SECRET`, and
//! the authenticated principal becomes a secret-keyed pseudonymous actor reference
//! before every engine request. The direct listener is loopback-only; remote access
//! must traverse a TLS/mTLS identity-binding gateway. Transactions fail closed because
//! isolation is unsupported.

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

/// Env var: when set (and the binary is built `--features stomp-wire`), the STOMP wire
/// listener binds this address (documented loopback default `127.0.0.1:61613`). Unset ⇒
/// no listener.
pub const STOMP_ADDR_ENV: &str = "EPISTEMIC_GRAPH_STOMP_ADDR";
/// Env var: the control graph broker state lives on. Defaults to `__commons__`.
pub const STOMP_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_STOMP_GRAPH";
/// Env var: the broker DIRECT exchange STOMP destinations route through. Defaults to
/// `stomp.direct`.
pub const STOMP_EXCHANGE_ENV: &str = "EPISTEMIC_GRAPH_STOMP_EXCHANGE";

const DEFAULT_EXCHANGE: &str = "stomp.direct";
const MAX_STOMP_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_STOMP_HEADERS: usize = 256;
const MAX_STOMP_HEADER_LINE_BYTES: usize = 8 * 1024;
const MAX_STOMP_COMMAND_BYTES: usize = 64;
const MAX_STOMP_IDENTIFIER_BYTES: usize = 4 * 1024;
const MAX_STOMP_SUBSCRIPTIONS: usize = 1_024;
const MAX_STOMP_UNACKED: usize = 65_536;
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
            MAX_STOMP_FRAME_BYTES,
            MAX_BROKER_RESULT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .ok()
}

static REQ_ID: AtomicU64 = AtomicU64::new(1);
type HmacSha256 = Hmac<Sha256>;

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Derive the STOMP CONNECT passcode for a principal.
pub fn derive_stomp_passcode(secret: &str, principal: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"stomp:");
    mac.update(principal.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_stomp_passcode(secret: &str, principal: &str, passcode: &str) -> bool {
    if secret.is_empty()
        || principal.is_empty()
        || principal.len() > MAX_STOMP_IDENTIFIER_BYTES
        || passcode.len() != 64
    {
        return false;
    }
    let Ok(candidate) = hex::decode(passcode) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"stomp:");
    mac.update(principal.as_bytes());
    mac.verify_slice(&candidate).is_ok()
}

/// Fail closed before binding the plaintext STOMP listener.
pub fn validate_startup_policy(addr: &str, secret: &str) -> std::io::Result<()> {
    crate::server::validate_direct_wire_security(addr, "stomp-wire", !secret.is_empty())
}

/// Serve the STOMP 1.2 wire protocol on `addr` until the listener errors (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let graph = std::env::var(STOMP_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let exchange =
        std::env::var(STOMP_EXCHANGE_ENV).unwrap_or_else(|_| DEFAULT_EXCHANGE.to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    validate_startup_policy(addr, &auth_secret)?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "stomp-wire: serving authenticated STOMP 1.2 on loopback (broker graph '{}', \
         exchange '{}'; remote access requires a TLS identity-binding gateway)",
        graph,
        exchange
    );
    accept_loop(listener, state, graph, exchange, auth_secret).await
}

/// The bind-agnostic accept loop (shared by `serve` and the test harness).
async fn accept_loop(
    listener: TcpListener,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
    auth_secret: String,
) -> std::io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let g = graph.clone();
        let ex = exchange.clone();
        let secret = auth_secret.clone();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, st, g, ex, secret).await {
                tracing::debug!("stomp-wire connection from {peer} ended: {e}");
            }
        });
    }
}

// ── Engine bridge (identical shape to amqp-wire) ──────────────────────────

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
    // `dispatch_authenticated_broker_actor` bottoms out in the same ENORMOUS
    // `dispatch()` future under `--features full` that `transport::handle_connection`
    // and `server::mod.rs::dispatch_on_heap` route around (see their doc comments) —
    // awaiting it un-boxed inline overflows the poll-time call stack once a broker
    // request finally reaches deep enough into the dispatch chain (only reachable
    // after this connection's identity clears isolation ACL, so it was latent until
    // then). Box::pin it, matching every OTHER production callsite.
    let resp = Box::pin(dispatch_authenticated_broker_actor(state, req, actor)).await;
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
/// `(node_id, routing_key, body)` or `None`.
async fn claim_one(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    queue: &str,
    consumer: &str,
) -> Option<(String, String, Vec<u8>)> {
    let payload = engine_call(
        state,
        graph,
        actor,
        Method::BrokerConsume {
            queue: queue.to_string(),
            group: "stomp".to_string(),
            consumer: consumer.to_string(),
            now_ms: current_time_ms(),
            lease_ms: BROKER_LEASE_MS,
            prefetch: BROKER_PREFETCH,
        },
    )
    .await;
    let ResultPayload::Raw(bytes) = payload else {
        return None;
    };
    let claimed: Option<(String, serde_json::Value)> = decode_broker_result(&bytes)?;
    let (id, props) = claimed?;
    if id.len() > MAX_STOMP_IDENTIFIER_BYTES {
        return None;
    }
    let rk = props
        .get("routing_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if rk.len() > MAX_STOMP_HEADER_LINE_BYTES {
        return None;
    }
    let rk = rk.to_string();
    let body = props
        .get("payload")
        .and_then(|v| v.as_str())
        .and_then(crate::broker::hex_decode)
        .unwrap_or_default();
    Some((id, rk, body))
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

/// Return a claimed message to the claimable pool through the native rejection path.
async fn requeue_message(
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
        Method::BrokerReject {
            queue: queue.to_string(),
            node_id: node_id.to_string(),
            requeue: true,
            now_ms: current_time_ms(),
        },
    )
    .await;
}

// ── Per-connection state ──────────────────────────────────────────────────

/// STOMP subscription ack discipline (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckMode {
    /// The message is considered acked the instant it is sent (no ACK expected).
    Auto,
    /// The client must ACK; ACK of a message acks it and all earlier ones (cumulative).
    Client,
    /// The client must ACK each message individually.
    ClientIndividual,
}

impl AckMode {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "" | "auto" => Some(AckMode::Auto),
            "client" => Some(AckMode::Client),
            "client-individual" => Some(AckMode::ClientIndividual),
            _ => None,
        }
    }
}

/// An active STOMP subscription.
struct Subscription {
    id: String,
    destination: String,
    queue: String,
    consumer_id: String,
    ack: AckMode,
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
    auth_secret: String,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut authenticated_actor: Option<String> = None;
    let mut subs: Vec<Subscription> = Vec::new();
    // ack-id → (queue, graph node id) for native broker acknowledgement.
    let mut unacked: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();

    loop {
        // Drain every complete frame already buffered before reading more.
        // Keep an offset and compact once: draining after every pipelined frame
        // repeatedly memmoves the entire tail and creates quadratic CPU work.
        let mut consumed_total = 0usize;
        while let Some(nul_relative) = buf[consumed_total..].iter().position(|byte| *byte == 0) {
            let nul = consumed_total + nul_relative;
            validate_stomp_frame_bounds(&buf[consumed_total..=nul]).map_err(invalid_data)?;
            let (frame, consumed) = Frame::parse(&buf[consumed_total..])
                .ok_or_else(|| invalid_data("invalid STOMP frame"))?;
            consumed_total = consumed_total
                .checked_add(consumed)
                .ok_or_else(|| invalid_data("invalid STOMP frame"))?;
            let action = handle_frame(
                socket,
                &state,
                &graph,
                &exchange,
                &frame,
                &auth_secret,
                &mut authenticated_actor,
                &mut subs,
                &mut unacked,
            )
            .await?;
            if action == FrameAction::Close {
                return Ok(());
            }
        }
        if consumed_total > 0 {
            buf.drain(..consumed_total);
        }

        // Need more bytes. With active subscriptions, bound the read so the pump runs.
        let mut tmp = [0u8; 4096];
        let n = if subs.is_empty() {
            socket.read(&mut tmp).await?
        } else {
            match tokio::time::timeout(std::time::Duration::from_millis(200), socket.read(&mut tmp))
                .await
            {
                Ok(r) => r?,
                Err(_) => {
                    let actor = authenticated_actor
                        .as_deref()
                        .ok_or_else(|| invalid_data("STOMP authentication required"))?;
                    pump_subscriptions(socket, &state, &graph, actor, &subs, &mut unacked).await?;
                    continue;
                }
            }
        };
        if n == 0 {
            break; // clean EOF
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_STOMP_FRAME_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "STOMP frame exceeds the resource limit",
            ));
        }
    }
    Ok(())
}

/// Whether the connection should continue or close after a frame.
#[derive(PartialEq, Eq)]
enum FrameAction {
    Continue,
    Close,
}

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    exchange: &str,
    frame: &Frame,
    auth_secret: &str,
    authenticated_actor: &mut Option<String>,
    subs: &mut Vec<Subscription>,
    unacked: &mut std::collections::HashMap<String, (String, String)>,
) -> std::io::Result<FrameAction> {
    if authenticated_actor.is_none() && !matches!(frame.command.as_str(), "CONNECT" | "STOMP") {
        return Err(invalid_data("STOMP command received before CONNECT"));
    }
    if matches!(frame.command.as_str(), "CONNECT" | "STOMP") {
        if authenticated_actor.is_some() {
            return Err(invalid_data("duplicate STOMP CONNECT"));
        }
        let principal = required_header(frame, "login")?;
        let passcode = required_header(frame, "passcode")?;
        if !verify_stomp_passcode(auth_secret, &principal, &passcode) {
            return Err(invalid_data("STOMP authentication failed"));
        }
        let actor = crate::server::pseudonymous_broker_actor(auth_secret, &principal)?;
        let _ = engine_call(
            state,
            graph,
            &actor,
            Method::DeclareExchange {
                exchange: exchange.to_string(),
                kind: "direct".to_string(),
            },
        )
        .await;
        *authenticated_actor = Some(actor);
        let session = format!("stomp-{}", next_req_id());
        let connected_frame = Frame::new(
            "CONNECTED",
            vec![
                ("version".into(), "1.2".into()),
                ("server".into(), "epistemic-graph".into()),
                ("session".into(), session),
                ("heart-beat".into(), "0,0".into()),
            ],
            Vec::new(),
        );
        write_frame(socket, &connected_frame).await?;
        return Ok(FrameAction::Continue);
    }
    let actor = authenticated_actor
        .as_deref()
        .ok_or_else(|| invalid_data("STOMP authentication required"))?;
    if frame.header("transaction").is_some() {
        return Err(invalid_data("STOMP transactions are unsupported"));
    }
    match frame.command.as_str() {
        "SEND" => {
            let destination = required_header(frame, "destination")?;
            let _ = engine_call(
                state,
                graph,
                actor,
                Method::Publish {
                    exchange: exchange.to_string(),
                    routing_key: destination.clone(),
                    payload: frame.body.clone(),
                },
            )
            .await;
            maybe_receipt(socket, frame).await?;
        }
        "SUBSCRIBE" => {
            if subs.len() >= MAX_STOMP_SUBSCRIPTIONS {
                return Err(invalid_data("STOMP subscription limit exceeded"));
            }
            let sub_id = required_header(frame, "id")?;
            if subs.iter().any(|subscription| subscription.id == sub_id) {
                return Err(invalid_data("duplicate STOMP subscription id"));
            }
            let destination = required_header(frame, "destination")?;
            let ack_header = frame.header("ack").unwrap_or_default();
            let ack = AckMode::parse(&ack_header)
                .ok_or_else(|| invalid_data("invalid STOMP acknowledgement mode"))?;
            let queue = format!("stomp.{}", next_req_id());
            // Bind the per-subscription queue to the destination (exact match).
            let _ = engine_call(
                state,
                graph,
                actor,
                Method::BindQueue {
                    exchange: exchange.to_string(),
                    queue: queue.clone(),
                    routing_key: destination.clone(),
                },
            )
            .await;
            subs.push(Subscription {
                id: sub_id,
                destination,
                queue,
                consumer_id: format!("{actor}:{}", next_req_id()),
                ack,
            });
            maybe_receipt(socket, frame).await?;
        }
        "UNSUBSCRIBE" => {
            let sub_id = required_header(frame, "id")?;
            if let Some(pos) = subs.iter().position(|s| s.id == sub_id) {
                let s = subs.remove(pos);
                let _ = engine_call(
                    state,
                    graph,
                    actor,
                    Method::UnbindQueue {
                        exchange: exchange.to_string(),
                        queue: s.queue.clone(),
                        routing_key: s.destination.clone(),
                    },
                )
                .await;
            }
            maybe_receipt(socket, frame).await?;
        }
        "ACK" => {
            // STOMP 1.2 ACK carries the message's `ack` id in the `id` header.
            let ack_id = required_header(frame, "id")?;
            if let Some((queue, node_id)) = unacked.remove(&ack_id) {
                ack_message(state, graph, actor, &queue, &node_id).await;
            }
            maybe_receipt(socket, frame).await?;
        }
        "NACK" => {
            let ack_id = required_header(frame, "id")?;
            if let Some((queue, node_id)) = unacked.remove(&ack_id) {
                requeue_message(state, graph, actor, &queue, &node_id).await;
            }
            maybe_receipt(socket, frame).await?;
        }
        // Never acknowledge transaction semantics that this protocol surface
        // cannot actually provide; doing so would invite callers to assume writes
        // are isolated when they are not.
        "BEGIN" | "COMMIT" | "ABORT" => {
            return Err(invalid_data("STOMP transactions are unsupported"));
        }
        "DISCONNECT" => {
            maybe_receipt(socket, frame).await?;
            return Ok(FrameAction::Close);
        }
        _ => {
            return Err(invalid_data("unsupported STOMP command"));
        }
    }
    Ok(FrameAction::Continue)
}

fn required_header(frame: &Frame, key: &str) -> std::io::Result<String> {
    frame
        .header(key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_data("missing required STOMP header"))
}

/// Emit a RECEIPT frame if the client asked for one (`receipt` header).
async fn maybe_receipt(socket: &mut TcpStream, frame: &Frame) -> std::io::Result<()> {
    if let Some(receipt_id) = frame.header("receipt") {
        let r = Frame::new(
            "RECEIPT",
            vec![("receipt-id".into(), receipt_id)],
            Vec::new(),
        );
        write_frame(socket, &r).await?;
    }
    Ok(())
}

/// Deliver pending messages from each subscription's queue as MESSAGE frames
/// (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit). Auto-ack subscriptions finalize immediately; client-ack ones record
/// the message for a later ACK/NACK.
async fn pump_subscriptions(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    subs: &[Subscription],
    unacked: &mut std::collections::HashMap<String, (String, String)>,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for sub in subs {
        for _ in 0..MAX_PER_POLL {
            if unacked.len() >= MAX_STOMP_UNACKED {
                return Ok(());
            }
            let Some((node_id, _rk, body)) =
                claim_one(state, graph, actor, &sub.queue, &sub.consumer_id).await
            else {
                break;
            };
            // The broker node id doubles as the STOMP message-id / ack id (ASCII-safe).
            let msg = Frame::new(
                "MESSAGE",
                vec![
                    ("subscription".into(), sub.id.clone()),
                    ("message-id".into(), node_id.clone()),
                    ("ack".into(), node_id.clone()),
                    ("destination".into(), sub.destination.clone()),
                    ("content-length".into(), body.len().to_string()),
                ],
                body,
            );
            write_frame(socket, &msg).await?;
            match sub.ack {
                AckMode::Auto => ack_message(state, graph, actor, &sub.queue, &node_id).await,
                AckMode::Client | AckMode::ClientIndividual => {
                    unacked.insert(node_id.clone(), (sub.queue.clone(), node_id));
                }
            }
        }
    }
    Ok(())
}

// ── STOMP frame codec ─────────────────────────────────────────────────────

/// Validate allocation-driving text structure before `Frame::parse` creates
/// owned command/header/body values. The socket buffer is already byte-capped;
/// these limits prevent one frame from turning that buffer into hundreds of
/// thousands of separate heap allocations. A declared `content-length` must
/// also match exactly, closing truncation and embedded-NUL framing ambiguity.
fn validate_stomp_frame_bounds(bytes: &[u8]) -> Result<(), &'static str> {
    if bytes.is_empty() || bytes.len() > MAX_STOMP_FRAME_BYTES || bytes.last() != Some(&0) {
        return Err("invalid STOMP frame bounds");
    }
    let mut start = 0usize;
    while start + 1 < bytes.len() && matches!(bytes[start], b'\r' | b'\n') {
        start += 1;
    }
    let frame = &bytes[start..bytes.len() - 1];
    let (head, body) = split_head_body(frame).ok_or("missing STOMP header terminator")?;
    let mut lines = head.split(|byte| *byte == b'\n');
    let command = trim_cr(lines.next().ok_or("missing STOMP command")?);
    if command.is_empty()
        || command.len() > MAX_STOMP_COMMAND_BYTES
        || !command.iter().all(u8::is_ascii_uppercase)
    {
        return Err("invalid STOMP command");
    }

    let mut header_count = 0usize;
    let mut content_length = None;
    for line in lines {
        let line = trim_cr(line);
        if line.is_empty() {
            continue;
        }
        header_count = header_count
            .checked_add(1)
            .ok_or("too many STOMP headers")?;
        if header_count > MAX_STOMP_HEADERS || line.len() > MAX_STOMP_HEADER_LINE_BYTES {
            return Err("STOMP header limit exceeded");
        }
        let colon = line
            .iter()
            .position(|byte| *byte == b':')
            .ok_or("invalid STOMP header")?;
        std::str::from_utf8(&line[..colon]).map_err(|_| "invalid STOMP header")?;
        std::str::from_utf8(&line[colon + 1..]).map_err(|_| "invalid STOMP header")?;
        if &line[..colon] == b"content-length" {
            if content_length.is_some() {
                return Err("duplicate STOMP content length");
            }
            let value = std::str::from_utf8(&line[colon + 1..])
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value <= MAX_STOMP_FRAME_BYTES)
                .ok_or("invalid STOMP content length")?;
            content_length = Some(value);
        }
    }
    if content_length.is_some_and(|declared| declared != body.len()) {
        return Err("STOMP content length mismatch");
    }
    Ok(())
}

/// One parsed STOMP frame: command + ordered headers + body.
struct Frame {
    command: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Frame {
    fn new(command: &str, headers: Vec<(String, String)>, body: Vec<u8>) -> Self {
        Self {
            command: command.to_string(),
            headers,
            body,
        }
    }

    /// First value for `key` (STOMP: the FIRST occurrence wins).
    fn header(&self, key: &str) -> Option<String> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    }

    /// Try to parse ONE frame from the front of `bytes`. Returns `(frame, consumed)` or
    /// `None` if a full frame (terminated by NUL) is not yet buffered. Leading EOL bytes
    /// (`\r`/`\n`, STOMP heart-beats + inter-frame newlines) are skipped and counted.
    fn parse(bytes: &[u8]) -> Option<(Frame, usize)> {
        // Skip leading heart-beat / separator EOLs.
        let mut start = 0;
        while start < bytes.len() && (bytes[start] == b'\n' || bytes[start] == b'\r') {
            start += 1;
        }
        // Find the NUL terminator.
        let nul_rel = bytes[start..].iter().position(|&b| b == 0)?;
        let nul = start + nul_rel;
        let frame_bytes = &bytes[start..nul];

        // Split the header block (command + headers) from the body at the first blank
        // line (`\n\n`). Headers are always UTF-8 text; the body may be binary.
        let (head, body) = split_head_body(frame_bytes)?;
        let mut lines = head.split(|&b| b == b'\n');
        let command = lines
            .next()
            .map(trim_cr)
            .and_then(|line| std::str::from_utf8(line).ok())?
            .to_owned();
        let mut headers = Vec::new();
        for line in lines {
            let line = trim_cr(line);
            if line.is_empty() {
                continue;
            }
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let k = decode_header(&line[..colon])?;
                let v = decode_header(&line[colon + 1..])?;
                headers.push((k, v));
            } else {
                return None;
            }
        }
        Some((
            Frame {
                command,
                headers,
                body: body.to_vec(),
            },
            nul + 1, // consume through the NUL
        ))
    }
}

/// Split a frame's bytes into `(header-block, body)` at the first blank line.
fn split_head_body(frame: &[u8]) -> Option<(&[u8], &[u8])> {
    // Look for "\n\n" (allowing "\r\n\r\n" via the trailing CR trim on each line).
    let mut i = 0;
    while i + 1 < frame.len() {
        if frame[i] == b'\n' {
            // A blank line: next byte is '\n', or '\r' then '\n'.
            if frame[i + 1] == b'\n' {
                return Some((&frame[..i], &frame[i + 2..]));
            }
            if frame[i + 1] == b'\r' && i + 2 < frame.len() && frame[i + 2] == b'\n' {
                return Some((&frame[..i], &frame[i + 3..]));
            }
        }
        i += 1;
    }
    None
}

fn trim_cr(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

/// Decode a STOMP 1.2 header octet-sequence (unescape `\c`→`:`, `\n`→LF, `\r`→CR,
/// `\\`→`\`).
fn decode_header(bytes: &[u8]) -> Option<String> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            let escaped = *bytes.get(i + 1)?;
            match escaped {
                b'c' => out.push(b':'),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b'\\' => out.push(b'\\'),
                _ => return None,
            }
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Encode a STOMP 1.2 header value (escape `:`→`\c`, LF→`\n`, CR→`\r`, `\`→`\\`).
fn encode_header(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            ':' => out.push_str("\\c"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out
}

/// Serialize a frame to the wire: `COMMAND\n` + `key:value\n`… + `\n` + body + `\0`.
fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(frame.command.as_bytes());
    out.push(b'\n');
    for (k, v) in &frame.headers {
        out.extend_from_slice(encode_header(k).as_bytes());
        out.push(b':');
        out.extend_from_slice(encode_header(v).as_bytes());
        out.push(b'\n');
    }
    out.push(b'\n'); // blank line ends the header block
    out.extend_from_slice(&frame.body);
    out.push(0); // NUL terminator
    out
}

async fn write_frame(socket: &mut TcpStream, frame: &Frame) -> std::io::Result<()> {
    let bytes = encode_frame(frame);
    if bytes.len() > MAX_STOMP_FRAME_BYTES {
        return Err(invalid_data(
            "STOMP output frame exceeds the resource limit",
        ));
    }
    socket.write_all(&bytes).await
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.ontology.stomp-frame-codec-unit — STOMP frame-codec unit tests (round-trips, header escaping,
    //! body/NUL framing) + a served listener round-trip (CONNECT/SEND/SUBSCRIBE/MESSAGE)
    //! that proves the mapping onto the broker end-to-end.
    use super::*;

    #[test]
    fn eg282_frame_roundtrips_command_headers_body() {
        let frame = Frame::new(
            "SEND",
            vec![
                ("destination".into(), "/queue/orders".into()),
                ("content-type".into(), "text/plain".into()),
            ],
            b"hello world".to_vec(),
        );
        let bytes = encode_frame(&frame);
        let (parsed, consumed) = Frame::parse(&bytes).expect("a full frame");
        assert_eq!(consumed, bytes.len());
        assert_eq!(parsed.command, "SEND");
        assert_eq!(
            parsed.header("destination").as_deref(),
            Some("/queue/orders")
        );
        assert_eq!(parsed.header("content-type").as_deref(), Some("text/plain"));
        assert_eq!(parsed.body, b"hello world".to_vec());
    }

    #[test]
    fn eg282_header_value_escaping_roundtrips() {
        // A value containing the STOMP-special chars must survive encode→decode.
        let raw = "a:b\nc\\d\re";
        let enc = encode_header(raw);
        assert_eq!(enc, "a\\cb\\nc\\\\d\\re");
        assert_eq!(decode_header(enc.as_bytes()).as_deref(), Some(raw));
    }

    #[test]
    fn eg282_parse_needs_full_frame_then_consumes_exactly_one() {
        let mut bytes = encode_frame(&Frame::new(
            "SUBSCRIBE",
            vec![
                ("id".into(), "0".into()),
                ("destination".into(), "/topic/a".into()),
            ],
            Vec::new(),
        ));
        // Truncated (no NUL yet) → None.
        assert!(Frame::parse(&bytes[..bytes.len() - 1]).is_none());
        // Append a SECOND frame; parse must consume ONLY the first.
        let first_len = bytes.len();
        bytes.extend_from_slice(&encode_frame(&Frame::new(
            "DISCONNECT",
            vec![("receipt".into(), "r1".into())],
            Vec::new(),
        )));
        let (f1, consumed) = Frame::parse(&bytes).unwrap();
        assert_eq!(consumed, first_len);
        assert_eq!(f1.command, "SUBSCRIBE");
        let (f2, _c2) = Frame::parse(&bytes[consumed..]).unwrap();
        assert_eq!(f2.command, "DISCONNECT");
    }

    #[test]
    fn eg282_leading_heartbeat_eols_are_skipped() {
        let mut bytes = vec![b'\n', b'\n']; // heart-beat newlines
        bytes.extend_from_slice(&encode_frame(&Frame::new(
            "DISCONNECT",
            Vec::new(),
            Vec::new(),
        )));
        let (f, consumed) = Frame::parse(&bytes).unwrap();
        assert_eq!(f.command, "DISCONNECT");
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn eg282_ack_mode_parse() {
        assert_eq!(AckMode::parse("client"), Some(AckMode::Client));
        assert_eq!(
            AckMode::parse("client-individual"),
            Some(AckMode::ClientIndividual)
        );
        assert_eq!(AckMode::parse(""), Some(AckMode::Auto));
        assert_eq!(AckMode::parse("auto"), Some(AckMode::Auto));
        assert_eq!(AckMode::parse("unknown"), None);
    }

    #[test]
    fn connect_passcode_is_verified_and_identity_bound() {
        let principal = "agent:subscriber";
        let passcode = derive_stomp_passcode("test", principal);
        assert!(verify_stomp_passcode("test", principal, &passcode));
        assert!(!verify_stomp_passcode("other", principal, &passcode));
        assert!(!verify_stomp_passcode("", principal, &passcode));
        assert!(!verify_stomp_passcode("test", "agent:other", &passcode));
        let actor = crate::server::pseudonymous_broker_actor("test", principal).unwrap();
        assert!(!actor.contains(principal));
    }

    #[test]
    fn startup_policy_rejects_anonymous_or_remote_stomp() {
        assert!(validate_startup_policy("127.0.0.1:61613", "").is_err());
        assert!(validate_startup_policy("0.0.0.0:61613", "test").is_err());
        assert!(validate_startup_policy("127.0.0.1:61613", "test").is_ok());
    }

    #[test]
    fn frame_preflight_rejects_header_and_content_length_abuse() {
        let too_many = format!("SEND\n{}\nbody\0", "x:y\n".repeat(MAX_STOMP_HEADERS + 1));
        assert!(validate_stomp_frame_bounds(too_many.as_bytes()).is_err());
        assert!(validate_stomp_frame_bounds(b"SEND\ncontent-length:9\n\nbody\0").is_err());
        assert!(validate_stomp_frame_bounds(b"SEND\ncontent-length:4\n\nbody\0").is_ok());
    }

    // ── Served listener round-trip (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit) ───────────────────────

    /// A minimal `ServerState` for the broker round-trip. Every optional/feature-gated
    /// field is `None`/empty so it compiles under any feature combination, except
    /// `persist_dir`/`persistence`: every broker method (`Publish`, `BindQueue`, …)
    /// is policy-classified `DurabilityDomain::Outbox` (see
    /// `eg_capabilities::policy`), so the commit gateway hard-errors without a real
    /// persistence backend — a durable `RedbBackend` is wired in under
    /// `feature = "redb"` (which `stomp-wire` does not itself require, but the
    /// round trip needs to actually publish/deliver a message).
    async fn test_state() -> Arc<RwLock<ServerState>> {
        use crate::channels::ChannelManager;
        use crate::isolation::IsolationLayer;
        use crate::registry::GraphRegistry;
        use dashmap::DashMap;
        use tokio::sync::Semaphore;
        let mut isolation = IsolationLayer::new();
        #[cfg(feature = "security")]
        {
            use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
            isolation.add_role(Role::new("commons-user"));
            isolation.add_grant(Grant {
                role: "commons-user".to_string(),
                resource: ResourceSelector::Graph("__commons__".to_string()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            isolation.add_grant(Grant {
                role: "commons-user".to_string(),
                resource: ResourceSelector::Graph("__commons__".to_string()),
                action: RbacAction::Write,
                effect: GrantEffect::Allow,
            });
        }
        // The engine ACL identity for a broker-wire connection is the pseudonymous
        // HMAC actor reference `pseudonymous_broker_actor` derives from the CONNECT
        // `login` — not the raw login string. Register the two principals the
        // round-trip test authenticates as (see `eg282_listener_connect_subscribe_send_message_roundtrip`).
        for principal in ["subscriber", "publisher"] {
            let actor_ref = crate::server::pseudonymous_broker_actor("test", principal)
                .expect("test principal pseudonymizes");
            isolation.register_agent(crate::isolation::AgentIdentity {
                agent_id: actor_ref,
                role: crate::isolation::AgentRole::Agent,
                teams: Vec::new(),
                #[cfg(feature = "security")]
                roles: vec!["commons-user".to_string()],
                #[cfg(not(feature = "security"))]
                roles: Vec::new(),
            });
        }
        #[cfg(feature = "redb")]
        let (persist_dir, persistence) = {
            use crate::durability::DurabilityPolicy;
            use crate::server::persistence::redb_backend::RedbBackend;
            let dir = std::env::temp_dir().join(format!(
                "eg-stomp-wire-test-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            let backend = RedbBackend::open(
                dir.to_string_lossy().to_string(),
                DurabilityPolicy::Each,
                64,
            )
            .expect("open stomp-wire test backend");
            let persistence: Arc<dyn crate::server::persistence::PersistenceBackend> =
                Arc::new(backend);
            persistence
                .register_graph(
                    "__commons__",
                    "__commons__",
                    crate::protocol::GraphType::Commons,
                )
                .await
                .unwrap();
            (Some(dir.to_string_lossy().into_owned()), Some(persistence))
        };
        #[cfg(not(feature = "redb"))]
        let (persist_dir, persistence): (
            Option<String>,
            Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
        ) = (None, None);
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir,
            persistence,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    async fn spawn_listener() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state().await;
        tokio::spawn(async move {
            let _ = accept_loop(
                listener,
                state,
                "__commons__".to_string(),
                DEFAULT_EXCHANGE.to_string(),
                "test".to_string(),
            )
            .await;
        });
        addr
    }

    /// Read one full STOMP frame off a client stream (blocking until a NUL arrives).
    async fn read_one_frame(sock: &mut TcpStream) -> Frame {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            if let Some((f, consumed)) = Frame::parse(&buf) {
                let _ = consumed;
                return f;
            }
            let n = tokio::time::timeout(std::time::Duration::from_secs(5), sock.read(&mut tmp))
                .await
                .expect("frame within timeout")
                .unwrap();
            assert!(n > 0, "unexpected EOF waiting for frame");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    #[tokio::test]
    async fn eg282_listener_connect_subscribe_send_message_roundtrip() {
        let addr = spawn_listener().await;

        // ── Subscriber connects + subscribes to /queue/orders (auto ack). ──
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        write_frame(
            &mut sub,
            &Frame::new(
                "CONNECT",
                vec![
                    ("accept-version".into(), "1.2".into()),
                    ("login".into(), "subscriber".into()),
                    (
                        "passcode".into(),
                        derive_stomp_passcode("test", "subscriber"),
                    ),
                ],
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_one_frame(&mut sub).await.command, "CONNECTED");
        write_frame(
            &mut sub,
            &Frame::new(
                "SUBSCRIBE",
                vec![
                    ("id".into(), "sub-0".into()),
                    ("destination".into(), "/queue/orders".into()),
                ],
                Vec::new(),
            ),
        )
        .await
        .unwrap();

        // ── Publisher connects + SENDs to /queue/orders with a receipt. ──
        let mut pubc = TcpStream::connect(&addr).await.unwrap();
        write_frame(
            &mut pubc,
            &Frame::new(
                "CONNECT",
                vec![
                    ("accept-version".into(), "1.2".into()),
                    ("login".into(), "publisher".into()),
                    (
                        "passcode".into(),
                        derive_stomp_passcode("test", "publisher"),
                    ),
                ],
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_one_frame(&mut pubc).await.command, "CONNECTED");
        write_frame(
            &mut pubc,
            &Frame::new(
                "SEND",
                vec![
                    ("destination".into(), "/queue/orders".into()),
                    ("receipt".into(), "snd-1".into()),
                ],
                b"order-42".to_vec(),
            ),
        )
        .await
        .unwrap();
        let receipt = read_one_frame(&mut pubc).await;
        assert_eq!(receipt.command, "RECEIPT");
        assert_eq!(receipt.header("receipt-id").as_deref(), Some("snd-1"));

        // ── Subscriber receives the MESSAGE frame. ──
        let msg = read_one_frame(&mut sub).await;
        assert_eq!(msg.command, "MESSAGE");
        assert_eq!(msg.header("destination").as_deref(), Some("/queue/orders"));
        assert_eq!(msg.header("subscription").as_deref(), Some("sub-0"));
        assert!(msg.header("message-id").is_some());
        assert_eq!(msg.body, b"order-42".to_vec());
    }
}
