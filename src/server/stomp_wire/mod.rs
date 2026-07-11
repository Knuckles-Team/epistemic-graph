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
//! whose messages are streamed back as `MESSAGE` frames via the KG-2.303 claim path
//! (`Method::ClaimNext` + `CompareAndSetNodeFields`, exactly as amqp-wire's consume).
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
//! RECEIPT handling. Auth is a localhost TRUST surface (any CONNECT accepted, like the
//! SQL wires' trust mode). DEFERRED (a client degrades gracefully): transactions
//! (BEGIN/COMMIT/ABORT are accepted but not isolated), heart-beating, and TLS.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::auth::compute_auth_token;
use crate::server::dispatch::dispatch;
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

static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Serve the STOMP 1.2 wire protocol on `addr` until the listener errors (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let graph = std::env::var(STOMP_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let exchange =
        std::env::var(STOMP_EXCHANGE_ENV).unwrap_or_else(|_| DEFAULT_EXCHANGE.to_string());
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "stomp-wire: serving STOMP 1.2 on {} (broker graph '{}', exchange '{}')",
        addr,
        graph,
        exchange
    );
    accept_loop(listener, state, graph, exchange).await
}

/// The bind-agnostic accept loop (shared by `serve` and the test harness).
async fn accept_loop(
    listener: TcpListener,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
) -> std::io::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let g = graph.clone();
        let ex = exchange.clone();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, st, g, ex).await {
                tracing::debug!("stomp-wire connection from {peer} ended: {e}");
            }
        });
    }
}

// ── Engine bridge (identical shape to amqp-wire) ──────────────────────────

async fn engine_call(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    method: Method,
) -> ResultPayload {
    let id = next_req_id();
    let secret = { state.read().await.auth_secret.clone() };
    let req = Request {
        id,
        graph: graph.to_string(),
        auth_token: compute_auth_token(&secret, id),
        agent_id: None,
        method,
    };
    let resp = dispatch(state, req).await;
    resp.result.unwrap_or(ResultPayload::Bool(false))
}

fn obj(map: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(map.as_object().unwrap()).unwrap_or_default()
}

/// Claim the oldest pending message from `queue` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending), marking it
/// `claimed`. Returns `(node_id, routing_key, body)` or `None`.
async fn claim_one(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    queue: &str,
) -> Option<(String, String, Vec<u8>)> {
    let updates = obj(serde_json::json!({ "status": "claimed" }));
    let payload = engine_call(
        state,
        graph,
        Method::ClaimNext {
            label: crate::broker::queue_msg_label(queue),
            updates_msgpack: updates,
        },
    )
    .await;
    let ResultPayload::Raw(bytes) = payload else {
        return None;
    };
    let claimed: Option<(String, serde_json::Value)> = rmp_serde::from_slice(&bytes).ok()?;
    let (id, props) = claimed?;
    let rk = props
        .get("routing_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let body = props
        .get("payload")
        .and_then(|v| v.as_str())
        .and_then(crate::broker::hex_decode)
        .unwrap_or_default();
    Some((id, rk, body))
}

/// Finalize a delivered message: CAS its status `claimed → acked` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending ack
/// path). Best-effort — a lost ack simply leaves the node `claimed`.
async fn ack_message(state: &Arc<RwLock<ServerState>>, graph: &str, node_id: &str) {
    let conditions = obj(serde_json::json!({ "status": "claimed" }));
    let updates = obj(serde_json::json!({ "status": "acked" }));
    let _ = engine_call(
        state,
        graph,
        Method::CompareAndSetNodeFields {
            node_id: node_id.to_string(),
            conditions_msgpack: conditions,
            updates_msgpack: updates,
        },
    )
    .await;
}

/// Return a claimed message to the claimable pool: CAS `claimed → pending` (a NACK
/// requeue). Best-effort.
async fn requeue_message(state: &Arc<RwLock<ServerState>>, graph: &str, node_id: &str) {
    let conditions = obj(serde_json::json!({ "status": "claimed" }));
    let updates = obj(serde_json::json!({ "status": "pending" }));
    let _ = engine_call(
        state,
        graph,
        Method::CompareAndSetNodeFields {
            node_id: node_id.to_string(),
            conditions_msgpack: conditions,
            updates_msgpack: updates,
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
    fn parse(s: &str) -> Self {
        match s {
            "client" => AckMode::Client,
            "client-individual" => AckMode::ClientIndividual,
            _ => AckMode::Auto,
        }
    }
}

/// An active STOMP subscription.
struct Subscription {
    id: String,
    destination: String,
    queue: String,
    ack: AckMode,
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut connected = false;
    let mut subs: Vec<Subscription> = Vec::new();
    // ack-id → (queue node id) for client/client-individual acknowledgement.
    let mut unacked: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    loop {
        // Drain every complete frame already buffered before reading more.
        while let Some((frame, consumed)) = Frame::parse(&buf) {
            buf.drain(..consumed);
            let action = handle_frame(
                socket,
                &state,
                &graph,
                &exchange,
                &frame,
                &mut connected,
                &mut subs,
                &mut unacked,
            )
            .await?;
            if action == FrameAction::Close {
                return Ok(());
            }
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
                    pump_subscriptions(socket, &state, &graph, &subs, &mut unacked).await?;
                    continue;
                }
            }
        };
        if n == 0 {
            break; // clean EOF
        }
        buf.extend_from_slice(&tmp[..n]);
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
    connected: &mut bool,
    subs: &mut Vec<Subscription>,
    unacked: &mut std::collections::HashMap<String, String>,
) -> std::io::Result<FrameAction> {
    match frame.command.as_str() {
        "CONNECT" | "STOMP" => {
            *connected = true;
            // Ensure the shared broker exchange exists (idempotent).
            let _ = engine_call(
                state,
                graph,
                Method::DeclareExchange {
                    exchange: exchange.to_string(),
                    kind: "direct".to_string(),
                },
            )
            .await;
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
        }
        "SEND" => {
            let _ = connected; // trust surface: accept without a strict CONNECT gate
            let destination = frame.header("destination").unwrap_or_default();
            let _ = engine_call(
                state,
                graph,
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
            let sub_id = frame
                .header("id")
                .unwrap_or_else(|| format!("sub-{}", next_req_id()));
            let destination = frame.header("destination").unwrap_or_default();
            let ack = AckMode::parse(&frame.header("ack").unwrap_or_default());
            let queue = format!("stomp.{}.{}", next_req_id(), sub_id);
            // Bind the per-subscription queue to the destination (exact match).
            let _ = engine_call(
                state,
                graph,
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
                ack,
            });
            maybe_receipt(socket, frame).await?;
        }
        "UNSUBSCRIBE" => {
            if let Some(sub_id) = frame.header("id") {
                if let Some(pos) = subs.iter().position(|s| s.id == sub_id) {
                    let s = subs.remove(pos);
                    let _ = engine_call(
                        state,
                        graph,
                        Method::UnbindQueue {
                            exchange: exchange.to_string(),
                            queue: s.queue.clone(),
                            routing_key: s.destination.clone(),
                        },
                    )
                    .await;
                }
            }
            maybe_receipt(socket, frame).await?;
        }
        "ACK" => {
            // STOMP 1.2 ACK carries the message's `ack` id in the `id` header.
            if let Some(ack_id) = frame.header("id") {
                if let Some(node_id) = unacked.remove(&ack_id) {
                    ack_message(state, graph, &node_id).await;
                }
            }
            maybe_receipt(socket, frame).await?;
        }
        "NACK" => {
            if let Some(ack_id) = frame.header("id") {
                if let Some(node_id) = unacked.remove(&ack_id) {
                    requeue_message(state, graph, &node_id).await;
                }
            }
            maybe_receipt(socket, frame).await?;
        }
        // Transactions are accepted (a receipt is honored) but NOT isolated — DEFERRED.
        "BEGIN" | "COMMIT" | "ABORT" => {
            maybe_receipt(socket, frame).await?;
        }
        "DISCONNECT" => {
            maybe_receipt(socket, frame).await?;
            return Ok(FrameAction::Close);
        }
        _ => {
            // Unknown command: spec allows an ERROR frame; be tolerant and ignore.
        }
    }
    Ok(FrameAction::Continue)
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
    subs: &[Subscription],
    unacked: &mut std::collections::HashMap<String, String>,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for sub in subs {
        for _ in 0..MAX_PER_POLL {
            let Some((node_id, _rk, body)) = claim_one(state, graph, &sub.queue).await else {
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
                AckMode::Auto => ack_message(state, graph, &node_id).await,
                AckMode::Client | AckMode::ClientIndividual => {
                    unacked.insert(node_id.clone(), node_id);
                }
            }
        }
    }
    Ok(())
}

// ── STOMP frame codec ─────────────────────────────────────────────────────

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
        let (head, body) = split_head_body(frame_bytes);
        let mut lines = head.split(|&b| b == b'\n');
        let command = lines
            .next()
            .map(trim_cr)
            .map(|l| String::from_utf8_lossy(l).into_owned())
            .unwrap_or_default();
        let mut headers = Vec::new();
        for line in lines {
            let line = trim_cr(line);
            if line.is_empty() {
                continue;
            }
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let k = decode_header(&line[..colon]);
                let v = decode_header(&line[colon + 1..]);
                headers.push((k, v));
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

/// Split a frame's bytes into `(header-block, body)` at the first blank line. If none is
/// found (malformed), the whole slice is treated as the header block.
fn split_head_body(frame: &[u8]) -> (&[u8], &[u8]) {
    // Look for "\n\n" (allowing "\r\n\r\n" via the trailing CR trim on each line).
    let mut i = 0;
    while i + 1 < frame.len() {
        if frame[i] == b'\n' {
            // A blank line: next byte is '\n', or '\r' then '\n'.
            if frame[i + 1] == b'\n' {
                return (&frame[..i], &frame[i + 2..]);
            }
            if frame[i + 1] == b'\r' && i + 2 < frame.len() && frame[i + 2] == b'\n' {
                return (&frame[..i], &frame[i + 3..]);
            }
        }
        i += 1;
    }
    (frame, &[])
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
fn decode_header(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            match bytes[i + 1] {
                b'c' => out.push(b':'),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b'\\' => out.push(b'\\'),
                other => {
                    out.push(b'\\');
                    out.push(other);
                }
            }
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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
    socket.write_all(&encode_frame(frame)).await
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
        assert_eq!(decode_header(enc.as_bytes()), raw);
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
        assert_eq!(AckMode::parse("client"), AckMode::Client);
        assert_eq!(
            AckMode::parse("client-individual"),
            AckMode::ClientIndividual
        );
        assert_eq!(AckMode::parse(""), AckMode::Auto);
        assert_eq!(AckMode::parse("auto"), AckMode::Auto);
    }

    // ── Served listener round-trip (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit) ───────────────────────

    /// A minimal `ServerState` for the broker round-trip. Every optional/feature-gated
    /// field is `None`/empty so it compiles under any feature combination.
    fn test_state() -> Arc<RwLock<ServerState>> {
        use crate::channels::ChannelManager;
        use crate::isolation::IsolationLayer;
        use crate::registry::GraphRegistry;
        use dashmap::DashMap;
        use tokio::sync::Semaphore;
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
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
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
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
            #[cfg(feature = "dataset-handle")]
            dataset_handles: Arc::new(
                crate::server::dataset_handle::DatasetHandleRegistry::new(),
            ),
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    async fn spawn_listener() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state();
        tokio::spawn(async move {
            let _ = accept_loop(
                listener,
                state,
                "__commons__".to_string(),
                DEFAULT_EXCHANGE.to_string(),
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
                vec![("accept-version".into(), "1.2".into())],
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
                vec![("accept-version".into(), "1.2".into())],
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
