//! AMQP 0.9.1 wire-protocol listener (CONCEPT:EG-275) — a HAND-ROLLED AMQP 0.9.1
//! server that lets a standard AMQP client (pika, amqplib, the `rabbitmqadmin`-style
//! tools) speak to the native message broker built on the KG-2.303 work-queue.
//!
//! ## What this is (and is NOT)
//! Like the pgwire / mysql-wire shims, this is an ADAPTER, not a second broker. Every
//! exchange/binding/queue/message lives as graph nodes on a control graph; this module
//! only frames AMQP 0.9.1 on the wire and maps each method onto the broker primitives
//! (`crate::broker`) THROUGH the engine dispatch (`crate::server::dispatch::dispatch`)
//! — so an AMQP publish takes the SAME routing + atomic enqueue + WAL/CDC path a
//! `Method::Publish` RPC does, and a consume rides the SAME `Method::ClaimNext`
//! (CONCEPT:KG-2.303) an RPC consumer uses. No parallel mechanism.
//!
//! It links NO AMQP crate — every byte layout is hand-rolled against the published
//! AMQP 0.9.1 spec (the Pi-contract idiom pgwire / mysql-wire / sparql-http use), so a
//! default/pi build carries zero AMQP dependency (asserted by `cargo tree`).
//!
//! ## Protocol subset (CONCEPT:EG-275)
//! LANDED: the connection handshake (`connection.start`/`start-ok`/`tune`/`tune-ok`/
//! `open`/`open-ok`/`close`), `channel.open`/`close`, `exchange.declare`/`delete`,
//! `queue.declare`/`bind`/`unbind`, `basic.publish` (+ content header/body frames),
//! `basic.consume`/`deliver` (a poll-driven push pump), `basic.get`/`get-ok`/
//! `get-empty`, and `basic.ack`. Auth is a localhost TRUST surface (PLAIN accepted
//! unconditionally, like the SQL wires' trust mode); TLS, publisher confirms, QoS
//! prefetch, transactions, and heartbeats are DEFERRED — a client negotiating them
//! degrades gracefully to this text path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::auth::compute_auth_token;
use crate::server::dispatch::dispatch;
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

static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Serve the AMQP 0.9.1 wire protocol on `addr` until the listener errors.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let default_graph = std::env::var(AMQP_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "amqp-wire: serving AMQP 0.9.1 on {} (broker graph '{}')",
        addr,
        default_graph
    );
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let g = default_graph.clone();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, st, g).await {
                tracing::debug!("amqp-wire connection from {peer} ended: {e}");
            }
        });
    }
}

// ── Engine bridge ─────────────────────────────────────────────────────────

/// Run one broker `Method` through the engine dispatch against the broker graph,
/// authenticating exactly as an RPC client would (compute the per-request HMAC token).
async fn engine_call(state: &Arc<RwLock<ServerState>>, graph: &str, method: Method) -> ResultPayload {
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

/// Claim the oldest pending message from `queue` (CONCEPT:KG-2.303), marking it
/// `claimed`. Returns `(node_id, routing_key, exchange, body)` or `None`.
async fn claim_one(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    queue: &str,
) -> Option<(String, String, String, Vec<u8>)> {
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

/// Finalize a delivered message: CAS its status `claimed → acked` (CONCEPT:KG-2.303
/// ack path). Best-effort — a lost ack simply leaves the node `claimed`.
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

// ── Per-connection state ────────────────────────────────────────────────

/// One raw AMQP frame off the wire.
struct Frame {
    kind: u8,
    channel: u16,
    payload: Vec<u8>,
}

/// A raised AMQP method: class/method ids + the argument bytes.
struct MethodCall {
    class: u16,
    method: u16,
    args: Vec<u8>,
}

/// An active `basic.consume` subscription.
struct Consumer {
    channel: u16,
    tag: String,
    queue: String,
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
) -> std::io::Result<()> {
    // ── Protocol header ──
    let mut hdr = [0u8; 8];
    socket.read_exact(&mut hdr).await?;
    if &hdr[0..4] != b"AMQP" {
        // Tell the client the version we speak, then close.
        socket.write_all(b"AMQP\x00\x00\x09\x01").await?;
        return Ok(());
    }
    // ── connection.start ──
    write_frame(socket, FRAME_METHOD, 0, &build_connection_start()).await?;

    let mut consumers: Vec<Consumer> = Vec::new();
    let mut delivery_tag: u64 = 0;
    // delivery-tag → (queue graph node id) for ack finalization.
    let mut unacked: std::collections::HashMap<u64, String> = std::collections::HashMap::new();

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
                    // Poll timeout: pump deliveries to every active consumer.
                    pump_consumers(
                        socket,
                        &state,
                        &graph,
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
            FRAME_HEARTBEAT => continue,
            FRAME_METHOD => {}
            // A stray header/body outside a publish is ignored (spec-tolerant).
            _ => continue,
        }
        let Some(mc) = parse_method(&frame.payload) else {
            continue;
        };
        let ch = frame.channel;

        match (mc.class, mc.method) {
            // connection.start-ok → tune
            (C_CONNECTION, 11) => {
                write_frame(socket, FRAME_METHOD, 0, &build_connection_tune()).await?;
            }
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
                let mut c = Cursor::new(&mc.args);
                c.u16(); // reserved-1
                let exchange = c.shortstr();
                let kind = c.shortstr();
                let kind = if kind.is_empty() { "direct".into() } else { kind };
                let _ = engine_call(
                    &state,
                    &graph,
                    Method::DeclareExchange { exchange, kind },
                )
                .await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_EXCHANGE, 11)).await?;
            }
            // exchange.delete
            (C_EXCHANGE, 20) => {
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let exchange = c.shortstr();
                let _ = engine_call(&state, &graph, Method::DeleteExchange { exchange }).await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_EXCHANGE, 21)).await?;
            }
            // queue.declare
            (C_QUEUE, 10) => {
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let mut queue = c.shortstr();
                if queue.is_empty() {
                    queue = format!("amq.gen-{}", next_req_id());
                }
                // Ensure the queue's durable seq counter exists so it is publishable.
                let _ = engine_call(
                    &state,
                    &graph,
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
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let queue = c.shortstr();
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                let _ = engine_call(
                    &state,
                    &graph,
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
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let queue = c.shortstr();
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                let _ = engine_call(
                    &state,
                    &graph,
                    Method::UnbindQueue {
                        exchange,
                        queue,
                        routing_key,
                    },
                )
                .await;
                write_frame(socket, FRAME_METHOD, ch, &method_header(C_QUEUE, 51)).await?;
            }
            // basic.publish → read content header + body, then Publish
            (C_BASIC, 40) => {
                let mut c = Cursor::new(&mc.args);
                c.u16(); // reserved-1
                let exchange = c.shortstr();
                let routing_key = c.shortstr();
                let body = read_content_body(socket).await?;
                let _ = engine_call(
                    &state,
                    &graph,
                    Method::Publish {
                        exchange,
                        routing_key,
                        payload: body,
                    },
                )
                .await;
            }
            // basic.consume → consume-ok, register subscription
            (C_BASIC, 20) => {
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let queue = c.shortstr();
                let mut tag = c.shortstr();
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
                });
            }
            // basic.get → get-ok + content, or get-empty
            (C_BASIC, 70) => {
                let mut c = Cursor::new(&mc.args);
                c.u16();
                let queue = c.shortstr();
                match claim_one(&state, &graph, &queue).await {
                    Some((node_id, rk, ex, body)) => {
                        delivery_tag += 1;
                        unacked.insert(delivery_tag, node_id);
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
                let mut c = Cursor::new(&mc.args);
                let tag = c.u64();
                if let Some(node_id) = unacked.remove(&tag) {
                    ack_message(&state, &graph, &node_id).await;
                }
            }
            // Unhandled method: acknowledge nothing (spec-tolerant no-op).
            _ => {}
        }
    }
    Ok(())
}

/// Deliver up to a bounded batch of pending messages to each active consumer.
async fn pump_consumers(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    consumers: &[Consumer],
    delivery_tag: &mut u64,
    unacked: &mut std::collections::HashMap<u64, String>,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for cons in consumers {
        for _ in 0..MAX_PER_POLL {
            let Some((node_id, rk, ex, body)) = claim_one(state, graph, &cons.queue).await else {
                break;
            };
            *delivery_tag += 1;
            unacked.insert(*delivery_tag, node_id);
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

/// Read the content-header frame (for the body size) then accumulate body frames.
async fn read_content_body(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    // Content header frame.
    let header = match read_frame(socket).await? {
        Some(f) if f.kind == FRAME_HEADER => f,
        _ => return Ok(Vec::new()),
    };
    // header payload: class(2) weight(2) body-size(8) property-flags(2)…
    if header.payload.len() < 12 {
        return Ok(Vec::new());
    }
    let body_size = u64::from_be_bytes([
        header.payload[4],
        header.payload[5],
        header.payload[6],
        header.payload[7],
        header.payload[8],
        header.payload[9],
        header.payload[10],
        header.payload[11],
    ]) as usize;
    let mut body = Vec::with_capacity(body_size);
    while body.len() < body_size {
        match read_frame(socket).await? {
            Some(f) if f.kind == FRAME_BODY => body.extend_from_slice(&f.payload),
            _ => break,
        }
    }
    Ok(body)
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
    let mut buf = Vec::with_capacity(payload.len() + 8);
    buf.push(kind);
    put_u16(&mut buf, channel);
    put_u32(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    buf.push(FRAME_END);
    socket.write_all(&buf).await
}

fn parse_method(payload: &[u8]) -> Option<MethodCall> {
    if payload.len() < 4 {
        return None;
    }
    let class = u16::from_be_bytes([payload[0], payload[1]]);
    let method = u16::from_be_bytes([payload[2], payload[3]]);
    Some(MethodCall {
        class,
        method,
        args: payload[4..].to_vec(),
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
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }
    fn u16(&mut self) -> u16 {
        if self.i + 2 > self.b.len() {
            return 0;
        }
        let x = u16::from_be_bytes([self.b[self.i], self.b[self.i + 1]]);
        self.i += 2;
        x
    }
    fn u64(&mut self) -> u64 {
        if self.i + 8 > self.b.len() {
            return 0;
        }
        let mut a = [0u8; 8];
        a.copy_from_slice(&self.b[self.i..self.i + 8]);
        self.i += 8;
        u64::from_be_bytes(a)
    }
    fn shortstr(&mut self) -> String {
        if self.i >= self.b.len() {
            return String::new();
        }
        let len = self.b[self.i] as usize;
        self.i += 1;
        let end = (self.i + len).min(self.b.len());
        let s = String::from_utf8_lossy(&self.b[self.i..end]).into_owned();
        self.i = end;
        s
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-275 — codec + arg-cursor unit tests (the byte layouts the hand-rolled
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
}
