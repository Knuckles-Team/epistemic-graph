//! MQTT 3.1.1 (+ basic 5.0) wire-protocol listener (CONCEPT:EG-KG.query.mqtt-packet-codec) — a HAND-ROLLED
//! MQTT broker front-end that lets a standard MQTT client (paho, mosquitto_pub/sub,
//! MQTT.js) speak to the native message broker built on the KG-2.303 work-queue.
//!
//! ## What this is (and is NOT)
//! Like the amqp-wire / pgwire / redis-wire shims, this is an ADAPTER, not a second
//! broker. Every exchange/binding/queue/message lives as graph nodes on a control graph
//! (`crate::broker`); this module only frames MQTT control packets on the wire and maps
//! each onto the SAME broker primitives THROUGH the engine dispatch
//! (`crate::server::dispatch::dispatch`) the AMQP wire (CONCEPT:EG-KG.compute.message-broker-exchanges/276..280) uses —
//! no parallel mechanism, no new broker method.
//!
//! MQTT topics map naturally onto a broker TOPIC exchange + bindings: a client PUBLISH
//! routes through `Method::Publish` (topic routing), and each SUBSCRIBE topic filter
//! (with MQTT `+`/`#` wildcards) binds a per-session queue to that exchange, whose
//! matching messages are streamed back to the subscriber via the KG-2.303 claim path
//! (`Method::ClaimNext` + `CompareAndSetNodeFields`, exactly as amqp-wire's consume).
//!
//! It links NO MQTT crate — every byte layout is hand-rolled against the published MQTT
//! 3.1.1 spec (the Pi-contract idiom pgwire / amqp-wire / redis-wire use), so a
//! default/pi build carries zero MQTT dependency.
//!
//! ## Protocol subset (CONCEPT:EG-KG.query.mqtt-packet-codec)
//! LANDED: CONNECT/CONNACK, PUBLISH (QoS 0 + QoS 1 with PUBACK), SUBSCRIBE/SUBACK
//! (topic filters incl. `+`/`#` wildcards → broker topic bindings), UNSUBSCRIBE/
//! UNSUBACK, PINGREQ/PINGRESP, DISCONNECT. Auth is a localhost TRUST surface (any
//! CONNECT accepted, like the SQL wires' trust mode). DEFERRED (a client degrades
//! gracefully): retained messages, will messages, QoS 2 (PUBREC/PUBREL/PUBCOMP), TLS,
//! and session persistence.
//!
//! ## Publisher confirms + idempotent publish (CONCEPT:EG-KG.ingest.mqtt-publish-property-block / EG-284)
//! MQTT's QoS-1 PUBLISH → PUBACK already IS the publisher-confirm surface (the broker
//! durably enqueues, then the PUBACK acknowledges), so no extra frame is needed. Every
//! PUBLISH is routed through `Method::PublishIdempotent` (CONCEPT:EG-KG.ingest.mqtt-publish-property-block): an MQTT 5.0
//! client MAY attach the User Properties `producer-id` + `producer-seq`, and the broker
//! dedups a re-published `(producer-id, seq)` against that producer's durable
//! high-water mark for effectively-once delivery — the QoS-1 PUBACK is still returned
//! for the (dropped) duplicate. A PUBLISH with no producer properties (or an MQTT 3.1.1
//! client) is unchanged (at-least-once). EG-283 stream reads have no MQTT frame and are
//! reached through the RPC surface (`Method::StreamRead`), as on the AMQP wire.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::auth::compute_auth_token;
use crate::server::dispatch::dispatch;
use crate::server::ServerState;

/// Env var: when set (and the binary is built `--features mqtt-wire`), the MQTT wire
/// listener binds this address (documented loopback default `127.0.0.1:1883`). Unset ⇒
/// no listener.
pub const MQTT_ADDR_ENV: &str = "EPISTEMIC_GRAPH_MQTT_ADDR";
/// Env var: the control graph broker state lives on. Defaults to `__commons__`
/// (mirrors amqp-wire's default-graph idiom).
pub const MQTT_GRAPH_ENV: &str = "EPISTEMIC_GRAPH_MQTT_GRAPH";
/// Env var: the broker TOPIC exchange MQTT publish/subscribe routes through. Defaults
/// to `amq.topic` (the conventional topic-exchange name).
pub const MQTT_EXCHANGE_ENV: &str = "EPISTEMIC_GRAPH_MQTT_EXCHANGE";

const DEFAULT_EXCHANGE: &str = "amq.topic";

// ── MQTT control packet types (high nibble of byte 1) ─────────────────────
const PKT_CONNECT: u8 = 1;
const PKT_CONNACK: u8 = 2;
const PKT_PUBLISH: u8 = 3;
const PKT_PUBACK: u8 = 4;
const PKT_SUBSCRIBE: u8 = 8;
const PKT_SUBACK: u8 = 9;
const PKT_UNSUBSCRIBE: u8 = 10;
const PKT_UNSUBACK: u8 = 11;
const PKT_PINGREQ: u8 = 12;
const PKT_PINGRESP: u8 = 13;
const PKT_DISCONNECT: u8 = 14;

static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Serve the MQTT wire protocol on `addr` until the listener errors (CONCEPT:EG-KG.query.mqtt-packet-codec).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let graph = std::env::var(MQTT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let exchange =
        std::env::var(MQTT_EXCHANGE_ENV).unwrap_or_else(|_| DEFAULT_EXCHANGE.to_string());
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "mqtt-wire: serving MQTT 3.1.1 on {} (broker graph '{}', topic exchange '{}')",
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
                tracing::debug!("mqtt-wire connection from {peer} ended: {e}");
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

// ── Topic ↔ routing-key translation (CONCEPT:EG-KG.query.mqtt-packet-codec) ──────────────────────

/// An MQTT PUBLISH topic (`sport/tennis`) → broker routing key (`sport.tennis`). MQTT
/// levels are `/`-delimited; the broker's topic matcher is `.`-delimited word lists.
fn mqtt_topic_to_key(topic: &str) -> String {
    topic.replace('/', ".")
}

/// A broker routing key (`sport.tennis`) → MQTT topic (`sport/tennis`) for delivery.
fn key_to_mqtt_topic(key: &str) -> String {
    key.replace('.', "/")
}

/// Scan an MQTT 5.0 PUBLISH property block for the idempotency User Properties
/// `producer-id` + `producer-seq` (CONCEPT:EG-KG.ingest.mqtt-publish-property-block), returning `(producer_id,
/// producer_seq)`. Steps over the other PUBLISH properties by their known wire widths;
/// an unrecognised property id ends the scan (its width is undeterminable) with whatever
/// was found so far. `producer-seq` accepts a decimal string value.
fn parse_publish_properties(bytes: &[u8]) -> (Option<String>, Option<i64>) {
    let mut c = Cursor::new(bytes);
    let mut producer_id: Option<String> = None;
    let mut producer_seq: Option<i64> = None;
    while c.remaining() > 0 {
        let id = c.u8();
        match id {
            0x01 => {
                c.u8();
            } // payload format indicator (byte)
            0x02 => {
                c.u32();
            } // message expiry interval (4 bytes)
            0x23 => {
                c.u16();
            } // topic alias (2 bytes)
            0x03 | 0x08 => {
                let _ = c.mqtt_str();
            } // content-type / response-topic (UTF-8)
            0x09 => {
                let n = c.u16() as usize;
                let _ = c.take(n);
            } // correlation data (binary)
            0x0B => {
                c.varint();
            } // subscription identifier (varint)
            0x26 => {
                // User Property: a UTF-8 string pair.
                let key = c.mqtt_str();
                let val = c.mqtt_str();
                match key.as_str() {
                    "producer-id" => producer_id = Some(val),
                    "producer-seq" => producer_seq = val.parse().ok(),
                    _ => {}
                }
            }
            _ => break, // unknown property — width unknown, stop the scan
        }
    }
    (producer_id, producer_seq)
}

/// An MQTT SUBSCRIBE topic FILTER (`sport/+/#`) → broker topic-binding pattern
/// (`sport.*.#`). MQTT `+` (one level) maps to the broker `*` (one word); MQTT `#`
/// (zero-or-more trailing levels) maps to the broker `#` (zero-or-more words) as-is.
fn mqtt_filter_to_pattern(filter: &str) -> String {
    filter.replace('/', ".").replace('+', "*")
}

// ── Per-connection state ──────────────────────────────────────────────────

/// An active MQTT subscription: the topic-binding pattern (for cleanup).
struct Subscription {
    pattern: String,
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
) -> std::io::Result<()> {
    // ── CONNECT ──
    let Some((ptype, _flags, payload)) = read_packet(socket).await? else {
        return Ok(()); // clean EOF before CONNECT
    };
    if ptype != PKT_CONNECT {
        return Ok(()); // protocol violation → drop silently
    }
    let version = parse_connect_version(&payload);
    write_packet(socket, PKT_CONNACK << 4, &build_connack(version)).await?;

    // Ensure the shared broker TOPIC exchange exists (idempotent).
    let _ = engine_call(
        &state,
        &graph,
        Method::DeclareExchange {
            exchange: exchange.clone(),
            kind: "topic".to_string(),
        },
    )
    .await;

    // A per-session queue receives this subscriber's routed copies.
    let session_queue = format!("mqtt.{}", next_req_id());
    let mut subs: Vec<Subscription> = Vec::new();
    // ack-id (packet id) → node id for QoS-1 delivery finalization (unused for QoS 0).
    let mut unacked: std::collections::HashMap<u16, String> = std::collections::HashMap::new();

    loop {
        // With active subscriptions, bound the read so the delivery pump can run.
        let pkt = if subs.is_empty() {
            match read_packet(socket).await? {
                Some(p) => p,
                None => break,
            }
        } else {
            match tokio::time::timeout(std::time::Duration::from_millis(200), read_packet(socket))
                .await
            {
                Ok(Ok(Some(p))) => p,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    pump_subscription(socket, &state, &graph, &session_queue, version).await?;
                    continue;
                }
            }
        };
        let (ptype, flags, payload) = pkt;

        match ptype {
            PKT_PUBLISH => {
                let qos = (flags >> 1) & 0x03;
                let mut c = Cursor::new(&payload);
                let topic = c.mqtt_str();
                let packet_id = if qos > 0 { c.u16() } else { 0 };
                // MQTT 5.0 property block → extract the EG-314 idempotency user
                // properties (`producer-id` / `producer-seq`); MQTT 3.1.1 has none.
                let (producer_id, producer_seq) = if version >= 5 {
                    let props = c.take_props();
                    parse_publish_properties(&props)
                } else {
                    (None, None)
                };
                let body = c.rest();
                // Route through the idempotent path — with no producer-id it is a plain
                // at-least-once publish (byte-identical to before); with one the broker
                // dedups (CONCEPT:EG-KG.ingest.mqtt-publish-property-block).
                let _ = engine_call(
                    &state,
                    &graph,
                    Method::PublishIdempotent {
                        exchange: exchange.clone(),
                        routing_key: mqtt_topic_to_key(&topic),
                        payload: body,
                        producer_id,
                        seq: producer_seq.unwrap_or(0),
                        priority: 0,
                        delay_ms: None,
                        ttl_ms: None,
                        now_ms: None,
                    },
                )
                .await;
                // QoS 1 requires a PUBACK echoing the packet id (QoS 0 is fire-and-forget;
                // QoS 2 is DEFERRED — such a PUBLISH is still routed but not acked here).
                if qos == 1 {
                    write_packet(socket, PKT_PUBACK << 4, &packet_id.to_be_bytes()).await?;
                }
            }
            PKT_SUBSCRIBE => {
                let mut c = Cursor::new(&payload);
                let packet_id = c.u16();
                if version >= 5 {
                    c.skip_props();
                }
                let mut granted: Vec<u8> = Vec::new();
                while c.remaining() >= 2 {
                    let filter = c.mqtt_str();
                    let _opts = c.u8(); // requested QoS / v5 sub-options (low 2 bits = QoS)
                    if filter.is_empty() {
                        break;
                    }
                    let pattern = mqtt_filter_to_pattern(&filter);
                    let _ = engine_call(
                        &state,
                        &graph,
                        Method::BindQueue {
                            exchange: exchange.clone(),
                            queue: session_queue.clone(),
                            routing_key: pattern.clone(),
                        },
                    )
                    .await;
                    subs.push(Subscription { pattern });
                    granted.push(0x00); // granted QoS 0
                }
                write_packet(
                    socket,
                    PKT_SUBACK << 4,
                    &build_suback(packet_id, &granted, version),
                )
                .await?;
            }
            PKT_UNSUBSCRIBE => {
                let mut c = Cursor::new(&payload);
                let packet_id = c.u16();
                if version >= 5 {
                    c.skip_props();
                }
                let mut count = 0usize;
                while c.remaining() >= 2 {
                    let filter = c.mqtt_str();
                    if filter.is_empty() {
                        break;
                    }
                    let pattern = mqtt_filter_to_pattern(&filter);
                    let _ = engine_call(
                        &state,
                        &graph,
                        Method::UnbindQueue {
                            exchange: exchange.clone(),
                            queue: session_queue.clone(),
                            routing_key: pattern.clone(),
                        },
                    )
                    .await;
                    subs.retain(|s| s.pattern != pattern);
                    count += 1;
                }
                write_packet(
                    socket,
                    PKT_UNSUBACK << 4,
                    &build_unsuback(packet_id, count, version),
                )
                .await?;
            }
            PKT_PUBACK if payload.len() >= 2 => {
                // A subscriber's QoS-1 delivery acknowledgement (echoed packet id).
                let id = u16::from_be_bytes([payload[0], payload[1]]);
                if let Some(node_id) = unacked.remove(&id) {
                    ack_message(&state, &graph, &node_id).await;
                }
            }
            PKT_PINGREQ => {
                write_packet(socket, PKT_PINGRESP << 4, &[]).await?;
            }
            PKT_DISCONNECT => break,
            _ => {} // spec-tolerant no-op
        }
    }

    // Best-effort teardown: unbind this session's routes.
    for s in &subs {
        let _ = engine_call(
            &state,
            &graph,
            Method::UnbindQueue {
                exchange: exchange.clone(),
                queue: session_queue.clone(),
                routing_key: s.pattern.clone(),
            },
        )
        .await;
    }
    Ok(())
}

/// Deliver a bounded batch of pending messages from the session queue as QoS-0 PUBLISH
/// packets (ack'd immediately). CONCEPT:EG-KG.query.mqtt-packet-codec — the poll-driven push pump.
async fn pump_subscription(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    queue: &str,
    version: u8,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for _ in 0..MAX_PER_POLL {
        let Some((node_id, rk, body)) = claim_one(state, graph, queue).await else {
            break;
        };
        let topic = key_to_mqtt_topic(&rk);
        let mut p = Vec::new();
        put_mqtt_str(&mut p, &topic);
        if version >= 5 {
            p.push(0); // MQTT 5.0 property length = 0
        }
        p.extend_from_slice(&body);
        // Header byte: PUBLISH, QoS 0 (flags 0).
        write_packet(socket, PKT_PUBLISH << 4, &p).await?;
        // QoS-0 delivery: finalize immediately.
        ack_message(state, graph, &node_id).await;
    }
    Ok(())
}

// ── Packet codec ──────────────────────────────────────────────────────────

/// Read one MQTT control packet: `(type, flags, variable-header+payload bytes)`. A clean
/// EOF at a packet boundary ⇒ `None`.
async fn read_packet(socket: &mut TcpStream) -> std::io::Result<Option<(u8, u8, Vec<u8>)>> {
    let mut b0 = [0u8; 1];
    if let Err(e) = socket.read_exact(&mut b0).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }
    let ptype = b0[0] >> 4;
    let flags = b0[0] & 0x0f;
    // Remaining-length: variable-byte integer (1..4 bytes).
    let mut mult: usize = 1;
    let mut len: usize = 0;
    for _ in 0..4 {
        let mut nb = [0u8; 1];
        socket.read_exact(&mut nb).await?;
        len += (nb[0] & 0x7f) as usize * mult;
        if nb[0] & 0x80 == 0 {
            break;
        }
        mult *= 128;
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        socket.read_exact(&mut payload).await?;
    }
    Ok(Some((ptype, flags, payload)))
}

/// Write one MQTT control packet: fixed-header byte (`type<<4 | flags`) + variable-byte
/// remaining length + payload.
async fn write_packet(
    socket: &mut TcpStream,
    header_byte: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(payload.len() + 5);
    buf.push(header_byte);
    encode_remaining_length(payload.len(), &mut buf);
    buf.extend_from_slice(payload);
    socket.write_all(&buf).await
}

/// Encode `len` as an MQTT variable-byte integer, appending to `out`.
fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

/// Decode an MQTT variable-byte integer → `(value, bytes_consumed)`, or `None` if the
/// buffer is truncated / malformed (>4 continuation bytes).
fn decode_remaining_length(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut mult: usize = 1;
    let mut value: usize = 0;
    for (i, &b) in bytes.iter().enumerate().take(4) {
        value += (b & 0x7f) as usize * mult;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        mult *= 128;
    }
    None
}

/// Append a UTF-8 MQTT string (2-byte big-endian length prefix + bytes).
fn put_mqtt_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    out.extend_from_slice(b);
}

// ── Handshake / ack builders ──────────────────────────────────────────────

/// Extract the MQTT protocol level (4 = 3.1.1, 5 = 5.0) from a CONNECT payload; defaults
/// to 4 on a short/odd packet.
fn parse_connect_version(payload: &[u8]) -> u8 {
    let mut c = Cursor::new(payload);
    let _proto_name = c.mqtt_str();
    let level = c.u8();
    if level == 0 {
        4
    } else {
        level
    }
}

/// CONNACK: session-present=0 + reason/return-code=0 (accepted). MQTT 5.0 appends a
/// zero-length property block.
fn build_connack(version: u8) -> Vec<u8> {
    if version >= 5 {
        vec![0x00, 0x00, 0x00] // ack flags, reason code, property length = 0
    } else {
        vec![0x00, 0x00] // ack flags, return code
    }
}

/// SUBACK: packet id + one granted-QoS byte per filter (MQTT 5.0 inserts a zero-length
/// property block after the packet id).
fn build_suback(packet_id: u16, granted: &[u8], version: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(granted.len() + 3);
    p.extend_from_slice(&packet_id.to_be_bytes());
    if version >= 5 {
        p.push(0); // property length = 0
    }
    p.extend_from_slice(granted);
    p
}

/// UNSUBACK: packet id (MQTT 5.0 adds a zero-length property block + one reason byte per
/// unsubscribed filter).
fn build_unsuback(packet_id: u16, count: usize, version: u8) -> Vec<u8> {
    let mut p = Vec::with_capacity(count + 3);
    p.extend_from_slice(&packet_id.to_be_bytes());
    if version >= 5 {
        p.push(0); // property length = 0
        p.resize(p.len() + count, 0x00); // one success reason byte per filter
    }
    p
}

// ── Read cursor over packet bytes ─────────────────────────────────────────

/// A minimal read cursor over MQTT variable-header / payload bytes.
struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, i: 0 }
    }
    fn u8(&mut self) -> u8 {
        if self.i >= self.b.len() {
            return 0;
        }
        let x = self.b[self.i];
        self.i += 1;
        x
    }
    fn u16(&mut self) -> u16 {
        if self.i + 2 > self.b.len() {
            return 0;
        }
        let x = u16::from_be_bytes([self.b[self.i], self.b[self.i + 1]]);
        self.i += 2;
        x
    }
    fn u32(&mut self) -> u32 {
        if self.i + 4 > self.b.len() {
            return 0;
        }
        let mut a = [0u8; 4];
        a.copy_from_slice(&self.b[self.i..self.i + 4]);
        self.i += 4;
        u32::from_be_bytes(a)
    }
    /// Read `n` bytes (clamped), advancing the cursor.
    fn take(&mut self, n: usize) -> &'a [u8] {
        let end = (self.i + n).min(self.b.len());
        let out = &self.b[self.i.min(self.b.len())..end];
        self.i = end;
        out
    }
    /// A variable-byte-length-prefixed MQTT binary block (a `u16`-length-prefixed slot
    /// is `mqtt_str`; this is the property-block form). Returns the block bytes and
    /// advances past them (CONCEPT:EG-KG.ingest.mqtt-publish-property-block — reach the PUBLISH property block).
    fn take_props(&mut self) -> Vec<u8> {
        let rem = &self.b[self.i.min(self.b.len())..];
        if let Some((plen, consumed)) = decode_remaining_length(rem) {
            let start = self.i + consumed;
            let end = (start + plen).min(self.b.len());
            let out = self.b[start.min(self.b.len())..end].to_vec();
            self.i = end;
            out
        } else {
            self.i = self.b.len();
            Vec::new()
        }
    }
    /// A length-prefixed UTF-8 MQTT string.
    fn mqtt_str(&mut self) -> String {
        let len = self.u16() as usize;
        let end = (self.i + len).min(self.b.len());
        let s = String::from_utf8_lossy(&self.b[self.i..end]).into_owned();
        self.i = end;
        s
    }
    /// Skip an MQTT 5.0 property block (a variable-byte length then that many bytes).
    fn skip_props(&mut self) {
        if let Some((plen, consumed)) = decode_remaining_length(&self.b[self.i.min(self.b.len())..])
        {
            self.i = (self.i + consumed + plen).min(self.b.len());
        }
    }
    fn remaining(&self) -> usize {
        self.b.len().saturating_sub(self.i)
    }
    /// Read an MQTT variable-byte integer, advancing the cursor (used for the MQTT 5.0
    /// Subscription-Identifier property).
    fn varint(&mut self) -> usize {
        let rem = &self.b[self.i.min(self.b.len())..];
        if let Some((val, consumed)) = decode_remaining_length(rem) {
            self.i = (self.i + consumed).min(self.b.len());
            val
        } else {
            self.i = self.b.len();
            0
        }
    }
    fn rest(&mut self) -> Vec<u8> {
        let out = self.b[self.i.min(self.b.len())..].to_vec();
        self.i = self.b.len();
        out
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.query.mqtt-packet-codec — MQTT packet-codec unit tests (the byte layouts the hand-rolled
    //! framing depends on) + a served listener round-trip (CONNECT/SUBSCRIBE/PUBLISH/
    //! deliver) that proves the mapping onto the broker end-to-end.
    use super::*;

    #[test]
    fn eg281_remaining_length_roundtrips() {
        for n in [0usize, 1, 127, 128, 16_383, 16_384, 2_097_151, 268_435_455] {
            let mut buf = Vec::new();
            encode_remaining_length(n, &mut buf);
            let (val, consumed) = decode_remaining_length(&buf).unwrap();
            assert_eq!(val, n, "value {n}");
            assert_eq!(consumed, buf.len(), "consumed all bytes for {n}");
        }
    }

    #[test]
    fn eg281_mqtt_string_roundtrips_through_cursor() {
        let mut buf = Vec::new();
        put_mqtt_str(&mut buf, "sport/tennis");
        put_mqtt_str(&mut buf, "");
        let mut c = Cursor::new(&buf);
        assert_eq!(c.mqtt_str(), "sport/tennis");
        assert_eq!(c.mqtt_str(), "");
        assert_eq!(c.mqtt_str(), ""); // exhausted
    }

    #[test]
    fn eg281_connack_v4_and_v5_shape() {
        assert_eq!(build_connack(4), vec![0x00, 0x00]);
        assert_eq!(build_connack(5), vec![0x00, 0x00, 0x00]);
    }

    #[test]
    fn eg281_puback_encodes_packet_id() {
        // A QoS-1 PUBLISH is answered with a PUBACK carrying the same packet id.
        let id: u16 = 0x1234;
        let bytes = id.to_be_bytes();
        assert_eq!(bytes, [0x12, 0x34]);
        assert_eq!(u16::from_be_bytes([bytes[0], bytes[1]]), id);
    }

    #[test]
    fn eg281_filter_to_pattern_maps_wildcards() {
        assert_eq!(mqtt_filter_to_pattern("sport/tennis"), "sport.tennis");
        assert_eq!(mqtt_filter_to_pattern("sport/+/player1"), "sport.*.player1");
        assert_eq!(mqtt_filter_to_pattern("sport/#"), "sport.#");
        assert_eq!(mqtt_filter_to_pattern("#"), "#");
        // The translated pattern matches the broker's topic matcher as expected.
        assert!(crate::broker::topic_matches(
            &mqtt_filter_to_pattern("sport/+/player1"),
            &mqtt_topic_to_key("sport/tennis/player1")
        ));
        assert!(crate::broker::topic_matches(
            &mqtt_filter_to_pattern("sport/#"),
            &mqtt_topic_to_key("sport")
        ));
    }

    #[test]
    fn eg281_topic_key_roundtrips() {
        assert_eq!(mqtt_topic_to_key("a/b/c"), "a.b.c");
        assert_eq!(key_to_mqtt_topic("a.b.c"), "a/b/c");
    }

    #[test]
    fn eg281_parse_publish_qos1_yields_topic_id_and_payload() {
        // Build a QoS-1 PUBLISH variable header + payload and parse it back.
        let mut p = Vec::new();
        put_mqtt_str(&mut p, "sensors/temp");
        p.extend_from_slice(&0x0007u16.to_be_bytes()); // packet id
        p.extend_from_slice(b"22.5");
        let mut c = Cursor::new(&p);
        assert_eq!(c.mqtt_str(), "sensors/temp");
        assert_eq!(c.u16(), 7);
        assert_eq!(c.rest(), b"22.5".to_vec());
    }

    #[test]
    fn eg281_parse_subscribe_topic_filters_with_wildcards() {
        // packet id + two (filter, qos) pairs.
        let mut p = Vec::new();
        p.extend_from_slice(&0x0001u16.to_be_bytes());
        put_mqtt_str(&mut p, "sport/+/score");
        p.push(0x01); // requested QoS 1
        put_mqtt_str(&mut p, "news/#");
        p.push(0x00); // requested QoS 0
        let mut c = Cursor::new(&p);
        assert_eq!(c.u16(), 1);
        let mut filters = Vec::new();
        while c.remaining() >= 2 {
            let f = c.mqtt_str();
            let _q = c.u8();
            filters.push(f);
        }
        assert_eq!(filters, vec!["sport/+/score", "news/#"]);
        assert_eq!(mqtt_filter_to_pattern(&filters[0]), "sport.*.score");
    }

    // ── CONCEPT:EG-KG.ingest.mqtt-publish-property-block idempotent publish over MQTT 5.0 user properties ───

    #[test]
    fn eg314_parse_publish_properties_extracts_producer_user_props() {
        let mut props = Vec::new();
        // A content-type property first, to prove non-user props are stepped over.
        props.push(0x03);
        put_mqtt_str(&mut props, "text/plain");
        // producer-id + producer-seq user properties (0x26).
        props.push(0x26);
        put_mqtt_str(&mut props, "producer-id");
        put_mqtt_str(&mut props, "prod-9");
        props.push(0x26);
        put_mqtt_str(&mut props, "producer-seq");
        put_mqtt_str(&mut props, "5");
        let (id, seq) = parse_publish_properties(&props);
        assert_eq!(id.as_deref(), Some("prod-9"));
        assert_eq!(seq, Some(5));
    }

    #[test]
    fn eg314_parse_publish_properties_absent_is_none() {
        assert_eq!(parse_publish_properties(&[]), (None, None));
    }

    #[test]
    fn eg314_take_props_reads_varint_length_prefixed_block() {
        let mut block = Vec::new();
        block.push(0x26);
        put_mqtt_str(&mut block, "producer-id");
        put_mqtt_str(&mut block, "P");
        let mut buf = Vec::new();
        encode_remaining_length(block.len(), &mut buf); // property-length prefix
        buf.extend_from_slice(&block);
        buf.extend_from_slice(b"BODY"); // payload after the property block
        let mut c = Cursor::new(&buf);
        assert_eq!(c.take_props(), block);
        assert_eq!(c.rest(), b"BODY".to_vec());
    }

    #[test]
    fn eg281_connect_version_parses_protocol_level() {
        let mut v4 = Vec::new();
        put_mqtt_str(&mut v4, "MQTT");
        v4.push(4);
        assert_eq!(parse_connect_version(&v4), 4);
        let mut v5 = Vec::new();
        put_mqtt_str(&mut v5, "MQTT");
        v5.push(5);
        assert_eq!(parse_connect_version(&v5), 5);
    }

    // ── Served listener round-trip (CONCEPT:EG-KG.query.mqtt-packet-codec) ───────────────────────

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

    #[tokio::test]
    async fn eg281_listener_connect_subscribe_publish_deliver_roundtrip() {
        let addr = spawn_listener().await;

        // ── Subscriber connects + subscribes to `sensors/#`. ──
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        let mut connect = Vec::new();
        put_mqtt_str(&mut connect, "MQTT");
        connect.push(4); // protocol level 3.1.1
        connect.push(0x02); // connect flags: clean session
        connect.extend_from_slice(&0u16.to_be_bytes()); // keepalive
        put_mqtt_str(&mut connect, "sub-client"); // client id
        write_packet(&mut sub, PKT_CONNECT << 4, &connect)
            .await
            .unwrap();
        let (ptype, _f, _p) = read_packet(&mut sub).await.unwrap().unwrap();
        assert_eq!(ptype, PKT_CONNACK);

        let mut subscribe = Vec::new();
        subscribe.extend_from_slice(&0x0001u16.to_be_bytes()); // packet id
        put_mqtt_str(&mut subscribe, "sensors/#");
        subscribe.push(0x00); // QoS 0
        write_packet(&mut sub, (PKT_SUBSCRIBE << 4) | 0x02, &subscribe)
            .await
            .unwrap();
        let (ptype, _f, sp) = read_packet(&mut sub).await.unwrap().unwrap();
        assert_eq!(ptype, PKT_SUBACK);
        assert_eq!(&sp[0..2], &0x0001u16.to_be_bytes()); // packet id echoed
        assert_eq!(sp[2], 0x00); // granted QoS 0

        // ── Publisher connects + publishes QoS 1 to `sensors/temp`. ──
        let mut pubc = TcpStream::connect(&addr).await.unwrap();
        let mut connect2 = Vec::new();
        put_mqtt_str(&mut connect2, "MQTT");
        connect2.push(4);
        connect2.push(0x02);
        connect2.extend_from_slice(&0u16.to_be_bytes());
        put_mqtt_str(&mut connect2, "pub-client");
        write_packet(&mut pubc, PKT_CONNECT << 4, &connect2)
            .await
            .unwrap();
        let (ptype, _f, _p) = read_packet(&mut pubc).await.unwrap().unwrap();
        assert_eq!(ptype, PKT_CONNACK);

        let mut publish = Vec::new();
        put_mqtt_str(&mut publish, "sensors/temp");
        publish.extend_from_slice(&0x0009u16.to_be_bytes()); // packet id
        publish.extend_from_slice(b"22.5");
        // Header byte: PUBLISH type + QoS 1 (flag bit 1 set).
        write_packet(&mut pubc, (PKT_PUBLISH << 4) | 0x02, &publish)
            .await
            .unwrap();
        // Publisher must receive a PUBACK for the QoS-1 publish.
        let (ptype, _f, ap) = read_packet(&mut pubc).await.unwrap().unwrap();
        assert_eq!(ptype, PKT_PUBACK);
        assert_eq!(&ap[0..2], &0x0009u16.to_be_bytes());

        // ── Subscriber receives the routed PUBLISH (QoS 0). ──
        let (ptype, flags, dp) =
            tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut sub))
                .await
                .expect("delivery within timeout")
                .unwrap()
                .unwrap();
        assert_eq!(ptype, PKT_PUBLISH);
        assert_eq!((flags >> 1) & 0x03, 0, "delivered at QoS 0");
        let mut c = Cursor::new(&dp);
        assert_eq!(c.mqtt_str(), "sensors/temp");
        assert_eq!(c.rest(), b"22.5".to_vec());
    }
}
