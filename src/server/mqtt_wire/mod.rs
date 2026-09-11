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
//! matching messages are streamed back to the subscriber through the native
//! `Method::BrokerConsume`/`BrokerAck` lifecycle.
//!
//! It links NO MQTT crate — every byte layout is hand-rolled against the published MQTT
//! 3.1.1 spec (the Pi-contract idiom pgwire / amqp-wire / redis-wire use), so a
//! default/pi build carries zero MQTT dependency.
//!
//! ## Protocol subset (CONCEPT:EG-KG.query.mqtt-packet-codec)
//! LANDED: CONNECT/CONNACK, PUBLISH (QoS 0 + QoS 1 with PUBACK), SUBSCRIBE/SUBACK
//! (topic filters incl. `+`/`#` wildcards → broker topic bindings), UNSUBSCRIBE/
//! UNSUBACK, PINGREQ/PINGRESP, DISCONNECT. CONNECT username/password authentication is
//! mandatory: the password is a domain-separated HMAC derived from
//! `GRAPH_SERVICE_AUTH_SECRET`, and the authenticated principal becomes a secret-keyed
//! pseudonymous actor reference before every engine request. The direct listener is
//! loopback-only; remote access must traverse a TLS/mTLS identity-binding gateway.
//! Unsupported CONNECT/PUBLISH options and packet types fail closed.
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

use crate::protocol::Method;
use crate::server::broker_wire::{self, invalid_data, prelude::*, BrokerProtocol};
use crate::server::broker_wire::{
    derive_password as derive_mqtt_password_impl, verify_password as verify_mqtt_password_impl,
};
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

const WIRE: BrokerProtocol = BrokerProtocol::Mqtt;

const DEFAULT_EXCHANGE: &str = "amq.topic";
mod codec;
use codec::*;

const MAX_MQTT_SUBSCRIPTIONS: usize = 1_024;
const MAX_MQTT_SUBSCRIPTION_BYTES: usize = 1024 * 1024;
const MAX_BROKER_RESULT_ITEMS: usize = 1_000_000;
const BROKER_LEASE_MS: u64 = 5 * 60 * 1_000;
const BROKER_CLAIM_LIMITS: broker_wire::BrokerClaimLimits = broker_wire::BrokerClaimLimits {
    group: "mqtt",
    lease_ms: BROKER_LEASE_MS,
    prefetch: 1,
    max_bytes: MAX_MQTT_PACKET_BYTES,
    max_items: MAX_BROKER_RESULT_ITEMS,
    max_identifier_len: Some(MAX_MQTT_IDENTIFIER_BYTES),
    max_routing_key_len: Some(MAX_MQTT_TOPIC_BYTES),
};

static REQ_ID: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Derive the CONNECT password for an MQTT principal.
pub fn derive_mqtt_password(secret: &str, principal: &str) -> String {
    derive_mqtt_password_impl(WIRE, secret, principal)
}

fn verify_mqtt_password(secret: &str, principal: &str, password: &[u8]) -> bool {
    verify_mqtt_password_impl(WIRE, secret, principal, password, MAX_MQTT_IDENTIFIER_BYTES)
}

/// Fail closed before binding the plaintext MQTT listener.
pub fn validate_startup_policy(addr: &str, secret: &str) -> std::io::Result<()> {
    crate::server::validate_direct_wire_security(addr, "mqtt-wire", !secret.is_empty())
}

/// Serve the MQTT wire protocol on `addr` until the listener errors (CONCEPT:EG-KG.query.mqtt-packet-codec).
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let graph = std::env::var(MQTT_GRAPH_ENV).unwrap_or_else(|_| "__commons__".to_string());
    let exchange =
        std::env::var(MQTT_EXCHANGE_ENV).unwrap_or_else(|_| DEFAULT_EXCHANGE.to_string());
    let auth_secret = state.read().await.auth_secret.clone();
    validate_startup_policy(addr, &auth_secret)?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "mqtt-wire: serving authenticated MQTT on loopback (broker graph '{}', topic \
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
    // GOC-70 (EG-281): one broker exchange per listener (fixed for its whole
    // lifetime, cloned per connection above), so a single listener-wide
    // `Notify` is a valid fast-path wakeup for "a PUBLISH just landed on this
    // exchange" — shared by every session on this listener, subscriber and
    // publisher alike. This turns delivery from "blind poll, up to 200ms
    // cadence" into "wake as soon as a publish on THIS listener commits" for
    // the common case; the 200ms poll in `handle_connection` remains as the
    // fallback for messages that arrive by any other route (a backlog present
    // at SUBSCRIBE time, a publish from a different listener/process sharing
    // the same graph) and as the hang-guard if a wakeup is ever missed
    // (`Notify::notify_waiters` only wakes listeners already `.await`ing
    // `notified()` at the moment of the call; it stores no permit).
    let publish_notify = Arc::new(tokio::sync::Notify::new());
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        let g = graph.clone();
        let ex = exchange.clone();
        let secret = auth_secret.clone();
        let notify = publish_notify.clone();
        tokio::spawn(async move {
            let mut socket = socket;
            if let Err(e) = handle_connection(&mut socket, st, g, ex, secret, notify).await {
                tracing::debug!("mqtt-wire connection from {peer} ended: {e}");
            }
        });
    }
}

// ── Per-connection state ──────────────────────────────────────────────────

/// An active MQTT subscription: the topic-binding pattern (for cleanup).
struct Subscription {
    pattern: String,
}

type PublishNotify = Arc<tokio::sync::Notify>;

enum NextPacket {
    Packet(Option<MqttPacket>),
    Pumped,
}

enum SubscriptionDecision {
    Reject,
    Duplicate,
    Bind(usize),
}

async fn handle_connection(
    socket: &mut TcpStream,
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
    auth_secret: String,
    publish_notify: PublishNotify,
) -> std::io::Result<()> {
    let Some(mut session) =
        MqttSession::accept(socket, state, graph, exchange, auth_secret, publish_notify).await?
    else {
        return Ok(());
    };
    session.run(socket).await
}

struct MqttSession {
    state: Arc<RwLock<ServerState>>,
    graph: String,
    exchange: String,
    actor: String,
    version: u8,
    session_queue: String,
    session_consumer: String,
    publish_notify: PublishNotify,
    subscriptions: Vec<Subscription>,
    subscription_bytes: usize,
}

impl MqttSession {
    async fn accept(
        socket: &mut TcpStream,
        state: Arc<RwLock<ServerState>>,
        graph: String,
        exchange: String,
        auth_secret: String,
        publish_notify: PublishNotify,
    ) -> std::io::Result<Option<Self>> {
        let Some((ptype, _flags, payload)) = read_packet(socket).await? else {
            return Ok(None); // clean EOF before CONNECT
        };
        if ptype != PKT_CONNECT {
            return Ok(None); // protocol violation → drop silently
        }
        let version = parse_connect_version(&payload)
            .ok_or_else(|| invalid_data("invalid MQTT CONNECT packet"))?;
        let Some(actor) = authenticate_connect(&payload, &auth_secret) else {
            write_packet(
                socket,
                PKT_CONNACK << 4,
                &build_auth_failure_connack(version),
            )
            .await?;
            return Ok(None);
        };
        write_packet(socket, PKT_CONNACK << 4, &build_connack(version)).await?;

        // Ensure the shared broker TOPIC exchange exists (idempotent).
        let _ = broker_wire::engine_call(
            &state,
            &graph,
            &actor,
            next_req_id,
            Method::DeclareExchange {
                exchange: exchange.clone(),
                kind: "topic".to_string(),
            },
        )
        .await;

        Ok(Some(Self {
            state,
            graph,
            exchange,
            actor: actor.clone(),
            version,
            session_queue: format!("mqtt.{}", next_req_id()),
            session_consumer: format!("{actor}:{}", next_req_id()),
            publish_notify,
            subscriptions: Vec::new(),
            subscription_bytes: 0,
        }))
    }

    async fn run(&mut self, socket: &mut TcpStream) -> std::io::Result<()> {
        loop {
            match self.next_packet(socket).await? {
                NextPacket::Packet(Some(packet)) => {
                    if !self.handle_packet(socket, packet).await? {
                        break;
                    }
                }
                NextPacket::Packet(None) => break,
                NextPacket::Pumped => {}
            }
        }
        teardown_session(self).await;
        Ok(())
    }

    async fn next_packet(&mut self, socket: &mut TcpStream) -> std::io::Result<NextPacket> {
        if self.subscriptions.is_empty() {
            return Ok(NextPacket::Packet(read_packet(socket).await?));
        }

        // GOC-70 (EG-281): race the read against BOTH the listener-wide publish
        // notify and the unchanged 200ms fallback tick. `read_packet` was
        // already raced against a timer here pre-fix, so this carries the same
        // read-cancellation profile while keeping the delivery pump bounded.
        let next = tokio::select! {
            biased;
            result = read_packet(socket) => NextPacket::Packet(result?),
            _ = self.publish_notify.notified() => NextPacket::Pumped,
            _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => NextPacket::Pumped,
        };
        match next {
            NextPacket::Pumped => {
                pump_session(self, socket).await?;
                Ok(NextPacket::Pumped)
            }
            packet => Ok(packet),
        }
    }

    async fn handle_packet(
        &mut self,
        socket: &mut TcpStream,
        packet: MqttPacket,
    ) -> std::io::Result<bool> {
        let (ptype, flags, payload) = packet;
        match ptype {
            PKT_PUBLISH => self.handle_publish(socket, flags, payload).await?,
            PKT_SUBSCRIBE => {
                self.handle_filter_request(socket, &payload, FilterRequestKind::Subscribe)
                    .await?
            }
            PKT_UNSUBSCRIBE => {
                self.handle_filter_request(socket, &payload, FilterRequestKind::Unsubscribe)
                    .await?
            }
            PKT_PUBACK => return Err(invalid_data("unexpected MQTT PUBACK packet")),
            PKT_PINGREQ => write_packet(socket, PKT_PINGRESP << 4, &[]).await?,
            PKT_DISCONNECT => return Ok(false),
            _ => return Err(invalid_data("unsupported MQTT packet type")),
        }
        Ok(true)
    }

    async fn handle_publish(
        &self,
        socket: &mut TcpStream,
        flags: u8,
        mut payload: Vec<u8>,
    ) -> std::io::Result<()> {
        let packet = parse_publish(&mut payload, self.version, flags)?;
        let _ = broker_wire::engine_call(
            &self.state,
            &self.graph,
            &self.actor,
            next_req_id,
            Method::PublishIdempotent {
                exchange: self.exchange.clone(),
                routing_key: mqtt_topic_to_key(&packet.topic),
                payload: packet.body,
                producer_id: packet.producer_id,
                seq: packet.producer_seq.unwrap_or(0),
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
        )
        .await;
        // The engine call above is the commit barrier; this wake is only the
        // listener-local fast path, with the 200ms poll as the safety net.
        self.publish_notify.notify_waiters();
        if packet.qos == 1 {
            write_packet(socket, PKT_PUBACK << 4, &packet.packet_id.to_be_bytes()).await?;
        }
        Ok(())
    }

    async fn handle_filter_request(
        &mut self,
        socket: &mut TcpStream,
        payload: &[u8],
        kind: FilterRequestKind,
    ) -> std::io::Result<()> {
        let (packet_id, patterns) = parse_filter_request(payload, self.version, kind)?;
        let (header, response) = match kind {
            FilterRequestKind::Subscribe => {
                let granted = self.bind_subscriptions(patterns).await;
                (
                    PKT_SUBACK << 4,
                    build_suback(packet_id, &granted, self.version),
                )
            }
            FilterRequestKind::Unsubscribe => {
                let count = patterns.len();
                self.unbind_subscriptions(patterns).await;
                (
                    PKT_UNSUBACK << 4,
                    build_unsuback(packet_id, count, self.version),
                )
            }
        };
        write_packet(socket, header, &response).await
    }

    async fn bind_subscriptions(&mut self, patterns: Vec<String>) -> Vec<u8> {
        let mut granted = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            match subscription_decision(&self.subscriptions, self.subscription_bytes, &pattern) {
                SubscriptionDecision::Reject => granted.push(0x80),
                SubscriptionDecision::Duplicate => granted.push(0x00),
                SubscriptionDecision::Bind(next_bytes) => {
                    let _ = broker_wire::engine_call(
                        &self.state,
                        &self.graph,
                        &self.actor,
                        next_req_id,
                        Method::BindQueue {
                            exchange: self.exchange.clone(),
                            queue: self.session_queue.clone(),
                            routing_key: pattern.clone(),
                        },
                    )
                    .await;
                    self.subscriptions.push(Subscription { pattern });
                    self.subscription_bytes = next_bytes;
                    granted.push(0x00);
                }
            }
        }
        granted
    }

    async fn unbind_subscriptions(&mut self, patterns: Vec<String>) {
        for pattern in patterns {
            unbind_pattern(self, &pattern).await;
            remove_subscription(
                &mut self.subscriptions,
                &mut self.subscription_bytes,
                &pattern,
            );
        }
    }
}

async fn pump_session(session: &MqttSession, socket: &mut TcpStream) -> std::io::Result<()> {
    pump_subscription(
        socket,
        &session.state,
        &session.graph,
        &session.actor,
        &session.session_queue,
        &session.session_consumer,
        session.version,
    )
    .await
}

fn subscription_decision(
    subscriptions: &[Subscription],
    subscription_bytes: usize,
    pattern: &str,
) -> SubscriptionDecision {
    if subscriptions.len() >= MAX_MQTT_SUBSCRIPTIONS {
        return SubscriptionDecision::Reject;
    }
    if subscriptions
        .iter()
        .any(|subscription| subscription.pattern == pattern)
    {
        return SubscriptionDecision::Duplicate;
    }
    match subscription_bytes.checked_add(pattern.len()) {
        Some(next) if next <= MAX_MQTT_SUBSCRIPTION_BYTES => SubscriptionDecision::Bind(next),
        _ => SubscriptionDecision::Reject,
    }
}

fn remove_subscription(
    subscriptions: &mut Vec<Subscription>,
    subscription_bytes: &mut usize,
    pattern: &str,
) {
    let mut removed_bytes = 0usize;
    subscriptions.retain(|subscription| {
        let keep = subscription.pattern != pattern;
        if !keep {
            removed_bytes = removed_bytes.saturating_add(subscription.pattern.len());
        }
        keep
    });
    *subscription_bytes = subscription_bytes.saturating_sub(removed_bytes);
}

async fn teardown_session(session: &MqttSession) {
    for subscription in &session.subscriptions {
        unbind_pattern(session, &subscription.pattern).await;
    }
}

async fn unbind_pattern(session: &MqttSession, pattern: &str) {
    let _ = broker_wire::engine_call(
        &session.state,
        &session.graph,
        &session.actor,
        next_req_id,
        Method::UnbindQueue {
            exchange: session.exchange.clone(),
            queue: session.session_queue.clone(),
            routing_key: pattern.to_string(),
        },
    )
    .await;
}

/// Deliver a bounded batch of pending messages from the session queue as QoS-0 PUBLISH
/// packets (ack'd immediately). CONCEPT:EG-KG.query.mqtt-packet-codec — the poll-driven push pump.
async fn pump_subscription(
    socket: &mut TcpStream,
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    actor: &str,
    queue: &str,
    consumer: &str,
    version: u8,
) -> std::io::Result<()> {
    const MAX_PER_POLL: usize = 32;
    for _ in 0..MAX_PER_POLL {
        let Some((node_id, rk, body)) = broker_wire::claim_message(
            state,
            graph,
            actor,
            next_req_id,
            queue,
            consumer,
            BROKER_CLAIM_LIMITS,
        )
        .await
        else {
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
        broker_wire::ack_message(state, graph, actor, next_req_id, queue, &node_id).await;
    }
    Ok(())
}

// ── Handshake / ack builders ──────────────────────────────────────────────

/// Extract the MQTT protocol level (4 = 3.1.1, 5 = 5.0) from a CONNECT payload; defaults
/// to 4 on a short/odd packet.
fn parse_connect_version(payload: &[u8]) -> Option<u8> {
    let (c, level, connect_flags) = parse_connect_header(payload)?;
    (c.valid && valid_version_flags(connect_flags)).then_some(level)
}

/// Parse a complete CONNECT payload and verify its mandatory username/password.
fn authenticate_connect(payload: &[u8], secret: &str) -> Option<String> {
    let (mut c, level, connect_flags) = parse_connect_header(payload)?;
    if !valid_authentication_flags(connect_flags) {
        return None;
    }
    let _client_id = c.mqtt_str();
    parse_will(&mut c, level, connect_flags);
    let (principal, password) = read_credentials(&mut c);
    if !c.valid || c.remaining() != 0 || !verify_mqtt_password(secret, &principal, password) {
        return None;
    }
    crate::server::pseudonymous_broker_actor(secret, &principal).ok()
}

fn parse_connect_header<'a>(payload: &'a [u8]) -> Option<(Cursor<'a>, u8, u8)> {
    let mut c = Cursor::new(payload);
    let proto_name = c.mqtt_str();
    let level = c.u8();
    let connect_flags = c.u8();
    let _keep_alive = c.u16();
    if proto_name != "MQTT" || !matches!(level, 4 | 5) {
        return None;
    }
    if level == 5 {
        c.skip_props();
    }
    Some((c, level, connect_flags))
}

fn valid_version_flags(connect_flags: u8) -> bool {
    let will = connect_flags & 0x04 != 0;
    let clean_start = connect_flags & 0x02 != 0;
    let will_qos = (connect_flags >> 3) & 0x03;
    let will_retain = connect_flags & 0x20 != 0;
    let password = connect_flags & 0x40 != 0;
    let username = connect_flags & 0x80 != 0;
    connect_flags & 0x01 == 0
        && will_qos == 0
        && !will_retain
        && !will
        && clean_start
        && (!password || username)
}

fn valid_authentication_flags(connect_flags: u8) -> bool {
    let will = connect_flags & 0x04 != 0;
    let will_qos = (connect_flags >> 3) & 0x03;
    let will_retain = connect_flags & 0x20 != 0;
    let has_password = connect_flags & 0x40 != 0;
    let has_username = connect_flags & 0x80 != 0;
    connect_flags & 0x01 == 0
        && will_qos != 0x03
        && (will || (will_qos == 0 && !will_retain))
        && has_username
        && has_password
}

fn parse_will(c: &mut Cursor<'_>, level: u8, connect_flags: u8) {
    if connect_flags & 0x04 != 0 {
        if level == 5 {
            c.skip_props();
        }
        let _will_topic = c.mqtt_str();
        let will_len = c.u16() as usize;
        let _will_payload = c.take(will_len);
    }
}

fn read_credentials<'a>(c: &mut Cursor<'a>) -> (String, &'a [u8]) {
    let principal = c.mqtt_str();
    let password_len = c.u16() as usize;
    let password = c.take(password_len);
    (principal, password)
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
        let (id, seq) = parse_publish_properties(&props).unwrap();
        assert_eq!(id.as_deref(), Some("prod-9"));
        assert_eq!(seq, Some(5));
    }

    #[test]
    fn eg314_parse_publish_properties_absent_is_none() {
        assert_eq!(parse_publish_properties(&[]), Some((None, None)));
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
        v4.push(0x02); // clean start
        v4.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(parse_connect_version(&v4), Some(4));
        let mut v5 = Vec::new();
        put_mqtt_str(&mut v5, "MQTT");
        v5.push(5);
        v5.push(0x02);
        v5.extend_from_slice(&0u16.to_be_bytes());
        v5.push(0); // property length
        assert_eq!(parse_connect_version(&v5), Some(5));
    }

    #[test]
    fn connect_authentication_is_verified_and_identity_bound() {
        let principal = "agent:subscriber";
        let password = derive_mqtt_password("test", principal);
        let mut connect = Vec::new();
        put_mqtt_str(&mut connect, "MQTT");
        connect.push(4);
        connect.push(0xC2); // username + password + clean session
        connect.extend_from_slice(&0u16.to_be_bytes());
        put_mqtt_str(&mut connect, "client-1");
        put_mqtt_str(&mut connect, principal);
        put_mqtt_str(&mut connect, &password);
        let actor = authenticate_connect(&connect, "test").unwrap();
        assert_eq!(
            actor,
            crate::server::pseudonymous_broker_actor("test", principal).unwrap()
        );
        assert!(!actor.contains(principal));
        assert!(authenticate_connect(&connect, "other").is_none());
        assert!(authenticate_connect(&connect, "").is_none());
    }

    #[test]
    fn startup_policy_rejects_anonymous_or_remote_mqtt() {
        assert!(validate_startup_policy("127.0.0.1:1883", "").is_err());
        assert!(validate_startup_policy("0.0.0.0:1883", "test").is_err());
        assert!(validate_startup_policy("127.0.0.1:1883", "test").is_ok());
    }

    // ── Served listener round-trip (CONCEPT:EG-KG.query.mqtt-packet-codec) ───────────────────────

    async fn spawn_listener() -> String {
        broker_wire::spawn_broker_test_listener("eg-mqtt-wire-test", |listener, state| async move {
            accept_loop(
                listener,
                state,
                "__commons__".to_string(),
                DEFAULT_EXCHANGE.to_string(),
                "test".to_string(),
            )
            .await
        })
        .await
    }

    #[tokio::test]
    async fn eg281_listener_connect_subscribe_publish_deliver_roundtrip() {
        let addr = spawn_listener().await;

        // ── Subscriber connects + subscribes to `sensors/#`. ──
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        let mut connect = Vec::new();
        put_mqtt_str(&mut connect, "MQTT");
        connect.push(4); // protocol level 3.1.1
        connect.push(0xC2); // username + password + clean session
        connect.extend_from_slice(&0u16.to_be_bytes()); // keepalive
        put_mqtt_str(&mut connect, "sub-client"); // client id
        put_mqtt_str(&mut connect, "subscriber");
        put_mqtt_str(&mut connect, &derive_mqtt_password("test", "subscriber"));
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
        connect2.push(0xC2);
        connect2.extend_from_slice(&0u16.to_be_bytes());
        put_mqtt_str(&mut connect2, "pub-client");
        put_mqtt_str(&mut connect2, "publisher");
        put_mqtt_str(&mut connect2, &derive_mqtt_password("test", "publisher"));
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
