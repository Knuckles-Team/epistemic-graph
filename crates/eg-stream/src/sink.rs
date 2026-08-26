//! CA-11 — the `CdcSink` trait + Kafka-backed implementation for eg's
//! CDC-to-Kafka bridge (DEC-CA-03: `eg.cdc.<graph>`, single partition,
//! compacted retention). Feature `cdc-kafka`, default OFF.
//!
//! Deliberately decoupled from `eg_types::wire::CdcEvent`: this crate is a
//! leaf (a sibling of `eg-ann`/`eg-geo`/`eg-tensor`, see the crate doc) and
//! must not depend on `eg-core`/`eg-types`. [`SinkEvent`] below is the small,
//! self-contained shape this crate actually needs; `src/server/cdc_sink/**`
//! (the sole caller, in the root `epistemic-graph` crate, which already
//! depends on `eg-types`) translates the real `CdcEvent` into a `SinkEvent`
//! and adapts [`KafkaCdcSink`] onto `crate::server::cdc::ExternalCdcSink`
//! before installing it on the live `CdcHub`.
//!
//! ## DEC-CA-03 envelope gap (report this, don't paper over it)
//!
//! DEC-CA-03's `eg.cdc.<graph>` JSON schema names twelve fields: `seq`,
//! `lsn`, `graph`, `op`, `node_id`, `edge_id`, `before`, `after`, `tenant`,
//! `marking`, `actor`, `ts`. This lane can only populate eight of them.
//! `lsn`/`tenant`/`marking`/`actor` are NOT fabricated — they are omitted
//! from [`Envelope`] entirely, rather than emitted as an empty string that a
//! consumer could mistake for a verified (if blank) value:
//!
//!   * **`tenant`/`actor`** — `CdcHub::emit`'s call site
//!     (`cdc::emit_for_method`, called from `mutation.rs::commit_finalize`)
//!     is given `graph: &str`, `method: &Method`, and a pre/post image —
//!     never a `CarrierAuthority`. `MutationCtx` (the struct that DOES carry
//!     `tenant_scope`/`caller`) is available one call frame up, in
//!     `commit_finalize`, but is not threaded into `CdcEvent` or
//!     `CdcHub::emit`'s signature. Sourcing them for real means widening
//!     `CdcEvent`/`CdcHub::emit` in `cdc.rs` — out of this lane's scope
//!     (`cdc.rs` is shared by every `streaming` consumer, not owned by
//!     CA-11), and NOT done here to avoid inventing a placeholder a
//!     downstream RLS/audit consumer could mistake for verified identity.
//!   * **`marking`** (a data-sensitivity/classification label) — grepped for
//!     across `src/server/access.rs` and `src/server/mutation.rs`: no such
//!     concept exists anywhere in eg's authority model today. DEC-CA-03
//!     appears to have carried this field over from the `cdc.*` (Debezium)
//!     row of its own topic table without checking it against what
//!     `eg.cdc.<graph>` can actually produce.
//!   * **`lsn`** — eg's durable tier is redb, not a WAL-based store; eg has
//!     no log-sequence-number concept. `seq` (this hub's own monotonic
//!     per-graph cursor, already in the envelope) is the actual ordering
//!     primitive `CdcHub::read`/P3's replay check use. `lsn` reads as
//!     copied from the same Debezium row (where it names Postgres's real
//!     WAL LSN) onto a producer that has no WAL at all.
//!
//! None of this blocks P3 (`seq` order + replay-from-0 checksum identity),
//! which only needs `seq`/`graph`/`op`/`node_id`/`before`/`after`. Whoever
//! reviews DEC-CA-03 next should either strike these four fields from the
//! `eg.cdc.<graph>` row specifically (they make sense for `cdc.*`) or land a
//! follow-up lane threading `CarrierAuthority` through `CdcHub::emit`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "cdc-kafka")]
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

#[cfg(feature = "cdc-kafka")]
use rdkafka::client::ClientContext;
#[cfg(feature = "cdc-kafka")]
use rdkafka::config::ClientConfig;
#[cfg(feature = "cdc-kafka")]
use rdkafka::error::KafkaError;
#[cfg(feature = "cdc-kafka")]
use rdkafka::message::{DeliveryResult, Message};
#[cfg(feature = "cdc-kafka")]
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};

/// `op` in DEC-CA-03's envelope. Every `eg_types::wire::CdcKind` variant
/// collapses onto one of these two (mapping lives at the `cdc_sink` call
/// site, next to the `CdcKind` import it needs): `AddNode`/`UpdateNode`/
/// `AddEdge` -> `Upsert`, `RemoveNode`/`RemoveEdge` -> `Tombstone`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkOp {
    Upsert,
    Tombstone,
}

impl SinkOp {
    fn as_str(self) -> &'static str {
        match self {
            SinkOp::Upsert => "upsert",
            SinkOp::Tombstone => "tombstone",
        }
    }
}

/// One CDC change, already decoded/translated by the caller (see module
/// doc). `before`/`after` are decoded JSON (the caller already holds
/// `eg_types::msgpack::decode_property_value` — this crate does not, so it
/// cannot decode the raw MessagePack blob itself).
#[derive(Clone, Debug, PartialEq)]
pub struct SinkEvent {
    pub seq: u64,
    pub graph: String,
    pub op: SinkOp,
    pub node_id: String,
    /// `Some("<source>-><target>")` for an edge change, `None` for a node
    /// change. eg addresses edges by the `(source, target)` pair, not an
    /// opaque id — this is synthesized for the envelope, not a native eg id.
    pub edge_id: Option<String>,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

/// The wire envelope actually published to `eg.cdc.<graph>` — DEC-CA-03's
/// schema minus the four fields the module doc explains are not fabricable
/// from what reaches this sink today.
#[derive(Serialize)]
struct Envelope<'a> {
    seq: u64,
    graph: &'a str,
    op: &'a str,
    node_id: &'a str,
    edge_id: Option<&'a str>,
    before: &'a serde_json::Value,
    after: &'a serde_json::Value,
    ts: String,
}

/// Delivery error from [`CdcSink::emit`]. Every variant is something the
/// caller logs/counts (W05's lag metric) and moves on from — `emit` MUST
/// NEVER block or panic the caller (`DEC-CA-01`/`DEC-CA-03`: "Kafka is
/// transport only; redb commits never block on it").
#[derive(Debug)]
pub enum SinkError {
    /// The producer's local outbound queue is full — broker down/slow.
    /// Backpressure, not data loss: the CDC ring still has the event.
    QueueFull,
    /// librdkafka rejected the message for another reason (bad config,
    /// serialization, unreachable topic metadata, ...).
    Kafka(String),
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkError::QueueFull => write!(f, "cdc-kafka sink: local queue full"),
            SinkError::Kafka(msg) => write!(f, "cdc-kafka sink: {msg}"),
        }
    }
}

impl std::error::Error for SinkError {}

/// The seam `src/server/cdc_sink` installs onto `CdcHub` (via its own
/// in-crate `ExternalCdcSink` adapter — see `cdc.rs`). Sync/blocking-safe by
/// contract: an implementation must return quickly (queue-and-return, never
/// wait on network I/O), because `CdcHub::emit` runs on the durable-commit
/// path and must never stall a write on a slow/absent broker.
pub trait CdcSink: Send + Sync {
    fn emit(&self, event: &SinkEvent) -> Result<(), SinkError>;

    /// Best-effort count of events queued locally but not yet
    /// broker-acknowledged (W05's backpressure metric). Default 0 — a sink
    /// with no real queue (e.g. a test double) reports no lag.
    fn lag(&self) -> u64 {
        0
    }

    /// Block up to `timeout_ms` waiting for every queued event to reach the
    /// broker. NEVER called from `CdcHub::emit`'s hot path (that would
    /// violate the "never blocks a commit" contract) — for graceful
    /// shutdown and tests that need delivery, not commit-time, confirmed
    /// before asserting anything about the topic. Default no-op (a sink
    /// with no real queue has nothing to flush).
    fn flush(&self, _timeout_ms: u64) {}
}

/// Producer configuration, env-driven at the `cdc_sink` call site.
#[cfg(feature = "cdc-kafka")]
#[derive(Clone, Debug, Default)]
pub struct KafkaConfig {
    pub brokers: String,
    /// Topic = `{topic_prefix}{graph}` (DEC-CA-03: `eg.cdc.<graph>`).
    pub topic_prefix: String,
    /// DEC-CA-03: "producer config must support SASL/SCRAM even before
    /// CA-51 wires the broker for it" — the broker is PLAINTEXT/no-auth
    /// today (BASELINE §1), so these are normally unset (`security.protocol`
    /// defaults to PLAINTEXT when `None`).
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

/// The default `rdkafka` producer context invokes no delivery callback at
/// all, so `BaseProducer::send`'s `Ok(())` (which only means "accepted into
/// the local queue") would be the ONLY signal this sink ever sees --
/// indistinguishable from real broker delivery. This context makes actual
/// broker-confirmed delivery/failure observable (`delivered_count`/
/// `delivery_failed_count`), which is what P3's "prove delivery, not
/// configuration" requires: a green `emit()` alone proves nothing about
/// whether the message reached Kafka.
#[cfg(feature = "cdc-kafka")]
struct DeliveryTracker {
    delivered: Arc<AtomicU64>,
    delivery_failed: Arc<AtomicU64>,
}

#[cfg(feature = "cdc-kafka")]
impl ClientContext for DeliveryTracker {}

#[cfg(feature = "cdc-kafka")]
impl ProducerContext for DeliveryTracker {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {
        match result {
            Ok(_) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Err((e, msg)) => {
                self.delivery_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "cdc-kafka sink: delivery failed for topic={} partition={}: {e}",
                    msg.topic(),
                    msg.partition()
                );
            }
        }
    }
}

/// `rdkafka`-backed [`CdcSink`] (feature `cdc-kafka`). One explicit
/// partition per graph — DEC-CA-03/CA-11 deliberately do NOT hash-route on
/// the graph key, because a topic can have more than one partition in
/// principle and key-hash routing would not guarantee the single-partition
/// `seq`-order contract P3 checks; targeting partition 0 explicitly does.
#[cfg(feature = "cdc-kafka")]
pub struct KafkaCdcSink {
    producer: Arc<BaseProducer<DeliveryTracker>>,
    topic_prefix: String,
    /// Locally enqueued (`BaseProducer::send` returned `Ok`) -- NOT proof of
    /// broker delivery. See `delivered`/`delivery_failed` for that.
    sent: AtomicU64,
    /// Locally rejected (queue full, bad config, ...) before ever reaching
    /// librdkafka's internal queue.
    failed: AtomicU64,
    /// Broker-confirmed delivered, via `DeliveryTracker`.
    delivered: Arc<AtomicU64>,
    /// Broker-confirmed delivery failure, via `DeliveryTracker`.
    delivery_failed: Arc<AtomicU64>,
}

#[cfg(feature = "cdc-kafka")]
impl KafkaCdcSink {
    /// Build the producer and start the background poll thread. Fails
    /// closed (`Err`, no thread spawned) on a bad broker string or a
    /// producer-creation error (e.g. `security.protocol` set to a SASL
    /// mode librdkafka wasn't linked to support) — the caller
    /// (`cdc_sink::install_from_env`) logs and leaves the hub sink-less
    /// (ring-only, exactly today's behavior) rather than crash the server.
    pub fn new(config: &KafkaConfig) -> Result<Self, SinkError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            // Mirrors `sparql_http.rs`'s 5s/30s connect/read pattern (lane
            // brief's "Algorithmic and resource budget" note) — bounded so a
            // dead broker never queues messages indefinitely.
            .set("message.timeout.ms", "5000")
            .set("socket.timeout.ms", "30000")
            // P3's ordering guarantee holds only if a RETRY can never
            // reorder two in-flight messages on the same partition.
            // Idempotence forces `max.in.flight.requests.per.connection<=5`
            // with per-partition sequencing and caps `acks=all` -- the
            // standard librdkafka mechanism for exactly this. Without it,
            // librdkafka's default `max.in.flight.requests.per.connection`
            // (1,000,000) can reorder on a retried send.
            .set("enable.idempotence", "true");
        if let Some(proto) = &config.security_protocol {
            client_config.set("security.protocol", proto);
        }
        if let Some(mech) = &config.sasl_mechanism {
            client_config.set("sasl.mechanisms", mech);
        }
        if let Some(user) = &config.sasl_username {
            client_config.set("sasl.username", user);
        }
        if let Some(pass) = &config.sasl_password {
            client_config.set("sasl.password", pass);
        }
        let delivered = Arc::new(AtomicU64::new(0));
        let delivery_failed = Arc::new(AtomicU64::new(0));
        let context = DeliveryTracker {
            delivered: Arc::clone(&delivered),
            delivery_failed: Arc::clone(&delivery_failed),
        };
        let producer: BaseProducer<DeliveryTracker> = client_config
            .create_with_context(context)
            .map_err(|e: KafkaError| SinkError::Kafka(e.to_string()))?;
        let producer = Arc::new(producer);
        // `BaseProducer::send` is non-blocking (enqueues into librdkafka's
        // local buffer; actual I/O runs on librdkafka's own background
        // threads regardless), but delivery-report/error events pile up in
        // an internal queue until `poll()` dispatches them — without a
        // periodic poller that queue grows unbounded over the process
        // life. One dedicated OS thread, detached (this sink lives for the
        // process's life; there is no shutdown path to join it against).
        let reaper = Arc::clone(&producer);
        let _ = std::thread::Builder::new()
            .name("eg-cdc-kafka-poll".into())
            .spawn(move || loop {
                reaper.poll(std::time::Duration::from_millis(200));
            });
        Ok(KafkaCdcSink {
            producer,
            topic_prefix: config.topic_prefix.clone(),
            sent: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            delivered,
            delivery_failed,
        })
    }

    fn topic_for(&self, graph: &str) -> String {
        format!("{}{}", self.topic_prefix, graph)
    }

    /// Locally enqueued -- NOT proof of broker delivery.
    pub fn sent_count(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn failed_count(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    /// Broker-confirmed delivered (via `DeliveryTracker`'s callback).
    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Broker-confirmed delivery failure (via `DeliveryTracker`'s callback).
    pub fn delivery_failed_count(&self) -> u64 {
        self.delivery_failed.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "cdc-kafka")]
impl CdcSink for KafkaCdcSink {
    fn emit(&self, event: &SinkEvent) -> Result<(), SinkError> {
        let topic = self.topic_for(&event.graph);
        let null = serde_json::Value::Null;
        let envelope = Envelope {
            seq: event.seq,
            graph: &event.graph,
            op: event.op.as_str(),
            node_id: &event.node_id,
            edge_id: event.edge_id.as_deref(),
            before: event.before.as_ref().unwrap_or(&null),
            after: event.after.as_ref().unwrap_or(&null),
            ts: rfc3339_utc_now(),
        };
        let payload = serde_json::to_vec(&envelope).map_err(|e| SinkError::Kafka(e.to_string()))?;
        let key = event.graph.as_bytes();
        let record = BaseRecord::to(&topic)
            .key(key)
            .payload(&payload)
            .partition(0);
        match self.producer.send(record) {
            Ok(()) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err((KafkaError::MessageProduction(code), _)) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                if format!("{code:?}").contains("QueueFull") {
                    Err(SinkError::QueueFull)
                } else {
                    Err(SinkError::Kafka(format!("{code:?}")))
                }
            }
            Err((e, _)) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                Err(SinkError::Kafka(e.to_string()))
            }
        }
    }

    fn lag(&self) -> u64 {
        self.producer.in_flight_count() as u64
    }

    fn flush(&self, timeout_ms: u64) {
        if let Err(e) = self
            .producer
            .flush(std::time::Duration::from_millis(timeout_ms))
        {
            eprintln!("cdc-kafka sink: flush error: {e}");
        }
    }
}

/// `RFC3339` UTC timestamp for `now()`, hand-rolled (no `chrono`/`time` dep —
/// this crate stays Pi-lean; see the crate doc). Civil-from-days conversion
/// is Howard Hinnant's well-known constant-time algorithm.
fn rfc3339_utc_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (secs / 86_400) as i64;
    let time_of_day = secs % 86_400;
    let (h, m, s) = (
        time_of_day / 3600,
        (time_of_day / 60) % 60,
        time_of_day % 60,
    );

    // Howard Hinnant's `civil_from_days`.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m_num = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m_num <= 2 { y + 1 } else { y };

    format!("{y:04}-{m_num:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_now_is_well_formed() {
        let ts = rfc3339_utc_now();
        assert_eq!(ts.len(), 20, "unexpected RFC3339 length: {ts:?}");
        assert!(ts.starts_with("20"), "unexpected year prefix: {ts:?}");
        assert!(ts.ends_with('Z'));
        // Round-trippable by any RFC3339 parser: fixed-width numeric fields.
        let bytes = ts.as_bytes();
        for i in [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18] {
            assert!(bytes[i].is_ascii_digit(), "byte {i} of {ts:?} not a digit");
        }
        assert_eq!(bytes[4], b'-');
        assert_eq!(bytes[7], b'-');
        assert_eq!(bytes[10], b'T');
        assert_eq!(bytes[13], b':');
        assert_eq!(bytes[16], b':');
        assert_eq!(bytes[19], b'Z');
    }

    #[test]
    fn sink_op_str_matches_dec_ca_03() {
        assert_eq!(SinkOp::Upsert.as_str(), "upsert");
        assert_eq!(SinkOp::Tombstone.as_str(), "tombstone");
    }

    #[test]
    fn envelope_serializes_null_before_after_when_absent() {
        let event = SinkEvent {
            seq: 7,
            graph: "g1".to_string(),
            op: SinkOp::Upsert,
            node_id: "n1".to_string(),
            edge_id: None,
            before: None,
            after: Some(serde_json::json!({"a": 1})),
        };
        let null = serde_json::Value::Null;
        let envelope = Envelope {
            seq: event.seq,
            graph: &event.graph,
            op: event.op.as_str(),
            node_id: &event.node_id,
            edge_id: event.edge_id.as_deref(),
            before: event.before.as_ref().unwrap_or(&null),
            after: event.after.as_ref().unwrap_or(&null),
            ts: "2026-01-01T00:00:00Z".to_string(),
        };
        let v = serde_json::to_value(&envelope).unwrap();
        assert_eq!(v["seq"], 7);
        assert_eq!(v["graph"], "g1");
        assert_eq!(v["op"], "upsert");
        assert_eq!(v["node_id"], "n1");
        assert!(v["edge_id"].is_null());
        assert!(v["before"].is_null());
        assert_eq!(v["after"]["a"], 1);
        // The four DEC-CA-03 fields this lane cannot source are absent, not
        // null — a consumer must be able to tell "not sourced" apart from
        // "verified blank".
        assert!(v.get("tenant").is_none());
        assert!(v.get("marking").is_none());
        assert!(v.get("actor").is_none());
        assert!(v.get("lsn").is_none());
    }
}
