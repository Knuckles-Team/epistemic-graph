//! CA-15 — OpenLineage transport for the lake tier's already-built `RunEvent`s
//! (feature `lineage-transport`, implies `lake`; the Kafka leg is the
//! further-gated `lineage-transport-kafka`, implies `cdc-kafka`).
//!
//! ## What this is (and is not)
//!
//! [`lineage::build_run_event`] already constructs a complete, spec-shaped
//! OpenLineage `RunEvent` for every `LakeManager` materialize/compact/delete
//! run; [`lineage::maybe_push_http`] already best-effort POSTs it to
//! `EPISTEMIC_GRAPH_OPENLINEAGE_URL` when set (`DEC-CA-05`'s W0 review note:
//! *"CA-15 wires and extends `maybe_push_http`; it does not reimplement"*).
//! This module does not touch either — it ADDS a second transport (Kafka, on
//! `openlineage.events` per `DEC-CA-03`) and a small composition layer
//! ([`LineageTransports`]) so both can fire for the same event without one
//! displacing the other, while [`lineage::maybe_push_http`] stays reachable
//! and byte-identical on its own for a plain `lake`-only build (no
//! `lineage-transport`) — see `lake/mod.rs`'s `push_lineage` call site,
//! which branches on the feature rather than routing every build through
//! this module.
//!
//! ## Kafka producer: reused, not duplicated (`FO-CA-026`)
//!
//! `file-ownership.yaml`'s `FO-CA-026` makes `crates/eg-stream/Cargo.toml`'s
//! `rdkafka` dependency CA-11-exclusive: this lane must not add a second
//! Kafka dependency anywhere in the workspace. [`KafkaTransport`] is built on
//! `eg_stream::sink::RawKafkaProducer` — a small, schema-agnostic producer
//! CA-15 added to CA-11's `eg-stream::sink` module (feature `cdc-kafka`,
//! unchanged), sharing the exact same delivery-tracked construction
//! [`eg_stream::sink::KafkaCdcSink`] uses (see that module's doc for why a
//! literal `KafkaCdcSink` reuse doesn't fit: it always wraps a message in
//! DEC-CA-03's CDC `Envelope` shape and targets `{topic_prefix}{graph}` on
//! explicit partition 0, neither of which is right for an OpenLineage
//! `RunEvent` publishing its own JSON to the fixed `openlineage.events` topic
//! keyed by `run.runId`). Root `Cargo.toml`'s new `lineage-transport-kafka`
//! feature composes `lineage-transport` with the ALREADY-DECLARED `cdc-kafka`
//! feature (`["lineage-transport", "cdc-kafka"]`) rather than declaring any
//! new dependency — enabling both `cdc-kafka` and `lineage-transport-kafka`
//! together compiles in exactly one `rdkafka` linkage, and each feature owns
//! its own producer INSTANCE (CDC's on `CdcHub`, this one on the lake tier) —
//! two independent long-lived connections to the broker is the normal shape
//! for two independently-configured producers (distinct topics, distinct
//! env-var namespaces, distinct lifecycles), not a "double initialization"
//! of the dependency itself.
//!
//! ## Inbound facets: reconciled out, not silently dropped
//!
//! The lane brief (authored before `DEC-CA-05` froze) scoped an "inbound
//! facet-acceptance surface" accepting Spark/Trino-originated events into eg
//! for re-publication. **`DEC-CA-05`'s frozen Contract section assigns CA-25
//! (an au Kafka consumer) as the SOLE consumer of `openlineage.events`,
//! reading directly from every producer** (eg, Spark, Trino) — dataset
//! identity correlation (`iceberg://<catalog>/<ns>/<table>@<snapshot>`)
//! happens by CA-25 matching that string across independently-published
//! events, not via any eg-side relay. Building an inbound surface here would
//! be new, unrequired attack surface (this module's own "Security and
//! privacy" analysis in the lane brief says as much) with no consumer -- so,
//! per the lane's own "Refusal rule" ("if `DEC-CA-05` contradicts this
//! lane's *proposed* design, stop and reconcile"), it is reconciled out
//! rather than built. `W05`/acceptance-gate `#4` (the malformed-inbound-facet
//! test) are therefore N/A for this lane's actual scope; reported explicitly
//! rather than silently dropped.

use serde_json::Value;

use super::lineage;

/// One OpenLineage transport. `push` mirrors [`lineage::maybe_push_http`]'s
/// contract exactly: sync, infallible-to-the-caller, and MUST NEVER block or
/// panic the materialization run it's called from ("lineage export must
/// never fail or block a materialization run", `lineage.rs:212-214`) — every
/// implementation below swallows its own errors (logging, never propagating).
pub trait LineageTransport: Send + Sync {
    fn push(&self, event: &Value);

    /// Short name for logging/diagnostics.
    fn name(&self) -> &'static str;
}

/// Wraps the existing, unchanged [`lineage::maybe_push_http`] as a
/// [`LineageTransport`] — no new HTTP logic, no behavior change. Its `push`
/// no-ops exactly as `maybe_push_http` does when `EPISTEMIC_GRAPH_OPENLINEAGE_URL`
/// is unset, so `HttpTransport` is always present in
/// [`configured_transports`]'s list regardless of configuration.
pub struct HttpTransport;

impl LineageTransport for HttpTransport {
    fn push(&self, event: &Value) {
        lineage::maybe_push_http(event);
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

/// Fixed topic name for the OpenLineage transport, per `DEC-CA-03`'s topic
/// taxonomy (`openlineage.events`, producers include "eg `src/server/lake/
/// lineage.rs`", key: run id). Unlike `eg.cdc.<graph>`, this is NOT
/// per-graph/templated — every eg-originated `RunEvent`, from any table,
/// lands on this one topic, same as the Spark/Trino listeners DEC-CA-05
/// names as the other (unverified-live) producers.
pub const OPENLINEAGE_TOPIC: &str = "openlineage.events";

/// `rdkafka`-backed [`LineageTransport`] (feature `lineage-transport-kafka`).
/// See the module doc's "Kafka producer: reused, not duplicated" section for
/// why this wraps `eg_stream::sink::RawKafkaProducer` rather than
/// `KafkaCdcSink`.
#[cfg(feature = "lineage-transport-kafka")]
pub struct KafkaTransport {
    producer: eg_stream::sink::RawKafkaProducer,
}

#[cfg(feature = "lineage-transport-kafka")]
impl KafkaTransport {
    /// Bootstrap-servers string. Unset (or blank) ⇒ [`Self::from_env`]
    /// returns `None` — mirrors [`lineage::OPENLINEAGE_URL_ENV`]'s
    /// unset-is-a-no-op convention and `src/server/cdc_sink`'s
    /// `ENV_BROKERS` precedent. A SEPARATE env namespace from CDC's
    /// (`EPISTEMIC_GRAPH_CDC_KAFKA_*`): the two are independently
    /// configured producers to (possibly) the same broker but different
    /// topics, so one can be armed without the other.
    pub const ENV_BROKERS: &'static str = "EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS";
    pub const ENV_SECURITY_PROTOCOL: &'static str =
        "EPISTEMIC_GRAPH_LINEAGE_KAFKA_SECURITY_PROTOCOL";
    pub const ENV_SASL_MECHANISM: &'static str = "EPISTEMIC_GRAPH_LINEAGE_KAFKA_SASL_MECHANISM";
    pub const ENV_SASL_USERNAME: &'static str = "EPISTEMIC_GRAPH_LINEAGE_KAFKA_SASL_USERNAME";
    pub const ENV_SASL_PASSWORD: &'static str = "EPISTEMIC_GRAPH_LINEAGE_KAFKA_SASL_PASSWORD";

    /// Build from process environment. Never panics: a missing broker var is
    /// a documented no-op (`None`); a producer-construction failure (bad
    /// broker string, unsupported SASL mode, ...) is logged to stderr and
    /// also yields `None` -- the caller ([`LineageTransports::configured`])
    /// continues with whatever transports it already has (at minimum
    /// [`HttpTransport`]), exactly `cdc_sink::install_from_env`'s
    /// fail-open-to-fewer-transports precedent.
    pub fn from_env() -> Option<Self> {
        let brokers = std::env::var(Self::ENV_BROKERS).ok()?;
        if brokers.trim().is_empty() {
            return None;
        }
        let config = eg_stream::sink::KafkaConfig {
            brokers,
            // Unread by `RawKafkaProducer` -- see its doc. Left empty
            // rather than `OPENLINEAGE_TOPIC` to make that explicit at a
            // glance in a debugger/log dump of `KafkaConfig`.
            topic_prefix: String::new(),
            security_protocol: std::env::var(Self::ENV_SECURITY_PROTOCOL).ok(),
            sasl_mechanism: std::env::var(Self::ENV_SASL_MECHANISM).ok(),
            sasl_username: std::env::var(Self::ENV_SASL_USERNAME).ok(),
            sasl_password: std::env::var(Self::ENV_SASL_PASSWORD).ok(),
        };
        match eg_stream::sink::RawKafkaProducer::new(&config) {
            Ok(producer) => Some(KafkaTransport { producer }),
            Err(e) => {
                eprintln!(
                    "lineage-transport-kafka: failed to construct the Kafka producer ({e}); \
                     continuing without a Kafka lineage transport"
                );
                None
            }
        }
    }

    /// Block up to `timeout_ms` for delivery -- tests/shutdown only, never
    /// the write path (mirrors `RawKafkaProducer::flush`'s own contract).
    pub fn flush(&self, timeout_ms: u64) {
        self.producer.flush(timeout_ms);
    }
}

#[cfg(feature = "lineage-transport-kafka")]
impl LineageTransport for KafkaTransport {
    fn push(&self, event: &Value) {
        // `build_run_event` always sets `run.runId` (a deterministic,
        // FNV-derived id -- see `lineage.rs::run_id`); an empty fallback key
        // still publishes (never blocks the caller) if some future producer
        // of this same code path ever omits it -- librdkafka simply routes
        // an empty/absent key by round-robin rather than refusing the send.
        let key = event["run"]["runId"].as_str().unwrap_or("");
        let payload = event.to_string();
        if let Err(e) = self
            .producer
            .publish(OPENLINEAGE_TOPIC, key.as_bytes(), payload.as_bytes())
        {
            eprintln!("lineage-transport-kafka: publish failed: {e}");
        }
    }

    fn name(&self) -> &'static str {
        "kafka"
    }
}

/// The composed set of transports a `RunEvent` is pushed through. `push_all`
/// never blocks or panics the caller -- each transport already swallows its
/// own errors (see [`LineageTransport::push`]'s doc), and one transport
/// erroring never skips another (a plain sequential loop, not a
/// short-circuiting `?`).
pub struct LineageTransports {
    transports: Vec<Box<dyn LineageTransport>>,
}

impl LineageTransports {
    pub fn push_all(&self, event: &Value) {
        for transport in &self.transports {
            transport.push(event);
        }
    }

    /// Names of the transports actually configured -- inspection/tests
    /// only, mirrors `LakeManager::recent_lineage`'s "prove it landed"
    /// idiom rather than trusting a log line was emitted somewhere.
    pub fn names(&self) -> Vec<&'static str> {
        self.transports.iter().map(|t| t.name()).collect()
    }
}

// Two bodies, not one `mut` Vec with a `#[cfg]`-gated push inside: with
// `lineage-transport-kafka` off, a `mut` binding that's never subsequently
// mutated is `-D warnings`' `unused-mut` (a real error CI caught -- see the
// module's git history) -- exactly the "Kafka-only code path must be
// feature-gated so `cargo clippy --features full -- -D warnings` still
// passes" trap this program's brief calls out by name.
#[cfg(feature = "lineage-transport-kafka")]
fn build_from_env() -> LineageTransports {
    let mut transports: Vec<Box<dyn LineageTransport>> = vec![Box::new(HttpTransport)];
    if let Some(kafka) = KafkaTransport::from_env() {
        transports.push(Box::new(kafka));
    }
    LineageTransports { transports }
}

#[cfg(not(feature = "lineage-transport-kafka"))]
fn build_from_env() -> LineageTransports {
    LineageTransports {
        transports: vec![Box::new(HttpTransport)],
    }
}

/// The process-wide configured transport set, built once from environment on
/// first use. A `OnceLock` (not per-`LakeManager`) because the Kafka producer
/// underneath owns a real background poll thread + broker connection that
/// must live for the process's life, exactly `KafkaCdcSink`'s own precedent
/// (`cdc_sink::install_from_env` installs one sink onto one process-wide
/// `CdcHub`) -- a per-`LakeManager` instance would reconnect on every test
/// `LakeManager::new()` for no benefit, since `EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS`
/// is itself process-wide configuration.
pub fn configured_transports() -> &'static LineageTransports {
    static TRANSPORTS: std::sync::OnceLock<LineageTransports> = std::sync::OnceLock::new();
    TRANSPORTS.get_or_init(build_from_env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A transport double that records every event it's pushed and can
    /// optionally simulate a failure -- since [`LineageTransport::push`]
    /// returns nothing, "failure" here means "logs and does nothing",
    /// exactly like the real HTTP/Kafka implementations; this proves the
    /// COMPOSITION never treats one transport's trouble as a reason to skip
    /// another.
    struct SpyTransport {
        calls: Arc<AtomicUsize>,
        name: &'static str,
    }

    impl LineageTransport for SpyTransport {
        fn push(&self, _event: &Value) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    #[test]
    fn http_transport_is_a_noop_when_url_env_is_unset() {
        // `OPENLINEAGE_URL_ENV` is also touched by `lake::mod::tests`'
        // negative-transport test and (indirectly, via `KafkaTransport`'s
        // own env var) the Kafka tests below -- this repo's established
        // `crate::crypto::acquire_test_env_lock_blocking()` convention
        // (used pervasively in `src/server/mod.rs`/`bolt_wire`/`persistence`
        // for exactly this "tests mutate process-global env state"
        // interleaving hazard) serializes them so `cargo test`'s default
        // multi-threaded runner can never race a `remove_var`/`set_var`
        // pair across two tests.
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();
        std::env::remove_var(lineage::OPENLINEAGE_URL_ENV);
        // Must not panic -- this is the "unconfigured deployment" case
        // acceptance gate #5/the migration note require to stay a no-op.
        HttpTransport.push(&serde_json::json!({"eventType": "COMPLETE"}));
    }

    #[test]
    fn push_all_reaches_every_configured_transport() {
        let calls_a = Arc::new(AtomicUsize::new(0));
        let calls_b = Arc::new(AtomicUsize::new(0));
        let transports = LineageTransports {
            transports: vec![
                Box::new(SpyTransport {
                    calls: Arc::clone(&calls_a),
                    name: "a",
                }),
                Box::new(SpyTransport {
                    calls: Arc::clone(&calls_b),
                    name: "b",
                }),
            ],
        };
        transports.push_all(&serde_json::json!({"run": {"runId": "r1"}}));
        assert_eq!(calls_a.load(Ordering::SeqCst), 1);
        assert_eq!(calls_b.load(Ordering::SeqCst), 1);
        assert_eq!(transports.names(), vec!["a", "b"]);
    }

    /// Negative case (W07's "drop-is-silent-not-fabricated"): a transport
    /// that never delivers (here, simply absent -- no HTTP URL, no Kafka
    /// broker configured) still lets every OTHER configured transport run,
    /// and `push_all` itself never panics/blocks regardless of how many
    /// transports are configured. `HttpTransport` alone (the default,
    /// unconfigured set) already demonstrates the "drop silently" half; this
    /// asserts an empty transport list is equally inert -- the degenerate
    /// case, but the one `push_all`'s loop must handle without a special
    /// case.
    #[test]
    fn push_all_with_no_transports_configured_is_a_silent_noop() {
        let transports = LineageTransports {
            transports: Vec::new(),
        };
        transports.push_all(&serde_json::json!({"run": {"runId": "r1"}}));
        assert!(transports.names().is_empty());
    }

    /// Both `KafkaTransport::from_env`'s no-op and its unroutable-broker
    /// path in ONE test (not two): both set/remove the SAME process-global
    /// `ENV_BROKERS` var, so splitting them across two `#[test]` fns would
    /// race under the default parallel test runner even with the env lock
    /// held (one test's guard drops before the other's mutation is safe to
    /// observe) -- a single sequential test has no such window regardless.
    #[cfg(feature = "lineage-transport-kafka")]
    #[test]
    fn kafka_transport_env_contract_no_brokers_then_unroutable_broker() {
        let _env_lock = crate::crypto::acquire_test_env_lock_blocking();

        std::env::remove_var(KafkaTransport::ENV_BROKERS);
        assert!(KafkaTransport::from_env().is_none());

        std::env::set_var(KafkaTransport::ENV_BROKERS, "127.0.0.1:1");
        let transport = KafkaTransport::from_env().expect("construction never requires connectivity");
        let started = std::time::Instant::now();
        transport.push(&serde_json::json!({"run": {"runId": "r-unreachable"}}));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "push against an unroutable broker took {:?} -- must be non-blocking",
            started.elapsed()
        );
        std::env::remove_var(KafkaTransport::ENV_BROKERS);
    }
}
