//! CA-11 — CDC → Kafka sink (feature `cdc-kafka`, off by default).
//!
//! Bridges the existing in-memory CDC ring ([`crate::server::cdc`], feature
//! `streaming`) onto Kafka per `DEC-CA-03`: every ordered [`crate::wire::CdcEvent`]
//! also lands on `eg.cdc.<graph>`, single explicit partition, DEC-CA-03's
//! JSON envelope shape. Everything here is glue: the trait + envelope +
//! `rdkafka` producer live in the leaf crate `eg_stream::sink` (must not
//! depend on `eg-core`/`eg-types`); this module is the one place that knows
//! both `crate::wire::CdcEvent` AND `eg_stream::sink::SinkEvent`, so it does
//! the translation and installs the bridge onto the live [`crate::server::cdc::CdcHub`].
//!
//! Migration/rollback: `EPISTEMIC_GRAPH_CDC_KAFKA_BROKERS` unset ⇒
//! [`install_from_env`] is a no-op and `CdcHub` behaves byte-identically to
//! a build without this feature at all (ring-only). A broker string that
//! fails to construct a producer (bad host, SASL mode not linked, ...) is
//! logged and left uninstalled — same ring-only fallback, never a crash.

use std::sync::Arc;

use eg_stream::sink::{CdcSink, KafkaCdcSink, KafkaConfig, SinkEvent, SinkOp};

use crate::server::cdc::{CdcHub, ExternalCdcSink};
use crate::wire::{CdcEvent, CdcKind};

/// Env var carrying the Kafka bootstrap-servers string. Unset ⇒ sink
/// disabled (see module doc).
pub const ENV_BROKERS: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_BROKERS";
/// Topic = `{prefix}{graph}`. Default matches `DEC-CA-03`'s `eg.cdc.<graph>`.
pub const ENV_TOPIC_PREFIX: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_TOPIC_PREFIX";
const DEFAULT_TOPIC_PREFIX: &str = "eg.cdc.";
/// `DEC-CA-03`: "producer config must support SASL/SCRAM even before CA-51
/// wires the broker for it" — the broker itself stays PLAINTEXT/no-auth
/// today, so these are normally unset.
pub const ENV_SECURITY_PROTOCOL: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_SECURITY_PROTOCOL";
pub const ENV_SASL_MECHANISM: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_SASL_MECHANISM";
pub const ENV_SASL_USERNAME: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_SASL_USERNAME";
pub const ENV_SASL_PASSWORD: &str = "EPISTEMIC_GRAPH_CDC_KAFKA_SASL_PASSWORD";

/// Install the Kafka sink on `hub` from process environment, if configured.
/// Called once at startup (`main.rs`, `state.rs`) right after `CdcHub::new()`.
/// A no-op when [`ENV_BROKERS`] is unset. Never panics — a producer-creation
/// failure is logged to stderr and the hub is left sink-less (ring-only).
pub fn install_from_env(hub: &Arc<CdcHub>) {
    let Ok(brokers) = std::env::var(ENV_BROKERS) else {
        return;
    };
    if brokers.trim().is_empty() {
        return;
    }
    let config = KafkaConfig {
        brokers,
        topic_prefix: std::env::var(ENV_TOPIC_PREFIX)
            .unwrap_or_else(|_| DEFAULT_TOPIC_PREFIX.to_string()),
        security_protocol: std::env::var(ENV_SECURITY_PROTOCOL).ok(),
        sasl_mechanism: std::env::var(ENV_SASL_MECHANISM).ok(),
        sasl_username: std::env::var(ENV_SASL_USERNAME).ok(),
        sasl_password: std::env::var(ENV_SASL_PASSWORD).ok(),
    };
    match KafkaCdcSink::new(&config) {
        Ok(sink) => {
            hub.install_sink(Arc::new(Bridge {
                inner: Arc::new(sink),
            }));
        }
        Err(e) => {
            eprintln!(
                "cdc-kafka: failed to construct the Kafka producer ({e}); \
                 continuing ring-only (CdcHub sink left uninstalled)"
            );
        }
    }
}

/// Adapts an `eg_stream::sink::CdcSink` onto `cdc::ExternalCdcSink`,
/// translating `CdcEvent` → `SinkEvent` (msgpack-decode before/after,
/// collapse `CdcKind` onto `SinkOp`) on every call and recording the W05
/// lag/failure metrics. This is the one place in the whole lane that knows
/// both wire shapes.
struct Bridge {
    inner: Arc<dyn CdcSink>,
}

impl ExternalCdcSink for Bridge {
    fn emit(&self, event: &CdcEvent) {
        let sink_event = translate(event);
        if let Err(e) = self.inner.emit(&sink_event) {
            crate::metrics::cdc_kafka_sink_send_failed();
            eprintln!(
                "cdc-kafka sink: emit failed for graph={} seq={}: {e}",
                event.graph, event.seq
            );
        }
        crate::metrics::cdc_kafka_sink_lag(self.inner.lag() as i64);
    }

    fn flush(&self, timeout_ms: u64) {
        self.inner.flush(timeout_ms);
    }
}

/// `CdcKind` → `SinkOp` (DEC-CA-03: `"op": "upsert|tombstone"`).
/// `AddNode`/`UpdateNode`/`AddEdge` -> `Upsert`; `RemoveNode`/`RemoveEdge` ->
/// `Tombstone`.
fn sink_op(kind: &CdcKind) -> SinkOp {
    match kind {
        CdcKind::AddNode | CdcKind::UpdateNode | CdcKind::AddEdge => SinkOp::Upsert,
        CdcKind::RemoveNode | CdcKind::RemoveEdge => SinkOp::Tombstone,
    }
}

/// Decode a `before`/`after` MessagePack property blob to JSON, `None` when
/// the event recorded no value at all (`had_before`/`had_after == false`) —
/// distinct from "decoded to an empty object".
fn decode(blob: &[u8], had: bool) -> Option<serde_json::Value> {
    if !had {
        return None;
    }
    eg_types::msgpack::decode_property_value(blob).ok()
}

/// `eg_types::wire::CdcEvent` → `eg_stream::sink::SinkEvent`. See the
/// `eg_stream::sink` module doc for the four DEC-CA-03 fields
/// (`lsn`/`tenant`/`marking`/`actor`) this cannot populate and why.
fn translate(event: &CdcEvent) -> SinkEvent {
    let is_edge = matches!(event.kind, CdcKind::AddEdge | CdcKind::RemoveEdge);
    SinkEvent {
        seq: event.seq,
        graph: event.graph.clone(),
        op: sink_op(&event.kind),
        node_id: event.node_id.clone(),
        edge_id: is_edge.then(|| format!("{}->{}", event.node_id, event.target_id)),
        before: decode(&event.before, event.had_before),
        after: decode(&event.after, event.had_after),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_event() -> CdcEvent {
        CdcEvent {
            seq: 3,
            graph: "g1".to_string(),
            kind: CdcKind::AddNode,
            node_id: "n1".to_string(),
            target_id: String::new(),
            label: "Person".to_string(),
            before: Vec::new(),
            after: rmp_serde::to_vec(&serde_json::json!({"type": "Person", "name": "a"})).unwrap(),
            had_before: false,
            had_after: true,
        }
    }

    #[test]
    fn add_node_maps_to_upsert_with_no_edge_id() {
        let e = translate(&base_event());
        assert_eq!(e.op, SinkOp::Upsert);
        assert_eq!(e.edge_id, None);
        assert!(e.before.is_none());
        assert_eq!(e.after.as_ref().unwrap()["name"], "a");
    }

    #[test]
    fn remove_node_maps_to_tombstone() {
        let mut ev = base_event();
        ev.kind = CdcKind::RemoveNode;
        ev.had_after = false;
        ev.after = Vec::new();
        ev.had_before = true;
        ev.before = rmp_serde::to_vec(&serde_json::json!({"type": "Person"})).unwrap();
        let e = translate(&ev);
        assert_eq!(e.op, SinkOp::Tombstone);
        assert!(e.after.is_none());
        assert!(e.before.is_some());
    }

    #[test]
    fn add_edge_synthesizes_edge_id_from_source_and_target() {
        let mut ev = base_event();
        ev.kind = CdcKind::AddEdge;
        ev.node_id = "src".to_string();
        ev.target_id = "dst".to_string();
        let e = translate(&ev);
        assert_eq!(e.op, SinkOp::Upsert);
        assert_eq!(e.edge_id.as_deref(), Some("src->dst"));
    }

    #[test]
    fn remove_edge_maps_to_tombstone_with_edge_id() {
        let mut ev = base_event();
        ev.kind = CdcKind::RemoveEdge;
        ev.node_id = "src".to_string();
        ev.target_id = "dst".to_string();
        let e = translate(&ev);
        assert_eq!(e.op, SinkOp::Tombstone);
        assert_eq!(e.edge_id.as_deref(), Some("src->dst"));
    }

    #[test]
    fn had_before_false_never_decodes_the_empty_blob_as_an_empty_object() {
        let ev = base_event();
        // before is empty AND had_before is false -- must be None, not Some(null-ish).
        assert!(translate(&ev).before.is_none());
    }

    #[test]
    fn install_from_env_is_a_noop_without_the_brokers_var() {
        std::env::remove_var(ENV_BROKERS);
        let hub = Arc::new(CdcHub::new());
        install_from_env(&hub);
        // No public way to assert "no sink installed" from outside cdc.rs;
        // this only proves it doesn't panic/short-circuit oddly on the
        // documented no-op path. `emit_installs_and_is_reachable_end_to_end`
        // in `tests/e2e_ca/cdc_kafka.rs` proves the installed path actually
        // forwards events.
    }
}
