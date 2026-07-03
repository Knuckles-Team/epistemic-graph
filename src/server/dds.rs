//! Native DDS/RTPS transport seam for the ROS2 wire (CONCEPT:EG-347).
//!
//! The EG-325 [`super::ros2_bridge`] joins a ROS2 graph WITHOUT a DDS stack — it tunnels
//! ROS2 topics through a `rosbridge_server` over WebSocket JSON. This module adds the
//! SECOND leg: a NATIVE DDS/RTPS wire, so the engine's CDC↔ROS2 topic path can speak DDS
//! directly to a ROS2 graph. Both legs sit behind ONE interface — the [`DdsTransport`]
//! trait — so the CDC↔ROS2 path targets EITHER the WebSocket bridge OR native DDS by
//! choosing an impl, with the SAME `std_msgs/String`-shaped payload on the wire either way
//! (the pure EG-325 mapping [`super::ros2_bridge::cdc_to_publish`] /
//! [`super::ros2_bridge::publish_to_method`] is reused verbatim).
//!
//! ## Honest scope of the native leg
//!
//! The native impl ([`NativeDdsTransport`], feature `ros2-dds`) is backed by
//! [`rustdds`](https://docs.rs/rustdds) — a **pure-Rust** DDS + RTPS implementation
//! (mio/pnet/speedy/cdr-encoding). It links **no CycloneDDS/rmw/`ros` C toolchain**, so —
//! unlike a `cyclonedds-rs`/`rmw` leg — it genuinely **builds in CI everywhere** and is
//! exercised by a real loopback pub/sub test below. It publishes/subscribes CDR-encoded
//! `std_msgs/String` messages over RTPS, which is a real DDS wire; full `rmw`
//! topic-name/type-hash mangling for zero-config interop with a live `ros2` daemon (the
//! `rt/…` prefix + `dds_::String_` type descriptor) is a documented follow-on. The
//! alternative CycloneDDS-C-backed `rmw` leg remains a toolchain-gated future option and is
//! deliberately NOT wired here (it cannot be CI-built without the C toolchain).
//!
//! Feature-gated: the [`DdsTransport`] trait compiles under either `ros2-bridge` or
//! `ros2-dds`; [`NativeDdsTransport`] + its deps compile only under `ros2-dds`. Kept OUT of
//! `pi`/`default`/`node`/`full` (only the `full-extras` bundle) — the Pi contract holds
//! (a `pi`/`full` build links no rustdds).

use serde_json::Value;

use super::ros2_bridge::{cdc_to_publish, publish_to_method, RosbridgeOp};
use crate::protocol::Method;
use crate::wire::CdcEvent;

/// The ROS2 transport seam (CONCEPT:EG-347): publish/subscribe/advertise ROS2 topics over
/// a concrete transport (native DDS/RTPS, or the EG-325 rosbridge WebSocket). Messages are
/// the `std_msgs/String`-shaped JSON `{"data": "<json-string>"}` the rosbridge protocol and
/// the CDC mapping already use, so ONE shaping serves both legs.
///
/// The engine's CDC→ROS2 path calls [`DdsTransport::publish_cdc`] (default-implemented on
/// top of [`publish`](DdsTransport::publish) via the EG-325 `cdc_to_publish` shaping); the
/// ROS2→engine path maps an inbound message with [`inbound_to_method`].
#[async_trait::async_trait]
pub trait DdsTransport: Send + Sync {
    /// Declare intent to publish `topic` (with a ROS2 message type name, e.g.
    /// `std_msgs/String`) before the first publish. Idempotent.
    async fn advertise(&self, topic: &str, msg_type: &str) -> Result<(), String>;

    /// Publish one `std_msgs/String`-shaped message (`{"data": …}`) on `topic`.
    async fn publish(&self, topic: &str, msg: &Value) -> Result<(), String>;

    /// Subscribe to `topic`; inbound messages surface via [`poll_inbound`](Self::poll_inbound).
    async fn subscribe(&self, topic: &str) -> Result<(), String>;

    /// Poll for the next inbound message across all subscribed topics. `Ok(None)` means
    /// nothing is ready yet (the caller re-polls); `Ok(Some((topic, msg)))` yields one
    /// `std_msgs/String`-shaped message the caller maps with [`inbound_to_method`].
    async fn poll_inbound(&self) -> Result<Option<(String, Value)>, String>;

    /// Publish an engine CDC change on `topic` (CONCEPT:EG-347). The default reuses the
    /// EG-325 [`cdc_to_publish`] shaping, so the DDS leg and the rosbridge leg put the
    /// IDENTICAL `std_msgs/String` payload on the wire — the ONE seam, two transports.
    async fn publish_cdc(&self, event: &CdcEvent, topic: &str) -> Result<(), String> {
        if let RosbridgeOp::Publish { topic: t, msg } = cdc_to_publish(event, topic) {
            self.publish(&t, &msg).await
        } else {
            Ok(())
        }
    }
}

/// Map an inbound DDS/ROS2 message to an engine [`Method`] (CONCEPT:EG-347). This is the
/// SAME EG-325 [`publish_to_method`] mapping, so inbound DDS and inbound rosbridge converge
/// on one ingest path (an `AddNode` applied via `crate::wal::apply`).
pub fn inbound_to_method(msg: &Value) -> Option<Method> {
    publish_to_method(msg)
}

// ── Native DDS/RTPS transport via pure-Rust rustdds (CONCEPT:EG-347) ──────────
#[cfg(feature = "ros2-dds")]
mod native {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use rustdds::no_key::{DataReader, DataWriter};
    use rustdds::*;
    use serde::{Deserialize, Serialize};

    use super::super::ros2_bridge::ROS_STRING_TYPE;

    /// CDR message mirroring ROS2 `std_msgs/String` — a single `data` string field. The
    /// engine carries the change/node payload as a JSON string in `data`, exactly as the
    /// rosbridge leg does, so the two transports are payload-identical.
    #[derive(Serialize, Deserialize, Clone, Debug)]
    struct Ros2String {
        data: String,
    }

    type Writer = DataWriter<Ros2String, CDRSerializerAdapter<Ros2String>>;
    type Reader = DataReader<Ros2String, CDRDeserializerAdapter<Ros2String>>;

    /// Native DDS/RTPS [`DdsTransport`] (CONCEPT:EG-347) over pure-Rust `rustdds`. Owns one
    /// DDS `DomainParticipant` + a publisher/subscriber pair, and a per-topic writer/reader
    /// map. NO CycloneDDS/rmw/C toolchain — 100% Rust on the wire.
    pub struct NativeDdsTransport {
        participant: DomainParticipant,
        publisher: Publisher,
        subscriber: Subscriber,
        qos: QosPolicies,
        writers: Mutex<HashMap<String, Writer>>,
        readers: Mutex<HashMap<String, Reader>>,
    }

    impl NativeDdsTransport {
        /// Join DDS domain `domain_id` and build the participant + pub/sub. `Reliable` +
        /// `KeepLast(16)` QoS — a small, bounded history so a late-joining reader still
        /// receives recent changes.
        pub fn new(domain_id: u16) -> Result<Self, String> {
            let participant =
                DomainParticipant::new(domain_id).map_err(|e| format!("dds participant: {e:?}"))?;
            let qos = QosPolicyBuilder::new()
                .reliability(policy::Reliability::Reliable {
                    max_blocking_time: rustdds::Duration::from_millis(100),
                })
                .history(policy::History::KeepLast { depth: 16 })
                .build();
            let publisher = participant
                .create_publisher(&qos)
                .map_err(|e| format!("dds publisher: {e:?}"))?;
            let subscriber = participant
                .create_subscriber(&qos)
                .map_err(|e| format!("dds subscriber: {e:?}"))?;
            Ok(Self {
                participant,
                publisher,
                subscriber,
                qos,
                writers: Mutex::new(HashMap::new()),
                readers: Mutex::new(HashMap::new()),
            })
        }

        /// Read the DDS domain id from `EPISTEMIC_GRAPH_ROS_DDS_DOMAIN` (default `0`).
        pub fn domain_from_env() -> u16 {
            std::env::var("EPISTEMIC_GRAPH_ROS_DDS_DOMAIN")
                .ok()
                .and_then(|v| v.trim().parse::<u16>().ok())
                .unwrap_or(0)
        }

        fn topic(&self, name: &str, type_name: &str) -> Result<Topic, String> {
            self.participant
                .create_topic(
                    name.to_string(),
                    type_name.to_string(),
                    &self.qos,
                    TopicKind::NoKey,
                )
                .map_err(|e| format!("dds topic {name}: {e:?}"))
        }
    }

    #[async_trait::async_trait]
    impl DdsTransport for NativeDdsTransport {
        async fn advertise(&self, topic: &str, msg_type: &str) -> Result<(), String> {
            if self.writers.lock().unwrap().contains_key(topic) {
                return Ok(());
            }
            let t = self.topic(topic, msg_type)?;
            let writer: Writer = self
                .publisher
                .create_datawriter_no_key(&t, None)
                .map_err(|e| format!("dds writer {topic}: {e:?}"))?;
            self.writers
                .lock()
                .unwrap()
                .insert(topic.to_string(), writer);
            Ok(())
        }

        async fn publish(&self, topic: &str, msg: &Value) -> Result<(), String> {
            // Ensure the topic is advertised (idempotent) before the first write.
            self.advertise(topic, ROS_STRING_TYPE).await?;
            let data = match msg.get("data") {
                Some(Value::String(s)) => s.clone(),
                _ => msg.to_string(),
            };
            let writers = self.writers.lock().unwrap();
            let writer = writers
                .get(topic)
                .ok_or_else(|| format!("no dds writer for {topic}"))?;
            writer
                .write(Ros2String { data }, None)
                .map_err(|e| format!("dds write {topic}: {e:?}"))
        }

        async fn subscribe(&self, topic: &str) -> Result<(), String> {
            if self.readers.lock().unwrap().contains_key(topic) {
                return Ok(());
            }
            let t = self.topic(topic, ROS_STRING_TYPE)?;
            let reader: Reader = self
                .subscriber
                .create_datareader_no_key(&t, None)
                .map_err(|e| format!("dds reader {topic}: {e:?}"))?;
            self.readers
                .lock()
                .unwrap()
                .insert(topic.to_string(), reader);
            Ok(())
        }

        async fn poll_inbound(&self) -> Result<Option<(String, Value)>, String> {
            let mut readers = self.readers.lock().unwrap();
            for (topic, reader) in readers.iter_mut() {
                match reader.take_next_sample() {
                    Ok(Some(sample)) => {
                        let msg = serde_json::json!({ "data": sample.value().data.clone() });
                        return Ok(Some((topic.clone(), msg)));
                    }
                    Ok(None) => {}
                    Err(e) => return Err(format!("dds read {topic}: {e:?}")),
                }
            }
            Ok(None)
        }
    }
}

#[cfg(feature = "ros2-dds")]
pub use native::NativeDdsTransport;

#[cfg(all(test, feature = "ros2-dds"))]
mod tests {
    use super::*;
    use crate::server::ros2_bridge::ROS_STRING_TYPE;

    /// EG-347: a REAL DDS/RTPS loopback — publish a `std_msgs/String`-shaped message on a
    /// topic through the native `rustdds` transport, subscribe to the same topic, and prove
    /// the round-trip (over an actual RTPS wire, with DDS discovery), then map the inbound
    /// message back to an engine `AddNode` via the shared EG-325 [`inbound_to_method`] path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eg347_native_dds_loopback_pub_sub_roundtrip() {
        let topic = "/epistemic_graph/eg347_test";
        let transport = NativeDdsTransport::new(0).expect("dds transport");
        transport.subscribe(topic).await.expect("subscribe");
        transport
            .advertise(topic, ROS_STRING_TYPE)
            .await
            .expect("advertise");

        // Let DDS SPDP/SEDP discovery match the writer and reader on this participant.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // The payload the CDC/ingest path carries: a std_msgs/String whose `data` is the
        // JSON describing a node — exactly the rosbridge shape.
        let payload = serde_json::json!({ "node_id": "robot_1", "properties": { "x": 1.5 } });
        let msg = serde_json::json!({ "data": payload.to_string() });
        transport.publish(topic, &msg).await.expect("publish");

        // Poll for delivery over the RTPS wire.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            if let Some((got_topic, got)) = transport.poll_inbound().await.expect("poll") {
                assert_eq!(got_topic, topic);
                // Inbound DDS message maps to an engine Method via the shared EG-325 path.
                let method = inbound_to_method(&got).expect("maps to a method");
                match method {
                    Method::AddNode { node_id, .. } => assert_eq!(node_id, "robot_1"),
                    _ => panic!("expected AddNode from the DDS round-trip"),
                }
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "EG-347: timed out waiting for the DDS/RTPS loopback sample"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}
