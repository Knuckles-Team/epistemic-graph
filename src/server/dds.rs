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
//! `std_msgs/String` messages over RTPS, which is a real DDS wire.
//!
//! ## rmw name/type mangling for live-`ros2` interop (CONCEPT:EG-349)
//!
//! On top of the raw RTPS wire, the native leg now applies the **rmw** name/type mangling
//! convention (ROS 2 Humble+, `rmw_cyclonedds`/`rmw_fastrtps`) so a topic published here is
//! discoverable/subscribable by a real `ros2` daemon with **zero config** — see
//! [`mangle_topic_name`] / [`mangle_type_name`]. A ROS topic `/chatter` is put on the DDS
//! wire as `rt/chatter`, and the ROS type `std_msgs/String` as
//! `std_msgs::msg::dds_::String_`; rustdds' `CDRSerializerAdapter` emits exactly the 4-byte
//! CDR encapsulation header ([`CDR_LE_ENCAPSULATION_HEADER`]) `ros2 topic echo` decodes.
//! The `DdsTransport` interface stays ROS-topic-oriented (callers pass `/chatter`); the
//! mangling is applied internally at DDS-`Topic` creation, so the writer/reader map keys and
//! [`poll_inbound`](DdsTransport::poll_inbound) still surface the un-mangled ROS name.
//!
//! **Residual (deferred):** the ROS 2 Iron/Jazzy **type-hash / typesupport descriptor**
//! (the `RIHS01_…` type hash distributed in endpoint discovery `TypeInformation`) is NOT
//! emitted — Humble matches endpoints by mangled type *name* only, which this leg satisfies;
//! richer type-hash negotiation stays a documented follow-on. The alternative
//! CycloneDDS-C-backed `rmw` leg also remains a toolchain-gated future option and is
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

// ── rmw name/type mangling for live-ros2 interop (CONCEPT:EG-349) ─────────────
//
// ROS 2 does NOT put a bare topic/type name on the DDS wire — the `rmw` layer
// (`rmw_cyclonedds`/`rmw_fastrtps`, Humble+) MANGLES both so DDS discovery (SPDP/SEDP)
// matches endpoints across implementations. A native DDS publisher must apply the SAME
// mangling to be discovered by a live `ros2` daemon with zero config. These pure functions
// (no DDS deps) encode that convention and are unit-tested against the spec below.

/// The `rmw` DDS-topic prefix for an ordinary ROS **topic** (as opposed to a service
/// request `rq`/reply `rr`) — CONCEPT:EG-349. rmw prepends `rt` to the fully-qualified ROS
/// topic name, so the ROS topic `/chatter` becomes the DDS topic `rt/chatter`.
pub const RMW_ROS_TOPIC_PREFIX: &str = "rt";

/// The 4-byte CDR **encapsulation header** ROS 2 (rmw) prepends to every serialized sample
/// (CONCEPT:EG-349): representation id `CDR_LE` (`0x00 0x01`, little-endian — the rmw
/// default on little-endian hosts) followed by two zero options bytes. rustdds'
/// `CDRSerializerAdapter` reports `RepresentationIdentifier::CDR_LE` as its output encoding
/// and the RTPS `SerializedPayload` frames it with exactly these bytes, so a sample written
/// by this leg is byte-compatible with what `ros2 topic echo` decodes.
pub const CDR_LE_ENCAPSULATION_HEADER: [u8; 4] = [0x00, 0x01, 0x00, 0x00];

/// Mangle a ROS 2 topic name to the DDS topic name a live `ros2` daemon uses
/// (CONCEPT:EG-349). rmw first fully-qualifies the ROS name (a leading `/`), then prepends
/// the [`RMW_ROS_TOPIC_PREFIX`], so `/chatter` → `rt/chatter` and a bare `chatter` (or
/// `/chatter`) both map to `rt/chatter`. Matches `rmw_cyclonedds`/`rmw_fastrtps`.
pub fn mangle_topic_name(ros_topic: &str) -> String {
    let qualified = if ros_topic.starts_with('/') {
        ros_topic.to_string()
    } else {
        format!("/{ros_topic}")
    };
    // `RMW_ROS_TOPIC_PREFIX` + a leading-`/` name => `rt/chatter`.
    format!("{RMW_ROS_TOPIC_PREFIX}{qualified}")
}

/// Mangle a ROS 2 message type to the DDS type name a live `ros2` daemon advertises in
/// discovery (CONCEPT:EG-349): `<pkg>::<namespace>::dds_::<Msg>_`. Accepts either the
/// rosbridge 2-part form `std_msgs/String` (the `<namespace>` defaults to `msg`) or the
/// 3-part `std_msgs/msg/String`. So `std_msgs/String` and `std_msgs/msg/String` both map to
/// `std_msgs::msg::dds_::String_` (matching `rmw_cyclonedds`/`rmw_fastrtps`).
pub fn mangle_type_name(ros_type: &str) -> String {
    let parts: Vec<&str> = ros_type.split('/').filter(|s| !s.is_empty()).collect();
    let (pkg, namespace, msg) = match parts.as_slice() {
        [pkg, namespace, msg] => (*pkg, *namespace, *msg),
        [pkg, msg] => (*pkg, "msg", *msg),
        [msg] => ("std_msgs", "msg", *msg),
        // Unrecognised/empty input: fall back to a `msg`-namespaced descriptor of the
        // whole string so the output is still a well-formed rmw type name.
        _ => ("std_msgs", "msg", ros_type),
    };
    format!("{pkg}::{namespace}::dds_::{msg}_")
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

        /// Create the underlying DDS [`Topic`] using the **rmw-mangled** name+type
        /// (CONCEPT:EG-349) so a live `ros2` daemon discovers/subscribes it zero-config:
        /// the ROS name `name` is put on the wire as [`mangle_topic_name`] (`rt/…`) and the
        /// ROS type `type_name` as [`mangle_type_name`] (`<pkg>::<ns>::dds_::<Msg>_`). The
        /// caller-facing writer/reader map still keys on the un-mangled ROS `name`.
        fn topic(&self, name: &str, type_name: &str) -> Result<Topic, String> {
            let dds_name = mangle_topic_name(name);
            let dds_type = mangle_type_name(type_name);
            self.participant
                .create_topic(dds_name, dds_type, &self.qos, TopicKind::NoKey)
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

    #[cfg(test)]
    mod cdr_tests {
        use super::*;
        use rustdds::no_key::SerializerAdapter;
        use rustdds::{RepresentationIdentifier, SerializedPayload};

        /// EG-349: prove the native leg serializes a `std_msgs/String`-shaped sample with
        /// EXACTLY the CDR encapsulation `ros2 topic echo` expects — the RTPS
        /// `SerializedPayload` framed by rustdds' `CDRSerializerAdapter` must begin with the
        /// 4-byte `CDR_LE` header (`00 01 00 00`). No live daemon needed: this checks the
        /// on-wire encapsulation bytes against the rmw/CDR spec directly.
        #[test]
        fn eg349_ros2_cdr_le_encapsulation_header() {
            // The adapter reports CDR_LE as its output encoding (rmw default on LE hosts).
            assert_eq!(
                CDRSerializerAdapter::<Ros2String>::output_encoding(),
                RepresentationIdentifier::CDR_LE,
                "EG-349: rmw requires the CDR_LE representation id for std_msgs/String",
            );
            // Serialize the real Ros2String and frame it as the RTPS SerializedPayload the
            // writer puts on the wire; its first 4 bytes are the encapsulation header.
            let body = CDRSerializerAdapter::<Ros2String>::to_bytes(&Ros2String {
                data: "eg349".to_string(),
            })
            .expect("cdr serialize");
            let payload = SerializedPayload::new(RepresentationIdentifier::CDR_LE, body.to_vec());
            let header = payload.bytes_slice(0, 4);
            assert_eq!(
                &header[..],
                &CDR_LE_ENCAPSULATION_HEADER,
                "EG-349: on-wire CDR encapsulation header must match what ros2 decodes",
            );
        }
    }
}

#[cfg(feature = "ros2-dds")]
pub use native::NativeDdsTransport;

#[cfg(all(test, feature = "ros2-dds"))]
mod tests {
    use super::*;
    use crate::server::ros2_bridge::ROS_STRING_TYPE;

    /// EG-349: the rmw topic-name mangling matches the `rmw_cyclonedds`/`rmw_fastrtps`
    /// convention (ROS 2 Humble+): a ROS topic gets the `rt` prefix on the DDS wire, and a
    /// bare (un-qualified) name is fully-qualified first. This is what a live `ros2` daemon
    /// looks for in SPDP/SEDP discovery.
    #[test]
    fn eg349_mangle_topic_name_matches_rmw_convention() {
        assert_eq!(mangle_topic_name("/chatter"), "rt/chatter");
        // A bare name is fully-qualified (leading `/`) before the prefix.
        assert_eq!(mangle_topic_name("chatter"), "rt/chatter");
        // Nested namespaces are preserved after the prefix.
        assert_eq!(
            mangle_topic_name("/epistemic_graph/eg349_test"),
            "rt/epistemic_graph/eg349_test"
        );
        assert_eq!(RMW_ROS_TOPIC_PREFIX, "rt");
    }

    /// EG-349: the rmw type-name mangling matches the `<pkg>::<ns>::dds_::<Msg>_`
    /// convention — the exact type descriptor a live `ros2` daemon advertises for
    /// `std_msgs/String`. Both the rosbridge 2-part and the ROS 3-part spellings converge.
    #[test]
    fn eg349_mangle_type_name_matches_rmw_convention() {
        assert_eq!(
            mangle_type_name("std_msgs/String"),
            "std_msgs::msg::dds_::String_"
        );
        assert_eq!(
            mangle_type_name("std_msgs/msg/String"),
            "std_msgs::msg::dds_::String_"
        );
        // The default the DDS leg advertises with (EG-325 `ROS_STRING_TYPE`) mangles right.
        assert_eq!(
            mangle_type_name(ROS_STRING_TYPE),
            "std_msgs::msg::dds_::String_"
        );
        // A non-std package + explicit namespace is preserved.
        assert_eq!(
            mangle_type_name("geometry_msgs/msg/Twist"),
            "geometry_msgs::msg::dds_::Twist_"
        );
    }

    /// EG-347/EG-349: a REAL DDS/RTPS loopback — publish a `std_msgs/String`-shaped message
    /// on a topic through the native `rustdds` transport, subscribe to the same topic, and
    /// prove the round-trip (over an actual RTPS wire, with DDS discovery), then map the
    /// inbound message back to an engine `AddNode` via the shared EG-325
    /// [`inbound_to_method`] path. The DDS wire now carries the **rmw-mangled** names
    /// (asserted here) so the same publish is discoverable by a live `ros2` daemon.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eg347_native_dds_loopback_pub_sub_roundtrip() {
        let topic = "/epistemic_graph/eg349_test";

        // The transport puts these rmw-mangled names on the DDS wire (what `ros2` matches).
        assert_eq!(mangle_topic_name(topic), "rt/epistemic_graph/eg349_test");
        assert_eq!(
            mangle_type_name(ROS_STRING_TYPE),
            "std_msgs::msg::dds_::String_"
        );
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
