//! Native DDS/RTPS transport seam for the ROS2 wire (CONCEPT:EG-KG.ingest.dds-transport).
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
//! [Dust DDS](https://docs.rs/dust_dds) — a **pure-Rust** DDS + RTPS implementation.
//! It links **no CycloneDDS/rmw/`ros` C toolchain**, so —
//! unlike a `cyclonedds-rs`/`rmw` leg — it genuinely **builds in CI everywhere** and is
//! exercised by a real loopback pub/sub test below. It publishes/subscribes CDR-encoded
//! `std_msgs/String` messages over RTPS, which is a real DDS wire.
//!
//! ## rmw name/type mangling for live-`ros2` interop (CONCEPT:EG-KG.ingest.rmw-topic-prefix)
//!
//! On top of the raw RTPS wire, the native leg now applies the **rmw** name/type mangling
//! convention (ROS 2 Humble+, `rmw_cyclonedds`/`rmw_fastrtps`) so a topic published here is
//! discoverable/subscribable by a real `ros2` daemon with **zero config** — see
//! [`mangle_topic_name`] / [`mangle_type_name`]. A ROS topic `/chatter` is put on the DDS
//! wire as `rt/chatter`, and the ROS type `std_msgs/String` as
//! `std_msgs::msg::dds_::String_`; Dust DDS emits the CDR encapsulation that
//! `ros2 topic echo` decodes.
//! The `DdsTransport` interface stays ROS-topic-oriented (callers pass `/chatter`); the
//! mangling is applied internally at DDS-`Topic` creation, so the writer/reader map keys and
//! [`poll_inbound`](DdsTransport::poll_inbound) still surface the un-mangled ROS name.
//!
//! **Residual (deferred):** the ROS 2 Iron/Jazzy **type-hash / typesupport descriptor**
//! (the `RIHS01_…` type hash distributed in endpoint discovery `TypeInformation`) is NOT
//! emitted — Humble matches endpoints by mangled type *name* only, which this leg satisfies;
//! richer type-hash negotiation stays a documented follow-on.
//!
//! ## The THIRD leg: CycloneDDS-C `rmw` (S5, feature `ros2-rmw`)
//!
//! [`CycloneDdsTransport`] (module `cyclone`, feature `ros2-rmw`) implements the SAME
//! [`DdsTransport`] trait a THIRD way: it links the REAL `rmw_cyclonedds`/CycloneDDS-C
//! stack (via the safe `cyclonedds` Rust crate over vendored, cmake-built C sources — see
//! the dependency doc in `Cargo.toml`), so it is genuine zero-config live-`ros2` interop
//! (a real `ros2` node discovers/pubs/subs with no bridge), not merely wire-compatible.
//! It reuses the SAME [`mangle_topic_name`]/[`mangle_type_name`] rmw mangling defined
//! above — no forked shaping — and the SAME `std_msgs/String` payload convention. This
//! leg needs a C toolchain (`cc`/`cmake`) to build, so it stays toolchain-gated behind
//! `ros2-rmw` (`full-extras`-only, never `default`/`full`), same as `ros2-dds`.
//!
//! Feature-gated: the [`DdsTransport`] trait compiles under `ros2-bridge`, `ros2-dds`, or
//! `ros2-rmw`; [`NativeDdsTransport`] + its deps compile only under `ros2-dds`;
//! [`CycloneDdsTransport`] + its deps compile only under `ros2-rmw`. Kept OUT of
//! `pi`/`default`/`node`/`full` (only the `full-extras` bundle) — the Pi contract holds
//! (a `pi`/`full` build links no Dust DDS/CycloneDDS).

use serde_json::Value;

#[cfg(test)]
use std::sync::atomic::{AtomicU32, Ordering};

use super::ros2_bridge::{cdc_to_publish, publish_to_method, RosbridgeOp};
use crate::protocol::Method;
use crate::wire::CdcEvent;

/// The ROS2 transport seam (CONCEPT:EG-KG.ingest.dds-transport): publish/subscribe/advertise ROS2 topics over
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

    /// Publish an engine CDC change on `topic` (CONCEPT:EG-KG.ingest.dds-transport). The default reuses the
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

/// Map an inbound DDS/ROS2 message to an engine [`Method`] (CONCEPT:EG-KG.ingest.dds-transport). This is the
/// SAME EG-325 [`publish_to_method`] mapping, so inbound DDS and inbound rosbridge converge
/// on one ingest path (an `AddNode` applied via `crate::mutation_apply::apply`).
pub fn inbound_to_method(msg: &Value) -> Option<Method> {
    publish_to_method(msg)
}

// ── rmw name/type mangling for live-ros2 interop (CONCEPT:EG-KG.ingest.rmw-topic-prefix) ─────────────
//
// ROS 2 does NOT put a bare topic/type name on the DDS wire — the `rmw` layer
// (`rmw_cyclonedds`/`rmw_fastrtps`, Humble+) MANGLES both so DDS discovery (SPDP/SEDP)
// matches endpoints across implementations. A native DDS publisher must apply the SAME
// mangling to be discovered by a live `ros2` daemon with zero config. These pure functions
// (no DDS deps) encode that convention and are unit-tested against the spec below.

/// The `rmw` DDS-topic prefix for an ordinary ROS **topic** (as opposed to a service
/// request `rq`/reply `rr`) — CONCEPT:EG-KG.ingest.rmw-topic-prefix. rmw prepends `rt` to the fully-qualified ROS
/// topic name, so the ROS topic `/chatter` becomes the DDS topic `rt/chatter`.
pub const RMW_ROS_TOPIC_PREFIX: &str = "rt";

/// The CDR **representation identifier** for ROS 2's little-endian XCDR1
/// payloads (CONCEPT:EG-KG.ingest.rmw-topic-prefix). The identifier is the
/// first two bytes of every CDR-LE sample; the final two encapsulation bytes
/// are serializer metadata and are not a fixed header across payloads.
pub const CDR_LE_REPRESENTATION_ID: [u8; 2] = [0x00, 0x01];

/// The four-byte CDR encapsulation observed for the EG-349 `std_msgs/String`
/// golden sample (`data = "eg349"`). Dust DDS stores the trailing alignment
/// padding length in the fourth byte, so this value is intentionally not a
/// universal header: a payload with a different length may end in `0`, `1`,
/// `2`, or `3` there.
pub const CDR_LE_ENCAPSULATION_HEADER: [u8; 4] = [0x00, 0x01, 0x00, 0x02];

#[cfg(test)]
const DDS_TEST_DOMAIN_BASE: u32 = 64;
#[cfg(test)]
const DDS_TEST_DOMAIN_SLOTS: u32 = 128;

/// Allocate a bounded, process-local DDS identity for lifecycle fixtures.
///
/// The slot is deliberately recycled rather than embedding a timestamp or
/// process id in the wire identity. Tests must release their participant before
/// a recycled slot is used again; the topic suffix still prevents collisions
/// between concurrently active fixtures.
#[cfg(test)]
fn next_isolated_dds_identity(prefix: &str) -> (u32, String) {
    static NEXT_SLOT: AtomicU32 = AtomicU32::new(0);
    let slot = NEXT_SLOT
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some((current + 1) % DDS_TEST_DOMAIN_SLOTS)
        })
        .unwrap_or(0);
    (
        DDS_TEST_DOMAIN_BASE + slot,
        format!("/{prefix}_{slot:03}"),
    )
}

/// Mangle a ROS 2 topic name to the DDS topic name a live `ros2` daemon uses
/// (CONCEPT:EG-KG.ingest.rmw-topic-prefix). rmw first fully-qualifies the ROS name (a leading `/`), then prepends
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
/// discovery (CONCEPT:EG-KG.ingest.rmw-topic-prefix): `<pkg>::<namespace>::dds_::<Msg>_`. Accepts either the
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

// ── Native DDS/RTPS transport via pure-Rust Dust DDS (CONCEPT:EG-KG.ingest.dds-transport) ────────
#[cfg(feature = "ros2-dds")]
mod native {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{atomic::{AtomicBool, Ordering}, Mutex};

    use dust_dds::{
        domain::{
            domain_participant::DomainParticipant,
            domain_participant_factory::DomainParticipantFactory,
        },
        infrastructure::{
            error::DdsError,
            qos::{DataReaderQos, DataWriterQos, QosKind},
            qos_policy::{
                HistoryQosPolicy, HistoryQosPolicyKind, ReliabilityQosPolicy,
                ReliabilityQosPolicyKind,
            },
            status::NO_STATUS,
            time::{Duration, DurationKind},
            type_support::DdsType,
        },
        listener::NO_LISTENER,
        publication::{data_writer::DataWriter, publisher::Publisher},
        subscription::{data_reader::DataReader, subscriber::Subscriber},
        topic_definition::topic_description::TopicDescription,
    };

    use super::super::ros2_bridge::ROS_STRING_TYPE;

    /// CDR message mirroring ROS2 `std_msgs/String` — a single `data` string field. The
    /// engine carries the change/node payload as a JSON string in `data`, exactly as the
    /// rosbridge leg does, so the two transports are payload-identical.
    #[derive(DdsType, Clone, Debug)]
    struct Ros2String {
        data: String,
    }

    type Writer = DataWriter<Ros2String>;
    type Reader = DataReader<Ros2String>;

    /// Native DDS/RTPS [`DdsTransport`] (CONCEPT:EG-KG.ingest.dds-transport) over pure-Rust Dust DDS. Owns one
    /// DDS `DomainParticipant` + a publisher/subscriber pair, and per-topic
    /// writer/reader maps. Topic creation is serialized and reuses the local
    /// topic description, so subscribe-then-advertise cannot create two DDS
    /// topics with the same mangled name. NO CycloneDDS/rmw/C toolchain —
    /// 100% Rust on the wire.
    pub struct NativeDdsTransport {
        writers: Mutex<HashMap<String, Writer>>,
        readers: Mutex<HashMap<String, Reader>>,
        topic_creation: Mutex<()>,
        publisher: Publisher,
        subscriber: Subscriber,
        writer_qos: DataWriterQos,
        reader_qos: DataReaderQos,
        closed: AtomicBool,
        participant: DomainParticipant,
    }

    impl NativeDdsTransport {
        /// Join DDS domain `domain_id` and build the participant + pub/sub. `Reliable` +
        /// `KeepLast(16)` QoS — a small, bounded history so a late-joining reader still
        /// receives recent changes.
        pub fn new(domain_id: u16) -> Result<Self, String> {
            let participant = DomainParticipantFactory::get_instance()
                .create_participant(domain_id.into(), QosKind::Default, NO_LISTENER, NO_STATUS)
                .map_err(|e| format!("dds participant: {e}"))?;
            let publisher = match participant
                .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            {
                Ok(publisher) => publisher,
                Err(e) => {
                    let _ = participant.delete_contained_entities();
                    let _ = DomainParticipantFactory::get_instance()
                        .delete_participant(&participant);
                    return Err(format!("dds publisher: {e}"));
                }
            };
            let subscriber = match participant
                .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            {
                Ok(subscriber) => subscriber,
                Err(e) => {
                    let _ = participant.delete_contained_entities();
                    let _ = DomainParticipantFactory::get_instance()
                        .delete_participant(&participant);
                    return Err(format!("dds subscriber: {e}"));
                }
            };
            let reliability = ReliabilityQosPolicy {
                kind: ReliabilityQosPolicyKind::Reliable,
                max_blocking_time: DurationKind::Finite(Duration::new(0, 100_000_000)),
            };
            let history = HistoryQosPolicy {
                kind: HistoryQosPolicyKind::KeepLast(16),
            };
            Ok(Self {
                writers: Mutex::new(HashMap::new()),
                readers: Mutex::new(HashMap::new()),
                topic_creation: Mutex::new(()),
                publisher,
                subscriber,
                writer_qos: DataWriterQos {
                    reliability: reliability.clone(),
                    history: history.clone(),
                    ..Default::default()
                },
                reader_qos: DataReaderQos {
                    reliability,
                    history,
                    ..Default::default()
                },
                closed: AtomicBool::new(false),
                participant,
            })
        }

        /// Release all locally-created DDS entities and then delete the
        /// participant. Consuming the transport makes the lifecycle explicit;
        /// [`Drop`] invokes the same cleanup for timeout/panic/early-return
        /// paths.
        pub fn close(mut self) -> Result<(), String> {
            self.close_inner()
        }

        fn close_inner(&mut self) -> Result<(), String> {
            if self.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            if let Ok(writers) = self.writers.get_mut() {
                writers.clear();
            }
            if let Ok(readers) = self.readers.get_mut() {
                readers.clear();
            }
            let factory = DomainParticipantFactory::get_instance();
            let contained = self
                .participant
                .delete_contained_entities()
                .map_err(|e| format!("dds contained entities: {e}"));
            let participant = factory
                .delete_participant(&self.participant)
                .map_err(|e| format!("dds participant delete: {e}"));
            let result = match contained {
                Err(error) => Err(error),
                Ok(()) => participant,
            };
            if result.is_err() {
                self.closed.store(false, Ordering::Release);
            }
            result
        }

        /// Read the DDS domain id from `EPISTEMIC_GRAPH_ROS_DDS_DOMAIN` (default `0`).
        pub fn domain_from_env() -> u16 {
            std::env::var("EPISTEMIC_GRAPH_ROS_DDS_DOMAIN")
                .ok()
                .and_then(|v| v.trim().parse::<u16>().ok())
                .unwrap_or(0)
        }

        /// Create the underlying DDS [`Topic`] using the **rmw-mangled** name+type
        /// (CONCEPT:EG-KG.ingest.rmw-topic-prefix) so a live `ros2` daemon discovers/subscribes it zero-config:
        /// the ROS name `name` is put on the wire as [`mangle_topic_name`] (`rt/…`) and the
        /// ROS type `type_name` as [`mangle_type_name`] (`<pkg>::<ns>::dds_::<Msg>_`). The
        /// caller-facing writer/reader map still keys on the un-mangled ROS `name`.
        fn topic(&self, name: &str, type_name: &str) -> Result<TopicDescription, String> {
            // Dust DDS rejects (and some versions hang on) a second local topic
            // with the same name. Serialize lookup/create and reuse the local
            // topic proxy for both the reader and writer.
            let _topic_guard = self
                .topic_creation
                .lock()
                .map_err(|_| "dds topic registry poisoned".to_string())?;
            let dds_name = mangle_topic_name(name);
            let dds_type = mangle_type_name(type_name);
            if let Some(topic) = self
                .participant
                .lookup_topicdescription(&dds_name)
                .map_err(|e| format!("dds topic lookup {name}: {e}"))?
            {
                if topic.get_type_name() != dds_type {
                    return Err(format!(
                        "dds topic {name}: type mismatch (existing {}, requested {dds_type})",
                        topic.get_type_name()
                    ));
                }
                return Ok(topic);
            }
            self.participant
                .create_topic::<Ros2String>(
                    &dds_name,
                    &dds_type,
                    QosKind::Default,
                    NO_LISTENER,
                    NO_STATUS,
                )
                .map_err(|e| format!("dds topic {name}: {e}"))
        }
    }

    #[async_trait::async_trait]
    impl DdsTransport for NativeDdsTransport {
        async fn advertise(&self, topic: &str, msg_type: &str) -> Result<(), String> {
            let mut writers = self
                .writers
                .lock()
                .map_err(|_| "dds writer registry poisoned".to_string())?;
            if writers.contains_key(topic) {
                return Ok(());
            }
            let t = self.topic(topic, msg_type)?;
            let writer: Writer = self
                .publisher
                .create_datawriter(
                    &t,
                    QosKind::Specific(self.writer_qos.clone()),
                    NO_LISTENER,
                    NO_STATUS,
                )
                .map_err(|e| format!("dds writer {topic}: {e}"))?;
            writers.insert(topic.to_string(), writer);
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
                .map_err(|e| format!("dds write {topic}: {e}"))
        }

        async fn subscribe(&self, topic: &str) -> Result<(), String> {
            let mut readers = self
                .readers
                .lock()
                .map_err(|_| "dds reader registry poisoned".to_string())?;
            if readers.contains_key(topic) {
                return Ok(());
            }
            let t = self.topic(topic, ROS_STRING_TYPE)?;
            let reader: Reader = self
                .subscriber
                .create_datareader(
                    &t,
                    QosKind::Specific(self.reader_qos.clone()),
                    NO_LISTENER,
                    NO_STATUS,
                )
                .map_err(|e| format!("dds reader {topic}: {e}"))?;
            readers.insert(topic.to_string(), reader);
            Ok(())
        }

        async fn poll_inbound(&self) -> Result<Option<(String, Value)>, String> {
            let mut readers = self.readers.lock().unwrap();
            for (topic, reader) in readers.iter_mut() {
                match reader.take_next_sample() {
                    Ok(sample) => {
                        if let Some(sample) = sample.data {
                            let msg = serde_json::json!({ "data": sample.data });
                            return Ok(Some((topic.clone(), msg)));
                        }
                    }
                    Err(DdsError::NoData) => {}
                    Err(e) => return Err(format!("dds read {topic}: {e}")),
                }
            }
            Ok(None)
        }
    }

    impl Drop for NativeDdsTransport {
        fn drop(&mut self) {
            let _ = self.close_inner();
        }
    }

    #[cfg(test)]
    mod cdr_tests {
        use super::*;
        use dust_dds::{
            infrastructure::type_support::TypeSupport, xtypes::serializer::Cdr1LeSerializer,
        };

        /// EG-349: prove the native leg serializes a `std_msgs/String`-shaped sample with
        /// the exact CDR-LE golden vector emitted by the pinned Dust DDS serializer.
        /// The fourth encapsulation byte is the trailing alignment-padding count;
        /// for `eg349` the string occupies two padding bytes, hence `00 01 00 02`.
        /// No live daemon is needed: this checks the on-wire bytes against the
        /// serializer's XCDR1 representation-id and padding contract directly.
        #[test]
        fn eg349_ros2_cdr_le_encapsulation_header() {
            let dynamic = Ros2String {
                data: "eg349".to_string(),
            }
            .create_dynamic_sample();
            let payload = Cdr1LeSerializer::serialize(&dynamic).expect("cdr serialize");
            assert_eq!(
                &payload[..4],
                &CDR_LE_ENCAPSULATION_HEADER,
                "EG-349: on-wire CDR encapsulation must match the XCDR1 golden vector",
            );
            assert_eq!(
                &payload[..2],
                &CDR_LE_REPRESENTATION_ID,
                "EG-349: representation id must be CDR little-endian",
            );
            assert_eq!(
                payload,
                vec![
                    0x00, 0x01, 0x00, 0x02, // CDR_LE + two bytes trailing padding
                    6, 0, 0, 0, // string length including the terminating NUL
                    b'e', b'g', b'3', b'4', b'9', 0, // std_msgs/String data
                    0, 0, // complete the 4-byte XCDR1 alignment
                ],
                "EG-349: serialized std_msgs/String bytes must remain interoperable",
            );
        }
    }
}

#[cfg(feature = "ros2-dds")]
pub use native::NativeDdsTransport;

// ── S5: CycloneDDS-C-backed `rmw` transport (CONCEPT:EG-KG.ingest.rmw-cyclonedds-leg) ──
//
// The SECOND native leg of the `DdsTransport` seam: where [`native::NativeDdsTransport`]
// (feature `ros2-dds`) is pure-Rust Dust DDS (wire-COMPATIBLE with rmw's mangled
// names/CDR framing, but not the C stack itself), this leg links the REAL
// `rmw_cyclonedds`/CycloneDDS-C stack via the safe `cyclonedds` Rust crate — so it is
// genuine zero-config live-`ros2` interop, not merely wire-compatible. It implements the
// IDENTICAL [`DdsTransport`] trait and reuses the SAME [`mangle_topic_name`] /
// [`mangle_type_name`] rmw mangling defined above (no forked shaping).
#[cfg(feature = "ros2-rmw")]
mod cyclone {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use cyclonedds::{
        adr, DataReader, DataWriter, DdsEntity, DdsString, DdsType, DomainParticipant, History,
        Publisher, Qos, QosBuilder, Reliability, Subscriber, Topic, TYPE_STR,
    };

    use super::super::ros2_bridge::ROS_STRING_TYPE;

    /// The rmw-mangled `std_msgs/String` type name this leg advertises with
    /// (CONCEPT:EG-KG.ingest.rmw-topic-prefix) — MUST match [`mangle_type_name`] applied to
    /// [`ROS_STRING_TYPE`]; asserted by [`cyclone_tests::eg347_cyclone_type_name_matches_mangling`].
    const CYCLONE_ROS_STRING_TYPE_NAME: &str = "std_msgs::msg::dds_::String_";

    /// CycloneDDS sample type mirroring ROS2 `std_msgs/String` — a single `data` field.
    /// `#[repr(C)]` + [`DdsString`] (a `#[repr(transparent)]` `char*` wrapper) so the Rust
    /// layout matches the C `dds_topic_descriptor_t` this hand-written [`DdsType`] impl
    /// declares: one ADR opcode, a bare string, at offset 0. No IDL compiler / codegen is
    /// involved — the descriptor is small enough (one field) to hand-roll directly against
    /// the crate's public `topic::{adr, TYPE_STR}` ops-array API.
    #[repr(C)]
    struct CycloneRos2String {
        data: DdsString,
    }

    impl DdsType for CycloneRos2String {
        fn type_name() -> &'static str {
            CYCLONE_ROS_STRING_TYPE_NAME
        }

        fn ops() -> Vec<u32> {
            adr(
                TYPE_STR,
                std::mem::offset_of!(CycloneRos2String, data) as u32,
            )
        }

        // The default `clone_out` does a raw `ptr::read` (a bitwise copy of the `char*`),
        // which would alias the SAME allocation the CycloneDDS loan owns — freed once by
        // this clone's `Drop` and once more when the loan is returned, a double-free. This
        // override does a proper `DdsString::clone` (a `dds_string_dup`), so the returned
        // value owns an INDEPENDENT allocation and outlives the loan return, satisfying the
        // `DdsType::clone_out` safety contract (see its doc comment).
        //
        // SAFETY: `ptr` is a CycloneDDS-owned loaned sample of `Self` for the current topic
        // descriptor (the `DdsType::clone_out` contract guarantees the loan is live for the
        // duration of this borrow — the caller in `DataReader::take_next`/`read_impl` holds
        // the loan until `dds_return_loan`, strictly after this returns). So `&*ptr` is a
        // valid shared borrow of an initialized `CycloneRos2String`, and cloning its
        // `DdsString` field (`dds_string_dup`) yields an owned copy with an independent C
        // allocation that outlives the loan. This is the C-FFI exception the repo's
        // `#![deny(unsafe_code)]` (src/lib.rs:6) allows only via a scoped `#[allow]` + note;
        // it is gated to the `ros2-rmw` cyclonedds leg, not a blanket crate-level allow.
        #[allow(unsafe_code)]
        unsafe fn clone_out(ptr: *const Self) -> Self {
            let loaned = unsafe { &*ptr };
            CycloneRos2String {
                data: loaned.data.clone(),
            }
        }
    }

    type Writer = DataWriter<CycloneRos2String>;
    type Reader = DataReader<CycloneRos2String>;

    /// CycloneDDS-C-backed [`DdsTransport`] (CONCEPT:EG-KG.ingest.rmw-cyclonedds-leg). Owns one DDS
    /// `DomainParticipant` + a publisher/subscriber pair, a per-ROS-topic cache of the
    /// underlying DDS `Topic` entity id, and a per-topic writer/reader map. Links the REAL
    /// CycloneDDS C stack (vendored + cmake-built — see the `cyclonedds` dependency doc in
    /// `Cargo.toml`).
    ///
    /// **One DDS `Topic` per name, shared by the writer and reader.** The writer (from
    /// `advertise`) and the reader (from `subscribe`) for a given ROS topic are created from
    /// the SAME underlying DDS `Topic` entity — created ONCE, on first use, by
    /// [`Self::topic_entity`] and cached in [`Self::topics`]. Creating a SECOND DDS `Topic`
    /// with the same mangled name on one participant makes this CycloneDDS build spin
    /// indefinitely inside `dds_stream_minimum_xcdr_version` (observed under `ros2-rmw`), so
    /// the cache guarantees `dds_create_topic` is called at most once per name — matching the
    /// crate's own examples (turtlesim etc.), which share one `Topic` between writer/reader.
    ///
    /// The DDS `Topic<T>` wrapper is intentionally NOT retained after its entity id is
    /// cached: the crate's `Topic<T>` holds an `Rc` (so keeping it would make this struct
    /// `!Send`/`!Sync`, breaking the `DdsTransport: Send + Sync` bound), and — as with any
    /// DDS DCPS entity tree — the topic's underlying C entity (the plain `dds_entity_t`
    /// `i32` we cache) stays valid for as long as its owning `DomainParticipant` is alive,
    /// independent of whether the local Rust `Topic<T>` handle was dropped early.
    /// [`Self::topic_entity`] therefore `std::mem::forget`s the `Topic<T>` right after
    /// reading its entity id, so the topic is torn down only transitively when `participant`
    /// drops (cascading `dds_delete`), never explicitly early.
    ///
    /// `Qos` is likewise NOT retained as a field: it wraps a raw `*mut dds_qos_t` with no
    /// `Send`/`Sync` impl in the crate (unlike `participant`/`publisher`/`subscriber` +
    /// the `dds_entity_t`/writer/reader handles, which are `Send`/`Sync`), so storing one
    /// would make this struct `!Send`/`!Sync` too. [`Self::build_qos`] builds a fresh,
    /// cheap, short-lived `Qos` wherever one is needed instead.
    pub struct CycloneDdsTransport {
        writers: Mutex<HashMap<String, Writer>>,
        readers: Mutex<HashMap<String, Reader>>,
        /// ROS topic name → the shared underlying DDS `Topic` entity id (`dds_entity_t`,
        /// an `i32`), created at most ONCE per name (see the struct doc).
        topics: Mutex<HashMap<String, i32>>,
        publisher: Publisher,
        subscriber: Subscriber,
        participant: DomainParticipant,
    }

    impl CycloneDdsTransport {
        /// The SAME `Reliable` + `KeepLast(16)` QoS policy
        /// [`native::NativeDdsTransport::new`] uses, so the two legs behave identically
        /// from the CDC↔ROS2 path's point of view.
        fn build_qos() -> Result<Qos, String> {
            QosBuilder::new()
                .reliability(Reliability::Reliable, 100_000_000)
                .history(History::KeepLast(16))
                .build()
                .map_err(|e| format!("dds qos: {e:?}"))
        }

        /// Join DDS domain `domain_id` and build the participant + pub/sub.
        pub fn new(domain_id: u32) -> Result<Self, String> {
            let participant =
                DomainParticipant::new(domain_id).map_err(|e| format!("dds participant: {e:?}"))?;
            let qos = Self::build_qos()?;
            let publisher = Publisher::with_qos(participant.entity(), Some(&qos))
                .map_err(|e| format!("dds publisher: {e:?}"))?;
            let subscriber = Subscriber::with_qos(participant.entity(), Some(&qos))
                .map_err(|e| format!("dds subscriber: {e:?}"))?;
            Ok(Self {
                writers: Mutex::new(HashMap::new()),
                readers: Mutex::new(HashMap::new()),
                topics: Mutex::new(HashMap::new()),
                publisher,
                subscriber,
                participant,
            })
        }

        /// Consume the transport so readers/writers, publisher/subscriber, and
        /// participant are dropped in DDS containment order. Scope-drop on an
        /// error or panic follows the same field order.
        pub fn close(mut self) {
            if let Ok(writers) = self.writers.get_mut() {
                writers.clear();
            }
            if let Ok(readers) = self.readers.get_mut() {
                readers.clear();
            }
            if let Ok(topics) = self.topics.get_mut() {
                topics.clear();
            }
        }

        /// Read the DDS domain id from `EPISTEMIC_GRAPH_ROS_DDS_DOMAIN` (default `0`) — the
        /// SAME env var [`native::NativeDdsTransport::domain_from_env`] reads, so switching
        /// legs needs no config change.
        pub fn domain_from_env() -> u32 {
            std::env::var("EPISTEMIC_GRAPH_ROS_DDS_DOMAIN")
                .ok()
                .and_then(|v| v.trim().parse::<u32>().ok())
                .unwrap_or(0)
        }

        /// Return the shared DDS `Topic` entity id for ROS topic `name`, creating it ONCE
        /// (on first use) with the rmw-mangled name and caching it. The ROS name `name` is
        /// put on the wire as [`mangle_topic_name`] (`rt/…`, the fixed `std_msgs/String`
        /// descriptor), matching what a live `ros2` daemon's SPDP/SEDP discovery looks for.
        /// Both the writer (`advertise`) and reader (`subscribe`) for a name share this one
        /// entity — a SECOND `dds_create_topic` with the same name hangs this CycloneDDS
        /// build (see the struct doc), which the cache prevents.
        fn topic_entity(&self, name: &str) -> Result<i32, String> {
            let mut topics = self.topics.lock().unwrap();
            if let Some(entity) = topics.get(name) {
                return Ok(*entity);
            }
            let dds_name = mangle_topic_name(name);
            let qos = Self::build_qos()?;
            let topic: Topic<CycloneRos2String> =
                Topic::with_qos(self.participant.entity(), &dds_name, Some(&qos))
                    .map_err(|e| format!("dds topic {name}: {e:?}"))?;
            let entity = topic.entity();
            // See the struct doc: the topic's C entity outlives the wrapper via the
            // participant; we keep only the entity id (a Send/Sync `i32`).
            std::mem::forget(topic);
            topics.insert(name.to_string(), entity);
            Ok(entity)
        }
    }

    #[async_trait::async_trait]
    impl DdsTransport for CycloneDdsTransport {
        async fn advertise(&self, topic: &str, _msg_type: &str) -> Result<(), String> {
            if self.writers.lock().unwrap().contains_key(topic) {
                return Ok(());
            }
            let topic_entity = self.topic_entity(topic)?;
            let qos = Self::build_qos()?;
            let writer = DataWriter::with_qos(self.publisher.entity(), topic_entity, Some(&qos))
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
            let sample = CycloneRos2String {
                data: DdsString::new(&data).map_err(|e| format!("dds string {topic}: {e:?}"))?,
            };
            let writers = self.writers.lock().unwrap();
            let writer = writers
                .get(topic)
                .ok_or_else(|| format!("no dds writer for {topic}"))?;
            writer
                .write(&sample)
                .map_err(|e| format!("dds write {topic}: {e:?}"))
        }

        async fn subscribe(&self, topic: &str) -> Result<(), String> {
            if self.readers.lock().unwrap().contains_key(topic) {
                return Ok(());
            }
            let topic_entity = self.topic_entity(topic)?;
            let qos = Self::build_qos()?;
            let reader = DataReader::with_qos(self.subscriber.entity(), topic_entity, Some(&qos))
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
                match reader.take_next() {
                    Ok(Some(sample)) => {
                        let msg = serde_json::json!({ "data": sample.data.data.to_string_lossy() });
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
    mod cyclone_tests {
        use super::*;

        /// EG-347 (S5): the type name this leg advertises with MUST equal
        /// [`mangle_type_name`] applied to the shared [`ROS_STRING_TYPE`] constant — kept
        /// as a `&'static str` on [`CycloneRos2String`] (the `DdsType` trait requires a
        /// compile-time constant) rather than computed, so this test guards against drift
        /// between the two.
        #[test]
        fn eg347_cyclone_type_name_matches_mangling() {
            assert_eq!(
                mangle_type_name(ROS_STRING_TYPE),
                CYCLONE_ROS_STRING_TYPE_NAME,
            );
            assert_eq!(CycloneRos2String::type_name(), CYCLONE_ROS_STRING_TYPE_NAME,);
        }

        /// EG-347 (S5): a REAL RTPS loopback over the CycloneDDS-C `rmw` stack — publish a
        /// `std_msgs/String`-shaped message on a topic, subscribe to the same topic, and
        /// prove the round-trip over an actual CycloneDDS-C wire (with real DDS discovery),
        /// then map the inbound message back to an engine `AddNode` via the shared EG-325
        /// [`inbound_to_method`] path — mirroring
        /// [`native::tests::eg347_native_dds_loopback_pub_sub_roundtrip`] so both legs are
        /// exercised identically.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn eg347_cyclone_dds_loopback_pub_sub_roundtrip() {
            let (domain, topic) = next_isolated_dds_identity("eg129_cyclone_loopback");

            assert_eq!(
                mangle_topic_name(&topic),
                format!("rt{topic}")
            );

            let transport = CycloneDdsTransport::new(domain).expect("dds transport");
            transport.subscribe(&topic).await.expect("subscribe");
            transport
                .advertise(&topic, ROS_STRING_TYPE)
                .await
                .expect("advertise");

            // Let DDS SPDP/SEDP discovery match the writer and reader on this participant.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

            let payload = serde_json::json!({ "node_id": "robot_1", "properties": { "x": 1.5 } });
            let msg = serde_json::json!({ "data": payload.to_string() });
            transport.publish(&topic, &msg).await.expect("publish");

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            loop {
                if let Some((got_topic, got)) = transport.poll_inbound().await.expect("poll") {
                    assert_eq!(got_topic, topic);
                    let method = inbound_to_method(&got).expect("maps to a method");
                    match method {
                        Method::AddNode { node_id, .. } => assert_eq!(node_id, "robot_1"),
                        _ => panic!("expected AddNode from the DDS round-trip"),
                    }
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "EG-347: timed out waiting for the CycloneDDS/RTPS loopback sample"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            transport.close();
        }
    }
}

#[cfg(feature = "ros2-rmw")]
pub use cyclone::CycloneDdsTransport;

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
    /// on a topic through the native Dust DDS transport, subscribe to the same topic, and
    /// prove the round-trip (over an actual RTPS wire, with DDS discovery), then map the
    /// inbound message back to an engine `AddNode` via the shared EG-325
    /// [`inbound_to_method`] path. The DDS wire now carries the **rmw-mangled** names
    /// (asserted here) so the same publish is discoverable by a live `ros2` daemon.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eg347_native_dds_loopback_pub_sub_roundtrip() {
        let (domain, topic) = next_isolated_dds_identity("eg129_native_loopback");

        // The transport puts these rmw-mangled names on the DDS wire (what `ros2` matches).
        assert_eq!(mangle_topic_name(&topic), format!("rt{topic}"));
        assert_eq!(
            mangle_type_name(ROS_STRING_TYPE),
            "std_msgs::msg::dds_::String_"
        );
        let transport = NativeDdsTransport::new(domain as u16).expect("dds transport");
        transport.subscribe(&topic).await.expect("subscribe");
        transport
            .advertise(&topic, ROS_STRING_TYPE)
            .await
            .expect("advertise");

        // Let DDS SPDP/SEDP discovery match the writer and reader on this participant.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // The payload the CDC/ingest path carries: a std_msgs/String whose `data` is the
        // JSON describing a node — exactly the rosbridge shape.
        let payload = serde_json::json!({ "node_id": "robot_1", "properties": { "x": 1.5 } });
        let msg = serde_json::json!({ "data": payload.to_string() });
        transport.publish(&topic, &msg).await.expect("publish");

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
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "EG-347: timed out waiting for the DDS/RTPS loopback sample"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        transport.close().expect("close dds transport");
    }

    async fn native_lifecycle_once(prefix: &str) -> Result<(), String> {
        let (domain, topic) = next_isolated_dds_identity(prefix);
        let transport = NativeDdsTransport::new(domain as u16)?;
        transport.subscribe(&topic).await?;
        transport.advertise(&topic, ROS_STRING_TYPE).await?;
        transport.close()
    }

    /// EG-129: lifecycle fixtures use bounded, unique identities and release
    /// participants on both concurrent and repeated paths. The subscribe-first
    /// ordering exercises the single-topic creation guard that prevents the
    /// process-global collision seen in the original loopback fixture.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eg129_native_dds_lifecycle_isolated_repeat_and_parallel() {
        let (parallel_a, parallel_b) = tokio::join!(
            native_lifecycle_once("eg129_native_parallel_a"),
            native_lifecycle_once("eg129_native_parallel_b"),
        );
        parallel_a.expect("parallel DDS lifecycle A");
        parallel_b.expect("parallel DDS lifecycle B");

        for _ in 0..4 {
            native_lifecycle_once("eg129_native_repeat")
                .await
                .expect("repeated DDS lifecycle");
        }
    }
}
