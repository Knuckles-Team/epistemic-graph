//! ROS2 bridge over the rosbridge-WebSocket JSON protocol (CONCEPT:EG-KG.domains.robotics-gpu-distribution).
//!
//! This bridges the engine to a ROS2 graph WITHOUT a DDS stack: it speaks the standard
//! `rosbridge_suite` protocol — JSON messages over a WebSocket to a `rosbridge_server` —
//! so there is **no CycloneDDS/rmw/`ros` C toolchain** anywhere in the build, just a
//! pure-Rust `tokio-tungstenite` client. It is the well-trodden way non-ROS processes
//! (web apps, other languages) join a ROS2 graph.
//!
//! Two directions, both over one WebSocket connection to the rosbridge server:
//!
//! * **Engine → ROS2 (publish).** The bridge tails the engine's CDC feed
//!   (CONCEPT:EG-KG.query.streaming-cdc-subscriptions) for a configured graph and, for each change, sends a rosbridge
//!   `{"op":"publish","topic":<out_topic>,"msg":{…}}` — so a graph mutation becomes a ROS2
//!   message any ROS2 node can subscribe to. It `advertise`s the out-topic first.
//! * **ROS2 → engine (subscribe).** The bridge `subscribe`s to a configured in-topic and,
//!   for each inbound `{"op":"publish","topic":<in_topic>,"msg":{…}}`, maps the message to
//!   an engine [`Request`] carrying an `AddNode` and applies it through authenticated
//!   engine dispatch — so a signed ROS2 request becomes graph state.
//!
//! The rosbridge protocol framing ([`RosbridgeOp`]) and the CDC↔ROS2 message mapping
//! ([`cdc_to_publish`] / [`publish_to_request`]) are pure and unit-tested; the WebSocket
//! driver ([`run_ros2_bridge`]) wires them onto a live connection.
//!
//! Feature-gated behind `ros2-bridge` (out of `pi`/`default`/`node`): a slim build links
//! no `tokio-tungstenite` (Pi contract). The PURE mapping here ([`cdc_to_publish`] /
//! [`publish_to_request`] / [`RosbridgeOp`]) is ALSO compiled under `ros2-dds`
//! (CONCEPT:EG-KG.ingest.dds-transport): the native DDS/RTPS transport seam ([`super::dds`]) reuses this exact
//! shaping, so the DDS leg puts the IDENTICAL `std_msgs/String` payload on the wire as the
//! rosbridge leg. Only the tungstenite driver [`run_ros2_bridge`] is `ros2-bridge`-specific.

use serde::{Deserialize, Serialize};

use crate::protocol::{Method, Request};
use crate::wire::{CdcEvent, CdcKind};

// ── The rosbridge JSON protocol (CONCEPT:EG-KG.domains.robotics-gpu-distribution) ─────────────────────────────

/// One rosbridge protocol message (CONCEPT:EG-KG.domains.robotics-gpu-distribution), tagged by the standard `op` field.
/// This is the subset the bridge uses of the `rosbridge_suite` protocol: `advertise`,
/// `unadvertise`, `publish`, `subscribe`, `unsubscribe`. `serde(tag = "op")` +
/// `rename_all = "snake_case"` produces exactly the wire shape rosbridge expects, e.g.
/// `{"op":"publish","topic":"/eg","msg":{…}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RosbridgeOp {
    /// Declare intent to publish a topic (before the first publish).
    Advertise {
        topic: String,
        #[serde(rename = "type")]
        msg_type: String,
    },
    /// Stop advertising a topic.
    Unadvertise { topic: String },
    /// Publish a message on a topic.
    Publish {
        topic: String,
        msg: serde_json::Value,
    },
    /// Subscribe to a topic (inbound messages arrive as `Publish` frames).
    Subscribe {
        topic: String,
        #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
        msg_type: Option<String>,
    },
    /// Unsubscribe from a topic.
    Unsubscribe { topic: String },
}

impl RosbridgeOp {
    /// Serialize to the compact JSON line rosbridge expects.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }

    /// Parse one inbound rosbridge JSON frame (CONCEPT:EG-KG.domains.robotics-gpu-distribution). Unknown ops / malformed
    /// frames return `None` (ignored — rosbridge also sends `status`/`service_response`
    /// frames the bridge does not act on).
    pub fn from_json(text: &str) -> Option<Self> {
        serde_json::from_str(text).ok()
    }
}

/// The `std_msgs/String`-shaped type name the bridge advertises/subscribes with by default
/// — a JSON payload carried in the message's `data` field, the rosbridge lingua franca.
pub const ROS_STRING_TYPE: &str = "std_msgs/String";

// ── CDC ↔ ROS2 message mapping (CONCEPT:EG-KG.domains.robotics-gpu-distribution) ──────────────────────────────

/// Map an engine CDC change to a rosbridge `publish` op (CONCEPT:EG-KG.domains.robotics-gpu-distribution). The ROS2 message
/// is a `std_msgs/String` whose `data` is a JSON string describing the change (kind, graph,
/// node/target ids, label) — so a downstream ROS2 node parses one field. Pure + tested.
pub fn cdc_to_publish(event: &CdcEvent, topic: &str) -> RosbridgeOp {
    let kind = match event.kind {
        CdcKind::AddNode => "add_node",
        CdcKind::RemoveNode => "remove_node",
        CdcKind::UpdateNode => "update_node",
        CdcKind::AddEdge => "add_edge",
        CdcKind::RemoveEdge => "remove_edge",
    };
    let data = serde_json::json!({
        "seq": event.seq,
        "graph": event.graph,
        "kind": kind,
        "node_id": event.node_id,
        "target_id": event.target_id,
        "label": event.label,
    })
    .to_string();
    RosbridgeOp::Publish {
        topic: topic.to_string(),
        msg: serde_json::json!({ "data": data }),
    }
}

/// Map an inbound rosbridge `publish` message to an engine [`Method`] (CONCEPT:EG-KG.domains.robotics-gpu-distribution).
///
/// Accepts a `std_msgs/String`-shaped message whose `data` is a JSON object carrying at
/// least a `node_id` (and optional `properties` object), OR a message that is itself that
/// JSON object. Produces an `AddNode` with the properties MessagePack-encoded (the engine's
/// node blob format). Returns `None` if no `node_id` can be found (a message the bridge
/// does not ingest). Pure + tested.
pub fn publish_to_method(msg: &serde_json::Value) -> Option<Method> {
    // Unwrap a std_msgs/String `data` string if present, else use the msg object directly.
    let obj: serde_json::Value = match msg.get("data") {
        Some(serde_json::Value::String(s)) => serde_json::from_str(s).ok()?,
        _ => msg.clone(),
    };
    let node_id = obj.get("node_id")?.as_str()?.to_string();
    let props = obj
        .get("properties")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let properties_msgpack = rmp_serde::to_vec_named(&props).ok()?;
    Some(Method::AddNode {
        node_id,
        properties_msgpack,
    })
}

/// Bind an inbound ROS2 payload to the exact current signed engine request.
///
/// The message retains the compact `node_id`/`properties` shape, but must also
/// carry `request_id`, the configured `graph`, and an `auth_token` containing a
/// current `eg2.` request envelope. The token is verified by ordinary dispatch
/// against this reconstructed `Method::AddNode`; it is never retained by the
/// bridge or written to graph state.
#[cfg(feature = "server")]
pub fn publish_to_request(msg: &serde_json::Value, graph: &str) -> Option<Request> {
    let obj: serde_json::Value = match msg.get("data") {
        Some(serde_json::Value::String(value)) => serde_json::from_str(value).ok()?,
        _ => msg.clone(),
    };
    let request_id = obj.get("request_id")?.as_u64()?;
    let request_graph = obj.get("graph")?.as_str()?;
    if request_graph != graph {
        return None;
    }
    let auth_token = obj.get("auth_token")?.as_str()?.to_string();
    if !auth_token.starts_with("eg2.") {
        return None;
    }
    let method = publish_to_method(&obj)?;
    Some(Request {
        id: request_id,
        graph: request_graph.to_string(),
        auth_token,
        agent_id: None,
        method,
    })
}

// ── Config + live WebSocket driver (CONCEPT:EG-KG.domains.robotics-gpu-distribution) ──────────────────────────

/// Env-driven configuration for the ROS2 bridge (CONCEPT:EG-KG.domains.robotics-gpu-distribution).
#[derive(Debug, Clone)]
pub struct Ros2BridgeConfig {
    /// The rosbridge server WebSocket URL, e.g. `ws://localhost:9090`.
    pub rosbridge_url: String,
    /// The engine graph whose CDC feed is published to ROS2 (and that inbound ROS2
    /// messages are applied to).
    pub graph: String,
    /// The ROS2 topic engine CDC changes are published on.
    pub out_topic: String,
    /// The ROS2 topic subscribed for inbound messages (empty ⇒ no inbound bridge).
    pub in_topic: String,
}

/// Env var naming the rosbridge server (CONCEPT:EG-KG.domains.robotics-gpu-distribution). Presence enables the bridge.
pub const ROSBRIDGE_URL_ENV: &str = "EPISTEMIC_GRAPH_ROSBRIDGE_URL";

impl Ros2BridgeConfig {
    /// Parse the bridge config from the environment (CONCEPT:EG-KG.domains.robotics-gpu-distribution). Returns `None` unless
    /// [`ROSBRIDGE_URL_ENV`] is set.
    pub fn from_env() -> Option<Self> {
        let rosbridge_url = std::env::var(ROSBRIDGE_URL_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        let get = |k: &str, d: &str| {
            std::env::var(k)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(|| d.to_string())
        };
        Some(Self {
            rosbridge_url: rosbridge_url.trim().to_string(),
            graph: get("EPISTEMIC_GRAPH_ROS_GRAPH", "__ros__"),
            out_topic: get("EPISTEMIC_GRAPH_ROS_OUT_TOPIC", "/epistemic_graph/changes"),
            in_topic: get("EPISTEMIC_GRAPH_ROS_IN_TOPIC", ""),
        })
    }
}

/// Apply an inbound ROS2 message through current authenticated dispatch.
///
/// The configured graph must already exist through the governed graph-lifecycle
/// surface. Dispatch verifies the exact request/session/tenant/idempotency
/// authority before routing `AddNode` through the graph mutation gateway.
/// Returns `true` only when the signed request commits successfully.
#[cfg(feature = "server")]
pub async fn apply_inbound(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    graph: &str,
    msg: &serde_json::Value,
) -> bool {
    let Some(request) = publish_to_request(msg, graph) else {
        crate::metrics::auth_failure();
        return false;
    };
    let response = crate::server::dispatch::dispatch(state, request).await;
    response.error.is_none()
}

/// Run the ROS2 bridge until the connection drops (CONCEPT:EG-KG.domains.robotics-gpu-distribution). Connects to the
/// rosbridge server, advertises the out-topic + subscribes the in-topic, then concurrently
/// (a) forwards the graph's CDC changes as ROS2 publishes and (b) applies inbound ROS2
/// publishes to the engine. Spawned from `main` when [`Ros2BridgeConfig::from_env`] is
/// `Some`; a dropped connection returns so the caller can reconnect with backoff.
///
/// True when the outbound CDC→ROS2 leg must refuse to publish (CONCEPT:EG-KG.sharding.row-level-security
/// fail-closed obligation — the same predicate the `unauthenticated_carrier_denied`
/// check enforces across the SPARQL/obs/federation/lake HTTP surfaces, applied here).
///
/// The bridge streams a graph's raw CDC feed to an external rosbridge server with no
/// per-subscriber identity: whoever is listening on the ROS2 topic is never asked to
/// present a credential, so no [`crate::server::access::CarrierAuthority`] can ever be
/// minted for them (`unauthenticated_carrier_denied(None)` is always `true` for this
/// leg). That is harmless while the target graph carries no row-ownership rules — there
/// is nothing a verified reader could see that an unfiltered feed doesn't. But once
/// authenticated row ownership IS registered for the engine (`isolation.has_rules()`),
/// publishing the unfiltered feed would leak rows a verified reader would never be
/// authorized to see, so the bridge must fail closed instead of blasting every change.
#[cfg(feature = "server")]
fn ros2_cdc_denied(isolation: &crate::isolation::IsolationLayer) -> bool {
    isolation.has_rules() && crate::server::access::unauthenticated_carrier_denied(None)
}

/// Gated on `ros2-bridge` specifically (not just `server`): it is the tokio-tungstenite
/// WebSocket driver, so a `ros2-dds`-only build (CONCEPT:EG-KG.ingest.dds-transport) — which links no
/// tokio-tungstenite — must not compile it. The pure CDC↔ROS2 mapping above stays shared.
#[cfg(all(feature = "server", feature = "ros2-bridge"))]
pub async fn run_ros2_bridge(
    state: std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    cfg: Ros2BridgeConfig,
    cdc: std::sync::Arc<crate::server::cdc::CdcHub>,
) -> Result<(), String> {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    // A18: this connection-level gate used to deny the bridge unconditionally.
    // The bridge itself is engine-internal system wiring, not a client request —
    // an operator explicitly configures + enables it (env var + `--features
    // ros2-bridge`), and it connects OUT to an already-trusted, operator-named
    // `rosbridge_url`; there is no inbound "carrier" presenting credentials on
    // this leg at all (same precedent as `server::registry_reaper`'s internal
    // sweep). The REAL per-message carrier check already lives where it
    // belongs: every inbound ROS2 publish must carry a genuine `eg2.`
    // `auth_token` (`publish_to_request`, below) that `apply_inbound` verifies
    // through the full `dispatch()` path, unaffected by this change — an
    // unauthenticated inbound message is denied there, same as before.
    //
    // The OUTBOUND leg is a separate exposure: it publishes the graph's CDC feed
    // to that same untrusted set of ROS2 subscribers. Fail closed up front when
    // authenticated row ownership is already active for this engine, before ever
    // opening the socket or advertising a topic.
    if ros2_cdc_denied(&state.read().await.isolation) {
        crate::metrics::access_denied();
        return Err(format!(
            "ACCESS_DENIED: ROS2 CDC bridge for graph {:?} refused — authenticated row \
             ownership is active and this carrier has no verified per-subscriber identity",
            cfg.graph
        ));
    }
    let (ws, _resp) = tokio_tungstenite::connect_async(&cfg.rosbridge_url)
        .await
        .map_err(|e| format!("rosbridge connect {} failed: {e}", cfg.rosbridge_url))?;
    let (mut write, mut read) = ws.split();

    // Advertise the outbound topic + subscribe the inbound topic.
    write
        .send(Message::Text(
            RosbridgeOp::Advertise {
                topic: cfg.out_topic.clone(),
                msg_type: ROS_STRING_TYPE.to_string(),
            }
            .to_json(),
        ))
        .await
        .map_err(|e| format!("advertise failed: {e}"))?;
    if !cfg.in_topic.is_empty() {
        write
            .send(Message::Text(
                RosbridgeOp::Subscribe {
                    topic: cfg.in_topic.clone(),
                    msg_type: Some(ROS_STRING_TYPE.to_string()),
                }
                .to_json(),
            ))
            .await
            .map_err(|e| format!("subscribe failed: {e}"))?;
    }

    // (a) CDC → ROS2 publish task.
    let notify = cdc.notifier(&cfg.graph);
    let out_state = state.clone();
    let out_cdc = cdc.clone();
    let out_graph = cfg.graph.clone();
    let out_topic = cfg.out_topic.clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1024);
    tokio::spawn(async move {
        let mut cursor: u64 = 0;
        loop {
            let batch = out_cdc.watch_batch(&out_graph, cursor, "", 256);
            // B-8: `watch_batch` now signals an unservicable cursor explicitly via
            // `gap` instead of an `Err` (e.g. `ClearGraph` reset the feed under this
            // bridge) — end the publish task the same way the old `Err` arm did,
            // rather than resume from a cursor whose history is no longer provable.
            if batch.gap {
                return;
            }
            // Re-check on every batch, not just at connect time: authenticated row
            // ownership can be registered for this engine while the bridge is
            // already streaming, and this unauthenticated carrier must stop
            // publishing the instant that happens rather than keep leaking rows a
            // verified reader would not be authorized to see.
            if ros2_cdc_denied(&out_state.read().await.isolation) {
                return;
            }
            for ev in &batch.events {
                if tx
                    .send(cdc_to_publish(ev, &out_topic).to_json())
                    .await
                    .is_err()
                {
                    return;
                }
            }
            cursor = batch.next_seq;
            notify.notified().await;
        }
    });

    // Drive the socket: pump queued publishes out, and apply inbound frames.
    loop {
        tokio::select! {
            outbound = rx.recv() => {
                match outbound {
                    Some(json) => {
                        if write.send(Message::Text(json)).await.is_err() {
                            return Err("rosbridge write failed".to_string());
                        }
                    }
                    None => return Ok(()),
                }
            }
            inbound = read.next() => {
                match inbound {
                    Some(Ok(Message::Text(text))) => {
                        if let Some(RosbridgeOp::Publish { topic, msg }) =
                            RosbridgeOp::from_json(&text)
                        {
                            if topic == cfg.in_topic {
                                apply_inbound(&state, &cfg.graph, &msg).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(_)) => {} // ping/pong/binary — ignore
                    Some(Err(e)) => return Err(format!("rosbridge read error: {e}")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg3_4_rosbridge_op_roundtrips_the_wire_shape() {
        // The exact rosbridge_suite JSON shape (op tag + snake_case fields).
        let pub_op = RosbridgeOp::Publish {
            topic: "/eg".to_string(),
            msg: serde_json::json!({ "data": "hi" }),
        };
        let json = pub_op.to_json();
        assert_eq!(
            json,
            r#"{"op":"publish","topic":"/eg","msg":{"data":"hi"}}"#
        );
        assert_eq!(RosbridgeOp::from_json(&json), Some(pub_op));

        let adv = RosbridgeOp::Advertise {
            topic: "/eg".to_string(),
            msg_type: "std_msgs/String".to_string(),
        };
        assert_eq!(
            adv.to_json(),
            r#"{"op":"advertise","topic":"/eg","type":"std_msgs/String"}"#
        );

        let sub = RosbridgeOp::Subscribe {
            topic: "/in".to_string(),
            msg_type: None,
        };
        assert_eq!(sub.to_json(), r#"{"op":"subscribe","topic":"/in"}"#);
    }

    #[test]
    fn eg3_4_rosbridge_ignores_unknown_frames() {
        // rosbridge also sends status/service_response frames the bridge ignores.
        assert_eq!(
            RosbridgeOp::from_json(r#"{"op":"status","level":"info"}"#),
            None
        );
        assert_eq!(RosbridgeOp::from_json("not json"), None);
    }

    #[cfg(feature = "streaming")]
    #[test]
    fn eg3_4_cdc_change_maps_to_ros_publish() {
        let ev = CdcEvent {
            seq: 7,
            graph: "g".to_string(),
            kind: CdcKind::AddNode,
            node_id: "n1".to_string(),
            target_id: String::new(),
            label: "Sensor".to_string(),
            before: vec![],
            after: vec![],
            had_before: false,
            had_after: true,
        };
        let RosbridgeOp::Publish { topic, msg } = cdc_to_publish(&ev, "/eg/changes") else {
            panic!("expected a publish op");
        };
        assert_eq!(topic, "/eg/changes");
        // The std_msgs/String `data` carries the change as a JSON string.
        let data = msg.get("data").and_then(|d| d.as_str()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(data).unwrap();
        assert_eq!(parsed["kind"], "add_node");
        assert_eq!(parsed["node_id"], "n1");
        assert_eq!(parsed["label"], "Sensor");
        assert_eq!(parsed["seq"], 7);
    }

    #[test]
    fn eg3_4_inbound_publish_maps_to_add_node() {
        // std_msgs/String data carrying a node.
        let msg = serde_json::json!({
            "data": r#"{"node_id":"robot_1","properties":{"x":1.5,"type":"Pose"}}"#
        });
        let method = publish_to_method(&msg).expect("maps to a method");
        match method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => {
                assert_eq!(node_id, "robot_1");
                let props: serde_json::Value = rmp_serde::from_slice(&properties_msgpack).unwrap();
                assert_eq!(props["x"], 1.5);
                assert_eq!(props["type"], "Pose");
            }
            _ => panic!("expected AddNode"),
        }

        // A bare object (no std_msgs/String wrapper) also works.
        let bare = serde_json::json!({ "node_id": "n2" });
        assert!(matches!(
            publish_to_method(&bare),
            Some(Method::AddNode { .. })
        ));

        // A message with no node_id is not ingested.
        assert!(publish_to_method(&serde_json::json!({"foo": 1})).is_none());
    }

    #[cfg(feature = "server")]
    #[test]
    fn inbound_request_is_exactly_graph_and_envelope_bound() {
        let message = serde_json::json!({
            "request_id": 41,
            "graph": "robotics",
            "auth_token": "eg2.opaque-envelope",
            "node_id": "robot-opaque",
            "properties": {"type": "Pose"}
        });
        let request = publish_to_request(&message, "robotics").expect("signed request");
        assert_eq!(request.id, 41);
        assert_eq!(request.graph, "robotics");
        assert_eq!(request.auth_token, "eg2.opaque-envelope");
        assert!(publish_to_request(&message, "different-graph").is_none());

        let mut unsigned = message;
        unsigned["auth_token"] = serde_json::json!("legacy-token");
        assert!(publish_to_request(&unsigned, "robotics").is_none());
    }

    #[cfg(feature = "server")]
    #[test]
    fn ros2_cdc_fails_closed_only_once_row_ownership_is_active() {
        use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};

        // No identities registered yet: nothing to protect, so the unauthenticated
        // outbound leg is allowed to publish, same as before this fix.
        let open = IsolationLayer::new();
        assert!(!open.has_rules());
        assert!(!ros2_cdc_denied(&open));

        // Once ANY identity is registered, authenticated row ownership is active
        // for this engine and the carrier — which can never present credentials
        // for whoever is listening on the ROS2 topic — must fail closed.
        let mut governed = IsolationLayer::new();
        governed.register_agent(AgentIdentity {
            agent_id: "agent-1".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        assert!(governed.has_rules());
        assert!(ros2_cdc_denied(&governed));
    }
}
