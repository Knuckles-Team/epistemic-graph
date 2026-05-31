use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::protocol::{Method, Request};
use crate::server::{dispatch, ServerState};

/// Background task that listens to Kafka and dispatches `ApplyMutation` requests.
pub async fn start_kafka_consumer(
    brokers: &str,
    group_id: &str,
    topic: &str,
    state: Arc<RwLock<ServerState>>,
) {
    info!("Starting Kafka consumer for topic '{}' at brokers '{}'", topic, brokers);

    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create Kafka consumer: {}", e);
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&[topic]) {
        error!("Failed to subscribe to topic '{}': {}", topic, e);
        return;
    }

    loop {
        match consumer.recv().await {
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => continue,
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {}", e);
                        continue;
                    }
                };

                // The Python client emits JSON like:
                // {"event_type": "TRIPLE_INSERT", "query": "INSERT DATA ...", "source": "fuseki_backend"}
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload.as_bytes()) {
                    if let (Some(event_type), Some(query)) = (
                        json.get("event_type").and_then(|v| v.as_str()),
                        json.get("query").and_then(|v| v.as_str()),
                    ) {
                        let req = Request {
                            id: 0, // EventBus events don't have a correlation ID
                            graph: "agent:global".to_string(), // Default fallback graph for mutations
                            auth_token: "".to_string(),
                            method: Method::ApplyMutation {
                                event_type: event_type.to_string(),
                                query: query.to_string(),
                            },
                        };

                        // Dispatch it just like a UDS/TCP request
                        let _resp = dispatch(&state, req).await;
                    }
                }
            }
            Err(e) => {
                error!("Kafka error: {}", e);
            }
        }
    }
}
