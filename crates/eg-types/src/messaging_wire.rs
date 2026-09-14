//! Pure-data result bodies of the `messaging` contract domain: broker publisher
//! confirms and the dynamic-channel views.
//!
//! They live here, at the bottom of the crate DAG, so the result contract
//! (`result_contract::messaging`) can name the exact body a handler encodes; the broker
//! (`eg-core`) and the channel manager (the facade) re-export them.

use serde::{Deserialize, Serialize};

use crate::protocol::ChannelType;

/// A publisher-confirm token (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos): a
/// broker-wide monotonic `delivery_tag` identifying the publish, plus whether the broker
/// durably accepted it (`confirmed`) or nacked it (unknown exchange). Mirrors AMQP
/// publisher confirms / Kafka acks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConfirmToken {
    pub delivery_tag: i64,
    pub confirmed: bool,
}

/// Outcome of an idempotent publish (CONCEPT:EG-KG.ingest.broker-reject-publish).
/// `confirmed` mirrors the EG-284 publisher-confirm (the exchange existed / the broker
/// accepted it); `duplicate` is `true` when a `(producer_id, seq)` stamp was recognised
/// as already-seen and the message was DROPPED (effectively-once — a duplicate still
/// confirms so the retrying publisher stops); `delivered` is the number of queues the
/// message was routed to (`0` for a duplicate or an unroutable/nacked publish).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdempotentPublish {
    pub confirmed: bool,
    pub duplicate: bool,
    pub delivered: usize,
}

/// A single message in a channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChannelMessage {
    pub sender: String,
    pub payload: String,
    pub timestamp: u64,
}

/// KG imprint created when a channel is closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChannelImprint {
    pub channel_id: String,
    pub channel_type: ChannelType,
    pub creator: String,
    pub participants: Vec<String>,
    pub message_count: usize,
    pub created_at: u64,
    pub closed_at: u64,
    pub summary_embedding: Option<Vec<f32>>,
    pub topic_metadata: Option<String>,
}

/// Result of `Method::CreateChannel`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChannelCreated {
    pub channel: String,
}

/// One channel visible to the caller, as `Method::ListChannels` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChannelSummary {
    pub id: String,
    #[serde(rename = "type")]
    pub channel_type: ChannelType,
    /// Number of members.
    pub members: usize,
}

/// How a leave or close ended when it produced no imprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChannelDepartureStatus {
    #[serde(rename = "left")]
    Left,
    #[serde(rename = "closed")]
    Closed,
}

/// Result of `Method::LeaveChannel` and `Method::CloseChannel`: the channel's imprint
/// when the call closed it, else a bare status string.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChannelDeparture {
    Closed(ChannelImprint),
    Status(ChannelDepartureStatus),
}

impl ChannelDeparture {
    /// The imprint when there is one, else `otherwise`.
    pub fn new(imprint: Option<ChannelImprint>, otherwise: ChannelDepartureStatus) -> Self {
        imprint.map_or(
            ChannelDeparture::Status(otherwise),
            ChannelDeparture::Closed,
        )
    }
}
