//! Owner-bound lifecycle payloads; the mutation outbox contains references only.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const RUN_STREAM_TOPIC: &str = "engine.run.event.v1";
pub const MAX_RUN_EVENT_BYTES: usize = 65_536;
pub const MAX_RUN_EVENT_PAGE: u16 = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RunStreamKind {
    TextDelta,
    ToolCall,
    ToolResult,
    State,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunStreamPayload {
    pub kind: RunStreamKind,
    /// Classified JSON, sealed in native private storage; never an outbox body.
    pub content: String,
    pub payload_digest: String,
}
impl RunStreamPayload {
    pub fn validate(&self) -> Result<(), String> {
        if self.content.is_empty() || self.content.len() > MAX_RUN_EVENT_BYTES {
            return Err("run event content must be 1..65536 bytes".into());
        }
        if !serde_json::from_str::<serde_json::Value>(&self.content)
            .map_err(|_| "run event content must be JSON")?
            .is_object()
        {
            return Err("run event content must be a JSON object".into());
        }
        if self.payload_digest != hex::encode(Sha256::digest(self.content.as_bytes())) {
            return Err("run event payload digest mismatch".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AppendRunEventRequest {
    pub tenant: String,
    pub work_item_id: String,
    pub run_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    /// Last accepted native index, zero for the first event.
    pub expected_index: u64,
    pub event: RunStreamPayload,
    pub idempotency_key: String,
}
impl AppendRunEventRequest {
    pub fn validate(&self) -> Result<(), String> {
        for value in [
            &self.tenant,
            &self.work_item_id,
            &self.run_id,
            &self.worker_id,
            &self.idempotency_key,
        ] {
            bounded_id(value)?;
        }
        self.expected_index
            .checked_add(1)
            .ok_or("run event index exhausted")?;
        self.event.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GetRunEventsRequest {
    pub tenant: String,
    pub work_item_id: String,
    pub run_id: String,
    #[serde(default)]
    pub after_cursor: Option<String>,
    pub limit: u16,
}
impl GetRunEventsRequest {
    pub fn validate(&self) -> Result<(), String> {
        for value in [&self.tenant, &self.work_item_id, &self.run_id] {
            bounded_id(value)?;
        }
        if self.limit == 0 || self.limit > MAX_RUN_EVENT_PAGE {
            return Err("run event limit must be 1..128".into());
        }
        if let Some(cursor) = &self.after_cursor {
            cursor_index(cursor)?;
        }
        Ok(())
    }
}
fn bounded_id(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        Err("run event identifiers must be bounded nonempty text".into())
    } else {
        Ok(())
    }
}
/// Only locates a candidate. The reader MUST compare the full freshly derived cursor.
pub fn cursor_index(cursor: &str) -> Result<u64, String> {
    let parts: Vec<_> = cursor.split(':').collect();
    if parts.len() != 3
        || parts[0] != "eg-run-v1"
        || parts[2].len() != 64
        || !parts[2]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid run event cursor".into());
    }
    let index = parts[1]
        .parse::<u64>()
        .map_err(|_| "invalid run event cursor")?;
    if index == 0 || parts[1] != index.to_string() {
        return Err("invalid run event cursor".into());
    }
    Ok(index)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunEventAppended {
    pub work_item_id: String,
    pub run_id: String,
    pub event_index: u64,
    pub payload_digest: String,
    pub replayed: bool,
    pub changed_work_item_ids: Vec<String>,
}

/// Safe intent in the one existing mutation outbox; no payload or lease secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunEventReference {
    pub work_item_id: String,
    pub run_id: String,
    pub event_index: u64,
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CommittedRunStreamEvent {
    pub event_index: u64,
    pub commit_sequence: u64,
    pub ordinal: u32,
    pub cursor: String,
    pub event: RunStreamPayload,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunEventPage {
    pub work_item: crate::work_item_read::WorkItemView,
    pub events: Vec<CommittedRunStreamEvent>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// Current tail, to use as expected_index on the next append.
    pub next_index: u64,
}

#[cfg(test)]
mod tests;
