//! Private, owner-bound run checkpoints committed with the native WorkItem fence.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_CHECKPOINT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CheckpointMemoryRef {
    pub document_id: String,
    pub revision: u64,
    pub digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CheckpointContent {
    pub content: String,
    pub payload_digest: String,
    pub short_term: Vec<CheckpointMemoryRef>,
    pub long_term: Vec<CheckpointMemoryRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CheckpointClaim {
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PublishRunCheckpointRequest {
    pub tenant: String,
    pub work_item_id: String,
    pub run_id: String,
    pub context_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub expected_revision: Option<u64>,
    pub snapshot: CheckpointContent,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GetRunCheckpointRequest {
    pub tenant: String,
    pub work_item_id: String,
    pub run_id: String,
    pub context_id: String,
    #[serde(default)]
    pub current_claim: Option<CheckpointClaim>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCheckpointPublished {
    pub changed_work_item_ids: Vec<String>,
    pub replayed: bool,
    pub work_item_id: String,
    pub run_id: String,
    pub context_id: String,
    pub revision: u64,
    pub payload_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCheckpointSnapshot {
    pub revision: u64,
    pub content: String,
    pub payload_digest: String,
    pub short_term: Vec<CheckpointMemoryRef>,
    pub long_term: Vec<CheckpointMemoryRef>,
}

impl CheckpointContent {
    pub fn validate(&self) -> Result<(), String> {
        if self.content.is_empty() || self.content.len() > MAX_CHECKPOINT_BYTES {
            return Err("checkpoint content is empty or exceeds one MiB".into());
        }
        if hex::encode(Sha256::digest(self.content.as_bytes())) != self.payload_digest {
            return Err("checkpoint content digest mismatch".into());
        }
        let json: serde_json::Value = serde_json::from_str(&self.content)
            .map_err(|_| "checkpoint content must be UTF-8 JSON".to_string())?;
        if !json.is_object() {
            return Err("checkpoint content must be a JSON object".into());
        }
        for reference in self.short_term.iter().chain(self.long_term.iter()) {
            bounded_id(&reference.document_id)?;
            if reference.digest.len() != 64
                || !reference
                    .digest
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            {
                return Err("checkpoint memory reference digest is invalid".into());
            }
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MAX_CHECKPOINT_BYTES {
            return Err("checkpoint snapshot exceeds one MiB".into());
        }
        Ok(())
    }
}

impl PublishRunCheckpointRequest {
    pub fn validate(&self) -> Result<(), String> {
        for id in [
            &self.tenant,
            &self.work_item_id,
            &self.run_id,
            &self.context_id,
            &self.worker_id,
            &self.idempotency_key,
        ] {
            bounded_id(id)?;
        }
        self.snapshot.validate()
    }
}

impl GetRunCheckpointRequest {
    pub fn validate(&self) -> Result<(), String> {
        for id in [
            &self.tenant,
            &self.work_item_id,
            &self.run_id,
            &self.context_id,
        ] {
            bounded_id(id)?;
        }
        if let Some(claim) = &self.current_claim {
            bounded_id(&claim.worker_id)?;
        }
        Ok(())
    }
}

fn bounded_id(id: &str) -> Result<(), String> {
    if id.trim().is_empty() || id.len() > 512 {
        return Err("checkpoint identifier is empty or oversized".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Owner-authorized checkpoint and WorkItem projection from one native read snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCheckpointRead {
    pub context_id: String,
    pub work_item: crate::work_item_read::WorkItemView,
    #[serde(flatten)]
    pub snapshot: RunCheckpointSnapshot,
}
