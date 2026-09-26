//! EH-590: bounded, durable input-required exchange for native WorkItems.
//!
//! Only references and a sanitized preview enter the WorkItem row. The
//! operation parameters and approval body remain in the caller's private store.

use serde::{Deserialize, Serialize};

pub const MAX_INPUT_ID_BYTES: usize = 512;
pub const MAX_INPUT_PREVIEW_BYTES: usize = 2_048;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RequestWorkItemInput {
    pub tenant: String,
    pub work_item_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub expected_version: u64,
    pub call_id: String,
    pub plan_ref: String,
    pub op: String,
    pub params_digest: String,
    pub preview: String,
    pub expires_at_ms: u64,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum InputDecision {
    Approve,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnswerWorkItemInput {
    pub tenant: String,
    pub work_item_id: String,
    pub expected_version: u64,
    pub call_id: String,
    pub plan_ref: String,
    pub op: String,
    pub params_digest: String,
    pub decision: InputDecision,
    /// Opaque private answer receipt; no approval body or operation parameters.
    pub answer_ref: String,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemPendingInput {
    pub work_item_id: String,
    pub version: u64,
    pub call_id: String,
    pub plan_ref: String,
    pub op: String,
    pub params_digest: String,
    pub preview: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemInputAnswer {
    pub work_item_id: String,
    pub version: u64,
    pub call_id: String,
    pub plan_ref: String,
    pub op: String,
    pub params_digest: String,
    pub decision: InputDecision,
    pub answer_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemInputOutcome {
    Applied,
    Missing,
    Fenced,
    Conflict,
    Expired,
    Denied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemInputTransition {
    pub outcome: WorkItemInputOutcome,
    pub work_item_id: String,
    pub version: Option<u64>,
    pub changed_work_item_ids: Vec<String>,
}

pub fn validate_id(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_INPUT_ID_BYTES {
        return Err(format!("{field} is outside native pending-input bounds"));
    }
    Ok(())
}

pub fn validate_digest(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("params_digest must be a lowercase SHA-256 hex value".into());
    }
    Ok(())
}

impl RequestWorkItemInput {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("tenant", self.tenant.as_str()),
            ("work_item_id", self.work_item_id.as_str()),
            ("worker_id", self.worker_id.as_str()),
            ("call_id", self.call_id.as_str()),
            ("plan_ref", self.plan_ref.as_str()),
            ("op", self.op.as_str()),
            ("idempotency_key", self.idempotency_key.as_str()),
        ] {
            validate_id(field, value)?;
        }
        validate_digest(&self.params_digest)?;
        if self.preview.len() > MAX_INPUT_PREVIEW_BYTES || self.expires_at_ms <= self.now_ms {
            return Err("pending-input preview or expiry is outside native bounds".into());
        }
        Ok(())
    }
}

impl AnswerWorkItemInput {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("tenant", self.tenant.as_str()),
            ("work_item_id", self.work_item_id.as_str()),
            ("call_id", self.call_id.as_str()),
            ("plan_ref", self.plan_ref.as_str()),
            ("op", self.op.as_str()),
            ("answer_ref", self.answer_ref.as_str()),
            ("idempotency_key", self.idempotency_key.as_str()),
        ] {
            validate_id(field, value)?;
        }
        validate_digest(&self.params_digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> RequestWorkItemInput {
        RequestWorkItemInput {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            worker_id: "worker-1".into(),
            lease_epoch: 2,
            fencing_token: 2,
            expected_version: 3,
            call_id: "call-1".into(),
            plan_ref: "plan-1".into(),
            op: "graphos.foo".into(),
            params_digest: "a".repeat(64),
            preview: "Change one item".into(),
            expires_at_ms: 2_000,
            idempotency_key: "request-1".into(),
            now_ms: 1_000,
        }
    }

    #[test]
    fn pending_request_bounds_refs_digest_preview_and_ttl() {
        let mut value = request();
        value.validate().unwrap();
        value.params_digest = "A".repeat(64);
        assert!(value.validate().is_err());
        value.params_digest = "a".repeat(64);
        value.preview = "x".repeat(MAX_INPUT_PREVIEW_BYTES + 1);
        assert!(value.validate().is_err());
        value.preview.clear();
        value.expires_at_ms = value.now_ms;
        assert!(value.validate().is_err());
    }

    #[test]
    fn answer_requires_an_opaque_receipt_and_exact_digest() {
        let mut value = AnswerWorkItemInput {
            tenant: "tenant-a".into(),
            work_item_id: "work-1".into(),
            expected_version: 4,
            call_id: "call-1".into(),
            plan_ref: "plan-1".into(),
            op: "graphos.foo".into(),
            params_digest: "b".repeat(64),
            decision: InputDecision::Deny,
            answer_ref: "receipt-1".into(),
            idempotency_key: "answer-1".into(),
            now_ms: 1_500,
        };
        value.validate().unwrap();
        value.answer_ref.clear();
        assert!(value.validate().is_err());
    }
}
