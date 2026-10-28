//! Ordinary admitted-run measurements, independent of delegated provenance.
use serde::{Deserialize, Serialize};

pub const RUN_COMPLETION_TOPIC: &str = "work_item.run_completion.v1";
pub const RUN_COMPLETION_BATCH: &str = "run_completion_batch";
pub const RUN_COMPLETION_DIGEST: &str = "run_completion_digest";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
}
fn validate_token_totals(usage: &RunUsage) -> Result<(), String> {
    if let Some(total) = usage.total_tokens {
        if usage.input_tokens.is_some_and(|n| n > total)
            || usage.output_tokens.is_some_and(|n| n > total)
            || matches!((usage.input_tokens, usage.output_tokens), (Some(i), Some(o)) if i + o != total)
        {
            return Err("incoherent run usage counters".into());
        }
    }
    Ok(())
}

impl RunUsage {
    pub fn validate(&self) -> Result<(), String> {
        if [
            self.requests,
            self.tool_calls,
            self.input_tokens,
            self.output_tokens,
            self.total_tokens,
        ]
        .into_iter()
        .flatten()
        .any(|n| n > 9_007_199_254_740_991)
        {
            return Err("run usage exceeds exact JSON integer range".into());
        }
        validate_token_totals(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCompletionInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_checkpoint: Option<Box<crate::inline_history::CompletedTextInput>>,
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<RunUsage>,
}
impl RunCompletionInput {
    pub fn receipt_input(&self) -> Self {
        let mut receipt = self.clone();
        receipt.completed_checkpoint = None;
        receipt
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 || self.run_id.trim().is_empty() || self.run_id.len() > 512 {
            return Err("invalid ordinary run completion version or run id".into());
        }
        if let Some(checkpoint) = &self.completed_checkpoint {
            checkpoint.validate(&self.run_id)?;
        }
        self.usage.as_ref().map_or(Ok(()), RunUsage::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCompletionPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_checkpoint: Option<crate::inline_history::InlineCheckpointRef>,
    pub completion: RunCompletionInput,
    pub tenant: String,
    pub work_item_id: String,
    pub batch_id: String,
    pub outcome: String,
    pub result_ref: Option<String>,
    pub error_ref: Option<String>,
}
impl RunCompletionPayload {
    pub fn from_method(
        method: &crate::protocol::Method,
        batch_id: &str,
    ) -> Result<Option<Self>, String> {
        let crate::protocol::Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            outcome,
            result_ref,
            error_ref,
            retryable,
            outcome_extension,
            run_completion: Some(completion),
            ..
        } = method
        else {
            return Ok(None);
        };
        completion.validate()?;
        if outcome_extension.is_some()
            || *retryable
            || !matches!(outcome.as_str(), "succeeded" | "failed" | "cancelled")
            || (outcome != "succeeded" && result_ref.is_some())
        {
            return Err("ordinary run completion requires an exclusive final result without failed answer artifacts".into());
        }
        if completion.completed_checkpoint.is_some() && outcome != "succeeded" {
            return Err("completed checkpoint requires successful terminal outcome".into());
        }
        Ok(Some(Self {
            completed_checkpoint: completion
                .completed_checkpoint
                .as_ref()
                .map(|c| c.checkpoint_ref(work_item_id)),
            completion: completion.receipt_input(),
            tenant: tenant.clone(),
            work_item_id: work_item_id.clone(),
            batch_id: batch_id.into(),
            outcome: outcome.clone(),
            result_ref: result_ref.clone(),
            error_ref: error_ref.clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CommittedRunCompletion {
    pub payload: RunCompletionPayload,
    pub scope: crate::MutationScopeIdentity,
    pub commit_sequence: u64,
    pub ordinal: u32,
    pub payload_digest: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_usage_is_not_zero() {
        let value: RunCompletionInput =
            serde_json::from_str(r#"{"schema_version":1,"run_id":"run","usage":{}}"#).unwrap();
        assert_eq!(
            serde_json::to_value(value).unwrap()["usage"],
            serde_json::json!({})
        );
    }
    #[test]
    fn reject_invalid_measurements() {
        for raw in [
            r#"{"requests":true}"#,
            r#"{"requests":-1}"#,
            r#"{"requests":1.5}"#,
            r#"{"cost":1}"#,
        ] {
            assert!(serde_json::from_str::<RunUsage>(raw).is_err());
        }
        for raw in [
            r#"{"requests":9007199254740992}"#,
            r#"{"input_tokens":5,"output_tokens":2,"total_tokens":6}"#,
            r#"{"input_tokens":5,"total_tokens":4}"#,
        ] {
            assert!(serde_json::from_str::<RunUsage>(raw)
                .unwrap()
                .validate()
                .is_err());
        }
    }
}
