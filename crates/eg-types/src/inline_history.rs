//! Bounded completed-text continuation; not interrupted-run restoration.
mod native_claim;
pub use native_claim::NativeClaimWorkItemResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_INLINE_HISTORY_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_HISTORY_TURNS: usize = 128;
pub const INLINE_CHECKPOINT: &str = "inline_checkpoint";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CompletedTextAdmission {
    pub version: u32,
    pub user_text_digest: String,
}
impl CompletedTextAdmission {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || self.user_text_digest.len() != 64
            || !self
                .user_text_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid completed-text admission profile".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CompletedTextInput {
    pub version: u32,
    pub user_text: String,
    pub assistant_text: String,
    pub content_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TextMessage {
    pub role: String,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CompletedTextEnvelope {
    pub version: u32,
    pub phase: String,
    pub run_id: String,
    pub messages: Vec<TextMessage>,
}
impl CompletedTextInput {
    pub fn content(&self, run_id: &str) -> Result<String, String> {
        serde_json::to_string(&CompletedTextEnvelope {
            version: 1,
            phase: "completed".into(),
            run_id: run_id.into(),
            messages: vec![
                TextMessage {
                    role: "user".into(),
                    text: self.user_text.clone(),
                },
                TextMessage {
                    role: "assistant".into(),
                    text: self.assistant_text.clone(),
                },
            ],
        })
        .map_err(|e| e.to_string())
    }
    pub fn validate(&self, run_id: &str) -> Result<(), String> {
        if self.version != 1
            || run_id.trim().is_empty()
            || run_id.len() > 512
            || self.user_text.len() > MAX_INLINE_HISTORY_BYTES
            || self.assistant_text.len() > MAX_INLINE_HISTORY_BYTES
            || self
                .user_text
                .len()
                .saturating_add(self.assistant_text.len())
                > MAX_INLINE_HISTORY_BYTES
            || self.user_text.trim().is_empty()
            || self.assistant_text.trim().is_empty()
        {
            return Err("invalid completed-text checkpoint".into());
        }
        let content = self.content(run_id)?;
        if content.len() > MAX_INLINE_HISTORY_BYTES
            || self.content_digest != content_digest(&content)
        {
            return Err("invalid completed-text checkpoint".into());
        }
        Ok(())
    }
    pub fn checkpoint_ref(&self, work_item_id: &str) -> InlineCheckpointRef {
        InlineCheckpointRef {
            document_id: format!("inline-checkpoint:{work_item_id}"),
            revision: 1,
            content_digest: self.content_digest.clone(),
        }
    }
}
pub fn content_digest(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct InlineCheckpointRef {
    pub document_id: String,
    pub revision: u64,
    pub content_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunSelectionRequest {
    pub version: u32,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RunCheckpointSelection {
    pub version: u32,
    pub run_id: String,
    pub work_item_id: String,
    pub context_id: String,
    pub admission_revision: u64,
    pub scope: crate::MutationScopeIdentity,
    pub snapshot_version: u64,
    pub current_run_checkpoint: Option<String>,
    pub completed_history: Option<String>,
    pub checkpoints: Vec<InlineCheckpointRef>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConversationHeadView {
    pub version: u32,
    pub context_id: String,
    pub revision: u64,
    pub run_id: String,
    pub scope: crate::MutationScopeIdentity,
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_utf8_envelope_and_digest() {
        let mut input = CompletedTextInput {
            version: 1,
            user_text: "héllo".into(),
            assistant_text: "yes".into(),
            content_digest: String::new(),
        };
        let content = input.content("r").unwrap();
        assert_eq!(
            content,
            r#"{"version":1,"phase":"completed","run_id":"r","messages":[{"role":"user","text":"héllo"},{"role":"assistant","text":"yes"}]}"#
        );
        input.content_digest = content_digest(&content);
        input.validate("r").unwrap();
        assert!(input.validate("other-run").is_err());
        input.assistant_text = "x".repeat(MAX_INLINE_HISTORY_BYTES);
        assert!(input.validate("r").is_err());
    }
    #[test]
    fn cross_language_escaping_golden() {
        let mut input = CompletedTextInput {
            version: 1,
            user_text: "héllo \"quote\"\n/".into(),
            assistant_text: "紫 yes\\end".into(),
            content_digest: String::new(),
        };
        input.content_digest = content_digest(&input.content("r/1").unwrap());
        assert_eq!(
            input.content_digest,
            "5fe52081485464c774065089358c1881dfdc2ff6beabfeba40023e5b7c7ea563"
        );
        assert_eq!(
            content_digest(&input.user_text),
            "f6e9ed20f03bcdb43411d0c4b84f63b10957445776ce5952026e0a4c36ad0f52"
        );
        input.validate("r/1").unwrap();
    }
    #[test]
    fn admission_is_digest_only_and_closed() {
        let profile = CompletedTextAdmission {
            version: 1,
            user_text_digest: content_digest("private"),
        };
        profile.validate().unwrap();
        let wire = serde_json::to_string(&profile).unwrap();
        assert!(!wire.contains("private"));
        assert!(serde_json::from_str::<CompletedTextAdmission>(
            r#"{"version":1,"user_text":"secret"}"#
        )
        .is_err());
        assert!(CompletedTextAdmission {
            version: 2,
            ..profile
        }
        .validate()
        .is_err());
    }

    #[test]
    fn history_read_wire_opt_in_is_strict_and_defaults_off() {
        use crate::protocol::Method;
        let legacy = serde_json::json!({"method":"GetRunWorkItem", "params": {"tenant":"tenant", "run_id":"run"}});
        let method: Method = serde_json::from_value(legacy.clone()).unwrap();
        assert!(matches!(
            method,
            Method::GetRunWorkItem {
                completed_text_history: false,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(method).unwrap(), legacy);
        for bad in [
            serde_json::json!("true"),
            serde_json::json!(1),
            serde_json::Value::Null,
        ] {
            let mut value = legacy.clone();
            value["params"]["completed_text_history"] = bad;
            assert!(serde_json::from_value::<Method>(value).is_err());
        }
        let mut opted = legacy;
        opted["params"]["completed_text_history"] = true.into();
        let method: Method = serde_json::from_value(opted.clone()).unwrap();
        assert!(matches!(
            method,
            Method::GetRunWorkItem {
                completed_text_history: true,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(method).unwrap(), opted);
    }
}
