//! Engine-native, governed unit of external change.
//!
//! [`ChangeEnvelope`] embeds the universal [`MutationBatch`] rather than defining
//! another graph-mutation language. Its additional rows are the source material
//! that must share the batch commit point: blobs, features, located evidence,
//! policy, lineage, content version, outbox, and a typed cursor.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::mutation_batch::MutationBatch;

pub const CHANGE_ENVELOPE_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CursorPosition {
    Sequence(u64),
    TimestampMillis(i64),
    /// Provider-defined cursors are never ordered lexically.
    Opaque {
        /// Stable provider cursor type (for example `page_token_v2`).
        cursor_type: String,
        value: String,
    },
}

impl CursorPosition {
    pub fn advances(&self, previous: &Self) -> bool {
        match (self, previous) {
            (Self::Sequence(next), Self::Sequence(prior)) => next > prior,
            (Self::TimestampMillis(next), Self::TimestampMillis(prior)) => next > prior,
            (
                Self::Opaque {
                    cursor_type: next_type,
                    value: next,
                },
                Self::Opaque {
                    cursor_type: prior_type,
                    value: prior,
                },
            ) => next_type == prior_type && !next.is_empty() && next != prior,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeCursor {
    pub source: String,
    #[serde(default)]
    pub partition: String,
    pub position: CursorPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_previous: Option<CursorPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentVersion {
    pub object_id: String,
    pub digest_algorithm: String,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_digest: Option<String>,
    /// Typed source-side version. It is never compared lexically.
    pub source_version: ContentVersionPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContentVersionPosition {
    Sequence(u64),
    TimestampMillis(i64),
    Opaque { version_type: String, value: String },
}

impl ContentVersionPosition {
    pub fn advances(&self, previous: &Self) -> bool {
        match (self, previous) {
            (Self::Sequence(next), Self::Sequence(prior)) => next > prior,
            (Self::TimestampMillis(next), Self::TimestampMillis(prior)) => next > prior,
            (
                Self::Opaque {
                    version_type: next_type,
                    value: next,
                },
                Self::Opaque {
                    version_type: prior_type,
                    value: prior,
                },
            ) => next_type == prior_type && !next.is_empty() && next != prior,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterialOperation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobReference {
    pub blob_id: String,
    pub operation: MaterialOperation,
    pub digest_algorithm: String,
    pub digest: String,
    pub media_type: String,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeatureRecord {
    pub feature_id: String,
    pub operation: MaterialOperation,
    pub object_id: String,
    pub kind: String,
    #[serde(with = "serde_bytes")]
    pub value_msgpack: Vec<u8>,
    pub model_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub evidence_id: String,
    pub operation: MaterialOperation,
    pub object_id: String,
    pub modality: String,
    #[serde(with = "serde_bytes")]
    pub locus_msgpack: Vec<u8>,
    pub content_digest: String,
}

/// Policy rows carry an opaque subject-set digest, never raw principals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRecord {
    pub policy_id: String,
    pub operation: MaterialOperation,
    pub object_id: String,
    pub tenant: String,
    pub classification: String,
    pub policy_version: String,
    pub subject_set_digest: String,
    #[serde(default)]
    pub retention_policy: String,
    #[serde(default)]
    pub legal_hold: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageRecord {
    pub lineage_id: String,
    pub operation: MaterialOperation,
    pub object_id: String,
    pub source_artifact_digest: String,
    pub transform_name: String,
    pub transform_version: String,
    pub parent_content_digests: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivacyAttestation {
    pub policy_version: String,
    pub sanitizer_version: String,
    pub sanitized_payload_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEnvelope {
    pub schema_version: u16,
    pub envelope_id: String,
    pub mutation: MutationBatch,
    pub content_version: ContentVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ChangeCursor>,
    #[serde(default)]
    pub blobs: Vec<BlobReference>,
    #[serde(default)]
    pub features: Vec<FeatureRecord>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRecord>,
    #[serde(default)]
    pub policies: Vec<PolicyRecord>,
    #[serde(default)]
    pub lineage: Vec<LineageRecord>,
    pub privacy: PrivacyAttestation,
}

impl ChangeEnvelope {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CHANGE_ENVELOPE_VERSION {
            return Err(format!(
                "unsupported ChangeEnvelope version {} (expected {})",
                self.schema_version, CHANGE_ENVELOPE_VERSION
            ));
        }
        self.mutation.validate()?;
        if self.envelope_id.trim().is_empty() {
            return Err("change envelope_id must not be empty".to_string());
        }
        for value in [
            self.envelope_id.as_str(),
            self.mutation.batch_id.as_str(),
            self.mutation.tenant.as_str(),
            self.mutation.graph.as_str(),
            self.mutation.idempotency_key.as_str(),
        ] {
            validate_safe_text(value)?;
        }
        let principal_digest = self
            .mutation
            .context
            .principal
            .strip_prefix("principal:sha256:")
            .ok_or_else(|| "durable mutation principal must be an opaque sha256 id".to_string())?;
        validate_digest("sha256", principal_digest)?;
        for optional in [
            self.mutation.context.purpose.as_deref(),
            self.mutation.context.policy_fingerprint.as_deref(),
            self.mutation.context.trace_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_safe_text(optional)?;
        }
        for intent in &self.mutation.outbox {
            validate_safe_text(&intent.topic)?;
            validate_safe_text(&intent.key)?;
            for (key, value) in &intent.headers {
                validate_safe_text(key)?;
                validate_safe_text(value)?;
            }
            validate_msgpack_privacy(&intent.payload)?;
        }
        if self.content_version.object_id.trim().is_empty() {
            return Err("change content object_id must not be empty".to_string());
        }
        validate_safe_text(&self.content_version.object_id)?;
        validate_digest(
            &self.content_version.digest_algorithm,
            &self.content_version.digest,
        )?;
        if let Some(previous) = &self.content_version.previous_digest {
            validate_digest(&self.content_version.digest_algorithm, previous)?;
            if previous == &self.content_version.digest {
                return Err("content version cannot replace itself".to_string());
            }
        }
        if self.privacy.policy_version.trim().is_empty()
            || self.privacy.sanitizer_version.trim().is_empty()
        {
            return Err("privacy policy and sanitizer versions are required".to_string());
        }
        validate_digest("sha256", &self.privacy.sanitized_payload_digest)?;
        if let Some(cursor) = &self.cursor {
            if cursor.source.trim().is_empty() {
                return Err("cursor source must not be empty".to_string());
            }
            validate_safe_text(&cursor.source)?;
            validate_safe_text(&cursor.partition)?;
            if let CursorPosition::Opaque { cursor_type, value } = &cursor.position {
                validate_safe_text(cursor_type)?;
                validate_safe_text(value)?;
            }
        }
        match &self.content_version.source_version {
            ContentVersionPosition::Opaque {
                version_type,
                value,
            } => {
                validate_safe_text(version_type)?;
                validate_safe_text(value)?;
            }
            ContentVersionPosition::Sequence(_) | ContentVersionPosition::TimestampMillis(_) => {}
        }
        // Governance proof is not optional: every materialized object's ACL row
        // must be present in the same envelope and therefore the same commit.
        let mut governed = BTreeSet::new();
        for policy in &self.policies {
            if policy.tenant != self.mutation.tenant {
                return Err("policy tenant does not match mutation tenant".to_string());
            }
            if policy.object_id.trim().is_empty()
                || policy.classification.trim().is_empty()
                || policy.policy_version.trim().is_empty()
            {
                return Err(
                    "policy object, classification, and policy version are required".to_string(),
                );
            }
            validate_safe_text(&policy.object_id)?;
            validate_safe_text(&policy.policy_id)?;
            validate_safe_text(&policy.classification)?;
            validate_safe_text(&policy.policy_version)?;
            validate_safe_text(&policy.retention_policy)?;
            validate_digest("sha256", &policy.subject_set_digest)?;
            governed.insert(policy.object_id.as_str());
        }
        let mut required_governance = BTreeSet::from([self.content_version.object_id.as_str()]);
        for blob in &self.blobs {
            if blob.blob_id.trim().is_empty() || blob.media_type.trim().is_empty() {
                return Err("blob identity and media type are required".to_string());
            }
            validate_safe_text(&blob.blob_id)?;
            validate_safe_text(&blob.media_type)?;
            validate_digest(&blob.digest_algorithm, &blob.digest)?;
        }
        for evidence in &self.evidence {
            validate_safe_text(&evidence.evidence_id)?;
            validate_safe_text(&evidence.object_id)?;
            validate_safe_text(&evidence.modality)?;
            required_governance.insert(evidence.object_id.as_str());
            validate_digest("sha256", &evidence.content_digest)?;
            validate_msgpack_privacy(&evidence.locus_msgpack)?;
        }
        for feature in &self.features {
            validate_safe_text(&feature.feature_id)?;
            validate_safe_text(&feature.object_id)?;
            validate_safe_text(&feature.kind)?;
            validate_safe_text(&feature.model_version)?;
            required_governance.insert(feature.object_id.as_str());
            validate_msgpack_privacy(&feature.value_msgpack)?;
        }
        for lineage in &self.lineage {
            validate_safe_text(&lineage.lineage_id)?;
            validate_safe_text(&lineage.object_id)?;
            validate_safe_text(&lineage.transform_name)?;
            validate_safe_text(&lineage.transform_version)?;
            required_governance.insert(lineage.object_id.as_str());
            validate_digest("sha256", &lineage.source_artifact_digest)?;
            for digest in &lineage.parent_content_digests {
                validate_digest("sha256", digest)?;
            }
        }
        if let Some(missing) = required_governance.difference(&governed).next() {
            return Err(format!(
                "ChangeEnvelope has no policy proof for material object '{missing}'"
            ));
        }
        for operation in &self.mutation.operations {
            match &operation.method {
                crate::protocol::Method::AddNode {
                    node_id,
                    properties_msgpack,
                } => {
                    validate_safe_text(node_id)?;
                    validate_msgpack_privacy(properties_msgpack)?;
                }
                crate::protocol::Method::AddEdge {
                    source_id,
                    target_id,
                    properties_msgpack,
                } => {
                    validate_safe_text(source_id)?;
                    validate_safe_text(target_id)?;
                    validate_msgpack_privacy(properties_msgpack)?;
                }
                crate::protocol::Method::RemoveNode { node_id } => {
                    validate_safe_text(node_id)?;
                }
                crate::protocol::Method::CompareAndSetNodeFields {
                    node_id,
                    conditions_msgpack,
                    updates_msgpack,
                } => {
                    validate_safe_text(node_id)?;
                    validate_msgpack_privacy(conditions_msgpack)?;
                    validate_msgpack_privacy(updates_msgpack)?;
                }
                crate::protocol::Method::RemoveEdge {
                    source_id,
                    target_id,
                } => {
                    validate_safe_text(source_id)?;
                    validate_safe_text(target_id)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn validate_digest(algorithm: &str, digest: &str) -> Result<(), String> {
    if !algorithm.eq_ignore_ascii_case("sha256") {
        return Err("only sha256 content digests are supported".to_string());
    }
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("content digest must be 64 hexadecimal characters".to_string());
    }
    Ok(())
}

fn validate_safe_text(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();
    if value.contains('@')
        || value.contains('\\')
        || lower.contains("/home/")
        || lower.contains("/users/")
        || lower.contains("/mnt/")
        || lower.contains("file://")
    {
        return Err("persistence privacy policy rejected inline text".to_string());
    }
    Ok(())
}

fn validate_json_privacy(value: &serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::String(text) => validate_safe_text(text),
        serde_json::Value::Array(values) => {
            for value in values {
                validate_json_privacy(value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                validate_safe_text(key)?;
                validate_json_privacy(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_msgpack_privacy(bytes: &[u8]) -> Result<(), String> {
    crate::msgpack::validate_single_value(
        bytes,
        crate::msgpack::MsgpackLimits::new(8 * 1024 * 1024, 200_000, 64),
    )
    .map_err(|_| "inline material must be bounded valid MessagePack JSON".to_string())?;
    let value: serde_json::Value = rmp_serde::from_slice(bytes)
        .map_err(|_| "inline material must be valid MessagePack JSON".to_string())?;
    validate_json_privacy(&value)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEnvelopeRecord {
    pub envelope: ChangeEnvelope,
    pub committed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEnvelopeCommit {
    pub envelope_id: String,
    pub batch_id: String,
    pub content_version: ContentVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ChangeCursor>,
    pub outbox_count: u32,
    pub replayed: bool,
}

pub fn compare_cursor_positions(left: &CursorPosition, right: &CursorPosition) -> Option<Ordering> {
    match (left, right) {
        (CursorPosition::Sequence(a), CursorPosition::Sequence(b)) => Some(a.cmp(b)),
        (CursorPosition::TimestampMillis(a), CursorPosition::TimestampMillis(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_never_lexically_orders_opaque_values() {
        let previous = CursorPosition::Opaque {
            cursor_type: "page_token_v2".into(),
            value: "cursor-9".into(),
        };
        let next = CursorPosition::Opaque {
            cursor_type: "page_token_v2".into(),
            value: "cursor-10".into(),
        };
        assert_eq!(compare_cursor_positions(&next, &previous), None);
        assert!(next.advances(&previous));
    }

    #[test]
    fn privacy_scan_rejects_machine_paths() {
        let bytes =
            rmp_serde::to_vec_named(&serde_json::json!({"path": "/home/person/file"})).unwrap();
        assert!(validate_msgpack_privacy(&bytes).is_err());
    }

    #[test]
    fn privacy_scan_rejects_declared_messagepack_allocation_bombs() {
        assert!(validate_msgpack_privacy(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
