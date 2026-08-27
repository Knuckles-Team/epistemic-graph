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

/// Server cap on the number of envelopes one `ApplyChangeEnvelopes` batch may carry.
/// A larger batch is rejected with a typed error before any transaction opens. The
/// per-envelope nested-MessagePack privacy/size limits still apply individually to
/// every envelope. Lives in `eg-types` so both the request boundary and the durable
/// commit kernel share ONE bound.
pub const MAX_ENVELOPES_PER_BATCH: usize = 1024;

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
    /// GOC-03 — the [`crate::commit_descriptor::CommitDescriptorV1::commit_seq`]
    /// this envelope's mutation was published under, once the durable
    /// commit-descriptor index is wired into the commit path (GOC-03-W03/W05).
    /// Additive and optional so an envelope predating that wiring, or one whose
    /// surface does not yet register a commit-descriptor participant, remains
    /// valid; a populated value MUST be paired with `commit_descriptor_ref`
    /// (see `validate`) and must match a real descriptor recorded by the commit
    /// index — this module does not itself construct or verify that pairing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_seq: Option<u64>,
    /// Opaque `CommitDescriptorV1::commit_id` this envelope's mutation was
    /// published under. See `commit_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_descriptor_ref: Option<String>,
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
        match (self.commit_seq, &self.commit_descriptor_ref) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(
                    "commit_seq and commit_descriptor_ref must be set together or both absent"
                        .to_string(),
                );
            }
            (Some(0), Some(_)) => {
                return Err("commit_seq must be non-zero when present".to_string());
            }
            (Some(_), Some(commit_descriptor_ref)) => {
                validate_safe_text(commit_descriptor_ref)?;
            }
            (None, None) => {}
        }
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

    fn minimal_envelope() -> ChangeEnvelope {
        let mutation = crate::mutation_batch::MutationBatch {
            schema_version: crate::mutation_batch::MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            context: crate::mutation_batch::MutationRequestContext {
                request_id: 1,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "tenant-a".into(),
            graph: "graph-a".into(),
            placement_epoch: 0,
            idempotency_key: "idem-1".into(),
            expected_graph_version: None,
            fencing_token: None,
            authoritative_state: None,
            operations: vec![crate::mutation_batch::MutationOperation {
                ordinal: 0,
                surface: crate::mutation_batch::MutationSurface::Graph,
                domain: crate::mutation_batch::MutationDomain::GraphRows,
                method: crate::protocol::Method::AddNode {
                    node_id: "n1".into(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": 1}))
                        .unwrap(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 0,
        };
        ChangeEnvelope {
            schema_version: CHANGE_ENVELOPE_VERSION,
            envelope_id: "envelope-1".into(),
            mutation,
            content_version: ContentVersion {
                object_id: "object-1".into(),
                digest_algorithm: "sha256".into(),
                digest: "a".repeat(64),
                previous_digest: None,
                source_version: ContentVersionPosition::Sequence(1),
            },
            cursor: None,
            blobs: Vec::new(),
            features: Vec::new(),
            evidence: Vec::new(),
            // `validate()` ALWAYS seeds `required_governance` with
            // `content_version.object_id`, and `governed` is filled only from
            // `policies` — so an envelope with no policy record can never
            // validate, regardless of how minimal it is otherwise. An empty
            // `policies` here made this fixture unconstructible-as-valid and
            // every test calling `.validate()` on it failed with
            // "no policy proof for material object 'object-1'". "Minimal" must
            // mean minimal-and-valid, or it proves nothing about the field
            // under test.
            policies: vec![PolicyRecord {
                policy_id: "policy-1".into(),
                operation: MaterialOperation::Upsert,
                object_id: "object-1".into(),
                tenant: "tenant-a".into(),
                classification: "internal".into(),
                policy_version: "policy-v1".into(),
                subject_set_digest: "c".repeat(64),
                retention_policy: "default".into(),
                legal_hold: false,
            }],
            lineage: Vec::new(),
            privacy: PrivacyAttestation {
                policy_version: "privacy-v1".into(),
                sanitizer_version: "sanitizer-v1".into(),
                sanitized_payload_digest: "b".repeat(64),
            },
            commit_seq: None,
            commit_descriptor_ref: None,
        }
    }

    #[test]
    fn commit_seq_defaults_absent_and_is_valid() {
        let envelope = minimal_envelope();
        envelope.validate().unwrap();
        assert!(envelope.commit_seq.is_none());
        assert!(envelope.commit_descriptor_ref.is_none());
        // Additive field: an encoding predating GOC-03 (no key present at all)
        // still decodes because of `#[serde(default)]`.
        let mut json = serde_json::to_value(&envelope).unwrap();
        let obj = json.as_object_mut().unwrap();
        assert!(
            !obj.contains_key("commit_seq"),
            "an absent commit_seq must not be serialized (skip_serializing_if)"
        );
        obj.remove("commit_seq");
        obj.remove("commit_descriptor_ref");
        let decoded: ChangeEnvelope = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.commit_seq, None);
    }

    #[test]
    fn commit_seq_and_descriptor_ref_must_be_set_together() {
        let mut only_seq = minimal_envelope();
        only_seq.commit_seq = Some(5);
        only_seq.commit_descriptor_ref = None;
        assert!(only_seq.validate().unwrap_err().contains("set together"));

        let mut only_ref = minimal_envelope();
        only_ref.commit_seq = None;
        only_ref.commit_descriptor_ref = Some("commit-1".into());
        assert!(only_ref.validate().unwrap_err().contains("set together"));

        let mut zero_seq = minimal_envelope();
        zero_seq.commit_seq = Some(0);
        zero_seq.commit_descriptor_ref = Some("commit-1".into());
        assert!(zero_seq.validate().unwrap_err().contains("non-zero"));

        let mut both = minimal_envelope();
        both.commit_seq = Some(5);
        both.commit_descriptor_ref = Some("commit-1".into());
        both.validate().unwrap();
    }

    // CXA-EG-03 characterization: `validate()` (CCN 90) pins every branch group
    // below against the UNMODIFIED function ahead of decomposing it into named
    // helpers. `minimal_envelope()` is a valid baseline, so each test mutates
    // exactly one field/collection to trip exactly one Err branch, proving the
    // assertion actually depends on that branch (each was confirmed to FAIL
    // against a deliberately broken helper during the refactor step, not just
    // shown green here).

    #[test]
    fn schema_version_mismatch_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.schema_version = CHANGE_ENVELOPE_VERSION + 1;
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("unsupported ChangeEnvelope version"), "got: {err}");
    }

    #[test]
    fn empty_envelope_id_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.envelope_id = String::new();
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("envelope_id must not be empty"), "got: {err}");
    }

    #[test]
    fn unsafe_text_in_core_mutation_field_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.tenant = "person@example.invalid".into();
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    // NOTE (CXA-EG-03 finding, not a test): `ChangeEnvelope::validate`'s own
    // principal/digest re-check (the `strip_prefix("principal:sha256:")` +
    // `validate_digest` pair right after `self.mutation.validate()?`) is
    // unreachable in practice. `self.mutation.validate()?` runs FIRST and
    // already enforces `principal:sha256:` + exactly 64 LOWERCASE hex chars
    // (`crates/eg-types/src/mutation_batch.rs:243` validate, principal check
    // ~line 255) -- a strictly narrower acceptance set than this function's
    // own `is_ascii_hexdigit()`-based recheck (which also accepts uppercase
    // A-F). Any principal that clears the first gate therefore always clears
    // the second; no input can trigger this function's own principal Err
    // branches. Confirmed by attempting exactly that: both
    // `principal:sha256:` missing and a too-short digest are caught by
    // `mutation.validate()` first (error text "mutation principal authority
    // must be an opaque digest"), never by this function's own
    // "durable mutation principal must be an opaque sha256 id" /
    // "content digest must be 64 hexadecimal characters" branches. See
    // BUGS FOUND in the lane report. Verbatim-moved during the refactor
    // below, not deleted (not "genuinely dead" per the 5-evidence-check bar,
    // and behaviour-preservation forbids touching it in this lane anyway).

    #[test]
    fn unsafe_optional_context_field_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.context.purpose = Some("/home/person/notes".into());
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn outbox_intent_unsafe_topic_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.outbox.push(crate::mutation_batch::MutationOutboxIntent {
            topic: "topic@bad".into(),
            key: "key-1".into(),
            payload: rmp_serde::to_vec_named(&serde_json::json!({"a": 1})).unwrap(),
            headers: Default::default(),
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn outbox_intent_privacy_violating_payload_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.outbox.push(crate::mutation_batch::MutationOutboxIntent {
            topic: "topic-1".into(),
            key: "key-1".into(),
            payload: rmp_serde::to_vec_named(&serde_json::json!({"path": "/home/person/x"}))
                .unwrap(),
            headers: Default::default(),
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn empty_content_object_id_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.content_version.object_id = String::new();
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("content object_id must not be empty"), "got: {err}");
    }

    #[test]
    fn malformed_content_digest_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.content_version.digest = "not-hex".into();
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("64 hexadecimal"), "got: {err}");
    }

    #[test]
    fn content_version_cannot_replace_itself() {
        let mut envelope = minimal_envelope();
        let digest = envelope.content_version.digest.clone();
        envelope.content_version.previous_digest = Some(digest);
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("cannot replace itself"), "got: {err}");
    }

    #[test]
    fn empty_privacy_versions_are_rejected() {
        let mut envelope = minimal_envelope();
        envelope.privacy.policy_version = String::new();
        let err = envelope.validate().unwrap_err();
        assert!(
            err.contains("privacy policy and sanitizer versions are required"),
            "got: {err}"
        );
    }

    #[test]
    fn malformed_privacy_digest_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.privacy.sanitized_payload_digest = "short".into();
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("64 hexadecimal"), "got: {err}");
    }

    #[test]
    fn cursor_with_empty_source_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.cursor = Some(ChangeCursor {
            source: String::new(),
            partition: "p0".into(),
            position: CursorPosition::Sequence(1),
            expected_previous: None,
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("cursor source must not be empty"), "got: {err}");
    }

    #[test]
    fn cursor_opaque_unsafe_text_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.cursor = Some(ChangeCursor {
            source: "src-1".into(),
            partition: "p0".into(),
            position: CursorPosition::Opaque {
                cursor_type: "page_token".into(),
                value: "cursor@bad".into(),
            },
            expected_previous: None,
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn source_version_opaque_unsafe_text_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.content_version.source_version = ContentVersionPosition::Opaque {
            version_type: "vtype@bad".into(),
            value: "v1".into(),
        };
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn policy_tenant_mismatch_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.policies[0].tenant = "other-tenant".into();
        let err = envelope.validate().unwrap_err();
        assert!(
            err.contains("policy tenant does not match mutation tenant"),
            "got: {err}"
        );
    }

    #[test]
    fn policy_missing_required_fields_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.policies[0].classification = String::new();
        let err = envelope.validate().unwrap_err();
        assert!(
            err.contains("policy object, classification, and policy version are required"),
            "got: {err}"
        );
    }

    #[test]
    fn blob_missing_identity_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.blobs.push(BlobReference {
            blob_id: String::new(),
            operation: MaterialOperation::Upsert,
            digest_algorithm: "sha256".into(),
            digest: "a".repeat(64),
            media_type: "application/octet-stream".into(),
            length: 0,
        });
        let err = envelope.validate().unwrap_err();
        assert!(
            err.contains("blob identity and media type are required"),
            "got: {err}"
        );
    }

    #[test]
    fn evidence_without_matching_policy_is_rejected_for_missing_governance() {
        let mut envelope = minimal_envelope();
        envelope.evidence.push(EvidenceRecord {
            evidence_id: "ev-1".into(),
            operation: MaterialOperation::Upsert,
            object_id: "ungoverned-object".into(),
            modality: "text".into(),
            locus_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"a": 1})).unwrap(),
            content_digest: "d".repeat(64),
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("no policy proof for material object"), "got: {err}");
    }

    #[test]
    fn feature_with_privacy_violating_value_is_rejected() {
        let mut envelope = minimal_envelope();
        // Governed by the fixture's existing policy (object-1) so the failure
        // observed is the privacy scan, not the governance-proof check.
        envelope.features.push(FeatureRecord {
            feature_id: "feat-1".into(),
            operation: MaterialOperation::Upsert,
            object_id: "object-1".into(),
            kind: "embedding".into(),
            value_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"path": "/home/x"}))
                .unwrap(),
            model_version: "v1".into(),
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn lineage_with_malformed_parent_digest_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.lineage.push(LineageRecord {
            lineage_id: "lin-1".into(),
            operation: MaterialOperation::Upsert,
            object_id: "object-1".into(),
            source_artifact_digest: "a".repeat(64),
            transform_name: "transform".into(),
            transform_version: "v1".into(),
            parent_content_digests: vec!["not-hex".into()],
        });
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("64 hexadecimal"), "got: {err}");
    }

    #[test]
    fn operation_addnode_unsafe_node_id_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.operations[0].method = crate::protocol::Method::AddNode {
            node_id: "node@bad".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": 1}))
                .unwrap(),
        };
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn operation_addnode_privacy_violating_properties_is_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.operations[0].method = crate::protocol::Method::AddNode {
            node_id: "n1".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"path": "/home/x"}))
                .unwrap(),
        };
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }

    #[test]
    fn operation_removeedge_unsafe_ids_are_rejected() {
        let mut envelope = minimal_envelope();
        envelope.mutation.operations[0].method = crate::protocol::Method::RemoveEdge {
            source_id: "s@bad".into(),
            target_id: "t1".into(),
        };
        let err = envelope.validate().unwrap_err();
        assert!(err.contains("persistence privacy policy"), "got: {err}");
    }
}
