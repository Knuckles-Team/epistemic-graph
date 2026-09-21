//! Governed connector write-back contracts (RF-ADR-009 D18).
//!
//! EG records authorization and source observations; it never invokes a vendor API.
//! Connector transports execute the source-side read, preview, apply and reconcile
//! operations and submit their bounded observations through this contract.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::contract::Digest256;

pub const WRITE_BACK_SCHEMA_VERSION: u16 = 1;
pub const MAX_WRITE_BACK_FIELDS: usize = 256;
pub const MAX_WRITE_BACK_RECEIPTS: usize = 4096;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteBackAuthorizationMode {
    ProposalApproval,
    StandingPolicy,
    ManualTrigger,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteBackAttemptKind {
    DryRun,
    Apply,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteBackOutcome {
    DryRunReady,
    Applied,
    Conflict,
    Rejected,
    OutcomeUncertain,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum WriteBackEffectStatus {
    NoEffect,
    Applied,
    OutcomeUncertain,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WriteBackAuthorizationDecision {
    pub mode: WriteBackAuthorizationMode,
    pub authorization_ref: String,
    pub decision_digest: Digest256,
    pub input_digest: Digest256,
    pub output_digest: Digest256,
    pub authorized: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceChangeSet {
    pub schema_version: u16,
    pub change_set_id: String,
    pub change_set_digest: Digest256,
    pub tenant_id: String,
    pub actor: String,
    pub purpose: String,
    pub connector_id: String,
    pub source_instance_id: String,
    pub entity_id: String,
    pub field_scope: Vec<String>,
    pub base_source_version: String,
    pub desired_patch: BTreeMap<String, serde_json::Value>,
    pub source_of_truth_rule: String,
    pub field_provenance: BTreeMap<String, String>,
    pub required_capability: String,
    pub policy_digest: Digest256,
    pub authorization: WriteBackAuthorizationDecision,
    pub idempotency_key: String,
    pub expires_at_ms: u64,
    pub reconciliation_procedure: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WriteBackAttempt {
    pub tenant_id: String,
    pub change_set_id: String,
    pub change_set_digest: Digest256,
    pub idempotency_key: String,
    pub kind: WriteBackAttemptKind,
    pub input_digest: Digest256,
    pub output_digest: Digest256,
    pub pre_source_version: String,
    pub post_source_version: String,
    pub applied_field_digest: Digest256,
    pub outcome: WriteBackOutcome,
    pub effect_status: WriteBackEffectStatus,
    pub connector_observation_digest: Digest256,
    pub provenance_digest: Digest256,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReconciliationObservation {
    pub tenant_id: String,
    pub change_set_id: String,
    pub change_set_digest: Digest256,
    pub idempotency_key: String,
    pub observed_source_version: String,
    pub effect_status: WriteBackEffectStatus,
    pub retry_allowed: bool,
    pub evidence_digest: Digest256,
    pub connector_observation_digest: Digest256,
    pub provenance_digest: Digest256,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WriteBackReceipt {
    pub schema_version: u16,
    pub receipt_id: String,
    pub sequence: u64,
    pub change_set_id: String,
    pub change_set_digest: Digest256,
    pub tenant_id: String,
    pub actor: String,
    pub authorization: WriteBackAuthorizationDecision,
    pub policy_digest: Digest256,
    pub idempotency_key: String,
    pub kind: WriteBackAttemptKind,
    pub input_digest: Digest256,
    pub output_digest: Digest256,
    pub pre_source_version: String,
    pub post_source_version: String,
    pub applied_field_digest: Digest256,
    pub outcome: WriteBackOutcome,
    pub effect_status: WriteBackEffectStatus,
    pub connector_observation_digest: Digest256,
    pub provenance_digest: Digest256,
    pub recorded_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReconciliationReceipt {
    pub schema_version: u16,
    pub receipt_id: String,
    pub sequence: u64,
    pub change_set_id: String,
    pub change_set_digest: Digest256,
    pub tenant_id: String,
    pub actor: String,
    pub authorization: WriteBackAuthorizationDecision,
    pub policy_digest: Digest256,
    pub idempotency_key: String,
    pub observed_source_version: String,
    pub effect_status: WriteBackEffectStatus,
    pub retry_allowed: bool,
    pub evidence_digest: Digest256,
    pub connector_observation_digest: Digest256,
    pub provenance_digest: Digest256,
    pub recorded_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "receipt_kind", content = "receipt", rename_all = "snake_case")]
pub enum WriteBackReceiptRecord {
    Attempt(WriteBackReceipt),
    Reconciliation(ReconciliationReceipt),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WriteBackReceiptPage {
    pub receipts: Vec<WriteBackReceiptRecord>,
    pub next_sequence: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum WriteBackOp {
    Create {
        change_set: Box<SourceChangeSet>,
    },
    Get {
        tenant_id: String,
        change_set_id: String,
    },
    RecordAttempt {
        attempt: Box<WriteBackAttempt>,
    },
    RecordReconciliation {
        observation: Box<ReconciliationObservation>,
    },
    Receipts {
        tenant_id: String,
        change_set_id: String,
        #[serde(default)]
        after_sequence: u64,
        limit: u16,
    },
}

impl SourceChangeSet {
    /// Digest the complete immutable change set, with the digest field itself zeroed.
    ///
    /// This convention makes the caller-provided digest independently reproducible by
    /// EG while retaining a single transport and persistence representation.
    pub fn canonical_digest(&self) -> Result<Digest256, String> {
        let mut projection = self.clone();
        projection.change_set_digest = Digest256::from_bytes([0; 32]);
        for value in projection.desired_patch.values_mut() {
            canonicalize_json(value);
        }
        let encoded = rmp_serde::to_vec_named(&projection).map_err(|error| error.to_string())?;
        Digest256::framed(b"eg/source-change-set/v1", &[encoded.as_slice()])
            .map_err(|error| error.to_string())
    }

    /// Digest the authorized field names and desired values exactly as persisted.
    pub fn patch_digest(&self) -> Result<Digest256, String> {
        let mut scope = self.field_scope.clone();
        scope.sort();
        let mut patch = self.desired_patch.clone();
        for value in patch.values_mut() {
            canonicalize_json(value);
        }
        let encoded =
            rmp_serde::to_vec_named(&(scope, patch)).map_err(|error| error.to_string())?;
        Digest256::framed(b"eg/source-change-set-patch/v1", &[encoded.as_slice()])
            .map_err(|error| error.to_string())
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_identity()?;
        self.validate_scope()?;
        self.validate_expiry_and_digest()
    }

    fn validate_identity(&self) -> Result<(), String> {
        if self.schema_version != WRITE_BACK_SCHEMA_VERSION {
            return Err("unsupported write-back schema version".to_string());
        }
        let required = [
            self.change_set_id.as_str(),
            self.tenant_id.as_str(),
            self.actor.as_str(),
            self.purpose.as_str(),
            self.connector_id.as_str(),
            self.source_instance_id.as_str(),
            self.entity_id.as_str(),
            self.base_source_version.as_str(),
            self.source_of_truth_rule.as_str(),
            self.required_capability.as_str(),
            self.idempotency_key.as_str(),
            self.reconciliation_procedure.as_str(),
            self.authorization.authorization_ref.as_str(),
        ];
        if required.iter().any(|value| value.is_empty()) {
            return Err("write-back identity fields must not be empty".to_string());
        }
        if !self.authorization.authorized {
            return Err("write-back authorization decision denied the change set".to_string());
        }
        Ok(())
    }

    fn validate_scope(&self) -> Result<(), String> {
        if self.field_scope.is_empty() || self.field_scope.len() > MAX_WRITE_BACK_FIELDS {
            return Err("write-back field scope exceeds resource limits".to_string());
        }
        let mut scope = self.field_scope.clone();
        scope.sort();
        scope.dedup();
        if scope.len() != self.field_scope.len()
            || scope.iter().any(|field| field.is_empty())
            || self.desired_patch.keys().ne(scope.iter())
            || self.field_provenance.keys().ne(scope.iter())
        {
            return Err("desired patch and provenance must exactly match field scope".to_string());
        }
        Ok(())
    }

    fn validate_expiry_and_digest(&self) -> Result<(), String> {
        if self.expires_at_ms == 0 {
            return Err("write-back change set requires an expiry".to_string());
        }
        if self.change_set_digest != self.canonical_digest()? {
            return Err("write-back change-set digest does not match its contents".to_string());
        }
        Ok(())
    }
}

fn canonicalize_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize_json(value);
            }
        }
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = std::mem::take(object).into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (key, mut value) in entries {
                canonicalize_json(&mut value);
                object.insert(key, value);
            }
        }
        _ => {}
    }
}

impl WriteBackOp {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Create { change_set } => &change_set.tenant_id,
            Self::Get { tenant_id, .. } | Self::Receipts { tenant_id, .. } => tenant_id,
            Self::RecordAttempt { attempt } => &attempt.tenant_id,
            Self::RecordReconciliation { observation } => &observation.tenant_id,
        }
    }

    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Create { .. } | Self::RecordAttempt { .. } | Self::RecordReconciliation { .. }
        )
    }

    pub fn authz_action(&self) -> &'static str {
        if self.is_mutation() {
            "connector:write-back"
        } else {
            "connector:write-back-read"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn change_set() -> SourceChangeSet {
        let mut value = SourceChangeSet {
            schema_version: WRITE_BACK_SCHEMA_VERSION,
            change_set_id: "change-1".to_string(),
            change_set_digest: digest(1),
            tenant_id: "tenant-1".to_string(),
            actor: format!("principal:sha256:{}", "a".repeat(64)),
            purpose: "approved maintenance".to_string(),
            connector_id: "connector-1".to_string(),
            source_instance_id: "source-1".to_string(),
            entity_id: "entity-1".to_string(),
            field_scope: vec!["priority".to_string()],
            base_source_version: "etag-1".to_string(),
            desired_patch: BTreeMap::from([(
                "priority".to_string(),
                serde_json::Value::String("high".to_string()),
            )]),
            source_of_truth_rule: "source_wins_outside_scope".to_string(),
            field_provenance: BTreeMap::from([("priority".to_string(), "approval:42".to_string())]),
            required_capability: "ticket:update".to_string(),
            policy_digest: digest(2),
            authorization: WriteBackAuthorizationDecision {
                mode: WriteBackAuthorizationMode::ProposalApproval,
                authorization_ref: "approval:42".to_string(),
                decision_digest: digest(3),
                input_digest: digest(4),
                output_digest: digest(5),
                authorized: true,
            },
            idempotency_key: "idempotency-1".to_string(),
            expires_at_ms: 4_000_000_000_000,
            reconciliation_procedure: "read_by_idempotency_and_version".to_string(),
        };
        value.change_set_digest = value.canonical_digest().expect("canonical digest");
        value
    }

    #[test]
    fn valid_change_set_has_exact_scoped_patch_and_authorization() {
        assert!(change_set().validate().is_ok());
    }

    #[test]
    fn caller_mode_without_affirmative_decision_fails_closed() {
        let mut value = change_set();
        value.authorization.authorized = false;
        assert!(value.validate().is_err());
    }

    #[test]
    fn patch_cannot_escape_field_scope() {
        let mut value = change_set();
        value
            .desired_patch
            .insert("unapproved".to_string(), serde_json::Value::Bool(true));
        assert!(value.validate().is_err());
    }

    #[test]
    fn canonical_digest_vectors_are_cross_language_stable() {
        let value = change_set();
        assert_eq!(
            value.change_set_digest.to_hex(),
            "6b8533d62f86e685d1f2649e7ead46c226e037dd66d0b4ea0a7af6460828541f"
        );
        assert_eq!(
            value.patch_digest().unwrap().to_hex(),
            "66625421e4f2816d7abad2a3a0cde2c058c0be1260a04cb6c7900b6d48ba0a7e"
        );
    }
}
