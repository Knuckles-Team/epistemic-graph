//! Tenant-bound reads of committed assembly records in the Agent Library.
//!
//! These are not statistical `DecisionLog` entries. A list is a bounded page
//! of component heads; detail and provenance verify the committed body digest.

use serde::{Deserialize, Serialize};

use super::record::{DecisionOutcome, DecisionRecord, EvidenceClass, PremiseRef, ResolutionKind};

pub const MAX_DECISION_READ_PAGE: u32 = 50;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionRecordReadRequest {
    List {
        tenant_id: String,
        limit: Option<u32>,
        cursor: Option<String>,
    },
    Detail {
        tenant_id: String,
        record_id: String,
    },
    Provenance {
        tenant_id: String,
        record_id: String,
    },
}

impl DecisionRecordReadRequest {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::List { tenant_id, .. }
            | Self::Detail { tenant_id, .. }
            | Self::Provenance { tenant_id, .. } => tenant_id,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::List {
                tenant_id,
                limit,
                cursor,
            } => {
                crate::agent_library::validate_key(tenant_id, "decision-read")?;
                if limit.is_some_and(|limit| limit == 0 || limit > MAX_DECISION_READ_PAGE) {
                    return Err(format!(
                        "decision record page limit must be 1..={MAX_DECISION_READ_PAGE}"
                    ));
                }
                if cursor.as_ref().is_some_and(|cursor| {
                    cursor.is_empty()
                        || cursor.len()
                            > crate::agent_component::MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES
                }) {
                    return Err("decision record cursor is outside its bound".to_string());
                }
            }
            Self::Detail {
                tenant_id,
                record_id,
            }
            | Self::Provenance {
                tenant_id,
                record_id,
            } => {
                crate::agent_library::validate_key(tenant_id, record_id)?;
                let digest = record_id.strip_prefix(super::DECISION_COMPONENT_ID_PREFIX);
                if !digest.is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                }) {
                    return Err("decision record id must be decision:<sha256-hex>".to_string());
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionRecordSummary {
    pub record_id: String,
    pub created_at_ms: u64,
    pub resolution_kind: ResolutionKind,
    pub evidence_class: EvidenceClass,
    /// Small list projection; the full proof and reasons live in `detail`.
    pub outcome_kind: String,
    pub graph_digest: Option<String>,
    pub record_digest: String,
}

impl From<&DecisionRecord> for DecisionRecordSummary {
    fn from(record: &DecisionRecord) -> Self {
        Self {
            record_id: record.record_id.clone(),
            created_at_ms: record.created_at_ms,
            resolution_kind: record.resolution_kind,
            evidence_class: record.evidence_class,
            outcome_kind: match &record.outcome {
                DecisionOutcome::Solved { .. } => "solved",
                DecisionOutcome::Abstained { .. } => "abstained",
            }
            .to_string(),
            graph_digest: match &record.outcome {
                DecisionOutcome::Solved { graph_digest, .. } => Some(graph_digest.clone()),
                DecisionOutcome::Abstained { .. } => None,
            },
            record_digest: record.record_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionRecordProvenance {
    pub record_id: String,
    pub record_digest: String,
    pub inputs_digest: String,
    pub ontology_digest: String,
    pub policy_digest: String,
    pub catalog_digest: String,
    pub evidence_class: EvidenceClass,
    pub premises: Vec<PremiseRef>,
}

impl From<&DecisionRecord> for DecisionRecordProvenance {
    fn from(record: &DecisionRecord) -> Self {
        Self {
            record_id: record.record_id.clone(),
            record_digest: record.record_digest.clone(),
            inputs_digest: record.inputs_digest.clone(),
            ontology_digest: record.inputs.ontology_digest.clone(),
            policy_digest: record.inputs.policy_digest.clone(),
            catalog_digest: record.inputs.catalog_digest.clone(),
            evidence_class: record.evidence_class,
            premises: record.premises.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DecisionRecordReadResult {
    List {
        entries: Vec<DecisionRecordSummary>,
        next_cursor: Option<String>,
    },
    Detail {
        record: Option<Box<DecisionRecord>>,
    },
    Provenance {
        provenance: Option<DecisionRecordProvenance>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_requests_refuse_unbounded_pages_and_non_decision_ids() {
        for limit in [0, MAX_DECISION_READ_PAGE + 1] {
            assert!(DecisionRecordReadRequest::List {
                tenant_id: "tenant-a".into(),
                limit: Some(limit),
                cursor: None,
            }
            .validate()
            .is_err());
        }
        assert!(DecisionRecordReadRequest::Detail {
            tenant_id: "tenant-a".into(),
            record_id: "not-a-decision".into(),
        }
        .validate()
        .is_err());
        let op = crate::agent_component::AgentComponentOp::DecisionRead {
            request: DecisionRecordReadRequest::Provenance {
                tenant_id: "tenant-a".into(),
                record_id: format!("decision:{}", "a".repeat(64)),
            },
        };
        assert_eq!(op.tenant_id(), "tenant-a");
        assert_eq!(op.authz_action(), "agent:component-read");
        assert!(!op.is_mutation());
    }
}
