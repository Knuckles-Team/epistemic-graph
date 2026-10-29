//! Governance, permission sync, deletion reconciliation and container records
//! for connector-sourced knowledge (MEDULLA-INGEST-WORKER-R900.1, R902.1, R903.1,
//! R904.1).
//!
//! Pure data. Admission parsing and the `access`, `groups`, `enumeration` and
//! `containers` batch kinds are later rows wired through `SourceIngest`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Maximum entries in a record's read-role or marking set.
pub const MAX_GOVERNANCE_SET: usize = 256;

/// Closed classification ladder. An unknown value fails deserialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Classification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// The governance properties a connector sends with one source record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "RecordGovernanceWire")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RecordGovernance {
    pub classification: Classification,
    /// Retention period in seconds; `None` means no retention was sent.
    #[serde(default)]
    pub retention: Option<u64>,
    #[serde(default)]
    pub legal_hold: bool,
    pub access_public: bool,
    #[serde(default)]
    pub access_read_roles: BTreeSet<String>,
    #[serde(default)]
    pub access_markings: BTreeSet<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
struct RecordGovernanceWire {
    classification: Classification,
    #[serde(default)]
    retention: Option<u64>,
    #[serde(default)]
    legal_hold: bool,
    access_public: bool,
    #[serde(default)]
    access_read_roles: BTreeSet<String>,
    #[serde(default)]
    access_markings: BTreeSet<String>,
}

/// Why a governance or source-structure record is refused at construction.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SourceGovernanceError {
    #[error("{field} holds more than {MAX_GOVERNANCE_SET} entries")]
    SetTooLarge { field: &'static str },
    #[error("{field} must not contain a blank entry")]
    BlankEntry { field: &'static str },
    #[error("{field} must not be blank")]
    BlankIdentifier { field: &'static str },
    #[error("container {0} names itself as its parent")]
    SelfParent(String),
}

fn check_set(field: &'static str, set: &BTreeSet<String>) -> Result<(), SourceGovernanceError> {
    if set.len() > MAX_GOVERNANCE_SET {
        return Err(SourceGovernanceError::SetTooLarge { field });
    }
    if set.iter().any(|entry| entry.trim().is_empty()) {
        return Err(SourceGovernanceError::BlankEntry { field });
    }
    Ok(())
}

fn check_id(field: &'static str, value: &str) -> Result<(), SourceGovernanceError> {
    if value.trim().is_empty() {
        return Err(SourceGovernanceError::BlankIdentifier { field });
    }
    Ok(())
}

impl TryFrom<RecordGovernanceWire> for RecordGovernance {
    type Error = SourceGovernanceError;

    fn try_from(wire: RecordGovernanceWire) -> Result<Self, Self::Error> {
        check_set("access_read_roles", &wire.access_read_roles)?;
        check_set("access_markings", &wire.access_markings)?;
        Ok(Self {
            classification: wire.classification,
            retention: wire.retention,
            legal_hold: wire.legal_hold,
            access_public: wire.access_public,
            access_read_roles: wire.access_read_roles,
            access_markings: wire.access_markings,
        })
    }
}

/// The access of one held record as last synced from its source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AccessSnapshot {
    pub source: String,
    pub record_ref: String,
    pub access_public: bool,
    #[serde(default)]
    pub read_principals: BTreeSet<String>,
    #[serde(default)]
    pub read_groups: BTreeSet<String>,
    /// Sync time in milliseconds since the Unix epoch.
    pub synced_at: u64,
}

impl AccessSnapshot {
    pub fn validate(&self) -> Result<(), SourceGovernanceError> {
        check_id("source", &self.source)?;
        check_id("record_ref", &self.record_ref)
    }
}

/// The member set of one source group as last synced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceGroup {
    pub source: String,
    pub group_id: String,
    #[serde(default)]
    pub members: BTreeSet<String>,
    pub synced_at: u64,
}

impl SourceGroup {
    pub fn validate(&self) -> Result<(), SourceGovernanceError> {
        check_id("source", &self.source)?;
        check_id("group_id", &self.group_id)
    }
}

/// An identifier-only listing of one source run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceEnumeration {
    pub source: String,
    pub run_id: String,
    pub ids_digest: String,
    /// Only a complete enumeration may ever drive a deletion.
    pub complete: bool,
}

impl SourceEnumeration {
    pub fn validate(&self) -> Result<(), SourceGovernanceError> {
        check_id("source", &self.source)?;
        check_id("run_id", &self.run_id)?;
        check_id("ids_digest", &self.ids_digest)
    }
}

/// Held records missing from a complete enumeration, pending their grace window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReconcilePlan {
    pub source: String,
    #[serde(default)]
    pub missing: BTreeSet<String>,
    /// End of the grace window in milliseconds since the Unix epoch.
    pub grace_until: u64,
}

impl ReconcilePlan {
    pub fn validate(&self) -> Result<(), SourceGovernanceError> {
        check_id("source", &self.source)
    }
}

/// One node of a source's container tree (space, folder, channel).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceContainer {
    pub source: String,
    pub container_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub title: String,
}

impl SourceContainer {
    pub fn validate(&self) -> Result<(), SourceGovernanceError> {
        check_id("source", &self.source)?;
        check_id("container_id", &self.container_id)?;
        if let Some(parent) = &self.parent_id {
            check_id("parent_id", parent)?;
            if parent == &self.container_id {
                return Err(SourceGovernanceError::SelfParent(self.container_id.clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // spec: MEDULLA-INGEST-WORKER-R900.1
    #[test]
    fn record_governance_round_trips_and_rejects_invalid_shapes() {
        let value = json!({
            "classification": "restricted",
            "retention": 31_536_000,
            "legal_hold": true,
            "access_public": false,
            "access_read_roles": ["legal", "auditor"],
            "access_markings": ["pii"]
        });
        let governance: RecordGovernance = serde_json::from_value(value).unwrap();
        assert_eq!(governance.classification, Classification::Restricted);
        assert_eq!(
            governance.access_read_roles.iter().collect::<Vec<_>>(),
            ["auditor", "legal"],
            "read roles are a sorted set"
        );
        let encoded = serde_json::to_value(&governance).unwrap();
        assert_eq!(
            serde_json::from_value::<RecordGovernance>(encoded).unwrap(),
            governance
        );

        let unknown = json!({"classification": "secret", "access_public": false});
        assert!(serde_json::from_value::<RecordGovernance>(unknown).is_err());

        let roles: Vec<String> = (0..=MAX_GOVERNANCE_SET).map(|i| format!("r{i}")).collect();
        let oversized = json!({
            "classification": "internal",
            "access_public": false,
            "access_read_roles": roles
        });
        assert!(serde_json::from_value::<RecordGovernance>(oversized).is_err());
        let at_bound: Vec<String> = (0..MAX_GOVERNANCE_SET).map(|i| format!("r{i}")).collect();
        let bounded = json!({
            "classification": "internal",
            "access_public": false,
            "access_markings": at_bound
        });
        assert!(serde_json::from_value::<RecordGovernance>(bounded).is_ok());
        let blank = json!({
            "classification": "internal",
            "access_public": false,
            "access_markings": [" "]
        });
        assert!(serde_json::from_value::<RecordGovernance>(blank).is_err());
    }
}
