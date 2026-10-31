//! Write-back proposal records: the approval queue and the inventory and
//! ticket proposals it releases (MEDULLA-DECISION-ENGINE-R615.1).
//!
//! The model layer of the write-back family: every proposal is typed,
//! carries the risk tier it was triaged into and the as-of instant it
//! derives from, and is deduplicated by its external id. The queue/approve/
//! execute behaviour (R615.2), the vendor execution wiring (R615.3) and the
//! `PROPOSAL_UNAPPROVED` refusal (R615.4) build on these records; nothing
//! here executes a vendor call or reads a clock.

use medulla_types::contract::{ResourceId, TenantId};
use serde::{Deserialize, Serialize};

/// The stable code of a refused write-back identifier.
pub const INVALID_WRITEBACK_IDENTIFIER: &str = "INVALID_WRITEBACK_IDENTIFIER";
/// The stable code of a refused as-of instant.
pub const INVALID_WRITEBACK_AS_OF: &str = "INVALID_WRITEBACK_AS_OF";
/// The stable code of a queue holding two proposals with one external id.
pub const WRITEBACK_DUPLICATE_EXTERNAL_ID: &str = "WRITEBACK_DUPLICATE_EXTERNAL_ID";
/// The stable code of a queue holding another tenant's proposal.
pub const WRITEBACK_TENANT_MISMATCH: &str = "WRITEBACK_TENANT_MISMATCH";

/// A refused write-back model value, with its stable code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritebackError {
    pub code: &'static str,
    pub detail: String,
}

impl WritebackError {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for WritebackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for WritebackError {}

fn resource_id<T: Into<String>>(value: T, field: &str) -> Result<ResourceId, WritebackError> {
    ResourceId::new(value).map_err(|detail| {
        WritebackError::new(INVALID_WRITEBACK_IDENTIFIER, format!("{field}: {detail}"))
    })
}

fn tenant_id<T: Into<String>>(value: T, field: &str) -> Result<TenantId, WritebackError> {
    TenantId::new(value).map_err(|detail| {
        WritebackError::new(INVALID_WRITEBACK_IDENTIFIER, format!("{field}: {detail}"))
    })
}

fn as_of(as_of_ms: i64) -> Result<(), WritebackError> {
    if as_of_ms <= 0 {
        return Err(WritebackError::new(
            INVALID_WRITEBACK_AS_OF,
            format!("as-of instant {as_of_ms} must be a positive unix-millisecond instant"),
        ));
    }
    Ok(())
}

/// The risk tier a write-back proposal is triaged into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskTier {
    Low,
    Medium,
    High,
}

/// One high-stakes write-back proposal, stamped with the as-of instant it
/// derives from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WritebackProposal {
    pub id: ResourceId,
    pub tenant: TenantId,
    /// The external id the proposal deduplicates by.
    pub external_id: ResourceId,
    pub risk_tier: RiskTier,
    /// Milliseconds since the Unix epoch: the instant the proposal derives from.
    pub as_of_ms: i64,
}

impl WritebackProposal {
    pub fn new(
        id: impl Into<String>,
        tenant: impl Into<String>,
        external_id: impl Into<String>,
        risk_tier: RiskTier,
        as_of_ms: i64,
    ) -> Result<Self, WritebackError> {
        let id = resource_id(id, "id")?;
        let tenant = tenant_id(tenant, "tenant")?;
        let external_id = resource_id(external_id, "external_id")?;
        as_of(as_of_ms)?;
        Ok(Self {
            id,
            tenant,
            external_id,
            risk_tier,
            as_of_ms,
        })
    }
}

/// The approval queue: high-stakes proposals previewed, not yet released.
/// Approval releases the queued proposals as one governed change set
/// (R615.2); nothing here executes them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalQueue {
    pub id: ResourceId,
    pub tenant: TenantId,
    /// The queued proposals, in enqueue order.
    pub proposals: Vec<WritebackProposal>,
}

impl ProposalQueue {
    /// A queue never holds two proposals with the same external id, and
    /// never holds another tenant's proposal.
    pub fn new(
        id: impl Into<String>,
        tenant: impl Into<String>,
        proposals: Vec<WritebackProposal>,
    ) -> Result<Self, WritebackError> {
        let id = resource_id(id, "id")?;
        let tenant = tenant_id(tenant, "tenant")?;
        let mut external: std::collections::BTreeSet<&ResourceId> =
            std::collections::BTreeSet::new();
        for proposal in &proposals {
            if proposal.tenant != tenant {
                return Err(WritebackError::new(
                    WRITEBACK_TENANT_MISMATCH,
                    format!(
                        "proposal {} belongs to tenant {}, not {}",
                        proposal.id.as_str(),
                        proposal.tenant.as_str(),
                        tenant.as_str()
                    ),
                ));
            }
            if !external.insert(&proposal.external_id) {
                return Err(WritebackError::new(
                    WRITEBACK_DUPLICATE_EXTERNAL_ID,
                    format!(
                        "external id {} is queued more than once",
                        proposal.external_id.as_str()
                    ),
                ));
            }
        }
        Ok(Self {
            id,
            tenant,
            proposals,
        })
    }
}

/// Reconciled inventory becoming a creation proposal for one enabled system
/// of record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryCreation {
    pub id: ResourceId,
    pub tenant: TenantId,
    /// The external id the creation deduplicates by.
    pub external_id: ResourceId,
    /// The system of record the creation reaches.
    pub system_of_record: ResourceId,
    /// Milliseconds since the Unix epoch: the instant the creation derives from.
    pub as_of_ms: i64,
}

impl InventoryCreation {
    pub fn new(
        id: impl Into<String>,
        tenant: impl Into<String>,
        external_id: impl Into<String>,
        system_of_record: impl Into<String>,
        as_of_ms: i64,
    ) -> Result<Self, WritebackError> {
        let id = resource_id(id, "id")?;
        let tenant = tenant_id(tenant, "tenant")?;
        let external_id = resource_id(external_id, "external_id")?;
        let system_of_record = resource_id(system_of_record, "system_of_record")?;
        as_of(as_of_ms)?;
        Ok(Self {
            id,
            tenant,
            external_id,
            system_of_record,
            as_of_ms,
        })
    }
}

/// A risk finding becoming a ticket proposal, stamped with the as-of
/// instant it derives from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FindingTicket {
    pub id: ResourceId,
    pub tenant: TenantId,
    /// The external id the ticket deduplicates by.
    pub external_id: ResourceId,
    /// The risk finding the ticket derives from.
    pub finding_id: ResourceId,
    pub risk_tier: RiskTier,
    /// Milliseconds since the Unix epoch: the instant the ticket derives from.
    pub as_of_ms: i64,
}

impl FindingTicket {
    pub fn new(
        id: impl Into<String>,
        tenant: impl Into<String>,
        external_id: impl Into<String>,
        finding_id: impl Into<String>,
        risk_tier: RiskTier,
        as_of_ms: i64,
    ) -> Result<Self, WritebackError> {
        let id = resource_id(id, "id")?;
        let tenant = tenant_id(tenant, "tenant")?;
        let external_id = resource_id(external_id, "external_id")?;
        let finding_id = resource_id(finding_id, "finding_id")?;
        as_of(as_of_ms)?;
        Ok(Self {
            id,
            tenant,
            external_id,
            finding_id,
            risk_tier,
            as_of_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(external_id: &str, tenant: &str) -> WritebackProposal {
        WritebackProposal::new(
            "wp:1",
            tenant,
            external_id,
            RiskTier::High,
            1_700_000_000_000,
        )
        .unwrap()
    }

    // spec: MEDULLA-DECISION-ENGINE-R615.1
    #[test]
    fn writeback_types_round_trip() {
        let queue =
            ProposalQueue::new("queue:1", "tenant-a", vec![proposal("ext:1", "tenant-a")]).unwrap();
        let creation = InventoryCreation::new(
            "inv:1",
            "tenant-a",
            "ext:1",
            "sor:ledger",
            1_700_000_000_000,
        )
        .unwrap();
        let ticket = FindingTicket::new(
            "tick:1",
            "tenant-a",
            "ext:2",
            "finding:1",
            RiskTier::Medium,
            1_700_000_000_001,
        )
        .unwrap();
        macro_rules! round_trip {
            ($value:expr, $ty:ty) => {{
                let json = serde_json::to_string(&$value).unwrap();
                assert_eq!(serde_json::from_str::<$ty>(&json).unwrap(), $value);
            }};
        }
        round_trip!(queue, ProposalQueue);
        round_trip!(creation, InventoryCreation);
        round_trip!(ticket, FindingTicket);
        assert_eq!(serde_json::to_value(ticket.risk_tier).unwrap(), "medium");
    }

    // spec: MEDULLA-DECISION-ENGINE-R615.1
    #[test]
    fn writeback_types_reject_invalid_identifiers() {
        for bad in ["", "has space", "é"] {
            assert_eq!(
                WritebackProposal::new(bad, "tenant-a", "ext:1", RiskTier::High, 1)
                    .unwrap_err()
                    .code,
                INVALID_WRITEBACK_IDENTIFIER,
                "{bad:?}"
            );
        }
        assert_eq!(
            WritebackProposal::new("wp:1", "bad tenant", "ext:1", RiskTier::High, 1)
                .unwrap_err()
                .code,
            INVALID_WRITEBACK_IDENTIFIER
        );
        assert_eq!(
            WritebackProposal::new("wp:1", "tenant-a", "bad external", RiskTier::High, 1)
                .unwrap_err()
                .code,
            INVALID_WRITEBACK_IDENTIFIER
        );
        assert_eq!(
            WritebackProposal::new("wp:1", "tenant-a", "ext:1", RiskTier::High, 0)
                .unwrap_err()
                .code,
            INVALID_WRITEBACK_AS_OF
        );
        let other = proposal("ext:1", "tenant-b");
        let queue = ProposalQueue::new("queue:1", "tenant-a", vec![other]);
        assert_eq!(queue.unwrap_err().code, WRITEBACK_TENANT_MISMATCH);
        let duplicate = ProposalQueue::new(
            "queue:1",
            "tenant-a",
            vec![proposal("ext:1", "tenant-a"), proposal("ext:1", "tenant-a")],
        );
        assert_eq!(duplicate.unwrap_err().code, WRITEBACK_DUPLICATE_EXTERNAL_ID);
        assert!(serde_json::from_str::<WritebackProposal>(
            r#"{"id":"wp:1","tenant":"tenant-a","external_id":"ext:1","risk_tier":"high","as_of_ms":1,"extra":true}"#
        )
        .is_err());
        assert!(serde_json::from_str::<FindingTicket>(
            r#"{"id":"t","tenant":"tenant-a","external_id":"ext","finding_id":"f","risk_tier":"urgent","as_of_ms":1}"#
        )
        .is_err());
    }
}
