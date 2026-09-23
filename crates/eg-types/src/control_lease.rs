//! Native control leases (graph-os EG-2/EG-3): a tenant-bound, time-boxed
//! grant record with a one-way lifecycle -- `active`, optionally `consumed`
//! (a single-use grant that has been spent but still binds its consumer), then
//! `revoked` or `expired`.
//!
//! # Why not `CapacityLease`
//!
//! A capacity lease is an ADMISSION against a capacity cell: it spends an
//! `amount` of a resource class, is held by the authenticated principal that
//! acquired it, and is renewed or released by that holder under its fence. A
//! control lease is a GRANT: it records WHAT was authorised (an opaque,
//! immutable grant body -- e.g. the tool allowlist and schema digests of an
//! attended browser session), consumes no capacity, is never renewed, and may
//! be ended by an actor other than the one it names. None of those four fit a
//! capacity cell, and a capacity lease has nowhere to carry the grant.
//!
//! # What EG enforces
//!
//! The record is written ONLY by the native methods here: generic node writes
//! may not create, update or remove a `ControlLease` row. The grant body and
//! the timing are immutable after issue; the only transitions are
//! `active -> consumed | revoked | expired` and `consumed -> revoked | expired`,
//! each compare-and-set on the row revision a caller read. `consumed` happens
//! at most once, which is what makes a lease a single-use receipt.
//! The expiry is authoritative in milliseconds, so a caller holding float
//! seconds converts with `floor` -- which can shorten a lease by less than a
//! millisecond and can never extend one.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::work_item_read::WORK_ITEM_ROW_REVISION;

/// `node_type` of a control-lease row.
pub const CONTROL_LEASE_NODE_TYPE: &str = "ControlLease";
/// Largest encoded grant body.
pub const MAX_CONTROL_LEASE_GRANT_BYTES: usize = 64 * 1024;
/// Longest `hard_expires_at_ms - issued_at_ms` a lease may span (24 hours).
pub const MAX_CONTROL_LEASE_SPAN_MS: u64 = 24 * 60 * 60 * 1000;
/// Bound on the tenant, lease id, kind and idempotency key.
const MAX_CONTROL_LEASE_REF_BYTES: usize = 512;

/// Where a control lease is in its one-way lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ControlLeaseStatus {
    Active,
    /// A single-use grant that has been spent; still live until it is
    /// revoked or expires.
    Consumed,
    Revoked,
    Expired,
}

/// The state a transition moves a lease to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ControlLeaseTarget {
    Consumed,
    Revoked,
    Expired,
}

/// Every legal `(from, to)` edge. A table so the lifecycle is read off one
/// list; anything absent -- leaving `revoked`/`expired`, consuming twice,
/// re-activating -- is a `conflict`.
const LEGAL_TRANSITIONS: [(ControlLeaseStatus, ControlLeaseTarget); 5] = [
    (ControlLeaseStatus::Active, ControlLeaseTarget::Consumed),
    (ControlLeaseStatus::Active, ControlLeaseTarget::Revoked),
    (ControlLeaseStatus::Active, ControlLeaseTarget::Expired),
    (ControlLeaseStatus::Consumed, ControlLeaseTarget::Revoked),
    (ControlLeaseStatus::Consumed, ControlLeaseTarget::Expired),
];

impl ControlLeaseTarget {
    pub fn status(self) -> ControlLeaseStatus {
        match self {
            Self::Consumed => ControlLeaseStatus::Consumed,
            Self::Revoked => ControlLeaseStatus::Revoked,
            Self::Expired => ControlLeaseStatus::Expired,
        }
    }

    /// Whether a lease currently in `from` may move to this target.
    pub fn allowed_from(self, from: ControlLeaseStatus) -> bool {
        LEGAL_TRANSITIONS.contains(&(from, self))
    }
}

/// The stored `status` text of each lifecycle state.
const STORED_STATUS: [(&str, ControlLeaseStatus); 4] = [
    ("active", ControlLeaseStatus::Active),
    ("consumed", ControlLeaseStatus::Consumed),
    ("revoked", ControlLeaseStatus::Revoked),
    ("expired", ControlLeaseStatus::Expired),
];

impl ControlLeaseStatus {
    pub fn as_stored(self) -> &'static str {
        STORED_STATUS
            .iter()
            .find(|(_, status)| *status == self)
            .map_or("", |(text, _)| text)
    }

    pub fn from_stored(stored: &str) -> Option<Self> {
        crate::work_item_read::stored_value(&STORED_STATUS, stored)
    }
}

/// `IssueControlLease`: create one active lease, refused if the id exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IssueControlLeaseRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub lease_id: String,
    /// Caller-defined lease family, e.g. `browser.control`.
    pub kind: String,
    /// The immutable grant body.
    pub grant: Map<String, Value>,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub hard_expires_at_ms: u64,
    /// Caller-stable retry identity for this issue.
    pub idempotency_key: String,
}

/// `TransitionControlLease`: consume or end a lease, CAS on its read revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TransitionControlLeaseRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub lease_id: String,
    /// The `revision` of the view the caller decided on.
    pub expected_revision: u64,
    pub to: ControlLeaseTarget,
    /// Caller-stable retry identity for this transition.
    pub idempotency_key: String,
}

/// The caller's view of one control lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ControlLeaseView {
    pub lease_id: String,
    pub kind: String,
    pub status: ControlLeaseStatus,
    pub grant: Map<String, Value>,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub hard_expires_at_ms: u64,
    /// Row revision: 1 at issue, bumped by every transition.
    pub revision: u64,
}

/// How an issue resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ControlLeaseIssueOutcome {
    Issued,
    /// A row with this id already exists; nothing was written.
    Collision,
}

/// How a transition resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ControlLeaseTransitionOutcome {
    Applied,
    /// The edge is not legal from the lease's current status, or the lease
    /// moved past `expected_revision`.
    Conflict,
    /// No lease with this id is visible to the tenant.
    NotFound,
}

/// Result of `IssueControlLease`. `changed_work_item_ids` names the rows the
/// commit wrote, so the serving projection refreshes from authority exactly as
/// it does for a WorkItem transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ControlLeaseIssued {
    pub outcome: ControlLeaseIssueOutcome,
    pub lease: Option<ControlLeaseView>,
    pub changed_work_item_ids: Vec<String>,
}

/// Result of `TransitionControlLease`. On `conflict` the CURRENT view is
/// returned so a caller can decide again without a second read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ControlLeaseTransition {
    pub outcome: ControlLeaseTransitionOutcome,
    pub lease: Option<ControlLeaseView>,
    pub changed_work_item_ids: Vec<String>,
}

fn bounded(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_CONTROL_LEASE_REF_BYTES {
        return Err(format!("control lease {field} is outside native bounds"));
    }
    Ok(())
}

/// Validate a `GetControlLease` request's own fields.
pub fn validate_control_lease_get(tenant: &str, lease_id: &str) -> Result<(), String> {
    bounded("tenant", tenant)?;
    bounded("lease_id", lease_id)
}

impl IssueControlLeaseRequest {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("tenant", &self.tenant),
            ("lease_id", &self.lease_id),
            ("kind", &self.kind),
            ("idempotency_key", &self.idempotency_key),
        ] {
            bounded(field, value)?;
        }
        let grant_bytes = serde_json::to_vec(&self.grant).map_err(|error| error.to_string())?;
        if grant_bytes.len() > MAX_CONTROL_LEASE_GRANT_BYTES {
            return Err("control lease grant exceeds its native bound".to_string());
        }
        let ordered = 0 < self.issued_at_ms
            && self.issued_at_ms <= self.expires_at_ms
            && self.expires_at_ms <= self.hard_expires_at_ms;
        if !ordered || self.hard_expires_at_ms - self.issued_at_ms > MAX_CONTROL_LEASE_SPAN_MS {
            return Err(
                "control lease timing must satisfy 0 < issued <= expires <= hard_expires \
                 within the native span"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// The stored row of a freshly issued lease (before the row writer stamps
    /// its revision).
    pub fn row(&self) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("node_type".into(), CONTROL_LEASE_NODE_TYPE.into());
        row.insert("tenant".into(), self.tenant.clone().into());
        row.insert("kind".into(), self.kind.clone().into());
        row.insert(
            "status".into(),
            ControlLeaseStatus::Active.as_stored().into(),
        );
        row.insert("grant".into(), Value::Object(self.grant.clone()));
        row.insert("issued_at_ms".into(), self.issued_at_ms.into());
        row.insert("expires_at_ms".into(), self.expires_at_ms.into());
        row.insert("hard_expires_at_ms".into(), self.hard_expires_at_ms.into());
        row
    }
}

impl TransitionControlLeaseRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant)?;
        bounded("lease_id", &self.lease_id)?;
        bounded("idempotency_key", &self.idempotency_key)
    }
}

/// Whether a stored node row is a control lease of `tenant`.
pub fn is_tenant_control_lease(row: &Map<String, Value>, tenant: &str) -> bool {
    is_control_lease_row(row) && row.get("tenant").and_then(Value::as_str) == Some(tenant)
}

/// Whether a stored node row is a control lease of any tenant. Generic node
/// writes consult this to refuse touching native lease authority.
pub fn is_control_lease_row(row: &Map<String, Value>) -> bool {
    row.get("node_type").and_then(Value::as_str) == Some(CONTROL_LEASE_NODE_TYPE)
}

impl ControlLeaseView {
    /// Project a stored control-lease row. The caller has already checked the
    /// row is a lease of the requesting tenant.
    pub fn from_row(lease_id: &str, row: &Map<String, Value>) -> Result<Self, String> {
        let number = |key: &str| row.get(key).and_then(Value::as_u64).unwrap_or(0);
        let text = |key: &str| row.get(key).and_then(Value::as_str).unwrap_or("");
        let status = ControlLeaseStatus::from_stored(text("status"))
            .ok_or_else(|| format!("control lease '{lease_id}' carries an unrecognized status"))?;
        Ok(Self {
            lease_id: lease_id.to_string(),
            kind: text("kind").to_string(),
            status,
            grant: row
                .get("grant")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            issued_at_ms: number("issued_at_ms"),
            expires_at_ms: number("expires_at_ms"),
            hard_expires_at_ms: number("hard_expires_at_ms"),
            revision: number(WORK_ITEM_ROW_REVISION).max(1),
        })
    }
}

#[cfg(test)]
mod tests;
