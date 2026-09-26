//! Hand-written native control-plane wire contracts.
//!
//! These DTOs intentionally extend the existing WorkItem/control operation
//! family instead of introducing a second scheduler authority.  They are kept
//! outside the generated AU operation projection because the current catalog
//! generator cannot yet express the bounded, nested capacity demand/fence
//! shapes.  The redb authority validates every bound again; serde attributes
//! are not the security boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::capacity_lease::{CapacityCell, CapacityLease, CapacityResourceClass, LeasePriority};
use crate::epistemic_operations::RequestContext;

pub const NATIVE_CONTROL_SCHEMA_VERSION: u16 = 1;
pub const MAX_CAPACITY_DEMANDS: usize = 16;
pub const MAX_CAPACITY_MUTATION_BATCH: usize = 16;
pub const MAX_CAPACITY_RECLAIM_BATCH: usize = 128;
pub const MAX_CAPACITY_STATUS_ROWS: usize = 128;
pub const MAX_CAPACITY_ID_BYTES: usize = 512;
pub const MAX_CAPACITY_AMOUNT: u64 = 1_000_000_000;
pub const MAX_CAPACITY_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_CAPACITY_BUDGET: u64 = 1_000_000_000_000;
pub const MAX_SUBMIT_BATCH: usize = 128;
pub const MAX_SUBMIT_DEPENDENCIES: usize = 1_024;
pub const MAX_SUBMIT_BATCH_CHANGED_IDS: usize = 4_096;
pub const MAX_SUBMIT_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_SUBMIT_PROVENANCE_REFS: usize = 64;
pub const MAX_SUBMIT_REF_BYTES: usize = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NativeControlSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CapacityDecision {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "replayed")]
    Replayed,
    #[serde(rename = "released")]
    Released,
    #[serde(rename = "renewed")]
    Renewed,
    #[serde(rename = "reclaimed")]
    Reclaimed,
    #[serde(rename = "exhausted")]
    Exhausted,
    #[serde(rename = "stale_epoch")]
    StaleEpoch,
    #[serde(rename = "stale_fence")]
    StaleFence,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "idempotency_conflict")]
    IdempotencyConflict,
    #[serde(rename = "invalid")]
    Invalid,
    #[serde(rename = "backpressure")]
    Backpressure,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityDemand {
    pub cell_id: String,
    pub resource_class: CapacityResourceClass,
    pub amount: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityLeaseFence {
    pub lease_id: String,
    pub lease_epoch: u64,
    pub fence_token: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityAcquireRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    /// Opaque owner digest.  The server compares this to the authenticated
    /// principal/agent binding; callers cannot choose a different owner.
    pub owner_digest: String,
    pub idempotency_key: String,
    pub priority: LeasePriority,
    pub demands: Vec<CapacityDemand>,
    pub lease_id: Option<String>,
    pub ttl_ms: u64,
    pub now_ms: u64,
    pub cost_budget_micros: Option<u64>,
    pub token_budget: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityLeaseMutationRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub tenant_ref: String,
    pub owner_digest: String,
    pub leases: Vec<CapacityLeaseFence>,
    pub now_ms: u64,
    pub ttl_ms: Option<u64>,
    pub idempotency_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityReclaimRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub tenant_ref: String,
    pub cell_id: Option<String>,
    pub max_count: u32,
    pub now_ms: u64,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityStatusRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub tenant_ref: String,
    pub cell_id: Option<String>,
    pub lease_id: Option<String>,
    pub max_count: u32,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityCellUpdateRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub cell: CapacityCell,
    pub expected_epoch: Option<u64>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityAcquireResult {
    pub schema_version: NativeControlSchemaVersion,
    pub decision: CapacityDecision,
    pub leases: Vec<CapacityLease>,
    pub available: Vec<CapacityAvailability>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityMutationResult {
    pub schema_version: NativeControlSchemaVersion,
    pub decision: CapacityDecision,
    pub leases: Vec<CapacityLease>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityAvailability {
    pub cell_id: String,
    pub resource_class: CapacityResourceClass,
    pub available: u64,
    pub requested: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityReclaimResult {
    pub schema_version: NativeControlSchemaVersion,
    pub decision: CapacityDecision,
    pub reclaimed_lease_ids: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityStatusResult {
    pub schema_version: NativeControlSchemaVersion,
    pub cells: Vec<CapacityCell>,
    pub leases: Vec<CapacityLease>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CapacityCellUpdateResult {
    pub schema_version: NativeControlSchemaVersion,
    pub decision: CapacityDecision,
    pub cell: CapacityCell,
    pub message: Option<String>,
}

/// `ThrottleCapacityCell` (EH-406): report one error-budget window for one
/// cell of the request graph.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityThrottleRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub cell_id: String,
    pub sample: crate::capacity_throttle::ErrorBudgetSample,
    /// Authority-owned: stamped by the engine, never trusted from a caller.
    pub now_ms: u64,
}

impl CapacityThrottleRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != NativeControlSchemaVersion::V1 {
            return Err("capacity throttle schema_version must be 1".to_string());
        }
        if self.cell_id.trim().is_empty() || self.cell_id.len() > MAX_CAPACITY_ID_BYTES {
            return Err("throttle cell_id is outside native bounds".to_string());
        }
        self.sample.validate(self.now_ms)
    }
}

/// Result of `ThrottleCapacityCell`: the cell as it now stands and the step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityThrottleResult {
    pub schema_version: NativeControlSchemaVersion,
    pub cell: CapacityCell,
    pub action: crate::capacity_throttle::ThrottleActionRecord,
}

/// Native WorkItem admission request.  `context.tenant_id` and
/// `context.graph` are checked against the verified request envelope; they are
/// not caller-controlled routing hints.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SubmitWorkItemRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub context: RequestContext,
    pub work_item_id: Option<String>,
    pub idempotency_key: String,
    pub command_digest: String,
    pub kind: String,
    pub priority: i64,
    pub depends_on: Vec<String>,
    pub input_ref: String,
    pub policy_digest: String,
    pub catalog_digest: String,
    pub model_digest: String,
    pub max_attempts: u64,
    pub deadline_unix: Option<f64>,
    pub metadata: BTreeMap<String, Value>,
    pub provenance_refs: Vec<String>,
    /// Tenant admission cap. The engine applies a bounded default when zero;
    /// it never permits a caller to raise the hard engine maximum.
    pub max_tenant_in_flight: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SubmitWorkItemsRequest {
    pub schema_version: NativeControlSchemaVersion,
    pub context: RequestContext,
    pub idempotency_key: String,
    pub requests: Vec<SubmitWorkItemRequest>,
    /// Engine-owned repository enrichment budget debit. Present only for an
    /// authenticated outbox consumer; ordinary WorkItem batches omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enrichment_budget: Option<EnrichmentBudgetReservation>,
}

/// Compare-and-swap reservation on the immutable source snapshot's global
/// compute budget. The native submit transaction applies this debit and all
/// child WorkItems together or neither. A capacity lease is a separate live
/// resource fence and cannot replace this spending ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichmentBudgetReservation {
    pub schema_version: NativeControlSchemaVersion,
    pub snapshot_digest: String,
    pub source_envelope: String,
    pub page_key: String,
    pub expected_next_index: u32,
    pub end_index: u32,
    pub expected_page_number: u32,
    pub expected_remaining_units: u64,
    pub reserve_units: u64,
    pub total_budget_units: u64,
}

/// Authoritative read of one repository snapshot's global compute budget.
/// `None` from the read method means no page has yet been admitted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichmentBudgetCheckpoint {
    pub schema_version: u8,
    pub tenant_id: String,
    pub snapshot_digest: String,
    pub source_envelope: String,
    pub next_index: u32,
    pub page_number: u32,
    pub reserved_units: u64,
    pub spent_units: u64,
    pub remaining_units: u64,
    pub total_budget_units: u64,
    pub last_page_key: String,
}

/// Durable reason this graph's enrichment outbox consumer must stop claiming.
/// The marker is written before a held outbox lease is released; it is never
/// an acknowledgement, retry, dead-letter, or authority to increase budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichmentBudgetPark {
    pub schema_version: u8,
    pub source_envelope: String,
    pub snapshot_digest: String,
    pub next_index: u32,
    pub page_number: u32,
    pub remaining_units: u64,
    pub required_units: u64,
    pub policy_digest: String,
    pub parked_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SubmitWorkItemResult {
    pub schema_version: NativeControlSchemaVersion,
    pub work_item_id: String,
    pub status: String,
    pub created: bool,
    pub replayed: bool,
    pub command_sequence: u64,
    pub idempotency_key: String,
    pub dependency_count: u32,
    pub admitted_count: u64,
    pub max_tenant_in_flight: u64,
    pub outbox_id: String,
    pub command_digest: String,
    pub provenance_refs: Vec<String>,
    pub changed_work_item_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SubmitWorkItemsResult {
    pub schema_version: NativeControlSchemaVersion,
    pub results: Vec<SubmitWorkItemResult>,
    pub replayed: bool,
    pub outbox_id: String,
    pub changed_work_item_ids: Vec<String>,
}
