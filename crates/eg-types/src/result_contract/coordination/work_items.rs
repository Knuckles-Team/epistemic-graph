//! WorkItem lease transition result bodies of the `coordination` domain.

use serde::{Deserialize, Serialize};

/// Why a `RenewWorkItemLease` renewed nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LeaseRenewalRefusal {
    /// No WorkItem has this id.
    Missing,
    /// The caller no longer holds the live lease it named.
    Fenced,
}

/// `RenewWorkItemLease`. A renewal carries the renewed lease; a refusal carries only
/// its reason. Every outcome names the WorkItem rows it changed, so the serving
/// projection can be refreshed from authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemLeaseRenewal {
    pub renewed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<LeaseRenewalRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at_ms: Option<u64>,
    pub changed_work_item_ids: Vec<String>,
}

/// The status a `CommitWorkItemResult` answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemCommitStatus {
    /// No WorkItem with this id is visible to the tenant.
    Missing,
    /// The WorkItem was already terminal; nothing changed.
    Noop,
    /// The caller no longer holds the live lease it named.
    Fenced,
    Succeeded,
    Failed,
    Cancelled,
    /// A retryable failure that exhausted its attempts.
    DeadLetter,
    /// A retryable failure below its attempt ceiling, rescheduled with backoff.
    RetryScheduled,
}

/// The status a `CancelWorkItem` answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemCancelStatus {
    /// No WorkItem with this id is visible to the tenant.
    Missing,
    /// The WorkItem was already terminal; nothing changed.
    Noop,
    /// The WorkItem is running under a live lease and cannot be cancelled now.
    InFlight,
    /// The WorkItem is in a state cancellation does not apply to.
    NotCancellable,
    Cancelled,
}

/// A terminal-direction WorkItem transition (`CommitWorkItemResult`,
/// `CancelWorkItem`). The WorkItem id and fence are present once the item was found;
/// the fence is present only when the transition applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemTransition<Status> {
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    pub changed_work_item_ids: Vec<String>,
}

/// The status a `DeferWorkItem` answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemDeferStatus {
    /// No WorkItem with this id exists.
    Missing,
    /// The caller no longer holds the live lease it named.
    Fenced,
    /// The lease was released and the WorkItem returned to `ready` for a later retry.
    Deferred,
}

/// `DeferWorkItem`. A deferral carries the bumped fence and the retry schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemDeferral {
    pub status: WorkItemDeferStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defer_count: Option<u64>,
    pub changed_work_item_ids: Vec<String>,
}
