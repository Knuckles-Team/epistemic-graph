//! The Gap's write methods after creation -- transition, settle, price -- and
//! the answer every market write returns. Each rule is a pure function of the
//! stored Gap (and, for settle, the stored WorkItem row); the store applies it
//! inside the durable WorkItem MutationBatch.

use serde::{Deserialize, Serialize};

use super::gap::{GapEvidence, GapView};
use super::offer::{
    work_offer_utility_rate, WorkOffer, WorkOfferView, DEFAULT_COST_FLOOR_MICROUNITS,
    WORK_OFFER_UTILITY_RATE_STAGE,
};
use super::{bounded, evidence_digest, GapStatus, MAX_MARKET_REF_BYTES};
use crate::work_item_read::WorkItemStatus;

/// How an upsert resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GapUpsertOutcome {
    /// A new Gap and its first WorkItem.
    Created,
    /// New evidence folded into a live Gap.
    Merged,
    /// New evidence reopened a closed Gap with a new WorkItem.
    Reopened,
    /// Nothing the Gap had not already seen; nothing was written.
    Unchanged,
}

/// Result of `GapUpsert`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapUpserted {
    pub outcome: GapUpsertOutcome,
    pub gap: GapView,
    /// Whether this upsert admitted the Gap's current WorkItem.
    pub work_item_created: bool,
    pub changed_work_item_ids: Vec<String>,
}

/// The state a transition moves a Gap to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GapTransitionTarget {
    /// A specification is in flight; `reference` names it.
    Specified,
    /// Closed on evidence; `reference` names it.
    Resolved,
    /// Parked; `reference` names why.
    Deferred,
}

/// Every legal `(from, to)` edge. Anything absent is a `conflict`.
const LEGAL_TRANSITIONS: [(GapStatus, GapTransitionTarget); 5] = [
    (GapStatus::Open, GapTransitionTarget::Specified),
    (GapStatus::Open, GapTransitionTarget::Resolved),
    (GapStatus::Open, GapTransitionTarget::Deferred),
    (GapStatus::Specified, GapTransitionTarget::Resolved),
    (GapStatus::Specified, GapTransitionTarget::Deferred),
];

impl GapTransitionTarget {
    pub fn status(self) -> GapStatus {
        match self {
            Self::Specified => GapStatus::Specified,
            Self::Resolved => GapStatus::Resolved,
            Self::Deferred => GapStatus::Deferred,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Specified => "specified",
            Self::Resolved => "resolved",
            Self::Deferred => "deferred",
        }
    }

    /// Whether a Gap currently in `from` may move to this target.
    pub fn allowed_from(self, from: GapStatus) -> bool {
        LEGAL_TRANSITIONS.contains(&(from, self))
    }
}

/// `GapTransition`: move a Gap along a legal edge, compare-and-set on the
/// revision the caller read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapTransitionRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub gap_id: String,
    pub expected_revision: u64,
    pub to: GapTransitionTarget,
    /// The specification, resolution evidence or deferral reason.
    pub reference: String,
    pub idempotency_key: String,
}

/// How a transition resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GapTransitionOutcome {
    Applied,
    /// The edge is not legal from the Gap's status, or it moved past the
    /// caller's revision.
    Conflict,
    /// No Gap with this id is visible to the tenant.
    NotFound,
}

/// Result of `GapTransition`. On `conflict` the CURRENT view is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapTransitioned {
    pub outcome: GapTransitionOutcome,
    pub gap: Option<GapView>,
    pub changed_work_item_ids: Vec<String>,
}

/// `GapSettle`: record the Gap's current WorkItem outcome as evidence. The
/// engine reads the outcome from the WorkItem row; the caller names only the
/// Gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapSettleRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub gap_id: String,
    pub idempotency_key: String,
}

/// How a settle resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GapSettleOutcome {
    /// The WorkItem succeeded; the Gap is resolved on its evidence.
    Resolved,
    /// The WorkItem ended without success; the Gap is parked until new
    /// evidence reopens it.
    Deferred,
    /// The outcome was recorded; the Gap had already been closed another way.
    Recorded,
    /// The WorkItem has not reached a terminal state; nothing was written.
    Pending,
    /// This WorkItem's outcome is already on the Gap; nothing was written.
    Unchanged,
    /// No Gap with this id is visible to the tenant.
    NotFound,
}

/// Result of `GapSettle`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapSettled {
    pub outcome: GapSettleOutcome,
    pub gap: Option<GapView>,
    pub changed_work_item_ids: Vec<String>,
}

/// `WorkOfferPut`: price the Gap's current WorkItem, compare-and-set on the
/// offer version the caller read (0 when the Gap had none).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkOfferPutRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub gap_id: String,
    pub expected_offer_version: u64,
    pub offer: WorkOffer,
    pub idempotency_key: String,
}

/// How an offer write resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkOfferPutOutcome {
    Applied,
    /// The Gap is not live, or its offer moved past the caller's version.
    Conflict,
    /// No Gap with this id is visible to the tenant.
    NotFound,
}

/// Result of `WorkOfferPut`. On `conflict` the CURRENT Gap is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkOfferRecorded {
    pub outcome: WorkOfferPutOutcome,
    pub gap: Option<GapView>,
    pub changed_work_item_ids: Vec<String>,
}

/// The terminal facts of a Gap's WorkItem, read from its stored row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledWorkItem {
    pub work_item_id: String,
    pub status: WorkItemStatus,
    /// `outcome_ref`, else `result_ref`, else `error_ref` -- whichever the
    /// terminal commit recorded first in that order.
    pub reference: String,
}

/// What a WorkItem's state means for its Gap. A table so the mapping is
/// read off one list; a status absent from it is not terminal.
const SETTLEMENTS: [(WorkItemStatus, GapSettleOutcome, GapStatus); 4] = [
    (
        WorkItemStatus::Succeeded,
        GapSettleOutcome::Resolved,
        GapStatus::Resolved,
    ),
    (
        WorkItemStatus::Failed,
        GapSettleOutcome::Deferred,
        GapStatus::Deferred,
    ),
    (
        WorkItemStatus::Cancelled,
        GapSettleOutcome::Deferred,
        GapStatus::Deferred,
    ),
    (
        WorkItemStatus::DeadLetter,
        GapSettleOutcome::Deferred,
        GapStatus::Deferred,
    ),
];

/// The evidence kind a settled WorkItem outcome is recorded under.
pub const WORK_ITEM_OUTCOME_EVIDENCE: &str = "work_item_outcome";

impl GapTransitionRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant)?;
        bounded("gap_id", &self.gap_id)?;
        bounded("reference", &self.reference)?;
        bounded("idempotency_key", &self.idempotency_key)
    }
}

impl GapSettleRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant)?;
        bounded("gap_id", &self.gap_id)?;
        bounded("idempotency_key", &self.idempotency_key)
    }
}

impl WorkOfferPutRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant)?;
        bounded("gap_id", &self.gap_id)?;
        bounded("idempotency_key", &self.idempotency_key)?;
        self.offer.validate()
    }
}

/// Validate a `GapGet` request's own fields.
pub fn validate_gap_get(tenant: &str, gap_id: &str) -> Result<(), String> {
    bounded("tenant", tenant)?;
    if gap_id.len() > MAX_MARKET_REF_BYTES {
        return Err("work market gap_id is outside native bounds".to_string());
    }
    bounded("gap_id", gap_id)
}

impl GapView {
    /// Apply a transition request: `true` when it moved the Gap.
    pub fn transition(&mut self, request: &GapTransitionRequest, now_ms: u64) -> bool {
        if !request.to.allowed_from(self.status) || self.revision != request.expected_revision {
            return false;
        }
        match request.to {
            GapTransitionTarget::Specified => {
                if !self.spec_refs.contains(&request.reference) {
                    self.spec_refs.push(request.reference.clone());
                }
                if self.spec_refs.len() > super::gap::MAX_GAP_SPEC_REFS {
                    self.spec_refs.remove(0);
                }
            }
            GapTransitionTarget::Resolved | GapTransitionTarget::Deferred => {
                let label = request.to.label();
                self.record_evidence(GapEvidence {
                    digest: evidence_digest(&[label, &request.gap_id, &request.reference]),
                    kind: label.to_string(),
                    reference: request.reference.clone(),
                    generation: self.generation,
                    recorded_at_ms: now_ms,
                });
            }
        }
        self.status = request.to.status();
        self.updated_at_ms = now_ms;
        true
    }

    /// Record a WorkItem's terminal outcome as evidence and close the Gap on
    /// it when the Gap is still live.
    pub fn settle(&mut self, item: &SettledWorkItem, now_ms: u64) -> GapSettleOutcome {
        let Some((effect, closed)) = SETTLEMENTS
            .iter()
            .find(|(status, _, _)| *status == item.status)
            .map(|(_, effect, closed)| (*effect, *closed))
        else {
            return GapSettleOutcome::Pending;
        };
        let digest = evidence_digest(&[
            WORK_ITEM_OUTCOME_EVIDENCE,
            &item.work_item_id,
            item.status.as_stored(),
            &item.reference,
        ]);
        if self.has_evidence(&digest) {
            return GapSettleOutcome::Unchanged;
        }
        self.record_evidence(GapEvidence {
            digest,
            kind: WORK_ITEM_OUTCOME_EVIDENCE.to_string(),
            reference: item.work_item_id.clone(),
            generation: self.generation,
            recorded_at_ms: now_ms,
        });
        self.updated_at_ms = now_ms;
        if !self.status.is_live() {
            return GapSettleOutcome::Recorded;
        }
        self.status = closed;
        effect
    }

    /// Record a priced offer for the Gap's current WorkItem. `Err` when the
    /// offer cites evidence the Gap does not hold; `Ok(false)` on a version or
    /// liveness conflict.
    pub fn price(&mut self, request: &WorkOfferPutRequest, now_ms: u64) -> Result<bool, String> {
        if let Some(digest) = request
            .offer
            .evidence_digests
            .iter()
            .find(|digest| !self.has_evidence(digest))
        {
            return Err(format!(
                "work offer cites evidence '{digest}' the Gap does not hold"
            ));
        }
        let current = self.offer_version;
        if !self.status.is_live() || current != request.expected_offer_version {
            return Ok(false);
        }
        let utility_rate = work_offer_utility_rate(&request.offer, DEFAULT_COST_FLOOR_MICROUNITS)
            .ok_or_else(|| {
            "work offer utility rate does not fit the native range".to_string()
        })?;
        self.offer_version = current + 1;
        self.offer = Some(WorkOfferView {
            version: current + 1,
            generation: self.generation,
            work_item_id: self.work_item_id.clone(),
            offer: request.offer.clone(),
            utility_rate,
            stage: WORK_OFFER_UTILITY_RATE_STAGE.to_string(),
            offered_at_ms: now_ms,
        });
        self.updated_at_ms = now_ms;
        Ok(true)
    }
}
