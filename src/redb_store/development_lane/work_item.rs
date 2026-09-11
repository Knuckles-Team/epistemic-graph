use super::super::{decode_durable, DurableCrypto};
use super::links::{lane_cleanup_value, lane_intent_value};
use super::reserve_support::work_item_kind_name;
use super::types::{LaneDecision, LaneWorkItem};
use crate::epistemic_operations::{DevelopmentLaneCleanupIntent, DevelopmentLaneIntent};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

// Private redb-transaction-scoped helper: every parameter is a distinct
// required assertion against the borrowed row (table handle, identity,
// fencing, and expected-state checks) with no natural grouping that would
// not just reintroduce the same fields (several borrow the table's own
// lifetime) behind an extra indirection. No external/public callers.
/// The projection this module keeps from a validated WorkItem row: the
/// lifecycle intent's placement, or the cleanup correlation.
pub(super) struct WorkItemProjection {
    host_ref: String,
    resource_reservation_id: String,
    cleanup: Option<DevelopmentLaneCleanupIntent>,
    lane_intent: Option<DevelopmentLaneIntent>,
}

/// Unseal and decode one WorkItem node's properties.
pub(super) fn load_work_item_props(
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<serde_json::Map<String, serde_json::Value>, LaneDecision> {
    let bytes = nodes
        .get((graph, work_item_id))
        .map_err(|_| LaneDecision::Invalid)?
        .map(|value| {
            crypto
                .unseal(value.value())
                .map_err(|_| LaneDecision::Invalid)
        })
        .transpose()?
        .ok_or(LaneDecision::NotFound)?;
    decode_durable(&bytes).map_err(|_| LaneDecision::Invalid)
}

/// The row must be a WorkItem, of this tenant, of the expected kind.
///
/// `kind` and `work_item_fence` are the only frozen WorkItem projection
/// fields.  Do not search generic aliases or nested metadata: an echoed
/// `work_item_kind`/`fence` must never become an authority claim.
pub(super) fn work_item_identity_decision(
    props: &serde_json::Map<String, serde_json::Value>,
    tenant: &str,
    expected_kind: DevelopmentLaneWorkItemKind,
) -> Result<(), LaneDecision> {
    if super::super::property_string(props, "node_type") != "WorkItem" {
        return Err(LaneDecision::NotFound);
    }
    if super::super::property_string(props, "tenant") != tenant {
        return Err(LaneDecision::WrongTenant);
    }
    if super::super::property_string(props, "kind") != work_item_kind_name(expected_kind) {
        return Err(LaneDecision::WrongKind);
    }
    Ok(())
}

/// The caller's attempt/lease/fence tuple must be non-zero and must be the
/// row's current tuple.  Each mismatch keeps its own decision.
pub(super) fn work_item_tuple_decision(
    props: &serde_json::Map<String, serde_json::Value>,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: &str,
) -> Result<(), LaneDecision> {
    if attempt == 0 || lease_epoch == 0 || fencing_token == 0 || work_item_fence.is_empty() {
        return Err(LaneDecision::Invalid);
    }
    if super::super::property_u64(props, "attempt") != attempt {
        return Err(LaneDecision::WrongAttempt);
    }
    if super::super::property_u64(props, "lease_epoch") != lease_epoch {
        return Err(LaneDecision::WrongLeaseEpoch);
    }
    if super::super::property_u64(props, "fencing_token") != fencing_token {
        return Err(LaneDecision::WrongFence);
    }
    if super::super::property_string(props, "work_item_fence") != work_item_fence {
        return Err(LaneDecision::WrongFence);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(super) enum WorkItemTerminalExpectation {
    Terminal,
    Live,
}

/// Prove the row's terminality against what the caller expects and, for a
/// live row, that its lease claim has not expired.  Returns the lease expiry.
pub(super) fn work_item_lease_decision(
    props: &serde_json::Map<String, serde_json::Value>,
    status: &str,
    terminal: bool,
    expectation: WorkItemTerminalExpectation,
    now_ms: u64,
) -> Result<u64, LaneDecision> {
    let expects_terminal = matches!(expectation, WorkItemTerminalExpectation::Terminal);
    if expects_terminal != terminal {
        // Both directions of this mismatch (expected terminal but the row
        // isn't yet, or expected non-terminal but it already finished) share
        // the one coarse `Terminal` decision, matching this function's other
        // checks (e.g. `WrongFence` above covers two distinct mismatches).
        return Err(LaneDecision::Terminal);
    }
    if !terminal && !matches!(status, "leased" | "running") {
        // A future lease timestamp on a ready/pending row is not a claim.  A
        // lane lifecycle or cleanup action must be tied to the WorkItem's
        // current live claim, not merely to an echoed fence tuple.
        return Err(LaneDecision::WrongOwner);
    }
    let lease_expires_at_ms = (super::super::property_f64(props, "lease_expires_at") * 1_000.0)
        .max(0.0)
        .min(u64::MAX as f64) as u64;
    if !terminal && lease_expires_at_ms <= now_ms {
        return Err(LaneDecision::Expired);
    }
    Ok(lease_expires_at_ms)
}

/// A terminal or non-live row is owned by its last lease owner; a live one by
/// its current lease owner.
pub(super) fn work_item_owner_decision(
    props: &serde_json::Map<String, serde_json::Value>,
    owner: Option<&str>,
    status: &str,
    terminal: bool,
) -> Result<(), LaneDecision> {
    let Some(owner) = owner else {
        return Ok(());
    };
    let current_owner = if terminal || !matches!(status, "leased" | "running") {
        super::super::property_string(props, "last_lease_owner")
    } else {
        super::super::property_string(props, "lease_owner")
    };
    if current_owner != owner {
        return Err(LaneDecision::WrongOwner);
    }
    Ok(())
}

/// A lifecycle WorkItem carries the typed lane intent, which must agree with
/// the caller's tenant/owner and, when supplied, the exact expected intent.
pub(super) fn work_item_lifecycle_projection(
    props: &serde_json::Map<String, serde_json::Value>,
    tenant: &str,
    owner: Option<&str>,
    expected_intent: Option<&DevelopmentLaneIntent>,
) -> Result<WorkItemProjection, LaneDecision> {
    let stored = lane_intent_value(props)?;
    if stored.tenant_ref != tenant {
        return Err(LaneDecision::WrongTenant);
    }
    if owner.is_some_and(|expected| stored.owner_id != expected) {
        return Err(LaneDecision::WrongOwner);
    }
    if expected_intent.is_some_and(|expected| stored != *expected) {
        return Err(LaneDecision::InputConflict);
    }
    Ok(WorkItemProjection {
        host_ref: stored.host_ref.clone(),
        resource_reservation_id: stored.resource_reservation_id.clone(),
        cleanup: None,
        lane_intent: Some(stored),
    })
}

/// A cleanup WorkItem carries the typed cleanup correlation, which must name
/// the exact hold/lane/revision when the caller supplies one.
pub(super) fn work_item_cleanup_projection(
    props: &serde_json::Map<String, serde_json::Value>,
    expected_cleanup: Option<(&str, &str, u64)>,
) -> Result<WorkItemProjection, LaneDecision> {
    let correlation = lane_cleanup_value(props)?;
    if let Some((hold_id, lane_id, expected_revision)) = expected_cleanup {
        if correlation.hold_id != hold_id
            || correlation.lane_id != lane_id
            || correlation.expected_hold_revision != expected_revision
        {
            return Err(LaneDecision::InputConflict);
        }
    }
    Ok(WorkItemProjection {
        host_ref: String::new(),
        resource_reservation_id: String::new(),
        cleanup: Some(correlation),
        lane_intent: None,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_work_item(
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    tenant: &str,
    owner: Option<&str>,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: &str,
    expected_kind: DevelopmentLaneWorkItemKind,
    require_terminal: bool,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
    expected_intent: Option<&DevelopmentLaneIntent>,
    expected_cleanup: Option<(&str, &str, u64)>,
) -> Result<LaneWorkItem, LaneDecision> {
    let props = load_work_item_props(nodes, graph, work_item_id, crypto)?;
    work_item_identity_decision(&props, tenant, expected_kind)?;
    work_item_tuple_decision(&props, attempt, lease_epoch, fencing_token, work_item_fence)?;
    let status = super::super::property_string(&props, "status").to_string();
    let terminal = matches!(
        status.as_str(),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    );
    let expectation = if require_terminal {
        WorkItemTerminalExpectation::Terminal
    } else {
        WorkItemTerminalExpectation::Live
    };
    let lease_expires_at_ms =
        work_item_lease_decision(&props, &status, terminal, expectation, now_ms)?;
    work_item_owner_decision(&props, owner, &status, terminal)?;
    let projection = match expected_kind {
        DevelopmentLaneWorkItemKind::Lifecycle => {
            work_item_lifecycle_projection(&props, tenant, owner, expected_intent)?
        }
        DevelopmentLaneWorkItemKind::Cleanup => {
            work_item_cleanup_projection(&props, expected_cleanup)?
        }
    };
    let cleanup = projection.cleanup;
    Ok(LaneWorkItem {
        status,
        terminal,
        lease_expires_at_ms,
        host_ref: projection.host_ref,
        resource_reservation_id: projection.resource_reservation_id,
        cleanup_hold_id: cleanup.as_ref().map(|value| value.hold_id.clone()),
        cleanup_lane_id: cleanup.as_ref().map(|value| value.lane_id.clone()),
        cleanup_expected_hold_revision: cleanup.as_ref().map(|value| value.expected_hold_revision),
        lane_intent: projection.lane_intent,
    })
}
