use super::DurableLaneCounter;
use crate::epistemic_operations::DevelopmentLaneIntent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LaneDecision {
    Accepted,
    Idempotent,
    Stale,
    Conflict,
    InputConflict,
    Quota,
    Policy,
    Drained,
    NotFound,
    WrongKind,
    WrongTenant,
    WrongOwner,
    WrongAttempt,
    WrongLeaseEpoch,
    WrongFence,
    Expired,
    Terminal,
    CleanupRequired,
    Exclusivity,
    Invalid,
}

#[derive(Debug, Clone)]
pub(super) struct LaneWorkItem {
    pub(super) status: String,
    pub(super) terminal: bool,
    pub(super) lease_expires_at_ms: u64,
    pub(super) host_ref: String,
    pub(super) resource_reservation_id: String,
    pub(super) cleanup_hold_id: Option<String>,
    pub(super) cleanup_lane_id: Option<String>,
    pub(super) cleanup_expected_hold_revision: Option<u64>,
    pub(super) lane_intent: Option<DevelopmentLaneIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Scope {
    Tenant,
    Owner,
    Session,
    Workspace,
    Repository,
    Host,
    Global,
}

#[derive(Debug, Clone)]
pub(super) struct ScopeCounter {
    pub(super) key: String,
    pub(super) scope: Scope,
    pub(super) value: DurableLaneCounter,
}
