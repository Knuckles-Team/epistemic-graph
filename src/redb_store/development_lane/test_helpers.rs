use super::*;
use crate::epistemic_operations::DevelopmentLaneIntentHostTargetKind;

pub(super) fn hold(tenant: &str, host: &str) -> DevelopmentLaneHold {
    DevelopmentLaneHold {
        schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
        hold_id: format!("v1:{}", "a".repeat(64)),
        lane_id: "lane:test".into(),
        tenant_ref: tenant.into(),
        request_id: "request:test".into(),
        work_item_id: "work:test".into(),
        owner_id: "owner:test".into(),
        session_id: "session:test".into(),
        fairness_group: "fairness:test".into(),
        workspace_ref: "workspace:test".into(),
        repository_id: "repository:test".into(),
        base_ref: "refs/heads/main".into(),
        base_sha: "a".repeat(40),
        branch: "branch:test".into(),
        worktree_locator: "lanes/test".into(),
        host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
        host_target_alias: None,
        host_ref: host.into(),
        quota_policy_name: "default".into(),
        quota_policy_version: "1".into(),
        input_fingerprint: format!("v1:{}", "b".repeat(64)),
        predicted_disk_bytes: 10,
        observed_disk_bytes: 0,
        retained_disk_bytes: 0,
        active_count_charged: true,
        quota_charge: empty_charge(1),
        state: DevelopmentLaneHoldState::Active,
        attempt: 1,
        lease_epoch: 1,
        fencing_token: 1,
        work_item_fence: "fence:test".into(),
        hold_revision: 1,
        lifecycle_revision: 1,
        allocation_revision: 1,
        cleanup_revision: 0,
        expires_at_ms: 10_000,
        last_renewed_at_ms: 1_000,
        cleanup_work_item_id: None,
        cleanup_work_item_fence: None,
        cleanup_attempt: None,
        cleanup_lease_epoch: None,
        cleanup_fencing_token: None,
        tombstone: false,
    }
}

pub(super) fn policy() -> DevelopmentLaneQuotaPolicy {
    DevelopmentLaneQuotaPolicy {
        schema_version: crate::epistemic_operations::DevelopmentLaneQuotaPolicySchemaVersion::V1,
        policy_name: "default".into(),
        policy_version: "1".into(),
        tenant_count_limit: 10,
        owner_count_limit: 10,
        session_count_limit: 10,
        workspace_count_limit: 10,
        repository_count_limit: 10,
        host_count_limit: 10,
        global_count_limit: 10,
        tenant_predicted_disk_bytes: 100,
        owner_predicted_disk_bytes: 100,
        session_predicted_disk_bytes: 100,
        workspace_predicted_disk_bytes: 100,
        repository_predicted_disk_bytes: 100,
        host_predicted_disk_bytes: 100,
        global_predicted_disk_bytes: 100,
        tenant_observed_disk_bytes: 100,
        owner_observed_disk_bytes: 100,
        session_observed_disk_bytes: 100,
        workspace_observed_disk_bytes: 100,
        repository_observed_disk_bytes: 100,
        host_observed_disk_bytes: 100,
        global_observed_disk_bytes: 100,
        tenant_retained_disk_bytes: 100,
        owner_retained_disk_bytes: 100,
        session_retained_disk_bytes: 100,
        workspace_retained_disk_bytes: 100,
        repository_retained_disk_bytes: 100,
        host_retained_disk_bytes: 100,
        global_retained_disk_bytes: 100,
        min_ttl_ms: 1,
        max_ttl_ms: 10_000,
        max_observation_staleness_ms: 100,
        drain_only: false,
    }
}

pub(super) fn row(scope: Scope, value: DurableLaneCounter) -> ScopeCounter {
    ScopeCounter {
        key: format!("scope:{scope:?}"),
        scope,
        value,
    }
}

pub(super) const TEST_GRAPH: &str = "graph-a";
pub(super) const TEST_NOW: u64 = 100;

pub(super) fn test_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "eg-development-lane-{label}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ))
}

pub(super) fn test_fingerprint(seed: char) -> String {
    format!("v1:{}", seed.to_string().repeat(64))
}

pub(super) fn test_intent(
    tenant: &str,
    suffix: &str,
    branch: &str,
    worktree_locator: &str,
) -> DevelopmentLaneIntent {
    DevelopmentLaneIntent {
        schema_version: DevelopmentLaneIntentSchemaVersion::V1,
        tenant_ref: tenant.to_string(),
        request_id: format!("request:{suffix}"),
        lane_id: format!("lane:{suffix}"),
        repository_id: "repository:test".into(),
        base_ref: "refs/heads/main".into(),
        base_sha: "a".repeat(40),
        branch: branch.into(),
        host_target_kind: DevelopmentLaneIntentHostTargetKind::Local,
        host_target_alias: None,
        host_ref: "host:test".into(),
        resource_reservation_id: format!("resource:{suffix}"),
        workspace_ref: "workspace:test".into(),
        worktree_locator: worktree_locator.into(),
        owner_id: format!("owner:{suffix}"),
        session_id: format!("session:{suffix}"),
        fairness_group: "fairness:test".into(),
        quota_policy_name: "default".into(),
        quota_policy_version: "1".into(),
        predicted_disk_bytes: 10,
        ttl_ms: 1_000,
        input_fingerprint: test_fingerprint('b'),
    }
}

pub(super) fn test_reserve_request(intent: DevelopmentLaneIntent) -> DevelopmentLaneReserveRequest {
    DevelopmentLaneReserveRequest {
        schema_version: DevelopmentLaneReserveRequestSchemaVersion::V1,
        tenant_ref: intent.tenant_ref.clone(),
        work_item_id: format!("work:{}", intent.request_id.trim_start_matches("request:")),
        owner_id: intent.owner_id.clone(),
        attempt: 1,
        lease_epoch: 1,
        fencing_token: 1,
        work_item_fence: format!("fence:{}", intent.request_id),
        intent,
        idempotency_key: "reserve:initial".into(),
        now_ms: TEST_NOW,
    }
}
