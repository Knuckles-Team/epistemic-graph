use super::*;

#[test]
fn native_global_and_tenant_drain_use_monotonic_cas_but_allow_lifecycle_cleanup() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted_bytes = fixture.commit(fixture.reserve_method("reserve:drain-existing"), TEST_NOW);
    let accepted: DevelopmentLaneResult = fixture.decode(&accepted_bytes);
    let hold = accepted.hold.expect("drain test hold");
    let observed: DevelopmentLaneObserveResult = fixture.decode(&fixture.commit(
        Method::ObserveDevelopmentLane {
            request: DevelopmentLaneObserveRequest {
                schema_version: DevelopmentLaneObserveRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: hold.owner_id.clone(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                hold_id: hold.hold_id.clone(),
                expected_hold_revision: hold.hold_revision,
                observed_disk_bytes: 10,
                observation_revision: 1,
                idempotency_key: "observe:drain".into(),
                now_ms: 0,
            },
        },
        150,
    ));
    assert_eq!(
        observed.decision,
        DevelopmentLaneObserveResultDecision::Accepted
    );
    let hold = observed.hold.expect("observed drain hold");
    let mut drain_policy = policy();
    drain_policy.drain_only = true;
    let global_drain: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
        Method::UpdateDevelopmentLaneQuota {
            request: DevelopmentLaneQuotaUpdateRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                tenant_ref: GLOBAL_POLICY_KEY.into(),
                policy: drain_policy.clone(),
                expected_policy_revision: 1,
                expected_policy_version: Some("1".into()),
                idempotency_key: "policy:global-drain".into(),
                now_ms: 0,
            },
        },
        160,
    ));
    assert_eq!(
        global_drain.decision,
        DevelopmentLaneQuotaUpdateResultDecision::Accepted
    );

    // Replay is checked before policy/drain/exclusivity evaluation.  An
    // acknowledgement retry therefore returns the original Accepted
    // bytes, even after the graph has entered drain-only mode.
    assert_eq!(
        fixture.commit(fixture.reserve_method("reserve:drain-existing"), 999),
        accepted_bytes
    );

    let candidate = fixture.candidate("drain-new", "branch:drain-new", "lanes/drain-new");
    let refused: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: DevelopmentLaneReserveRequest {
                idempotency_key: "reserve:drain-new".into(),
                ..candidate
            },
        },
        170,
    ));
    assert_eq!(refused.decision, DevelopmentLaneResultDecision::Drained);

    let tenant_drain: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
        Method::UpdateDevelopmentLaneQuota {
            request: DevelopmentLaneQuotaUpdateRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                policy: drain_policy,
                expected_policy_revision: 1,
                expected_policy_version: Some("1".into()),
                idempotency_key: "policy:tenant-drain".into(),
                now_ms: 0,
            },
        },
        171,
    ));
    assert_eq!(
        tenant_drain.decision,
        DevelopmentLaneQuotaUpdateResultDecision::Accepted
    );
    let stale_tenant: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
        Method::UpdateDevelopmentLaneQuota {
            request: DevelopmentLaneQuotaUpdateRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                policy: policy(),
                expected_policy_revision: 1,
                expected_policy_version: Some("1".into()),
                idempotency_key: "policy:tenant-stale".into(),
                now_ms: 0,
            },
        },
        172,
    ));
    assert_eq!(
        stale_tenant.decision,
        DevelopmentLaneQuotaUpdateResultDecision::Stale
    );

    let renewed: DevelopmentLaneRenewResult = fixture.decode(&fixture.commit(
        Method::RenewDevelopmentLane {
            request: DevelopmentLaneRenewRequest {
                schema_version: DevelopmentLaneRenewRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: hold.owner_id.clone(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                hold_id: hold.hold_id.clone(),
                expected_hold_revision: hold.hold_revision,
                ttl_ms: 1_000,
                idempotency_key: "renew:drain".into(),
                now_ms: 0,
            },
        },
        170,
    ));
    assert_eq!(
        renewed.decision,
        DevelopmentLaneRenewResultDecision::Accepted
    );
    let hold = renewed.hold.expect("renewed drain hold");
    fixture
        .commit_work_item_result("succeeded", false, "finish-drain-terminal", 180)
        .expect("generic terminal WorkItem commit");
    let finished: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: DevelopmentLaneFinishRequest {
                schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: hold.owner_id.clone(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                hold_id: hold.hold_id.clone(),
                expected_hold_revision: hold.hold_revision,
                terminal_state: DevelopmentLaneFinishRequestTerminalState::Succeeded,
                idempotency_key: "finish:drain".into(),
                now_ms: 0,
            },
        },
        180,
    ));
    assert_eq!(
        finished.decision,
        DevelopmentLaneFinishResultDecision::Idempotent
    );
    let hold = finished.hold.expect("finished drain hold");
    fixture.seed_cleanup_work_item(&hold, "cleanup:drain", "cleanup-fence:drain");
    let cleaned: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
        Method::CleanupDevelopmentLane {
            request: DevelopmentLaneCleanupCompleteRequest {
                schema_version: DevelopmentLaneCleanupCompleteRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: hold.owner_id.clone(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                cleanup_work_item_id: "cleanup:drain".into(),
                cleanup_work_item_fence: "cleanup-fence:drain".into(),
                cleanup_attempt: 1,
                cleanup_lease_epoch: 1,
                cleanup_fencing_token: 1,
                hold_id: hold.hold_id.clone(),
                expected_hold_revision: hold.hold_revision,
                removal_proof_ref: "proof:drain".into(),
                idempotency_key: "cleanup:drain".into(),
                now_ms: 0,
            },
        },
        190,
    ));
    assert_eq!(
        cleaned.decision,
        DevelopmentLaneCleanupCompleteResultDecision::Accepted
    );
}

#[test]
fn native_reopen_preserves_exact_hold_and_status_is_tenant_bounded() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:reopen"), TEST_NOW));
    let hold = accepted.hold.expect("reopen hold");
    let path = fixture.close_for_reopen();
    let reopened = Shard::open(&path).expect("reopen lane shard");
    let query = read_development_lane(
        &reopened,
        TEST_GRAPH,
        &DevelopmentLaneQueryRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
            tenant_ref: "tenant:a".into(),
            hold_id: hold.hold_id.clone(),
            now_ms: 0,
        },
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("query after reopen");
    assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
    assert_eq!(query.hold.expect("reopened hold"), hold);
    let isolated = read_development_lane_status(
        &reopened,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: "tenant:other".into(),
            hold_id: None,
            lane_id: None,
            work_item_id: None,
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("tenant-isolated status");
    assert!(isolated.holds.is_empty());
    assert_eq!(isolated.tenant_active_count, 0);
    drop(reopened);
    std::fs::remove_file(path).expect("remove reopened lane database");
}
