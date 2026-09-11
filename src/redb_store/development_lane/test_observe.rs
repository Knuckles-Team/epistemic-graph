use super::*;

#[test]
fn native_observe_replaces_exact_delta_and_renew_checks_each_hold_freshness() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:observe"), TEST_NOW));
    let hold = accepted.hold.expect("accepted observation hold");
    let observe = |hold: &DevelopmentLaneHold,
                   observed_disk_bytes: u64,
                   observation_revision: u64,
                   expected_hold_revision: u64,
                   key: &str,
                   now_ms: u64|
     -> DevelopmentLaneObserveResult {
        let request = DevelopmentLaneObserveRequest {
            schema_version: DevelopmentLaneObserveRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            work_item_id: hold.work_item_id.clone(),
            owner_id: hold.owner_id.clone(),
            attempt: hold.attempt,
            lease_epoch: hold.lease_epoch,
            fencing_token: hold.fencing_token,
            work_item_fence: hold.work_item_fence.clone(),
            hold_id: hold.hold_id.clone(),
            expected_hold_revision,
            observed_disk_bytes,
            observation_revision,
            idempotency_key: key.into(),
            now_ms,
        };
        fixture.decode(&fixture.commit(Method::ObserveDevelopmentLane { request }, now_ms))
    };
    let first = observe(&hold, 20, 1, hold.hold_revision, "observe:20", 200);
    assert_eq!(
        first.decision,
        DevelopmentLaneObserveResultDecision::Accepted
    );
    let hold = first.hold.expect("first observation hold");
    assert_eq!(hold.observed_disk_bytes, 20);
    assert_eq!(hold.hold_revision, 2);
    let stale = observe(&hold, 10, 2, hold.hold_revision, "observe:lower", 201);
    assert_eq!(stale.decision, DevelopmentLaneObserveResultDecision::Stale);
    let replacement = observe(&hold, 30, 3, hold.hold_revision, "observe:30", 200);
    assert_eq!(
        replacement.decision,
        DevelopmentLaneObserveResultDecision::Accepted
    );
    let hold = replacement.hold.expect("replacement observation hold");
    assert_eq!(hold.observed_disk_bytes, 30);
    let status = read_development_lane_status(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: "tenant:a".into(),
            hold_id: None,
            lane_id: None,
            work_item_id: None,
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        200,
        DurableCrypto::none(),
    )
    .expect("status after observation");
    assert_eq!(status.counters.tenant_observed_disk_bytes, 30);
    {
        let handle = fixture
            .shard
            .graph(TEST_GRAPH)
            .expect("bind lane graph scope");
        let read = fixture.shard.read(&handle).expect("lane scoped read");
        let pressure_index = read
            .scoped_owner_table(PRESSURE_INDEX)
            .expect("open observed pressure index");
        for scope in [
            Scope::Owner,
            Scope::Session,
            Scope::Workspace,
            Scope::Repository,
            Scope::Host,
        ] {
            assert_eq!(
                pressure_max(
                    &pressure_index,
                    TEST_GRAPH,
                    "tenant:a",
                    scope,
                    Metric::Observed
                )
                .expect("read observed scope pressure"),
                30,
                "observed pressure for {scope:?}"
            );
        }
    }

    let second_request = fixture.candidate("never-observed", "branch:never", "lanes/never");
    let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: DevelopmentLaneReserveRequest {
                idempotency_key: "reserve:never-observed".into(),
                ..second_request
            },
        },
        TEST_NOW,
    ));
    assert_eq!(second.decision, DevelopmentLaneResultDecision::Accepted);
    let second_hold = second.hold.expect("never-observed hold");

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
                idempotency_key: "renew:fresh".into(),
                now_ms: 0,
            },
        },
        200,
    ));
    assert_eq!(
        renewed.decision,
        DevelopmentLaneRenewResultDecision::Accepted
    );
    let stale_never_observed: DevelopmentLaneRenewResult = fixture.decode(&fixture.commit(
        Method::RenewDevelopmentLane {
            request: DevelopmentLaneRenewRequest {
                schema_version: DevelopmentLaneRenewRequestSchemaVersion::V1,
                tenant_ref: second_hold.tenant_ref.clone(),
                work_item_id: second_hold.work_item_id.clone(),
                owner_id: second_hold.owner_id.clone(),
                attempt: second_hold.attempt,
                lease_epoch: second_hold.lease_epoch,
                fencing_token: second_hold.fencing_token,
                work_item_fence: second_hold.work_item_fence.clone(),
                hold_id: second_hold.hold_id.clone(),
                expected_hold_revision: second_hold.hold_revision,
                ttl_ms: 1_000,
                idempotency_key: "renew:never-observed".into(),
                now_ms: 0,
            },
        },
        200,
    ));
    assert_eq!(
        stale_never_observed.decision,
        DevelopmentLaneRenewResultDecision::Stale
    );
}

#[test]
fn native_retryable_failure_refuses_before_leaving_an_active_hold() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:retry"), TEST_NOW));
    let hold = accepted.hold.expect("accepted retry hold");

    let fenced = fixture
        .commit_work_item_result_with_tuple(
            hold.lease_epoch,
            hold.fencing_token + 1,
            "succeeded",
            false,
            "wrong-work-item-fence",
            200,
        )
        .expect("wrong WorkItem fence response");
    let fenced_result: serde_json::Value = rmp_serde::from_slice(
        fenced
            .record
            .result_msgpack
            .as_deref()
            .expect("fenced WorkItem result bytes"),
    )
    .expect("decode fenced WorkItem result");
    assert_eq!(fenced_result["status"], "fenced");

    let error = fixture
        .commit_work_item_result("failed", true, "retryable-failure", 200)
        .expect_err("retryable failure must not orphan an active lane hold");
    assert_eq!(error, ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM);

    let status = read_development_lane_status(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            hold_id: Some(hold.hold_id.clone()),
            lane_id: None,
            work_item_id: Some(hold.work_item_id.clone()),
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        200,
        DurableCrypto::none(),
    )
    .expect("read unchanged lane after refused retry");
    assert_eq!(status.tenant_active_count, 1);
    assert_eq!(status.tenant_retained_disk_bytes, 0);
    assert_eq!(status.holds[0].state, DevelopmentLaneHoldState::Active);

    // The transaction rolled back the local ready/epoch mutation, so the
    // exact original worker tuple can still terminalize the WorkItem and
    // release the linked hold atomically.
    let committed = fixture
        .commit_work_item_result("succeeded", false, "retryable-recovery", 200)
        .expect("terminal recovery after refused retry");
    assert!(!committed.replayed);
    let status = read_development_lane_status(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref,
            hold_id: Some(hold.hold_id),
            lane_id: None,
            work_item_id: None,
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        200,
        DurableCrypto::none(),
    )
    .expect("read terminalized lane after recovery");
    assert_eq!(status.tenant_active_count, 0);
    assert_eq!(status.tenant_retained_disk_bytes, 10);
    assert_eq!(
        status.holds[0].state,
        DevelopmentLaneHoldState::CleanupPending
    );
}
