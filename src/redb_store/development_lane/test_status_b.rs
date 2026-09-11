use super::*;

#[test]
fn native_status_limit_uses_an_extra_row_probe() {
    let fixture = NativeLaneFixture::new(policy());
    let initial: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:initial"), TEST_NOW));
    assert_eq!(initial.decision, DevelopmentLaneResultDecision::Accepted);
    let status = |limit: u64, cursor: Option<String>| {
        read_development_lane_status(
            &fixture.shard,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit,
                cursor,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("read bounded status page")
    };
    let exact = status(1, None);
    assert_eq!(exact.holds.len(), 1);
    assert_eq!(exact.tenant_active_count, 1);
    assert_eq!(exact.holds[0].worktree_locator, REDACTED_PRIVATE_ID);
    assert_eq!(exact.holds[0].host_ref, REDACTED_PRIVATE_ID);
    assert!(exact.holds[0].host_target_alias.is_none());
    assert!(exact.complete);
    assert!(exact.next_cursor.is_none());

    let candidate = fixture.candidate(
        "status-second",
        "branch:status-second",
        "lanes/status-second",
    );
    let accepted: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: DevelopmentLaneReserveRequest {
                idempotency_key: "reserve:status-second".into(),
                ..candidate
            },
        },
        TEST_NOW,
    ));
    assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
    let first_page = status(1, None);
    assert_eq!(first_page.holds.len(), 1);
    assert_eq!(first_page.tenant_active_count, 2);
    assert!(!first_page.complete);
    let cursor = first_page.next_cursor.clone().expect("next status cursor");
    let second_page = status(1, Some(cursor));
    assert_eq!(second_page.holds.len(), 1);
    assert_eq!(second_page.tenant_active_count, 2);
    assert!(second_page.complete);
    assert!(second_page.next_cursor.is_none());
}

#[test]
fn native_last_quota_unit_refuses_without_partial_indexes_or_counters() {
    let mut limited = policy();
    limited.tenant_count_limit = 1;
    limited.global_count_limit = 1;
    let fixture = NativeLaneFixture::new(limited);
    let first: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:limited-a"), TEST_NOW));
    assert_eq!(first.decision, DevelopmentLaneResultDecision::Accepted);
    let second = fixture.candidate("limited-b", "branch:limited-b", "lanes/limited-b");
    let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: DevelopmentLaneReserveRequest {
                idempotency_key: "reserve:limited-b".into(),
                ..second
            },
        },
        TEST_NOW,
    ));
    assert_eq!(second.decision, DevelopmentLaneResultDecision::Quota);
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
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("read bounded status");
    assert_eq!(status.holds.len(), 1);
    assert_eq!(status.tenant_active_count, 1);
    assert_eq!(status.counters.global_count, 1);
}
