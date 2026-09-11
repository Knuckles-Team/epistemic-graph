use super::*;

#[test]
fn global_scope_is_graph_wide_and_worktree_is_host_scoped() {
    let a = hold("tenant:a", "host:a");
    let mut b = a.clone();
    b.tenant_ref = "tenant:b".into();
    b.host_ref = "host:b".into();
    assert_eq!(scope_key(Scope::Global, &a), "global\0*");
    assert_eq!(scope_key(Scope::Global, &a), scope_key(Scope::Global, &b));
    assert_ne!(scope_key(Scope::Tenant, &a), scope_key(Scope::Tenant, &b));
    assert_ne!(worktree_key(&a), worktree_key(&b));
}

#[test]
fn lane_index_is_tenant_scoped_for_reused_lane_ids() {
    let fixture = NativeLaneFixture::new(policy());
    let tenant_b_policy =
        fixture.update_policy("tenant:b", policy(), 0, "policy:tenant-b", TEST_NOW);
    assert_eq!(
        tenant_b_policy.decision,
        crate::epistemic_operations::DevelopmentLaneQuotaUpdateResultDecision::Accepted
    );

    let mut first = test_reserve_request(test_intent(
        "tenant:a",
        "shared-a",
        "branch:shared-a",
        "lanes/shared-a",
    ));
    first.intent.lane_id = "lane:shared".into();
    seed_lane_work_item(&fixture.shard, &first).expect("seed tenant-a shared lane");
    let mut second = test_reserve_request(test_intent(
        "tenant:b",
        "shared-b",
        "branch:shared-b",
        "lanes/shared-b",
    ));
    second.intent.lane_id = "lane:shared".into();
    seed_lane_work_item(&fixture.shard, &second).expect("seed tenant-b shared lane");

    let first_result: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: first.clone(),
        },
        TEST_NOW,
    ));
    let second_result: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: second.clone(),
        },
        TEST_NOW,
    ));
    assert_eq!(
        first_result.decision,
        DevelopmentLaneResultDecision::Accepted
    );
    assert_eq!(
        second_result.decision,
        DevelopmentLaneResultDecision::Accepted
    );

    let first_hold = first_result.hold.expect("tenant-a shared hold");
    let second_hold = second_result.hold.expect("tenant-b shared hold");
    fixture.with_read(|read| {
        let lane_index = read
            .scoped_owner_table(LANE_INDEX)
            .expect("open lane index");
        assert_eq!(
            lane_index
                .get((TEST_GRAPH, "tenant:a", "lane:shared"))
                .expect("read tenant-a lane")
                .map(|value| value.value().to_string()),
            Some(first_hold.hold_id)
        );
        assert_eq!(
            lane_index
                .get((TEST_GRAPH, "tenant:b", "lane:shared"))
                .expect("read tenant-b lane")
                .map(|value| value.value().to_string()),
            Some(second_hold.hold_id)
        );
    });
}

#[test]
fn observation_freshness_is_per_hold_and_boundary_exact() {
    let mut row = DurableLaneHold {
        hold: hold("tenant:a", "host:a"),
        observation_revision: 0,
        last_observed_at_ms: None,
        terminal_state: None,
        terminal_expected_hold_revision: None,
        cleanup_removal_proof_ref: None,
        cleanup_expected_hold_revision: None,
        terminal_source_attempt: None,
        terminal_source_lease_epoch: None,
        terminal_source_fencing_token: None,
        terminal_source_work_item_fence: None,
        resource_reservation_id: "reservation:test".into(),
        ttl_ms: 1_000,
    };
    let policy = policy();
    assert!(!observation_fresh(&row, &policy, 1_000));
    row.last_observed_at_ms = Some(900);
    assert!(observation_fresh(&row, &policy, 1_000));
    assert!(!observation_fresh(&row, &policy, 1_001));
    row.last_observed_at_ms = Some(1_001);
    assert!(!observation_fresh(&row, &policy, 1_000));
    let mut fresh = row;
    fresh.last_observed_at_ms = Some(1_000);
    let stale = DurableLaneHold {
        hold: hold("tenant:a", "host:a"),
        observation_revision: 0,
        last_observed_at_ms: None,
        terminal_state: None,
        terminal_expected_hold_revision: None,
        cleanup_removal_proof_ref: None,
        cleanup_expected_hold_revision: None,
        terminal_source_attempt: None,
        terminal_source_lease_epoch: None,
        terminal_source_fencing_token: None,
        terminal_source_work_item_fence: None,
        resource_reservation_id: "reservation:stale".into(),
        ttl_ms: 1_000,
    };
    assert!(observation_fresh(&fresh, &policy, 1_000));
    assert!(!observation_fresh(&stale, &policy, 1_000));
}

#[test]
fn observed_and_retained_pressure_are_separate_from_prediction() {
    let mut policy = policy();
    let mut observed = DurableLaneCounter {
        observed_disk_bytes: 101,
        ..DurableLaneCounter::default()
    };
    let counters = vec![row(Scope::Tenant, observed.clone())];
    assert_eq!(
        reserve_counter_check(&counters, &policy, 1),
        Err(LaneDecision::Quota)
    );
    observed.observed_disk_bytes = 0;
    observed.retained_disk_bytes = 101;
    assert_eq!(
        reserve_counter_check(&[row(Scope::Tenant, observed)], &policy, 1),
        Err(LaneDecision::Quota)
    );
    policy.tenant_observed_disk_bytes = 200;
    assert!(reserve_counter_check(
        &[row(Scope::Tenant, DurableLaneCounter::default())],
        &policy,
        1
    )
    .is_ok());
    let overflowing = DurableLaneCounter {
        active_count: u64::MAX,
        ..DurableLaneCounter::default()
    };
    assert_eq!(
        reserve_counter_check(&[row(Scope::Tenant, overflowing)], &policy, 1),
        Err(LaneDecision::Quota)
    );
}

#[test]
fn checked_counter_arithmetic_refuses_overflow_and_underflow() {
    assert_eq!(
        adjust(u64::MAX, 1, true, "test"),
        Err("development lane test counter overflow".into())
    );
    assert_eq!(
        adjust(0, 1, false, "test"),
        Err("development lane test counter underflow".into())
    );
}

#[test]
fn checkpoint_restore_status_mapping_is_exact_for_retained_states() {
    let active = DurableLaneHold {
        hold: hold("tenant:a", "host:a"),
        observation_revision: 0,
        last_observed_at_ms: None,
        terminal_state: None,
        terminal_expected_hold_revision: None,
        cleanup_removal_proof_ref: None,
        cleanup_expected_hold_revision: None,
        terminal_source_attempt: None,
        terminal_source_lease_epoch: None,
        terminal_source_fencing_token: None,
        terminal_source_work_item_fence: None,
        resource_reservation_id: "reservation:active".into(),
        ttl_ms: 1_000,
    };
    assert!(checkpoint_lifecycle_status_matches(&active, "running"));
    assert!(!checkpoint_lifecycle_status_matches(&active, "ready"));
    assert!(!checkpoint_lifecycle_status_matches(&active, "succeeded"));

    let mut cleanup_pending = active.clone();
    cleanup_pending.hold.state = DevelopmentLaneHoldState::CleanupPending;
    cleanup_pending.hold.tombstone = true;
    cleanup_pending.hold.active_count_charged = false;
    cleanup_pending.hold.retained_disk_bytes = 10;
    cleanup_pending.terminal_state = Some("succeeded".into());
    cleanup_pending.terminal_expected_hold_revision = Some(1);
    assert!(checkpoint_lifecycle_status_matches(
        &cleanup_pending,
        "succeeded"
    ));
    assert!(!checkpoint_lifecycle_status_matches(
        &cleanup_pending,
        "failed"
    ));
    assert!(!checkpoint_lifecycle_status_matches(
        &cleanup_pending,
        "pending"
    ));

    let mut expired = active.clone();
    expired.hold.state = DevelopmentLaneHoldState::Expired;
    expired.hold.tombstone = true;
    expired.hold.active_count_charged = false;
    expired.hold.retained_disk_bytes = 10;
    assert!(checkpoint_lifecycle_status_matches(&expired, "running"));
    assert!(checkpoint_lifecycle_status_matches(&expired, "succeeded"));
    expired.terminal_state = Some("succeeded".into());
    assert!(!checkpoint_lifecycle_status_matches(&expired, "succeeded"));

    let mut released = cleanup_pending.clone();
    released.hold.state = DevelopmentLaneHoldState::Released;
    assert!(checkpoint_lifecycle_status_matches(&released, "succeeded"));
    released.terminal_state = Some("failed".into());
    assert!(!checkpoint_lifecycle_status_matches(&released, "succeeded"));

    let mut cleaned = released.clone();
    cleaned.hold.state = DevelopmentLaneHoldState::Cleaned;
    cleaned.hold.retained_disk_bytes = 0;
    cleaned.terminal_state = Some("succeeded".into());
    assert!(checkpoint_lifecycle_status_matches(&cleaned, "succeeded"));
    assert!(!checkpoint_lifecycle_status_matches(&cleaned, "running"));
}
