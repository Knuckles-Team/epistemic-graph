use super::*;

#[test]
fn native_policy_cas_sees_non_tenant_scope_totals_without_a_scan() {
    let fixture = NativeLaneFixture::new(policy());
    let first: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:scope-a"), TEST_NOW));
    assert_eq!(first.decision, DevelopmentLaneResultDecision::Accepted);
    let mut second = test_reserve_request(test_intent(
        "tenant:a",
        "scope-b",
        "branch:scope-b",
        "lanes/scope-b",
    ));
    second.owner_id = fixture.reserve.owner_id.clone();
    second.intent.owner_id = fixture.reserve.intent.owner_id.clone();
    second.intent.session_id = fixture.reserve.intent.session_id.clone();
    seed_lane_work_item(&fixture.shard, &second).expect("seed shared-scope candidate");
    let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: DevelopmentLaneReserveRequest {
                idempotency_key: "reserve:scope-b".into(),
                ..second
            },
        },
        TEST_NOW,
    ));
    assert_eq!(second.decision, DevelopmentLaneResultDecision::Accepted);

    {
        let handle = fixture
            .shard
            .graph(TEST_GRAPH)
            .expect("bind lane graph scope");
        let read = fixture.shard.read(&handle).expect("lane scoped read");
        let pressure_index = read
            .scoped_owner_table(PRESSURE_INDEX)
            .expect("open maintained pressure index");
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
                    Metric::Count
                )
                .expect("read maintained count pressure"),
                2,
                "count pressure for {scope:?}"
            );
            assert_eq!(
                pressure_max(
                    &pressure_index,
                    TEST_GRAPH,
                    "tenant:a",
                    scope,
                    Metric::Predicted
                )
                .expect("read maintained predicted pressure"),
                20,
                "predicted pressure for {scope:?}"
            );
            assert_eq!(
                pressure_max(
                    &pressure_index,
                    TEST_GRAPH,
                    "tenant:a",
                    scope,
                    Metric::Observed
                )
                .expect("read maintained observed pressure"),
                0,
                "observed pressure for {scope:?}"
            );
            assert_eq!(
                pressure_max(
                    &pressure_index,
                    TEST_GRAPH,
                    "tenant:a",
                    scope,
                    Metric::Retained
                )
                .expect("read maintained retained pressure"),
                0,
                "retained pressure for {scope:?}"
            );
        }
    }

    let mut drain_policy = policy();
    drain_policy.drain_only = true;
    let global: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
        Method::UpdateDevelopmentLaneQuota {
            request: DevelopmentLaneQuotaUpdateRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                tenant_ref: GLOBAL_POLICY_KEY.into(),
                policy: drain_policy.clone(),
                expected_policy_revision: 1,
                expected_policy_version: Some("1".into()),
                idempotency_key: "policy:scope-global-drain".into(),
                now_ms: 0,
            },
        },
        150,
    ));
    assert_eq!(
        global.decision,
        DevelopmentLaneQuotaUpdateResultDecision::Accepted
    );

    type QuotaReduction = (&'static str, fn(&mut DevelopmentLaneQuotaPolicy));
    let reductions: [QuotaReduction; 10] = [
        ("owner-count", |value| value.owner_count_limit = 1),
        ("session-count", |value| value.session_count_limit = 1),
        ("workspace-count", |value| value.workspace_count_limit = 1),
        ("repository-count", |value| value.repository_count_limit = 1),
        ("host-count", |value| value.host_count_limit = 1),
        ("owner-predicted", |value| {
            value.owner_predicted_disk_bytes = 10
        }),
        ("session-predicted", |value| {
            value.session_predicted_disk_bytes = 10
        }),
        ("workspace-predicted", |value| {
            value.workspace_predicted_disk_bytes = 10
        }),
        ("repository-predicted", |value| {
            value.repository_predicted_disk_bytes = 10
        }),
        ("host-predicted", |value| {
            value.host_predicted_disk_bytes = 10
        }),
    ];
    for (name, reduce) in reductions {
        let mut reduced = drain_policy.clone();
        reduce(&mut reduced);
        let refused: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: "tenant:a".into(),
                    policy: reduced,
                    expected_policy_revision: 1,
                    expected_policy_version: Some("1".into()),
                    idempotency_key: format!("policy:scope-reduction:{name}"),
                    now_ms: 0,
                },
            },
            151,
        ));
        assert_eq!(
            refused.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Quota,
            "reduction {name} must observe maintained pressure"
        );
    }
}
