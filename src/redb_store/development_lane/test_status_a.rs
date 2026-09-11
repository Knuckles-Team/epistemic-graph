use super::*;

#[test]
fn checkpoint_restore_requires_the_exact_linked_lane_work_item() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:checkpoint"), TEST_NOW));
    let hold = accepted.hold.expect("checkpoint hold");
    let handle = fixture
        .shard
        .graph(TEST_GRAPH)
        .expect("bind lane graph scope");
    let read = fixture.shard.read(&handle).expect("lane scoped read");
    let nodes = read
        .scoped_owner_table(NODES)
        .expect("open checkpoint nodes");
    let holds = read
        .scoped_owner_table(HOLDS)
        .expect("open checkpoint holds");
    assert!(
        validate_checkpoint_lane_links(TEST_GRAPH, &[], &holds, DurableCrypto::none()).is_err()
    );
    let node = nodes
        .get((TEST_GRAPH, hold.work_item_id.as_str()))
        .expect("read linked checkpoint WorkItem")
        .expect("linked checkpoint WorkItem exists");
    let incoming = vec![(hold.work_item_id.clone(), node.value().to_vec())];
    validate_checkpoint_lane_links(TEST_GRAPH, &incoming, &holds, DurableCrypto::none())
        .expect("exact linked WorkItem preserves lane authority");
    let mut stale_props: serde_json::Map<String, serde_json::Value> =
        decode_durable(node.value()).expect("decode checkpoint WorkItem");
    stale_props.insert("status".into(), serde_json::json!("succeeded"));
    let stale_bytes = rmp_serde::to_vec_named(&stale_props).expect("encode stale WorkItem");
    assert!(validate_checkpoint_lane_links(
        TEST_GRAPH,
        &[(hold.work_item_id, stale_bytes)],
        &holds,
        DurableCrypto::none()
    )
    .is_err());
}

#[test]
fn native_same_branch_and_worktree_races_leave_one_winner() {
    let fixture = NativeLaneFixture::new(policy());
    let first = fixture.candidate("race-a", "branch:race", "lanes/race-a");
    let second = fixture.candidate("race-b", "branch:race", "lanes/race-b");
    let first_for_indexes = first.clone();
    let second_for_indexes = second.clone();
    let shard = &fixture.shard;
    let start = Arc::new(Barrier::new(3));
    let (first_decision, second_decision) = std::thread::scope(|scope| {
        let first_start = Arc::clone(&start);
        let first_thread = scope.spawn(move || {
            first_start.wait();
            let mut request = first;
            request.idempotency_key = "reserve:race-a".into();
            let bytes = commit_development_lane(
                shard,
                TEST_GRAPH,
                &Method::ReserveDevelopmentLane { request },
                TEST_NOW,
                DurableCrypto::none(),
            )
            .expect("branch race transaction");
            rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                .expect("decode branch race result")
                .decision
        });
        let second_start = Arc::clone(&start);
        let second_thread = scope.spawn(move || {
            second_start.wait();
            let mut request = second;
            request.idempotency_key = "reserve:race-b".into();
            let bytes = commit_development_lane(
                shard,
                TEST_GRAPH,
                &Method::ReserveDevelopmentLane { request },
                TEST_NOW,
                DurableCrypto::none(),
            )
            .expect("branch race transaction");
            rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                .expect("decode branch race result")
                .decision
        });
        start.wait();
        (
            first_thread.join().expect("first branch race thread"),
            second_thread.join().expect("second branch race thread"),
        )
    });
    let accepted_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
        &first_for_indexes
    } else {
        &second_for_indexes
    };
    let refused_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
        &second_for_indexes
    } else {
        &first_for_indexes
    };
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
    .expect("read branch race status");
    assert_eq!(status.holds.len(), 1);
    assert_eq!(status.tenant_active_count, 1);
    assert_eq!(status.counters.global_count, 1);
    let expected_hold_id = hold_id(&accepted_request.intent);
    let branch_key = format!(
        "{}\0{}",
        accepted_request.intent.repository_id, accepted_request.intent.branch
    );
    let accepted_worktree_key = format!(
        "{}\0{}\0{}",
        accepted_request.intent.host_ref,
        accepted_request.intent.workspace_ref,
        accepted_request.intent.worktree_locator
    );
    let refused_worktree_key = format!(
        "{}\0{}\0{}",
        refused_request.intent.host_ref,
        refused_request.intent.workspace_ref,
        refused_request.intent.worktree_locator
    );
    let handle = fixture
        .shard
        .graph(TEST_GRAPH)
        .expect("bind lane graph scope");
    let read = fixture.shard.read(&handle).expect("lane scoped read");
    let branch_index = read
        .scoped_owner_table(REPOSITORY_BRANCH_INDEX)
        .expect("open branch race index");
    let worktree_index = read
        .scoped_owner_table(WORKTREE_INDEX)
        .expect("open worktree race index");
    let lane_index = read
        .scoped_owner_table(LANE_INDEX)
        .expect("open lane race index");
    let work_item_index = read
        .scoped_owner_table(WORK_ITEM_INDEX)
        .expect("open WorkItem race index");
    let counters = read
        .scoped_owner_table(COUNTERS)
        .expect("open race counters");
    let pressure_index = read
        .scoped_owner_table(PRESSURE_INDEX)
        .expect("open race pressure index");
    assert_eq!(
        branch_index
            .get((TEST_GRAPH, "tenant:a", branch_key.as_str()))
            .expect("read branch winner")
            .map(|value| value.value().to_string()),
        Some(expected_hold_id.clone())
    );
    assert_eq!(
        worktree_index
            .get((TEST_GRAPH, accepted_worktree_key.as_str()))
            .expect("read worktree winner")
            .map(|value| value.value().to_string()),
        Some(expected_hold_id.clone())
    );
    assert!(worktree_index
        .get((TEST_GRAPH, refused_worktree_key.as_str()))
        .expect("read worktree refusal")
        .is_none());
    assert_eq!(
        lane_index
            .get((
                TEST_GRAPH,
                accepted_request.intent.tenant_ref.as_str(),
                accepted_request.intent.lane_id.as_str(),
            ))
            .expect("read lane winner")
            .map(|value| value.value().to_string()),
        Some(expected_hold_id.clone())
    );
    assert!(lane_index
        .get((
            TEST_GRAPH,
            refused_request.intent.tenant_ref.as_str(),
            refused_request.intent.lane_id.as_str(),
        ))
        .expect("read refused lane index")
        .is_none());
    assert!(work_item_index
        .get((
            TEST_GRAPH,
            refused_request.work_item_id.as_str(),
            refused_request.attempt
        ))
        .expect("read refused WorkItem index")
        .is_none());
    for key in [
        "tenant\0tenant:a\0tenant:a".to_string(),
        format!("owner\0tenant:a\0{}", accepted_request.intent.owner_id),
        format!("session\0tenant:a\0{}", accepted_request.intent.session_id),
        format!(
            "workspace\0tenant:a\0{}",
            accepted_request.intent.workspace_ref
        ),
        format!(
            "repository\0tenant:a\0{}",
            accepted_request.intent.repository_id
        ),
        format!("host\0tenant:a\0{}", accepted_request.intent.host_ref),
        "global\0*".to_string(),
    ] {
        let counter = counters
            .get((TEST_GRAPH, key.as_str()))
            .expect("read exact race counter")
            .map(|value| {
                resource_decode::<DurableLaneCounter>(value.value(), DurableCrypto::none())
            })
            .transpose()
            .expect("decode exact race counter")
            .expect("race counter exists");
        assert_eq!(counter.active_count, 1, "counter {key}");
        assert_eq!(counter.predicted_disk_bytes, 10, "counter {key}");
        assert_eq!(counter.observed_disk_bytes, 0, "counter {key}");
        assert_eq!(counter.retained_disk_bytes, 0, "counter {key}");
    }
    assert_eq!(
        pressure_max(
            &pressure_index,
            TEST_GRAPH,
            "tenant:a",
            Scope::Workspace,
            Metric::Count
        )
        .expect("read workspace pressure"),
        1
    );
    assert_eq!(
        pressure_max(
            &pressure_index,
            TEST_GRAPH,
            "tenant:a",
            Scope::Workspace,
            Metric::Predicted
        )
        .expect("read workspace predicted pressure"),
        10
    );
    let accepted = [first_decision, second_decision]
        .into_iter()
        .filter(|decision| *decision == DevelopmentLaneResultDecision::Accepted)
        .count();
    let exclusive = [first_decision, second_decision]
        .into_iter()
        .filter(|decision| *decision == DevelopmentLaneResultDecision::Exclusivity)
        .count();
    assert_eq!(accepted, 1);
    assert_eq!(exclusive, 1);

    let fixture = NativeLaneFixture::new(policy());
    let first = fixture.candidate("tree-a", "branch:tree-a", "lanes/same");
    let second = fixture.candidate("tree-b", "branch:tree-b", "lanes/same");
    let first_for_indexes = first.clone();
    let second_for_indexes = second.clone();
    let shard = &fixture.shard;
    let start = Arc::new(Barrier::new(3));
    let (first_decision, second_decision) = std::thread::scope(|scope| {
        let first_start = Arc::clone(&start);
        let first_thread = scope.spawn(move || {
            first_start.wait();
            let mut request = first;
            request.idempotency_key = "reserve:tree-a".into();
            let bytes = commit_development_lane(
                shard,
                TEST_GRAPH,
                &Method::ReserveDevelopmentLane { request },
                TEST_NOW,
                DurableCrypto::none(),
            )
            .expect("worktree race transaction");
            rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                .expect("decode worktree race result")
                .decision
        });
        let second_start = Arc::clone(&start);
        let second_thread = scope.spawn(move || {
            second_start.wait();
            let mut request = second;
            request.idempotency_key = "reserve:tree-b".into();
            let bytes = commit_development_lane(
                shard,
                TEST_GRAPH,
                &Method::ReserveDevelopmentLane { request },
                TEST_NOW,
                DurableCrypto::none(),
            )
            .expect("worktree race transaction");
            rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                .expect("decode worktree race result")
                .decision
        });
        start.wait();
        (
            first_thread.join().expect("first worktree race thread"),
            second_thread.join().expect("second worktree race thread"),
        )
    });
    let accepted_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
        &first_for_indexes
    } else {
        &second_for_indexes
    };
    let expected_hold_id = hold_id(&accepted_request.intent);
    let worktree_key = format!(
        "{}\0{}\0{}",
        accepted_request.intent.host_ref,
        accepted_request.intent.workspace_ref,
        accepted_request.intent.worktree_locator
    );
    let handle = fixture
        .shard
        .graph(TEST_GRAPH)
        .expect("bind lane graph scope");
    let read = fixture.shard.read(&handle).expect("lane scoped read");
    let worktree_index = read
        .scoped_owner_table(WORKTREE_INDEX)
        .expect("open worktree race index");
    assert_eq!(
        worktree_index
            .get((TEST_GRAPH, worktree_key.as_str()))
            .expect("read worktree race winner")
            .map(|value| value.value().to_string()),
        Some(expected_hold_id)
    );
    assert_eq!(
        [first_decision, second_decision]
            .into_iter()
            .filter(|decision| *decision == DevelopmentLaneResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        [first_decision, second_decision]
            .into_iter()
            .filter(|decision| *decision == DevelopmentLaneResultDecision::Exclusivity)
            .count(),
        1
    );
}
