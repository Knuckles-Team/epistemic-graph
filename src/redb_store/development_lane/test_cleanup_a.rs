use super::*;

#[test]
fn native_finish_retains_charge_and_cleanup_has_a_separate_fence_and_replay_tuple() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:finish"), TEST_NOW));
    let hold = accepted.hold.expect("accepted finish hold");
    let committed = fixture
        .commit_work_item_result("succeeded", false, "finish-terminal", 200)
        .expect("generic terminal WorkItem commit");
    assert!(!committed.replayed);
    let finish_request = DevelopmentLaneFinishRequest {
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
        idempotency_key: "finish:one".into(),
        now_ms: 0,
    };
    let finished: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: finish_request.clone(),
        },
        200,
    ));
    assert_eq!(
        finished.decision,
        DevelopmentLaneFinishResultDecision::Idempotent
    );
    let finished_hold = finished.hold.clone().expect("finished hold");
    assert_eq!(
        finished_hold.state,
        DevelopmentLaneHoldState::CleanupPending
    );
    assert!(!finished_hold.active_count_charged);
    assert_eq!(finished_hold.retained_disk_bytes, 10);
    {
        let handle = fixture
            .shard
            .graph(TEST_GRAPH)
            .expect("bind lane graph scope");
        let read = fixture.shard.read(&handle).expect("lane scoped read");
        let pressure_index = read
            .scoped_owner_table(PRESSURE_INDEX)
            .expect("open retained pressure index");
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
                    Metric::Retained
                )
                .expect("read retained scope pressure"),
                10,
                "retained pressure for {scope:?}"
            );
        }
    }
    let replay: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: finish_request.clone(),
        },
        200,
    ));
    assert_eq!(replay, finished);
    let mut wrong_terminal = finish_request.clone();
    wrong_terminal.idempotency_key = "finish:wrong-terminal".into();
    wrong_terminal.terminal_state = DevelopmentLaneFinishRequestTerminalState::Failed;
    let wrong_terminal: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: wrong_terminal,
        },
        200,
    ));
    assert_eq!(
        wrong_terminal.decision,
        DevelopmentLaneFinishResultDecision::InputConflict
    );

    fixture.seed_cleanup_work_item(&finished_hold, "cleanup:one", "cleanup-fence:one");
    let cleanup_request = DevelopmentLaneCleanupCompleteRequest {
        schema_version: DevelopmentLaneCleanupCompleteRequestSchemaVersion::V1,
        tenant_ref: finished_hold.tenant_ref.clone(),
        work_item_id: finished_hold.work_item_id.clone(),
        owner_id: finished_hold.owner_id.clone(),
        attempt: finished_hold.attempt,
        lease_epoch: finished_hold.lease_epoch,
        fencing_token: finished_hold.fencing_token,
        work_item_fence: finished_hold.work_item_fence.clone(),
        cleanup_work_item_id: "cleanup:one".into(),
        cleanup_work_item_fence: "cleanup-fence:one".into(),
        cleanup_attempt: 1,
        cleanup_lease_epoch: 1,
        cleanup_fencing_token: 1,
        hold_id: finished_hold.hold_id.clone(),
        expected_hold_revision: finished_hold.hold_revision,
        removal_proof_ref: "proof:one".into(),
        idempotency_key: "cleanup:one".into(),
        now_ms: 0,
    };
    let cleaned_bytes = fixture.commit(
        Method::CleanupDevelopmentLane {
            request: cleanup_request.clone(),
        },
        200,
    );
    let cleaned: DevelopmentLaneCleanupCompleteResult = fixture.decode(&cleaned_bytes);
    assert_eq!(
        cleaned.decision,
        DevelopmentLaneCleanupCompleteResultDecision::Accepted
    );
    assert_eq!(
        cleaned.hold.as_ref().map(|value| value.state),
        Some(DevelopmentLaneHoldState::Cleaned)
    );
    let clean_replay_bytes = fixture.commit(
        Method::CleanupDevelopmentLane {
            request: cleanup_request.clone(),
        },
        200,
    );
    assert_eq!(clean_replay_bytes, cleaned_bytes);
    let clean_replay: DevelopmentLaneCleanupCompleteResult = fixture.decode(&clean_replay_bytes);
    assert_eq!(
        clean_replay.decision,
        DevelopmentLaneCleanupCompleteResultDecision::Accepted
    );

    let status_before_fresh = read_development_lane_status(
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
    .expect("status before fresh cleanup replay");
    let mut fresh_replay_request = cleanup_request.clone();
    fresh_replay_request.idempotency_key = "cleanup:fresh-replay".into();
    let fresh_replay: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
        Method::CleanupDevelopmentLane {
            request: fresh_replay_request,
        },
        200,
    ));
    assert_eq!(
        fresh_replay.decision,
        DevelopmentLaneCleanupCompleteResultDecision::Idempotent
    );
    let status_after_fresh = read_development_lane_status(
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
    .expect("status after fresh cleanup replay");
    assert_eq!(status_after_fresh.counters, status_before_fresh.counters);
    assert_eq!(status_after_fresh.holds, status_before_fresh.holds);

    let mut wrong_revision = cleanup_request.clone();
    wrong_revision.idempotency_key = "cleanup:wrong-revision".into();
    wrong_revision.expected_hold_revision = wrong_revision
        .expected_hold_revision
        .checked_sub(1)
        .expect("finished hold revision is nonzero");
    let revision_conflict: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
        Method::CleanupDevelopmentLane {
            request: wrong_revision,
        },
        200,
    ));
    assert_eq!(
        revision_conflict.decision,
        DevelopmentLaneCleanupCompleteResultDecision::InputConflict
    );

    let mut wrong_proof = cleanup_request.clone();
    wrong_proof.idempotency_key = "cleanup:wrong-proof".into();
    wrong_proof.removal_proof_ref = "proof:forged".into();
    let conflict: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
        Method::CleanupDevelopmentLane {
            request: wrong_proof,
        },
        200,
    ));
    assert_eq!(
        conflict.decision,
        DevelopmentLaneCleanupCompleteResultDecision::InputConflict
    );

    let mut wrong_fence = cleanup_request;
    wrong_fence.idempotency_key = "cleanup:wrong-fence".into();
    wrong_fence.cleanup_work_item_fence = "cleanup-fence:forged".into();
    let fence_refusal: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
        Method::CleanupDevelopmentLane {
            request: wrong_fence,
        },
        200,
    ));
    assert_eq!(
        fence_refusal.decision,
        DevelopmentLaneCleanupCompleteResultDecision::WrongFence
    );
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
    .expect("status after cleanup");
    assert_eq!(status.holds.len(), 1);
    assert_eq!(status.tenant_active_count, 0);
    assert_eq!(status.tenant_retained_disk_bytes, 0);

    // A cleaned tombstone remains queryable for bounded replay, so a
    // checkpoint must carry both its terminal lifecycle WorkItem and the
    // exact cleanup WorkItem correlation rather than silently preserving a
    // native row after either node was dropped.
    let cleaned_hold = cleaned.hold.as_ref().expect("cleaned hold link");
    let handle = fixture
        .shard
        .graph(TEST_GRAPH)
        .expect("bind lane graph scope");
    let read = fixture.shard.read(&handle).expect("lane scoped read");
    let nodes = read
        .scoped_owner_table(NODES)
        .expect("open cleaned checkpoint nodes");
    let holds = read
        .scoped_owner_table(HOLDS)
        .expect("open cleaned checkpoint holds");
    let lifecycle = nodes
        .get((TEST_GRAPH, cleaned_hold.work_item_id.as_str()))
        .expect("read cleaned lifecycle WorkItem")
        .expect("cleaned lifecycle WorkItem exists");
    let cleanup = nodes
        .get((TEST_GRAPH, "cleanup:one"))
        .expect("read cleaned cleanup WorkItem")
        .expect("cleaned cleanup WorkItem exists");
    let incoming = vec![
        (
            cleaned_hold.work_item_id.clone(),
            lifecycle.value().to_vec(),
        ),
        ("cleanup:one".to_string(), cleanup.value().to_vec()),
    ];
    validate_checkpoint_lane_links(TEST_GRAPH, &incoming, &holds, DurableCrypto::none())
        .expect("cleaned tombstone has exact linked WorkItems");
    assert!(validate_checkpoint_lane_links(
        TEST_GRAPH,
        &[(
            cleaned_hold.work_item_id.clone(),
            lifecycle.value().to_vec()
        )],
        &holds,
        DurableCrypto::none(),
    )
    .is_err());
}
