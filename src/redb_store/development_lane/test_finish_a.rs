use super::*;

#[test]
fn native_public_owner_cas_drift_rolls_back_before_terminal_lane_transition() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:owner-cas"), TEST_NOW));
    let hold = accepted.hold.expect("accepted owner binding hold");
    let status_before = read_development_lane_status(
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
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("read owner binding baseline");

    // This is the public compact MutationBatch CAS path, not a direct NODES
    // edit.  A lane-linked WorkItem cannot change its authoritative owner
    // while preserving the lifecycle tuple and intent.
    //
    // RMDD-29's native WorkItem-authority migration
    // (`work_item_capability::validate_generic_method`'s `CompareAndSetNodeFields` arm)
    // now refuses any generic CAS that touches a protected authority field
    // (`lease_owner` is one of `WORK_ITEM_AUTHORITY_KEYS`) on an existing WorkItem node
    // BEFORE the lane's own fence-mismatch check in `apply_mutation_in_wtx` ever runs --
    // a broader, earlier-firing refusal than the lane-specific one this assertion
    // originally named. The invariant this test proves (a foreign owner drift never lands)
    // holds a fortiori.
    let drift = fixture.compare_and_set_work_item_owner(
        &hold.owner_id,
        "owner:foreign",
        "owner-drift",
        200,
    );
    assert_eq!(
        drift.unwrap_err(),
        "native WorkItem authority required for protected field 'lease_owner'"
    );

    let stored = read_one_node(
        &fixture.shard,
        TEST_GRAPH,
        &hold.work_item_id,
        DurableCrypto::none(),
    )
    .expect("read rolled-back WorkItem")
    .expect("linked WorkItem remains present");
    let stored: serde_json::Map<String, serde_json::Value> =
        decode_durable(&stored).expect("decode rolled-back WorkItem");
    assert_eq!(property_string(&stored, "status"), "running");
    assert_eq!(
        property_string(&stored, "lease_owner"),
        hold.owner_id.as_str()
    );

    let status_after_drift = read_development_lane_status(
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
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("read unchanged lane after owner drift");
    assert_eq!(status_after_drift.counters, status_before.counters);
    assert_eq!(status_after_drift.holds, status_before.holds);
    assert_eq!(status_after_drift.tenant_active_count, 1);
    assert_eq!(status_after_drift.tenant_retained_disk_bytes, 0);

    let finish_request =
        |tenant_ref: &str, owner_id: &str, idempotency_key: &str| DevelopmentLaneFinishRequest {
            schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
            tenant_ref: tenant_ref.into(),
            work_item_id: hold.work_item_id.clone(),
            owner_id: owner_id.into(),
            attempt: hold.attempt,
            lease_epoch: hold.lease_epoch,
            fencing_token: hold.fencing_token,
            work_item_fence: hold.work_item_fence.clone(),
            hold_id: hold.hold_id.clone(),
            expected_hold_revision: hold.hold_revision,
            terminal_state: DevelopmentLaneFinishRequestTerminalState::Succeeded,
            idempotency_key: idempotency_key.into(),
            now_ms: 0,
        };
    let foreign_policy =
        fixture.update_policy("tenant:foreign", policy(), 0, "policy:foreign", 200);
    assert_eq!(
        foreign_policy.decision,
        DevelopmentLaneQuotaUpdateResultDecision::Accepted
    );
    let wrong_tenant: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: finish_request("tenant:foreign", &hold.owner_id, "finish:wrong-tenant"),
        },
        200,
    ));
    assert_eq!(
        wrong_tenant.decision,
        DevelopmentLaneFinishResultDecision::WrongTenant
    );
    let wrong_owner: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
        Method::FinishDevelopmentLane {
            request: finish_request("tenant:a", "owner:foreign", "finish:wrong-owner"),
        },
        200,
    ));
    assert_eq!(
        wrong_owner.decision,
        DevelopmentLaneFinishResultDecision::WrongOwner
    );

    // A foreign worker cannot terminalize the hold even after the failed
    // CAS attempt; the WorkItem CAS and lane state remain untouched.
    let foreign_commit = fixture
        .commit_work_item(
            Method::CommitWorkItemResult {
                tenant: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                worker_id: "owner:foreign".into(),
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                idempotency_key: "work-item:foreign-owner".into(),
                outcome: "succeeded".into(),
                result_ref: None,
                outcome_extension: None,
                error_ref: None,
                retryable: false,
                now_ms: 200,
            },
            "foreign-owner",
            200,
        )
        .expect("foreign owner result is durably fenced");
    let foreign_result: serde_json::Value = fixture.decode(
        foreign_commit
            .record
            .result_msgpack
            .as_deref()
            .expect("foreign owner result bytes"),
    );
    assert_eq!(foreign_result["status"], "fenced");
    let status_after_foreign = read_development_lane_status(
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
    .expect("read unchanged lane after foreign result");
    assert_eq!(status_after_foreign.counters, status_before.counters);
    assert_eq!(status_after_foreign.holds, status_before.holds);
    let stored = read_one_node(
        &fixture.shard,
        TEST_GRAPH,
        &hold.work_item_id,
        DurableCrypto::none(),
    )
    .expect("read non-terminal WorkItem")
    .expect("WorkItem remains present");
    let stored: serde_json::Map<String, serde_json::Value> =
        decode_durable(&stored).expect("decode non-terminal WorkItem");
    assert_eq!(property_string(&stored, "status"), "running");
    assert_eq!(
        property_string(&stored, "lease_owner"),
        hold.owner_id.as_str()
    );

    // The original owner still has the exact authority, and its replay is
    // byte-identical after the terminal hold transition.
    let committed = fixture
        .commit_work_item_result("succeeded", false, "owner-correct", 200)
        .expect("owner-correct terminal result");
    assert!(!committed.replayed);
    let replay = fixture
        .commit_work_item_result("succeeded", false, "owner-correct", 200)
        .expect("owner-correct terminal replay");
    assert!(replay.replayed);
    let final_status = read_development_lane_status(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            hold_id: Some(hold.hold_id.clone()),
            lane_id: None,
            work_item_id: None,
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        200,
        DurableCrypto::none(),
    )
    .expect("read terminal lane");
    assert_eq!(final_status.tenant_active_count, 0);
    assert_eq!(final_status.tenant_retained_disk_bytes, 10);
    assert_eq!(
        final_status.holds[0].state,
        DevelopmentLaneHoldState::CleanupPending
    );
    let stored = read_one_node(
        &fixture.shard,
        TEST_GRAPH,
        &hold.work_item_id,
        DurableCrypto::none(),
    )
    .expect("read terminal WorkItem")
    .expect("terminal WorkItem remains present");
    let stored: serde_json::Map<String, serde_json::Value> =
        decode_durable(&stored).expect("decode terminal WorkItem");
    assert_eq!(property_string(&stored, "status"), "succeeded");
    assert!(property_string(&stored, "lease_owner").is_empty());
    assert_eq!(
        property_string(&stored, "last_lease_owner"),
        "owner:initial"
    );
}
