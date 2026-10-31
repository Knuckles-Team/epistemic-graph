use super::*;

// spec: MEDULLA-DURABLE-KERNEL-R028.5
#[test]
fn current_plan_status_matches_scaffold_contract() {
    assert_eq!(
        serde_json::to_string(&PlanStatus::Done).unwrap(),
        "\"done\""
    );
    assert!(serde_json::from_str::<PlanStatus>("\"completed\"").is_err());
    let mut request = sample_publish_request();
    assert!(request.validate().is_ok());
    request.snapshot.items.push(PlanItem {
        text: " ".into(),
        status: PlanStatus::Pending,
    });
    assert!(request.validate().is_err());
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5.1
#[test]
fn current_plan_task_plan_validation_and_legacy_roundtrip() {
    for request in [sample_publish_request(), sample_task_plan_publish_request()] {
        assert!(request.validate().is_ok());
        let json = serde_json::to_value(&request).unwrap();
        if request.snapshot.task_plan.is_none() {
            assert!(json.get("task_plan_proposal").is_none());
            assert!(json["snapshot"].get("task_plan").is_none());
        }
        assert_eq!(
            serde_json::from_value::<PublishCurrentPlanRequest>(json).unwrap(),
            request
        );
        let named = rmp_serde::to_vec_named(&request).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<PublishCurrentPlanRequest>(&named).unwrap(),
            request
        );
    }
    // Legacy rows predate both optional fields. Read them using the new DTO.
    let legacy = serde_json::json!({
        "run_id": "run", "revision": 0, "observed_at_ms": 1,
        "expires_at_ms": 2, "items": []
    });
    assert_eq!(
        serde_json::from_value::<CurrentPlanSnapshot>(legacy.clone()).unwrap(),
        sample_publish_request().snapshot
    );
    let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
    assert_eq!(
        rmp_serde::from_slice::<CurrentPlanSnapshot>(&bytes).unwrap(),
        sample_publish_request().snapshot
    );
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5.1
#[test]
fn current_plan_task_plan_rejects_unpaired_or_changed_content() {
    let request = sample_task_plan_publish_request();
    let mut unpaired = request.clone();
    unpaired.task_plan_proposal = None;
    assert!(unpaired.validate().is_err());
    unpaired = request.clone();
    unpaired.snapshot.task_plan = None;
    assert!(unpaired.validate().is_err());
    for (field, value) in [
        ("output_task_id", serde_json::json!("a")),
        ("max_concurrency", serde_json::json!(1)),
        ("max_steps", serde_json::json!(4)),
        ("total_tasks", serde_json::json!(2)),
        ("policy_revision", serde_json::json!("untrusted")),
        (
            "policy_digest",
            serde_json::json!(format!("sha256:{}", "0".repeat(64))),
        ),
        (
            "input_digest",
            serde_json::json!(format!("sha256:{}", "1".repeat(64))),
        ),
        (
            "plan_digest",
            serde_json::json!(format!("sha256:{}", "2".repeat(64))),
        ),
    ] {
        let mut changed = serde_json::to_value(&request).unwrap();
        changed["snapshot"]["task_plan"][field] = value;
        let changed: PublishCurrentPlanRequest = serde_json::from_value(changed).unwrap();
        assert!(changed.validate().is_err(), "{field} must be recomputed");
    }
    for pointer in [
        "/snapshot/task_plan/tasks/0/prompt",
        "/task_plan_proposal/tasks/0/prompt",
    ] {
        let mut changed = serde_json::to_value(&request).unwrap();
        *changed.pointer_mut(pointer).unwrap() = serde_json::json!("Changed work");
        assert!(serde_json::from_value::<PublishCurrentPlanRequest>(changed)
            .unwrap()
            .validate()
            .is_err());
    }
    let mut reordered = request.clone();
    reordered.snapshot.task_plan.as_mut().unwrap().tasks =
        request.task_plan_proposal.as_ref().unwrap().tasks.clone();
    assert!(
        reordered.validate().is_err(),
        "noncanonical task order must refuse"
    );
}

// spec: MEDULLA-DURABLE-KERNEL-R028.5
#[test]
fn current_plan_identifier_byte_boundary_and_error_precedence() {
    let mut request = sample_publish_request();
    request.tenant = "é".repeat(256);
    assert!(request.validate().is_ok());
    request.tenant.push('x');
    request.snapshot.expires_at_ms = 0;
    assert_eq!(
        request.validate().unwrap_err(),
        "current-plan identifier is empty or oversized"
    );
    request.tenant = "tenant".into();
    request.snapshot.items.push(PlanItem {
        text: " ".into(),
        status: PlanStatus::Pending,
    });
    assert_eq!(
        request.validate().unwrap_err(),
        "current-plan validity interval is empty"
    );
    request.snapshot.expires_at_ms = 2;
    assert_eq!(
        request.validate().unwrap_err(),
        "current-plan items require nonblank text"
    );
}
