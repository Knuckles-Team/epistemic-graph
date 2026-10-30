use super::*;
use crate::decision::statistical::QuestionKind;

fn quota() -> RuntimeQuota {
    RuntimeQuota {
        wall_time_ms: 1_800_000,
        memory_bytes: 8_589_934_592,
        process_count: 16,
        grace_ms: 25_000,
    }
}

fn record() -> RunTraceRecord {
    RunTraceRecord {
        run_id: "run:tenant:a".into(),
        task_signature: "sig:tenant:sha256:abcd".into(),
        steps: BoundedVec::new(vec!["step-1".into(), "step-2".into()]).unwrap(),
        outcome: "success".into(),
        started_at: 1_700_000_000_000,
    }
}

// spec: EG-DECISION-ENGINE-R133.1
#[test]
fn quota_round_trips() {
    let quota = quota();
    let wire = serde_json::to_string(&quota).unwrap();
    assert_eq!(serde_json::from_str::<RuntimeQuota>(&wire).unwrap(), quota);

    let record = record();
    let wire = serde_json::to_string(&record).unwrap();
    assert_eq!(
        serde_json::from_str::<RunTraceRecord>(&wire).unwrap(),
        record
    );

    // The question kind that selects this decision, on its wire name.
    let wire = serde_json::to_string(&QuestionKind::RuntimeQuota).unwrap();
    assert_eq!(wire, "\"runtime_quota\"");
    assert_eq!(
        serde_json::from_str::<QuestionKind>(&wire).unwrap(),
        QuestionKind::RuntimeQuota
    );

    // Unknown fields are refused on both records.
    let mut value: serde_json::Value = serde_json::to_value(quota).unwrap();
    value["cpu_percent"] = serde_json::json!(50);
    assert!(serde_json::from_value::<RuntimeQuota>(value).is_err());
    let mut value: serde_json::Value = serde_json::to_value(record).unwrap();
    value["wall_time_ms"] = serde_json::json!(1);
    assert!(serde_json::from_value::<RunTraceRecord>(value).is_err());

    // The step list is bounded on the way in.
    let too_many: Vec<String> = (0..MAX_RUN_TRACE_STEPS + 1)
        .map(|i| format!("step-{i}"))
        .collect();
    assert!(BoundedVec::<String, MAX_RUN_TRACE_STEPS>::new(too_many).is_err());
}
