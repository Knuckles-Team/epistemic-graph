use super::*;

fn row(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().cloned().unwrap()
}

fn snapshot(sealed_at: u64) -> serde_json::Map<String, serde_json::Value> {
    row(serde_json::json!({
        "type": "AnalysisSnapshot", "analysisDigest": "sha256:ab", "record": "{}",
        "sealedAtMs": sealed_at,
    }))
}

#[test]
fn a_policy_names_retirable_classes_with_positive_retentions() {
    let policy = parse_policy(" AnalysisSnapshot=1000 ,").unwrap();
    assert_eq!(policy.len(), 1);
    assert_eq!(policy[0].class.name, "AnalysisSnapshot");
    assert_eq!(policy[0].retention_ms, 1_000);
    assert!(parse_policy("").unwrap().is_empty());
    for bad in [
        "Doc=1000",
        "SealedRecordTombstone=1000",
        "AnalysisSnapshot",
        "AnalysisSnapshot=soon",
        "AnalysisSnapshot=0",
    ] {
        assert!(parse_policy(bad).is_err(), "{bad}");
    }
}

#[test]
fn an_expired_record_owes_a_deterministic_retirement() {
    let policy = parse_policy("AnalysisSnapshot=1000").unwrap();
    let due = due_retirement("t", "g", "snap", &snapshot(500), &policy, 1_500).unwrap();
    assert_eq!(due.digest, "sha256:ab");
    assert_eq!(
        due.retired_at_ms, 1_500,
        "retired at its expiry instant, not the sweep's clock"
    );
    assert_eq!(due.retired_by, RETENTION_ACTOR);
    assert_eq!(due.idempotency_key, "sealed-retention:g:snap:sha256:ab");
    assert!(due.reason.starts_with("expired: AnalysisSnapshot"));
    due.validate().unwrap();
    let later = due_retirement("t", "g", "snap", &snapshot(500), &policy, 9_999).unwrap();
    assert_eq!(later, due, "a later sweep replays the same request");
}

#[test]
fn nothing_else_is_due() {
    let policy = parse_policy("AnalysisSnapshot=1000").unwrap();
    let mut unstamped = snapshot(0);
    unstamped.remove("sealedAtMs");
    let tombstone = row(serde_json::json!({
        "type": "SealedRecordTombstone", "digest": "sha256:ab", "retired_at_ms": 1,
    }));
    let doc = row(serde_json::json!({"type": "Doc", "sealedAtMs": 1}));
    for (label, stored, now) in [
        ("young", snapshot(500), 1_499),
        ("unstamped", unstamped, 9_999),
        ("tombstone", tombstone, 9_999),
        ("ordinary", doc, 9_999),
    ] {
        assert!(
            due_retirement("t", "g", "n", &stored, &policy, now).is_none(),
            "{label}"
        );
    }
    assert!(due_retirement("t", "g", "snap", &snapshot(1), &[], 9_999).is_none());
}
