use super::*;

fn row(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap()
}

fn snapshot() -> Map<String, Value> {
    row(serde_json::json!({
        "type": "AnalysisSnapshot", "analysisDigest": "sha256:ab", "record": "{}",
        "sealedAtMs": 1_000,
    }))
}

fn request(digest: &str) -> RetireSealedRecordRequest {
    RetireSealedRecordRequest {
        tenant: "t".into(),
        node_id: "snap".into(),
        digest: digest.into(),
        reason: "share revoked".into(),
        retired_at_ms: 5_000,
        idempotency_key: "retire-1".into(),
        retired_by: "actor-fingerprint".into(),
    }
}

#[test]
fn only_registered_classes_under_the_type_key_are_sealed() {
    assert_eq!(
        sealed_record_class(&snapshot()).map(|c| c.name),
        Some("AnalysisSnapshot")
    );
    assert!(is_sealed_record_class(SEALED_TOMBSTONE_TYPE));
    for plain in [
        serde_json::json!({"type": "Doc"}),
        serde_json::json!({"node_type": "AnalysisSnapshot"}),
        serde_json::json!({"type": "analysissnapshot"}),
        serde_json::json!({"type": 7}),
        serde_json::json!({}),
    ] {
        assert!(!is_sealed_record_row(&row(plain.clone())), "{plain}");
    }
}

#[test]
fn retiring_with_the_seal_writes_the_audited_tombstone() {
    let resolved = request("sha256:ab").resolve(Some(&snapshot()));
    assert_eq!(resolved.outcome, SealedRecordRetireOutcome::Retired);
    let written = resolved.write.clone().unwrap();
    assert_eq!(written["type"], SEALED_TOMBSTONE_TYPE);
    assert_eq!(written["record_class"], "AnalysisSnapshot");
    assert_eq!(written["digest"], "sha256:ab");
    assert_eq!(written["retired_by"], "actor-fingerprint");
    assert_eq!(written["retired_at_ms"], 5_000);
    assert_eq!(written["reason"], "share revoked");
    assert!(
        !written.contains_key("record"),
        "the sealed content is gone"
    );
    assert_eq!(
        resolved.result().changed_work_item_ids,
        vec!["snap".to_string()]
    );

    // Retrying against the tombstone is idempotent and writes nothing.
    let again = request("sha256:ab").resolve(Some(&written));
    assert_eq!(again.outcome, SealedRecordRetireOutcome::AlreadyRetired);
    assert!(again.write.is_none());
    assert_eq!(again.tombstone, resolved.tombstone);
}

#[test]
fn retirement_refuses_a_wrong_seal_an_ordinary_row_or_an_absent_row() {
    let cases = [
        (
            Some(snapshot()),
            "sha256:ff",
            SealedRecordRetireOutcome::DigestMismatch,
        ),
        (
            Some(row(serde_json::json!({"type": "Doc"}))),
            "x",
            SealedRecordRetireOutcome::NotSealed,
        ),
        (None, "sha256:ab", SealedRecordRetireOutcome::NotFound),
    ];
    for (stored, digest, outcome) in cases {
        let resolved = request(digest).resolve(stored.as_ref());
        assert_eq!(resolved.outcome, outcome);
        assert!(resolved.write.is_none());
        assert!(resolved.result().changed_work_item_ids.is_empty());
    }
}

#[test]
fn validation_requires_bounded_fields_and_a_stamped_actor() {
    request("d").validate().unwrap();
    let mut unstamped = request("d");
    unstamped.retired_by.clear();
    assert!(unstamped.validate().is_err());
    let mut untimed = request("d");
    untimed.retired_at_ms = 0;
    assert!(untimed.validate().is_err());
    let mut long_reason = request("d");
    long_reason.reason = "x".repeat(4_096);
    assert!(long_reason.validate().is_err());
}

#[test]
fn expiry_ages_records_by_their_sealed_at_time_but_never_tombstones() {
    assert_eq!(expires_at_ms(&snapshot(), 500), Some(1_500));
    let mut unstamped = snapshot();
    unstamped.remove("sealedAtMs");
    assert_eq!(expires_at_ms(&unstamped, 500), None);
    let tombstone = request("sha256:ab")
        .resolve(Some(&snapshot()))
        .write
        .unwrap();
    assert_eq!(expires_at_ms(&tombstone, 500), None);
}
