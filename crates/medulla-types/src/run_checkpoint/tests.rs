use super::*;

fn content(text: &str) -> CheckpointContent {
    CheckpointContent {
        content: text.into(),
        payload_digest: hex::encode(Sha256::digest(text.as_bytes())),
        short_term: vec![],
        long_term: vec![],
    }
}

fn publish() -> PublishRunCheckpointRequest {
    PublishRunCheckpointRequest {
        tenant: "tenant-a".into(),
        work_item_id: "work-item:one".into(),
        run_id: "run-one".into(),
        context_id: "context".into(),
        worker_id: "worker-one".into(),
        lease_epoch: 3,
        fencing_token: 9,
        expected_revision: None,
        snapshot: content(r#"{"phase":"completed","next_step":1}"#),
        idempotency_key: "publication-one".into(),
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn digest_binds_exact_content_bytes_not_normalized_json() {
    let mut snapshot = content(r#"{"phase":"completed"}"#);
    assert!(snapshot.validate().is_ok());
    snapshot.content = r#"{ "phase": "completed" }"#.into();
    assert!(snapshot.validate().unwrap_err().contains("digest mismatch"));
    snapshot.payload_digest = hex::encode(Sha256::digest(snapshot.content.as_bytes()));
    assert!(snapshot.validate().is_ok());
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn rejects_empty_malformed_and_non_object_content() {
    for text in ["", "{", "null", "[]", "42", r#""text""#] {
        assert!(content(text).validate().is_err(), "{text}");
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn snapshot_limit_includes_content_escaping_and_reference_overhead() {
    let oversized = content(&"x".repeat(MAX_CHECKPOINT_BYTES + 1));
    assert!(oversized.validate().unwrap_err().contains("content"));
    let text = format!(r#"{{"text":"{}"}}"#, "x".repeat(MAX_CHECKPOINT_BYTES - 11));
    assert_eq!(text.len(), MAX_CHECKPOINT_BYTES);
    assert!(content(&text)
        .validate()
        .unwrap_err()
        .contains("snapshot exceeds"));
    let mut snapshot = content("{}");
    let reference = CheckpointMemoryRef {
        document_id: "d".repeat(512),
        revision: 1,
        digest: "a".repeat(64),
    };
    snapshot.long_term = vec![reference; 2048];
    assert!(snapshot
        .validate()
        .unwrap_err()
        .contains("snapshot exceeds"));
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn memory_references_require_bounded_ids_and_lowercase_sha256() {
    for long_term in [false, true] {
        for (id, digest) in [
            ("doc".to_string(), "a".repeat(64)),
            (" ".to_string(), "a".repeat(64)),
            ("d".repeat(513), "a".repeat(64)),
            ("doc".to_string(), "a".repeat(63)),
            ("doc".to_string(), "A".repeat(64)),
            ("doc".to_string(), "g".repeat(64)),
        ] {
            let valid = id == "doc" && digest == "a".repeat(64);
            let mut snapshot = content("{}");
            let reference = CheckpointMemoryRef {
                document_id: id,
                revision: 7,
                digest,
            };
            if long_term {
                snapshot.long_term.push(reference);
            } else {
                snapshot.short_term.push(reference);
            }
            assert_eq!(snapshot.validate().is_ok(), valid);
        }
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn publish_rejects_empty_or_oversized_binding_and_idempotency_ids() {
    assert!(publish().validate().is_ok());
    for field in [
        "tenant",
        "work_item_id",
        "run_id",
        "context_id",
        "worker_id",
        "idempotency_key",
    ] {
        for bad in [" ".to_string(), "a".repeat(513)] {
            let mut wire = serde_json::to_value(publish()).unwrap();
            wire[field] = bad.into();
            let request: PublishRunCheckpointRequest = serde_json::from_value(wire).unwrap();
            assert!(request.validate().is_err(), "{field}");
        }
    }
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn optional_claim_validates_worker_and_unknown_fields_fail_closed() {
    let base =
        serde_json::json!({"tenant":"t", "work_item_id":"w", "run_id":"r", "context_id":"context"});
    let request: GetRunCheckpointRequest = serde_json::from_value(base.clone()).unwrap();
    assert_eq!(request.current_claim, None);
    assert!(request.validate().is_ok());
    let mut wire = base;
    wire["current_claim"] = serde_json::json!({"worker_id":" ","lease_epoch":2,"fencing_token":3});
    let request: GetRunCheckpointRequest = serde_json::from_value(wire.clone()).unwrap();
    assert!(request.validate().is_err());
    wire["current_claim"]["worker_id"] = "worker".into();
    wire["current_claim"]["unknown"] = true.into();
    assert!(serde_json::from_value::<GetRunCheckpointRequest>(wire).is_err());
    let mut wire = serde_json::to_value(publish()).unwrap();
    wire["snapshot"]["owner_override"] = "other".into();
    assert!(serde_json::from_value::<PublishRunCheckpointRequest>(wire).is_err());
}

// spec: MEDULLA-DURABLE-KERNEL-R028.1.1
#[test]
fn publication_receipt_does_not_expose_content_or_claim_credentials() {
    let receipt = RunCheckpointPublished {
        changed_work_item_ids: vec!["work-item:one".into()],
        replayed: false,
        work_item_id: "work-item:one".into(),
        run_id: "run-one".into(),
        context_id: "context".into(),
        revision: 1,
        payload_digest: "a".repeat(64),
    };
    let mut wire = serde_json::to_value(receipt).unwrap();
    for field in ["content", "worker_id", "lease_epoch", "fencing_token"] {
        assert!(wire.get(field).is_none());
    }
    wire["fencing_token"] = 9.into();
    assert!(serde_json::from_value::<RunCheckpointPublished>(wire).is_err());
}
