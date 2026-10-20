use super::*;
// spec: EG-DURABLE-KERNEL-R028.2
#[test]
fn payload_is_bounded_and_digest_bound() {
    let content = r#"{"text":"hello"}"#.to_owned();
    let mut value = RunStreamPayload {
        kind: RunStreamKind::TextDelta,
        payload_digest: hex::encode(Sha256::digest(content.as_bytes())),
        content,
    };
    value.validate().unwrap();
    value.content.push(' ');
    assert!(value.validate().is_err());
    value.content = "[]".into();
    value.payload_digest = hex::encode(Sha256::digest(value.content.as_bytes()));
    assert!(value.validate().is_err());
    value.content = "x".repeat(MAX_RUN_EVENT_BYTES + 1);
    assert!(value.validate().is_err());
}
// spec: EG-DURABLE-KERNEL-R028.2
#[test]
fn cursor_requires_canonical_index_and_digest_shape() {
    assert_eq!(
        cursor_index(&format!("eg-run-v1:3:{}", "a".repeat(64))).unwrap(),
        3
    );
    for prefix in ["eg-run-v1:0:", "eg-run-v1:01:", "other:1:", "eg-run-v1:-1:"] {
        assert!(cursor_index(&format!("{prefix}{}", "a".repeat(64))).is_err());
    }
    assert!(cursor_index("eg-run-v1:1:x").is_err());
}
