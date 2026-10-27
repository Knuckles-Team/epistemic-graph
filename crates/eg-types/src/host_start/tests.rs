use super::*;
use crate::control_lease::ControlLeaseView;

fn request() -> IssueHostStartGrantRequest {
    crate::test_support::host_start::host_start_request("tenant-a")
}

#[test]
fn native_clock_and_session_bound_grant_expiry() {
    let lease = request().lease_at(1_000).unwrap();
    assert_eq!(
        (
            lease.issued_at_ms,
            lease.expires_at_ms,
            lease.hard_expires_at_ms
        ),
        (1_000, 5_000, 5_000)
    );
    assert!(request().lease_at(5_000).is_err());
    assert!(request().lease_at(0).is_err());
    let mut long = request();
    long.expires_at_ms = u64::MAX;
    long.binding.session_expires_at_ms = u64::MAX;
    assert_eq!(
        long.lease_at(1).unwrap().expires_at_ms,
        1 + MAX_CONTROL_LEASE_SPAN_MS
    );
}

#[test]
fn generic_issue_cannot_mint_host_start_and_legacy_json_cannot_stamp_provenance() {
    let lease = request().lease_at(1_000).unwrap();
    assert!(lease.validate().unwrap_err().contains("ACCESS_DENIED"));
    let mut row = lease.row();
    row.get_mut("grant")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert(
            "host_start_provenance".into(),
            serde_json::json!({"schema_version":1,"issuer_ref":"forged"}),
        );
    assert!(ControlLeaseView::from_row("host-grant", &row)
        .unwrap()
        .host_start_provenance
        .is_none());
    row.insert(
        "host_start_provenance".into(),
        serde_json::json!({"schema_version":2,"issuer_ref":"issuer"}),
    );
    assert!(ControlLeaseView::from_row("host-grant", &row).is_err());
}

#[test]
fn typed_request_refuses_issuer_injection_and_invalid_bindings() {
    let mut value = serde_json::to_value(request()).unwrap();
    value["issuer_ref"] = "forged".into();
    assert!(serde_json::from_value::<IssueHostStartGrantRequest>(value).is_err());
    let mut req = request();
    req.binding.configuration_digest = "A".repeat(64);
    assert!(req.lease_at(1_000).is_err());
    req = request();
    req.binding.session_ref = "x".repeat(513);
    assert!(req.lease_at(1_000).is_err());
    req = request();
    req.binding.workspace_ref = "workspace\nother".into();
    assert!(req.lease_at(1_000).is_err());
}
