use super::*;

fn issue() -> IssueControlLeaseRequest {
    let mut grant = Map::new();
    grant.insert("tool_ids".into(), serde_json::json!(["click"]));
    IssueControlLeaseRequest {
        tenant: "tenant-a".into(),
        lease_id: "browserlease_1".into(),
        kind: "browser.control".into(),
        grant,
        issued_at_ms: 1_000,
        expires_at_ms: 301_000,
        hard_expires_at_ms: 901_000,
        idempotency_key: "issue-1".into(),
    }
}

#[test]
fn every_status_round_trips_through_its_stored_text() {
    for (text, status) in STORED_STATUS {
        assert_eq!(status.as_stored(), text);
        assert_eq!(ControlLeaseStatus::from_stored(text), Some(status));
    }
    assert_eq!(
        ControlLeaseTarget::Revoked.status(),
        ControlLeaseStatus::Revoked
    );
    assert_eq!(
        ControlLeaseTarget::Expired.status(),
        ControlLeaseStatus::Expired
    );
    assert_eq!(ControlLeaseStatus::from_stored("renewed"), None);
}

#[test]
fn an_issued_row_projects_back_to_the_request_as_revision_one() {
    let request = issue();
    request.validate().unwrap();
    let row = request.row();
    assert!(is_tenant_control_lease(&row, "tenant-a"));
    assert!(!is_tenant_control_lease(&row, "tenant-b"));
    let view = ControlLeaseView::from_row("browserlease_1", &row).unwrap();
    assert_eq!(view.status, ControlLeaseStatus::Active);
    assert_eq!(view.revision, 1);
    assert_eq!(view.grant, request.grant);
    assert_eq!(
        (
            view.issued_at_ms,
            view.expires_at_ms,
            view.hard_expires_at_ms
        ),
        (1_000, 301_000, 901_000)
    );
}

#[test]
fn issue_refuses_disordered_or_unbounded_timing_and_oversized_grants() {
    let mut late = issue();
    late.expires_at_ms = late.hard_expires_at_ms + 1;
    assert!(late.validate().is_err());
    let mut unissued = issue();
    unissued.issued_at_ms = 0;
    assert!(unissued.validate().is_err());
    let mut forever = issue();
    forever.hard_expires_at_ms = forever.issued_at_ms + MAX_CONTROL_LEASE_SPAN_MS + 1;
    assert!(forever.validate().is_err());
    let mut heavy = issue();
    heavy.grant.insert(
        "blob".into(),
        "x".repeat(MAX_CONTROL_LEASE_GRANT_BYTES).into(),
    );
    assert!(heavy.validate().is_err());
    let mut anonymous = issue();
    anonymous.tenant = " ".into();
    assert!(anonymous.validate().is_err());
}

#[test]
fn transition_and_get_requests_are_bounded() {
    let transition = TransitionControlLeaseRequest {
        tenant: "tenant-a".into(),
        lease_id: "browserlease_1".into(),
        expected_revision: 1,
        to: ControlLeaseTarget::Revoked,
        idempotency_key: String::new(),
    };
    assert!(transition.validate().is_err());
    assert!(validate_control_lease_get("tenant-a", "").is_err());
    validate_control_lease_get("tenant-a", "browserlease_1").unwrap();
}

#[test]
fn only_the_declared_edges_are_legal() {
    use ControlLeaseStatus::{Active, Consumed, Expired, Revoked};
    use ControlLeaseTarget as To;
    for from in [Active, Consumed, Revoked, Expired] {
        for to in [To::Consumed, To::Revoked, To::Expired] {
            let legal = matches!(
                (from, to),
                (Active, _) | (Consumed, To::Revoked | To::Expired)
            );
            assert_eq!(to.allowed_from(from), legal, "{from:?} -> {to:?}");
        }
    }
}

#[test]
fn a_lease_listing_selects_the_tenants_leases_by_kind_status_and_grant() {
    use crate::keyset_page::{KeysetListing, KeysetScan};
    let mut approval = issue();
    approval.kind = "action.approval".into();
    approval
        .grant
        .insert("request_digest".into(), serde_json::json!("d-1"));
    let rows = [
        ("a-1", approval.row()),
        ("b-1", issue().row()),
        (
            "c-1",
            IssueControlLeaseRequest {
                tenant: "tenant-b".into(),
                ..approval.clone()
            }
            .row(),
        ),
    ];
    let request = ListControlLeasesRequest {
        tenant: "tenant-a".into(),
        kind: Some("action.approval".into()),
        status: Some(ControlLeaseStatus::Active),
        grant_match: serde_json::json!({"request_digest": "d-1"})
            .as_object()
            .cloned(),
        cursor: None,
        limit: 10,
    };
    request.validate().unwrap();
    let mut scan = KeysetScan::new(&request);
    for (id, row) in &rows {
        assert!(scan.admits_another_row());
        scan.consume(id, 10, row).unwrap();
    }
    let page = scan.finish();
    let ids: Vec<&str> = page
        .items
        .iter()
        .map(|lease| lease.lease_id.as_str())
        .collect();
    assert_eq!(ids, ["a-1"]);
    assert_eq!(page.next_cursor, None);
    assert_eq!(request.limit(), 10);
    assert!(ListControlLeasesRequest {
        limit: 0,
        ..request
    }
    .validate()
    .is_err());
}

#[test]
fn the_elevation_kind_cannot_be_issued_as_a_generic_control_lease() {
    let mut request = issue();
    request.kind = RBAC_ELEVATION_KIND.to_string();
    let error = request.validate().unwrap_err();
    assert!(error.contains("reserved"), "{error}");
}

/// EH-560: a governed kind is issued only through `GovernedChange`; the
/// generic lease path refuses the whole `governed.` namespace, while an
/// ordinary kind next to it is still accepted.
// spec: EG-DURABLE-KERNEL-R027.1
#[test]
fn a_governed_kind_cannot_be_issued_as_a_generic_control_lease() {
    for kind in [
        crate::governed_change::SCHEMA_REPAIR_KIND,
        "governed.anything-new",
    ] {
        let mut request = issue();
        request.kind = kind.to_string();
        let error = request.validate().unwrap_err();
        assert!(error.contains("reserved"), "{kind}: {error}");
    }
    let mut ordinary = issue();
    ordinary.kind = "action.approval".to_string();
    assert!(ordinary.validate().is_ok());
}
