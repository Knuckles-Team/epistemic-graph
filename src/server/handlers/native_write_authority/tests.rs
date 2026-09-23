//! Table-driven proof that every WorkItem-kernel write refuses a body tenant
//! other than the verified carrier's, and that fleet streams need the fleet
//! authority.

use serde_json::{json, Value};

use super::*;

const VERIFIED: &str = "tenant-a";

fn context(tenant: &str) -> Value {
    json!({
        "schema_version": "2", "request_id": "r", "subject_id": "s", "tenant_id": tenant,
        "agent_id": "a", "scopes": [], "audience": "epistemic-graph",
        "authentication_method": "oidc", "policy_version": "p", "graph": "g",
        "placement_epoch": null, "trace_id": "t", "issued_at_ms": 1, "expires_at_ms": 2,
    })
}

fn submit(tenant: &str) -> Value {
    json!({
        "schema_version": "1", "context": context(tenant), "work_item_id": null,
        "idempotency_key": "k", "command_digest": "d", "kind": "k", "priority": 0,
        "depends_on": [], "input_ref": "i", "policy_digest": "p", "catalog_digest": "c",
        "model_digest": "m", "max_attempts": 1, "deadline_unix": null, "metadata": {},
        "provenance_refs": [], "max_tenant_in_flight": 0,
    })
}

/// One wire body per WorkItem-kernel write, naming `tenant` where its body does.
fn writes(tenant: &str) -> Vec<(&'static str, Value)> {
    let fenced = json!({
        "tenant": tenant, "work_item_id": "w", "worker_id": "x", "lease_epoch": 1,
        "fencing_token": 1, "now_ms": 1,
    });
    let with = |extra: Value| {
        let mut body = fenced.clone();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        body
    };
    vec![
        (
            "ClaimWorkItem",
            json!({"request": {
            "schema_version": "1", "tenant_ref": tenant, "work_item_id": null, "queue_ref": null,
            "resource_class": null, "fairness_group": null, "worker_ref": "x", "now_ms": 1,
            "lease_ms": 1, "max_tenant_in_flight": 1}}),
        ),
        ("RenewWorkItemLease", with(json!({"lease_ms": 1}))),
        (
            "CommitWorkItemResult",
            with(json!({"idempotency_key": "k", "outcome": "succeeded",
            "result_ref": null})),
        ),
        (
            "CancelWorkItem",
            json!({"tenant": tenant, "work_item_id": "w",
            "idempotency_key": "k", "now_ms": 1}),
        ),
        (
            "DeferWorkItem",
            with(json!({"idempotency_key": "k", "next_retry_at_ms": 2})),
        ),
        (
            "CasWorkItemMetadata",
            json!({"request": {
            "schema_version": "1", "tenant_ref": tenant, "work_item_id": "w",
            "expected_lease": null, "expected_status": ["ready"],
            "expected_checkpoint_id": null, "set_checkpoint_id": "c",
            "expected_metadata_msgpack": null, "set_metadata_msgpack": null,
            "expected_prio_bucket": null, "set_prio_bucket": null, "now_ms": 1}}),
        ),
        ("SubmitWorkItem", json!({"request": submit(tenant)})),
        (
            "SubmitWorkItems",
            json!({"request": {"schema_version": "1",
            "context": context(VERIFIED), "idempotency_key": "k",
            "requests": [submit(tenant)]}}),
        ),
        (
            "IssueControlLease",
            json!({"request": {"tenant": tenant, "lease_id": "l",
            "kind": "k", "grant": {}, "issued_at_ms": 1, "expires_at_ms": 2,
            "hard_expires_at_ms": 3, "idempotency_key": "k"}}),
        ),
        (
            "TransitionControlLease",
            json!({"request": {"tenant": tenant, "lease_id": "l",
            "expected_revision": 1, "to": "revoked", "idempotency_key": "k"}}),
        ),
    ]
}

fn method(name: &str, params: Value) -> Method {
    serde_json::from_value(json!({"method": name, "params": params}))
        .unwrap_or_else(|error| panic!("{name} fixture does not deserialize: {error}"))
}

#[test]
fn every_work_item_write_refuses_a_foreign_body_tenant() {
    for (name, params) in writes("tenant-b") {
        let error =
            refuse_foreign_native_write(&method(name, params), VERIFIED, FleetAuthority::Absent)
                .expect_err(name);
        assert!(error.starts_with("ACCESS_DENIED:"), "{name}: {error}");
    }
    for (name, params) in writes(VERIFIED) {
        refuse_foreign_native_write(&method(name, params), VERIFIED, FleetAuthority::Absent)
            .unwrap_or_else(|error| panic!("{name} own tenant refused: {error}"));
    }
}

#[test]
fn fleet_streams_need_the_fleet_authority_and_other_streams_do_not() {
    let publish = |stream: &str| {
        method(
            "StreamPublish",
            json!({"stream": stream, "payload": [], "now_ms": 1}),
        )
    };
    let fleet = publish("fleet.events");
    assert!(refuse_foreign_native_write(&fleet, VERIFIED, FleetAuthority::Absent).is_err());
    refuse_foreign_native_write(&fleet, VERIFIED, FleetAuthority::Held).unwrap();
    let other = publish("app.events");
    refuse_foreign_native_write(&other, VERIFIED, FleetAuthority::Absent).unwrap();
    let trim = method("StreamTrim", json!({"stream": "fleet.events", "now_ms": 1}));
    assert!(refuse_foreign_native_write(&trim, VERIFIED, FleetAuthority::Absent).is_err());
}

#[test]
fn an_unverified_tenant_owns_nothing() {
    assert!(require_carrier_tenant("", "").is_err());
    require_carrier_tenant("tenant-a", "tenant-a").unwrap();
}
