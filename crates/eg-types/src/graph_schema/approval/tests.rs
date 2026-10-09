use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::*;
use crate::graph_schema::repair::{FieldContract, JsonType};
use crate::graph_schema::GraphSchemaOp;

const SOURCE: &str = "approved:container-manager-mcp";
const NOW: u64 = 1_700_000_000_000;

fn contract_with(field: &str) -> RecordContract {
    let mut fields = BTreeMap::new();
    fields.insert(
        field.to_string(),
        FieldContract {
            required: true,
            types: vec![JsonType::String],
        },
    );
    RecordContract { fields }
}

fn contract() -> RecordContract {
    contract_with("name")
}

fn candidate() -> String {
    approved_candidate_digest(SOURCE, &contract())
}

fn lease(status: ControlLeaseStatus, grant: Value) -> ControlLeaseView {
    let Value::Object(grant) = grant else {
        unreachable!("fixture grants are objects")
    };
    ControlLeaseView {
        lease_id: "action_approval:1".to_string(),
        kind: SCHEMA_APPROVAL_LEASE_KIND.to_string(),
        status,
        grant,
        issued_at_ms: NOW - 1_000,
        expires_at_ms: NOW + 1_000,
        hard_expires_at_ms: NOW + 1_000,
        revision: 2,
    }
}

fn approving_grant() -> Value {
    json!({
        "kind": SCHEMA_APPROVAL_ACTION,
        "target": SOURCE,
        "candidate_digest": candidate(),
    })
}

fn refusal(lease: Option<&ControlLeaseView>) -> String {
    verify_schema_approval(lease, SOURCE, &candidate(), NOW).unwrap_err()
}

#[test]
fn an_approved_unexpired_lease_naming_the_candidate_admits_it() {
    let approved = lease(ControlLeaseStatus::Consumed, approving_grant());
    verify_schema_approval(Some(&approved), SOURCE, &candidate(), NOW).unwrap();
}

#[test]
fn no_lease_pending_refused_and_expired_approvals_are_all_refused() {
    assert!(refusal(None).starts_with("SCHEMA_APPROVAL_REQUIRED"));
    for status in [
        ControlLeaseStatus::Active,
        ControlLeaseStatus::Revoked,
        ControlLeaseStatus::Expired,
    ] {
        let not_approved = lease(status, approving_grant());
        assert!(refusal(Some(&not_approved)).starts_with("SCHEMA_APPROVAL_REQUIRED"));
    }
    let mut lapsed = lease(ControlLeaseStatus::Consumed, approving_grant());
    lapsed.expires_at_ms = NOW;
    assert!(refusal(Some(&lapsed)).starts_with("SCHEMA_APPROVAL_REQUIRED"));
    let mut hard_lapsed = lease(ControlLeaseStatus::Consumed, approving_grant());
    hard_lapsed.hard_expires_at_ms = NOW;
    assert!(refusal(Some(&hard_lapsed)).starts_with("SCHEMA_APPROVAL_REQUIRED"));
}

#[test]
fn an_approval_of_another_kind_target_or_candidate_is_a_mismatch() {
    let mut other_kind = lease(ControlLeaseStatus::Consumed, approving_grant());
    other_kind.kind = "browser.control".to_string();
    assert!(refusal(Some(&other_kind)).contains("lease kind"));
    for (key, value) in [
        ("kind", "restart_service"),
        ("target", "approved:another-source"),
        ("candidate_digest", "0".repeat(64).as_str()),
    ] {
        let mut grant = approving_grant();
        grant[key] = Value::String(value.to_string());
        let wrong = lease(ControlLeaseStatus::Consumed, grant);
        let error = refusal(Some(&wrong));
        assert!(error.starts_with("SCHEMA_APPROVAL_MISMATCH"), "{error}");
        assert!(error.ends_with(key), "{error}");
    }
    let bare = lease(ControlLeaseStatus::Consumed, Value::Object(Map::new()));
    assert!(refusal(Some(&bare)).starts_with("SCHEMA_APPROVAL_MISMATCH"));
}

// spec: EG-TYPED-PACKS-R083
#[test]
fn the_candidate_digest_binds_the_key_and_the_typed_contract() {
    let base = candidate();
    assert_eq!(base.len(), 64);
    assert_eq!(base, approved_candidate_digest(SOURCE, &contract()));
    assert_ne!(
        base,
        approved_candidate_digest("approved:other", &contract())
    );
    assert_ne!(
        base,
        approved_candidate_digest(SOURCE, &contract_with("title"))
    );
}

// spec: EG-TYPED-PACKS-R083
#[test]
fn the_candidate_digest_matches_its_documented_framing() {
    // The AU proposer computes the same bytes from the same typed contract
    // (agent_utilities schema_drift.candidate.approved_candidate_digest).
    let framed = format!(
        "{APPROVED_CANDIDATE_DOMAIN}\0{SOURCE}\0{}\0",
        r#"{"fields":{"name":{"required":true,"types":["string"]}}}"#
    );
    assert_eq!(candidate(), Digest256::sha256(framed.as_bytes()).to_hex());
}

fn approved_op(source_id: &str, lease_id: &str) -> GraphSchemaOp {
    GraphSchemaOp::AttachApproved {
        source_id: source_id.to_string(),
        contract: contract(),
        approval_lease_id: lease_id.to_string(),
        if_composed_digest: None,
    }
}

// spec: EG-TYPED-PACKS-R083
#[test]
fn attach_approved_requires_its_namespace_a_lease_id_and_a_contract() {
    approved_op(SOURCE, "action_approval:1").validate().unwrap();
    for bad_key in ["admin:x", "approved:", "pack:x"] {
        assert!(approved_op(bad_key, "action_approval:1")
            .validate()
            .is_err());
    }
    assert!(approved_op(SOURCE, " ").validate().is_err());
    assert!(approved_op(SOURCE, &"x".repeat(513)).validate().is_err());
    let empty = GraphSchemaOp::ValidateRepair {
        source_id: SOURCE.to_string(),
        contract: RecordContract {
            fields: BTreeMap::new(),
        },
        if_composed_digest: None,
    };
    assert!(empty.validate().is_err());
}

#[test]
fn a_generic_attach_can_never_write_the_approved_namespace() {
    let generic = GraphSchemaOp::Attach {
        source_id: SOURCE.to_string(),
        shapes_ttl: Some("@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string()),
        ontology_ttl: None,
        if_composed_digest: None,
    };
    assert!(generic
        .validate()
        .unwrap_err()
        .starts_with("SCHEMA_SOURCE_RESERVED"));
}
