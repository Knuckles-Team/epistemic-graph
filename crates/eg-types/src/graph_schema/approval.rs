//! EH-403: the approval record a governed schema candidate needs before it
//! may be attached.
//!
//! Schema drift detection (AU, EH-402) contains a drifted delta and proposes a
//! repair; it never activates one. Activation is `GraphSchemaOp::AttachApproved`,
//! and the engine refuses it unless the request names an approval that already
//! exists in the caller's tenant:
//!
//! * a native control lease of kind [`SCHEMA_APPROVAL_LEASE_KIND`] -- the same
//!   `action.approval` record the AU approvals queue issues and a human decides;
//! * in state `consumed` (the approvals queue's "approved"), not `active`
//!   (still pending), `revoked` (refused) or `expired`;
//! * before its `expires_at_ms` and `hard_expires_at_ms`;
//! * whose grant names the action [`SCHEMA_APPROVAL_ACTION`], the exact
//!   `approved:<name>` key as `target`, and the exact candidate as
//!   `candidate_digest` ([`approved_candidate_digest`]).
//!
//! The digest binds the key and both documents, so an approval of one
//! candidate can never activate another. The check is pure: the served handler
//! reads the lease and supplies its clock.

use serde_json::Value;

use super::GraphSchemaErrorCode;
use crate::contract::Digest256;
use crate::control_lease::{ControlLeaseStatus, ControlLeaseView};

/// Key prefix of a governed, approval-bound schema source.
pub const APPROVED_SOURCE_PREFIX: &str = "approved:";
/// Control-lease kind of the approval record (the AU approvals queue).
pub const SCHEMA_APPROVAL_LEASE_KIND: &str = "action.approval";
/// The approved action a schema-activation grant must name.
pub const SCHEMA_APPROVAL_ACTION: &str = "schema_repair";
/// Domain separator of the candidate digest.
pub const APPROVED_CANDIDATE_DOMAIN: &str = "eg/approved-schema-candidate/v1";
/// Bound on the approval lease id (the control-lease reference bound).
const MAX_APPROVAL_LEASE_ID_BYTES: usize = 512;

/// The identity an approval binds: the key plus the SHA-256 of each document
/// (empty for an absent one), under [`APPROVED_CANDIDATE_DOMAIN`], each part
/// terminated by a NUL byte. Lower-case hex.
pub fn approved_candidate_digest(
    source_id: &str,
    shapes_ttl: Option<&str>,
    ontology_ttl: Option<&str>,
) -> String {
    let document = |body: Option<&str>| {
        body.map(|text| Digest256::sha256(text.as_bytes()).to_hex())
            .unwrap_or_default()
    };
    let shapes = document(shapes_ttl);
    let ontology = document(ontology_ttl);
    let mut framed = String::new();
    for part in [APPROVED_CANDIDATE_DOMAIN, source_id, &shapes, &ontology] {
        framed.push_str(part);
        framed.push('\0');
    }
    Digest256::sha256(framed.as_bytes()).to_hex()
}

/// `approved:<name>` with a non-empty, printable, bounded name.
pub fn validate_approved_source_id(source_id: &str) -> Result<(), String> {
    super::validate_source_key_shape(source_id)?;
    match source_id.strip_prefix(APPROVED_SOURCE_PREFIX) {
        Some(name) if !name.is_empty() => Ok(()),
        _ => Err(format!(
            "graph schema attach_approved needs the {APPROVED_SOURCE_PREFIX}<name> namespace"
        )),
    }
}

/// A non-empty, printable approval lease id within the control-lease bound.
pub fn validate_approval_lease_id(lease_id: &str) -> Result<(), String> {
    let printable = !lease_id.trim().is_empty() && !lease_id.chars().any(char::is_control);
    if printable && lease_id.len() <= MAX_APPROVAL_LEASE_ID_BYTES {
        Ok(())
    } else {
        Err(format!(
            "approval_lease_id must be printable and 1..={MAX_APPROVAL_LEASE_ID_BYTES} bytes"
        ))
    }
}

fn required(detail: &str) -> String {
    format!(
        "{}: {detail}",
        GraphSchemaErrorCode::ApprovalRequired.as_str()
    )
}

fn mismatch(field: &str) -> String {
    format!(
        "{}: the approval grant does not name this {field}",
        GraphSchemaErrorCode::ApprovalMismatch.as_str()
    )
}

/// Whether `lease` approves attaching `candidate_digest` under `source_id` at
/// `now_ms`. `None` (no lease of the caller's tenant with that id) is refused.
pub fn verify_schema_approval(
    lease: Option<&ControlLeaseView>,
    source_id: &str,
    candidate_digest: &str,
    now_ms: u64,
) -> Result<(), String> {
    let lease = lease.ok_or_else(|| required("no approval lease with this id is visible"))?;
    if lease.kind != SCHEMA_APPROVAL_LEASE_KIND {
        return Err(mismatch("lease kind"));
    }
    verify_approved_state(lease, now_ms)?;
    let grant_text = |key: &str| lease.grant.get(key).and_then(Value::as_str);
    let bindings = [
        ("kind", SCHEMA_APPROVAL_ACTION),
        ("target", source_id),
        ("candidate_digest", candidate_digest),
    ];
    match bindings
        .iter()
        .find(|(key, expected)| grant_text(key) != Some(*expected))
    {
        Some((key, _)) => Err(mismatch(key)),
        None => Ok(()),
    }
}

fn verify_approved_state(lease: &ControlLeaseView, now_ms: u64) -> Result<(), String> {
    match lease.status {
        ControlLeaseStatus::Consumed => {}
        ControlLeaseStatus::Active => return Err(required("the approval is still pending")),
        ControlLeaseStatus::Revoked => return Err(required("the approval was refused")),
        ControlLeaseStatus::Expired => return Err(required("the approval has expired")),
    }
    if now_ms >= lease.expires_at_ms.min(lease.hard_expires_at_ms) {
        return Err(required("the approval has expired"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
