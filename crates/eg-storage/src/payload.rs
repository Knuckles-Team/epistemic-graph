//! Digest binding between a sealed private payload and its parent receipt.

use eg_types::MutationBatchRecord;

/// The digest that BINDS a sealed private payload to its parent record.
///
/// Deliberately does not inspect `event_type`. This value is an authentication
/// input on the hot read/write path; what makes it valid is that the parent
/// carries a single `ApplyMutation` whose query is a `sha256:` digest, not which
/// family of plan it belongs to. Gating the binding on a hard-coded event type is
/// what silently broke every SPARQL-HTTP saga: before this store was refactored
/// the string check lived only in the backup scan, and extracting a shared helper
/// carried it onto paths that never had it.
pub fn private_payload_digest(record: &MutationBatchRecord) -> Option<&str> {
    let operation = record.batch.operations.first()?;
    if record.batch.operations.len() != 1 {
        return None;
    }
    match &operation.method {
        eg_types::protocol::Method::ApplyMutation { query, .. }
            if query.len() == 71
                && query.starts_with("sha256:")
                && query[7..]
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            Some(&query[7..])
        }
        _ => None,
    }
}

/// A payload digest whose parent is one of the DECLARED private-payload plan
/// shapes (`eg_types::mutation_batch::PRIVATE_PAYLOAD_EVENT_TYPES`).
///
/// Used only by the recovery/backup scan, which legitimately asserts that every
/// sealed row it walks belongs to a known plan family. The hot path must use
/// [`private_payload_digest`] instead.
pub(crate) fn recovery_plan_digest(record: &MutationBatchRecord) -> Option<&str> {
    let operation = record.batch.operations.first()?;
    let eg_types::protocol::Method::ApplyMutation { event_type, .. } = &operation.method else {
        return None;
    };
    if !eg_types::mutation_batch::PRIVATE_PAYLOAD_EVENT_TYPES
        .iter()
        .any(|known| known == event_type)
    {
        return None;
    }
    private_payload_digest(record)
}
