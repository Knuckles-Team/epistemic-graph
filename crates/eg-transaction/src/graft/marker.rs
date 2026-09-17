//! The exact-match checks behind [`super::Marker::from_record`], split out of
//! `graft.rs` (KISS file-budget) purely so they don't grow the parent file past its
//! aggregate line/function caps. Behaviour is unchanged from when this was one long
//! OR-chain inline in `from_record`.

use super::*;

/// The receipt is committed and names the expected scope on both its own identity and
/// its batch's.
pub(super) fn receipt_identity_matches(
    identity: &MutationScopeIdentity,
    record: &eg_types::MutationBatchRecord,
) -> bool {
    record.status == MutationBatchStatus::Committed
        && record.identity == *identity
        && record.batch.schema_version == MUTATION_BATCH_VERSION
        && record.batch.identity == *identity
}

/// The batch's own keys (id, idempotency, envelope, fence, version expectation) are
/// exactly the ones a fresh kernel marker for this intent would carry. Computes the
/// expected envelope itself (rather than taking it as a parameter) purely to keep
/// [`super::Marker::from_record`]'s own statement count down for the KISS file budget.
pub(super) fn batch_keys_match(
    identity: &MutationScopeIdentity,
    record: &eg_types::MutationBatchRecord,
    intent: &GraftIntent,
) -> Result<bool, String> {
    let expected_envelope = MutationEnvelope::maintenance_for_scope(
        identity,
        record.batch.serving_principal(),
        GRAFT_MARKER,
        record.batch.idempotency_key(),
    )?;
    Ok(record.batch.batch_id == GraftIntent::batch_id(&intent.destination)
        && record.batch.idempotency_key() == record.batch.batch_id.as_str()
        && record.batch.envelope == expected_envelope
        && record.batch.placement_epoch == GRAFT_FENCE
        && record.batch.fencing_token == Some(GRAFT_FENCE)
        && record.batch.version_expectation == scope_expectation(identity, intent.version))
}

/// The batch carries exactly one operation, in the marker's fixed shape.
pub(super) fn operation_shape_matches(
    identity: &MutationScopeIdentity,
    operation: &MutationOperation,
    record: &eg_types::MutationBatchRecord,
) -> bool {
    operation.ordinal == 0
        && record.batch.operations.len() == 1
        && operation.surface == MutationSurface::Other
        && operation.domain == scope_domain(identity)
}

/// The operation's payload encodes exactly this intent, the batch carries no outbox
/// entries or authoritative state, its `created_at_ms` is the marker sentinel, and the
/// committed version is exactly one past the intent's.
pub(super) fn marker_content_matches(
    intent: &GraftIntent,
    committed: u64,
    event_type: &str,
    query: &str,
    record: &eg_types::MutationBatchRecord,
) -> bool {
    event_type == GRAFT_MARKER
        && query == intent.encode().as_str()
        && record.batch.outbox.is_empty()
        && record.batch.authoritative_state.is_none()
        && record.batch.created_at_ms == 0
        && intent.version.checked_add(1) == Some(committed)
}
