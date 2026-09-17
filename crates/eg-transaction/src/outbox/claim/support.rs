//! Small helpers behind [`super::install_leases`], split out of `claim.rs` (KISS file
//! budget) purely so they don't grow the parent file past its aggregate line/statement
//! caps. Behaviour is unchanged from when this was inline in `install_leases`.

use super::*;

/// A delivery row already on file for this key must be stamped for the same scope, key
/// itself to the same claim identity, and consistent with the caller's acked-through
/// watermark, before it is trusted for the resolved/dead-letter/lease decisions below.
pub(super) fn validate_existing_delivery(
    existing: &OutboxDelivery,
    identity: &MutationScopeIdentity,
    consumer: &str,
    position: &OutboxPosition,
    acked_through: Option<&OutboxPosition>,
) -> Result<(), String> {
    validate_stamp(&existing.identity, identity)?;
    validate_delivery_key(existing, consumer, position)?;
    validate_delivery_state(existing, acked_through)?;
    Ok(())
}

/// Extend the contiguous resolved prefix with `position` — but only while every row
/// before it in this page was ALSO already resolved (`prefix_intact`); once a claimable
/// row is found the prefix stops advancing even if a later row is independently
/// resolved.
pub(super) fn mark_resolved_if_prefix_intact(
    prefix_intact: bool,
    resolved_through: &mut Option<OutboxPosition>,
    position: &OutboxPosition,
) {
    if prefix_intact {
        *resolved_through = Some(position.clone());
    }
}
