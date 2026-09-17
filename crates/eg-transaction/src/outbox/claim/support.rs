//! Small helpers behind [`super::install_leases`], split out of `claim.rs` (KISS file
//! budget) purely so they don't grow the parent file past its aggregate line/statement
//! caps. Behaviour is unchanged from when this was inline in `install_leases`.

use super::*;

/// Dead-letter `current` when it has exhausted its retries, writing the dead-lettered
/// row and the fairness-counter adjustments that go with it. Returns whether the row
/// was dead-lettered (the caller then advances the resolved prefix and moves on).
pub(super) fn try_dead_letter(
    deliveries: &mut ScopedTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    key: (&str, &str, &str, u32),
    current: Option<&OutboxDelivery>,
    at: &Claiming<'_>,
    position: &OutboxPosition,
    expired: bool,
    state: &mut OutboxConsumerState,
) -> Result<bool, String> {
    let Some(dead) = dead_letter(current, at.identity, at.consumer, position, at.budget) else {
        return Ok(false);
    };
    if expired {
        decrement_inflight(state, "dead-lettered lease is absent from counter")?;
    }
    deliveries.insert(key, encode_row(&dead, "outbox delivery row")?.as_slice())?;
    increment_dead_lettered(state)?;
    Ok(true)
}

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

pub(super) fn decrement_inflight(
    state: &mut OutboxConsumerState,
    absent_message: &str,
) -> Result<(), String> {
    state.inflight = state
        .inflight
        .checked_sub(1)
        .ok_or_else(|| format!("CORRUPT_OUTBOX_FAIRNESS: {absent_message}"))?;
    Ok(())
}

pub(super) fn increment_inflight(state: &mut OutboxConsumerState) -> Result<(), String> {
    state.inflight = state
        .inflight
        .checked_add(1)
        .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: in-flight counter overflow".to_string())?;
    Ok(())
}

pub(super) fn increment_dead_lettered(state: &mut OutboxConsumerState) -> Result<(), String> {
    state.dead_lettered = state
        .dead_lettered
        .checked_add(1)
        .ok_or_else(|| "CORRUPT_OUTBOX_FAIRNESS: dead-letter counter overflow".to_string())?;
    Ok(())
}
