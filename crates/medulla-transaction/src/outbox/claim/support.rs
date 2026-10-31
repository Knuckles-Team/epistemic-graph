//! Small helpers behind [`super::install_leases`], split out of `claim.rs` (KISS file
//! budget) purely so they don't grow the parent file past its aggregate line/statement
//! caps. Behaviour is unchanged from when this was inline in `install_leases`.

use super::*;

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
