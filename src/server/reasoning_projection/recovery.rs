//! How the reasoning projection survives a skipped event (X10, PX10b item 7).
//!
//! The projection is a positional incremental index: an event it never
//! applied leaves it silently wrong. Two things can skip one:
//!
//! * the kernel dead-lettered rows while leasing (bounded retry exhausted), and
//!   the claim outcome reports their positions; or
//! * this worker finds a wake-up it can never apply, and rejects it at once
//!   instead of letting it burn sixteen 30-second leases.
//!
//! Either way the index is rebuilt from the live graph through the ordinary
//! initialization path. A rebuilt index has no position, which exempts it from
//! the snapshot-behind-watermark check, and it is complete as of the live graph
//! -- so later events apply on top of it.

use std::sync::Arc;

use eg_transaction::{OutboxClaimOutcome, OutboxRejectReason};
use eg_types::mutation_batch::MutationOutboxLease;

use super::{initialize_index, reset_retired_projection, ProjectionContext, CONSUMER, TOPIC};

/// Rebuild the index when this claim dead-lettered rows ahead of the leases
/// it returned. Returns whether the index is ready for the leases.
pub(super) async fn rebuild_after_skipped(
    context: &ProjectionContext,
    graph_fname: &str,
    core: &Arc<eg_core::graph::GraphCore>,
    outcome: &OutboxClaimOutcome,
) -> bool {
    if outcome.dead_lettered.is_empty() {
        return true;
    }
    crate::metrics::outbox_dead_lettered(
        CONSUMER,
        TOPIC,
        "exhausted",
        outcome.dead_lettered.len() as u64,
    );
    rebuild(context.persist_dir.clone(), graph_fname, core.clone()).await
}

/// Reject a wake-up that can never apply, then rebuild: the event it carried
/// is now skipped.
pub(super) async fn reject_invalid(
    context: &ProjectionContext,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
    lease: &MutationOutboxLease,
) {
    let write = crate::server::outbox_operator::OutboxWrite::Reject {
        lease: Box::new(lease.clone()),
        reason: OutboxRejectReason::InvalidEvent,
        now_ms: (context.clock)(),
    };
    if let Err(error) = context
        .persistence
        .write_mutation_outbox(graph_fname, write)
        .await
    {
        tracing::warn!(
            code = "REASONING_PROJECTION_REJECT_FAILED",
            %error,
            "reasoning projection could not reject an invalid wake-up; its lease will expire"
        );
        return;
    }
    crate::metrics::outbox_dead_lettered(CONSUMER, TOPIC, "invalid_event", 1);
    rebuild(context.persist_dir.clone(), graph_fname, core).await;
}

async fn rebuild(
    persist_dir: Option<String>,
    graph_fname: &str,
    core: Arc<eg_core::graph::GraphCore>,
) -> bool {
    reset_retired_projection(persist_dir.clone(), graph_fname.to_string()).await
        && initialize_index(persist_dir, graph_fname.to_string(), core).await
}
