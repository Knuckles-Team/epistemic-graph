use super::graph_pipeline::dispatch_graph_op;
#[cfg(feature = "raft")]
use super::request_boundary::dispatch_with_context;
use super::*;

#[cfg(all(feature = "raft", feature = "jobs"))]
mod publication;
mod registry;
mod replicated;
#[cfg(feature = "raft")]
mod routing;
#[cfg(feature = "raft")]
mod sanitization;
#[cfg(feature = "raft")]
mod transaction;

#[cfg(all(feature = "raft", feature = "jobs"))]
use publication::{execute_consensus_job_publication, JobPublicationExecution};
pub(super) use registry::handle_register_server;
#[cfg(feature = "raft")]
use replicated::capability_authority_unavailable;
#[cfg(not(feature = "raft"))]
pub(super) use replicated::replicated_identity_bootstrap_authorized;
#[cfg(feature = "raft")]
pub(super) use replicated::replicated_identity_bootstrap_authorized;
#[cfg(feature = "raft")]
pub(super) use routing::propose_native_mutation;
#[cfg(feature = "raft")]
use sanitization::sanitize_native_proposal;
#[cfg(feature = "raft")]
use transaction::{execute_consensus_transaction, TransactionExecution};

#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
struct ReplicatedApplyScope {
    committed_at_ms: u64,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    identity_bootstrap: bool,
}

#[cfg(feature = "raft")]
tokio::task_local! {
    static REPLICATED_APPLY: ReplicatedApplyScope;
}

/// One authoritative clock for a replicated native command. Followers must not
/// sample their local wall clocks while applying the same committed entry.
pub(crate) fn authoritative_now_ms() -> u64 {
    #[cfg(feature = "raft")]
    if let Ok(value) = REPLICATED_APPLY.try_with(|scope| scope.committed_at_ms) {
        return value;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn authoritative_now_secs() -> u64 {
    authoritative_now_ms() / 1_000
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) use replicated::{
    apply_replicated_job_publication_commit, apply_replicated_job_publication_finalize,
};
#[cfg(feature = "raft")]
pub(crate) use replicated::{
    apply_replicated_native, apply_replicated_transaction_decision,
    apply_replicated_transaction_finalize, apply_replicated_transaction_participant,
    apply_replicated_transaction_prepare, is_replicated_apply, replicated_placement_authority,
    ReplicatedParticipantRef,
};
