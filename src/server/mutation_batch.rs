//! Server-side compiler and serialization lane for canonical MutationBatch.
//!
//! Public mutation surfaces lower their already-validated `Method` values here;
//! persistence owns the atomic commit and handlers publish RAM only afterward.
//!
//! This namespace split is transitional organization around the current
//! implementation. It does not claim the target `MutationKernel` extraction.

mod canonical;
mod commit;
mod compile;
mod digest;
mod terminal_outcome;

#[cfg(feature = "raft")]
pub(crate) use canonical::is_work_item_method;
pub(crate) use canonical::{
    domain_for, is_capacity_method, is_resource_reservation_method,
    is_resource_reservation_query_method, is_work_item_mutation_method,
};
#[cfg(any(feature = "jobs", all(feature = "raft", feature = "epistemic-tms")))]
pub(crate) use commit::commit_internal_graph_methods;
pub(crate) use commit::{
    commit_internal_graph_methods_with_nonce, commit_lifecycle, commit_work_item,
    lifecycle_was_committed, lock_graph, publish_change_envelope_projection,
    InternalGraphCommitRequest, LifecycleCommitRequest, WorkItemCommitRequest,
};
#[cfg(feature = "program-optimization")]
pub(crate) use commit::{
    commit_program_promotion, resolve_program_promotion_identity, ProgramPromotionRequest,
};
#[cfg(feature = "jobs")]
pub(crate) use compile::compile_opaque_method_in_scope;
pub(crate) use compile::{
    authoritative_graph_version, compile_crossmodal, compile_methods, CompileBatch,
};
#[cfg(feature = "redb")]
pub(crate) use compile::{compile_opaque_digest, COMPILED_BATCH_INCARNATION};
#[cfg(feature = "redb")]
pub(crate) use compile::{compile_opaque_method, effective_policy_digest};
#[cfg(any(
    feature = "raft",
    feature = "blob",
    feature = "tsdb",
    feature = "statechart",
    feature = "jobs",
    feature = "sparql-http"
))]
pub(crate) use digest::opaque_request_key;
#[cfg(feature = "redb")]
pub(crate) use digest::work_item_batch_identity;
#[cfg(test)]
pub(crate) use digest::ENGINE_LEDGER_PRINCIPAL;
pub(crate) use digest::{
    batch_actor, lifecycle_batch_id, opaque_coordinator_key, opaque_idempotency_key_for_context,
    principal_fingerprint,
};

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "redb"))]
mod lifecycle_tests;
