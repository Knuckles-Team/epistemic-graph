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

pub(crate) use canonical::{
    domain_for, is_capacity_method, is_resource_reservation_method,
    is_resource_reservation_query_method, is_work_item_method, is_work_item_mutation_method,
};
pub(crate) use commit::{
    commit_internal_graph_methods, commit_lifecycle, commit_work_item, lifecycle_was_committed,
    lock_graph, publish_change_envelope_projection,
};
pub(crate) use compile::{
    authoritative_graph_version, compile_crossmodal, compile_methods, compile_opaque_digest,
    compile_opaque_method, CompileBatch, COMPILED_BATCH_INCARNATION,
};
pub(crate) use digest::{
    batch_actor, lifecycle_batch_id, opaque_coordinator_key, opaque_request_key,
    opaque_request_key_for_context, principal_fingerprint, ENGINE_LEDGER_PRINCIPAL,
};

#[cfg(test)]
mod tests;
