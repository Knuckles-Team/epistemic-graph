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
    lifecycle_was_committed, lock_graph, publish_change_envelope_projection, CommitOrigin,
    InternalGraphCommitRequest, LifecycleCommitRequest, WorkItemCommitRequest,
};
#[cfg(feature = "program-optimization")]
pub(crate) use commit::{
    commit_program_promotion, resolve_program_promotion_identity, ProgramPromotionRequest,
};
#[cfg(feature = "jobs")]
pub(crate) use compile::compile_opaque_method_in_scope;
#[cfg(feature = "query")]
pub(crate) use compile::compile_sql_source_batch;
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

/// The verified coordinates one governed graph-row write is compiled under:
/// the request, the verified caller, the tenant/graph it targets and the
/// placement fence (epoch, graph version, fencing token) it commits behind.
pub(crate) struct GraphWriteScope<'a> {
    pub request_id: u64,
    pub graph_name: &'a str,
    pub tenant_scope: &'a str,
    pub verified: &'a crate::server::auth::VerifiedRequestContext,
    pub graph_version: u64,
    pub placement_epoch: u64,
    pub fencing_token: Option<u64>,
}

impl GraphWriteScope<'_> {
    /// Compile `methods` as one graph-surface batch `batch_id` under this scope.
    pub(crate) fn compile(
        &self,
        batch_id: &str,
        methods: Vec<crate::protocol::Method>,
    ) -> Result<crate::mutation_batch::MutationBatch, String> {
        let principal = self.verified.principal_persistence_id();
        compile_methods(
            CompileBatch {
                batch_id,
                request_id: self.request_id,
                attempt_nonce: self.verified.attempt_nonce(),
                principal: Some(&principal),
                tenant: self.tenant_scope,
                graph: self.graph_name,
                placement_epoch: self.placement_epoch,
                idempotency_key: self.verified.idempotency_key(),
                expected_graph_version: Some(self.graph_version),
                fencing_token: self.fencing_token,
                created_at_ms: crate::server::dispatch::authoritative_now_ms(),
                default_surface: crate::mutation_batch::MutationSurface::Graph,
                authoritative_state: None,
            },
            methods,
        )
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "redb"))]
mod lifecycle_tests;
