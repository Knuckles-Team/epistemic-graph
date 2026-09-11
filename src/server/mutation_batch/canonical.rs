//! Canonical mutation classification and one-to-one method lowering.

use crate::mutation_batch::{DurabilityDomain, MutationSurface};
use crate::protocol::{CypherMode, Method};

/// Exhaustive durability-domain classifier for mutating methods accepted by a
/// batch adapter. Public mutation inventory tests ensure no mutating method lacks
/// a domain. Methods that coordinate child batches are explicitly `MultiGraph` or
/// `CrossModal`; they are never mistaken for a local graph-row write.
///
/// The domain a method lands in decides which store owns its state and its
/// version counter, so every arm below is a data-routing statement, not a label.
/// `tests::durability_domain_classification_matches_the_golden` pins the whole
/// map so a reclassification cannot arrive as a cleanup.
///
/// # `AddEmbedding` is NOT `SemanticIndex`, and moving it there would break it
///
/// It reads like the obvious home for the semantic family, and RF-RULING-007's
/// slice plan proposed exactly that arm. It is wrong for this method, in both
/// directions, and the reason is the commit ROUTE, not the family name:
///
/// * `AddEmbedding`'s authoritative durable effect is a **graph-shard row
///   write** — `redb_store::supports_atomic_batch_rows` lists it, and
///   `upsert_durable_embedding` writes the vector into the target graph's own
///   `semantic` table inside the same transaction that advances that graph's
///   version. Its authority is the graph, and the graph's counter versions it.
/// * `DurabilityDomain::SemanticIndex` is `forbidden_in_graph_scope()`, so tagging
///   it that way and keeping the graph scope makes `MutationBatch::validate`
///   reject every `AddEmbedding` ("graph mutation scope contains a
///   store-authoritative operation").
/// * Deriving a native scope from the tag instead moves the failure one call
///   later: `AddEmbedding` is `mutation::GATEWAY_ROUTED`, so it commits through
///   `commit_gateway` -> `mutation::commit_mutation` ->
///   `PersistenceBackend::commit_mutation_batch`, and the redb backend's
///   `mutation_batch_graph_name` fails closed on any non-graph scope ("mutation
///   batch is not graph-scoped") — losing the durable embedding write.
///
/// The `Native(SemanticIndex)` producer RF-RULING-007 asks for already exists
/// and is a different write: `compute::semantic_ann_codes::SemanticCodeStore`
/// makes one ANN index GENERATION durable as one admitted maintenance mutation,
/// triggered from the dispatch write tail by
/// `server::semantic_activation::maybe_activate_after_write`. The embedding and
/// the index built from it are two writes with two authorities; only the second
/// one is store-authoritative.
pub(crate) fn domain_for(method: &Method, surface: MutationSurface) -> DurabilityDomain {
    match method {
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => DurabilityDomain::Lifecycle,
        Method::MultiGraphBatchUpdate { .. } => DurabilityDomain::MultiGraph,
        Method::Commit { .. } => DurabilityDomain::CrossModal,
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => DurabilityDomain::BlobStore,
        #[cfg(feature = "kv")]
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => {
            DurabilityDomain::KvStore
        }
        #[cfg(feature = "tsdb")]
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. } => {
            DurabilityDomain::TimeSeries
        }
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => DurabilityDomain::AnalyticsJob,
        Method::KgDelegate { .. }
        | Method::SubmitWorkItem { .. }
        | Method::SubmitWorkItems { .. }
        | Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }
        | Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }
        | Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. } => DurabilityDomain::ControlPlane,
        #[cfg(feature = "query")]
        Method::Sql { .. } => DurabilityDomain::SqlCatalog,
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            DurabilityDomain::RdfDataset
        }
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. }
        | Method::Publish { .. }
        | Method::DeclareQueue { .. }
        | Method::PublishEx { .. }
        | Method::BrokerConsume { .. }
        | Method::BrokerAck { .. }
        | Method::BrokerReject { .. }
        | Method::SweepExpired { .. }
        | Method::StreamDeclare { .. }
        | Method::StreamPublish { .. }
        | Method::StreamTrim { .. }
        | Method::StreamCommitOffset { .. }
        | Method::PublishConfirmed { .. }
        | Method::PublishIdempotent { .. }
        | Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => DurabilityDomain::Broker,
        _ if matches!(surface, MutationSurface::Transaction) => DurabilityDomain::GraphRows,
        _ => DurabilityDomain::GraphSnapshot,
    }
}

/// Safe one-to-one canonical operations lowered before persistence. More complex RDF
/// additions/removals and query-language DML require their parser/planner to emit
/// explicit graph-row methods; they are intentionally not guessed here.
pub(super) fn lower_canonical_operation(method: Method) -> Method {
    match method {
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => Method::ClearGraph,
        other => other,
    }
}

/// Query/RDF/lifecycle adapters are classified here, not in persistence.  Their
/// operations still use exactly the same Method payload and commit machinery.
pub(super) fn surface_for(method: &Method) -> Option<MutationSurface> {
    match method {
        Method::Sql { .. }
        | Method::CypherQuery {
            mode: CypherMode::Write,
            ..
        } => Some(MutationSurface::Query),
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => Some(MutationSurface::Query),
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            Some(MutationSurface::Rdf)
        }
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => Some(MutationSurface::Lifecycle),
        Method::KgDelegate { .. }
        | Method::SubmitWorkItem { .. }
        | Method::SubmitWorkItems { .. }
        | Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }
        | Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. } => Some(MutationSurface::Job),
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => Some(MutationSurface::Job),
        Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. } => {
            Some(MutationSurface::Query)
        }
        #[cfg(feature = "broker")]
        Method::DeclareExchange { .. }
        | Method::DeleteExchange { .. }
        | Method::BindQueue { .. }
        | Method::UnbindQueue { .. }
        | Method::Publish { .. }
        | Method::DeclareQueue { .. }
        | Method::PublishEx { .. }
        | Method::BrokerConsume { .. }
        | Method::BrokerAck { .. }
        | Method::BrokerReject { .. }
        | Method::SweepExpired { .. }
        | Method::StreamDeclare { .. }
        | Method::StreamPublish { .. }
        | Method::StreamTrim { .. }
        | Method::StreamCommitOffset { .. }
        | Method::PublishConfirmed { .. }
        | Method::PublishIdempotent { .. }
        | Method::BrokerAckTag { .. }
        | Method::BrokerNackTag { .. }
        | Method::BrokerRenewTag { .. } => Some(MutationSurface::Broker),
        _ => None,
    }
}

pub(crate) fn is_work_item_method(method: &Method) -> bool {
    matches!(
        method,
        Method::KgDelegate { .. }
            | Method::SubmitWorkItem { .. }
            | Method::SubmitWorkItems { .. }
            | Method::ClaimWorkItem { .. }
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
            | Method::CasWorkItemMetadata { .. }
    )
}

/// Native capacity-cell/lease operations are served by the dedicated redb
/// ledger, not lowered into graph-node mutations.  Keeping the classifier
/// beside the existing WorkItem/resource families prevents a generic gateway
/// from accidentally treating lease authority as ordinary graph data.
pub(crate) fn is_capacity_method(method: &Method) -> bool {
    matches!(
        method,
        Method::AcquireCapacity { .. }
            | Method::RenewCapacity { .. }
            | Method::ReleaseCapacity { .. }
            | Method::ReclaimExpiredCapacity { .. }
            | Method::ReconcileCapacity { .. }
            | Method::CapacityStatus { .. }
            | Method::UpdateCapacityCell { .. }
    )
}

/// Result-producing native lifecycle mutations that must bypass the graph-core
/// coordinator. Resource reservation methods share the WorkItem transaction
/// kernel, but are not WorkItem state transitions themselves; keeping the
/// classifier distinct lets the durable-applier inventory account for their
/// GraphRedb authority separately.
pub(crate) fn is_work_item_mutation_method(method: &Method) -> bool {
    is_work_item_method(method) || is_resource_reservation_method(method)
}

pub(crate) fn is_resource_reservation_query_method(method: &Method) -> bool {
    matches!(
        method,
        Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. }
    )
}

pub(crate) fn is_resource_reservation_method(method: &Method) -> bool {
    matches!(
        method,
        Method::ReserveWorkItemResources { .. }
            | Method::ReleaseWorkItemResources { .. }
            | Method::ReclaimWorkItemResources { .. }
            | Method::QueryWorkItemReservation { .. }
            | Method::ResourceReservationStatus { .. }
            | Method::UpdateResourceHost { .. }
    )
}
