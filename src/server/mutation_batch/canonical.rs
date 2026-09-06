//! Canonical mutation classification and one-to-one method lowering.

use crate::mutation_batch::{MutationDomain, MutationSurface};
use crate::protocol::{CypherMode, Method};

/// Exhaustive durability-domain classifier for mutating methods accepted by a
/// batch adapter. Public mutation inventory tests ensure no mutating method lacks
/// a domain. Methods that coordinate child batches are explicitly `MultiGraph` or
/// `CrossModal`; they are never mistaken for a local graph-row write.
pub(crate) fn domain_for(method: &Method, surface: MutationSurface) -> MutationDomain {
    match method {
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => MutationDomain::Lifecycle,
        Method::MultiGraphBatchUpdate { .. } => MutationDomain::MultiGraph,
        Method::Commit { .. } => MutationDomain::CrossModal,
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => MutationDomain::BlobStore,
        #[cfg(feature = "kv")]
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => {
            MutationDomain::KvStore
        }
        #[cfg(feature = "tsdb")]
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. } => {
            MutationDomain::TimeSeries
        }
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => MutationDomain::AnalyticsJob,
        Method::SubmitWorkItem { .. }
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
        | Method::UpdateCapacityCell { .. } => MutationDomain::ControlPlane,
        #[cfg(feature = "query")]
        Method::Sql { .. } => MutationDomain::SqlCatalog,
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            MutationDomain::RdfDataset
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
        | Method::BrokerRenewTag { .. } => MutationDomain::Broker,
        _ if matches!(surface, MutationSurface::Transaction) => MutationDomain::GraphRows,
        _ => MutationDomain::GraphSnapshot,
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
        Method::SubmitWorkItem { .. }
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
        Method::SubmitWorkItem { .. }
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

/// RMDD-28 native development-lane hold/quota authority (`redb_store::development_lane`).
/// Covers all 8 wire methods -- the 6 write ops plus the exact-query/status reads -- so
/// dispatch.rs's dedicated block (beside the WorkItem-claim-capability block) can classify
/// the whole surface with one guard, same shape as `is_resource_reservation_method` above.
/// The module deliberately stops at the redb transaction boundary (no MutationBatch/CDC/
/// audit projection), so every one of these bypasses `is_resource_reservation_method`/
/// `is_work_item_mutation_method` and the ordinary gateway entirely.
pub(crate) fn is_development_lane_method(method: &Method) -> bool {
    matches!(
        method,
        Method::ReserveDevelopmentLane { .. }
            | Method::RenewDevelopmentLane { .. }
            | Method::ObserveDevelopmentLane { .. }
            | Method::FinishDevelopmentLane { .. }
            | Method::CleanupDevelopmentLane { .. }
            | Method::UpdateDevelopmentLaneQuota { .. }
            | Method::QueryDevelopmentLane { .. }
            | Method::DevelopmentLaneStatus { .. }
    )
}
