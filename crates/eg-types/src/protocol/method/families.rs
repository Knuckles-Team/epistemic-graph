//! Named families of mutating `Method` variants.
//!
//! Several engine classifiers (write access, durable replay, atomic row
//! batches, canonical mutation domain and surface) each need the same variant
//! groups. They are declared once here, next to the enum, and each classifier
//! selects the families it covers with `matches!(method.write_family(), ...)`
//! plus its own remaining members.

use super::Method;

/// A named group of mutating `Method` variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MethodWriteFamily {
    /// Node/edge writes that change graph elements directly.
    GraphElement,
    /// WorkItem submission and delegation.
    WorkItemSubmission,
    /// Capacity-lease writes. `ReconcileCapacity` and `CapacityStatus` are not
    /// members.
    CapacityLease,
    /// Lease-scoped WorkItem transitions after a claim.
    WorkItemLease,
    /// WorkItem resource reservation and host accounting writes.
    WorkItemResource,
    /// Agent-memory, scene-graph and trajectory writes. Their paired reads
    /// (SummaryChildren, WorldTransform, BestTrajectory, ...) are not members.
    MemoryScene,
    /// Message-broker and stream writes (feature `broker`): exchange/queue
    /// administration, publish, consume/ack, streams, publisher confirms and
    /// idempotent producers. Pure reads (`StreamRead`, `StreamCommittedOffset`)
    /// are not members.
    Broker,
}

impl Method {
    /// The write family this variant belongs to, or `None` for a variant that is
    /// in no named family.
    pub fn write_family(&self) -> Option<MethodWriteFamily> {
        Some(match self {
            Self::AddNode { .. }
            | Self::CreateNodeIfAbsent { .. }
            | Self::RemoveNode { .. }
            | Self::CompareAndSetNodeFields { .. }
            | Self::AddEdge { .. }
            | Self::RemoveEdge { .. }
            | Self::InvalidateEdge { .. }
            | Self::SupersedeEdge { .. }
            | Self::BatchUpdate { .. } => MethodWriteFamily::GraphElement,
            Self::KgDelegate { .. }
            | Self::SubmitWorkItem { .. }
            | Self::SubmitWorkItems { .. } => MethodWriteFamily::WorkItemSubmission,
            Self::AcquireCapacity { .. }
            | Self::RenewCapacity { .. }
            | Self::ReleaseCapacity { .. }
            | Self::ReclaimExpiredCapacity { .. }
            | Self::UpdateCapacityCell { .. } => MethodWriteFamily::CapacityLease,
            Self::ClaimWorkItem { .. }
            | Self::RenewWorkItemLease { .. }
            | Self::CommitWorkItemResult { .. }
            | Self::CancelWorkItem { .. }
            | Self::DeferWorkItem { .. }
            | Self::CasWorkItemMetadata { .. }
            | Self::IssueControlLease { .. }
            | Self::TransitionControlLease { .. } => MethodWriteFamily::WorkItemLease,
            Self::ReserveWorkItemResources { .. }
            | Self::ReleaseWorkItemResources { .. }
            | Self::ReclaimWorkItemResources { .. }
            | Self::UpdateResourceHost { .. } => MethodWriteFamily::WorkItemResource,
            Self::CreateSummaryNode { .. }
            | Self::Consolidate { .. }
            | Self::Reinforce { .. }
            | Self::DecayNode { .. }
            | Self::DecayMemories { .. }
            | Self::EvictBelow { .. }
            | Self::Maintain { .. }
            | Self::AddSceneObject { .. }
            | Self::SetPose { .. }
            | Self::Reparent { .. }
            | Self::StartTrajectory { .. }
            | Self::AppendStep { .. } => MethodWriteFamily::MemoryScene,
            #[cfg(feature = "broker")]
            Self::DeclareExchange { .. }
            | Self::DeleteExchange { .. }
            | Self::BindQueue { .. }
            | Self::UnbindQueue { .. }
            | Self::Publish { .. }
            | Self::DeclareQueue { .. }
            | Self::PublishEx { .. }
            | Self::BrokerConsume { .. }
            | Self::BrokerAck { .. }
            | Self::BrokerReject { .. }
            | Self::SweepExpired { .. }
            | Self::StreamDeclare { .. }
            | Self::StreamPublish { .. }
            | Self::StreamTrim { .. }
            | Self::StreamCommitOffset { .. }
            | Self::PublishConfirmed { .. }
            | Self::PublishIdempotent { .. }
            | Self::BrokerAckTag { .. }
            | Self::BrokerNackTag { .. }
            | Self::BrokerRenewTag { .. } => MethodWriteFamily::Broker,
            _ => return None,
        })
    }
}
