//! Declared results of the `coordination` contract domain.

mod jobs;
mod resources;
mod statechart;
mod work_items;

pub use jobs::*;
pub use resources::*;
pub use statechart::*;
pub use work_items::*;

use crate::delegation::KgDelegateResult;
use crate::epistemic_operations::{
    ClaimWorkItemResult, DevelopmentLaneCleanupCompleteResult, DevelopmentLaneFinishResult,
    DevelopmentLaneObserveResult, DevelopmentLaneQueryResult, DevelopmentLaneQuotaUpdateResult,
    DevelopmentLaneRenewResult, DevelopmentLaneResult, DevelopmentLaneStatusResult,
    ResourceHostUpdateResult, ResourceReservationResult, ResourceReservationStatusResult,
};
use crate::epistemic_operations_ext::{CasWorkItemMetadataResult, WorkItemClaimCapabilityResult};
use crate::native_control::{
    CapacityAcquireResult, CapacityCellUpdateResult, CapacityMutationResult, CapacityReclaimResult,
    CapacityStatusResult, SubmitWorkItemResult, SubmitWorkItemsResult,
};

method_results! {
    visit_coordination;
    // The claimed node's id and its updated properties, or nil when nothing matched.
    ClaimNext(ClaimNext) => Raw<Option<(String, serde_json::Value)>>;
    ClaimWorkItem(ClaimWorkItem) => Raw<ClaimWorkItemResult>;
    AcquireCapacity(AcquireCapacity) => Raw<CapacityAcquireResult>;
    RenewCapacity(RenewCapacity) => Raw<CapacityMutationResult>;
    ReleaseCapacity(ReleaseCapacity) => Raw<CapacityMutationResult>;
    ReclaimExpiredCapacity(ReclaimExpiredCapacity) => Raw<CapacityReclaimResult>;
    ReconcileCapacity(ReconcileCapacity) => Raw<CapacityStatusResult>;
    CapacityStatus(CapacityStatus) => Raw<CapacityStatusResult>;
    UpdateCapacityCell(UpdateCapacityCell) => Raw<CapacityCellUpdateResult>;
    KgDelegate(KgDelegate) => Raw<KgDelegateResult>;
    SubmitWorkItem(SubmitWorkItem) => Raw<SubmitWorkItemResult>;
    SubmitWorkItems(SubmitWorkItems) => Raw<SubmitWorkItemsResult>;
    MintWorkItemClaimCapability(MintWorkItemClaimCapability) => Raw<WorkItemClaimCapabilityResult>;
    VerifyWorkItemClaimCapability(VerifyWorkItemClaimCapability) => Raw<WorkItemClaimCapabilityResult>;
    RenewWorkItemLease(RenewWorkItemLease) => Json<WorkItemLeaseRenewal>;
    CommitWorkItemResult(CommitWorkItemResult) => Json<WorkItemTransition<WorkItemCommitStatus>>;
    CancelWorkItem(CancelWorkItem) => Json<WorkItemTransition<WorkItemCancelStatus>>;
    DeferWorkItem(DeferWorkItem) => Json<WorkItemDeferral>;
    CasWorkItemMetadata(CasWorkItemMetadata) => Raw<CasWorkItemMetadataResult>;
    ReserveWorkItemResources(ReserveWorkItemResources) => Raw<ResourceReservationResult>;
    ReleaseWorkItemResources(ReleaseWorkItemResources) => Raw<ResourceReservationResult>;
    ReclaimWorkItemResources(ReclaimWorkItemResources) => Raw<ResourceReservationResult>;
    QueryWorkItemReservation(QueryWorkItemReservation) => Raw<ResourceReservationResult>;
    ResourceReservationStatus(ResourceReservationStatus) => Raw<ResourceReservationStatusResult>;
    UpdateResourceHost(UpdateResourceHost) => Raw<ResourceHostUpdateResult>;
    ReserveDevelopmentLane(ReserveDevelopmentLane) => Raw<DevelopmentLaneResult>;
    RenewDevelopmentLane(RenewDevelopmentLane) => Raw<DevelopmentLaneRenewResult>;
    ObserveDevelopmentLane(ObserveDevelopmentLane) => Raw<DevelopmentLaneObserveResult>;
    FinishDevelopmentLane(FinishDevelopmentLane) => Raw<DevelopmentLaneFinishResult>;
    CleanupDevelopmentLane(CleanupDevelopmentLane) => Raw<DevelopmentLaneCleanupCompleteResult>;
    QueryDevelopmentLane(QueryDevelopmentLane) => Raw<DevelopmentLaneQueryResult>;
    DevelopmentLaneStatus(DevelopmentLaneStatus) => Raw<DevelopmentLaneStatusResult>;
    UpdateDevelopmentLaneQuota(UpdateDevelopmentLaneQuota) => Raw<DevelopmentLaneQuotaUpdateResult>;
    ResourceStatsPage(ResourceStatsPage) => Json<ResourceSnapshot>;
    JobSubmit(AnalyticsJob / "Submit") => Json<AnalyticsJobRecord>;
    JobStatus(AnalyticsJob / "Status") => Json<AnalyticsJobRecord>;
    JobCancel(AnalyticsJob / "Cancel") => Json<AnalyticsJobRecord>;
    JobResume(AnalyticsJob / "Resume") => Json<AnalyticsJobRecord>;
    // `null` when no eligible job is waiting.
    JobWorkerClaim(AnalyticsJob / "WorkerClaim") => Json<Option<WorkerJobClaim>>;
    JobWorkerRenew(AnalyticsJob / "WorkerRenew") => Json<JobWorkerLease>;
    JobWorkerCheckpoint(AnalyticsJob / "WorkerCheckpoint") => Json<AnalyticsJobRecord>;
    JobWorkerStage(AnalyticsJob / "WorkerStage") => Json<AnalyticsJobRecord>;
    JobWorkerPublish(AnalyticsJob / "WorkerPublish") => Json<AnalyticsJobRecord>;
    JobWorkerFail(AnalyticsJob / "WorkerFail") => Json<AnalyticsJobRecord>;
    JobWorkerCancel(AnalyticsJob / "WorkerCancel") => Json<AnalyticsJobRecord>;
    StatechartDefine(Statechart / "Define") => Json<StatechartDefinitionId>;
    StatechartInstantiate(Statechart / "Instantiate") => Json<StatechartInstance>;
    StatechartSendEvent(Statechart / "SendEvent") => Json<StatechartEventOutcome>;
    StatechartGetState(Statechart / "GetState") => Json<StatechartInstance>;
    StatechartList(Statechart / "List") => Json<StatechartInstanceList>;
}
