//! Declared results of the `coordination` contract domain.

#[cfg(feature = "jobs")]
mod jobs;
mod resources;
#[cfg(feature = "statechart")]
mod statechart;
mod work_items;

#[cfg(feature = "jobs")]
pub use jobs::*;
pub use resources::*;
#[cfg(feature = "statechart")]
pub use statechart::*;
pub use work_items::*;

use crate::control_lease::{ControlLeaseIssued, ControlLeaseTransition, ControlLeaseView};
use crate::decision::DecisionJobRecord;
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
use crate::work_item_read::{WorkItemPage, WorkItemView};

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
    // EH-219 typed WorkItem reads. `null` when no WorkItem with this id is
    // visible to the verified tenant.
    GetWorkItem(GetWorkItem) => Raw<Option<WorkItemView>>;
    ListWorkItems(ListWorkItems) => Raw<WorkItemPage>;
    // graph-os EG-2 native control leases. `null` when no lease with this id
    // is visible to the verified tenant.
    IssueControlLease(IssueControlLease) => Json<ControlLeaseIssued>;
    TransitionControlLease(TransitionControlLease) => Json<ControlLeaseTransition>;
    GetControlLease(GetControlLease) => Raw<Option<ControlLeaseView>>;
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
    #[cfg(feature = "jobs")]
    JobSubmit(AnalyticsJob / "Submit") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobStatus(AnalyticsJob / "Status") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobCancel(AnalyticsJob / "Cancel") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobResume(AnalyticsJob / "Resume") => Json<AnalyticsJobRecord>;
    // `null` when no eligible job is waiting.
    #[cfg(feature = "jobs")]
    JobWorkerClaim(AnalyticsJob / "WorkerClaim") => Json<Option<WorkerJobClaim>>;
    #[cfg(feature = "jobs")]
    JobWorkerRenew(AnalyticsJob / "WorkerRenew") => Json<JobWorkerLease>;
    #[cfg(feature = "jobs")]
    JobWorkerCheckpoint(AnalyticsJob / "WorkerCheckpoint") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobWorkerStage(AnalyticsJob / "WorkerStage") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobWorkerPublish(AnalyticsJob / "WorkerPublish") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobWorkerFail(AnalyticsJob / "WorkerFail") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "jobs")]
    JobWorkerCancel(AnalyticsJob / "WorkerCancel") => Json<AnalyticsJobRecord>;
    #[cfg(feature = "statechart")]
    StatechartDefine(Statechart / "Define") => Json<StatechartDefinitionId>;
    #[cfg(feature = "statechart")]
    StatechartInstantiate(Statechart / "Instantiate") => Json<StatechartInstance>;
    #[cfg(feature = "statechart")]
    StatechartSendEvent(Statechart / "SendEvent") => Json<StatechartEventOutcome>;
    #[cfg(feature = "statechart")]
    StatechartGetState(Statechart / "GetState") => Json<StatechartInstance>;
    #[cfg(feature = "statechart")]
    StatechartList(Statechart / "List") => Json<StatechartInstanceList>;
    DecisionFitSubmit(DecisionFit / "submit") => Raw<DecisionJobRecord>;
    DecisionFitStatus(DecisionFit / "status") => Raw<Option<DecisionJobRecord>>;
    DecisionEvalSubmit(DecisionEval / "submit") => Raw<DecisionJobRecord>;
    DecisionEvalStatus(DecisionEval / "status") => Raw<Option<DecisionJobRecord>>;
}
