//! Declared results of the `cluster` contract domain.

mod lifecycle;
mod registry;
mod sharding;
mod topology;

pub use lifecycle::*;
pub use registry::*;
pub use sharding::*;
pub use topology::*;

use super::Dynamic;

method_results! {
    visit_cluster;
    CreateGraph(CreateGraph) => Json<GraphCreated>;
    DeleteGraph(DeleteGraph) => Json<GraphDeleted>;
    ListGraphs(ListGraphs) => Json<Vec<GraphListing>>;
    Reshard(Reshard) => Json<ShardReshardReport>;
    CatalogAssign(CatalogAssign) => Bool<bool>;
    CatalogReassign(CatalogReassign) => Bool<bool>;
    CatalogRemove(CatalogRemove) => Bool<bool>;
    CatalogList(CatalogList) => Json<CatalogListing>;
    RebalancePlan(RebalancePlan) => Json<RebalancePlanReport>;
    RebalanceExecute(RebalanceExecute) => Json<RebalanceExecution>;
    PlacementRoute(PlacementRoute) => Raw<PlacementRouteWire>;
    RaftAddLearner(RaftAddLearner) => Bool<bool>;
    RaftChangeMembership(RaftChangeMembership) => Bool<bool>;
    ClusterMembers(ClusterMembers) => Json<ClusterDiscoverySnapshot>;
    // The acknowledgement of the `__commons__` server-row write it performs.
    RegisterServer(RegisterServer) => Text<String>;
    ListRegisteredServers(ListRegisteredServers) => Raw<RegisteredServerListPage>;
    FleetCatalogRecordDiscovery(FleetCatalog / "record_discovery") => Raw<crate::fleet_catalog::FleetWriteReceipt>;
    FleetCatalogSetOverride(FleetCatalog / "set_override") => Raw<crate::fleet_catalog::FleetWriteReceipt>;
    FleetCatalogClearOverride(FleetCatalog / "clear_override") => Raw<crate::fleet_catalog::FleetWriteReceipt>;
    FleetCatalogList(FleetCatalog / "list") => Raw<crate::fleet_catalog::FleetCatalogPage>;
    FleetCatalogLookupRows(FleetCatalog / "lookup") => Raw<crate::fleet_catalog::FleetCatalogLookup>;
    PlacementAssign(PlacementAdmin / "assign") => Json<PlacementEpoch>;
    PlacementMove(PlacementAdmin / "move") => Json<PlacementMoveResult>;
    PlacementAbortMove(PlacementAdmin / "abort_move") => Bool<bool>;
    Ping(Ping) => Text<String>;
    Health(Health) => Json<HealthReport>;
    Shutdown(Shutdown) => Text<String>;
    CancelRequest(CancelRequest) => Bool<bool>;
    RegisterForeignSource(RegisterForeignSource) => Text<String>;
    RegisterUdf(RegisterUdf) => Text<String>;
    CreateMatView(CreateMatView) => Count<u64>;
    #[cfg(feature = "compute-dist")]
    GetMatView(GetMatView) => Raw<DistResult>;
    RefreshMatView(RefreshMatView) => Count<u64>;
    PlanMatViewDefine(PlanMatViewDefine) => Count<u64>;
    // Rows of the view's caller-authored plan, served from the result cache or the
    // incremental circuit as the MessagePack they were materialized to.
    PlanMatViewGet(PlanMatViewGet) => Raw<Dynamic> dynamic QueryRows;
    PlanMatViewRefresh(PlanMatViewRefresh) => Count<u64>;
    PlanMatViewDrop(PlanMatViewDrop) => Bool<bool>;
}
