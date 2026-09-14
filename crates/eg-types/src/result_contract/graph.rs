//! Declared results of the `graph` contract domain.

use super::transactions::SparqlUpdateReport;
use super::Dynamic;
use crate::rdf_report::LoadReport;
use crate::types::{
    CompactNodesResult, DecayStats, GraphDiff, GraphMetrics, PropertyBlob, PruneStats, ScenePose,
    SubgraphResult,
};

method_results! {
    visit_graph;
    ApplyMutation(ApplyMutation) => Json<SparqlUpdateReport>;
    CreateNodeIfAbsent(CreateNodeIfAbsent) => Bool<bool>;
    HasNode(HasNode) => Bool<bool>;
    GetNodes(GetNodes) => NodeList<Vec<(String, serde_json::Value)>>;
    GetNodesByLabel(GetNodesByLabel) => NodeList<Vec<(String, serde_json::Value)>>;
    CompareAndSetNodeFields(CompareAndSetNodeFields) => Bool<bool>;
    CreateSummaryNode(CreateSummaryNode) => Text<String>;
    Consolidate(Consolidate) => Text<String>;
    Reinforce(Reinforce) => Bool<bool>;
    DecayNode(DecayNode) => Bool<bool>;
    DecayMemories(DecayMemories) => Count<u64>;
    EvictBelow(EvictBelow) => Ids<Vec<String>>;
    SummaryChildren(SummaryChildren) => Ids<Vec<String>>;
    SummariesAtLevel(SummariesAtLevel) => Ids<Vec<String>>;
    Reparent(Reparent) => Bool<bool>;
    SceneChildren(SceneChildren) => Ids<Vec<String>>;
    StartTrajectory(StartTrajectory) => Text<String>;
    DiscountedReturn(DiscountedReturn) => Float<f64>;
    NodeCount(NodeCount) => Count<u64>;
    NodeIds(NodeIds) => Ids<Vec<String>>;
    AddEdge(AddEdge) => Text<String>;
    InvalidateEdge(InvalidateEdge) => Count<u64>;
    HasEdge(HasEdge) => Bool<bool>;
    GetEdges(GetEdges) => EdgeList<Vec<(String, String, Vec<u8>)>>;
    ClearGraph(ClearGraph) => Text<String>;
    EdgeCount(EdgeCount) => Count<u64>;
    InDegree(InDegree) => Count<u64>;
    OutDegree(OutDegree) => Count<u64>;
    GetPredecessors(GetPredecessors) => Ids<Vec<String>>;
    GetSuccessors(GetSuccessors) => Ids<Vec<String>>;
    GetNeighbors(GetNeighbors) => Ids<Vec<String>>;
    UnionGetNeighbors(UnionGetNeighbors) => Ids<Vec<String>>;
    EvictLRU(EvictLRU) => Count<u64>;
    TouchNodes(TouchNodes) => Count<u64>;
    Reconcile(Reconcile) => Text<String>;
    DropNamedGraph(DropNamedGraph) => Text<String>;
    AddNode(AddNode) => Text<String>;
    RemoveNode(RemoveNode) => Text<String>;
    // The node's stored property object, verbatim; `null` when the node is absent or hidden.
    GetNodeProperties(GetNodeProperties) => RawOrNull<Dynamic> dynamic CallerProperties;
    // `(decayed count, evicted ids)`.
    Maintain(Maintain) => Raw<(usize, Vec<String>)>;
    AddSceneObject(AddSceneObject) => Text<String>;
    SetPose(SetPose) => Bool<bool>;
    WorldTransform(WorldTransform) => Json<Option<ScenePose>>;
    AppendStep(AppendStep) => Raw<Option<String>>;
    BestTrajectory(BestTrajectory) => Raw<Option<String>>;
    // `[id, properties | nil]` in request order.
    GetNodePropertiesBatch(GetNodePropertiesBatch) => Raw<Vec<(String, Option<PropertyBlob>)>>;
    HasNodesBatch(HasNodesBatch) => Raw<Vec<bool>>;
    RemoveEdge(RemoveEdge) => Text<String>;
    SupersedeEdge(SupersedeEdge) => Text<String>;
    // `[source, target, ordinal, properties]` edge keys after the cursor.
    GetEdgesPage(GetEdgesPage) => Raw<Vec<(String, String, u32, Vec<u8>)>>;
    // The decoded property object of every parallel edge between the pair.
    GetEdgeProperties(GetEdgeProperties) => Json<Dynamic> dynamic CallerProperties;
    // Per requested pair, the property blobs of its parallel edges.
    GetEdgePropertiesBatch(GetEdgePropertiesBatch) => Raw<Vec<Vec<PropertyBlob>>>;
    GetNeighborsBatch(GetNeighborsBatch) => Raw<Vec<(String, Vec<String>)>>;
    UnionGetNodeProperties(UnionGetNodeProperties) => RawOrNull<Dynamic> dynamic CallerProperties;
    UnionGetNodesByLabel(UnionGetNodesByLabel) => NodeList<Vec<(String, serde_json::Value)>>;
    PruneByLifecycle(PruneByLifecycle) => Json<PruneStats>;
    Metrics(Metrics) => Json<GraphMetrics>;
    DecaySweep(DecaySweep) => Json<DecayStats>;
    GetSubgraph(GetSubgraph) => Json<SubgraphResult>;
    // A JSON rendering of the forked graph snapshot, whose node and edge rows are the
    // property objects callers wrote.
    Fork(Fork) => Json<Dynamic> dynamic CallerProperties;
    DiffAgainst(DiffAgainst) => Json<GraphDiff>;
    CompactNodesByType(CompactNodesByType) => Json<CompactNodesResult>;
    AddTriples(AddTriples) => Raw<LoadReport>;
    RemoveTriples(RemoveTriples) => Count<u64>;
}
