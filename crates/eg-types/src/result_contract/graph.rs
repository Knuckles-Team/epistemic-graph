//! Declared results of the `graph` contract domain.

use super::transactions::SparqlUpdateReport;

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
}
