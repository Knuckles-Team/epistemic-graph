//! Declared results of the `cluster` contract domain.

method_results! {
    visit_cluster;
    CatalogAssign(CatalogAssign) => Bool<bool>;
    CatalogReassign(CatalogReassign) => Bool<bool>;
    CatalogRemove(CatalogRemove) => Bool<bool>;
    RaftAddLearner(RaftAddLearner) => Bool<bool>;
    RaftChangeMembership(RaftChangeMembership) => Bool<bool>;
    Ping(Ping) => Text<String>;
    Shutdown(Shutdown) => Text<String>;
    CancelRequest(CancelRequest) => Bool<bool>;
    RegisterForeignSource(RegisterForeignSource) => Text<String>;
    RegisterUdf(RegisterUdf) => Text<String>;
}
