//! Declared results of the `transactions` contract domain.

method_results! {
    visit_transactions;
    BeginTxn(BeginTxn) => Text<String>;
    TxnAddNode(TxnAddNode) => Bool<bool>;
    TxnRemoveNode(TxnRemoveNode) => Bool<bool>;
    TxnAddEdge(TxnAddEdge) => Bool<bool>;
    TxnRemoveEdge(TxnRemoveEdge) => Bool<bool>;
    TxnCas(TxnCas) => Bool<bool>;
    TxnAddEmbedding(TxnAddEmbedding) => Bool<bool>;
    TxnBlobRef(TxnBlobRef) => Bool<bool>;
    TxnAddMeasurement(TxnAddMeasurement) => Bool<bool>;
    TxnAxiom(TxnAxiom) => Bool<bool>;
    TxnConstruct(TxnConstruct) => Bool<bool>;
    TxnPlanWriteback(TxnPlanWriteback) => Bool<bool>;
    Rollback(Rollback) => Bool<bool>;
}
