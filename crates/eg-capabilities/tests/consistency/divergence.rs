//! Documented, intentional seams between the policy registry and current classifiers.

// Where the policy table and the mirrored classifiers above disagree TODAY, on purpose:
// this is the audit value of the whole exercise -- surfacing the seams instead of
// papering over them. Each entry is `(variant, workstream, reason)`.

/// Category 1: `policy(m).mutates == true` is a conservative UPPER BOUND because the REAL
/// `access::requires_write(m)` answer depends on a runtime field (`writeback: bool`) or a
/// parsed query string, which a static per-variant table cannot model. Closing this for
/// real means making the handlers consult a PER-INVOCATION policy (EG-P0-2/EG-P0-6), not a
/// per-variant one.
pub(crate) const RUNTIME_CONDITIONAL: &[(&str, &str, &str)] = &[
    #[cfg(feature = "modality-serving")]
    (
        "ServedModality",
        "P2.14",
        "mutates is operation-conditional; authority/query/events/capabilities are reads",
    ),
    (
        "CypherQuery",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "GraphLearnFit",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "GraphLearnPredict",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "GraphQl",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineAnomaly",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineAssociate",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineCausalImpact",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineClassifyPredict",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineCluster",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineCommunity",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineEntityResolve",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineForecast",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineOntologyGap",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineProcess",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineReduce",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineRetrievalQuality",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineRiskPropagation",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineRootCause",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineSequence",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineSubgraph",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "MineText",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
    (
        "Sql",
        "EG-P0-2",
        "mutates is conservative; real answer is the `writeback` field / parsed query at runtime",
    ),
];

/// Category 2: `policy(m).mutates == true` on plain semantic/documentation grounds (the
/// method plainly changes server state), yet the variant is not mentioned ANYWHERE in
/// `access.rs::requires_write` at all -- not even in its unconditional-false fallthrough
/// path in a way that was deliberate. Some of these are legitimately governed by a
/// DIFFERENT mechanism (the OCC `Txn*` family self-routes before `dispatch_graph_op` and is
/// gated once at `BeginTxn`; KV/Blob/series ops are namespace-scoped and self-route too, and
/// in KV's case `access.rs` itself says as much in a comment) -- for those this is an
/// observation, not necessarily a bug. Others (channel/trigger/continuous-query/catalog/
/// identity/rbac/matview/foreign-source/udf-registration ops) have NO comment anywhere
/// explaining why they're absent from the classifier, which is a genuine open question this
/// workstream surfaces but does not resolve (no assigned workstream number exists yet for
/// this bucket -- recommend triaging it as a new EG-P0-x).
pub(crate) const ACCESS_RS_COVERAGE_GAP: &[(&str, &str, &str)] = &[
    #[cfg(feature = "jobs")]
    ("AnalyticsJob", "UNASSIGNED", "self-routes before dispatch_graph_op (own jobs.redb, CONCEPT:INT-P2-1), mirrors RbacAdmin's access.rs coverage gap"),
    #[cfg(feature = "statechart")]
    ("Statechart", "UNASSIGNED", "self-routes before dispatch_graph_op (own statecharts.redb, CONCEPT:INT-P2-2), mirrors AnalyticsJob's access.rs coverage gap"),
    ("BlobBegin", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobChunkPut", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobCommit", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobGc", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobRef", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("BlobUnref", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogAssign", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogReassign", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CatalogRemove", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CepSubscribe", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    // RMDD-27/-28: the whole DevelopmentLane family (Reserve/Renew/Observe/Finish/
    // Cleanup/UpdateQuota) route through `handlers::development_lane::try_handle`
    // straight to PersistenceBackend's native
    // development_lane_* redb tables, gated by the current raft placement leader --
    // like DevelopmentLaneStatus/QueryDevelopmentLane (see access.rs's own
    // REASON_NATIVE_DEVELOPMENT_LANE_READ comment), it bypasses
    // requires_write and the generic mutation gateway through its explicit
    // domain handler, so the missing requires_write entry is not an oversight.
    ("CleanupDevelopmentLane", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
    ("CepUnsubscribe", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CloseChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Commit", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateGraph", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("CreateMatView", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("DropContinuousQuery", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("DropTrigger", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("FinishDevelopmentLane", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
    ("JoinChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("LeaveChannel", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("MultiGraphBatchUpdate", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("ObserveDevelopmentLane", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
    ("PlanMatViewDefine", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlanMatViewDrop", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlanMatViewRefresh", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PlacementAdmin", "UNASSIGNED", "self-routing placement-catalog admin op (DIST-P2-5, like Reshard/CatalogAssign above); mutates per policy/semantics, but absent from access.rs::requires_write entirely -- it is not graph-scoped and never reaches dispatch_graph_op"),
    ("PublishConfirmed", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("PublishIdempotent", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RaftAddLearner", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely (self-routes in dispatch.rs before dispatch_graph_op, like Reshard/CatalogAssign)"),
    ("RenewDevelopmentLane", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
    ("ReserveDevelopmentLane", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
    ("RaftChangeMembership", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely (self-routes in dispatch.rs before dispatch_graph_op, like Reshard/CatalogAssign)"),
    ("RbacAdmin", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RebalanceExecute", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    // Pre-existing gap (predates the statechart work): `RecomputeMaterialization`
    // mutates per policy (ReasoningProjection writeback) but, like its matview siblings
    // `CreateMatView`/`RefreshMatView`, is absent from access.rs::requires_write
    // entirely. It was simply never added to this table; documented here so the
    // `mutates_matches_access_rs_for_every_governed_variant` invariant is accurate.
    ("RecomputeMaterialization", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RefreshMatView", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterContinuousQuery", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterForeignSource", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterIdentity", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterServer", "UNASSIGNED", "W2.5 fleet server push-registration/heartbeat (self-translates into Method::AddNode against __commons__, like ApplyMultisigMutation above translates into ApplyMutation); mutates per policy/semantics, but absent from access.rs::requires_write entirely -- it is not graph-scoped and never reaches dispatch_graph_op with its own identity"),
    ("RegisterTrigger", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("RegisterUdf", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Reshard", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Restore", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("SendMessage", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("Shutdown", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamCommitOffset", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamDeclare", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamPublish", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("StreamTrim", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TsAppend", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TsDeleteSeries", "UNASSIGNED", "self-routes via dispatch.rs's tsdb block to timeseries.rs, like TsAppend above; mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TsEvict", "UNASSIGNED", "self-routes via dispatch.rs's tsdb block to timeseries.rs, like TsAppend above; mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddEdge", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddEmbedding", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddMeasurement", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAddNode", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnAxiom", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnBlobRef", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnCas", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnConstruct", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnMaterializeBelief", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnPlanWriteback", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnRemoveEdge", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("TxnRemoveNode", "UNASSIGNED", "mutates per policy/semantics, but absent from access.rs::requires_write entirely"),
    ("UpdateDevelopmentLaneQuota", "UNASSIGNED", "routes via handlers::development_lane::try_handle to the native development_lane_* redb tables under the raft placement-leader gate"),
];

pub(crate) fn all_known_divergence_names() -> std::collections::HashSet<&'static str> {
    RUNTIME_CONDITIONAL
        .iter()
        .chain(ACCESS_RS_COVERAGE_GAP.iter())
        .map(|(n, _, _)| *n)
        .collect()
}
