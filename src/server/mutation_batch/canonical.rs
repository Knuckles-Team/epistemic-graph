//! Canonical mutation classification and one-to-one method lowering.

use crate::mutation_batch::{DurabilityDomain, MutationSurface};
use crate::protocol::{CypherMode, Method};

/// Exhaustive durability-domain classifier for mutating methods accepted by a
/// batch adapter. Public mutation inventory tests ensure no mutating method lacks
/// a domain. Methods that coordinate child batches are explicitly `MultiGraph` or
/// `CrossModal`; they are never mistaken for a local graph-row write.
///
/// The domain a method lands in decides which store owns its state and its
/// version counter, so every arm below is a data-routing statement, not a label.
/// `tests::durability_domain_classification_matches_the_golden` pins the whole
/// map so a reclassification cannot arrive as a cleanup.
///
/// # `AddEmbedding` is NOT `SemanticIndex`, and moving it there would break it
///
/// It reads like the obvious home for the semantic family, and RF-RULING-007's
/// slice plan proposed exactly that arm. It is wrong for this method, in both
/// directions, and the reason is the commit ROUTE, not the family name:
///
/// * `AddEmbedding`'s authoritative durable effect is a **graph-shard row
///   write** — `redb_store::supports_atomic_batch_rows` lists it, and
///   `upsert_durable_embedding` writes the vector into the target graph's own
///   `semantic` table inside the same transaction that advances that graph's
///   version. Its authority is the graph, and the graph's counter versions it.
/// * `DurabilityDomain::SemanticIndex` is `forbidden_in_graph_scope()`, so tagging
///   it that way and keeping the graph scope makes `MutationBatch::validate`
///   reject every `AddEmbedding` ("graph mutation scope contains a
///   store-authoritative operation").
/// * Deriving a native scope from the tag instead moves the failure one call
///   later: `AddEmbedding` is `mutation::GATEWAY_ROUTED`, so it commits through
///   `commit_gateway` -> `mutation::commit_mutation` ->
///   `PersistenceBackend::commit_mutation_batch`, and the redb backend's
///   `mutation_batch_graph_name` fails closed on any non-graph scope ("mutation
///   batch is not graph-scoped") — losing the durable embedding write.
///
/// The `Native(SemanticIndex)` producer RF-RULING-007 asks for already exists
/// and is a different write: `compute::semantic_ann_codes::SemanticCodeStore`
/// makes one ANN index GENERATION durable as one admitted maintenance mutation,
/// triggered from the dispatch write tail by
/// `server::semantic_activation::maybe_activate_after_write`. The embedding and
/// the index built from it are two writes with two authorities; only the second
/// one is store-authoritative.
pub(crate) fn domain_for(method: &Method, surface: MutationSurface) -> DurabilityDomain {
    match method {
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => DurabilityDomain::Lifecycle,
        Method::MultiGraphBatchUpdate { .. } => DurabilityDomain::MultiGraph,
        Method::Commit { .. } => DurabilityDomain::CrossModal,
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => DurabilityDomain::BlobStore,
        #[cfg(feature = "kv")]
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => {
            DurabilityDomain::KvStore
        }
        // `TsAppend`/`TsEvict`/`TsDeleteSeries` are wire-unconditional (only the
        // SERVER-SIDE handling is gated behind `tsdb`; a slim build still carries
        // the variant and routes it to the "not built" catch-all downstream) --
        // this arm must be too, or a `tsdb`-off build leaves them with no arm at
        // all now that the match below is exhaustive.
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. } => {
            DurabilityDomain::TimeSeries
        }
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => DurabilityDomain::AnalyticsJob,
        Method::KgDelegate { .. }
        | Method::SubmitWorkItem { .. }
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
        | Method::UpdateCapacityCell { .. } => DurabilityDomain::ControlPlane,
        // `Sql` is likewise wire-unconditional (gated only downstream behind
        // `query`); see the `Ts*` note above -- same reason, same fix.
        Method::Sql { .. } => DurabilityDomain::SqlCatalog,
        #[cfg(feature = "rdf")]
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph => {
            DurabilityDomain::RdfDataset
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
        | Method::BrokerRenewTag { .. } => DurabilityDomain::Broker,
        // Every other `Method` variant (query/analytics/mining/finance/... surfaces,
        // and the plain graph-row CRUD family) was never routed to a store-specific
        // domain: it always fell through to the surface-keyed default below. Naming
        // them here (instead of `_`) keeps that default AND makes the dispatch
        // exhaustive -- a future `Method` variant is a compile error at this match,
        // not a silent default.
        #[cfg(feature = "asr-native")]
        Method::Asr { .. } => default_mutation_domain(surface),
        #[cfg(feature = "blob")]
        Method::BlobFetchBegin { .. } | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. } => default_mutation_domain(surface),
        #[cfg(feature = "broker")]
        Method::StreamRead { .. } | Method::StreamCommittedOffset { .. } => default_mutation_domain(surface),
        #[cfg(feature = "compute-dist")]
        Method::DistributedCompute { .. } | Method::CreateMatView { .. }
        | Method::GetMatView { .. } | Method::RefreshMatView { .. } => default_mutation_domain(surface),
        #[cfg(feature = "cost")]
        Method::ResourceStatsPage { .. } => default_mutation_domain(surface),
        #[cfg(feature = "datascience")]
        Method::DsFitEstimator { .. } | Method::DsPredictEstimator { .. } => default_mutation_domain(surface),
        #[cfg(feature = "epistemic")]
        Method::ExplainBelief { .. } | Method::EpistemicStatus { .. }
        | Method::WhatChanged { .. } | Method::RecomputeMaterialization { .. }
        | Method::MaterializationStatus { .. } | Method::StaleMaterializations
        | Method::ResolveConflict { .. } | Method::ExplainEvidence { .. }
        | Method::CausalEstimate { .. } | Method::CausalCounterfactual { .. }
        | Method::RankByProvenance { .. } | Method::TxnMaterializeBelief { .. } => default_mutation_domain(surface),
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => default_mutation_domain(surface),
        #[cfg(feature = "finance")]
        Method::FinanceMatchOrders { .. } | Method::FinanceForensicReport { .. } => default_mutation_domain(surface),
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnFit { .. } | Method::GraphLearnPredict { .. } => default_mutation_domain(surface),
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => default_mutation_domain(surface),
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { .. } => default_mutation_domain(surface),
        #[cfg(feature = "kv")]
        Method::KvGet { .. } | Method::KvScan { .. } => default_mutation_domain(surface),
        #[cfg(feature = "matview")]
        Method::PlanMatViewDefine { .. } | Method::PlanMatViewGet { .. }
        | Method::PlanMatViewRefresh { .. } | Method::PlanMatViewDrop { .. } => default_mutation_domain(surface),
        #[cfg(feature = "mining")]
        Method::MineAssociate { .. } | Method::MineCluster { .. } | Method::MineAnomaly { .. }
        | Method::MineClassifyFit { .. } | Method::MineClassifyPredict { .. }
        | Method::MineReduce { .. } | Method::MineSequence { .. } | Method::MineForecast { .. }
        | Method::MineText { .. } | Method::MineSubgraph { .. }
        | Method::MineEntityResolve { .. } | Method::MineCausalImpact { .. }
        | Method::MineProcess { .. } | Method::MineRootCause { .. }
        | Method::MineRiskPropagation { .. } | Method::MineOntologyGap { .. }
        | Method::MineRetrievalQuality { .. } | Method::MineCommunity { .. } => default_mutation_domain(surface),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain { .. } | Method::MiningPipelineEvaluate { .. }
        | Method::MiningPipelineServe { .. } | Method::MiningPipelinePredict { .. }
        | Method::MiningPipelineCompare { .. } => default_mutation_domain(surface),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => default_mutation_domain(surface),
        #[cfg(feature = "obda")]
        Method::SparqlVirtual { .. } => default_mutation_domain(surface),
        #[cfg(feature = "owl")]
        Method::TxnAxiom { .. } | Method::OwlReason { .. } | Method::OwlReasonDistributed { .. }
        | Method::OwlExplain { .. } => default_mutation_domain(surface),
        #[cfg(feature = "quantum")]
        Method::Quantum { .. } => default_mutation_domain(surface),
        #[cfg(feature = "query")]
        Method::UnifiedQuery { .. } | Method::UnifiedQueryText { .. }
        | Method::ExplainPlan { .. } | Method::ExplainProvenance { .. }
        | Method::ExplainProvenanceByIds { .. } | Method::ExplainPolicy { .. }
        | Method::TxnPlanWriteback { .. } | Method::TxnUnifiedQuery { .. }
        | Method::TxnUnifiedQueryText { .. } => default_mutation_domain(surface),
        #[cfg(feature = "rdf")]
        Method::GetRdf | Method::RunRules { .. } => default_mutation_domain(surface),
        #[cfg(feature = "security")]
        Method::AuditVerify | Method::AuditProveInclusion { .. } => default_mutation_domain(surface),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { .. } | Method::Sparql { .. } => default_mutation_domain(surface),
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. } => default_mutation_domain(surface),
        #[cfg(feature = "statechart")]
        Method::Statechart { .. } => default_mutation_domain(surface),
        #[cfg(feature = "streaming")]
        Method::CdcRead { .. } | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. } | Method::DropContinuousQuery { .. }
        | Method::Watch { .. } | Method::RegisterTrigger { .. } | Method::DropTrigger { .. }
        | Method::ListTriggers { .. } | Method::FiredTriggers { .. }
        | Method::CepSubscribe { .. } | Method::CepPoll { .. } | Method::CepUnsubscribe { .. } => default_mutation_domain(surface),
        #[cfg(feature = "viz")]
        Method::Viz { .. } => default_mutation_domain(surface),
        #[cfg(feature = "wasm-udf")]
        Method::RegisterUdf { .. } | Method::RunUdf { .. } => default_mutation_domain(surface),
        Method::AddNode { .. } | Method::CreateNodeIfAbsent { .. } | Method::RemoveNode { .. }
        | Method::HasNode { .. } | Method::GetNodes | Method::GetNodesByLabel { .. }
        | Method::GetNodeProperties { .. } | Method::CompareAndSetNodeFields { .. }
        | Method::ClaimNext { .. } | Method::ReconcileCapacity { .. }
        | Method::CapacityStatus { .. } | Method::AgentLibrary { .. }
        | Method::AgentGraph { .. } | Method::AgentComponent { .. }
        | Method::AgentTemplate { .. } | Method::SemanticIndex { .. }
        | Method::MintWorkItemClaimCapability { .. }
        | Method::VerifyWorkItemClaimCapability { .. } | Method::QueryWorkItemReservation { .. }
        | Method::ResourceReservationStatus { .. } | Method::ReserveDevelopmentLane { .. }
        | Method::RenewDevelopmentLane { .. } | Method::ObserveDevelopmentLane { .. }
        | Method::FinishDevelopmentLane { .. } | Method::CleanupDevelopmentLane { .. }
        | Method::QueryDevelopmentLane { .. } | Method::DevelopmentLaneStatus { .. }
        | Method::UpdateDevelopmentLaneQuota { .. } | Method::CreateSummaryNode { .. }
        | Method::Consolidate { .. } | Method::Reinforce { .. } | Method::DecayNode { .. }
        | Method::DecayMemories { .. } | Method::EvictBelow { .. } | Method::Maintain { .. }
        | Method::SummaryChildren { .. } | Method::SummariesAtLevel { .. }
        | Method::AddSceneObject { .. } | Method::SetPose { .. } | Method::Reparent { .. }
        | Method::WorldTransform { .. } | Method::SceneChildren { .. }
        | Method::StartTrajectory { .. } | Method::AppendStep { .. }
        | Method::DiscountedReturn { .. } | Method::BestTrajectory { .. }
        | Method::GetNodePropertiesBatch { .. } | Method::HasNodesBatch { .. }
        | Method::NodeCount | Method::NodeIds | Method::AddEdge { .. }
        | Method::RemoveEdge { .. } | Method::InvalidateEdge { .. }
        | Method::SupersedeEdge { .. } | Method::HasEdge { .. } | Method::GetEdges
        | Method::GetEdgesPage { .. } | Method::ClearGraph | Method::GetEdgeProperties { .. }
        | Method::GetEdgePropertiesBatch { .. } | Method::EdgeCount | Method::InDegree { .. }
        | Method::OutDegree { .. } | Method::GetPredecessors { .. }
        | Method::GetSuccessors { .. } | Method::GetNeighbors { .. }
        | Method::GetNeighborsBatch { .. } | Method::UnionGetNodeProperties { .. }
        | Method::UnionGetNodesByLabel { .. } | Method::UnionGetNeighbors { .. }
        | Method::TopologicalSort | Method::FindCycle | Method::GetShortestPath { .. }
        | Method::GetBlastRadius { .. } | Method::DegreeCentrality { .. }
        | Method::DegreeCentralityAll | Method::BetweennessCentrality | Method::PageRank { .. }
        | Method::PersonalizedPageRank { .. } | Method::ConnectedComponents
        | Method::StronglyConnectedComponents | Method::MinimumSpanningTree
        | Method::CommunityDetection { .. } | Method::CommunityDetectEphemeral { .. }
        | Method::GraphColoring | Method::ComputeSimilarityEdges { .. }
        | Method::ResolveCandidates { .. } | Method::ClusterHierarchyRefresh { .. }
        | Method::ClusterHierarchyClusters { .. } | Method::ClusterHierarchyExpand { .. }
        | Method::PruneByLifecycle { .. } | Method::GetContextView { .. }
        | Method::BatchUpdate { .. } | Method::Metrics | Method::EvictLRU { .. }
        | Method::DecaySweep { .. } | Method::TouchNodes { .. } | Method::ToMsgpack
        | Method::FromMsgpack { .. } | Method::GetLedger | Method::ClearLedger
        | Method::ApplyLedger { .. } | Method::GetSubgraph { .. } | Method::Fork
        | Method::DiffAgainst { .. } | Method::CompactNodesByType { .. }
        | Method::RunDatalogReasoning { .. } | Method::ApplyChangeEnvelope { .. }
        | Method::ApplyChangeEnvelopes { .. } | Method::GetChangeEnvelope { .. }
        | Method::GetContentVersion { .. } | Method::GetChangeCursor { .. } | Method::ListGraphs
        | Method::Reshard { .. } | Method::CatalogAssign { .. } | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. } | Method::CatalogList | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. } | Method::RaftAddLearner { .. }
        | Method::RaftChangeMembership { .. } | Method::ClusterMembers
        | Method::RegisterServer { .. } | Method::PlacementRoute { .. }
        | Method::PlacementAdmin { .. } | Method::Backup { .. } | Method::Restore { .. }
        | Method::CreateChannel { .. } | Method::JoinChannel { .. }
        | Method::LeaveChannel { .. } | Method::CloseChannel { .. } | Method::SendMessage { .. }
        | Method::GetChannelMessages { .. } | Method::ListChannels
        | Method::GetChannelMembers { .. } | Method::Ping | Method::Health | Method::Shutdown
        | Method::CancelRequest { .. } | Method::Reconcile { .. } | Method::ApplyMutation { .. }
        | Method::Vf2SubgraphMatch { .. } | Method::ParseFile { .. } | Method::ParseFiles { .. }
        | Method::IndexRepository { .. } | Method::ObserveScreen { .. }
        | Method::AddEmbedding { .. } | Method::SemanticSearch { .. } | Method::Discover { .. }
        | Method::MatchOntologyTerms { .. } | Method::BatchL2Normalize { .. }
        | Method::FinanceOptimizePortfolio { .. } | Method::FinanceRiskParity { .. }
        | Method::FinanceBlackLitterman { .. } | Method::FinanceEfficientFrontier { .. }
        | Method::DsLinearRegression { .. } | Method::DsKMeans { .. } | Method::DsPca { .. }
        | Method::DsComputeStats { .. } | Method::DsTrainTestSplit { .. }
        | Method::DsSoftmax { .. } | Method::DsLogSoftmax { .. } | Method::DsCrossEntropy { .. }
        | Method::DsDpoLoss { .. } | Method::DsGrpoSurrogate { .. }
        | Method::DsKlDivergence { .. } | Method::DsAdamStep { .. } | Method::DsSgdStep { .. }
        | Method::FinanceVar { .. } | Method::FinanceCvar { .. }
        | Method::FinanceMaxDrawdown { .. } | Method::FinanceDrawdownSeries { .. }
        | Method::FinanceDownsideDeviation { .. } | Method::FinanceRiskMetrics { .. }
        | Method::FinanceMonteCarloVar { .. } | Method::FinanceStressTest { .. }
        | Method::FinanceDetectRegimes { .. } | Method::FinanceRollingZscore { .. }
        | Method::FinanceEwma { .. } | Method::FinanceSignalDecay { .. }
        | Method::FinanceCombineAlphas { .. } | Method::FinanceCrossSectionalRank { .. }
        | Method::FinanceMomentum { .. } | Method::FinanceMeanReversion { .. }
        | Method::FinanceInformationCoefficient { .. } | Method::FinanceTwap { .. }
        | Method::FinanceVwap { .. } | Method::FinanceMarketImpact { .. }
        | Method::FinancePairsTrading { .. } | Method::FinanceAvellanedaStoikov { .. }
        | Method::FinanceGltQuotes { .. } | Method::FinanceLogitQuotes { .. }
        | Method::FinanceGlostenMilgromSpread { .. } | Method::FinanceExpectedPnlRate { .. }
        | Method::FinanceBreakevenAlpha { .. } | Method::FinanceOfiSeries { .. }
        | Method::FinanceMicropriceSeries { .. } | Method::FinanceVpinPm { .. }
        | Method::FinanceHawkesMle { .. } | Method::FinanceHardimanBouchaud { .. }
        | Method::FinanceKyleLambda { .. } | Method::FinanceSurveillanceRisk { .. }
        | Method::FinanceKellyFraction { .. } | Method::FinanceBayesianKelly { .. }
        | Method::FinancePosteriorCredibleInterval { .. } | Method::FinancePurgedCpcv { .. }
        | Method::FinanceDeflatedSharpe { .. }
        | Method::FinanceProbabilityBacktestOverfit { .. }
        | Method::FinanceDieboldMariano { .. } | Method::FinanceKalmanFilter1d { .. }
        | Method::FinanceKalmanBeta { .. } | Method::FinanceKalmanVolatility { .. }
        | Method::FinanceAdfTest { .. } | Method::FinanceOuCalibrate { .. }
        | Method::FinanceOuOptimalThresholds { .. }
        | Method::FinanceMarkovTransitionMatrix { .. }
        | Method::FinanceOrderBookImbalance { .. } | Method::FinanceQueueImbalance { .. }
        | Method::FinanceRealizedVolTick { .. } | Method::FinanceSpreadReversion { .. }
        | Method::FinanceInformationRatio { .. } | Method::FinanceEffectiveIndependentN { .. }
        | Method::FinanceAlphaCombinationEngine { .. } | Method::FinanceBrierScore { .. }
        | Method::FinanceConvergenceGate { .. } | Method::FinanceEmpiricalKelly { .. }
        | Method::FinanceSabrImpliedVol { .. } | Method::FinanceSabrSmile { .. }
        | Method::FinanceSabrCalibrate { .. } | Method::RegisterIdentity { .. }
        | Method::GetIdentity { .. } | Method::RbacAdmin { .. }
        | Method::ApplyMultisigMutation { .. } | Method::CypherQuery { .. }
        | Method::NlQuery { .. } | Method::BeginTxn { .. } | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. } | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. } | Method::TxnCas { .. } | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. } | Method::TxnAddMeasurement { .. } | Method::Rollback { .. }
        | Method::TsRange { .. } | Method::TsAsofJoin { .. } | Method::TsWindow { .. }
        | Method::TsGapFill { .. } | Method::TsListSeries | Method::ShaclValidate { .. }
        | Method::IcvConfigure { .. } | Method::ShexValidate { .. } => default_mutation_domain(surface),
    }
}

/// Domain for every `Method` variant with no store-specific home: a plain
/// graph-row mutation inside an explicit transaction lands in `GraphRows`;
/// everything else (including read-only surfaces, which never reach a batch
/// adapter in practice but still need a value for `domain_for`'s exhaustive
/// match) lands in `GraphSnapshot`.
fn default_mutation_domain(surface: MutationSurface) -> DurabilityDomain {
    if matches!(surface, MutationSurface::Transaction) {
        DurabilityDomain::GraphRows
    } else {
        DurabilityDomain::GraphSnapshot
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
        Method::KgDelegate { .. }
        | Method::SubmitWorkItem { .. }
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
        Method::KgDelegate { .. }
            | Method::SubmitWorkItem { .. }
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
