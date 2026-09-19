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
        #[cfg(feature = "asr-whisper")]
        Method::Asr { .. } => default_mutation_domain(surface),
        #[cfg(feature = "blob")]
        Method::BlobFetchEnd { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchBegin { .. } => default_mutation_domain(surface),
        #[cfg(feature = "broker")]
        Method::StreamCommittedOffset { .. } | Method::StreamRead { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "compute-dist")]
        Method::GetMatView { .. }
        | Method::CreateMatView { .. }
        | Method::DistributedCompute { .. }
        | Method::RefreshMatView { .. } => default_mutation_domain(surface),
        #[cfg(feature = "cost")]
        Method::ResourceStatsPage { .. } => default_mutation_domain(surface),
        #[cfg(feature = "datascience")]
        Method::DsFitEstimator { .. } | Method::DsPredictEstimator { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { .. }
        | Method::StaleMaterializations
        | Method::MaterializationStatus { .. }
        | Method::RecomputeMaterialization { .. }
        | Method::CausalEstimate { .. }
        | Method::ExplainBelief { .. }
        | Method::CausalCounterfactual { .. }
        | Method::EpistemicStatus { .. }
        | Method::ExplainEvidence { .. }
        | Method::RankByProvenance { .. }
        | Method::WhatChanged { .. }
        | Method::ResolveConflict { .. } => default_mutation_domain(surface),
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => default_mutation_domain(surface),
        #[cfg(feature = "finance")]
        Method::FinanceMatchOrders { .. } | Method::FinanceForensicReport { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "graphlearn")]
        Method::GraphLearnPredict { .. } | Method::GraphLearnFit { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "graphql")]
        Method::GraphQl { .. } => default_mutation_domain(surface),
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { .. } => default_mutation_domain(surface),
        #[cfg(feature = "kv")]
        Method::KvScan { .. } | Method::KvGet { .. } => default_mutation_domain(surface),
        #[cfg(feature = "matview")]
        Method::PlanMatViewRefresh { .. }
        | Method::PlanMatViewGet { .. }
        | Method::PlanMatViewDefine { .. }
        | Method::PlanMatViewDrop { .. } => default_mutation_domain(surface),
        #[cfg(feature = "mining")]
        Method::MineProcess { .. }
        | Method::MineForecast { .. }
        | Method::MineSubgraph { .. }
        | Method::MineEntityResolve { .. }
        | Method::MineAnomaly { .. }
        | Method::MineClassifyPredict { .. }
        | Method::MineReduce { .. }
        | Method::MineRiskPropagation { .. }
        | Method::MineCommunity { .. }
        | Method::MineCausalImpact { .. }
        | Method::MineOntologyGap { .. }
        | Method::MineSequence { .. }
        | Method::MineAssociate { .. }
        | Method::MineClassifyFit { .. }
        | Method::MineCluster { .. }
        | Method::MineRetrievalQuality { .. }
        | Method::MineRootCause { .. }
        | Method::MineText { .. } => default_mutation_domain(surface),
        #[cfg(feature = "ml-pipeline")]
        Method::MiningPipelineTrain { .. }
        | Method::MiningPipelineServe { .. }
        | Method::MiningPipelinePredict { .. }
        | Method::MiningPipelineEvaluate { .. }
        | Method::MiningPipelineCompare { .. } => default_mutation_domain(surface),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => default_mutation_domain(surface),
        #[cfg(feature = "obda")]
        Method::SparqlVirtual { .. } => default_mutation_domain(surface),
        #[cfg(feature = "owl")]
        Method::OwlReasonDistributed { .. }
        | Method::OwlExplain { .. }
        | Method::OwlReason { .. }
        | Method::TxnAxiom { .. } => default_mutation_domain(surface),
        #[cfg(feature = "quantum")]
        Method::Quantum { .. } => default_mutation_domain(surface),
        #[cfg(feature = "query")]
        Method::UnifiedQueryText { .. }
        | Method::TxnUnifiedQueryText { .. }
        | Method::TxnPlanWriteback { .. }
        | Method::TxnUnifiedQuery { .. }
        | Method::UnifiedQuery { .. }
        | Method::ExplainProvenanceByIds { .. }
        | Method::ExplainProvenance { .. }
        | Method::ExplainPlan { .. }
        | Method::ExplainPolicy { .. } => default_mutation_domain(surface),
        #[cfg(feature = "rdf")]
        Method::RunRules { .. } | Method::GetRdf => default_mutation_domain(surface),
        #[cfg(feature = "security")]
        Method::AuditProveInclusion { .. } | Method::AuditVerify => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "sparql")]
        Method::Sparql { .. } | Method::TxnConstruct { .. } => default_mutation_domain(surface),
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "statechart")]
        Method::Statechart { .. } => default_mutation_domain(surface),
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { .. }
        | Method::CepUnsubscribe { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropTrigger { .. }
        | Method::CepSubscribe { .. }
        | Method::RegisterTrigger { .. }
        | Method::CepPoll { .. }
        | Method::FiredTriggers { .. }
        | Method::Watch { .. }
        | Method::ListTriggers { .. }
        | Method::DropContinuousQuery { .. }
        | Method::CdcRead { .. } => default_mutation_domain(surface),
        #[cfg(feature = "viz")]
        Method::Viz { .. } => default_mutation_domain(surface),
        #[cfg(feature = "wasm-udf")]
        Method::RunUdf { .. } | Method::RegisterUdf { .. } => default_mutation_domain(surface),
        Method::ApplyMultisigMutation { .. }
        | Method::AddEmbedding { .. }
        | Method::JoinChannel { .. }
        | Method::AddSceneObject { .. }
        | Method::VerifyWorkItemClaimCapability { .. }
        | Method::FromMsgpack { .. }
        | Method::BatchUpdate { .. }
        | Method::DsAdamStep { .. }
        | Method::ClusterHierarchyRefresh { .. }
        | Method::TxnBlobRef { .. }
        | Method::Maintain { .. }
        | Method::AgentComponent { .. }
        | Method::BeginTxn { .. }
        | Method::AddNode { .. }
        | Method::FinanceHawkesMle { .. }
        | Method::CleanupDevelopmentLane { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::ConnectedComponents
        | Method::DecaySweep { .. }
        | Method::GetNodesByLabel { .. }
        | Method::ApplyLedger { .. }
        | Method::ResourceReservationStatus { .. }
        | Method::TxnAddEdge { .. }
        | Method::FinanceMarkovTransitionMatrix { .. }
        | Method::DsCrossEntropy { .. }
        | Method::RemoveNode { .. }
        | Method::TouchNodes { .. }
        | Method::CatalogRemove { .. }
        | Method::DsSgdStep { .. }
        | Method::GetEdgeProperties { .. }
        | Method::InvalidateEdge { .. }
        | Method::Consolidate { .. }
        | Method::DsLinearRegression { .. }
        | Method::FinanceBreakevenAlpha { .. }
        | Method::DsLogSoftmax { .. }
        | Method::FinanceEwma { .. }
        | Method::DsSoftmax { .. }
        | Method::FinanceOuOptimalThresholds { .. }
        | Method::FinancePurgedCpcv { .. }
        | Method::FinanceCrossSectionalRank { .. }
        | Method::FinishDevelopmentLane { .. }
        | Method::LeaveChannel { .. }
        | Method::RunDatalogReasoning { .. }
        | Method::GraphColoring
        | Method::FinanceLogitQuotes { .. }
        | Method::FinanceOptimizePortfolio { .. }
        | Method::TxnAddNode { .. }
        | Method::RaftAddLearner { .. }
        | Method::Reshard { .. }
        | Method::ResolveCandidates { .. }
        | Method::SemanticIndex { .. }
        | Method::FinanceQueueImbalance { .. }
        | Method::OutDegree { .. }
        | Method::SendMessage { .. }
        | Method::WorldTransform { .. }
        | Method::FinanceVwap { .. }
        | Method::FinanceDrawdownSeries { .. }
        | Method::DecayNode { .. }
        | Method::FinanceMomentum { .. }
        | Method::ObserveScreen { .. }
        | Method::GetEdgePropertiesBatch { .. }
        | Method::MinimumSpanningTree
        | Method::FinanceCombineAlphas { .. }
        | Method::QueryDevelopmentLane { .. }
        | Method::AgentLibrary { .. }
        | Method::FinanceExpectedPnlRate { .. }
        | Method::DiffAgainst { .. }
        | Method::Health
        | Method::FinanceInformationCoefficient { .. }
        | Method::DsTrainTestSplit { .. }
        | Method::GetContextView { .. }
        | Method::CloseChannel { .. }
        | Method::ClaimNext { .. }
        | Method::FinanceMaxDrawdown { .. }
        | Method::MintWorkItemClaimCapability { .. }
        | Method::ClusterHierarchyClusters { .. }
        | Method::HasNode { .. }
        | Method::SceneChildren { .. }
        | Method::RaftChangeMembership { .. }
        | Method::FinanceSabrSmile { .. }
        | Method::RenewDevelopmentLane { .. }
        | Method::FinanceMarketImpact { .. }
        | Method::RebalancePlan { .. }
        | Method::SemanticSearch { .. }
        | Method::BetweennessCentrality
        | Method::NodeCount
        | Method::GetPredecessors { .. }
        | Method::ReconcileCapacity { .. }
        | Method::GetNodes
        | Method::ApplyMutation { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::PruneByLifecycle { .. }
        | Method::FinanceDetectRegimes { .. }
        | Method::TxnAddMeasurement { .. }
        | Method::DsKlDivergence { .. }
        | Method::ParseFile { .. }
        | Method::FinanceBayesianKelly { .. }
        | Method::FinanceDieboldMariano { .. }
        | Method::AgentGraph { .. }
        | Method::Ping
        | Method::PageRank { .. }
        | Method::SummaryChildren { .. }
        | Method::FinanceAlphaCombinationEngine { .. }
        | Method::QueryWorkItemReservation { .. }
        | Method::ApplyChangeEnvelopes { .. }
        | Method::GetShortestPath { .. }
        | Method::TsGapFill { .. }
        | Method::GetChangeCursor { .. }
        | Method::FinanceProbabilityBacktestOverfit { .. }
        | Method::PlacementAdmin { .. }
        | Method::FinanceDeflatedSharpe { .. }
        | Method::ListChannels
        | Method::GetLedger
        | Method::ShaclValidate { .. }
        | Method::FinanceConvergenceGate { .. }
        | Method::FinanceGlostenMilgromSpread { .. }
        | Method::FinanceCvar { .. }
        | Method::RegisterServer { .. }
        | Method::Reparent { .. }
        | Method::FinanceSurveillanceRisk { .. }
        | Method::AddEdge { .. }
        | Method::CapacityStatus { .. }
        | Method::FinanceVar { .. }
        | Method::DsPca { .. }
        | Method::ObserveDevelopmentLane { .. }
        | Method::Backup { .. }
        | Method::MatchOntologyTerms { .. }
        | Method::IndexRepository { .. }
        | Method::ClearLedger
        | Method::FinanceKalmanBeta { .. }
        | Method::RegisterIdentity { .. }
        | Method::FinanceSabrCalibrate { .. }
        | Method::SetPose { .. }
        | Method::TxnCas { .. }
        | Method::PersonalizedPageRank { .. }
        | Method::GetSuccessors { .. }
        | Method::FinanceTwap { .. }
        | Method::FinanceOrderBookImbalance { .. }
        | Method::Fork
        | Method::RbacAdmin { .. }
        | Method::FinanceKalmanFilter1d { .. }
        | Method::FinanceRiskParity { .. }
        | Method::ReserveDevelopmentLane { .. }
        | Method::NlQuery { .. }
        | Method::FinanceMeanReversion { .. }
        | Method::FinanceHardimanBouchaud { .. }
        | Method::DsDpoLoss { .. }
        | Method::SupersedeEdge { .. }
        | Method::DiscountedReturn { .. }
        | Method::CatalogReassign { .. }
        | Method::RemoveEdge { .. }
        | Method::EdgeCount
        | Method::PlacementRoute { .. }
        | Method::FinanceOuCalibrate { .. }
        | Method::AppendStep { .. }
        | Method::ParseFiles { .. }
        | Method::Metrics
        | Method::CancelRequest { .. }
        | Method::DegreeCentrality { .. }
        | Method::Restore { .. }
        | Method::GetBlastRadius { .. }
        | Method::EvictLRU { .. }
        | Method::FinancePairsTrading { .. }
        | Method::DegreeCentralityAll
        | Method::ClusterMembers
        | Method::FinanceMicropriceSeries { .. }
        | Method::TsRange { .. }
        | Method::IcvConfigure { .. }
        | Method::ShexValidate { .. }
        | Method::DevelopmentLaneStatus { .. }
        | Method::CommunityDetection { .. }
        | Method::NodeIds
        | Method::TsAsofJoin { .. }
        | Method::InDegree { .. }
        | Method::FinanceRealizedVolTick { .. }
        | Method::TsWindow { .. }
        | Method::GetNeighborsBatch { .. }
        | Method::GetNodeProperties { .. }
        | Method::GetIdentity { .. }
        | Method::TsListSeries
        | Method::UpdateDevelopmentLaneQuota { .. }
        | Method::Reconcile { .. }
        | Method::UnionGetNodesByLabel { .. }
        | Method::StartTrajectory { .. }
        | Method::BestTrajectory { .. }
        | Method::FinanceGltQuotes { .. }
        | Method::TopologicalSort
        | Method::Vf2SubgraphMatch { .. }
        | Method::RebalanceExecute { .. }
        | Method::FinanceSpreadReversion { .. }
        | Method::FinanceStressTest { .. }
        | Method::GetSubgraph { .. }
        | Method::FinanceBlackLitterman { .. }
        | Method::UnionGetNeighbors { .. }
        | Method::FinanceOfiSeries { .. }
        | Method::FinanceEmpiricalKelly { .. }
        | Method::FinanceKellyFraction { .. }
        | Method::FinanceDownsideDeviation { .. }
        | Method::FinanceKalmanVolatility { .. }
        | Method::Reinforce { .. }
        | Method::FinanceEfficientFrontier { .. }
        | Method::CreateNodeIfAbsent { .. }
        | Method::Rollback { .. }
        | Method::ToMsgpack
        | Method::GetContentVersion { .. }
        | Method::ListGraphs
        | Method::CatalogList
        | Method::DsGrpoSurrogate { .. }
        | Method::FinanceRollingZscore { .. }
        | Method::FinanceMonteCarloVar { .. }
        | Method::GetNeighbors { .. }
        | Method::FinanceRiskMetrics { .. }
        | Method::StronglyConnectedComponents
        | Method::FindCycle
        | Method::Discover { .. }
        | Method::CompactNodesByType { .. }
        | Method::CreateChannel { .. }
        | Method::FinanceKyleLambda { .. }
        | Method::FinanceInformationRatio { .. }
        | Method::GetEdgesPage { .. }
        | Method::CreateSummaryNode { .. }
        | Method::GetChannelMessages { .. }
        | Method::SummariesAtLevel { .. }
        | Method::FinanceAdfTest { .. }
        | Method::DsKMeans { .. }
        | Method::FinanceAvellanedaStoikov { .. }
        | Method::CypherQuery { .. }
        | Method::GetEdges
        | Method::DsComputeStats { .. }
        | Method::AgentTemplate { .. }
        | Method::FinanceSabrImpliedVol { .. }
        | Method::CatalogAssign { .. }
        | Method::EvictBelow { .. }
        | Method::Shutdown
        | Method::UnionGetNodeProperties { .. }
        | Method::ClusterHierarchyExpand { .. }
        | Method::FinanceVpinPm { .. }
        | Method::GetNodePropertiesBatch { .. }
        | Method::ApplyChangeEnvelope { .. }
        | Method::TxnRemoveNode { .. }
        | Method::GetChannelMembers { .. }
        | Method::FinanceBrierScore { .. }
        | Method::HasNodesBatch { .. }
        | Method::CommunityDetectEphemeral { .. }
        | Method::BatchL2Normalize { .. }
        | Method::HasEdge { .. }
        | Method::ComputeSimilarityEdges { .. }
        | Method::FinanceSignalDecay { .. }
        | Method::FinancePosteriorCredibleInterval { .. }
        | Method::FinanceEffectiveIndependentN { .. }
        | Method::GetChangeEnvelope { .. }
        | Method::CompareAndSetNodeFields { .. }
        | Method::ClearGraph
        | Method::DecayMemories { .. } => default_mutation_domain(surface),
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
