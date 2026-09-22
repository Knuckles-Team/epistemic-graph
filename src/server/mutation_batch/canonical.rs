//! Canonical mutation classification and one-to-one method lowering.

use crate::mutation_batch::{DurabilityDomain, MutationSurface};
use crate::protocol::{CypherMode, Method, MethodWriteFamily};

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
    owner_domain(method).unwrap_or_else(|| remaining_default_domain(method, surface))
}

/// Methods whose durable effect belongs to a lifecycle, coordinator or native
/// store owner rather than to the target graph's rows or snapshot.
fn owner_domain(method: &Method) -> Option<DurabilityDomain> {
    Some(match method {
        Method::CreateGraph { .. } | Method::DeleteGraph { .. } => DurabilityDomain::Lifecycle,
        Method::MultiGraphBatchUpdate { .. } => DurabilityDomain::MultiGraph,
        Method::Commit { .. } => DurabilityDomain::CrossModal,
        // Wire-unconditional, same reason as the `datascience`/`finance`/etc.
        // notes below: `eg-capabilities` forces `eg-types/blob` on unconditionally
        // for its policy ledger, regardless of whether this root crate's own
        // `blob` feature is enabled -- a `blob`-off `server` build still carries
        // these variants and must still claim them here (EH-319).
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => DurabilityDomain::BlobStore,
        // Wire-unconditional, same reason as `blob` just above: `eg-capabilities`
        // forces `eg-types/kv` on unconditionally too (EH-319).
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
        // RF-ADR-010's two admin decide-layer jobs are classified in
        // `decide_layer_job_domain`, called from this arm's own fallthrough
        // rather than added as a match arm here -- extracted, not inlined, so
        // this match's own arm count (and complexity-staged's regression check
        // on it) is unaffected by their addition. See that function's doc
        // comment for why they share `AnalyticsJob`'s domain.
        _ => return decide_layer_job_domain(method).or_else(|| service_owner_domain(method)),
    })
}

/// `DecisionFit`/`DecisionEval` are wire-unconditional (S1's decide layer
/// carries no feature gate at all), unlike `AnalyticsJob`'s own
/// `#[cfg(feature = "jobs")]` gate in `owner_domain` above -- so they cannot
/// join that arm directly. Both are explicitly documented as sharing its
/// store: `DecisionFit`'s own doc comment says "runtime-conditional like
/// AnalyticsJob: status is a read; submit commits a native MutationBatch in
/// jobs.redb carrying the decision job row and its receipt" (`DecisionEval`'s
/// says the same for the evaluation job row) -- the SAME job-plane redb
/// `AnalyticsJob` owns, so they share its domain rather than falling to the
/// surface-keyed default.
fn decide_layer_job_domain(method: &Method) -> Option<DurabilityDomain> {
    matches!(
        method,
        Method::DecisionFit { .. } | Method::DecisionEval { .. }
    )
    .then_some(DurabilityDomain::AnalyticsJob)
}

/// Control-plane, SQL, RDF and broker owners.
fn service_owner_domain(method: &Method) -> Option<DurabilityDomain> {
    Some(match (method.write_family(), method) {
        (
            Some(
                MethodWriteFamily::WorkItemSubmission
                | MethodWriteFamily::WorkItemLease
                | MethodWriteFamily::WorkItemResource
                | MethodWriteFamily::CapacityLease,
            ),
            _,
        ) => DurabilityDomain::ControlPlane,
        // `Sql` is likewise wire-unconditional (gated only downstream behind
        // `query`); see the `Ts*` note above -- same reason, same fix.
        (_, Method::Sql { .. }) => DurabilityDomain::SqlCatalog,
        // EH-319 CORRECTION: this arm's previous comment claimed `query` is
        // "nothing forcing that feature on unconditionally", but
        // `eg-capabilities`'s `[dependencies.eg-types] features` list DOES
        // include `"query"` (the same list that forces `rdf` on for
        // `AddTriples`/etc. below) -- that was the mistaken assumption, not a
        // real difference from `Sql`. `SqlSourceBatch` is wire-unconditional in
        // any `server` build for the identical reason, so it keeps `Sql`'s
        // unconditional arm rather than a separate feature-gated one.
        (_, Method::SqlSourceBatch { .. }) => DurabilityDomain::SqlCatalog,
        // Wire-unconditional, same reason as the `datascience`/`finance`/etc.
        // notes above: `full` does not list plain `rdf` (only `rdf-xml`/
        // `sparql-*`/etc.), but `eg-capabilities` forces `eg-types/rdf` on
        // regardless.
        (_, Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph) => {
            DurabilityDomain::RdfDataset
        }
        // Wire-unconditional, same reason as `SqlSourceBatch` above:
        // `eg-capabilities` forces `eg-types/broker` on unconditionally, so
        // every Broker-family variant exists in any `server` build regardless
        // of this root crate's own `broker` feature (EH-319; previously a
        // `--no-default-features --features server` build failed to compile
        // here with `non-exhaustive patterns` once the constrained-parallelism
        // harness fix let the slim-server lint step run to completion).
        (Some(MethodWriteFamily::Broker), _) => DurabilityDomain::Broker,
        _ => return None,
    })
}

/// Every remaining `Method` variant `owner_domain` didn't claim
/// (query/analytics/mining/finance/... surfaces, and the plain graph-row CRUD
/// family): none of them is routed to a store-specific domain, so all of them
/// fall through to the surface-keyed default. Naming them here (instead of
/// `_`) keeps that default AND makes the dispatch exhaustive -- a future
/// `Method` variant is a compile error at this match, not a silent default.
fn remaining_default_domain(method: &Method, surface: MutationSurface) -> DurabilityDomain {
    match method {
        // Every other `Method` variant (query/analytics/mining/finance/... surfaces,
        // and the plain graph-row CRUD family) was never routed to a store-specific
        // domain: it always fell through to the surface-keyed default below. Naming
        // them here (instead of `_`) keeps that default AND makes the dispatch
        // exhaustive -- a future `Method` variant is a compile error at this match,
        // not a silent default.
        #[cfg(feature = "asr-whisper")]
        Method::Asr { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/blob` on unconditionally (EH-319).
        Method::BlobFetchEnd { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchBegin { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/broker` on unconditionally (EH-319).
        Method::StreamCommittedOffset { .. } | Method::StreamRead { .. } => {
            default_mutation_domain(surface)
        }
        // `GetMatView`/`CreateMatView`/`DistributedCompute`/`RefreshMatView` are
        // wire-unconditional too, for a reason `cargo tree -e features -p eg-types`
        // makes visible but a Cargo.toml grep does not: this crate's own
        // `eg-capabilities` dependency (linked unconditionally by the `server`
        // feature, which this whole module already requires -- see `src/lib.rs`'s
        // `#[cfg(feature = "server")] pub mod server;`) forces `eg-types/compute-dist`
        // on in its base `[dependencies.eg-types] features` list, deliberately
        // unconditionally: `crates/eg-capabilities/Cargo.toml`'s policy ledger must
        // see every `Method` variant regardless of serving tier, exactly like the
        // `Ts*` note above. That forcing is NOT gated by this root crate's own
        // `compute-dist` feature (which additionally pulls `raft`/openraft for the
        // real cross-shard Pregel engine -- `compute-dist = ["raft",
        // "eg-types/compute-dist"]`), so these variants exist in ANY `server` build,
        // `full` included, even though `full` does not list `compute-dist` and the
        // main/default build must link no openraft (see the root Cargo.toml's
        // `cluster`-layer comment). A `#[cfg(feature = "compute-dist")]` gate here
        // was therefore always false under `--no-default-features --features full`
        // while the variants were still present -- the non-exhaustive-match defect
        // this comment replaces. Gating eg-capabilities' forcing behind its own
        // mirrored `compute-dist` companion feature (the same fix already applied to
        // `jobs`/`statechart`/`quantum-agent-api`/`viz`/`asr-native` there) would let
        // this arm go back to being feature-gated, but that is a contract-regenerating
        // change to a crate this lane does not own the artifacts for; tracked instead
        // of done here (see WRAPUP).
        Method::GetMatView { .. }
        | Method::CreateMatView { .. }
        | Method::DistributedCompute { .. }
        | Method::RefreshMatView { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/cost` on unconditionally (EH-319).
        Method::ResourceStatsPage { .. } => default_mutation_domain(surface),
        // Wire-unconditional: `eg-capabilities` forces `eg-types/datascience` on
        // unconditionally for its policy ledger (its `[dependencies.eg-types]`
        // features list), which this root crate's own `datascience` feature does
        // not gate -- `full` does not list `datascience` at all. Same bug class as
        // the `compute-dist`/`Quantum` notes elsewhere in this file.
        Method::DsFitEstimator { .. } | Method::DsPredictEstimator { .. } => {
            default_mutation_domain(surface)
        }
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/epistemic` on unconditionally (EH-319).
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
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/federation` on unconditionally (EH-319).
        Method::RegisterForeignSource { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `full` does not
        // list `finance`, but `eg-capabilities` forces `eg-types/finance` on
        // regardless.
        Method::FinanceMatchOrders { .. } | Method::FinanceForensicReport { .. } => {
            default_mutation_domain(surface)
        }
        // Wire-unconditional, same reason as `datascience` above: `full` does not
        // list `graphlearn`, but `eg-capabilities` forces `eg-types/graphlearn` on
        // regardless.
        Method::GraphLearnPredict { .. } | Method::GraphLearnFit { .. } => {
            default_mutation_domain(surface)
        }
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/graphql` on unconditionally (EH-319).
        Method::GraphQl { .. } => default_mutation_domain(surface),
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/kv` on unconditionally (EH-319).
        Method::KvScan { .. } | Method::KvGet { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `GetMatView`/etc. above and
        // `datascience` below: `eg-capabilities` forces `eg-types/matview` on
        // unconditionally too (EH-319).
        Method::PlanMatViewRefresh { .. }
        | Method::PlanMatViewGet { .. }
        | Method::PlanMatViewDefine { .. }
        | Method::PlanMatViewDrop { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `full` does not
        // list `mining`, but `eg-capabilities` forces `eg-types/mining` on
        // regardless.
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
        // Wire-unconditional, same reason as `datascience` above: `full` does not
        // list `ml-pipeline`, but `eg-capabilities` forces `eg-types/ml-pipeline`
        // on regardless.
        Method::MiningPipelineTrain { .. }
        | Method::MiningPipelineServe { .. }
        | Method::MiningPipelinePredict { .. }
        | Method::MiningPipelineEvaluate { .. }
        | Method::MiningPipelineCompare { .. } => default_mutation_domain(surface),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/obda` on unconditionally (EH-319).
        Method::SparqlVirtual { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/owl` on unconditionally (EH-319).
        Method::OwlReasonDistributed { .. }
        | Method::OwlExplain { .. }
        | Method::OwlReason { .. }
        | Method::TxnAxiom { .. } => default_mutation_domain(surface),
        // Same bug class as the `Asr` note above (see git history): this root
        // crate's bare `quantum` feature only pulls `dep:eg-quantum-core` (the IR +
        // planner, `quantum = ["dep:eg-quantum-core"]`) and never forwards
        // `eg-types/quantum`. Only `quantum-agent-api` does
        // (`["quantum-sim", "server", "eg-types/quantum", "eg-capabilities/quantum"]`,
        // root Cargo.toml) -- it is the ONE feature that makes `Method::Quantum`
        // reachable at all. Gating on bare `quantum` was always wrong: it let this
        // arm compile under `--features quantum,server` even though `Method::Quantum`
        // does not exist there (eg-capabilities deliberately excludes `quantum` from
        // its unconditional eg-types forcing, exactly like `jobs`/`statechart`, so
        // nothing else turns the variant on) -- a real non-exhaustive-match defect
        // that `--all-features`/`full` both mask because they also enable
        // `quantum-agent-api`.
        #[cfg(feature = "quantum-agent-api")]
        Method::Quantum { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `Sql`/`SqlSourceBatch` above:
        // `eg-capabilities` forces `eg-types/query` on unconditionally (EH-319).
        Method::UnifiedQueryText { .. }
        | Method::TxnUnifiedQueryText { .. }
        | Method::TxnPlanWriteback { .. }
        | Method::TxnUnifiedQuery { .. }
        | Method::UnifiedQuery { .. }
        | Method::ExplainProvenanceByIds { .. }
        | Method::ExplainProvenance { .. }
        | Method::ExplainPlan { .. }
        | Method::ExplainPolicy { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `full` does not
        // list plain `rdf` (only `rdf-xml`/`sparql-*`/etc.), but `eg-capabilities`
        // forces `eg-types/rdf` on regardless.
        Method::RunRules { .. } | Method::GetRdf => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/security` on unconditionally (EH-319).
        Method::AuditProveInclusion { .. } | Method::AuditVerify => {
            default_mutation_domain(surface)
        }
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/sparql` on unconditionally (EH-319).
        Method::Sparql { .. } | Method::TxnConstruct { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/sqlite-file` on unconditionally (EH-319).
        Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. } => {
            default_mutation_domain(surface)
        }
        #[cfg(feature = "statechart")]
        Method::Statechart { .. } => default_mutation_domain(surface),
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/streaming` on unconditionally (EH-319).
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
        // Wire-unconditional, same reason as `datascience` above: `eg-capabilities`
        // forces `eg-types/wasm-udf` on unconditionally (EH-319).
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
        | Method::ListRegisteredServers { .. }
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
        // RF-ADR-010's Decide layer (S1): all `Stability::Internal`, `NO_CONSUMER`,
        // refusal-only dispatch arms in this wave -- none of them serves a write
        // yet, so none has an authority to route to other than the surface-keyed
        // default (the same treatment `AgentComponent`/`AgentLibrary`/
        // `AgentGraph`/`AgentTemplate` above already get, despite each of THEM
        // being a native write too: this classifier's `ControlPlane` domain is
        // reserved for the WorkItem/capacity-lease control-plane redb, a
        // DIFFERENT physical store than agent_library.redb, so a native write
        // there does not by itself imply `ControlPlane` here -- see the four
        // Agent* arms' own precedent). Examined individually, not defaulted
        // reflexively:
        //  * `AgentAssemble`/`Decide`/`GraphSchemaList` read only, commit nothing.
        //  * `Solve` is pure compute: no store, no clock, no float.
        //  * `DecisionCommit`/`ConnectorPack` DO write natively (agent_library.redb,
        //    one WTX per their own doc comments) -- same store family, same
        //    treatment as the four Agent* arms just above.
        //  * `GraphSchema` is "gateway-routed exactly like `IcvConfigure`" per its
        //    own doc comment -- a graph-row/snapshot write through the graph
        //    commit kernel, i.e. exactly what this default already means; see
        //    `IcvConfigure` a few arms below.
        //  * `MutationOutbox`'s `rewind` writes through bounded `eg-transaction`
        //    transactions (a saga), not a graph or native `MutationBatch` WTX, and
        //    this classifier has no dedicated outbox domain to route it to. The
        //    package that lands `rewind`'s real dispatch arm must revisit this.
        //  * `DecisionFit`/`DecisionEval` are NOT here: they write jobs.redb like
        //    `AnalyticsJob` and are classified in `owner_domain` alongside it.
        | Method::AgentAssemble { .. }
        | Method::Decide { .. }
        | Method::DecisionCommit { .. }
        | Method::Solve { .. }
        | Method::ConnectorPack { .. }
        | Method::SourceIngest { .. }
        | Method::SourceIngestStatus { .. }
        | Method::WriteBack { .. }
        | Method::GraphSchema { .. }
        | Method::GraphSchemaList
        // EH-219 / graph-os EG-2/3 typed reads: MVCC snapshot reads that commit
        // nothing, so like every other read they carry the surface-keyed default.
        | Method::GetWorkItem { .. }
        | Method::ListWorkItems { .. }
        | Method::GetWorkItemOutcome { .. }
        | Method::GetControlLease { .. }
        | Method::MutationOutbox { .. } => default_mutation_domain(surface),
        // `DecisionFit`/`DecisionEval` are NOT surface-keyed: both commit a native
        // MutationBatch in jobs.redb -- the same job-plane store `AnalyticsJob`
        // owns -- so they carry its domain. They cannot join that arm directly
        // because `AnalyticsJob` is `#[cfg(feature = "jobs")]` gated while S1's
        // decide layer is wire-unconditional, so the single authority for their
        Method::FinanceSabrImpliedVol { .. }
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
        // `owner_domain` (directly, or via its `service_owner_domain` delegate)
        // already classifies every variant below -- `domain_for`'s
        // `owner_domain(method).unwrap_or_else(...)` never actually reaches this
        // arm for them. They still need a home HERE: this match carries no
        // wildcard (see the module doc comment above `remaining_default_domain`),
        // so it must independently be exhaustive over every `Method` variant --
        // Rust checks each `match` against its own patterns, not against what a
        // caller's control flow already ruled out. Delegating back to
        // `owner_domain` (which itself falls through to `service_owner_domain`
        // for anything it doesn't list directly) keeps this arm correct by
        // construction -- one source of truth -- instead of hand-duplicating a
        // domain value that could silently drift out of sync with it.
        Method::CreateGraph { .. }
        | Method::DeleteGraph { .. }
        | Method::MultiGraphBatchUpdate { .. }
        | Method::Commit { .. }
        | Method::TsAppend { .. }
        | Method::TsEvict { .. }
        | Method::TsDeleteSeries { .. }
        | Method::KgDelegate { .. }
        | Method::SubmitWorkItem { .. }
        | Method::SubmitWorkItems { .. }
        | Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. }
        | Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::CasWorkItemMetadata { .. }
        | Method::IssueControlLease { .. }
        | Method::TransitionControlLease { .. }
        | Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. }
        | Method::UpdateResourceHost { .. }
        | Method::Sql { .. }
        | Method::AddTriples { .. }
        | Method::RemoveTriples { .. }
        | Method::DropNamedGraph => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        // Wire-unconditional (`eg-capabilities` forces `eg-types/blob` on
        // unconditionally), same reason as `Sql`/`AddTriples` above -- claimed
        // by `owner_domain`'s own now-unconditional arm (EH-319).
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        // Wire-unconditional (`eg-capabilities` forces `eg-types/kv` on
        // unconditionally), same reason as `blob` above (EH-319).
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. } => owner_domain(
            method,
        )
        .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { .. } => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        // Claimed by owner_domain's decide_layer_job_domain delegate, same
        // reasoning as AnalyticsJob just above (and wire-unconditional, so no
        // cfg gate here either).
        Method::DecisionFit { .. } | Method::DecisionEval { .. } => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        // Wire-unconditional (`eg-capabilities` forces `eg-types/query` on
        // unconditionally); claimed by `service_owner_domain`'s now-unconditional
        // `SqlSourceBatch` arm (EH-319 -- see that arm's comment for why the
        // previous "nothing forces `query` on" premise was wrong).
        Method::SqlSourceBatch { .. } => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
        // Wire-unconditional (`eg-capabilities` forces `eg-types/broker` on
        // unconditionally); claimed by `service_owner_domain`'s now-unconditional
        // `MethodWriteFamily::Broker` arm (EH-319: this was the exact
        // non-exhaustive-match compile failure under
        // `--no-default-features --features server`).
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
        | Method::BrokerRenewTag { .. } => owner_domain(method)
            .unwrap_or_else(|| unreachable!("{method:?} is claimed by owner_domain")),
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
        // Wire-unconditional, same reason as the `datascience`/`finance`/etc.
        // notes above: `full` does not list plain `rdf` (only `rdf-xml`/
        // `sparql-*`/etc.), but `eg-capabilities` forces `eg-types/rdf` on
        // regardless.
        Method::DropNamedGraph => Method::ClearGraph,
        other => other,
    }
}

/// Query/RDF/lifecycle adapters are classified here, not in persistence.  Their
/// operations still use exactly the same Method payload and commit machinery.
pub(super) fn surface_for(method: &Method) -> Option<MutationSurface> {
    match (method.write_family(), method) {
        (
            _,
            Method::Sql { .. }
            | Method::SqlSourceBatch { .. }
            | Method::CypherQuery {
                mode: CypherMode::Write,
                ..
            },
        ) => Some(MutationSurface::Query),
        // Wire-unconditional, same reason as `rdf` below: `eg-capabilities`
        // forces `eg-types/graphql` on unconditionally (EH-319).
        (_, Method::GraphQl { .. }) => Some(MutationSurface::Query),
        // Wire-unconditional, same reason as the `datascience`/`finance`/etc.
        // notes above: `full` does not list plain `rdf` (only `rdf-xml`/
        // `sparql-*`/etc.), but `eg-capabilities` forces `eg-types/rdf` on
        // regardless.
        (_, Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph) => {
            Some(MutationSurface::Rdf)
        }
        (_, Method::CreateGraph { .. } | Method::DeleteGraph { .. }) => {
            Some(MutationSurface::Lifecycle)
        }
        (
            Some(
                MethodWriteFamily::WorkItemSubmission
                | MethodWriteFamily::WorkItemResource
                | MethodWriteFamily::CapacityLease,
            ),
            _,
        ) => Some(MutationSurface::Job),
        #[cfg(feature = "jobs")]
        (_, Method::AnalyticsJob { .. }) => Some(MutationSurface::Job),
        (_, Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. }) => {
            Some(MutationSurface::Query)
        }
        // Wire-unconditional, same reason as `graphql` above: `eg-capabilities`
        // forces `eg-types/broker` on unconditionally (EH-319).
        (Some(MethodWriteFamily::Broker), _) => Some(MutationSurface::Broker),
        _ => None,
    }
}

pub(crate) fn is_work_item_method(method: &Method) -> bool {
    matches!(
        method.write_family(),
        Some(MethodWriteFamily::WorkItemSubmission | MethodWriteFamily::WorkItemLease)
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
