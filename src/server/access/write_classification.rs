//! Graph write-access classification: whether a graph-targeted method needs
//! Write or only Read access. Split by surface so each classifier stays small.

#[cfg(feature = "graphql")]
use super::graphql_is_mutation;
#[cfg(feature = "query")]
use super::sql_is_write;
#[cfg(feature = "cypher")]
use crate::protocol::CypherMode;
use crate::protocol::Method;

/// The operation-conditional agent-layer and semantic-index surfaces.
fn requires_write_agent_surface(method: &Method) -> Option<bool> {
    // Agent Library is a runtime-conditional native ControlPlane surface:
    // publish/retire commit durable owner rows, while current/history/status
    // are authenticated tenant-bound snapshots and must remain reads.
    if let Method::AgentLibrary { op } = method {
        return Some(matches!(
            op,
            eg_types::AgentLibraryOp::Publish { .. } | eg_types::AgentLibraryOp::Retire { .. }
        ));
    }
    // Agent graphs are the same runtime-conditional shape. `is_mutation` lives
    // on the op itself so this classifier and the capability policy cannot
    // drift apart about which operations write.
    if let Method::AgentGraph { op } = method {
        return Some(op.is_mutation());
    }
    if let Method::AgentComponent { op } = method {
        return Some(op.is_mutation());
    }
    // Agent templates, likewise. `Instantiate` is a READ here because it is
    // one: it binds parameters and hands back a draft. The `AgentLibrary`
    // publish that stores the resulting instance is a separate method and
    // carries the write on its own.
    if let Method::AgentTemplate { op } = method {
        return Some(op.is_mutation());
    }
    // The semantic index's S1-S6 queue is the same runtime-conditional shape,
    // and the same rule applies: `is_mutation` lives on the op, so this
    // classifier and `eg_capabilities::semantic_index_policy` cannot disagree.
    // Three of its reads-by-name are writes -- subscribing a consumer, claiming
    // leases, and replaying an already-committed S1 all advance durable rows --
    // and the op is the one place that fact is recorded.
    if let Method::SemanticIndex { op } = method {
        return Some(op.is_mutation());
    }
    None
}

fn requires_write_native_surface(method: &Method) -> Option<bool> {
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        return Some(op.mutates());
    }
    // `AddTriples` / `RemoveTriples` / `DropNamedGraph` (feature `rdf`) mutate the
    // target graph's RDF content (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / EG-017).
    #[cfg(feature = "rdf")]
    if matches!(
        method,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return Some(true);
    }
    // Key→Value mutations (CONCEPT:EG-KG.storage.namespaced-kv-surface, feature `kv`). KV is namespace-scoped (NOT
    // graph-scoped) and self-routes BEFORE `dispatch_graph_op`, so this classifier is
    // not on the KV routing path — but it is the canonical read/write classifier, so
    // `KvPut`/`KvDelete`/`KvCas` are recorded here as writes (`KvGet`/`KvScan` read).
    #[cfg(feature = "kv")]
    if matches!(
        method,
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. }
    ) {
        return Some(true);
    }
    #[cfg(feature = "sqlite-file")]
    if matches!(method, Method::ImportSqliteFile { .. }) {
        return Some(true);
    }
    #[cfg(feature = "query")]
    if matches!(method, Method::SqlSourceBatch { .. }) {
        return Some(true);
    }
    // Message-broker admin + publish (CONCEPT:EG-KG.compute.message-broker-exchanges, feature `broker`) all mutate the
    // control graph's exchange/binding/message nodes, so they classify as writes (Write
    // access + WAL record). Consume/ack ride `ClaimNext`/`CompareAndSetNodeFields`,
    // already classified below.
    //
    // L10 (EG-P0-6 security finding): the stream + tag-addressed publisher-confirm/
    // consumer-ack family mutates the SAME Outbox control-graph state as the ops
    // above, so the ACL-write and durability classifications agree exactly.
    #[cfg(feature = "broker")]
    if matches!(
        method,
        Method::DeclareExchange { .. }
            | Method::DeleteExchange { .. }
            | Method::BindQueue { .. }
            | Method::UnbindQueue { .. }
            | Method::Publish { .. }
            // Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280) all mutate control-graph
            // nodes (policy/message/dead-letter/claim state) → writes.
            | Method::DeclareQueue { .. }
            | Method::PublishEx { .. }
            | Method::BrokerConsume { .. }
            | Method::BrokerAck { .. }
            | Method::BrokerReject { .. }
            | Method::SweepExpired { .. }
            // L10: streams (CONCEPT:EG-KG.compute.replayable-append-log).
            | Method::StreamDeclare { .. }
            | Method::StreamPublish { .. }
            | Method::StreamTrim { .. }
            | Method::StreamCommitOffset { .. }
            // L10: publisher-confirm / tag-addressed consumer ack/nack (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos).
            | Method::PublishConfirmed { .. }
            | Method::PublishIdempotent { .. }
            | Method::BrokerAckTag { .. }
            | Method::BrokerNackTag { .. }
            | Method::BrokerRenewTag { .. }
    ) {
        return Some(true);
    }
    None
}

fn requires_write_query_surface(method: &Method) -> Option<bool> {
    // Query-surface writes (CONCEPT:EG-KG.query.mirrors-pgwire): the `Sql`/`CypherQuery`/`GraphQl` variants
    // carry a query STRING, so whether they mutate the graph depends on the statement,
    // not the variant. Parse just enough to classify; a write needs Write access and
    // (post-success) the dispatch shell's `mark_dirty` so the next checkpoint persists
    // it. A read (or an unparseable statement — the handler surfaces the parse error)
    // stays Read. Each detector is feature-gated to its surface.
    #[cfg(feature = "query")]
    if let Method::Sql { query, .. } = method {
        return Some(sql_is_write(query));
    }
    #[cfg(feature = "cypher")]
    if let Method::CypherQuery { query, mode } = method {
        return Some(match eg_query::classify_cypher(query) {
            Ok(eg_query::CypherStatementKind::Read) => false,
            Ok(eg_query::CypherStatementKind::Write) => true,
            // The dispatcher rejects parser errors and mode mismatches before
            // authorization. Until then, preserve the caller's more restrictive
            // declaration rather than accidentally admitting a declared write to
            // the reserved read lane.
            Err(_) => matches!(mode, CypherMode::Write),
        });
    }
    #[cfg(feature = "graphql")]
    if let Method::GraphQl { query, .. } = method {
        return Some(graphql_is_mutation(query));
    }
    None
}

fn requires_write_mining_surface(method: &Method) -> Option<bool> {
    // Data mining (CONCEPT:EG-KG.mining.frequent-itemset-mining / dbscan-density /
    // isolation-forest): only a write when it writes back the mined
    // `:AssociationRule` / `:Cluster` / `:Anomaly` nodes; a pure query
    // (writeback=false) reads its rows off an off-lock snapshot.
    #[cfg(feature = "mining")]
    if let Method::MineAssociate { writeback, .. }
    | Method::MineCluster { writeback, .. }
    | Method::MineAnomaly { writeback, .. }
    | Method::MineClassifyPredict { writeback, .. }
    | Method::MineReduce { writeback, .. }
    | Method::MineSequence { writeback, .. }
    | Method::MineForecast { writeback, .. } = method
    {
        return Some(*writeback);
    }
    // MineText: writeback only mutates for lda/nmf (their :Topic nodes) — tfidf
    // is always read-only regardless of the flag (the handler ignores it too).
    #[cfg(feature = "mining")]
    if let Method::MineText {
        writeback,
        algorithm,
        ..
    } = method
    {
        return Some(*writeback && !matches!(algorithm, crate::protocol::TextAlgorithm::Tfidf));
    }
    // MineSubgraph: writeback only mutates for gspan (its :FrequentSubgraph
    // nodes) — motif is always read-only (a pure census, no patterns to write).
    #[cfg(feature = "mining")]
    if let Method::MineSubgraph {
        writeback,
        algorithm,
        ..
    } = method
    {
        return Some(*writeback && !matches!(algorithm, crate::protocol::SubgraphAlgorithm::Motif));
    }
    // Classification FIT is read-only (returns a model blob; no graph mutation).
    #[cfg(feature = "mining")]
    if matches!(method, Method::MineClassifyFit { .. }) {
        return Some(false);
    }
    // Residual insight/mining families (CONCEPT:EG-KG.mining.entity-resolution /
    // causal-impact / process-mining / root-cause / risk-propagation /
    // ontology-gap / retrieval-quality / community-writeback): only a write when
    // it writes back the family's typed nodes; a pure query (writeback=false)
    // reads its rows off an off-lock snapshot, mirroring the mining family above.
    #[cfg(feature = "mining")]
    if let Method::MineEntityResolve { writeback, .. }
    | Method::MineCausalImpact { writeback, .. }
    | Method::MineProcess { writeback, .. }
    | Method::MineRootCause { writeback, .. }
    | Method::MineRiskPropagation { writeback, .. }
    | Method::MineOntologyGap { writeback, .. }
    | Method::MineRetrievalQuality { writeback, .. }
    | Method::MineCommunity { writeback, .. } = method
    {
        return Some(*writeback);
    }
    None
}

fn requires_write_learning_surface(method: &Method) -> Option<bool> {
    // Graph learning (CONCEPT:EG-KG.graphlearn.link-predictor): only a write when it
    // writes back the `:EdgeFunction` / `:PredictedEdge` nodes; a pure fit/predict
    // (writeback=false) reads its rows off an off-lock snapshot.
    #[cfg(feature = "graphlearn")]
    if let Method::GraphLearnFit { writeback, .. } | Method::GraphLearnPredict { writeback, .. } =
        method
    {
        return Some(*writeback);
    }
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): Train/Predict mutate only when
    // their `writeback` writes back the `:Model` / `:Prediction` nodes; Evaluate and
    // Compare are pure reads (they fall through to `false`).
    #[cfg(feature = "ml-pipeline")]
    if let Method::MiningPipelineTrain { writeback, .. }
    | Method::MiningPipelinePredict { writeback, .. } = method
    {
        return Some(*writeback);
    }
    // Serve ALWAYS writes the `:ServedModel` pointer (deploy the version).
    #[cfg(feature = "ml-pipeline")]
    if matches!(method, Method::MiningPipelineServe { .. }) {
        return Some(true);
    }
    None
}

/// Whether a graph-targeted method mutates the target graph (Write) or only
/// reads from it (Read). Native ControlPlane surfaces such as Agent Library
/// can be operation-conditional: its publish/retire writes are distinct from
/// the current/history/status read sub-operations. Pure-compute methods
/// (finance, datascience, parse) never touch graph state and classify as Read.
pub(crate) fn requires_write(method: &Method) -> bool {
    if let Some(result) = requires_write_agent_surface(method) {
        return result;
    }
    if let Some(result) = requires_write_native_surface(method) {
        return result;
    }
    if let Some(result) = requires_write_query_surface(method) {
        return result;
    }
    if let Some(result) = requires_write_mining_surface(method) {
        return result;
    }
    if let Some(result) = requires_write_learning_surface(method) {
        return result;
    }
    matches!(
        method,
        Method::BeginTxn { .. }
            | Method::Rollback { .. }
            | Method::AddNode { .. }
            | Method::CreateNodeIfAbsent { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::InvalidateEdge { .. }
            | Method::SupersedeEdge { .. }
            | Method::ClearGraph
            | Method::AddEmbedding { .. }
            | Method::PruneByLifecycle { .. }
            | Method::BatchUpdate { .. }
            | Method::EvictLRU { .. }
            | Method::DecaySweep { .. }
            | Method::TouchNodes { .. }
            | Method::FromMsgpack { .. }
            | Method::ClearLedger
            | Method::ApplyLedger { .. }
            | Method::CompactNodesByType { .. }
            | Method::RunDatalogReasoning { .. }
            | Method::ApplyChangeEnvelope { .. }
            | Method::ApplyChangeEnvelopes { .. }
            | Method::Reconcile { .. }
            | Method::ApplyMutation { .. }
            | Method::ApplyMultisigMutation { .. }
            // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): configuring the ICV
            // shapes for a graph is a security-relevant operation. Its
            // `security:admin` capability is enforced before this graph Write check;
            // the graph ACL then binds that admin operation to its authorized route.
            | Method::IcvConfigure { .. }
            | Method::DeleteGraph { .. }
            | Method::ClaimNext { .. }
            | Method::ClaimWorkItem { .. }
            | Method::SubmitWorkItem { .. }
            | Method::KgDelegate { .. }
            | Method::SubmitWorkItems { .. }
            | Method::AcquireCapacity { .. }
            | Method::RenewCapacity { .. }
            | Method::ReleaseCapacity { .. }
            | Method::ReclaimExpiredCapacity { .. }
            | Method::UpdateCapacityCell { .. }
            | Method::MintWorkItemClaimCapability { .. }
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
            | Method::CasWorkItemMetadata { .. }
            | Method::ReserveWorkItemResources { .. }
            | Method::ReleaseWorkItemResources { .. }
            | Method::ReclaimWorkItemResources { .. }
            | Method::UpdateResourceHost { .. }
            // Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-KG.memory.eg-batch-decay-caller):
            // each writes nodes/edges (summaries, semantic nodes, decay/evict
            // bookkeeping, scene objects, trajectories/steps) → Write access + WAL
            // record. The paired reads (SummaryChildren/SummariesAtLevel/
            // WorldTransform/SceneChildren/DiscountedReturn/BestTrajectory) stay Read.
            | Method::CreateSummaryNode { .. }
            | Method::Consolidate { .. }
            | Method::Reinforce { .. }
            | Method::DecayNode { .. }
            | Method::DecayMemories { .. }
            | Method::EvictBelow { .. }
            | Method::Maintain { .. }
            | Method::AddSceneObject { .. }
            | Method::SetPose { .. }
            | Method::Reparent { .. }
            | Method::StartTrajectory { .. }
            | Method::AppendStep { .. }
    )
}
