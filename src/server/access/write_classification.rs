//! Graph write-access classification: whether a graph-targeted method needs
//! Write or only Read access. Split by surface so each classifier stays small.
//!
//! # EH-316/EH-319: wire-unconditional `Method` variants vs. this crate's own facade features
//!
//! `eg-capabilities` (linked whenever this root crate's `server` feature is on
//! -- see its own Cargo.toml doc comment) forces most `eg-types` feature flags
//! on UNCONDITIONALLY for its exhaustive policy ledger, including `query`,
//! `graphql`, `broker`, `blob`, `kv`, `sqlite-file`, `mining`, `graphlearn` and
//! `ml-pipeline`. That means the `Method` variants those `eg-types` features
//! gate (`Sql`'s siblings `SqlSourceBatch`/`GraphQl`, the whole Broker family,
//! `KvPut`/`KvDelete`/`KvCas`, `ImportSqliteFile`, every `Mine*`/`GraphLearn*`/
//! `MiningPipeline*` variant, ...) exist in ANY `server` build -- including
//! `--no-default-features --features server` -- regardless of whether THIS
//! root crate's own same-named facade feature (which additionally gates the
//! real handler/dependency, e.g. `dep:eg-query`) is enabled.
//!
//! Gating a classification arm below behind this crate's own facade feature
//! was therefore wrong for every one of those methods: under a slim build the
//! arm silently disappeared and the method fell through to the default `Read`
//! classification, independent of what it actually does. `requires_write`
//! feeds the QoS admission pool AND the graph Read/Write ACL gate
//! (`graph_op_access_level`), both BEFORE any handler gets a chance to refuse
//! an unbuilt method as "not built" -- so a wrong `Read` here is a real
//! authorization mis-classification, not just a cosmetic one, even though the
//! not-built handler happens to make it unreachable today. The fix mirrors the
//! established `canonical.rs` "wire-unconditional" pattern: classify by the
//! `Method` variant/field, not by this crate's facade feature, wherever the
//! classification itself needs no dependency the facade feature would pull in.
//!
//! The three query-LANGUAGE surfaces (`Sql`/`CypherQuery`/`GraphQl`) are
//! different: classifying them by parsing the query text genuinely needs the
//! optional parser crate (`eg-query`/`eg-graphql`), which only links when this
//! crate's OWN `query`/`cypher`/`graphql` feature is on. When that parser is
//! not compiled in, this module fails CLOSED to `true` (Write) rather than
//! guessing `false` (Read): the caller still cannot mutate anything (the
//! handler refuses the method as not-built either way), but the ACL/QoS
//! decision is never silently wrong about what kind of request this is.
//!
//! `modality-serving` is NOT in `eg-capabilities`'s forced list (it has its
//! own mirrored lockstep feature there instead, like `jobs`/`statechart`), so
//! `Method::ServedModality` genuinely does not exist without this crate's own
//! `modality-serving` feature; that one arm keeps its cfg gate.
use crate::protocol::{CypherMode, Method, MethodWriteFamily};

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

/// The decision and catalog-administration surfaces, whose read/write split
/// lives on their own ops.
///
/// Four methods, one classifier: each delegates to `is_mutation()` on the op,
/// so this file and the capability ledger cannot drift apart about an
/// operation. `ConnectorPack.status`, `DecisionFit.status`,
/// `DecisionEval.status`, `MutationOutbox.status` and
/// `MutationOutbox.dead_letters` are genuinely reads; everything else writes.
fn requires_write_decision_surface(method: &Method) -> Option<bool> {
    match method {
        Method::ConnectorPack { op } => Some(op.is_mutation()),
        Method::DecisionFit { op } => Some(op.is_mutation()),
        Method::DecisionEval { op } => Some(op.is_mutation()),
        Method::MutationOutbox { op } => Some(op.is_mutation()),
        _ => None,
    }
}

fn requires_write_native_surface(method: &Method) -> Option<bool> {
    // `modality-serving` genuinely does not exist without this crate's own
    // feature (see the module doc); this is the one arm here that keeps its
    // cfg gate.
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        return Some(op.mutates());
    }
    // `AddTriples` / `RemoveTriples` / `DropNamedGraph` mutate the target
    // graph's RDF content (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / EG-017).
    // Wire-unconditional (see module doc): no cfg gate.
    if matches!(
        method,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return Some(true);
    }
    // Key→Value mutations (CONCEPT:EG-KG.storage.namespaced-kv-surface). KV is namespace-scoped (NOT
    // graph-scoped) and self-routes BEFORE `dispatch_graph_op`, so this classifier is
    // not on the KV routing path — but it is the canonical read/write classifier, so
    // `KvPut`/`KvDelete`/`KvCas` are recorded here as writes (`KvGet`/`KvScan` read).
    // Wire-unconditional (see module doc): no cfg gate.
    if matches!(
        method,
        Method::KvPut { .. } | Method::KvDelete { .. } | Method::KvCas { .. }
    ) {
        return Some(true);
    }
    // Wire-unconditional (see module doc): no cfg gate.
    if matches!(method, Method::ImportSqliteFile { .. }) {
        return Some(true);
    }
    // Wire-unconditional (see module doc): no cfg gate.
    if matches!(method, Method::SqlSourceBatch { .. }) {
        return Some(true);
    }
    // Message-broker admin + publish (CONCEPT:EG-KG.compute.message-broker-exchanges) all mutate the
    // control graph's exchange/binding/message nodes, so they classify as writes (Write
    // access + WAL record). Consume/ack ride `ClaimNext`/`CompareAndSetNodeFields`,
    // already classified below.
    //
    // Broker policy extensions (CONCEPT:EG-KG.compute.dead-letter-queues..280) mutate
    // policy/message/dead-letter/claim state. L10 (EG-P0-6 security finding): the
    // streams (CONCEPT:EG-KG.compute.replayable-append-log) and tag-addressed
    // publisher-confirm/consumer-ack family
    // (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos) mutate the SAME Outbox
    // control-graph state as the ops above, so the ACL-write and durability
    // classifications agree exactly. Wire-unconditional (see module doc): no cfg
    // gate (EH-316/EH-319: previously gated behind this crate's own `broker`
    // feature, which silently misclassified the whole Broker family as Read
    // under a `broker`-off `server` build even though the variants exist).
    if matches!(method.write_family(), Some(MethodWriteFamily::Broker)) {
        return Some(true);
    }
    None
}

fn requires_write_query_surface(method: &Method) -> Option<bool> {
    // Query-surface writes (CONCEPT:EG-KG.query.mirrors-pgwire): the `Sql`/`CypherQuery`/`GraphQl` variants
    // are wire-unconditional (see module doc) and carry a query STRING, so
    // whether they mutate the graph depends on the statement, not the
    // variant. Parse just enough to classify when this crate's own parser
    // feature is compiled in; a write needs Write access and (post-success)
    // the dispatch shell's `mark_dirty` so the next checkpoint persists it. A
    // read (or an unparseable statement — the handler surfaces the parse
    // error) stays Read. When the parser feature is NOT compiled in, classify
    // as a write (fail closed): the method variant still exists and still
    // reaches this classifier before the handler's not-built refusal, so
    // guessing Read here would be an ACL mis-classification, not merely an
    // unreachable branch.
    if let Method::Sql { query, .. } = method {
        return Some(classify_sql_write(query));
    }
    if let Method::CypherQuery { query, mode } = method {
        return Some(classify_cypher_write(query, *mode));
    }
    if let Method::GraphQl { query, .. } = method {
        return Some(classify_graphql_write(query));
    }
    None
}

#[cfg(feature = "query")]
fn classify_sql_write(query: &str) -> bool {
    super::sql_is_write(query)
}

#[cfg(not(feature = "query"))]
fn classify_sql_write(_query: &str) -> bool {
    true
}

#[cfg(feature = "cypher")]
fn classify_cypher_write(query: &str, mode: CypherMode) -> bool {
    match eg_query::classify_cypher(query) {
        Ok(eg_query::CypherStatementKind::Read) => false,
        Ok(eg_query::CypherStatementKind::Write) => true,
        // The dispatcher rejects parser errors and mode mismatches before
        // authorization. Until then, preserve the caller's more restrictive
        // declaration rather than accidentally admitting a declared write to
        // the reserved read lane.
        Err(_) => matches!(mode, CypherMode::Write),
    }
}

#[cfg(not(feature = "cypher"))]
fn classify_cypher_write(_query: &str, mode: CypherMode) -> bool {
    // No parser compiled in: preserve the caller's own declared mode (the
    // same fail-closed behavior as the parse-error arm above) rather than
    // guessing Read.
    matches!(mode, CypherMode::Write)
}

#[cfg(feature = "graphql")]
fn classify_graphql_write(query: &str) -> bool {
    super::graphql_is_mutation(query)
}

#[cfg(not(feature = "graphql"))]
fn classify_graphql_write(_query: &str) -> bool {
    true
}

fn requires_write_mining_surface(method: &Method) -> Option<bool> {
    // Data mining (CONCEPT:EG-KG.mining.frequent-itemset-mining / dbscan-density /
    // isolation-forest): only a write when it writes back the mined
    // `:AssociationRule` / `:Cluster` / `:Anomaly` nodes; a pure query
    // (writeback=false) reads its rows off an off-lock snapshot.
    // Wire-unconditional (see module doc): `eg-capabilities` forces
    // `eg-types/mining` on unconditionally, so no cfg gate here.
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
    if let Method::MineSubgraph {
        writeback,
        algorithm,
        ..
    } = method
    {
        return Some(*writeback && !matches!(algorithm, crate::protocol::SubgraphAlgorithm::Motif));
    }
    // Classification FIT is read-only (returns a model blob; no graph mutation).
    if matches!(method, Method::MineClassifyFit { .. }) {
        return Some(false);
    }
    // Residual insight/mining families (CONCEPT:EG-KG.mining.entity-resolution /
    // causal-impact / process-mining / root-cause / risk-propagation /
    // ontology-gap / retrieval-quality / community-writeback): only a write when
    // it writes back the family's typed nodes; a pure query (writeback=false)
    // reads its rows off an off-lock snapshot, mirroring the mining family above.
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
    // Wire-unconditional (see module doc): `eg-capabilities` forces
    // `eg-types/graphlearn` on unconditionally, so no cfg gate here.
    if let Method::GraphLearnFit { writeback, .. } | Method::GraphLearnPredict { writeback, .. } =
        method
    {
        return Some(*writeback);
    }
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): Train/Predict mutate only when
    // their `writeback` writes back the `:Model` / `:Prediction` nodes; Evaluate and
    // Compare are pure reads (they fall through to `false`).
    // Wire-unconditional (see module doc): `eg-capabilities` forces
    // `eg-types/ml-pipeline` on unconditionally, so no cfg gate here.
    if let Method::MiningPipelineTrain { writeback, .. }
    | Method::MiningPipelinePredict { writeback, .. } = method
    {
        return Some(*writeback);
    }
    // Serve ALWAYS writes the `:ServedModel` pointer (deploy the version).
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
    /// The runtime-conditional classifiers, in resolution order. A table rather
    /// than a chain of `if let`s so a sixth surface is one entry rather than
    /// one more branch in this function.
    const CONDITIONAL: &[fn(&Method) -> Option<bool>] = &[
        requires_write_agent_surface,
        requires_write_decision_surface,
        requires_write_native_surface,
        requires_write_query_surface,
        requires_write_mining_surface,
        requires_write_learning_surface,
    ];
    if let Some(result) = CONDITIONAL.iter().find_map(|classify| classify(method)) {
        return result;
    }
    // Agent-memory / scene-graph / trajectory mutations (CONCEPT:EG-KG.memory.eg-batch-decay-caller):
    // each writes nodes/edges (summaries, semantic nodes, decay/evict
    // bookkeeping, scene objects, trajectories/steps) → Write access + WAL
    // record. The paired reads (SummaryChildren/SummariesAtLevel/
    // WorldTransform/SceneChildren/DiscountedReturn/BestTrajectory) stay Read.
    matches!(
        (method.write_family(), method),
        (
            Some(
                MethodWriteFamily::GraphElement
                    | MethodWriteFamily::WorkItemSubmission
                    | MethodWriteFamily::CapacityLease
                    | MethodWriteFamily::WorkItemLease
                    | MethodWriteFamily::WorkItemResource
                    | MethodWriteFamily::MemoryScene
            ),
            _
        ) | (
            _,
            Method::BeginTxn { .. }
                | Method::Rollback { .. }
                | Method::ClearGraph
                | Method::AddEmbedding { .. }
                | Method::PruneByLifecycle { .. }
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
                // X9: attaching, replacing or detaching a schema source
                // changes what the graph's own data is validated against, so
                // it is a graph write for the same reason IcvConfigure is.
                | Method::GraphSchema { .. }
                // RF-ADR-010: committing a decision record writes one
                // component revision into the agent-library owner.
                | Method::DecisionCommit { .. }
                | Method::DeleteGraph { .. }
                | Method::ClaimNext { .. }
                | Method::MintWorkItemClaimCapability { .. }
        )
    )
}

/// EH-316/EH-319 regression coverage: every `Method` variant named here is
/// wire-unconditional (see the module doc) -- `eg-capabilities` forces its
/// `eg-types` feature on for ANY `server` build, so these tests compile and
/// run under `--no-default-features --features server` (the exact profile
/// `.github/workflows/release.yml`'s "Test (slim server)" step exercises)
/// with no `#[cfg]` gate of their own needed for the variant to exist. Before
/// the fix, each of these was silently misclassified `Read` under that
/// profile because its classifier arm was wrongly gated behind this crate's
/// OWN (unrelated to variant existence) facade feature.
#[cfg(test)]
mod eh316_eh319_slim_profile_tests {
    use super::*;

    /// The whole Broker family, not just one variant: `requires_write_native_surface`'s
    /// `MethodWriteFamily::Broker` arm was gated behind this crate's own `broker`
    /// feature even though every Broker `Method` variant is wire-unconditional.
    #[test]
    fn broker_family_requires_write_regardless_of_broker_feature() {
        let m = Method::StreamPublish {
            stream: "s1".into(),
            payload: vec![1, 2, 3],
            now_ms: 0,
        };
        assert!(
            requires_write(&m),
            "a wire-unconditional Broker-family method must require write \
             regardless of this crate's own `broker` feature"
        );
    }

    #[test]
    fn kv_put_requires_write_regardless_of_kv_feature() {
        let m = Method::KvPut {
            namespace: "ns".into(),
            key: "k".into(),
            value: vec![1],
        };
        assert!(
            requires_write(&m),
            "a wire-unconditional KV mutation must require write regardless \
             of this crate's own `kv` feature"
        );
    }

    #[test]
    fn import_sqlite_file_requires_write_regardless_of_sqlite_file_feature() {
        let m = Method::ImportSqliteFile { path: "x.db".into() };
        assert!(
            requires_write(&m),
            "a wire-unconditional sqlite-file import must require write \
             regardless of this crate's own `sqlite-file` feature"
        );
    }

    #[test]
    fn mining_pipeline_serve_requires_write_regardless_of_ml_pipeline_feature() {
        let m = Method::MiningPipelineServe {
            name: "p1".into(),
            version: 1,
        };
        assert!(
            requires_write(&m),
            "Serve always deploys the served-model pointer; a \
             wire-unconditional MiningPipelineServe must require write \
             regardless of this crate's own `ml-pipeline` feature"
        );
    }

    /// Without the `query` parser compiled in, `Sql` must fail CLOSED to write
    /// rather than silently classify as `Read`.
    #[cfg(not(feature = "query"))]
    #[test]
    fn sql_fails_closed_to_write_without_the_query_parser() {
        let m = Method::Sql {
            query: "SELECT 1".into(),
            params_msgpack: Vec::new(),
        };
        assert!(
            requires_write(&m),
            "Sql must fail closed to write when the `query` parser is not \
             compiled in, not silently classify as Read"
        );
    }

    /// Without the `cypher` parser compiled in, `CypherQuery` must preserve the
    /// caller's OWN declared mode rather than silently classify as `Read`.
    #[cfg(not(feature = "cypher"))]
    #[test]
    fn cypher_query_falls_back_to_the_declared_mode_without_the_cypher_parser() {
        let read = Method::CypherQuery {
            query: "MATCH (n) RETURN n".into(),
            mode: CypherMode::Read,
        };
        assert!(!requires_write(&read));

        let write = Method::CypherQuery {
            query: "MATCH (n) RETURN n".into(),
            mode: CypherMode::Write,
        };
        assert!(
            requires_write(&write),
            "a declared-Write CypherQuery must fail closed to write when the \
             `cypher` parser is not compiled in, not silently classify as Read"
        );
    }

    /// Without the `graphql` parser compiled in, `GraphQl` must fail CLOSED to
    /// write rather than silently classify as `Read`.
    #[cfg(not(feature = "graphql"))]
    #[test]
    fn graphql_fails_closed_to_write_without_the_graphql_parser() {
        let m = Method::GraphQl {
            query: "mutation { noop }".into(),
            variables: None,
        };
        assert!(
            requires_write(&m),
            "GraphQl must fail closed to write when the `graphql` parser is \
             not compiled in, not silently classify as Read"
        );
    }
}
