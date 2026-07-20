//! Write/read classification + isolation-ACL enforcement for graph ops.

use super::auth::VerifiedRequestContext;
use crate::graph::{GraphCore, GraphView};
use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::{CypherMode, Method};
use std::sync::Arc;

/// Verified ownership carried into stores that are not naturally graph-scoped.
///
/// A method body's `tenant`, `actor`, namespace, cursor, or job id is never an
/// authority claim.  This object can only be derived from the verified v2 request
/// context and supplies stable opaque tenant/actor keys for durable ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CarrierAuthority {
    tenant_scope: String,
    actor_scope: String,
    owner_scope: String,
    agent_id: String,
    admin: bool,
}

impl CarrierAuthority {
    pub(crate) fn from_verified(context: &VerifiedRequestContext) -> Result<Self, String> {
        let tenant = context.tenant().trim();
        let principal = context.principal().trim();
        let agent_id = context.agent_id().trim();
        if tenant.is_empty() || principal.is_empty() || agent_id.is_empty() {
            crate::metrics::access_denied();
            return Err(
                "ACCESS_DENIED: verified data carrier is missing tenant, principal, or actor"
                    .to_string(),
            );
        }
        let tenant_scope = if opaque_scope(tenant, "carrier-tenant") {
            tenant.to_string()
        } else {
            crate::server::mutation_batch::opaque_coordinator_key(
                "carrier-tenant",
                "verified",
                tenant,
            )
        };
        let actor_scope = if opaque_scope(principal, "principal:sha256") {
            principal.to_string()
        } else {
            context.principal_persistence_id()
        };
        let owner_scope = crate::server::mutation_batch::opaque_coordinator_key(
            "carrier-owner",
            &tenant_scope,
            &actor_scope,
        );
        let admin = context
            .claims()
            .scopes
            .iter()
            .any(|scope| scope == "*" || scope == "kg:admin");
        Ok(Self {
            tenant_scope,
            actor_scope,
            owner_scope,
            agent_id: agent_id.to_string(),
            admin,
        })
    }

    pub(crate) fn tenant_scope(&self) -> &str {
        &self.tenant_scope
    }

    pub(crate) fn actor_scope(&self) -> &str {
        &self.actor_scope
    }

    pub(crate) fn owner_scope(&self) -> &str {
        &self.owner_scope
    }

    pub(crate) fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Collision-proof tenant+actor namespace for a caller-controlled local name.
    pub(crate) fn namespace(&self, domain: &str, local: &str) -> String {
        crate::server::mutation_batch::opaque_coordinator_key(domain, &self.owner_scope, local)
    }

    pub(crate) fn owns(&self, tenant_scope: &str, actor_scope: &str) -> bool {
        self.tenant_scope == tenant_scope && self.actor_scope == actor_scope
    }

    pub(crate) fn is_admin(&self) -> bool {
        self.admin
    }

    pub(crate) fn require_admin(&self, domain: &str) -> Result<(), String> {
        if self.admin {
            Ok(())
        } else {
            crate::metrics::access_denied();
            Err(format!(
                "ACCESS_DENIED: {domain} has no per-row ownership and requires kg:admin"
            ))
        }
    }
}

fn opaque_scope(value: &str, namespace: &str) -> bool {
    value
        .strip_prefix(namespace)
        .and_then(|rest| rest.strip_prefix(':'))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
}

/// Any carrier that has not produced the current verified context is denied
/// before touching engine state.
pub(crate) fn unauthenticated_carrier_denied(_isolation: &IsolationLayer) -> bool {
    true
}

/// The single row-level authority carried by every served graph read.
///
/// The actor is derived from [`VerifiedRequestContext`], never from the unsigned
/// display fields on the request envelope.  When row isolation is active, the
/// authority projects a detached graph containing only visible nodes, edges whose
/// two endpoints are visible, and embeddings belonging to visible nodes.  Passing
/// that projection to primitive/algorithm handlers closes existence, count,
/// batch, semantic, topology, and path side channels at the lowest shared read
/// boundary instead of relying on every response serializer to remember a filter.
#[derive(Clone)]
pub(crate) struct GraphReadAuthority {
    carrier: Option<CarrierAuthority>,
    #[cfg(feature = "security")]
    actor: String,
    #[cfg(feature = "security")]
    isolation: Arc<IsolationLayer>,
}

impl GraphReadAuthority {
    /// Build a read authority after request verification and graph ACL checks.
    /// The context is already verified before this constructor is reachable.
    pub(crate) fn from_verified(
        context: &VerifiedRequestContext,
        isolation: &IsolationLayer,
    ) -> Result<Self, String> {
        let carrier = Some(CarrierAuthority::from_verified(context)?);
        #[cfg(feature = "security")]
        {
            let actor = context.agent_id().trim().to_string();
            if actor.is_empty() {
                crate::metrics::access_denied();
                return Err(
                    "ACCESS_DENIED: verified row-level graph read has no actor identity"
                        .to_string(),
                );
            }
            return Ok(Self {
                carrier,
                actor,
                isolation: Arc::new(isolation.clone()),
            });
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = (context, isolation);
            Ok(Self { carrier })
        }
    }

    /// Verified tenant+actor authority for non-graph legs fused into a graph read.
    pub(crate) fn carrier(&self) -> Option<&CarrierAuthority> {
        self.carrier.as_ref()
    }

    /// Effective actor for graph ACL checks on secondary/cross-graph reads.
    pub(crate) fn actor(&self) -> Option<&str> {
        #[cfg(feature = "security")]
        {
            return (!self.actor.is_empty()).then_some(self.actor.as_str());
        }
        #[cfg(not(feature = "security"))]
        {
            None
        }
    }

    /// Return the non-empty actor bound to this verified read authority.
    /// Optional transport identity exists only before authorization; served
    /// handlers use this strict accessor before cache, RLS, admission, or state
    /// ownership logic.
    pub(crate) fn verified_actor(&self) -> Result<&str, String> {
        #[cfg(feature = "security")]
        let actor = self.actor.as_str();
        #[cfg(not(feature = "security"))]
        let actor = match self.carrier.as_ref() {
            Some(carrier) => carrier.agent_id(),
            None => {
                crate::metrics::access_denied();
                return Err("ACCESS_DENIED: verified row-level actor is required".to_string());
            }
        };
        if actor.trim().is_empty() {
            crate::metrics::access_denied();
            Err("ACCESS_DENIED: verified row-level actor is required".to_string())
        } else {
            Ok(actor)
        }
    }

    /// Whether per-row projection is required for this request.
    pub(crate) fn is_active(&self) -> bool {
        #[cfg(feature = "security")]
        {
            true
        }
        #[cfg(not(feature = "security"))]
        {
            false
        }
    }

    /// Apply the same lowest-level visibility predicate used by SQL/Cypher/RDF.
    pub(crate) fn filter_view(&self, view: &mut GraphView) {
        #[cfg(feature = "security")]
        self.isolation.filter_view(&self.actor, view);
        #[cfg(not(feature = "security"))]
        let _ = view;
    }

    /// Evaluate one CDC before/after property blob with the exact graph-row RLS
    /// predicate.  An absent image is not evidence of visibility; callers normally
    /// authorize an event when either its before or after image is visible.
    pub(crate) fn can_see_blob(&self, blob: &[u8]) -> bool {
        #[cfg(feature = "security")]
        {
            return self
                .isolation
                .can_see_row(&self.actor, &crate::isolation::row_visibility(blob));
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = blob;
            true
        }
    }

    /// Return the original core when RLS is inactive, otherwise a detached,
    /// fully filtered core safe for arbitrary primitive/algorithm reads.
    pub(crate) fn project_core(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
        if !self.is_active() {
            return core.clone();
        }

        let mut view = core.analysis_snapshot();
        self.filter_view(&mut view);
        let projected = GraphCore::new();

        // Stable ordering makes serialized projections and adversarial tests
        // deterministic even though the source maps are concurrent hash maps.
        let mut node_ids: Vec<String> = view.node_map.keys().cloned().collect();
        node_ids.sort();
        for node_id in &node_ids {
            let properties = view
                .node_properties
                .get(node_id)
                .map(|value| value.as_ref().clone())
                .unwrap_or_default();
            projected.add_node(node_id.clone(), properties);
        }

        let mut edge_keys: Vec<(String, String)> = view.edge_properties.keys().cloned().collect();
        edge_keys.sort();
        for (source, target) in edge_keys {
            if let Some(properties) = view.edge_properties.get(&(source.clone(), target.clone())) {
                for property in properties {
                    // Both endpoints survived `filter_view`; failure would indicate
                    // an internally inconsistent snapshot, so simply omit that edge
                    // rather than reintroducing a topology side channel.
                    let _ = projected.add_edge(
                        source.clone(),
                        target.clone(),
                        property.as_ref().clone(),
                    );
                }
            }
        }

        // Semantic search is a row read too. Rebuild only the visible portion so
        // ANN candidates cannot reveal hidden ids or alter result cardinality via a
        // post-hoc serializer filter.
        {
            let source = core.semantic_store.read();
            let mut target = projected.semantic_store.write();
            for node_id in &node_ids {
                if let Some(embedding) = source.get_embedding(node_id) {
                    target.add_embedding(node_id.clone(), embedding);
                }
            }
        }

        // Construction uses the ordinary mutation helpers, which append synthetic
        // ADD_* records. They are implementation detail, not source history. The
        // original ledger cannot be safely row-filtered from its unstructured string
        // representation, so active RLS serves no ledger rather than a fabricated
        // or cross-row history.
        projected.ledger.lock().clear();

        Arc::new(projected)
    }
}

/// Whether a graph-targeted method mutates the target graph (Write) or only
/// reads from it (Read). Pure-compute methods (finance, datascience, parse)
/// never touch graph state and classify as Read.
pub(crate) fn requires_write(method: &Method) -> bool {
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = method {
        return op.mutates();
    }
    // `AddTriples` / `RemoveTriples` / `DropNamedGraph` (feature `rdf`) mutate the
    // target graph's RDF content (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / EG-017).
    #[cfg(feature = "rdf")]
    if matches!(
        method,
        Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
    ) {
        return true;
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
        return true;
    }
    #[cfg(feature = "sqlite-file")]
    if matches!(method, Method::ImportSqliteFile { .. }) {
        return true;
    }
    // Message-broker admin + publish (CONCEPT:EG-KG.compute.message-broker-exchanges, feature `broker`) all mutate the
    // control graph's exchange/binding/message nodes, so they classify as writes (Write
    // access + WAL record). Consume/ack ride `ClaimNext`/`CompareAndSetNodeFields`,
    // already classified below.
    //
    // L10 (EG-P0-6 security finding): the stream + tag-addressed publisher-confirm/
    // consumer-ack family (`StreamDeclare`/`StreamPublish`/`StreamTrim`/
    // `StreamCommitOffset`/`PublishConfirmed`/`BrokerAckTag`/`BrokerNackTag`/
    // `BrokerRenewTag`/`PublishIdempotent`) mutates the SAME Outbox control-graph
    // state as the ops above — `mutation_apply.rs::is_durable_mutation` already
    // durable-logs all nine — but was
    // previously MISSING from this classifier, so a caller holding only Read access
    // to a graph could invoke them. Added here to close that gap: the ACL
    // classification now matches the durability classification exactly.
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
        return true;
    }
    // Query-surface writes (CONCEPT:EG-KG.query.mirrors-pgwire): the `Sql`/`CypherQuery`/`GraphQl` variants
    // carry a query STRING, so whether they mutate the graph depends on the statement,
    // not the variant. Parse just enough to classify; a write needs Write access and
    // (post-success) the dispatch shell's `mark_dirty` so the next checkpoint persists
    // it. A read (or an unparseable statement — the handler surfaces the parse error)
    // stays Read. Each detector is feature-gated to its surface.
    #[cfg(feature = "query")]
    if let Method::Sql { query, .. } = method {
        return sql_is_write(query);
    }
    #[cfg(feature = "cypher")]
    if let Method::CypherQuery { query, mode } = method {
        return match eg_query::classify_cypher(query) {
            Ok(eg_query::CypherStatementKind::Read) => false,
            Ok(eg_query::CypherStatementKind::Write) => true,
            // The dispatcher rejects parser errors and mode mismatches before
            // authorization. Until then, preserve the caller's more restrictive
            // declaration rather than accidentally admitting a declared write to
            // the reserved read lane.
            Err(_) => matches!(mode, CypherMode::Write),
        };
    }
    #[cfg(feature = "graphql")]
    if let Method::GraphQl { query, .. } = method {
        return graphql_is_mutation(query);
    }
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
        return *writeback;
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
        return *writeback && !matches!(algorithm, crate::protocol::TextAlgorithm::Tfidf);
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
        return *writeback && !matches!(algorithm, crate::protocol::SubgraphAlgorithm::Motif);
    }
    // Classification FIT is read-only (returns a model blob; no graph mutation).
    #[cfg(feature = "mining")]
    if matches!(method, Method::MineClassifyFit { .. }) {
        return false;
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
        return *writeback;
    }
    // Graph learning (CONCEPT:EG-KG.graphlearn.link-predictor): only a write when it
    // writes back the `:EdgeFunction` / `:PredictedEdge` nodes; a pure fit/predict
    // (writeback=false) reads its rows off an off-lock snapshot.
    #[cfg(feature = "graphlearn")]
    if let Method::GraphLearnFit { writeback, .. } | Method::GraphLearnPredict { writeback, .. } =
        method
    {
        return *writeback;
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
            | Method::RenewWorkItemLease { .. }
            | Method::CommitWorkItemResult { .. }
            | Method::CancelWorkItem { .. }
            | Method::DeferWorkItem { .. }
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

/// Whether a wire `Method::Sql` statement mutates state (CONCEPT:EG-KG.query.mirrors-pgwire). Reuses the
/// SAME `eg_query::classify` the pgwire shim routes on, so a graph-node DML
/// (INSERT/UPDATE/DELETE on `nodes`), a user-table DDL/DML (CREATE/ALTER/DROP TABLE,
/// INSERT/UPDATE/DELETE/COPY on a user table), classify as writes; a `SELECT`/`WITH`/
/// transaction-control statement, or one that does not parse, is not a write.
#[cfg(feature = "query")]
pub(crate) fn sql_is_write(query: &str) -> bool {
    use eg_query::StatementKind;
    !matches!(
        eg_query::classify(query),
        Ok(StatementKind::Read)
            | Ok(StatementKind::Begin)
            | Ok(StatementKind::Commit)
            | Ok(StatementKind::Rollback)
            | Err(_)
    )
}

/// Whether a GraphQL document is a MUTATION (a write) rather than a `query`/
/// `subscription` (CONCEPT:EG-KG.query.mirrors-pgwire). Uses eg-graphql's own `parse_operation`, so the
/// classification matches the executor exactly; an unparseable document is treated as
/// a non-write (the handler surfaces the parse error on a read snapshot).
#[cfg(feature = "graphql")]
pub(crate) fn graphql_is_mutation(query: &str) -> bool {
    matches!(
        eg_graphql::parse_operation(query),
        Ok(eg_graphql::Operation::Mutation(_))
    )
}

/// Enforce the isolation ACL for a graph-targeted operation.
///
/// A provisioned identity/RBAC policy is mandatory. Once rules exist,
/// `check_access` decides: peer agent graphs are denied,
/// managers reach subordinate graphs, team graphs are member-read/manager-write,
/// the `__commons__` stays open to all authenticated agents.
pub(crate) fn check_graph_access(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    access: AccessLevel,
) -> Result<(), String> {
    check_graph_access_with_policy(
        isolation,
        caller,
        graph_name,
        graph_type,
        graph_owner,
        access,
    )
}

fn check_graph_access_with_policy(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    access: AccessLevel,
) -> Result<(), String> {
    let agent = require_verified_caller(caller)?;
    if !isolation.has_rules() {
        crate::metrics::access_denied();
        return Err("ACCESS_DENIED: a provisioned identity/RBAC policy is required".to_string());
    }
    if isolation.check_access(agent, graph_name, graph_type, graph_owner, access) {
        Ok(())
    } else {
        crate::metrics::access_denied();
        Err(format!(
            "ACCESS_DENIED: verified principal lacks {access:?} access to graph '{graph_name}'"
        ))
    }
}

/// Resolve the authenticated ACL actor. Transport objects may exist before
/// authentication, but an absent or empty identity must never reach ACL, quota,
/// admission, or durable state as a synthetic bucket.
fn require_verified_caller(caller: Option<&str>) -> Result<&str, String> {
    caller
        .filter(|agent| !agent.trim().is_empty())
        .ok_or_else(|| {
            crate::metrics::access_denied();
            "ACCESS_DENIED: verified caller identity is required".to_string()
        })
}

/// L10 (EG-P0-6 security finding): every broker/stream mutating op that
/// `wal.rs::is_durable_mutation` already durable-logs must ALSO be classified a
/// write by `requires_write`, so a Read-only caller can never invoke it. Each case
/// below previously FAILED this assertion (classified `Read`, letting a Read-access
/// caller mutate the control graph); this test locks in the fix.
#[cfg(feature = "broker")]
#[cfg(test)]
mod l10_broker_stream_write_tests {
    use super::*;
    use crate::protocol::Method;

    #[test]
    fn stream_declare_requires_write() {
        assert!(requires_write(&Method::StreamDeclare {
            stream: "s1".into(),
            max_messages: None,
            max_age_ms: None,
        }));
    }

    #[test]
    fn stream_publish_requires_write() {
        assert!(requires_write(&Method::StreamPublish {
            stream: "s1".into(),
            payload: vec![1, 2, 3],
            now_ms: 0,
        }));
    }

    #[test]
    fn stream_trim_requires_write() {
        assert!(requires_write(&Method::StreamTrim {
            stream: "s1".into(),
            now_ms: 0,
        }));
    }

    #[test]
    fn stream_commit_offset_requires_write() {
        assert!(requires_write(&Method::StreamCommitOffset {
            stream: "s1".into(),
            group: "g1".into(),
            offset: 0,
        }));
    }

    #[test]
    fn publish_confirmed_requires_write() {
        assert!(requires_write(&Method::PublishConfirmed {
            exchange: "ex".into(),
            routing_key: "rk".into(),
            payload: vec![],
            priority: 0,
            delay_ms: None,
            ttl_ms: None,
            now_ms: None,
        }));
    }

    #[test]
    fn publish_idempotent_requires_write() {
        assert!(requires_write(&Method::PublishIdempotent {
            exchange: "ex".into(),
            routing_key: "rk".into(),
            payload: vec![],
            producer_id: None,
            seq: 0,
            priority: 0,
            delay_ms: None,
            ttl_ms: None,
            now_ms: None,
        }));
    }

    #[test]
    fn broker_ack_tag_requires_write() {
        assert!(requires_write(&Method::BrokerAckTag {
            delivery_tag: 1,
            consumer: "c1".into(),
        }));
    }

    #[test]
    fn broker_nack_tag_requires_write() {
        assert!(requires_write(&Method::BrokerNackTag {
            delivery_tag: 1,
            consumer: "c1".into(),
            requeue: true,
            now_ms: 0,
        }));
    }

    #[test]
    fn broker_renew_tag_requires_write() {
        assert!(requires_write(&Method::BrokerRenewTag {
            delivery_tag: 1,
            consumer: "c1".into(),
            now_ms: 0,
            lease_ms: 1,
        }));
    }

    /// Cross-check with the durability classifier: every one of these 9 ops is
    /// ALSO durable, so the ACL-write and WAL-durable classifications agree exactly
    /// (mirrors `durability_closure_tests::assert_write_implies_durable` below).
    #[test]
    fn all_nine_are_also_durable() {
        use crate::mutation_apply::is_durable_mutation;
        let methods = vec![
            Method::StreamDeclare {
                stream: "s1".into(),
                max_messages: None,
                max_age_ms: None,
            },
            Method::StreamPublish {
                stream: "s1".into(),
                payload: vec![],
                now_ms: 0,
            },
            Method::StreamTrim {
                stream: "s1".into(),
                now_ms: 0,
            },
            Method::StreamCommitOffset {
                stream: "s1".into(),
                group: "g1".into(),
                offset: 0,
            },
            Method::PublishConfirmed {
                exchange: "ex".into(),
                routing_key: "rk".into(),
                payload: vec![],
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            Method::PublishIdempotent {
                exchange: "ex".into(),
                routing_key: "rk".into(),
                payload: vec![],
                producer_id: None,
                seq: 0,
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            Method::BrokerAckTag {
                delivery_tag: 1,
                consumer: "c1".into(),
            },
            Method::BrokerNackTag {
                delivery_tag: 1,
                consumer: "c1".into(),
                requeue: true,
                now_ms: 0,
            },
            Method::BrokerRenewTag {
                delivery_tag: 1,
                consumer: "c1".into(),
                now_ms: 0,
                lease_ms: 1,
            },
        ];
        for m in &methods {
            assert!(requires_write(m), "{m:?} must require write");
            assert!(is_durable_mutation(m), "{m:?} must be durable");
        }
    }
}

/// Admin-scope enforcement (CONCEPT:EG-KG.compute.feature, EG-P0-6): is `action` (an
/// `eg_capabilities::MethodPolicy::authz_action` string) one of the system-wide
/// administrative actions that requires the caller to hold admin capability, rather
/// than the ordinary per-graph Read/Write ACL? Driven ENTIRELY off the ledger's
/// `authz_action` string — never a parallel hand-maintained method-name list — so a
/// future `Method` variant that declares one of these action strings in
/// `eg_capabilities::policy` is gated automatically, with no dispatch.rs edit.
///
/// Today this covers exactly the methods the EG-P0-1 divergence report flagged as
/// the "Zero-Trust Consensus" + M3 cluster-admin + DR family: `RegisterIdentity` /
/// `RbacAdmin` / `ApplyMultisigMutation` (`"security:admin"`), `Reshard` /
/// `CatalogAssign` / `CatalogReassign` / `CatalogRemove` (`"admin:cluster"`),
/// `RebalanceExecute` (`"admin:cluster"`), `CatalogList` / `RebalancePlan`
/// (`"admin:cluster-read"`), and `Backup` / `Restore` (`"admin:backup"`) — the exact
/// set `src/server/dispatch.rs`'s M3-admin arm + Zero-Trust-Consensus arms route.
pub(crate) fn is_admin_authz_action(action: &str) -> bool {
    action == "security:admin" || action.starts_with("admin:")
}

/// Enforce admin-scope for a method whose `authz_action` [`is_admin_authz_action`].
///
/// A provisioned identity/RBAC policy is mandatory. The caller must hold admin capability
/// ([`IsolationLayer::has_admin_capability`] — `System` role, or an explicit RBAC
/// `Admin` grant) — there is no coarse-ACL fallback for admin actions the way graph
/// Read/Write has one, so an agent with no admin grant is DENIED, not defaulted
/// open.
pub(crate) fn require_admin_capability(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    action: &'static str,
) -> Result<(), String> {
    require_admin_capability_with_policy(isolation, caller, action)
}

fn require_admin_capability_with_policy(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    action: &'static str,
) -> Result<(), String> {
    let agent = require_verified_caller(caller)?;
    if !isolation.has_rules() {
        crate::metrics::access_denied();
        return Err(format!(
            "ACCESS_DENIED: a provisioned identity/RBAC policy is required for '{action}'"
        ));
    }
    if isolation.has_admin_capability(agent) {
        Ok(())
    } else {
        crate::metrics::access_denied();
        Err(format!(
            "ACCESS_DENIED: verified principal lacks admin capability required for '{action}'"
        ))
    }
}

#[cfg(test)]
mod secure_empty_policy_tests {
    use super::*;
    use crate::protocol::GraphType;

    #[test]
    fn secure_graph_access_fails_closed_when_identity_store_is_empty() {
        let isolation = IsolationLayer::new();
        let result = check_graph_access_with_policy(
            &isolation,
            Some("agent:a"),
            "agent:a",
            GraphType::Agent,
            Some("agent:a"),
            AccessLevel::Read,
        );
        assert!(result
            .unwrap_err()
            .contains("provisioned identity/RBAC policy"));
    }

    #[test]
    fn secure_admin_access_fails_closed_when_identity_store_is_empty() {
        let isolation = IsolationLayer::new();
        let result =
            require_admin_capability_with_policy(&isolation, Some("agent:a"), "security:admin");
        assert!(result
            .unwrap_err()
            .contains("provisioned identity/RBAC policy"));
    }

    #[test]
    fn graph_access_rejects_absent_or_empty_verified_identity() {
        let isolation = IsolationLayer::new();
        for caller in [None, Some(""), Some("   ")] {
            let error = check_graph_access_with_policy(
                &isolation,
                caller,
                "__commons__",
                GraphType::Commons,
                None,
                AccessLevel::Read,
            )
            .unwrap_err();
            assert_eq!(error, "ACCESS_DENIED: verified caller identity is required");
        }
    }

    #[test]
    fn admin_access_rejects_absent_or_empty_verified_identity() {
        let isolation = IsolationLayer::new();
        for caller in [None, Some(""), Some("   ")] {
            let error = require_admin_capability_with_policy(&isolation, caller, "security:admin")
                .unwrap_err();
            assert_eq!(error, "ACCESS_DENIED: verified caller identity is required");
        }
    }
}

/// EG-P0-3 (WAL durability closure): `requires_write` (this file) classifies a
/// method as a mutation for the isolation-ACL check; `crate::mutation_apply::is_durable_mutation`
/// classifies it as needing a WAL record so it survives a crash. The two MUST agree
/// for every method that can actually mutate durable data — a method acknowledged as
/// a write but never logged is silently lost on crash. This lives here (not in
/// `tests/`) because `requires_write` is `pub(crate)`: an external integration-test
/// crate cannot name it, so the invariant can only be asserted from inside the crate.
///
/// Covers the 5 methods audited under EG-P0-3 (all previously FAILED this
/// assertion): `MineSequence`, `MineForecast`, the `MineText` non-tfidf variant, the
/// `MineSubgraph` gspan variant, and `AddEmbedding`.
#[cfg(test)]
mod durability_closure_tests {
    use super::*;
    use crate::mutation_apply::is_durable_mutation;
    use crate::protocol::Method;

    /// The one assertion every case below exercises: whenever `requires_write`
    /// says a method mutates (and therefore requires Write ACL + a write lock),
    /// `is_durable_mutation` must ALSO say it belongs in the WAL. (The converse
    /// need not hold — e.g. a durable method reached only via a self-routing
    /// surface like KV is fine — so this is a one-directional implication, not
    /// an equality.)
    fn assert_write_implies_durable(m: &Method) {
        assert!(
            !requires_write(m) || is_durable_mutation(m),
            "EG-P0-3: {m:?} is classified a write by `access::requires_write` but \
             NOT durable by `mutation_apply::is_durable_mutation` — it would be acknowledged \
             then silently lost on crash"
        );
    }

    #[cfg(feature = "mining")]
    #[test]
    fn mine_sequence_writeback_is_durable() {
        use crate::protocol::MineSeqAlgorithm;
        let m = Method::MineSequence {
            sequences: vec![vec!["a".into(), "b".into()]],
            source: None,
            min_support: 0.5,
            algorithm: MineSeqAlgorithm::Prefixspan,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        assert!(
            requires_write(&m),
            "writeback=true must classify as a write"
        );
        assert_write_implies_durable(&m);
    }

    #[cfg(feature = "mining")]
    #[test]
    fn mine_forecast_writeback_is_durable() {
        use crate::protocol::ForecastAlgorithm;
        let m = Method::MineForecast {
            values: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            algorithm: ForecastAlgorithm::Arima,
            horizon: 2,
            p: 1,
            d: 0,
            q: 0,
            period: 0,
            alpha: 0.3,
            beta: 0.1,
            gamma: 0.1,
            confidence: 0.95,
            series_id: "s1".into(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        assert!(
            requires_write(&m),
            "writeback=true must classify as a write"
        );
        assert_write_implies_durable(&m);
    }

    /// `MineText` writeback only mutates for `lda`/`nmf` — the durable set must
    /// mirror that EXACT condition, not just "writeback=true" (a `tfidf` write
    /// would otherwise be logged for an op that never touches the graph).
    #[cfg(feature = "mining")]
    #[test]
    fn mine_text_lda_writeback_is_durable_but_tfidf_is_not() {
        use crate::protocol::TextAlgorithm;
        let base = |algorithm: TextAlgorithm| Method::MineText {
            docs: vec![vec!["a".into(), "b".into()]],
            source: None,
            algorithm,
            k: 2,
            alpha: 0.1,
            beta: 0.01,
            iterations: 10,
            seed: 1,
            top_n: 5,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let lda = base(TextAlgorithm::Lda);
        assert!(
            requires_write(&lda),
            "lda writeback=true must classify as a write"
        );
        assert_write_implies_durable(&lda);

        // tfidf never mutates regardless of `writeback` — both classifiers must
        // agree it is NOT a write (never durable, never write-locked).
        let tfidf = base(TextAlgorithm::Tfidf);
        assert!(
            !requires_write(&tfidf),
            "tfidf must never classify as a write, even with writeback=true"
        );
        assert!(
            !is_durable_mutation(&tfidf),
            "tfidf must never be durable, even with writeback=true"
        );
    }

    /// `MineSubgraph` writeback only mutates for `gspan` — mirrors the `MineText`
    /// exact-condition contract above (`motif` is a pure census, never a write).
    #[cfg(feature = "mining")]
    #[test]
    fn mine_subgraph_gspan_writeback_is_durable_but_motif_is_not() {
        use crate::protocol::SubgraphAlgorithm;
        let base = |algorithm: SubgraphAlgorithm| Method::MineSubgraph {
            label: None,
            min_support: 0.1,
            max_edges: 2,
            algorithm,
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let gspan = base(SubgraphAlgorithm::Gspan);
        assert!(
            requires_write(&gspan),
            "gspan writeback=true must classify as a write"
        );
        assert_write_implies_durable(&gspan);

        let motif = base(SubgraphAlgorithm::Motif);
        assert!(
            !requires_write(&motif),
            "motif must never classify as a write, even with writeback=true"
        );
        assert!(
            !is_durable_mutation(&motif),
            "motif must never be durable, even with writeback=true"
        );
    }

    #[test]
    fn add_embedding_is_durable() {
        let m = Method::AddEmbedding {
            node_id: "n1".into(),
            embedding: vec![0.1, 0.2, 0.3],
        };
        assert!(requires_write(&m));
        assert_write_implies_durable(&m);
    }
}

#[cfg(all(test, feature = "security"))]
mod universal_row_read_tests {
    use super::*;
    use crate::isolation::{AgentIdentity, AgentRole};

    fn properties(values: &[(&str, &str)]) -> Vec<u8> {
        let map: std::collections::BTreeMap<String, String> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        rmp_serde::to_vec_named(&map).unwrap()
    }

    fn shared_graph() -> (Arc<GraphCore>, IsolationLayer) {
        let core = Arc::new(GraphCore::new());
        core.add_node(
            "alice-private".to_string(),
            properties(&[
                ("_owner", "alice"),
                ("_visibility", "private"),
                ("type", "Thing"),
            ]),
        );
        core.add_node(
            "bob-private".to_string(),
            properties(&[
                ("_owner", "bob"),
                ("_visibility", "private"),
                ("type", "Thing"),
            ]),
        );
        core.add_node(
            "public".to_string(),
            properties(&[("_visibility", "public"), ("type", "Thing")]),
        );
        // A topology row with no valid property metadata is hidden in strict mode.
        core.add_node("untagged".to_string(), Vec::new());
        core.add_edge("alice-private".into(), "public".into(), Vec::new())
            .unwrap();
        core.add_edge("bob-private".into(), "public".into(), Vec::new())
            .unwrap();
        core.add_edge("public".into(), "untagged".into(), Vec::new())
            .unwrap();
        {
            let mut semantic = core.semantic_store.write();
            semantic.add_embedding("alice-private".into(), vec![1.0, 0.0]);
            semantic.add_embedding("bob-private".into(), vec![0.0, 1.0]);
            semantic.add_embedding("public".into(), vec![0.7, 0.7]);
            semantic.add_embedding("untagged".into(), vec![-1.0, 0.0]);
        }

        let mut isolation = IsolationLayer::new();
        for agent_id in ["alice", "bob"] {
            isolation.register_agent(AgentIdentity {
                agent_id: agent_id.to_string(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        (core, isolation)
    }

    #[test]
    fn alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows() {
        let (core, isolation) = shared_graph();
        let alice_context = super::super::auth::VerifiedRequestContext::verified_for_test("alice");
        let bob_context = super::super::auth::VerifiedRequestContext::verified_for_test("bob");
        let alice = GraphReadAuthority::from_verified(&alice_context, &isolation).unwrap();
        let bob = GraphReadAuthority::from_verified(&bob_context, &isolation).unwrap();
        assert_eq!(alice.verified_actor().unwrap(), "alice");
        assert_eq!(bob.verified_actor().unwrap(), "bob");
        let alice_core = alice.project_core(&core);
        let bob_core = bob.project_core(&core);

        // Existence, point, batch, ids, and aggregate-count side channels.
        assert!(alice_core.has_node("alice-private"));
        assert!(!alice_core.has_node("bob-private"));
        assert!(!alice_core.has_node("untagged"));
        assert_eq!(alice_core.node_count(), 2);
        assert_eq!(bob_core.node_count(), 2);
        assert!(alice_core.get_node_properties("bob-private").is_none());
        let batch = ["alice-private", "bob-private", "public"]
            .map(|id| alice_core.get_node_properties(id).is_some());
        assert_eq!(batch, [true, false, true]);
        let alice_ids: std::collections::BTreeSet<_> = alice_core.node_ids().into_iter().collect();
        assert_eq!(
            alice_ids,
            ["alice-private".to_string(), "public".to_string()]
                .into_iter()
                .collect()
        );

        // Edge existence, degree, neighborhoods, topology, and path computation
        // all run on the projection, not on a post-filtered response.
        assert_eq!(alice_core.edge_count(), 1);
        assert!(alice_core.has_edge("alice-private", "public"));
        assert!(!alice_core.has_edge("bob-private", "public"));
        assert_eq!(alice_core.in_degree("public").unwrap(), 1);
        let neighbors: std::collections::BTreeSet<_> = alice_core
            .get_neighbors("public")
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            neighbors,
            ["alice-private".to_string()].into_iter().collect()
        );
        let topology = alice_core.topology_snapshot();
        assert_eq!(
            crate::algorithms::get_shortest_path(&topology, "alice-private", "public"),
            Some(vec!["alice-private".to_string(), "public".to_string()])
        );
        assert!(crate::algorithms::get_shortest_path(&topology, "bob-private", "public").is_none());

        // Semantic candidates are rebuilt from visible embeddings only; querying
        // exactly for Bob's hidden vector still cannot return Bob's id to Alice.
        let semantic = alice_core
            .semantic_store
            .read()
            .semantic_search(&[0.0, 1.0], 10);
        assert!(!semantic.is_empty());
        assert!(semantic.iter().all(|(id, _)| id != "bob-private"));
        assert!(semantic
            .iter()
            .all(|(id, _)| alice_ids.contains(id.as_str())));

        assert!(bob_core.has_node("bob-private"));
        assert!(!bob_core.has_node("alice-private"));
    }

    #[test]
    fn carrier_ownership_separates_same_tenant_and_cross_tenant_callers() {
        let alice = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "alice", "tenant-a",
            ),
        )
        .unwrap();
        let bob = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "bob", "tenant-a",
            ),
        )
        .unwrap();
        let alice_other_tenant = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "alice", "tenant-b",
            ),
        )
        .unwrap();

        assert_eq!(alice.tenant_scope(), bob.tenant_scope());
        assert_ne!(alice.actor_scope(), bob.actor_scope());
        assert_ne!(alice.owner_scope(), bob.owner_scope());
        assert_ne!(alice.tenant_scope(), alice_other_tenant.tenant_scope());
        assert_ne!(
            alice.namespace("kv-namespace", "shared"),
            bob.namespace("kv-namespace", "shared")
        );
        assert!(!bob.owns(alice.tenant_scope(), alice.actor_scope()));
        assert!(!alice_other_tenant.owns(alice.tenant_scope(), alice.actor_scope()));
    }
}
