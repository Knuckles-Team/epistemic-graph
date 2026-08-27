//! Write/read classification + isolation-ACL enforcement for graph ops.

use super::auth::VerifiedRequestContext;
use crate::graph::{GraphCore, GraphView};
#[cfg(feature = "security")]
use crate::isolation::row_visibility;
use crate::isolation::{AccessLevel, IsolationLayer};
#[cfg(feature = "cypher")]
use crate::protocol::CypherMode;
use crate::protocol::Method;
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
    can_read: bool,
    can_write: bool,
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
        let scopes = &context.claims().scopes;
        let admin = scopes
            .iter()
            .any(|scope| scope == "*" || scope == "kg:admin");
        // Coarse per-verb capability, independent of `admin`: every existing
        // carrier-minting site (SigV4, KV-cache, native SQL, broker, ...) mints
        // BOTH `kg:read` and `kg:write` unconditionally, so these two remain
        // `true` for all of them exactly as before this field existed. The
        // Iceberg-REST bearer path (`auth::authenticated_iceberg_bearer`,
        // NE-048) is the first minting site whose scopes are NOT hardcoded --
        // they are projected from the verified bearer's own claim -- so a
        // `kg:read`-only bearer now yields `can_write() == false` here, closing
        // the P0 privilege escalation where every Iceberg-REST bearer silently
        // received write authority regardless of what it was actually issued.
        let can_read = admin || scopes.iter().any(|scope| scope == "kg:read");
        let can_write = admin || scopes.iter().any(|scope| scope == "kg:write");
        Ok(Self {
            tenant_scope,
            actor_scope,
            owner_scope,
            agent_id: agent_id.to_string(),
            admin,
            can_read,
            can_write,
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

    /// Coarse read capability (`kg:read` or admin). See the field's doc
    /// comment in [`Self::from_verified`] for why this is `true` for every
    /// pre-existing carrier-minting site and only meaningfully varies for the
    /// Iceberg-REST bearer path (NE-048).
    pub(crate) fn can_read(&self) -> bool {
        self.can_read
    }

    /// Coarse write capability (`kg:write` or admin). See [`Self::can_read`].
    pub(crate) fn can_write(&self) -> bool {
        self.can_write
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

/// Any carrier that has not produced a verified [`CarrierAuthority`] is denied
/// before touching engine state (A18). Each auxiliary surface must first attempt
/// to authenticate the caller through its OWN protocol-appropriate mechanism —
/// SigV4 for the S3-compatible REST surface, the `eg2.` envelope for SPARQL
/// mutations, the bearer/JWT guard for the KV-cache surface, ... — and mint a
/// `CarrierAuthority` only on success (mirroring how `pgwire`/`mysql-wire`/
/// `bolt-wire` bind a server-owned tenant+actor authority only after their own
/// cryptographic proof succeeds). This function's only job is to ask whether
/// that succeeded; it cannot itself authenticate anything, since a per-request
/// credential is never available at this layer.
pub(crate) fn unauthenticated_carrier_denied(carrier: Option<&CarrierAuthority>) -> bool {
    carrier.is_none()
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
            Ok(Self {
                carrier,
                actor,
                isolation: Arc::new(isolation.clone()),
            })
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
            (!self.actor.is_empty()).then_some(self.actor.as_str())
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

    /// perf/row-visibility-index (B-sweep) — the `FilteredViewCache`-aware
    /// equivalent of a caller's own `core.analysis_snapshot()` + `self.filter_view(&mut
    /// view)` pair, for a call site that wants a PLAIN (no write-set overlay)
    /// RLS-filtered read of the current graph. Mirrors `Method::CypherQuery`'s
    /// probe-then-build shape (`src/server/handlers/query.rs`) using THIS
    /// authority's own actor: a hit returns the SAME shared `Arc<GraphView>` a
    /// concurrent request for this actor at this version already built; a miss
    /// builds+filters exactly as before and publishes the result for the next
    /// caller. `version` is read BEFORE the snapshot is taken — a safe LOWER
    /// bound (an under-estimate only risks one extra rebuild, never a stale
    /// serve — see `crate::server::handlers::query::rls_snapshot`'s doc for the
    /// full argument), since [`GraphReadAuthority`] does not hold the topology
    /// lock across both reads the way `GraphCore::analysis_snapshot_versioned`
    /// does.
    ///
    /// Callers that overlay buffered writes onto the view before use (a staged
    /// transaction's own read-your-own-writes path) MUST NOT use this — the
    /// cached entry is a pure function of (actor, version) alone and is shared
    /// across every caller at that version; mutating it would corrupt another
    /// caller's cached read. Build an owned snapshot via
    /// `core.analysis_snapshot()` + [`Self::filter_view`] instead for that case,
    /// exactly as today.
    #[cfg(feature = "security")]
    pub(crate) fn cached_filter_view(&self, core: &Arc<GraphCore>) -> Arc<GraphView> {
        let probe_version = core.version();
        if let Some(cached) = core.cached_filtered_view(&self.actor, probe_version) {
            return cached;
        }
        let generation = core.filtered_view_cache_generation();
        let mut fresh = core.analysis_snapshot();
        self.filter_view(&mut fresh);
        let fresh = Arc::new(fresh);
        core.put_cached_filtered_view(self.actor.clone(), probe_version, generation, fresh.clone());
        fresh
    }

    /// Answer ONE node-membership question without materializing a projection.
    ///
    /// `project_core` builds a whole filtered `GraphCore`
    /// (`O(V log V + E log E + V*d)`), which a point lookup does not need: a
    /// `HasNode` used to pay the same cost as a full graph dump, and on a live
    /// 56k-node graph that measured ~1.2s per call even WITH the projection cache
    /// in front of it (the cache is keyed on `GraphCore::version()`, so an ingest
    /// that interleaves reads and writes misses on essentially every read).
    ///
    /// Visibility is delegated to `IsolationLayer::can_see_node` — the SAME
    /// function `filter_view` applies to every node — so this fast path cannot
    /// drift from the bulk path it short-circuits. It is id-aware, which the
    /// blob-only `can_see_node` below deliberately is not: a node present in the
    /// topology with NO property blob must be judged `default_public` (what
    /// `filter_view` does), and `row_visibility(&[])` is not that. `analysis_snapshot()` is the
    /// cheap half of `build_projection` (it clones concurrent-map handles and the
    /// `schema_node_ids` reverse index, not every node's property blob); the
    /// expensive half is the sort + per-node clone into a second core, which is
    /// exactly what this skips.
    pub(crate) fn node_visible(&self, core: &GraphCore, node_id: &str) -> bool {
        let view = core.analysis_snapshot();
        if !view.node_map.contains_key(node_id) {
            return false;
        }
        #[cfg(feature = "security")]
        {
            self.isolation.can_see_node(&self.actor, &view, node_id)
        }
        #[cfg(not(feature = "security"))]
        {
            true
        }
    }

    /// Evaluate one CDC before/after property blob with the exact graph-row RLS
    /// predicate.  An absent image is not evidence of visibility; callers normally
    /// authorize an event when either its before or after image is visible.
    pub(crate) fn can_see_blob(&self, blob: &[u8]) -> bool {
        #[cfg(feature = "security")]
        {
            self.isolation
                .can_see_row(&self.actor, &crate::isolation::row_visibility(blob))
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = blob;
            true
        }
    }

    /// Return the original core when RLS is inactive, otherwise a detached,
    /// fully filtered core safe for arbitrary primitive/algorithm reads.
    ///
    /// D-OP-1 / D-OB-20: the filtered core is expensive to build (sort + clone every
    /// visible node/edge, copy every visible embedding — `O(V log V + E log E +
    /// V*d)`), so it is cached per (actor, `GraphCore::version()`) and reused across
    /// repeat calls between writes — see `crate::rls_projection_cache` for the full
    /// rationale. A cache hit returns byte-identical content to a fresh rebuild for
    /// the same (actor, version) pair (proven by
    /// `alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows` and
    /// `project_core_reflects_a_mutation_after_the_previous_projection_was_cached`,
    /// both below) — this amortizes the existing guarantee, it does not relax it.
    pub(crate) fn project_core(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
        if !self.is_active() {
            return core.clone();
        }
        // `is_active()` is only ever `true` when the `security` feature is compiled
        // in (see its own doc), so the actor field + cache accessors this delegates
        // to — both `security`-gated — are always present past this point. Split
        // into its own `#[cfg]`-gated method rather than inlining `self.actor` here
        // directly: this function itself is NOT `#[cfg]`-gated (it must exist and
        // type-check in a `not(security)` build too, where it is simply dead code
        // behind the early return above).
        self.project_core_active(core)
    }

    /// The cache-checking half of [`Self::project_core`], split out because it
    /// touches the `security`-only `actor` field and cache accessors — see that
    /// method's doc. Only ever called when `is_active()` is `true`.
    #[cfg(feature = "security")]
    fn project_core_active(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
        let actor = self.actor.clone();
        let current_version = core.version();
        if let Some(cached) = core.cached_projection(&actor, current_version) {
            crate::metrics::projection_cache_hit();
            return cached;
        }

        // U-142/U-143/U-145 (BUG-130): capture the whole-image generation BEFORE the
        // (potentially slow, unlocked) rebuild starts, not after. A whole-image
        // replacement/clear (`GraphCore::replace_snapshot`/`clear`/`hibernate`) that
        // lands while this build is in flight bumps the generation; `put_cached_projection`
        // then refuses to publish this now-stale result. See
        // `GraphCore::projection_cache_generation`'s doc.
        let generation = core.projection_cache_generation();
        // D-EGP-1 (t1-grounding-0802): time the rebuild a miss triggers — see
        // `crate::metrics::projection_cache_miss` for why this is the decisive
        // measurement for the RLS-cache-thrash hypothesis.
        let build_started = std::time::Instant::now();
        let projected = self.build_projection(core);
        crate::metrics::projection_cache_miss(build_started.elapsed().as_secs_f64());
        core.put_cached_projection(actor, current_version, generation, projected.clone());
        projected
    }

    /// Unreachable in practice (`is_active()` is always `false` without `security`,
    /// so `project_core` already returned above) — exists only so `project_core`
    /// type-checks in a `not(security)` build.
    #[cfg(not(feature = "security"))]
    fn project_core_active(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
        core.clone()
    }

    /// The single-row sibling of [`Self::filter_view`] (which `build_projection`
    /// uses in bulk): may this authority's actor see `properties_msgpack`?
    ///
    /// `build_projection`'s RAM-only `analysis_snapshot()` can never discover a
    /// node `EvictLRU` fully evicted from the live topology (its id is gone from
    /// `topo.node_map`, not merely its properties — see
    /// `GraphCore::evict_resident_nodes`), so a point read that falls back to the
    /// RAW core's `read_through` for such a node (`Method::GetNodeProperties`)
    /// must re-derive the SAME visibility decision `filter_view` would have made,
    /// on exactly that one row, rather than serving it unfiltered.
    ///
    /// `is_schema` (BUG A3, 2026-08-12) is the caller-supplied, DERIVED
    /// TBox/ABox answer for this exact node id (`GraphCore::is_schema_node`) —
    /// this function has no id, only a blob, so it cannot look it up itself;
    /// `row_visibility`'s own `.schema` is always `false` (it no longer
    /// decodes a `_schema` property, which nothing clears on axiom deletion).
    /// See `IsolationLayer::filter_view` for the bulk-path sibling of this
    /// same derivation.
    #[cfg(feature = "security")]
    pub(crate) fn can_see_node(&self, properties_msgpack: &[u8], is_schema: bool) -> bool {
        let mut vis = row_visibility(properties_msgpack);
        vis.schema = is_schema;
        self.isolation.can_see_row(&self.actor, &vis)
    }

    /// Unreachable in practice (`is_active()` is always `false` without
    /// `security`, so the projection is never bypassed) — exists only so callers
    /// type-check in a `not(security)` build, mirroring `project_core_active`.
    #[cfg(not(feature = "security"))]
    pub(crate) fn can_see_node(&self, _properties_msgpack: &[u8], _is_schema: bool) -> bool {
        true
    }

    /// The actual (expensive) materialization [`Self::project_core_active`] caches. Kept as
    /// its own method so the cache lookup/store bracketing it stays visually obvious
    /// at the call site and so a cache bypass (e.g. a future forced-fresh caller) has
    /// a name to call directly.
    ///
    /// D-EGP-1 (t1-grounding-0802): uses `add_node_no_ledger`/`add_edge_no_ledger`
    /// rather than the ordinary `add_node`/`add_edge`. Construction used to go
    /// through the ordinary mutation helpers, which append synthetic `ADD_*`
    /// records that this method then threw away wholesale (`ledger.lock().clear()`)
    /// because the original ledger cannot be safely row-filtered from its
    /// unstructured string representation, so active RLS serves no ledger rather
    /// than a fabricated or cross-row history. Every one of those records was a
    /// `HexLedger`-formatted (per-byte `write!`) copy of the node/edge's full
    /// property blob — on a live pod this measured as the DOMINANT cost of a cache
    /// miss (`epistemic_graph_projection_cache_miss_build_seconds`: 2.4-15s per
    /// miss on a 27,969-node graph). The `_no_ledger` variants skip the format +
    /// hex-encode + ledger-mutex-push entirely; there is no longer a ledger to
    /// clear, and the resulting `GraphCore` is byte-for-byte identical in every
    /// OTHER respect (node_map/graph/node_properties/edge_properties/bloom).
    fn build_projection(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
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
            projected.add_node_no_ledger(node_id.clone(), properties);
            // BUG A3 follow-up (2026-08-12): a fresh `GraphCore::new()` starts
            // with an EMPTY `schema_refs`, so without this, a schema node that
            // legitimately survived `filter_view` above (via the SOURCE
            // core's real TBox membership) would fail RLS again on the very
            // next `analysis_snapshot()` taken of THIS projected core — every
            // read surface that calls `rls.filter_view` a second time against
            // an already-projected core (SQL/UQL, `handlers/query.rs`) would
            // silently re-hide it, exactly reproducing BUG A3's symptom one
            // layer downstream of the fix in `eg_core::isolation`. Mirror the
            // SOURCE core's TBox membership onto the projection for every node
            // that survived filtering; a single ref is enough (the projection
            // is a throwaway, read-only snapshot — never mutated further, so
            // exact refcount fidelity does not matter here).
            if core.is_schema_node(node_id) {
                projected.mark_schema_ref(node_id);
            }
        }

        let mut edge_keys: Vec<(String, String)> = view.edge_properties.keys().cloned().collect();
        edge_keys.sort();
        for (source, target) in edge_keys {
            if let Some(properties) = view.edge_properties.get(&(source.clone(), target.clone())) {
                for property in properties {
                    // Both endpoints survived `filter_view`; failure would indicate
                    // an internally inconsistent snapshot, so simply omit that edge
                    // rather than reintroducing a topology side channel.
                    let _ = projected.add_edge_no_ledger(
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
                    // `target` starts empty and every embedding copied here comes
                    // from the SAME already-valid, uniform-dimension `source`, so
                    // this cannot legitimately mismatch (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard,
                    // BUG-007). `build_projection` returns `Arc<GraphCore>` (not a
                    // Result), so a surprise here degrades gracefully — the node is
                    // logged and simply absent from this read's kNN candidates —
                    // rather than panicking a read path over a should-never-happen.
                    if let Err(error) = target.add_embedding(node_id.clone(), embedding) {
                        tracing::warn!(
                            node_id = node_id.as_str(),
                            %error,
                            "RLS projection dropped a node's embedding — source store had an unexpected dimension"
                        );
                    }
                }
            }
        }

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
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): Train/Predict mutate only when
    // their `writeback` writes back the `:Model` / `:Prediction` nodes; Evaluate and
    // Compare are pure reads (they fall through to `false`).
    #[cfg(feature = "ml-pipeline")]
    if let Method::MiningPipelineTrain { writeback, .. }
    | Method::MiningPipelinePredict { writeback, .. } = method
    {
        return *writeback;
    }
    // Serve ALWAYS writes the `:ServedModel` pointer (deploy the version).
    #[cfg(feature = "ml-pipeline")]
    if matches!(method, Method::MiningPipelineServe { .. }) {
        return true;
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

/// Deny an unregistered/unauthenticated caller (or an unprovisioned isolation
/// policy) BEFORE the target graph's existence is resolved.
///
/// [`IsolationLayer::check_access`] denies an unregistered agent unconditionally —
/// `self.agents.get(agent_id)` is the very first thing it checks, before it ever
/// looks at the graph's type/owner — so this mirrors just that identity-only slice
/// of the decision. Calling it ahead of the graph-existence lookup means a caller
/// who could never pass ACL for ANY graph is denied with the SAME `ACCESS_DENIED`
/// message they would eventually get anyway, instead of first learning whether the
/// target graph exists via a distinguishable "Graph '...' not found" response — an
/// unregistered caller must not be able to probe graph names this way. A caller who
/// IS registered always falls through unchanged to the existing
/// existence-then-`check_graph_access` sequence, which still makes the real,
/// graph-type/owner-aware decision.
pub(crate) fn check_caller_is_known(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
) -> Result<(), String> {
    let agent = require_verified_caller(caller)?;
    if !isolation.has_rules() {
        crate::metrics::access_denied();
        return Err("ACCESS_DENIED: a provisioned identity/RBAC policy is required".to_string());
    }
    if isolation.is_registered(agent) {
        return Ok(());
    }
    crate::metrics::access_denied();
    Err(format!(
        "ACCESS_DENIED: verified principal lacks {access:?} access to graph '{graph_name}'"
    ))
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

    /// BUG-193: the "public" row below is tagged `_owner_id` (the BUG-052/GOC-61
    /// canonical convention), NOT a bare `_visibility: "public"` with no owner —
    /// that shape is exactly the 21,064-row BUG-064 incident population, and
    /// `row_visibility`'s BUG-192 middle branch correctly denies it (see
    /// `isolation::tests::rls::default_deny::unowned_bare_visibility_tag_alone_is_no_longer_trusted`).
    /// A real production write through the mutation gateway now stamps
    /// `_owner_id` from the caller's identity when absent
    /// (`stamp_owner_id_if_absent`, wired in `handlers::graph_ops::
    /// try_handle_gateway`); this fixture models that shape rather than the
    /// unstamped one no production write path can produce anymore. With no
    /// `_visibility`/`_shared_scope` key set, the row keeps the pre-existing
    /// bare-absent-default (visible beyond its owner), so it stays visible to
    /// both `alice` and `bob` exactly as this fixture's tests require.
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
            properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
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
            semantic
                .add_embedding("alice-private".into(), vec![1.0, 0.0])
                .unwrap();
            semantic
                .add_embedding("bob-private".into(), vec![0.0, 1.0])
                .unwrap();
            semantic
                .add_embedding("public".into(), vec![0.7, 0.7])
                .unwrap();
            semantic
                .add_embedding("untagged".into(), vec![-1.0, 0.0])
                .unwrap();
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

    /// The equivalence oracle for the `HasNode`/`HasNodesBatch` point-lookup fast
    /// path (`GraphReadAuthority::node_visible`).
    ///
    /// That fast path exists so a boolean membership question stops paying for a
    /// whole `O(V log V + E log E + V*d)` projection. The ONLY thing that makes it
    /// safe is that it must answer identically to the projection it skips — this
    /// crate enforces row-level isolation, so a fast path that disagrees is a
    /// data-leak bug, not a performance bug.
    ///
    /// Asserting equivalence (rather than hardcoding expected booleans) is
    /// deliberate: it pins the fast path TO the bulk path, so a future change to
    /// RLS policy that moves one also has to move the other or fail here. The
    /// fixture deliberately spans every visibility class the predicate
    /// distinguishes — another actor's private row, one's own private row, a
    /// public row, a topology row with NO property blob (`untagged`, which
    /// `filter_view` judges as `default_public` rather than implicitly visible),
    /// and an id that is absent entirely.
    #[test]
    fn has_node_fast_path_answers_exactly_as_the_projection_it_skips() {
        let (core, isolation) = shared_graph();
        for agent_id in ["alice", "bob"] {
            let context = super::super::auth::VerifiedRequestContext::verified_for_test(agent_id);
            let authority = GraphReadAuthority::from_verified(&context, &isolation).unwrap();
            let projected = authority.project_core(&core);
            for node_id in [
                "alice-private",
                "bob-private",
                "public",
                "untagged",
                "does-not-exist",
            ] {
                assert_eq!(
                    authority.node_visible(&core, node_id),
                    projected.has_node(node_id),
                    "point-lookup fast path disagreed with the projection for \
                     actor={agent_id} node={node_id}: node_visible() is the \
                     short-circuit `HasNode`/`HasNodesBatch` take instead of \
                     building a filtered core, so any disagreement is an RLS \
                     divergence between the two read paths"
                );
            }
        }
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

    /// D-OP-1 / D-OB-20: `project_core` now caches its (expensive) materialization
    /// per (actor, `GraphCore::version()`). This is the direct proof that caching
    /// does not turn `project_core` stale: a mutation between two calls MUST be
    /// visible on the second call, not silently served from a version-0 cache entry.
    /// The version check is what makes this a correctness-preserving amortization
    /// rather than a relaxation of `project_core`'s guarantee (see that method's doc).
    #[test]
    fn project_core_reflects_a_mutation_after_the_previous_projection_was_cached() {
        let (core, isolation) = shared_graph();
        let alice_context = super::super::auth::VerifiedRequestContext::verified_for_test("alice");
        let alice = GraphReadAuthority::from_verified(&alice_context, &isolation).unwrap();

        // First call: populates the cache at the graph's current version. Alice's
        // "public" node is visible; a brand-new node does not exist yet.
        let first = alice.project_core(&core);
        assert!(first.has_node("public"));
        assert!(!first.has_node("alice-new-public-node"));

        // Mutate the SOURCE graph, then call again with the SAME actor. The raw
        // `GraphCore::add_node` primitive does NOT itself bump `version()` — in the
        // real server, `mark_dirty()` is called separately by the mutation commit
        // path (`src/server/mutation.rs::commit_finalize`) right after a successful
        // write, which is what actually advances the OCC version every committed
        // write goes through. Call it explicitly here to reproduce a real committed
        // write, not just a raw topology poke. A stale cache would still show the
        // old snapshot; the fix must miss on the version mismatch and rebuild.
        core.add_node(
            "alice-new-public-node".to_string(),
            properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
        );
        core.mark_dirty();
        let second = alice.project_core(&core);
        assert!(
            second.has_node("alice-new-public-node"),
            "a committed write between two project_core calls for the SAME actor \
             must be visible on the next call — the cache must invalidate on the \
             graph's version advancing, not serve a stale (actor, old-version) entry"
        );

        // And a THIRD call at the now-stable (post-mutation) version must hit the
        // freshly rebuilt cache entry rather than rebuilding yet again — same
        // observable content as `second`, proving the cache re-populated correctly
        // after the miss (not just that the miss itself produced a correct one-off).
        let third = alice.project_core(&core);
        assert!(third.has_node("alice-new-public-node"));
        assert_eq!(third.node_count(), second.node_count());
    }

    /// D-OP-1 / D-OB-20 benchmark harness: a READ MIX shaped like what a
    /// production grounding delegation actually issues — many sequential
    /// point-ish reads (`HasNode` + a `GetNodeProperties`-equivalent) by the SAME
    /// actor against a STABLE graph version (no writes in the burst, mirroring a
    /// read-only grounding phase), not one isolated call. This is what turns a
    /// per-call floor into the reported "grounding alone was 90.14s against a
    /// 10.0s production budget" — N reads each paying the full O(V) rebuild.
    /// Prints wall-clock numbers (`cargo test -- --nocapture`) so a before/after
    /// comparison does not depend on the pass/fail boundary alone.
    #[test]
    fn project_core_read_burst_mirrors_grounding_read_mix() {
        const NODE_COUNT: usize = 20_000;
        const EMBEDDING_DIM: usize = 1024;
        const BURST_READS: usize = 25;

        let core = Arc::new(GraphCore::new());
        {
            let mut semantic = core.semantic_store.write();
            for i in 0..NODE_COUNT {
                let id = format!("n{i}");
                core.add_node(
                    id.clone(),
                    properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
                );
                semantic
                    .add_embedding(id, vec![(i % 7) as f32 * 0.01; EMBEDDING_DIM])
                    .unwrap();
            }
        }

        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "grounding-service".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let context =
            super::super::auth::VerifiedRequestContext::verified_for_test("grounding-service");
        let authority = GraphReadAuthority::from_verified(&context, &isolation).unwrap();

        // Warm-up call: excludes one-time allocator/page-fault effects, matching a
        // live server's Nth request for this actor, not its 1st.
        let _ = authority.project_core(&core);

        let mut per_call = Vec::with_capacity(BURST_READS);
        let burst_started = std::time::Instant::now();
        for i in 0..BURST_READS {
            let call_started = std::time::Instant::now();
            let projected = authority.project_core(&core);
            let target = format!("n{}", i % NODE_COUNT);
            assert!(projected.has_node(&target));
            let _ = projected.get_node_properties(&target);
            per_call.push(call_started.elapsed());
        }
        let total = burst_started.elapsed();
        let avg = total / BURST_READS as u32;
        let max = per_call.iter().max().copied().unwrap_or_default();
        let min = per_call.iter().min().copied().unwrap_or_default();
        eprintln!(
            "D-OP-1 read-mix burst: reads={BURST_READS} total={total:?} avg={avg:?} \
             min={min:?} max={max:?} (node_count={NODE_COUNT}, embedding_dim={EMBEDDING_DIM})"
        );

        assert!(
            total < std::time::Duration::from_millis(500),
            "D-OP-1 read-mix burst of {BURST_READS} sequential reads by the SAME \
             actor at a STABLE graph version took {total:?} (avg {avg:?}/call, \
             max {max:?}) — budget 500ms total. This mirrors a grounding \
             delegation's read pattern; each call paying project_core's full \
             O(V) rebuild is exactly the reported 90.14s-against-10s-budget \
             production failure (D-OB-20)."
        );
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

    /// D-IDENT-AUDIT-1: `commit_cross_modal_txn`'s RBAC recheck (and every other
    /// `check_graph_access` call site) is keyed by `CarrierAuthority::agent_id` --
    /// the SAME identity space `IsolationLayer::register_agent` registers under.
    /// `actor_scope` is a DIFFERENT identity space (an opaque durable-ownership
    /// hash used for KV/coordinator namespacing, ownership stamping, and
    /// MutationBatch provenance fingerprints) and is never a registered RBAC
    /// identity. Passing it into an RBAC check does not error -- it silently
    /// denies, because the hash never matches any grant.
    ///
    /// This is the EXACT confusion that produced three real bugs in one day:
    /// `cbcabdf` (`commit_graph_methods`/`commit_txn_state` in
    /// `src/server/wire/mod.rs` broke every wire-native graph write --
    /// pgwire/MySQL/MSSQL -- with ACCESS_DENIED regardless of RBAC grants) and
    /// `6417f31` (`commit_graphql_cross_modal` in
    /// `src/server/handlers/txn.rs` broke every GraphQL cross-modal commit the
    /// same way). This test locks in the fix at the shared `check_graph_access`
    /// boundary all three call sites route through, independent of any one
    /// surface's own (heavier) integration fixtures. Reverting either fix --
    /// passing `authority.actor_scope()` instead of `authority.agent_id()` as
    /// `commit_cross_modal_txn`'s `caller` -- reproduces exactly the failure
    /// this test's second assertion checks for.
    #[test]
    fn rbac_check_is_keyed_by_agent_id_not_actor_scope() {
        let alice = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "alice", "tenant-a",
            ),
        )
        .unwrap();
        // `agent_id()` and `actor_scope()` must actually differ for this test to
        // mean anything -- `verified_for_test_in_tenant`'s principal is
        // `principal:alice` (not already an opaque `principal:sha256:...`
        // digest), so `CarrierAuthority::from_verified` re-hashes it into an
        // opaque `principal:sha256:...` scope, distinct from the raw "alice"
        // agent_id.
        assert_ne!(alice.agent_id(), alice.actor_scope());

        let mut isolation = IsolationLayer::new();
        // `AgentRole::System` grants full access to any REGISTERED identity with
        // no RBAC roles/grants needed -- isolates this test to the one thing it's
        // proving (which string is the lookup key), not RBAC policy evaluation
        // (covered elsewhere, e.g. `l10_broker_stream_write_tests`).
        isolation.register_agent(AgentIdentity {
            agent_id: alice.agent_id().to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });

        // The correct identity space: the registered agent_id resolves and is
        // allowed.
        assert!(check_graph_access(
            &isolation,
            Some(alice.agent_id()),
            "agent:alice",
            crate::protocol::GraphType::Agent,
            Some("agent:alice"),
            AccessLevel::Write,
        )
        .is_ok());

        // The confused identity space: actor_scope() was never registered, so the
        // lookup misses and the check fails CLOSED -- exactly the silent
        // ACCESS_DENIED the three fixed bugs produced for every caller regardless
        // of their real RBAC grants.
        let denied = check_graph_access(
            &isolation,
            Some(alice.actor_scope()),
            "agent:alice",
            crate::protocol::GraphType::Agent,
            Some("agent:alice"),
            AccessLevel::Write,
        );
        assert!(denied.is_err());
        assert!(denied.unwrap_err().contains("ACCESS_DENIED"));
    }

    /// A18: `unauthenticated_carrier_denied` used to be `{ true }` unconditionally
    /// — every caller, verified or not, was denied. It must now discriminate: a
    /// genuine `CarrierAuthority` allows, its absence denies. This is the direct,
    /// minimal proof of the fixed invariant; the per-surface integration proofs
    /// (S3 SigV4, KV-cache bearer/JWT) live alongside each surface's own tests.
    #[test]
    fn a18_carrier_authority_present_allows_absent_denies() {
        let carrier = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test("alice"),
        )
        .unwrap();
        assert!(
            !unauthenticated_carrier_denied(Some(&carrier)),
            "a real, verified carrier must be ALLOWED, not denied"
        );
        assert!(
            unauthenticated_carrier_denied(None),
            "no carrier at all must still be denied (fail-closed)"
        );
    }

    /// D-OP-1 (orchestrator lane, 2026-07-31): `project_core` materialises an
    /// entire second graph on EVERY call when RLS is active — sort every node id,
    /// clone every node's properties, sort every edge key, clone every edge's
    /// properties, and copy every visible embedding out of the semantic store.
    /// That is `O(V log V + E log E + V*d)` PER READ, independent of what was
    /// asked for, with no cache/memo/`OnceCell`/lazy path anywhere in this file.
    ///
    /// Live measurement (orchestrator, 2026-07-31, `__commons__`): 25,075 nodes,
    /// 1024-dim embeddings -> ~103 MB memcpy'd per call; `HasNode` (ONE hash
    /// lookup) measured 2.7-3.0s over dozens of live samples with ZERO under
    /// 1s, while `CypherQuery` (which keeps its snapshot-level filter instead of
    /// calling `project_core`) answered in <0.0001s on 31 samples.
    ///
    /// This reproduces that shape in-process at a smaller scale (large enough
    /// that the O(V) cost dominates any fixed per-call overhead, small enough to
    /// run in a normal `cargo test` pass) and asserts a budget for a single
    /// `has_node` point lookup THROUGH `project_core`, the exact shape
    /// `try_handle` pays on every terminal read handler
    /// (`src/server/handlers/graph_ops.rs`, `let core = read_authority
    /// .project_core(&core);` before the `match`).
    ///
    /// **Fixed**: `project_core` now caches the projected core per (actor,
    /// `GraphCore::version()`), invalidated when the version advances — see
    /// `docs/architecture/d-op-1-projection-cache.md` for the full design and
    /// `crate::rls_projection_cache` for the cache itself. This test's warm-up +
    /// measured-call shape (same actor, no write in between) is exactly a cache
    /// hit, so it now passes at microsecond cost. It is intentionally strict
    /// enough that an eager-rebuild regression cannot pass it by accident: the
    /// budget is generously above a real cached point-lookup's cost
    /// (microseconds) and generously below the O(V) rebuild's cost at this node
    /// count, so there is no ambiguous middle ground.
    #[test]
    fn project_core_of_a_single_has_node_call_must_not_rebuild_the_whole_graph() {
        const NODE_COUNT: usize = 20_000;
        const EMBEDDING_DIM: usize = 1024;
        const BUDGET: std::time::Duration = std::time::Duration::from_millis(50);

        let core = Arc::new(GraphCore::new());
        {
            let mut semantic = core.semantic_store.write();
            for i in 0..NODE_COUNT {
                let id = format!("n{i}");
                core.add_node(
                    id.clone(),
                    properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
                );
                // A real embedding-shaped vector, not a zero-cost stand-in — the
                // memcpy cost `project_core` pays is proportional to this, not to
                // a placeholder.
                semantic
                    .add_embedding(id, vec![(i % 7) as f32 * 0.01; EMBEDDING_DIM])
                    .unwrap();
            }
        }

        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "alice".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let alice_context = super::super::auth::VerifiedRequestContext::verified_for_test("alice");
        let alice = GraphReadAuthority::from_verified(&alice_context, &isolation).unwrap();

        // Warm-up call: excludes one-time allocator/page-fault effects so the
        // measured call is a fair repeat-call comparison, matching how a live
        // server serves this same actor's Nth request, not its 1st.
        let _ = alice.project_core(&core);

        let started = std::time::Instant::now();
        let projected = alice.project_core(&core);
        let elapsed = started.elapsed();
        assert!(
            projected.has_node("n0"),
            "sanity: the projection must be usable"
        );

        assert!(
            elapsed < BUDGET,
            "project_core() for a single point lookup took {elapsed:?} against a \
             {NODE_COUNT}-node / {EMBEDDING_DIM}-dim graph (budget {BUDGET:?}). \
             This is the D-OP-1 regression: every RLS-active read rebuilds the \
             entire visible graph (sort + clone every node/edge, copy every \
             embedding) instead of reusing a cached projection keyed on \
             (actor, graph version). Fix: cache in project_core() / \
             GraphReadAuthority, invalidated on GraphCore::version() advancing \
             (see docs/architecture/d-op-1-projection-cache.md)."
        );
    }

    /// D-OP-1 — the REMAINING structural cost after the (actor, version) memoization
    /// fix above: a cache MISS (the version advanced since the last projection for
    /// this actor — i.e. any write happened, even one unrelated node) still pays the
    /// full `O(V log V + E log E + V*d)` rebuild, because the fix is a memo on top of
    /// an eager full-materialization build, not an incremental/copy-on-write
    /// projection. This is NOT a regression to fix in this test — it is the
    /// documented remainder D-OP-1 is still open for (see `docs/architecture/
    /// d-op-1-projection-cache.md`, "remaining work"): closing it needs the
    /// projection itself to be maintained incrementally alongside writes (or lazily
    /// per-row rather than eagerly for the whole visible set), which is a genuine
    /// data-structure change to RLS-filtered read serving, not a caching layer, and
    /// needs a security review before landing (this crate enforces row-level
    /// isolation; an incremental-maintenance bug here is a data-leak bug). This test
    /// exists to keep a REAL, current number attached to that remaining gap instead
    /// of a stale one from a prior report.
    #[test]
    fn project_core_cache_miss_after_any_write_still_pays_the_full_rebuild_d_op_1() {
        const NODE_COUNT: usize = 20_000;
        const EMBEDDING_DIM: usize = 1024;

        let core = Arc::new(GraphCore::new());
        {
            let mut semantic = core.semantic_store.write();
            for i in 0..NODE_COUNT {
                let id = format!("n{i}");
                core.add_node(
                    id.clone(),
                    properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
                );
                semantic
                    .add_embedding(id, vec![(i % 7) as f32 * 0.01; EMBEDDING_DIM])
                    .unwrap();
            }
        }

        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "alice".to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let alice_context = super::super::auth::VerifiedRequestContext::verified_for_test("alice");
        let alice = GraphReadAuthority::from_verified(&alice_context, &isolation).unwrap();

        // Warm the cache for `alice` at the CURRENT version (a genuine hit baseline).
        let _ = alice.project_core(&core);
        let hit_started = std::time::Instant::now();
        let _ = alice.project_core(&core);
        let hit_elapsed = hit_started.elapsed();

        // ONE unrelated write bumps `GraphCore::version()`, invalidating every cached
        // projection for every actor — this is the realistic shape of a live server
        // under any write traffic at all, not a contrived worst case.
        core.add_node(
            "unrelated-write".to_string(),
            properties(&[("_owner_id", "system-writer"), ("type", "Thing")]),
        );

        let miss_started = std::time::Instant::now();
        let projected = alice.project_core(&core);
        let miss_elapsed = miss_started.elapsed();
        assert!(
            projected.has_node("n0"),
            "sanity: the post-write projection must still be usable"
        );

        println!(
            "[D-OP-1] {NODE_COUNT}-node / {EMBEDDING_DIM}-dim graph: cache-HIT project_core() \
             = {hit_elapsed:?}; cache-MISS (after one unrelated write) project_core() = \
             {miss_elapsed:?} — {:.0}x slower than the hit path, still O(V) per miss.",
            miss_elapsed.as_secs_f64() / hit_elapsed.as_secs_f64().max(1e-9)
        );

        // Deliberately NOT a pass/fail budget assertion (unlike the hit-path test
        // above): there is no fix landed yet for this path, so a strict budget here
        // would just be a permanently-red or permanently-loosened gate. The point of
        // this test is the printed measurement, kept current on every run.
    }
}

// ── L-RLS-1 (next-level-analysis report §9 item #10): read-method RLS coverage ──

/// Every non-mutating (`mutates == false`) protocol method falls into EXACTLY
/// one documented bucket below — there is no silent fourth category. Mirrors
/// `server::mutation::tests::gateway_routed_set_matches_mutating_policy_surface`'s
/// exhaustive-partition idiom (mutation side) on the READ side.
///
/// **[`RLS_ROUTED`]** — the method's serving handler filters its `GraphView`/
/// `GraphCore` through [`GraphReadAuthority::filter_view`]/
/// [`GraphReadAuthority::project_core`] (which call
/// `crate::isolation::IsolationLayer::can_see_row`,
/// `crates/eg-core/src/isolation.rs`) before any row reaches the caller. Every
/// entry was confirmed by reading its owning handler's actual `project_core`/
/// `filter_view` call site, not assumed from the method's name — see
/// `every_read_method_routes_through_rls_or_is_a_documented_exception`'s own
/// test module for the file:line evidence recorded per group.
///
/// **[`NON_ROW_SCOPED`]** — the method never constructs a `GraphView`/
/// `GraphCore` at all: either it is pure compute over caller-supplied data
/// with no server-held row (finance/datascience math, a sandboxed WASM UDF),
/// or it reads a DIFFERENT store whose authority is a verified tenant/actor/
/// owner/membership scope rather than per-node `_owner`/`_visibility`/
/// `_grants` — each entry names the real mechanism and why RLS's public/
/// grants sharing model does not apply to that store's data shape.
/// `TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill`
/// (`src/server/handlers/timeseries.rs`) are the exact gap the next-level-
/// analysis report §9 item #10 named; see that file's module doc for the full
/// reasoning [`REASON_TENANT_KEY_SCOPED_SERIES`] cites.
///
/// **[`NOT_YET_AUDITED`]** — this pass did not trace the method's handler to
/// a `project_core`/`filter_view` call with enough confidence to assert
/// either of the above buckets. Deliberately distinct from both: a flagged,
/// visible TODO for a human/future pass, never a silent assumption of safety
/// in either direction.
///
/// Ratchet, mirroring the mutation-side gate: `UNDOCUMENTED` (a method whose
/// name matches none of the three lists) is the only HARD test failure below.
/// Shrinking [`NOT_YET_AUDITED`] to zero — by moving an entry into
/// `RLS_ROUTED` or `NON_ROW_SCOPED` backed by a real, cited call site — is the
/// intended burn-down, exactly like `OPEN_NOT_JUSTIFIED` on the mutation side.
///
/// **Blind spot this method-level ratchet cannot see, and where its coverage
/// continues (W1b / L-RLS-2):** `AnalyticsJob` never appears in `RLS_ROUTED`/
/// `NON_ROW_SCOPED`/`NOT_YET_AUDITED` below at all, because
/// `eg_capabilities`'s static policy table classifies the WHOLE method
/// `mutates: true` (a conservative upper bound — one wire method carries
/// `Submit`/`Status`/`Cancel`/`Resume` `op`s of different shapes, and `Status`
/// is genuinely a read), so it never reaches this file's `!p.mutates` read
/// partition. The actual per-request row-read question for `AnalyticsJob`
/// isn't "does this method read a row" (bundled with three writes, it always
/// says yes) but "does THIS SUBMISSION's `JobKind` read a graph row
/// server-side" — a question this method-granularity table cannot express.
/// That finer axis is ratcheted separately, at job-KIND granularity, by
/// `handlers::jobs::reads_graph_rows_server_side` (an exhaustive, no-wildcard
/// match a new `JobKind` variant cannot silently skip) plus the architecture
/// test in `handlers::jobs_read_rls_architecture` (declared from
/// `handlers::jobs`'s own test module), which independently cross-checks that
/// classification and textually pins `handle_submit`'s fail-closed ordering.
/// Both ratchets currently agree: zero shipped read surfaces of any kind
/// bypass RLS without a documented, checked reason.
const RLS_ROUTED: &[&str] = &[
    "BatchL2Normalize",
    "BestTrajectory",
    "BetweennessCentrality",
    "CdcRead",
    "CommunityDetectEphemeral",
    "CommunityDetection",
    "ComputeSimilarityEdges",
    "ConnectedComponents",
    "DegreeCentrality",
    "DegreeCentralityAll",
    "DiffAgainst",
    "DiscountedReturn",
    "Discover",
    "DistributedCompute",
    "EdgeCount",
    "EpistemicStatus",
    "ExplainBelief",
    "ExplainEvidence",
    "ExplainPlan",
    "ExplainPolicy",
    "ExplainProvenance",
    "ExplainProvenanceByIds",
    "FindCycle",
    "FiredTriggers",
    "Fork",
    "GetBlastRadius",
    "GetContextView",
    "GetEdgeProperties",
    "GetEdgePropertiesBatch",
    "GetEdges",
    "GetEdgesPage",
    "GetMatView",
    "GetNeighbors",
    "GetNeighborsBatch",
    "GetNodeProperties",
    "GetNodePropertiesBatch",
    "GetNodes",
    "GetNodesByLabel",
    "GetPredecessors",
    "GetRdf",
    "GetShortestPath",
    "GetSubgraph",
    "GetSuccessors",
    "GraphColoring",
    "HasEdge",
    "HasNode",
    "HasNodesBatch",
    "InDegree",
    "IndexRepository",
    "KnowledgeStream",
    "ListGraphs",
    "ListTriggers",
    "MatchOntologyTerms",
    "MaterializationStatus",
    "MineClassifyFit",
    // t1-grounding-0802 follow-up audit: `handlers::pipeline::try_handle` (module doc,
    // `src/server/handlers/pipeline.rs`) receives the SAME `core.clone()` the mining/
    // graphlearn handlers ahead of it in dispatch.rs's `'dispatch` block do -- the
    // already-`project_core`-projected core built once at the top of
    // `graph_ops::try_handle`. `Evaluate`/`Compare`'s feature-extraction steps read
    // the live subgraph through that core; `Train`/`Serve`/`Predict` are separately
    // GATEWAY_ROUTED writes.
    "MiningPipelineCompare",
    "MiningPipelineEvaluate",
    "MinimumSpanningTree",
    "NlQuery",
    "NodeCount",
    "NodeIds",
    "ObserveScreen",
    "OutDegree",
    "OwlExplain",
    "OwlReason",
    "OwlReasonDistributed",
    "PageRank",
    "ParseFile",
    "ParseFiles",
    "PersonalizedPageRank",
    "PlanMatViewGet",
    "ReadContinuousQuery",
    "ResolveCandidates",
    "ResolveConflict",
    "RunRules",
    "SceneChildren",
    "SemanticSearch",
    "ShaclValidate",
    "ShexValidate",
    "Sparql",
    "SparqlVirtual",
    "StaleMaterializations",
    "StreamCommittedOffset",
    "StreamRead",
    "StronglyConnectedComponents",
    "SummariesAtLevel",
    "SummaryChildren",
    "ToMsgpack",
    "TopologicalSort",
    "TxnUnifiedQuery",
    "TxnUnifiedQueryText",
    "UnifiedQuery",
    "UnifiedQueryText",
    "UnionGetNeighbors",
    "UnionGetNodeProperties",
    "UnionGetNodesByLabel",
    "Vf2SubgraphMatch",
    "Watch",
    "WhatChanged",
    "WorldTransform",
];

const REASON_SERVER_LIFECYCLE: &str =
    "server-lifecycle / liveness methods touch no tenant-owned row";
const REASON_AUDIT_CHAIN_ADMIN_GATED: &str =
    "AuditVerify/AuditProveInclusion walk the hash-chained audit log (incl. its provenance-anchor entries) under the kg:admin capability gate -- not a graph row read";
// BUG A1 (2026-08-12): GetLedger was previously (wrongly) RLS_ROUTED. Its handler
// (`handlers::graph_ops::try_handle`, `Method::GetLedger` arm) reads the mutation
// ledger off `raw_core` (captured before `read_authority.project_core` shadows
// `core`), the SAME raw-core pattern `raw_ledger_len` uses just above in that file
// for `Metrics.total_mutations` -- the ledger is process-observability (an
// audit/debug trail of every committed mutation string), never row-visible node/edge
// data, so RLS's per-row `_owner`/`_visibility`/`_grants` model does not apply to it
// at all. It is instead authorized by its own dedicated `ledger:read` RBAC action
// (`eg_capabilities::policy`), enforced in `dispatch.rs` (`verified_context.
// allows_method`) BEFORE any handler runs -- routing it through row-level
// `project_core` as well was a redundant SECOND gate that, instead of narrowing
// visibility, silently destroyed the data: `project_core`'s detached copy is built
// via `add_node_no_ledger`/`add_edge_no_ledger` and therefore never carries a ledger
// at all (see `build_projection`'s own doc), so GetLedger returned `[]` on every
// request in the default (`security`-compiled) build regardless of how many
// mutations had committed.
const REASON_LEDGER_ADMIN_OBSERVABILITY: &str =
    "src/server/handlers/graph_ops.rs's GetLedger arm reads raw_core.get_ledger() (captured before project_core shadows core, same pattern as raw_ledger_len for Metrics.total_mutations) -- the mutation ledger is process-observability, not row-visible data, and is gated by its own ledger:read RBAC authz_action (eg_capabilities::policy) enforced in dispatch.rs before any handler runs, not by per-row RLS";
const REASON_VERIFIED_TENANT_CLAIM: &str =
    "dispatch.rs compares the request's tenant against verified_context.claims().tenant before serving it (GetChangeEnvelope/GetContentVersion/GetChangeCursor) -- an explicit verified-tenant-claim check, not a graph row";
const REASON_NATIVE_CAPACITY_TENANT_GATED: &str =
    "CapacityStatus/ReconcileCapacity page native capacity CELLS and LEASES out of redb (redb_store::capacity_lease::read) -- control-plane rows keyed by (graph, cell/lease id), never a GraphView node/edge row, so there is no row for RLS to filter. dispatch.rs compares the request's tenant against verified_context.tenant() and requires kg:admin or capacity:read:aggregate to cross it, on top of the capacity:read method gate in allows_method";
const REASON_CLUSTER_ADMIN_GATED: &str =
    "cluster-wide / control-plane methods gated by the kg:admin capability (IsolationLayer::require_admin_capability) -- span the whole registry/cluster, not one resolved graph's rows, mirroring mutation.rs's own NON_GATEWAY_COORDINATED classification of the equivalent admin writes";
const REASON_CHANNEL_MEMBERSHIP: &str =
    "dispatch.rs checks ServerState::channels.authorize_member(channel_id, tenant_scope, agent_id) before returning channel messages/roster -- channel membership is the authority, not graph row visibility";
const REASON_PURE_COMPUTE: &str =
    "pure compute over caller-supplied arrays/parameters (finance/datascience primitives) -- no server-held graph row is read";
const REASON_ASR_PURE_COMPUTE: &str =
    "src/server/handlers/asr.rs::handle/transcribe_file only verifies a caller-resolved model artifact and transcribes caller-supplied audio -- it never reads GraphCore or a tenant-owned row, so cross-tenant row RLS is not applicable";
const REASON_QUANTUM_PURE_COMPUTE: &str =
    "src/server/handlers/quantum.rs::handle builds and executes a bounded circuit from caller-supplied candidates/programs -- it never reads GraphCore or a tenant-owned row, so ranking is not a graph-row read and cannot bypass RLS";
const REASON_TTS_PURE_COMPUTE: &str =
    "src/server/handlers/tts.rs::synthesize validates and authorizes the caller-supplied carrier/input, then runs bounded Piper inference inline -- it never reads GraphCore or a tenant-owned row, so no graph-row RLS projection is required";
const REASON_VIZ_CARRIER_SCOPED: &str =
    "src/server/handlers/viz.rs never reads GraphCore: its persistent ColumnStore/provenance side-store is an owner-scoped non-row surface, and verified CarrierAuthority namespaces dataset handles plus result references before lookup, cache, ingestion, or shaping";
const REASON_SANDBOXED_UDF: &str =
    "wasm_udf.rs's RunUdf executes a sandboxed WASM module over an opaque caller-supplied byte payload only -- no graph state is read";
const REASON_TENANT_KEY_SCOPED_SERIES: &str =
    "src/server/handlers/timeseries.rs::scoped_key derives the SeriesKey from CarrierAuthority (verified tenant+actor) -- a series is namespaced under the CALLING actor's own scope, so cross-actor addressing is structurally impossible (never constructs a GraphView). Stronger than default-deny in one respect (no address collision possible at all), but does not support _visibility:public/_grants sharing -- the exact gap the next-level-analysis report section 9 item #10 named; see that file's module doc for the full reasoning";
const REASON_BLOB_OWNER_SCOPED: &str =
    "src/server/handlers/blob.rs checks authority.owner_scope() against the stored manifest/cursor owner_scope on every fetch (ensure_blob_owner/authorize_fetch); content is addressed by digest, not by a graph row";
const REASON_TENANT_KEY_SCOPED_KV: &str =
    "src/server/kv.rs namespaces every key via CarrierAuthority::namespace(\"kv-namespace\", ...) -- the identical tenant+actor-scoped-key pattern as timeseries; never constructs a GraphView";
const REASON_ADMIN_ONLY_CEP: &str =
    "src/server/cep.rs gates CEP subscription management, including polling one, behind CarrierAuthority::require_admin -- stricter than row RLS, not row-scoped at all";
// L-RLS-1 follow-up audit (CONCEPT:EPI-P3-3/P3-6): the 5 formerly-`NOT_YET_AUDITED` epistemic
// read methods were traced to their handlers in `src/server/handlers/query.rs`. Two
// (`ResolveConflict`/`ExplainEvidence`) DO build a `BeliefGraph` off `core.analysis_snapshot()`
// and are now `RLS_ROUTED` above (their handler arms were fixed to `rls.filter_view` the
// snapshot first -- see those arms' own doc comments). The other three below never touch
// `core`/`state`/`GraphView` at all.
const REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE: &str =
    "src/server/handlers/query.rs's causal_estimate_wire/causal_counterfactual_wire/rank_by_provenance_wire (Method::CausalEstimate/CausalCounterfactual/RankByProvenance) build an ephemeral eg_epistemic::CausalGraph, or rank eg_epistemic::RetrievalCandidates, purely from the REQUEST's own variables/do_values/actual/candidates/weights fields -- these three handler arms never call core.analysis_snapshot() or reference state/core/GraphView at all (confirmed by reading each arm and its wire fn body), so there is no tenant-owned graph row in play to RLS-scope; the same posture as REASON_PURE_COMPUTE's finance/datascience primitives, just epistemic-causal's own request-scoped SCM/ranking inputs instead";
// t1-grounding-0802 follow-up audit: `ClusterMembers` was added (ADR-1/W1.1
// engine-authoritative cluster-topology discovery) without a classification here.
// Auth-unification wave 1 (Workstream C): `GetIdentity` is the identity READ-BACK that
// closes `RegisterIdentity`'s blind-upsert gap -- `RegisterIdentity` REPLACES a principal's
// whole role set, so a merge-before-register caller must be able to read the current set
// first (the production graph-os scheduler logged `existing_roles is unknown (None)` at every
// boot precisely because no such RPC existed).
const REASON_IDENTITY_STORE_ADMIN_GATED: &str =
    "src/server/dispatch.rs's GetIdentity arm answers from IsolationLayer::get_identity -- a point read of the in-memory RBAC identity map (`agents: HashMap<String, AgentIdentity>`, crates/eg-core/src/isolation.rs), the SAME store RegisterIdentity writes and RbacAdmin governs. It is control-plane principal metadata (agent_id/role/teams/roles), not one resolved graph's rows: it never constructs a GraphView/GraphCore and never calls project_core/filter_view, so there is no per-node `_owner`/`_visibility`/`_grants` for RLS to filter on. Authority is the same `security:admin` authz_action the rest of the Zero-Trust Consensus family carries (crates/eg-capabilities/src/lib.rs), so it grants no visibility to any caller who could not already call RegisterIdentity/RbacAdmin against that identical store";
const REASON_CLUSTER_TOPOLOGY_READ: &str =
    "src/server/handlers/topology.rs::handle_cluster_members answers from the durable NodeInfoStore + live MultiRaft membership (self-reported node/raft-group topology) -- not one resolved graph's rows, never touches core/GraphView/project_core. Gated by its own authz_action `cluster:topology-read` (crates/eg-capabilities/src/lib.rs), deliberately NOT kg:admin (REASON_CLUSTER_ADMIN_GATED) so ordinary service roles can re-resolve after a failover -- a distinct, weaker gate than the admin-only cluster methods above, so it gets its own reason rather than being folded into theirs";
// BUG-044-class facade audit (push/eg-merge-artifacts): `QueryWorkItemReservation`/
// `ResourceReservationStatus` route in `dispatch.rs` (the `is_resource_reservation_query_method`
// guard, just above the WorkItem claim-capability block) straight to
// `PersistenceBackend::read_resource_reservation`/`read_resource_reservation_status` against the
// dedicated native reservation ledger -- never a `GraphView`/`core.analysis_snapshot()`. Under
// placement it is served only by the current group leader (the SAME `routed_raft`/linearizable
// read-barrier gate `REASON_CLUSTER_TOPOLOGY_READ`'s neighbor uses); `ResourceReservationStatus`
// additionally redacts aggregate host totals unless the caller holds `resource:read:aggregate`/
// `kg:admin` (see `redact_resource_status_result`) -- native reservation-scoped authority, not
// per-row RLS.
const REASON_NATIVE_RESERVATION_READ: &str =
    "dispatch.rs's is_resource_reservation_query_method guard routes QueryWorkItemReservation/ResourceReservationStatus directly to PersistenceBackend::read_resource_reservation/read_resource_reservation_status against the dedicated native reservation ledger (redb_store.rs), gated by the current placement leader + linearizable read barrier under raft -- never a GraphView/core.analysis_snapshot() row read; ResourceReservationStatus additionally redacts aggregate totals unless the caller holds resource:read:aggregate/kg:admin";
// `VerifyWorkItemClaimCapability` shares `MintWorkItemClaimCapability`'s dedicated block in
// dispatch.rs ("Native WorkItem claim capabilities use a dedicated private ledger and never
// enter MutationBatch/result/outbox/CDC projections"): it reads
// `redb_store::work_item_capability::verify_work_item_claim_capability` keyed by an
// `AuthenticatedAuthority` built from the verified request context (tenant/principal/session),
// never a `GraphView`.
const REASON_NATIVE_CAPABILITY_LEDGER: &str =
    "dispatch.rs's dedicated WorkItem-claim-capability block routes VerifyWorkItemClaimCapability to redb_store::work_item_capability::verify_work_item_claim_capability against its own private ledger, keyed by an AuthenticatedAuthority derived from the verified request context -- never enters MutationBatch/result/outbox/CDC projections or a GraphView, same posture as MintWorkItemClaimCapability's write-side native ledger";
// RMDD-28 dispatch wiring (fix/eg-devlane-dispatch): `DevelopmentLaneStatus`/
// `QueryDevelopmentLane` now route through dispatch.rs's dedicated development-lane block
// (`is_development_lane_method`, sitting beside the WorkItem-claim-capability block above) to
// `PersistenceBackend::read_development_lane[_status]`, which call
// `redb_store::development_lane::read_development_lane[_status]` directly against the native
// `development_lane_*` redb tables -- never a `GraphView`/`core.analysis_snapshot()`. Gated by
// the current placement leader under raft, same posture as `REASON_NATIVE_RESERVATION_READ`.
// The kernel's own `public_hold` projection (not a dispatch-layer redaction step) already
// redacts `worktree_locator`/`host_ref`/`host_target_alias` on every row returned, so no
// additional per-caller redaction is needed here the way `ResourceReservationStatus` needs one.
const REASON_NATIVE_DEVELOPMENT_LANE_READ: &str =
    "dispatch.rs's is_development_lane_method block routes DevelopmentLaneStatus/QueryDevelopmentLane directly to PersistenceBackend::read_development_lane/read_development_lane_status, which call redb_store::development_lane::read_development_lane/read_development_lane_status against the native development_lane_* redb tables (redb_store/development_lane.rs), gated by the current placement leader under raft -- never a GraphView/core.analysis_snapshot() row read; the kernel's own public_hold projection already redacts worktree_locator/host_ref/host_target_alias on every row, so no dispatch-layer redaction is needed";

// CA-16 (DEC-CA-04): the policy-bundle export reads the caller-supplied marking/
// principal input plus IsolationLayer's own configuration surface (never one
// resolved graph's `_owner`/`_visibility`/`_grants` rows) and returns a policy
// DESCRIPTION, not row data -- so per-row RLS categorically does not apply to its
// own response. Gated by its own `policy:export` authz_action (`eg_capabilities::
// policy`), deliberately NOT `security:admin` -- see that policy table entry's own
// doc for why (avoiding the `rbac.redb`/M7 coupling DEC-CA-04 A2 flags).
const REASON_POLICY_EXPORT_ADMIN_SCOPED: &str =
    "src/server/policy_export/mod.rs's generate_bundle reads caller-supplied marking-registry/principal input plus IsolationLayer's own row-visibility predicate SHAPE (never one resolved graph's rows) and returns a policy description -- never constructs a GraphView/GraphCore, so per-row RLS is not applicable. Gated by its own policy:export authz_action (kg:admin also clears it via allows_method's unconditional fallback), deliberately NOT security:admin/an admin:-prefixed action -- see eg_capabilities::policy's Method::PolicyExport entry for why that would have wrongly coupled this to rbac.redb/M7 instead of the effective request-time role set (DEC-CA-04 A2)";

const NON_ROW_SCOPED: &[(&str, &str)] = &[
    // REASON_POLICY_EXPORT_ADMIN_SCOPED
    #[cfg(feature = "policy_export")]
    ("PolicyExport", REASON_POLICY_EXPORT_ADMIN_SCOPED),
    // REASON_SERVER_LIFECYCLE
    ("CancelRequest", REASON_SERVER_LIFECYCLE),
    ("Health", REASON_SERVER_LIFECYCLE),
    ("Metrics", REASON_SERVER_LIFECYCLE),
    ("Ping", REASON_SERVER_LIFECYCLE),
    ("ResourceStats", REASON_SERVER_LIFECYCLE),
    ("ResourceStatsPage", REASON_SERVER_LIFECYCLE),
    // REASON_AUDIT_CHAIN_ADMIN_GATED
    ("AuditVerify", REASON_AUDIT_CHAIN_ADMIN_GATED),
    ("AuditProveInclusion", REASON_AUDIT_CHAIN_ADMIN_GATED),
    // REASON_LEDGER_ADMIN_OBSERVABILITY
    ("GetLedger", REASON_LEDGER_ADMIN_OBSERVABILITY),
    // REASON_VERIFIED_TENANT_CLAIM
    ("GetChangeCursor", REASON_VERIFIED_TENANT_CLAIM),
    ("GetChangeEnvelope", REASON_VERIFIED_TENANT_CLAIM),
    ("GetContentVersion", REASON_VERIFIED_TENANT_CLAIM),
    // REASON_CLUSTER_ADMIN_GATED
    ("Backup", REASON_CLUSTER_ADMIN_GATED),
    ("CatalogList", REASON_CLUSTER_ADMIN_GATED),
    ("ExportSqliteFile", REASON_CLUSTER_ADMIN_GATED),
    ("PlacementRoute", REASON_CLUSTER_ADMIN_GATED),
    ("RebalancePlan", REASON_CLUSTER_ADMIN_GATED),
    // REASON_NATIVE_CAPACITY_TENANT_GATED
    ("CapacityStatus", REASON_NATIVE_CAPACITY_TENANT_GATED),
    ("ReconcileCapacity", REASON_NATIVE_CAPACITY_TENANT_GATED),
    // REASON_CLUSTER_TOPOLOGY_READ
    ("ClusterMembers", REASON_CLUSTER_TOPOLOGY_READ),
    ("GetIdentity", REASON_IDENTITY_STORE_ADMIN_GATED),
    // REASON_CHANNEL_MEMBERSHIP
    ("GetChannelMembers", REASON_CHANNEL_MEMBERSHIP),
    ("GetChannelMessages", REASON_CHANNEL_MEMBERSHIP),
    ("ListChannels", REASON_CHANNEL_MEMBERSHIP),
    // REASON_PURE_COMPUTE
    ("Asr", REASON_ASR_PURE_COMPUTE),
    ("Quantum", REASON_QUANTUM_PURE_COMPUTE),
    ("TtsSynthesize", REASON_TTS_PURE_COMPUTE),
    ("DsAdamStep", REASON_PURE_COMPUTE),
    ("DsComputeStats", REASON_PURE_COMPUTE),
    ("DsCrossEntropy", REASON_PURE_COMPUTE),
    ("DsDpoLoss", REASON_PURE_COMPUTE),
    ("DsFitEstimator", REASON_PURE_COMPUTE),
    ("DsGrpoSurrogate", REASON_PURE_COMPUTE),
    ("DsKMeans", REASON_PURE_COMPUTE),
    ("DsKlDivergence", REASON_PURE_COMPUTE),
    ("DsLinearRegression", REASON_PURE_COMPUTE),
    ("DsLogSoftmax", REASON_PURE_COMPUTE),
    ("DsPca", REASON_PURE_COMPUTE),
    ("DsPredictEstimator", REASON_PURE_COMPUTE),
    ("DsSgdStep", REASON_PURE_COMPUTE),
    ("DsSoftmax", REASON_PURE_COMPUTE),
    ("DsTrainTestSplit", REASON_PURE_COMPUTE),
    ("FinanceAdfTest", REASON_PURE_COMPUTE),
    ("FinanceAlphaCombinationEngine", REASON_PURE_COMPUTE),
    ("FinanceAvellanedaStoikov", REASON_PURE_COMPUTE),
    ("FinanceBayesianKelly", REASON_PURE_COMPUTE),
    ("FinanceBlackLitterman", REASON_PURE_COMPUTE),
    ("FinanceBreakevenAlpha", REASON_PURE_COMPUTE),
    ("FinanceBrierScore", REASON_PURE_COMPUTE),
    ("FinanceCombineAlphas", REASON_PURE_COMPUTE),
    ("FinanceConvergenceGate", REASON_PURE_COMPUTE),
    ("FinanceCrossSectionalRank", REASON_PURE_COMPUTE),
    ("FinanceCvar", REASON_PURE_COMPUTE),
    ("FinanceDeflatedSharpe", REASON_PURE_COMPUTE),
    ("FinanceDetectRegimes", REASON_PURE_COMPUTE),
    ("FinanceDieboldMariano", REASON_PURE_COMPUTE),
    ("FinanceDownsideDeviation", REASON_PURE_COMPUTE),
    ("FinanceDrawdownSeries", REASON_PURE_COMPUTE),
    ("FinanceEffectiveIndependentN", REASON_PURE_COMPUTE),
    ("FinanceEfficientFrontier", REASON_PURE_COMPUTE),
    ("FinanceEmpiricalKelly", REASON_PURE_COMPUTE),
    ("FinanceEwma", REASON_PURE_COMPUTE),
    ("FinanceExpectedPnlRate", REASON_PURE_COMPUTE),
    ("FinanceForensicReport", REASON_PURE_COMPUTE),
    ("FinanceGlostenMilgromSpread", REASON_PURE_COMPUTE),
    ("FinanceGltQuotes", REASON_PURE_COMPUTE),
    ("FinanceHardimanBouchaud", REASON_PURE_COMPUTE),
    ("FinanceHawkesMle", REASON_PURE_COMPUTE),
    ("FinanceInformationCoefficient", REASON_PURE_COMPUTE),
    ("FinanceInformationRatio", REASON_PURE_COMPUTE),
    ("FinanceKalmanBeta", REASON_PURE_COMPUTE),
    ("FinanceKalmanFilter1d", REASON_PURE_COMPUTE),
    ("FinanceKalmanVolatility", REASON_PURE_COMPUTE),
    ("FinanceKellyFraction", REASON_PURE_COMPUTE),
    ("FinanceKyleLambda", REASON_PURE_COMPUTE),
    ("FinanceLogitQuotes", REASON_PURE_COMPUTE),
    ("FinanceMarketImpact", REASON_PURE_COMPUTE),
    ("FinanceMarkovTransitionMatrix", REASON_PURE_COMPUTE),
    ("FinanceMatchOrders", REASON_PURE_COMPUTE),
    ("FinanceMaxDrawdown", REASON_PURE_COMPUTE),
    ("FinanceMeanReversion", REASON_PURE_COMPUTE),
    ("FinanceMicropriceSeries", REASON_PURE_COMPUTE),
    ("FinanceMomentum", REASON_PURE_COMPUTE),
    ("FinanceMonteCarloVar", REASON_PURE_COMPUTE),
    ("FinanceOfiSeries", REASON_PURE_COMPUTE),
    ("FinanceOptimizePortfolio", REASON_PURE_COMPUTE),
    ("FinanceOrderBookImbalance", REASON_PURE_COMPUTE),
    ("FinanceOuCalibrate", REASON_PURE_COMPUTE),
    ("FinanceOuOptimalThresholds", REASON_PURE_COMPUTE),
    ("FinancePairsTrading", REASON_PURE_COMPUTE),
    ("FinancePosteriorCredibleInterval", REASON_PURE_COMPUTE),
    ("FinanceProbabilityBacktestOverfit", REASON_PURE_COMPUTE),
    ("FinancePurgedCpcv", REASON_PURE_COMPUTE),
    ("FinanceQueueImbalance", REASON_PURE_COMPUTE),
    ("FinanceRealizedVolTick", REASON_PURE_COMPUTE),
    ("FinanceRiskMetrics", REASON_PURE_COMPUTE),
    ("FinanceRiskParity", REASON_PURE_COMPUTE),
    ("FinanceRollingZscore", REASON_PURE_COMPUTE),
    ("FinanceSabrCalibrate", REASON_PURE_COMPUTE),
    ("FinanceSabrImpliedVol", REASON_PURE_COMPUTE),
    ("FinanceSabrSmile", REASON_PURE_COMPUTE),
    ("FinanceSignalDecay", REASON_PURE_COMPUTE),
    ("FinanceSpreadReversion", REASON_PURE_COMPUTE),
    ("FinanceStressTest", REASON_PURE_COMPUTE),
    ("FinanceSurveillanceRisk", REASON_PURE_COMPUTE),
    ("FinanceTwap", REASON_PURE_COMPUTE),
    ("FinanceVar", REASON_PURE_COMPUTE),
    ("FinanceVpinPm", REASON_PURE_COMPUTE),
    ("FinanceVwap", REASON_PURE_COMPUTE),
    // REASON_SANDBOXED_UDF
    ("RunUdf", REASON_SANDBOXED_UDF),
    // REASON_VIZ_CARRIER_SCOPED
    ("Viz", REASON_VIZ_CARRIER_SCOPED),
    // REASON_TENANT_KEY_SCOPED_SERIES
    ("TsAsofJoin", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsGapFill", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsListSeries", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsRange", REASON_TENANT_KEY_SCOPED_SERIES),
    ("TsWindow", REASON_TENANT_KEY_SCOPED_SERIES),
    // REASON_BLOB_OWNER_SCOPED
    ("BlobChunkGet", REASON_BLOB_OWNER_SCOPED),
    ("BlobFetchBegin", REASON_BLOB_OWNER_SCOPED),
    ("BlobFetchEnd", REASON_BLOB_OWNER_SCOPED),
    // REASON_TENANT_KEY_SCOPED_KV
    ("KvGet", REASON_TENANT_KEY_SCOPED_KV),
    ("KvScan", REASON_TENANT_KEY_SCOPED_KV),
    // REASON_ADMIN_ONLY_CEP
    ("CepPoll", REASON_ADMIN_ONLY_CEP),
    // REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE
    ("CausalCounterfactual", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    ("CausalEstimate", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    ("RankByProvenance", REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
    // REASON_NATIVE_RESERVATION_READ
    ("QueryWorkItemReservation", REASON_NATIVE_RESERVATION_READ),
    ("ResourceReservationStatus", REASON_NATIVE_RESERVATION_READ),
    // REASON_NATIVE_CAPABILITY_LEDGER
    (
        "VerifyWorkItemClaimCapability",
        REASON_NATIVE_CAPABILITY_LEDGER,
    ),
    // REASON_NATIVE_DEVELOPMENT_LANE_READ
    ("DevelopmentLaneStatus", REASON_NATIVE_DEVELOPMENT_LANE_READ),
    ("QueryDevelopmentLane", REASON_NATIVE_DEVELOPMENT_LANE_READ),
];

// L-RLS-1 burn-down (CONCEPT:EPI-P3-3/P3-6): the 5 methods this pass covered (see the
// `REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE` doc above) were the entire prior contents of this
// list. Empty is the intended end state, not an initial one -- a future audit pass adds a
// new entry here ONLY as a flagged, visible TODO, never silently.
//
// fix/eg-devlane-dispatch: `DevelopmentLaneStatus`/`QueryDevelopmentLane` were parked here as
// NOT_YET_AUDITED by the push/eg-merge-artifacts facade audit (commit 174c381) because they had
// no dispatch.rs routing arm at all. They are now genuinely dispatch-routed (see
// `REASON_NATIVE_DEVELOPMENT_LANE_READ` in `NON_ROW_SCOPED` above), so this list returns to its
// intended empty end state.
const NOT_YET_AUDITED: &[(&str, &str)] = &[
    // VIZ-1 hierarchical clustering (merged in 6fc8c552). These read the
    // non-authoritative `cluster_hierarchy_store`, not graph rows, so they do not
    // construct a GraphView -- which argues for NON_ROW_SCOPED. But cluster
    // MEMBERSHIP is derived from graph structure, so a cluster listing can reveal
    // that two nodes are connected without the caller being able to read either
    // node. Whether that is a disclosure worth gating has NOT been traced end to
    // end, so these are parked here deliberately rather than asserted safe.
    // Burn down by tracing the handler to a project_core/filter_view call and
    // moving them to RLS_ROUTED, or by justifying NON_ROW_SCOPED with a cited
    // call site.
    // Declared mutates:false because it writes only the derived cache, never
    // graph rows -- but it READS the entire graph to compute the clustering,
    // which is the widest read in this group. Same untraced question, higher
    // stakes: park it, do not assume it safe.
    ("ClusterHierarchyRefresh", "VIZ-1: recomputes the hierarchy by reading the whole graph; writes only the derived cluster_hierarchy store. Widest read of the three; disclosure question untraced"),
    ("ClusterHierarchyClusters", "VIZ-1: reads the derived cluster_hierarchy store, not graph rows; cluster membership still reflects graph structure, and that disclosure question is untraced"),
    ("ClusterHierarchyExpand", "VIZ-1: same store and same untraced disclosure question as ClusterHierarchyClusters"),
];
#[cfg(test)]
mod read_rls_coverage_tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};

    /// Every non-mutating (`mutates == false`) protocol method routes through
    /// the audited RLS primitive, or is a documented, named exception. `UNDOCUMENTED`
    /// (a method whose name matches none of `RLS_ROUTED`/`NON_ROW_SCOPED`/
    /// `NOT_YET_AUDITED`) is the only hard failure -- mirrors
    /// `server::mutation::tests::gateway_routed_set_matches_mutating_policy_surface`'s
    /// exhaustive-partition idiom on the read side.
    ///
    /// A handful of methods (e.g. `KnowledgeStream`, feature `knowledge-batch`)
    /// are themselves `#[cfg(feature = ...)]`-gated INSIDE `eg_capabilities::
    /// ALL_METHODS`'s own array literal, so `all_read` legitimately varies by
    /// build. This test therefore checks every method ACTUALLY PRESENT in the
    /// CURRENT build's `all_read` against the three lists (which are a
    /// superset spanning every feature combination) rather than requiring
    /// every listed name to be present -- run with `--features full` (or at
    /// least `knowledge-batch`/`jobs`/`tensor`/…) for the widest single-run
    /// coverage; CI's per-feature test matrix covers the rest.
    #[test]
    fn every_read_method_routes_through_rls_or_is_a_documented_exception() {
        let all_read: BTreeSet<&'static str> = eg_capabilities::ALL_METHODS
            .iter()
            .filter(|(_, p, _)| !p.mutates)
            .map(|(name, _, _)| *name)
            .collect();

        let routed_set: BTreeSet<&'static str> = RLS_ROUTED.iter().copied().collect();
        assert_eq!(
            routed_set.len(),
            RLS_ROUTED.len(),
            "RLS_ROUTED contains a duplicate entry"
        );
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        assert_eq!(
            non_row.len(),
            NON_ROW_SCOPED.len(),
            "NON_ROW_SCOPED contains a duplicate method name"
        );
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert_eq!(
            not_audited.len(),
            NOT_YET_AUDITED.len(),
            "NOT_YET_AUDITED contains a duplicate method name"
        );

        // The three lists must be pairwise disjoint -- a method double-listed
        // (e.g. both RLS_ROUTED and NON_ROW_SCOPED) is a self-contradiction
        // regardless of which methods the current build actually compiled in.
        for name in non_row.keys().chain(not_audited.keys()) {
            assert!(
                !routed_set.contains(name),
                "'{name}' is listed in both RLS_ROUTED and NON_ROW_SCOPED/NOT_YET_AUDITED"
            );
        }
        for name in non_row.keys() {
            assert!(
                !not_audited.contains_key(name),
                "'{name}' is listed in both NON_ROW_SCOPED and NOT_YET_AUDITED"
            );
        }

        // Every method ACTUALLY PRESENT in this build's non-mutating surface
        // must be classified -- the hard failure. A documented name that this
        // build happens not to compile in (a narrower `--features` selection
        // than `full`) is not checked here; it is still verified whenever a
        // build that includes it runs this test.
        let undocumented: Vec<&&'static str> = all_read
            .iter()
            .filter(|name| {
                !routed_set.contains(*name)
                    && !non_row.contains_key(*name)
                    && !not_audited.contains_key(*name)
            })
            .collect();
        assert!(
            undocumented.is_empty(),
            "UNDOCUMENTED read methods (silently unclassified, not allowed): {undocumented:?} -- \
             add each to RLS_ROUTED, NON_ROW_SCOPED, or NOT_YET_AUDITED in server::access"
        );

        let present_routed = all_read.intersection(&routed_set).count();
        let present_non_row = all_read.iter().filter(|n| non_row.contains_key(*n)).count();
        let present_not_audited = all_read
            .iter()
            .filter(|n| not_audited.contains_key(*n))
            .count();
        println!(
            "L-RLS-1 read coverage (this build): {present_routed} RLS-routed / \
             {present_non_row} non-row-scoped (justified) / {present_not_audited} not-yet-audited \
             / {} non-mutating methods total present ({} / {} / {} known across all builds)",
            all_read.len(),
            RLS_ROUTED.len(),
            NON_ROW_SCOPED.len(),
            NOT_YET_AUDITED.len(),
        );
    }

    /// The exact, task-named gap this gate exists to close: `timeseries.rs`'s 5
    /// read methods (the original 4 query primitives plus `TsListSeries`, the
    /// tenant/graph-scoped series enumeration retention needs to discover what
    /// to sweep) must be a DOCUMENTED, justified `NON_ROW_SCOPED` exception --
    /// never `RLS_ROUTED` (they never construct a `GraphView`) and never
    /// silently absent from every bucket.
    #[test]
    fn timeseries_read_methods_are_a_documented_non_row_scoped_exception() {
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        for method in [
            "TsRange",
            "TsAsofJoin",
            "TsWindow",
            "TsGapFill",
            "TsListSeries",
        ] {
            assert!(
                non_row.contains_key(method),
                "'{method}' must be a documented NON_ROW_SCOPED exception"
            );
            assert!(
                !RLS_ROUTED.contains(&method),
                "'{method}' must not also claim to be RLS_ROUTED"
            );
        }
    }

    /// Self-routed modality/compute handlers have no graph row target to
    /// project. Their negative control is therefore the verified carrier's
    /// owner boundary: a foreign tenant cannot mint or claim the same
    /// owner-scoped handle. Viz applies this boundary to its persistent
    /// dataset/result handles in `handlers::viz`; the three pure-compute
    /// handlers have no server-held handle at all, so there is no row or
    /// non-row resource for them to read across tenants.
    fn assert_self_routed_cross_tenant_control(method: &str, expected_reason: &'static str) {
        let owner = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "owner", "tenant-a",
            ),
        )
        .unwrap();
        let foreign = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "foreign", "tenant-b",
            ),
        )
        .unwrap();
        let owner_handle = owner.namespace("read-rls-control", method);
        let foreign_handle = foreign.namespace("read-rls-control", method);
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();

        assert_eq!(
            non_row.get(method).copied(),
            Some(expected_reason),
            "{method} must remain in the documented non-row exception bucket"
        );
        assert!(!RLS_ROUTED.contains(&method));
        assert_ne!(
            owner_handle, foreign_handle,
            "foreign tenants must not receive the same {method} owner handle"
        );
        assert!(
            !foreign.owns(owner.tenant_scope(), owner.actor_scope()),
            "foreign tenant must be denied ownership of the {method} scope"
        );
    }

    #[test]
    fn asr_is_non_row_scoped_and_foreign_tenant_control_is_denied() {
        assert_self_routed_cross_tenant_control("Asr", REASON_ASR_PURE_COMPUTE);
    }

    #[test]
    fn quantum_is_non_row_scoped_and_foreign_tenant_control_is_denied() {
        assert_self_routed_cross_tenant_control("Quantum", REASON_QUANTUM_PURE_COMPUTE);
    }

    #[test]
    fn tts_is_non_row_scoped_and_foreign_tenant_control_is_denied() {
        assert_self_routed_cross_tenant_control("TtsSynthesize", REASON_TTS_PURE_COMPUTE);
    }

    #[test]
    fn viz_is_owner_scoped_non_row_and_foreign_tenant_control_is_denied() {
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        assert_eq!(
            non_row.get("Viz").copied(),
            Some(REASON_VIZ_CARRIER_SCOPED),
            "Viz must be documented as an owner-scoped non-row surface"
        );
        assert!(!RLS_ROUTED.contains(&"Viz"));

        let owner = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "owner", "tenant-a",
            ),
        )
        .unwrap();
        let foreign = CarrierAuthority::from_verified(
            &super::super::auth::VerifiedRequestContext::verified_for_test_in_tenant(
                "foreign", "tenant-b",
            ),
        )
        .unwrap();
        assert!(
            !foreign.owns(owner.tenant_scope(), owner.actor_scope()),
            "foreign tenant must be denied the owner of Viz's persistent side-store"
        );
    }

    // ── L-RLS-1 follow-up: the 5 formerly-`NOT_YET_AUDITED` epistemic read methods ──
    //
    // One focused test per method (task-required granularity), each pinning its
    // resolved routing bucket AND asserting it is absent from the other two -- so a
    // future regression on any ONE of these 5 names fails at ITS OWN assertion, not
    // just the exhaustive partition test above. `ResolveConflict`/`ExplainEvidence`
    // were traced to a real `core.analysis_snapshot()` read in
    // `src/server/handlers/query.rs` (their handler arms now `rls.filter_view` it
    // before use, see those arms' doc comments) -- `RLS_ROUTED`. `CausalEstimate`/
    // `CausalCounterfactual`/`RankByProvenance` were traced to the SAME file's
    // `causal_estimate_wire`/`causal_counterfactual_wire`/`rank_by_provenance_wire`,
    // none of which reference `core`/`state`/`GraphView` at all -- `NON_ROW_SCOPED`.

    #[test]
    fn resolve_conflict_is_rls_routed() {
        assert!(
            RLS_ROUTED.contains(&"ResolveConflict"),
            "ResolveConflict's handler builds a BeliefGraph off core.analysis_snapshot() \
             (src/server/handlers/query.rs's Method::ResolveConflict arm) and must \
             rls.filter_view it before eg_epistemic::BeliefGraph::from_graph_view sees it -- \
             RLS_ROUTED"
        );
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert!(!non_row.contains_key("ResolveConflict"));
        assert!(!not_audited.contains_key("ResolveConflict"));
    }

    #[test]
    fn explain_evidence_is_rls_routed() {
        assert!(
            RLS_ROUTED.contains(&"ExplainEvidence"),
            "ExplainEvidence's handler builds a BeliefGraph off core.analysis_snapshot() \
             (src/server/handlers/query.rs's Method::ExplainEvidence arms) and must \
             rls.filter_view it before eg_epistemic::BeliefGraph::from_graph_view sees it -- \
             RLS_ROUTED"
        );
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert!(!non_row.contains_key("ExplainEvidence"));
        assert!(!not_audited.contains_key("ExplainEvidence"));
    }

    #[test]
    fn causal_estimate_is_a_documented_non_row_scoped_exception() {
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        assert_eq!(
            non_row.get("CausalEstimate").copied(),
            Some(REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
            "CausalEstimate's handler (causal_estimate_wire) never reads core/state/GraphView -- \
             pure compute over the request's own variables/do_values/mode"
        );
        assert!(!RLS_ROUTED.contains(&"CausalEstimate"));
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert!(!not_audited.contains_key("CausalEstimate"));
    }

    #[test]
    fn causal_counterfactual_is_a_documented_non_row_scoped_exception() {
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        assert_eq!(
            non_row.get("CausalCounterfactual").copied(),
            Some(REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
            "CausalCounterfactual's handler (causal_counterfactual_wire) never reads \
             core/state/GraphView -- pure compute over the request's own \
             variables/actual/do_values"
        );
        assert!(!RLS_ROUTED.contains(&"CausalCounterfactual"));
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert!(!not_audited.contains_key("CausalCounterfactual"));
    }

    #[test]
    fn rank_by_provenance_is_a_documented_non_row_scoped_exception() {
        let non_row: HashMap<&'static str, &'static str> = NON_ROW_SCOPED.iter().copied().collect();
        assert_eq!(
            non_row.get("RankByProvenance").copied(),
            Some(REASON_EPISTEMIC_CAUSAL_PURE_COMPUTE),
            "RankByProvenance's handler (rank_by_provenance_wire) never reads \
             core/state/GraphView -- pure compute over the request's own candidates/weights"
        );
        assert!(!RLS_ROUTED.contains(&"RankByProvenance"));
        let not_audited: HashMap<&'static str, &'static str> =
            NOT_YET_AUDITED.iter().copied().collect();
        assert!(!not_audited.contains_key("RankByProvenance"));
    }
}
