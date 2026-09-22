use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use petgraph::visit::EdgeRef;

#[cfg(feature = "result-cache")]
use super::view::ProjectionScope;
use super::{lift_v2_integrity_policy, GraphCore, GraphSchemaSources, GraphView, Topology};

/// Owned, serializable graph state used for isolated mutation staging and portable
/// transfer (CONCEPT:EG-KG.storage.nonblocking-checkpoint). Two properties matter:
///
/// * **Short lock scope:** producing it clones node/edge/ledger/semantic data so
///   encoding and isolated execution happen after the topology lock is released.
/// * **Direct serialization (A3):** encoded straight via `rmp_serde`. Node/edge
///   properties are already MessagePack byte blobs, so the current snapshot schema
///   never detours through `serde_json::Value` or allocates a second property image.
/// * **Strict persisted schema:** the mandatory version and unknown-field rejection
///   prevent a partial or differently shaped image from being accepted as current.
pub const GRAPH_SNAPSHOT_SCHEMA_VERSION: u16 = 3;

/// Exact legacy v2 graph-scoped integrity-policy shape, retained only by the
/// read-old/write-current migration decoders.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityPolicyV2 {
    pub shapes_ttl: String,
}

/// KNOWN GAP (2026-08-12): does NOT carry `GraphCore::schema_refs` (the A18
/// TBox/ABox live reverse index) — see that field's own doc for the full
/// consequence (the mutation gateway's staged-commit pipeline silently drops
/// schema marks for gateway-routed native writes) and why fixing it is a
/// deliberately deferred, separately-scoped durable-schema migration.
#[derive(Clone, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSnapshot {
    pub schema_version: u16,
    /// Required current-schema authority. Core entries are reconciled to the
    /// running binary as one atomic set when an older engine image is loaded.
    pub schema_sources: Arc<GraphSchemaSources>,
    // Arc-valued (Phase C-A): building a snapshot clones Arc pointers, not the
    // property bytes. The serialized current schema remains an owned byte array.
    pub nodes: Vec<(String, Arc<Vec<u8>>)>,
    pub edges: Vec<(String, String, Arc<Vec<u8>>)>,
    pub ledger: Vec<String>,
    pub semantic_store: crate::compute::semantic::SemanticStore,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphSnapshotCurrent {
    schema_version: u16,
    schema_sources: Arc<GraphSchemaSources>,
    nodes: Vec<(String, Arc<Vec<u8>>)>,
    edges: Vec<(String, String, Arc<Vec<u8>>)>,
    ledger: Vec<String>,
    semantic_store: crate::compute::semantic::SemanticStore,
}

/// Exact persisted v2 layout. This is deliberately private to decoding: new
/// snapshots can only encode the current keyed authority.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphSnapshotV2 {
    schema_version: u16,
    integrity_policy: Option<IntegrityPolicyV2>,
    nodes: Vec<(String, Arc<Vec<u8>>)>,
    edges: Vec<(String, String, Arc<Vec<u8>>)>,
    ledger: Vec<String>,
    semantic_store: crate::compute::semantic::SemanticStore,
}

#[cfg(test)]
mod schema_migration_tests {
    use super::*;

    #[cfg(feature = "ann")]
    const V2_SNAPSHOT_GOLDEN: &str = "86ae736368656d615f76657273696f6e02b0696e746567726974795f706f6c69637981aa7368617065735f74746cd92b407072656669782073683a203c687474703a2f2f7777772e77332e6f72672f6e732f736861636c233e202ea56e6f6465739192a16e93010203a5656467657390a66c656467657291a56576656e74ae73656d616e7469635f73746f726584a364696d00a369647390a46461746190a57370616365c0";
    #[cfg(not(feature = "ann"))]
    const V2_SNAPSHOT_GOLDEN: &str = "86ae736368656d615f76657273696f6e02b0696e746567726974795f706f6c69637981aa7368617065735f74746cd92b407072656669782073683a203c687474703a2f2f7777772e77332e6f72672f6e732f736861636c233e202ea56e6f6465739192a16e93010203a5656467657390a66c656467657291a56576656e74ae73656d616e7469635f73746f726582aa656d62656464696e677380a57370616365c0";

    #[test]
    fn legacy_v2_snapshot_lifts_policy_into_operator_source() {
        let legacy = GraphSnapshotV2 {
            schema_version: 2,
            integrity_policy: Some(IntegrityPolicyV2 {
                shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
            }),
            nodes: vec![("n".to_string(), Arc::new(vec![1, 2, 3]))],
            edges: Vec::new(),
            ledger: vec!["event".to_string()],
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        };
        let bytes = hex::decode(V2_SNAPSHOT_GOLDEN).unwrap();
        assert_eq!(rmp_serde::to_vec_named(&legacy).unwrap(), bytes);
        let decoded: GraphSnapshot = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.schema_version, GRAPH_SNAPSHOT_SCHEMA_VERSION);
        assert!(decoded
            .schema_sources
            .dynamic
            .contains_key(super::super::OPERATOR_SOURCE_ID));
        assert!(!decoded.schema_sources.core.is_empty());
        assert_eq!(decoded.nodes[0].0, "n");
    }

    #[test]
    fn current_snapshot_restart_preserves_dynamic_sources_and_core_identity() {
        let original = GraphCore::new();
        let mut sources = (*original.schema_sources()).clone();
        sources
            .attach_dynamic(
                "admin:restart".to_string(),
                super::super::GraphSchemaSource::new(
                    super::super::SchemaSourceOrigin::Admin {
                        name: "restart".to_string(),
                    },
                    None,
                    Some(Arc::from("@prefix owl: <http://www.w3.org/2002/07/owl#> .")),
                    0,
                )
                .unwrap(),
            )
            .unwrap();
        original.install_schema_sources(Arc::new(sources));

        let bytes = original.to_msgpack().unwrap();
        let restored = GraphCore::new();
        restored.from_msgpack(&bytes).unwrap();
        assert_eq!(restored.schema_sources(), original.schema_sources());
        restored.schema_sources().validate().unwrap();
    }

    #[test]
    fn invalid_snapshot_schema_source_is_an_atomic_noop() {
        let live = GraphCore::new();
        let before = live.schema_sources();
        let mut snapshot = live.snapshot();
        let mut sources = (*snapshot.schema_sources).clone();
        let mut forged = super::super::GraphSchemaSource::new(
            super::super::SchemaSourceOrigin::Admin {
                name: "forged".to_string(),
            },
            Some(Arc::from("@prefix sh: <http://www.w3.org/ns/shacl#> .")),
            None,
            0,
        )
        .unwrap();
        forged.shapes_sha256 = Some(eg_types::contract::Digest256::sha256(b"different"));
        sources.dynamic.insert("admin:forged".to_string(), forged);
        snapshot.schema_sources = Arc::new(sources);

        assert!(live.replace_snapshot(snapshot).is_err());
        assert_eq!(live.schema_sources(), before);
    }

    #[test]
    fn oversized_v2_policy_is_a_typed_decode_error_not_a_panic() {
        let legacy = GraphSnapshotV2 {
            schema_version: 2,
            integrity_policy: Some(IntegrityPolicyV2 {
                shapes_ttl: "x".repeat(eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES + 1),
            }),
            nodes: Vec::new(),
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        };
        let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        assert!(rmp_serde::from_slice::<GraphSnapshot>(&bytes).is_err());
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum GraphSnapshotWire {
    Current(GraphSnapshotCurrent),
    V2(GraphSnapshotV2),
}

impl<'de> serde::Deserialize<'de> for GraphSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        match GraphSnapshotWire::deserialize(deserializer)? {
            GraphSnapshotWire::Current(value) => {
                if value.schema_version != GRAPH_SNAPSHOT_SCHEMA_VERSION {
                    return Err(D::Error::custom(format!(
                        "unsupported graph snapshot schema version {}; expected {}",
                        value.schema_version, GRAPH_SNAPSHOT_SCHEMA_VERSION
                    )));
                }
                let schema_sources = Arc::new(
                    Arc::unwrap_or_clone(value.schema_sources)
                        .reconciled_current_core()
                        .map_err(D::Error::custom)?,
                );
                Ok(Self {
                    schema_version: value.schema_version,
                    schema_sources,
                    nodes: value.nodes,
                    edges: value.edges,
                    ledger: value.ledger,
                    semantic_store: value.semantic_store,
                })
            }
            GraphSnapshotWire::V2(value) => {
                if value.schema_version != 2 {
                    return Err(D::Error::custom(format!(
                        "unsupported legacy graph snapshot schema version {}",
                        value.schema_version
                    )));
                }
                Ok(Self {
                    schema_version: GRAPH_SNAPSHOT_SCHEMA_VERSION,
                    schema_sources: lift_v2_integrity_policy(value.integrity_policy)
                        .map_err(D::Error::custom)?,
                    nodes: value.nodes,
                    edges: value.edges,
                    ledger: value.ledger,
                    semantic_store: value.semantic_store,
                })
            }
        }
    }
}

// Snapshots may be substantially larger than one RPC frame, but they still cross
// a restore boundary and must be structurally bounded before serde can honor any
// attacker-controlled collection length hint.
const MAX_GRAPH_SNAPSHOT_BYTES: usize = 1024 * 1024 * 1024;
const MAX_GRAPH_SNAPSHOT_ITEMS: usize = 16_000_000;
impl GraphSnapshot {
    /// Serialize this snapshot to MessagePack (called OFF the graph lock).
    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        self.validate_schema()?;
        rmp_serde::to_vec_named(self).map_err(|e| e.to_string())
    }

    fn validate_schema(&self) -> Result<(), String> {
        if self.schema_version != GRAPH_SNAPSHOT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported graph snapshot schema version {}; expected {}",
                self.schema_version, GRAPH_SNAPSHOT_SCHEMA_VERSION
            ));
        }
        self.schema_sources.validate()
    }
}

/// Fully validated replacement image built away from the live graph locks.
/// Keeping this preparation result explicit makes the restore boundary's
/// validate/build-before-publish rule visible: `GraphCore::replace_snapshot`
/// only acquires its writer barrier after this value has been constructed.
struct PreparedGraphSnapshot {
    topology: Topology,
    node_properties: Vec<(String, Arc<Vec<u8>>)>,
    edge_properties: HashMap<(String, String), Vec<Arc<Vec<u8>>>>,
    ledger: Vec<String>,
    semantic_store: crate::compute::semantic::SemanticStore,
    schema_sources: Arc<GraphSchemaSources>,
    node_bloom: crate::bloom::NodeBloomFilter,
}

fn prepare_graph_snapshot(snapshot: GraphSnapshot) -> Result<PreparedGraphSnapshot, String> {
    snapshot.validate_schema()?;
    let GraphSnapshot {
        schema_version: _,
        schema_sources,
        nodes,
        edges,
        ledger,
        semantic_store,
    } = snapshot;

    // Validate and construct the complete replacement OFF the live graph.
    // A duplicate node or dangling endpoint must leave the prior image intact;
    // clearing first turned a malformed restore into partial data loss.
    let mut topology = Topology::default();
    let mut node_properties = Vec::with_capacity(nodes.len());
    for (node_id, properties) in nodes {
        if topology.node_map.contains_key(&node_id) {
            return Err(format!("snapshot contains duplicate node '{}'", node_id));
        }
        let index = topology.graph.add_node(node_id.clone());
        topology.node_map.insert(node_id.clone(), index);
        node_properties.push((node_id, properties));
    }
    let mut edge_properties: HashMap<_, Vec<_>> = HashMap::new();
    for (source, target, properties) in edges {
        let source_index = topology
            .node_map
            .get(&source)
            .copied()
            .ok_or_else(|| format!("snapshot edge source '{}' is missing", source))?;
        let target_index = topology
            .node_map
            .get(&target)
            .copied()
            .ok_or_else(|| format!("snapshot edge target '{}' is missing", target))?;
        topology
            .graph
            .add_edge(source_index, target_index, format!("{}:{}", source, target));
        edge_properties
            .entry((source, target))
            .or_default()
            .push(properties);
    }

    // A snapshot is by construction a COMPLETE point-in-time node set (unlike a
    // bounded `MaterialPage`), so the bloom guard can be rebuilt fresh and
    // trusted immediately (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard) — sized off this
    // exact cardinality rather than the prior graph's.
    let node_bloom = crate::bloom::NodeBloomFilter::new(node_properties.len(), 0.01);
    for (node_id, _) in &node_properties {
        node_bloom.insert(node_id);
    }

    Ok(PreparedGraphSnapshot {
        topology,
        node_properties,
        edge_properties,
        ledger,
        semantic_store,
        schema_sources,
        node_bloom,
    })
}

impl GraphCore {
    // ── Serialization ────────────────────────────────────────────────────

    /// Owned, serializable snapshot of this graph's persistent state. Cheap
    /// relative to serialization (clones the node/edge/ledger/semantic data), so a
    /// checkpoint takes it under a BRIEF lock and serializes OFF the lock.
    /// (CONCEPT:EG-KG.storage.nonblocking-checkpoint — non-blocking checkpoint, A1)
    pub fn snapshot(&self) -> GraphSnapshot {
        // Hold the topology read lock for the duration: every mutation goes through
        // a write txn (topo.write()), so a read guard excludes all writers and the
        // node/edge/ledger views below are a single consistent point-in-time.
        let _topo = self.topo.read();
        // Zero-copy (Phase C-A): clone the Arc POINTERS, not the property bytes —
        // turns the A1-residual ~3s lock-held deep clone of a 450MB graph into a
        // ~µs pointer copy, and removes the transient memory doubling.
        GraphSnapshot {
            schema_version: GRAPH_SNAPSHOT_SCHEMA_VERSION,
            schema_sources: Arc::clone(&self.schema_sources.read()),
            nodes: self.get_nodes_arc(),
            edges: self.get_edges_arc(),
            ledger: self.ledger.lock().clone(),
            semantic_store: self.semantic_store.read().clone(),
        }
    }

    /// Build an isolated mutable graph from an owned snapshot without copying
    /// property blobs. Mutation gateways use this as their staging image: execute
    /// against the fork, durably commit its resulting snapshot, then publish it to
    /// the live core. The caller-supplied version is the authoritative OCC source
    /// version captured with the snapshot.
    pub fn from_snapshot(snapshot: GraphSnapshot, version: u64) -> Result<Self, String> {
        let core = Self::new();
        core.replace_snapshot(snapshot)?;
        core.version
            .store(version, std::sync::atomic::Ordering::Release);
        core.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(core)
    }

    /// Publish the authoritative version of a newly materialized projection.
    ///
    /// Recovery and graph creation build an unpublished [`GraphCore`] from
    /// durable rows using the ordinary row primitives. Those primitives do not
    /// advance the serving version because replay is not a new mutation. Before
    /// the core becomes visible, its version must therefore adopt the exact
    /// authoritative source watermark. This is a one-shot transition from the
    /// fresh-core version (`0`); refusing any later transition prevents recovery
    /// plumbing from rewinding a live projection.
    #[doc(hidden)]
    pub fn adopt_materialized_version(&self, authoritative_version: u64) -> Result<(), String> {
        if authoritative_version == 0 {
            return Err(
                "materialized version publication requires a committed non-zero version"
                    .to_string(),
            );
        }
        self.version
            .compare_exchange(
                0,
                authoritative_version,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|current| {
                format!(
                    "materialized version publication requires a fresh projection (current {current}, authoritative {authoritative_version})"
                )
            })?;
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Atomically replace this core's persistent image while retaining its
    /// read-through, notifier, and index-service attachments. Property `Arc`s are
    /// moved from the staged snapshot, avoiding a second graph-sized byte copy.
    /// Version/dirty publication is intentionally left to the commit gateway.
    pub fn replace_snapshot(&self, snapshot: GraphSnapshot) -> Result<(), String> {
        let PreparedGraphSnapshot {
            topology,
            node_properties,
            edge_properties,
            ledger,
            semantic_store,
            schema_sources,
            node_bloom,
        } = prepare_graph_snapshot(snapshot)?;

        // No fallible validation remains beyond this point. Publish under the
        // topology writer barrier, then invalidate derivatives of the old image.
        let mut topo = self.topo.write();
        *topo = topology;
        self.node_properties.clear();
        for (node_id, properties) in node_properties {
            self.node_properties.insert(node_id, properties);
        }
        *self.node_bloom.write() = node_bloom;
        self.bloom_complete
            .store(true, std::sync::atomic::Ordering::Release);
        self.edge_properties.clear();
        for (endpoints, properties) in edge_properties {
            self.edge_properties.insert(endpoints, properties);
        }
        *self.ledger.lock() = ledger;
        *self.semantic_store.write() = semantic_store;
        *self.schema_sources.write() = schema_sources;
        drop(topo);
        self.invalidate_indexes();
        #[cfg(feature = "result-cache")]
        {
            self.result_cache.invalidate_all();
            // The whole image was replaced; the dependency clock's old per-label/per-key versions
            // are meaningless against the new graph and would otherwise spuriously invalidate
            // future entries. Reset it (the cache's entries were just cleared, so nothing survives
            // to revalidate) — W1.6/P7.
            self.dep_clock.reset();
        }
        // U-142/U-143/U-145 (BUG-130): this whole-image replacement may publish at
        // the SAME `version()` a caller (`prepare_snapshot_publish`/
        // `install_committed_snapshot`) already reconciled to, so the per-actor RLS
        // projection cache's (actor, version) key alone would not detect it. See
        // `Self::invalidate_projection_cache`'s doc.
        #[cfg(feature = "security")]
        self.invalidate_projection_cache();
        // perf/cold-query-floor-analysis (UNCOMPILED proposal): the filtered-view cache
        // is subject to the exact same same-version whole-image race.
        #[cfg(feature = "security")]
        self.invalidate_filtered_view_cache();
        Ok(())
    }

    /// Install a staged image at its pre-commit version. The server immediately
    /// calls `mark_dirty()` after this non-awaiting publication step, advancing to
    /// the durable target version and emitting the normal change notification.
    pub fn prepare_snapshot_publish(
        &self,
        snapshot: GraphSnapshot,
        source_version: u64,
    ) -> Result<(), String> {
        self.replace_snapshot(snapshot)?;
        self.version
            .store(source_version, std::sync::atomic::Ordering::Release);
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Reconcile RAM after a retry discovers that durability committed before the
    /// prior process could publish. This does not increment the already-committed
    /// version; it installs that exact version and wakes local subscribers once.
    pub fn install_committed_snapshot(
        &self,
        snapshot: GraphSnapshot,
        committed_version: u64,
    ) -> Result<(), String> {
        self.replace_snapshot(snapshot)?;
        self.version
            .store(committed_version, std::sync::atomic::Ordering::Release);
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
        self.changes.emit(committed_version);
        Ok(())
    }

    /// Serialize the whole graph using the sole current typed MessagePack schema.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        self.snapshot().to_msgpack()
    }

    pub fn from_msgpack(&self, msgpack: &[u8]) -> Result<(), String> {
        let limits = eg_types::msgpack::MsgpackLimits::new(
            MAX_GRAPH_SNAPSHOT_BYTES,
            MAX_GRAPH_SNAPSHOT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        );
        let snapshot: GraphSnapshot = eg_types::msgpack::decode_bounded(msgpack, limits)
            .map_err(|_| "graph snapshot is invalid or exceeds resource limits".to_string())?;

        // `replace_snapshot` constructs and validates all topology/property state
        // before touching the live image, so a malformed edge or duplicate id is
        // a fail-closed no-op rather than a partially cleared graph.
        self.replace_snapshot(snapshot)
            .map_err(|_| "graph snapshot is invalid or exceeds resource limits".to_string())
    }

    // ── Subgraph Extraction ──────────────────────────────────────────────

    /// Extract a subgraph (read view) containing only the specified node IDs.
    pub fn get_subgraph(&self, node_ids: &[String]) -> GraphView {
        let topo = self.topo.read();
        let mut view = GraphView {
            schema_sources: self.schema_sources(),
            ..GraphView::default()
        };
        // Both halves run under the SAME held topology read guard, exactly as the
        // single-body version did — neither takes a lock of its own.
        self.copy_subgraph_nodes(&topo, node_ids, &mut view);
        self.copy_subgraph_edges(&topo, &mut view);
        view
    }

    /// Copy the requested nodes (those that actually exist) into `view`, with
    /// their properties and their point-in-time TBox membership.
    ///
    /// The caller holds the topology READ guard and passes it in as `topo`.
    fn copy_subgraph_nodes(&self, topo: &Topology, node_ids: &[String], view: &mut GraphView) {
        for nid in node_ids {
            if !topo.node_map.contains_key(nid) || view.node_map.contains_key(nid) {
                continue;
            }
            let new_idx = view.graph.add_node(nid.clone());
            view.node_map.insert(nid.clone(), new_idx);
            if let Some(props) = self.node_properties.get(nid) {
                view.node_properties.insert(nid.clone(), props.clone());
            }
            // BUG A3: point-in-time TBox membership for this induced
            // subgraph, consulted by `filter_view`.
            if self.is_schema_node(nid) {
                view.schema_node_ids.insert(nid.clone());
            }
        }
    }

    /// Copy the induced edges into `view` by walking only the OUTGOING adjacency
    /// of the selected nodes, instead of scanning the complete edge-property map.
    /// Parallel topology edges share one endpoint property vector, so each
    /// endpoint pair is visited once.
    ///
    /// The caller holds the topology READ guard and passes it in as `topo`. The
    /// selected ids are materialised first so the per-pair copy can take `view`
    /// mutably.
    fn copy_subgraph_edges(&self, topo: &Topology, view: &mut GraphView) {
        let mut seen_pairs = std::collections::HashSet::new();
        let sources: Vec<String> = view.node_map.keys().cloned().collect();
        for src in &sources {
            let Some(&source_index) = topo.node_map.get(src) else {
                continue;
            };
            for edge in topo
                .graph
                .edges_directed(source_index, petgraph::Direction::Outgoing)
            {
                let tgt = &topo.graph[edge.target()];
                if !view.node_map.contains_key(tgt)
                    || !seen_pairs.insert((src.clone(), tgt.clone()))
                {
                    continue;
                }
                self.copy_subgraph_edge_pair(src, tgt, view);
            }
        }
    }

    /// Copy every parallel edge blob between one selected `(src, tgt)` pair.
    fn copy_subgraph_edge_pair(&self, src: &str, tgt: &str, view: &mut GraphView) {
        let Some(props) = self
            .edge_properties
            .get(&(src.to_string(), tgt.to_string()))
        else {
            return;
        };
        let (Some(&s), Some(&t)) = (view.node_map.get(src), view.node_map.get(tgt)) else {
            return;
        };
        for prop in props.iter() {
            view.graph.add_edge(s, t, format!("{}:{}", src, tgt));
            view.edge_properties
                .entry((src.to_string(), tgt.to_string()))
                .or_default()
                .push(prop.clone());
        }
    }

    // ── Read-Only Compute Snapshots (CONCEPT:EG-KG.txn.per-graph-write-isolation) ────────────────────
    // CPU-heavy read-only algorithms must not run while holding a graph lock —
    // they would starve writers for the whole computation. These snapshots take a
    // cheap O(V+E) structural copy under the topology READ lock (concurrent with
    // other readers; excludes only structural writers) into an unlocked
    // `GraphView`, so the algorithm runs on the blocking pool with no lock held.
    // The ledger and embedding store are never copied — algorithms don't read them.

    /// Topology-only snapshot: petgraph structure + id↔index map. For algorithms
    /// that read only the graph shape (PageRank, betweenness, community detection,
    /// graph coloring, …).
    pub fn topology_snapshot(&self) -> GraphView {
        let topo = self.topo.read();
        GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: HashMap::new(),
            edge_properties: HashMap::new(),
            schema_sources: self.schema_sources(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // No version captured alongside this snapshot (unlike `analysis_snapshot_versioned`),
            // so a freshly materialized projection couldn't be soundly stamped — omit the handle;
            // a `gds.*` procedure over this view just always re-projects.
            #[cfg(feature = "result-cache")]
            projection_scope: None,
            // No property blobs at all in this snapshot, so nothing for
            // `filter_view` to row-filter here (BUG A3) — empty, not omitted.
            schema_node_ids: std::collections::HashSet::new(),
            // perf/row-visibility-index: same reasoning as `schema_node_ids`
            // immediately above — no property blobs here for `can_see_node` to
            // decide visibility over, so nothing to carry. (`can_see_node`'s
            // missing-entry fallback also makes this safe even if it were used.)
            #[cfg(feature = "security")]
            visibility_index: HashMap::new(),
        }
    }

    /// Topology + property-blob snapshot (still no ledger / embedding store). For
    /// algorithms that also read node/edge property blobs: MST edge weights, VF2
    /// matching, similarity edges, lifecycle metrics.
    pub fn analysis_snapshot(&self) -> GraphView {
        let topo = self.topo.read();
        GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: self
                .node_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            edge_properties: self
                .edge_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            schema_sources: self.schema_sources(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // No version captured alongside this snapshot — see `topology_snapshot`'s identical
            // note. Callers that need named-projection reuse must use
            // `analysis_snapshot_versioned` instead.
            #[cfg(feature = "result-cache")]
            projection_scope: None,
            // BUG A3: point-in-time TBox membership, consulted by `filter_view`.
            schema_node_ids: self.live_schema_node_ids(),
            // perf/row-visibility-index: point-in-time RLS visibility for every
            // node above, consulted by `can_see_node`/`filter_view` — see
            // `GraphView::visibility_index`'s doc.
            #[cfg(feature = "security")]
            visibility_index: self.live_visibility_index(),
        }
    }

    /// An [`analysis_snapshot`](Self::analysis_snapshot) paired with the OCC
    /// `version()` read UNDER the same topology read lock (CONCEPT:EG-KG.coordination.distributed-cache-coherence). Taking
    /// both atomically lets the result cache store a query's bytes under exactly the
    /// version the snapshot reflects: a topology write bumps `version` via
    /// `mark_dirty` only while holding the topo WRITE lock, mutually exclusive with
    /// the read lock held here — so the `(view, version)` pair is point-in-time
    /// consistent and a cache entry can never claim a version newer than its data.
    #[cfg(feature = "result-cache")]
    pub fn analysis_snapshot_versioned(&self) -> (GraphView, u64) {
        let topo = self.topo.read();
        let version = self.version();
        let view = GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: self
                .node_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            edge_properties: self
                .edge_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            schema_sources: self.schema_sources(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // CONCEPT:EG-KG.query.named-graph-projection-catalog (W4.5/N5) — hand this view a
            // SHARED handle (Arc clones — the same live objects, not forks) to the graph's named
            // projection catalog + the dependency clock that validates it, stamped with the SAME
            // `version` just read atomically above. This is the ONE snapshot constructor that
            // populates it (see `ProjectionScope`'s doc for why the others deliberately don't).
            projection_scope: Some(ProjectionScope {
                catalog: Arc::clone(&self.graph_projections),
                dep_clock: Arc::clone(&self.dep_clock),
                version,
            }),
            // BUG A3: point-in-time TBox membership, consulted by `filter_view`.
            schema_node_ids: self.live_schema_node_ids(),
            // perf/row-visibility-index: same as `analysis_snapshot` — see
            // `GraphView::visibility_index`'s doc.
            #[cfg(feature = "security")]
            visibility_index: self.live_visibility_index(),
        };
        (view, version)
    }
}
