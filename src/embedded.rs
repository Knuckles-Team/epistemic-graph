//! In-process embedded library API (CONCEPT:KG-2.216).
//!
//! SQLite/DuckDB-style: `EmbeddedEngine::open(persist_dir, options)` hands back an
//! in-process handle that owns a [`GraphRegistry`] + (optionally) the redb durable
//! store DIRECTLY — **NO Tokio server, NO socket, NO HMAC** (it is in-process, so
//! the caller IS the trusted party; there is no network boundary to authenticate).
//! Core ops are plain method calls.
//!
//! ## One core, two transports
//!
//! This is the *embedded* transport over the SAME engine core the out-of-process
//! Tokio server drives. The server's `dispatch` → `handlers::graph_ops` arms call
//! [`GraphCore`] methods; the durable side calls [`crate::wal::apply`] (the
//! canonical "durable `Method` → `GraphCore`" applier) + the redb row writers in
//! [`crate::redb_store`]. The embedded engine calls the **exact same** pieces:
//!
//! * in-memory mutations apply through `GraphCore` (and for the durable set,
//!   `wal::apply`, byte-identical to WAL replay / the Raft state machine); and
//! * durable mutations commit through `crate::redb_store` — the SAME tables, keys,
//!   and `apply_method_rows`/`apply_checkpoint`/`read_all_dumps` the server's
//!   `redb_backend` uses. A graph written embedded reopens in the server and
//!   vice-versa.
//!
//! The ONLY things the embedded transport drops are the network concerns the
//! in-process model doesn't need: HMAC auth, the ACL isolation layer (a trusted
//! local caller), the Tokio reactor + the per-graph write coalescer (a
//! concurrency-amortization for thousands of socket clients — an embedded caller
//! writes inline). No engine logic is duplicated.
//!
//! ## Durability
//!
//! When a persist dir is configured and `durable` is on (the default), the engine
//! is redb-AUTHORITATIVE in-process: an `add_node`/`add_edge`/… returns ONLY after
//! its row is committed to redb with `Durability::Immediate` (commit-before-return,
//! the in-process analogue of the server's commit-before-ack). So an embedded write
//! that returned `Ok` survives a `kill -9`. `close()`/`checkpoint()` snapshot the
//! whole registry (incl. semantic vectors) into redb. Reopening replays the durable
//! store back into a fresh registry.
//!
//! With NO persist dir the engine is in-memory only (a scratch graph), exactly like
//! the server with no persist dir.

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::graph::{GraphCore, GraphView};
use crate::protocol::{GraphType, Method};
use crate::registry::GraphRegistry;

#[cfg(feature = "redb")]
mod store;
#[cfg(feature = "redb")]
use store::EmbeddedRedbStore;

/// Configuration for opening an [`EmbeddedEngine`].
#[derive(Debug, Clone, Default)]
pub struct EmbeddedOptions {
    /// When `true` (the default when a persist dir is given) every durable mutation
    /// is committed to redb before the call returns (commit-before-return). When
    /// `false`, or when there is no persist dir, the engine is in-memory only.
    pub durable: bool,
}

impl EmbeddedOptions {
    /// Durable (the SQLite-style default): commit-before-return when a persist dir
    /// is configured.
    pub fn durable() -> Self {
        Self { durable: true }
    }
    /// In-memory only — no durable writes even if a persist dir is given.
    pub fn in_memory() -> Self {
        Self { durable: false }
    }
}

/// An in-process handle to the epistemic-graph engine core.
///
/// Cheaply cloneable (it is `Arc`-backed); clones share the same registry + durable
/// store, so several call sites in one process drive ONE engine.
#[derive(Clone)]
pub struct EmbeddedEngine {
    inner: Arc<Inner>,
}

struct Inner {
    /// The multi-tenant registry — the SAME type the server holds in `ServerState`.
    /// Behind a sync `parking_lot::RwLock` (no Tokio): registry ops (create/delete/
    /// lookup) are sub-microsecond, and per-graph topology mutation takes its own
    /// internal lock inside `GraphCore`, so this guards only the name→core map.
    registry: RwLock<GraphRegistry>,
    /// Durable redb store, present when a persist dir is configured AND `durable`.
    #[cfg(feature = "redb")]
    store: Option<EmbeddedRedbStore>,
    /// In-memory-only when there is no durable store.
    #[cfg(not(feature = "redb"))]
    _persist: Option<std::path::PathBuf>,
}

impl EmbeddedEngine {
    /// Open an embedded engine over an optional persist dir.
    ///
    /// * `Some(dir)` + `options.durable` (and the `redb` feature) ⇒ a durable
    ///   source of truth: tables are opened/created under `{dir}/graph.redb`, any
    ///   prior durable state is replayed into the registry, and every durable
    ///   mutation commits before it returns.
    /// * `None`, or `durable=false`, or built without `redb` ⇒ in-memory only.
    pub fn open<P: AsRef<Path>>(
        persist_dir: Option<P>,
        options: EmbeddedOptions,
    ) -> Result<Self, String> {
        let persist_dir = persist_dir.map(|p| p.as_ref().to_path_buf());

        #[cfg(feature = "redb")]
        {
            let registry = RwLock::new(GraphRegistry::new());
            let store = match (&persist_dir, options.durable) {
                (Some(dir), true) => {
                    let store = EmbeddedRedbStore::open(dir)?;
                    // Replay the durable store into the fresh registry (the SAME
                    // reconstruction the server's redb load_all does).
                    let dumps = store.load_all()?;
                    {
                        let mut reg = registry.write();
                        for dump in dumps {
                            if !reg.exists(&dump.name) {
                                let _ = reg.create_graph(&dump.name, dump.graph_type, None);
                            }
                            if let Some(core) = reg.get(&dump.name).map(|e| e.core.clone()) {
                                for (id, props) in dump.nodes {
                                    core.add_node(id, props);
                                }
                                for (src, tgt, props) in dump.edges {
                                    let _ = core.add_edge(src, tgt, props);
                                }
                                if !dump.semantic.is_empty() {
                                    if let Ok(s) =
                                        rmp_serde::from_slice::<
                                            crate::compute::semantic::SemanticStore,
                                        >(&dump.semantic)
                                    {
                                        *core.semantic_store.write() = s;
                                    }
                                }
                            }
                        }
                    }
                    Some(store)
                }
                _ => None,
            };
            Ok(Self {
                inner: Arc::new(Inner { registry, store }),
            })
        }

        #[cfg(not(feature = "redb"))]
        {
            let _ = options;
            Ok(Self {
                inner: Arc::new(Inner {
                    registry: RwLock::new(GraphRegistry::new()),
                    _persist: persist_dir,
                }),
            })
        }
    }

    /// `true` if durable writes land on disk (a persist dir + `durable` + `redb`).
    pub fn is_durable(&self) -> bool {
        #[cfg(feature = "redb")]
        {
            self.inner.store.is_some()
        }
        #[cfg(not(feature = "redb"))]
        {
            false
        }
    }

    // ── graph lifecycle ──────────────────────────────────────────────────

    /// Create a named graph (durably registering its identity when authoritative).
    pub fn create_graph(&self, name: &str, graph_type: GraphType) -> Result<(), String> {
        self.inner
            .registry
            .write()
            .create_graph(name, graph_type, None)?;
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            store.register_graph(&crate::redb_store::sanitize(name), name, graph_type)?;
        }
        Ok(())
    }

    /// Delete a named graph (cannot delete `__commons__`).
    pub fn delete_graph(&self, name: &str) -> Result<(), String> {
        self.inner.registry.write().delete_graph(name)
    }

    /// List `(name, type)` for every registered graph.
    pub fn list_graphs(&self) -> Vec<(String, GraphType)> {
        self.inner.registry.read().list()
    }

    /// Resolve a graph's core, creating the graph on demand if `auto_create`.
    fn core(&self, graph: &str, auto_create: bool) -> Result<Arc<GraphCore>, String> {
        if let Some(core) = self
            .inner
            .registry
            .read()
            .get(graph)
            .map(|e| e.core.clone())
        {
            return Ok(core);
        }
        if auto_create {
            self.create_graph(graph, GraphType::Global)?;
            return self
                .inner
                .registry
                .read()
                .get(graph)
                .map(|e| e.core.clone())
                .ok_or_else(|| format!("Graph '{graph}' not found after create"));
        }
        Err(format!("Graph '{graph}' not found"))
    }

    // ── writes (durable: commit-before-return) ───────────────────────────

    /// Apply a durable mutation to `graph`: in-memory via `wal::apply` (the SAME
    /// applier WAL replay + the Raft state machine use), then — when authoritative —
    /// commit it to redb before returning (commit-before-return). The graph is
    /// auto-created if it does not exist (matching the firehose ingestion path).
    fn apply_durable(&self, graph: &str, method: Method) -> Result<(), String> {
        let core = self.core(graph, true)?;
        // 1) In-memory apply — the canonical durable Method → GraphCore path.
        crate::wal::apply(&core, &method);
        // 2) Durable commit-before-return.
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            store.commit(&crate::redb_store::sanitize(graph), &method)?;
        }
        Ok(())
    }

    /// Add (or overwrite) a node with MessagePack-encoded properties.
    pub fn add_node(
        &self,
        graph: &str,
        node_id: &str,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        self.apply_durable(
            graph,
            Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack,
            },
        )
    }

    /// Remove a node (and its incident edges).
    pub fn remove_node(&self, graph: &str, node_id: &str) -> Result<(), String> {
        self.apply_durable(
            graph,
            Method::RemoveNode {
                node_id: node_id.to_string(),
            },
        )
    }

    /// Add an edge with MessagePack-encoded properties.
    pub fn add_edge(
        &self,
        graph: &str,
        source_id: &str,
        target_id: &str,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        self.apply_durable(
            graph,
            Method::AddEdge {
                source_id: source_id.to_string(),
                target_id: target_id.to_string(),
                properties_msgpack,
            },
        )
    }

    /// Remove every edge between `source_id` and `target_id`.
    pub fn remove_edge(&self, graph: &str, source_id: &str, target_id: &str) -> Result<(), String> {
        self.apply_durable(
            graph,
            Method::RemoveEdge {
                source_id: source_id.to_string(),
                target_id: target_id.to_string(),
            },
        )
    }

    // ── reads ────────────────────────────────────────────────────────────

    /// A node's MessagePack-encoded properties, or `None` if absent.
    pub fn get_node_properties(
        &self,
        graph: &str,
        node_id: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(self.core(graph, false)?.get_node_properties(node_id))
    }

    /// Whether `node_id` exists in `graph`.
    pub fn has_node(&self, graph: &str, node_id: &str) -> Result<bool, String> {
        Ok(self.core(graph, false)?.has_node(node_id))
    }

    /// Node count of `graph`.
    pub fn node_count(&self, graph: &str) -> Result<usize, String> {
        Ok(self.core(graph, false)?.node_count())
    }

    /// All node ids of `graph`.
    pub fn node_ids(&self, graph: &str) -> Result<Vec<String>, String> {
        Ok(self.core(graph, false)?.node_ids())
    }

    // ── semantic search ──────────────────────────────────────────────────

    /// Attach an embedding vector to a node (for `semantic_search`).
    pub fn add_embedding(
        &self,
        graph: &str,
        node_id: &str,
        embedding: Vec<f32>,
    ) -> Result<(), String> {
        let core = self.core(graph, true)?;
        core.semantic_store
            .write()
            .add_embedding(node_id.to_string(), embedding);
        Ok(())
    }

    /// k-NN semantic search over `graph`'s embeddings: `(node_id, similarity)`.
    pub fn semantic_search(
        &self,
        graph: &str,
        query_embedding: &[f32],
        n_results: usize,
    ) -> Result<Vec<(String, f32)>, String> {
        let core = self.core(graph, false)?;
        let res = core
            .semantic_store
            .read()
            .semantic_search(query_embedding, n_results);
        Ok(res)
    }

    // ── algorithms ───────────────────────────────────────────────────────

    /// An off-lock topology snapshot of `graph` for running an algorithm. Mirrors
    /// the server handler's `core.topology_snapshot()` idiom.
    fn topology_view(&self, graph: &str) -> Result<GraphView, String> {
        Ok(self.core(graph, false)?.topology_snapshot())
    }

    /// Topological sort (DAG order); `Err` on a cycle.
    pub fn topological_sort(&self, graph: &str) -> Result<Vec<String>, String> {
        crate::algorithms::topological_sort(&self.topology_view(graph)?)
    }

    /// PageRank over `graph`.
    pub fn pagerank(
        &self,
        graph: &str,
        damping: f64,
        iterations: usize,
    ) -> Result<Vec<(String, f64)>, String> {
        Ok(crate::algorithms::pagerank(
            &self.topology_view(graph)?,
            damping,
            iterations,
        ))
    }

    /// Weakly-connected components of `graph`.
    pub fn connected_components(&self, graph: &str) -> Result<Vec<Vec<String>>, String> {
        Ok(crate::algorithms::connected_components(
            &self.topology_view(graph)?,
        ))
    }

    // ── gated query surface ──────────────────────────────────────────────

    /// Run read-only SQL (`SELECT … FROM nodes/edges …`) over `graph` via the SAME
    /// DataFusion path the server's `Sql` handler uses (`eg_query::exec_sql` over an
    /// off-lock `analysis_snapshot`). Available with the `query` feature.
    #[cfg(feature = "query")]
    pub fn sql(&self, graph: &str, sql: &str) -> Result<crate::protocol::QueryResult, String> {
        let snap = self.core(graph, false)?.analysis_snapshot();
        eg_query::exec_sql(&snap, sql)
    }

    /// Run read-only Cypher (`MATCH … RETURN …`, dep-free) over `graph` via the SAME
    /// path the server's `CypherQuery` handler uses (`eg_query::exec_cypher`).
    /// Available with the `cypher` feature (the lean-Pi query surface).
    #[cfg(feature = "cypher")]
    pub fn cypher(
        &self,
        graph: &str,
        cypher: &str,
    ) -> Result<crate::protocol::QueryResult, String> {
        let snap = self.core(graph, false)?.analysis_snapshot();
        eg_query::exec_cypher(&snap, cypher)
    }

    // ── durability control ───────────────────────────────────────────────

    /// Snapshot the WHOLE registry (nodes, edges, ledger, semantic vectors) into
    /// redb in one durable transaction per the server's checkpoint discipline. A
    /// no-op for an in-memory engine. Returns the number of graphs written.
    pub fn checkpoint(&self) -> Result<usize, String> {
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            let dumps = {
                let reg = self.inner.registry.read();
                reg.all_entries()
                    .iter()
                    .map(|e| crate::redb_store::GraphDump {
                        graph: crate::redb_store::sanitize(&e.name),
                        name: e.name.clone(),
                        graph_type: e.graph_type,
                        nodes: e.core.get_nodes(),
                        edges: e.core.get_edges(),
                        ledger: e.core.get_ledger(),
                        semantic: rmp_serde::to_vec_named(&*e.core.semantic_store.read())
                            .unwrap_or_default(),
                    })
                    .collect::<Vec<_>>()
            };
            return store.checkpoint(dumps);
        }
        Ok(0)
    }

    /// Flush + checkpoint, then drop the durable handle. After `close` the engine is
    /// still usable in-memory but no longer durable. Reopening with `open` recovers
    /// the durable state. Returns the number of graphs checkpointed.
    pub fn close(self) -> Result<usize, String> {
        let n = self.checkpoint()?;
        Ok(n)
    }
}

#[cfg(all(test, feature = "redb"))]
mod tests;
