//! In-process embedded library API (CONCEPT:EG-KG.backend.engine-modes).
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
//! [`GraphCore`] methods; the durable side calls [`crate::mutation_apply::apply`] (the
//! canonical "durable `Method` → `GraphCore`" applier) + the redb row writers in
//! [`crate::redb_store`]. The embedded engine calls the **exact same** pieces:
//!
//! * in-memory mutations apply through `GraphCore` (and for the durable set,
//!   `mutation_apply::apply`, byte-identical to WAL replay / the Raft state machine); and
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
//!
//! `open()` also takes the SAME single-writer `engine.lock` `flock` guard the
//! standalone server takes (`eg_core::persist_lock`, BUG-PE-031): a second
//! `EmbeddedEngine::open()` — or a `main.rs`-launched server — against the same
//! `persist_dir` while this handle is alive is refused, not merely racy.
//!
//! ## SQLite-equivalent SQL (CONCEPT:EG-KG.storage.namespaced-kv-surface / EG-018)
//!
//! SQLite is *embedded* — its equivalence here is this in-process mode PLUS arbitrary
//! SQL user tables. On a `query` build [`EmbeddedEngine::sql_exec`] runs `CREATE
//! TABLE` / `INSERT` / `SELECT` (and `ALTER`/`DROP TABLE`) against a single-file user-
//! table store (`{persist_dir}/sql_tables.redb`) WITHOUT a server, socket, or auth —
//! open a file, create tables, insert rows, query them, durably. A `SELECT` reads the
//! user tables AND the knowledge graph in ONE DataFusion plan (the SAME path the
//! out-of-process pgwire shim serves), and the table file uses the SAME name/layout
//! the shim uses, so a table created embedded is visible out-of-process and vice-versa
//! (one store, two transports — exactly like the redb graph tier).

use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

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
    /// Serializes durable admission with publication into the live graph image.
    /// A redb commit must become durable before readers can observe its
    /// in-memory effect; the same gate preserves durable order across cloned
    /// embedded handles writing concurrently.
    write_gate: Mutex<()>,
    /// Durable redb store, present when a persist dir is configured AND `durable`.
    #[cfg(feature = "redb")]
    store: Option<EmbeddedRedbStore>,
    /// Single-writer lock on `persist_dir`, held for the lifetime of this `Inner`
    /// (i.e. until the last `EmbeddedEngine` clone drops) whenever `store` is
    /// `Some` — the embedded-transport half of BUG-PE-031: `src/main.rs`'s
    /// standalone server already refuses a second writer on the same persist dir
    /// via `persist_lock::acquire`; this closes the same class for the embedded
    /// transport by taking the SAME `eg_core::persist_lock` guard. `None` for an
    /// in-memory engine (nothing to guard).
    #[cfg(feature = "redb")]
    _persist_lock: Option<eg_core::persist_lock::PersistDirLock>,
    /// In-memory-only when there is no durable store.
    #[cfg(not(feature = "redb"))]
    _persist: Option<std::path::PathBuf>,
    /// SQLite-equivalent arbitrary user-table store (CONCEPT:EG-KG.query.register-user-tables-alongside / EG-022),
    /// present on a `query` build. A single-file `{persist_dir}/sql_tables.redb` when
    /// durable, else an ephemeral temp file (SQLite's `:memory:` analogue). `sql_exec`
    /// runs CREATE TABLE / INSERT / SELECT against it WITHOUT a server — the embedded
    /// half of the user-table SQL surface the pgwire shim serves out-of-process.
    #[cfg(feature = "query")]
    tables: eg_query::TableStore,
}

impl EmbeddedEngine {
    /// Open an embedded engine over an optional persist dir.
    ///
    /// * `Some(dir)` + `options.durable` (and the `redb` feature) ⇒ a durable
    ///   source of truth: tables are opened/created under `{dir}/graph-0.redb`, any
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
            let mut persist_lock = None;
            let store = match (&persist_dir, options.durable) {
                (Some(dir), true) => {
                    // Single-writer guard FIRST — refuse (and touch nothing) if another
                    // engine already owns this persist dir, exactly like `src/main.rs`'s
                    // standalone-server guard (BUG-PE-031).
                    persist_lock = eg_core::persist_lock::open_persist_mode(
                        eg_core::persist_lock::PersistMode::Durable(dir.clone()),
                    )?;
                    let store = EmbeddedRedbStore::open(dir)?;
                    // Replay the durable store into the fresh registry (the SAME
                    // reconstruction the server's redb load_all does).
                    let dumps = store.load_all()?;
                    {
                        let mut reg = registry.write();
                        for dump in dumps {
                            // `GraphRegistry::new` seeds an in-memory
                            // `__commons__` placeholder so in-memory callers
                            // can use it immediately. A durable commons dump
                            // is a committed image, however, and must replace
                            // that placeholder so its authoritative
                            // Graph(version) is adopted before publication.
                            // Replaying rows into the bootstrap core leaves
                            // its version at zero and makes the next
                            // checkpoint look stale.
                            if dump.name == "__commons__" {
                                let semantic_store = if dump.semantic.is_empty() {
                                    crate::compute::semantic::SemanticStore::new()
                                } else {
                                    rmp_serde::from_slice::<
                                        crate::compute::semantic::SemanticStore,
                                    >(&dump.semantic)
                                    .map_err(|error| {
                                        format!(
                                            "failed to decode durable __commons__ semantic store: {error}"
                                        )
                                    })?
                                };
                                let snapshot = crate::graph::GraphSnapshot {
                                    schema_version: crate::graph::GRAPH_SNAPSHOT_SCHEMA_VERSION,
                                    integrity_policy: dump.integrity_policy,
                                    nodes: dump
                                        .nodes
                                        .into_iter()
                                        .map(|(id, properties)| (id, Arc::new(properties)))
                                        .collect(),
                                    edges: dump
                                        .edges
                                        .into_iter()
                                        .map(|(source, target, properties)| {
                                            (source, target, Arc::new(properties))
                                        })
                                        .collect(),
                                    ledger: dump.ledger,
                                    semantic_store,
                                };
                                reg.install_committed_graph(
                                    "__commons__",
                                    dump.graph_type,
                                    None,
                                    dump.incarnation_id,
                                    snapshot,
                                    dump.source_snapshot_version,
                                )?;
                                continue;
                            }
                            if !reg.exists(&dump.name) {
                                let _ = reg.create_graph_with_incarnation(
                                    &dump.name,
                                    dump.graph_type,
                                    None,
                                    dump.incarnation_id.clone(),
                                    dump.source_snapshot_version,
                                );
                            }
                            if let Some(core) = reg.get(&dump.name).map(|e| e.core.clone()) {
                                core.install_integrity_policy(dump.integrity_policy.clone());
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
            #[cfg(feature = "query")]
            let tables = Self::open_table_store(persist_dir.as_deref(), options.durable)?;
            Ok(Self {
                inner: Arc::new(Inner {
                    registry,
                    write_gate: Mutex::new(()),
                    store,
                    _persist_lock: persist_lock,
                    #[cfg(feature = "query")]
                    tables,
                }),
            })
        }

        #[cfg(not(feature = "redb"))]
        {
            #[cfg(feature = "query")]
            let tables = Self::open_table_store(persist_dir.as_deref(), options.durable)?;
            #[cfg(not(feature = "query"))]
            let _ = options;
            Ok(Self {
                inner: Arc::new(Inner {
                    registry: RwLock::new(GraphRegistry::new()),
                    write_gate: Mutex::new(()),
                    _persist: persist_dir,
                    #[cfg(feature = "query")]
                    tables,
                }),
            })
        }
    }

    /// Open the SQLite-equivalent user-table store (CONCEPT:EG-KG.query.register-user-tables-alongside / EG-022): a
    /// single-file `{persist_dir}/sql_tables.redb` when durable (the SAME filename the
    /// pgwire shim uses, so a table created embedded is visible out-of-process and
    /// vice-versa), else an ephemeral temp file (the `:memory:` analogue).
    #[cfg(feature = "query")]
    fn open_table_store(
        persist_dir: Option<&Path>,
        durable: bool,
    ) -> Result<eg_query::TableStore, String> {
        match (persist_dir, durable) {
            (Some(dir), true) => {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
                // RF-RULING-004: the storage kernel never interprets proof
                // bytes -- the composition root decides which principal may
                // serve this file's scopes, and for the embedded API this
                // library IS that root.
                let authority = crate::store_authority::process_authority();
                eg_query::TableStore::open(
                    dir.join("sql_tables.redb"),
                    crate::store_authority::process_verifier(),
                    authority.principal(),
                    &authority.proof(),
                )
            }
            // The `:memory:` analogue is still opened by the composition root,
            // not by `eg-query`'s `dev-scope-grant` stand-in: ephemeral is a
            // lifetime, not a weaker authority.
            _ => crate::store_authority::open_ephemeral_sql_store().map(|(store, _path)| store),
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
        let _write_gate = self.inner.write_gate.lock();
        self.create_graph_inner(name, graph_type)
    }

    /// Create a graph while the caller already holds `write_gate`.
    fn create_graph_inner(&self, name: &str, graph_type: GraphType) -> Result<(), String> {
        // Keep the registry write guard through durable registration. The write
        // gate serializes writers, but readers use the registry lock directly;
        // dropping this guard before the shard commit would expose a transient
        // RAM-only graph to a concurrent reader.
        let mut registry = self.inner.registry.write();
        registry.create_graph(name, graph_type, None)?;
        let incarnation_id = registry
            .catalog_record(name)
            .map(|record| record.incarnation_id)
            .ok_or_else(|| "created graph is absent from lifecycle catalog".to_string())?;
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            if let Err(error) = store.register_graph(
                &crate::redb_store::sanitize(name),
                name,
                graph_type,
                &incarnation_id,
            ) {
                // The registry is the live projection, while the shard is the
                // durable authority. Registration can fail after the registry
                // has admitted the name, so roll that projection back while the
                // lifecycle write gate is still held. A failed create must be
                // absent and retryable, rather than becoming a RAM-only graph.
                let rollback = registry.delete_graph(name);
                return match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(format!(
                        "{error}; failed to roll back in-memory graph registration: {rollback_error}"
                    )),
                };
            }
        }
        Ok(())
    }

    /// Delete a named graph (cannot delete `__commons__`).
    ///
    /// Under the durable store this also PURGES the graph's redb rows (nodes/edges/
    /// ledger/semantic + the `graph_meta` identity, CONCEPT:EG-KG.backend.tenant-delete-recreate-same) so a recreate
    /// of the SAME name starts from a clean durable slate instead of inheriting the
    /// deleted incarnation's rows on the next `load_all` — the embedded analogue of
    /// the server's tenant-delete teardown.
    pub fn delete_graph(&self, name: &str) -> Result<(), String> {
        let _write_gate = self.inner.write_gate.lock();
        // Validate the live projection before touching durable rows. Once the
        // graph is known, purge the authoritative shard first; only then remove
        // the registry entry. A failed purge therefore leaves both projections
        // intact and the same delete can be retried.
        {
            let registry = self.inner.registry.read();
            if name == "__commons__" {
                return Err("Cannot delete the __commons__ graph".to_string());
            }
            if !registry.exists(name) {
                return Err(format!("Graph '{name}' not found"));
            }
        }
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            store.purge(&crate::redb_store::sanitize(name))?;
        }
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

    /// Apply a durable mutation to `graph`: admit and commit it through the
    /// authoritative shard first, then publish it through the SAME
    /// `mutation_apply::apply` path WAL replay and the Raft state machine use.
    /// The graph is auto-created if it does not exist (matching the firehose
    /// ingestion path), and the call returns only after the durable commit.
    fn apply_durable(&self, graph: &str, method: Method) -> Result<(), String> {
        let _write_gate = self.inner.write_gate.lock();

        // `mutation_apply::apply` is deliberately a void replay applier and
        // logs an AddEmbedding validation error rather than returning it. Do
        // the exact backend validation on a detached store before graph
        // creation, durable admission, and the serving watermark can change.
        // This keeps the embedded API's historical error surface while making
        // an invalid vector atomic in both durable and in-memory modes.
        if let Method::AddEmbedding { embedding, .. } = &method {
            let validation = match self.core(graph, false) {
                Ok(core) => core.semantic_store.read().validate_embedding(embedding),
                Err(_) => {
                    crate::compute::semantic::SemanticStore::new().validate_embedding(embedding)
                }
            };
            validation.map_err(|error| error.to_string())?;
        }

        let core = match self.core(graph, false) {
            Ok(core) => core,
            Err(_) => {
                self.create_graph_inner(graph, GraphType::Global)?;
                self.core(graph, false)?
            }
        };
        // 1) Durable typed admission/commit. The shard's graph member resolves
        // its authoritative Graph(version) while the kernel write is held.
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            store.commit(&crate::redb_store::sanitize(graph), &method)?;
        }
        // 2) Publish only after the durable commit succeeds. This is the
        // canonical durable Method → GraphCore applier used by WAL replay and
        // the Raft state machine, so a failed admission never becomes visible.
        crate::mutation_apply::apply(&core, &method);
        // The replay applier deliberately only applies rows to a core; it does
        // not advance the serving OCC clock. The embedded write gate owns the
        // publication boundary, so advance that clock exactly once after the
        // authoritative commit and projection apply. Keeping this watermark in
        // step with the shard's Graph(version) is required for checkpoint
        // images while retaining the durable stale-image refusal.
        core.mark_dirty();
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
        self.apply_durable(
            graph,
            Method::AddEmbedding {
                node_id: node_id.to_string(),
                embedding,
            },
        )
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
        eg_query::exec_sql(&snap, sql, &eg_query::CancellationToken::new())
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

    /// Execute a SQL statement SQLite-style, in-process, WITHOUT a server
    /// (CONCEPT:EG-KG.storage.namespaced-kv-surface / EG-018). This is the embedded equivalence to SQLite: open a
    /// file → `CREATE TABLE` / `INSERT` / `SELECT` over arbitrary user tables, durably,
    /// with no socket and no auth.
    ///
    /// * `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` mutate the single-file user-
    ///   table store (`{persist_dir}/sql_tables.redb`).
    /// * `INSERT INTO <table> … VALUES …` durably appends rows (commit-before-return).
    /// * `SELECT …` runs over the graph snapshot AND the user tables in ONE DataFusion
    ///   plan (the SAME path the pgwire shim serves), so a SELECT can read a user table,
    ///   JOIN it to the graph, or both.
    ///
    /// Node-graph DML (`INSERT INTO nodes …`, `UPDATE/DELETE` over the graph) is NOT
    /// handled here — use the typed `add_node`/`add_edge`/`remove_*` methods for the
    /// graph. Available with the `query` feature.
    #[cfg(feature = "query")]
    pub fn sql_exec(&self, graph: &str, sql: &str) -> Result<eg_query::TypedQueryResult, String> {
        use eg_query::StatementKind;
        let store = &self.inner.tables;
        match eg_query::classify(sql)? {
            // SELECT / WITH: the read path over the graph snapshot + the user tables.
            // Auto-create the (possibly empty) graph so a table-only SELECT works
            // without a pre-existing graph, matching the firehose write path.
            StatementKind::Read => {
                let snap = self.core(graph, true)?.analysis_snapshot();
                eg_query::exec_sql_typed_with_tables(&snap, store, sql)
            }
            StatementKind::CreateTable(plan) => {
                let columns = to_store_columns(&plan.columns)?;
                let schema = eg_query::TableSchema::new(plan.name, columns);
                store.create_table(&schema, plan.if_not_exists)?;
                Ok(status_result("CREATE TABLE"))
            }
            StatementKind::DropTable(plan) => {
                store.drop_table(&plan.name, plan.if_exists)?;
                Ok(status_result("DROP TABLE"))
            }
            // CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog the rest — one dispatch helper.
            StatementKind::AlterTable(plan) => {
                apply_alter_table(store, plan)?;
                Ok(status_result("ALTER TABLE"))
            }
            StatementKind::InsertTable(ins) => {
                let n = store.insert_rows(&ins.table, &ins.columns, &ins.rows)?;
                Ok(count_result(n))
            }
            // CONCEPT:EG-KG.query.create-drop-function — CREATE/DROP FUNCTION over the durable function catalog; a
            // later SELECT fn(args)/FROM fn(args) expands it on the read path above.
            StatementKind::CreateFunction(plan) => {
                store.create_function(&plan.func, plan.or_replace)?;
                Ok(status_result("CREATE FUNCTION"))
            }
            StatementKind::DropFunction(plan) => {
                store.drop_function(&plan.name, plan.if_exists)?;
                Ok(status_result("DROP FUNCTION"))
            }
            other => Err(format!(
                "embedded sql_exec supports user-table DDL/DML (CREATE/ALTER/DROP \
                 TABLE, INSERT … VALUES) + SELECT; for node-graph mutations use the \
                 typed add_node/add_edge/remove_* methods (statement: {other:?})"
            )),
        }
    }

    // ── durability control ───────────────────────────────────────────────

    /// Snapshot the WHOLE registry (nodes, edges, ledger, semantic vectors) into
    /// redb in one durable transaction per the server's checkpoint discipline. A
    /// no-op for an in-memory engine. Returns the number of graphs written.
    pub fn checkpoint(&self) -> Result<usize, String> {
        let _write_gate = self.inner.write_gate.lock();
        #[cfg(feature = "redb")]
        if let Some(store) = &self.inner.store {
            let dumps = {
                let reg = self.inner.registry.read();
                reg.all_entries()
                    .iter()
                    .map(|e| {
                        crate::redb_store::GraphDump::in_place_core_checkpoint(
                            crate::redb_store::InPlaceCoreCheckpoint {
                                graph: crate::redb_store::sanitize(&e.name),
                                name: e.name.clone(),
                                graph_type: e.graph_type,
                                incarnation_id: e.incarnation_id.clone(),
                                source_snapshot_version: e.core.version(),
                                integrity_policy: e.core.integrity_policy(),
                                nodes: e.core.get_nodes(),
                                edges: e.core.get_edges(),
                                ledger: e.core.get_ledger(),
                                semantic: rmp_serde::to_vec_named(&*e.core.semantic_store.read())
                                    .unwrap_or_default(),
                            },
                        )
                    })
                    .collect::<Vec<_>>()
            };
            let count = store.checkpoint(dumps)?;
            // The checkpoint itself is an admitted owner-maintenance write, so
            // the shard advances every resident graph's authoritative version
            // once after replacing its image. Keep the live projection's OCC
            // watermark aligned with that maintenance commit; otherwise a
            // second checkpoint on this handle would be rejected as stale even
            // though its image was just written successfully.
            let reg = self.inner.registry.read();
            for entry in reg.all_entries() {
                entry.core.mark_dirty();
            }
            return Ok(count);
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

/// Resolve classify `ColumnDef`s (raw SQL type spellings) into store [`Column`]s
/// (CONCEPT:EG-KG.query.register-user-tables-alongside). Mirrors the pgwire shim's `to_store_columns`, but on the public
/// eg-query API so the embedded path adds no cross-crate coupling.
#[cfg(feature = "query")]
fn to_store_columns(cols: &[eg_query::ColumnDef]) -> Result<Vec<eg_query::Column>, String> {
    cols.iter()
        .map(|c| {
            let ty = eg_query::ColumnType::parse(&c.type_name)?;
            Ok(eg_query::Column {
                name: c.name.clone(),
                ty,
                nullable: c.nullable,
                primary_key: c.primary_key,
                unique: c.unique,
                serial: c.serial,
                default: c.default.clone(),
                check: c.check.clone(),
            })
        })
        .collect()
}

/// Route a decoded `ALTER TABLE` action to the matching durable `TableStore` mutation
/// (CONCEPT:EG-KG.query.register-user-tables-alongside ADD COLUMN + CONCEPT:EG-KG.query.rename-table-moves-catalog DROP/RENAME COLUMN, RENAME TABLE, ALTER
/// COLUMN TYPE, DROP CONSTRAINT). The single facade mapping the embedded path reuses.
#[cfg(feature = "query")]
fn apply_alter_table(
    store: &eg_query::TableStore,
    plan: eg_query::AlterTablePlan,
) -> Result<(), String> {
    use eg_query::AlterTableAction as A;
    match plan.action {
        A::AddColumn(col) => {
            let columns = to_store_columns(std::slice::from_ref(&col))?;
            let column = columns.into_iter().next().expect("one column");
            store.add_column(&plan.name, column)
        }
        A::DropColumn { column, if_exists } => store.drop_column(&plan.name, &column, if_exists),
        A::RenameColumn { from, to } => store.rename_column(&plan.name, &from, &to),
        A::RenameTable { new_name } => store.rename_table(&plan.name, &new_name),
        A::AlterColumnType { column, new_type } => {
            let ty = eg_query::ColumnType::parse(&new_type)?;
            store.alter_column_type(&plan.name, &column, ty)
        }
        A::DropConstraint {
            constraint,
            if_exists,
        } => store.drop_constraint(&plan.name, &constraint, if_exists),
    }
}

/// A one-row `status` result for a DDL statement (CREATE/ALTER/DROP TABLE).
#[cfg(feature = "query")]
fn status_result(tag: &str) -> eg_query::TypedQueryResult {
    eg_query::TypedQueryResult {
        columns: vec![eg_query::TypedColumn {
            name: "status".to_string(),
            ty: eg_query::PgColType::Text,
        }],
        rows: vec![vec![serde_json::Value::String(tag.to_string())]],
    }
}

/// A one-row `inserted` count result for an INSERT statement.
#[cfg(feature = "query")]
fn count_result(n: usize) -> eg_query::TypedQueryResult {
    eg_query::TypedQueryResult {
        columns: vec![eg_query::TypedColumn {
            name: "inserted".to_string(),
            ty: eg_query::PgColType::Int8,
        }],
        rows: vec![vec![serde_json::Value::from(n as i64)]],
    }
}

#[cfg(all(test, feature = "redb"))]
mod tests;

#[cfg(all(test, feature = "redb"))]
mod lifecycle_failure_tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::mpsc::{self, TryRecvError};
    use std::thread;

    fn temp_dir(tag: &str) -> PathBuf {
        crate::test_support::temp_dir("eg-embedded-lifecycle", tag)
    }

    fn has_graph(engine: &EmbeddedEngine, name: &str) -> bool {
        engine.list_graphs().iter().any(|(graph, _)| graph == name)
    }

    #[test]
    fn invalid_embedding_does_not_mutate_in_memory_projection_or_version() {
        let engine =
            EmbeddedEngine::open(None::<&std::path::Path>, EmbeddedOptions::in_memory()).unwrap();
        engine.create_graph("g", GraphType::Global).unwrap();
        engine.add_embedding("g", "n", vec![1.0, 0.0, 0.0]).unwrap();

        let core = engine.inner.registry.read().get("g").unwrap().core.clone();
        let version_before = core.version();
        let embedding_before = core.semantic_store.read().get_embedding("n");
        let error = engine.add_embedding("g", "n", vec![1.0, 0.0]).unwrap_err();

        assert!(error.contains("embedding dimension mismatch"), "{error}");
        assert_eq!(core.version(), version_before);
        assert_eq!(
            core.semantic_store.read().get_embedding("n"),
            embedding_before
        );
    }

    #[test]
    fn checkpoint_reopen_checkpoint_adopts_commons_version() {
        let dir = temp_dir("commons-checkpoint-reopen");
        {
            let engine = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
            engine
                .add_node(
                    "__commons__",
                    "n",
                    rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
                )
                .unwrap();
            engine.checkpoint().unwrap();
        }

        let reopened = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        reopened
            .checkpoint()
            .expect("a recovered commons image must be current for the next checkpoint");
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_create_is_not_published_before_failure_and_retry_survives_reopen() {
        let dir = temp_dir("create-register-failure");
        let engine = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        let (registration_entered, registration_release) = engine
            .inner
            .store
            .as_ref()
            .unwrap()
            .block_next_register_failure();
        let creator_engine = engine.clone();
        let creator = thread::spawn(move || creator_engine.create_graph("g", GraphType::Global));
        registration_entered.recv().unwrap();

        let (reader_started_tx, reader_started_rx) = mpsc::sync_channel(1);
        let (reader_done_tx, reader_done_rx) = mpsc::sync_channel(1);
        let reader_engine = engine.clone();
        let reader = thread::spawn(move || {
            reader_started_tx.send(()).unwrap();
            reader_done_tx.send(has_graph(&reader_engine, "g")).unwrap();
        });
        reader_started_rx.recv().unwrap();

        // The registration hook is blocked while create_graph_inner holds the
        // registry write guard. A reader cannot observe the uncommitted graph.
        assert!(engine.inner.registry.try_read().is_none());
        assert!(matches!(
            reader_done_rx.try_recv(),
            Err(TryRecvError::Empty)
        ));

        registration_release.send(()).unwrap();
        let error = creator.join().unwrap().unwrap_err();
        assert!(error.contains("injected embedded graph registration failure"));
        assert!(!has_graph(&engine, "g"));
        assert!(!reader_done_rx.recv().unwrap());
        reader.join().unwrap();

        // The failed admission left no live projection, so the exact same
        // lifecycle request is still valid and can complete.
        engine.create_graph("g", GraphType::Global).unwrap();
        assert!(has_graph(&engine, "g"));
        drop(engine);

        let reopened = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        assert!(has_graph(&reopened, "g"));
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_delete_keeps_projection_and_retry_purges_across_reopen() {
        let dir = temp_dir("delete-purge-failure");
        {
            let engine = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
            engine.create_graph("g", GraphType::Global).unwrap();
            engine
                .add_node(
                    "g",
                    "n",
                    rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
                )
                .unwrap();
            engine.inner.store.as_ref().unwrap().fail_next_purge();

            let error = engine.delete_graph("g").unwrap_err();
            assert!(error.contains("injected embedded graph purge failure"));
            assert!(has_graph(&engine, "g"));
            assert!(engine.has_node("g", "n").unwrap());
        }

        // The failed purge did not remove the durable identity or payload.
        {
            let reopened = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
            assert!(has_graph(&reopened, "g"));
            assert!(reopened.has_node("g", "n").unwrap());
        }

        {
            let engine = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
            engine.delete_graph("g").unwrap();
            assert!(!has_graph(&engine, "g"));
        }

        let reopened = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        assert!(!has_graph(&reopened, "g"));
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
