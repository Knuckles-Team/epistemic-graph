use super::*;
use crate::protocol::GraphType;
use crate::redb_store::{read_all_dumps, read_all_graph_meta};
use crate::server::ServerState;

impl RedbBackend {
    /// Reconstruct every graph from the redb store into the registry. The actual
    /// DB read runs on the owner thread (via the `Load` command) because redb holds
    /// an exclusive per-process file lock; this rebuilds each `GraphCore` from the
    /// returned dumps via the SAME `add_node`/`add_edge` calls the WAL replay uses.
    pub(super) async fn load_into(
        &self,
        state: &Arc<RwLock<ServerState>>,
    ) -> Result<usize, String> {
        let dumps = self.load_graph_dumps().await?;
        let mut count = 0usize;
        for dump in dumps {
            if let Some(core) = self.load_graph_core(state, &dump).await {
                restore_graph_dump(&core, dump)?;
                count += 1;
            }
        }
        Ok(count)
    }

    // PARALLEL cross-shard read fan-out (CONCEPT:AU-KG.backend.roadmap-f-parallel-cross,
    // roadmap F). Every shard captures its MVCC snapshot on the blocking pool before
    // this function awaits, so independent shard reads overlap without using a writer
    // thread or forcing a group commit.
    async fn load_graph_dumps(&self) -> Result<Vec<GraphDump>, String> {
        let mut tasks = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            #[cfg(feature = "security")]
            let cipher = writer.cipher.clone();
            tasks.push(move || {
                #[cfg(feature = "security")]
                let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
                #[cfg(not(feature = "security"))]
                let crypto = crate::redb_store::DurableCrypto::none();
                read_all_dumps(&shard, crypto)
            });
        }
        Ok(join_blocking_in_order(tasks)
            .await?
            .into_iter()
            .flatten()
            .collect())
    }

    async fn load_graph_core(
        &self,
        state: &Arc<RwLock<ServerState>>,
        dump: &GraphDump,
    ) -> Option<Arc<GraphCore>> {
        let mut s = state.write().await;
        if !s.registry.exists(&dump.name) {
            let _ = s.registry.create_graph_with_incarnation(
                &dump.name,
                dump.graph_type,
                None,
                dump.incarnation_id.clone(),
                dump.source_snapshot_version,
            );
        }
        s.registry
            .get_mut(&dump.name)
            .map(|entry| entry.core.clone())
    }

    /// Populate the registry's CATALOG from every shard's `graph_meta` table ONLY
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — NO node/edge/ledger/semantic row is
    /// read. Mirrors `load_into`'s parallel cross-shard fan-out (each shard's
    /// cheap meta scan runs concurrently on the blocking pool) but with a vastly
    /// smaller per-shard read: one small `{name, graph_type}` table instead of
    /// four. Catalog rows are registered through the registry's `DashMap`, but
    /// startup also has to reconcile the synthetic `__commons__` placeholder
    /// seeded by `GraphRegistry::new` with its durable incarnation. Take one
    /// bounded write lock for that reconciliation, then retain a shared lock
    /// for the bulk catalog scan rather than leaving an empty resident core that
    /// would shadow lazy materialization after restart.
    pub(super) async fn load_catalog_into(
        &self,
        state: &Arc<RwLock<ServerState>>,
    ) -> Result<usize, String> {
        // Write-new half of the one-time graph_meta migration, before the read
        // below. A store written by a pre-versioned build has rows the current
        // record cannot decode; `read_all_graph_meta` can now READ them via the
        // legacy fallback, but leaving them on disk in the old shape would mean
        // taking that path on every subsequent open. Converting here makes the
        // fallback genuinely one-time (and is a no-op on an already-current
        // store, so it costs one read pass per shard at startup and nothing else).
        let mut upgrade_tasks = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            upgrade_tasks.push(move || crate::redb_store::upgrade_legacy_graph_meta(&shard));
        }
        let upgraded: usize = join_blocking_in_order(upgrade_tasks)
            .await?
            .into_iter()
            .sum();
        if upgraded > 0 {
            tracing::info!(
                "redb: migrated {upgraded} graph metadata row(s) from the pre-versioned \
                 format to schema v{}",
                crate::redb_store::graph_meta_schema_version()
            );
        }

        let mut tasks = Vec::with_capacity(self.shards.len());
        for writer in &self.shards {
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            tasks.push(move || read_all_graph_meta(&shard));
        }
        let rows: Vec<(String, String, GraphType, String)> = join_blocking_in_order(tasks)
            .await?
            .into_iter()
            .flatten()
            .collect();

        let count = rows.len();
        if let Some((_, name, graph_type, incarnation_id)) =
            rows.iter().find(|(_, name, _, _)| name == "__commons__")
        {
            let mut s = state.write().await;
            s.registry.reconcile_bootstrap_catalog_entry(
                name,
                *graph_type,
                None,
                incarnation_id.clone(),
            );
        }
        let s = state.read().await;
        for (_fname, name, graph_type, incarnation_id) in rows {
            s.registry.register_catalog_only_with_incarnation(
                &name,
                graph_type,
                None,
                incarnation_id,
            );
        }
        Ok(count)
    }

    /// Enqueue one writer command for `graph_fname`.
    ///
    /// When a tenant catalog is attached, the routing-quiesce READ guard is held
    /// across both the shard resolve and the send, so an online reshard cannot flip
    /// the route between the two (no lost or misrouted write). With no catalog —
    /// the default — there is no guard and this is the plain EG-026 send.
    pub(super) async fn enqueue(
        &self,
        graph_fname: &str,
        cmd: Cmd,
        what: &str,
    ) -> Result<(), String> {
        let routing = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let tx = self.shard_for(graph_fname).tx.clone();
        tokio::task::spawn_blocking(move || {
            let _routing = routing;
            tx.send(cmd).map_err(|_| ())
        })
        .await
        .map_err(|error| format!("{what} join error: {error}"))?
        .map_err(|_| "redb writer thread is gone".to_string())
    }

    /// Run one typed MVCC read on Tokio's blocking pool. Redb snapshot reads
    /// are independent of the writer channel, but opening a snapshot and
    /// decoding rows are still synchronous work; keeping that shell here makes
    /// every typed adapter off-reactor without hiding its reader-specific
    /// arguments or return type. When a tenant catalog is attached, retain the
    /// routing read guard from shard resolution through the snapshot so an
    /// online reshard cannot flip and purge the selected shard while this read
    /// is waiting for the blocking pool.
    ///
    /// A private helper shared by several `PersistenceBackend` trait method
    /// implementations below (NOT itself a trait method — `PersistenceBackend`
    /// declares no generic methods, so this stays in `RedbBackend`'s own
    /// inherent impl; Rust method resolution finds it from `self.read_snapshot(...)`
    /// regardless of which impl block it lives in).
    pub(super) async fn read_snapshot<T, F>(&self, graph_fname: &str, read: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a Shard, crate::redb_store::DurableCrypto<'a>) -> Result<T, String>
            + Send
            + 'static,
    {
        let routing_guard = if self.catalog.is_some() {
            Some(self.routing_epoch.clone().read_owned().await)
        } else {
            None
        };
        let writer = self.shard_for(graph_fname);
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        #[cfg(feature = "security")]
        let cipher = writer.cipher.clone();
        tokio::task::spawn_blocking(move || {
            let _routing_guard = routing_guard;
            #[cfg(feature = "security")]
            let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
            #[cfg(not(feature = "security"))]
            let crypto = crate::redb_store::DurableCrypto::none();
            read(shard.as_ref(), crypto)
        })
        .await
        .map_err(|error| format!("redb snapshot read join error: {error}"))?
    }
}

fn restore_graph_dump(core: &GraphCore, dump: GraphDump) -> Result<(), String> {
    // A binary upgrade atomically reconciles the immutable core catalog during
    // decode.  Validate that new core together with the persisted dynamic
    // sources before publishing even the durable watermark: an old attachment
    // that conflicts with a newly-owned core term must make startup fail closed,
    // never expose a partially upgraded graph.
    #[cfg(feature = "shacl")]
    crate::server::graph_schema::compose::validate_and_compose(&dump.schema_sources)?;
    // `GraphRegistry::new` pre-creates `__commons__`, so its fresh projection does
    // not pass through `create_graph_with_incarnation`. Adopt the durable watermark
    // before replaying rows, while leaving an already materialized projection alone.
    if dump.source_snapshot_version > 0 && core.version() == 0 {
        core.adopt_materialized_version(dump.source_snapshot_version)?;
    }
    // Rebuild through the same add_node/add_edge calls used by WAL replay. The
    // durable ledger is a mirror and therefore is intentionally not replayed here.
    core.install_schema_sources(dump.schema_sources);
    for (id, props) in dump.nodes {
        core.add_node(id, props);
    }
    for (src, tgt, props) in dump.edges {
        let _ = core.add_edge(src, tgt, props);
    }
    if !dump.semantic.is_empty() {
        if let Ok(store) = decode_durable_semantic(&dump.semantic) {
            *core.semantic_store.write() = store;
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "shacl"))]
mod graph_schema_restart_tests {
    use super::*;

    #[test]
    fn conflicting_binary_core_upgrade_is_an_atomic_restart_refusal() {
        let core = GraphCore::new();
        core.add_node("live".to_string(), vec![1, 2, 3]);
        let before_sources = core.schema_sources();
        let before_version = core.version();

        let mut sources = crate::graph::GraphSchemaSources::default();
        sources
            .attach_dynamic(
                "admin:old-release".to_string(),
                crate::graph::GraphSchemaSource::new(
                    crate::graph::SchemaSourceOrigin::Admin {
                        name: "old-release".to_string(),
                    },
                    None,
                    Some(std::sync::Arc::from(
                        "@prefix owl: <http://www.w3.org/2002/07/owl#> . \
                         @prefix eg: <http://knuckles.team/kg#> . \
                         eg:Tool a owl:ObjectProperty .",
                    )),
                    0,
                )
                .unwrap(),
            )
            .unwrap();
        let dump = GraphDump {
            kind: crate::redb_store::GraphDumpKind::DurableReadOnlyMaterialization,
            graph: "g".to_string(),
            name: "g".to_string(),
            graph_type: GraphType::Global,
            incarnation_id: "inc".to_string(),
            source_snapshot_version: 9,
            schema_sources: std::sync::Arc::new(sources),
            nodes: Vec::new(),
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic: Vec::new(),
            native: crate::redb_store::NativeOperationDumpRows::default(),
        };

        let error = restore_graph_dump(&core, dump).unwrap_err();
        assert!(error.contains("SCHEMA_SOURCE_CONFLICT"), "{error}");
        assert!(core.node_properties.contains_key("live"));
        assert_eq!(core.version(), before_version);
        assert_eq!(core.schema_sources(), before_sources);
    }
}
