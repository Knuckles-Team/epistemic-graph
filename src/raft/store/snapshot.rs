use super::*;

impl EgStore {
    /// Dump THIS group's graphs for a snapshot (CONCEPT:AU-KG.ingest.staged). When the store runs
    /// under a [`super::super::multi::MultiRaft`] its ctx carries the router, so the dump is
    /// SCOPED to graphs whose tenant range resolves to this group — a large tenant in
    /// one group never bloats another group's snapshot. Enumeration uses the complete
    /// durable catalog, not only resident cores, so an evicted cold graph remains part
    /// of the state-machine image. Without a router (a direct single-store open) the
    /// whole registry is dumped (the unscoped scaffold path).
    pub(super) async fn dump_graphs(&self) -> Result<Vec<GraphSnapshot>, String> {
        let identities: Vec<(String, GraphType, String)> = {
            let s = self.ctx.state.read().await;
            s.registry
                .list()
                .into_iter()
                .filter(|(name, _)| match &self.ctx.router {
                    Some(router) => router.group_of(name) == self.group_id,
                    None => true,
                })
                .map(|(name, graph_type)| {
                    let fname = crate::persist::sanitize(&name);
                    (name, graph_type, fname)
                })
                .collect()
        };
        let mut graphs = Vec::with_capacity(identities.len());
        for (expected_name, expected_type, fname) in identities {
            let graph = GraphSnapshot {
                schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
                durable: self.redb().export_graph_raw_for_snapshot(&fname).await?,
                fname,
            };
            let (durable_name, durable_type, _) = graph.validate_and_identity()?;
            if durable_name != expected_name || durable_type != expected_type {
                return Err(format!(
                    "Raft snapshot registry identity for '{}' disagrees with durable authority",
                    expected_name
                ));
            }
            graphs.push(graph);
        }
        Ok(graphs)
    }

    /// Test-only: the sorted graph NAMES this group's snapshot would capture, AFTER
    /// per-group scoping (CONCEPT:AU-KG.ingest.staged). Lets a test assert a group's snapshot
    /// carries ONLY its own tenant-range graphs without reaching into private types.
    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_graph_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .dump_graphs()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|g| g.validate_and_identity().ok().map(|identity| identity.0))
            .collect();
        names.sort();
        names
    }

    pub(super) fn validate_snapshot_graphs(
        &self,
        graphs: &[GraphSnapshot],
    ) -> Result<Vec<(String, GraphType, String)>, String> {
        let mut names = std::collections::BTreeSet::new();
        let mut fnames = std::collections::BTreeSet::new();
        let mut identities = Vec::with_capacity(graphs.len());
        for graph in graphs {
            let (name, graph_type, incarnation_id) = graph.validate_and_identity()?;
            if !names.insert(name.clone()) || !fnames.insert(graph.fname.clone()) {
                return Err("Raft graph snapshot identity is invalid or duplicated".to_string());
            }
            if self
                .ctx
                .router
                .as_ref()
                .is_some_and(|router| router.group_of(&name) != self.group_id)
            {
                return Err("Raft graph snapshot contains a graph from another group".to_string());
            }
            identities.push((name, graph_type, incarnation_id));
        }
        Ok(identities)
    }

    /// Rebuild graphs from a snapshot into the registry + M2 store.
    pub(super) async fn install_graphs(&self, graphs: &[GraphSnapshot]) -> Result<(), String> {
        let identities = self.validate_snapshot_graphs(graphs)?;
        let names: std::collections::BTreeSet<&str> = identities
            .iter()
            .map(|(name, _, _)| name.as_str())
            .collect();
        let stale_names = self.stale_snapshot_graph_names(&names).await?;
        for (graph, identity) in graphs.iter().zip(&identities) {
            self.install_snapshot_graph(graph, identity).await?;
        }
        for name in stale_names {
            self.remove_stale_snapshot_graph(&name).await?;
        }
        Ok(())
    }

    async fn stale_snapshot_graph_names<'a>(
        &self,
        names: &std::collections::BTreeSet<&'a str>,
    ) -> Result<Vec<String>, String> {
        // Identify stale graph authority before the first durable import. Installing
        // a Raft snapshot is replacement, not a merge: graphs in this group but
        // absent from the committed image must not survive a rollback/rejoin.
        let s = self.ctx.state.read().await;
        let stale: Vec<String> = s
            .registry
            .list()
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| {
                let belongs_to_group = match &self.ctx.router {
                    Some(router) => router.group_of(name) == self.group_id,
                    None => true,
                };
                belongs_to_group && !names.contains(name.as_str())
            })
            .collect();
        if stale.iter().any(|name| name == "__commons__") {
            return Err("Raft snapshot omits the mandatory commons graph".to_string());
        }
        Ok(stale)
    }

    async fn install_snapshot_graph(
        &self,
        graph: &GraphSnapshot,
        identity: &(String, GraphType, String),
    ) -> Result<(), String> {
        let (name, graph_type, incarnation_id) = identity;
        self.redb()
            .import_graph_raw_from_snapshot(&graph.fname, graph.durable.clone())
            .await?;
        let (snapshot, version) = self
            .backend
            .read_authoritative_graph_snapshot(&graph.fname)
            .await?
            .ok_or_else(|| format!("snapshot graph '{}' has no durable image", name))?;
        let mut s = self.ctx.state.write().await;
        let owner = s.registry.catalog_record(name).and_then(|record| {
            if record.incarnation_id == incarnation_id.as_str() {
                record.owner
            } else {
                None
            }
        });
        let core = s.registry.install_committed_graph(
            name,
            *graph_type,
            owner,
            incarnation_id.clone(),
            snapshot,
            version,
        )?;
        s.write_coalescer.remove(name);
        s.routed_write_coalescer.remove(name);
        s.per_graph_inflight.remove(name);
        #[cfg(feature = "redb")]
        s.cold_tracker.forget(name);
        crate::metrics::set_graph_size(
            name,
            i64::try_from(core.node_count()).unwrap_or(i64::MAX),
            i64::try_from(core.edge_count()).unwrap_or(i64::MAX),
        );
        Ok(())
    }

    async fn remove_stale_snapshot_graph(&self, name: &str) -> Result<(), String> {
        let fname = crate::persist::sanitize(name);
        self.redb()
            .import_graph_raw_from_snapshot(
                &fname,
                crate::server::persistence::online_reshard::RawGraphRows::default(),
            )
            .await?;
        let mut s = self.ctx.state.write().await;
        if s.registry.exists(name) {
            s.registry.delete_graph(name)?;
        }
        s.write_coalescer.remove(name);
        s.routed_write_coalescer.remove(name);
        s.per_graph_inflight.remove(name);
        #[cfg(feature = "redb")]
        s.cold_tracker.forget(name);
        crate::metrics::drop_graph(name);
        Ok(())
    }

    /// Read ONE stored log entry by index from redb (helper for `get_log_state`).
    pub(super) fn read_one_entry(&self, idx: u64) -> Result<Option<EntryOf<TypeConfig>>, String> {
        let blobs = self.redb().raft_log_read(self.group_id, idx, idx)?;
        match blobs.into_iter().next() {
            Some(b) => Ok(Some(decode_raft_value(
                &b,
                MAX_RAFT_LOG_ENTRY_BYTES,
                MAX_RAFT_LOG_ITEMS,
            )?)),
            None => Ok(None),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<EgStore> {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig>, io::Error> {
        let _snapshot_gate = self.apply_snapshot_gate.lock().await;
        let (last_applied_log, last_membership) = {
            let sm = self.sm.read().await;
            (sm.last_applied_log, sm.last_membership.clone())
        };
        let graphs = self.dump_graphs().await.map_err(ioerr)?;
        let native_indexes: Vec<u64> = self.native_history.read().await.iter().copied().collect();
        let mut native_history = Vec::with_capacity(native_indexes.len());
        for log_index in native_indexes {
            let bytes = self
                .redb()
                .raft_meta_get(self.group_id, &native_history_key(log_index))
                .map_err(ioerr)?
                .ok_or_else(|| ioerr("native snapshot history command is missing"))?;
            let request: RaftRequest =
                decode_raft_value(&bytes, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                    .map_err(ioerr)?;
            if !is_replayable_native_request(&request) {
                return Err(ioerr("native snapshot history command is invalid"));
            }
            native_history.push(NativeHistoryEntry { log_index, request });
        }
        let body = SmSnapshotData {
            schema_version: RAFT_SNAPSHOT_SCHEMA_VERSION,
            last_applied_log,
            last_membership: last_membership.clone(),
            graphs,
            native_history,
        };
        let data = rmp_serde::to_vec_named(&body).map_err(ioerr)?;
        validate_raft_value(&data, MAX_RAFT_SNAPSHOT_BYTES, MAX_RAFT_SNAPSHOT_ITEMS)
            .map_err(ioerr)?;

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock();
            *l += 1;
            *l
        };
        let snapshot_id = match &last_applied_log {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx),
            None => format!("--{}", snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };
        *self.current_snapshot.write().await = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}
