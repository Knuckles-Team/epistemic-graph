macro_rules! persistence_graph_reads {
    () => {
    fn read_graph_material_blocking(
        &self,
        graph_fname: &str,
    ) -> Result<Option<crate::registry::GraphMaterial>, String> {
        Ok(self
            .read_graph_dump_blocking(graph_fname)?
            .map(|dump| crate::registry::GraphMaterial {
                nodes: dump.nodes,
                edges: dump.edges,
                semantic: dump.semantic,
                integrity_policy: dump.integrity_policy,
                incarnation_id: Some(dump.incarnation_id),
                source_snapshot_version: Some(dump.source_snapshot_version),
            }))
    }

    async fn read_authoritative_graph_snapshot(
        &self,
        graph_fname: &str,
    ) -> Result<Option<(crate::graph::GraphSnapshot, u64)>, String> {
        let graph = graph_fname.to_string();
        let writer = self.shard_for(graph_fname);
        let tx = writer.tx.clone();
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let version_graph = graph_fname.to_string();
        let read = move || {
            let (reply, rx) = std::sync::mpsc::sync_channel(1);
            tx.send(Cmd::ReadGraphDump { graph, reply })
                .map_err(|_| "redb writer thread is gone".to_string())?;
            let dump = await_writer_reply(&rx, "authoritative snapshot")??;
            let version = read_mutation_graph_version_record(&shard, &version_graph)?;
            Ok::<_, String>((dump, version))
        };
        let (dump, version) = if self.catalog.is_some() {
            let routing = self.routing_epoch.clone().read_owned().await;
            tokio::task::spawn_blocking(move || {
                let _routing = routing;
                read()
            })
            .await
            .map_err(|e| format!("authoritative snapshot join error: {e}"))??
        } else {
            tokio::task::spawn_blocking(read)
                .await
                .map_err(|e| format!("authoritative snapshot join error: {e}"))??
        };
        dump.map(|dump| {
            let semantic_store = if dump.semantic.is_empty() {
                crate::compute::semantic::SemanticStore::default()
            } else {
                decode_durable_semantic(&dump.semantic)?
            };
            Ok((
                crate::graph::GraphSnapshot {
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
                        .map(|(source, target, properties)| (source, target, Arc::new(properties)))
                        .collect(),
                    ledger: dump.ledger,
                    semantic_store,
                },
                version,
            ))
        })
        .transpose()
    }

    /// SYNC bounded-page durable-material fetch (CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged
    /// adjacency") — reuses [`Self::read_graph_dump_page_blocking`], a genuinely
    /// SOURCE-bounded scan (never collects the whole graph's rows into memory first,
    /// unlike the [`Self::read_graph_material_blocking`] override above / the
    /// default trait fallback), closing the honest limitation
    /// `docs/architecture/epistemic-os-hardening.md` names as open ledger item L38:
    /// "first access to a lazily-opened graph still fully rehydrates it".
    fn read_graph_material_page_blocking(
        &self,
        graph_fname: &str,
        cursor: Option<crate::registry::MaterializeCursor>,
        page_size: usize,
    ) -> Result<Option<crate::registry::MaterialPage>, String> {
        let (node_offset, edge_offset, node_after, edge_after) =
            cursor.map_or((0, 0, None, None), |cursor| {
                (
                    cursor.node_offset,
                    cursor.edge_offset,
                    cursor.node_after,
                    cursor.edge_after,
                )
            });
        Ok(self
            .read_graph_dump_page_blocking(
                graph_fname,
                node_offset,
                edge_offset,
                node_after,
                edge_after,
                page_size,
            )?
            .map(|page| {
                let next_cursor = if page.nodes_exhausted && page.edges_exhausted {
                    None
                } else {
                    Some(crate::registry::MaterializeCursor {
                        node_offset: node_offset + page.nodes.len(),
                        edge_offset: if page.nodes_exhausted {
                            edge_offset + page.edges.len()
                        } else {
                            edge_offset
                        },
                        node_after: page.node_after,
                        edge_after: page.edge_after,
                    })
                };
                crate::registry::MaterialPage {
                    nodes: page.nodes,
                    edges: page.edges,
                    semantic: page.semantic,
                    integrity_policy: page.integrity_policy,
                    next_cursor,
                    incarnation_id: Some(page.incarnation_id),
                    source_snapshot_version: Some(page.source_snapshot_version),
                }
            }))
    }

    /// COMMIT-BEFORE-ACK (CONCEPT:EG-KG.backend.authoritative-dispatch). Enqueue the mutation with a completion
    /// oneshot and await its durable commit. Backpressure-NOT-drop: a full queue
    /// BLOCKS for capacity (`SyncSender::send`) instead of shedding the write. The enqueue
    /// + the blocking send both happen on the blocking pool so the Tokio worker is
    /// never parked on disk/lock pressure. Completion is signalled by the writer
    /// AFTER its group-commit `WriteTransaction` commits, so concurrent callers still
    /// coalesce into ONE fsync.
    };
}

pub(crate) use persistence_graph_reads;
