macro_rules! persistence_graph {
    ($next:ident { $($methods:tt)* }) => {
    $next! {
    $($methods)*
        async fn commit_crossmodal(
            &self,
            graph_fname: &str,
            methods: &[Method],
            vectors: &[(String, Vec<f32>)],
            blob_refs: &[(String, String)],
            measurements: &[crate::MeasurementBatch],
        ) -> Result<(), String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::CrossModalCommit {
                payload: Box::new(CrossModalPayload {
                    graph: graph_fname.to_string(),
                    methods: methods.to_vec(),
                    vectors: vectors.to_vec(),
                    blob_refs: blob_refs.to_vec(),
                    measurements: measurements.to_vec(),
                }),
                done,
            };
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — same routing-epoch quiesce as `record_durable` when a catalog
            // is attached, so a cross-modal commit cannot race an online reshard's route flip.
            self.enqueue(graph_fname, cmd, "commit_crossmodal").await?;
            match rx.await {
                Ok(res) => res,
                Err(_) => Err("redb writer dropped commit_crossmodal completion".to_string()),
            }
        }

        async fn register_graph(
            &self,
            graph_fname: &str,
            name: &str,
            graph_type: GraphType,
        ) -> Result<(), String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::RegisterGraph {
                graph: graph_fname.to_string(),
                name: name.to_string(),
                graph_type,
                done,
            };
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("redb register_graph join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
            match rx.await {
                Ok(res) => res,
                Err(_) => Err("redb writer dropped register_graph completion".to_string()),
            }
        }

        async fn purge_graph(&self, graph_fname: &str) -> Result<(), String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::PurgeGraph {
                graph: graph_fname.to_string(),
                done,
            };
            let tx = self.shard_for(graph_fname).tx.clone();
            tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                .await
                .map_err(|e| format!("redb purge_graph join error: {e}"))?
                .map_err(|_| "redb writer thread is gone".to_string())?;
            match rx.await {
                Ok(res) => res,
                Err(_) => Err("redb writer dropped purge_graph completion".to_string()),
            }
        }

        async fn read_node(
            &self,
            graph_fname: &str,
            node_id: &str,
        ) -> Result<Option<Vec<u8>>, String> {
            self.read_node_blocking(graph_fname, node_id)
        }

        fn durable_node_presence(
            &self,
            graph_fname: &str,
            node_ids: &[String],
        ) -> Result<Vec<bool>, String> {
            let writer = self.shard_for(graph_fname);
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            read_durable_node_presence(&shard, graph_fname, node_ids)
        }

        fn read_node_blocking(
            &self,
            graph_fname: &str,
            node_id: &str,
        ) -> Result<Option<Vec<u8>>, String> {
            // CONCEPT:EG-KG.storage.snapshot-read-off-writer — SNAPSHOT READ OFF THE WRITER. The read-through point-read
            // (only hit on a RAM miss, CONCEPT:EG-KG.storage.read-through-seam-exercised) now serves the node DIRECTLY from
            // a kernel-issued MVCC snapshot on the TARGET SHARD's shared `Shard`
            // (routed by the SAME EG-026 `shard_for` the writer uses). It NEVER routes
            // through the writer thread's channel and NEVER forces a group-commit, so a
            // read can no longer block on / be serialized behind the durable write path —
            // critical on a Pi (frequent eviction/read-through) and across shards.
            //
            // Consistency: redb is MVCC, so the snapshot sees the LATEST COMMITTED state
            // of this shard. Commit-before-ack (CONCEPT:EG-KG.backend.authoritative-dispatch) guarantees any ACKED
            // write is already committed, so a snapshot opened after that ack sees
            // it. Writes still buffered in the writer's `Pending` are NOT yet acked (no
            // happens-before to any reader), so omitting the old forced commit changes no
            // observable read result. Eviction is durability-gated (a node leaves RAM only
            // after redb confirms it on disk), so an evicted node is always served here.
            let writer = self.shard_for(graph_fname);
            // Upgrade the `Weak` to the writer's shared `Shard` (CONCEPT:EG-KG.storage.snapshot-read-off-writer). `None`
            // only after shutdown dropped the writer's strong Arc — fail fast like the old
            // "writer thread is gone" channel error.
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            #[cfg(feature = "security")]
            let crypto = crate::redb_store::DurableCrypto::new(writer.cipher.as_ref());
            #[cfg(not(feature = "security"))]
            let crypto = crate::redb_store::DurableCrypto::none();
            read_one_node(&shard, graph_fname, node_id, crypto)
        }

        fn shutdown(&self) {
            // Stop every shard's writer thread (CONCEPT:EG-KG.backend.sharded-k-way-durable).
            for shard in &self.shards {
                shard.shutdown();
            }
        }

        fn as_redb(&self) -> Option<&RedbBackend> {
            Some(self)
        }
    }
    };
}

pub(crate) use persistence_graph;
