macro_rules! persistence_mutations {
    ($next:ident { $($methods:tt)* }) => {
    $next! {
    $($methods)*
        async fn record_durable(&self, graph_fname: &str, method: &Method) -> Result<(), String> {
            let (done_tx, done_rx) = oneshot::channel();
            let cmd = Cmd::Mutation {
                graph: graph_fname.to_string(),
                method: Box::new(method.clone()),
                done: done_tx,
            };
            // Blocking send = backpressure: park until the bounded channel has room
            // rather than dropping. Off the reactor via spawn_blocking so a saturated
            // writer can't stall the Tokio worker pool. Routed to the graph's shard.
            //
            // CONCEPT:EG-KG.backend.catalog-shard-resolve — when a tenant catalog is attached, resolve the shard AND enqueue
            // the op while holding a SHARED `routing_epoch` READ guard, so an online reshard's
            // exclusive flip cannot interleave (no lost / misrouted write). The guard is moved
            // INTO the blocking send so it is held exactly until the op is enqueued, then
            // dropped. With NO catalog (the default) this is byte-for-byte the EG-026 path.
            if self.catalog.is_some() {
                let guard = self.routing_epoch.clone().read_owned().await;
                let tx = self.shard_for(graph_fname).tx.clone();
                tokio::task::spawn_blocking(move || {
                    let _routing = guard;
                    tx.send(cmd).map_err(|_| ())
                })
                .await
                .map_err(|e| format!("redb record_durable join error: {e}"))?
                .map_err(|_| {
                    "redb writer thread is gone; durable mutation not persisted".to_string()
                })?;
            } else {
                let tx = self.shard_for(graph_fname).tx.clone();
                tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ()))
                    .await
                    .map_err(|e| format!("redb record_durable join error: {e}"))?
                    .map_err(|_| {
                        "redb writer thread is gone; durable mutation not persisted".to_string()
                    })?;
            }
            // Await the writer's post-commit signal. A dropped sender (writer gone /
            // commit thread died) is a durability failure, surfaced as Err.
            match done_rx.await {
                Ok(res) => res,
                Err(_) => Err("redb writer dropped durable-commit completion".to_string()),
            }
        }

        /// Authoritative universal batch commit.  The bounded writer channel is
        /// entered with blocking `send` on Tokio's blocking pool, so saturation
        /// propagates backpressure and can never shed/partially enqueue a batch.
        async fn commit_mutation_batch(
            &self,
            graph_fname: &str,
            batch: &MutationBatch,
            result_msgpack: Option<&[u8]>,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::MutationBatchCommit {
                payload: Box::new(MutationBatchPayload {
                    graph: graph_fname.to_string(),
                    batch: batch.clone(),
                    authoritative_state_msgpack: None,
                    result_msgpack: result_msgpack.map(ToOwned::to_owned),
                    committed_at_ms,
                    // No authoritative_state -> audit is gated per-operation from the
                    // (identity-preserving) method itself downstream; this flag is inert.
                    audited: true,
                }),
                done,
            };
            self.enqueue(graph_fname, cmd, "commit_mutation_batch")
                .await?;
            rx.await
                .map_err(|_| "redb writer dropped MutationBatch completion".to_string())?
        }

        async fn commit_mutation_batch_state(
            &self,
            graph_fname: &str,
            batch: &MutationBatch,
            authoritative_state_msgpack: Vec<u8>,
            result_msgpack: Option<&[u8]>,
            committed_at_ms: u64,
            audited: bool,
        ) -> Result<MutationBatchCommit, String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::MutationBatchCommit {
                payload: Box::new(MutationBatchPayload {
                    graph: graph_fname.to_string(),
                    batch: batch.clone(),
                    authoritative_state_msgpack: Some(authoritative_state_msgpack),
                    result_msgpack: result_msgpack.map(ToOwned::to_owned),
                    committed_at_ms,
                    audited,
                }),
                done,
            };
            self.enqueue(graph_fname, cmd, "commit_mutation_batch_state")
                .await?;
            rx.await
                .map_err(|_| "redb writer dropped staged MutationBatch completion".to_string())?
        }

        async fn commit_mutation_batch_crossmodal(
            &self,
            args: super::super::CrossModalCommitArgs<'_>,
        ) -> Result<MutationBatchCommit, String> {
            let graph_fname = args.graph_fname;
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::CrossModalBatchCommit {
                payload: Box::new(CrossModalBatchPayload {
                    graph: graph_fname.to_string(),
                    batch: args.batch.clone(),
                    methods: args.methods.to_vec(),
                    vectors: args.vectors.to_vec(),
                    blob_refs: args.blob_refs.to_vec(),
                    measurements: args.measurements.to_vec(),
                    result_msgpack: args.result_msgpack.map(ToOwned::to_owned),
                    committed_at_ms: args.committed_at_ms,
                }),
                done,
            };
            self.enqueue(graph_fname, cmd, "commit_mutation_batch_crossmodal")
                .await?;
            rx.await.map_err(|_| {
                "redb writer dropped cross-modal MutationBatch completion".to_string()
            })?
        }

        async fn read_mutation_batch(
            &self,
            graph_fname: &str,
            batch_id: &str,
        ) -> Result<Option<MutationBatchRecord>, String> {
            let batch_id = batch_id.to_owned();
            let requested_graph = graph_fname.to_owned();
            self.read_snapshot(graph_fname, move |shard, _crypto| {
                read_mutation_batch_record(shard, &requested_graph, &batch_id)
            })
            .await
        }
    }
    };
}

pub(crate) use persistence_mutations;
