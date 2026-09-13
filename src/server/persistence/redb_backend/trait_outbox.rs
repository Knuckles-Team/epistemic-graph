macro_rules! persistence_outbox {
    () => {
        async fn read_mutation_graph_version(
            &self,
            graph_fname: &str,
        ) -> Result<Option<u64>, String> {
            let writer = self.shard_for(graph_fname);
            let shard = writer
                .shard
                .upgrade()
                .ok_or_else(|| "redb writer thread is gone".to_string())?;
            read_mutation_graph_version_record(&shard, graph_fname).map(Some)
        }

        async fn read_mutation_outbox(
            &self,
            graph_fname: &str,
            batch_id: &str,
        ) -> Result<Vec<MutationOutboxRecord>, String> {
            let batch_id = batch_id.to_owned();
            let requested_graph = graph_fname.to_owned();
            self.read_snapshot(graph_fname, move |shard, _crypto| {
                read_mutation_outbox_records(shard, &requested_graph, &batch_id)
            })
            .await
        }

        async fn subscribe_mutation_outbox(
            &self,
            graph_fname: &str,
            consumer: &str,
            topic: &str,
        ) -> Result<(), String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::MutationOutboxSubscribe {
                graph: graph_fname.to_string(),
                consumer: consumer.to_string(),
                topic: topic.to_string(),
                done,
            };
            self.enqueue(graph_fname, cmd, "subscribe_mutation_outbox")
                .await?;
            rx.await
                .map_err(|_| "redb writer dropped outbox subscription completion".to_string())?
        }

        async fn claim_mutation_outbox(
            &self,
            graph_fname: &str,
            consumer: &str,
            budget: &mut OutboxClaimBudget,
        ) -> Result<OutboxClaimOutcome, String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::MutationOutboxClaim {
                graph: graph_fname.to_string(),
                consumer: consumer.to_string(),
                budget: Box::new(budget.clone()),
                done,
            };
            self.enqueue(graph_fname, cmd, "claim_mutation_outbox")
                .await?;
            let (outcome, updated) = rx
                .await
                .map_err(|_| "redb writer dropped outbox claim completion".to_string())??;
            *budget = updated;
            Ok(outcome)
        }

        async fn ack_mutation_outbox(
            &self,
            graph_fname: &str,
            lease: &MutationOutboxLease,
            now_ms: u64,
        ) -> Result<MutationProjectionCursor, String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::MutationOutboxAck {
                graph: graph_fname.to_string(),
                lease: Box::new(lease.clone()),
                now_ms,
                done,
            };
            self.enqueue(graph_fname, cmd, "ack_mutation_outbox")
                .await?;
            rx.await
                .map_err(|_| "redb writer dropped outbox ack completion".to_string())?
        }

        /// One consumer's durable projection watermark.
        ///
        /// `projection` and `consumer` were two names for one thing and are now one:
        /// under a single ledger the cursor is keyed `(scope, consumer)`. The caller's
        /// tenant left the key with them — a graph shard's scope is
        /// `(GRAPH_SHARD_TENANT, graph)` (RF-RULING-004 application note 2), so the
        /// graph name IS the isolation here, as it has always been for every other
        /// shard row.
        async fn read_mutation_projection_cursor(
            &self,
            graph_fname: &str,
            consumer: &str,
        ) -> Result<Option<MutationProjectionCursor>, String> {
            let graph_fname = graph_fname.to_owned();
            let routing_graph = graph_fname.clone();
            let consumer = consumer.to_owned();
            self.read_snapshot(&routing_graph, move |shard, _crypto| {
                shard.outbox_cursor(&graph_fname, &consumer)
            })
            .await
        }
    };
}

pub(crate) use persistence_outbox;
