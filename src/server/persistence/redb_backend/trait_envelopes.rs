macro_rules! persistence_envelopes {
    () => {
        async fn commit_change_envelope(
            &self,
            graph_fname: &str,
            envelope: &ChangeEnvelope,
            committed_at_ms: u64,
        ) -> Result<ChangeEnvelopeCommit, String> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::ChangeEnvelopeCommit {
                payload: Box::new(ChangeEnvelopePayload {
                    graph: graph_fname.to_string(),
                    envelope: envelope.clone(),
                    committed_at_ms,
                }),
                done,
            };
            self.enqueue(graph_fname, cmd, "commit_change_envelope")
                .await?;
            rx.await
                .map_err(|_| "redb writer dropped ChangeEnvelope completion".to_string())?
        }

        async fn commit_change_envelopes(
            &self,
            graph_fname: &str,
            envelopes: &[ChangeEnvelope],
            committed_at_ms: u64,
        ) -> Result<Vec<ChangeEnvelopeCommit>, (usize, String)> {
            let (done, rx) = oneshot::channel();
            let cmd = Cmd::ChangeEnvelopesCommit {
                payload: Box::new(ChangeEnvelopesPayload {
                    graph: graph_fname.to_string(),
                    envelopes: envelopes.to_vec(),
                    committed_at_ms,
                }),
                done,
            };
            let send_result = if self.catalog.is_some() {
                let guard = self.routing_epoch.clone().read_owned().await;
                let tx = self.shard_for(graph_fname).tx.clone();
                tokio::task::spawn_blocking(move || {
                    let _routing = guard;
                    tx.send(cmd).map_err(|_| ())
                })
                .await
            } else {
                let tx = self.shard_for(graph_fname).tx.clone();
                tokio::task::spawn_blocking(move || tx.send(cmd).map_err(|_| ())).await
            };
            send_result
                .map_err(|e| (0usize, format!("commit_change_envelopes join error: {e}")))?
                .map_err(|_| (0usize, "redb writer thread is gone".to_string()))?;
            rx.await.map_err(|_| {
                (
                    0usize,
                    "redb writer dropped ChangeEnvelopes completion".to_string(),
                )
            })?
        }

        async fn read_change_envelope(
            &self,
            graph_fname: &str,
            envelope_id: &str,
        ) -> Result<Option<ChangeEnvelopeRecord>, String> {
            let graph_fname = graph_fname.to_owned();
            let routing_graph = graph_fname.clone();
            let envelope_id = envelope_id.to_owned();
            self.read_snapshot(&routing_graph, move |shard, crypto| {
                read_change_envelope_record(shard, &graph_fname, &envelope_id, crypto)
            })
            .await
        }

        async fn read_content_version(
            &self,
            graph_fname: &str,
            tenant: &str,
            object_id: &str,
        ) -> Result<Option<ContentVersion>, String> {
            let graph_fname = graph_fname.to_owned();
            let routing_graph = graph_fname.clone();
            let tenant = tenant.to_owned();
            let object_id = object_id.to_owned();
            self.read_snapshot(&routing_graph, move |shard, crypto| {
                read_content_version_record(shard, &tenant, &graph_fname, &object_id, crypto)
            })
            .await
        }

        async fn read_change_cursor(
            &self,
            graph_fname: &str,
            tenant: &str,
            source: &str,
            partition: &str,
        ) -> Result<Option<ChangeCursor>, String> {
            let graph_fname = graph_fname.to_owned();
            let routing_graph = graph_fname.clone();
            let tenant = tenant.to_owned();
            let source = source.to_owned();
            let partition = partition.to_owned();
            self.read_snapshot(&routing_graph, move |shard, crypto| {
                read_change_cursor_record(shard, &tenant, &graph_fname, &source, &partition, crypto)
            })
            .await
        }
    };
}

pub(crate) use persistence_envelopes;
