use super::*;

fn decode_snapshot(
    meta: &SnapshotMetaOf<TypeConfig>,
    snapshot: SnapshotDataOf<TypeConfig>,
) -> Result<(SmSnapshotData, Vec<u8>), io::Error> {
    let data = snapshot.into_inner();
    let body: SmSnapshotData =
        decode_raft_value(&data, MAX_RAFT_SNAPSHOT_BYTES, MAX_RAFT_SNAPSHOT_ITEMS)
            .map_err(ioerr)?;
    if body.schema_version != RAFT_SNAPSHOT_SCHEMA_VERSION {
        return Err(ioerr(format!(
            "unsupported Raft snapshot schema {} (expected {})",
            body.schema_version, RAFT_SNAPSHOT_SCHEMA_VERSION
        )));
    }
    if meta.last_log_id != body.last_applied_log {
        return Err(ioerr("Raft snapshot metadata does not match its body"));
    }
    Ok((body, data))
}

impl EgStore {
    fn validate_snapshot_history(
        &self,
        body: &SmSnapshotData,
        server_secret: &str,
    ) -> Result<(), io::Error> {
        let mut previous_native_index = None;
        for entry in &body.native_history {
            let history_is_invalid = previous_native_index
                .is_some_and(|index| index >= entry.log_index)
                || body.last_applied_log.is_none()
                || body
                    .last_applied_log
                    .is_some_and(|last| entry.log_index > last.index)
                || !is_replayable_native_request(&entry.request)
                || self.ctx.router.as_ref().is_some_and(|router| {
                    router.group_of(&entry.request.graph_name) != self.group_id
                });
            if history_is_invalid {
                return Err(ioerr("Raft native snapshot history is invalid"));
            }
            entry.request.validate().map_err(ioerr)?;
            let ReplicatedMutation::Native { command } = &entry.request.command else {
                return Err(ioerr("Raft native snapshot history is invalid"));
            };
            command
                .validate_replay_authentication(server_secret)
                .map_err(ioerr)?;
            previous_native_index = Some(entry.log_index);
        }
        Ok(())
    }

    async fn replay_snapshot_history(
        &self,
        entries: &[NativeHistoryEntry],
    ) -> Result<BTreeSet<u64>, io::Error> {
        let mut native_history = BTreeSet::new();
        for entry in entries {
            let response = self.apply_request(&entry.request).await.map_err(ioerr)?;
            if let Some(error) = response.native_error {
                return Err(ioerr(format!(
                    "Raft native snapshot replay failed: {error}"
                )));
            }
            self.persist_native_history_entry(entry.log_index, &entry.request)
                .await
                .map_err(ioerr)?;
            native_history.insert(entry.log_index);
        }
        Ok(native_history)
    }

    async fn finalize_snapshot_install(
        &self,
        meta: &SnapshotMetaOf<TypeConfig>,
        data: Vec<u8>,
        body: &SmSnapshotData,
        native_history: BTreeSet<u64>,
    ) -> Result<(), io::Error> {
        {
            let mut sm = self.sm.write().await;
            sm.last_applied_log = body.last_applied_log;
            sm.last_membership = body.last_membership.clone();
            let snapshot = sm.clone();
            drop(sm);
            self.persist_applied(&snapshot).await.map_err(ioerr)?;
        }
        *self.native_history.write().await = native_history;
        *self.current_snapshot.write().await = Some((meta.clone(), data));
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for Arc<EgStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let _apply_gate = self.apply_snapshot_gate.lock().await;
            let resp = match &entry.payload {
                EntryPayload::Blank => RaftResponse {
                    applied: false,
                    ..Default::default()
                },
                EntryPayload::Normal(req) => self.apply_request(req).await.map_err(ioerr)?,
                EntryPayload::Membership(mem) => {
                    let mut sm = self.sm.write().await;
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    RaftResponse {
                        applied: false,
                        ..Default::default()
                    }
                }
            };
            if resp.native_error.is_none() {
                if let EntryPayload::Normal(request) = &entry.payload {
                    if is_replayable_native_request(request) {
                        self.persist_native_history_entry(entry.log_id.index, request)
                            .await
                            .map_err(ioerr)?;
                        self.native_history.write().await.insert(entry.log_id.index);
                    }
                }
            }
            // Record the applied index (durably) AFTER the effect landed.
            {
                let mut sm = self.sm.write().await;
                sm.last_applied_log = Some(entry.log_id);
                let snapshot = sm.clone();
                drop(sm);
                self.persist_applied(&snapshot).await.map_err(ioerr)?;
            }
            // Send the client response (only present for entries proposed on THIS
            // node as leader — followers get `None`).
            if let Some(responder) = responder {
                responder.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: SnapshotDataOf<TypeConfig>,
    ) -> Result<(), io::Error> {
        let _snapshot_gate = self.apply_snapshot_gate.lock().await;
        let (body, data) = decode_snapshot(meta, snapshot)?;
        // Validate the complete graph and replay manifests before the first state
        // transition. A malformed tail must not leave a valid prefix applied.
        self.validate_snapshot_graphs(&body.graphs).map_err(ioerr)?;
        let server_secret = self.ctx.state.read().await.auth_secret.clone();
        self.validate_snapshot_history(&body, &server_secret)?;
        let native_history = self.replay_snapshot_history(&body.native_history).await?;
        // Materialize the final graph image after native replay. This overwrites
        // graph projections affected by replay with the exact committed prefix.
        self.install_graphs(&body.graphs).await.map_err(ioerr)?;
        self.finalize_snapshot_install(meta, data, &body, native_history)
            .await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<TypeConfig>>, io::Error> {
        match &*self.current_snapshot.read().await {
            Some((meta, data)) => Ok(Some(Snapshot {
                meta: meta.clone(),
                snapshot: Cursor::new(data.clone()),
            })),
            None => Ok(None),
        }
    }
}
