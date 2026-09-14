use super::*;

/// Translate an arbitrary `RangeBounds<u64>` into the inclusive `[lo, hi]` redb
/// scans on (a saturating-bounded variant of) the requested range.
fn inclusive_bounds<RB: RangeBounds<u64>>(range: &RB) -> (u64, u64) {
    let lo = match range.start_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => i.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let hi = match range.end_bound() {
        Bound::Included(i) => *i,
        Bound::Excluded(i) => i.saturating_sub(1),
        Bound::Unbounded => u64::MAX,
    };
    (lo, hi)
}

impl RaftLogReader<TypeConfig> for Arc<EgStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let (lo, hi) = inclusive_bounds(&range);
        if lo > hi {
            return Ok(Vec::new());
        }
        let blobs = self
            .redb()
            .raft_log_read(self.group_id, lo, hi)
            .map_err(ioerr)?;
        if blobs.len() > MAX_RAFT_LOG_BATCH_ENTRIES
            || blobs
                .iter()
                .try_fold(0usize, |total, blob| total.checked_add(blob.len()))
                .is_none_or(|total| total > MAX_RAFT_SNAPSHOT_BYTES)
        {
            return Err(ioerr("raft log read exceeds resource limits"));
        }
        let mut out = Vec::with_capacity(blobs.len());
        for b in blobs {
            let e: EntryOf<TypeConfig> =
                decode_raft_value(&b, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                    .map_err(ioerr)?;
            out.push(e);
        }
        Ok(out)
    }

    /// The last saved vote (moved onto [`RaftLogReader`] in openraft 0.10).
    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(*self.vote.read().await)
    }
}
impl RaftLogStorage<TypeConfig> for Arc<EgStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let (_, last_idx) = self.redb().raft_log_bounds(self.group_id).map_err(ioerr)?;
        let last_purged = *self.last_purged_log_id.read().await;
        // Reconstruct the last log id from the stored entry (redb holds the Entry),
        // so a restart knows its log tail WITHOUT the leader.
        let last_log_id = match last_idx {
            Some(i) => match self.read_one_entry(i).map_err(ioerr)? {
                Some(e) => Some(e.log_id()),
                None => last_purged,
            },
            None => last_purged,
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.persist_vote(vote).await.map_err(ioerr)?;
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        *self.committed.write().await = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for entry in entries {
            if batch.len() >= MAX_RAFT_LOG_BATCH_ENTRIES {
                return Err(ioerr("raft log batch exceeds resource limits"));
            }
            let blob = rmp_serde::to_vec_named(&entry).map_err(ioerr)?;
            validate_raft_value(&blob, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)
                .map_err(ioerr)?;
            batch_bytes = batch_bytes
                .checked_add(blob.len())
                .filter(|total| *total <= MAX_RAFT_SNAPSHOT_BYTES)
                .ok_or_else(|| ioerr("raft log batch exceeds resource limits"))?;
            batch.push((entry.log_id().index, blob));
        }
        // Durable append: rides the SAME group-commit transaction as any concurrent
        // M2 graph mutation (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — one fsync covers both. Our append is
        // synchronously durable, so we fire the 0.10 `IOFlushed` callback the moment
        // the group-commit fsync resolves (openraft treats the entry as on-disk then).
        match self.redb().raft_log_append(self.group_id, batch).await {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = ioerr(&e);
                callback.io_completed(Err(ioerr(&e)));
                Err(err)
            }
        }
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        // Delete every entry AFTER `last_log_id` (exclusive). `None` ⇒ wipe the whole
        // log. `raft_log_delete_from(from)` removes index >= from.
        let from = match last_log_id {
            Some(id) => id.index + 1,
            None => 0,
        };
        self.redb()
            .raft_log_delete_from(self.group_id, from)
            .await
            .map_err(ioerr)
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            if ld.as_ref().map(|l| l.index) < Some(log_id.index) {
                *ld = Some(log_id);
            }
        }
        let b = rmp_serde::to_vec_named(&Some(log_id)).map_err(ioerr)?;
        self.redb()
            .raft_meta_put(self.group_id, KEY_PURGED, b)
            .await
            .map_err(ioerr)?;
        self.redb()
            .raft_log_purge_upto(self.group_id, log_id.index)
            .await
            .map_err(ioerr)
    }
}
