use super::*;

fn load_open_metadata(
    redb: &RedbBackend,
    group_id: GroupId,
) -> Result<
    (
        Option<VoteOf<TypeConfig>>,
        AppliedState,
        Option<LogIdOf<TypeConfig>>,
    ),
    String,
> {
    let vote = match redb.raft_meta_get(group_id, KEY_VOTE)? {
        Some(bytes) => decode_raft_value(&bytes, MAX_RAFT_META_BYTES, 100_000)?,
        None => None,
    };
    let applied = match redb.raft_meta_get(group_id, KEY_APPLIED)? {
        Some(bytes) => decode_raft_value(&bytes, MAX_RAFT_META_BYTES, 100_000)?,
        None => AppliedState::default(),
    };
    let purged = match redb.raft_meta_get(group_id, KEY_PURGED)? {
        Some(bytes) => decode_raft_value(&bytes, MAX_RAFT_META_BYTES, 100_000)?,
        None => None,
    };
    Ok((vote, applied, purged))
}

fn load_history_chunk(
    redb: &RedbBackend,
    group_id: GroupId,
    chunk: u64,
    last_applied_index: u64,
) -> Result<Vec<u64>, String> {
    let Some(bitmap) = redb.raft_meta_get(group_id, &native_history_bitmap_key(chunk))? else {
        return Ok(Vec::new());
    };
    if bitmap.len() != NATIVE_HISTORY_BITMAP_BYTES {
        return Err("persisted native history bitmap is invalid".to_string());
    }
    let mut indexes = Vec::new();
    for (byte_index, byte) in bitmap.into_iter().enumerate() {
        for bit in 0..8u8 {
            if byte & (1u8 << bit) == 0 {
                continue;
            }
            let index =
                chunk * NATIVE_HISTORY_BITMAP_BITS + (byte_index as u64) * 8 + u64::from(bit);
            if index > last_applied_index {
                return Err("persisted native history exceeds applied state".to_string());
            }
            let bytes = redb
                .raft_meta_get(group_id, &native_history_key(index))?
                .ok_or_else(|| "persisted native history bitmap has no command".to_string())?;
            let request: RaftRequest =
                decode_raft_value(&bytes, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)?;
            if !is_replayable_native_request(&request) {
                return Err("persisted native history entry is invalid".to_string());
            }
            indexes.push(index);
        }
    }
    Ok(indexes)
}

fn load_native_history(
    redb: &RedbBackend,
    group_id: GroupId,
    last_applied: Option<LogIdOf<TypeConfig>>,
) -> Result<BTreeSet<u64>, String> {
    let Some(last_applied) = last_applied else {
        return Ok(BTreeSet::new());
    };
    let last_chunk = last_applied.index / NATIVE_HISTORY_BITMAP_BITS;
    let mut native_history = BTreeSet::new();
    for chunk in 0..=last_chunk {
        native_history.extend(load_history_chunk(
            redb,
            group_id,
            chunk,
            last_applied.index,
        )?);
    }
    Ok(native_history)
}

impl EgStore {
    /// Open the store for `group_id`, recovering the durable vote + applied state +
    /// last-purged pointer from the shared authoritative shard (keyed by group id). The
    /// graph DATA is recovered separately by the M2 `load_all` path before Raft
    /// starts, so on boot the applied pointers and the on-disk graph data agree.
    pub fn open(
        group_id: GroupId,
        backend: Arc<dyn PersistenceBackend>,
        ctx: AppCtx,
    ) -> Result<Arc<Self>, String> {
        let redb = backend
            .as_redb()
            .ok_or_else(|| "raft requires the redb persistence backend".to_string())?;
        let (vote, applied, purged) = load_open_metadata(redb, group_id)?;
        let native_history = load_native_history(redb, group_id, applied.last_applied_log)?;
        Ok(Arc::new(Self {
            group_id,
            backend,
            last_purged_log_id: RwLock::new(purged),
            committed: RwLock::new(None),
            vote: RwLock::new(vote),
            sm: RwLock::new(StateMachine {
                last_applied_log: applied.last_applied_log,
                last_membership: applied.last_membership,
            }),
            current_snapshot: RwLock::new(None),
            native_history: RwLock::new(native_history),
            snapshot_idx: parking_lot::Mutex::new(0),
            apply_snapshot_gate: Mutex::new(()),
            ctx,
        }))
    }

    /// The concrete redb backend (the raft store is only constructed over redb).
    pub(super) fn redb(&self) -> &RedbBackend {
        self.backend
            .as_redb()
            .expect("raft store backend is always redb (checked at open)")
    }

    pub(super) async fn persist_vote(&self, vote: &VoteOf<TypeConfig>) -> Result<(), String> {
        let b = rmp_serde::to_vec_named(vote).map_err(|e| e.to_string())?;
        self.redb().raft_meta_put(self.group_id, KEY_VOTE, b).await
    }

    pub(super) async fn persist_applied(&self, sm: &StateMachine) -> Result<(), String> {
        let a = AppliedState {
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        };
        let b = rmp_serde::to_vec_named(&a).map_err(|e| e.to_string())?;
        self.redb()
            .raft_meta_put(self.group_id, KEY_APPLIED, b)
            .await
    }

    pub(super) async fn persist_native_history_entry(
        &self,
        log_index: u64,
        request: &RaftRequest,
    ) -> Result<(), String> {
        if !is_replayable_native_request(request) {
            return Err("attempted to persist a non-native replay entry".to_string());
        }
        let bytes = rmp_serde::to_vec_named(request).map_err(|error| error.to_string())?;
        validate_raft_value(&bytes, MAX_RAFT_LOG_ENTRY_BYTES, MAX_RAFT_LOG_ITEMS)?;
        self.redb()
            .raft_meta_put(self.group_id, &native_history_key(log_index), bytes)
            .await?;
        let chunk = log_index / NATIVE_HISTORY_BITMAP_BITS;
        let offset = log_index % NATIVE_HISTORY_BITMAP_BITS;
        let bitmap_key = native_history_bitmap_key(chunk);
        let mut bitmap = match self.redb().raft_meta_get(self.group_id, &bitmap_key)? {
            Some(value) if value.len() == NATIVE_HISTORY_BITMAP_BYTES => value,
            Some(_) => return Err("persisted native history bitmap is invalid".to_string()),
            None => vec![0; NATIVE_HISTORY_BITMAP_BYTES],
        };
        bitmap[(offset / 8) as usize] |= 1u8 << (offset % 8);
        self.redb()
            .raft_meta_put(self.group_id, &bitmap_key, bitmap)
            .await
    }
}
