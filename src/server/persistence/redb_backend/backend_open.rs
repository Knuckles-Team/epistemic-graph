use super::*;
use crate::redb_layout::shard_filename;

type ShardSpec = (usize, String, String);

fn shard_specs(persist_dir: &str, k: usize) -> Vec<ShardSpec> {
    (0..k)
        .map(|i| {
            let db_path = std::path::Path::new(persist_dir)
                .join(shard_filename(i))
                .to_string_lossy()
                .to_string();
            let thread_name = if k <= 1 {
                "eg-redb-writer".to_string()
            } else {
                format!("eg-redb-writer-{i}")
            };
            (i, db_path, thread_name)
        })
        .collect()
}

fn prepare_one_shard(
    i: usize,
    k: usize,
    db_path: String,
    thread_name: String,
) -> Result<(PreparedShard, String), String> {
    let bytes_on_disk = std::fs::metadata(&db_path).map(|m| m.len()).ok();
    tracing::info!(
        "redb: opening shard {i}/{k} ({db_path}, {} bytes on disk) ...",
        bytes_on_disk
            .map(|n| n.to_string())
            .unwrap_or_else(|| "new".to_string())
    );
    let started = std::time::Instant::now();
    let result = ShardWriter::prepare(db_path);
    match &result {
        Ok(_) => tracing::info!(
            "redb: shard {i}/{k} open finished in {:?}",
            started.elapsed()
        ),
        Err(error) => tracing::warn!(
            "redb: shard {i}/{k} open FAILED after {:?}: {error}",
            started.elapsed()
        ),
    }
    result.map(|prepared| (prepared, thread_name))
}

fn prepare_shards(specs: Vec<ShardSpec>, k: usize) -> Vec<Result<(PreparedShard, String), String>> {
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(specs.len());
        for (i, db_path, thread_name) in specs {
            handles.push(scope.spawn(move || prepare_one_shard(i, k, db_path, thread_name)));
        }
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            results.push(
                handle
                    .join()
                    .unwrap_or_else(|_| Err("redb shard-open thread panicked".to_string())),
            );
        }
        results
    })
}

fn spawn_shards(
    prepared: Vec<(PreparedShard, String)>,
    capacity: usize,
    flush_threshold: usize,
    group_commit: &RedbGroupCommitConfig,
) -> Result<Vec<ShardWriter>, String> {
    prepared
        .into_iter()
        .map(|(shard, thread_name)| {
            ShardWriter::spawn(
                shard,
                thread_name,
                capacity,
                flush_threshold,
                group_commit.clone(),
            )
        })
        .collect()
}

impl RedbBackend {
    /// Open (or create) the sharded durable tier under `persist_dir` and spawn one
    /// off-reactor group-commit writer thread per shard (CONCEPT:EG-KG.backend.sharded-k-way-durable). The shard
    /// count K is auto-sized (`resolve_shard_count`) and reconciled against any
    /// existing current on-disk layout. The exclusive per-file redb lock for every
    /// shard is acquired here at open.
    pub fn open(persist_dir: String, capacity: usize) -> Result<Self, String> {
        let backend = Self::open_with_shards(persist_dir.clone(), capacity, resolve_shard_count())?;
        Ok(backend.maybe_attach_catalog_from_env(&persist_dir))
    }

    /// Catalog auto-attach gate (CONCEPT:EG-KG.sharding.r5-feature, R5). At startup attach the durable tenant
    /// catalog to the LIVE routing seam when `EPISTEMIC_GRAPH_TENANT_CATALOG=1` is set OR a
    /// durable `catalog.redb` already exists (a populated catalog from a prior run must be
    /// honored). When NEITHER holds — the default — NO catalog is attached and routing is
    /// byte-for-byte EG-026 FNV-1a. An attached-but-EMPTY catalog also routes identically
    /// (`resolve_shard` == `shard_index`), so turning the flag on is a no-op until an
    /// online reshard assigns a placement. Only [`Self::open`] (the live boot path) calls
    /// this; the explicit-K test constructor [`Self::open_with_shards`] never auto-attaches.
    fn maybe_attach_catalog_from_env(self, persist_dir: &str) -> Self {
        let flag = std::env::var("EPISTEMIC_GRAPH_TENANT_CATALOG")
            .ok()
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes" || v == "on"
            })
            .unwrap_or(false);
        let catalog_exists = std::path::Path::new(persist_dir)
            .join("catalog.redb")
            .exists();
        if !flag && !catalog_exists {
            return self; // DEFAULT: pure EG-026, no catalog, no behavior change.
        }
        match crate::server::persistence::tenant_catalog::TenantCatalog::open(persist_dir) {
            Ok(cat) => {
                if cat.is_empty() {
                    tracing::info!(
                        "tenant catalog attached (CONCEPT:EG-KG.sharding.r5-feature) — empty ⇒ pure EG-026 routing \
                         until an online reshard assigns a placement"
                    );
                } else {
                    tracing::info!(
                        "tenant catalog attached (CONCEPT:EG-KG.sharding.r5-feature) — {} explicit placement(s) \
                         override EG-026 hash routing",
                        cat.len()
                    );
                }
                self.with_catalog(Arc::new(cat))
            }
            Err(e) => {
                tracing::warn!(
                    "tenant catalog open failed ({e}); continuing with pure EG-026 routing"
                );
                self
            }
        }
    }

    /// Open with an EXPLICIT requested shard count (CONCEPT:EG-KG.backend.sharded-k-way-durable). Used by `open`
    /// (auto-sized K) and by the sharding tests (deterministic K). The requested K is
    /// still reconciled against the on-disk layout so an existing dir's K wins.
    pub fn open_with_shards(
        persist_dir: String,
        capacity: usize,
        requested_k: usize,
    ) -> Result<Self, String> {
        Self::open_with_shards_and_config(
            persist_dir,
            capacity,
            requested_k,
            RedbGroupCommitConfig::from_env(),
        )
    }

    #[cfg(test)]
    pub(super) fn open_with_group_commit_config(
        persist_dir: String,
        capacity: usize,
        group_commit: RedbGroupCommitConfig,
    ) -> Result<Self, String> {
        Self::open_with_shards_and_config(persist_dir, capacity, 1, group_commit)
    }

    fn open_with_shards_and_config(
        persist_dir: String,
        capacity: usize,
        requested_k: usize,
        group_commit: RedbGroupCommitConfig,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(&persist_dir).map_err(|e| e.to_string())?;
        let requested_k = crate::redb_layout::validate_shard_count(requested_k)?;
        let k = crate::redb_layout::reconcile_shard_layout(
            std::path::Path::new(&persist_dir),
            requested_k,
        )?;
        if k != requested_k {
            tracing::warn!(
                "redb: persist dir has {k} canonical shard file(s) but K={requested_k} requested; \
                 using the on-disk K (changing it requires an offline migration)"
            );
        }
        let flush_threshold = resolve_flush_threshold(capacity);
        if k > 1 {
            tracing::info!(
                "redb: sharded durable writer — K={k} graph-<n>.redb files, {k} writer threads \
                 (flush_threshold={flush_threshold})"
            );
        }
        let shard_open_start = std::time::Instant::now();
        let opened = prepare_shards(shard_specs(&persist_dir, k), k);
        // PHASE 1 of the open is now complete and NOTHING has been written. A
        // refusal from ANY shard aborts here, leaving the persist dir
        // byte-identical -- which is what makes the refusals above (and their
        // advice to unset the key and carry on) actually true for a K > 1 store.
        // See `CanaryPlan`'s doc.
        let mut prepared = Vec::with_capacity(k);
        for shard in opened {
            prepared.push(shard?);
        }
        // PHASE 2: every shard agreed, so commit each decided plan and start each
        // writer thread.
        let shards = spawn_shards(prepared, capacity, flush_threshold, &group_commit)?;
        if k > 1 {
            tracing::info!(
                "redb: all {k} shard(s) open in {:?} (wall clock; ran concurrently)",
                shard_open_start.elapsed()
            );
        }
        let admin_path = std::path::Path::new(&persist_dir).join("admin-mutations.redb");
        let admin_mutations = open_admin_mutations(&admin_path)?;
        let node_info = Arc::new(super::super::node_info_store::NodeInfoStore::open(
            &persist_dir,
        )?);
        let cluster_hierarchy = Arc::new(
            super::super::cluster_hierarchy_store::ClusterHierarchyStore::open(&persist_dir)?,
        );
        Ok(Self {
            shards,
            catalog: None,
            routing_epoch: Arc::new(RwLock::new(())),
            admin_mutations,
            node_info,
            cluster_hierarchy,
        })
    }
}
