use super::*;
use crate::redb_layout::shard_filename;

type BackupReport = super::super::backup::BackupReport;

/// The recovery-coordinator fingerprints taken before a backup starts, plus the
/// shard-0 handle the cross-shard fingerprint is re-read from afterwards.
struct BackupBoundaries {
    admin: [u8; 32],
    shard0: Arc<Shard>,
    xshard: [u8; 32],
}

fn backup_boundaries(backend: &RedbBackend) -> Result<BackupBoundaries, String> {
    let admin = eg_storage::recovery_store_fingerprint(backend.admin_mutations.kernel())?;
    let shard = backend
        .shard0()
        .shard
        .upgrade()
        .ok_or_else(|| "redb writer thread is gone".to_string())?;
    let xshard = super::super::backup::xshard_recovery_fingerprint(&shard)?;
    Ok(BackupBoundaries {
        admin,
        shard0: shard,
        xshard,
    })
}

#[cfg(feature = "security")]
fn record_encryption_key(backend: &RedbBackend, report: &mut BackupReport) -> Result<(), String> {
    let key_ref = backend.shards.iter().find_map(|writer| {
        writer
            .cipher
            .as_ref()
            .map(|cipher| cipher.key_ref().clone())
    });
    if backend.shards.iter().any(|writer| {
        writer
            .cipher
            .as_ref()
            .map(|cipher| Some(cipher.key_ref()) != key_ref.as_ref())
            .unwrap_or(false)
    }) {
        return Err(
            "encryption key reference differs between durable shards; refusing to publish backup"
                .to_string(),
        );
    }
    if let Some(key_ref) = key_ref {
        report.encryption_key_id = Some(key_ref.id);
        report.encryption_key_version = Some(key_ref.version);
    }
    Ok(())
}

fn copy_shards(
    backend: &RedbBackend,
    dst_dir: &std::path::Path,
    report: &mut BackupReport,
) -> Result<(), String> {
    for (i, writer) in backend.shards.iter().enumerate() {
        let shard = writer
            .shard
            .upgrade()
            .ok_or_else(|| "redb writer thread is gone".to_string())?;
        let dst_path = dst_dir.join(shard_filename(i));
        let counts = super::super::backup::write_bundle_shard(&shard, &dst_path)?;
        report.add_shard(counts);
    }
    Ok(())
}

fn copy_owned_stores(
    backend: &RedbBackend,
    dst_dir: &std::path::Path,
    extra_stores: &[&dyn super::super::durable_stores::BundledStoreSource],
    report: &mut BackupReport,
) -> Result<(), String> {
    report.admin_mutations = eg_storage::backup_recovery_store(
        backend.admin_mutations.kernel(),
        &dst_dir.join(super::super::backup::ADMIN_MUTATIONS_FILE),
    )?;
    let node_info = backend.node_info();
    let catalog = backend.catalog.clone();
    let mut owned: Vec<&dyn super::super::durable_stores::BundledStoreSource> =
        vec![node_info.as_ref()];
    if let Some(catalog) = catalog.as_deref() {
        owned.push(catalog);
    }
    for store in owned.into_iter().chain(extra_stores.iter().copied()) {
        copy_store(store, dst_dir, report)?;
    }
    Ok(())
}

fn copy_store(
    store: &dyn super::super::durable_stores::BundledStoreSource,
    dst_dir: &std::path::Path,
    report: &mut BackupReport,
) -> Result<(), String> {
    if !store.is_durable() {
        return Ok(());
    }
    let name = store.file_name();
    match super::super::durable_stores::lookup(name).map(|entry| entry.scope) {
        Some(super::super::durable_stores::BackupScope::Bundled) => {}
        _ => {
            return Err(format!(
                "{name} is not a registered bundled durable store; declare it in \
                 durable_stores::DURABLE_STORES before backing it up"
            ));
        }
    }
    if report.bundled_stores.contains_key(name) {
        return Err(format!("duplicate bundled durable store {name}"));
    }
    let rows = store.copy_into(&dst_dir.join(name))?;
    report.bundled_stores.insert(name.to_string(), rows);
    Ok(())
}

fn ensure_backup_stable(backend: &RedbBackend, before: &BackupBoundaries) -> Result<(), String> {
    let admin_after = eg_storage::recovery_store_fingerprint(backend.admin_mutations.kernel())?;
    let xshard_after = super::super::backup::xshard_recovery_fingerprint(&before.shard0)?;
    if before.admin != admin_after || before.xshard != xshard_after {
        return Err(
            "recovery coordinator changed during backup; bundle remains unpublished".to_string(),
        );
    }
    Ok(())
}

impl RedbBackend {
    /// Take an ONLINE consistent backup of the whole durable store into `dst_dir`
    /// (CONCEPT:EG-KG.sharding.reshard-on-restore), while the engine keeps serving. Per shard, opens an
    /// MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the LIVE writer's shared
    /// `Shard` and streams every table verbatim into a bundle shard file named by
    /// the EG-026 [`shard_filename`] scheme, then writes a `MANIFEST.json`
    /// ([`super::super::backup::BackupManifest`]). No quiesce: MVCC lets the snapshot read the
    /// shard's latest committed state concurrently with the writer, and commit-before-ack
    /// (CONCEPT:EG-KG.backend.authoritative-dispatch) makes each per-shard snapshot a self-consistent committed prefix.
    ///
    /// `engine_version` / `timestamp_secs` / `label` are CALLER-SUPPLIED — this library
    /// never reads the wall clock. `dst_dir` is created if absent and must not already
    /// hold bundle shard files (it refuses to overwrite).
    ///
    /// `extra_stores` carries the durable stores this backend does NOT own but that a
    /// restore is incomplete without — `rbac.redb` (identity/RBAC) and `kv.redb`. redb
    /// takes an exclusive per-file lock, so this path cannot open them itself; the
    /// caller (which holds the live `ServerState`) hands in the live handles. The
    /// stores this backend DOES own (`node_info.redb`, `catalog.redb`) are added here.
    /// Every bundled store is declared in the manifest, as is every store deliberately
    /// left out — see [`super::super::durable_stores`].
    pub fn backup(
        &self,
        dst_dir: &std::path::Path,
        engine_version: &str,
        timestamp_secs: u64,
        label: &str,
        extra_stores: &[&dyn super::super::durable_stores::BundledStoreSource],
    ) -> Result<super::super::backup::BackupReport, String> {
        std::fs::create_dir_all(dst_dir).map_err(|e| e.to_string())?;
        let boundaries_before = backup_boundaries(self)?;
        let mut report = BackupReport {
            shards: self.shards.len(),
            ..Default::default()
        };
        #[cfg(feature = "security")]
        record_encryption_key(self, &mut report)?;
        copy_shards(self, dst_dir, &mut report)?;
        copy_owned_stores(self, dst_dir, extra_stores, &mut report)?;
        ensure_backup_stable(self, &boundaries_before)?;
        super::super::backup::write_manifest(
            dst_dir,
            &report,
            engine_version,
            timestamp_secs,
            label,
        )?;
        tracing::info!(
            "online backup complete: {} shards, {} graphs, {} non-shard durable store(s)",
            report.shards,
            report.graph_scopes(),
            report.bundled_stores.len()
        );
        Ok(report)
    }

    /// Group-commit batch-size / linger counters (CONCEPT:EG-KG.backend.adaptive-linger-coalesce). Returns shard 0's
    /// LIVE counter Arc (the only shard under K=1; observability callers are K=1). Use
    /// [`commit_stats_all`] for the per-shard view under K>1.
    pub fn commit_stats(&self) -> Arc<RedbCommitStats> {
        self.shard0().stats.clone()
    }

    /// Per-shard group-commit counters (CONCEPT:EG-KG.backend.sharded-k-way-durable observability).
    pub fn commit_stats_all(&self) -> Vec<Arc<RedbCommitStats>> {
        self.shards.iter().map(|s| s.stats.clone()).collect()
    }

    /// On-disk file path of each shard's redb database (CONCEPT:EG-KG.backend.sharded-k-way-durable diagnostics).
    pub fn shard_db_paths(&self) -> Vec<String> {
        self.shards.iter().map(|s| s.db_path.clone()).collect()
    }
}
