//! Online consistent BACKUP + RESTORE + PITR foundation (CONCEPT:EG-KG.sharding.reshard-on-restore).
//!
//! ## What it solves
//!
//! EG-030 (`shard_migrate`) can copy a durable store verbatim, but only OFFLINE — the
//! engine must be stopped because it opens each authoritative shard with `Database::open`
//! (an exclusive per-file lock). A disaster-recovery story needs a **consistent backup
//! taken while the engine RUNS**, and a matching restore. This is that.
//!
//! ## Online consistent backup — no stop-the-world
//!
//! [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup) takes, PER SHARD, a
//! `Database::begin_read()` MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) on the LIVE writer's shared
//! handle — the same snapshot mechanism the read-through path uses. redb 4.1 is MVCC, so
//! that snapshot sees the shard's LATEST COMMITTED state and runs CONCURRENTLY with the
//! single writer (no writer involvement, no group-commit, no quiesce). Every table is
//! then streamed **verbatim** into a fresh bundle shard file, reusing EG-030's raw-row
//! copy: value blobs are copied byte-for-byte, so
//!
//! * encryption-at-rest blobs survive WITHOUT the key (no decrypt), and
//! * MutationBatch replay/outbox/fence and governed ChangeEnvelope material remain
//!   recoverable with the same typed content versions and cursors, and
//! * the tamper-evident hash-chained `AUDIT` log (CONCEPT:EG-KG.sharding.row-level-security) stays verifiable
//!   (re-deriving it would break verification; copying preserves it).
//!
//! **Cross-shard consistency** rides the commit-before-ack guarantee (CONCEPT:EG-KG.backend.authoritative-dispatch):
//! any ACKED write is already durably committed, so each per-shard snapshot — opened
//! independently — sees a self-consistent committed prefix of the durable history. The
//! backup brackets those copies with cryptographic change tokens for both the admin
//! saga ledger and cross-shard prepare/decision tables. If either recovery boundary
//! changes, no manifest is published and the caller retries. A stable prepared parent
//! is safe because its authenticated recovery plan remains available for idempotent
//! startup replay.
//!
//! ## Bundle format — a portable shard set + manifest
//!
//! A backup bundle is a directory holding:
//!
//! * `graph-<n>.redb` — one verbatim redb file per shard for every K,
//!   using the EG-026 [`shard_filename`](super::redb_backend::shard_filename) names, so
//!   the bundle IS a valid durable shard set on its own.
//! * `admin-mutations.redb` — the portable local projection of placement-group
//!   consensus: admin coordinator receipts, fences, child outbox rows and
//!   authenticated encrypted plans for prepared parents.
//! * `MANIFEST.json` — [`BackupManifest`]: format version, engine version, shard count K,
//!   caller-supplied timestamp, opaque label reference, aggregate-only copied row totals,
//!   and exact portable-file digests.
//!
//! ## Restore — verbatim import, re-shard-on-restore
//!
//! [`restore_bundle`] validates the manifest, then rebuilds a persist-dir from the bundle
//! by DELEGATING to EG-030's [`shard_migrate::migrate_shards`] — the bundle's shard files
//! are exactly the canonical `graph-<n>.redb` set that tool consumes. Restoring at the
//! manifest's own K is a 1:1 verbatim row import; restoring at a DIFFERENT K re-shards on
//! restore (each graph re-routed by the SAME EG-026 `FNV-1a % K`). No decode/re-derive —
//! the audit chain and at-rest ciphertext survive the round trip.
//!
//! ## Point-in-time recovery (PITR)
//!
//! The bundle plus the durable ledger/WAL tail are the low-RPO/RTO DR primitives:
//! restore the latest bundle, then replay the durable ledger tail forward to a target
//! instant. The replay-to-timestamp mechanism is documented in `docs/deployment.md`
//! ("Point-in-time recovery"); this module builds the backup + restore halves it rides.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::path::Path;

use redb::{Database, Durability, ReadOnlyDatabase, ReadableDatabase, ReadableTable};
use sha2::{Digest, Sha256};

#[cfg(feature = "compute-dist")]
use crate::redb_store::MATVIEWS;
use crate::redb_store::{
    AUDIT, CHANGE_BLOBS, CHANGE_CURSORS, CHANGE_ENVELOPES, CHANGE_EVIDENCE, CHANGE_FEATURES,
    CHANGE_LINEAGE, CHANGE_POLICIES, CONTENT_VERSIONS, EDGES, GRAPH_META, LEDGER, MUTATION_BATCHES,
    MUTATION_FENCE, MUTATION_GRAPH_VERSION, MUTATION_IDEMPOTENCY, MUTATION_LIFECYCLE_HEAD,
    MUTATION_OUTBOX, MUTATION_OUTBOX_DELIVERY, MUTATION_PROJECTION_CURSOR, NODES, RAFT_LOG,
    SEMANTIC, XSHARD_DECISION, XSHARD_PREPARE,
};
use crate::server::persistence::redb_backend::RAFT_META;
use crate::server::persistence::shard_migrate;

/// The bundle manifest file name.
pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// The backup bundle on-disk format version. Bumped only on an incompatible layout
/// change; `restore_bundle` refuses a newer format than it understands.
pub const BUNDLE_FORMAT_VERSION: u32 = 4;

/// Separate coordinator store file captured with every portable bundle.
pub const ADMIN_MUTATIONS_FILE: &str = "admin-mutations.redb";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_BACKUP_SHARDS: usize = 64;

/// Cryptographic change token for the cross-shard recovery boundary.
///
/// A backup compares this before and after its per-shard MVCC copies. Any
/// prepare/decision transition makes the in-progress bundle unpublished, rather
/// than presenting a fuzzy cross-shard cut as a recovery point.
pub(crate) fn xshard_recovery_fingerprint(db: &Database) -> Result<[u8; 32], String> {
    fn field(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    field(&mut hasher, b"prepare");
    if let Ok(table) = rtx.open_table(XSHARD_PREPARE) {
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (transaction_id, group_id) = key.value();
            field(&mut hasher, transaction_id.as_bytes());
            field(&mut hasher, &group_id.to_be_bytes());
            field(&mut hasher, value.value());
        }
    }
    field(&mut hasher, b"decision");
    if let Ok(table) = rtx.open_table(XSHARD_DECISION) {
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            field(&mut hasher, key.value().as_bytes());
            field(&mut hasher, &[value.value()]);
        }
    }
    Ok(hasher.finalize().into())
}

/// Row totals copied for ONE shard (CONCEPT:EG-KG.sharding.reshard-on-restore) — the per-shard slice of a
/// [`BackupReport`]. Mirrors the dimensions EG-030's `MigrationReport` tracks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ShardCounts {
    /// Distinct graphs (rows in `GRAPH_META`) in this shard.
    pub graphs: u64,
    /// Node rows.
    pub nodes: u64,
    /// Edge rows.
    pub edges: u64,
    /// Ledger rows.
    pub ledger: u64,
    /// Semantic-store rows.
    pub semantic: u64,
    /// Audit-chain rows (verbatim — chain preserved).
    pub audit: u64,
    /// Mutation replay/outbox and governed ChangeEnvelope rows.
    pub auxiliary: u64,
    /// Global rows (raft log/meta + 2PC + matviews) — non-zero only for shard 0.
    pub global: u64,
    /// In-doubt cross-shard participant prepare records.
    pub xshard_prepares: u64,
    /// Retained cross-shard coordinator decisions.
    pub xshard_decisions: u64,
}

impl std::ops::AddAssign for ShardCounts {
    fn add_assign(&mut self, o: Self) {
        self.graphs += o.graphs;
        self.nodes += o.nodes;
        self.edges += o.edges;
        self.ledger += o.ledger;
        self.semantic += o.semantic;
        self.audit += o.audit;
        self.auxiliary += o.auxiliary;
        self.global += o.global;
        self.xshard_prepares += o.xshard_prepares;
        self.xshard_decisions += o.xshard_decisions;
    }
}

/// Outcome of a backup run (CONCEPT:EG-KG.sharding.reshard-on-restore) — the shard count + copied totals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackupReport {
    /// Number of shard files written into the bundle (= K).
    pub shards: usize,
    /// Distinct graphs across all shards.
    pub graphs: u64,
    /// Node rows copied.
    pub nodes: u64,
    /// Edge rows copied.
    pub edges: u64,
    /// Ledger rows copied.
    pub ledger: u64,
    /// Semantic-store rows copied.
    pub semantic: u64,
    /// Audit-chain rows copied.
    pub audit: u64,
    pub auxiliary: u64,
    /// Global rows copied (shard-0 raft/2PC/matviews).
    pub global: u64,
    pub xshard_prepares: u64,
    pub xshard_decisions: u64,
    pub admin_mutations: eg_mutation_store::RecoveryStoreCounts,
}

impl BackupReport {
    /// Fold one shard's counts into the running totals.
    pub fn add_shard(&mut self, c: ShardCounts) {
        self.graphs += c.graphs;
        self.nodes += c.nodes;
        self.edges += c.edges;
        self.ledger += c.ledger;
        self.semantic += c.semantic;
        self.audit += c.audit;
        self.auxiliary += c.auxiliary;
        self.global += c.global;
        self.xshard_prepares += c.xshard_prepares;
        self.xshard_decisions += c.xshard_decisions;
    }
}

/// The bundle manifest (CONCEPT:EG-KG.sharding.reshard-on-restore) — serialized to `MANIFEST.json` at backup and
/// validated at restore. All non-derived fields (timestamp, label reference, engine version) are
/// CALLER-SUPPLIED — this module never calls `Date::now` (no wall-clock / randomness in
/// library code).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    /// On-disk bundle format version ([`BUNDLE_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Engine version that produced the bundle (caller-supplied, e.g. `CARGO_PKG_VERSION`).
    pub engine_version: String,
    /// Shard count K — the number of `graph*.redb` files in the bundle.
    pub shard_count: usize,
    /// Caller-supplied Unix-seconds timestamp of the backup.
    pub timestamp: u64,
    /// Opaque SHA-256 reference to the caller-supplied label (including empty).
    pub label_ref: String,
    /// Distinct graphs captured.
    pub graphs: u64,
    /// Node rows captured.
    pub nodes: u64,
    /// Edge rows captured.
    pub edges: u64,
    /// Ledger rows captured.
    pub ledger: u64,
    /// Semantic-store rows captured.
    pub semantic: u64,
    /// Audit-chain rows captured.
    pub audit: u64,
    pub auxiliary: u64,
    /// Global rows captured.
    pub global: u64,
    /// Retained in-doubt participant records needed by recovery.
    pub xshard_prepares: u64,
    /// Retained coordinator decisions needed to resolve prepared participants.
    pub xshard_decisions: u64,
    /// Integrity totals for the separate admin coordinator ledger.
    pub admin_mutations: eg_mutation_store::RecoveryStoreCounts,
    /// Exact SHA-256 for every portable graph shard and coordinator store.
    /// Keys are bundle-local generic file names, never host paths.
    pub file_digests: BTreeMap<String, String>,
}

impl BackupManifest {
    fn from_report(
        report: &BackupReport,
        engine_version: &str,
        timestamp: u64,
        label: &str,
        file_digests: BTreeMap<String, String>,
    ) -> Self {
        Self {
            format_version: BUNDLE_FORMAT_VERSION,
            engine_version: engine_version.to_string(),
            shard_count: report.shards,
            timestamp,
            label_ref: opaque_text_ref(label),
            graphs: report.graphs,
            nodes: report.nodes,
            edges: report.edges,
            ledger: report.ledger,
            semantic: report.semantic,
            audit: report.audit,
            auxiliary: report.auxiliary,
            global: report.global,
            xshard_prepares: report.xshard_prepares,
            xshard_decisions: report.xshard_decisions,
            admin_mutations: report.admin_mutations,
            file_digests,
        }
    }
}

fn opaque_text_ref(value: &str) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("sha256:{encoded}")
}

fn is_sha256_ref(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn file_sha256(path: &Path) -> Result<String, String> {
    let mut stream = std::fs::File::open(path).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = stream
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

fn portable_file_digests(
    dir: &Path,
    shard_files: &[std::path::PathBuf],
) -> Result<BTreeMap<String, String>, String> {
    let mut files = shard_files.to_vec();
    files.push(dir.join(ADMIN_MUTATIONS_FILE));
    let mut digests = BTreeMap::new();
    for path in files {
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|_| "backup contains an unavailable portable file".to_string())?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("backup portable files must be regular files".to_string());
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| "backup contains a non-portable file name".to_string())?
            .to_string();
        if digests.insert(name, file_sha256(&path)?).is_some() {
            return Err("backup contains a duplicate portable file name".to_string());
        }
    }
    Ok(digests)
}

/// Copy EVERY durable table from a source read snapshot into a fresh destination write
/// txn, VERBATIM (CONCEPT:EG-KG.sharding.reshard-on-restore). Unlike EG-030's migration (which routes rows by
/// `shard_index`), backup mirrors ONE shard 1:1 — the source snapshot already holds
/// exactly the rows that shard owns, so no routing/filter is applied. `is_shard0`
/// includes the global (non-per-graph) tables, which live on shard 0 only (EG-026).
///
/// Value blobs are copied byte-for-byte (no decode/unseal), so encryption-at-rest blobs
/// and the KG-2.231 hash-chained audit log survive without the key and stay verifiable.
pub(crate) fn copy_snapshot_verbatim(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
    is_shard0: bool,
) -> Result<ShardCounts, String> {
    let mut counts = ShardCounts::default();

    // Open (create) every per-graph table on the destination so the bundle file matches
    // what `Shard::open` / `migrate_shards` expect.
    let mut d_nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
    let mut d_edges = wtx.open_table(EDGES).map_err(|e| e.to_string())?;
    let mut d_ledger = wtx.open_table(LEDGER).map_err(|e| e.to_string())?;
    let mut d_semantic = wtx.open_table(SEMANTIC).map_err(|e| e.to_string())?;
    let mut d_meta = wtx.open_table(GRAPH_META).map_err(|e| e.to_string())?;
    let mut d_audit = wtx.open_table(AUDIT).map_err(|e| e.to_string())?;

    if let Ok(t) = rtx.open_table(GRAPH_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_meta
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.graphs += 1;
        }
    }
    if let Ok(t) = rtx.open_table(NODES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_nodes
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.nodes += 1;
        }
    }
    if let Ok(t) = rtx.open_table(EDGES) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_edges
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.edges += 1;
        }
    }
    if let Ok(t) = rtx.open_table(LEDGER) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_ledger
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.ledger += 1;
        }
    }
    if let Ok(t) = rtx.open_table(SEMANTIC) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_semantic
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.semantic += 1;
        }
    }
    // Audit — verbatim to keep the hash chain verifiable (CONCEPT:EG-KG.sharding.row-level-security).
    if let Ok(t) = rtx.open_table(AUDIT) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_audit
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            counts.audit += 1;
        }
    }

    macro_rules! copy_aux_table {
        ($definition:expr) => {{
            let mut destination = wtx.open_table($definition).map_err(|e| e.to_string())?;
            if let Ok(source) = rtx.open_table($definition) {
                for row in source.iter().map_err(|e| e.to_string())? {
                    let (key, value) = row.map_err(|e| e.to_string())?;
                    destination
                        .insert(key.value(), value.value())
                        .map_err(|e| e.to_string())?;
                    counts.auxiliary += 1;
                }
            }
        }};
    }
    copy_aux_table!(MUTATION_BATCHES);
    copy_aux_table!(MUTATION_IDEMPOTENCY);
    copy_aux_table!(MUTATION_OUTBOX);
    copy_aux_table!(MUTATION_OUTBOX_DELIVERY);
    copy_aux_table!(MUTATION_PROJECTION_CURSOR);
    copy_aux_table!(MUTATION_GRAPH_VERSION);
    copy_aux_table!(MUTATION_FENCE);
    copy_aux_table!(MUTATION_LIFECYCLE_HEAD);
    copy_aux_table!(CHANGE_ENVELOPES);
    copy_aux_table!(CONTENT_VERSIONS);
    copy_aux_table!(CHANGE_CURSORS);
    copy_aux_table!(CHANGE_BLOBS);
    copy_aux_table!(CHANGE_FEATURES);
    copy_aux_table!(CHANGE_EVIDENCE);
    copy_aux_table!(CHANGE_POLICIES);
    copy_aux_table!(CHANGE_LINEAGE);

    if is_shard0 {
        let (global, prepares, decisions) = copy_global_verbatim(rtx, wtx)?;
        counts.global += global;
        counts.xshard_prepares += prepares;
        counts.xshard_decisions += decisions;
    }
    Ok(counts)
}

/// Copy the GLOBAL (non-per-graph) tables verbatim — the Raft log/meta, the cross-shard
/// 2PC records, and the materialized views (CONCEPT:EG-KG.sharding.reshard-on-restore). These are EG-026 "shard 0
/// home" records, captured only from the shard-0 snapshot.
fn copy_global_verbatim(
    rtx: &redb::ReadTransaction,
    wtx: &redb::WriteTransaction,
) -> Result<(u64, u64, u64), String> {
    let mut count = 0u64;
    let mut prepares = 0u64;
    let mut decisions = 0u64;
    let mut d_raft_log = wtx.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
    let mut d_raft_meta = wtx.open_table(RAFT_META).map_err(|e| e.to_string())?;
    let mut d_xprep = wtx.open_table(XSHARD_PREPARE).map_err(|e| e.to_string())?;
    let mut d_xdec = wtx.open_table(XSHARD_DECISION).map_err(|e| e.to_string())?;
    #[cfg(feature = "compute-dist")]
    let mut d_matviews = wtx.open_table(MATVIEWS).map_err(|e| e.to_string())?;

    if let Ok(t) = rtx.open_table(RAFT_LOG) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_raft_log
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    if let Ok(t) = rtx.open_table(RAFT_META) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_raft_meta
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    if let Ok(t) = rtx.open_table(XSHARD_PREPARE) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_xprep
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
            prepares += 1;
        }
    }
    if let Ok(t) = rtx.open_table(XSHARD_DECISION) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_xdec
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
            decisions += 1;
        }
    }
    #[cfg(feature = "compute-dist")]
    if let Ok(t) = rtx.open_table(MATVIEWS) {
        for row in t.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            d_matviews
                .insert(k.value(), v.value())
                .map_err(|e| e.to_string())?;
            count += 1;
        }
    }
    Ok((count, prepares, decisions))
}

/// Write ONE shard's `begin_read()` snapshot verbatim into `dst_path` (a fresh bundle
/// redb file) and return the copied counts (CONCEPT:EG-KG.sharding.reshard-on-restore). Called once per shard by
/// [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup).
pub(crate) fn write_bundle_shard(
    src_db: &Database,
    dst_path: &Path,
    is_shard0: bool,
) -> Result<ShardCounts, String> {
    if dst_path.exists() {
        return Err(format!(
            "bundle shard file already exists: {} (refusing to overwrite)",
            dst_path.display()
        ));
    }
    let rtx = src_db.begin_read().map_err(|e| e.to_string())?;
    let dst_db =
        Database::create(dst_path).map_err(|e| format!("create {}: {e}", dst_path.display()))?;
    let mut wtx = dst_db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let counts = copy_snapshot_verbatim(&rtx, &wtx, is_shard0)?;
    wtx.commit().map_err(|e| e.to_string())?;
    Ok(counts)
}

/// Serialize + write the bundle manifest to `<dir>/MANIFEST.json` (CONCEPT:EG-KG.sharding.reshard-on-restore).
pub(crate) fn write_manifest(
    dir: &Path,
    report: &BackupReport,
    engine_version: &str,
    timestamp: u64,
    label: &str,
) -> Result<BackupManifest, String> {
    let final_path = dir.join(MANIFEST_FILE);
    if final_path.exists() {
        return Err("backup manifest already exists (refusing to overwrite)".to_string());
    }
    let shard_files = crate::redb_layout::discover_current_shards(dir)?;
    if shard_files.len() != report.shards {
        return Err("backup shard-file count changed before publication".to_string());
    }
    let file_digests = portable_file_digests(dir, &shard_files)?;
    let manifest =
        BackupManifest::from_report(report, engine_version, timestamp, label, file_digests);
    let json = serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?;
    let temporary_path = dir.join(".MANIFEST.json.tmp");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(|error| error.to_string())?;
    if let Err(error) = file.write_all(&json).and_then(|()| file.sync_all()) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error.to_string());
    }
    drop(file);
    if let Err(error) = std::fs::rename(&temporary_path, &final_path) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error.to_string());
    }
    #[cfg(unix)]
    std::fs::File::open(dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(manifest)
}

/// Read + validate a bundle manifest from `<dir>/MANIFEST.json` (CONCEPT:EG-KG.sharding.reshard-on-restore).
pub fn read_manifest(dir: &Path) -> Result<BackupManifest, String> {
    let path = dir.join(MANIFEST_FILE);
    let metadata =
        std::fs::symlink_metadata(&path).map_err(|_| "read bundle manifest failed".to_string())?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_MANIFEST_BYTES
    {
        return Err("backup manifest must be a bounded regular file".to_string());
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::fs::File::open(&path)
        .and_then(|file| file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|_| "read bundle manifest failed".to_string())?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err("backup manifest exceeds the size limit".to_string());
    }
    let manifest: BackupManifest =
        serde_json::from_slice(&bytes).map_err(|_| "parse bundle manifest failed".to_string())?;
    if manifest.format_version != BUNDLE_FORMAT_VERSION {
        return Err(format!(
            "bundle format version {} is not the portable coordinator-aware format ({})",
            manifest.format_version, BUNDLE_FORMAT_VERSION
        ));
    }
    if !is_sha256_ref(&manifest.label_ref) {
        return Err("backup label reference is not an opaque digest".to_string());
    }
    if manifest.shard_count == 0 || manifest.shard_count > MAX_BACKUP_SHARDS {
        return Err("backup manifest shard count is outside the supported range".to_string());
    }
    if manifest.engine_version.is_empty()
        || manifest.engine_version.len() > 128
        || manifest.engine_version.chars().any(char::is_control)
    {
        return Err("backup manifest engine version is invalid".to_string());
    }
    if manifest.file_digests.len() != manifest.shard_count + 1
        || manifest
            .file_digests
            .values()
            .any(|digest| !is_sha256_ref(digest))
    {
        return Err("backup manifest file digest inventory is invalid".to_string());
    }
    let shard_files = crate::redb_layout::discover_current_shards(dir)?;
    if shard_files.len() != manifest.shard_count {
        return Err("backup shard-file count does not match the manifest".to_string());
    }
    let admin_path = dir.join(ADMIN_MUTATIONS_FILE);
    let admin_metadata = std::fs::symlink_metadata(&admin_path)
        .map_err(|_| "backup bundle omits the admin mutation coordinator store".to_string())?;
    if admin_metadata.file_type().is_symlink() || !admin_metadata.is_file() {
        return Err("backup bundle omits the admin mutation coordinator store".to_string());
    }
    // Opened READ-ONLY (not `Database::open`) deliberately: this is a pure
    // validation read, and `Database`'s `Drop` unconditionally commits its own
    // allocator-state quick-repair, which would advance the file's transaction id
    // and change its on-disk bytes on every validation pass — permanently
    // breaking the digest check just below (see `validate_recovery_store`'s
    // doc comment).
    let admin = ReadOnlyDatabase::open(&admin_path).map_err(|error| error.to_string())?;
    let counts = eg_mutation_store::validate_recovery_store(&admin)?;
    if counts != manifest.admin_mutations {
        return Err("admin mutation coordinator totals do not match the manifest".to_string());
    }
    drop(admin);
    let actual_digests = portable_file_digests(dir, &shard_files)?;
    if actual_digests != manifest.file_digests {
        return Err("backup portable-file digests do not match the manifest".to_string());
    }
    Ok(manifest)
}

/// Outcome of a restore (CONCEPT:EG-KG.sharding.reshard-on-restore) — the validated manifest + the verbatim import
/// totals produced by the EG-030 migration engine.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// The bundle's validated manifest.
    pub manifest: BackupManifest,
    /// Explicit shard count the persist-dir was rebuilt at.
    pub restored_shards: usize,
    /// Verbatim row-import totals (EG-030 `MigrationReport`).
    pub migration: shard_migrate::MigrationReport,
    /// Validated coordinator receipts and encrypted staged recovery plans.
    pub admin_mutations: eg_mutation_store::RecoveryStoreCounts,
}

/// Rebuild a persist-dir from a backup bundle (CONCEPT:EG-KG.sharding.reshard-on-restore). Validates the manifest,
/// then verbatim-imports every bundle shard into `persist_dir` by delegating to EG-030's
/// [`shard_migrate::migrate_shards`] (the bundle IS a valid `graph*.redb` shard set).
///
/// `target_shards` is mandatory. Passing the manifest's own K performs a 1:1 import;
/// passing a different K performs RE-SHARD ON RESTORE through the same EG-026
/// `FNV-1a % K` routing rule.
///
/// `persist_dir` must not already hold target shard files (the migration refuses to
/// clobber) — restore into a FRESH dir. OFFLINE with respect to the TARGET: nothing may
/// be serving out of `persist_dir` while it is rebuilt.
pub fn restore_bundle(
    bundle_dir: &Path,
    persist_dir: &Path,
    target_shards: usize,
) -> Result<RestoreReport, String> {
    let manifest = read_manifest(bundle_dir)?;
    if !(1..=64).contains(&target_shards) {
        return Err("restore target shard count is outside bounds".to_string());
    }
    let k = target_shards;
    // The bundle's graph*.redb files are exactly the source shard set EG-030 consumes.
    let migration = shard_migrate::migrate_shards(bundle_dir, persist_dir, k)?;
    if migration.source_shards != manifest.shard_count
        || migration.graphs as u64 != manifest.graphs
        || migration.nodes != manifest.nodes
        || migration.edges != manifest.edges
        || migration.ledger != manifest.ledger
        || migration.semantic != manifest.semantic
        || migration.audit != manifest.audit
        || migration.auxiliary != manifest.auxiliary
        || migration.global != manifest.global
    {
        return Err("restored graph totals do not match the backup manifest".to_string());
    }
    let admin_source = bundle_dir.join(ADMIN_MUTATIONS_FILE);
    let admin_target = persist_dir.join(ADMIN_MUTATIONS_FILE);
    if admin_target.exists() {
        return Err("restore target already contains an admin mutation store".to_string());
    }
    std::fs::copy(&admin_source, &admin_target).map_err(|error| error.to_string())?;
    // Read-only for the same reason as `read_manifest`'s admin validation above:
    // pure validation, and a read-write `Database::open` would leave an
    // unnecessary extra transaction committed into the freshly-restored store
    // before the persistence backend ever takes ownership of it.
    let restored_admin =
        ReadOnlyDatabase::open(&admin_target).map_err(|error| error.to_string())?;
    let admin_mutations = eg_mutation_store::validate_recovery_store(&restored_admin)?;
    if admin_mutations != manifest.admin_mutations {
        return Err("restored admin mutation coordinator totals changed".to_string());
    }
    Ok(RestoreReport {
        manifest,
        restored_shards: k,
        migration,
        admin_mutations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::DurabilityPolicy;
    use crate::protocol::{GraphType, Method};
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Write G graphs (each with two nodes + an edge) durably through a backend.
    async fn seed(dir: &str, shards: usize, graphs: &[&str]) {
        let backend =
            RedbBackend::open_with_shards(dir.to_string(), DurabilityPolicy::Each, 256, shards)
                .expect("open backend");
        for g in graphs {
            backend
                .register_graph(g, g, GraphType::Global)
                .await
                .expect("register");
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "a".into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task", "g": g})),
                    },
                )
                .await
                .expect("node a");
            backend
                .record_durable(
                    g,
                    &Method::AddNode {
                        node_id: "b".into(),
                        properties_msgpack: props(serde_json::json!({"type": "Task"})),
                    },
                )
                .await
                .expect("node b");
            backend
                .record_durable(
                    g,
                    &Method::AddEdge {
                        source_id: "a".into(),
                        target_id: "b".into(),
                        properties_msgpack: props(serde_json::json!({"w": 1})),
                    },
                )
                .await
                .expect("edge");
        }
        backend.shutdown();
    }

    /// CONCEPT:EG-KG.sharding.reshard-on-restore — the DR round trip: populate a durable dir → ONLINE backup (live,
    /// no quiesce) → restore into a FRESH dir → reopen and assert every graph's
    /// nodes/edges/ledger survive identically.
    #[tokio::test(flavor = "multi_thread")]
    async fn backup_restore_roundtrip_preserves_everything() {
        // Held for the whole test: it opens a `RedbBackend` (`src`), backs it up, then
        // opens a SECOND `RedbBackend` (`restored`) and requires the restored data to
        // read back identically — which only holds if `EPISTEMIC_GRAPH_ENCRYPTION_KEY`
        // resolves the SAME way at both opens. See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = std::env::temp_dir().join(format!("eg-backup-rt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = ["alpha", "beta", "gamma", "delta", "epsilon"];
        seed(&src_s, 3, &graphs).await;

        // ── ONLINE backup: reopen the SAME dir (K=3) and back it up while it is live ──
        let backend = RedbBackend::open_with_shards(src_s.clone(), DurabilityPolicy::Each, 256, 3)
            .expect("reopen");
        assert_eq!(backend.shard_count(), 3);
        let report = backend
            .backup(&bundle, "test-engine", 1_700_000_000, "nightly")
            .expect("backup");
        assert_eq!(report.shards, 3);
        assert_eq!(report.graphs, graphs.len() as u64);
        assert_eq!(report.nodes, (graphs.len() * 2) as u64);
        assert_eq!(report.edges, graphs.len() as u64);
        // Capture the LIVE per-graph shape so restore can be proven identical.
        let mut source: std::collections::HashMap<String, (usize, usize, usize)> =
            std::collections::HashMap::new();
        for g in &graphs {
            let d = backend.read_graph_dump_blocking(g).unwrap().unwrap();
            source.insert(
                (*g).to_string(),
                (d.nodes.len(), d.edges.len(), d.ledger.len()),
            );
        }
        backend.shutdown();

        // Bundle holds K=3 shard files + a manifest.
        for i in 0..3 {
            assert!(bundle.join(format!("graph-{i}.redb")).exists(), "shard {i}");
        }
        assert!(
            bundle.join(ADMIN_MUTATIONS_FILE).exists(),
            "admin coordinator store"
        );
        let manifest = read_manifest(&bundle).expect("manifest");
        assert_eq!(manifest.shard_count, 3);
        assert_eq!(manifest.engine_version, "test-engine");
        assert_eq!(manifest.timestamp, 1_700_000_000);
        assert_eq!(manifest.label_ref, opaque_text_ref("nightly"));
        assert_eq!(manifest.graphs, graphs.len() as u64);

        // ── restore into a FRESH dir at the same K ──
        let rr = restore_bundle(&bundle, &restored, 3).expect("restore");
        assert_eq!(rr.restored_shards, 3);
        assert_eq!(rr.migration.graphs, graphs.len());
        assert_eq!(rr.migration.nodes, (graphs.len() * 2) as u64);

        // ── reopen the restored dir and verify every graph is intact ──
        let restored_s = restored.to_string_lossy().to_string();
        let rb = RedbBackend::open_with_shards(restored_s, DurabilityPolicy::Each, 256, 3)
            .expect("reopen");
        assert_eq!(rb.shard_count(), 3);
        for g in &graphs {
            let dump = rb
                .read_graph_dump_blocking(g)
                .expect("read")
                .unwrap_or_else(|| panic!("graph {g} missing after restore"));
            assert_eq!(dump.name, *g);
            assert_eq!(dump.nodes.len(), 2, "graph {g} nodes");
            assert_eq!(dump.edges.len(), 1, "graph {g} edges");
            // Restored shape is IDENTICAL to the live source (nodes, edges, ledger).
            let src = source.get(*g).copied().expect("source shape");
            assert_eq!(
                (dump.nodes.len(), dump.edges.len(), dump.ledger.len()),
                src,
                "graph {g} shape identical after restore"
            );
            let a = dump
                .nodes
                .iter()
                .find(|(id, _)| id == "a")
                .map(|(_, blob)| blob.clone())
                .expect("node a present");
            let val: serde_json::Value = rmp_serde::from_slice(&a).unwrap();
            assert_eq!(val.get("g").and_then(|x| x.as_str()), Some(*g));
        }
        rb.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONCEPT:EG-KG.sharding.reshard-on-restore — RE-SHARD ON RESTORE: a K=1 bundle restores into a K=4 persist-dir,
    /// every graph re-routed by EG-026 and still fully readable.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_can_reshard() {
        // See `backup_restore_roundtrip_preserves_everything` above: held for the
        // whole test (this one also opens a source backend and a re-sharded restored
        // backend that must resolve the same cipher).
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = std::env::temp_dir().join(format!("eg-backup-reshard-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let graphs = ["one", "two", "three", "four", "five", "six", "seven"];
        seed(&src_s, 1, &graphs).await;

        let backend = RedbBackend::open_with_shards(src_s.clone(), DurabilityPolicy::Each, 256, 1)
            .expect("reopen");
        let report = backend
            .backup(&bundle, "test-engine", 42, "")
            .expect("backup");
        assert_eq!(report.shards, 1);
        assert!(bundle.join("graph-0.redb").exists(), "K=1 bundle file");
        backend.shutdown();

        // Restore at K=4 (re-shard on restore).
        let rr = restore_bundle(&bundle, &restored, 4).expect("restore reshard");
        assert_eq!(rr.restored_shards, 4);
        assert_eq!(rr.migration.graphs, graphs.len());

        let restored_s = restored.to_string_lossy().to_string();
        let rb = RedbBackend::open_with_shards(restored_s, DurabilityPolicy::Each, 256, 4)
            .expect("reopen");
        assert_eq!(rb.shard_count(), 4, "restored at K=4");
        for g in &graphs {
            assert!(
                rb.read_graph_dump_blocking(g).unwrap().is_some(),
                "graph {g} after reshard-restore"
            );
        }
        rb.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A restore refuses a manifest with a future format version.
    #[test]
    fn rejects_future_format() {
        let dir = std::env::temp_dir().join(format!("eg-backup-fmt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // The manifest schema has grown fields (`label_ref`/`auxiliary`/
        // `xshard_prepares`/`xshard_decisions`/`admin_mutations`/`file_digests`) since
        // this fixture was first written; a manifest missing any of them now fails
        // `serde(deny_unknown_fields)` DESERIALIZATION before `read_manifest` ever
        // reaches its `format_version` check, so the old shape asserted the wrong
        // error message. This fixture is kept in sync with the CURRENT
        // `BackupManifest` shape (every field present, `format_version` deliberately
        // one past what this build understands) so the test exercises exactly the
        // format-version guard it names, not a parse failure one step earlier.
        let m = serde_json::json!({
            "format_version": BUNDLE_FORMAT_VERSION + 1,
            "engine_version": "x",
            "shard_count": 1,
            "timestamp": 0,
            "label_ref": opaque_text_ref(""),
            "graphs": 0, "nodes": 0, "edges": 0, "ledger": 0, "semantic": 0, "audit": 0,
            "auxiliary": 0, "global": 0,
            "xshard_prepares": 0, "xshard_decisions": 0,
            "admin_mutations": eg_mutation_store::RecoveryStoreCounts::default(),
            "file_digests": {},
        });
        std::fs::write(dir.join(MANIFEST_FILE), serde_json::to_vec(&m).unwrap()).unwrap();
        let err = read_manifest(&dir).unwrap_err();
        assert!(
            err.contains("not the portable coordinator-aware format"),
            "got: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
