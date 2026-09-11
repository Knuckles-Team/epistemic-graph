//! Online consistent BACKUP + RESTORE + PITR foundation (CONCEPT:EG-KG.sharding.reshard-on-restore).
//!
//! ## What it solves
//!
//! EG-030 (`shard_migrate`) can copy a durable store verbatim, but only OFFLINE — the
//! engine must be stopped because it opens each authoritative shard exclusively. A
//! disaster-recovery story needs a **consistent backup taken while the engine RUNS**,
//! and a matching restore. This is that.
//!
//! ## Online consistent backup — no stop-the-world, and no table list
//!
//! [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup) calls
//! `write_bundle_shard` once per shard, which is one call to
//! [`eg_storage::backup_recovery_store`] on that shard file's `StorageKernel`.
//! The kernel takes its own MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer) of the LIVE writer's
//! store — redb 4.1 is MVCC, so it sees the shard's LATEST COMMITTED state and runs
//! CONCURRENTLY with the single writer (no writer involvement, no group-commit, no
//! quiesce) — and copies, into a fresh bundle file:
//!
//! * every table of the DECLARED `OwnerLayout::GraphShard` census
//!   (`copy_declared_owner_tables`), and
//! * every ledger table of the authoritative list (`visit_ledger_content_tables!`),
//!   rebinding each logical scope to the destination's own physical root.
//!
//! That census is the whole point. Unlike `shard_migrate`/`online_reshard`, which route
//! rows by graph, **nothing here is graph-filtered — the whole shard moves as one unit**,
//! which is exactly the operation the kernel already owns. WD5-BUG-04 (and the
//! BUG-CX-016/BUG-CX-054 class before it) was a hand-maintained copy list that silently
//! omitted 30 tables; a list nobody maintains cannot omit anything, so "no table is
//! silently missed" stopped being a property this module asserts and became one it
//! cannot violate. The same census carries, for free and without a line of code here:
//!
//! * value blobs byte-for-byte (no decode/unseal), so encryption-at-rest survives
//!   WITHOUT the key and the tamper-evident hash-chained `audit_chain`
//!   (CONCEPT:EG-KG.sharding.row-level-security) stays verifiable;
//! * `encryption_canary` — a declared FileWide owner table — so a restore retains the
//!   original key identity/version boundary and cannot silently establish a new canary
//!   under a different key;
//! * the file-wide Raft log/meta, the cross-shard 2PC records and the matview key
//!   spaces, so the shard-0-only special case is gone as well;
//! * the MutationKernel ledger (receipts, idempotency, versions, fences, outbox,
//!   deliveries, cursors, replay evidence, classes) — the shard's own retired private
//!   `mutation_*` tables no longer exist, and the ledger that replaced them is copied by
//!   the same authoritative list rather than by eight more hand-written copies.
//!
//! `backup_recovery_store` validates the destination before returning, so a bundle shard
//! is a proven-recoverable owner file, and its [`RecoveryStoreCounts`] census is what the
//! manifest records per shard.
//!
//! **Cross-shard consistency** rides the commit-before-ack guarantee (CONCEPT:EG-KG.backend.authoritative-dispatch):
//! any ACKED write is already durably committed, so each per-shard snapshot — opened
//! independently — sees a self-consistent committed prefix of the durable history. The
//! backup brackets those copies with cryptographic change tokens for both the admin
//! saga ledger ([`eg_storage::recovery_store_fingerprint`]) and the shard's cross-shard
//! prepare/decision records (`xshard_recovery_boundary`). If either recovery boundary
//! changes, no manifest is published and the caller retries. A stable prepared parent
//! is safe because its authenticated recovery plan remains available for idempotent
//! startup replay.
//!
//! ## Bundle format — a portable shard set + manifest
//!
//! A backup bundle is a directory holding:
//!
//! * `graph-<n>.redb` — one verbatim kernel owner file per shard for every K,
//!   using the EG-026 [`shard_filename`](super::redb_backend::shard_filename) names, so
//!   the bundle IS a valid durable shard set on its own.
//! * `admin-mutations.redb` — the portable local projection of placement-group
//!   consensus: admin coordinator receipts, fences, child outbox rows and
//!   authenticated encrypted plans for prepared parents.
//! * the NON-SHARD durable stores a restore is incomplete without — `rbac.redb`
//!   (roles, grants, registered identities, bootstrap lifecycle), `kv.redb`,
//!   `node_info.redb` and `catalog.redb`. See [`super::durable_stores`].
//! * `MANIFEST.json` — [`BackupManifest`]: format version, engine version, shard count K,
//!   caller-supplied timestamp, opaque label reference, the per-shard recovery census,
//!   exact portable-file digests, the `bundled_stores` inventory, and the
//!   `excluded_stores` map naming every durable store this bundle deliberately does NOT
//!   carry, WITH ITS REASON.
//!
//! ## Scope is declared, never implied (BUG-PE-054)
//!
//! Before this the bundle held graph shards + `admin-mutations.redb` and NOTHING else,
//! while the manifest described only what it had captured. A restore therefore came up
//! with no RBAC/identity state at all — a backup that silently cannot restore identity
//! is worse than no backup, because it is trusted. redb's exclusive per-file lock means
//! this module can never simply open a sibling store, so the two halves of the fix are:
//! the caller hands in the live handles it owns ([`super::durable_stores::BundledStoreSource`]),
//! and everything still left out is NAMED IN THE MANIFEST with the reason it was left
//! out. `durable_stores`' `registry_covers_every_redb_store` test is what stops a future
//! `.redb` from being silently forgotten again.
//!
//! ## Restore — verbatim import, re-shard-on-restore
//!
//! [`restore_bundle`] validates the manifest — including re-deriving every bundle
//! shard's census from the FILE and comparing it to what the manifest claims — then
//! rebuilds a persist-dir from the bundle by DELEGATING to EG-030's
//! [`shard_migrate::migrate_shards`]: the bundle's shard files are exactly the canonical
//! `graph-<n>.redb` set that tool consumes. Restoring at the manifest's own K is a 1:1
//! verbatim row import; restoring at a DIFFERENT K re-shards on restore (each graph
//! re-routed by the SAME EG-026 `FNV-1a % K`). No decode/re-derive — the audit chain and
//! at-rest ciphertext survive the round trip.
//!
//! ## Point-in-time recovery (PITR)
//!
//! The bundle plus the durable ledger/WAL tail are the low-RPO/RTO DR primitives:
//! restore the latest bundle, then replay the durable ledger tail forward to a target
//! instant. The replay-to-timestamp mechanism is documented in `docs/deployment.md`
//! ("Point-in-time recovery"); this module builds the backup + restore halves it rides.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use eg_storage::RecoveryStoreCounts;
use redb::ReadableTable;
use sha2::{Digest, Sha256};

use crate::redb_store::shard::Shard;
use crate::redb_store::{XSHARD_DECISION, XSHARD_PREPARE};
use crate::server::persistence::shard_migrate;

/// The bundle manifest file name.
pub const MANIFEST_FILE: &str = "MANIFEST.json";

/// The backup bundle on-disk format version. Bumped only on an incompatible layout
/// change; `restore_bundle` refuses a newer format than it understands.
///
/// Version 5 is the kernel-census manifest: the hand-counted row dimensions
/// (`nodes`/`edges`/`ledger`/`semantic`/`audit`/`auxiliary`/`global`/
/// `capability_and_resource`) are replaced by [`BackupManifest::shard_counts`], the
/// per-shard [`RecoveryStoreCounts`] the kernel derives from the declared census. A
/// version-4 bundle cannot be read as one, and must not be: its counters described a
/// copy list that no longer exists.
pub const BUNDLE_FORMAT_VERSION: u32 = 5;

/// Separate coordinator store file captured with every portable bundle.
pub const ADMIN_MUTATIONS_FILE: &str = "admin-mutations.redb";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_BACKUP_SHARDS: usize = 64;

/// The cross-shard recovery boundary of ONE shard file: a cryptographic change token
/// over the in-doubt participant records and retained coordinator decisions, plus the
/// count of each.
///
/// A backup compares the whole boundary before and after its per-shard copies. Any
/// prepare/decision transition makes the in-progress bundle unpublished, rather than
/// presenting a fuzzy cross-shard cut as a recovery point. The counts ride along
/// because they are read from the same two tables in the same pass — the operator
/// receipt reports "how many in-doubt records is this bundle carrying", and reading
/// them twice would be a second census of the same rows.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct XshardRecoveryBoundary {
    pub(crate) fingerprint: [u8; 32],
    /// In-doubt cross-shard participant prepare records.
    pub(crate) prepares: u64,
    /// Retained cross-shard coordinator decisions.
    pub(crate) decisions: u64,
}

/// Read one shard file's cross-shard recovery boundary.
///
/// `xshard_prepare` and `xshard_decision` are FileWide (`TableScope::StorePrivate`)
/// tables, so this is a control-scope read: the boundary belongs to the file, not to
/// any one graph. This stays hand-written — deliberately — because it must be NARROW.
/// `recovery_store_fingerprint` over the shard's whole ledger would also change on
/// every ordinary committed write, which would make an online backup of a serving
/// shard unpublishable by construction; the boundary that invalidates a bundle is a
/// 2PC transition, not any write at all.
pub(crate) fn xshard_recovery_boundary(shard: &Shard) -> Result<XshardRecoveryBoundary, String> {
    fn field(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let read = shard.control_read()?;
    let mut hasher = Sha256::new();
    let mut prepares = 0u64;
    let mut decisions = 0u64;
    field(&mut hasher, b"prepare");
    let prepare = read.open_owner_table(XSHARD_PREPARE)?;
    for row in prepare.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (transaction_id, group_id) = key.value();
        field(&mut hasher, transaction_id.as_bytes());
        field(&mut hasher, &group_id.to_be_bytes());
        field(&mut hasher, value.value());
        prepares = prepares.saturating_add(1);
    }
    drop(prepare);
    field(&mut hasher, b"decision");
    let decision = read.open_owner_table(XSHARD_DECISION)?;
    for row in decision.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        field(&mut hasher, key.value().as_bytes());
        field(&mut hasher, &[value.value()]);
        decisions = decisions.saturating_add(1);
    }
    drop(decision);
    Ok(XshardRecoveryBoundary {
        fingerprint: hasher.finalize().into(),
        prepares,
        decisions,
    })
}

/// Stable digest-only view used by the online-backup bracket.  Keep the
/// prepare/decision census in [`xshard_recovery_boundary`] as the single read
/// implementation; callers that only need the change token must not duplicate
/// its table walk.
pub(crate) fn xshard_recovery_fingerprint(shard: &Shard) -> Result<[u8; 32], String> {
    Ok(xshard_recovery_boundary(shard)?.fingerprint)
}

/// Outcome of a backup run (CONCEPT:EG-KG.sharding.reshard-on-restore) — the shard count + the census the kernel
/// derived for every file the bundle carries.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackupReport {
    /// Number of shard files written into the bundle (= K).
    pub shards: usize,
    /// Per-shard recovery census, in shard order, exactly as
    /// [`eg_storage::backup_recovery_store`] validated each bundle file.
    ///
    /// This is the ONE counter. The dimensions the hand-written copy loops used to
    /// increment (`nodes`, `edges`, `audit`, `capability_and_resource`, …) were a
    /// second census of the same rows, maintained by the same list that WD5-BUG-04
    /// found 30 tables missing from; a backup's completeness is now read back from the
    /// file it wrote instead of reported by the loop that wrote it.
    pub shard_counts: Vec<RecoveryStoreCounts>,
    /// In-doubt cross-shard participant prepare records the bundle carries.
    pub xshard_prepares: u64,
    /// Retained cross-shard coordinator decisions the bundle carries.
    pub xshard_decisions: u64,
    /// Integrity totals for the separate admin coordinator ledger.
    pub admin_mutations: RecoveryStoreCounts,
    /// Non-shard durable stores copied into the bundle, as `file name → rows copied`
    /// (CONCEPT:EG-KG.sharding.reshard-on-restore).
    pub bundled_stores: BTreeMap<String, u64>,
    /// Stable, non-secret encryption key identity captured from the live store.
    /// Material is never included in a report or bundle manifest.
    pub encryption_key_id: Option<String>,
    pub encryption_key_version: Option<String>,
}

impl BackupReport {
    /// Fold one shard's census into the report, in shard order.
    pub fn add_shard(&mut self, counts: RecoveryStoreCounts) {
        self.shard_counts.push(counts);
    }

    /// Record one shard's cross-shard recovery boundary counts.
    pub(crate) fn set_xshard_boundary(&mut self, boundary: XshardRecoveryBoundary) {
        self.xshard_prepares = boundary.prepares;
        self.xshard_decisions = boundary.decisions;
    }

    /// Distinct graph scopes captured across every shard.
    pub fn graph_scopes(&self) -> u64 {
        graph_scopes(&self.shard_counts)
    }
}

/// Distinct graph scopes a shard set carries.
///
/// Every shard file binds exactly one reserved control scope at open — that is what
/// lets the boot scan read the catalog before any graph is known — and one serving
/// scope per graph it hosts. So the graph count IS the census: total scope bindings
/// minus one control scope per file. Unlike a row total it is invariant across a
/// re-shard, because re-sharding moves a graph's scope between files without creating
/// or destroying one.
fn graph_scopes(counts: &[RecoveryStoreCounts]) -> u64 {
    counts.iter().fold(0u64, |total, shard| {
        total.saturating_add(shard.scope_bindings.saturating_sub(1))
    })
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
    /// Per-shard recovery census, in shard order. [`read_manifest`] re-derives this
    /// from the bundle files themselves and refuses a manifest that disagrees, so the
    /// declaration is checkable rather than merely recorded.
    pub shard_counts: Vec<RecoveryStoreCounts>,
    /// Retained in-doubt participant records needed by recovery.
    pub xshard_prepares: u64,
    /// Retained coordinator decisions needed to resolve prepared participants.
    pub xshard_decisions: u64,
    /// Integrity totals for the separate admin coordinator ledger.
    pub admin_mutations: RecoveryStoreCounts,
    /// Stable, non-secret encryption key identity required to open this bundle.
    /// `None` means the source store used plaintext values; no key material is ever
    /// written to the manifest.
    #[serde(default)]
    pub encryption_key_id: Option<String>,
    #[serde(default)]
    pub encryption_key_version: Option<String>,
    /// Non-shard durable stores captured in this bundle, as `file name → rows copied`.
    #[serde(default)]
    pub bundled_stores: BTreeMap<String, u64>,
    /// Durable stores this bundle deliberately does NOT capture, as
    /// `file name → reason` (from
    /// [`durable_stores::DURABLE_STORES`](super::durable_stores::DURABLE_STORES)).
    /// A bundle that documents its own scope cannot silently mislead an operator into
    /// trusting a restore it was never able to perform.
    #[serde(default)]
    pub excluded_stores: BTreeMap<String, String>,
    /// Exact SHA-256 for every portable graph shard and coordinator store.
    /// Keys are bundle-local generic file names, never host paths.
    pub file_digests: BTreeMap<String, String>,
}

impl BackupManifest {
    /// Distinct graph scopes this bundle carries: total scope bindings minus one
    /// reserved control scope per shard file, which is invariant across a re-shard.
    pub fn graph_scopes(&self) -> u64 {
        graph_scopes(&self.shard_counts)
    }

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
            shard_counts: report.shard_counts.clone(),
            xshard_prepares: report.xshard_prepares,
            xshard_decisions: report.xshard_decisions,
            admin_mutations: report.admin_mutations,
            encryption_key_id: report.encryption_key_id.clone(),
            encryption_key_version: report.encryption_key_version.clone(),
            bundled_stores: report.bundled_stores.clone(),
            excluded_stores: super::durable_stores::excluded_store_reasons(),
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

fn valid_key_metadata_component(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
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
    shard_files: &[PathBuf],
    bundled_stores: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, String>, String> {
    let mut files = shard_files.to_vec();
    files.push(dir.join(ADMIN_MUTATIONS_FILE));
    for name in bundled_stores.keys() {
        files.push(dir.join(name));
    }
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

/// Re-derive the recovery census of a shard set from the FILES, in shard order.
///
/// Opened READ-ONLY, exactly as the coordinator store is: a bundle's bytes are what
/// [`portable_file_digests`] hashes, so validating one may never open a write
/// transaction against it. `open_read_only` derives the store's physical identity from
/// `(dev, ino)` only, so a bundle staged under a different path still validates.
fn shard_census(shard_files: &[PathBuf]) -> Result<Vec<RecoveryStoreCounts>, String> {
    let mut census = Vec::with_capacity(shard_files.len());
    for path in shard_files {
        let store = eg_storage::open_read_only(path, None).map_err(|error| {
            format!(
                "shard {} is not a readable owner file: {error}",
                path.display()
            )
        })?;
        census.push(eg_storage::validate_recovery_store_read_only(&store)?);
    }
    Ok(census)
}

/// Copy ONE shard file into `dst_path` (a fresh bundle file) and return the census the
/// kernel validated on it (CONCEPT:EG-KG.sharding.reshard-on-restore). Called once per shard by
/// [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup).
///
/// The whole shard moves as one unit — no `is_shard0` special case, because the
/// file-wide Raft, 2PC and matview tables are declared owner tables of the same census
/// as the per-graph ones, and a census does not need to be told which file it is
/// looking at.
pub(crate) fn write_bundle_shard(
    source: &Shard,
    dst_path: &Path,
) -> Result<RecoveryStoreCounts, String> {
    eg_storage::backup_recovery_store(source.kernel(), dst_path)
        .map_err(|error| format!("bundle shard {}: {error}", dst_path.display()))
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
    if shard_files.len() != report.shards || report.shard_counts.len() != report.shards {
        return Err("backup shard-file count changed before publication".to_string());
    }
    let file_digests = portable_file_digests(dir, &shard_files, &report.bundled_stores)?;
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
    match (
        manifest.encryption_key_id.as_deref(),
        manifest.encryption_key_version.as_deref(),
    ) {
        (Some(id), Some(version))
            if valid_key_metadata_component(id, 128)
                && valid_key_metadata_component(version, 64) => {}
        (None, None) => {}
        _ => return Err("backup manifest encryption key reference is invalid".to_string()),
    }
    if manifest.file_digests.len() != manifest.shard_count + 1 + manifest.bundled_stores.len()
        || manifest
            .file_digests
            .values()
            .any(|digest| !is_sha256_ref(digest))
    {
        return Err("backup manifest file digest inventory is invalid".to_string());
    }
    // Every declared bundled store must be a KNOWN durable store, and must not also be
    // declared excluded — otherwise the manifest describes a scope it does not have.
    for name in manifest.bundled_stores.keys() {
        match super::durable_stores::lookup(name) {
            Some(store) if matches!(store.scope, super::durable_stores::BackupScope::Bundled) => {}
            _ => {
                return Err(
                    "backup manifest declares a store this build does not bundle".to_string(),
                )
            }
        }
        if manifest.excluded_stores.contains_key(name) {
            return Err("backup manifest declares a store both bundled and excluded".to_string());
        }
    }
    let shard_files = crate::redb_layout::discover_current_shards(dir)?;
    if shard_files.len() != manifest.shard_count
        || manifest.shard_counts.len() != manifest.shard_count
    {
        return Err("backup shard-file count does not match the manifest".to_string());
    }
    // The bundle's own completeness, re-derived from the shard FILES rather than
    // trusted from the manifest: every declared owner table present, every ledger row
    // resolving its scope binding, and the same census the backup recorded. This is
    // where the row-dimension cross-check that used to live in the restore path went —
    // measured from the artifact instead of self-reported by the copy that produced it.
    if shard_census(&shard_files)? != manifest.shard_counts {
        return Err("bundle shard totals do not match the manifest".to_string());
    }
    let admin_path = dir.join(ADMIN_MUTATIONS_FILE);
    let admin_metadata = std::fs::symlink_metadata(&admin_path)
        .map_err(|_| "backup bundle omits the admin mutation coordinator store".to_string())?;
    if admin_metadata.file_type().is_symlink() || !admin_metadata.is_file() {
        return Err("backup bundle omits the admin mutation coordinator store".to_string());
    }
    // SEC-FINDING-V1-INCARNATION-BREAKS-RESTORE-20260903, resolved: this bundle
    // may have been renamed here from a different private staging path than the
    // one it was written at, and it must be validated WITHOUT mutating it —
    // its bytes are exactly what the `portable_file_digests` check below
    // re-hashes and compares against the manifest. `open_read_only` derives
    // the store's physical identity from `(dev, ino)` only (never the path, so
    // a rename doesn't change it) and never opens a write transaction against
    // the file, so validating it here cannot perturb the digest check that
    // follows.
    let admin = eg_storage::open_read_only(
        &admin_path,
        crate::server::persistence::redb_backend::admin_mutations_private_integrity(),
    )?;
    let counts = eg_storage::validate_recovery_store_read_only(&admin)?;
    if counts != manifest.admin_mutations {
        return Err("admin mutation coordinator totals do not match the manifest".to_string());
    }
    drop(admin);
    let actual_digests = portable_file_digests(dir, &shard_files, &manifest.bundled_stores)?;
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
    /// Recovery census re-derived from the RESTORED shard files, in shard order.
    pub restored_counts: Vec<RecoveryStoreCounts>,
    /// Validated coordinator receipts and encrypted staged recovery plans.
    pub admin_mutations: RecoveryStoreCounts,
    /// Non-shard durable store files copied back into the persist dir.
    pub restored_stores: Vec<String>,
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
    #[cfg(feature = "security")]
    if let Some(configured) = crate::crypto::resolve_key_config()? {
        match (
            manifest.encryption_key_id.as_deref(),
            manifest.encryption_key_version.as_deref(),
        ) {
            (Some(id), Some(version))
                if id == configured.key_ref().id.as_str()
                    && version == configured.key_ref().version.as_str() => {}
            (Some(_), Some(_)) => {
                return Err(
                    "restore key reference does not match the backup; configure the original \
                     key identity/version or complete an explicit offline re-encryption \
                     rotation before restore"
                        .to_string(),
                );
            }
            (None, None) => {
                return Err(
                    "restore bundle has no encryption key reference but the current \
                     deployment supplied an encryption key; refuse ambiguous restore"
                        .to_string(),
                );
            }
            _ => return Err("restore bundle encryption key reference is incomplete".to_string()),
        }
    }
    if !(1..=64).contains(&target_shards) {
        return Err("restore target shard count is outside bounds".to_string());
    }
    let k = target_shards;
    // The bundle's graph*.redb files are exactly the source shard set EG-030 consumes.
    let migration = shard_migrate::migrate_shards(bundle_dir, persist_dir, k)?;
    if migration.source_shards != manifest.shard_count {
        return Err("restored graph totals do not match the backup manifest".to_string());
    }
    // The restore's own output, measured: every target shard is a recoverable owner
    // file, and it carries exactly the graph scopes the bundle declared. A row total
    // cannot be compared across a re-shard (the routing moves rows between files), but
    // a SCOPE cannot be created or destroyed by re-routing one, so this is the
    // completeness claim that survives a K change — and it is read back from the
    // rebuilt files rather than reported by the migration about itself.
    let restored_counts = shard_census(&crate::redb_layout::discover_current_shards(persist_dir)?)?;
    if restored_counts.len() != k || graph_scopes(&restored_counts) != manifest.graph_scopes() {
        return Err("restored graph scopes do not match the backup manifest".to_string());
    }
    let admin_source = bundle_dir.join(ADMIN_MUTATIONS_FILE);
    let admin_target = persist_dir.join(ADMIN_MUTATIONS_FILE);
    if admin_target.exists() {
        return Err("restore target already contains an admin mutation store".to_string());
    }
    std::fs::copy(&admin_source, &admin_target).map_err(|error| error.to_string())?;
    // SEC-FINDING-V1-INCARNATION-BREAKS-RESTORE-20260903: the `std::fs::copy` above
    // always allocates a NEW inode, so the copy's incarnation can never match the one
    // the bundle was stamped with and every ordinary open fails closed. A restore is
    // an INTENDED substitution and needs the kernel's explicit staged adoption, which
    // re-anchors the physical root and every scope binding to the new inode after
    // proving the image is byte-identical to what it validated.
    let restored_admin = adopt_bundled_store(
        &admin_target,
        ADMIN_MUTATIONS_FILE,
        crate::server::persistence::redb_backend::admin_mutations_private_integrity(),
    )?
    .ok_or_else(|| "admin mutation store is not a declared bundled owner".to_string())?;
    let admin_mutations = eg_storage::validate_recovery_store(&restored_admin)?;
    if admin_mutations != manifest.admin_mutations {
        return Err("restored admin mutation coordinator totals changed".to_string());
    }
    // Restore every non-shard durable store the bundle carried. Without this an engine
    // rebuilt from a bundle comes up with no RBAC/identity state at all — the failure
    // this whole scope declaration exists to make impossible.
    let mut restored_stores = Vec::new();
    for name in manifest.bundled_stores.keys() {
        let target = persist_dir.join(name);
        if target.exists() {
            return Err("restore target already contains a bundled durable store".to_string());
        }
        std::fs::copy(bundle_dir.join(name), &target).map_err(|error| error.to_string())?;
        // A copied file is a NEW inode, so a kernel-owned bundled store carries an
        // incarnation that no longer describes it and its owner's ordinary `open`
        // fails closed (what BUG-PE-054's reopen assertion caught). Adoption belongs
        // here, at the restore boundary. The expected authority is DECLARED by
        // `durable_stores::bundled_store_authority` — the kernel no longer infers it.
        adopt_bundled_store(&target, name, None)
            .map_err(|error| format!("restore adopt {name}: {error}"))?;
        restored_stores.push(name.clone());
    }
    Ok(RestoreReport {
        manifest,
        restored_shards: k,
        migration,
        restored_counts,
        admin_mutations,
        restored_stores,
    })
}

/// Adopt one restored bundled store into its new inode.
///
/// Returns `None` for a bundled file that declares no kernel owner authority —
/// there is none today, and the `None` arm is what makes a future plain bundled
/// file fail loudly at its own call site rather than being adopted as something
/// it is not.
fn adopt_bundled_store(
    path: &Path,
    file_name: &str,
    private_integrity: Option<std::sync::Arc<dyn eg_storage::PrivatePayloadIntegrity>>,
) -> Result<Option<eg_storage::StorageKernel>, String> {
    let Some((physical_name, layout)) = super::durable_stores::bundled_store_authority(file_name)
    else {
        return Ok(None);
    };
    let staged = eg_storage::inspect_staged_mutation_store(
        path,
        eg_storage::PhysicalStoreIdentity::new(physical_name)?,
        layout,
        private_integrity,
    )?;
    eg_storage::adopt_staged_mutation_store(staged).map(Some)
}

/// Set one environment variable for the duration of a test and restore its
/// previous value on drop — including on panic, which a bare set/remove pair
/// would leak into every later test in the same process. Parameterised on the
/// var name so both the at-rest encryption key (`crypto::ENCRYPTION_KEY_ENV`,
/// this module's own coverage) and the transaction-recovery key
/// (`crypto::TXN_RECOVERY_KEY_ENV`, `handlers::txn`'s keyed-recovery coverage)
/// share the one guard: `Some(previous) => restore it`, `None => remove the
/// var entirely`.
#[cfg(test)]
pub(crate) struct EnvVarGuard {
    key: &'static str,
    // `OsString`, not `String`: an environment value that is not valid UTF-8 must
    // still be restored exactly. Reading it through `var()` would drop such a
    // value on the floor and the guard would silently unset it instead.
    previous: Option<std::ffi::OsString>,
}

#[cfg(test)]
impl EnvVarGuard {
    pub(crate) fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

#[cfg(test)]
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{GraphType, Method};
    use crate::redb_store::shard::SHARD_PHYSICAL_STORE;
    use crate::server::persistence::redb_backend::seed_raw_row;
    use crate::server::persistence::redb_backend::RedbBackend;
    #[cfg(feature = "security")]
    use crate::server::persistence::redb_backend::ENCRYPTION_CANARY;
    use crate::server::persistence::PersistenceBackend;
    use eg_storage::{
        GraphShardOwner, OwnerLayout, PhysicalStoreIdentity, StorageKernel, StrictRecoveryEvidence,
    };
    #[cfg(feature = "security")]
    use redb::TableHandle;
    use redb::{Database, ReadableDatabase};

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// Write G graphs (each with two nodes + an edge) durably through a backend.
    async fn seed(dir: &str, shards: usize, graphs: &[&str]) {
        let backend =
            RedbBackend::open_with_shards(dir.to_string(), 256, shards).expect("open backend");
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

    /// Reopen ONE shard file as the kernel owner it is, for offline inspection.
    /// Only valid once nothing holds the file (redb's exclusive lock).
    fn shard_kernel(path: &Path) -> StorageKernel {
        StorageKernel::open_owner::<GraphShardOwner>(
            path,
            PhysicalStoreIdentity::new(SHARD_PHYSICAL_STORE).expect("shard physical identity"),
            None,
        )
        .expect("open shard as a kernel owner file")
    }

    /// Row count of ONE table in one census, or 0 when the census does not declare it.
    /// Presence is asserted separately, against the declared table list.
    fn census_rows(evidence: &StrictRecoveryEvidence, table: &str) -> u64 {
        evidence
            .tables
            .iter()
            .find(|entry| entry.table_id == table)
            .map(|entry| entry.rows)
            .unwrap_or_default()
    }

    /// Row count of ONE table in a shard file opened fresh (offline inspection —
    /// mirrors `shard_migrate.rs`'s own test helper of the same name).
    fn table_row_count<K, V>(path: &Path, def: redb::TableDefinition<K, V>) -> usize
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let db = Database::open(path).expect("open shard for inspection");
        let rtx = db.begin_read().expect("begin read");
        match rtx.open_table(def) {
            Ok(t) => t.iter().expect("iterate table").count(),
            Err(_) => 0,
        }
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
        // Exercise the ENCRYPTED restore path deliberately, instead of whatever the
        // ambient environment happens to configure.
        //
        // This test used to pass with encryption OFF (no key in env => no canary rows
        // => the cross-shard key check short-circuits), and fail the moment a key was
        // present. That is backwards: the K>1 restore is exactly where the key-binding
        // comparison runs, so the interesting path was the one never covered. Pinning
        // a key here makes the encrypted multi-shard round trip the default assertion.
        #[cfg(feature = "security")]
        let _key_guard = EnvVarGuard::set(
            crate::crypto::ENCRYPTION_KEY_ENV,
            "backup-roundtrip-key-material",
        );
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
        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 3).expect("reopen");
        assert_eq!(backend.shard_count(), 3);
        let report = backend
            .backup(&bundle, "test-engine", 1_700_000_000, "nightly", &[])
            .expect("backup");
        assert_eq!(report.shards, 3);
        assert_eq!(report.shard_counts.len(), 3, "one census per bundle shard");
        // Every graph is one bound serving scope; the control scope of each file is
        // the `- K`. This is the census's own answer to "how many graphs did we
        // capture", replacing a `GRAPH_META` row counter the copy loop kept about
        // itself.
        assert_eq!(report.graph_scopes(), graphs.len() as u64);
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
        // `read_manifest` re-derives every bundle shard's census from the FILE, so a
        // manifest that survives this call is one the bundle actually backs up.
        let manifest = read_manifest(&bundle).expect("manifest");
        assert_eq!(manifest.shard_count, 3);
        assert_eq!(manifest.engine_version, "test-engine");
        assert_eq!(manifest.timestamp, 1_700_000_000);
        assert_eq!(manifest.label_ref, opaque_text_ref("nightly"));
        assert_eq!(manifest.shard_counts, report.shard_counts);
        assert_eq!(manifest.graph_scopes(), graphs.len() as u64);

        // ── restore into a FRESH dir at the same K ──
        let rr = restore_bundle(&bundle, &restored, 3).expect("restore");
        assert_eq!(rr.restored_shards, 3);
        assert_eq!(rr.restored_counts.len(), 3);
        assert_eq!(graph_scopes(&rr.restored_counts), graphs.len() as u64);

        // ── reopen the restored dir and verify every graph is intact ──
        let restored_s = restored.to_string_lossy().to_string();
        let rb = RedbBackend::open_with_shards(restored_s, 256, 3).expect("reopen");
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
            // This is where the report's old `nodes`/`edges`/`ledger` counters are
            // re-pinned: they claimed the copy was complete, this proves it is.
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

        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 1).expect("reopen");
        let report = backend
            .backup(&bundle, "test-engine", 42, "", &[])
            .expect("backup");
        assert_eq!(report.shards, 1);
        assert!(bundle.join("graph-0.redb").exists(), "K=1 bundle file");
        backend.shutdown();

        // Restore at K=4 (re-shard on restore). A scope cannot be created or
        // destroyed by re-routing it, so the graph-scope census is invariant across
        // the K change and `restore_bundle` refuses a restore that loses one.
        let rr = restore_bundle(&bundle, &restored, 4).expect("restore reshard");
        assert_eq!(rr.restored_shards, 4);
        assert_eq!(rr.restored_counts.len(), 4);
        assert_eq!(graph_scopes(&rr.restored_counts), graphs.len() as u64);

        let restored_s = restored.to_string_lossy().to_string();
        let rb = RedbBackend::open_with_shards(restored_s, 256, 4).expect("reopen");
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

    /// BUG-PE-054 — the DR round trip must carry IDENTITY state.
    ///
    /// The bundle used to hold graph shards + `admin-mutations.redb` only, so a restore
    /// came up with no RBAC/identity, KV, cluster-topology or placement-catalog state —
    /// silently, from a manifest that said nothing about its own scope. This proves the
    /// non-shard durable stores survive the round trip, and that the manifest declares
    /// both what it captured and what it deliberately did not.
    #[tokio::test(flavor = "multi_thread")]
    async fn backup_restore_carries_non_shard_durable_stores() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let root = std::env::temp_dir().join(format!("eg-backup-sib-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        seed(&src_s, 1, &["alpha"]).await;

        // Durable RBAC/identity state beside the shards — the omission that mattered.
        let identity = eg_core::acl::AgentIdentity {
            agent_id: "restore-probe".to_string(),
            role: eg_core::acl::AgentRole::Agent,
            teams: Vec::new(),
            roles: vec!["reader".to_string()],
        };
        let authority = crate::store_authority::process_authority();
        let rbac = eg_core::rbac_persist::RbacStore::open(
            &src,
            authority.as_ref(),
            authority.principal(),
            &authority.proof(),
        )
        .expect("open rbac store");
        let mut identities = std::collections::BTreeMap::new();
        identities.insert(identity.agent_id.clone(), identity);
        rbac.save(
            &eg_core::rbac::RbacPolicy::new(),
            &identities,
            eg_core::rbac_persist::IdentityBootstrapState::Consumed,
        )
        .expect("persist identity");

        let kv = crate::server::kv::KvStore::open(Some(&src_s)).expect("open kv");
        kv.put("probe", "key", b"value".to_vec()).expect("kv put");

        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 1).expect("reopen");
        // Exactly the adapter the production admin handler hands in.
        let rbac_source = super::super::durable_stores::RbacBundledStore(std::sync::Arc::new(rbac));
        let extra: Vec<&dyn super::super::durable_stores::BundledStoreSource> =
            vec![&rbac_source, &kv];
        let report = backend
            .backup(&bundle, "test-engine", 7, "sibling", &extra)
            .expect("backup");
        backend.shutdown();
        drop(rbac_source);
        drop(kv);

        assert!(
            report.bundled_stores.contains_key("rbac.redb"),
            "rbac.redb bundled, got {:?}",
            report.bundled_stores
        );
        assert!(
            report.bundled_stores.contains_key("kv.redb"),
            "kv.redb bundled, got {:?}",
            report.bundled_stores
        );
        assert!(
            report.bundled_stores.contains_key("node_info.redb"),
            "node_info.redb bundled, got {:?}",
            report.bundled_stores
        );

        let manifest = read_manifest(&bundle).expect("manifest validates");
        assert_eq!(manifest.bundled_stores, report.bundled_stores);
        // Self-describing: every deliberately-excluded store is named WITH its reason.
        for (name, reason) in &manifest.excluded_stores {
            assert!(!reason.is_empty(), "{name} excluded with no reason");
        }
        assert!(
            manifest.excluded_stores.contains_key("blob.redb"),
            "the manifest must state that blob bytes are not captured"
        );

        let restore = restore_bundle(&bundle, &restored, 1).expect("restore");
        for expected in ["rbac.redb", "kv.redb", "node_info.redb"] {
            assert!(
                restore.restored_stores.contains(&expected.to_string()),
                "{expected} restored, got {:?}",
                restore.restored_stores
            );
        }
        // The restored engine knows the identity again — the whole point of BUG-PE-054.
        let restored_rbac = eg_core::rbac_persist::RbacStore::open(
            &restored,
            authority.as_ref(),
            authority.principal(),
            &authority.proof(),
        )
        .expect("reopen restored rbac");
        let (_, restored_identities, bootstrap) = restored_rbac.load().expect("load identities");
        assert!(
            restored_identities.contains_key("restore-probe"),
            "registered identity survived the round trip, got {:?}",
            restored_identities.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            bootstrap,
            eg_core::rbac_persist::IdentityBootstrapState::Consumed,
            "identity bootstrap lifecycle survived the round trip"
        );
        drop(restored_rbac);
        // The restored KV surface answers the write taken before the backup.
        let restored_s = restored.to_string_lossy().to_string();
        let restored_kv = crate::server::kv::KvStore::open(Some(&restored_s)).expect("open kv");
        assert_eq!(
            restored_kv.get("probe", "key").expect("kv get").as_deref(),
            Some(&b"value"[..]),
            "KV row survived the round trip"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// BUG-PE-054 — an unregistered durable store cannot be smuggled into a bundle.
    /// The registry is the single list; anything else must fail loudly at backup time.
    #[tokio::test(flavor = "multi_thread")]
    async fn backup_refuses_an_unregistered_bundled_store() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        struct Bogus;
        impl super::super::durable_stores::BundledStoreSource for Bogus {
            fn file_name(&self) -> &'static str {
                "not-in-the-registry.redb"
            }
            fn copy_into(&self, _destination: &Path) -> Result<u64, String> {
                panic!("must never be copied");
            }
        }
        let root = std::env::temp_dir().join(format!("eg-backup-bogus-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();
        seed(&src_s, 1, &["alpha"]).await;
        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 1).expect("reopen");
        let bogus = Bogus;
        let extra: Vec<&dyn super::super::durable_stores::BundledStoreSource> = vec![&bogus];
        let error = backend
            .backup(&root.join("bundle"), "test-engine", 7, "", &extra)
            .expect_err("unregistered store must be refused");
        assert!(
            error.contains("not a registered bundled durable store"),
            "got: {error}"
        );
        backend.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A restore refuses a manifest with a future format version.
    #[test]
    fn rejects_future_format() {
        let dir = std::env::temp_dir().join(format!("eg-backup-fmt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // The manifest schema has grown and shed fields since this fixture was first
        // written; a manifest missing any current field fails
        // `serde(deny_unknown_fields)` DESERIALIZATION before `read_manifest` ever
        // reaches its `format_version` check, so a stale shape asserts the wrong
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
            "shard_counts": [RecoveryStoreCounts::default()],
            "xshard_prepares": 0,
            "xshard_decisions": 0,
            "admin_mutations": RecoveryStoreCounts::default(),
            "bundled_stores": {},
            "excluded_stores": {},
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

    /// WD5-BUG-04, re-pinned onto the DECLARED CENSUS.
    ///
    /// The defect was a hand-maintained copy list that silently omitted 30 tables, and
    /// the test that caught it enumerated the same list back — so it could only ever
    /// catch the tables someone had already thought of. `backup_recovery_store` copies
    /// the declared `OwnerLayout::GraphShard` census instead
    /// (`copy_declared_owner_tables` + `visit_ledger_tables!`), so the property to
    /// assert is no longer "these 30 tables came across" but "the census IS what came
    /// across". This test asserts exactly that, three ways:
    ///
    /// 1. the bundle's table inventory equals `declared_table_names(GraphShard)` —
    ///    a table added to the census joins the backup with no edit here, and one
    ///    dropped from the copy cannot pass;
    /// 2. every declared OWNER table is byte-identical to the source's, fingerprint
    ///    included — which covers `encryption_canary` (a FileWide owner table) and so
    ///    proves the restore keeps the original key identity/version boundary and
    ///    cannot establish a new canary, without this module copying a canary row;
    /// 3. the WD5-BUG-04 tables named in the original defect still carry their seeded
    ///    row after a full backup + restore round trip.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_declared_census_is_what_a_backup_copies() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        // A key is pinned so `encryption_canary` actually holds its sealed canary and
        // key-binding rows: an empty table would make assertion (2) vacuous.
        #[cfg(feature = "security")]
        let _key_guard = EnvVarGuard::set(
            crate::crypto::ENCRYPTION_KEY_ENV,
            "backup-census-key-material",
        );
        use crate::redb_layout::shard_filename;
        use crate::redb_store::{
            capacity_lease, development_lane, MATVIEW_OPERATOR_STATE, PLAN_MATVIEWS,
            PROVENANCE_ANCHOR_MEMBERS, RESOURCE_RESERVATIONS, WORK_ITEM_COMMAND_SEQUENCE,
        };

        let root = std::env::temp_dir().join(format!("eg-backup-census-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let src = root.join("live");
        let bundle = root.join("bundle");
        let restored = root.join("restored");
        std::fs::create_dir_all(&src).unwrap();
        let src_s = src.to_string_lossy().to_string();

        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 1).expect("open backend");
        backend
            .register_graph("g", "g", GraphType::Global)
            .await
            .expect("register");
        backend.shutdown();
        // `shutdown()` only stops the shard writer thread (which releases the
        // graph-N.redb file lock); it does NOT drop `admin_mutations`/`node_info`/
        // `cluster_hierarchy` on this `RedbBackend`. Rust does not drop a
        // shadowed `let` binding until the END of the enclosing scope, so
        // WITHOUT this explicit drop the reopen below (`let backend = ...`,
        // same `src_s` directory) still holds this backend's admin-mutations.redb
        // handle open and fails with "Database already open. Cannot acquire
        // lock." (WD5-BUG-05 — this was a test defect, not a fix defect: every
        // other reopen in this module targets a DIFFERENT directory via a
        // differently-named variable, so this is the only test that shadows
        // `backend` on the SAME directory).
        drop(backend);

        let shard0 = src.join(shard_filename(0));
        seed_raw_row(&shard0, RESOURCE_RESERVATIONS, ("g", "r1"), b"reservation");
        seed_raw_row(&shard0, development_lane::HOLDS, ("g", "h1"), b"hold");
        seed_raw_row(&shard0, capacity_lease::CELLS, ("g", "c1"), b"cell");
        seed_raw_row(&shard0, WORK_ITEM_COMMAND_SEQUENCE, "g", 7u64);
        seed_raw_row(
            &shard0,
            PROVENANCE_ANCHOR_MEMBERS,
            ("g", 1u64),
            b"anchor-member",
        );
        seed_raw_row(&shard0, PLAN_MATVIEWS, "mv-1", b"plan-def");
        seed_raw_row(&shard0, MATVIEW_OPERATOR_STATE, "mv-1", b"operator-state");

        // ── ONLINE backup (live, reopened — matches every other test in this module) ──
        let backend = RedbBackend::open_with_shards(src_s.clone(), 256, 1).expect("reopen");
        backend
            .backup(&bundle, "test-engine", 1, "census", &[])
            .expect("backup");
        backend.shutdown();
        drop(backend);

        // ── restore into a FRESH dir, which also validates the bundle's manifest ──
        restore_bundle(&bundle, &restored, 1).expect("restore");

        // (1) + (2): the bundle file's own census, compared table by table against the
        // source's. Taken after the restore so nothing here can perturb the bytes the
        // manifest's digests were computed over.
        let source_evidence = {
            let kernel = shard_kernel(&shard0);
            eg_storage::strict_recovery_evidence(&kernel).expect("source census")
        };
        let bundle_evidence = {
            let kernel = shard_kernel(&bundle.join(shard_filename(0)));
            eg_storage::strict_recovery_evidence(&kernel).expect("bundle census")
        };
        let mut inventory: Vec<&str> = bundle_evidence
            .tables
            .iter()
            .map(|entry| entry.table_id.as_str())
            .collect();
        inventory.sort_unstable();
        assert_eq!(
            inventory,
            eg_storage::declared_table_names(OwnerLayout::GraphShard),
            "the bundle's tables ARE the declared graph-shard census"
        );
        for owner_table in eg_storage::owner_table_names(OwnerLayout::GraphShard) {
            let expected = source_evidence
                .tables
                .iter()
                .find(|entry| entry.table_id == *owner_table)
                .unwrap_or_else(|| panic!("source census omits {owner_table}"));
            let actual = bundle_evidence
                .tables
                .iter()
                .find(|entry| entry.table_id == *owner_table)
                .unwrap_or_else(|| panic!("bundle census omits {owner_table}"));
            assert_eq!(
                (expected.rows, expected.fingerprint),
                (actual.rows, actual.fingerprint),
                "{owner_table} is not byte-identical in the bundle"
            );
        }
        // The canary is carried BY THE CENSUS, not by a hand-written copy: a restore
        // therefore retains the original key identity/version boundary and cannot
        // silently establish a new canary under a different key.
        #[cfg(feature = "security")]
        assert!(
            census_rows(&bundle_evidence, ENCRYPTION_CANARY.name()) > 0,
            "the sealed canary + key-binding rows must be in the bundle"
        );

        // (3) the tables WD5-BUG-04 named, still present after backup AND restore.
        let restored_shard0 = restored.join(shard_filename(0));
        for (table, rows) in [
            ("resource_reservations", 1u64),
            ("development_lane_holds", 1),
            ("capacity_cells", 1),
            ("work_item_command_sequence", 1),
            ("provenance_anchor_members", 1),
            ("plan_matviews", 1),
            ("matview_operator_state", 1),
        ] {
            assert_eq!(
                census_rows(&bundle_evidence, table),
                rows,
                "{table} survives the backup (WD5-BUG-04)"
            );
        }
        assert_eq!(
            table_row_count(&restored_shard0, RESOURCE_RESERVATIONS),
            1,
            "resource_reservations survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, development_lane::HOLDS),
            1,
            "development_lane_holds survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, capacity_lease::CELLS),
            1,
            "capacity_cells survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, WORK_ITEM_COMMAND_SEQUENCE),
            1,
            "work_item_command_sequence survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, PROVENANCE_ANCHOR_MEMBERS),
            1,
            "provenance_anchor_members survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, PLAN_MATVIEWS),
            1,
            "plan_matviews survives backup/restore (WD5-BUG-04)"
        );
        assert_eq!(
            table_row_count(&restored_shard0, MATVIEW_OPERATOR_STATE),
            1,
            "matview_operator_state survives backup/restore (WD5-BUG-04)"
        );

        let _ = std::fs::remove_dir_all(&root);
    }
}
