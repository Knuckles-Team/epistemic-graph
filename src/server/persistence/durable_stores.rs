//! The canonical registry of every durable redb store the engine opens directly under
//! a persist dir, and each store's BACKUP SCOPE (CONCEPT:EG-KG.sharding.reshard-on-restore).
//!
//! ## Why this exists
//!
//! [`RedbBackend::backup`](super::redb_backend::RedbBackend::backup) can only snapshot
//! stores it holds a live `redb::Database` handle for — redb takes an exclusive per-file
//! lock, so a second in-process open of a sibling store is impossible. Before this module
//! the bundle therefore held ONLY the graph shards plus `admin-mutations.redb`, and the
//! manifest said nothing about that scope. A restore came up with **no RBAC/identity
//! state at all**, silently, from a bundle that looked complete.
//!
//! The fix has two halves:
//!
//! 1. Stores that a backup CAN reach are bundled. Some are owned by `RedbBackend`
//!    itself (`node_info.redb`, `catalog.redb`); the rest are handed in by the caller
//!    as [`BundledStoreSource`]s (`rbac.redb` from the isolation layer, `kv.redb` from
//!    the server state). Each is copied verbatim, digested into the manifest, and
//!    restored back into the persist dir.
//! 2. Stores that are deliberately NOT bundled are declared, WITH THEIR REASON, in the
//!    manifest's `excluded_stores` map. A bundle that documents its own scope cannot
//!    silently mislead an operator into trusting a restore it cannot perform.
//!
//! ## Keeping the registry honest
//!
//! [`DURABLE_STORES`] is the single list. `backup.rs`'s `registry_covers_every_redb_store`
//! test scans the crate's own sources for `*.redb` filename literals and fails if any is
//! unclassified, so a NEW durable store cannot be silently forgotten by a future change.

use std::collections::BTreeMap;
use std::path::Path;

/// Whether a durable store is captured by the online backup bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupScope {
    /// Copied verbatim into the bundle and restored into the persist dir.
    Bundled,
    /// Deliberately NOT captured. The reason is recorded in the manifest so the
    /// bundle is self-describing about what a restore will not bring back.
    ExcludedByDesign(&'static str),
    /// A file a persist dir may still contain, but that no current build opens.
    /// Not captured and not a gap — recorded so the coverage test stays exhaustive.
    Retired(&'static str),
}

/// One durable store the engine can open directly under a persist dir.
#[derive(Debug, Clone, Copy)]
pub struct DurableStore {
    /// Bundle-local / persist-dir-local file name. Never a host path.
    pub file_name: &'static str,
    pub scope: BackupScope,
}

const fn bundled(file_name: &'static str) -> DurableStore {
    DurableStore {
        file_name,
        scope: BackupScope::Bundled,
    }
}

const fn excluded(file_name: &'static str, reason: &'static str) -> DurableStore {
    DurableStore {
        file_name,
        scope: BackupScope::ExcludedByDesign(reason),
    }
}

const fn retired(file_name: &'static str, reason: &'static str) -> DurableStore {
    DurableStore {
        file_name,
        scope: BackupScope::Retired(reason),
    }
}

/// Every durable store the engine opens directly under a persist dir.
///
/// The authoritative graph shards (`graph-<n>.redb`) are NOT listed here: they are
/// discovered by index (`redb_layout::discover_current_shards`) and are always bundled.
pub const DURABLE_STORES: &[DurableStore] = &[
    // ── bundled ─────────────────────────────────────────────────────────────────
    bundled("admin-mutations.redb"),
    bundled("catalog.redb"),
    bundled("kv.redb"),
    bundled("node_info.redb"),
    bundled("rbac.redb"),
    // ── deliberately excluded, declared in the manifest ─────────────────────────
    excluded(
        "blob.redb",
        "content-addressed blob bytes are unbounded in size and are not copied into a \
         bundle; a restore leaves blob references dangling until blob.redb is copied \
         alongside the bundle by the operator",
    ),
    excluded(
        "request-replay.redb",
        "anti-replay nonce window only; entries expire with the request window and \
         carry no recovery value, so they are deliberately not restored",
    ),
    excluded(
        "series.redb",
        "timeseries store (feature `tsdb`) is a separate durability domain with its own \
         retention policy and is not captured by the graph bundle",
    ),
    excluded(
        "jobs.redb",
        "analytics-job plane (feature `jobs`) is a separate durability domain and is not \
         captured by the graph bundle",
    ),
    excluded(
        "statecharts.redb",
        "statechart engine (feature `statechart`) is a separate durability domain and is \
         not captured by the graph bundle",
    ),
    excluded(
        "cold.redb",
        "cold-tier cache is a rebuildable projection of authoritative shard state",
    ),
    excluded(
        "viz_provenance.redb",
        "visualization provenance is a rebuildable projection of authoritative shard state",
    ),
    excluded(
        "sql_tables.redb",
        "embedded-mode SQL table store; never present beside a served engine's shards",
    ),
    excluded(
        "path_index.redb",
        "path index is a rebuildable projection of authoritative shard state",
    ),
    // ── retired / never opened by a current build ───────────────────────────────
    retired(
        "graph.redb",
        "retired unindexed K=1 shard filename; only the offline migrator consumes it",
    ),
    retired(
        "rdf_quads.redb",
        "removed with the opt-in `rdf-redb` quad table; no current build opens it, so a \
         persist dir holding one carries an orphan from an earlier engine version",
    ),
];

/// The registry entry for `file_name`, or `None` when it is not a known durable store.
pub fn lookup(file_name: &str) -> Option<&'static DurableStore> {
    DURABLE_STORES
        .iter()
        .find(|store| store.file_name == file_name)
}

/// Every store that is deliberately not captured, as `file name → reason`. Written into
/// the manifest so a bundle states its own scope.
pub fn excluded_store_reasons() -> BTreeMap<String, String> {
    DURABLE_STORES
        .iter()
        .filter_map(|store| match store.scope {
            BackupScope::ExcludedByDesign(reason) => {
                Some((store.file_name.to_string(), reason.to_string()))
            }
            BackupScope::Bundled | BackupScope::Retired(_) => None,
        })
        .collect()
}

/// Create the FRESH bundle file a [`BundledStoreSource::copy_into`] writes into,
/// refusing to overwrite an existing one (the same rule the shard copy applies).
pub(crate) fn create_bundle_file(destination: &Path) -> Result<redb::Database, String> {
    if destination.exists() {
        return Err("bundled store file already exists (refusing to overwrite)".to_string());
    }
    redb::Database::create(destination).map_err(|error| error.to_string())
}

/// Stream every row of `$definition` from a read snapshot into the destination write
/// txn, VERBATIM — value blobs are copied byte-for-byte, so encryption-at-rest
/// ciphertext survives without the key. A table absent from the source is skipped
/// (a store opened by an older build may not have created it yet).
///
/// `$rows` accumulates the copied row count.
#[macro_export]
#[doc(hidden)]
macro_rules! copy_bundled_table {
    ($rtx:expr, $wtx:expr, $rows:expr, $definition:expr) => {{
        let mut destination = $wtx
            .open_table($definition)
            .map_err(|error| error.to_string())?;
        if let Ok(source) = $rtx.open_table($definition) {
            for row in source.iter().map_err(|error| error.to_string())? {
                let (key, value) = row.map_err(|error| error.to_string())?;
                destination
                    .insert(key.value(), value.value())
                    .map_err(|error| error.to_string())?;
                $rows += 1;
            }
        }
    }};
}

/// A durable store that can copy its own committed image into a fresh bundle file.
///
/// Implemented next to each store's table definitions (the copy is a typed, verbatim
/// table-by-table stream off a `begin_read()` MVCC snapshot, exactly like the shard
/// copy), so the backup path needs no knowledge of any store's schema.
pub trait BundledStoreSource: Send + Sync {
    /// Bundle-local file name — MUST be a `BackupScope::Bundled` entry in
    /// [`DURABLE_STORES`].
    fn file_name(&self) -> &'static str;

    /// Copy the store's latest committed state verbatim into a FRESH file at
    /// `destination`, returning the number of rows copied. Must refuse to overwrite.
    fn copy_into(&self, destination: &Path) -> Result<u64, String>;

    /// `false` when this store has no on-disk file (an in-memory/test adapter). A
    /// non-durable store is skipped by the backup rather than failing it — there is
    /// nothing to lose and nothing to restore.
    fn is_durable(&self) -> bool {
        true
    }
}

/// Adapter that presents the isolation layer's durable RBAC/identity store as a
/// [`BundledStoreSource`].
///
/// `rbac.redb` is the omission that made a restore dangerous rather than merely
/// incomplete: the bundle carried graph shards and coordinator receipts but NO roles,
/// grants or registered identities, so a restored engine came up default-deny with an
/// already-`Consumed` bootstrap — unrecoverable through the normal admission path.
#[cfg(feature = "security")]
pub struct RbacBundledStore(pub std::sync::Arc<dyn eg_core::rbac_persist::RbacPolicyStore>);

#[cfg(feature = "security")]
impl BundledStoreSource for RbacBundledStore {
    fn file_name(&self) -> &'static str {
        "rbac.redb"
    }

    fn copy_into(&self, destination: &Path) -> Result<u64, String> {
        self.0
            .backup_into(destination)
            .unwrap_or_else(|| Err("RBAC store is in-memory; nothing to bundle".to_string()))
    }

    fn is_durable(&self) -> bool {
        self.0.has_durable_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `*.redb` file names that appear in this crate's sources but are NOT durable
    /// stores under a served persist dir. Every entry is a deliberate classification,
    /// so `registry_covers_every_redb_store` can treat ANY other name as an
    /// unclassified new store and fail.
    const NOT_A_PERSIST_DIR_STORE: &[(&str, &str)] = &[
        ("native.redb", "eg-mutation-store unit-test fixture (tempdir)"),
        ("coordinator.redb", "dispatch unit-test fixture (tempdir)"),
        ("compensation.redb", "dispatch unit-test fixture (tempdir)"),
        ("ts.redb", "eg-tsdb unit-test fixture (tempdir)"),
        ("persist.redb", "eg-tsdb unit-test fixture (tempdir)"),
        ("ann.redb", "eg-ann unit-test fixture (tempdir)"),
        (
            "not-in-the-registry.redb",
            "backup.rs's negative fixture proving an unregistered store is refused",
        ),
    ];

    /// THE anti-rot gate for the backup set (BUG-PE-054).
    ///
    /// A file-list change alone rots: the next durable store someone adds is silently
    /// left out of every bundle, exactly as `rbac.redb`, `kv.redb` and `node_info.redb`
    /// were. So scan the crate's OWN sources for `*.redb` filename literals and require
    /// each one to be classified — bundled, deliberately excluded (with a reason),
    /// retired, or explicitly not a persist-dir store. An unclassified name fails here,
    /// at the point the store is introduced, instead of at a restore years later.
    #[test]
    fn registry_covers_every_redb_store() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut stack = vec![root.join("src"), root.join("crates")];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    // `target*` holds build output, not this crate's sources.
                    if !path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("target"))
                    {
                        stack.push(path);
                    }
                    continue;
                }
                if path.extension().and_then(|value| value.to_str()) != Some("rs") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&path) else {
                    continue;
                };
                let mut rest = text.as_str();
                while let Some(end) = rest.find(".redb\"") {
                    let head = &rest[..end];
                    rest = &rest[end + 6..];
                    let Some(start) = head.rfind('"') else {
                        continue;
                    };
                    let stem = &head[start + 1..];
                    if stem.is_empty() {
                        // This scanner's own search literal, `".redb\""`.
                        continue;
                    }
                    let name = format!("{stem}.redb");
                    // `graph-<n>.redb` shards are discovered by index, never by name.
                    if name.starts_with("graph-") {
                        continue;
                    }
                    // Only a bare file name is a candidate. This filters interpolated
                    // temp names (`format!("eg-tsdb-{}.redb", ...)`) and prose that
                    // happens to end a string literal with a store name.
                    if !name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
                    {
                        continue;
                    }
                    names.insert(name);
                }
            }
        }
        assert!(
            names.len() > 5,
            "the source scan found almost nothing ({names:?}); it is not doing its job"
        );
        let fixtures: std::collections::BTreeSet<&str> =
            NOT_A_PERSIST_DIR_STORE.iter().map(|(name, _)| *name).collect();
        let unclassified: Vec<&String> = names
            .iter()
            .filter(|name| lookup(name).is_none() && !fixtures.contains(name.as_str()))
            .collect();
        assert!(
            unclassified.is_empty(),
            "unclassified durable redb store(s) {unclassified:?} — add each to \
             DURABLE_STORES (Bundled, or ExcludedByDesign with a reason that the backup \
             manifest will carry) or to NOT_A_PERSIST_DIR_STORE. A new store must never \
             be silently left out of the backup set."
        );
        // The registry must not accumulate entries for stores nobody opens any more,
        // except the ones deliberately marked Retired.
        for store in DURABLE_STORES {
            if matches!(store.scope, BackupScope::Retired(_)) {
                continue;
            }
            assert!(
                names.contains(store.file_name),
                "{} is registered but no source opens it; mark it Retired or remove it",
                store.file_name
            );
        }
    }

    /// Every registry entry is unique and sorted-lookup-safe, and every bundled entry
    /// names a real file (no empty/placeholder names).
    #[test]
    fn registry_entries_are_well_formed() {
        let mut seen = std::collections::BTreeSet::new();
        for store in DURABLE_STORES {
            assert!(
                store.file_name.ends_with(".redb"),
                "{} is not a redb file name",
                store.file_name
            );
            assert!(
                seen.insert(store.file_name),
                "duplicate registry entry {}",
                store.file_name
            );
            if let BackupScope::ExcludedByDesign(reason) | BackupScope::Retired(reason) =
                store.scope
            {
                assert!(
                    reason.len() > 20,
                    "{} needs a real reason, got {reason:?}",
                    store.file_name
                );
            }
        }
    }

    /// Every deliberately-excluded store reaches the manifest with its reason.
    #[test]
    fn excluded_reasons_cover_every_excluded_store() {
        let reasons = excluded_store_reasons();
        for store in DURABLE_STORES {
            match store.scope {
                BackupScope::ExcludedByDesign(_) => {
                    assert!(
                        reasons.contains_key(store.file_name),
                        "{} missing from the manifest exclusion map",
                        store.file_name
                    );
                }
                BackupScope::Bundled | BackupScope::Retired(_) => {
                    assert!(
                        !reasons.contains_key(store.file_name),
                        "{} must not be declared as excluded-by-design",
                        store.file_name
                    );
                }
            }
        }
    }
}
