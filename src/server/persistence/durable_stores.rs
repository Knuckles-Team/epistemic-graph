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
    bundled("agent_library.redb"),
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
        "cluster_hierarchy.redb",
        "VIZ-1 Leiden cluster hierarchy is DERIVED from the graph's own nodes and \
         edges and is fully recomputable by ClusterHierarchyRefresh (measured 23.5s \
         for 1M nodes / 7.8M edges), so it is a cache rather than a source of truth; \
         a restore rebuilds it on the next refresh instead of carrying it",
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

/// The kernel owner authority of one bundled store: the exact
/// `PhysicalStoreIdentity` name its owner opens the file under, and the
/// `OwnerLayout` that owner declares.
///
/// A restore copies a bundled file, which always allocates a NEW inode, so the
/// copy's `StoreIncarnation` can never match the one the bundle was stamped
/// with and an ordinary open fails closed
/// (SEC-FINDING-V1-INCARNATION-BREAKS-RESTORE-20260903). Staged adoption is the
/// kernel's explicit substitution path, and it requires the caller to DECLARE
/// what it expects the file to be — `eg_storage` no longer infers it. This
/// registry already owns the bundled file-name list, so the declaration belongs
/// here rather than being re-derived at the restore site.
///
/// `rbac.redb`'s identity string is `eg-core`'s (`RBAC_PHYSICAL_STORE`, a
/// private const in `crates/eg-core/src/rbac_persist.rs`); it is restated here
/// because that crate exports no accessor for it. The guard against the two
/// drifting is `backup.rs`'s `backup_restore_carries_non_shard_durable_stores`,
/// which reopens the restored `rbac.redb` through `RbacStore::open` — a
/// mismatched identity fails that adoption closed.
pub(crate) fn bundled_store_authority(
    file_name: &str,
) -> Option<(&'static str, eg_storage::OwnerLayout)> {
    match file_name {
        "admin-mutations.redb" => Some((
            super::redb_backend::ADMIN_MUTATIONS_STORE,
            eg_storage::OwnerLayout::LedgerOnly,
        )),
        "catalog.redb" => Some((
            super::tenant_catalog::CATALOG_PHYSICAL_STORE,
            eg_storage::OwnerLayout::TenantCatalog,
        )),
        #[cfg(feature = "kv")]
        "kv.redb" => Some((
            crate::server::kv::KV_PHYSICAL_STORE,
            eg_storage::OwnerLayout::Kv,
        )),
        "node_info.redb" => Some((
            super::node_info_store::NODE_INFO_PHYSICAL_STORE,
            eg_storage::OwnerLayout::NodeInfo,
        )),
        "agent_library.redb" => Some((
            super::agent_library::AGENT_LIBRARY_PHYSICAL_STORE,
            eg_storage::OwnerLayout::AgentLibrary,
        )),
        "rbac.redb" => Some((
            "eg-core:rbac-security-control",
            eg_storage::OwnerLayout::Rbac,
        )),
        _ => None,
    }
}

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

/// A durable store that can copy its own committed image into a fresh bundle file.
///
/// Implemented next to each store's own identity, because the copy is the STORAGE
/// KERNEL's whole-image backup (`eg_storage::backup_recovery_store`): it carries the
/// ledger and every declared owner table, and it derives a fresh destination root and
/// rebinds each scope to it, which is what makes the bundled file adoptable at
/// restore. The hand-rolled table-by-table stream this replaced produced a plain redb
/// file with no physical root or owner manifest, which a restore could not adopt.
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

    /// Every bundled store must declare the kernel authority a restore adopts it
    /// under. A new bundled file with no entry would be copied and then silently
    /// left un-adopted, which is the failure `bundled_store_authority`'s `None`
    /// arm exists to make impossible.
    #[test]
    fn every_bundled_store_declares_its_restore_authority() {
        for store in DURABLE_STORES
            .iter()
            .filter(|store| store.scope == BackupScope::Bundled)
        {
            assert!(
                bundled_store_authority(store.file_name).is_some(),
                "bundled store {} declares no restore authority",
                store.file_name
            );
        }
    }

    /// `*.redb` file names that appear in this crate's sources but are NOT durable
    /// stores under a served persist dir. Every entry is a deliberate classification,
    /// so `registry_covers_every_redb_store` can treat ANY other name as an
    /// unclassified new store and fail.
    const NOT_A_PERSIST_DIR_STORE: &[(&str, &str)] = &[
        (
            "control-plane.redb",
            "eg-transaction outbox lease unit-test fixture (tempdir)",
        ),
        (
            "dest.redb",
            "eg-transaction graft destination unit-test fixture (tempdir)",
        ),
        (
            "loser.redb",
            "eg-transaction competing graft loser unit-test fixture (tempdir)",
        ),
        (
            "native-current.redb",
            "eg-transaction mixed current/scoped group unit-test fixture (tempdir)",
        ),
        (
            "nonce-only.redb",
            "eg-transaction replay unit-test fixture (tempdir): a batch whose only difference \
             from its predecessor is a fresh attempt nonce",
        ),
        (
            "outbox-key.redb",
            "eg-transaction replay unit-test fixture (tempdir): outbox idempotency-key reuse",
        ),
        (
            "replay-concurrent.redb",
            "eg-transaction replay unit-test fixture (tempdir): two concurrent replays of one \
             admitted batch",
        ),
        (
            "replay-finalize.redb",
            "eg-transaction replay unit-test fixture (tempdir): replay sealed by the commit \
             finalizer without reapplying owner rows",
        ),
        (
            "same.redb",
            "eg-transaction same-root graft refusal unit-test fixture (tempdir)",
        ),
        (
            "scope.redb",
            "eg-transaction graft scope-fence unit-test fixture (tempdir)",
        ),
        (
            "typed-replay.redb",
            "eg-transaction fault/restart unit-test fixture (tempdir): a typed result survives \
             restart and replays once",
        ),
        (
            "typed-replay-missing-result.redb",
            "eg-transaction fault/restart unit-test fixture (tempdir): replay of a typed batch \
             whose stored result is absent",
        ),
        (
            "winner.redb",
            "eg-transaction competing graft winner unit-test fixture (tempdir)",
        ),
        (
            "native.redb",
            "storage/mutation kernel unit-test fixture (tempdir)",
        ),
        (
            "prototype.redb",
            "storage/mutation kernel unit-test fixture (tempdir): a deliberately hand-built \
             legacy-named table used to prove reject_prototype_names quarantines it",
        ),
        (
            "source.redb",
            "storage/mutation kernel unit-test fixture (tempdir): the pre-backup store in the \
             backup_derives_a_distinct_physical_root_and_rebinds_scopes round-trip test",
        ),
        (
            "backup.redb",
            "storage/mutation kernel unit-test fixture (tempdir): the backup destination in the \
             backup_derives_a_distinct_physical_root_and_rebinds_scopes round-trip test",
        ),
        ("coordinator.redb", "dispatch unit-test fixture (tempdir)"),
        ("compensation.redb", "dispatch unit-test fixture (tempdir)"),
        ("ts.redb", "eg-tsdb unit-test fixture (tempdir)"),
        ("persist.redb", "eg-tsdb unit-test fixture (tempdir)"),
        ("ann.redb", "eg-ann unit-test fixture (tempdir)"),
        (
            "not-in-the-registry.redb",
            "backup.rs's negative fixture proving an unregistered store is refused",
        ),
        (
            "changed-after-inspection.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "closed-world.redb",
            "eg-storage owner-adoption unit-test fixture (tempdir)",
        ),
        (
            "decide-then-write.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "destination-kv-epoch-two.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "destination-kv.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "destination-many-bindings.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "destination.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "empty-private-source.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "empty-private-target.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "ledger.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "missing-owner.redb",
            "eg-storage physical-backup negative unit-test fixture (tempdir)",
        ),
        (
            "other.redb",
            "eg-storage owner/blob-sharing unit-test fixture (tempdir)",
        ),
        (
            "owner-backup.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "owner-rows.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "owner-source.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "replay.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "semantic.redb",
            "eg-storage owner-adoption unit-test fixture (tempdir)",
        ),
        (
            "shared-read-tamper.redb",
            "eg-storage physical-backup negative unit-test fixture (tempdir)",
        ),
        (
            "shared.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "source-kv.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "source-many-bindings.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "staged-source.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "staged-target.redb",
            "eg-transaction atomic-commit unit-test fixture (tempdir)",
        ),
        (
            "stale-manifest.redb",
            "eg-storage physical-backup negative unit-test fixture (tempdir)",
        ),
        (
            "statechart-staged.redb",
            "eg-storage physical-backup unit-test fixture (tempdir)",
        ),
        (
            "strict.redb",
            "eg-storage/eg-transaction strict-authority unit-test fixture (tempdir)",
        ),
        (
            "wrong-table.redb",
            "eg-storage owner-adoption negative unit-test fixture (tempdir)",
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
    /// Extract `*.redb` filename literals from one source file's text into `names`
    /// (the `".redb\""` scan half of [`scan_redb_store_names`]).
    fn collect_redb_literals(text: &str, names: &mut std::collections::BTreeSet<String>) {
        let mut rest = text;
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

    /// Walk `src/` and `crates/` under `root`, collecting every `*.redb` filename
    /// literal found in `.rs` source files — the directory-walk half of
    /// [`registry_covers_every_redb_store`]'s anti-rot scan.
    fn scan_redb_store_names(root: &Path) -> std::collections::BTreeSet<String> {
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
                collect_redb_literals(&text, &mut names);
            }
        }
        names
    }

    #[test]
    fn registry_covers_every_redb_store() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let names = scan_redb_store_names(root);
        assert!(
            names.len() > 5,
            "the source scan found almost nothing ({names:?}); it is not doing its job"
        );
        let fixtures: std::collections::BTreeSet<&str> = NOT_A_PERSIST_DIR_STORE
            .iter()
            .map(|(name, _)| *name)
            .collect();
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
