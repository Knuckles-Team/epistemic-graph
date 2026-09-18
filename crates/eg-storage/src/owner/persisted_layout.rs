//! Named refusal of a known predecessor owner file, decided read-only before
//! anything opens the file for write.
//!
//! An owner file whose table set predates this build fails the normal open with
//! a generic layout-digest mismatch, which cannot be told apart from corruption
//! or a foreign file. A store that changes its layout declares the layout it
//! replaced as a [`LayoutPredecessor`] and calls [`refuse_known_predecessor`]
//! before opening: a genuine file of that predecessor is refused with the
//! derived `{LAYOUT}_FORMAT_UPGRADE_REQUIRED` error and its removal step; every
//! other file (current, corrupt, foreign, absent) is left to the normal open.
//! The inspection never opens a write transaction, so a refused file's bytes
//! are unchanged (redb repairs a file on a writable open).
//!
//! This is the per-store slice of the layout lineage the X11 design generalises;
//! the error code and message follow that design so the registry can absorb the
//! declarations unchanged.

use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::{layout_digest_over, OwnerLayout};
use crate::owner::manifest_io::{read_manifest_slot, write_new_manifest};
use crate::owner::registry::owner_table_names;
use crate::physical::manifest::{OwnerManifest, TableOwnership};
use crate::physical::read_only::open_read_only;
use crate::tables::OWNER_MANIFEST;
use redb::{ReadableDatabase, TableHandle};
use std::path::Path;

/// One earlier table set of a layout that this build refuses by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutPredecessor {
    /// The layout the predecessor file declares.
    pub layout: OwnerLayout,
    /// Operator-facing name of the predecessor generation.
    pub label: &'static str,
    /// The owner tables that generation declared, in manifest order.
    pub owner_tables: &'static [&'static str],
    /// What is not carried over when the file is moved aside.
    pub data_lost: &'static str,
    /// The file name the server registers for this layout.
    pub file_name: &'static str,
}

impl LayoutPredecessor {
    /// `{CANONICAL_NAME_UPPER}_FORMAT_UPGRADE_REQUIRED`.
    pub fn error_code(&self) -> String {
        format!(
            "{}_FORMAT_UPGRADE_REQUIRED",
            self.layout.canonical_name().to_ascii_uppercase()
        )
    }

    fn refusal(&self, path: &Path) -> String {
        format!(
            "{code}: {path} is a {label} file and this build does not open it; {lost}. \
             Stop the engine, move {file} aside (keep it until the restarted engine is \
             confirmed healthy), and restart: a fresh store is created.",
            code = self.error_code(),
            path = path.display(),
            label = self.label,
            lost = self.data_lost,
            file = self.file_name,
        )
    }

    fn matches(&self, manifest: &OwnerManifest) -> bool {
        manifest.layout == self.layout
            && manifest.validate().is_err()
            && layout_digest_over(manifest.layout, &manifest.tables) == manifest.layout_digest
            && owner_table_ids(manifest).eq(self.owner_tables.iter().copied())
    }
}

fn owner_table_ids(manifest: &OwnerManifest) -> impl Iterator<Item = &str> {
    manifest
        .tables
        .iter()
        .filter(|table| table.ownership == TableOwnership::Owner)
        .map(|table| table.table_id.as_str())
}

/// Refuse `path` with the predecessor's named error when it is a genuine file of
/// that predecessor. Anything that is not (including a file that cannot be
/// inspected) returns `Ok`, and the caller's normal open reports it.
pub fn refuse_known_predecessor(
    path: &Path,
    predecessor: &LayoutPredecessor,
) -> Result<(), String> {
    match read_persisted_manifest(path) {
        Ok(manifest) if predecessor.matches(&manifest) => Err(predecessor.refusal(path)),
        // Not a predecessor, or not inspectable: the normal open is the
        // authority on what is wrong with it, so its error is the one reported.
        Ok(_) | Err(_) => Ok(()),
    }
}

fn read_persisted_manifest(path: &Path) -> Result<OwnerManifest, String> {
    let store = open_read_only(path, None)?;
    let transaction = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let table = transaction
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_manifest_slot(&table)
}

/// Fixture builder: write a genuine file of `predecessor` at `path` (which must
/// not exist), so a store's refusal can be proven against a real file rather
/// than a mock. The file is created current and then reduced to the
/// predecessor's table set and self-consistent manifest; no build of this crate
/// opens it through the kernel.
#[doc(hidden)]
pub fn create_predecessor_owner_file(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    predecessor: &LayoutPredecessor,
) -> Result<(), String> {
    drop(crate::kernel::create_physical_with(
        path,
        physical_identity,
        None,
        predecessor.layout,
        crate::StoreOpenOptions::default(),
    )?);
    let database = redb::Database::open(path).map_err(|error| error.to_string())?;
    let write = database.begin_write().map_err(|error| error.to_string())?;
    let mut manifest = {
        let table = write
            .open_table(OWNER_MANIFEST)
            .map_err(|error| error.to_string())?;
        read_manifest_slot(&table)?
    };
    manifest.tables.retain(|table| {
        table.ownership == TableOwnership::Ledger
            || predecessor.owner_tables.contains(&table.table_id.as_str())
    });
    manifest.layout_digest = layout_digest_over(manifest.layout, &manifest.tables);
    write
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .retain(|_, _| false)
        .map_err(|error| error.to_string())?;
    write_new_manifest(&write, &manifest)?;
    let retired: Vec<_> = write
        .list_tables()
        .map_err(|error| error.to_string())?
        .filter(|table| {
            owner_table_names(predecessor.layout).contains(&table.name())
                && !predecessor.owner_tables.contains(&table.name())
        })
        .collect();
    for table in retired {
        write
            .delete_table(table)
            .map_err(|error| error.to_string())?;
    }
    write.commit().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owner::domain::KvOwner;
    use crate::StorageKernel;
    use sha2::{Digest, Sha256};

    const KV_BEFORE_COLD_CACHE: LayoutPredecessor = LayoutPredecessor {
        layout: OwnerLayout::Kv,
        label: "kv store before the cold KV-cache table",
        owner_tables: &["kv"],
        data_lost: "its key-value rows are not migrated",
        file_name: "kv.redb",
    };

    fn identity() -> PhysicalStoreIdentity {
        PhysicalStoreIdentity::new("physical:test:persisted-layout").unwrap()
    }

    fn file_digest(path: &Path) -> Vec<u8> {
        Sha256::digest(std::fs::read(path).unwrap()).to_vec()
    }

    #[test]
    fn a_genuine_predecessor_is_refused_by_name_without_touching_its_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kv.redb");
        create_predecessor_owner_file(&path, identity(), &KV_BEFORE_COLD_CACHE).unwrap();
        let before = file_digest(&path);
        let error = refuse_known_predecessor(&path, &KV_BEFORE_COLD_CACHE).unwrap_err();
        assert!(error.starts_with("KV_FORMAT_UPGRADE_REQUIRED: "), "{error}");
        assert!(error.contains("move kv.redb aside"), "{error}");
        assert!(
            error.contains("its key-value rows are not migrated"),
            "{error}"
        );
        assert_eq!(file_digest(&path), before, "the refusal must not write");
        let normal = StorageKernel::open_owner::<KvOwner>(&path, identity(), None)
            .err()
            .expect("the kernel must not open a predecessor file");
        assert!(!normal.contains("FORMAT_UPGRADE_REQUIRED"), "{normal}");
    }

    #[test]
    fn current_absent_and_other_generation_files_are_left_to_the_normal_open() {
        let dir = tempfile::tempdir().unwrap();
        let current = dir.path().join("current.redb");
        drop(StorageKernel::create_owner::<KvOwner>(&current, identity(), None).unwrap());
        assert_eq!(
            refuse_known_predecessor(&current, &KV_BEFORE_COLD_CACHE),
            Ok(())
        );
        let absent = dir.path().join("absent.redb");
        assert_eq!(
            refuse_known_predecessor(&absent, &KV_BEFORE_COLD_CACHE),
            Ok(())
        );
        let other = LayoutPredecessor {
            owner_tables: &["eg_kvcache_cold"],
            ..KV_BEFORE_COLD_CACHE
        };
        let predecessor = dir.path().join("kv.redb");
        create_predecessor_owner_file(&predecessor, identity(), &KV_BEFORE_COLD_CACHE).unwrap();
        assert_eq!(refuse_known_predecessor(&predecessor, &other), Ok(()));
    }

    #[test]
    fn a_manifest_whose_digest_disagrees_with_its_tables_is_not_a_predecessor() {
        let mut manifest = OwnerManifest::new(identity(), OwnerLayout::Kv).unwrap();
        manifest
            .tables
            .retain(|table| table.table_id != "eg_kvcache_cold");
        assert!(!KV_BEFORE_COLD_CACHE.matches(&manifest), "stale digest");
        manifest.layout_digest = layout_digest_over(OwnerLayout::Kv, &manifest.tables);
        assert!(KV_BEFORE_COLD_CACHE.matches(&manifest));
        manifest.layout_digest[0] ^= 1;
        assert!(!KV_BEFORE_COLD_CACHE.matches(&manifest), "damaged digest");
    }
}
