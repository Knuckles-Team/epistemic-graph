use crate::physical::incarnation::StoreIncarnation;
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use redb::ReadOnlyDatabase;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Read-only physical database plus its non-forgeable root and integrity
/// authority. Unlike [`crate::StorageKernelV1`] this never opens a write transaction:
/// `redb::Database`'s `Drop` unconditionally commits its own allocator-state
/// quick-repair transaction, which advances the file's transaction id and
/// changes its on-disk bytes on every open -- fine for a live store, fatal
/// for validating a backup bundle byte-for-byte against a manifest digest
/// computed when it was written. `ReadOnlyDatabase` never opens for write, so
/// opening and reading one never mutates the file.
pub struct ReadOnlyStore {
    database: ReadOnlyDatabase,
    incarnation: StoreIncarnation,
    physical_path: PathBuf,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

impl ReadOnlyStore {
    pub(crate) fn database(&self) -> &ReadOnlyDatabase {
        &self.database
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        &self.incarnation
    }

    pub(crate) fn validate_physical_root(&self) -> Result<(), String> {
        let (derived, canonical) = StoreIncarnation::derive(&self.physical_path)?;
        if derived != self.incarnation || canonical != self.physical_path {
            return Err("mutation store physical root identity changed".to_string());
        }
        Ok(())
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        authenticate_with(self.private_integrity.as_deref(), sealed, digest)
    }
}

/// Open the exact physical database READ-ONLY, deriving its expected
/// incarnation from the file's current physical identity but never writing
/// to it. Use this to validate a backup bundle (via
/// [`crate::validate_recovery_store_read_only`]) without perturbing the
/// bytes a manifest's digests were computed over. This does NOT bootstrap or
/// bind anything -- the caller must already know the file holds an
/// initialized store, or every read against it will fail closed with a
/// missing-table error.
pub fn open_read_only(
    path: &Path,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<ReadOnlyStore, String> {
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let database = ReadOnlyDatabase::open(path).map_err(|error| error.to_string())?;
    Ok(ReadOnlyStore {
        database,
        incarnation,
        physical_path,
        private_integrity,
    })
}
