//! Clean-shutdown marker (EG-DURABLE-KERNEL-R006.3).
//!
//! A sidecar file `<store>.clean` beside an owner file. It records that the
//! file was closed gracefully and has not been written since, so the next open
//! may skip `validate_recovery_content`'s O(data) row scan. It is a startup
//! optimisation, NOT an integrity proof: a store whose rows were tampered with
//! behind a valid marker is a documented non-goal.
//!
//! Crash safety rests on ordering, not on the marker's own durability:
//!   * the marker is written only AFTER the database handle is dropped (redb
//!     clears its own recovery flag on drop), so it describes the final file;
//!   * the marker is unlinked, and the directory fsynced, BEFORE the first write
//!     transaction of an open begins. No commit can land while a durable marker
//!     exists, so a crash after any write leaves no valid marker;
//!   * the marker is never fsynced when written: losing it merely forces the
//!     full scan, and a torn file fails its digest.
//!
//! The marker vouches for content that passed `validate_recovery_content`. An
//! open that scanned, or that skipped on a valid marker, starts from validated
//! content. A session that began any write (or created the file) no longer
//! does, so its close re-runs the scan on the closed file, read-only, and
//! writes the marker only when the scan passes. A refused store therefore
//! never gets a marker and its next open scans and refuses again. The skipped
//! open still runs the O(scopes) identity checks
//! (`validate_recovery_identity`).
//!
//! redb's writable open rewrites the header's recovery flag (and so the
//! mtime) before any write. An open that found a valid marker re-stamps it
//! against that post-open file, so a process killed before its first write
//! keeps a marker that is still true.
//! The digest also binds the store incarnation (device + inode), the physical
//! identity, the file length and its modification time, so a copied, restored,
//! upgraded or otherwise rewritten file never matches.

use crate::physical::incarnation::StoreIncarnation;
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const DOMAIN: &[u8] = b"eg/clean-shutdown-marker/v1\0";

pub(crate) fn marker_path(database: &Path) -> PathBuf {
    let mut name = database.as_os_str().to_os_string();
    name.push(".clean");
    PathBuf::from(name)
}

fn binding(database: &Path, incarnation: &StoreIncarnation, physical_name: &str) -> Option<String> {
    let meta = fs::metadata(database).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    hasher.update(incarnation.identity_digest().as_bytes());
    hasher.update((physical_name.len() as u64).to_be_bytes());
    hasher.update(physical_name.as_bytes());
    hasher.update(meta.len().to_be_bytes());
    hasher.update(meta.mtime().to_be_bytes());
    hasher.update(meta.mtime_nsec().to_be_bytes());
    // The redb header page carries the two commit slots (transaction id and
    // root checksums), so this binds the marker to the LAST COMMIT itself and
    // not only to file metadata that a coarse clock could leave unchanged.
    let mut header = vec![0u8; 4096];
    let mut file = fs::File::open(database).ok()?;
    let read = std::io::Read::read(&mut file, &mut header).ok()?;
    hasher.update(&header[..read]);
    let digest: [u8; 32] = hasher.finalize().into();
    Some(digest.iter().map(|b| format!("{b:02x}")).collect())
}

/// True only when a marker exists and matches the file's current state.
pub(crate) fn is_valid(
    database: &Path,
    incarnation: &StoreIncarnation,
    physical_name: &str,
) -> bool {
    let Ok(text) = fs::read_to_string(marker_path(database)) else {
        return false;
    };
    match binding(database, incarnation, physical_name) {
        Some(expected) => text.trim() == expected,
        None => false,
    }
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => fs::File::open(parent)?.sync_all(),
        _ => Ok(()),
    }
}

/// Per-open marker state. Declared as the LAST field of `PhysicalStore` so its
/// `Drop` runs after the database handle has been dropped.
pub(crate) struct CleanMarker {
    database: PathBuf,
    incarnation: StoreIncarnation,
    physical_name: String,
    writable: bool,
    /// A marker may exist on disk and must be removed before the first write.
    armed: AtomicBool,
    clear_lock: Mutex<()>,
    skipped_scan: bool,
    /// This open's content has not been through the recovery scan: the file
    /// was created by it, or a write transaction has begun on it.
    content_unvalidated: AtomicBool,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

impl CleanMarker {
    pub(crate) fn new(
        database: &Path,
        incarnation: &StoreIncarnation,
        physical_name: &str,
        writable: bool,
        present_and_valid: bool,
        content_validated: bool,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Self {
        Self {
            database: database.to_path_buf(),
            incarnation: incarnation.clone(),
            physical_name: physical_name.to_string(),
            writable,
            armed: AtomicBool::new(present_and_valid || marker_path(database).exists()),
            clear_lock: Mutex::new(()),
            skipped_scan: present_and_valid,
            content_unvalidated: AtomicBool::new(!content_validated),
            private_integrity,
        }
    }

    /// Rewrite the marker against the file as it is now. Called only while
    /// the marker is still true (valid at open, no write begun). Best effort:
    /// a failure leaves a stale marker, which only forces the full scan.
    pub(crate) fn restamp(&self) {
        if self.writable {
            self.write_marker();
        }
    }

    fn write_marker(&self) {
        let Some(text) = binding(&self.database, &self.incarnation, &self.physical_name) else {
            return;
        };
        let path = marker_path(&self.database);
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        if fs::write(&tmp, text).is_ok() && fs::rename(&tmp, &path).is_err() {
            let _ = fs::remove_file(&tmp);
        }
    }

    /// Re-run the full recovery content scan on the CLOSED file, read-only.
    fn closed_content_is_valid(&self) -> bool {
        let Ok(store) =
            crate::physical::read_only::open_read_only(&self.database, self.private_integrity.clone())
        else {
            return false;
        };
        let Ok(rtx) = redb::ReadableDatabase::begin_read(store.database()) else {
            return false;
        };
        let authenticate = |sealed: &[u8], digest: &str| {
            authenticate_with(self.private_integrity.as_deref(), sealed, digest)
        };
        crate::recovery::validate::validate_recovery_content(&self.incarnation, &rtx, &authenticate)
            .is_ok()
    }

    /// Whether this open skipped the recovery content scan.
    pub(crate) fn skipped_scan(&self) -> bool {
        self.skipped_scan
    }

    /// Durably remove the marker before the first write of this open.
    pub(crate) fn clear_before_write(&self) -> Result<(), String> {
        // From here on this open's content is unproven until its close scans it.
        self.content_unvalidated.store(true, Ordering::Release);
        if !self.armed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = self.clear_lock.lock().map_err(|e| e.to_string())?;
        if !self.armed.load(Ordering::Acquire) {
            return Ok(());
        }
        let path = marker_path(&self.database);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("clean-shutdown marker not removable: {error}")),
        }
        sync_parent(&path)
            .map_err(|error| format!("clean-shutdown marker removal not durable: {error}"))?;
        self.armed.store(false, Ordering::Release);
        Ok(())
    }
}

impl Drop for CleanMarker {
    fn drop(&mut self) {
        if !self.writable || std::thread::panicking() {
            return;
        }
        if self.content_unvalidated.load(Ordering::Acquire) && !self.closed_content_is_valid() {
            // Refused content never carries a marker, not even a stale one.
            let _ = fs::remove_file(marker_path(&self.database));
            return;
        }
        self.write_marker();
    }
}
