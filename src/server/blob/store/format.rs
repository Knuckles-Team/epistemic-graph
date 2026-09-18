//! The blob store's refused predecessor layout.
//!
//! Holder-scoped references, manifest retention and the upload cursor
//! high-water mark added three tables to `blob.redb`. Existing data is wiped,
//! not migrated (operator ruling for EG 2.27.x): a `blob.redb` written with the
//! previous four-table layout is refused before any writable open with
//! `BLOB_FORMAT_UPGRADE_REQUIRED` and the removal step below. Its reference
//! counts carry no holder identity, so no holder rows could be derived from it.
//!
//! Removal step: stop the engine, move `{persist_dir}/blob.redb` aside (keep it
//! until the restarted engine is confirmed healthy), restart. A fresh blob store
//! is created; media blobs referenced by graphs must be uploaded again.

use eg_storage::{LayoutPredecessor, OwnerLayout};

/// `blob.redb` before holder rows, retention and the cursor high-water mark.
pub(crate) const BLOB_BEFORE_HOLDERS: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::Blob,
    label: "blob store from before holder-scoped references",
    owner_tables: &["cas_chunks", "cas_blobs", "cas_refcount", "cas_uploads"],
    data_lost: "its chunks, manifests, reference counts and uploads are not migrated, \
                so media blobs referenced by graphs must be uploaded again",
    file_name: "blob.redb",
};

/// Refuse a predecessor `blob.redb` by name before anything opens it for write.
pub(super) fn refuse_predecessor(path: &std::path::Path) -> Result<(), String> {
    eg_storage::refuse_known_predecessor(path, &BLOB_BEFORE_HOLDERS)
}
