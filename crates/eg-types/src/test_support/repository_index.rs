//! Branch-aware `IndexRepository` fixtures shared by eg-compute's projection
//! tests and the root crate's durable repository-index test (EH-280).

use crate::ingestion_wire::{IndexFileVersion, IndexRef, IndexRefStatus};

/// A live ref whose revision id is `revision` repeated to a 40-hex git id.
pub fn live_ref(name: &str, revision: char) -> IndexRef {
    IndexRef {
        ref_name: name.to_string(),
        revision_id: revision.to_string().repeat(40),
        status: IndexRefStatus::Live,
    }
}

/// One file version on `ref_name` at `path`, pinned to `blob_digest`.
pub fn file_version(ref_name: &str, path: &str, blob_digest: String) -> IndexFileVersion {
    IndexFileVersion {
        ref_name: ref_name.to_string(),
        path: path.to_string(),
        blob_digest,
    }
}
