//! Current-format direct snapshot contract for DEFAULT_GROUP-owned sidecar state.
//!
//! The stores represented here are process-wide authorities whose rows do not carry
//! reversible Raft-group ownership.  They are therefore captured exactly once by
//! [`DEFAULT_GROUP`].  A provider captures one stable physical artifact
//! (a redb image or a deterministic bounded archive for a file-set catalog), validates
//! it off-serving, stages a replacement, and assembles one complete generation.
//! The authority publishes exactly one generation pointer while the state-image
//! install permit is held; serving code can access stores only through a non-Clone
//! read session that pins that whole generation. Logical row reconstruction is
//! deliberately excluded: mutation-store-private scope bindings, receipts, and
//! versions are part of the authority and must survive exactly.  A copied target
//! file's physical root and scope bindings are deliberately re-anchored by the
//! canonical mutation-store adoption API; that is the only permitted byte change.

use std::any::{Any, TypeId};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
#[cfg(test)]
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as SyncRwLock};

#[cfg(unix)]
use rustix::fs::{linkat, mkdirat, open as rustix_open, openat, unlinkat, AtFlags, Mode, OFlags};
#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
use rustix::fs::{renameat_with, RenameFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock as TokioRwLock};

/// Raft group identifier.
///
/// Moved here from the root binary's `raft` module: direct state is the only
/// consumer of the alias inside the storage kernel, and the kernel may not
/// depend on the binary. `src/raft/mod.rs` re-exports both from here.
pub type GroupId = u64;

/// The single group that owns process-wide sidecar state.
pub const DEFAULT_GROUP: GroupId = 0;

/// `tempfile::tempdir` creates with mode 0o777 masked by the ambient umask, so under
/// the common 0o022/0o002 umasks it yields 0o755/0o775 and every
/// `PinnedPrivateDirectory::open` on it fails the mode-0700 check. Tests of
/// direct-state staging (here and in the SQL checkpoint upgrade) assert behaviour,
/// not the caller's umask, so they mint their own 0700 root.
#[cfg(test)]
pub(crate) fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

#[cfg(test)]
thread_local! {
    static FAIL_PREPARED_RETIREMENT_AFTER_SYNC: Cell<bool> = const { Cell::new(false) };
}

mod authority;
mod capture;
mod contract;
mod filesystem;
mod generation;
mod image;
mod journal;
mod provider;
mod registry;
mod transport;

/// The seal. The module is private, so no crate outside this one can name these
/// traits and therefore none can implement the public traits that require them.
/// The traits themselves are `pub` only so the public bounds are not private.
mod sealed {
    pub trait DirectStateDomainValue {}
    pub trait DirectStatePinnedValue {}
    pub trait DirectStateProvider {}
}

pub use authority::{
    DirectStatePinnedOperation, DirectStateReadSession, StateImageAuthority,
    StateImageInstallPermit, StateImageWritePermit,
};
pub use capture::{
    capture_physical_image_source, DirectStateCaptureBinding, RegisteredDirectStateRoots,
};
pub use contract::{
    DirectStateAuthorityKind, DirectStateDomain, DirectStateOwnerManifest, DirectStateScope,
    DirectStateSectionManifest, DEFAULT_MAX_DIRECT_STATE_BYTES, DIRECT_STATE_SCHEMA_VERSION,
    HARD_MAX_DIRECT_STATE_BYTES, MAX_DIRECT_STATE_CHUNK_BYTES, MAX_DIRECT_STATE_CHUNK_ITEMS,
    MAX_DIRECT_STATE_MANIFEST_BYTES, MAX_DIRECT_STATE_OWNER_NAME_BYTES,
    MAX_DIRECT_STATE_OWNER_TABLES,
};
pub use filesystem::ensure_private_directory;
pub(crate) use filesystem::PinnedPrivateDirectory;
pub use generation::{
    AssembledDirectStateGeneration, DirectStateDomainValue, DirectStateGeneration,
    DirectStateGenerationEntry, DirectStatePinnedValue, DirectStateRegistryIdentity,
};
pub use image::{
    bind_prepared_generation, generation_file_name, incoming_file_name, prepare_mutable_generation,
    resolve_generation_path, resolve_incoming_path, verify_pending_incoming,
    DirectStatePhysicalImage, PreparedDirectStateGeneration, StagedDirectStateGeneration,
    VerifiedDirectStateGeneration, VerifiedDirectStateIncoming,
};
pub use journal::{
    CurrentCleanupRecovery, CurrentDurabilityRecovery, CurrentPromotion, DirectStateCurrentImage,
    DirectStateGenerationManifest, DirectStateInstallJournal, DirectStateInstallPhase,
    DirectStateInstallSection, DirectStateRecoveryCompletion, DurableCurrentImage,
    DurablePendingJournal, DurablePreparedJournal, DurablePublishedJournal, PendingAbandonRecovery,
    PendingAbandonment, PreparedGenerationRecovery, PreparedJournalPublication,
    PreparedJournalRecovery, PreparedWholeGeneration, PublishedJournalPublication,
    PublishedJournalRecovery,
};
pub use provider::{
    DirectStateProvider, DirectStateProviderStage, DirectStateRecoveredValue,
    DirectStateRegistryContract,
};
pub use registry::{
    DirectStateRegistry, RecoveredDirectStateGeneration, RecoveredPublishedCleanup,
    StagedDirectStateSection, StagedWholeGeneration, ValidatedDirectStateSection,
    ValidatedWholeGeneration,
};
pub use transport::{
    CapturedWholeGeneration, DirectStateChunk, DirectStateChunkStream, DirectStateRemoteTransport,
    DirectStateSectionSource, DirectStateTransportGeneration, DirectStateTransportHeader,
    ValidatingDirectStateChunks,
};

#[cfg(test)]
mod tests;
