use crate::capability::PhysicalWriteCapability;
use crate::owner::domain::BlobOwner;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::table_api::OwnerTableAccess;
use crate::owner::{validate_declared_owner_tables, validate_manifest_read};
use crate::physical::root::{validate_incarnation_read, PhysicalStore};
use crate::recovery::evidence::strict_snapshot_read;
use redb::{ReadTransaction, ReadableTable, ReadableTableMetadata, TableDefinition};

/// Independent authority for the two physical shared-service CAS tables.
pub trait BlobSharedServiceVerifier: Send + Sync {
    fn verify(
        &self,
        physical: &PhysicalStoreIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String>;
}

pub struct BlobSharedServiceHandle {
    principal: String,
    authority_digest: [u8; 32],
}

pub trait BlobSharedTable: sealed::Sealed {
    const TABLE_ID: &'static str;

    /// Shared physical tables are only reachable through independently
    /// authenticated blob-service operations, never an owner row adapter.
    fn access_class() -> OwnerTableAccess {
        OwnerTableAccess::SharedService
    }
}

mod sealed {
    pub trait Sealed {}
}

pub struct CasChunkRows;
pub struct CasRefcountRows;
impl sealed::Sealed for CasChunkRows {}
impl sealed::Sealed for CasRefcountRows {}
impl BlobSharedTable for CasChunkRows {
    const TABLE_ID: &'static str = "cas_chunks";
}
impl BlobSharedTable for CasRefcountRows {
    const TABLE_ID: &'static str = "cas_refcount";
}

pub struct BlobSharedRead {
    transaction: ReadTransaction,
}

pub(crate) fn authenticate_blob_shared_service(
    store: &PhysicalStore,
    verifier: &dyn BlobSharedServiceVerifier,
    principal: String,
    proof: &[u8],
) -> Result<BlobSharedServiceHandle, String> {
    {
        let manifest = crate::kernel::current_owner_manifest(store)?;
        if manifest.layout != OwnerLayout::Blob {
            return Err("shared blob authority requires the blob owner layout".to_string());
        }
        verifier.verify(&manifest.physical_identity, &principal, proof)?;
        Ok(BlobSharedServiceHandle {
            principal,
            authority_digest: manifest.authority_digest(store.incarnation()),
        })
    }
}

pub(crate) fn read_blob_shared(
    store: &PhysicalStore,
    owner: &BlobSharedServiceHandle,
    principal: &str,
) -> Result<BlobSharedRead, String> {
    {
        let transaction = store.begin_read()?;
        store.validate_physical_root()?;
        validate_incarnation_read(&transaction, store.incarnation())?;
        let cached = store.manifest();
        let manifest =
            validate_manifest_read(&transaction, &cached.physical_identity, cached.layout)?;
        validate_declared_owner_tables(&transaction, manifest.layout)?;
        if manifest != *cached
            || manifest.layout != OwnerLayout::Blob
            || manifest.authority_digest(store.incarnation()) != owner.authority_digest
            || owner.principal != principal
        {
            return Err(
                "shared blob read authority does not match this store or actor".to_string(),
            );
        }
        Ok(BlobSharedRead { transaction })
    }
}

impl BlobSharedRead {
    pub fn table_rows<T: BlobSharedTable>(&self) -> Result<u64, String> {
        strict_snapshot_read(&self.transaction, OwnerLayout::Blob)?
            .tables
            .into_iter()
            .find(|table| table.table_id == T::TABLE_ID)
            .map(|table| table.rows)
            .ok_or_else(|| "shared blob table is outside the closed manifest".to_string())
    }
}

// ── shared-service row I/O ──────────────────────────────────────────────────

/// The two shared-service CAS tables, addressed by **definition, never by
/// name**: no method on either capability below takes a table name, a table
/// handle or a `redb` transaction, so an undeclared name, another layout's
/// table, or one of the three physical-identity tables is not expressible
/// through this surface at all. The layout bound is still re-checked on every
/// open, because [`PhysicalWriteCapability::open_table`] runs the same
/// `permit_table` census check the owner-row path runs.
const CAS_CHUNKS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("cas_chunks");
const CAS_REFCOUNT: TableDefinition<'static, &str, u64> = TableDefinition::new("cas_refcount");

/// Largest chunk body one shared-service row may carry. A CAS chunk is a
/// bounded slice of a blob, not an arbitrary value, and an unbounded row here
/// is exactly the RSS exposure the blob cache cap exists to bound.
const MAX_SHARED_CHUNK_BYTES: usize = 64 * 1024 * 1024;

/// Every key of both shared tables is a content digest: 64 hex characters.
///
/// This is the shared-service key contract, enforced by the kernel rather than
/// by each caller, so a non-content key — an unbounded one especially — cannot
/// reach a CAS table at all.
fn permit_digest(digest: &str) -> Result<(), String> {
    if digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err("shared blob key is not a content digest".to_string())
    }
}

fn permit_chunk_bytes(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_SHARED_CHUNK_BYTES {
        return Err("shared blob chunk exceeds resource limits".to_string());
    }
    Ok(())
}

/// The typed write surface over the two shared-service CAS tables, minted
/// **against a write transaction that is already open and already admitted**.
///
/// `redb` permits exactly one writer, and on every batch path that writer is
/// the caller's [`PhysicalWriteCapability`] — the one an `AdmittedMutation`
/// holds. So a shared-service write cannot begin a transaction of its own
/// without either deadlocking against, or splitting the atomicity of, the
/// mutation it is part of. It borrows the caller's instead: a chunk row and the
/// refcount row that accounts for it commit together with the ledger rows of
/// the same batch, or neither lands.
///
/// Confinement is the owner-row path's, unchanged:
/// * no accessor returns a `redb::Table`, `WriteTransaction` or `PhysicalStore`;
/// * the reachable tables are the two constants above and nothing else;
/// * the capability re-proves the store's layout, its incarnation-anchored
///   authority digest and the blob service's own principal at mint time.
///
/// The bound is the **layout**, not a serving scope, for the same reason
/// [`PhysicalWriteCapability::open_owner_write`]'s is: both CAS keys are
/// content digests and carry no scope component, so there is no scope to bind
/// them to. Dedup across scopes is the point of a content-addressed store.
pub struct BlobSharedWrite<'a> {
    capability: &'a PhysicalWriteCapability<'a, BlobOwner>,
}

/// Mint the shared-service write over the caller's already-open admitted write.
pub(crate) fn write_blob_shared<'a>(
    capability: &'a PhysicalWriteCapability<'a, BlobOwner>,
    owner: &BlobSharedServiceHandle,
    principal: &str,
) -> Result<BlobSharedWrite<'a>, String> {
    let store = capability.store();
    let manifest = store.manifest();
    if manifest.layout != OwnerLayout::Blob
        || manifest.authority_digest(store.incarnation()) != owner.authority_digest
        || owner.principal != principal
    {
        return Err("shared blob write authority does not match this store or actor".to_string());
    }
    Ok(BlobSharedWrite { capability })
}

impl BlobSharedWrite<'_> {
    /// Run one shared-service operation, poisoning the caller's transaction if
    /// it fails.
    ///
    /// This surface has no admission window of its own — `AdmittedOwnerWrite`
    /// poisons its write when a row window is dropped unfinished, and nothing
    /// did that here. So a caller that swallowed a refusal (the refcount
    /// underflow above all) could still commit the rows it had already
    /// written. Now it cannot: the poison lives on the transaction, so the
    /// commit is refused for the sole writer and for the whole group alike.
    fn guard<T>(&self, run: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
        self.capability.poison_shared_on_error(run())
    }

    /// One chunk body, read inside the caller's own write transaction so the
    /// dedup decision it feeds is serialized against every concurrent writer.
    pub fn chunk_bytes(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let chunks = self.capability.open_table(CAS_CHUNKS)?;
            let row = chunks.get(digest).map_err(|error| error.to_string())?;
            row.map(|value| {
                permit_chunk_bytes(value.value())?;
                Ok(value.value().to_vec())
            })
            .transpose()
        })
    }

    /// Whether `digest` is already a committed chunk row.
    pub fn chunk_present(&self, digest: &str) -> Result<bool, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let chunks = self.capability.open_table(CAS_CHUNKS)?;
            // Bind before returning: the `AccessGuard` borrows `chunks`, and a tail
            // expression's temporaries outlive the local (E0597).
            let present = chunks
                .get(digest)
                .map_err(|error| error.to_string())?
                .is_some();
            Ok(present)
        })
    }

    /// Row count of `cas_chunks`, inside the write.
    pub fn chunk_rows(&self) -> Result<u64, String> {
        self.guard(|| {
            self.capability
                .open_table(CAS_CHUNKS)?
                .len()
                .map_err(|error| error.to_string())
        })
    }

    /// Store `bytes` at `digest` unless the row is already there; reports
    /// whether it was new. This is the dedup answer, computed and acted on in
    /// one transaction so two concurrent writers cannot both report "new".
    pub fn insert_chunk_if_absent(&self, digest: &str, bytes: &[u8]) -> Result<bool, String> {
        self.guard(|| {
            self.insert_chunks_if_absent(&[(digest, bytes)])
                .map(|was_new| was_new[0])
        })
    }

    /// The group-commit form: one table open for a whole staged chunk window,
    /// each row reporting whether it was new, all in the caller's transaction.
    pub fn insert_chunks_if_absent(&self, rows: &[(&str, &[u8])]) -> Result<Vec<bool>, String> {
        self.guard(|| {
            for (digest, bytes) in rows {
                permit_digest(digest)?;
                permit_chunk_bytes(bytes)?;
            }
            let mut chunks = self.capability.open_table(CAS_CHUNKS)?;
            let mut was_new = Vec::with_capacity(rows.len());
            for (digest, bytes) in rows {
                let absent = chunks
                    .get(*digest)
                    .map_err(|error| error.to_string())?
                    .is_none();
                if absent {
                    chunks
                        .insert(*digest, *bytes)
                        .map_err(|error| error.to_string())?;
                }
                was_new.push(absent);
            }
            Ok(was_new)
        })
    }

    /// Drop one chunk row; reports whether it was there. The sweep's reclaim.
    pub fn remove_chunk(&self, digest: &str) -> Result<bool, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let mut chunks = self.capability.open_table(CAS_CHUNKS)?;
            let removed = chunks
                .remove(digest)
                .map_err(|error| error.to_string())?
                .is_some();
            Ok(removed)
        })
    }

    /// The reference count of `digest`; an absent row is zero references.
    pub fn refcount(&self, digest: &str) -> Result<u64, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let refs = self.capability.open_table(CAS_REFCOUNT)?;
            let count = refs
                .get(digest)
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .unwrap_or(0);
            Ok(count)
        })
    }

    /// Move a reference count by `delta` and report the new value.
    ///
    /// **Underflow fails closed**, it does not saturate: a count that drops
    /// below zero means a release was accounted twice, and clamping it to zero
    /// makes the next sweep reclaim chunks a live blob still references. The
    /// caller sees the error inside its own transaction and aborts, so no row
    /// of that batch lands. Overflow fails closed for the same reason.
    pub fn adjust_refcount(&self, digest: &str, delta: i64) -> Result<u64, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let mut refs = self.capability.open_table(CAS_REFCOUNT)?;
            let current = refs
                .get(digest)
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .unwrap_or(0);
            let updated = match u64::try_from(delta) {
                Ok(up) => current
                    .checked_add(up)
                    .ok_or_else(|| "shared blob reference count overflow".to_string())?,
                Err(_) => current
                    .checked_sub(delta.unsigned_abs())
                    .ok_or_else(|| "shared blob reference count underflow".to_string())?,
            };
            refs.insert(digest, updated)
                .map_err(|error| error.to_string())?;
            Ok(updated)
        })
    }

    /// Set the reference count of `digest` only if it currently reads
    /// `expected` (an absent row reads zero). The compare-and-set form for a
    /// caller that computed `next` from a value it read earlier in this same
    /// transaction.
    pub fn compare_and_set_refcount(
        &self,
        digest: &str,
        expected: u64,
        next: u64,
    ) -> Result<(), String> {
        self.guard(|| {
            permit_digest(digest)?;
            let mut refs = self.capability.open_table(CAS_REFCOUNT)?;
            let current = refs
                .get(digest)
                .map_err(|error| error.to_string())?
                .map(|value| value.value())
                .unwrap_or(0);
            if current != expected {
                return Err("shared blob reference count changed under this write".to_string());
            }
            refs.insert(digest, next)
                .map_err(|error| error.to_string())?;
            Ok(())
        })
    }

    /// Drop one refcount row; reports whether it was there.
    pub fn remove_refcount(&self, digest: &str) -> Result<bool, String> {
        self.guard(|| {
            permit_digest(digest)?;
            let mut refs = self.capability.open_table(CAS_REFCOUNT)?;
            let removed = refs
                .remove(digest)
                .map_err(|error| error.to_string())?
                .is_some();
            Ok(removed)
        })
    }

    /// Row count of `cas_refcount`, inside the write.
    pub fn refcount_rows(&self) -> Result<u64, String> {
        self.guard(|| {
            self.capability
                .open_table(CAS_REFCOUNT)?
                .len()
                .map_err(|error| error.to_string())
        })
    }

    /// Visit every refcount row in key order. Streaming rather than collecting,
    /// so the sweep applies its own tracked-digest budget and the kernel never
    /// materialises an unbounded set on the caller's behalf.
    pub fn for_each_refcount(
        &self,
        mut visit: impl FnMut(&str, u64) -> Result<(), String>,
    ) -> Result<(), String> {
        self.guard(|| {
            let refs = self.capability.open_table(CAS_REFCOUNT)?;
            for row in refs.iter().map_err(|error| error.to_string())? {
                let (key, value) = row.map_err(|error| error.to_string())?;
                permit_digest(key.value())?;
                visit(key.value(), value.value())?;
            }
            Ok(())
        })
    }
}

impl BlobSharedRead {
    /// The read twin of [`BlobSharedWrite::chunk_bytes`].
    pub fn chunk_bytes(&self, digest: &str) -> Result<Option<Vec<u8>>, String> {
        permit_digest(digest)?;
        let chunks = self
            .transaction
            .open_table(CAS_CHUNKS)
            .map_err(|error| error.to_string())?;
        chunks
            .get(digest)
            .map_err(|error| error.to_string())?
            .map(|value| {
                permit_chunk_bytes(value.value())?;
                Ok(value.value().to_vec())
            })
            .transpose()
    }

    /// The read twin of [`BlobSharedWrite::chunk_present`].
    pub fn chunk_present(&self, digest: &str) -> Result<bool, String> {
        permit_digest(digest)?;
        let chunks = self
            .transaction
            .open_table(CAS_CHUNKS)
            .map_err(|error| error.to_string())?;
        Ok(chunks
            .get(digest)
            .map_err(|error| error.to_string())?
            .is_some())
    }

    /// The read twin of [`BlobSharedWrite::refcount`].
    pub fn refcount(&self, digest: &str) -> Result<u64, String> {
        permit_digest(digest)?;
        let refs = self
            .transaction
            .open_table(CAS_REFCOUNT)
            .map_err(|error| error.to_string())?;
        Ok(refs
            .get(digest)
            .map_err(|error| error.to_string())?
            .map(|value| value.value())
            .unwrap_or(0))
    }

    /// The read twin of [`BlobSharedWrite::for_each_refcount`].
    pub fn for_each_refcount(
        &self,
        mut visit: impl FnMut(&str, u64) -> Result<(), String>,
    ) -> Result<(), String> {
        let refs = self
            .transaction
            .open_table(CAS_REFCOUNT)
            .map_err(|error| error.to_string())?;
        for row in refs.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            permit_digest(key.value())?;
            visit(key.value(), value.value())?;
        }
        Ok(())
    }
}
