//! The blob layout's shared-service CAS surface, and the physical open options.
//!
//! `cas_chunks`/`cas_refcount` are `TableScope::SharedService`: they are not
//! owner rows of a serving scope, they are the blob service's own physical
//! state, and every row of them is reached through an independently
//! authenticated handle. These cases prove that surface is complete, that it is
//! confined to the blob layout and to content-digest keys, that a reference
//! count cannot go negative, and that a chunk row and the refcount that
//! accounts for it live or die with the caller's one transaction.

use crate::capability::PhysicalWriteCapability;
use crate::kernel::{StorageKernel, StoreOpenOptions};
use crate::owner::blob_shared::{
    BlobSharedServiceHandle, BlobSharedServiceVerifier, BlobSharedTable,
};
use crate::owner::domain::{BlobOwner, KvOwner};
use crate::owner::grant::ScopeGrantVerifier;
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::registry::owner_table_names;
use crate::owner::table_api::{owner_table_access, OwnerTableAccess};
use crate::{CasChunkRows, CasRefcountRows, MutationOwnerAuthority};
use eg_types::mutation_batch::{IncarnationId, LogicalName, DurabilityDomain, ScopeTenantId};
use eg_types::MutationScopeIdentity;
use redb::TableDefinition;
use std::path::Path;

const SERVICE: &str = "blob-service";
const PRINCIPAL: &str = "principal:test:blob";
/// Two distinct 64-character hex content digests.
const DIGEST_A: &str = "aa11bb22cc33dd44ee55ff6600778899aa11bb22cc33dd44ee55ff6600778899";
const DIGEST_B: &str = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

struct AnyLayoutVerifier;

impl ScopeGrantVerifier for AnyLayoutVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        _layout: OwnerLayout,
        _identity: &MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        (principal == PRINCIPAL && proof == b"verified")
            .then_some(())
            .ok_or_else(|| "test scope authority rejected".to_string())
    }
}

struct SharedVerifier;

impl BlobSharedServiceVerifier for SharedVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        (principal == SERVICE && proof == b"verified")
            .then_some(())
            .ok_or_else(|| "test shared blob authority rejected".to_string())
    }
}

fn identity(domain: DurabilityDomain, tenant: &str) -> MutationScopeIdentity {
    MutationScopeIdentity::native(
        ScopeTenantId::new(tenant).unwrap(),
        domain,
        LogicalName::new("cas").unwrap(),
        IncarnationId::new("incarnation:cas:1").unwrap(),
    )
    .unwrap()
}

/// One created owner file, its bound serving scope, its mutation authority and
/// its authenticated shared-service handle.
struct Cas {
    kernel: StorageKernel,
    authority: MutationOwnerAuthority,
    owner: OwnedStoreHandle<BlobOwner>,
    service: BlobSharedServiceHandle,
}

fn cas(path: &Path, physical: &str) -> Cas {
    let kernel = StorageKernel::create_owner::<BlobOwner>(
        path,
        PhysicalStoreIdentity::new(physical).unwrap(),
        None,
    )
    .unwrap();
    open_cas(kernel)
}

fn open_cas(kernel: StorageKernel) -> Cas {
    let service = kernel
        .authenticate_blob_shared_service(&SharedVerifier, SERVICE.to_string(), b"verified")
        .unwrap();
    let grant = kernel
        .authenticate_scope::<BlobOwner>(
            &AnyLayoutVerifier,
            identity(DurabilityDomain::BlobStore, "tenant-a"),
            PRINCIPAL.to_string(),
            b"verified",
        )
        .unwrap();
    let owner = kernel.bind_serving_scope::<BlobOwner>(grant, 0).unwrap();
    let (kernel, authority) = kernel.into_read_and_mutation_authority().unwrap();
    Cas {
        kernel,
        authority,
        owner,
        service,
    }
}

impl Cas {
    fn write(&self) -> PhysicalWriteCapability<'_, BlobOwner> {
        self.authority.write_capability(&self.owner).unwrap()
    }
}

/// Every chunk and refcount operation the root blob store performs has a
/// counterpart here, and each does what its name says.
#[test]
fn the_shared_service_write_serves_every_chunk_and_refcount_operation() {
    let dir = tempfile::tempdir().unwrap();
    let cas = cas(&dir.path().join("blob.redb"), "physical:blob:complete");

    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();

    // chunk insert-if-absent reports the dedup answer, twice over.
    assert!(shared.insert_chunk_if_absent(DIGEST_A, b"first").unwrap());
    assert!(!shared.insert_chunk_if_absent(DIGEST_A, b"first").unwrap());
    // the group form is the same answer per row, in one table open.
    assert_eq!(
        shared
            .insert_chunks_if_absent(&[(DIGEST_A, b"first".as_slice()), (DIGEST_B, b"second")])
            .unwrap(),
        vec![false, true]
    );
    assert_eq!(shared.chunk_bytes(DIGEST_A).unwrap().as_deref(), Some(&b"first"[..]));
    assert!(shared.chunk_present(DIGEST_B).unwrap());
    assert_eq!(shared.chunk_rows().unwrap(), 2);

    // refcounts: absent is zero, adjust moves it, compare-and-set gates on it.
    assert_eq!(shared.refcount(DIGEST_A).unwrap(), 0);
    assert_eq!(shared.adjust_refcount(DIGEST_A, 3).unwrap(), 3);
    assert_eq!(shared.adjust_refcount(DIGEST_A, -1).unwrap(), 2);
    shared.compare_and_set_refcount(DIGEST_A, 2, 7).unwrap();
    assert_eq!(shared.refcount(DIGEST_A).unwrap(), 7);
    shared.compare_and_set_refcount(DIGEST_B, 0, 0).unwrap();
    assert_eq!(shared.refcount_rows().unwrap(), 2);

    // the sweep's enumeration, streamed in key order.
    let mut seen = Vec::new();
    shared
        .for_each_refcount(|digest, count| {
            seen.push((digest.to_string(), count));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        seen,
        vec![(DIGEST_B.to_string(), 0), (DIGEST_A.to_string(), 7)]
    );

    // (A compare-and-set that disagrees with the row is refused; that case is
    // asserted in its own transaction, because a refusal poisons the commit.)

    // the sweep's reclaim.
    assert!(shared.remove_refcount(DIGEST_B).unwrap());
    assert!(!shared.remove_refcount(DIGEST_B).unwrap());
    assert!(shared.remove_chunk(DIGEST_B).unwrap());
    assert!(!shared.remove_chunk(DIGEST_B).unwrap());
    assert_eq!(shared.chunk_rows().unwrap(), 1);
    write.commit().unwrap();

    // the read twin sees exactly the committed rows, and the row COUNT the
    // pre-existing `table_rows` reported still agrees with it.
    let read = cas.kernel.read_blob_shared(&cas.service, SERVICE).unwrap();
    assert_eq!(read.chunk_bytes(DIGEST_A).unwrap().as_deref(), Some(&b"first"[..]));
    assert!(!read.chunk_present(DIGEST_B).unwrap());
    assert_eq!(read.refcount(DIGEST_A).unwrap(), 7);
    assert_eq!(read.refcount(DIGEST_B).unwrap(), 0);
    assert_eq!(read.table_rows::<CasChunkRows>().unwrap(), 1);
    assert_eq!(read.table_rows::<CasRefcountRows>().unwrap(), 1);
    let mut committed = Vec::new();
    read.for_each_refcount(|digest, count| {
        committed.push((digest.to_string(), count));
        Ok(())
    })
    .unwrap();
    assert_eq!(committed, vec![(DIGEST_A.to_string(), 7)]);
}

/// A reference count fails closed rather than clamping: a release accounted
/// twice must surface inside the caller's transaction, because saturating it to
/// zero makes the next sweep reclaim chunks a live blob still references.
#[test]
fn a_shared_reference_count_cannot_underflow() {
    let dir = tempfile::tempdir().unwrap();
    let cas = cas(&dir.path().join("blob.redb"), "physical:blob:underflow");
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();

    assert_eq!(shared.adjust_refcount(DIGEST_A, 1).unwrap(), 1);
    assert_eq!(shared.adjust_refcount(DIGEST_A, -1).unwrap(), 0);
    assert!(shared
        .adjust_refcount(DIGEST_A, -1)
        .unwrap_err()
        .contains("underflow"));
    // The refused decrement wrote nothing, and the read still answers inside
    // the (now poisoned) transaction.
    assert_eq!(shared.refcount(DIGEST_A).unwrap(), 0);
    // An absent row is zero references, and zero cannot be decremented either.
    assert!(shared.adjust_refcount(DIGEST_B, -1).is_err());
    assert_eq!(shared.refcount_rows().unwrap(), 1);
    // A refused operation makes the whole transaction uncommittable.
    assert!(write.commit().unwrap_err().contains("poisoned"));

    // Overflow is symmetric, in its own transaction.
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.compare_and_set_refcount(DIGEST_A, 0, u64::MAX).unwrap();
    assert!(shared
        .adjust_refcount(DIGEST_A, 1)
        .unwrap_err()
        .contains("overflow"));
    assert!(shared.compare_and_set_refcount(DIGEST_A, 5, 9).is_err());
    write.abort().unwrap();
}

/// A chunk row and the refcount that accounts for it are written through the
/// caller's own transaction, so a batch that fails leaves neither — no chunk is
/// orphaned by a refcount that never landed, and no refcount survives a chunk
/// that did not.
#[test]
fn a_failed_batch_cannot_orphan_a_shared_chunk_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let cas = cas(&path, "physical:blob:atomic");

    // One committed chunk, to prove the abort below is selective.
    let committed = cas.write();
    let shared = committed.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, b"kept").unwrap();
    shared.adjust_refcount(DIGEST_A, 1).unwrap();
    committed.commit().unwrap();

    // A second write inserts a chunk, bumps both refcounts, then fails.
    let failing = cas.write();
    let shared = failing.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_B, b"lost").unwrap();
    shared.adjust_refcount(DIGEST_B, 1).unwrap();
    shared.adjust_refcount(DIGEST_A, 1).unwrap();
    assert_eq!(shared.chunk_rows().unwrap(), 2);
    failing.abort().unwrap();

    let read = cas.kernel.read_blob_shared(&cas.service, SERVICE).unwrap();
    assert!(!read.chunk_present(DIGEST_B).unwrap());
    assert_eq!(read.refcount(DIGEST_B).unwrap(), 0);
    assert!(read.chunk_present(DIGEST_A).unwrap());
    assert_eq!(read.refcount(DIGEST_A).unwrap(), 1);
    assert_eq!(read.table_rows::<CasChunkRows>().unwrap(), 1);
    assert_eq!(read.table_rows::<CasRefcountRows>().unwrap(), 1);
}

/// The shared surface is nameless by construction — no method takes a table —
/// and the census bound underneath it refuses the CAS tables to any other
/// layout's capability.
#[test]
fn the_shared_service_write_is_confined_to_the_blob_layout() {
    const CAS_CHUNKS: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_chunks");
    const UNDECLARED: TableDefinition<&str, &[u8]> = TableDefinition::new("cas_shadow");

    // Both tables are declared members of the blob layout and are the layout's
    // only shared-service members, so the two definitions above name exactly
    // the surface this module may reach.
    let blob_tables = owner_table_names(OwnerLayout::Blob);
    assert!(blob_tables.contains(&CasChunkRows::TABLE_ID));
    assert!(blob_tables.contains(&CasRefcountRows::TABLE_ID));
    assert_eq!(
        blob_tables
            .iter()
            .filter(|name| owner_table_access(name) == OwnerTableAccess::SharedService)
            .copied()
            .collect::<Vec<_>>(),
        vec![CasChunkRows::TABLE_ID, CasRefcountRows::TABLE_ID]
    );

    let dir = tempfile::tempdir().unwrap();

    // A non-blob owner file has no shared-service authority to authenticate.
    let kv_path = dir.path().join("kv.redb");
    let kv = StorageKernel::create_owner::<KvOwner>(
        &kv_path,
        PhysicalStoreIdentity::new("physical:kv:shared").unwrap(),
        None,
    )
    .unwrap();
    assert!(kv
        .authenticate_blob_shared_service(&SharedVerifier, SERVICE.to_string(), b"verified")
        .err()
        .unwrap()
        .contains("blob owner layout"));

    // And a capability on that file cannot open a CAS table: it is undeclared
    // for the Kv layout, exactly as an invented name is for any layout.
    let grant = kv
        .authenticate_scope::<KvOwner>(
            &AnyLayoutVerifier,
            identity(DurabilityDomain::KvStore, "tenant-a"),
            PRINCIPAL.to_string(),
            b"verified",
        )
        .unwrap();
    let kv_owner = kv.bind_serving_scope::<KvOwner>(grant, 0).unwrap();
    let (_kv, kv_authority) = kv.into_read_and_mutation_authority().unwrap();
    let kv_write = kv_authority.write_capability(&kv_owner).unwrap();
    assert!(kv_write
        .open_owner_write(CAS_CHUNKS)
        .unwrap_err()
        .contains("outside its layout"));
    kv_write.abort().unwrap();

    // On the blob file itself an undeclared name still fails closed.
    let blob = cas(&dir.path().join("blob.redb"), "physical:blob:confined");
    let write = blob.write();
    assert!(write
        .open_owner_write(UNDECLARED)
        .unwrap_err()
        .contains("outside its layout"));

    // Both keys must be content digests; nothing else reaches a CAS row.
    let shared = write.blob_shared_write(&blob.service, SERVICE).unwrap();
    for bad in [
        "",
        "not-hex",
        // one character short, one over, and the right length but not hex
        &DIGEST_A[..63],
        &format!("{DIGEST_A}0")[..],
        &"z".repeat(64)[..],
    ] {
        assert!(shared.insert_chunk_if_absent(bad, b"x").is_err());
        assert!(shared.chunk_bytes(bad).is_err());
        assert!(shared.chunk_present(bad).is_err());
        assert!(shared.remove_chunk(bad).is_err());
        assert!(shared.refcount(bad).is_err());
        assert!(shared.adjust_refcount(bad, 1).is_err());
        assert!(shared.compare_and_set_refcount(bad, 0, 1).is_err());
        assert!(shared.remove_refcount(bad).is_err());
    }
    assert_eq!(shared.chunk_rows().unwrap(), 0);

    // A handle authenticated for another actor, or against another store, is
    // refused against this one.
    assert!(write.blob_shared_write(&blob.service, "someone-else").is_err());
    let other_cas = cas(&dir.path().join("other.redb"), "physical:blob:other");
    assert!(write
        .blob_shared_write(&other_cas.service, SERVICE)
        .err()
        .unwrap()
        .contains("does not match this store or actor"));
    write.abort().unwrap();
}

// ── B9: physical open options ───────────────────────────────────────────────

/// The cache cap reaches `redb`'s builder and bounds the handle, and it changes
/// nothing about what the store IS: the same file opened with two different
/// caps has the same manifest digest, the same incarnation-anchored authority
/// digest, and the same rows.
#[test]
fn open_options_bound_the_handle_without_entering_the_store_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let physical = || PhysicalStoreIdentity::new("physical:blob:options").unwrap();
    let capped = StoreOpenOptions::default()
        .with_cache_bytes(StoreOpenOptions::MIN_CACHE_BYTES)
        .unwrap();
    assert_eq!(
        capped.cache_bytes(),
        Some(StoreOpenOptions::MIN_CACHE_BYTES)
    );
    assert_eq!(StoreOpenOptions::default().cache_bytes(), None);

    let kernel =
        StorageKernel::create_owner_with::<BlobOwner>(&path, physical(), None, capped).unwrap();
    assert_eq!(kernel.open_options(), capped);
    let created_manifest = kernel.owner_manifest_digest().unwrap();
    let cas = open_cas(kernel);
    // A value far larger than the whole cache still round-trips: the cap bounds
    // resident pages, it does not bound what the store can hold.
    let big = vec![7u8; 4 * 1024 * 1024];
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, &big).unwrap();
    write.commit().unwrap();
    drop(cas);

    let roomy = StoreOpenOptions::default()
        .with_cache_bytes(128 * 1024 * 1024)
        .unwrap();
    let reopened =
        StorageKernel::open_owner_with::<BlobOwner>(&path, physical(), None, roomy).unwrap();
    assert_eq!(reopened.open_options(), roomy);
    assert_eq!(reopened.owner_manifest_digest().unwrap(), created_manifest);
    let reopened = open_cas(reopened);
    let read = reopened
        .kernel
        .read_blob_shared(&reopened.service, SERVICE)
        .unwrap();
    assert_eq!(read.chunk_bytes(DIGEST_A).unwrap(), Some(big));

    // The default-options open of the same file is the same store too.
    drop(read);
    drop(reopened);
    let plain = StorageKernel::open_owner::<BlobOwner>(&path, physical(), None).unwrap();
    assert_eq!(plain.open_options(), StoreOpenOptions::default());
    assert_eq!(plain.owner_manifest_digest().unwrap(), created_manifest);
}

/// Out-of-range and self-contradictory options are refused at construction,
/// never clamped.
#[test]
fn open_options_refuse_a_value_they_cannot_honour() {
    for bad in [
        0,
        StoreOpenOptions::MIN_CACHE_BYTES - 1,
        StoreOpenOptions::MAX_CACHE_BYTES + 1,
        usize::MAX,
    ] {
        assert!(StoreOpenOptions::default()
            .with_cache_bytes(bad)
            .unwrap_err()
            .contains("outside the accepted range"));
    }
    StoreOpenOptions::default()
        .with_cache_bytes(StoreOpenOptions::MIN_CACHE_BYTES)
        .unwrap();
    StoreOpenOptions::default()
        .with_cache_bytes(StoreOpenOptions::MAX_CACHE_BYTES)
        .unwrap();

    // Creating a store that this process may never write is refused.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    assert!(StorageKernel::create_owner_with::<BlobOwner>(
        &path,
        PhysicalStoreIdentity::new("physical:blob:ro").unwrap(),
        None,
        StoreOpenOptions::default().read_only(),
    )
    .err()
    .unwrap()
    .contains("cannot be created read-only"));
    assert!(!path.exists());
}

/// A read-only open yields no mutation authority, and the physical file refuses
/// a write transaction independently of that token.
#[test]
fn a_read_only_open_has_no_write_authority_at_either_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let physical = || PhysicalStoreIdentity::new("physical:blob:readonly").unwrap();
    let cas = cas(&path, "physical:blob:readonly");
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, b"durable").unwrap();
    write.commit().unwrap();
    drop(cas);

    let kernel = StorageKernel::open_owner_with::<BlobOwner>(
        &path,
        physical(),
        None,
        StoreOpenOptions::default().read_only(),
    )
    .unwrap();
    assert!(kernel.open_options().is_read_only());
    // Reads still work.
    let service = kernel
        .authenticate_blob_shared_service(&SharedVerifier, SERVICE.to_string(), b"verified")
        .unwrap();
    assert!(kernel
        .read_blob_shared(&service, SERVICE)
        .unwrap()
        .chunk_present(DIGEST_A)
        .unwrap());
    // Binding a serving scope is a write, and the physical bound refuses it.
    let grant = kernel
        .authenticate_scope::<BlobOwner>(
            &AnyLayoutVerifier,
            identity(DurabilityDomain::BlobStore, "tenant-a"),
            PRINCIPAL.to_string(),
            b"verified",
        )
        .unwrap();
    assert!(kernel
        .bind_serving_scope::<BlobOwner>(grant, 0)
        .err()
        .unwrap()
        .contains("read-only"));
    // And the token bound refuses to hand out a mutation authority at all.
    assert!(kernel
        .into_read_and_mutation_authority()
        .err()
        .unwrap()
        .contains("read-only"));
}

/// There is no way to select a weaker durability than `Immediate`.
///
/// The open options carry a page-cache bound and a read-only flag and nothing
/// else, and `begin_write` reads a constant, so no caller — and no future
/// option value — can weaken the level every mutation commit runs at.
#[test]
fn no_open_option_can_weaken_write_durability() {
    // `redb::Durability` is not `PartialEq`, so match it structurally.
    assert!(matches!(
        crate::physical::root::WRITE_DURABILITY,
        redb::Durability::Immediate
    ));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    // The whole option surface: a bounded page cache and a read-only flag.
    // Anything a caller can build differs from the default in one of those two
    // and in nothing else, so there is no durability value to pass.
    for options in [
        StoreOpenOptions::default(),
        StoreOpenOptions::default()
            .with_cache_bytes(StoreOpenOptions::MIN_CACHE_BYTES)
            .unwrap(),
        StoreOpenOptions::default()
            .with_cache_bytes(StoreOpenOptions::MAX_CACHE_BYTES)
            .unwrap(),
        StoreOpenOptions::default().read_only(),
    ] {
        let rebuilt = match (options.cache_bytes(), options.is_read_only()) {
            (None, false) => StoreOpenOptions::default(),
            (None, true) => StoreOpenOptions::default().read_only(),
            (Some(bytes), false) => StoreOpenOptions::default()
                .with_cache_bytes(bytes)
                .unwrap(),
            (Some(bytes), true) => StoreOpenOptions::default()
                .with_cache_bytes(bytes)
                .unwrap()
                .read_only(),
        };
        assert_eq!(rebuilt, options);
    }

    // And the transaction a real store opens runs at that constant.
    let cas = cas(&path, "physical:blob:durability");
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, b"durable").unwrap();
    write.commit().unwrap();
    assert!(cas
        .kernel
        .read_blob_shared(&cas.service, SERVICE)
        .unwrap()
        .chunk_present(DIGEST_A)
        .unwrap());
}

/// A failed shared-service operation poisons the caller's transaction, so a
/// caller that swallows the refusal cannot commit the rows it already wrote.
#[test]
fn a_failed_shared_operation_poisons_the_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blob.redb");
    let cas = cas(&path, "physical:blob:poison");

    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, b"written").unwrap();
    // The headline refusal, deliberately ignored by the caller.
    let _ = shared.adjust_refcount(DIGEST_A, -1);
    assert!(write
        .commit()
        .unwrap_err()
        .contains("poisoned by a failed operation"));

    // Nothing landed, and a fresh transaction is unaffected.
    let read = cas.kernel.read_blob_shared(&cas.service, SERVICE).unwrap();
    assert!(!read.chunk_present(DIGEST_A).unwrap());
    drop(read);
    let write = cas.write();
    let shared = write.blob_shared_write(&cas.service, SERVICE).unwrap();
    shared.insert_chunk_if_absent(DIGEST_A, b"written").unwrap();
    write.commit().unwrap();
    assert!(cas
        .kernel
        .read_blob_shared(&cas.service, SERVICE)
        .unwrap()
        .chunk_present(DIGEST_A)
        .unwrap());
}
