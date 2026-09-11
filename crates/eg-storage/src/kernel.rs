//! `StorageKernel` — the sole physical-state authority.

use crate::capability::{PhysicalWriteCapability, ScopedRead, ScopedSnapshot};
use crate::owner::blob_shared::{
    BlobSharedRead, BlobSharedServiceHandle, BlobSharedServiceVerifier,
};
use crate::owner::domain::OwnerDomain;
use crate::owner::grant::{AuthenticatedScopeGrant, ScopeGrantVerifier};
use crate::owner::handle::OwnedStoreHandle;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::row_key::{is_control_scope, permit_reserved_name};
use crate::owner::{
    open_declared_owner_tables, validate_declared_owner_tables, validate_manifest_read,
    write_new_manifest,
};
use crate::physical::binding::bind_scope_in;
use crate::physical::incarnation::StoreIncarnation;
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::{OwnerManifest, OwnerManifestDigest};
use crate::physical::root::{
    initialize_strict_in, store_handle, validate_incarnation_read, PhysicalStore,
};
use crate::recovery::validate::validate_recovery_content;
use crate::tables::SCOPE_BINDINGS;
use eg_types::MutationScopeIdentity;
use redb::{Database, ReadableDatabase};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Largest admitted scope group.
///
/// Every other collection this kernel touches is bounded (`encode_bounded`,
/// `CollectionBudget`, a batch's write budget), and N unbounded members would
/// be one unbounded write transaction holding redb's single write lock. The
/// shard's coalescer drains a burst of graph batches, so the bound is generous
/// but finite.
const MAX_SCOPE_GROUP_MEMBERS: usize = 1024;

/// The sole physical owner of one durable store file.
///
/// It alone opens and identifies the file, owns its complete table/domain
/// registry, and issues scoped read, snapshot and write capabilities. Domain
/// crates hold only the capabilities it issues.
pub struct StorageKernel {
    store: Arc<PhysicalStore>,
    mutation_authority: Option<MutationOwnerAuthority>,
}

/// Move-once physical write authority.
///
/// It is not `Clone`, not `Default`, not serializable, and has no public
/// constructor. [`StorageKernel::into_read_and_mutation_authority`] yields
/// the single instance for one kernel, so a domain crate can never name one and
/// therefore can never obtain a write capability.
///
/// ```compile_fail
/// # use eg_storage::MutationOwnerAuthority;
/// fn forge() -> MutationOwnerAuthority {
///     MutationOwnerAuthority {}
/// }
/// ```
pub struct MutationOwnerAuthority {
    store: Arc<PhysicalStore>,
}

/// How one physical owner file is **opened** — never what it *is*.
///
/// None of these values reaches [`PhysicalStoreIdentity`], the owner manifest,
/// the store incarnation or any digest derived from them, so the same file
/// opened with a 64 MiB cache and with the default cache is the same store,
/// with the same identity, to every check in this crate. That is deliberate:
/// a cache size is an operational property of one process's handle, and making
/// it identity-bearing would mean a tuning change looked like a different
/// store and failed adoption closed.
///
/// The default is exactly the pre-options behaviour: `redb`'s own default
/// cache, read-write. Every setter validates, so an out-of-range value is
/// refused at construction rather than silently clamped.
///
/// There is deliberately **no durability knob**. `PhysicalStore::begin_write`
/// is the one path every mutation commit takes, and it hard-codes
/// `redb::Durability::Immediate`: a weaker level would let redb roll a
/// committed ledger back on crash, un-consuming an acknowledged replay nonce
/// and re-enabling a double apply. B9's need was the page-cache bound alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StoreOpenOptions {
    cache_bytes: Option<usize>,
    read_only: bool,
}

impl StoreOpenOptions {
    /// Smallest accepted page-cache bound. Below this `redb` cannot hold the
    /// working set of a single write transaction, so a "cap" here would only
    /// mean unbounded re-reads, not bounded memory.
    pub const MIN_CACHE_BYTES: usize = 1024 * 1024;
    /// Largest accepted page-cache bound; past this the value is a units
    /// mistake, not a tuning choice.
    pub const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024 * 1024;

    /// Bound the `redb` page cache for this handle.
    ///
    /// This is the injection point a store with multi-MB values needs: without
    /// it the cache is `redb`'s 1 GiB default and the resident set of a blob
    /// CAS is unbounded by anything the kernel states.
    pub fn with_cache_bytes(mut self, bytes: usize) -> Result<Self, String> {
        if !(Self::MIN_CACHE_BYTES..=Self::MAX_CACHE_BYTES).contains(&bytes) {
            return Err("store cache size is outside the accepted range".to_string());
        }
        self.cache_bytes = Some(bytes);
        Ok(self)
    }

    /// Open without any write authority: the kernel issues no
    /// [`MutationOwnerAuthority`], and the physical store refuses to begin a
    /// write transaction at all. Both halves fail closed, so neither a lost
    /// token nor a direct physical call can write.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn cache_bytes(&self) -> Option<usize> {
        self.cache_bytes
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    fn builder(&self) -> redb::Builder {
        let mut builder = Database::builder();
        if let Some(bytes) = self.cache_bytes {
            builder.set_cache_size(bytes);
        }
        builder
    }

    fn create_database(&self, path: &Path) -> Result<Database, String> {
        self.builder()
            .create(path)
            .map_err(|error| error.to_string())
    }

    fn open_database(&self, path: &Path) -> Result<Database, String> {
        self.builder().open(path).map_err(|error| error.to_string())
    }
}

impl StorageKernel {
    /// Create a current-format owner file under the default open options. No
    /// serving scope is created here.
    pub fn create_owner<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Result<Self, String> {
        Self::create_owner_with::<D>(
            path,
            physical_identity,
            private_integrity,
            StoreOpenOptions::default(),
        )
    }

    /// Create a current-format owner file under explicit open options.
    ///
    /// `options` bounds how this process's handle behaves — its page cache, its
    /// its page cache, whether it may write — and is not part of the store's
    /// identity. Creating a store
    /// with [`StoreOpenOptions::read_only`] is refused: it would produce a file
    /// nothing in this process could ever write, including the bootstrap the
    /// create path itself performs.
    pub fn create_owner_with<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
        options: StoreOpenOptions,
    ) -> Result<Self, String> {
        if options.is_read_only() {
            return Err("a store cannot be created read-only".to_string());
        }
        create_physical_with(
            path,
            physical_identity,
            private_integrity,
            D::LAYOUT,
            options,
        )
        .map(Self::from_store)
    }

    /// Open only an exact current-format owner file under the default open
    /// options; absent or invalid declarations fail closed.
    pub fn open_owner<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Result<Self, String> {
        Self::open_owner_with::<D>(
            path,
            physical_identity,
            private_integrity,
            StoreOpenOptions::default(),
        )
    }

    /// Open an exact current-format owner file under explicit open options.
    pub fn open_owner_with<D: OwnerDomain>(
        path: &Path,
        physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
        options: StoreOpenOptions,
    ) -> Result<Self, String> {
        open_physical_with(
            path,
            physical_identity,
            private_integrity,
            D::LAYOUT,
            options,
        )
        .map(Self::from_store)
    }

    /// The open options this kernel's handle is actually running under.
    pub fn open_options(&self) -> StoreOpenOptions {
        self.store.options()
    }

    pub(crate) fn from_store(store: PhysicalStore) -> Self {
        let read_only = store.options().is_read_only();
        let store = Arc::new(store);
        Self {
            mutation_authority: (!read_only).then(|| MutationOwnerAuthority {
                store: Arc::clone(&store),
            }),
            store,
        }
    }

    /// Split this kernel into its read/snapshot half and the one mutation
    /// authority token. Callable exactly once: the token is moved out.
    pub fn into_read_and_mutation_authority(
        mut self,
    ) -> Result<(Self, MutationOwnerAuthority), String> {
        if self.store.options().is_read_only() {
            return Err("store was opened read-only; it has no mutation authority".to_string());
        }
        let authority = self
            .mutation_authority
            .take()
            .ok_or_else(|| "mutation owner authority was already issued".to_string())?;
        Ok((self, authority))
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        self.store.incarnation()
    }

    pub fn layout(&self) -> OwnerLayout {
        self.store.manifest().layout
    }

    /// Authenticate one exact logical serving identity against this store's
    /// declared owner authority.
    pub fn authenticate_scope<D: OwnerDomain>(
        &self,
        verifier: &dyn ScopeGrantVerifier,
        identity: MutationScopeIdentity,
        principal: String,
        proof: &[u8],
    ) -> Result<AuthenticatedScopeGrant<D>, String> {
        authenticate_scope_in(&self.store, verifier, identity, principal, proof)
    }

    /// Bind an authenticated grant once, yielding the owner handle every scoped
    /// capability is issued against.
    pub fn bind_serving_scope<D: OwnerDomain>(
        &self,
        grant: AuthenticatedScopeGrant<D>,
        initial_version: u64,
    ) -> Result<OwnedStoreHandle<D>, String> {
        bind_serving_scope_in(&self.store, grant, initial_version)
    }

    /// Issue one scoped read over this owner file.
    pub fn read_scope<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<ScopedRead<'_, D>, String> {
        ScopedRead::open(&self.store, owner)
    }

    /// Tell a graft retry whether its source binding still exists.
    ///
    /// A failed write open is not by itself proof of retirement: it can also
    /// be an I/O, manifest or recovery failure. Keep that distinction in the
    /// storage kernel, where binding validation is authoritative, so callers
    /// can only treat the one exact missing-binding result as an idempotent
    /// completed retirement.
    pub fn scope_is_bound<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<bool, String> {
        match self.read_scope(owner) {
            Ok(read) => {
                drop(read);
                Ok(true)
            }
            Err(error) if error == "mutation scope is not bound to this store" => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Prove binding absence for graft recovery when the original typed owner
    /// handle cannot be reconstructed after retirement.  This checks the
    /// authoritative physical binding row directly, while still validating
    /// the current root, manifest and declared table census first.
    pub fn scope_binding_exists(&self, identity: &MutationScopeIdentity) -> Result<bool, String> {
        identity.validate_digest()?;
        self.store.validate_physical_root()?;
        let read = self
            .store
            .database
            .begin_read()
            .map_err(|error| error.to_string())?;
        let manifest = validate_manifest_read(
            &read,
            &self.store.manifest().physical_identity,
            self.store.manifest().layout,
        )?;
        if manifest != *self.store.manifest() {
            return Err("mutation read manifest authority changed".to_string());
        }
        validate_declared_owner_tables(&read, manifest.layout)?;
        let table = read
            .open_table(SCOPE_BINDINGS)
            .map_err(|error| error.to_string())?;
        let key = identity.binding_digest().to_hex();
        Ok(table
            .get(key.as_str())
            .map_err(|error| error.to_string())?
            .is_some())
    }

    /// Capture one complete owner-scoped physical snapshot.
    pub fn snapshot<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<ScopedSnapshot<D>, String> {
        ScopedSnapshot::capture(&self.store, owner)
    }

    pub fn owner_manifest_digest(&self) -> Result<OwnerManifestDigest, String> {
        self.current_owner_manifest()?.digest()
    }

    /// Digest of the current persisted owner manifest anchored to this exact
    /// store incarnation. Unlike the manifest digest, this changes when an
    /// otherwise identical store image is adopted at a new inode.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(self
            .current_owner_manifest()?
            .authority_digest(self.incarnation()))
    }

    pub(crate) fn store(&self) -> &PhysicalStore {
        &self.store
    }

    /// Authenticate the independent shared-service authority over the two
    /// physical CAS tables.
    pub fn authenticate_blob_shared_service(
        &self,
        verifier: &dyn BlobSharedServiceVerifier,
        principal: String,
        proof: &[u8],
    ) -> Result<BlobSharedServiceHandle, String> {
        crate::owner::blob_shared::authenticate_blob_shared_service(
            &self.store,
            verifier,
            principal,
            proof,
        )
    }

    pub fn read_blob_shared(
        &self,
        owner: &BlobSharedServiceHandle,
        principal: &str,
    ) -> Result<BlobSharedRead, String> {
        crate::owner::blob_shared::read_blob_shared(&self.store, owner, principal)
    }

    pub(crate) fn current_owner_manifest(&self) -> Result<OwnerManifest, String> {
        current_owner_manifest(&self.store)
    }
}

impl MutationOwnerAuthority {
    /// Mint the one physical write capability for one bound serving scope.
    pub fn write_capability<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
    ) -> Result<PhysicalWriteCapability<'_, D>, String> {
        PhysicalWriteCapability::open(&self.store, owner)
    }

    /// Mint one write capability per member of an admitted **scope group**,
    /// all over ONE physical write transaction (RF-RULING-008).
    ///
    /// The graph shard's authoritative writer folds many scopes into one commit
    /// by design: the adaptive linger coalesces N graph-scoped batches into one
    /// fsync, and the Raft log and meta rows — keyed by Raft group, not by
    /// graph — ride that same fsync so graph state and consensus state become
    /// durable together. One transaction per scope would delete both. So the
    /// group is a kernel primitive rather than a relaxation of the per-scope
    /// bound: each returned capability binds exactly one serving scope, inside
    /// this one transaction, by the same [`crate::physical::binding`] check a
    /// sole writer runs, and each carries its own ledger row ACL. None of them
    /// can commit or abort; only the group can, through
    /// [`PhysicalWriteCapability::end_group_transaction`], which unwraps the
    /// shared transaction and therefore succeeds only once every member is
    /// gone.
    ///
    /// Fails closed on an empty group and on a repeated scope: two members on
    /// one scope would each own that scope's version, fence and receipt rows
    /// and would double-advance its authoritative version.
    pub fn group_write_capabilities<D: OwnerDomain>(
        &self,
        control: &OwnedStoreHandle<D>,
        members: &[&OwnedStoreHandle<D>],
    ) -> Result<Vec<PhysicalWriteCapability<'_, D>>, String> {
        // Everything that can refuse the group is checked BEFORE the one redb
        // write lock is taken, so a rejected group never blocks a live writer.
        if members.len() + 1 > MAX_SCOPE_GROUP_MEMBERS {
            return Err("admitted scope group exceeds its member budget".to_string());
        }
        if !is_control_scope(D::LAYOUT, control.identity()) {
            return Err(
                "a scope group's control member must be the file's reserved control scope"
                    .to_string(),
            );
        }
        let mut seen = BTreeSet::new();
        seen.insert(control.identity().binding_digest().to_hex());
        for owner in members {
            if is_control_scope(D::LAYOUT, owner.identity()) {
                return Err(
                    "a scope group's control scope may not also be a scoped member".to_string(),
                );
            }
            if !seen.insert(owner.identity().binding_digest().to_hex()) {
                return Err("an admitted scope group may not repeat a scope".to_string());
            }
        }

        let transaction = Arc::new(self.store.begin_write()?);
        let poison = Arc::new(AtomicBool::new(false));
        let mut capabilities = Vec::with_capacity(members.len() + 1);
        for owner in std::iter::once(&control).chain(members.iter()) {
            match PhysicalWriteCapability::open_member(
                &self.store,
                Arc::clone(&transaction),
                Arc::clone(&poison),
                owner,
            ) {
                Ok(capability) => capabilities.push(capability),
                Err(error) => {
                    // End the transaction we just opened rather than leaving it
                    // to `Drop`, so the write lock is released on a named path.
                    drop(capabilities);
                    if let Ok(transaction) = Arc::try_unwrap(transaction) {
                        let _ = transaction.abort();
                    }
                    return Err(error);
                }
            }
        }
        Ok(capabilities)
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        self.store.incarnation()
    }

    /// Digest of the persisted owner manifest anchored to this incarnation.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(current_owner_manifest(&self.store)?.authority_digest(self.store.incarnation()))
    }
}

/// Authenticate one exact logical serving identity against a store's declared
/// owner authority.
pub(crate) fn authenticate_scope_in<D: OwnerDomain>(
    store: &PhysicalStore,
    verifier: &dyn ScopeGrantVerifier,
    identity: MutationScopeIdentity,
    principal: String,
    proof: &[u8],
) -> Result<AuthenticatedScopeGrant<D>, String> {
    identity.validate_digest()?;
    let manifest = current_owner_manifest(store)?;
    if manifest.layout != D::LAYOUT || !manifest.layout.accepts(&identity) {
        return Err("serving scope is outside the declared owner layout".to_string());
    }
    permit_reserved_name(manifest.layout, &identity)?;
    verifier.verify(
        &manifest.physical_identity,
        manifest.layout,
        &identity,
        &principal,
        proof,
    )?;
    let authority_digest = manifest.authority_digest(store.incarnation());
    Ok(AuthenticatedScopeGrant::new(
        identity,
        principal,
        authority_digest,
    ))
}

/// Bind one authenticated grant once, yielding its owner handle.
pub(crate) fn bind_serving_scope_in<D: OwnerDomain>(
    store: &PhysicalStore,
    grant: AuthenticatedScopeGrant<D>,
    initial_version: u64,
) -> Result<OwnedStoreHandle<D>, String> {
    let manifest = current_owner_manifest(store)?;
    if manifest.layout != D::LAYOUT
        || manifest.authority_digest(store.incarnation()) != *grant.authority_digest()
        || !manifest.layout.accepts(grant.identity())
    {
        return Err("authenticated serving grant does not match this store".to_string());
    }
    let transaction = store.begin_write()?;
    bind_scope_in(
        &store.handle,
        &transaction,
        grant.identity(),
        initial_version,
    )?;
    transaction.commit().map_err(|error| error.to_string())?;
    let (identity, principal, authority_digest) = grant.into_parts();
    Ok(OwnedStoreHandle::new(identity, principal, authority_digest))
}

pub(crate) fn current_owner_manifest(store: &PhysicalStore) -> Result<OwnerManifest, String> {
    store.validate_physical_root()?;
    let cached = store.manifest();
    let transaction = store.begin_read()?;
    let persisted = validate_manifest_read(&transaction, &cached.physical_identity, cached.layout)?;
    validate_declared_owner_tables(&transaction, persisted.layout)?;
    if persisted != *cached {
        return Err("cached owner manifest differs from persisted authority".to_string());
    }
    Ok(persisted)
}

/// Create one current-format physical owner file at `path` under the default
/// open options.
pub(crate) fn create_physical(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<PhysicalStore, String> {
    create_physical_with(
        path,
        physical_identity,
        private_integrity,
        layout,
        StoreOpenOptions::default(),
    )
}

/// Create one current-format physical owner file at `path` under `options`.
pub(crate) fn create_physical_with(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
    options: StoreOpenOptions,
) -> Result<PhysicalStore, String> {
    if path.exists() {
        return Err("mutation store create target already exists".to_string());
    }
    let database = options.create_database(path)?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let manifest = OwnerManifest::new(physical_identity, layout)?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let handle = initialize_strict_in(&wtx, &incarnation, &manifest)?;
    open_declared_owner_tables(&wtx, layout)?;
    write_new_manifest(&wtx, &manifest)?;
    wtx.commit().map_err(|error| error.to_string())?;
    Ok(
        PhysicalStore::from_parts(database, handle, physical_path, private_integrity, manifest)
            .with_options(options),
    )
}

/// Open one exact current-format physical owner file under the default open
/// options.
pub(crate) fn open_physical(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<PhysicalStore, String> {
    open_physical_with(
        path,
        physical_identity,
        private_integrity,
        layout,
        StoreOpenOptions::default(),
    )
}

/// Open one exact current-format physical owner file under `options`.
pub(crate) fn open_physical_with(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
    options: StoreOpenOptions,
) -> Result<PhysicalStore, String> {
    physical_identity.validate()?;
    let database = options.open_database(path)?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let rtx = database.begin_read().map_err(|error| error.to_string())?;
    validate_incarnation_read(&rtx, &incarnation)?;
    let manifest = validate_manifest_read(&rtx, &physical_identity, layout)?;
    validate_declared_owner_tables(&rtx, layout)?;
    let authenticate = |sealed: &[u8], digest: &str| {
        authenticate_with(private_integrity.as_deref(), sealed, digest)
    };
    validate_recovery_content(&incarnation, &rtx, &authenticate)?;
    drop(rtx);
    Ok(PhysicalStore::from_parts(
        database,
        store_handle(incarnation),
        physical_path,
        private_integrity,
        manifest,
    )
    .with_options(options))
}
